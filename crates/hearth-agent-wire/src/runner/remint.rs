//! Binding remint / AttachResume policy for [`super::AttachRunner`].
//!
//! Bakes remint cuts 2026-09-27 through 2026-10-02 plus the 2026-10-06
//! fail-closed remint guard (acpx shared-runtime stance, not CLI silent
//! fork):
//!
//! 1. **Resume-not-load** — ACP `session/resume` only; no resume →
//!    [`RemintError::ResumeNotSupported`] (never `session/load`).
//! 2. **Cancel-before-reattach** — mid-turn disconnect → close/cancel
//!    before AttachResume; idle skips cancel.
//! 3. **Rehydrate-pending-permission** — [`RemintEvent::PermissionResurface`]
//!    from Host pending; do not cancel healthy HITL.
//! 4. **Finalize-orphaned-toolcalls** — after mid-turn cancel, append Host
//!    [`hearth::EventBody::ToolCallInterrupted`] (Indeterminate) for each
//!    unmatched `tool_call_id` in the Host EventLog; idempotent across a
//!    second remint; do not finalize healthy HITL. (2026-10-07 typed EventLog)
//! 5. **Truncated-replay-resync** — producer `truncated:true` → hydrate
//!    from Host EventLog watermark (`ResyncFromHost`), not partial tail
//!    or replay-from-zero.
//! 6. **Stale-teardown-skip-rebinding** — revalidate ownership under lock;
//!    skip close/detach if Session already rebound.
//! 7. **Fail-closed remint guard** — after a failed remint on an existing
//!    Session, only resume with the SAME agent session id OR a typed
//!    fail-closed Event. NEVER `session/new` under that Session. NEVER
//!    `session/load` as fallback. NEVER resubmit an in-flight prompt
//!    (mark turn interrupted). Gate resume on live `initialize` caps.
//!
//! SoftExpiring / Flush / Stage / Evidence / EffectId / Queue / seventh
//! noun / Gemini `session/load` stay parked. Typed remint keys orphans by
//! Host `tool_call_id` (2026-10-07) — never by tool name / string lists.

use std::sync::Mutex;

use hearth::{
    tool_call_is_terminal, unmatched_tool_calls, AgentId, Event, EventBody, InMemory, Member,
    Session as HostSession, ToolInterruptStatus, UserId,
};
use serde_json::Value;

/// Live agent capabilities from an ACP `initialize` result (not static
/// catalog flags alone).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiveCaps {
    /// Agent advertises `session/resume` (or `loadSession: false` with
    /// an explicit resume capability).
    pub resume: bool,
    /// Agent advertises `loadSession` / `session/load`. Copilot shape:
    /// load without resume — Hearth still fail-closes on remint.
    pub load_session: bool,
    pub cancel: bool,
    pub close: bool,
}

impl LiveCaps {
    /// Parse ACP `initialize` result. Looks under `agentCapabilities`
    /// and top-level `capabilities` (both shapes appear in the wild).
    pub fn from_initialize_result(result: &Value) -> Self {
        let caps = result
            .get("agentCapabilities")
            .or_else(|| result.get("capabilities"))
            .cloned()
            .unwrap_or(Value::Null);
        let load_session = flag(&caps, &["loadSession", "load_session", "sessionLoad"])
            || method_listed(&caps, "session/load");
        let resume = flag(&caps, &["sessionResume", "resume", "session_resume"])
            || method_listed(&caps, "session/resume");
        let cancel = flag(&caps, &["sessionCancel", "cancel", "promptCancel"])
            || method_listed(&caps, "session/cancel");
        let close =
            flag(&caps, &["sessionClose", "close"]) || method_listed(&caps, "session/close");
        Self {
            resume,
            load_session,
            cancel,
            close,
        }
    }

    /// Remint may AttachResume only when live initialize advertised resume.
    pub fn allows_resume(self) -> bool {
        self.resume
    }
}

fn flag(caps: &Value, names: &[&str]) -> bool {
    for name in names {
        if truthy(caps.get(*name)) {
            return true;
        }
    }
    // Nested ACP shape: agentCapabilities.session.{resume,loadSession,cancel,close}
    if let Some(session) = caps.get("session") {
        for name in names {
            if truthy(session.get(*name)) {
                return true;
            }
            let key = match *name {
                "sessionResume" | "resume" | "session_resume" => "resume",
                "loadSession" | "load_session" | "sessionLoad" => "loadSession",
                "sessionCancel" | "cancel" | "promptCancel" => "cancel",
                "sessionClose" | "close" => "close",
                other => other,
            };
            if truthy(session.get(key)) {
                return true;
            }
        }
    }
    false
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        Some(Value::Bool(true)) => true,
        Some(obj) if obj.is_object() => {
            obj.get("supported").and_then(|s| s.as_bool()) == Some(true)
        }
        _ => false,
    }
}

fn method_listed(caps: &Value, method: &str) -> bool {
    caps.get("methods")
        .and_then(|m| m.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).any(|m| m == method))
        .unwrap_or(false)
}

pub type SessionId = String;
pub type BindingId = String;
pub type AgentSessionId = String;
pub type ToolCallId = String;
pub type PermissionId = String;
pub type Seq = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemintEventKind {
    User,
    Agent,
    /// Remint-policy marker only. Tool terminals live on the Host EventLog
    /// as [`EventBody::ToolCallInterrupted`] (not here).
    TurnCancelled,
    PermissionRequested,
    PermissionResurface,
    PermissionResolved,
    ResyncFromHost,
    /// Typed fail-closed marker (never silent fork).
    FailClosed,
    AttachResumeHeld,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemintEvent {
    pub id: Seq,
    pub kind: RemintEventKind,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAction {
    SessionCancel,
    SessionClose,
    AttachResume,
    None,
}

/// Wire method the remint path is about to issue. Production remint only
/// ever emits [`Self::SessionResume`] (plus cancel/close beforehand).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemintWireMethod {
    SessionResume,
    SessionCancel,
    SessionClose,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemintError {
    /// Agent lacks live resume — fail closed (never fall through to load).
    ResumeNotSupported,
    /// Mid-turn remint but agent advertises neither cancel nor close.
    CancelNotSupported,
    /// Attempted `session/new` under an existing Session after failed remint.
    SilentForkBlocked,
    /// Attempted `session/load` as remint fallback (Copilot load-only shape).
    LoadFallbackBlocked,
    /// Attempted to resubmit an in-flight prompt after remint.
    PromptResubmitBlocked,
    /// Session was closed / gone.
    SessionGone,
    /// `resolve_permission` permission_id does not match the pending id
    /// (late answer to an earlier request after remint resurface).
    StalePermission,
}

impl RemintError {
    pub fn as_event_text(&self) -> &'static str {
        match self {
            Self::ResumeNotSupported => "ResumeNotSupported",
            Self::CancelNotSupported => "CancelNotSupported",
            Self::SilentForkBlocked => "SilentForkBlocked",
            Self::LoadFallbackBlocked => "LoadFallbackBlocked",
            Self::PromptResubmitBlocked => "PromptResubmitBlocked",
            Self::SessionGone => "SessionGone",
            Self::StalePermission => "StalePermission",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolStatus {
    Pending,
    Running,
    Completed,
    /// Orphan interrupted — indeterminate external fate (not Cancelled).
    Interrupted,
}

impl ToolStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Interrupted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPermission {
    pub id: PermissionId,
    pub title: String,
    pub options: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySlice {
    pub events: Vec<RemintEvent>,
    pub truncated: bool,
    pub latest_seq: Seq,
    pub first_retained_seq: Seq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportOwner {
    Binding(BindingId),
    Detached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemintOutcome {
    pub session_id: SessionId,
    pub binding_id: BindingId,
    pub agent_session_id: AgentSessionId,
    pub wire_actions: Vec<WireAction>,
    pub resumed: bool,
    pub agent_context_restored: bool,
    pub event_count: usize,
    pub permission_resurfaced: bool,
    pub tools_finalized: usize,
    pub resynced_from_host: bool,
    pub turn_interrupted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeardownSnapshot {
    pub session_id: SessionId,
    pub old_binding_id: BindingId,
    pub observed_owner: TransportOwner,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeardownOutcome {
    pub skipped: bool,
    pub remint_survives: bool,
    pub closed: bool,
    pub transport_owner: TransportOwner,
    pub binding_id: BindingId,
}

struct Inner {
    session_id: SessionId,
    binding_id: BindingId,
    next_binding: u64,
    agent_session_id: AgentSessionId,
    live_caps: LiveCaps,
    /// Remint-policy markers (FailClosed, AttachResumeHeld, …). Tool
    /// Call / Result / Interrupted live only on [`Self::host_session`].
    events: Vec<RemintEvent>,
    next_event: Seq,
    turn_in_flight: bool,
    /// In-flight user prompt text (never auto-resubmitted after remint).
    in_flight_prompt: Option<String>,
    pending_permission: Option<PendingPermission>,
    attach_cursor: Seq,
    transport_owner: TransportOwner,
    closed: bool,
    /// Producer bounded ring for truncated-replay tests.
    producer_ring: Vec<RemintEvent>,
    producer_ring_cap: usize,
    /// Keep InMemory alive for the Host Session EventLog.
    _store: InMemory,
    host_session: HostSession,
    host_agent: AgentId,
    host_user: UserId,
}

/// Host-owned remint state for AttachRunner. Session-first: remint never
/// mints `session/new` under an existing Session.
pub struct RemintSession {
    inner: Mutex<Inner>,
}

impl RemintSession {
    pub fn open(
        session_id: impl Into<String>,
        agent_session_id: impl Into<String>,
        live_caps: LiveCaps,
    ) -> (Self, SessionId, BindingId) {
        let session_id = session_id.into();
        let binding_id = "bind-1".to_string();
        let host_store = InMemory::new();
        let host_user = host_store.create_user("remint-user");
        let host_agent = host_store.create_agent("remint-agent", "");
        let host_session = host_store.create_session();
        host_session
            .join(Member::User(host_user.id))
            .expect("join remint user");
        host_session
            .join(Member::Agent(host_agent.id))
            .expect("join remint agent");
        let store = Self {
            inner: Mutex::new(Inner {
                session_id: session_id.clone(),
                binding_id: binding_id.clone(),
                next_binding: 2,
                agent_session_id: agent_session_id.into(),
                live_caps,
                events: Vec::new(),
                next_event: 1,
                turn_in_flight: false,
                in_flight_prompt: None,
                pending_permission: None,
                attach_cursor: 0,
                transport_owner: TransportOwner::Binding(binding_id.clone()),
                closed: false,
                producer_ring: Vec::new(),
                producer_ring_cap: 3,
                _store: host_store,
                host_session,
                host_agent: host_agent.id,
                host_user: host_user.id,
            }),
        };
        (store, session_id, binding_id)
    }

    pub fn session_id(&self) -> SessionId {
        self.inner.lock().unwrap().session_id.clone()
    }

    pub fn binding_id(&self) -> BindingId {
        self.inner.lock().unwrap().binding_id.clone()
    }

    pub fn agent_session_id(&self) -> AgentSessionId {
        self.inner.lock().unwrap().agent_session_id.clone()
    }

    pub fn live_caps(&self) -> LiveCaps {
        self.inner.lock().unwrap().live_caps
    }

    pub fn set_live_caps(&self, caps: LiveCaps) {
        self.inner.lock().unwrap().live_caps = caps;
    }

    pub fn event_count(&self) -> usize {
        self.inner.lock().unwrap().events.len()
    }

    pub fn observe(&self) -> Vec<RemintEvent> {
        self.inner.lock().unwrap().events.clone()
    }

    pub fn transport_owner(&self) -> TransportOwner {
        self.inner.lock().unwrap().transport_owner.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().unwrap().closed
    }

    pub fn attach_cursor(&self) -> Seq {
        self.inner.lock().unwrap().attach_cursor
    }

    pub fn host_watermark(&self) -> Seq {
        let g = self.inner.lock().unwrap();
        g.events.last().map(|e| e.id).unwrap_or(0)
    }

    pub fn pending_permission(&self) -> Option<PendingPermission> {
        self.inner.lock().unwrap().pending_permission.clone()
    }

    /// Derive tool statuses from the Host EventLog (keyed by tool_call_id).
    pub fn open_tool_statuses(&self) -> Vec<(ToolCallId, ToolStatus)> {
        let g = self.inner.lock().unwrap();
        let events = g.host_session.events().expect("host events");
        let mut statuses: Vec<(ToolCallId, ToolStatus)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for e in &events {
            if let EventBody::ToolCall { tool_call_id, .. } = &e.body {
                if seen.insert(tool_call_id.clone()) {
                    let st = if tool_call_is_terminal(&events, tool_call_id) {
                        // Distinguish Result vs Interrupted.
                        let interrupted = events.iter().any(|x| {
                            matches!(
                                &x.body,
                                EventBody::ToolCallInterrupted {
                                    tool_call_id: id,
                                    ..
                                } if id == tool_call_id
                            )
                        });
                        if interrupted {
                            ToolStatus::Interrupted
                        } else {
                            ToolStatus::Completed
                        }
                    } else {
                        ToolStatus::Running
                    };
                    statuses.push((tool_call_id.clone(), st));
                }
            }
        }
        statuses
    }

    /// Host EventLog (tool Call / Result / Interrupted live here).
    pub fn host_events(&self) -> Vec<Event> {
        self.inner
            .lock()
            .unwrap()
            .host_session
            .events()
            .expect("host events")
    }

    pub fn host_agent_id(&self) -> AgentId {
        self.inner.lock().unwrap().host_agent
    }

    pub fn turn_in_flight(&self) -> bool {
        self.inner.lock().unwrap().turn_in_flight
    }

    fn push_event(g: &mut Inner, kind: RemintEventKind, text: impl Into<String>) -> RemintEvent {
        let id = g.next_event;
        g.next_event += 1;
        let ev = RemintEvent {
            id,
            kind,
            text: text.into(),
        };
        g.events.push(ev.clone());
        g.producer_ring.push(ev.clone());
        if g.producer_ring.len() > g.producer_ring_cap {
            let overflow = g.producer_ring.len() - g.producer_ring_cap;
            g.producer_ring.drain(..overflow);
        }
        ev
    }

    /// On fail-closed remint that abandons an in-flight turn, mark it
    /// interrupted before returning — never leave `turn_in_flight` sticky.
    fn mark_interrupted_if_in_flight(g: &mut Inner, why: &str) {
        if g.turn_in_flight {
            Self::push_event(g, RemintEventKind::TurnCancelled, why);
            g.turn_in_flight = false;
            g.in_flight_prompt = None;
        }
    }

    pub fn append_turn(&self, user: &str, agent: &str) {
        let mut g = self.inner.lock().unwrap();
        Self::push_event(&mut g, RemintEventKind::User, user);
        Self::push_event(&mut g, RemintEventKind::Agent, agent);
        g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(0);
    }

    pub fn begin_turn(&self, user: &str) {
        let mut g = self.inner.lock().unwrap();
        Self::push_event(&mut g, RemintEventKind::User, user);
        g.turn_in_flight = true;
        g.in_flight_prompt = Some(user.to_string());
    }

    pub fn complete_turn(&self, agent: &str) {
        let mut g = self.inner.lock().unwrap();
        Self::push_event(&mut g, RemintEventKind::Agent, agent);
        g.turn_in_flight = false;
        g.in_flight_prompt = None;
        g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(0);
    }

    pub fn request_permission(&self, id: &str, title: &str, options: &[&str]) {
        let mut g = self.inner.lock().unwrap();
        g.pending_permission = Some(PendingPermission {
            id: id.into(),
            title: title.into(),
            options: options.iter().map(|s| (*s).to_string()).collect(),
        });
        Self::push_event(
            &mut g,
            RemintEventKind::PermissionRequested,
            format!("{id}:{title}"),
        );
        // Permission-wait is not orphan mid-turn for cancel purposes.
        g.turn_in_flight = true;
    }

    /// Resolve the pending permission. `permission_id` must match the
    /// current pending id so a late answer to an earlier request cannot
    /// resolve the current one after remint resurfaces.
    pub fn resolve_permission(
        &self,
        permission_id: &str,
        option_id: &str,
    ) -> Result<(), RemintError> {
        let mut g = self.inner.lock().unwrap();
        let Some(pending) = g.pending_permission.clone() else {
            return Err(RemintError::SessionGone);
        };
        if pending.id != permission_id {
            // Do not take() — keep current pending for the matching id.
            return Err(RemintError::StalePermission);
        }
        g.pending_permission = None;
        Self::push_event(
            &mut g,
            RemintEventKind::PermissionResolved,
            format!("{}:{option_id}", pending.id),
        );
        let _ = g.host_session.decide_permission(
            format!("{}:{}", pending.id, pending.title),
            option_id == "allow" || option_id.starts_with("allow"),
            g.host_user,
        );
        g.turn_in_flight = false;
        g.in_flight_prompt = None;
        Ok(())
    }

    /// Append a Host [`EventBody::ToolCall`] keyed by `tool_call_id`.
    pub fn start_tool(&self, tool_call_id: &str, title: &str) {
        let mut g = self.inner.lock().unwrap();
        g.host_session
            .append(EventBody::ToolCall {
                agent: g.host_agent,
                tool_call_id: tool_call_id.into(),
                name: title.into(),
                input: "{}".into(),
            })
            .expect("append ToolCall");
        g.turn_in_flight = true;
    }

    /// Append a Host [`EventBody::ToolResult`]. No-op (dropped) if the
    /// call is already terminal (including ToolCallInterrupted).
    pub fn complete_tool(&self, tool_call_id: &str) -> bool {
        self.append_tool_result(tool_call_id, "ok")
    }

    /// Append ToolResult by tool_call_id. Returns false if dropped as stale
    /// after an interrupted marker (one call never gets two terminals).
    pub fn append_tool_result(&self, tool_call_id: &str, output: &str) -> bool {
        let g = self.inner.lock().unwrap();
        let events = g.host_session.events().expect("host events");
        if tool_call_is_terminal(&events, tool_call_id) {
            return false;
        }
        // Recover tool name from the matching ToolCall when present.
        let name = events
            .iter()
            .rev()
            .find_map(|e| match &e.body {
                EventBody::ToolCall {
                    tool_call_id: id,
                    name,
                    ..
                } if id == tool_call_id => Some(name.clone()),
                _ => None,
            })
            .unwrap_or_else(|| tool_call_id.to_string());
        g.host_session
            .append(EventBody::ToolResult {
                agent: g.host_agent,
                tool_call_id: tool_call_id.into(),
                name,
                output: output.into(),
            })
            .expect("append ToolResult");
        true
    }

    /// Count Host ToolCallInterrupted events for `tool_call_id`.
    pub fn interrupted_marker_count(&self, tool_call_id: &str) -> usize {
        self.host_events()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::ToolCallInterrupted {
                        tool_call_id: id,
                        ..
                    } if id == tool_call_id
                )
            })
            .count()
    }

    pub fn set_attach_cursor(&self, seq: Seq) {
        self.inner.lock().unwrap().attach_cursor = seq;
    }

    pub fn set_producer_ring_cap(&self, cap: usize) {
        let mut g = self.inner.lock().unwrap();
        g.producer_ring_cap = cap.max(1);
        if g.producer_ring.len() > g.producer_ring_cap {
            let overflow = g.producer_ring.len() - g.producer_ring_cap;
            g.producer_ring.drain(..overflow);
        }
    }

    pub fn producer_events_since(&self, last_seen: Seq) -> ReplaySlice {
        let g = self.inner.lock().unwrap();
        let first_retained = g.producer_ring.first().map(|e| e.id).unwrap_or(0);
        let truncated =
            (last_seen > 0 || !g.producer_ring.is_empty()) && last_seen + 1 < first_retained;
        let events: Vec<_> = g
            .producer_ring
            .iter()
            .filter(|e| e.id > last_seen)
            .cloned()
            .collect();
        let latest = g.events.last().map(|e| e.id).unwrap_or(0);
        ReplaySlice {
            events,
            truncated,
            latest_seq: latest,
            first_retained_seq: first_retained,
        }
    }

    /// Correct remint path: compose cancel / permission / finalize /
    /// truncated-resync / resume. Never `session/new`, never `session/load`.
    pub fn remint_and_attach(&self) -> Result<RemintOutcome, RemintError> {
        let mut g = self.inner.lock().unwrap();
        if g.closed {
            return Err(RemintError::SessionGone);
        }

        // Gate on live initialize caps (not static flags alone).
        if !g.live_caps.allows_resume() {
            // Copilot load-only: still fail closed — never session/load fallback.
            let reason = if g.live_caps.load_session {
                RemintError::LoadFallbackBlocked
            } else {
                RemintError::ResumeNotSupported
            };
            Self::push_event(&mut g, RemintEventKind::FailClosed, reason.as_event_text());
            Self::mark_interrupted_if_in_flight(&mut g, reason.as_event_text());
            return Err(reason);
        }

        let permission_pending = g.pending_permission.is_some();
        let mid_turn = g.turn_in_flight && !permission_pending;
        let mut wire_actions = Vec::new();
        let mut tools_finalized = 0usize;
        let mut turn_interrupted = false;

        // Cancel-before-reattach (09-28): mid-turn orphan only.
        // Rehydrate-pending-permission (09-29): do NOT cancel healthy HITL.
        if mid_turn {
            let action = if g.live_caps.close {
                WireAction::SessionClose
            } else if g.live_caps.cancel {
                WireAction::SessionCancel
            } else {
                let reason = RemintError::CancelNotSupported;
                Self::push_event(&mut g, RemintEventKind::FailClosed, reason.as_event_text());
                Self::mark_interrupted_if_in_flight(&mut g, reason.as_event_text());
                return Err(reason);
            };
            wire_actions.push(action);
            Self::push_event(&mut g, RemintEventKind::TurnCancelled, "mid-turn");
            turn_interrupted = true;

            // Finalize-orphaned-toolcalls (09-30 / 10-07): Host EventLog
            // by tool_call_id. Idempotent — already-interrupted ids skip.
            let host_log = g.host_session.events().expect("host events");
            let orphans = unmatched_tool_calls(&host_log);
            for (agent, tool_id, name) in orphans {
                // Re-check terminal under the same lock (double remint).
                let latest = g.host_session.events().expect("host events");
                if tool_call_is_terminal(&latest, &tool_id) {
                    continue;
                }
                g.host_session
                    .append(EventBody::ToolCallInterrupted {
                        agent,
                        tool_call_id: tool_id,
                        name,
                        status: ToolInterruptStatus::Indeterminate,
                    })
                    .expect("append ToolCallInterrupted");
                tools_finalized += 1;
            }
            g.turn_in_flight = false;
            // NEVER resubmit in-flight prompt after remint.
            g.in_flight_prompt = None;
        }

        // Truncated-replay-resync (10-01).
        let cursor = g.attach_cursor;
        let first_retained = g.producer_ring.first().map(|e| e.id).unwrap_or(0);
        let host_wm = g.events.last().map(|e| e.id).unwrap_or(0);
        let truncated = cursor > 0 && !g.producer_ring.is_empty() && first_retained > cursor + 1;
        let mut resynced_from_host = false;
        if truncated {
            // Hydrate from Host EventLog after cursor — not producer tail.
            Self::push_event(
                &mut g,
                RemintEventKind::ResyncFromHost,
                format!("cursor={cursor};watermark={host_wm}"),
            );
            resynced_from_host = true;
            g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(cursor);
        } else if !g.events.is_empty() {
            // Contiguous: advance to Host watermark without full resync.
            g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(cursor);
        }

        // Remint Binding + AttachResume (09-27). Same agent_session_id.
        let new_binding = format!("bind-{}", g.next_binding);
        g.next_binding += 1;
        g.binding_id = new_binding.clone();
        g.transport_owner = TransportOwner::Binding(new_binding.clone());
        wire_actions.push(WireAction::AttachResume);
        let agent_sid = g.agent_session_id.clone();
        Self::push_event(&mut g, RemintEventKind::AttachResumeHeld, agent_sid);

        let mut permission_resurfaced = false;
        if let Some(ref pending) = g.pending_permission.clone() {
            Self::push_event(
                &mut g,
                RemintEventKind::PermissionResurface,
                format!("{}:{}", pending.id, pending.title),
            );
            permission_resurfaced = true;
            // Turn stays pending until resolve_permission.
        }

        // Cursor tracks Host watermark after remint markers land.
        g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(g.attach_cursor);

        Ok(RemintOutcome {
            session_id: g.session_id.clone(),
            binding_id: new_binding,
            agent_session_id: g.agent_session_id.clone(),
            wire_actions,
            resumed: true,
            agent_context_restored: true,
            event_count: g.events.len(),
            permission_resurfaced,
            tools_finalized,
            resynced_from_host,
            turn_interrupted,
        })
    }

    /// After a failed remint, only resume(same id) or typed fail-closed
    /// Event are allowed. Blocks silent fork / load fallback / prompt
    /// resubmit (2026-10-06 guard).
    pub fn after_failed_remint(
        &self,
        attempted: RemintWireMethod,
    ) -> Result<RemintWireMethod, RemintError> {
        let mut g = self.inner.lock().unwrap();
        if g.closed {
            return Err(RemintError::SessionGone);
        }
        match attempted {
            RemintWireMethod::SessionResume => {
                if !g.live_caps.allows_resume() {
                    let reason = if g.live_caps.load_session {
                        RemintError::LoadFallbackBlocked
                    } else {
                        RemintError::ResumeNotSupported
                    };
                    Self::push_event(&mut g, RemintEventKind::FailClosed, reason.as_event_text());
                    return Err(reason);
                }
                Ok(RemintWireMethod::SessionResume)
            }
            RemintWireMethod::SessionCancel => Ok(RemintWireMethod::SessionCancel),
            RemintWireMethod::SessionClose => Ok(RemintWireMethod::SessionClose),
        }
    }

    /// Explicitly refuse `session/new` under an existing Session.
    pub fn refuse_session_new(&self) -> RemintError {
        let mut g = self.inner.lock().unwrap();
        let err = RemintError::SilentForkBlocked;
        Self::push_event(&mut g, RemintEventKind::FailClosed, err.as_event_text());
        err
    }

    /// Explicitly refuse `session/load` as remint fallback.
    pub fn refuse_session_load(&self) -> RemintError {
        let mut g = self.inner.lock().unwrap();
        let err = RemintError::LoadFallbackBlocked;
        Self::push_event(&mut g, RemintEventKind::FailClosed, err.as_event_text());
        err
    }

    /// Explicitly refuse resubmitting an in-flight prompt after remint.
    pub fn refuse_prompt_resubmit(&self) -> RemintError {
        let mut g = self.inner.lock().unwrap();
        let err = RemintError::PromptResubmitBlocked;
        Self::push_event(&mut g, RemintEventKind::FailClosed, err.as_event_text());
        // Mark turn interrupted; do not clear EventLog.
        if g.turn_in_flight {
            Self::push_event(
                &mut g,
                RemintEventKind::TurnCancelled,
                "prompt-resubmit-blocked",
            );
            g.turn_in_flight = false;
            g.in_flight_prompt = None;
        }
        err
    }

    pub fn snapshot_teardown_for_binding(&self, old_bid: &str) -> TeardownSnapshot {
        let g = self.inner.lock().unwrap();
        TeardownSnapshot {
            session_id: g.session_id.clone(),
            old_binding_id: old_bid.into(),
            observed_owner: g.transport_owner.clone(),
        }
    }

    /// Stale-teardown-skip-rebinding (10-02): revalidate under lock.
    pub fn apply_teardown(&self, snapshot: &TeardownSnapshot) -> TeardownOutcome {
        let mut g = self.inner.lock().unwrap();
        let still_owns = match &g.transport_owner {
            TransportOwner::Binding(bid) => bid == &snapshot.old_binding_id,
            TransportOwner::Detached => false,
        };
        if !still_owns {
            // Session already rebound — skip close/detach.
            return TeardownOutcome {
                skipped: true,
                remint_survives: !g.closed,
                closed: g.closed,
                transport_owner: g.transport_owner.clone(),
                binding_id: g.binding_id.clone(),
            };
        }
        // Idle path: teardown still owns → close + detach.
        g.closed = true;
        g.transport_owner = TransportOwner::Detached;
        TeardownOutcome {
            skipped: false,
            remint_survives: false,
            closed: true,
            transport_owner: TransportOwner::Detached,
            binding_id: g.binding_id.clone(),
        }
    }

    /// Map a remint EventLog marker onto a Host [`hearth::EventBody`] text
    /// for append by callers that own a [`hearth::Session`].
    pub fn fail_closed_host_text(err: &RemintError) -> String {
        format!("remint:{}", err.as_event_text())
    }
}

/// Encode ACP wire JSON for remint actions (tests / AttachRunner write path).
pub fn encode_remint_rpc(method: RemintWireMethod, agent_session_id: &str, id: u64) -> Value {
    let method_str = match method {
        RemintWireMethod::SessionResume => "session/resume",
        RemintWireMethod::SessionCancel => "session/cancel",
        RemintWireMethod::SessionClose => "session/close",
    };
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method_str,
        "params": { "sessionId": agent_session_id }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps_resume() -> LiveCaps {
        LiveCaps {
            resume: true,
            load_session: false,
            cancel: true,
            close: true,
        }
    }

    #[test]
    fn live_caps_from_initialize_resume_and_load() {
        let resume = serde_json::json!({
            "agentCapabilities": {
                "session": { "resume": true, "close": true },
                "promptCancel": true
            }
        });
        let c = LiveCaps::from_initialize_result(&resume);
        assert!(c.resume);
        assert!(c.close);
        assert!(c.cancel);
        assert!(!c.load_session);

        let load_only = serde_json::json!({
            "agentCapabilities": { "loadSession": true }
        });
        let c = LiveCaps::from_initialize_result(&load_only);
        assert!(!c.resume);
        assert!(c.load_session);
        assert!(!c.allows_resume());
    }

    #[test]
    fn resume_not_load_ids_and_log_stable() {
        let (s, sid, bid0) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("hello", "hi");
        s.append_turn("next", "ok");
        let n0 = s.event_count();
        let aid0 = s.agent_session_id();
        let out = s.remint_and_attach().unwrap();
        assert_eq!(out.session_id, sid);
        assert_ne!(out.binding_id, bid0);
        assert_eq!(out.agent_session_id, aid0);
        assert!(out.resumed);
        assert!(out.agent_context_restored);
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        assert!(!out.wire_actions.contains(&WireAction::SessionCancel));
        // AttachResumeHeld marker only — no replay doubling.
        assert_eq!(s.event_count(), n0 + 1);
        assert_eq!(s.agent_session_id(), aid0);
    }

    #[test]
    fn no_resume_fail_closed_never_load() {
        let (s, _, bid0) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: false,
                load_session: false,
                cancel: true,
                close: true,
            },
        );
        s.append_turn("u", "a");
        let n0 = s.event_count();
        let err = s.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::ResumeNotSupported);
        assert_eq!(s.binding_id(), bid0);
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported"));
        assert_eq!(s.event_count(), n0 + 1);
    }

    #[test]
    fn load_only_caps_fail_closed_not_session_load() {
        let (s, _, _) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: false,
                load_session: true,
                ..LiveCaps::default()
            },
        );
        s.append_turn("u", "a");
        let err = s.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        assert!(s.observe().iter().any(|e| e.text == "LoadFallbackBlocked"));
    }

    #[test]
    fn cancel_before_reattach_mid_turn() {
        let (s, _, bid0) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("a", "b");
        s.begin_turn("in-flight");
        let out = s.remint_and_attach().unwrap();
        assert_ne!(out.binding_id, bid0);
        assert!(out.turn_interrupted);
        assert!(out.wire_actions.contains(&WireAction::SessionClose));
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::TurnCancelled));
        assert!(!s.turn_in_flight());
    }

    #[test]
    fn idle_remint_skips_cancel() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("a", "b");
        let out = s.remint_and_attach().unwrap();
        assert!(!out.turn_interrupted);
        assert!(!out.wire_actions.contains(&WireAction::SessionCancel));
        assert!(!out.wire_actions.contains(&WireAction::SessionClose));
        assert!(!s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::TurnCancelled));
    }

    #[test]
    fn rehydrate_pending_permission_no_cancel() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("need-perm");
        s.request_permission("p1", "Allow shell?", &["allow", "deny"]);
        let out = s.remint_and_attach().unwrap();
        assert!(out.permission_resurfaced);
        assert!(!out.turn_interrupted);
        assert!(!out.wire_actions.contains(&WireAction::SessionCancel));
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::PermissionResurface));
        assert!(s.pending_permission().is_some());
        assert!(s.turn_in_flight());
    }

    #[test]
    fn finalize_orphaned_toolcalls_after_mid_turn_cancel() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("work");
        s.start_tool("t1", "bash");
        s.start_tool("t2", "read");
        assert!(s.complete_tool("t2"));
        let out = s.remint_and_attach().unwrap();
        assert_eq!(out.tools_finalized, 1);
        let statuses = s.open_tool_statuses();
        assert!(statuses
            .iter()
            .any(|(id, st)| id == "t1" && *st == ToolStatus::Interrupted));
        assert!(statuses
            .iter()
            .any(|(id, st)| id == "t2" && *st == ToolStatus::Completed));
        assert_eq!(s.interrupted_marker_count("t1"), 1);
        assert!(s.host_events().iter().any(|e| matches!(
            &e.body,
            EventBody::ToolCallInterrupted {
                tool_call_id,
                status: ToolInterruptStatus::Indeterminate,
                ..
            } if tool_call_id == "t1"
        )));
        // Interrupted is Host Event — not a RemintEvent string list.
        assert!(!s
            .observe()
            .iter()
            .any(|e| e.text.contains("cancelled") || e.text.contains("t1:bash")));
    }

    #[test]
    fn orphans_keyed_by_tool_call_id_parallel_same_name() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("parallel");
        s.start_tool("x1", "bash");
        s.start_tool("x2", "bash");
        assert!(s.complete_tool("x1"));
        let out = s.remint_and_attach().unwrap();
        assert_eq!(out.tools_finalized, 1);
        assert_eq!(s.interrupted_marker_count("x2"), 1);
        assert_eq!(s.interrupted_marker_count("x1"), 0);
        let statuses = s.open_tool_statuses();
        assert!(statuses
            .iter()
            .any(|(id, st)| id == "x1" && *st == ToolStatus::Completed));
        assert!(statuses
            .iter()
            .any(|(id, st)| id == "x2" && *st == ToolStatus::Interrupted));
    }

    #[test]
    fn double_remint_writes_one_interrupted_marker() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("work");
        s.start_tool("t1", "bash");
        s.start_tool("t2", "bash");
        assert!(s.complete_tool("t1"));
        let o1 = s.remint_and_attach().unwrap();
        assert_eq!(o1.tools_finalized, 1);
        // Second remint: turn no longer in-flight; marker already terminal.
        let o2 = s.remint_and_attach().unwrap();
        assert_eq!(o2.tools_finalized, 0);
        assert_eq!(s.interrupted_marker_count("t2"), 1);
        assert_eq!(s.interrupted_marker_count("t1"), 0);
    }

    #[test]
    fn late_tool_result_after_interrupted_is_dropped() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("work");
        s.start_tool("t2", "bash");
        let _ = s.remint_and_attach().unwrap();
        assert_eq!(s.interrupted_marker_count("t2"), 1);
        // Straggler result from old Binding — must not create a second terminal.
        assert!(!s.append_tool_result("t2", "late-ok"));
        let terminals = s
            .host_events()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::ToolResult {
                        tool_call_id,
                        ..
                    }
                    | EventBody::ToolCallInterrupted {
                        tool_call_id,
                        ..
                    } if tool_call_id == "t2"
                )
            })
            .count();
        assert_eq!(terminals, 1);
    }

    #[test]
    fn resolve_permission_bound_to_pending_id() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("need-perm");
        s.request_permission("p1", "Allow shell?", &["allow", "deny"]);
        let _ = s.remint_and_attach().unwrap();
        // Late answer for an earlier id must not resolve current pending.
        let err = s.resolve_permission("p0-stale", "allow").unwrap_err();
        assert_eq!(err, RemintError::StalePermission);
        assert!(s.pending_permission().is_some());
        s.resolve_permission("p1", "allow").unwrap();
        assert!(s.pending_permission().is_none());
    }

    /// Fake ACP peer that refuses `session/new` under an existing Session.
    /// Double remint keeps the same agent session id and one interrupted marker.
    #[test]
    fn fake_acp_refuses_session_new_double_remint() {
        struct FakeAcp {
            methods: Vec<String>,
            agent_session_id: String,
        }
        impl FakeAcp {
            fn dispatch(
                &mut self,
                method: &str,
                session: &RemintSession,
            ) -> Result<(), RemintError> {
                self.methods.push(method.to_string());
                match method {
                    "session/new" => Err(session.refuse_session_new()),
                    "session/load" => Err(session.refuse_session_load()),
                    "session/resume" => {
                        assert_eq!(session.agent_session_id(), self.agent_session_id);
                        Ok(())
                    }
                    "session/cancel" | "session/close" => Ok(()),
                    other => panic!("unexpected method {other}"),
                }
            }
        }

        let (s, _, _) = RemintSession::open("sess-1", "S-stable", caps_resume());
        let mut peer = FakeAcp {
            methods: Vec::new(),
            agent_session_id: "S-stable".into(),
        };
        s.begin_turn("work");
        s.start_tool("t1", "bash");
        s.start_tool("t2", "bash");
        assert!(s.complete_tool("t1"));

        let o1 = s.remint_and_attach().unwrap();
        for a in &o1.wire_actions {
            let method = match a {
                WireAction::SessionClose => "session/close",
                WireAction::SessionCancel => "session/cancel",
                WireAction::AttachResume => "session/resume",
                WireAction::None => continue,
            };
            peer.dispatch(method, &s).unwrap();
        }
        assert_eq!(s.interrupted_marker_count("t2"), 1);

        let o2 = s.remint_and_attach().unwrap();
        for a in &o2.wire_actions {
            if let WireAction::AttachResume = a {
                peer.dispatch("session/resume", &s).unwrap();
            }
        }
        assert_eq!(s.interrupted_marker_count("t2"), 1);
        assert_eq!(s.agent_session_id(), "S-stable");

        // Remint wire path never emitted session/new — only close + resume.
        assert!(peer.methods.iter().all(|m| m != "session/new"));
        assert!(peer.methods.iter().any(|m| m == "session/resume"));
        assert!(peer.methods.iter().any(|m| m == "session/close"));
        // acpx CLI would session/new here — fake ACP + remint refuse.
        let err = peer.dispatch("session/new", &s).unwrap_err();
        assert_eq!(err, RemintError::SilentForkBlocked);
        assert_eq!(
            peer.methods.iter().filter(|m| *m == "session/new").count(),
            1
        );
        // encode helper still never produces session/new.
        let allowed = encode_remint_rpc(RemintWireMethod::SessionResume, "S-stable", 1);
        assert_ne!(allowed["method"], "session/new");
    }

    #[test]
    fn permission_pending_does_not_finalize_tools_as_orphan() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("hitl");
        s.start_tool("t1", "bash");
        s.request_permission("p1", "Allow?", &["allow"]);
        // Tool left running under healthy HITL — do not cancel/finalize.
        let out = s.remint_and_attach().unwrap();
        assert_eq!(out.tools_finalized, 0);
        assert!(out.permission_resurfaced);
        assert!(!out.turn_interrupted);
        let statuses = s.open_tool_statuses();
        assert!(statuses
            .iter()
            .any(|(id, st)| id == "t1" && *st == ToolStatus::Running));
    }

    #[test]
    fn truncated_replay_resync_from_host_watermark() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.set_producer_ring_cap(2);
        s.append_turn("a", "b"); // ids 1,2
        s.append_turn("c", "d"); // ids 3,4 — ring keeps 3,4
        s.set_attach_cursor(1); // predates retained ring
        let slice = s.producer_events_since(1);
        assert!(slice.truncated);
        let out = s.remint_and_attach().unwrap();
        assert!(out.resynced_from_host);
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::ResyncFromHost));
        assert_eq!(s.attach_cursor(), s.host_watermark());
    }

    #[test]
    fn contiguous_replay_skips_resync_marker() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("a", "b");
        s.set_attach_cursor(s.host_watermark());
        let out = s.remint_and_attach().unwrap();
        assert!(!out.resynced_from_host);
        assert!(!s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::ResyncFromHost));
    }

    #[test]
    fn stale_teardown_skips_when_rebinding() {
        let (s, _, bid0) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("a", "b");
        let snap = s.snapshot_teardown_for_binding(&bid0);
        let out = s.remint_and_attach().unwrap();
        assert_ne!(out.binding_id, bid0);
        let td = s.apply_teardown(&snap);
        assert!(td.skipped);
        assert!(td.remint_survives);
        assert!(!td.closed);
        assert_eq!(
            td.transport_owner,
            TransportOwner::Binding(out.binding_id.clone())
        );
        assert!(!s.is_closed());
    }

    #[test]
    fn idle_teardown_while_owning_closes() {
        let (s, _, bid0) = RemintSession::open("sess-1", "agent-1", caps_resume());
        let snap = s.snapshot_teardown_for_binding(&bid0);
        let td = s.apply_teardown(&snap);
        assert!(!td.skipped);
        assert!(td.closed);
        assert_eq!(td.transport_owner, TransportOwner::Detached);
    }

    #[test]
    fn silent_fork_hole_blocked_after_failed_remint() {
        let (s, _, _) = RemintSession::open(
            "sess-1",
            "agent-old",
            LiveCaps {
                resume: false,
                load_session: false,
                ..LiveCaps::default()
            },
        );
        s.append_turn("u", "a");
        let aid0 = s.agent_session_id();
        let _ = s.remint_and_attach().unwrap_err();
        // CLI acpx would session/new here — Hearth refuses.
        let err = s.refuse_session_new();
        assert_eq!(err, RemintError::SilentForkBlocked);
        assert_eq!(s.agent_session_id(), aid0);
        assert!(s.observe().iter().any(|e| e.text == "SilentForkBlocked"));
    }

    #[test]
    fn after_failed_remint_load_fallback_blocked() {
        let (s, _, _) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: false,
                load_session: true,
                ..LiveCaps::default()
            },
        );
        let err = s
            .after_failed_remint(RemintWireMethod::SessionResume)
            .unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        let err = s.refuse_session_load();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
    }

    #[test]
    fn never_resubmit_in_flight_prompt_after_remint() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("do the thing");
        let out = s.remint_and_attach().unwrap();
        assert!(out.turn_interrupted);
        let err = s.refuse_prompt_resubmit();
        assert_eq!(err, RemintError::PromptResubmitBlocked);
        // Prompt was cleared on remint; second refuse still fail-closes.
        assert!(!s.turn_in_flight());
    }

    #[test]
    fn encode_remint_rpc_is_resume_not_new_or_load() {
        let v = encode_remint_rpc(RemintWireMethod::SessionResume, "S-abc", 7);
        assert_eq!(v["method"], "session/resume");
        assert_eq!(v["params"]["sessionId"], "S-abc");
        assert_ne!(v["method"], "session/new");
        assert_ne!(v["method"], "session/load");
    }

    #[test]
    fn mid_turn_without_cancel_or_close_fail_closed() {
        let (s, _, bid0) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: true,
                load_session: false,
                cancel: false,
                close: false,
            },
        );
        s.begin_turn("x");
        let err = s.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::CancelNotSupported);
        assert_eq!(s.binding_id(), bid0);
        // Must mark interrupted before return — sticky in-flight is a hole.
        assert!(!s.turn_in_flight());
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::TurnCancelled));
    }

    #[test]
    fn mid_turn_resume_not_supported_marks_interrupted() {
        let (s, _, bid0) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: false,
                load_session: false,
                cancel: true,
                close: true,
            },
        );
        s.begin_turn("in-flight");
        let err = s.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::ResumeNotSupported);
        assert_eq!(s.binding_id(), bid0);
        assert!(!s.turn_in_flight());
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::TurnCancelled));
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported"));
    }

    #[test]
    fn mid_turn_load_only_fail_closed_marks_interrupted() {
        let (s, _, bid0) = RemintSession::open(
            "sess-1",
            "agent-1",
            LiveCaps {
                resume: false,
                load_session: true,
                cancel: true,
                close: true,
            },
        );
        s.begin_turn("in-flight");
        let err = s.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        assert_eq!(s.binding_id(), bid0);
        assert!(!s.turn_in_flight());
        assert!(s
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::TurnCancelled));
    }

    #[test]
    fn double_resume_keeps_agent_session_id() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.append_turn("a", "b");
        let aid = s.agent_session_id();
        let o1 = s.remint_and_attach().unwrap();
        let o2 = s.remint_and_attach().unwrap();
        assert_ne!(o1.binding_id, o2.binding_id);
        assert_eq!(o1.agent_session_id, aid);
        assert_eq!(o2.agent_session_id, aid);
    }
}
