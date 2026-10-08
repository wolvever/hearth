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
//! 3. **Rehydrate-pending-permission** — [`RemintEventKind::PermissionResurface`]
//!    from Host pending; do not cancel healthy HITL. (2026-10-08) Pending
//!    asks are a map keyed by `(Binding, JSON-RPC id)`; remint moves the old
//!    Binding's asks to *resurfaced* and NEVER answers a dead Binding's rpc
//!    id. A resumed agent's re-ask adopts a resurfaced ask only when its
//!    `toolCallId` is unique (copilot-cli #989 reuses ids).
//! 4. **Finalize-orphaned-toolcalls** — after mid-turn cancel, append Host
//!    [`hearth::EventBody::ToolCallInterrupted`] (Indeterminate) for each
//!    unmatched `tool_call_id` via [`hearth::Session::finalize_unmatched_tool_calls`]
//!    (shared with crash reclaim / `resume_interrupted_turn`); idempotent
//!    across a second remint; do not finalize healthy HITL. (2026-10-07)
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
//! ACP `session/request_permission` is a JSON-RPC request: answers go out
//! as [`AgentCommand::ReplyPermission`] on the same rpc id with a nested
//! `outcome` and only an offered `optionId`; [`RemintSession::cancel_turn`]
//! answers every pending ask `cancelled` (claude-agent-acp #851).
//!
//! SoftExpiring / Flush / Stage / Evidence / EffectId / Queue / seventh
//! noun / Gemini `session/load` stay parked. Typed remint keys orphans by
//! Host `tool_call_id` (2026-10-07) — never by tool name / string lists.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use hearth::{
    tool_call_is_terminal, AgentId, Event, EventBody, Host, InMemory, Member, PermissionRpc,
    Session as HostSession, UserId,
};
use serde_json::Value;

use crate::host::option_allows;
use crate::{AgentCommand, AgentEvent, PermissionOption, RpcId};

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
/// Pending-ask key: the Binding that carried the JSON-RPC request plus its id.
pub type PermissionKey = (BindingId, RpcId);

/// Typed address of one permission ask on a remint marker (not a string).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRef {
    pub binding: BindingId,
    pub rpc_id: RpcId,
    pub tool_call_id: Option<ToolCallId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemintEventKind {
    User,
    Agent,
    /// Remint-policy marker only. Tool terminals live on the Host EventLog
    /// as [`EventBody::ToolCallInterrupted`] (not here).
    TurnCancelled,
    PermissionRequested(PermissionRef),
    /// Old Binding's ask shown again after remint; its rpc id is dead.
    PermissionResurface(PermissionRef),
    /// Resumed agent re-asked a resurfaced ask (unique toolCallId match).
    PermissionReasked {
        old: PermissionRef,
        new: PermissionRef,
    },
    /// Answered (`text` = optionId) or cancelled (`text` = "cancelled").
    PermissionResolved(PermissionRef),
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
    /// `resolve_permission` key is not pending (already answered /
    /// cancelled / never asked).
    StalePermission,
    /// Answer aimed at a dead Binding's rpc id (resurfaced after remint).
    /// Never written to the wire — the new process reuses rpc ids.
    DeadBinding,
    /// `optionId` was not offered on that ask.
    OptionNotOffered,
    /// Agent reused a JSON-RPC id that is still pending on this Binding.
    DuplicateRpcId,
    /// Event is not a JSON-RPC permission request (no rpc id).
    NotAPermissionRequest,
    /// Host EventLog write (PermissionAsked / PermissionDecided) failed;
    /// the ask's state is unchanged and nothing is written to the wire.
    HostLog(String),
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
            Self::DeadBinding => "DeadBinding",
            Self::OptionNotOffered => "OptionNotOffered",
            Self::DuplicateRpcId => "DuplicateRpcId",
            Self::NotAPermissionRequest => "NotAPermissionRequest",
            Self::HostLog(_) => "HostLog",
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

/// One permission ask awaiting a decision, keyed by `(binding, rpc_id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPermission {
    pub binding: BindingId,
    pub rpc_id: RpcId,
    /// Not unique across concurrent asks (copilot-cli #989).
    pub tool_call_id: Option<ToolCallId>,
    pub title: String,
    pub options: Vec<PermissionOption>,
}

impl PendingPermission {
    pub fn key(&self) -> PermissionKey {
        (self.binding.clone(), self.rpc_id.clone())
    }

    pub fn as_ref(&self) -> PermissionRef {
        PermissionRef {
            binding: self.binding.clone(),
            rpc_id: self.rpc_id.clone(),
            tool_call_id: self.tool_call_id.clone(),
        }
    }
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
    /// Bumped by [`RemintSession::begin_turn`]; a late terminal for an
    /// older turn never clears a newer one.
    turn: u64,
    /// In-flight user prompt text (never auto-resubmitted after remint).
    in_flight_prompt: Option<String>,
    /// Live asks on the current Binding — answerable on the wire.
    pending: BTreeMap<PermissionKey, PendingPermission>,
    /// Asks from dead Bindings — shown to the user, never answered on wire.
    resurfaced: BTreeMap<PermissionKey, PendingPermission>,
    /// Remint Binding id → Host [`hearth::BindingId`] for typed Host Events.
    host_bindings: HashMap<BindingId, hearth::BindingId>,
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
        let agent_session_id: AgentSessionId = agent_session_id.into();
        let host_binding = host_session
            .bind_host(
                Some(host_agent.id),
                Host::from_bind("acp", Some(agent_session_id.clone()), None),
            )
            .expect("bind remint host");
        let mut host_bindings = HashMap::new();
        host_bindings.insert(binding_id.clone(), host_binding.id);
        let store = Self {
            inner: Mutex::new(Inner {
                session_id: session_id.clone(),
                binding_id: binding_id.clone(),
                next_binding: 2,
                agent_session_id,
                live_caps,
                events: Vec::new(),
                next_event: 1,
                turn_in_flight: false,
                turn: 0,
                in_flight_prompt: None,
                pending: BTreeMap::new(),
                resurfaced: BTreeMap::new(),
                host_bindings,
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

    /// Live asks on the current Binding, ordered by `(binding, rpc_id)`.
    pub fn pending_permissions(&self) -> Vec<PendingPermission> {
        self.inner
            .lock()
            .unwrap()
            .pending
            .values()
            .cloned()
            .collect()
    }

    /// Asks from dead Bindings shown after remint (never answerable on wire).
    pub fn resurfaced_permissions(&self) -> Vec<PendingPermission> {
        self.inner
            .lock()
            .unwrap()
            .resurfaced
            .values()
            .cloned()
            .collect()
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

    /// Start a prompt turn; returns its turn number for [`Self::turn_ended`].
    pub fn begin_turn(&self, user: &str) -> u64 {
        let mut g = self.inner.lock().unwrap();
        Self::push_event(&mut g, RemintEventKind::User, user);
        g.turn += 1;
        g.turn_in_flight = true;
        g.in_flight_prompt = Some(user.to_string());
        g.turn
    }

    /// The agent ended `turn` (its `session/prompt` response arrived, e.g.
    /// `stopReason: "cancelled"` after [`Self::cancel_turn`]). Clears the
    /// in-flight turn only if `turn` is still the current one; a late
    /// terminal for an older turn is ignored. Returns whether it applied.
    pub fn turn_ended(&self, turn: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        if turn != g.turn || !g.turn_in_flight {
            return false;
        }
        g.turn_in_flight = false;
        g.in_flight_prompt = None;
        true
    }

    pub fn complete_turn(&self, agent: &str) {
        let mut g = self.inner.lock().unwrap();
        Self::push_event(&mut g, RemintEventKind::Agent, agent);
        g.turn_in_flight = false;
        g.in_flight_prompt = None;
        g.attach_cursor = g.events.last().map(|e| e.id).unwrap_or(0);
    }

    fn host_rpc(g: &Inner, p: &PendingPermission) -> Result<PermissionRpc, RemintError> {
        let binding = g
            .host_bindings
            .get(&p.binding)
            .ok_or_else(|| RemintError::HostLog(format!("no Host Binding for {}", p.binding)))?;
        Ok(PermissionRpc {
            binding: *binding,
            rpc_id: p.rpc_id.to_string(),
            tool_call_id: p.tool_call_id.clone(),
        })
    }

    /// Record a decision on the Host EventLog (`option_id: None` =
    /// cancelled). Errors propagate (occupancy / store) — callers run this
    /// before mutating pending state or emitting any wire command.
    fn host_decide(
        g: &Inner,
        p: &PendingPermission,
        option_id: Option<&str>,
        allowed: bool,
    ) -> Result<(), RemintError> {
        let rpc = Self::host_rpc(g, p)?;
        g.host_session
            .decide_permission_rpc(
                p.title.clone(),
                allowed,
                g.host_user,
                rpc,
                option_id.map(str::to_string),
            )
            .map(|_| ())
            .map_err(|e| RemintError::HostLog(e.to_string()))
    }

    /// Record a decoded ACP `session/request_permission` (must carry its
    /// JSON-RPC id). See [`Self::request_permission`].
    pub fn on_permission_ask(&self, ev: &AgentEvent) -> Result<PermissionRef, RemintError> {
        match ev {
            AgentEvent::PermissionAsk {
                rpc_id: Some(rpc_id),
                tool_item_id,
                title,
                options,
                ..
            } => self.request_permission(
                rpc_id.clone(),
                tool_item_id.as_deref(),
                title,
                options.clone(),
            ),
            _ => Err(RemintError::NotAPermissionRequest),
        }
    }

    /// Record an in-flight permission ask on the **current** Binding, keyed
    /// by `(binding, rpc_id)`. A second concurrent ask never overwrites the
    /// first. If the resumed agent re-asks a resurfaced ask and its
    /// `toolCallId` is unique (exactly one resurfaced match and no live ask
    /// sharing it), that resurfaced entry is adopted (removed); otherwise
    /// nothing is routed by `toolCallId`.
    pub fn request_permission(
        &self,
        rpc_id: RpcId,
        tool_call_id: Option<&str>,
        title: &str,
        options: Vec<PermissionOption>,
    ) -> Result<PermissionRef, RemintError> {
        let mut g = self.inner.lock().unwrap();
        let key = (g.binding_id.clone(), rpc_id.clone());
        if g.pending.contains_key(&key) {
            return Err(RemintError::DuplicateRpcId);
        }
        let ask = PendingPermission {
            binding: key.0.clone(),
            rpc_id,
            tool_call_id: tool_call_id.map(str::to_string),
            title: title.into(),
            options,
        };
        let new_ref = ask.as_ref();
        // Host EventLog first: on failure nothing is adopted or tracked.
        let rpc = Self::host_rpc(&g, &ask)?;
        g.host_session
            .ask_permission_rpc(g.host_agent, title.to_string(), rpc)
            .map_err(|e| RemintError::HostLog(e.to_string()))?;
        if let Some(tc) = tool_call_id {
            let live_shares = g
                .pending
                .values()
                .any(|p| p.tool_call_id.as_deref() == Some(tc));
            let matches: Vec<PermissionKey> = g
                .resurfaced
                .iter()
                .filter(|(_, p)| p.tool_call_id.as_deref() == Some(tc))
                .map(|(k, _)| k.clone())
                .collect();
            if !live_shares && matches.len() == 1 {
                if let Some(old) = g.resurfaced.remove(&matches[0]) {
                    Self::push_event(
                        &mut g,
                        RemintEventKind::PermissionReasked {
                            old: old.as_ref(),
                            new: new_ref.clone(),
                        },
                        title,
                    );
                }
            }
        }
        g.pending.insert(key, ask);
        Self::push_event(
            &mut g,
            RemintEventKind::PermissionRequested(new_ref.clone()),
            title,
        );
        // Permission-wait is not orphan mid-turn for cancel purposes.
        g.turn_in_flight = true;
        Ok(new_ref)
    }

    /// Answer one ask with `{outcome:"selected", optionId}` on its own rpc
    /// id. Returns the [`AgentCommand::ReplyPermission`] to write on the
    /// live wire. Errors (nothing is written):
    /// - [`RemintError::DeadBinding`] — key belongs to a reminted-away Binding;
    /// - [`RemintError::StalePermission`] — not pending;
    /// - [`RemintError::OptionNotOffered`] — ask stays pending.
    pub fn resolve_permission(
        &self,
        binding: &str,
        rpc_id: &RpcId,
        option_id: &str,
    ) -> Result<AgentCommand, RemintError> {
        let mut g = self.inner.lock().unwrap();
        if g.closed {
            return Err(RemintError::SessionGone);
        }
        let key = (binding.to_string(), rpc_id.clone());
        if binding != g.binding_id || g.resurfaced.contains_key(&key) {
            return Err(RemintError::DeadBinding);
        }
        let Some(ask) = g.pending.get(&key).cloned() else {
            return Err(RemintError::StalePermission);
        };
        let Some(opt) = ask.options.iter().find(|o| o.option_id == option_id) else {
            return Err(RemintError::OptionNotOffered);
        };
        let allowed = option_allows(opt);
        // Host decision first: on failure the ask stays pending, no reply.
        Self::host_decide(&g, &ask, Some(option_id), allowed)?;
        g.pending.remove(&key);
        Self::push_event(
            &mut g,
            RemintEventKind::PermissionResolved(ask.as_ref()),
            option_id,
        );
        if g.pending.is_empty() && g.resurfaced.is_empty() {
            g.turn_in_flight = false;
            g.in_flight_prompt = None;
        }
        Ok(AgentCommand::ReplyPermission {
            session_id: g.agent_session_id.clone(),
            permission_id: rpc_id.to_string(),
            allow: allowed,
            option_id: Some(option_id.into()),
            rpc_id: Some(rpc_id.clone()),
        })
    }

    /// User cancel: answer **every** live ask `{outcome:"cancelled"}` on its
    /// own rpc id, then `session/cancel` ([`AgentCommand::Abort`]). Resurfaced
    /// asks from dead Bindings are dropped as cancelled on the Host log only —
    /// never written to the wire. Pending and resurfaced maps end empty.
    ///
    /// Host decisions are recorded first; if one fails ([`RemintError::HostLog`])
    /// no command is returned and both maps are left as they were.
    ///
    /// The turn stays in flight: a returned `session/cancel` is not known
    /// delivered (the wire may be dead). It clears on [`Self::turn_ended`];
    /// a remint before that still runs Cancel-before-reattach.
    pub fn cancel_turn(&self) -> Result<Vec<AgentCommand>, RemintError> {
        let mut g = self.inner.lock().unwrap();
        for ask in g.pending.values().chain(g.resurfaced.values()) {
            Self::host_decide(&g, ask, None, false)?;
        }
        let mut out = Vec::new();
        let live = std::mem::take(&mut g.pending);
        let dead = std::mem::take(&mut g.resurfaced);
        for ask in live.values() {
            out.push(AgentCommand::ReplyPermission {
                session_id: g.agent_session_id.clone(),
                permission_id: ask.rpc_id.to_string(),
                allow: false,
                option_id: None,
                rpc_id: Some(ask.rpc_id.clone()),
            });
        }
        for ask in live.values().chain(dead.values()) {
            Self::push_event(
                &mut g,
                RemintEventKind::PermissionResolved(ask.as_ref()),
                "cancelled",
            );
        }
        out.push(AgentCommand::Abort {
            session_id: g.agent_session_id.clone(),
        });
        if g.turn_in_flight {
            Self::push_event(&mut g, RemintEventKind::TurnCancelled, "cancel");
        }
        Ok(out)
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

        // Live or resurfaced asks are healthy HITL (also across a 2nd remint).
        let permission_pending = !g.pending.is_empty() || !g.resurfaced.is_empty();
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

            // Finalize-orphaned-toolcalls (09-30 / 10-07): shared Host
            // helper (same as Runtime::resume_interrupted_turn). Keyed by
            // tool_call_id; Indeterminate; idempotent.
            tools_finalized = g
                .host_session
                .finalize_unmatched_tool_calls()
                .expect("finalize unmatched tool calls");
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
        // Host Binding swap first: on failure the remint Binding is unchanged.
        if let Some(old_host) = g.host_bindings.get(&g.binding_id).copied() {
            match g.host_session.unbind(old_host) {
                // Already released (e.g. agent left): release is idempotent.
                Ok(_) | Err(hearth::Error::UnknownBinding(_)) => {}
                Err(e) => return Err(RemintError::HostLog(e.to_string())),
            }
        }
        let host_agent = g.host_agent;
        let host_binding = g
            .host_session
            .bind_host(
                Some(host_agent),
                Host::from_bind("acp", Some(g.agent_session_id.clone()), None),
            )
            .map_err(|e| RemintError::HostLog(e.to_string()))?;
        let new_binding = format!("bind-{}", g.next_binding);
        g.next_binding += 1;
        g.binding_id = new_binding.clone();
        g.host_bindings.insert(new_binding.clone(), host_binding.id);
        g.transport_owner = TransportOwner::Binding(new_binding.clone());
        wire_actions.push(WireAction::AttachResume);
        let agent_sid = g.agent_session_id.clone();
        Self::push_event(&mut g, RemintEventKind::AttachResumeHeld, agent_sid);

        // Old Binding's rpc ids are dead: move live asks to resurfaced and
        // never answer them on the new wire. Pending map ends empty.
        let moved = std::mem::take(&mut g.pending);
        g.resurfaced.extend(moved);
        let resurfaced: Vec<PendingPermission> = g.resurfaced.values().cloned().collect();
        for ask in &resurfaced {
            Self::push_event(
                &mut g,
                RemintEventKind::PermissionResurface(ask.as_ref()),
                ask.title.clone(),
            );
        }
        // Turn stays pending until the resumed agent re-asks / cancel.
        let permission_resurfaced = !resurfaced.is_empty();

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
    use crate::adapters::acp::AcpCodec;
    use crate::adapters::AdapterCodec;
    use crate::transport::WireFrame;
    use hearth::ToolInterruptStatus;

    fn opts() -> Vec<PermissionOption> {
        vec![
            PermissionOption {
                option_id: "allow-once".into(),
                name: "Allow".into(),
                kind: "allow_once".into(),
            },
            PermissionOption {
                option_id: "reject-once".into(),
                name: "Reject".into(),
                kind: "reject_once".into(),
            },
        ]
    }

    /// Fake ACP peer. Refuses `session/new` / `session/load` under an
    /// existing Session (PR #13) and, like a spec agent, blocks each
    /// `session/request_permission` until a JSON-RPC *response* with that
    /// exact id and a nested `result.outcome` arrives. Notifications and
    /// unknown / dead ids never resolve an ask.
    struct FakeAcp {
        methods: Vec<String>,
        agent_session_id: String,
        next_rpc: i64,
        /// rpc id → the agent's own (true) tool call id.
        waiting: BTreeMap<RpcId, String>,
        /// (true tool call id, outcome) in delivery order.
        got: Vec<(String, Value)>,
        /// Frames that resolved nothing (dead id / notification / bad shape).
        ignored: Vec<Value>,
        /// copilot-cli #989: advertise this toolCallId on every ask.
        shared_tool_call_id: Option<&'static str>,
    }

    impl FakeAcp {
        fn new(agent_session_id: &str) -> Self {
            Self {
                methods: Vec::new(),
                agent_session_id: agent_session_id.into(),
                next_rpc: 100,
                waiting: BTreeMap::new(),
                got: Vec::new(),
                ignored: Vec::new(),
                shared_tool_call_id: None,
            }
        }

        fn dispatch(&mut self, method: &str, session: &RemintSession) -> Result<(), RemintError> {
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

        /// Agent → client `session/request_permission` (ACP v1 shape).
        fn ask(&mut self, tool_call_id: &str, title: &str) -> WireFrame {
            self.next_rpc += 1;
            let rpc = RpcId::Num(self.next_rpc);
            self.waiting.insert(rpc.clone(), tool_call_id.into());
            let advertised = self.shared_tool_call_id.unwrap_or(tool_call_id);
            WireFrame::Json(serde_json::json!({
                "jsonrpc": "2.0",
                "id": rpc.to_json(),
                "method": "session/request_permission",
                "params": {
                    "sessionId": self.agent_session_id,
                    "toolCall": {"toolCallId": advertised, "title": title},
                    "options": [
                        {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"},
                        {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"}
                    ]
                }
            }))
        }

        /// Client → agent frame.
        fn deliver(&mut self, frame: WireFrame) {
            let WireFrame::Json(v) = frame else {
                panic!("ACP is JSON-RPC");
            };
            if let Some(method) = v.get("method").and_then(|m| m.as_str()) {
                if method == "session/cancel" {
                    self.methods.push(method.into());
                } else {
                    self.ignored.push(v);
                }
                return;
            }
            let Some(rpc) = v.get("id").and_then(RpcId::from_json) else {
                self.ignored.push(v);
                return;
            };
            let outcome = v.pointer("/result/outcome").cloned();
            let ok_shape = outcome
                .as_ref()
                .and_then(|o| o.get("outcome"))
                .and_then(|o| o.as_str())
                .is_some_and(|o| o == "selected" || o == "cancelled");
            match (self.waiting.contains_key(&rpc), ok_shape) {
                (true, true) => {
                    let tc = self.waiting.remove(&rpc).unwrap();
                    self.got.push((tc, outcome.unwrap()));
                }
                _ => self.ignored.push(v),
            }
        }

        fn hung(&self) -> bool {
            !self.waiting.is_empty()
        }

        fn outcome(&self, tool_call_id: &str) -> Option<&Value> {
            self.got
                .iter()
                .find(|(tc, _)| tc == tool_call_id)
                .map(|(_, o)| o)
        }
    }

    /// Wire path: fake agent frame → AcpCodec decode → RemintSession.
    fn ask_via_codec(
        s: &RemintSession,
        peer: &mut FakeAcp,
        tool_call_id: &str,
        title: &str,
    ) -> Result<PermissionRef, RemintError> {
        let frame = peer.ask(tool_call_id, title);
        let ev = AcpCodec.decode_event(&frame).unwrap().unwrap();
        s.on_permission_ask(&ev)
    }

    /// Wire path: RemintSession command → AcpCodec encode → fake agent.
    fn send_via_codec(peer: &mut FakeAcp, cmd: &AgentCommand) {
        peer.deliver(AcpCodec.encode_command(cmd).unwrap());
    }

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
        s.request_permission(RpcId::Num(1), Some("t1"), "Allow shell?", opts())
            .unwrap();
        let out = s.remint_and_attach().unwrap();
        assert!(out.permission_resurfaced);
        assert!(!out.turn_interrupted);
        assert!(!out.wire_actions.contains(&WireAction::SessionCancel));
        assert!(s.observe().iter().any(|e| matches!(
            &e.kind,
            RemintEventKind::PermissionResurface(r)
                if r.binding == "bind-1" && r.rpc_id == RpcId::Num(1)
                    && r.tool_call_id.as_deref() == Some("t1")
        )));
        assert_eq!(s.resurfaced_permissions().len(), 1);
        assert!(s.pending_permissions().is_empty());
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
    fn resolve_permission_bound_to_binding_and_rpc_id() {
        let (s, _, _) = RemintSession::open("sess-1", "agent-1", caps_resume());
        s.begin_turn("need-perm");
        s.request_permission(RpcId::Num(1), Some("t1"), "Allow shell?", opts())
            .unwrap();
        let out = s.remint_and_attach().unwrap();
        // Late answer to the dead Binding's rpc id is refused, never emitted.
        let err = s
            .resolve_permission("bind-1", &RpcId::Num(1), "allow-once")
            .unwrap_err();
        assert_eq!(err, RemintError::DeadBinding);
        // Same rpc id on the live Binding was never asked there.
        let err = s
            .resolve_permission(&out.binding_id, &RpcId::Num(1), "allow-once")
            .unwrap_err();
        assert_eq!(err, RemintError::StalePermission);
        assert_eq!(s.resurfaced_permissions().len(), 1);
        // Resumed agent re-asks on the new Binding (rpc ids restart).
        s.request_permission(RpcId::Num(1), Some("t1"), "Allow shell?", opts())
            .unwrap();
        assert!(s.resurfaced_permissions().is_empty());
        let cmd = s
            .resolve_permission(&out.binding_id, &RpcId::Num(1), "allow-once")
            .unwrap();
        assert!(matches!(
            cmd,
            AgentCommand::ReplyPermission { rpc_id: Some(RpcId::Num(1)), ref option_id, .. }
                if option_id.as_deref() == Some("allow-once")
        ));
        assert!(s.pending_permissions().is_empty());
    }

    /// Fake ACP peer that refuses `session/new` under an existing Session.
    /// Double remint keeps the same agent session id and one interrupted marker.
    #[test]
    fn fake_acp_refuses_session_new_double_remint() {
        let (s, _, _) = RemintSession::open("sess-1", "S-stable", caps_resume());
        let mut peer = FakeAcp::new("S-stable");
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
        s.request_permission(RpcId::Num(1), Some("t1"), "Allow?", opts())
            .unwrap();
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

    fn assert_maps_empty(s: &RemintSession, when: &str) {
        assert!(
            s.pending_permissions().is_empty(),
            "pending leak {when}: {:?}",
            s.pending_permissions()
        );
        assert!(
            s.resurfaced_permissions().is_empty(),
            "resurfaced leak {when}: {:?}",
            s.resurfaced_permissions()
        );
    }

    /// claude-agent-acp #851: controller + background subagent ask at once.
    /// Each is answered on its own rpc id, out of order; neither hangs.
    #[test]
    fn fake_acp_two_concurrent_asks_answered_by_rpc_id() {
        let (s, _, bid) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut peer = FakeAcp::new("S-1");
        s.begin_turn("work");
        let ctrl = ask_via_codec(&s, &mut peer, "call_ctrl", "git log").unwrap();
        let sub = ask_via_codec(&s, &mut peer, "toolu_sub", "npm test").unwrap();
        assert_ne!(ctrl.rpc_id, sub.rpc_id);
        assert_eq!(
            s.pending_permissions().len(),
            2,
            "second ask must not overwrite"
        );

        let c = s
            .resolve_permission(&bid, &sub.rpc_id, "allow-once")
            .unwrap();
        send_via_codec(&mut peer, &c);
        assert_eq!(s.pending_permissions().len(), 1);
        let c = s
            .resolve_permission(&bid, &ctrl.rpc_id, "reject-once")
            .unwrap();
        send_via_codec(&mut peer, &c);

        assert!(!peer.hung());
        assert!(peer.ignored.is_empty());
        assert_eq!(
            peer.outcome("toolu_sub").unwrap(),
            &serde_json::json!({"outcome": "selected", "optionId": "allow-once"})
        );
        assert_eq!(
            peer.outcome("call_ctrl").unwrap(),
            &serde_json::json!({"outcome": "selected", "optionId": "reject-once"})
        );
        assert_maps_empty(&s, "after answers");
        // Double answer is stale, nothing emitted.
        assert_eq!(
            s.resolve_permission(&bid, &sub.rpc_id, "allow-once"),
            Err(RemintError::StalePermission)
        );
    }

    /// copilot-cli #989: both asks advertise toolCallId "shell-permission".
    /// Routing is by (Binding, rpc id), so each gets its own answer.
    #[test]
    fn fake_acp_shared_tool_call_id_routes_by_rpc_id() {
        let (s, _, bid) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut peer = FakeAcp::new("S-1");
        peer.shared_tool_call_id = Some("shell-permission");
        s.begin_turn("rm");
        let a = ask_via_codec(&s, &mut peer, "t1", "rm a").unwrap();
        let b = ask_via_codec(&s, &mut peer, "t2", "rm b").unwrap();
        assert_eq!(a.tool_call_id.as_deref(), Some("shell-permission"));
        assert_eq!(b.tool_call_id.as_deref(), Some("shell-permission"));
        assert_eq!(s.pending_permissions().len(), 2);

        send_via_codec(
            &mut peer,
            &s.resolve_permission(&bid, &a.rpc_id, "allow-once").unwrap(),
        );
        send_via_codec(
            &mut peer,
            &s.resolve_permission(&bid, &b.rpc_id, "reject-once")
                .unwrap(),
        );
        assert!(!peer.hung());
        assert_eq!(peer.outcome("t1").unwrap()["optionId"], "allow-once");
        assert_eq!(peer.outcome("t2").unwrap()["optionId"], "reject-once");
        assert_maps_empty(&s, "after shared-id answers");

        // Host Event carries typed rpc id + toolCallId, not "id:title".
        let asked: Vec<_> = s
            .host_events()
            .into_iter()
            .filter_map(|e| match e.body {
                EventBody::PermissionAsked { rpc: Some(rpc), .. } => Some(rpc),
                _ => None,
            })
            .collect();
        assert_eq!(asked.len(), 2);
        assert_eq!(asked[0].rpc_id, a.rpc_id.to_string());
        assert_eq!(asked[1].rpc_id, b.rpc_id.to_string());
        assert!(asked
            .iter()
            .all(|r| r.tool_call_id.as_deref() == Some("shell-permission")));
        assert_eq!(asked[0].binding, asked[1].binding);
        let decided = s
            .host_events()
            .into_iter()
            .filter(|e| matches!(&e.body, EventBody::PermissionDecided { rpc: Some(_), .. }))
            .count();
        assert_eq!(decided, 2);
    }

    /// Only an optionId the agent offered is accepted; the ask stays pending.
    #[test]
    fn fake_acp_unoffered_option_is_typed_error() {
        let (s, _, bid) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut peer = FakeAcp::new("S-1");
        let a = ask_via_codec(&s, &mut peer, "t1", "x").unwrap();
        assert_eq!(
            s.resolve_permission(&bid, &a.rpc_id, "allow"),
            Err(RemintError::OptionNotOffered)
        );
        assert_eq!(
            s.resolve_permission(&bid, &a.rpc_id, ""),
            Err(RemintError::OptionNotOffered)
        );
        assert_eq!(s.pending_permissions().len(), 1);
        assert!(peer.hung());
        send_via_codec(
            &mut peer,
            &s.resolve_permission(&bid, &a.rpc_id, "allow-once").unwrap(),
        );
        assert!(!peer.hung());
        assert_maps_empty(&s, "after valid answer");
        // A non-request event (no rpc id) is a typed error too.
        let notif = AgentEvent::PermissionAsk {
            session_id: "S-1".into(),
            permission_id: "perm".into(),
            title: "x".into(),
            description: None,
            tool_item_id: None,
            options: opts(),
            rpc_id: None,
        };
        assert_eq!(
            s.on_permission_ask(&notif),
            Err(RemintError::NotAPermissionRequest)
        );
        // Reused in-flight rpc id is refused, not overwritten.
        s.request_permission(RpcId::Num(7), Some("t7"), "x", opts())
            .unwrap();
        assert_eq!(
            s.request_permission(RpcId::Num(7), Some("t8"), "y", opts()),
            Err(RemintError::DuplicateRpcId)
        );
        assert_eq!(s.pending_permissions().len(), 1);
    }

    /// session/cancel: every pending ask is answered `cancelled` on its own
    /// rpc id, then session/cancel goes out; no ask left hanging.
    #[test]
    fn fake_acp_cancel_answers_every_pending_ask_cancelled() {
        let (s, _, _) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut peer = FakeAcp::new("S-1");
        peer.shared_tool_call_id = Some("shell-permission");
        let turn = s.begin_turn("work");
        ask_via_codec(&s, &mut peer, "t1", "x").unwrap();
        ask_via_codec(&s, &mut peer, "t2", "y").unwrap();
        let cmds = s.cancel_turn().unwrap();
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds.last(), Some(AgentCommand::Abort { .. })));
        for c in &cmds {
            send_via_codec(&mut peer, c);
        }
        assert!(!peer.hung());
        assert_eq!(peer.got.len(), 2);
        assert!(peer
            .got
            .iter()
            .all(|(_, o)| o == &serde_json::json!({"outcome": "cancelled"})));
        assert!(peer.methods.iter().any(|m| m == "session/cancel"));
        assert!(s.turn_in_flight(), "in flight until the prompt response");
        assert!(s.turn_ended(turn));
        assert!(!s.turn_in_flight());
        assert_maps_empty(&s, "after cancel");
    }

    /// Cancel is sent but the wire is dead (never delivered). A later
    /// remint must still Cancel-before-reattach and finalize the tool.
    #[test]
    fn cancel_then_remint_with_lost_cancel_still_cancels() {
        let (s, _, _) = RemintSession::open("sess-1", "S-1", caps_resume());
        s.begin_turn("work");
        s.start_tool("t1", "build");
        let cmds = s.cancel_turn().unwrap();
        assert!(matches!(cmds.last(), Some(AgentCommand::Abort { .. })));
        // Abort dropped on the floor: no prompt response ever arrives.
        let o = s.remint_and_attach().unwrap();
        assert!(o.turn_interrupted);
        assert!(o
            .wire_actions
            .iter()
            .any(|a| matches!(a, WireAction::SessionClose | WireAction::SessionCancel)));
        assert_eq!(o.tools_finalized, 1);
        assert!(!s.turn_in_flight());
    }

    /// A late prompt response for cancelled turn N must not clear turn N+1.
    #[test]
    fn late_terminal_for_old_turn_leaves_new_turn_in_flight() {
        let (s, _, _) = RemintSession::open("sess-1", "S-1", caps_resume());
        let n = s.begin_turn("first");
        s.cancel_turn().unwrap();
        let n1 = s.begin_turn("second");
        assert_ne!(n, n1);
        assert!(!s.turn_ended(n), "stale terminal ignored");
        assert!(s.turn_in_flight());
        assert!(s.turn_ended(n1));
        assert!(!s.turn_in_flight());
        assert!(!s.turn_ended(n1), "idempotent");
    }

    /// Remint with asks in flight, twice. Old Binding's rpc ids are dead:
    /// resurfaced, never answered (the new process restarts ids, so 101
    /// collides). Resumed agent re-asks: unique toolCallId adopts the
    /// resurfaced ask; shared toolCallId does not. Healthy HITL is not
    /// cancelled on either remint, no session/new, no interrupted markers.
    #[test]
    fn fake_acp_double_remint_never_answers_dead_rpc_id() {
        let (s, _, bid1) = RemintSession::open("sess-1", "S-stable", caps_resume());
        let mut old = FakeAcp::new("S-stable");
        s.begin_turn("deploy");
        s.start_tool("t1", "deploy");
        let a1 = ask_via_codec(&s, &mut old, "t1", "deploy").unwrap();
        let a2 = ask_via_codec(&s, &mut old, "t2", "migrate").unwrap();
        assert_eq!(a1.rpc_id, RpcId::Num(101));
        assert_eq!(s.pending_permissions().len(), 2);

        let o1 = s.remint_and_attach().unwrap();
        assert!(o1.permission_resurfaced);
        assert!(!o1.turn_interrupted);
        assert_eq!(o1.tools_finalized, 0);
        assert!(!o1.wire_actions.contains(&WireAction::SessionCancel));
        assert!(!o1.wire_actions.contains(&WireAction::SessionClose));
        assert!(
            s.pending_permissions().is_empty(),
            "no pending leak across remint"
        );
        assert_eq!(s.resurfaced_permissions().len(), 2);
        // Late UI answer aimed at the dead Binding: refused, nothing written.
        assert_eq!(
            s.resolve_permission(&bid1, &a1.rpc_id, "allow-once"),
            Err(RemintError::DeadBinding)
        );

        // Second remint before the agent re-asks: still healthy HITL.
        let o2 = s.remint_and_attach().unwrap();
        assert_ne!(o1.binding_id, o2.binding_id);
        assert!(o2.permission_resurfaced);
        assert!(!o2.turn_interrupted);
        assert_eq!(o2.tools_finalized, 0);
        assert_eq!(s.interrupted_marker_count("t1"), 0);
        assert!(s.pending_permissions().is_empty());
        assert_eq!(s.resurfaced_permissions().len(), 2);
        assert_eq!(
            s.resolve_permission(&o1.binding_id, &a2.rpc_id, "allow-once"),
            Err(RemintError::DeadBinding)
        );
        let mut peer = FakeAcp::new("S-stable");
        for a in o2.wire_actions.iter() {
            if let WireAction::AttachResume = a {
                peer.dispatch("session/resume", &s).unwrap();
            }
        }

        // Resumed process restarts rpc ids: 101 again (collides with dead a1).
        let r1 = ask_via_codec(&s, &mut peer, "t1", "deploy").unwrap();
        assert_eq!(r1.rpc_id, a1.rpc_id);
        assert_eq!(r1.binding, o2.binding_id);
        assert!(s.observe().iter().any(|e| matches!(
            &e.kind,
            RemintEventKind::PermissionReasked { old, new }
                if old.binding == bid1 && old.rpc_id == a1.rpc_id && new.binding == o2.binding_id
        )));
        assert_eq!(s.resurfaced_permissions().len(), 1, "t1 adopted, t2 waits");
        send_via_codec(
            &mut peer,
            &s.resolve_permission(&o2.binding_id, &r1.rpc_id, "allow-once")
                .unwrap(),
        );
        assert_eq!(peer.outcome("t1").unwrap()["optionId"], "allow-once");
        assert!(!peer.hung());
        // Old process got nothing — dead ids never answered.
        assert!(old.got.is_empty() && old.ignored.is_empty());
        assert!(old.hung());

        let r2 = ask_via_codec(&s, &mut peer, "t2", "migrate").unwrap();
        send_via_codec(
            &mut peer,
            &s.resolve_permission(&o2.binding_id, &r2.rpc_id, "reject-once")
                .unwrap(),
        );
        assert!(!peer.hung());
        assert_maps_empty(&s, "after re-asks answered");
        assert!(peer.methods.iter().all(|m| m != "session/new"));
        assert_eq!(s.agent_session_id(), "S-stable");
    }

    /// After remint, a shared toolCallId (copilot-cli #989) is ambiguous:
    /// do not adopt either resurfaced ask by toolCallId. Cancel clears
    /// resurfaced asks on the Host log only (no wire write to dead ids).
    #[test]
    fn fake_acp_remint_shared_tool_call_id_is_not_adopted() {
        let (s, _, _) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut old = FakeAcp::new("S-1");
        old.shared_tool_call_id = Some("shell-permission");
        s.begin_turn("rm");
        ask_via_codec(&s, &mut old, "t1", "rm a").unwrap();
        ask_via_codec(&s, &mut old, "t2", "rm b").unwrap();
        let o = s.remint_and_attach().unwrap();
        assert_eq!(s.resurfaced_permissions().len(), 2);

        let mut peer = FakeAcp::new("S-1");
        peer.shared_tool_call_id = Some("shell-permission");
        let r = ask_via_codec(&s, &mut peer, "t1", "rm a").unwrap();
        assert_eq!(r.binding, o.binding_id);
        assert_eq!(
            s.resurfaced_permissions().len(),
            2,
            "ambiguous: none adopted"
        );
        assert!(!s
            .observe()
            .iter()
            .any(|e| matches!(e.kind, RemintEventKind::PermissionReasked { .. })));

        let cmds = s.cancel_turn().unwrap();
        // One live ask answered cancelled + session/cancel; dead ids untouched.
        assert_eq!(cmds.len(), 2);
        for c in &cmds {
            send_via_codec(&mut peer, c);
        }
        assert!(!peer.hung());
        assert!(old.got.is_empty());
        assert_maps_empty(&s, "after cancel post-remint");
    }

    /// Accept check: the (Binding, rpc id) map is empty after answer, after
    /// cancel, and after remint (moved to resurfaced, then drained).
    #[test]
    fn pending_map_empty_after_answer_cancel_and_remint() {
        let (s, _, bid) = RemintSession::open("sess-1", "S-1", caps_resume());
        s.begin_turn("a");
        s.request_permission(RpcId::Num(1), Some("t1"), "x", opts())
            .unwrap();
        s.resolve_permission(&bid, &RpcId::Num(1), "allow-once")
            .unwrap();
        assert_maps_empty(&s, "after answer");

        s.begin_turn("b");
        s.request_permission(RpcId::Num(2), Some("t2"), "x", opts())
            .unwrap();
        s.request_permission(RpcId::Str("2".into()), Some("t3"), "y", opts())
            .unwrap();
        assert_eq!(s.pending_permissions().len(), 2, "5 and \"5\" are distinct");
        s.cancel_turn().unwrap();
        assert_maps_empty(&s, "after cancel");

        s.begin_turn("c");
        s.request_permission(RpcId::Num(3), Some("t4"), "x", opts())
            .unwrap();
        let o = s.remint_and_attach().unwrap();
        assert!(s.pending_permissions().is_empty(), "after remint");
        s.request_permission(RpcId::Num(1), Some("t4"), "x", opts())
            .unwrap();
        s.resolve_permission(&o.binding_id, &RpcId::Num(1), "reject-once")
            .unwrap();
        assert_maps_empty(&s, "after remint + re-ask answered");
        assert!(!s.turn_in_flight());
    }

    /// Pep Should 1 (PR #16): Host decision errors propagate. With the Host
    /// user out of the Session, resolve / cancel return HostLog, emit no
    /// command, keep the ask pending and write no PermissionDecided.
    #[test]
    fn host_decide_error_propagates_and_keeps_ask_pending() {
        let (s, _, bid) = RemintSession::open("sess-1", "S-1", caps_resume());
        let mut peer = FakeAcp::new("S-1");
        let a = ask_via_codec(&s, &mut peer, "t1", "x").unwrap();
        let host_user = s.inner.lock().unwrap().host_user;
        s.inner
            .lock()
            .unwrap()
            .host_session
            .leave(Member::User(host_user))
            .unwrap();
        let decided = |s: &RemintSession| {
            s.host_events()
                .iter()
                .filter(|e| matches!(e.body, EventBody::PermissionDecided { .. }))
                .count()
        };
        assert!(matches!(
            s.resolve_permission(&bid, &a.rpc_id, "allow-once"),
            Err(RemintError::HostLog(_))
        ));
        assert!(matches!(s.cancel_turn(), Err(RemintError::HostLog(_))));
        assert_eq!(s.pending_permissions().len(), 1);
        assert_eq!(decided(&s), 0);
        assert!(peer.hung());

        s.inner
            .lock()
            .unwrap()
            .host_session
            .join(Member::User(host_user))
            .unwrap();
        send_via_codec(
            &mut peer,
            &s.resolve_permission(&bid, &a.rpc_id, "allow-once").unwrap(),
        );
        assert!(!peer.hung());
        assert_eq!(decided(&s), 1);
        assert_maps_empty(&s, "after recovery");
    }
}
