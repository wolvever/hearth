//! Long-lived keeper of Sessions. Same class as [`crate::Store`] / [`crate::Provisioner`]
//! / [`crate::Host`] / [`crate::FakeSandbox`] — not a seventh product name.
//!
//! [`Runtime::keep`] stores a [`Host`] recipe and occupancy. [`Runtime::wake`] is
//! find_server: classify the agent's Binding, remint only when none is live
//! (`NeverBound` / `PositivelyDead`), link a prompt to a live idle Binding, or
//! wait. A live Binding in an open turn does **not** remint and does **not**
//! `turn_start`. Missing heartbeat is [`Liveness::Unknown`], not death.
//! It does **not** spawn a CLI, invent [`crate::EventBody::AgentMessage`],
//! or call [`crate::Session::turn_end`] — the host runner ends the turn later.
//!
//! Intra-turn durable steps: [`Runtime::begin_turn`] / [`Runtime::end_turn`] hold a
//! [`SessionTurnLease`] (holder + fence). [`Runtime::durable_step`] memoizes via
//! [`crate::EventBody::StepCompleted`]. [`Runtime::resume_interrupted_turn`]
//! reclaims after host crash, finalizes unmatched tool calls as
//! [`crate::EventBody::ToolCallInterrupted`] (Indeterminate) via the same
//! helper as wire remint, and does **not** remint Binding.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::{
    now_ts, open_turn_of, step_result_of, AgentId, BindingId, Error, Event, EventBody, Host,
    InMemory, Member, Result, Session, SessionId, WaitReason, WakeSource,
};

/// Why a kept session is being woken.
#[derive(Clone, Debug, PartialEq)]
pub enum Wake {
    UserQuery { user: crate::UserId, text: String },
    Timer,
    Trigger { name: String },
}

impl Wake {
    fn source(&self) -> WakeSource {
        match self {
            Wake::UserQuery { .. } => WakeSource::UserQuery,
            Wake::Timer => WakeSource::Timer,
            Wake::Trigger { name } => WakeSource::Trigger { name: name.clone() },
        }
    }
}

/// Binding lease as seen by [`Runtime::wake`]. Not a product name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Liveness {
    LiveIdle,
    LiveInTurn,
    PositivelyDead,
    Unknown,
    NeverBound,
}

/// Recipe Runtime uses to remint a Binding. Not a product concept.
#[derive(Clone, Debug, PartialEq)]
pub struct KeepSpec {
    pub agent: AgentId,
    pub host: Host,
}

/// Who currently drives a leased turn. Runtime bookkeeping, not a seventh name.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct HolderId(pub String);

impl HolderId {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl From<&str> for HolderId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for HolderId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Live [`SessionTurnLease`] snapshot. Not a kernel noun.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionTurnLease {
    pub holder: HolderId,
    pub fence: u64,
    pub turn_id: u64,
}

/// Result of [`Runtime::durable_step`]: ran `f` or replayed `StepCompleted`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepOutcome {
    Executed(String),
    Memoized(String),
}

impl StepOutcome {
    pub fn result(&self) -> &str {
        match self {
            Self::Executed(s) | Self::Memoized(s) => s,
        }
    }

    pub fn was_memoized(&self) -> bool {
        matches!(self, Self::Memoized(_))
    }
}

/// Internal keep record: recipe plus optional timer and turn lease.
struct Kept {
    agent: AgentId,
    host: Host,
    /// If Some, [`Runtime::tick`] wakes when `now >= next_due_ms` (ms since epoch,
    /// same clock as [`crate::Event::ts`]).
    timer_every_ms: Option<u64>,
    next_due_ms: Option<u64>,
    /// Live or interrupted session-turn lease. `holder: None` after a crash.
    lease: Option<TurnLease>,
}

struct TurnLease {
    holder: Option<HolderId>,
    fence: u64,
    turn_id: u64,
}

/// Long-lived keeper. Owns an [`InMemory`] store; clone the handle out via
/// [`Runtime::store`] so Session methods still work.
#[derive(Clone)]
pub struct Runtime {
    store: InMemory,
    kept: Arc<Mutex<HashMap<SessionId, Kept>>>,
    /// Serializes lease mutations and durable steps so `(turn, step)` runs at most once.
    durable: Arc<Mutex<()>>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            store: InMemory::new(),
            kept: Arc::new(Mutex::new(HashMap::new())),
            durable: Arc::new(Mutex::new(())),
        }
    }

    pub fn store(&self) -> &InMemory {
        &self.store
    }

    /// Same as [`InMemory::create_session`]. The session is **not** kept until [`Self::keep`].
    pub fn create_session(&self) -> Session {
        self.store.create_session()
    }

    fn lock_kept(&self) -> Result<std::sync::MutexGuard<'_, HashMap<SessionId, Kept>>> {
        self.kept.lock().map_err(|_| Error::Poisoned)
    }

    fn lock_durable(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.durable.lock().map_err(|_| Error::Poisoned)
    }

    fn kept_recipe(&self, session: SessionId) -> Result<(AgentId, Host)> {
        let g = self.lock_kept()?;
        let kept = g.get(&session).ok_or(Error::NotKept(session))?;
        Ok((kept.agent, kept.host.clone()))
    }

    /// Record a Host recipe for this session. Session and Agent must exist.
    /// Joins `Member::Agent` if needed (occupancy so later turns are legal).
    /// Does **not** bind. Overwriting keep on the same session updates the recipe.
    pub fn keep(&self, session: SessionId, agent: AgentId, host: Host) -> Result<()> {
        let spec = KeepSpec { agent, host };
        let sess = self.store.session(session)?;
        let _ = self.store.agent(spec.agent)?;
        ensure_agent_member(&sess, spec.agent)?;
        let mut g = self.lock_kept()?;
        match g.get_mut(&session) {
            Some(existing) => {
                existing.agent = spec.agent;
                existing.host = spec.host;
            }
            None => {
                g.insert(
                    session,
                    Kept {
                        agent: spec.agent,
                        host: spec.host,
                        timer_every_ms: None,
                        next_due_ms: None,
                        lease: None,
                    },
                );
            }
        }
        Ok(())
    }

    /// Stop keeping. Does **not** delete Session, log, Places, or Bindings.
    pub fn release(&self, session: SessionId) -> Result<()> {
        let mut g = self.lock_kept()?;
        g.remove(&session);
        Ok(())
    }

    pub fn kept(&self) -> Vec<SessionId> {
        let Ok(g) = self.lock_kept() else {
            return Vec::new();
        };
        let mut ids: Vec<SessionId> = g.keys().copied().collect();
        ids.sort_by_key(|id| id.0);
        ids
    }

    /// Session must be kept. Sets `timer_every_ms` and `next_due_ms = now + every_ms`.
    pub fn schedule(&self, session: SessionId, every_ms: u64) -> Result<()> {
        let mut g = self.lock_kept()?;
        let kept = g.get_mut(&session).ok_or(Error::NotKept(session))?;
        kept.timer_every_ms = Some(every_ms);
        kept.next_due_ms = Some(now_ts().saturating_add(every_ms));
        Ok(())
    }

    /// Classify the kept agent's Binding lease. Absence of a heartbeat is
    /// [`Liveness::Unknown`], not death. No descriptor store yet: a live
    /// Binding is idle or in-turn; no live Binding plus `BindingReleased` is
    /// positively dead; never attached is [`Liveness::NeverBound`].
    pub fn classify(&self, session: SessionId) -> Result<Liveness> {
        let (agent, _) = self.kept_recipe(session)?;
        classify_session(&self.store.session(session)?, agent)
    }

    /// Idle remint. Refused if a Binding is live or liveness is unknown.
    pub fn remint(&self, session: SessionId) -> Result<crate::BindingId> {
        // Lock order (all rebind paths): durable gate -> kept -> store.
        let _gate = self.lock_durable()?;
        let (agent, host) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;
        ensure_agent_member(&sess, agent)?;
        match classify_session(&sess, agent)? {
            Liveness::LiveIdle | Liveness::LiveInTurn => {
                Err(Error::Waiting(WaitReason::AlreadyLive))
            }
            Liveness::Unknown => Err(Error::Waiting(WaitReason::LivenessUnknown)),
            Liveness::NeverBound | Liveness::PositivelyDead => {
                Ok(claim_binding(&sess, agent, host)?.id)
            }
        }
    }

    /// Wake a kept session: find_server on the agent's Binding.
    ///
    /// Prompt path never remints a live Binding. `NeverBound` / `PositivelyDead`
    /// remint first (idle launch / empty pool), then link. `LiveInTurn` /
    /// `Unknown` wait.
    /// Does **not** call [`Session::turn_end`] — the host runner would end the turn later.
    /// Does **not** spawn a real CLI or invent `AgentMessage`.
    pub fn wake(&self, session: SessionId, wake: Wake) -> Result<Vec<Event>> {
        let _gate = self.lock_durable()?;
        let (agent, host) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;
        let start_len = sess.events()?.len();
        ensure_agent_member(&sess, agent)?;

        match classify_session(&sess, agent)? {
            Liveness::LiveInTurn => return Err(Error::Waiting(WaitReason::TurnOpen)),
            Liveness::Unknown => return Err(Error::Waiting(WaitReason::LivenessUnknown)),
            Liveness::LiveIdle => {}
            Liveness::NeverBound | Liveness::PositivelyDead => {
                claim_binding(&sess, agent, host)?;
            }
        }
        record_wake(&sess, &wake)?;
        sess.turn_start(agent)?;

        let events = sess.events()?;
        Ok(events[start_len..].to_vec())
    }

    /// For each kept session with a due timer (`next_due_ms <= now`), `wake(Timer)`,
    /// then set `next_due_ms = now + every_ms`. Pass `now` for a deterministic clock
    /// (same epoch-ms as [`crate::Event::ts`]); `None` uses the wall clock.
    /// This is how tests drive the infinite loop without sleeping.
    pub fn tick(&self, now_ms: Option<u64>) -> Result<Vec<(SessionId, Vec<Event>)>> {
        let now = now_ms.unwrap_or_else(now_ts);
        let mut due: Vec<SessionId> = {
            let g = self.lock_kept()?;
            g.iter()
                .filter(|(_, k)| k.next_due_ms.map(|due| due <= now).unwrap_or(false))
                .map(|(id, _)| *id)
                .collect()
        };
        due.sort_by_key(|id| id.0);

        let mut out = Vec::new();
        for sid in due {
            let events = self.wake(sid, Wake::Timer)?;
            {
                let mut g = self.lock_kept()?;
                if let Some(k) = g.get_mut(&sid) {
                    if let Some(every) = k.timer_every_ms {
                        k.next_due_ms = Some(now.saturating_add(every));
                    }
                }
            }
            out.push((sid, events));
        }
        Ok(out)
    }

    /// Live lease, if a holder currently owns the session turn.
    pub fn turn_lease(&self, session: SessionId) -> Result<Option<SessionTurnLease>> {
        let g = self.lock_kept()?;
        let kept = g.get(&session).ok_or(Error::NotKept(session))?;
        Ok(kept.lease.as_ref().and_then(|l| {
            l.holder.as_ref().map(|h| SessionTurnLease {
                holder: h.clone(),
                fence: l.fence,
                turn_id: l.turn_id,
            })
        }))
    }

    /// Open a turn under a live [`SessionTurnLease`].
    ///
    /// Remints Binding only when `NeverBound` / `PositivelyDead` (same as wake).
    /// If `wake` already opened a turn and no lease exists, attaches a lease to
    /// that turn. An interrupted turn (lease holder dropped) must use
    /// [`Self::resume_interrupted_turn`].
    pub fn begin_turn(
        &self,
        session: SessionId,
        holder: HolderId,
    ) -> Result<(u64, u64, BindingId)> {
        let _gate = self.lock_durable()?;
        let (agent, host) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;
        ensure_agent_member(&sess, agent)?;

        {
            let g = self.lock_kept()?;
            let kept = g.get(&session).ok_or(Error::NotKept(session))?;
            if let Some(lease) = &kept.lease {
                if lease.holder.is_some() {
                    return Err(Error::LiveTurnOpen);
                }
                return Err(Error::Waiting(WaitReason::TurnOpen));
            }
        }

        if let Some((turn_id, _)) = open_turn_of(&sess.events()?) {
            sess.restore_open_turn()?;
            let binding = binding_for_agent(&sess, agent)?;
            let fence = self.install_lease(session, holder, turn_id)?;
            return Ok((turn_id, fence, binding));
        }

        match classify_session(&sess, agent)? {
            Liveness::LiveInTurn => return Err(Error::Waiting(WaitReason::TurnOpen)),
            Liveness::Unknown => return Err(Error::Waiting(WaitReason::LivenessUnknown)),
            Liveness::LiveIdle => {}
            Liveness::NeverBound | Liveness::PositivelyDead => {
                claim_binding(&sess, agent, host)?;
            }
        }

        let start = sess.turn_start(agent)?;
        let turn_id = start.turn.ok_or(Error::TurnNotOpen)?;
        let binding = binding_for_agent(&sess, agent)?;
        let fence = self.install_lease(session, holder, turn_id)?;
        Ok((turn_id, fence, binding))
    }

    /// End a leased turn. `interrupted: true` drops the live holder without
    /// `TurnEnd` (host crash). `false` appends `TurnEnd` and clears the lease.
    pub fn end_turn(
        &self,
        session: SessionId,
        holder: HolderId,
        fence: u64,
        interrupted: bool,
    ) -> Result<()> {
        let _gate = self.lock_durable()?;
        let (agent, _) = self.kept_recipe(session)?;
        self.require_live_holder_fence(session, &holder, fence)?;

        if interrupted {
            let mut g = self.lock_kept()?;
            let kept = g.get_mut(&session).ok_or(Error::NotKept(session))?;
            if let Some(lease) = kept.lease.as_mut() {
                lease.holder = None;
            }
            return Ok(());
        }

        let sess = self.store.session(session)?;
        sess.turn_end(agent)?;
        let mut g = self.lock_kept()?;
        let kept = g.get_mut(&session).ok_or(Error::NotKept(session))?;
        kept.lease = None;
        Ok(())
    }

    /// Intra-turn step memoization. If `StepCompleted` for `(turn_id, step_id)`
    /// is already on the EventLog, returns [`StepOutcome::Memoized`] and does
    /// not run `f`. Otherwise runs `f`, appends `StepCompleted` under the live
    /// lease fence, then returns [`StepOutcome::Executed`].
    ///
    /// InMemory EventLog is append-then-return (no fsync). A durable EventLog
    /// must persist `StepCompleted` before returning `Executed`.
    pub fn durable_step<F>(
        &self,
        session: SessionId,
        holder: HolderId,
        fence: u64,
        turn_id: u64,
        step_id: impl Into<String>,
        f: F,
    ) -> Result<StepOutcome>
    where
        F: FnOnce() -> Result<String>,
    {
        let _gate = self.lock_durable()?;
        let step_id = step_id.into();
        self.require_live_lease(session, &holder, fence, turn_id)?;

        let sess = self.store.session(session)?;
        if sess.current_turn()? != Some(turn_id) {
            return Err(Error::TurnLeaseLost);
        }
        if let Some(result) = step_result_of(&sess.events()?, turn_id, &step_id) {
            return Ok(StepOutcome::Memoized(result));
        }

        let result = f()?;

        self.require_live_lease(session, &holder, fence, turn_id)?;
        sess.append(EventBody::StepCompleted {
            turn_id,
            step_id,
            result: result.clone(),
        })?;
        Ok(StepOutcome::Executed(result))
    }

    /// After host crash: reclaim the lease, restore the open turn from the
    /// EventLog (`TurnStart` without `TurnEnd`), finalize unmatched tool
    /// calls as [`EventBody::ToolCallInterrupted`]
    /// ([`crate::ToolInterruptStatus::Indeterminate`]) via
    /// [`Session::finalize_unmatched_tool_calls`] (same helper as wire remint),
    /// return `(turn_id, fence, binding_id)`.
    /// Does **not** remint Binding. Refuses if a holder is still live.
    pub fn resume_interrupted_turn(
        &self,
        session: SessionId,
        new_holder: HolderId,
    ) -> Result<(u64, u64, BindingId)> {
        let _gate = self.lock_durable()?;
        let (agent, _) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;

        {
            let g = self.lock_kept()?;
            let kept = g.get(&session).ok_or(Error::NotKept(session))?;
            if kept
                .lease
                .as_ref()
                .and_then(|l| l.holder.as_ref())
                .is_some()
            {
                return Err(Error::LiveTurnOpen);
            }
        }

        let (turn_id, _) = open_turn_of(&sess.events()?).ok_or(Error::TurnNotOpen)?;
        sess.restore_open_turn()?;
        // Crash reclaim must not leave ToolCalls looking live (tempt a
        // re-run). Same Indeterminate finalize as wire remint — never
        // Completed-without-output, never Cancelled.
        sess.finalize_unmatched_tool_calls()?;
        let binding = binding_for_agent(&sess, agent)?;
        let fence = self.install_lease(session, new_holder, turn_id)?;
        Ok((turn_id, fence, binding))
    }

    fn install_lease(&self, session: SessionId, holder: HolderId, turn_id: u64) -> Result<u64> {
        let mut g = self.lock_kept()?;
        let kept = g.get_mut(&session).ok_or(Error::NotKept(session))?;
        let fence = kept.lease.as_ref().map(|l| l.fence).unwrap_or(0) + 1;
        kept.lease = Some(TurnLease {
            holder: Some(holder),
            fence,
            turn_id,
        });
        Ok(fence)
    }

    fn require_live_holder_fence(
        &self,
        session: SessionId,
        holder: &HolderId,
        fence: u64,
    ) -> Result<()> {
        let g = self.lock_kept()?;
        let kept = g.get(&session).ok_or(Error::NotKept(session))?;
        let lease = kept.lease.as_ref().ok_or(Error::SessionNotOwned)?;
        match &lease.holder {
            Some(h) if h == holder => {}
            Some(_) | None => return Err(Error::SessionNotOwned),
        }
        if lease.fence != fence {
            return Err(Error::TurnLeaseLost);
        }
        Ok(())
    }

    fn require_live_lease(
        &self,
        session: SessionId,
        holder: &HolderId,
        fence: u64,
        turn_id: u64,
    ) -> Result<()> {
        self.require_live_holder_fence(session, holder, fence)?;
        let g = self.lock_kept()?;
        let kept = g.get(&session).ok_or(Error::NotKept(session))?;
        let lease = kept.lease.as_ref().ok_or(Error::SessionNotOwned)?;
        if lease.turn_id != turn_id {
            return Err(Error::TurnLeaseLost);
        }
        Ok(())
    }
}

fn record_wake(session: &Session, wake: &Wake) -> Result<Event> {
    match wake {
        // Prefer UserMessage alone so queries are not doubled with Wake.
        Wake::UserQuery { user, text } => session.user_message(*user, text),
        Wake::Timer | Wake::Trigger { .. } => session.append(EventBody::Wake {
            source: wake.source(),
        }),
    }
}

fn classify_session(session: &Session, agent: AgentId) -> Result<Liveness> {
    let bindings = session.bindings()?;
    if bindings.iter().any(|b| b.agent == Some(agent)) {
        return Ok(if session.current_turn()?.is_some() {
            Liveness::LiveInTurn
        } else {
            Liveness::LiveIdle
        });
    }
    let live_ids: HashSet<_> = bindings.iter().map(|b| b.id).collect();
    let mut attached = false;
    let mut released_without_live = false;
    for e in session.events()? {
        match e.body {
            EventBody::BindingAttached { .. } => {
                attached = true;
                released_without_live = false;
            }
            EventBody::BindingReleased { binding } => {
                if !live_ids.contains(&binding) {
                    released_without_live = true;
                }
            }
            _ => {}
        }
    }
    Ok(if released_without_live {
        Liveness::PositivelyDead
    } else if attached {
        // Claimed on the log, no live Binding, no BindingReleased — vanished.
        Liveness::Unknown
    } else {
        Liveness::NeverBound
    })
}

fn ensure_agent_member(session: &Session, agent: AgentId) -> Result<()> {
    if session.members()?.contains(&Member::Agent(agent)) {
        return Ok(());
    }
    match session.join(Member::Agent(agent)) {
        Ok(_) | Err(Error::AlreadyMember) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Claim a Binding for a NeverBound / PositivelyDead agent. The snapshot
/// (`classify_session`) is rechecked inside [`Session::rebind`]: a Binding
/// that appeared since is reported as [`WaitReason::AlreadyLive`], never
/// duplicated.
fn claim_binding(session: &Session, agent: AgentId, host: Host) -> Result<crate::Binding> {
    session.rebind(None, agent, host).map_err(|e| match e {
        Error::BindingOwnerChanged => Error::Waiting(WaitReason::AlreadyLive),
        e => e,
    })
}

fn binding_for_agent(session: &Session, agent: AgentId) -> Result<BindingId> {
    session
        .bindings()?
        .into_iter()
        .find(|b| b.agent == Some(agent))
        .map(|b| b.id)
        .ok_or(Error::Waiting(WaitReason::NoBinding))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{is_model_visible, HostKind, Member};

    fn goose() -> Host {
        HostKind::Goose.host(None, None)
    }

    #[test]
    fn create_session_is_not_kept_until_keep() {
        let rt = Runtime::new();
        let session = rt.create_session();
        assert!(!rt.kept().contains(&session.id()));
        let agent = rt.store().create_agent("scribe", "");
        rt.keep(session.id(), agent.id, goose()).unwrap();
        assert!(rt.kept().contains(&session.id()));
    }

    #[test]
    fn keep_wake_user_query_joins_remints_no_second_binding() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        assert!(!session
            .members()
            .unwrap()
            .contains(&Member::Agent(agent.id)));

        rt.keep(session.id(), agent.id, goose()).unwrap();
        assert!(session
            .members()
            .unwrap()
            .contains(&Member::Agent(agent.id)));
        assert!(session.bindings().unwrap().is_empty());

        let evs = rt
            .wake(
                session.id(),
                Wake::UserQuery {
                    user: user.id,
                    text: "hi".into(),
                },
            )
            .unwrap();
        assert!(session
            .members()
            .unwrap()
            .contains(&Member::Agent(agent.id)));
        let binds = session.bindings().unwrap();
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0].kind, "goose");
        assert_eq!(binds[0].agent, Some(agent.id));
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "hi"
        )));
        assert!(evs
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnStart { .. })));
        assert!(evs
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingAttached { .. })));
        assert!(!evs.iter().any(|e| matches!(e.body, EventBody::Wake { .. })));

        let evs2 = rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "again".into(),
            },
        );
        assert!(matches!(evs2, Err(Error::Waiting(WaitReason::TurnOpen))));
        assert_eq!(session.bindings().unwrap().len(), 1);
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(e.body, EventBody::TurnStart { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn unbind_all_then_wake_remints_same_kind() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "first".into(),
            },
        )
        .unwrap();
        let kind = session.bindings().unwrap()[0].kind.clone();
        assert_eq!(kind, "goose");
        for b in session.bindings().unwrap() {
            session.unbind(b.id).unwrap();
        }
        assert!(session.bindings().unwrap().is_empty());

        rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "again".into(),
            },
        )
        .unwrap();
        let binds = session.bindings().unwrap();
        assert_eq!(binds.len(), 1);
        assert_eq!(binds[0].kind, kind);
    }

    #[test]
    fn release_then_wake_is_not_kept_session_survives() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        let sid = session.id();
        rt.keep(sid, agent.id, goose()).unwrap();
        rt.release(sid).unwrap();
        assert!(!rt.kept().contains(&sid));
        assert!(matches!(
            rt.wake(sid, Wake::Timer).unwrap_err(),
            Error::NotKept(id) if id == sid
        ));
        assert!(rt.store().session(sid).is_ok());
        assert!(session.events().is_ok());
    }

    #[test]
    fn schedule_tick_wakes_timer_and_reschedules() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        rt.schedule(session.id(), 1_000).unwrap();

        assert!(rt.tick(Some(0)).unwrap().is_empty());

        let now = now_ts().saturating_add(5_000);
        let fired = rt.tick(Some(now)).unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].0, session.id());
        assert!(fired[0].1.iter().any(|e| matches!(
            e.body,
            EventBody::Wake {
                source: WakeSource::Timer
            }
        )));
        assert!(fired[0]
            .1
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnStart { .. })));

        session.turn_end(agent.id).unwrap();

        // Rescheduled to now + every_ms; same now is not due.
        assert!(rt.tick(Some(now)).unwrap().is_empty());
        let again = rt.tick(Some(now.saturating_add(1_000))).unwrap();
        assert_eq!(again.len(), 1);
        assert!(again[0].1.iter().any(|e| matches!(
            e.body,
            EventBody::Wake {
                source: WakeSource::Timer
            }
        )));
    }

    #[test]
    fn trigger_appends_wake_and_turn_start() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let evs = rt
            .wake(
                session.id(),
                Wake::Trigger {
                    name: "hook".into(),
                },
            )
            .unwrap();
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::Wake {
                source: WakeSource::Trigger { name }
            } if name == "hook"
        )));
        assert!(evs
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnStart { .. })));
    }

    #[test]
    fn wake_is_not_in_surface() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "hi".into(),
            },
        )
        .unwrap();
        session.turn_end(agent.id).unwrap();
        rt.wake(
            session.id(),
            Wake::Trigger {
                name: "hook".into(),
            },
        )
        .unwrap();
        session.turn_end(agent.id).unwrap();
        rt.wake(session.id(), Wake::Timer).unwrap();

        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(e.body, EventBody::Wake { .. })));
        let surface = session.surface().unwrap();
        assert!(!surface
            .iter()
            .any(|e| matches!(e.body, EventBody::Wake { .. })));
        assert!(surface.iter().all(|e| is_model_visible(&e.body)));
        assert!(surface.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "hi"
        )));
    }

    #[test]
    fn eight_prompts_after_remint_one_link_seven_wait() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        rt.keep(session.id(), agent.id, goose()).unwrap();

        let mut linked = 0u32;
        let mut waited = 0u32;
        for i in 0..8 {
            let r = rt.wake(
                session.id(),
                Wake::UserQuery {
                    user: user.id,
                    text: format!("p{i}"),
                },
            );
            match r {
                Ok(evs)
                    if evs
                        .iter()
                        .any(|e| matches!(e.body, EventBody::TurnStart { .. })) =>
                {
                    linked += 1;
                }
                Err(Error::Waiting(WaitReason::TurnOpen)) => waited += 1,
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(linked, 1);
        assert_eq!(waited, 7);
        assert_eq!(session.bindings().unwrap().len(), 1);
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(e.body, EventBody::TurnStart { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn remint_refused_while_live() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "hi".into(),
            },
        )
        .unwrap();
        assert!(matches!(
            rt.remint(session.id()),
            Err(Error::Waiting(WaitReason::AlreadyLive))
        ));
        assert_eq!(session.bindings().unwrap().len(), 1);
    }

    #[test]
    fn crash_mid_turn_resume_skips_memoized_steps() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (turn, fence, binding) = rt.begin_turn(session.id(), host_a.clone()).unwrap();

        let charges = std::cell::Cell::new(0);
        let refunds = std::cell::Cell::new(0);
        let charged = rt
            .durable_step(session.id(), host_a.clone(), fence, turn, "charge", || {
                charges.set(charges.get() + 1);
                Ok("charged".into())
            })
            .unwrap();
        assert!(matches!(charged, StepOutcome::Executed(ref s) if s == "charged"));
        assert_eq!(charges.get(), 1);

        rt.end_turn(session.id(), host_a.clone(), fence, true)
            .unwrap();
        assert_eq!(session.current_turn().unwrap(), Some(turn));
        assert!(session.open_turn().unwrap() == Some(turn));
        assert!(rt.turn_lease(session.id()).unwrap().is_none());

        let host_b = HolderId::new("host-b");
        let (turn2, fence2, binding2) = rt
            .resume_interrupted_turn(session.id(), host_b.clone())
            .unwrap();
        assert_eq!(turn2, turn);
        assert_ne!(fence2, fence);
        assert_eq!(binding2, binding);

        let replayed = rt
            .durable_step(
                session.id(),
                host_b.clone(),
                fence2,
                turn2,
                "charge",
                || {
                    charges.set(charges.get() + 1);
                    Ok("should-not-run".into())
                },
            )
            .unwrap();
        assert!(matches!(replayed, StepOutcome::Memoized(ref s) if s == "charged"));
        assert_eq!(charges.get(), 1);

        let refunded = rt
            .durable_step(
                session.id(),
                host_b.clone(),
                fence2,
                turn2,
                "refund",
                || {
                    refunds.set(refunds.get() + 1);
                    Ok("refunded".into())
                },
            )
            .unwrap();
        assert!(matches!(refunded, StepOutcome::Executed(ref s) if s == "refunded"));
        assert_eq!(refunds.get(), 1);

        rt.end_turn(session.id(), host_b, fence2, false).unwrap();
        assert!(session.current_turn().unwrap().is_none());
        assert!(session.open_turn().unwrap().is_none());
    }

    #[test]
    fn resume_does_not_remint_binding() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (turn, fence, binding) = rt.begin_turn(session.id(), host_a.clone()).unwrap();
        rt.durable_step(session.id(), host_a.clone(), fence, turn, "step", || {
            Ok("ok".into())
        })
        .unwrap();
        rt.end_turn(session.id(), host_a, fence, true).unwrap();

        let before = session.bindings().unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].id, binding);

        let (_, _, resumed) = rt
            .resume_interrupted_turn(session.id(), HolderId::new("host-b"))
            .unwrap();
        let after = session.bindings().unwrap();
        assert_eq!(resumed, binding);
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].id, binding);
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
                .count(),
            1
        );
        assert!(!session
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingReleased { .. })));
    }

    #[test]
    fn stale_fence_is_turn_lease_lost() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), host_a.clone()).unwrap();
        rt.durable_step(session.id(), host_a.clone(), fence, turn, "a", || {
            Ok("1".into())
        })
        .unwrap();
        rt.end_turn(session.id(), host_a, fence, true).unwrap();

        let host_b = HolderId::new("host-b");
        let (turn2, fence2, _) = rt
            .resume_interrupted_turn(session.id(), host_b.clone())
            .unwrap();
        assert!(matches!(
            rt.durable_step(session.id(), host_b.clone(), fence, turn2, "b", || {
                Ok("stale".into())
            }),
            Err(Error::TurnLeaseLost)
        ));
        let ok = rt
            .durable_step(session.id(), host_b, fence2, turn2, "b", || {
                Ok("fresh".into())
            })
            .unwrap();
        assert!(matches!(ok, StepOutcome::Executed(ref s) if s == "fresh"));
    }

    #[test]
    fn live_holder_refuses_resume() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (_turn, fence, _) = rt.begin_turn(session.id(), host_a.clone()).unwrap();
        assert!(matches!(
            rt.resume_interrupted_turn(session.id(), HolderId::new("host-b")),
            Err(Error::LiveTurnOpen)
        ));
        assert!(rt.turn_lease(session.id()).unwrap().is_some());

        rt.end_turn(session.id(), host_a, fence, true).unwrap();
        rt.resume_interrupted_turn(session.id(), HolderId::new("host-b"))
            .unwrap();
    }

    #[test]
    fn events_include_step_completed_not_surface() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let holder = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), holder.clone()).unwrap();
        rt.durable_step(session.id(), holder.clone(), fence, turn, "charge", || {
            Ok("42".into())
        })
        .unwrap();

        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::StepCompleted {
                turn_id,
                step_id,
                result
            } if *turn_id == turn && step_id == "charge" && result == "42"
        )));
        assert_eq!(
            session.step_result(turn, "charge").unwrap().as_deref(),
            Some("42")
        );
        let surface = session.surface().unwrap();
        assert!(!surface
            .iter()
            .any(|e| matches!(e.body, EventBody::StepCompleted { .. })));
        assert!(surface.iter().all(|e| is_model_visible(&e.body)));
    }

    #[test]
    fn durable_step_same_id_is_memoized_without_crash() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let holder = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), holder.clone()).unwrap();
        let runs = std::cell::Cell::new(0);
        let first = rt
            .durable_step(session.id(), holder.clone(), fence, turn, "once", || {
                runs.set(runs.get() + 1);
                Ok("v".into())
            })
            .unwrap();
        let second = rt
            .durable_step(session.id(), holder.clone(), fence, turn, "once", || {
                runs.set(runs.get() + 1);
                Ok("nope".into())
            })
            .unwrap();
        assert!(matches!(first, StepOutcome::Executed(_)));
        assert!(matches!(second, StepOutcome::Memoized(ref s) if s == "v"));
        assert_eq!(runs.get(), 1);
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(e.body, EventBody::StepCompleted { .. }))
                .count(),
            1
        );
    }

    #[test]
    fn durable_step_requires_live_lease() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        assert!(matches!(
            rt.durable_step(session.id(), HolderId::new("ghost"), 1, 1, "x", || Ok(
                "no".into()
            )),
            Err(Error::SessionNotOwned)
        ));
        let holder = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), holder.clone()).unwrap();
        assert!(matches!(
            rt.durable_step(
                session.id(),
                HolderId::new("other"),
                fence,
                turn,
                "x",
                || Ok("no".into())
            ),
            Err(Error::SessionNotOwned)
        ));
    }

    #[test]
    fn resume_without_open_turn_is_turn_not_open() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        assert!(matches!(
            rt.resume_interrupted_turn(session.id(), HolderId::new("host-b")),
            Err(Error::TurnNotOpen)
        ));
        let holder = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), holder.clone()).unwrap();
        rt.end_turn(session.id(), holder, fence, false).unwrap();
        assert!(matches!(
            rt.resume_interrupted_turn(session.id(), HolderId::new("host-b")),
            Err(Error::TurnNotOpen)
        ));
        let _ = turn;
    }

    #[test]
    fn resume_interrupted_turn_finalizes_open_tool_as_indeterminate() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (_turn, fence, _) = rt.begin_turn(session.id(), host_a.clone()).unwrap();
        session
            .append(EventBody::ToolCall {
                agent: agent.id,
                tool_call_id: "t-open".into(),
                name: "bash".into(),
                input: "{}".into(),
            })
            .unwrap();
        session
            .append(EventBody::ToolCall {
                agent: agent.id,
                tool_call_id: "t-done".into(),
                name: "read".into(),
                input: "{}".into(),
            })
            .unwrap();
        session
            .append(EventBody::ToolResult {
                agent: agent.id,
                tool_call_id: "t-done".into(),
                name: "read".into(),
                output: "ok".into(),
            })
            .unwrap();
        rt.end_turn(session.id(), host_a, fence, true).unwrap();

        let host_b = HolderId::new("host-b");
        rt.resume_interrupted_turn(session.id(), host_b.clone())
            .unwrap();

        let log = session.events().unwrap();
        let interrupts: Vec<_> = log
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::ToolCallInterrupted {
                    tool_call_id,
                    status,
                    ..
                } => Some((tool_call_id.as_str(), *status)),
                _ => None,
            })
            .collect();
        assert_eq!(interrupts.len(), 1);
        assert_eq!(interrupts[0].0, "t-open");
        assert_eq!(interrupts[0].1, crate::ToolInterruptStatus::Indeterminate);
        assert!(crate::tool_call_is_terminal(&log, "t-open"));
        assert!(crate::tool_call_is_terminal(&log, "t-done"));
        assert!(crate::unmatched_tool_calls(&log).is_empty());
    }

    #[test]
    fn second_crash_reclaim_does_not_duplicate_interrupt() {
        let rt = Runtime::new();
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let host_a = HolderId::new("host-a");
        let (turn, fence, _) = rt.begin_turn(session.id(), host_a.clone()).unwrap();
        session
            .append(EventBody::ToolCall {
                agent: agent.id,
                tool_call_id: "t1".into(),
                name: "bash".into(),
                input: "{}".into(),
            })
            .unwrap();
        rt.end_turn(session.id(), host_a, fence, true).unwrap();

        let host_b = HolderId::new("host-b");
        let (turn2, fence2, _) = rt
            .resume_interrupted_turn(session.id(), host_b.clone())
            .unwrap();
        assert_eq!(turn2, turn);
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(
                    &e.body,
                    EventBody::ToolCallInterrupted {
                        tool_call_id,
                        ..
                    } if tool_call_id == "t1"
                ))
                .count(),
            1
        );

        // Crash again under the new holder; second reclaim must not duplicate.
        rt.end_turn(session.id(), host_b, fence2, true).unwrap();
        let host_c = HolderId::new("host-c");
        rt.resume_interrupted_turn(session.id(), host_c).unwrap();
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(
                    &e.body,
                    EventBody::ToolCallInterrupted {
                        tool_call_id,
                        ..
                    } if tool_call_id == "t1"
                ))
                .count(),
            1
        );
        assert!(crate::unmatched_tool_calls(&session.events().unwrap()).is_empty());
    }

    #[test]
    fn begin_turn_after_wake_attaches_lease() {
        let rt = Runtime::new();
        let user = rt.store().create_user("cheng");
        let agent = rt.store().create_agent("scribe", "");
        let session = rt.create_session();
        session.join(Member::User(user.id)).unwrap();
        rt.keep(session.id(), agent.id, goose()).unwrap();
        rt.wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: "hi".into(),
            },
        )
        .unwrap();
        let turn_before = session.current_turn().unwrap();
        let binds_before = session.bindings().unwrap();
        let holder = HolderId::new("host-a");
        let (turn, fence, binding) = rt.begin_turn(session.id(), holder.clone()).unwrap();
        assert_eq!(Some(turn), turn_before);
        assert_eq!(binding, binds_before[0].id);
        let out = rt
            .durable_step(session.id(), holder, fence, turn, "after-wake", || {
                Ok("ok".into())
            })
            .unwrap();
        assert!(matches!(out, StepOutcome::Executed(_)));
        assert_eq!(
            session
                .events()
                .unwrap()
                .iter()
                .filter(|e| matches!(e.body, EventBody::TurnStart { .. }))
                .count(),
            1
        );
    }

    /// Rebind paths on Runtime (remint / wake / begin_turn) claim through
    /// `Session::rebind(None, ..)`: a Binding that appears after the
    /// classify snapshot is never duplicated.
    #[test]
    fn runtime_remint_wake_begin_turn_never_duplicate_binding() {
        let rt = Runtime::new();
        let session = rt.create_session();
        let agent = rt.store().create_agent("scribe", "");
        rt.keep(session.id(), agent.id, goose()).unwrap();
        let first = rt.remint(session.id()).unwrap();
        assert!(matches!(
            rt.remint(session.id()),
            Err(Error::Waiting(WaitReason::AlreadyLive))
        ));
        assert!(matches!(
            claim_binding(&session, agent.id, goose()),
            Err(Error::Waiting(WaitReason::AlreadyLive))
        ));
        rt.wake(session.id(), Wake::Timer).unwrap();
        let live: Vec<_> = session.bindings().unwrap().iter().map(|b| b.id).collect();
        assert_eq!(live, vec![first]);
    }
}
