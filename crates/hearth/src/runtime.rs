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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::{
    now_ts, AgentId, Error, Event, EventBody, Host, InMemory, Member, Result, Session, SessionId,
    WaitReason, WakeSource,
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

/// Internal keep record: recipe plus optional timer.
struct Kept {
    agent: AgentId,
    host: Host,
    /// If Some, [`Runtime::tick`] wakes when `now >= next_due_ms` (ms since epoch,
    /// same clock as [`crate::Event::ts`]).
    timer_every_ms: Option<u64>,
    next_due_ms: Option<u64>,
}

/// Long-lived keeper. Owns an [`InMemory`] store; clone the handle out via
/// [`Runtime::store`] so Session methods still work.
#[derive(Clone)]
pub struct Runtime {
    store: InMemory,
    kept: Arc<Mutex<HashMap<SessionId, Kept>>>,
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
        let (agent, host) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;
        ensure_agent_member(&sess, agent)?;
        match classify_session(&sess, agent)? {
            Liveness::LiveIdle | Liveness::LiveInTurn => Err(Error::Waiting(WaitReason::AlreadyLive)),
            Liveness::Unknown => Err(Error::Waiting(WaitReason::LivenessUnknown)),
            Liveness::NeverBound | Liveness::PositivelyDead => {
                Ok(sess.bind_host(Some(agent), host)?.id)
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
        let (agent, host) = self.kept_recipe(session)?;
        let sess = self.store.session(session)?;
        let start_len = sess.events()?.len();
        ensure_agent_member(&sess, agent)?;

        match classify_session(&sess, agent)? {
            Liveness::LiveInTurn => return Err(Error::Waiting(WaitReason::TurnOpen)),
            Liveness::Unknown => return Err(Error::Waiting(WaitReason::LivenessUnknown)),
            Liveness::LiveIdle => {}
            Liveness::NeverBound | Liveness::PositivelyDead => {
                sess.bind_host(Some(agent), host)?;
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
        assert!(matches!(
            evs2,
            Err(Error::Waiting(WaitReason::TurnOpen))
        ));
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
                Ok(evs) if evs.iter().any(|e| matches!(e.body, EventBody::TurnStart { .. })) => {
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
}
