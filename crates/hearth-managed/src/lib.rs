//! Thin name-mapping from Claude Tag / Managed Agents onto hearth.
//!
//! **MA Session ≠ hearth Session.**
//! A Managed Agents "session" is a run / conversation handle (process-like).
//! That is closer to [`hearth::Binding`] plus a turn, not the joinable room.
//! The durable multi-party room is [`hearth::Session`].
//!
//! MA terms → hearth:
//! - Agent → [`hearth::Agent`]
//! - Environment / sandbox → [`hearth::Binding::sandbox_id`] and `kind`
//! - MA Session → Binding (+ run), **not** hearth Session
//! - MA Events → [`hearth::Event`] / [`hearth::EventBody`]
//! - Claude Tag **channel** → hearth [`hearth::Session`] (occupancy + one log).
//!   Not a Channel type. Two users steering is two User members on one Session.
//!
//! Resume after environment recycle: unbind the old Binding, bind a new one
//! on the same Session, replay [`hearth::Session::surface`].

use hearth::{Binding, BindingId, Event, EventBody, Result, Session, SessionId, UserId};

/// Claude Managed Agents agent id → hearth Agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaAgentId(pub String);

/// MA Environment (sandbox / isolation context).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaEnvironment {
    pub id: String,
    pub runtime: String,
}

/// MA Session: a disposable run. Do not treat this as [`hearth::Session`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaSession {
    pub id: String,
    pub agent: MaAgentId,
    pub environment: Option<MaEnvironment>,
    pub native_resume_id: Option<String>,
}

/// Fields needed to attach an MA run as a hearth binding.
pub struct BindingSpec {
    pub kind: String,
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
}

pub fn binding_from_ma_session(run: &MaSession) -> BindingSpec {
    BindingSpec {
        kind: "claude-managed".into(),
        native_resume_id: run.native_resume_id.clone(),
        sandbox_id: run.environment.as_ref().map(|e| e.id.clone()),
    }
}

/// Documented mismatch: MA session ids are run ids, not room ids.
pub fn ma_session_is_not_hearth_session(run: &MaSession, room: SessionId) -> bool {
    run.id != format!("{}", room.0)
}

pub fn event_from_ma_text(agent: hearth::AgentId, text: impl Into<String>) -> EventBody {
    EventBody::AgentMessage {
        agent,
        text: text.into(),
    }
}

pub fn kind_of(binding: &Binding) -> &str {
    &binding.kind
}

/// MA user event -> hearth UserMessage. Room stays a hearth Session.
pub fn user_event(user: UserId, text: impl Into<String>) -> EventBody {
    EventBody::UserMessage {
        user,
        text: text.into(),
    }
}

/// MA interrupt: UserMessage plus optional unbind of the run Binding.
pub fn interrupt(
    room: &Session,
    user: UserId,
    text: impl Into<String>,
    unbind: Option<BindingId>,
) -> Result<(Event, Option<Event>)> {
    let message = room.append(user_event(user, text))?;
    let released = match unbind {
        Some(id) => Some(room.unbind(id)?),
        None => None,
    };
    Ok((message, released))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hearth::{InMemory, Member};

    #[test]
    fn ma_session_maps_to_binding_not_room() {
        let store = InMemory::new();
        let agent = store.create_agent("tag", "managed");
        let room = store.create_session();
        room.join(Member::Agent(agent.id)).unwrap();

        let run = MaSession {
            id: "ma-sess-42".into(),
            agent: MaAgentId("tag".into()),
            environment: Some(MaEnvironment {
                id: "env-1".into(),
                runtime: "isolated".into(),
            }),
            native_resume_id: Some("resume-ma".into()),
        };
        assert!(ma_session_is_not_hearth_session(&run, room.id()));
        let spec = binding_from_ma_session(&run);
        let binding = room
            .bind(spec.kind, spec.native_resume_id, spec.sandbox_id)
            .unwrap();
        assert_eq!(kind_of(&binding), "claude-managed");
        assert_eq!(binding.sandbox_id.as_deref(), Some("env-1"));
        assert_eq!(binding.native_resume_id.as_deref(), Some("resume-ma"));

        room.unbind(binding.id).unwrap();
        assert!(room
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingReleased { .. })));
        assert!(store.session(room.id()).is_ok());
    }

    #[test]
    fn ma_event_is_hearth_event() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("tag", "");
        let room = store.create_session();
        let ev = room.append(event_from_ma_text(agent.id, "done")).unwrap();
        assert!(matches!(ev.body, EventBody::AgentMessage { .. }));
        room.append(user_event(user.id, "hello")).unwrap();
        let run = room.bind("claude-managed", None, None).unwrap();
        let (msg, released) = interrupt(&room, user.id, "stop", Some(run.id)).unwrap();
        assert!(matches!(msg.body, EventBody::UserMessage { .. }));
        assert!(released.is_some());
        assert!(room.bindings().unwrap().is_empty());
        assert!(store.session(room.id()).is_ok());
    }

    #[test]
    fn recycle_environment_unbind_bind_replay_surface() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("tag", "managed");
        let room = store.create_session();
        room.join(Member::User(user.id)).unwrap();
        room.join(Member::Agent(agent.id)).unwrap();

        let first = MaSession {
            id: "ma-run-1".into(),
            agent: MaAgentId("tag".into()),
            environment: Some(MaEnvironment {
                id: "env-old".into(),
                runtime: "isolated".into(),
            }),
            native_resume_id: Some("resume-old".into()),
        };
        let spec = binding_from_ma_session(&first);
        let old = room
            .bind(spec.kind, spec.native_resume_id, spec.sandbox_id)
            .unwrap();
        room.append(EventBody::UserMessage {
            user: user.id,
            text: "hello".into(),
        })
        .unwrap();
        room.append(EventBody::AgentThink {
            agent: agent.id,
            text: "thinking".into(),
        })
        .unwrap();
        room.append(event_from_ma_text(agent.id, "hi")).unwrap();
        room.append(EventBody::PermissionAsked {
            agent: agent.id,
            request: "bash".into(),
        })
        .unwrap();

        let before = room.surface().unwrap();
        room.unbind(old.id).unwrap();

        let second = MaSession {
            id: "ma-run-2".into(),
            agent: MaAgentId("tag".into()),
            environment: Some(MaEnvironment {
                id: "env-new".into(),
                runtime: "isolated".into(),
            }),
            native_resume_id: Some("resume-new".into()),
        };
        let spec = binding_from_ma_session(&second);
        let new_binding = room
            .bind(spec.kind, spec.native_resume_id, spec.sandbox_id)
            .unwrap();

        assert_eq!(new_binding.sandbox_id.as_deref(), Some("env-new"));
        assert_eq!(room.bindings().unwrap().len(), 1);
        assert_eq!(store.session(room.id()).unwrap().id(), room.id());

        let after = room.surface().unwrap();
        assert_eq!(before, after);
        assert_eq!(after.len(), 2);
        assert!(after.iter().all(|e| matches!(
            e.body,
            EventBody::UserMessage { .. } | EventBody::AgentMessage { .. }
        )));
        assert!(room
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingReleased { .. })));
        assert!(room
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingAttached { .. })));
    }
}
