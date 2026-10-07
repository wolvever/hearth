//! Thin name-mapping from Paseo onto hearth.
//!
//! This crate does **not** reimplement the Paseo daemon, worktree manager,
//! or CLI supervisor. It only documents and converts identifiers.
//!
//! Paseo terms → hearth:
//! - client `Session` (the UI/room) → [`hearth::Session`]
//! - `ManagedAgent` (long-lived agent definition) → [`hearth::Agent`]
//! - worktree + running CLI/runtime → [`hearth::Binding`]
//! - native resume / Claude session id on the worktree process →
//!   [`hearth::Binding::native_resume_id`], never [`hearth::Session::id`]
//!
//! Tension: Paseo "session" often means both the chat room *and* the live
//! CLI. hearth keeps those as Session vs Binding.
//!
//! Multica **Issue** (if a host surfaces one) maps to a hearth Session, not
//! a kernel Issue/Thread/Squad type. Adapter-only name; refused in kernel.
//!
//! Paseo's timeline is RAM and rehydrates from the provider. hearth records
//! that as `Binding.native_resume_id` plus optional imported events on the
//! one session log — not a second log.

use hearth::{Binding, EventBody, Result, Session, SessionId};

/// Paseo client/UI session id. Maps to a hearth session (the room).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaseoSessionId(pub String);

/// Paseo ManagedAgent id. Maps to a hearth agent identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaseoManagedAgentId(pub String);

/// Paseo worktree / runtime attachment. Maps to a hearth binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaseoWorktree {
    pub id: String,
    /// Native Claude/Codex resume token, if the CLI is still resumable.
    pub native_resume_id: Option<String>,
}

/// Remember which hearth session a Paseo client session occupies.
pub fn room_from_paseo(id: &PaseoSessionId, known: SessionId) -> Option<SessionId> {
    if id.0 == format!("{}", known.0) {
        Some(known)
    } else {
        None
    }
}

/// Project a Paseo worktree onto a hearth binding's resume fields.
pub fn binding_from_worktree(kind: &str, worktree: &PaseoWorktree) -> (String, Option<String>) {
    let _ = kind;
    (worktree.id.clone(), worktree.native_resume_id.clone())
}

/// True when this binding is the live Paseo CLI for a worktree.
pub fn is_paseo_runtime(binding: &Binding) -> bool {
    binding.kind == "paseo-cli" || binding.kind == "worktree"
}

/// A hearth session is the Paseo *room*, not the spawned process.
pub fn session_is_room(_session: &Session) -> bool {
    true
}

/// Provider hydrate: store the provider resume token on a Binding and
/// optionally import events into the existing session log. Not a second log.
pub fn hydrate_from_provider(
    session: &Session,
    kind: impl Into<String>,
    native_resume_id: impl Into<String>,
    imported: impl IntoIterator<Item = EventBody>,
) -> Result<Binding> {
    let binding = session.bind(kind, Some(native_resume_id.into()), None)?;
    for body in imported {
        session.append(body)?;
    }
    Ok(binding)
}

/// Paseo permission_requested -> kernel PermissionAsked. No RPC, no client Session type.
pub fn permission_requested(agent: hearth::AgentId, request: impl Into<String>) -> EventBody {
    EventBody::PermissionAsked {
        agent,
        request: request.into(),
        rpc: None,
    }
}

/// Paseo permission_resolved -> kernel PermissionDecided (who decided is a User).
pub fn permission_resolved(
    request: impl Into<String>,
    allowed: bool,
    by: hearth::UserId,
) -> EventBody {
    EventBody::PermissionDecided {
        request: request.into(),
        allowed,
        by,
        rpc: None,
        option_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hearth::{EventBody, InMemory, Member};

    #[test]
    fn worktree_resume_lands_on_binding() {
        let store = InMemory::new();
        let session = store.create_session();
        let wt = PaseoWorktree {
            id: "wt-1".into(),
            native_resume_id: Some("claude-resume-9".into()),
        };
        let (sandbox, resume) = binding_from_worktree("paseo-cli", &wt);
        let binding = session.bind("paseo-cli", resume, Some(sandbox)).unwrap();
        assert_eq!(binding.native_resume_id.as_deref(), Some("claude-resume-9"));
        assert_ne!(format!("{}", session.id().0), "claude-resume-9");
        assert!(is_paseo_runtime(&binding));
        assert!(session_is_room(&session));
    }

    #[test]
    fn paseo_agent_joins_hearth_session() {
        let store = InMemory::new();
        let _paseo_agent = PaseoManagedAgentId("ma-1".into());
        let agent = store.create_agent("paseo-managed", "follow the user");
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        session
            .append(EventBody::ConfigSet {
                key: "model".into(),
                value: "default".into(),
            })
            .unwrap();
        let user = store.create_user("cheng");
        session.join(Member::User(user.id)).unwrap();
        session
            .append(permission_requested(agent.id, "ls"))
            .unwrap();
        session
            .append(permission_resolved("ls", true, user.id))
            .unwrap();
        assert_eq!(session.members().unwrap().len(), 2);
        assert!(session
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::PermissionAsked { .. })));
        assert!(session
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::PermissionDecided { .. })));
    }

    #[test]
    fn provider_hydrate_is_binding_plus_imported_events() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("paseo-managed", "");
        let session = store.create_session();
        session.join(Member::User(user.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();

        let imported = vec![
            EventBody::UserMessage {
                user: user.id,
                text: "from provider".into(),
            },
            EventBody::AgentThink {
                agent: agent.id,
                text: "hidden from surface".into(),
            },
            EventBody::AgentMessage {
                agent: agent.id,
                text: "ok".into(),
            },
        ];
        let binding =
            hydrate_from_provider(&session, "paseo-cli", "provider-resume-77", imported).unwrap();
        assert_eq!(
            binding.native_resume_id.as_deref(),
            Some("provider-resume-77")
        );
        assert_eq!(session.bindings().unwrap().len(), 1);
        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "from provider"
        )));
        assert!(log
            .iter()
            .any(|e| matches!(e.body, EventBody::AgentThink { .. })));
        let surface = session.surface().unwrap();
        assert_eq!(surface.len(), 2);
        assert!(!surface
            .iter()
            .any(|e| matches!(e.body, EventBody::AgentThink { .. })));
        assert!(!surface
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingAttached { .. })));
    }
}
