//! Thin Host attach: one existing Binding + a [`CodingAgent`].
//!
//! Same class as [`hearth::PlaceMemory`] / [`hearth::FakeSandbox`] — **not** a
//! seventh product noun and not a Queue. [`HostAttach::attach`] / [`HostAttach::resume`]
//! never remint [`hearth::Binding::id`]. Compact goes through
//! [`hearth::Session::compact_with_handoff`] (PreCompactHandoff) then
//! [`AgentCommand::Compact`]; it does not replace EventLog compact.
//!
//! SoftExpiring / DualGate / AdmitCommit / Flush-before-dispatch stay parked.

use crate::{AgentCommand, AgentEvent, AgentKind, BusError, CodingAgent, LoopbackAgent};
use hearth::{
    AgentId, Binding, Event, EventBody, EventId, Host, HostKind, PlaceMemory, Session, UserId,
    WorkingState,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AttachError {
    #[error(transparent)]
    Hearth(#[from] hearth::Error),
    #[error(transparent)]
    Bus(#[from] BusError),
}

pub type AttachResult<T> = std::result::Result<T, AttachError>;

/// Binding `kind` string for an [`AgentKind`]. [`AgentKind::GrokBuild`] is
/// `grok_build`, distinct from chat [`HostKind::Grok`] (`grok`).
pub fn binding_kind(kind: AgentKind) -> &'static str {
    match kind {
        AgentKind::GrokBuild => HostKind::GrokBuild.as_str(),
        AgentKind::Codex => HostKind::Codex.as_str(),
        AgentKind::Pi => HostKind::Pi.as_str(),
        AgentKind::OpenCode => HostKind::OpenCode.as_str(),
    }
}

/// Typed [`Host`] ticket for a bus agent kind. Does not mint a Binding.
pub fn host_for(
    kind: AgentKind,
    native_resume_id: Option<String>,
    sandbox_id: Option<String>,
) -> Host {
    Host::from_bind(binding_kind(kind), native_resume_id, sandbox_id)
}

/// Map one inbound [`AgentEvent`] onto EventLog bodies.
///
/// Lifecycle / native / compact-ack events return empty: [`HostAttach::drain`]
/// uses [`Session::turn_start`] / [`Session::turn_end`] for turns, records the
/// native session id without reminting Binding, and never auto-appends
/// [`EventBody::Compact`] (that is PreCompactHandoff's job).
pub fn event_bodies(agent: AgentId, ev: &AgentEvent) -> Vec<EventBody> {
    match ev {
        AgentEvent::Message { role, text, .. }
            if role == "assistant" || role == "agent" || role == "model" =>
        {
            vec![EventBody::AgentMessage {
                agent,
                text: text.clone(),
            }]
        }
        AgentEvent::MessageDelta { delta, .. } if !delta.is_empty() => {
            vec![EventBody::AgentMessage {
                agent,
                text: delta.clone(),
            }]
        }
        AgentEvent::Thinking { text, .. } => vec![EventBody::AgentThink {
            agent,
            text: text.clone(),
        }],
        AgentEvent::ThinkingDelta { delta, .. } if !delta.is_empty() => {
            vec![EventBody::AgentThink {
                agent,
                text: delta.clone(),
            }]
        }
        AgentEvent::ToolCall {
            name, arguments, ..
        } => vec![EventBody::ToolCall {
            agent,
            name: name.clone(),
            input: arguments.to_string(),
        }],
        AgentEvent::ToolResult {
            item_id, output, ..
        } => vec![EventBody::ToolResult {
            agent,
            name: item_id.clone(),
            output: output.to_string(),
        }],
        AgentEvent::Plan { entries, .. } if !entries.is_empty() => {
            vec![EventBody::AgentMessage {
                agent,
                text: entries.join("\n"),
            }]
        }
        AgentEvent::PermissionAsk {
            permission_id,
            title,
            description,
            ..
        } => {
            let request = description
                .clone()
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| title.clone());
            vec![EventBody::PermissionAsked {
                agent,
                request: format!("{permission_id}:{request}"),
            }]
        }
        AgentEvent::QuestionAsk { prompts, .. } => vec![EventBody::AskUser {
            agent,
            prompt: prompts.join("\n"),
        }],
        AgentEvent::TaskCompleted { task_id, ok, .. } => vec![EventBody::AgentMessage {
            agent,
            text: format!("task {task_id} {}", if *ok { "ok" } else { "failed" }),
        }],
        _ => vec![],
    }
}

/// One existing Binding plus a [`CodingAgent`]. Not a kernel noun.
pub struct HostAttach<A: CodingAgent> {
    binding: Binding,
    agent: AgentId,
    coding: A,
    native_session: Option<String>,
}

impl<A: CodingAgent> HostAttach<A> {
    /// Attach to an **existing** Binding. Does not remint [`Binding::id`].
    pub fn attach(binding: Binding, agent: AgentId, coding: A) -> Self {
        let native_session = binding.native_resume_id.clone();
        Self {
            binding,
            agent,
            coding,
            native_session,
        }
    }

    /// Resume is attach: same Binding id, same native resume token if present.
    pub fn resume(binding: Binding, agent: AgentId, coding: A) -> Self {
        Self::attach(binding, agent, coding)
    }

    /// Mint Binding once via [`Session::bind_host`], then attach. Further
    /// reconnects must use [`Self::attach`] / [`Self::resume`].
    pub fn bind(session: &Session, agent: AgentId, host: Host, coding: A) -> AttachResult<Self> {
        let binding = session.bind_host(Some(agent), host)?;
        Ok(Self::attach(binding, agent, coding))
    }

    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    pub fn agent_id(&self) -> AgentId {
        self.agent
    }

    pub fn native_session(&self) -> Option<&str> {
        self.native_session.as_deref()
    }

    pub fn kind(&self) -> AgentKind {
        self.coding.kind()
    }

    fn wire_session(&self) -> String {
        self.native_session
            .clone()
            .unwrap_or_else(|| format!("{}", self.binding.id.0))
    }

    pub fn send(&mut self, cmd: AgentCommand) -> AttachResult<()> {
        self.coding.send(cmd)?;
        Ok(())
    }

    /// Open the native agent session. Does not remint Binding.
    pub fn open(&mut self, cwd: Option<String>) -> AttachResult<()> {
        self.coding.send(AgentCommand::OpenSession {
            project_id: self
                .binding
                .sandbox_id
                .clone()
                .unwrap_or_else(|| format!("{}", self.binding.id.0)),
            cwd,
        })?;
        Ok(())
    }

    /// Occupancy-checked UserMessage on the Session, then outbound command.
    pub fn user_message(
        &mut self,
        session: &Session,
        user: UserId,
        text: impl Into<String>,
    ) -> AttachResult<Event> {
        let text = text.into();
        let ev = session.user_message(user, text.clone())?;
        self.coding.send(AgentCommand::UserMessage {
            session_id: self.wire_session(),
            text,
        })?;
        Ok(ev)
    }

    /// Steer is a UserMessage on the log plus outbound [`AgentCommand::Steer`].
    pub fn steer(
        &mut self,
        session: &Session,
        user: UserId,
        text: impl Into<String>,
    ) -> AttachResult<Event> {
        let text = text.into();
        let ev = session.user_message(user, text.clone())?;
        self.coding.send(AgentCommand::Steer {
            session_id: self.wire_session(),
            text,
        })?;
        Ok(ev)
    }

    pub fn reply_permission(
        &mut self,
        session: &Session,
        request: impl Into<String>,
        allowed: bool,
        by: UserId,
        permission_id: impl Into<String>,
    ) -> AttachResult<Event> {
        let ev = session.decide_permission(request, allowed, by)?;
        self.coding.send(AgentCommand::ReplyPermission {
            session_id: self.wire_session(),
            permission_id: permission_id.into(),
            allow: allowed,
            option_id: None,
        })?;
        Ok(ev)
    }

    /// PreCompactHandoff (Place `handoff.md` + EventLog Compact) then
    /// [`AgentCommand::Compact`]. Does not replace [`Session::compact`].
    pub fn compact_with_handoff(
        &mut self,
        session: &Session,
        memory: &PlaceMemory,
        start: EventId,
        end: EventId,
        working: Option<WorkingState>,
    ) -> AttachResult<Event> {
        let ev = session.compact_with_handoff(memory, self.binding.id, start, end, working)?;
        self.coding.send(AgentCommand::Compact {
            session_id: self.wire_session(),
        })?;
        self.drain(session)?;
        Ok(ev)
    }

    /// Drain inbound [`AgentEvent`]s onto the Session EventLog.
    pub fn drain(&mut self, session: &Session) -> AttachResult<Vec<Event>> {
        let mut out = Vec::new();
        while let Some(ev) = self.coding.try_recv()? {
            out.extend(self.apply(session, ev)?);
        }
        Ok(out)
    }

    fn apply(&mut self, session: &Session, ev: AgentEvent) -> AttachResult<Vec<Event>> {
        match &ev {
            AgentEvent::SessionStarted { session_id, .. } => {
                self.native_session = Some(session_id.clone());
                Ok(vec![])
            }
            AgentEvent::TurnStarted { .. } => Ok(vec![session.turn_start(self.agent)?]),
            AgentEvent::TurnCompleted { .. } => Ok(vec![session.turn_end(self.agent)?]),
            _ => {
                let mut appended = Vec::new();
                for body in event_bodies(self.agent, &ev) {
                    appended.push(session.append(body)?);
                }
                Ok(appended)
            }
        }
    }
}

impl HostAttach<LoopbackAgent> {
    /// Bind + LoopbackAgent for Host tests.
    pub fn loopback(session: &Session, agent: AgentId, kind: AgentKind) -> AttachResult<Self> {
        Self::bind(
            session,
            agent,
            host_for(kind, None, None),
            LoopbackAgent::new(kind),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentKind, LoopbackAgent};
    use hearth::{
        EventBody, HostKind, InMemory, Member, MemoryPolicy, Place, PlaceAttach, PlaceMemory,
    };

    fn room() -> (InMemory, hearth::User, hearth::Agent, Session) {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("coder", "");
        let session = store.create_session();
        session.join(Member::User(user.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        (store, user, agent, session)
    }

    #[test]
    fn host_loopback_roundtrip_records_eventlog() {
        let (_store, user, agent, session) = room();
        let mut attach = HostAttach::loopback(&session, agent.id, AgentKind::Codex).unwrap();
        let binding_id = attach.binding().id;
        assert_eq!(attach.binding().kind, "codex");
        assert_eq!(attach.binding().agent, Some(agent.id));

        attach.open(None).unwrap();
        attach.drain(&session).unwrap();
        assert_eq!(attach.native_session(), Some("sess-loop"));

        attach.user_message(&session, user.id, "hi").unwrap();
        let drained = attach.drain(&session).unwrap();
        assert!(drained
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnStart { .. })));
        assert!(drained.iter().any(|e| matches!(
            &e.body,
            EventBody::AgentMessage { text, .. } if text == "echo:hi"
        )));
        assert!(drained
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnEnd { .. })));

        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "hi"
        )));
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::AgentMessage { text, .. } if text == "echo:hi"
        )));
        let surface = session.surface().unwrap();
        assert!(surface.iter().any(|e| matches!(e.body, EventBody::UserMessage { .. })));
        assert!(surface.iter().any(|e| matches!(e.body, EventBody::AgentMessage { .. })));
        assert!(!surface
            .iter()
            .any(|e| matches!(e.body, EventBody::TurnStart { .. })));

        // Resume: same Binding id, no remint.
        let binding = session
            .bindings()
            .unwrap()
            .into_iter()
            .find(|b| b.id == binding_id)
            .unwrap();
        let attach2 = HostAttach::resume(binding, agent.id, LoopbackAgent::new(AgentKind::Codex));
        assert_eq!(attach2.binding().id, binding_id);
        assert_eq!(session.bindings().unwrap().len(), 1);
        assert_eq!(session.bindings().unwrap()[0].id, binding_id);
    }

    #[test]
    fn attach_does_not_remint_existing_binding() {
        let (_store, _user, agent, session) = room();
        let first = session
            .bind_host(
                Some(agent.id),
                HostKind::GrokBuild.host(Some("native-1".into()), Some("box".into())),
            )
            .unwrap();
        let id = first.id;
        let attach = HostAttach::attach(first.clone(), agent.id, LoopbackAgent::new(AgentKind::GrokBuild));
        assert_eq!(attach.binding().id, id);
        assert_eq!(attach.native_session(), Some("native-1"));
        let again = HostAttach::resume(first, agent.id, LoopbackAgent::new(AgentKind::GrokBuild));
        assert_eq!(again.binding().id, id);
        assert_eq!(session.bindings().unwrap().len(), 1);
    }

    #[test]
    fn compact_composes_with_precompact_handoff() {
        let (_store, user, agent, session) = room();
        let dir = std::env::temp_dir().to_string_lossy().into_owned();
        let place = session
            .attach_place(Place::local_dir(dir, PlaceAttach::MustExist))
            .unwrap();
        let mut attach = HostAttach::loopback(&session, agent.id, AgentKind::Pi).unwrap();
        let mem = PlaceMemory::for_place(&place, MemoryPolicy::PlaceBacked);
        mem.acquire_claim(attach.binding().id).unwrap();
        mem.set_memory_md(attach.binding().id, "index: keep going")
            .unwrap();

        attach.open(None).unwrap();
        attach.drain(&session).unwrap();
        attach.user_message(&session, user.id, "old-a").unwrap();
        attach.drain(&session).unwrap();

        let events = session.events().unwrap();
        let start = events
            .iter()
            .find(|e| matches!(&e.body, EventBody::UserMessage { text, .. } if text == "old-a"))
            .unwrap()
            .id;
        let end = events
            .iter()
            .find(|e| matches!(&e.body, EventBody::AgentMessage { text, .. } if text == "echo:old-a"))
            .unwrap()
            .id;

        let compact = attach
            .compact_with_handoff(
                &session,
                &mem,
                start,
                end,
                Some(WorkingState {
                    objective: "ship bus".into(),
                    next: "review".into(),
                    touched_files: vec!["crates/hearth-agent-bus/src/host.rs".into()],
                }),
            )
            .unwrap();
        assert!(matches!(
            &compact.body,
            EventBody::Compact { summary, .. }
                if summary.contains("handoff.md") && summary.contains(&place.id.0.to_string())
        ));
        assert!(mem.handoff().unwrap().unwrap().contains("ship bus"));
        // Agent compact-ack must not append a second EventLog Compact.
        let compact_count = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::Compact { .. }))
            .count();
        assert_eq!(compact_count, 1);
        assert_eq!(session.bindings().unwrap()[0].id, attach.binding().id);
    }

    #[test]
    fn framed_agent_jsonrpc_push_decoded_appends_tool_call() {
        use crate::adapters::grok_build::GrokBuildCodec;
        use crate::{FramedAgent, JsonRpcTransport};

        let (_store, _user, agent, session) = room();
        let binding = session
            .bind_host(Some(agent.id), host_for(AgentKind::GrokBuild, None, None))
            .unwrap();
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "adapters/grok_build/fixtures/session_update_tool_call.json"
        ))
        .unwrap();
        let mut agent_io = FramedAgent::new(JsonRpcTransport::new(), GrokBuildCodec);
        agent_io.transport_mut().push_decoded(raw);
        let mut attach = HostAttach::attach(binding, agent.id, agent_io);
        attach.drain(&session).unwrap();
        assert!(session.events().unwrap().iter().any(|e| matches!(
            &e.body,
            EventBody::ToolCall { name, .. } if name == "read"
        )));
    }
}
