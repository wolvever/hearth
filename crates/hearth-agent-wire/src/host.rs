//! Thin Host attach: one existing Binding + a [`CodingAgent`].
//!
//! Same class as [`hearth::PlaceMemory`] / [`hearth::FakeSandbox`] — **not** a
//! seventh product noun and not a Queue. [`HostAttach::attach`] / [`HostAttach::resume`]
//! never remint [`hearth::Binding::id`]. Compact goes through
//! [`hearth::Session::compact_with_handoff`] (PreCompactHandoff) then
//! [`AgentCommand::Compact`]; it does not replace EventLog compact.
//!
//! SoftExpiring / DualGate / AdmitCommit / Flush-before-dispatch stay parked.

use std::collections::HashMap;

use crate::{
    AgentCommand, AgentEvent, AgentKind, BusError, CodingAgent, LoopbackAgent, PermissionOption,
    RpcId,
};
use hearth::{
    AgentId, Binding, BindingId, Event, EventBody, EventId, Host, HostKind, PermissionRpc,
    PlaceMemory, Session, UserId, WorkingState,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AttachError {
    #[error(transparent)]
    Hearth(#[from] hearth::Error),
    #[error(transparent)]
    Bus(#[from] BusError),
    /// No pending ask on this Binding with that JSON-RPC id (already
    /// answered, cancelled, or never asked).
    #[error("no pending permission ask with rpc id {0}")]
    UnknownPermission(RpcId),
    /// The reply names an optionId the agent did not offer on that ask.
    #[error("optionId {option_id:?} was not offered on permission ask {rpc_id}")]
    OptionNotOffered { rpc_id: RpcId, option_id: String },
    /// Agent reused a JSON-RPC id that is still awaiting a response.
    #[error("permission ask rpc id {0} is already pending")]
    DuplicateRpcId(RpcId),
}

/// One in-flight JSON-RPC permission ask the agent is blocked on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAsk {
    pub rpc_id: RpcId,
    /// Display / correlation only — not unique (copilot-cli #989).
    pub tool_call_id: Option<String>,
    pub title: String,
    pub options: Vec<PermissionOption>,
}

impl PendingAsk {
    fn offers(&self, option_id: &str) -> Option<&PermissionOption> {
        self.options.iter().find(|o| o.option_id == option_id)
    }
}

/// ACP option kinds `allow_once` / `allow_always` are allows; reject_* are not.
pub(crate) fn option_allows(opt: &PermissionOption) -> bool {
    if opt.kind.is_empty() {
        opt.option_id.starts_with("allow")
    } else {
        opt.kind.starts_with("allow")
    }
}

pub type AttachResult<T> = std::result::Result<T, AttachError>;

/// Binding `kind` string for an [`AgentKind`]. [`AgentKind::GrokBuild`] is
/// `grok_build`, distinct from chat [`HostKind::Grok`] (`grok`).
/// [`AgentKind::Acp`] is `"acp"` — `HostKind` has no Acp variant; [`host_for`]
/// goes through [`Host::from_bind`] → [`Host::Other`]. Claude later uses the
/// existing [`HostKind::ClaudeCode`] (`claude_code`).
pub fn binding_kind(kind: AgentKind) -> &'static str {
    match kind {
        AgentKind::GrokBuild => HostKind::GrokBuild.as_str(),
        AgentKind::Codex => HostKind::Codex.as_str(),
        AgentKind::Pi => HostKind::Pi.as_str(),
        AgentKind::OpenCode => HostKind::OpenCode.as_str(),
        AgentKind::Acp => "acp",
    }
}

/// Typed [`Host`] ticket for an agent-wire kind. Does not mint a Binding.
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
///
/// A JSON-RPC permission ask becomes [`EventBody::PermissionAsked`] with a
/// typed [`PermissionRpc`] (`binding`, `rpc_id`, `tool_call_id`) — not an
/// `"id:title"` string.
pub fn event_bodies(binding: BindingId, agent: AgentId, ev: &AgentEvent) -> Vec<EventBody> {
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
            item_id,
            name,
            arguments,
            ..
        } => vec![EventBody::ToolCall {
            agent,
            tool_call_id: item_id.clone(),
            name: name.clone(),
            input: arguments.to_string(),
        }],
        AgentEvent::ToolResult {
            item_id, output, ..
        } => vec![EventBody::ToolResult {
            agent,
            tool_call_id: item_id.clone(),
            // Wire ToolResult has no tool name; keep id as display name.
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
            title,
            description,
            tool_item_id,
            rpc_id,
            ..
        } => {
            let request = description
                .clone()
                .filter(|d| !d.is_empty())
                .unwrap_or_else(|| title.clone());
            vec![EventBody::PermissionAsked {
                agent,
                request,
                rpc: rpc_id.as_ref().map(|id| PermissionRpc {
                    binding,
                    rpc_id: id.to_string(),
                    tool_call_id: tool_item_id.clone(),
                }),
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
///
/// In-flight JSON-RPC permission asks are keyed by `(Binding, rpc id)` —
/// never "the current one" (claude-agent-acp #851) and never by
/// `toolCallId` alone (copilot-cli #989).
pub struct HostAttach<A: CodingAgent> {
    binding: Binding,
    agent: AgentId,
    coding: A,
    native_session: Option<String>,
    pending: HashMap<(BindingId, RpcId), PendingAsk>,
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
            pending: HashMap::new(),
        }
    }

    /// Resume is attach: same Binding id, same native resume token if present.
    /// Not Gemini-style `session/load` — catalog profiles do not add a load RPC.
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

    /// In-flight permission asks on this Binding, ordered by rpc id.
    pub fn pending_permissions(&self) -> Vec<PendingAsk> {
        let mut v: Vec<PendingAsk> = self
            .pending
            .iter()
            .filter(|((b, _), _)| *b == self.binding.id)
            .map(|(_, a)| a.clone())
            .collect();
        v.sort_by(|a, b| a.rpc_id.cmp(&b.rpc_id));
        v
    }

    fn permission_rpc(&self, ask: &PendingAsk) -> PermissionRpc {
        PermissionRpc {
            binding: self.binding.id,
            rpc_id: ask.rpc_id.to_string(),
            tool_call_id: ask.tool_call_id.clone(),
        }
    }

    /// Answer one ask on its JSON-RPC id with `{outcome:"selected", optionId}`.
    /// `option_id` must be one the agent offered on that ask; otherwise
    /// [`AttachError::OptionNotOffered`] and the ask stays pending.
    pub fn answer_permission(
        &mut self,
        session: &Session,
        by: UserId,
        rpc_id: &RpcId,
        option_id: &str,
    ) -> AttachResult<Event> {
        let key = (self.binding.id, rpc_id.clone());
        let ask = self
            .pending
            .get(&key)
            .cloned()
            .ok_or_else(|| AttachError::UnknownPermission(rpc_id.clone()))?;
        let Some(opt) = ask.offers(option_id) else {
            return Err(AttachError::OptionNotOffered {
                rpc_id: rpc_id.clone(),
                option_id: option_id.into(),
            });
        };
        let allowed = option_allows(opt);
        self.coding.send(AgentCommand::ReplyPermission {
            session_id: self.wire_session(),
            permission_id: rpc_id.to_string(),
            allow: allowed,
            option_id: Some(option_id.into()),
            rpc_id: Some(rpc_id.clone()),
        })?;
        self.pending.remove(&key);
        let ev = session.decide_permission_rpc(
            ask.title.clone(),
            allowed,
            by,
            self.permission_rpc(&ask),
            Some(option_id.into()),
        )?;
        Ok(ev)
    }

    /// Cancel the turn: answer **every** pending ask on this Binding with
    /// `{outcome:"cancelled"}` (ACP MUST), then send [`AgentCommand::Abort`]
    /// (`session/cancel`). Returns the Host decisions recorded.
    pub fn cancel(&mut self, session: &Session, by: UserId) -> AttachResult<Vec<Event>> {
        let mut out = Vec::new();
        for ask in self.pending_permissions() {
            self.coding.send(AgentCommand::ReplyPermission {
                session_id: self.wire_session(),
                permission_id: ask.rpc_id.to_string(),
                allow: false,
                option_id: None,
                rpc_id: Some(ask.rpc_id.clone()),
            })?;
            self.pending.remove(&(self.binding.id, ask.rpc_id.clone()));
            out.push(session.decide_permission_rpc(
                ask.title.clone(),
                false,
                by,
                self.permission_rpc(&ask),
                None,
            )?);
        }
        match self.coding.send(AgentCommand::Abort {
            session_id: self.wire_session(),
        }) {
            Ok(()) | Err(BusError::Unsupported(_)) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(out)
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
            AgentEvent::PermissionAsk {
                rpc_id: Some(rpc_id),
                tool_item_id,
                title,
                options,
                ..
            } => {
                let key = (self.binding.id, rpc_id.clone());
                if self.pending.contains_key(&key) {
                    return Err(AttachError::DuplicateRpcId(rpc_id.clone()));
                }
                self.pending.insert(
                    key,
                    PendingAsk {
                        rpc_id: rpc_id.clone(),
                        tool_call_id: tool_item_id.clone(),
                        title: title.clone(),
                        options: options.clone(),
                    },
                );
                let mut appended = Vec::new();
                for body in event_bodies(self.binding.id, self.agent, &ev) {
                    appended.push(session.append(body)?);
                }
                Ok(appended)
            }
            _ => {
                let mut appended = Vec::new();
                let log = session.events()?;
                for body in event_bodies(self.binding.id, self.agent, &ev) {
                    // Drop late ToolResult once tool_call_id is already
                    // terminal (ToolResult or ToolCallInterrupted) so one
                    // call never gets two terminals (2026-10-07 remint).
                    if let EventBody::ToolResult { tool_call_id, .. } = &body {
                        if hearth::tool_call_is_terminal(&log, tool_call_id) {
                            continue;
                        }
                    }
                    let ev = session.append(body)?;
                    appended.push(ev);
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
        assert!(surface
            .iter()
            .any(|e| matches!(e.body, EventBody::UserMessage { .. })));
        assert!(surface
            .iter()
            .any(|e| matches!(e.body, EventBody::AgentMessage { .. })));
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
        let attach = HostAttach::attach(
            first.clone(),
            agent.id,
            LoopbackAgent::new(AgentKind::GrokBuild),
        );
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
            .find(
                |e| matches!(&e.body, EventBody::AgentMessage { text, .. } if text == "echo:old-a"),
            )
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
                    touched_files: vec!["crates/hearth-agent-wire/src/host.rs".into()],
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
    fn acp_binding_kind_is_host_other() {
        assert_eq!(binding_kind(AgentKind::Acp), "acp");
        let host = host_for(AgentKind::Acp, Some("native-acp".into()), None);
        assert_eq!(host.kind_str(), "acp");
        assert!(matches!(
            &host,
            Host::Other {
                kind,
                native_resume_id: Some(id),
                sandbox_id: None,
            } if kind == "acp" && id == "native-acp"
        ));
    }

    #[test]
    fn framed_agent_jsonl_pi_rpc_appends_tool_call() {
        use crate::adapters::pi::PiCodec;
        use crate::{FramedAgent, JsonlRpcTransport};

        let (_store, _user, agent, session) = room();
        let binding = session
            .bind_host(Some(agent.id), host_for(AgentKind::Pi, None, None))
            .unwrap();
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "adapters/pi/fixtures/tool_execution_start.json"
        ))
        .unwrap();
        let mut agent_io = FramedAgent::new(JsonlRpcTransport::new(), PiCodec);
        agent_io
            .transport_mut()
            .push_bytes(&JsonlRpcTransport::encode_jsonl(&raw).unwrap());
        let mut attach = HostAttach::attach(binding, agent.id, agent_io);
        attach.drain(&session).unwrap();
        assert!(session.events().unwrap().iter().any(|e| matches!(
            &e.body,
            EventBody::ToolCall { name, .. } if name == "bash"
        )));
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

    fn acp_attach(
        session: &Session,
        agent: AgentId,
    ) -> HostAttach<crate::FramedAgent<crate::JsonlRpcTransport, crate::adapters::acp::AcpCodec>>
    {
        let binding = session
            .bind_host(
                Some(agent),
                host_for(AgentKind::Acp, Some("S-1".into()), None),
            )
            .unwrap();
        HostAttach::attach(
            binding,
            agent,
            crate::FramedAgent::new(
                crate::JsonlRpcTransport::without_rpc_chunks(),
                crate::adapters::acp::AcpCodec,
            ),
        )
    }

    fn push_ask(
        attach: &mut HostAttach<
            crate::FramedAgent<crate::JsonlRpcTransport, crate::adapters::acp::AcpCodec>,
        >,
        rpc_id: serde_json::Value,
        tool_call_id: &str,
    ) {
        let raw = serde_json::json!({
            "jsonrpc": "2.0",
            "id": rpc_id,
            "method": "session/request_permission",
            "params": {
                "sessionId": "S-1",
                "toolCall": {"toolCallId": tool_call_id, "title": "bash"},
                "options": [
                    {"optionId": "allow-once", "name": "Allow", "kind": "allow_once"},
                    {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"}
                ]
            }
        });
        attach
            .coding
            .transport_mut()
            .push_bytes(&crate::JsonlRpcTransport::encode_jsonl(&raw).unwrap());
    }

    #[test]
    fn acp_permission_asks_keyed_by_binding_and_rpc_id() {
        let (_store, user, agent, session) = room();
        let mut attach = acp_attach(&session, agent.id);
        let bid = attach.binding().id;
        // copilot-cli #989: both asks share toolCallId.
        push_ask(&mut attach, serde_json::json!(1), "shell-permission");
        push_ask(&mut attach, serde_json::json!("1"), "shell-permission");
        attach.drain(&session).unwrap();
        assert_eq!(attach.pending_permissions().len(), 2);

        let asked: Vec<PermissionRpc> = session
            .events()
            .unwrap()
            .into_iter()
            .filter_map(|e| match e.body {
                EventBody::PermissionAsked { rpc, request, .. } => {
                    assert_eq!(request, "bash", "no \"id:title\" string");
                    rpc
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            asked,
            vec![
                PermissionRpc {
                    binding: bid,
                    rpc_id: "1".into(),
                    tool_call_id: Some("shell-permission".into()),
                },
                PermissionRpc {
                    binding: bid,
                    rpc_id: "\"1\"".into(),
                    tool_call_id: Some("shell-permission".into()),
                },
            ]
        );

        // Unoffered option / unknown id → typed errors, nothing written.
        let err = attach
            .answer_permission(&session, user.id, &RpcId::Num(1), "allow")
            .unwrap_err();
        assert!(matches!(err, AttachError::OptionNotOffered { .. }));
        let err = attach
            .answer_permission(&session, user.id, &RpcId::Num(9), "allow-once")
            .unwrap_err();
        assert!(matches!(err, AttachError::UnknownPermission(RpcId::Num(9))));
        assert!(attach.coding.transport().outbound().is_empty());
        assert_eq!(attach.pending_permissions().len(), 2);

        let decided = attach
            .answer_permission(&session, user.id, &RpcId::Str("1".into()), "reject-once")
            .unwrap();
        assert!(matches!(
            &decided.body,
            EventBody::PermissionDecided { allowed: false, option_id: Some(o), rpc: Some(r), .. }
                if o == "reject-once" && r.rpc_id == "\"1\""
        ));
        let out = attach.coding.transport().outbound().to_vec();
        assert_eq!(
            out,
            vec![serde_json::json!({"jsonrpc": "2.0", "id": "1",
                "result": {"outcome": {"outcome": "selected", "optionId": "reject-once"}}})]
        );
        assert_eq!(attach.pending_permissions().len(), 1);
        attach
            .answer_permission(&session, user.id, &RpcId::Num(1), "allow-once")
            .unwrap();
        assert!(
            attach.pending_permissions().is_empty(),
            "map empty after answer"
        );
        assert_eq!(attach.coding.transport().outbound()[1]["id"], 1);
    }

    #[test]
    fn acp_cancel_answers_all_pending_then_session_cancel() {
        let (_store, user, agent, session) = room();
        let mut attach = acp_attach(&session, agent.id);
        push_ask(&mut attach, serde_json::json!(7), "call_ctrl");
        push_ask(&mut attach, serde_json::json!(8), "toolu_sub");
        attach.drain(&session).unwrap();
        let decided = attach.cancel(&session, user.id).unwrap();
        assert_eq!(decided.len(), 2);
        assert!(
            attach.pending_permissions().is_empty(),
            "map empty after cancel"
        );
        let out = attach.coding.transport().outbound().to_vec();
        assert_eq!(out.len(), 3);
        for (frame, id) in out.iter().zip([7, 8]) {
            assert_eq!(frame["id"], id);
            assert_eq!(
                frame["result"]["outcome"],
                serde_json::json!({"outcome": "cancelled"})
            );
        }
        assert_eq!(out[2]["method"], "session/cancel");
    }

    #[test]
    fn acp_malformed_or_duplicate_ask_is_typed_error() {
        let (_store, _user, agent, session) = room();
        let mut attach = acp_attach(&session, agent.id);
        // Notification-shaped ask (no JSON-RPC id) → Bus decode error.
        let raw = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/request_permission",
            "params": {"sessionId": "S-1", "toolCall": {"toolCallId": "t"}, "options": []}
        });
        attach
            .coding
            .transport_mut()
            .push_bytes(&crate::JsonlRpcTransport::encode_jsonl(&raw).unwrap());
        assert!(matches!(
            attach.drain(&session),
            Err(AttachError::Bus(BusError::Decode(_)))
        ));
        // Reused in-flight rpc id → DuplicateRpcId, first ask kept.
        push_ask(&mut attach, serde_json::json!(3), "a");
        push_ask(&mut attach, serde_json::json!(3), "b");
        assert!(matches!(
            attach.drain(&session),
            Err(AttachError::DuplicateRpcId(RpcId::Num(3)))
        ));
        let pending = attach.pending_permissions();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].tool_call_id.as_deref(), Some("a"));
    }
}
