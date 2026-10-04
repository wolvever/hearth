//! Unified coding-agent wire for Hearth.
//!
//! Normalizes generic ACP, Grok Build (ACP profile), Codex App Server, Pi
//! harness, and OpenCode SSE into one `AgentEvent` / `AgentCommand` vocabulary
//! so Hearth can talk to any of them without a seventh kernel noun.
//!
//! [`HostAttach`] composes with Place / Session / Binding the way
//! [`hearth::PlaceMemory`] and [`hearth::FakeSandbox`] compose: claim-aware
//! where writes matter, Binding ids not reminted on attach or resume.
//!
//! # Architecture
//! - **Transport** owns framing (Content-Length JSON-RPC, SSE EventSource, WS JSON, JSONL RPC).
//! - **AdapterCodec** maps typed [`transport::WireFrame`]s ↔ [`AgentEvent`] / [`AgentCommand`].
//! - **Catalog** maps an ACP profile (Copilot, Cursor) to [`catalog::LaunchSpec`] data.
//!   Shared [`adapters::acp::AcpCodec`] on newline-delimited JSON-RPC (`jsonl-rpc`),
//!   not Content-Length. No process spawn. Pi `rpc_chunk` is not used for ACP.
//! - Prefer [`FramedAgent<T, C>`] for live attach. [`LoopbackAgent`] stays for Host tests.
//! - Never scrape unstructured stdout/stderr or regex logs for events.
//! - There is no public `map_wire_line` / `push_line`.
//! - SoftExpiring / Flush-before-dispatch stay parked — not in this crate.

pub mod adapters;
pub mod capabilities;
pub mod catalog;
pub mod host;
pub mod transport;

pub use adapters::{lookup, registry, AdapterCodec, AdapterInfo};
pub use capabilities::CapabilityFlags;
pub use catalog::{
    builtin_profiles, load_profiles, load_profiles_str, lookup_profile, CatalogExtends,
    CatalogProfile, LaunchSpec,
};
pub use host::{binding_kind, event_bodies, host_for, AttachError, AttachResult, HostAttach};
pub use transport::{
    JsonlProblem, JsonlRpcTransport, JsonRpcTransport, SseFrame, SseTransport, Transport,
    WebSocketJsonTransport, WireFrame, WireKind,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type SessionId = String;
pub type ProjectId = String;
pub type TurnId = String;
pub type ItemId = String;
pub type TaskId = String;
pub type PermissionId = String;
pub type QuestionId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentKind {
    GrokBuild,
    Codex,
    Pi,
    OpenCode,
    /// Generic ACP / catalog profile. Binding kind is `"acp"` via `Host::Other`.
    Acp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum AgentCommand {
    CreateProject { name: String, cwd: String },
    OpenSession { project_id: ProjectId, cwd: Option<String> },
    CloseSession { session_id: SessionId },
    UserMessage { session_id: SessionId, text: String },
    Steer { session_id: SessionId, text: String },
    Abort { session_id: SessionId },
    ReplyPermission {
        session_id: SessionId,
        permission_id: PermissionId,
        allow: bool,
        option_id: Option<String>,
    },
    ReplyQuestion {
        session_id: SessionId,
        question_id: QuestionId,
        answers: Vec<String>,
    },
    /// Condense context — pair with Hearth PreCompactHandoff first.
    Compact { session_id: SessionId },
    SpawnTask {
        session_id: SessionId,
        prompt: String,
        background: bool,
    },
    CancelTask { session_id: SessionId, task_id: TaskId },
    /// Switch the session mode (plan / ask / …). Encode is a stub until slice 7.
    SetMode { session_id: SessionId, mode_id: String },
    /// Opaque provider feature toggle. Encode is a stub until slice 7.
    SetFeature {
        session_id: SessionId,
        feature_id: String,
        value: serde_json::Value,
    },
    /// Opaque MCP server map. Encode is a stub until slice 7.
    ConfigureMcp {
        session_id: SessionId,
        servers: serde_json::Value,
    },
    RevertConversation { session_id: SessionId, message_id: String },
    RevertFiles { session_id: SessionId, message_id: String },
    RevertBoth { session_id: SessionId, message_id: String },
}

impl AgentCommand {
    /// Discriminant name for `BusError::Unsupported` encode stubs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CreateProject { .. } => "CreateProject",
            Self::OpenSession { .. } => "OpenSession",
            Self::CloseSession { .. } => "CloseSession",
            Self::UserMessage { .. } => "UserMessage",
            Self::Steer { .. } => "Steer",
            Self::Abort { .. } => "Abort",
            Self::ReplyPermission { .. } => "ReplyPermission",
            Self::ReplyQuestion { .. } => "ReplyQuestion",
            Self::Compact { .. } => "Compact",
            Self::SpawnTask { .. } => "SpawnTask",
            Self::CancelTask { .. } => "CancelTask",
            Self::SetMode { .. } => "SetMode",
            Self::SetFeature { .. } => "SetFeature",
            Self::ConfigureMcp { .. } => "ConfigureMcp",
            Self::RevertConversation { .. } => "RevertConversation",
            Self::RevertFiles { .. } => "RevertFiles",
            Self::RevertBoth { .. } => "RevertBoth",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum AgentEvent {
    ProjectReady { project_id: ProjectId, cwd: String },
    SessionStarted { session_id: SessionId, project_id: Option<ProjectId> },
    SessionEnded { session_id: SessionId },
    TurnStarted { session_id: SessionId, turn_id: TurnId },
    TurnCompleted { session_id: SessionId, turn_id: TurnId },
    Message {
        session_id: SessionId,
        item_id: ItemId,
        role: String,
        text: String,
    },
    MessageDelta {
        session_id: SessionId,
        item_id: ItemId,
        delta: String,
    },
    Thinking {
        session_id: SessionId,
        item_id: ItemId,
        text: String,
    },
    ThinkingDelta {
        session_id: SessionId,
        item_id: ItemId,
        delta: String,
    },
    ToolCall {
        session_id: SessionId,
        item_id: ItemId,
        name: String,
        arguments: serde_json::Value,
        status: ToolStatus,
    },
    ToolResult {
        session_id: SessionId,
        item_id: ItemId,
        output: serde_json::Value,
        status: ToolStatus,
    },
    Plan {
        session_id: SessionId,
        item_id: ItemId,
        entries: Vec<String>,
    },
    PermissionAsk {
        session_id: SessionId,
        permission_id: PermissionId,
        title: String,
        description: Option<String>,
        tool_item_id: Option<ItemId>,
        options: Vec<PermissionOption>,
    },
    QuestionAsk {
        session_id: SessionId,
        question_id: QuestionId,
        prompts: Vec<String>,
    },
    TaskStarted {
        session_id: SessionId,
        task_id: TaskId,
        parent_session_id: Option<SessionId>,
        prompt: String,
    },
    TaskProgress {
        session_id: SessionId,
        task_id: TaskId,
        detail: String,
    },
    TaskCompleted {
        session_id: SessionId,
        task_id: TaskId,
        ok: bool,
    },
    SubagentStarted {
        session_id: SessionId,
        child_session_id: SessionId,
        parent_session_id: SessionId,
    },
    SubagentEnded {
        session_id: SessionId,
        child_session_id: SessionId,
    },
    CompactStarted { session_id: SessionId },
    Compacted { session_id: SessionId },
    /// Session mode list / current mode. Decoders may emit this before encode lands.
    ModeChanged {
        session_id: SessionId,
        mode_id: Option<String>,
        available: Vec<ModeInfo>,
    },
    /// Conversation and/or files rewound to `message_id`.
    Rewound {
        session_id: SessionId,
        kind: RewindKind,
        message_id: String,
    },
    Status {
        session_id: SessionId,
        busy: bool,
        detail: Option<String>,
    },
    Error {
        session_id: Option<SessionId>,
        message: String,
    },
    /// Unmapped native payload — preserve for forward-compat.
    Native {
        agent: AgentKind,
        method: String,
        payload: serde_json::Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: String,
}

/// One selectable session mode on [`AgentEvent::ModeChanged`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModeInfo {
    pub mode_id: String,
    pub name: Option<String>,
}

/// Which surface a revert command or [`AgentEvent::Rewound`] applied to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RewindKind {
    Conversation,
    Files,
    Both,
}

#[derive(Debug, Error)]
pub enum BusError {
    #[error("unsupported command for this adapter: {0}")]
    Unsupported(&'static str),
    #[error("transport: {0}")]
    Transport(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("encode: {0}")]
    Encode(String),
}

/// What Hearth Host talks to. Live adapters own a child process / socket;
/// mappers are pure JSON translators used by tests and by live adapters.
pub trait CodingAgent: Send {
    fn kind(&self) -> AgentKind;
    fn send(&mut self, cmd: AgentCommand) -> Result<(), BusError>;
    fn try_recv(&mut self) -> Result<Option<AgentEvent>, BusError>;
}

/// Combines a [`Transport`] with an [`adapters::AdapterCodec`].
/// Host attach should prefer this over line-scraping helpers.
pub struct FramedAgent<T: transport::Transport, C: adapters::AdapterCodec> {
    transport: T,
    codec: C,
}

impl<T: transport::Transport, C: adapters::AdapterCodec> FramedAgent<T, C> {
    pub fn new(transport: T, codec: C) -> Self {
        Self { transport, codec }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
}

impl<T: transport::Transport, C: adapters::AdapterCodec> CodingAgent for FramedAgent<T, C> {
    fn kind(&self) -> AgentKind {
        self.codec.kind()
    }

    fn send(&mut self, cmd: AgentCommand) -> Result<(), BusError> {
        let frame = self.codec.encode_command(&cmd)?;
        self.transport.send_frame(frame)
    }

    fn try_recv(&mut self) -> Result<Option<AgentEvent>, BusError> {
        loop {
            match self.transport.try_recv_frame()? {
                None => return Ok(None),
                Some(frame) => {
                    if let Some(ev) = self.codec.decode_event(&frame)? {
                        return Ok(Some(ev));
                    }
                }
            }
        }
    }
}

/// In-memory loopback for Host-side tests: commands enqueue synthetic events.
pub struct LoopbackAgent {
    kind: AgentKind,
    out: Vec<AgentEvent>,
    session: Option<SessionId>,
}

impl LoopbackAgent {
    pub fn new(kind: AgentKind) -> Self {
        Self {
            kind,
            out: Vec::new(),
            session: None,
        }
    }
}

impl CodingAgent for LoopbackAgent {
    fn kind(&self) -> AgentKind {
        self.kind
    }

    fn send(&mut self, cmd: AgentCommand) -> Result<(), BusError> {
        match cmd {
            AgentCommand::OpenSession { .. } => {
                let id: SessionId = "sess-loop".into();
                self.session = Some(id.clone());
                self.out.push(AgentEvent::SessionStarted {
                    session_id: id,
                    project_id: None,
                });
            }
            AgentCommand::UserMessage { session_id, text } => {
                self.out.push(AgentEvent::TurnStarted {
                    session_id: session_id.clone(),
                    turn_id: "t1".into(),
                });
                self.out.push(AgentEvent::Message {
                    session_id: session_id.clone(),
                    item_id: "m1".into(),
                    role: "assistant".into(),
                    text: format!("echo:{text}"),
                });
                self.out.push(AgentEvent::TurnCompleted {
                    session_id,
                    turn_id: "t1".into(),
                });
            }
            AgentCommand::Compact { session_id } => {
                self.out
                    .push(AgentEvent::CompactStarted { session_id: session_id.clone() });
                self.out.push(AgentEvent::Compacted { session_id });
            }
            AgentCommand::ReplyPermission { session_id, allow, .. } => {
                self.out.push(AgentEvent::Status {
                    session_id,
                    busy: false,
                    detail: Some(if allow { "allowed".into() } else { "denied".into() }),
                });
            }
            AgentCommand::SpawnTask {
                session_id,
                prompt,
                ..
            } => {
                self.out.push(AgentEvent::TaskStarted {
                    session_id: session_id.clone(),
                    task_id: "task-1".into(),
                    parent_session_id: Some(session_id),
                    prompt,
                });
                self.out.push(AgentEvent::TaskCompleted {
                    session_id: self.session.clone().unwrap_or_else(|| "sess-loop".into()),
                    task_id: "task-1".into(),
                    ok: true,
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Result<Option<AgentEvent>, BusError> {
        if self.out.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.out.remove(0)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::{codex, grok_build, opencode, pi};

    #[test]
    fn loopback_message_roundtrip() {
        let mut a = LoopbackAgent::new(AgentKind::Codex);
        a.send(AgentCommand::OpenSession {
            project_id: "p".into(),
            cwd: None,
        })
        .unwrap();
        a.send(AgentCommand::UserMessage {
            session_id: "sess-loop".into(),
            text: "hi".into(),
        })
        .unwrap();
        let mut kinds = Vec::new();
        while let Some(ev) = a.try_recv().unwrap() {
            kinds.push(std::mem::discriminant(&ev));
        }
        assert!(kinds.len() >= 4);
    }

    #[test]
    fn grok_acp_tool_call_maps() {
        let raw = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "c1",
                    "title": "read",
                    "kind": "read",
                    "status": "pending"
                }
            }
        });
        let ev = grok_build::map_notification(&raw).unwrap();
        match ev {
            AgentEvent::ToolCall { item_id, name, status, .. } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "read");
                assert_eq!(status, ToolStatus::Pending);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn codex_item_reasoning_maps_to_thinking() {
        let raw = serde_json::json!({
            "method": "item/completed",
            "params": {
                "threadId": "th1",
                "turnId": "tu1",
                "item": {
                    "type": "reasoning",
                    "id": "r1",
                    "text": "consider options"
                }
            }
        });
        let ev = codex::map_notification(&raw).unwrap();
        match ev {
            AgentEvent::Thinking { text, .. } => assert!(text.contains("consider")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn pi_run_events_map() {
        let raw = serde_json::json!({
            "type": "run.start",
            "lane": "main",
            "runId": "run-1",
            "sessionId": "s1"
        });
        let ev = pi::map_event(&raw).unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));
    }

    #[test]
    fn pi_rpc_dialect_maps() {
        let ev = pi::map_event(&serde_json::json!({"type":"turn_start"})).unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));
        let ev = pi::map_event(&serde_json::json!({
            "type": "tool_execution_start",
            "toolCallId": "c1",
            "toolName": "bash"
        }))
        .unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { .. }));
    }

    #[test]
    fn opencode_permission_maps() {
        let raw = serde_json::json!({
            "type": "permission.asked",
            "properties": {
                "sessionID": "s1",
                "id": "perm-1",
                "title": "Allow shell?",
                "options": [{"option_id": "allow", "name": "Allow", "kind": "allow_once"}]
            }
        });
        let ev = opencode::map_event(&raw).unwrap();
        match ev {
            AgentEvent::PermissionAsk { title, .. } => assert!(title.contains("Allow")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn compact_command_serde_roundtrip() {
        let c = AgentCommand::Compact {
            session_id: "s".into(),
        };
        let v = serde_json::to_value(&c).unwrap();
        let back: AgentCommand = serde_json::from_value(v).unwrap();
        assert_eq!(c, back);
    }

    fn slice2_commands() -> Vec<AgentCommand> {
        vec![
            AgentCommand::SetMode {
                session_id: "s".into(),
                mode_id: "plan".into(),
            },
            AgentCommand::SetFeature {
                session_id: "s".into(),
                feature_id: "thinking".into(),
                value: serde_json::json!(true),
            },
            AgentCommand::ConfigureMcp {
                session_id: "s".into(),
                servers: serde_json::json!({"docs": {"command": "mcp-docs"}}),
            },
            AgentCommand::RevertConversation {
                session_id: "s".into(),
                message_id: "m1".into(),
            },
            AgentCommand::RevertFiles {
                session_id: "s".into(),
                message_id: "m1".into(),
            },
            AgentCommand::RevertBoth {
                session_id: "s".into(),
                message_id: "m1".into(),
            },
        ]
    }

    #[test]
    fn slice2_command_names_are_exhaustive() {
        let names: Vec<&str> = slice2_commands().iter().map(AgentCommand::name).collect();
        assert_eq!(
            names,
            [
                "SetMode",
                "SetFeature",
                "ConfigureMcp",
                "RevertConversation",
                "RevertFiles",
                "RevertBoth",
            ]
        );
    }

    #[test]
    fn slice2_commands_serde_roundtrip() {
        for c in slice2_commands() {
            let v = serde_json::to_value(&c).unwrap();
            let back: AgentCommand = serde_json::from_value(v).unwrap();
            assert_eq!(c, back);
        }
    }

    #[test]
    fn slice2_events_serde_roundtrip() {
        let events = [
            AgentEvent::ModeChanged {
                session_id: "s".into(),
                mode_id: Some("plan".into()),
                available: vec![ModeInfo {
                    mode_id: "plan".into(),
                    name: Some("Plan".into()),
                }],
            },
            AgentEvent::Rewound {
                session_id: "s".into(),
                kind: RewindKind::Conversation,
                message_id: "m1".into(),
            },
            AgentEvent::Rewound {
                session_id: "s".into(),
                kind: RewindKind::Files,
                message_id: "m1".into(),
            },
            AgentEvent::Rewound {
                session_id: "s".into(),
                kind: RewindKind::Both,
                message_id: "m1".into(),
            },
        ];
        for ev in events {
            let v = serde_json::to_value(&ev).unwrap();
            let back: AgentEvent = serde_json::from_value(v).unwrap();
            assert_eq!(ev, back);
        }
    }

    #[test]
    fn framed_agent_jsonrpc_roundtrip() {
        use crate::adapters::grok_build::GrokBuildCodec;
        use crate::transport::JsonRpcTransport;

        let mut agent = FramedAgent::new(JsonRpcTransport::new(), GrokBuildCodec);
        agent
            .transport_mut()
            .push_decoded(serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": "s1",
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "c1",
                        "title": "read",
                        "status": "pending"
                    }
                }
            }));
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { .. }));

        agent
            .send(AgentCommand::UserMessage {
                session_id: "s1".into(),
                text: "hi".into(),
            })
            .unwrap();
        assert_eq!(agent.transport().outbound().len(), 1);
    }

    #[test]
    fn framed_agent_sse_opencode() {
        use crate::adapters::opencode::OpenCodeCodec;
        use crate::transport::SseTransport;

        let mut agent = FramedAgent::new(SseTransport::new(), OpenCodeCodec);
        agent.transport_mut().push_decoded(
            Some("permission"),
            serde_json::json!({
                "type": "permission.asked",
                "properties": {
                    "sessionID": "s1",
                    "id": "perm-1",
                    "title": "Allow shell?",
                    "options": []
                }
            }),
        );
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
    }

    #[test]
    fn registry_discoverable() {
        assert_eq!(crate::registry().len(), 5);
        assert!(crate::lookup(AgentKind::Pi).unwrap().flags.subagent);
        assert_eq!(crate::lookup(AgentKind::Pi).unwrap().wire, "jsonl-rpc");
        assert_eq!(crate::lookup(AgentKind::Acp).unwrap().wire, "jsonl-rpc");
        assert_eq!(
            crate::lookup(AgentKind::GrokBuild).unwrap().wire,
            "jsonrpc-content-length"
        );
    }

    #[test]
    fn framed_agent_jsonl_pi_rpc() {
        use crate::adapters::pi::PiCodec;
        use crate::transport::JsonlRpcTransport;

        let mut agent = FramedAgent::new(JsonlRpcTransport::new(), PiCodec);
        agent.transport_mut().push_decoded(serde_json::json!({
            "type": "tool_execution_start",
            "toolCallId": "c1",
            "toolName": "bash"
        }));
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { .. }));
    }
}
