//! Per-agent codecs: typed protocol frames ↔ [`AgentEvent`] / [`AgentCommand`].
//!
//! # Contrib layout
//! One folder per agent under `adapters/<name>/`:
//! - `mod.rs` — `map_*` + [`AdapterCodec`] impl
//! - `README.md` — wire, `CapabilityFlags`, how to run fixtures
//! - `fixtures/*.json` — captured native frames for `map_*` tests
//!
//! Generic ACP catalog agents start from [`acp`]. Never scrape unstructured
//! stdout/stderr. Transports deliver [`WireFrame`]s. Pi uses `jsonl-rpc`
//! (typed RPC frames after [`crate::transport::JsonlRpcTransport`]).

pub mod acp;
pub mod codex;
pub mod grok_build;
pub mod opencode;
pub mod pi;

use crate::transport::WireFrame;
use crate::{AgentCommand, AgentEvent, AgentKind, BusError, CapabilityFlags};

/// Maps framed protocol messages ↔ unified wire types. Pure; no I/O.
pub trait AdapterCodec: Send + Sync {
    fn kind(&self) -> AgentKind;
    /// Preferred wire for this agent (discovery / CAPABILITIES matrix).
    fn wire(&self) -> &'static str;
    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError>;
    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError>;
}

/// Static discovery row for contributors and Host attach selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterInfo {
    pub kind: AgentKind,
    pub name: &'static str,
    pub wire: &'static str,
    pub folder: &'static str,
    /// Typed flags — must match the `CAPABILITIES.md` matrix for this row.
    pub flags: CapabilityFlags,
}

impl AdapterInfo {
    /// Parsed [`crate::transport::WireKind`] for this row. All registry wires
    /// must be an allowed framed kind (`jsonl-rpc` included).
    pub fn wire_kind(self) -> Option<crate::transport::WireKind> {
        crate::transport::WireKind::parse(self.wire)
    }
}

/// Registry of built-in adapters — add a row when you land a new agent folder.
const REGISTRY: &[AdapterInfo] = &[
    AdapterInfo {
        kind: AgentKind::GrokBuild,
        name: "grok_build",
        wire: crate::transport::WireKind::JsonRpc.as_str(),
        folder: "adapters/grok_build",
        flags: CapabilityFlags::for_agent(AgentKind::GrokBuild),
    },
    AdapterInfo {
        kind: AgentKind::Codex,
        name: "codex",
        wire: crate::transport::WireKind::JsonRpc.as_str(),
        folder: "adapters/codex",
        flags: CapabilityFlags::for_agent(AgentKind::Codex),
    },
    AdapterInfo {
        kind: AgentKind::Pi,
        name: "pi",
        wire: crate::transport::WireKind::JsonlRpc.as_str(),
        folder: "adapters/pi",
        flags: CapabilityFlags::for_agent(AgentKind::Pi),
    },
    AdapterInfo {
        kind: AgentKind::OpenCode,
        name: "opencode",
        wire: crate::transport::WireKind::Sse.as_str(),
        folder: "adapters/opencode",
        flags: CapabilityFlags::for_agent(AgentKind::OpenCode),
    },
    AdapterInfo {
        kind: AgentKind::Acp,
        name: "acp",
        wire: crate::transport::WireKind::JsonRpc.as_str(),
        folder: "adapters/acp",
        flags: CapabilityFlags::for_agent(AgentKind::Acp),
    },
];

pub fn registry() -> &'static [AdapterInfo] {
    REGISTRY
}

pub fn lookup(kind: AgentKind) -> Option<&'static AdapterInfo> {
    registry().iter().find(|i| i.kind == kind)
}

/// Dispatch a decoded JSON frame to the right mapper (tests / Host helpers).
pub fn map_native(kind: AgentKind, raw: &serde_json::Value) -> Result<AgentEvent, BusError> {
    match kind {
        AgentKind::GrokBuild => grok_build::map_notification(raw),
        AgentKind::Codex => codex::map_notification(raw),
        AgentKind::Pi => pi::map_event(raw),
        AgentKind::OpenCode => opencode::map_event(raw),
        AgentKind::Acp => acp::map_notification(raw, AgentKind::Acp),
    }
}

/// Decode via the registered codec for `kind`.
pub fn decode_frame(kind: AgentKind, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
    match kind {
        AgentKind::GrokBuild => grok_build::GrokBuildCodec.decode_event(frame),
        AgentKind::Codex => codex::CodexCodec.decode_event(frame),
        AgentKind::Pi => pi::PiCodec.decode_event(frame),
        AgentKind::OpenCode => opencode::OpenCodeCodec.decode_event(frame),
        AgentKind::Acp => acp::AcpCodec.decode_event(frame),
    }
}

/// Encode via the registered codec for `kind`.
pub fn encode_command(kind: AgentKind, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
    match kind {
        AgentKind::GrokBuild => grok_build::GrokBuildCodec.encode_command(cmd),
        AgentKind::Codex => codex::CodexCodec.encode_command(cmd),
        AgentKind::Pi => pi::PiCodec.encode_command(cmd),
        AgentKind::OpenCode => opencode::OpenCodeCodec.encode_command(cmd),
        AgentKind::Acp => acp::AcpCodec.encode_command(cmd),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_lists_five_agents() {
        let r = registry();
        assert_eq!(r.len(), 5);
        assert!(lookup(AgentKind::GrokBuild)
            .unwrap()
            .wire
            .contains("jsonrpc"));
        assert_eq!(lookup(AgentKind::OpenCode).unwrap().wire, "http-sse");
        assert_eq!(
            lookup(AgentKind::Acp).unwrap().wire,
            "jsonrpc-content-length"
        );
        assert_eq!(lookup(AgentKind::Pi).unwrap().wire, "jsonl-rpc");
        assert_eq!(
            lookup(AgentKind::Pi).unwrap().wire_kind(),
            Some(crate::transport::WireKind::JsonlRpc)
        );
        for info in r {
            assert!(
                info.wire_kind().is_some(),
                "{} wire {:?} is not an allowed WireKind",
                info.name,
                info.wire
            );
        }
    }

    fn slice2_stub_commands() -> [AgentCommand; 6] {
        [
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
                servers: serde_json::json!({}),
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
    fn slice2_commands_encode_unsupported_on_all_adapters() {
        for info in registry() {
            assert!(
                !info.flags.dynamic_modes
                    && !info.flags.mcp_servers
                    && !info.flags.rewind_conversation
                    && !info.flags.rewind_files
                    && !info.flags.rewind_both,
                "{} reserved slice-2 flags must stay false while encode is a stub",
                info.name
            );
            for cmd in &slice2_stub_commands() {
                match encode_command(info.kind, cmd) {
                    Err(BusError::Unsupported(name)) => assert_eq!(name, cmd.name()),
                    other => panic!("{} {} => {other:?}", info.name, cmd.name()),
                }
            }
        }
    }
}
