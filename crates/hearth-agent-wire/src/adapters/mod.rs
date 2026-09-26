//! Per-agent codecs: typed protocol frames ↔ [`AgentEvent`] / [`AgentCommand`].
//!
//! # Contrib layout
//! One folder per agent under `adapters/<name>/`:
//! - `mod.rs` — `map_*` + [`AdapterCodec`] impl
//! - `README.md` — wire, capabilities, how to run fixtures
//! - `fixtures/*.json` — captured native frames for `map_*` tests
//!
//! Generic ACP catalog agents start from [`acp`]. Never scrape unstructured
//! stdout/stderr. Transports deliver [`WireFrame`]s.

pub mod acp;
pub mod codex;
pub mod grok_build;
pub mod opencode;
pub mod pi;

use crate::transport::WireFrame;
use crate::{AgentCommand, AgentEvent, AgentKind, BusError};

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
    pub capabilities: &'static [&'static str],
}

/// Registry of built-in adapters — add a row when you land a new agent folder.
pub fn registry() -> &'static [AdapterInfo] {
    &[
        AdapterInfo {
            kind: AgentKind::GrokBuild,
            name: "grok_build",
            wire: "jsonrpc-content-length",
            folder: "adapters/grok_build",
            capabilities: &[
                "session",
                "message",
                "tool",
                "plan",
                "permission",
                "thinking",
            ],
        },
        AdapterInfo {
            kind: AgentKind::Codex,
            name: "codex",
            wire: "jsonrpc-content-length",
            folder: "adapters/codex",
            capabilities: &[
                "session",
                "message",
                "tool",
                "thinking",
                "compact",
                "permission",
            ],
        },
        AdapterInfo {
            kind: AgentKind::Pi,
            name: "pi",
            wire: "websocket-json",
            folder: "adapters/pi",
            capabilities: &["session", "message", "tool", "subagent"],
        },
        AdapterInfo {
            kind: AgentKind::OpenCode,
            name: "opencode",
            wire: "http-sse",
            folder: "adapters/opencode",
            capabilities: &[
                "session",
                "message",
                "tool",
                "permission",
                "question",
                "thinking",
                "compact",
            ],
        },
        AdapterInfo {
            kind: AgentKind::Acp,
            name: "acp",
            wire: "jsonrpc-content-length",
            folder: "adapters/acp",
            capabilities: &[
                "session",
                "message",
                "tool",
                "plan",
                "permission",
                "thinking",
            ],
        },
    ]
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
    }
}
