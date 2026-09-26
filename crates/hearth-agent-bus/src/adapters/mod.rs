//! Adapter: [`Frame`] / [`Value`] → [`crate::AgentEvent`].
//!
//! Transport (bytes → Frame) lives in [`crate::transport`]. Do not parse
//! stdio lines here. Each adapter is a folder: `README.md`, `fixtures/*.json`,
//! and `map_*`. See crate-root `CAPABILITIES.md` / `CONTRIBUTING.md`.

pub mod codex;
pub mod grok_build;
pub mod opencode;
pub mod pi;

use crate::transport::Frame;
use crate::{AgentEvent, AgentKind, BusError};
use serde_json::Value;

/// Dispatch a native JSON payload to the adapter for `kind`.
pub fn map_native(kind: AgentKind, raw: &Value) -> Result<AgentEvent, BusError> {
    match kind {
        AgentKind::GrokBuild => grok_build::map_notification(raw),
        AgentKind::Codex => codex::map_notification(raw),
        AgentKind::Pi => pi::map_event(raw),
        AgentKind::OpenCode => opencode::map_event(raw),
    }
}

/// Adapter entry from a decoded transport [`Frame`].
pub fn map_frame(kind: AgentKind, frame: &Frame) -> Result<AgentEvent, BusError> {
    map_native(kind, &frame.value)
}
