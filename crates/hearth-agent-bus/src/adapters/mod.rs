//! Pure JSON → [`crate::AgentEvent`] mappers. Live spawn stays Host-side.

pub mod codex;
pub mod grok_build;
pub mod opencode;
pub mod pi;

use crate::{AgentEvent, AgentKind, BusError};
use serde_json::Value;

/// Dispatch a native payload to the adapter for `kind`.
pub fn map_native(kind: AgentKind, raw: &Value) -> Result<AgentEvent, BusError> {
    match kind {
        AgentKind::GrokBuild => grok_build::map_notification(raw),
        AgentKind::Codex => codex::map_notification(raw),
        AgentKind::Pi => pi::map_event(raw),
        AgentKind::OpenCode => opencode::map_event(raw),
    }
}

/// One stdio JSON line or OpenCode SSE `data:` line → optional [`AgentEvent`].
/// Empty lines and SSE `[DONE]` are skipped. Transport-local — not a Queue noun.
pub fn map_wire_line(kind: AgentKind, line: &str) -> Result<Option<AgentEvent>, BusError> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(None);
    }
    let payload = if let Some(rest) = line.strip_prefix("data:") {
        let rest = rest.trim();
        if rest.is_empty() || rest == "[DONE]" {
            return Ok(None);
        }
        rest
    } else {
        line
    };
    let value: Value =
        serde_json::from_str(payload).map_err(|e| BusError::Decode(e.to_string()))?;
    map_native(kind, &value).map(Some)
}
