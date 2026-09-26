//! Pi durable harness events (earendil-works/pi).
//! Snapshot + watch; events not replayed on reconnect.

use crate::{AgentEvent, AgentKind, BusError};
use serde_json::Value;

pub fn map_event(msg: &Value) -> Result<AgentEvent, BusError> {
    let ty = msg
        .get("type")
        .or_else(|| msg.get("event"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    let session_id = msg
        .get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    match ty {
        "run.start" | "run.started" => Ok(AgentEvent::TurnStarted {
            session_id,
            turn_id: msg
                .get("runId")
                .and_then(|r| r.as_str())
                .unwrap_or("run")
                .into(),
        }),
        "run.end" | "run.ended" => Ok(AgentEvent::TurnCompleted {
            session_id,
            turn_id: msg
                .get("runId")
                .and_then(|r| r.as_str())
                .unwrap_or("run")
                .into(),
        }),
        "message" | "entry.message" => Ok(AgentEvent::Message {
            session_id,
            item_id: msg
                .get("entryId")
                .and_then(|i| i.as_str())
                .unwrap_or("m")
                .into(),
            role: msg
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("assistant")
                .into(),
            text: msg
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .into(),
        }),
        "tool.start" | "tool.started" => Ok(AgentEvent::ToolCall {
            session_id,
            item_id: msg
                .get("callId")
                .and_then(|i| i.as_str())
                .unwrap_or("c")
                .into(),
            name: msg
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("tool")
                .into(),
            arguments: msg.get("arguments").cloned().unwrap_or(Value::Null),
            status: crate::ToolStatus::InProgress,
        }),
        "tool.end" | "tool.ended" => Ok(AgentEvent::ToolResult {
            session_id,
            item_id: msg
                .get("callId")
                .and_then(|i| i.as_str())
                .unwrap_or("c")
                .into(),
            output: msg.get("result").cloned().unwrap_or(Value::Null),
            status: crate::ToolStatus::Completed,
        }),
        "lane.start" => Ok(AgentEvent::SubagentStarted {
            session_id: session_id.clone(),
            child_session_id: msg
                .get("lane")
                .and_then(|l| l.as_str())
                .unwrap_or("lane")
                .into(),
            parent_session_id: session_id,
        }),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::Pi,
            method: other.into(),
            payload: msg.clone(),
        }),
    }
}


use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::AgentCommand;

#[derive(Debug, Default, Clone, Copy)]
pub struct PiCodec;

impl AdapterCodec for PiCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::Pi
    }

    fn wire(&self) -> &'static str {
        "websocket-json"
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        let v = frame
            .as_json()
            .ok_or_else(|| BusError::Decode("pi expects JSON frame".into()))?;
        map_event(v).map(Some)
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        let body = match cmd {
            AgentCommand::UserMessage { session_id, text } => serde_json::json!({
                "type": "prompt",
                "sessionId": session_id,
                "text": text,
            }),
            AgentCommand::Abort { session_id } => serde_json::json!({
                "type": "abort",
                "sessionId": session_id,
            }),
            _ => return Err(BusError::Unsupported("pi command stub")),
        };
        Ok(WireFrame::Json(body))
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/pi/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_run_start_fixture() {
        let ev = map_event(&fixture("run_start.json")).unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));
    }

    #[test]
    fn map_tool_start_fixture() {
        let ev = map_event(&fixture("tool_start.json")).unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { .. }));
    }
}
