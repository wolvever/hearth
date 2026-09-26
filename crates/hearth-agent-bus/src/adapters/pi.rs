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
