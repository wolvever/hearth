//! Codex App Server JSON-RPC (openai/codex).
//! thread/turn/item lifecycle, approvals, compaction.

use crate::{AgentEvent, AgentKind, BusError, ToolStatus};
use serde_json::Value;

pub fn map_notification(msg: &Value) -> Result<AgentEvent, BusError> {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let session_id = params
        .get("threadId")
        .or_else(|| params.get("sessionId"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    match method {
        "thread/started" => Ok(AgentEvent::SessionStarted {
            session_id,
            project_id: None,
        }),
        "turn/started" => Ok(AgentEvent::TurnStarted {
            session_id,
            turn_id: params
                .get("turnId")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .into(),
        }),
        "turn/completed" => Ok(AgentEvent::TurnCompleted {
            session_id,
            turn_id: params
                .get("turnId")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .into(),
        }),
        "thread/compact/start" => Ok(AgentEvent::CompactStarted { session_id }),
        "thread/compacted" => Ok(AgentEvent::Compacted { session_id }),
        "item/started" | "item/completed" => map_item(method, &session_id, &params),
        "item/agentMessage/delta" => Ok(AgentEvent::MessageDelta {
            session_id,
            item_id: params
                .get("itemId")
                .and_then(|i| i.as_str())
                .unwrap_or("m")
                .into(),
            delta: params
                .get("delta")
                .and_then(|d| d.as_str())
                .unwrap_or("")
                .into(),
        }),
        "item/reasoning/summaryTextDelta" | "item/reasoning/textDelta" => {
            Ok(AgentEvent::ThinkingDelta {
                session_id,
                item_id: params
                    .get("itemId")
                    .and_then(|i| i.as_str())
                    .unwrap_or("r")
                    .into(),
                delta: params
                    .get("delta")
                    .and_then(|d| d.as_str())
                    .unwrap_or("")
                    .into(),
            })
        }
        other if other.contains("compact") => Ok(AgentEvent::Compacted { session_id }),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::Codex,
            method: other.into(),
            payload: msg.clone(),
        }),
    }
}

fn map_item(method: &str, session_id: &str, params: &Value) -> Result<AgentEvent, BusError> {
    let item = params.get("item").cloned().unwrap_or(Value::Null);
    let ty = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let id = item
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or("")
        .to_string();

    match ty {
        "agentMessage" => Ok(AgentEvent::Message {
            session_id: session_id.into(),
            item_id: id,
            role: "assistant".into(),
            text: item
                .get("text")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .into(),
        }),
        "reasoning" => Ok(AgentEvent::Thinking {
            session_id: session_id.into(),
            item_id: id,
            text: item
                .get("text")
                .or_else(|| item.get("summary"))
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .into(),
        }),
        "commandExecution" | "mcpToolCall" | "dynamicToolCall" | "fileChange" => {
            let status = if method.ends_with("completed") {
                match item.get("status").and_then(|s| s.as_str()).unwrap_or("completed") {
                    "failed" => ToolStatus::Failed,
                    _ => ToolStatus::Completed,
                }
            } else {
                ToolStatus::InProgress
            };
            if method.ends_with("completed") {
                Ok(AgentEvent::ToolResult {
                    session_id: session_id.into(),
                    item_id: id,
                    output: item.get("result").cloned().unwrap_or(item.clone()),
                    status,
                })
            } else {
                Ok(AgentEvent::ToolCall {
                    session_id: session_id.into(),
                    item_id: id,
                    name: ty.into(),
                    arguments: item.get("arguments").cloned().unwrap_or(Value::Null),
                    status,
                })
            }
        }
        "contextCompaction" | "compaction" => Ok(AgentEvent::CompactStarted {
            session_id: session_id.into(),
        }),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::Codex,
            method: format!("item:{other}"),
            payload: params.clone(),
        }),
    }
}
