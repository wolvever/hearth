//! Grok Build — Agent Client Protocol (ACP) JSON-RPC.
//! Source: xai-org/grok-build (`grok agent stdio` / `serve`).
//!
//! Transport: [`crate::transport::JsonRpcFramer`]. Mapper: [`map_notification`].

use crate::{AgentEvent, AgentKind, BusError, ToolStatus};
use serde_json::Value;

pub fn map_notification(msg: &Value) -> Result<AgentEvent, BusError> {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = msg.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "session/update" => map_session_update(&params),
        "session/request_permission" => map_permission_request(&params),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::GrokBuild,
            method: other.into(),
            payload: msg.clone(),
        }),
    }
}

fn map_session_update(params: &Value) -> Result<AgentEvent, BusError> {
    let session_id = params
        .get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let update = params.get("update").cloned().unwrap_or(Value::Null);
    let kind = update
        .get("sessionUpdate")
        .and_then(|s| s.as_str())
        .unwrap_or("");

    match kind {
        "agent_message_chunk" | "agent_message" => {
            let text = update
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(|t| t.as_str())
                .or_else(|| update.get("text").and_then(|t| t.as_str()))
                .unwrap_or("")
                .to_string();
            Ok(AgentEvent::MessageDelta {
                session_id,
                item_id: update
                    .get("messageId")
                    .and_then(|i| i.as_str())
                    .unwrap_or("msg")
                    .to_string(),
                delta: text,
            })
        }
        "tool_call" | "tool_call_update" => {
            let status = match update.get("status").and_then(|s| s.as_str()).unwrap_or("pending") {
                "pending" => ToolStatus::Pending,
                "in_progress" | "running" => ToolStatus::InProgress,
                "completed" => ToolStatus::Completed,
                "failed" => ToolStatus::Failed,
                "cancelled" => ToolStatus::Cancelled,
                _ => ToolStatus::Pending,
            };
            Ok(AgentEvent::ToolCall {
                session_id,
                item_id: update
                    .get("toolCallId")
                    .and_then(|i| i.as_str())
                    .unwrap_or("")
                    .to_string(),
                name: update
                    .get("title")
                    .or_else(|| update.get("kind"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("tool")
                    .to_string(),
                arguments: update.get("rawInput").cloned().unwrap_or(Value::Null),
                status,
            })
        }
        "plan" => Ok(AgentEvent::Plan {
            session_id,
            item_id: "plan".into(),
            entries: update
                .get("entries")
                .and_then(|e| e.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.get("content").and_then(|c| c.as_str()).map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
        }),
        "agent_thought_chunk" => Ok(AgentEvent::ThinkingDelta {
            session_id,
            item_id: "think".into(),
            delta: update
                .get("content")
                .and_then(|c| c.get("text"))
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string(),
        }),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::GrokBuild,
            method: format!("session/update:{other}"),
            payload: params.clone(),
        }),
    }
}

fn map_permission_request(params: &Value) -> Result<AgentEvent, BusError> {
    let session_id = params
        .get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let options = params
        .get("options")
        .and_then(|o| o.as_array())
        .map(|a| {
            a.iter()
                .map(|o| crate::PermissionOption {
                    option_id: o
                        .get("optionId")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .into(),
                    name: o.get("name").and_then(|x| x.as_str()).unwrap_or("").into(),
                    kind: o.get("kind").and_then(|x| x.as_str()).unwrap_or("").into(),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(AgentEvent::PermissionAsk {
        session_id,
        permission_id: params
            .get("id")
            .and_then(|i| i.as_str())
            .unwrap_or("perm")
            .into(),
        title: params
            .get("title")
            .and_then(|t| t.as_str())
            .unwrap_or("Permission")
            .into(),
        description: params
            .get("description")
            .and_then(|d| d.as_str())
            .map(str::to_string),
        tool_item_id: params
            .pointer("/subject/toolCall/toolCallId")
            .and_then(|t| t.as_str())
            .map(str::to_string),
        options,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_tool_call() {
        let raw: Value =
            serde_json::from_str(include_str!("fixtures/session_update_tool_call.json")).unwrap();
        match map_notification(&raw).unwrap() {
            AgentEvent::ToolCall { item_id, name, status, .. } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "read");
                assert_eq!(status, ToolStatus::Pending);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
