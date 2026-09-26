//! Grok Build — Agent Client Protocol (ACP) JSON-RPC.
//! Source: xai-org/grok-build (`grok agent stdio` / `serve`).

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


use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::AgentCommand;

/// ACP codec for Grok Build. Consumes JSON-RPC [`WireFrame::Json`] only.
#[derive(Debug, Default, Clone, Copy)]
pub struct GrokBuildCodec;

impl AdapterCodec for GrokBuildCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::GrokBuild
    }

    fn wire(&self) -> &'static str {
        "jsonrpc-content-length"
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        match frame {
            WireFrame::Json(v) => map_notification(v).map(Some),
            WireFrame::Sse { .. } => Err(BusError::Decode(
                "grok_build expects JSON-RPC frames, not SSE".into(),
            )),
        }
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        let (method, params) = match cmd {
            AgentCommand::UserMessage { session_id, text } => (
                "session/prompt",
                serde_json::json!({
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": text}],
                }),
            ),
            AgentCommand::ReplyPermission {
                session_id,
                permission_id,
                allow,
                option_id,
            } => (
                "session/request_permission/result",
                serde_json::json!({
                    "sessionId": session_id,
                    "id": permission_id,
                    "outcome": if *allow { "selected" } else { "cancelled" },
                    "optionId": option_id,
                }),
            ),
            AgentCommand::Compact { session_id } => (
                "session/compact",
                serde_json::json!({ "sessionId": session_id }),
            ),
            other => {
                return Err(BusError::Unsupported(match other {
                    AgentCommand::CreateProject { .. } => "CreateProject",
                    AgentCommand::OpenSession { .. } => "OpenSession",
                    AgentCommand::CloseSession { .. } => "CloseSession",
                    AgentCommand::Steer { .. } => "Steer",
                    AgentCommand::Abort { .. } => "Abort",
                    AgentCommand::ReplyQuestion { .. } => "ReplyQuestion",
                    AgentCommand::SpawnTask { .. } => "SpawnTask",
                    AgentCommand::CancelTask { .. } => "CancelTask",
                    _ => "command",
                }))
            }
        };
        Ok(WireFrame::Json(serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        })))
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;
    use crate::adapters::AdapterCodec;
    use crate::transport::WireFrame;
    use crate::ToolStatus;

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/grok_build/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_tool_call_fixture() {
        let ev = map_notification(&fixture("session_update_tool_call.json")).unwrap();
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
    fn map_permission_fixture() {
        let ev = map_notification(&fixture("permission_request.json")).unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
    }

    #[test]
    fn codec_rejects_sse_frame() {
        let frame = WireFrame::Sse {
            event: None,
            id: None,
            data: serde_json::json!({}),
        };
        assert!(GrokBuildCodec.decode_event(&frame).is_err());
    }
}
