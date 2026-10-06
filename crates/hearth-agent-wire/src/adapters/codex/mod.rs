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
                match item
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("completed")
                {
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

use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::AgentCommand;

#[derive(Debug, Default, Clone, Copy)]
pub struct CodexCodec;

impl AdapterCodec for CodexCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::Codex
    }

    fn wire(&self) -> &'static str {
        "jsonrpc-content-length"
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        match frame {
            WireFrame::Json(v) => map_notification(v).map(Some),
            WireFrame::Sse { .. } => Err(BusError::Decode(
                "codex expects JSON-RPC frames, not SSE".into(),
            )),
        }
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        let (method, params) = match cmd {
            AgentCommand::UserMessage { session_id, text } => (
                "turn/start",
                serde_json::json!({ "threadId": session_id, "input": text }),
            ),
            AgentCommand::Compact { session_id } => (
                "thread/compact",
                serde_json::json!({ "threadId": session_id }),
            ),
            AgentCommand::Abort { session_id } => {
                ("turn/abort", serde_json::json!({ "threadId": session_id }))
            }
            _ => return Err(BusError::Unsupported(cmd.name())),
        };
        Ok(WireFrame::Json(serde_json::json!({
            "method": method,
            "params": params,
        })))
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/codex/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_reasoning_fixture() {
        let ev = map_notification(&fixture("item_reasoning.json")).unwrap();
        match ev {
            AgentEvent::Thinking { text, .. } => assert!(text.contains("consider")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_turn_started_fixture() {
        let ev = map_notification(&fixture("turn_started.json")).unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));
    }
}
