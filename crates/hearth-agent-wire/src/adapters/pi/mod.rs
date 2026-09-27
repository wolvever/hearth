//! Pi adapter: Paseo `rpc-types` dialect + earendil-works/pi harness events.
//!
//! Typed frames only — snapshot / watch RPC objects after
//! [`crate::transport::JsonlRpcTransport`] (or WS JSON) has framed them.
//! Do not scrape stdout lines.

use crate::adapters::AdapterCodec;
use crate::transport::{WireFrame, WireKind};
use crate::{AgentCommand, AgentEvent, AgentKind, BusError, ToolStatus};
use serde_json::Value;

pub fn map_event(msg: &Value) -> Result<AgentEvent, BusError> {
    let ty = event_type(msg);
    let session_id = session_id(msg);

    match ty {
        // --- Paseo / Pi RPC dialect (rpc-types) ---
        "agent_start" | "turn_start" => Ok(AgentEvent::TurnStarted {
            session_id,
            turn_id: turn_id(msg),
        }),
        "turn_end" | "agent_end" => Ok(AgentEvent::TurnCompleted {
            session_id,
            turn_id: turn_id(msg),
        }),
        "agent_settled" => Ok(AgentEvent::Status {
            session_id,
            busy: false,
            detail: Some("settled".into()),
        }),
        "message_update" => map_message_update(msg, session_id),
        "message_end" => map_message_end(msg, session_id),
        "tool_execution_start" => Ok(AgentEvent::ToolCall {
            session_id,
            item_id: tool_id(msg),
            name: tool_name(msg),
            arguments: tool_args(msg),
            status: ToolStatus::InProgress,
        }),
        "tool_execution_update" => Ok(AgentEvent::ToolCall {
            session_id,
            item_id: tool_id(msg),
            name: tool_name(msg),
            arguments: msg
                .get("partialResult")
                .cloned()
                .or_else(|| msg.get("args").cloned())
                .unwrap_or(Value::Null),
            status: ToolStatus::InProgress,
        }),
        "tool_execution_end" => Ok(AgentEvent::ToolResult {
            session_id,
            item_id: tool_id(msg),
            output: msg.get("result").cloned().unwrap_or(Value::Null),
            status: if msg
                .get("isError")
                .and_then(|e| e.as_bool())
                .unwrap_or(false)
            {
                ToolStatus::Failed
            } else {
                ToolStatus::Completed
            },
        }),
        "compaction_start" => Ok(AgentEvent::CompactStarted { session_id }),
        "compaction_end" => Ok(AgentEvent::Compacted { session_id }),

        // --- Existing durable-harness types (coexist on the same codec) ---
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
            status: ToolStatus::InProgress,
        }),
        "tool.end" | "tool.ended" => Ok(AgentEvent::ToolResult {
            session_id,
            item_id: msg
                .get("callId")
                .and_then(|i| i.as_str())
                .unwrap_or("c")
                .into(),
            output: msg.get("result").cloned().unwrap_or(Value::Null),
            status: ToolStatus::Completed,
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

fn event_type(msg: &Value) -> &str {
    msg.get("type")
        .or_else(|| msg.get("event"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
}

fn session_id(msg: &Value) -> String {
    msg.get("sessionId")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string()
}

fn turn_id(msg: &Value) -> String {
    msg.get("turnId")
        .or_else(|| msg.get("runId"))
        .and_then(|r| r.as_str())
        .unwrap_or("turn")
        .into()
}

fn tool_id(msg: &Value) -> String {
    msg.get("toolCallId")
        .or_else(|| msg.get("callId"))
        .and_then(|i| i.as_str())
        .unwrap_or("c")
        .into()
}

fn tool_name(msg: &Value) -> String {
    msg.get("toolName")
        .or_else(|| msg.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("tool")
        .into()
}

fn tool_args(msg: &Value) -> Value {
    msg.get("args")
        .or_else(|| msg.get("arguments"))
        .cloned()
        .unwrap_or(Value::Null)
}

fn map_message_update(msg: &Value, session_id: String) -> Result<AgentEvent, BusError> {
    let ev = msg.get("assistantMessageEvent");
    let ev_ty = ev
        .and_then(|e| e.get("type"))
        .and_then(|t| t.as_str())
        .unwrap_or("");
    let delta = ev
        .and_then(|e| e.get("delta"))
        .and_then(|d| d.as_str())
        .unwrap_or("")
        .to_string();
    let item_id = msg
        .get("message")
        .and_then(|m| m.get("id"))
        .and_then(|i| i.as_str())
        .unwrap_or("m")
        .to_string();
    match ev_ty {
        "thinking_delta" => Ok(AgentEvent::ThinkingDelta {
            session_id,
            item_id,
            delta,
        }),
        _ => Ok(AgentEvent::MessageDelta {
            session_id,
            item_id,
            delta,
        }),
    }
}

fn map_message_end(msg: &Value, session_id: String) -> Result<AgentEvent, BusError> {
    let message = msg.get("message").cloned().unwrap_or(Value::Null);
    let role = message
        .get("role")
        .and_then(|r| r.as_str())
        .unwrap_or("assistant")
        .to_string();
    let item_id = message
        .get("id")
        .and_then(|i| i.as_str())
        .unwrap_or("m")
        .to_string();
    let text = message_text(&message);
    Ok(AgentEvent::Message {
        session_id,
        item_id,
        role,
        text,
    })
}

fn message_text(message: &Value) -> String {
    if let Some(s) = message.get("content").and_then(|c| c.as_str()) {
        return s.to_string();
    }
    let Some(arr) = message.get("content").and_then(|c| c.as_array()) else {
        return message
            .get("text")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
    };
    let mut out = String::new();
    for part in arr {
        let ty = part.get("type").and_then(|t| t.as_str()).unwrap_or("");
        let piece = match ty {
            "text" => part.get("text").and_then(|t| t.as_str()),
            "thinking" => part.get("thinking").and_then(|t| t.as_str()),
            _ => None,
        };
        if let Some(p) = piece {
            out.push_str(p);
        }
    }
    out
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PiCodec;

impl AdapterCodec for PiCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::Pi
    }

    fn wire(&self) -> &'static str {
        WireKind::JsonlRpc.as_str()
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
                "message": text,
            }),
            AgentCommand::Steer { session_id, text } => serde_json::json!({
                "type": "steer",
                "sessionId": session_id,
                "message": text,
            }),
            AgentCommand::Abort { session_id } => serde_json::json!({
                "type": "abort",
                "sessionId": session_id,
            }),
            _ => return Err(BusError::Unsupported(cmd.name())),
        };
        Ok(WireFrame::Json(body))
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;
    use crate::transport::JsonlRpcTransport;
    use crate::{CodingAgent, FramedAgent};

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

    #[test]
    fn map_rpc_turn_start_fixture() {
        let ev = map_event(&fixture("turn_start.json")).unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));
    }

    #[test]
    fn map_rpc_tool_execution_start_fixture() {
        let ev = map_event(&fixture("tool_execution_start.json")).unwrap();
        match ev {
            AgentEvent::ToolCall { item_id, name, .. } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "bash");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_rpc_message_update_thinking() {
        let ev = map_event(&fixture("message_update.json")).unwrap();
        match ev {
            AgentEvent::MessageDelta { delta, .. } => assert_eq!(delta, "hello"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_rpc_thinking_delta() {
        let ev = map_event(&serde_json::json!({
            "type": "message_update",
            "assistantMessageEvent": {"type": "thinking_delta", "delta": "hmm"}
        }))
        .unwrap();
        match ev {
            AgentEvent::ThinkingDelta { delta, .. } => assert_eq!(delta, "hmm"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_rpc_compaction_start() {
        let ev = map_event(&serde_json::json!({
            "type": "compaction_start",
            "sessionId": "s1",
            "reason": "threshold"
        }))
        .unwrap();
        assert!(matches!(ev, AgentEvent::CompactStarted { .. }));
    }

    #[test]
    fn map_rpc_agent_end() {
        let ev = map_event(&serde_json::json!({"type":"agent_end"})).unwrap();
        assert!(matches!(ev, AgentEvent::TurnCompleted { .. }));
    }

    #[test]
    fn codec_wire_is_jsonl_rpc() {
        assert_eq!(PiCodec.wire(), WireKind::JSONL_RPC);
    }

    #[test]
    fn encode_prompt_is_paseo_rpc() {
        let frame = PiCodec
            .encode_command(&AgentCommand::UserMessage {
                session_id: "s1".into(),
                text: "hi".into(),
            })
            .unwrap();
        let v = frame.as_json().unwrap();
        assert_eq!(v["type"], "prompt");
        assert_eq!(v["message"], "hi");
        assert_eq!(v["sessionId"], "s1");
    }

    #[test]
    fn framed_jsonl_typed_frames_not_line_scrape() {
        let mut agent = FramedAgent::new(JsonlRpcTransport::new(), PiCodec);
        agent.transport_mut().push_bytes(b"Pi starting...\n");
        let err = agent.try_recv().unwrap_err();
        assert!(err.to_string().contains("invalid-json"));

        let frame = serde_json::json!({"type":"turn_start","sessionId":"s1"});
        agent
            .transport_mut()
            .push_bytes(&JsonlRpcTransport::encode_jsonl(&frame).unwrap());
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::TurnStarted { .. }));

        let payload = r#"{"type":"tool_execution_start","toolCallId":"c1","toolName":"bash"}"#;
        let mid = payload.len() / 2;
        agent.transport_mut().push_bytes(
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"rpc_chunk","chunkId":"1","index":0,"count":2,"data":&payload[..mid]}),
                serde_json::json!({"type":"rpc_chunk","chunkId":"1","index":1,"count":2,"data":&payload[mid..]}),
            )
            .as_bytes(),
        );
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { name, .. } if name == "bash"));

        agent
            .send(AgentCommand::UserMessage {
                session_id: "s1".into(),
                text: "hi".into(),
            })
            .unwrap();
        assert_eq!(agent.transport().outbound()[0]["type"], "prompt");
    }
}
