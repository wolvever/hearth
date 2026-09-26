//! OpenCode SSE / EventV2 bus (anomalyco/opencode).

use crate::{AgentEvent, AgentKind, BusError, PermissionOption, ToolStatus};
use serde_json::Value;

pub fn map_event(msg: &Value) -> Result<AgentEvent, BusError> {
    let ty = msg.get("type").and_then(|t| t.as_str()).unwrap_or("");
    let props = msg
        .get("properties")
        .cloned()
        .unwrap_or_else(|| msg.clone());
    let session_id = props
        .get("sessionID")
        .or_else(|| props.get("sessionId"))
        .or_else(|| props.pointer("/info/id"))
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();

    match ty {
        "session.created" => Ok(AgentEvent::SessionStarted {
            session_id: props
                .pointer("/info/id")
                .and_then(|s| s.as_str())
                .unwrap_or(&session_id)
                .into(),
            project_id: None,
        }),
        "session.status" => Ok(AgentEvent::Status {
            session_id,
            busy: props
                .get("status")
                .and_then(|s| s.as_str())
                .map(|s| s == "busy")
                .unwrap_or(false),
            detail: props
                .get("status")
                .and_then(|s| s.as_str())
                .map(str::to_string),
        }),
        "message.part.updated" | "message.part.delta" => {
            let part_type = props
                .pointer("/part/type")
                .and_then(|t| t.as_str())
                .unwrap_or("text");
            let text = props
                .get("delta")
                .and_then(|d| d.as_str())
                .or_else(|| props.pointer("/part/text").and_then(|t| t.as_str()))
                .unwrap_or("")
                .to_string();
            let item_id = props
                .pointer("/part/id")
                .and_then(|i| i.as_str())
                .unwrap_or("part")
                .to_string();
            match part_type {
                "reasoning" => Ok(AgentEvent::ThinkingDelta {
                    session_id,
                    item_id,
                    delta: text,
                }),
                "tool" => Ok(AgentEvent::ToolCall {
                    session_id,
                    item_id,
                    name: props
                        .pointer("/part/tool")
                        .and_then(|t| t.as_str())
                        .unwrap_or("tool")
                        .into(),
                    arguments: props
                        .pointer("/part/state/input")
                        .cloned()
                        .unwrap_or(Value::Null),
                    status: ToolStatus::InProgress,
                }),
                "compaction" => Ok(AgentEvent::CompactStarted { session_id }),
                _ => Ok(AgentEvent::MessageDelta {
                    session_id,
                    item_id,
                    delta: text,
                }),
            }
        }
        "permission.asked" => Ok(AgentEvent::PermissionAsk {
            session_id,
            permission_id: props
                .get("id")
                .and_then(|i| i.as_str())
                .unwrap_or("perm")
                .into(),
            title: props
                .get("title")
                .and_then(|t| t.as_str())
                .unwrap_or("Permission")
                .into(),
            description: None,
            tool_item_id: None,
            options: props
                .get("options")
                .and_then(|o| o.as_array())
                .map(|a| {
                    a.iter()
                        .map(|o| PermissionOption {
                            option_id: o
                                .get("option_id")
                                .or_else(|| o.get("optionId"))
                                .and_then(|x| x.as_str())
                                .unwrap_or("")
                                .into(),
                            name: o.get("name").and_then(|x| x.as_str()).unwrap_or("").into(),
                            kind: o.get("kind").and_then(|x| x.as_str()).unwrap_or("").into(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }),
        "question.asked" => Ok(AgentEvent::QuestionAsk {
            session_id,
            question_id: props
                .get("id")
                .and_then(|i| i.as_str())
                .unwrap_or("q")
                .into(),
            prompts: props
                .get("questions")
                .and_then(|q| q.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
        }),
        "session.error" => Ok(AgentEvent::Error {
            session_id: Some(session_id),
            message: props
                .pointer("/error/message")
                .and_then(|m| m.as_str())
                .unwrap_or("error")
                .into(),
        }),
        other => Ok(AgentEvent::Native {
            agent: AgentKind::OpenCode,
            method: other.into(),
            payload: msg.clone(),
        }),
    }
}


use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::AgentCommand;

#[derive(Debug, Default, Clone, Copy)]
pub struct OpenCodeCodec;

impl AdapterCodec for OpenCodeCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::OpenCode
    }

    fn wire(&self) -> &'static str {
        "http-sse"
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        // Prefer the SSE data payload; also accept plain JSON for fixtures.
        let v = match frame {
            WireFrame::Sse { data, .. } => data,
            WireFrame::Json(v) => v,
        };
        map_event(v).map(Some)
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        let body = match cmd {
            AgentCommand::UserMessage { session_id, text } => serde_json::json!({
                "type": "session.prompt",
                "sessionID": session_id,
                "text": text,
            }),
            AgentCommand::ReplyPermission {
                session_id,
                permission_id,
                allow,
                option_id,
            } => serde_json::json!({
                "type": "permission.reply",
                "sessionID": session_id,
                "id": permission_id,
                "allow": allow,
                "optionID": option_id,
            }),
            _ => return Err(BusError::Unsupported(cmd.name())),
        };
        Ok(WireFrame::Json(body))
    }
}

#[cfg(test)]
mod map_tests {
    use super::*;
    use crate::adapters::AdapterCodec;
    use crate::transport::{SseTransport, Transport, WireFrame};

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/src/adapters/opencode/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_permission_fixture() {
        let ev = map_event(&fixture("permission_asked.json")).unwrap();
        match ev {
            AgentEvent::PermissionAsk { title, .. } => assert!(title.contains("Allow")),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_message_delta_fixture() {
        let ev = map_event(&fixture("message_part_delta.json")).unwrap();
        assert!(matches!(ev, AgentEvent::MessageDelta { .. }));
    }

    #[test]
    fn codec_via_sse_frame() {
        let data = fixture("permission_asked.json");
        let frame = WireFrame::Sse {
            event: Some("permission".into()),
            id: None,
            data,
        };
        let ev = OpenCodeCodec.decode_event(&frame).unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
    }

    #[test]
    fn sse_transport_then_codec() {
        let mut t = SseTransport::new();
        t.push_decoded(Some("permission"), fixture("permission_asked.json"));
        let frame = t.try_recv_frame().unwrap().unwrap();
        let ev = OpenCodeCodec.decode_event(&frame).unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
    }
}
