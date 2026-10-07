//! Generic Agent Client Protocol (ACP) JSON-RPC map.
//!
//! Shared `session/update` + `session/request_permission` surface used by
//! catalog agents and by [`crate::adapters::grok_build`]. Per-binary launch
//! argv lives in [`crate::catalog`] (`builtin.toml` → `LaunchSpec`); this
//! codec does not spawn and does not encode `session/load`.

use crate::adapters::AdapterCodec;
use crate::transport::WireFrame;
use crate::{AgentCommand, AgentEvent, AgentKind, BusError, PermissionOption, RpcId, ToolStatus};
use serde_json::Value;

/// Dispatch a decoded ACP JSON-RPC frame. `agent` stamps [`AgentEvent::Native`].
///
/// `session/update` is a notification. `session/request_permission` is a
/// JSON-RPC **request**: the agent blocks until the client answers on the
/// same `id`, so [`AgentEvent::PermissionAsk::rpc_id`] keeps that id.
pub fn map_notification(msg: &Value, agent: AgentKind) -> Result<AgentEvent, BusError> {
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = msg.get("params").cloned().unwrap_or(Value::Null);

    match method {
        "session/update" => map_session_update(&params, agent),
        "session/request_permission" => map_permission_request(msg),
        other => Ok(AgentEvent::Native {
            agent,
            method: other.into(),
            payload: msg.clone(),
        }),
    }
}

pub fn map_session_update(params: &Value, agent: AgentKind) -> Result<AgentEvent, BusError> {
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
            let status = match update
                .get("status")
                .and_then(|s| s.as_str())
                .unwrap_or("pending")
            {
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
                        .filter_map(|x| {
                            x.get("content")
                                .and_then(|c| c.as_str())
                                .map(str::to_string)
                        })
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
            agent,
            method: format!("session/update:{other}"),
            payload: params.clone(),
        }),
    }
}

fn bad_permission(why: &str) -> BusError {
    BusError::Decode(format!("session/request_permission: {why}"))
}

/// Map a full `session/request_permission` JSON-RPC request frame.
///
/// - `id` (the JSON-RPC request id) is required and kept as
///   [`AgentEvent::PermissionAsk::rpc_id`]; `permission_id` is its JSON text.
///   ACP params carry no `id` of their own.
/// - `toolCallId` is read from v1 `/toolCall/toolCallId` or v2
///   `/subject/toolCall/toolCallId`. It is not unique across concurrent asks
///   (copilot-cli #989) — never route a reply by it.
/// - `options[*].optionId` must be non-empty strings, unique per ask.
///
/// Malformed requests return [`BusError::Decode`] (never a fabricated id).
pub fn map_permission_request(msg: &Value) -> Result<AgentEvent, BusError> {
    let rpc_id = msg.get("id").and_then(RpcId::from_json).ok_or_else(|| {
        bad_permission("missing or invalid JSON-RPC id (request, not notification)")
    })?;
    let params = msg
        .get("params")
        .filter(|p| p.is_object())
        .ok_or_else(|| bad_permission("params must be an object"))?;
    let session_id = params
        .get("sessionId")
        .and_then(|s| s.as_str())
        .ok_or_else(|| bad_permission("missing sessionId"))?
        .to_string();
    // v1: params.toolCall; v2: params.subject.toolCall.
    let tool_call = params
        .get("toolCall")
        .filter(|t| t.is_object())
        .or_else(|| {
            params
                .pointer("/subject/toolCall")
                .filter(|t| t.is_object())
        });
    let tool_item_id = tool_call
        .and_then(|t| t.get("toolCallId"))
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .ok_or_else(|| {
            bad_permission("missing toolCall.toolCallId (v1 /toolCall or v2 /subject/toolCall)")
        })?
        .to_string();
    let raw_options = params
        .get("options")
        .and_then(|o| o.as_array())
        .ok_or_else(|| bad_permission("options must be an array"))?;
    let mut options: Vec<PermissionOption> = Vec::with_capacity(raw_options.len());
    for o in raw_options {
        let option_id = o
            .get("optionId")
            .and_then(|x| x.as_str())
            .filter(|x| !x.is_empty())
            .ok_or_else(|| bad_permission("option without a string optionId"))?;
        if options.iter().any(|p| p.option_id == option_id) {
            return Err(bad_permission("duplicate optionId"));
        }
        options.push(PermissionOption {
            option_id: option_id.into(),
            name: o.get("name").and_then(|x| x.as_str()).unwrap_or("").into(),
            kind: o.get("kind").and_then(|x| x.as_str()).unwrap_or("").into(),
        });
    }
    let title = params
        .get("title")
        .and_then(|t| t.as_str())
        .or_else(|| {
            tool_call
                .and_then(|t| t.get("title"))
                .and_then(|t| t.as_str())
        })
        .unwrap_or("Permission")
        .to_string();
    Ok(AgentEvent::PermissionAsk {
        session_id,
        permission_id: rpc_id.to_string(),
        title,
        description: params
            .get("description")
            .and_then(|d| d.as_str())
            .map(str::to_string),
        tool_item_id: Some(tool_item_id),
        options,
        rpc_id: Some(rpc_id),
    })
}

/// JSON-RPC response answering `session/request_permission` on `rpc_id`.
/// `option_id: Some` → `{outcome:"selected", optionId}`, `None` →
/// `{outcome:"cancelled"}` (ACP v1 tool-calls; v2 `session/cancel` MUST
/// answer every pending ask this way).
pub fn permission_response(rpc_id: &RpcId, option_id: Option<&str>) -> Value {
    let outcome = match option_id {
        Some(opt) => serde_json::json!({ "outcome": "selected", "optionId": opt }),
        None => serde_json::json!({ "outcome": "cancelled" }),
    };
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": rpc_id.to_json(),
        "result": { "outcome": outcome },
    })
}

/// Encode an outbound command as an ACP JSON-RPC frame.
///
/// [`AgentCommand::ReplyPermission`] encodes a JSON-RPC **response** on its
/// `rpc_id` (required); there is no `session/request_permission/result`
/// notification in ACP. [`AgentCommand::Abort`] is the `session/cancel`
/// notification — callers answer pending asks as cancelled first.
pub fn encode_frame(cmd: &AgentCommand) -> Result<WireFrame, BusError> {
    let (method, params) = match cmd {
        AgentCommand::UserMessage { session_id, text } => (
            "session/prompt",
            serde_json::json!({
                "sessionId": session_id,
                "prompt": [{"type": "text", "text": text}],
            }),
        ),
        AgentCommand::ReplyPermission {
            rpc_id, option_id, ..
        } => {
            let rpc_id = rpc_id.as_ref().ok_or_else(|| {
                BusError::Encode(
                    "ACP ReplyPermission needs the session/request_permission JSON-RPC id".into(),
                )
            })?;
            return Ok(WireFrame::Json(permission_response(
                rpc_id,
                option_id.as_deref(),
            )));
        }
        AgentCommand::Abort { session_id } => (
            "session/cancel",
            serde_json::json!({ "sessionId": session_id }),
        ),
        AgentCommand::Compact { session_id } => (
            "session/compact",
            serde_json::json!({ "sessionId": session_id }),
        ),
        other => return Err(BusError::Unsupported(other.name())),
    };
    Ok(WireFrame::Json(serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    })))
}

/// Generic ACP codec for catalog profiles. [`crate::adapters::grok_build::GrokBuildCodec`]
/// keeps [`AgentKind::GrokBuild`].
#[derive(Debug, Default, Clone, Copy)]
pub struct AcpCodec;

impl AdapterCodec for AcpCodec {
    fn kind(&self) -> AgentKind {
        AgentKind::Acp
    }

    fn wire(&self) -> &'static str {
        // ACP stdio is newline-delimited JSON-RPC, not Content-Length.
        // Pi `rpc_chunk` stays off — see `JsonlRpcTransport::without_rpc_chunks`.
        crate::transport::WireKind::JsonlRpc.as_str()
    }

    fn decode_event(&self, frame: &WireFrame) -> Result<Option<AgentEvent>, BusError> {
        match frame {
            WireFrame::Json(v) => map_notification(v, AgentKind::Acp).map(Some),
            WireFrame::Sse { .. } => Err(BusError::Decode(
                "acp expects JSON-RPC frames, not SSE".into(),
            )),
        }
    }

    fn encode_command(&self, cmd: &AgentCommand) -> Result<WireFrame, BusError> {
        encode_frame(cmd)
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
            "{}/src/adapters/acp/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    #[test]
    fn map_tool_call_fixture() {
        let ev =
            map_notification(&fixture("session_update_tool_call.json"), AgentKind::Acp).unwrap();
        match ev {
            AgentEvent::ToolCall {
                item_id,
                name,
                status,
                ..
            } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "read");
                assert_eq!(status, ToolStatus::Pending);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    fn ask_parts(ev: AgentEvent) -> (Option<RpcId>, Option<String>, String, Vec<String>) {
        match ev {
            AgentEvent::PermissionAsk {
                rpc_id,
                tool_item_id,
                title,
                options,
                ..
            } => (
                rpc_id,
                tool_item_id,
                title,
                options.into_iter().map(|o| o.option_id).collect(),
            ),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn map_permission_fixture_v1_tool_call() {
        let ev = map_notification(&fixture("permission_request.json"), AgentKind::Acp).unwrap();
        let (rpc_id, tc, title, opts) = ask_parts(ev);
        assert_eq!(rpc_id, Some(RpcId::Num(5)));
        assert_eq!(tc.as_deref(), Some("call_001"));
        assert_eq!(title, "Reading configuration file");
        assert_eq!(opts, ["allow-once", "reject-once"]);
    }

    #[test]
    fn map_permission_fixture_v2_subject_tool_call() {
        let ev = map_notification(&fixture("permission_request_v2.json"), AgentKind::Acp).unwrap();
        let (rpc_id, tc, title, opts) = ask_parts(ev);
        assert_eq!(rpc_id, Some(RpcId::Str("req-7".into())));
        assert_eq!(tc.as_deref(), Some("toolu_42"));
        assert_eq!(title, "Allow npm test?");
        assert_eq!(opts, ["allow-once", "reject-once"]);
    }

    #[test]
    fn permission_id_is_rpc_id_not_params_id() {
        // A stray params.id must not leak in (old code defaulted every ask to "perm").
        let msg = serde_json::json!({
            "jsonrpc": "2.0", "id": 9, "method": "session/request_permission",
            "params": {"sessionId": "s", "id": "perm", "toolCall": {"toolCallId": "t"},
                       "options": [{"optionId": "a", "name": "A", "kind": "allow_once"}]}
        });
        match map_notification(&msg, AgentKind::Acp).unwrap() {
            AgentEvent::PermissionAsk { permission_id, .. } => assert_eq!(permission_id, "9"),
            other => panic!("unexpected {other:?}"),
        }
        // Numeric 5 and string "5" are distinct ids.
        assert_ne!(
            RpcId::Num(5).to_string(),
            RpcId::Str("5".into()).to_string()
        );
    }

    #[test]
    fn malformed_permission_requests_are_typed_decode_errors() {
        let good = fixture("permission_request.json");
        let cases: Vec<(&str, serde_json::Value)> = vec![
            ("notification (no id)", {
                let mut v = good.clone();
                v.as_object_mut().unwrap().remove("id");
                v
            }),
            ("null id", {
                let mut v = good.clone();
                v["id"] = serde_json::Value::Null;
                v
            }),
            ("float id", {
                let mut v = good.clone();
                v["id"] = serde_json::json!(1.5);
                v
            }),
            ("params not object", {
                let mut v = good.clone();
                v["params"] = serde_json::json!([1, 2]);
                v
            }),
            ("no sessionId", {
                let mut v = good.clone();
                v["params"].as_object_mut().unwrap().remove("sessionId");
                v
            }),
            ("no toolCallId", {
                let mut v = good.clone();
                v["params"]["toolCall"] = serde_json::json!({"title": "x"});
                v
            }),
            ("options missing", {
                let mut v = good.clone();
                v["params"].as_object_mut().unwrap().remove("options");
                v
            }),
            ("option without optionId", {
                let mut v = good.clone();
                v["params"]["options"] = serde_json::json!([{"name": "Allow"}]);
                v
            }),
            ("duplicate optionId", {
                let mut v = good.clone();
                v["params"]["options"] = serde_json::json!([{"optionId": "a"}, {"optionId": "a"}]);
                v
            }),
        ];
        for (name, msg) in cases {
            match map_notification(&msg, AgentKind::Acp) {
                Err(BusError::Decode(why)) => {
                    assert!(
                        why.starts_with("session/request_permission"),
                        "{name}: {why}"
                    )
                }
                other => panic!("{name}: expected Decode error, got {other:?}"),
            }
            let frame = WireFrame::Json(msg);
            assert!(AcpCodec.decode_event(&frame).is_err(), "{name}");
        }
    }

    #[test]
    fn reply_is_jsonrpc_response_on_same_id_with_nested_outcome() {
        let reply = |rpc_id: Option<RpcId>, option_id: Option<&str>| {
            AcpCodec.encode_command(&AgentCommand::ReplyPermission {
                session_id: "s".into(),
                permission_id: "ignored".into(),
                allow: true,
                option_id: option_id.map(str::to_string),
                rpc_id,
            })
        };
        let WireFrame::Json(v) = reply(Some(RpcId::Num(5)), Some("allow-once")).unwrap() else {
            panic!("json frame");
        };
        assert_eq!(
            v,
            serde_json::json!({"jsonrpc": "2.0", "id": 5,
                "result": {"outcome": {"outcome": "selected", "optionId": "allow-once"}}})
        );
        assert!(v.get("method").is_none());
        let WireFrame::Json(v) = reply(Some(RpcId::Str("req-7".into())), None).unwrap() else {
            panic!("json frame");
        };
        assert_eq!(v["id"], "req-7");
        assert_eq!(
            v["result"]["outcome"],
            serde_json::json!({"outcome": "cancelled"})
        );
        // No rpc id → typed encode error, never an invented notification.
        assert!(matches!(
            reply(None, Some("allow-once")),
            Err(BusError::Encode(_))
        ));
    }

    #[test]
    fn abort_encodes_session_cancel_notification() {
        let WireFrame::Json(v) = AcpCodec
            .encode_command(&AgentCommand::Abort {
                session_id: "s".into(),
            })
            .unwrap()
        else {
            panic!("json frame");
        };
        assert_eq!(v["method"], "session/cancel");
        assert!(v.get("id").is_none());
    }

    #[test]
    fn codec_rejects_sse_frame() {
        let frame = WireFrame::Sse {
            event: None,
            id: None,
            data: serde_json::json!({}),
        };
        assert!(AcpCodec.decode_event(&frame).is_err());
    }
}
