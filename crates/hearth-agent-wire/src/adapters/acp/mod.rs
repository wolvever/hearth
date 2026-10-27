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

/// `update._meta.claudeCode.parentToolUseId`: the Task / Agent tool call a
/// Claude Code (claude-agent-acp) item ran under. Fail-closed: a missing,
/// non-string, or empty value is `None`, never a guessed parent. `self_id`
/// (the item's own id) is never its own parent.
fn claude_parent(update: &Value, self_id: Option<&str>) -> Option<String> {
    update
        .pointer("/_meta/claudeCode/parentToolUseId")
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty() && Some(*p) != self_id)
        .map(str::to_string)
}

/// `update._meta.claudeCode.subagent == true` (JSON `true` only): this tool
/// call launches a subagent. Anything else is `false`.
fn claude_subagent(update: &Value) -> bool {
    update
        .pointer("/_meta/claudeCode/subagent")
        .and_then(Value::as_bool)
        .unwrap_or(false)
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
                parent: claude_parent(&update, None),
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
            let item_id = update
                .get("toolCallId")
                .and_then(|i| i.as_str())
                .unwrap_or("")
                .to_string();
            Ok(AgentEvent::ToolCall {
                session_id,
                parent: claude_parent(&update, Some(&item_id)),
                subagent: claude_subagent(&update),
                item_id,
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

/// JSON-RPC "method not found" (ACP: unadvertised / unknown agent→client request).
pub const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC "request cancelled": in-flight agent→client request closed by
/// session cancel or Binding remint / teardown.
pub const REQUEST_CANCELLED: i64 = -32800;

/// Agent→client JSON-RPC requests Host already routes elsewhere (the
/// permission pending map). These never fall through to `-32601`.
pub const HANDLED_AGENT_REQUESTS: &[&str] = &["session/request_permission"];

/// What the client advertises in `initialize.clientCapabilities`, and the
/// single source of truth for which agent→client requests Host accepts.
///
/// [`Self::HOST`] is what Host truly implements today: nothing. Host has no
/// file-system or terminal handler, so every flag is `false` and every
/// `fs/*` / `terminal/*` request is answered `-32601` at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ClientCapabilities {
    pub read_text_file: bool,
    pub write_text_file: bool,
    pub terminal: bool,
}

impl ClientCapabilities {
    /// Host's real implemented set (none today). No stubs.
    pub const HOST: Self = Self {
        read_text_file: false,
        write_text_file: false,
        terminal: false,
    };

    /// `clientCapabilities` JSON for `initialize`.
    pub fn to_json(self) -> Value {
        serde_json::json!({
            "fs": {
                "readTextFile": self.read_text_file,
                "writeTextFile": self.write_text_file,
            },
            "terminal": self.terminal,
        })
    }

    /// Whether an agent→client request `method` is covered by this set.
    pub fn advertises(self, method: &str) -> bool {
        match method {
            "fs/read_text_file" => self.read_text_file,
            "fs/write_text_file" => self.write_text_file,
            m if m.starts_with("terminal/") => self.terminal,
            _ => false,
        }
    }
}

/// Client `initialize` request advertising exactly `caps`.
pub fn encode_initialize(rpc_id: &RpcId, caps: ClientCapabilities) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": rpc_id.to_json(),
        "method": "initialize",
        "params": {
            "protocolVersion": 1,
            "clientCapabilities": caps.to_json(),
        },
    })
}

/// JSON-RPC error response on `rpc_id`.
pub fn error_response(rpc_id: &RpcId, code: i64, message: &str) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": rpc_id.to_json(),
        "error": {"code": code, "message": message},
    })
}

/// An agent→client JSON-RPC request Host has no dedicated route for
/// (decoded as [`AgentEvent::Native`] with a `method` and an `id`).
/// Notifications (no id) and responses (no method) return `None`, and so
/// do [`HANDLED_AGENT_REQUESTS`].
pub fn unrouted_agent_request(ev: &AgentEvent) -> Option<(RpcId, String)> {
    let AgentEvent::Native { payload, .. } = ev else {
        return None;
    };
    let method = payload.get("method")?.as_str()?;
    if HANDLED_AGENT_REQUESTS.contains(&method) {
        return None;
    }
    let rpc_id = RpcId::from_json(payload.get("id")?)?;
    Some((rpc_id, method.to_string()))
}

/// Host's decision for one unrouted agent→client request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRequestRoute {
    /// Advertised: track in flight until answered / cancelled.
    Track,
    /// Not advertised or unknown: answer this `-32601` now.
    Reject(AgentCommand),
}

/// Route an unrouted agent→client request against the advertised set.
pub fn route_client_request(
    caps: ClientCapabilities,
    rpc_id: &RpcId,
    method: &str,
) -> ClientRequestRoute {
    if caps.advertises(method) {
        ClientRequestRoute::Track
    } else {
        ClientRequestRoute::Reject(AgentCommand::ReplyError {
            rpc_id: rpc_id.clone(),
            code: METHOD_NOT_FOUND,
            message: format!("method not found: {method} (not advertised by client)"),
        })
    }
}

/// `-32800` reply closing one in-flight agent→client request.
pub fn cancelled_reply(rpc_id: &RpcId, method: &str, why: &str) -> AgentCommand {
    AgentCommand::ReplyError {
        rpc_id: rpc_id.clone(),
        code: REQUEST_CANCELLED,
        message: format!("{method} cancelled: {why}"),
    }
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
        AgentCommand::ReplyError {
            rpc_id,
            code,
            message,
        } => return Ok(WireFrame::Json(error_response(rpc_id, *code, message))),
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
        crate::adapters::fixture("acp", name)
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

    /// (kind, id, parent, subagent) per decoded frame. Panics on any event
    /// other than ToolCall / MessageDelta: no SubagentStarted, no
    /// SessionStarted, no child-session event is invented.
    fn nesting(name: &str) -> Vec<(&'static str, String, Option<String>, bool)> {
        let frames = fixture(name);
        frames
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                match AcpCodec
                    .decode_event(&WireFrame::Json(f.clone()))
                    .unwrap()
                    .unwrap()
                {
                    AgentEvent::ToolCall {
                        item_id,
                        parent,
                        subagent,
                        session_id,
                        ..
                    } => {
                        assert_eq!(session_id, "s1");
                        ("tool", item_id, parent, subagent)
                    }
                    AgentEvent::MessageDelta {
                        item_id,
                        parent,
                        session_id,
                        ..
                    } => {
                        assert_eq!(session_id, "s1");
                        ("msg", item_id, parent, false)
                    }
                    other => panic!("{name}: unexpected {other:?}"),
                }
            })
            .collect()
    }

    /// claude-agent-acp origin-main zed snapshot `subagent-task-legacy`:
    /// Task launch carries `subagent:true`; every child frame names
    /// `parentToolUseId: toolu_task`; the Task's own frames have no parent.
    #[test]
    fn claude_task_fixture_decodes_parent_and_subagent() {
        let rows = nesting("claude_subagent_task.json");
        assert_eq!(rows.len(), 25);
        let mut launch_marked = 0;
        for (kind, id, parent, subagent) in &rows {
            if id == "toolu_task" {
                assert_eq!(*kind, "tool");
                assert_eq!(parent, &None, "launch is top level");
                launch_marked += usize::from(*subagent);
            } else {
                assert_eq!(parent.as_deref(), Some("toolu_task"), "{kind} {id}");
                assert!(!subagent, "{id} is not a launch");
            }
        }
        // Only the four frames that carry the flag; later Task updates
        // (toolResponse / completion) drop it on the wire and decode false.
        assert_eq!(launch_marked, 4);
        assert!(rows[0].3, "initial tool_call is the marked launch");
        assert!(rows
            .iter()
            .any(|r| r.0 == "msg" && r.1 == "msg_sub_1" && r.2.as_deref() == Some("toolu_task")));
    }

    /// Agent → Agent launch: the inner launch is both a child (of outer) and
    /// a launch (`subagent:true`); the grandchild names the inner call.
    #[test]
    fn claude_nested_fixture_decodes_two_levels() {
        let rows = nesting("claude_subagent_nested.json");
        let p = |id: &str| -> Vec<(Option<&str>, bool)> {
            rows.iter()
                .filter(|r| r.1 == id)
                .map(|r| (r.2.as_deref(), r.3))
                .collect()
        };
        assert_eq!(p("toolu_outer"), [(None, true), (None, false)]);
        assert_eq!(
            p("toolu_inner"),
            [(Some("toolu_outer"), true), (Some("toolu_outer"), false)]
        );
        assert_eq!(
            p("toolu_grandchild_read"),
            [(Some("toolu_inner"), false), (Some("toolu_inner"), false)]
        );
        assert_eq!(p("msg_outer_1"), [(Some("toolu_outer"), false)]);
        assert_eq!(p("msg_root"), [(None, false)]);
    }

    /// Missing / malformed meta never invents a parent or a launch.
    #[test]
    fn claude_meta_fail_closed() {
        let tool = |meta: Option<serde_json::Value>| {
            let mut update = serde_json::json!({
                "sessionUpdate": "tool_call", "toolCallId": "t1", "title": "x"
            });
            if let Some(m) = meta {
                update["_meta"] = m;
            }
            let msg = serde_json::json!({"jsonrpc": "2.0", "method": "session/update",
                "params": {"sessionId": "s", "update": update}});
            match map_notification(&msg, AgentKind::Acp).unwrap() {
                AgentEvent::ToolCall {
                    parent, subagent, ..
                } => (parent, subagent),
                other => panic!("unexpected {other:?}"),
            }
        };
        let none = (None, false);
        assert_eq!(tool(None), none);
        assert_eq!(tool(Some(serde_json::json!(null))), none);
        assert_eq!(tool(Some(serde_json::json!({"other": {}}))), none);
        assert_eq!(tool(Some(serde_json::json!({"claudeCode": "x"}))), none);
        for bad in [
            serde_json::json!(null),
            serde_json::json!(7),
            serde_json::json!(""),
            serde_json::json!({"id": "p"}),
            serde_json::json!("t1"), // self-parent
        ] {
            assert_eq!(
                tool(Some(
                    serde_json::json!({"claudeCode": {"parentToolUseId": bad}})
                )),
                none,
                "{bad}"
            );
        }
        for bad in [
            serde_json::json!("true"),
            serde_json::json!(1),
            serde_json::json!(false),
        ] {
            assert_eq!(
                tool(Some(serde_json::json!({"claudeCode": {"subagent": bad}}))),
                none,
                "{bad}"
            );
        }
        // Vendor key is exact: parent under another namespace is ignored.
        assert_eq!(
            tool(Some(serde_json::json!({"codex": {"parentToolUseId": "p"}}))),
            none
        );
        assert_eq!(
            tool(Some(
                serde_json::json!({"claudeCode": {"parentToolUseId": "p", "subagent": true}})
            )),
            (Some("p".to_string()), true)
        );
        // Message chunk without meta: no parent.
        let msg = serde_json::json!({"jsonrpc": "2.0", "method": "session/update",
            "params": {"sessionId": "s", "update": {"sessionUpdate": "agent_message_chunk",
                "content": {"text": "hi"}, "_meta": {"claudeCode": {"toolName": "Read"}}}}});
        assert!(matches!(
            map_notification(&msg, AgentKind::Acp).unwrap(),
            AgentEvent::MessageDelta { parent: None, .. }
        ));
    }

    /// Absent fields keep the old serde shape (back-compat both ways).
    #[test]
    fn parent_fields_are_additive_on_serde() {
        let old = serde_json::json!({"event": "tool_call", "session_id": "s", "item_id": "c",
            "name": "read", "arguments": null, "status": "pending"});
        let ev: AgentEvent = serde_json::from_value(old.clone()).unwrap();
        assert!(matches!(
            ev,
            AgentEvent::ToolCall {
                parent: None,
                subagent: false,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(&ev).unwrap(), old);
        let nested = AgentEvent::MessageDelta {
            session_id: "s".into(),
            item_id: "m".into(),
            delta: "d".into(),
            parent: Some("toolu_task".into()),
        };
        let v = serde_json::to_value(&nested).unwrap();
        assert_eq!(v["parent"], "toolu_task");
        assert_eq!(serde_json::from_value::<AgentEvent>(v).unwrap(), nested);
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
