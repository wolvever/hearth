//! Transport: bytes → [`Frame`]. Adapters never see raw stdio lines.
//!
//! - [`JsonRpcFramer`] — LSP / ACP `Content-Length` JSON-RPC. Bare NDJSON is rejected.
//! - [`SseFramer`] — WHATWG EventSource (blank-line dispatch, multi-line `data:`).
//! - [`WsJsonFramer`] — one WebSocket text message = one JSON value.
//!
//! Fail closed on partial leftover at [`Framer::finish`] and on corrupt frames.
//! Incomplete input on [`Framer::push_bytes`] waits (returns what is complete).
//! Newline-delimited "parse this line" is not a public path.


pub mod jsonrpc;
pub mod sse;
pub mod websocket;

pub use jsonrpc::{encode_jsonrpc, JsonRpcFramer};
pub use sse::{encode_sse, SseFramer};
pub use websocket::WsJsonFramer;

use crate::{adapters, AgentCommand, AgentEvent, AgentKind, BusError, CodingAgent};
use serde_json::Value;
use thiserror::Error;

/// Hard cap so a bogus Content-Length cannot grow the buffer without bound.
pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TransportError {
    #[error("transport closed: {0}")]
    Closed(String),
    #[error("corrupt frame: {0}")]
    Corrupt(String),
    #[error("payload too large ({0} bytes)")]
    TooLarge(usize),
}

impl From<TransportError> for BusError {
    fn from(e: TransportError) -> Self {
        BusError::Transport(e.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    JsonRpc,
    Sse,
    Ws,
}

/// One decoded native payload. Adapters consume [`Frame::value`].
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub kind: FrameKind,
    pub value: Value,
    /// SSE `event:` field, when the frame came from [`SseFramer`].
    pub sse_event: Option<String>,
    /// SSE `id:` field.
    pub sse_id: Option<String>,
}

impl Frame {
    pub fn json_rpc(value: Value) -> Self {
        Self {
            kind: FrameKind::JsonRpc,
            value,
            sse_event: None,
            sse_id: None,
        }
    }

    pub fn sse(value: Value, event: Option<String>, id: Option<String>) -> Self {
        Self {
            kind: FrameKind::Sse,
            value,
            sse_event: event,
            sse_id: id,
        }
    }

    pub fn ws(value: Value) -> Self {
        Self {
            kind: FrameKind::Ws,
            value,
            sse_event: None,
            sse_id: None,
        }
    }
}

pub trait Framer {
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, TransportError>;
    /// End of stream. Leftover partial/corrupt input fails closed.
    fn finish(&mut self) -> Result<Vec<Frame>, TransportError>;
}

pub(crate) fn fail_closed<T>(reason: impl Into<String>) -> Result<T, TransportError> {
    Err(TransportError::Corrupt(reason.into()))
}

pub(crate) fn parse_one_json(bytes: &[u8]) -> Result<Value, TransportError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| TransportError::Corrupt("frame body is not utf-8".into()))?;
    let text = text.trim();
    if text.is_empty() {
        return fail_closed("empty JSON body");
    }
    serde_json::from_str(text).map_err(|e| TransportError::Corrupt(format!("invalid JSON: {e}")))
}

/// Host-side test / attach helper: push decoded frames (or bytes through a [`Framer`]).
/// Transport-local buffer only — not a Queue noun.
pub struct FramedAgent {
    kind: AgentKind,
    inbound: Vec<AgentEvent>,
    sent: Vec<AgentCommand>,
}

impl FramedAgent {
    pub fn new(kind: AgentKind) -> Self {
        Self {
            kind,
            inbound: Vec::new(),
            sent: Vec::new(),
        }
    }

    /// Already-decoded native JSON (after Transport).
    pub fn push_frame(&mut self, value: Value) -> Result<(), BusError> {
        let ev = adapters::map_native(self.kind, &value)?;
        self.inbound.push(ev);
        Ok(())
    }

    pub fn push_decoded(&mut self, frame: Frame) -> Result<(), BusError> {
        self.push_frame(frame.value)
    }

    /// Decode `bytes` with `framer`, then map each frame.
    pub fn push_bytes<F: Framer>(&mut self, framer: &mut F, bytes: &[u8]) -> Result<(), BusError> {
        for frame in framer.push_bytes(bytes)? {
            self.push_decoded(frame)?;
        }
        Ok(())
    }

    pub fn sent(&self) -> &[AgentCommand] {
        &self.sent
    }
}

impl CodingAgent for FramedAgent {
    fn kind(&self) -> AgentKind {
        self.kind
    }

    fn send(&mut self, cmd: AgentCommand) -> Result<(), BusError> {
        self.sent.push(cmd);
        Ok(())
    }

    fn try_recv(&mut self) -> Result<Option<AgentEvent>, BusError> {
        if self.inbound.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.inbound.remove(0)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grok_tool_call() -> Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "c1",
                    "title": "read",
                    "status": "pending"
                }
            }
        })
    }

    #[test]
    fn jsonrpc_content_length_roundtrip() {
        let value = grok_tool_call();
        let bytes = encode_jsonrpc(&value).unwrap();
        assert!(bytes.starts_with(b"Content-Length:"));
        let mut f = JsonRpcFramer::new();
        let frames = f.push_bytes(&bytes).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].kind, FrameKind::JsonRpc);
        assert_eq!(frames[0].value["method"], "session/update");
        assert!(f.finish().unwrap().is_empty());
    }

    #[test]
    fn jsonrpc_split_across_pushes() {
        let bytes = encode_jsonrpc(&grok_tool_call()).unwrap();
        let mid = bytes.len() / 2;
        let mut f = JsonRpcFramer::new();
        assert!(f.push_bytes(&bytes[..mid]).unwrap().is_empty());
        let frames = f.push_bytes(&bytes[mid..]).unwrap();
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn jsonrpc_rejects_ndjson() {
        let mut f = JsonRpcFramer::new();
        let err = f
            .push_bytes(br#"{"method":"session/update"}"#)
            .unwrap_err();
        assert!(matches!(err, TransportError::Corrupt(s) if s.contains("Content-Length")));
        assert!(f.push_bytes(b"Content-Length: 2\r\n\r\n{}").is_err());
    }

    #[test]
    fn jsonrpc_finish_fails_on_partial() {
        let mut f = JsonRpcFramer::new();
        let bytes = encode_jsonrpc(&grok_tool_call()).unwrap();
        f.push_bytes(&bytes[..8]).unwrap();
        assert!(matches!(f.finish(), Err(TransportError::Corrupt(_))));
    }

    #[test]
    fn jsonrpc_invalid_json_fails_closed() {
        let mut f = JsonRpcFramer::new();
        let err = f
            .push_bytes(b"Content-Length: 3\r\n\r\n{no")
            .unwrap_err();
        assert!(matches!(err, TransportError::Corrupt(_)));
    }

    #[test]
    fn sse_dispatches_on_blank_line_only() {
        let value = serde_json::json!({
            "type": "permission.asked",
            "properties": {"sessionID": "s", "id": "p1", "title": "run?", "options": []}
        });
        let bytes = encode_sse(&value, Some("message")).unwrap();
        let mut f = SseFramer::new();
        // Without the terminating blank line, nothing is dispatched.
        let body_only = b"data: {\"type\":\"x\"}\n";
        assert!(f.push_bytes(body_only).unwrap().is_empty());
        assert!(matches!(f.finish(), Err(TransportError::Corrupt(_))));

        let mut f = SseFramer::new();
        let frames = f.push_bytes(&bytes).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].kind, FrameKind::Sse);
        assert_eq!(frames[0].sse_event.as_deref(), Some("message"));
        assert_eq!(frames[0].value["type"], "permission.asked");
    }

    #[test]
    fn sse_done_is_terminator_not_json() {
        let mut f = SseFramer::new();
        assert!(f.push_bytes(b"data: [DONE]\n\n").unwrap().is_empty());
        assert!(f.finish().unwrap().is_empty());
    }

    #[test]
    fn sse_corrupt_json_fails_closed() {
        let mut f = SseFramer::new();
        let err = f.push_bytes(b"data: not-json\n\n").unwrap_err();
        assert!(matches!(err, TransportError::Corrupt(_)));
    }

    #[test]
    fn ws_one_message_one_value() {
        let mut f = WsJsonFramer::new();
        let frame = f.push_message(br#"{"type":"run.start","sessionId":"s","runId":"r1"}"#).unwrap();
        assert_eq!(frame.kind, FrameKind::Ws);
        assert_eq!(frame.value["type"], "run.start");
        let err = f
            .push_message(br#"{"a":1}{"b":2}"#)
            .unwrap_err();
        assert!(matches!(err, TransportError::Corrupt(_)));
    }

    #[test]
    fn frame_agent_push_bytes_jsonrpc() {
        let mut agent = FramedAgent::new(AgentKind::GrokBuild);
        let mut framer = JsonRpcFramer::new();
        agent
            .push_bytes(&mut framer, &encode_jsonrpc(&grok_tool_call()).unwrap())
            .unwrap();
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::ToolCall { .. }));
    }
}
