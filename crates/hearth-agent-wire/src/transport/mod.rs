//! Framed wire I/O. Transports produce/consume [`WireFrame`]s.
//! Adapters never see unstructured stdout/stderr or ad-hoc log lines.
//!
//! # Allowed wires ([`WireKind`])
//! - Length-prefixed or Content-Length JSON-RPC (LSP / ACP style)
//! - HTTP + SSE with typed EventSource frames
//! - WebSocket JSON messages
//! - JSONL RPC — newline frame delimiter + optional `rpc_chunk` reassembly
//!
//! # Banned
//! - Line-splitting stdout/stderr as the event stream
//! - Regex-of-logs / scraping human-oriented CLI output
//! - Mixing SSE `data:` prefix stripping into a generic "stdio line" parser
//! - Treating banner / log noise as NDJSON events (`map_wire_line` / `push_line`)

pub mod jsonl;
pub mod jsonrpc;
pub mod sse;
pub mod websocket;

use crate::BusError;
use serde_json::Value;

/// Catalog of allowed framed wires. Registry rows store [`WireKind::as_str`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireKind {
    /// LSP / ACP `Content-Length` (or u32-prefixed) JSON-RPC.
    JsonRpc,
    /// HTTP + typed EventSource (`event` / `data` / `id`).
    Sse,
    /// One JSON value per WebSocket message.
    WebSocket,
    /// Newline-delimited JSON-RPC (one line, one JSON value).
    ///
    /// Pi / Paseo turn on `rpc_chunk` reassembly via [`JsonlRpcTransport::new`].
    /// ACP stdio uses the same framing with reassembly off
    /// ([`JsonlRpcTransport::without_rpc_chunks`]).
    JsonlRpc,
}

impl WireKind {
    pub const JSONRPC_CONTENT_LENGTH: &'static str = "jsonrpc-content-length";
    pub const HTTP_SSE: &'static str = "http-sse";
    pub const WEBSOCKET_JSON: &'static str = "websocket-json";
    pub const JSONL_RPC: &'static str = "jsonl-rpc";

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::JsonRpc => Self::JSONRPC_CONTENT_LENGTH,
            Self::Sse => Self::HTTP_SSE,
            Self::WebSocket => Self::WEBSOCKET_JSON,
            Self::JsonlRpc => Self::JSONL_RPC,
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            Self::JSONRPC_CONTENT_LENGTH | "jsonrpc" => Some(Self::JsonRpc),
            Self::HTTP_SSE | "sse" => Some(Self::Sse),
            Self::WEBSOCKET_JSON | "websocket" => Some(Self::WebSocket),
            Self::JSONL_RPC | "jsonl" => Some(Self::JsonlRpc),
            _ => None,
        }
    }
}

/// Already-decoded unit handed to an [`crate::adapters::AdapterCodec`].
/// Transports own framing; codecs own protocol semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum WireFrame {
    /// One JSON-RPC / WS JSON / JSONL message (object or array).
    Json(Value),
    /// Typed Server-Sent Event after EventSource framing is applied.
    Sse {
        event: Option<String>,
        id: Option<String>,
        data: Value,
    },
}

impl WireFrame {
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            WireFrame::Json(v) => Some(v),
            WireFrame::Sse { data, .. } => Some(data),
        }
    }
}

/// Byte-level framed I/O. Implementations must not scrape unstructured logs.
pub trait Transport: Send {
    fn send_frame(&mut self, frame: WireFrame) -> Result<(), BusError>;
    fn try_recv_frame(&mut self) -> Result<Option<WireFrame>, BusError>;
}

pub use jsonl::{JsonlProblem, JsonlRpcTransport};
pub use jsonrpc::JsonRpcTransport;
pub use sse::{SseFrame, SseTransport};
pub use websocket::WebSocketJsonTransport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_kind_roundtrip_known_names() {
        for kind in [
            WireKind::JsonRpc,
            WireKind::Sse,
            WireKind::WebSocket,
            WireKind::JsonlRpc,
        ] {
            assert_eq!(WireKind::parse(kind.as_str()), Some(kind));
        }
        assert_eq!(WireKind::parse("jsonl-rpc"), Some(WireKind::JsonlRpc));
        assert_eq!(WireKind::parse("ndjson"), None);
    }
}
