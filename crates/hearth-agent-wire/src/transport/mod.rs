//! Framed wire I/O. Transports produce/consume [`WireFrame`]s.
//! Adapters never see unstructured stdout/stderr or ad-hoc log lines.
//!
//! # Allowed wires
//! - Length-prefixed or Content-Length JSON-RPC (LSP / ACP style)
//! - HTTP + SSE with typed EventSource frames
//! - WebSocket JSON messages
//!
//! # Banned
//! - Line-splitting stdout/stderr as the event stream
//! - Regex-of-logs / scraping human-oriented CLI output
//! - Mixing SSE `data:` prefix stripping into a generic "stdio line" parser

pub mod jsonrpc;
pub mod sse;
pub mod websocket;

use crate::BusError;
use serde_json::Value;

/// Already-decoded unit handed to an [`crate::adapters::AdapterCodec`].
/// Transports own framing; codecs own protocol semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum WireFrame {
    /// One JSON-RPC / WS JSON message (object or array).
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

pub use jsonrpc::JsonRpcTransport;
pub use sse::{SseFrame, SseTransport};
pub use websocket::WebSocketJsonTransport;
