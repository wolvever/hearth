//! Content-Length / length-prefixed JSON-RPC transport (LSP / ACP style).
//!
//! Callers feed **already-framed** bytes or **already-decoded** [`Value`]s.
//! This type never treats a bare newline as a message boundary for the body
//! (headers use CRLF per LSP; NDJSON-of-logs is explicitly rejected).

use super::{Transport, WireFrame};
use crate::BusError;
use serde_json::Value;
use std::collections::VecDeque;

/// In-memory / stub JSON-RPC transport.
#[derive(Debug, Default)]
pub struct JsonRpcTransport {
    inbound: VecDeque<Value>,
    outbound: Vec<Value>,
    /// Partial Content-Length bytes awaiting a complete frame.
    byte_buf: Vec<u8>,
}

impl JsonRpcTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push an already-decoded JSON-RPC message (notification, request, or response).
    pub fn push_decoded(&mut self, msg: Value) {
        self.inbound.push_back(msg);
    }

    pub fn outbound(&self) -> &[Value] {
        &self.outbound
    }

    /// Feed stdout / socket bytes. Complete Content-Length frames are
    /// decoded into the inbound queue; leftovers stay buffered. Bare
    /// NDJSON without a Content-Length header stays buffered (never
    /// line-scraped into events).
    pub fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), BusError> {
        self.byte_buf.extend_from_slice(bytes);
        loop {
            match Self::try_decode_content_length(&mut self.byte_buf)? {
                Some(v) => self.inbound.push_back(v),
                None => break,
            }
        }
        Ok(())
    }

    /// Encode one message as LSP/ACP Content-Length framed bytes.
    pub fn encode_content_length(msg: &Value) -> Result<Vec<u8>, BusError> {
        let body = serde_json::to_vec(msg).map_err(|e| BusError::Encode(e.to_string()))?;
        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        let mut out = header.into_bytes();
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Decode one Content-Length framed message from a growable buffer.
    /// Returns `Ok(None)` if the buffer is incomplete. Leaves leftovers in `buf`.
    ///
    /// Does **not** split on bare newlines for the JSON body.
    pub fn try_decode_content_length(buf: &mut Vec<u8>) -> Result<Option<Value>, BusError> {
        let Some(header_end) = find_header_end(buf) else {
            return Ok(None);
        };
        let header = std::str::from_utf8(&buf[..header_end])
            .map_err(|e| BusError::Decode(format!("utf8 header: {e}")))?;
        let mut content_length: Option<usize> = None;
        for line in header.split("\r\n") {
            if line.is_empty() {
                continue;
            }
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name.eq_ignore_ascii_case("Content-Length") {
                content_length = Some(
                    value
                        .trim()
                        .parse()
                        .map_err(|e| BusError::Decode(format!("Content-Length: {e}")))?,
                );
            }
        }
        let len = content_length
            .ok_or_else(|| BusError::Decode("missing Content-Length header".into()))?;
        let body_start = header_end + 4; // \r\n\r\n
        let body_end = body_start + len;
        if buf.len() < body_end {
            return Ok(None);
        }
        let body = &buf[body_start..body_end];
        let value: Value =
            serde_json::from_slice(body).map_err(|e| BusError::Decode(e.to_string()))?;
        buf.drain(..body_end);
        Ok(Some(value))
    }

    /// Decode one big-endian u32 length-prefixed JSON message.
    pub fn try_decode_length_prefixed(buf: &mut Vec<u8>) -> Result<Option<Value>, BusError> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + len {
            return Ok(None);
        }
        let body = &buf[4..4 + len];
        let value: Value =
            serde_json::from_slice(body).map_err(|e| BusError::Decode(e.to_string()))?;
        buf.drain(..4 + len);
        Ok(Some(value))
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

impl Transport for JsonRpcTransport {
    fn send_frame(&mut self, frame: WireFrame) -> Result<(), BusError> {
        match frame {
            WireFrame::Json(v) => {
                self.outbound.push(v);
                Ok(())
            }
            WireFrame::Sse { .. } => Err(BusError::Transport(
                "JsonRpcTransport rejects SSE frames — use SseTransport".into(),
            )),
        }
    }

    fn try_recv_frame(&mut self) -> Result<Option<WireFrame>, BusError> {
        Ok(self.inbound.pop_front().map(WireFrame::Json))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_length_roundtrip() {
        let msg = serde_json::json!({"jsonrpc":"2.0","method":"session/update","params":{}});
        let bytes = JsonRpcTransport::encode_content_length(&msg).unwrap();
        let mut buf = bytes;
        let decoded = JsonRpcTransport::try_decode_content_length(&mut buf)
            .unwrap()
            .unwrap();
        assert_eq!(decoded, msg);
        assert!(buf.is_empty());
    }

    #[test]
    fn content_length_incomplete_returns_none() {
        let mut buf = b"Content-Length: 100\r\n\r\n{".to_vec();
        assert!(JsonRpcTransport::try_decode_content_length(&mut buf)
            .unwrap()
            .is_none());
        assert_eq!(buf.len(), 24);
    }

    #[test]
    fn length_prefixed_roundtrip() {
        let msg = serde_json::json!({"method":"ping"});
        let body = serde_json::to_vec(&msg).unwrap();
        let mut buf = (body.len() as u32).to_be_bytes().to_vec();
        buf.extend_from_slice(&body);
        let decoded = JsonRpcTransport::try_decode_length_prefixed(&mut buf)
            .unwrap()
            .unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn push_decoded_recv() {
        let mut t = JsonRpcTransport::new();
        t.push_decoded(serde_json::json!({"a":1}));
        match t.try_recv_frame().unwrap() {
            Some(WireFrame::Json(v)) => assert_eq!(v["a"], 1),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn push_bytes_decodes_content_length_frames() {
        let msg = serde_json::json!({"jsonrpc":"2.0","method":"ping"});
        let bytes = JsonRpcTransport::encode_content_length(&msg).unwrap();
        let mut t = JsonRpcTransport::new();
        // Split header/body to exercise the byte buffer.
        t.push_bytes(&bytes[..10]).unwrap();
        assert!(t.try_recv_frame().unwrap().is_none());
        t.push_bytes(&bytes[10..]).unwrap();
        match t.try_recv_frame().unwrap() {
            Some(WireFrame::Json(v)) => assert_eq!(v["method"], "ping"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn push_bytes_does_not_line_scrape_ndjson() {
        let mut t = JsonRpcTransport::new();
        t.push_bytes(b"{\"method\":\"nope\"}\n").unwrap();
        assert!(t.try_recv_frame().unwrap().is_none());
    }
}
