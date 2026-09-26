//! WebSocket JSON transport stub — one JSON message per WS text/binary frame.
//! No line scraping; the WS layer already delivers discrete messages.

use super::{Transport, WireFrame};
use crate::BusError;
use serde_json::Value;
use std::collections::VecDeque;

#[derive(Debug, Default)]
pub struct WebSocketJsonTransport {
    inbound: VecDeque<Value>,
    outbound: Vec<Value>,
}

impl WebSocketJsonTransport {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_decoded(&mut self, msg: Value) {
        self.inbound.push_back(msg);
    }

    pub fn outbound(&self) -> &[Value] {
        &self.outbound
    }

    pub fn decode_text(text: &str) -> Result<Value, BusError> {
        serde_json::from_str(text).map_err(|e| BusError::Decode(e.to_string()))
    }
}

impl Transport for WebSocketJsonTransport {
    fn send_frame(&mut self, frame: WireFrame) -> Result<(), BusError> {
        match frame {
            WireFrame::Json(v) => {
                self.outbound.push(v);
                Ok(())
            }
            WireFrame::Sse { .. } => Err(BusError::Transport(
                "WebSocketJsonTransport rejects SSE frames".into(),
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
    fn decode_text_json() {
        let v = WebSocketJsonTransport::decode_text(r#"{"ok":true}"#).unwrap();
        assert_eq!(v["ok"], true);
    }
}
