//! HTTP + SSE transport stub — typed EventSource frames only.
//!
//! Adapters receive [`SseFrame`] / [`WireFrame::Sse`] after framing is done.
//! Do not strip `data:` prefixes inside adapter mappers; that belongs here
//! (or at the HTTP client boundary that yields typed frames).

use super::{Transport, WireFrame};
use crate::BusError;
use serde_json::Value;
use std::collections::VecDeque;

/// One Server-Sent Event after EventSource framing.
#[derive(Debug, Clone, PartialEq)]
pub struct SseFrame {
    pub event: Option<String>,
    pub id: Option<String>,
    pub data: Value,
}

impl From<SseFrame> for WireFrame {
    fn from(f: SseFrame) -> Self {
        WireFrame::Sse {
            event: f.event,
            id: f.id,
            data: f.data,
        }
    }
}

/// Stub SSE transport that accepts already-decoded EventSource frames.
#[derive(Debug, Default)]
pub struct SseTransport {
    inbound: VecDeque<SseFrame>,
    outbound: Vec<SseFrame>,
}

impl SseTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a typed frame (e.g. from an EventSource client that already parsed
    /// `event:` / `data:` / `id:` fields and JSON-decoded the data payload).
    pub fn push_frame(&mut self, frame: SseFrame) {
        self.inbound.push_back(frame);
    }

    /// Convenience: push JSON data with optional event name.
    pub fn push_decoded(&mut self, event: Option<&str>, data: Value) {
        self.push_frame(SseFrame {
            event: event.map(str::to_string),
            id: None,
            data,
        });
    }

    pub fn outbound(&self) -> &[SseFrame] {
        &self.outbound
    }

    /// Parse one complete SSE event block (fields separated by `\n`, block by blank line).
    /// `data` must be JSON. Skips `[DONE]` sentinels. Not for unstructured log lines.
    pub fn parse_event_block(block: &str) -> Result<Option<SseFrame>, BusError> {
        let block = block.trim();
        if block.is_empty() {
            return Ok(None);
        }
        let mut event: Option<String> = None;
        let mut id: Option<String> = None;
        let mut data_lines: Vec<&str> = Vec::new();
        for line in block.lines() {
            if line.starts_with(':') {
                continue; // comment
            }
            if let Some(rest) = line.strip_prefix("event:") {
                event = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("id:") {
                id = Some(rest.trim().to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.trim_start());
            } else {
                return Err(BusError::Decode(format!(
                    "SSE: unknown field line (not event/id/data/comment): {line:?}"
                )));
            }
        }
        if data_lines.is_empty() {
            return Ok(None);
        }
        let payload = data_lines.join("\n");
        if payload == "[DONE]" {
            return Ok(None);
        }
        let data: Value = serde_json::from_str(&payload)
            .map_err(|e| BusError::Decode(format!("SSE data JSON: {e}")))?;
        Ok(Some(SseFrame { event, id, data }))
    }
}

impl Transport for SseTransport {
    fn send_frame(&mut self, frame: WireFrame) -> Result<(), BusError> {
        match frame {
            WireFrame::Sse { event, id, data } => {
                self.outbound.push(SseFrame { event, id, data });
                Ok(())
            }
            WireFrame::Json(data) => {
                // Outbound HTTP POST bodies may be plain JSON; wrap as data-only SSE frame.
                self.outbound.push(SseFrame {
                    event: None,
                    id: None,
                    data,
                });
                Ok(())
            }
        }
    }

    fn try_recv_frame(&mut self) -> Result<Option<WireFrame>, BusError> {
        Ok(self.inbound.pop_front().map(Into::into))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_typed_event_block() {
        let block = "event: permission\nid: 1\ndata: {\"type\":\"permission.asked\",\"properties\":{\"sessionID\":\"s\",\"id\":\"p1\",\"title\":\"run?\",\"options\":[]}}\n";
        let frame = SseTransport::parse_event_block(block).unwrap().unwrap();
        assert_eq!(frame.event.as_deref(), Some("permission"));
        assert_eq!(frame.id.as_deref(), Some("1"));
        assert_eq!(frame.data["type"], "permission.asked");
    }

    #[test]
    fn done_sentinel_skipped() {
        assert!(SseTransport::parse_event_block("data: [DONE]\n")
            .unwrap()
            .is_none());
    }

    #[test]
    fn rejects_non_field_garbage() {
        let err = SseTransport::parse_event_block("INFO agent started\n").unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn push_decoded_recv() {
        let mut t = SseTransport::new();
        t.push_decoded(Some("msg"), serde_json::json!({"type":"x"}));
        match t.try_recv_frame().unwrap() {
            Some(WireFrame::Sse { event, data, .. }) => {
                assert_eq!(event.as_deref(), Some("msg"));
                assert_eq!(data["type"], "x");
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}
