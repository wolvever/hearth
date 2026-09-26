//! WHATWG EventSource / SSE.

use super::{parse_one_json, Frame, Framer, TransportError};
use serde_json::Value;

/// Encode one SSE event (`data:` + blank-line terminator). Multi-line JSON is split.
pub fn encode_sse(value: &Value, event: Option<&str>) -> Result<Vec<u8>, TransportError> {
    let body = serde_json::to_string(value)
        .map_err(|e| TransportError::Corrupt(format!("encode JSON: {e}")))?;
    let mut out = String::new();
    if let Some(ev) = event {
        out.push_str("event: ");
        out.push_str(ev);
        out.push('\n');
    }
    for line in body.split('\n') {
        out.push_str("data: ");
        out.push_str(line);
        out.push('\n');
    }
    out.push('\n');
    Ok(out.into_bytes())
}

/// WHATWG EventSource framer. Dispatches only on a blank line.
pub struct SseFramer {
    buf: Vec<u8>,
    data: Vec<String>,
    event: Option<String>,
    id: Option<String>,
    failed: Option<TransportError>,
}

impl Default for SseFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl SseFramer {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            data: Vec::new(),
            event: None,
            id: None,
            failed: None,
        }
    }

    fn check(&self) -> Result<(), TransportError> {
        match &self.failed {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    fn die<T>(&mut self, err: TransportError) -> Result<T, TransportError> {
        self.failed = Some(err.clone());
        self.buf.clear();
        self.data.clear();
        self.event = None;
        self.id = None;
        Err(err)
    }

    fn pending(&self) -> bool {
        !self.data.is_empty() || self.event.is_some() || self.id.is_some()
    }

    fn take_line(&mut self) -> Option<Vec<u8>> {
        let nl = self
            .buf
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|i| (i, 2))
            .or_else(|| self.buf.iter().position(|&b| b == b'\n').map(|i| (i, 1)))
            .or_else(|| self.buf.iter().position(|&b| b == b'\r').map(|i| (i, 1)))?;
        let (at, skip) = nl;
        let line = self.buf[..at].to_vec();
        self.buf.drain(..at + skip);
        Some(line)
    }

    fn apply_line(&mut self, line: &[u8]) -> Result<Option<Frame>, TransportError> {
        if line.is_empty() {
            return self.dispatch();
        }
        if line.first() == Some(&b':') {
            return Ok(None);
        }
        let text = std::str::from_utf8(line)
            .map_err(|_| TransportError::Corrupt("SSE field is not utf-8".into()))?;
        let (name, value) = match text.split_once(':') {
            Some((n, v)) => (n, v.strip_prefix(' ').unwrap_or(v)),
            None => (text, ""),
        };
        match name {
            "data" => self.data.push(value.to_string()),
            "event" => self.event = Some(value.to_string()),
            "id" => {
                if !value.contains('\0') {
                    self.id = Some(value.to_string());
                }
            }
            "retry" => {}
            _ => {}
        }
        Ok(None)
    }

    fn dispatch(&mut self) -> Result<Option<Frame>, TransportError> {
        if self.data.is_empty() {
            self.event = None;
            return Ok(None);
        }
        let data = self.data.join("\n");
        self.data.clear();
        let event = self.event.take();
        let id = self.id.clone();
        if data == "[DONE]" {
            return Ok(None);
        }
        match parse_one_json(data.as_bytes()) {
            Ok(value) => Ok(Some(Frame::sse(value, event, id))),
            Err(e) => self.die(e),
        }
    }
}

impl Framer for SseFramer {
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, TransportError> {
        self.check()?;
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        while let Some(line) = self.take_line() {
            if let Some(frame) = self.apply_line(&line)? {
                out.push(frame);
            }
        }
        Ok(out)
    }

    fn finish(&mut self) -> Result<Vec<Frame>, TransportError> {
        self.check()?;
        if !self.buf.is_empty() || self.pending() {
            return self.die(TransportError::Corrupt(
                "partial SSE event at end of stream".into(),
            ));
        }
        Ok(vec![])
    }
}

