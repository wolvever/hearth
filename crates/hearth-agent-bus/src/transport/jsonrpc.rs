//! Content-Length JSON-RPC (ACP / LSP / Codex app-server).

use super::{
    parse_one_json, Frame, Framer, TransportError, MAX_FRAME_BYTES,
};
use serde_json::Value;

/// Encode a JSON-RPC / ACP message with `Content-Length` headers.
pub fn encode_jsonrpc(value: &Value) -> Result<Vec<u8>, TransportError> {
    let body = serde_json::to_vec(value)
        .map_err(|e| TransportError::Corrupt(format!("encode JSON: {e}")))?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(TransportError::TooLarge(body.len()));
    }
    let mut out = format!("Content-Length: {}\r\n\r\n", body.len()).into_bytes();
    out.extend_from_slice(&body);
    Ok(out)
}

/// LSP / ACP Content-Length framer. Rejects newline-delimited JSON.
pub struct JsonRpcFramer {
    buf: Vec<u8>,
    failed: Option<TransportError>,
}

impl Default for JsonRpcFramer {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonRpcFramer {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
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
        Err(err)
    }

    fn reject_bare_json(&self) -> Result<(), TransportError> {
        let start = match self.buf.iter().position(|b| !b.is_ascii_whitespace()) {
            Some(i) => self.buf[i],
            None => return Ok(()),
        };
        if start == b'{' || start == b'[' {
            return Err(TransportError::Corrupt(
                "json-rpc requires Content-Length headers; newline-delimited JSON is rejected"
                    .into(),
            ));
        }
        Ok(())
    }

    fn extract_one(&mut self) -> Result<Option<Value>, TransportError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        if let Err(e) = self.reject_bare_json() {
            return self.die(e);
        }
        let Some(header_end) = find_header_end(&self.buf) else {
            return Ok(None);
        };
        let header_bytes = &self.buf[..header_end];
        let headers = match std::str::from_utf8(header_bytes) {
            Ok(s) => s,
            Err(_) => return self.die(TransportError::Corrupt("headers are not utf-8".into())),
        };
        let content_length = match parse_content_length(headers) {
            Ok(n) => n,
            Err(e) => return self.die(e),
        };
        if content_length > MAX_FRAME_BYTES {
            return self.die(TransportError::TooLarge(content_length));
        }
        let total = header_end + content_length;
        if self.buf.len() < total {
            return Ok(None);
        }
        let body = self.buf[header_end..total].to_vec();
        self.buf.drain(..total);
        match parse_one_json(&body) {
            Ok(v) => Ok(Some(v)),
            Err(e) => self.die(e),
        }
    }
}

impl Framer for JsonRpcFramer {
    fn push_bytes(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, TransportError> {
        self.check()?;
        if bytes.len() > MAX_FRAME_BYTES && self.buf.is_empty() {
            return self.die(TransportError::TooLarge(bytes.len()));
        }
        self.buf.extend_from_slice(bytes);
        if self.buf.len() > MAX_FRAME_BYTES.saturating_add(4096) {
            return self.die(TransportError::TooLarge(self.buf.len()));
        }
        let mut out = Vec::new();
        while let Some(value) = self.extract_one()? {
            out.push(Frame::json_rpc(value));
        }
        Ok(out)
    }

    fn finish(&mut self) -> Result<Vec<Frame>, TransportError> {
        self.check()?;
        let leftover = skip_ws(&self.buf);
        if leftover.is_empty() {
            self.buf.clear();
            return Ok(vec![]);
        }
        self.die(TransportError::Corrupt(
            "partial JSON-RPC frame at end of stream".into(),
        ))
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| i + 4)
        .or_else(|| buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2))
}

fn parse_content_length(headers: &str) -> Result<usize, TransportError> {
    let mut found = None;
    for raw in headers.split(['\n', '\r']) {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(TransportError::Corrupt(format!(
                "malformed header line: {line:?}"
            )));
        };
        if name.trim().eq_ignore_ascii_case("content-length") {
            let n = value.trim().parse::<usize>().map_err(|_| {
                TransportError::Corrupt(format!("invalid Content-Length: {:?}", value.trim()))
            })?;
            found = Some(n);
        }
    }
    found.ok_or_else(|| TransportError::Corrupt("missing Content-Length".into()))
}

fn skip_ws(buf: &[u8]) -> &[u8] {
    let i = buf
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(buf.len());
    &buf[i..]
}

