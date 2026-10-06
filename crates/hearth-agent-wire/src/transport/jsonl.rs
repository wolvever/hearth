//! Newline-delimited JSON-RPC transport (Paseo `JsonlRpcProcess` / `JsonlFrameDecoder`).
//!
//! Newline (`\n`, optional `\r`) is the **frame delimiter**. Partial writes stay
//! buffered until a newline arrives. Optional `rpc_chunk` frames reassemble one
//! logical JSON object (Paseo protocol v2 / Pi RPC).
//!
//! This is **not** NDJSON log scrape: invalid JSON is a [`JsonlProblem`], never
//! an [`super::WireFrame`] and never an [`crate::AgentEvent`]. Banner text such
//! as `Pi starting...` must not become an event.

use super::{Transport, WireFrame};
use crate::BusError;
use serde_json::Value;
use std::collections::VecDeque;

/// Why a physical JSONL line was rejected. Strings match Paseo's
/// `JsonlFrameProblem` so tests and logs stay comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonlProblem {
    InvalidJson,
    InvalidChunk,
    InvalidBase64,
    OutOfOrderChunk,
    ChunkLengthMismatch,
    InvalidChunkPayload,
}

impl JsonlProblem {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidJson => "invalid-json",
            Self::InvalidChunk => "invalid-chunk",
            Self::InvalidBase64 => "invalid-base64",
            Self::OutOfOrderChunk => "out-of-order-chunk",
            Self::ChunkLengthMismatch => "chunk-length-mismatch",
            Self::InvalidChunkPayload => "invalid-chunk-payload",
        }
    }
}

impl From<JsonlProblem> for BusError {
    fn from(p: JsonlProblem) -> Self {
        BusError::Decode(p.as_str().into())
    }
}

#[derive(Debug)]
struct Assembly {
    id: String,
    count: usize,
    byte_length: Option<usize>,
    parts: Vec<Vec<u8>>,
    bytes: usize,
}

/// In-memory JSONL RPC transport: bytes ↔ [`WireFrame::Json`].
///
/// [`Self::new`] reassembles Pi / Paseo `rpc_chunk` lines. ACP stdio uses
/// [`Self::without_rpc_chunks`]: one newline, one JSON value, no reassembly.
#[derive(Debug)]
pub struct JsonlRpcTransport {
    line_buf: Vec<u8>,
    inbound: VecDeque<Result<Value, JsonlProblem>>,
    outbound: Vec<Value>,
    assembly: Option<Assembly>,
    /// Pi / Paseo only. ACP catalog profiles leave this off.
    assemble_chunks: bool,
}

impl Default for JsonlRpcTransport {
    fn default() -> Self {
        Self {
            line_buf: Vec::new(),
            inbound: VecDeque::new(),
            outbound: Vec::new(),
            assembly: None,
            assemble_chunks: true,
        }
    }
}

impl JsonlRpcTransport {
    /// Pi / Paseo JSONL RPC, including `rpc_chunk` reassembly.
    pub fn new() -> Self {
        Self::default()
    }

    /// Plain newline-delimited JSON-RPC (ACP stdio). One line is one JSON
    /// value. Does not reassemble Pi `rpc_chunk` frames.
    pub fn without_rpc_chunks() -> Self {
        Self {
            assemble_chunks: false,
            ..Self::default()
        }
    }

    /// Push an already-decoded JSON object (tests / Host helpers).
    /// Does not run `rpc_chunk` assembly — use [`Self::push_bytes`] for that.
    pub fn push_decoded(&mut self, msg: Value) {
        self.inbound.push_back(Ok(msg));
    }

    pub fn outbound(&self) -> &[Value] {
        &self.outbound
    }

    /// Encode one message as a JSONL frame (`json\n`).
    pub fn encode_jsonl(msg: &Value) -> Result<Vec<u8>, BusError> {
        let mut out = serde_json::to_vec(msg).map_err(|e| BusError::Encode(e.to_string()))?;
        out.push(b'\n');
        Ok(out)
    }

    /// Feed stdout / socket bytes. Complete lines are framed; leftovers stay
    /// in the buffer. Invalid JSON is queued as [`JsonlProblem::InvalidJson`].
    pub fn push_bytes(&mut self, bytes: &[u8]) {
        self.line_buf.extend_from_slice(bytes);
        loop {
            let Some(i) = self.line_buf.iter().position(|&b| b == b'\n') else {
                break;
            };
            let mut line: Vec<u8> = self.line_buf.drain(..=i).collect();
            line.pop(); // '\n'
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            match std::str::from_utf8(&line) {
                Ok(s) => self.consume_line(s),
                Err(_) => self.inbound.push_back(Err(JsonlProblem::InvalidJson)),
            }
        }
    }

    /// Pop the next framed result (frame or problem). `None` if the buffer is
    /// incomplete / empty. Prefer [`Transport::try_recv_frame`] at the Host
    /// boundary — problems become [`BusError::Decode`].
    pub fn try_recv_result(&mut self) -> Option<Result<Value, JsonlProblem>> {
        self.inbound.pop_front()
    }

    fn consume_line(&mut self, line: &str) {
        let parsed: Result<Value, _> = serde_json::from_str(line);
        let Ok(v) = parsed else {
            self.inbound.push_back(Err(JsonlProblem::InvalidJson));
            return;
        };
        if self.assemble_chunks && v.get("type").and_then(|t| t.as_str()) == Some("rpc_chunk") {
            self.consume_chunk(&v);
            return;
        }
        self.inbound.push_back(Ok(v));
    }

    fn consume_chunk(&mut self, v: &Value) {
        let Some(header) = chunk_header(v) else {
            self.drop_assembly(JsonlProblem::InvalidChunk);
            return;
        };
        let Ok(part) = chunk_part_bytes(header.data) else {
            self.drop_assembly(JsonlProblem::InvalidBase64);
            return;
        };
        if self.assembly.is_none() {
            if header.index != 0 {
                self.inbound.push_back(Err(JsonlProblem::OutOfOrderChunk));
                return;
            }
            self.assembly = Some(Assembly {
                id: header.id.to_string(),
                count: header.count,
                byte_length: header.byte_length,
                parts: Vec::new(),
                bytes: 0,
            });
        }
        let ok = {
            let asm = self.assembly.as_ref().expect("assembly just set");
            asm.id == header.id
                && asm.count == header.count
                && asm.byte_length == header.byte_length
                && asm.parts.len() == header.index
        };
        if !ok {
            self.drop_assembly(JsonlProblem::OutOfOrderChunk);
            return;
        }
        let asm = self.assembly.as_mut().expect("assembly checked");
        asm.bytes += part.len();
        if let Some(limit) = asm.byte_length {
            if asm.bytes > limit {
                self.drop_assembly(JsonlProblem::ChunkLengthMismatch);
                return;
            }
        }
        asm.parts.push(part);
        if asm.parts.len() < asm.count {
            return;
        }
        let asm = self.assembly.take().expect("complete assembly");
        if let Some(limit) = asm.byte_length {
            if asm.bytes != limit {
                self.inbound
                    .push_back(Err(JsonlProblem::ChunkLengthMismatch));
                return;
            }
        }
        match String::from_utf8(asm.parts.into_iter().flatten().collect()) {
            Ok(joined) => match serde_json::from_str::<Value>(&joined) {
                Ok(frame) => self.inbound.push_back(Ok(frame)),
                Err(_) => self
                    .inbound
                    .push_back(Err(JsonlProblem::InvalidChunkPayload)),
            },
            Err(_) => self
                .inbound
                .push_back(Err(JsonlProblem::InvalidChunkPayload)),
        }
    }

    fn drop_assembly(&mut self, problem: JsonlProblem) {
        self.assembly = None;
        self.inbound.push_back(Err(problem));
    }
}

struct ChunkHeader<'a> {
    id: &'a str,
    index: usize,
    count: usize,
    byte_length: Option<usize>,
    data: &'a str,
}

fn chunk_header(v: &Value) -> Option<ChunkHeader<'_>> {
    let id = v.get("chunkId").and_then(|s| s.as_str()).unwrap_or("");
    let index = v.get("index").and_then(|n| n.as_u64())? as usize;
    let count = v.get("count").and_then(|n| n.as_u64())? as usize;
    let data = v.get("data").and_then(|s| s.as_str())?;
    let byte_length = v
        .get("byteLength")
        .and_then(|n| n.as_u64())
        .map(|n| n as usize);
    if id.is_empty() || count < 2 || data.is_empty() || index >= count {
        return None;
    }
    Some(ChunkHeader {
        id,
        index,
        count,
        byte_length,
        data,
    })
}

/// Paseo v2 sends base64; the slice-3 toy (and small fixtures) send raw UTF-8
/// fragments. Strict base64 wins when the alphabet matches; otherwise the
/// string is treated as a payload slice.
fn chunk_part_bytes(data: &str) -> Result<Vec<u8>, ()> {
    if looks_like_base64(data) {
        decode_base64(data).ok_or(())
    } else {
        Ok(data.as_bytes().to_vec())
    }
}

fn looks_like_base64(data: &str) -> bool {
    !data.is_empty()
        && data.len().is_multiple_of(4)
        && data.bytes().all(|b| {
            matches!(
                b,
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' | b'='
            )
        })
}

fn decode_base64(data: &str) -> Option<Vec<u8>> {
    fn val(b: u8) -> Option<u8> {
        Some(match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' => 0,
            _ => return None,
        })
    }
    if !data.len().is_multiple_of(4) {
        return None;
    }
    let pad = data.bytes().rev().take_while(|&b| b == b'=').count();
    if pad > 2 {
        return None;
    }
    let mut out = Vec::with_capacity(data.len() / 4 * 3);
    for chunk in data.as_bytes().chunks_exact(4) {
        let (a, b, c, d) = (
            val(chunk[0])?,
            val(chunk[1])?,
            val(chunk[2])?,
            val(chunk[3])?,
        );
        out.push((a << 2) | (b >> 4));
        out.push((b << 4) | (c >> 2));
        out.push((c << 6) | d);
    }
    for _ in 0..pad {
        out.pop();
    }
    Some(out)
}

impl Transport for JsonlRpcTransport {
    fn send_frame(&mut self, frame: WireFrame) -> Result<(), BusError> {
        match frame {
            WireFrame::Json(v) => {
                self.outbound.push(v);
                Ok(())
            }
            WireFrame::Sse { .. } => Err(BusError::Transport(
                "JsonlRpcTransport rejects SSE frames — use SseTransport".into(),
            )),
        }
    }

    fn try_recv_frame(&mut self) -> Result<Option<WireFrame>, BusError> {
        match self.inbound.pop_front() {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some(WireFrame::Json(v))),
            Some(Err(p)) => Err(p.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recv_ok(t: &mut JsonlRpcTransport) -> Value {
        match t.try_recv_result().expect("queued") {
            Ok(v) => v,
            Err(p) => panic!("expected frame, got {p:?}"),
        }
    }

    fn recv_problem(t: &mut JsonlRpcTransport) -> JsonlProblem {
        match t.try_recv_result().expect("queued") {
            Ok(v) => panic!("expected problem, got {v}"),
            Err(p) => p,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let msg = serde_json::json!({"type":"turn_start","sessionId":"s1"});
        let bytes = JsonlRpcTransport::encode_jsonl(&msg).unwrap();
        assert!(bytes.ends_with(b"\n"));
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(&bytes);
        assert_eq!(recv_ok(&mut t), msg);
        assert!(t.try_recv_result().is_none());
    }

    #[test]
    fn incomplete_line_waits() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(br#"{"type":"turn_start"}"#);
        assert!(t.try_recv_result().is_none());
        t.push_bytes(b"\n");
        assert_eq!(recv_ok(&mut t)["type"], "turn_start");
    }

    #[test]
    fn banner_is_problem_not_frame() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(b"Pi starting...\n");
        assert_eq!(recv_problem(&mut t), JsonlProblem::InvalidJson);
        assert!(t.try_recv_frame().unwrap().is_none());
    }

    #[test]
    fn crlf_and_split_writes() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(b"{\"type\":\"ready\"}\r\n{\"type\":");
        t.push_bytes(b"\"notice\"}\n");
        assert_eq!(recv_ok(&mut t)["type"], "ready");
        assert_eq!(recv_ok(&mut t)["type"], "notice");
    }

    #[test]
    fn empty_lines_skipped() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(b"\n\n{\"type\":\"ready\"}\n\n");
        assert_eq!(recv_ok(&mut t)["type"], "ready");
        assert!(t.try_recv_result().is_none());
    }

    #[test]
    fn raw_chunk_reassembly() {
        let payload = r#"{"type":"tool_execution_start","toolCallId":"c1","toolName":"bash"}"#;
        let mid = payload.len() / 2;
        let (a, b) = (&payload[..mid], &payload[mid..]);
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"rpc_chunk","chunkId":"1","index":0,"count":2,"byteLength":payload.len(),"data":a}),
                serde_json::json!({"type":"rpc_chunk","chunkId":"1","index":1,"count":2,"byteLength":payload.len(),"data":b}),
            )
            .as_bytes(),
        );
        let v = recv_ok(&mut t);
        assert_eq!(v["type"], "tool_execution_start");
        assert_eq!(v["toolCallId"], "c1");
    }

    #[test]
    fn base64_chunk_reassembly() {
        let payload = br#"{"type":"turn_start"}"#;
        let mid = payload.len() / 2;
        let a = encode_base64(&payload[..mid]);
        let b = encode_base64(&payload[mid..]);
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"rpc_chunk","chunkId":"b64","index":0,"count":2,"byteLength":payload.len(),"data":a}),
                serde_json::json!({"type":"rpc_chunk","chunkId":"b64","index":1,"count":2,"byteLength":payload.len(),"data":b}),
            )
            .as_bytes(),
        );
        assert_eq!(recv_ok(&mut t)["type"], "turn_start");
    }

    #[test]
    fn out_of_order_chunk_is_problem() {
        let payload = r#"{"type":"turn_start"}"#;
        let mid = payload.len() / 2;
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(
            format!(
                "{}\n{{\"type\":\"ready\"}}\n",
                serde_json::json!({"type":"rpc_chunk","chunkId":"1","index":1,"count":2,"byteLength":payload.len(),"data":&payload[mid..]}),
            )
            .as_bytes(),
        );
        assert_eq!(recv_problem(&mut t), JsonlProblem::OutOfOrderChunk);
        assert_eq!(recv_ok(&mut t)["type"], "ready");
    }

    #[test]
    fn invalid_chunk_header() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(br#"{"type":"rpc_chunk","chunkId":"","index":0,"count":2,"data":"xx"}"#);
        t.push_bytes(b"\n");
        assert_eq!(recv_problem(&mut t), JsonlProblem::InvalidChunk);
    }

    #[test]
    fn try_recv_frame_surfaces_decode_error() {
        let mut t = JsonlRpcTransport::new();
        t.push_bytes(b"not-json\n");
        let err = t.try_recv_frame().unwrap_err();
        assert!(err.to_string().contains("invalid-json"));
    }

    #[test]
    fn send_frame_json_only() {
        let mut t = JsonlRpcTransport::new();
        t.send_frame(WireFrame::Json(serde_json::json!({"type":"prompt"})))
            .unwrap();
        assert_eq!(t.outbound().len(), 1);
        let err = t
            .send_frame(WireFrame::Sse {
                event: None,
                id: None,
                data: Value::Null,
            })
            .unwrap_err();
        assert!(err.to_string().contains("SSE"));
    }

    #[test]
    fn plain_newline_does_not_reassemble_rpc_chunk() {
        let line = serde_json::json!({
            "type": "rpc_chunk",
            "chunkId": "1",
            "index": 0,
            "count": 2,
            "data": "abc"
        });
        let mut t = JsonlRpcTransport::without_rpc_chunks();
        t.push_bytes(&JsonlRpcTransport::encode_jsonl(&line).unwrap());
        let v = recv_ok(&mut t);
        assert_eq!(v["type"], "rpc_chunk");
        assert_eq!(v["chunkId"], "1");
        assert!(t.try_recv_result().is_none());
    }

    fn encode_base64(bytes: &[u8]) -> String {
        const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        let mut i = 0;
        while i < bytes.len() {
            let b0 = bytes[i];
            let b1 = if i + 1 < bytes.len() { bytes[i + 1] } else { 0 };
            let b2 = if i + 2 < bytes.len() { bytes[i + 2] } else { 0 };
            let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | b2 as u32;
            out.push(T[((n >> 18) & 63) as usize] as char);
            out.push(T[((n >> 12) & 63) as usize] as char);
            if i + 1 < bytes.len() {
                out.push(T[((n >> 6) & 63) as usize] as char);
            } else {
                out.push('=');
            }
            if i + 2 < bytes.len() {
                out.push(T[(n & 63) as usize] as char);
            } else {
                out.push('=');
            }
            i += 3;
        }
        out
    }
}
