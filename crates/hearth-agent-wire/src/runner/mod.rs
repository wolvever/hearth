//! Host-adjacent AttachRunner: spawn / pump framed stdio into a Transport.
//!
//! Not a kernel noun and not a Queue. Binding ids stay Host-owned —
//! [`crate::host::HostAttach`] never remints them here. Remint policy
//! (resume-not-load through stale-teardown + fail-closed guard) lives in
//! [`remint`].
//!
//! ACP catalog profiles use [`WireKind::JsonlRpc`] with
//! [`JsonlRpcTransport::without_rpc_chunks`]. Content-Length agents use
//! [`JsonRpcTransport`]. SoftExpiring / Flush / Stage / Evidence /
//! EffectId stay parked.

pub mod remint;

use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::Duration;

use crate::catalog::LaunchSpec;
use crate::transport::{JsonRpcTransport, JsonlRpcTransport, WireKind};
use crate::BusError;

pub use remint::{
    encode_remint_rpc, LiveCaps, RemintError, RemintEvent, RemintEventKind, RemintOutcome,
    RemintSession, RemintWireMethod, ReplaySlice, TeardownOutcome, TeardownSnapshot,
    TransportOwner, WireAction,
};

/// Cap how long a Real-child stdout read may block. A silent agent must
/// not hang the Host pump forever (same class as Copilot Content-Length hang).
const READ_STDOUT_TIMEOUT: Duration = Duration::from_millis(200);

/// Build the transport matching a [`LaunchSpec`] wire.
///
/// ACP / catalog `jsonl-rpc` → [`JsonlRpcTransport::without_rpc_chunks`]
/// (same WireKind as Pi, no `rpc_chunk` reassembly). Pi harness that wants
/// chunks should call [`JsonlRpcTransport::new`] itself.
pub fn transport_for_wire(wire: WireKind) -> RunnerTransport {
    match wire {
        WireKind::JsonlRpc => RunnerTransport::Jsonl(JsonlRpcTransport::without_rpc_chunks()),
        WireKind::JsonRpc => RunnerTransport::JsonRpc(JsonRpcTransport::new()),
        WireKind::Sse | WireKind::WebSocket => RunnerTransport::Unsupported(wire),
    }
}

/// Transport held by the runner pump path.
#[derive(Debug)]
pub enum RunnerTransport {
    Jsonl(JsonlRpcTransport),
    JsonRpc(JsonRpcTransport),
    Unsupported(WireKind),
}

impl RunnerTransport {
    pub fn as_jsonl_mut(&mut self) -> Option<&mut JsonlRpcTransport> {
        match self {
            Self::Jsonl(t) => Some(t),
            _ => None,
        }
    }

    pub fn as_jsonrpc_mut(&mut self) -> Option<&mut JsonRpcTransport> {
        match self {
            Self::JsonRpc(t) => Some(t),
            _ => None,
        }
    }
}

enum ChildIo {
    Real {
        child: Child,
        stdin: Option<ChildStdin>,
        stdout: Option<ChildStdout>,
    },
    /// Fixture / unit-test child: queued stdout bytes, recorded stdin.
    Fake {
        stdout: Vec<u8>,
        pos: usize,
        stdin_log: Vec<u8>,
        alive: bool,
        exit_code: Option<i32>,
    },
}

/// Spawn / pump framed agent stdio. Does not remint Binding.
pub struct AttachRunner {
    io: ChildIo,
    wire: WireKind,
}

impl AttachRunner {
    /// Spawn a real child from a catalog [`LaunchSpec`].
    pub fn spawn(spec: LaunchSpec) -> Result<Self, BusError> {
        let mut cmd = Command::new(&spec.program);
        cmd.args(&spec.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(cwd) = &spec.cwd {
            cmd.current_dir(cwd);
        }
        for (k, v) in &spec.env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| BusError::Transport(format!("spawn {}: {e}", spec.program)))?;
        let stdin = child.stdin.take();
        let mut stdout = child.stdout.take();
        if let Some(out) = stdout.as_mut() {
            set_nonblocking(out)?;
        }
        Ok(Self {
            io: ChildIo::Real {
                child,
                stdin,
                stdout,
            },
            wire: spec.wire,
        })
    }

    /// Fake child for fixture tests. `stdout` is the raw framed byte stream.
    pub fn spawn_fake(wire: WireKind, stdout: Vec<u8>) -> Self {
        Self {
            io: ChildIo::Fake {
                stdout,
                pos: 0,
                stdin_log: Vec::new(),
                alive: true,
                exit_code: None,
            },
            wire,
        }
    }

    pub fn wire(&self) -> WireKind {
        self.wire
    }

    /// Read available stdout bytes into a JSONL transport.
    pub fn pump_jsonl(&mut self, t: &mut JsonlRpcTransport) -> Result<usize, BusError> {
        let chunk = self.read_stdout()?;
        let n = chunk.len();
        if n > 0 {
            t.push_bytes(&chunk);
        }
        Ok(n)
    }

    /// Read available stdout bytes into a Content-Length JSON-RPC transport.
    pub fn pump_jsonrpc(&mut self, t: &mut JsonRpcTransport) -> Result<usize, BusError> {
        let chunk = self.read_stdout()?;
        let n = chunk.len();
        if n > 0 {
            t.push_bytes(&chunk)?;
        }
        Ok(n)
    }

    /// Pump into the transport selected by [`Self::wire`].
    pub fn pump(&mut self, transport: &mut RunnerTransport) -> Result<usize, BusError> {
        match transport {
            RunnerTransport::Jsonl(t) => self.pump_jsonl(t),
            RunnerTransport::JsonRpc(t) => self.pump_jsonrpc(t),
            RunnerTransport::Unsupported(w) => Err(BusError::Transport(format!(
                "AttachRunner does not pump {} yet",
                w.as_str()
            ))),
        }
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), BusError> {
        match &mut self.io {
            ChildIo::Real { stdin, .. } => {
                let Some(stdin) = stdin.as_mut() else {
                    return Err(BusError::Transport("child stdin closed".into()));
                };
                stdin
                    .write_all(bytes)
                    .map_err(|e| BusError::Transport(format!("write stdin: {e}")))?;
                stdin
                    .flush()
                    .map_err(|e| BusError::Transport(format!("flush stdin: {e}")))?;
                Ok(())
            }
            ChildIo::Fake {
                stdin_log, alive, ..
            } => {
                if !*alive {
                    return Err(BusError::Transport("fake child exited".into()));
                }
                stdin_log.extend_from_slice(bytes);
                Ok(())
            }
        }
    }

    /// Write one JSONL frame (`json\n`) — ACP catalog path.
    pub fn write_jsonl(&mut self, msg: &serde_json::Value) -> Result<(), BusError> {
        let bytes = JsonlRpcTransport::encode_jsonl(msg)?;
        self.write_bytes(&bytes)
    }

    /// Write one Content-Length frame.
    pub fn write_jsonrpc(&mut self, msg: &serde_json::Value) -> Result<(), BusError> {
        let bytes = JsonRpcTransport::encode_content_length(msg)?;
        self.write_bytes(&bytes)
    }

    pub fn stdin_log(&self) -> Option<&[u8]> {
        match &self.io {
            ChildIo::Fake { stdin_log, .. } => Some(stdin_log),
            ChildIo::Real { .. } => None,
        }
    }

    pub fn try_wait(&mut self) -> Result<Option<i32>, BusError> {
        match &mut self.io {
            ChildIo::Real { child, .. } => match child.try_wait() {
                Ok(Some(status)) => Ok(Some(status.code().unwrap_or(-1))),
                Ok(None) => Ok(None),
                Err(e) => Err(BusError::Transport(format!("try_wait: {e}"))),
            },
            ChildIo::Fake {
                alive,
                exit_code,
                pos,
                stdout,
                ..
            } => {
                if *pos >= stdout.len() && *alive {
                    *alive = false;
                    *exit_code = Some(0);
                }
                Ok(*exit_code)
            }
        }
    }

    pub fn kill(&mut self) -> Result<(), BusError> {
        match &mut self.io {
            ChildIo::Real { child, .. } => {
                let _ = child.kill();
                let _ = child.wait();
                Ok(())
            }
            ChildIo::Fake {
                alive, exit_code, ..
            } => {
                *alive = false;
                *exit_code = Some(9);
                Ok(())
            }
        }
    }

    fn read_stdout(&mut self) -> Result<Vec<u8>, BusError> {
        match &mut self.io {
            ChildIo::Real { stdout, .. } => {
                let Some(out) = stdout.as_mut() else {
                    return Ok(Vec::new());
                };
                if !poll_readable(out.as_raw_fd(), READ_STDOUT_TIMEOUT)? {
                    // Timeout / no data — return empty rather than hang.
                    return Ok(Vec::new());
                }
                let mut buf = [0u8; 4096];
                match out.read(&mut buf) {
                    Ok(0) => Ok(Vec::new()),
                    Ok(n) => Ok(buf[..n].to_vec()),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(Vec::new()),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(Vec::new()),
                    Err(e) => Err(BusError::Transport(format!("read stdout: {e}"))),
                }
            }
            ChildIo::Fake {
                stdout, pos, alive, ..
            } => {
                if *pos >= stdout.len() {
                    *alive = false;
                    return Ok(Vec::new());
                }
                // Chunked reads exercise partial-frame buffering.
                let end = (*pos + 64).min(stdout.len());
                let chunk = stdout[*pos..end].to_vec();
                *pos = end;
                if *pos >= stdout.len() {
                    *alive = false;
                }
                Ok(chunk)
            }
        }
    }
}

fn set_nonblocking(out: &ChildStdout) -> Result<(), BusError> {
    let fd = out.as_raw_fd();
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(BusError::Transport(format!(
            "fcntl F_GETFL: {}",
            std::io::Error::last_os_error()
        )));
    }
    let rc = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if rc < 0 {
        return Err(BusError::Transport(format!(
            "fcntl F_SETFL O_NONBLOCK: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn poll_readable(fd: libc::c_int, timeout: Duration) -> Result<bool, BusError> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
    loop {
        let rc = unsafe { libc::poll(&mut pfd, 1, ms) };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(BusError::Transport(format!("poll stdout: {err}")));
        }
        if rc == 0 {
            return Ok(false);
        }
        return Ok((pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) != 0);
    }
}

impl Drop for AttachRunner {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::acp::AcpCodec;
    use crate::adapters::AdapterCodec;
    use crate::catalog::{lookup_profile, LaunchSpec};
    use crate::transport::{Transport, WireFrame};
    use crate::{AgentEvent, CodingAgent, FramedAgent};
    use remint::{LiveCaps, RemintSession, RemintWireMethod, WireAction};

    #[test]
    fn acp_launch_spec_uses_jsonl_without_rpc_chunks() {
        let profile = lookup_profile("copilot").expect("copilot");
        let spec = profile.launch_spec().unwrap();
        assert_eq!(spec.wire, WireKind::JsonlRpc);
        let mut transport = transport_for_wire(spec.wire);
        let t = transport.as_jsonl_mut().expect("jsonl");
        // without_rpc_chunks: rpc_chunk line is plain JSON, not reassembled.
        t.push_bytes(br#"{"type":"rpc_chunk","id":"x","index":0,"count":1,"data":"e30="}"#);
        t.push_bytes(b"\n");
        let frame = t.try_recv_frame().unwrap().unwrap();
        assert_eq!(
            frame
                .as_json()
                .unwrap()
                .get("type")
                .and_then(|t| t.as_str()),
            Some("rpc_chunk")
        );
    }

    #[test]
    fn fake_child_pumps_jsonl_into_acp_framed_agent() {
        let notification = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "native-99",
                "update": {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "c1",
                    "title": "read",
                    "status": "pending"
                }
            }
        });
        let bytes = JsonlRpcTransport::encode_jsonl(&notification).unwrap();
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, bytes);
        let mut transport = JsonlRpcTransport::without_rpc_chunks();
        let mut saw = None;
        for _ in 0..16 {
            runner.pump_jsonl(&mut transport).unwrap();
            if let Some(frame) = transport.try_recv_frame().unwrap() {
                saw = Some(frame);
                break;
            }
            if runner.try_wait().unwrap().is_some() && saw.is_none() {
                break;
            }
        }
        let frame = saw.expect("framed notification");
        let ev = AcpCodec.decode_event(&frame).unwrap().unwrap();
        match ev {
            AgentEvent::ToolCall { item_id, name, .. } => {
                assert_eq!(item_id, "c1");
                assert_eq!(name, "read");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn fake_child_pumps_content_length_jsonrpc() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"sessionId": "s1", "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "hi"}}}
        });
        let bytes = JsonRpcTransport::encode_content_length(&msg).unwrap();
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonRpc, bytes);
        let mut t = JsonRpcTransport::new();
        for _ in 0..32 {
            runner.pump_jsonrpc(&mut t).unwrap();
            if t.try_recv_frame().unwrap().is_some() {
                break;
            }
        }
        // push_bytes path must have decoded one frame
        // (re-pump empty after recv)
        let mut t2 = JsonRpcTransport::new();
        let mut runner2 = AttachRunner::spawn_fake(
            WireKind::JsonRpc,
            JsonRpcTransport::encode_content_length(&msg).unwrap(),
        );
        let mut got = None;
        for _ in 0..32 {
            runner2.pump_jsonrpc(&mut t2).unwrap();
            if let Some(f) = t2.try_recv_frame().unwrap() {
                got = Some(f);
                break;
            }
        }
        assert_eq!(got.unwrap().as_json().unwrap()["params"]["sessionId"], "s1");
    }

    #[test]
    fn resume_wire_write_is_session_resume_not_new() {
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, Vec::new());
        let rpc = encode_remint_rpc(RemintWireMethod::SessionResume, "S-keep", 1);
        runner.write_jsonl(&rpc).unwrap();
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        assert!(log.contains("session/resume"));
        assert!(!log.contains("session/new"));
        assert!(!log.contains("session/load"));
    }

    #[test]
    fn remint_resume_then_fake_pump_keeps_agent_session() {
        let (session, _, _) = RemintSession::open(
            "sess-1",
            "S-stable",
            LiveCaps {
                resume: true,
                cancel: true,
                close: true,
                load_session: false,
            },
        );
        session.append_turn("hi", "hello");
        let out = session.remint_and_attach().unwrap();
        assert_eq!(out.agent_session_id, "S-stable");
        assert!(out.wire_actions.contains(&WireAction::AttachResume));

        let resumed = encode_remint_rpc(RemintWireMethod::SessionResume, &out.agent_session_id, 2);
        let mut runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": { "sessionId": "S-stable" }
            }))
            .unwrap(),
        );
        runner.write_jsonl(&resumed).unwrap();
        let mut t = JsonlRpcTransport::without_rpc_chunks();
        for _ in 0..8 {
            runner.pump_jsonl(&mut t).unwrap();
            if let Some(WireFrame::Json(v)) = t.try_recv_frame().unwrap() {
                assert_eq!(v["result"]["sessionId"], "S-stable");
                break;
            }
        }
        assert_eq!(session.agent_session_id(), "S-stable");
    }

    #[test]
    fn silent_fork_after_failed_remint_is_blocked() {
        let (session, _, _) = RemintSession::open(
            "sess-1",
            "S-orig",
            LiveCaps {
                resume: false,
                load_session: true,
                ..LiveCaps::default()
            },
        );
        let err = session.remint_and_attach().unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        // acpx CLI would session/new — AttachRunner remint must not.
        let fork = session.refuse_session_new();
        assert_eq!(fork, RemintError::SilentForkBlocked);
        assert_eq!(session.agent_session_id(), "S-orig");
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, Vec::new());
        // Even a mistaken write of session/new is detectable in tests;
        // production remint APIs never produce this method.
        let banned = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "session/new",
            "params": {}
        });
        runner.write_jsonl(&banned).unwrap();
        // Guard: remint encode helper never emits session/new.
        let allowed = encode_remint_rpc(RemintWireMethod::SessionResume, "S-orig", 1);
        assert_ne!(allowed["method"], "session/new");
        assert_ne!(allowed["method"], "session/load");
    }

    #[test]
    fn framed_agent_jsonl_roundtrip_via_runner_transport() {
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let t = transport.as_jsonl_mut().unwrap();
        let mut agent = FramedAgent::new(
            // Move out — rebuild with without_rpc_chunks explicitly.
            JsonlRpcTransport::without_rpc_chunks(),
            AcpCodec,
        );
        let raw = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/request_permission",
            "params": {
                "sessionId": "s1",
                "id": "perm-1",
                "title": "Allow?",
                "options": [{"optionId": "allow", "name": "Allow", "kind": "allow_once"}]
            }
        });
        agent
            .transport_mut()
            .push_bytes(&JsonlRpcTransport::encode_jsonl(&raw).unwrap());
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(ev, AgentEvent::PermissionAsk { .. }));
        // silence unused
        let _ = t;
    }

    #[test]
    fn ndjson_without_content_length_does_not_frame_on_jsonrpc_pump() {
        let mut runner =
            AttachRunner::spawn_fake(WireKind::JsonRpc, b"{\"method\":\"nope\"}\n".to_vec());
        let mut t = JsonRpcTransport::new();
        runner.pump_jsonrpc(&mut t).unwrap();
        assert!(t.try_recv_frame().unwrap().is_none());
    }

    #[test]
    fn silent_child_read_stdout_returns_within_timeout() {
        use std::time::Instant;
        let spec = LaunchSpec {
            program: "sleep".into(),
            args: vec!["30".into()],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        };
        let mut runner = AttachRunner::spawn(spec).expect("spawn sleep");
        let mut t = JsonlRpcTransport::without_rpc_chunks();
        let start = Instant::now();
        let n = runner.pump_jsonl(&mut t).expect("pump");
        let elapsed = start.elapsed();
        assert_eq!(n, 0, "silent child must yield no bytes");
        // Must return near READ_STDOUT_TIMEOUT, never hang for the child lifetime.
        assert!(
            elapsed < Duration::from_secs(2),
            "read_stdout hung: elapsed={elapsed:?}"
        );
        assert!(
            elapsed >= READ_STDOUT_TIMEOUT / 2,
            "expected a bounded wait, got {elapsed:?}"
        );
        let _ = runner.kill();
    }
}
