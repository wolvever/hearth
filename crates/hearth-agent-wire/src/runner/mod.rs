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

use crate::adapters::acp::{self, ClientCapabilities};
use crate::catalog::LaunchSpec;
use crate::transport::{JsonRpcTransport, JsonlRpcTransport, Transport, WireFrame, WireKind};
use crate::{AgentCommand, BusError, RpcId};

pub use remint::{
    encode_remint_rpc, flush_and_teardown_old_wire, flush_old_wire_replies,
    write_new_wire_attach_resume, write_old_wire_teardown, LiveCaps, OldWireFlush, OldWireWrite,
    PendingPermission, PermissionKey, PermissionRef, RemintError, RemintEvent, RemintEventKind,
    RemintOutcome, RemintSession, RemintWireMethod, ReplaySlice, TeardownOutcome, TeardownSnapshot,
    TransportOwner, WireAction,
};

/// Cap how long a Real-child stdout read may block. A silent agent must
/// not hang the Host pump forever (same class as Copilot Content-Length hang).
const READ_STDOUT_TIMEOUT: Duration = Duration::from_millis(200);

/// Upper bound on [`AttachRunner::wait_rpc_result`]: 256 pumps of
/// [`READ_STDOUT_TIMEOUT`] (~51s) for a real child.
const WAIT_RPC_RESULT_TIMEOUT: Duration = Duration::from_millis(256 * 200);

/// JSON-RPC "method not found" — answer for agent->client requests that
/// arrive while the handshake owns the wire.
const METHOD_NOT_FOUND: i64 = -32601;

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
    /// Set only when [`Self::initialize_host`] returns Ok. Agent-bound
    /// writes fail with [`BusError::NotInitialized`] until then.
    initialized: bool,
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
            initialized: false,
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
            initialized: false,
        }
    }

    pub fn is_initialized(&self) -> bool {
        self.initialized
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

    /// Agent-bound raw write. Fails with [`BusError::NotInitialized`]
    /// until [`Self::initialize_host`] has returned Ok.
    pub fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), BusError> {
        if !self.initialized {
            return Err(BusError::NotInitialized);
        }
        self.write_bytes_ungated(bytes)
    }

    /// Handshake-only write (initialize + replies during the wait).
    fn write_bytes_ungated(&mut self, bytes: &[u8]) -> Result<(), BusError> {
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
            ChildIo::Fake { stdout, pos, .. } => {
                // Exhausting stdout does not kill the fake child — try_wait
                // does. Live attach may still write after initialize returns.
                if *pos >= stdout.len() {
                    return Ok(Vec::new());
                }
                // Chunked reads exercise partial-frame buffering.
                let end = (*pos + 64).min(stdout.len());
                let chunk = stdout[*pos..end].to_vec();
                *pos = end;
                Ok(chunk)
            }
        }
    }

    /// Write one JSON value on the runner's wire (JSONL or Content-Length).
    pub fn write_value(&mut self, msg: &serde_json::Value) -> Result<(), BusError> {
        if !self.initialized {
            return Err(BusError::NotInitialized);
        }
        self.write_value_ungated(msg)
    }

    fn write_value_ungated(&mut self, msg: &serde_json::Value) -> Result<(), BusError> {
        let bytes = match self.wire {
            WireKind::JsonlRpc => JsonlRpcTransport::encode_jsonl(msg)?,
            WireKind::JsonRpc => JsonRpcTransport::encode_content_length(msg)?,
            WireKind::Sse | WireKind::WebSocket => {
                return Err(BusError::Transport(format!(
                    "AttachRunner does not write {} yet",
                    self.wire.as_str()
                )))
            }
        };
        self.write_bytes_ungated(&bytes)
    }

    /// Encode an ACP [`AgentCommand`] and write it on this wire.
    pub fn write_command(&mut self, cmd: &AgentCommand) -> Result<(), BusError> {
        let frame = acp::encode_frame(cmd)?;
        let WireFrame::Json(v) = frame else {
            return Err(BusError::Encode(
                "ACP command encode produced a non-JSON frame".into(),
            ));
        };
        self.write_value(&v)
    }

    /// Live ACP attach handshake: send `initialize` advertising
    /// [`ClientCapabilities::HOST`] (all false today), pump until the matching
    /// JSON-RPC result lands, then return [`LiveCaps`].
    ///
    /// Agent-bound RPCs (`session/new`, `session/prompt`, …) must not be
    /// written before this returns — the handshake owns the wire until the
    /// initialize reply is observed. Pep's rule: no client capability goes
    /// true in this cut.
    pub fn initialize_host(
        &mut self,
        transport: &mut RunnerTransport,
        rpc_id: &RpcId,
    ) -> Result<LiveCaps, BusError> {
        // Honest caps: ClientCapabilities::HOST is all false (no stub
        // fs/terminal handlers). Pep's rule — nothing goes true in this cut.
        let init = acp::encode_initialize(rpc_id, ClientCapabilities::HOST);
        self.write_value_ungated(&init)?;
        let result = self.wait_rpc_result(transport, rpc_id)?;
        self.initialized = true;
        Ok(LiveCaps::from_initialize_result(&result))
    }

    /// After [`RemintSession::remint_and_attach`]: flush `old_wire_replies`
    /// on THIS (old) wire, then write SessionClose / SessionCancel.
    /// AttachResume is left for the NEW wire. Dead-wire writes drop and
    /// never block remint.
    ///
    /// Thin runner adapter over [`flush_and_teardown_old_wire`] — one
    /// [`OldWireWrite`] sink so both flush and teardown share a single
    /// `&mut self` borrow (fixes the dual-closure residual left by #23).
    pub fn flush_and_teardown_after_remint(
        &mut self,
        outcome: RemintOutcome,
        next_rpc_id: &mut u64,
    ) -> OldWireFlush {
        remint::flush_and_teardown_old_wire(
            outcome,
            |w| match w {
                remint::OldWireWrite::Reply(cmd) => self.write_command(&cmd),
                remint::OldWireWrite::Rpc(rpc) => self.write_value(&rpc),
            },
            next_rpc_id,
        )
    }

    /// After OLD-wire [`Self::flush_and_teardown_after_remint`] returned
    /// `needs_attach_resume`, land AttachResume on THIS (NEW) Binding/wire.
    ///
    /// Order is load-bearing:
    /// 1. [`Self::initialize_host`] — `ClientCapabilities::HOST` all false
    /// 2. [`RemintSession::set_live_caps`] from that live result
    /// 3. If LiveCaps advertise resume → write ACP `session/resume` with the
    ///    same agent session id; else fail-closed (typed [`RemintError`], no
    ///    silent skip, never `session/load` / `session/new`).
    ///
    /// AttachResume must not be written on the old wire (already torn down).
    pub fn attach_resume_on_new_wire(
        &mut self,
        transport: &mut RunnerTransport,
        session: &RemintSession,
        agent_session_id: &str,
        init_rpc_id: &RpcId,
        next_rpc_id: &mut u64,
    ) -> Result<LiveCaps, RemintError> {
        let caps = self
            .initialize_host(transport, init_rpc_id)
            .map_err(|e| RemintError::NewWire(format!("initialize_host: {e}")))?;
        remint::write_new_wire_attach_resume(
            session,
            agent_session_id,
            caps,
            |rpc| self.write_value(&rpc),
            next_rpc_id,
        )?;
        Ok(caps)
    }

    /// Pump until a JSON-RPC response (result or error) with `rpc_id` arrives.
    fn wait_rpc_result(
        &mut self,
        transport: &mut RunnerTransport,
        rpc_id: &RpcId,
    ) -> Result<serde_json::Value, BusError> {
        // Fake children exhaust a finite stdout; real children are bounded
        // by READ_STDOUT_TIMEOUT per pump.
        let max_pumps =
            (WAIT_RPC_RESULT_TIMEOUT.as_millis() / READ_STDOUT_TIMEOUT.as_millis()) as usize;
        for _ in 0..max_pumps {
            self.pump(transport)?;
            loop {
                let frame = match transport {
                    RunnerTransport::Jsonl(t) => t.try_recv_frame()?,
                    RunnerTransport::JsonRpc(t) => t.try_recv_frame()?,
                    RunnerTransport::Unsupported(w) => {
                        return Err(BusError::Transport(format!(
                            "AttachRunner does not pump {} yet",
                            w.as_str()
                        )));
                    }
                };
                let Some(frame) = frame else {
                    break;
                };
                let Some(v) = frame.as_json() else {
                    continue;
                };
                if v.get("method").is_some() {
                    // Agent->client request mid-wait: answer -32601 so the
                    // agent is never left hanging. Notifications are skipped.
                    if let Some(id) = v.get("id") {
                        let reply = serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": METHOD_NOT_FOUND,
                                "message": "client not ready: initialize in progress"
                            }
                        });
                        self.write_value_ungated(&reply)?;
                    }
                    continue;
                }
                let Some(id_v) = v.get("id") else {
                    continue;
                };
                let Some(got) = RpcId::from_json(id_v) else {
                    continue;
                };
                if &got != rpc_id {
                    continue;
                }
                if let Some(err) = v.get("error") {
                    return Err(BusError::Transport(format!("initialize error: {err}")));
                }
                if let Some(result) = v.get("result") {
                    return Ok(result.clone());
                }
                return Err(BusError::Transport(
                    "initialize response missing result".into(),
                ));
            }
            if self.try_wait()?.is_some() {
                break;
            }
        }
        Err(BusError::Transport(
            "initialize timed out waiting for result".into(),
        ))
    }
}

/// Inputs for [`host_remint_two_step`] besides the OLD/NEW runners and
/// NEW-wire transport (keeps the Host entry arity small).
pub struct HostRemintArgs<'a> {
    pub session: &'a RemintSession,
    pub outcome: RemintOutcome,
    pub init_rpc_id: &'a RpcId,
    pub old_next_rpc_id: &'a mut u64,
    pub new_next_rpc_id: &'a mut u64,
}

/// Result of [`host_remint_two_step`]: OLD-wire flush plus NEW-wire LiveCaps
/// when AttachResume was owed and landed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRemintWires {
    pub flush: OldWireFlush,
    /// `Some` when `flush.needs_attach_resume` and NEW-wire attach_resume
    /// succeeded. `None` when AttachResume was not owed.
    pub live_caps: Option<LiveCaps>,
}

/// Host remint production path after [`RemintSession::remint_and_attach`].
///
/// Order is load-bearing (never resume on the old wire):
/// 1. [`AttachRunner::flush_and_teardown_after_remint`] on the OLD Binding
/// 2. When `needs_attach_resume`, [`AttachRunner::attach_resume_on_new_wire`]
///    on the NEW Binding (same agent session id)
///
/// LiveCaps fail-closed stays inside attach_resume. Never `session/load` or
/// `session/new`. [`ClientCapabilities::HOST`] stays all false.
///
/// This is the Host orchestration that must compose the two runner steps —
/// callers must not write AttachResume on the old wire or skip the NEW-wire
/// resume when `needs_attach_resume` is set.
pub fn host_remint_two_step(
    old: &mut AttachRunner,
    new: &mut AttachRunner,
    new_transport: &mut RunnerTransport,
    args: HostRemintArgs<'_>,
) -> Result<HostRemintWires, RemintError> {
    let HostRemintArgs {
        session,
        outcome,
        init_rpc_id,
        old_next_rpc_id,
        new_next_rpc_id,
    } = args;
    let agent_session_id = outcome.agent_session_id.clone();
    let flush = old.flush_and_teardown_after_remint(outcome, old_next_rpc_id);
    if !flush.needs_attach_resume {
        return Ok(HostRemintWires {
            flush,
            live_caps: None,
        });
    }
    let live_caps = new.attach_resume_on_new_wire(
        new_transport,
        session,
        &agent_session_id,
        init_rpc_id,
        new_next_rpc_id,
    )?;
    Ok(HostRemintWires {
        flush,
        live_caps: Some(live_caps),
    })
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
    use crate::adapters::acp::ClientCapabilities;
    use crate::adapters::AdapterCodec;
    use crate::catalog::{lookup_profile, LaunchSpec};
    use crate::transport::{Transport, WireFrame};
    use crate::RpcId;
    use crate::{AgentEvent, CodingAgent, FramedAgent};
    use remint::{
        flush_and_teardown_old_wire, write_new_wire_attach_resume, LiveCaps, OldWireWrite,
        RemintError, RemintEventKind, RemintSession, RemintWireMethod, WireAction,
    };

    /// Fake JSONL child that answers `initialize` (id 0) first, then emits
    /// `rest`. Returned runner has completed the real handshake; keep
    /// pumping into the returned transport (it may hold buffered bytes).
    fn initialized_fake_with_transport(rest: Vec<u8>) -> (AttachRunner, RunnerTransport) {
        let mut stdout = JsonlRpcTransport::encode_jsonl(&serde_json::json!({
            "jsonrpc": "2.0", "id": 0, "result": { "protocolVersion": 1 }
        }))
        .unwrap();
        stdout.extend(rest);
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, stdout);
        let mut t = transport_for_wire(WireKind::JsonlRpc);
        runner.initialize_host(&mut t, &RpcId::Num(0)).unwrap();
        (runner, t)
    }

    fn initialized_fake(rest: Vec<u8>) -> AttachRunner {
        initialized_fake_with_transport(rest).0
    }

    /// Outbound frames after the leading `initialize`.
    fn frames_after_initialize(runner: &AttachRunner) -> Vec<serde_json::Value> {
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let mut frames: Vec<serde_json::Value> = log
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(frames.remove(0)["method"], "initialize");
        frames
    }

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
        let mut runner = initialized_fake(Vec::new());
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
        let (mut runner, mut transport) = initialized_fake_with_transport(
            JsonlRpcTransport::encode_jsonl(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": { "sessionId": "S-stable" }
            }))
            .unwrap(),
        );
        runner.write_jsonl(&resumed).unwrap();
        let t = transport.as_jsonl_mut().unwrap();
        let mut saw = false;
        for _ in 0..8 {
            runner.pump_jsonl(t).unwrap();
            if let Some(WireFrame::Json(v)) = t.try_recv_frame().unwrap() {
                assert_eq!(v["result"]["sessionId"], "S-stable");
                saw = true;
                break;
            }
        }
        assert!(saw, "resume result must arrive");
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
        let mut runner = initialized_fake(Vec::new());
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
            "id": 3,
            "params": {
                "sessionId": "s1",
                "toolCall": {"toolCallId": "c1", "title": "Allow?"},
                "options": [{"optionId": "allow", "name": "Allow", "kind": "allow_once"}]
            }
        });
        agent
            .transport_mut()
            .push_bytes(&JsonlRpcTransport::encode_jsonl(&raw).unwrap());
        let ev = agent.try_recv().unwrap().unwrap();
        assert!(matches!(
            ev,
            AgentEvent::PermissionAsk {
                rpc_id: Some(crate::RpcId::Num(3)),
                ..
            }
        ));
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

    /// Pep Accept (1): live AttachRunner actually SENDS initialize with
    /// ClientCapabilities::HOST (all false) and waits for the reply before
    /// any agent-bound RPC is written.
    #[test]
    fn live_attach_runner_sends_initialize_host_before_agent_bound() {
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "session": { "resume": true, "cancel": true, "close": true }
                }
            }
        });
        let stdout = JsonlRpcTransport::encode_jsonl(&init_result).unwrap();
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, stdout);
        let mut transport = transport_for_wire(WireKind::JsonlRpc);

        let caps = runner
            .initialize_host(&mut transport, &RpcId::Num(1))
            .expect("initialize result");
        assert!(caps.resume && caps.cancel && caps.close);

        // Outbound frame: exactly initialize with HOST caps (all false).
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let outbound: Vec<serde_json::Value> = log
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(outbound.len(), 1, "only initialize so far: {outbound:?}");
        assert_eq!(outbound[0]["method"], "initialize");
        assert_eq!(outbound[0]["id"], 1);
        assert_eq!(
            outbound[0]["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
        assert_eq!(
            outbound[0]["params"]["clientCapabilities"],
            serde_json::json!({
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false
            })
        );

        // After the reply lands, agent-bound traffic may proceed — and must
        // appear AFTER initialize on the wire.
        let prompt = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/prompt",
            "params": {
                "sessionId": "S1",
                "prompt": [{"type": "text", "text": "hi"}]
            }
        });
        runner.write_jsonl(&prompt).unwrap();
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let methods: Vec<String> = log
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                v["method"].as_str().unwrap_or("").to_string()
            })
            .collect();
        assert_eq!(
            methods,
            vec!["initialize".to_string(), "session/prompt".to_string()]
        );
        assert_eq!(
            ClientCapabilities::HOST,
            ClientCapabilities {
                read_text_file: false,
                write_text_file: false,
                terminal: false,
            }
        );
    }

    /// Pep Accept (2): remint production path flushes old_wire_replies
    /// BEFORE SessionClose / SessionCancel on the old wire.
    #[test]
    fn remint_flush_old_wire_replies_before_session_teardown() {
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
        // Advertise read so an in-flight request is tracked (test-only).
        session.set_client_capabilities(crate::adapters::acp::ClientCapabilities {
            read_text_file: true,
            ..ClientCapabilities::HOST
        });
        let ask = crate::AgentEvent::Native {
            agent: crate::AgentKind::Acp,
            method: "fs/read_text_file".into(),
            payload: serde_json::json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "fs/read_text_file",
                "params": {"path": "/tmp/x"}
            }),
        };
        assert!(session.on_client_request(&ask).unwrap().is_none());
        assert_eq!(session.pending_client_requests().len(), 1);

        session.begin_turn("mid");
        let out = session.remint_and_attach().unwrap();
        assert_eq!(out.old_wire_replies.len(), 1);
        assert!(
            out.wire_actions.contains(&WireAction::SessionClose)
                || out.wire_actions.contains(&WireAction::SessionCancel)
        );
        assert!(out.wire_actions.contains(&WireAction::AttachResume));

        let mut runner = initialized_fake(Vec::new());
        let mut next_id = 10u64;
        let flush = runner.flush_and_teardown_after_remint(out, &mut next_id);
        assert_eq!(flush.dropped, 0);
        assert!(flush.needs_attach_resume);
        assert!(flush.teardown_failed.is_empty());

        // Byte order: the -32800 reply bytes precede every teardown method.
        let raw = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let reply_at = raw.find("-32800").expect("flush reply written");
        for m in ["session/close", "session/cancel"] {
            if let Some(at) = raw.find(m) {
                assert!(reply_at < at, "flush bytes must precede {m}: {raw}");
            }
        }

        let frames = frames_after_initialize(&runner);
        // Order: -32800 error reply first, then session/close or session/cancel.
        // AttachResume must NOT appear on the old wire.
        assert!(frames.len() >= 2, "expected reply + teardown: {frames:?}");
        assert!(
            frames[0].get("error").is_some(),
            "first frame must be old-wire reply: {frames:?}"
        );
        assert_eq!(frames[0]["error"]["code"], -32800);
        assert_eq!(frames[0]["id"], 7);
        let teardown_method = frames[1]["method"].as_str().unwrap_or("");
        assert!(
            teardown_method == "session/close" || teardown_method == "session/cancel",
            "second frame must be teardown, got {teardown_method}: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/resume")),
            "AttachResume must not go on the old wire: {frames:?}"
        );
        assert!(
            frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/new")
                    && f["method"].as_str() != Some("session/load")),
            "never session/new or session/load: {frames:?}"
        );
    }

    /// Dead old wire: flush drops replies, remint path still succeeds, and
    /// nothing is retried on a new Binding's runner.
    #[test]
    fn remint_dead_old_wire_flush_does_not_block() {
        let (session, _, _) = RemintSession::open(
            "sess-1",
            "S-stable",
            LiveCaps {
                resume: true,
                cancel: true,
                close: false,
                load_session: false,
            },
        );
        session.set_client_capabilities(crate::adapters::acp::ClientCapabilities {
            read_text_file: true,
            ..ClientCapabilities::HOST
        });
        let ask = crate::AgentEvent::Native {
            agent: crate::AgentKind::Acp,
            method: "fs/read_text_file".into(),
            payload: serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "fs/read_text_file",
                "params": {}
            }),
        };
        assert!(session.on_client_request(&ask).unwrap().is_none());
        session.begin_turn("mid");
        let out = session.remint_and_attach().expect("remint succeeds");
        assert_eq!(out.old_wire_replies.len(), 1);

        let mut attempts = 0usize;
        let mut next_id = 1u64;
        let flush = flush_and_teardown_old_wire(
            out,
            |w| {
                if matches!(w, OldWireWrite::Reply(_)) {
                    attempts += 1;
                }
                Err(BusError::Transport("old wire dead".into()))
            },
            &mut next_id,
        );
        assert_eq!(attempts, 1, "one try then drop");
        assert_eq!(flush.dropped, 1);
        assert_eq!(flush.teardown_failed, vec![RemintWireMethod::SessionCancel]);
        assert!(flush.needs_attach_resume);

        // New Binding runner must not see the dropped reply.
        let new_runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, Vec::new());
        assert!(new_runner.stdin_log().unwrap().is_empty());
    }

    /// session/request_permission still routes to pending (regression).
    #[test]
    fn live_attach_permission_ask_still_pending_after_initialize() {
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "protocolVersion": 1, "agentCapabilities": {} }
        });
        let perm = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "session/request_permission",
            "params": {
                "sessionId": "s1",
                "toolCall": {"toolCallId": "c1", "title": "Allow?"},
                "options": [{"optionId": "allow", "name": "Allow", "kind": "allow_once"}]
            }
        });
        let mut stdout = JsonlRpcTransport::encode_jsonl(&init_result).unwrap();
        stdout.extend(JsonlRpcTransport::encode_jsonl(&perm).unwrap());
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, stdout);
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let _ = runner
            .initialize_host(&mut transport, &RpcId::Num(1))
            .expect("init");

        // Pump the permission ask that arrived after initialize.
        let mut saw_perm = false;
        for _ in 0..16 {
            runner.pump(&mut transport).unwrap();
            let t = transport.as_jsonl_mut().unwrap();
            while let Some(frame) = t.try_recv_frame().unwrap() {
                let ev = AcpCodec.decode_event(&frame).unwrap().unwrap();
                if matches!(
                    ev,
                    crate::AgentEvent::PermissionAsk {
                        rpc_id: Some(RpcId::Num(9)),
                        ..
                    }
                ) {
                    saw_perm = true;
                }
            }
            if saw_perm || runner.try_wait().unwrap().is_some() {
                break;
            }
        }
        assert!(
            saw_perm,
            "permission ask must still decode after initialize"
        );
        // HOST caps unchanged — outbound initialize still all-false.
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let init: serde_json::Value = serde_json::from_str(log.lines().next().unwrap()).unwrap();
        assert_eq!(
            init["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
    }
    /// Init gate: before initialize_host returns Ok, every agent-bound
    /// write fails typed; the fake's stdin sees only `initialize`.
    #[test]
    fn agent_bound_writes_rejected_until_initialized() {
        let prompt = serde_json::json!({
            "jsonrpc": "2.0", "id": 2, "method": "session/prompt",
            "params": {"sessionId": "S1", "prompt": []}
        });
        let init_ok = JsonlRpcTransport::encode_jsonl(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "result": { "protocolVersion": 1 }
        }))
        .unwrap();
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, init_ok);
        assert!(matches!(
            runner.write_value(&prompt),
            Err(BusError::NotInitialized)
        ));
        assert!(matches!(
            runner.write_jsonl(&prompt),
            Err(BusError::NotInitialized)
        ));
        assert!(matches!(
            runner.write_bytes(b"{}\n"),
            Err(BusError::NotInitialized)
        ));
        assert!(matches!(
            runner.write_command(&AgentCommand::UserMessage {
                session_id: "S1".into(),
                text: "hi".into(),
            }),
            Err(BusError::NotInitialized)
        ));
        assert!(runner.stdin_log().unwrap().is_empty());

        let mut t = transport_for_wire(WireKind::JsonlRpc);
        runner.initialize_host(&mut t, &RpcId::Num(1)).unwrap();
        let log = std::str::from_utf8(runner.stdin_log().unwrap()).unwrap();
        let methods: Vec<serde_json::Value> = log
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap()["method"].clone())
            .collect();
        assert_eq!(methods, vec![serde_json::json!("initialize")]);
        assert!(runner.is_initialized());
        runner.write_value(&prompt).unwrap();
    }

    /// Failed initialize keeps the gate closed.
    #[test]
    fn initialize_error_keeps_gate_closed() {
        let err = JsonlRpcTransport::encode_jsonl(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "error": {"code": -32603, "message": "boom"}
        }))
        .unwrap();
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, err);
        let mut t = transport_for_wire(WireKind::JsonlRpc);
        assert!(runner.initialize_host(&mut t, &RpcId::Num(1)).is_err());
        assert!(matches!(
            runner.write_value(&serde_json::json!({"method": "session/prompt"})),
            Err(BusError::NotInitialized)
        ));
    }

    /// Agent->client request mid-initialize is answered -32601, not dropped.
    #[test]
    fn wait_rpc_result_answers_mid_wait_agent_request() {
        let mut stdout = JsonlRpcTransport::encode_jsonl(&serde_json::json!({
            "jsonrpc": "2.0", "id": "ask-1", "method": "fs/read_text_file",
            "params": {"path": "/tmp/x"}
        }))
        .unwrap();
        stdout.extend(
            JsonlRpcTransport::encode_jsonl(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "result": { "protocolVersion": 1 }
            }))
            .unwrap(),
        );
        let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, stdout);
        let mut t = transport_for_wire(WireKind::JsonlRpc);
        runner.initialize_host(&mut t, &RpcId::Num(1)).unwrap();
        let frames = frames_after_initialize(&runner);
        assert_eq!(frames.len(), 1, "{frames:?}");
        assert_eq!(frames[0]["id"], "ask-1");
        assert_eq!(frames[0]["error"]["code"], METHOD_NOT_FOUND);
    }

    /// Old-wire teardown write failures are reported, not swallowed.
    #[test]
    fn runner_flush_reports_teardown_write_failure() {
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
        session.begin_turn("mid");
        let out = session.remint_and_attach().unwrap();
        let expected: Vec<RemintWireMethod> = out
            .wire_actions
            .iter()
            .filter_map(|a| match a {
                WireAction::SessionCancel => Some(RemintWireMethod::SessionCancel),
                WireAction::SessionClose => Some(RemintWireMethod::SessionClose),
                _ => None,
            })
            .collect();
        assert!(!expected.is_empty());
        let mut runner = initialized_fake(Vec::new());
        runner.kill().unwrap();
        let mut next_id = 1u64;
        let flush = runner.flush_and_teardown_after_remint(out, &mut next_id);
        assert_eq!(flush.teardown_failed, expected);
        assert!(flush.needs_attach_resume);
    }

    /// Pep Accept (Oct 14): after remint flush+teardown on the OLD wire,
    /// AttachResume lands on the NEW wire — initialize_host (HOST all-false)
    /// → set_live_caps → session/resume with the same agent session id.
    #[test]
    fn remint_attach_resume_on_new_wire_after_teardown() {
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
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        let agent_sid = out.agent_session_id.clone();

        // OLD wire: flush + teardown; AttachResume must not appear here.
        let mut old_runner = initialized_fake(Vec::new());
        let mut old_next = 10u64;
        let flush = old_runner.flush_and_teardown_after_remint(out, &mut old_next);
        assert!(flush.needs_attach_resume);
        let old_frames = frames_after_initialize(&old_runner);
        assert!(
            old_frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/resume")),
            "AttachResume must not go on the old wire: {old_frames:?}"
        );

        // NEW wire: initialize advertises resume, then session/resume.
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "session": { "resume": true, "cancel": true, "close": true }
                }
            }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut new_next = 2u64;
        let caps = new_runner
            .attach_resume_on_new_wire(
                &mut transport,
                &session,
                &agent_sid,
                &RpcId::Num(1),
                &mut new_next,
            )
            .expect("attach resume on new wire");
        assert!(caps.resume);
        assert!(session.live_caps().resume);
        assert_eq!(new_next, 3);

        let log = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        let frames: Vec<serde_json::Value> = log
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            frames.len(),
            2,
            "initialize then session/resume: {frames:?}"
        );
        assert_eq!(frames[0]["method"], "initialize");
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            serde_json::json!({
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false
            })
        );
        assert_eq!(frames[1]["method"], "session/resume");
        assert_eq!(frames[1]["params"]["sessionId"], agent_sid);
        assert_eq!(frames[1]["id"], 2);
        assert!(
            frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/new")
                    && f["method"].as_str() != Some("session/load")),
            "never session/new or session/load: {frames:?}"
        );
    }

    /// NEW-wire live caps without resume → fail-closed; no session/resume
    /// written; never silent skip that pretends resume happened.
    #[test]
    fn remint_attach_resume_fail_closed_when_new_wire_lacks_resume() {
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
        let out = session.remint_and_attach().unwrap();
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        let agent_sid = out.agent_session_id.clone();

        let mut old_runner = initialized_fake(Vec::new());
        let mut old_next = 1u64;
        let flush = old_runner.flush_and_teardown_after_remint(out, &mut old_next);
        assert!(flush.needs_attach_resume);

        // NEW wire initialize: loadSession only (Copilot shape) — no resume.
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": { "loadSession": true }
            }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut new_next = 2u64;
        let err = new_runner
            .attach_resume_on_new_wire(
                &mut transport,
                &session,
                &agent_sid,
                &RpcId::Num(1),
                &mut new_next,
            )
            .unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        assert!(session.live_caps().load_session);
        assert!(!session.live_caps().resume);
        assert!(session
            .observe()
            .iter()
            .any(|e| { e.kind == RemintEventKind::FailClosed && e.text == "LoadFallbackBlocked" }));

        let log = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        let frames: Vec<serde_json::Value> = log
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(frames.len(), 1, "only initialize, no resume: {frames:?}");
        assert_eq!(frames[0]["method"], "initialize");
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
        assert!(!log.contains("session/resume"));
        assert!(!log.contains("session/load"));
        assert!(!log.contains("session/new"));
        assert_eq!(
            new_next, 2,
            "rpc id must not advance when resume not written"
        );
    }

    /// Resume-not-advertised (no loadSession either) → ResumeNotSupported.
    #[test]
    fn remint_attach_resume_fail_closed_resume_not_supported() {
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
        let out = session.remint_and_attach().unwrap();
        let agent_sid = out.agent_session_id.clone();
        let mut old_runner = initialized_fake(Vec::new());
        let mut old_next = 1u64;
        assert!(
            old_runner
                .flush_and_teardown_after_remint(out, &mut old_next)
                .needs_attach_resume
        );

        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "protocolVersion": 1, "agentCapabilities": {} }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut new_next = 5u64;
        let err = new_runner
            .attach_resume_on_new_wire(
                &mut transport,
                &session,
                &agent_sid,
                &RpcId::Num(1),
                &mut new_next,
            )
            .unwrap_err();
        assert_eq!(err, RemintError::ResumeNotSupported);
        assert!(session
            .observe()
            .iter()
            .any(|e| { e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported" }));
        let log = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        assert!(!log.contains("session/resume"));
        assert!(!log.contains("session/load"));
    }

    /// Ordering: OLD flush/teardown completes before NEW-wire resume;
    /// free helper write_new_wire_attach_resume encodes session/resume only.
    #[test]
    fn remint_old_teardown_before_new_wire_resume_ordering() {
        let (session, _, _) = RemintSession::open(
            "sess-1",
            "S-keep",
            LiveCaps {
                resume: true,
                cancel: true,
                close: true,
                load_session: false,
            },
        );
        session.begin_turn("mid");
        let out = session.remint_and_attach().unwrap();
        assert!(
            out.wire_actions.contains(&WireAction::SessionClose)
                || out.wire_actions.contains(&WireAction::SessionCancel)
        );
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        let agent_sid = out.agent_session_id.clone();

        let mut old_runner = initialized_fake(Vec::new());
        let mut old_next = 20u64;
        let flush = old_runner.flush_and_teardown_after_remint(out, &mut old_next);
        assert!(flush.needs_attach_resume);
        let old_raw = std::str::from_utf8(old_runner.stdin_log().unwrap()).unwrap();
        // Teardown methods present; resume absent on old wire.
        assert!(
            old_raw.contains("session/close") || old_raw.contains("session/cancel"),
            "old wire must teardown: {old_raw}"
        );
        assert!(!old_raw.contains("session/resume"));

        // Free helper (caps already known): writes resume only.
        let mut written: Vec<serde_json::Value> = Vec::new();
        let mut next = 30u64;
        write_new_wire_attach_resume(
            &session,
            &agent_sid,
            LiveCaps {
                resume: true,
                cancel: true,
                close: true,
                load_session: false,
            },
            |rpc| {
                written.push(rpc);
                Ok(())
            },
            &mut next,
        )
        .unwrap();
        assert_eq!(written.len(), 1);
        assert_eq!(written[0]["method"], "session/resume");
        assert_eq!(written[0]["params"]["sessionId"], "S-keep");
        assert_eq!(written[0]["id"], 30);
        assert_eq!(next, 31);
        assert!(session.live_caps().resume);
    }

    /// Host two-step: flush+teardown OLD then attach_resume NEW via
    /// [`host_remint_two_step`] — ordering + same agent session id; never
    /// resume on old wire; never session/load or session/new.
    #[test]
    fn host_remint_two_step_composes_flush_then_attach_resume() {
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
        session.begin_turn("mid");
        let out = session.remint_and_attach().unwrap();
        assert!(
            out.wire_actions.contains(&WireAction::SessionClose)
                || out.wire_actions.contains(&WireAction::SessionCancel)
        );
        assert!(out.wire_actions.contains(&WireAction::AttachResume));
        let agent_sid = out.agent_session_id.clone();

        let mut old_runner = initialized_fake(Vec::new());
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": {
                    "session": { "resume": true, "cancel": true, "close": true }
                }
            }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut old_next = 10u64;
        let mut new_next = 2u64;

        let wires = host_remint_two_step(
            &mut old_runner,
            &mut new_runner,
            &mut transport,
            HostRemintArgs {
                session: &session,
                outcome: out,
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
        .expect("host remint two-step");
        assert!(wires.flush.needs_attach_resume);
        let caps = wires.live_caps.expect("attach resume owed");
        assert!(caps.resume);
        assert!(session.live_caps().resume);

        // OLD: teardown present, resume absent.
        let old_raw = std::str::from_utf8(old_runner.stdin_log().unwrap()).unwrap();
        assert!(
            old_raw.contains("session/close") || old_raw.contains("session/cancel"),
            "old wire must teardown: {old_raw}"
        );
        assert!(!old_raw.contains("session/resume"));

        // NEW: initialize (HOST all-false) then session/resume same id.
        let new_raw = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        let frames: Vec<serde_json::Value> = new_raw
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(frames.len(), 2, "initialize then resume: {frames:?}");
        assert_eq!(frames[0]["method"], "initialize");
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
        assert_eq!(frames[1]["method"], "session/resume");
        assert_eq!(frames[1]["params"]["sessionId"], agent_sid);
        assert!(
            frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/new")
                    && f["method"].as_str() != Some("session/load")),
            "never session/new or session/load: {frames:?}"
        );
        // OLD teardown bytes precede NEW resume in wall-clock: Host calls
        // flush on old before attach_resume on new (composition order).
        assert_eq!(new_next, 3);
    }

    /// Host two-step fail-closed: NEW wire lacks resume → typed error; no
    /// load/new fallback; old wire still tore down without resume.
    #[test]
    fn host_remint_two_step_fail_closed_no_load_new_fallback() {
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
        let out = session.remint_and_attach().unwrap();
        assert!(out.wire_actions.contains(&WireAction::AttachResume));

        let mut old_runner = initialized_fake(Vec::new());
        // Copilot shape: loadSession only — no resume.
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "protocolVersion": 1,
                "agentCapabilities": { "loadSession": true }
            }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut old_next = 1u64;
        let mut new_next = 2u64;

        let err = host_remint_two_step(
            &mut old_runner,
            &mut new_runner,
            &mut transport,
            HostRemintArgs {
                session: &session,
                outcome: out,
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
        .unwrap_err();
        assert_eq!(err, RemintError::LoadFallbackBlocked);
        assert!(session
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "LoadFallbackBlocked"));

        let old_raw = std::str::from_utf8(old_runner.stdin_log().unwrap()).unwrap();
        assert!(!old_raw.contains("session/resume"));

        let new_raw = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        assert!(new_raw.contains("initialize"));
        assert!(!new_raw.contains("session/resume"));
        assert!(!new_raw.contains("session/load"));
        assert!(!new_raw.contains("session/new"));
        assert_eq!(
            new_next, 2,
            "rpc id must not advance when resume not written"
        );
    }

    /// Host two-step: empty NEW caps → ResumeNotSupported; still no load/new.
    #[test]
    fn host_remint_two_step_fail_closed_resume_not_supported() {
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
        let out = session.remint_and_attach().unwrap();

        let mut old_runner = initialized_fake(Vec::new());
        let init_result = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "protocolVersion": 1, "agentCapabilities": {} }
        });
        let mut new_runner = AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init_result).unwrap(),
        );
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut old_next = 1u64;
        let mut new_next = 5u64;

        let err = host_remint_two_step(
            &mut old_runner,
            &mut new_runner,
            &mut transport,
            HostRemintArgs {
                session: &session,
                outcome: out,
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
        .unwrap_err();
        assert_eq!(err, RemintError::ResumeNotSupported);
        let new_raw = std::str::from_utf8(new_runner.stdin_log().unwrap()).unwrap();
        assert!(!new_raw.contains("session/resume"));
        assert!(!new_raw.contains("session/load"));
        assert!(!new_raw.contains("session/new"));
    }
}
