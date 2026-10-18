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
    ///
    /// [`StdoutRead::Eof`] yields `Ok(0)` here (no frames to push). Wake
    /// callers that need the loss proof must use [`Self::probe_stdout`]
    /// or map [`Self::read_stdout`] via [`old_wire_from_stdout`].
    pub fn pump_jsonl(&mut self, t: &mut JsonlRpcTransport) -> Result<usize, BusError> {
        match self.read_stdout()? {
            StdoutRead::Bytes(chunk) => {
                let n = chunk.len();
                t.push_bytes(&chunk);
                Ok(n)
            }
            StdoutRead::Empty | StdoutRead::Eof => Ok(0),
        }
    }

    /// Read available stdout bytes into a Content-Length JSON-RPC transport.
    pub fn pump_jsonrpc(&mut self, t: &mut JsonRpcTransport) -> Result<usize, BusError> {
        match self.read_stdout()? {
            StdoutRead::Bytes(chunk) => {
                let n = chunk.len();
                t.push_bytes(&chunk)?;
                Ok(n)
            }
            StdoutRead::Empty | StdoutRead::Eof => Ok(0),
        }
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

    /// One Real/Fake stdout read. Distinguishes real EOF (`Ok(0)` after
    /// poll-readable / POLLHUP) from timeout / WouldBlock empty so wake can
    /// surface [`OldWire::StdoutEof`] without treating idle as lost.
    pub fn read_stdout(&mut self) -> Result<StdoutRead, BusError> {
        match &mut self.io {
            ChildIo::Real { stdout, .. } => {
                let Some(out) = stdout.as_mut() else {
                    // Handle already gone — wire closed.
                    return Ok(StdoutRead::Eof);
                };
                if !poll_readable(out.as_raw_fd(), READ_STDOUT_TIMEOUT)? {
                    // Timeout / no data — NOT proof of loss.
                    return Ok(StdoutRead::Empty);
                }
                let mut buf = [0u8; 4096];
                match out.read(&mut buf) {
                    // Poll said readable (incl. POLLHUP) but no bytes: EOF.
                    Ok(0) => Ok(StdoutRead::Eof),
                    Ok(n) => Ok(StdoutRead::Bytes(buf[..n].to_vec())),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(StdoutRead::Empty),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => Ok(StdoutRead::Empty),
                    Err(e) => Err(BusError::Transport(format!("read stdout: {e}"))),
                }
            }
            ChildIo::Fake { stdout, pos, .. } => {
                // Fake queue exhaust is Empty, not Eof — try_wait owns the
                // Exited proof when the buffer drains. Real Ok(0) is the only
                // StdoutEof source from I/O. Live attach may still write after
                // initialize returns.
                if *pos >= stdout.len() {
                    return Ok(StdoutRead::Empty);
                }
                // Chunked reads exercise partial-frame buffering.
                let end = (*pos + 64).min(stdout.len());
                let chunk = stdout[*pos..end].to_vec();
                *pos = end;
                Ok(StdoutRead::Bytes(chunk))
            }
        }
    }

    /// Probe stdout once for wake: [`StdoutRead::Eof`] → [`OldWire::StdoutEof`],
    /// timeout/WouldBlock empty → [`OldWire::Unknown`], bytes → [`OldWire::LiveIdle`]
    /// (caller with turn state may upgrade to [`OldWire::LiveInTurn`]).
    ///
    /// Bytes returned here are consumed from the pipe; prefer
    /// [`Self::read_stdout`] + push when also pumping the transport.
    pub fn probe_stdout(&mut self) -> Result<OldWire, BusError> {
        self.read_stdout().map(|r| old_wire_from_stdout(&r))
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
    #[allow(clippy::too_many_arguments)]
    pub fn attach_resume_on_new_wire(
        &mut self,
        transport: &mut RunnerTransport,
        session: &RemintSession,
        agent_session_id: &str,
        place: Option<&hearth::Place>,
        mcp_servers: &[serde_json::Value],
        init_rpc_id: &RpcId,
        next_rpc_id: &mut u64,
    ) -> Result<LiveCaps, RemintError> {
        let caps = self
            .initialize_host(transport, init_rpc_id)
            .map_err(|e| RemintError::NewWire(format!("initialize_host: {e}")))?;
        remint::write_new_wire_attach_resume(
            session,
            agent_session_id,
            place,
            mcp_servers,
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
    /// Session's LocalDir Place → ACP `cwd` on `session/resume` (absolute).
    pub place: Option<&'a hearth::Place>,
    /// Session config `mcpServers` (`[]` if none) → ACP `mcpServers`.
    pub mcp_servers: &'a [serde_json::Value],
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

/// Host remint wire steps after [`RemintSession::remint_and_attach`].
///
/// Prefer [`host_remint_full_path`] in production (it owns remint_and_attach
/// → this two-step). Order is load-bearing (never resume on the old wire):
/// 1. [`AttachRunner::flush_and_teardown_after_remint`] on the OLD Binding
/// 2. When `needs_attach_resume`, [`AttachRunner::attach_resume_on_new_wire`]
///    on the NEW Binding (same agent session id)
///
/// LiveCaps fail-closed stays inside attach_resume. Never `session/load` or
/// `session/new`. [`ClientCapabilities::HOST`] stays all false.
///
/// Callers must not write AttachResume on the old wire or skip the NEW-wire
/// resume when `needs_attach_resume` is set.
pub fn host_remint_two_step(
    old: &mut AttachRunner,
    new: &mut AttachRunner,
    new_transport: &mut RunnerTransport,
    args: HostRemintArgs<'_>,
) -> Result<HostRemintWires, RemintError> {
    let HostRemintArgs {
        session,
        place,
        mcp_servers,
        outcome,
        init_rpc_id,
        old_next_rpc_id,
        new_next_rpc_id,
    } = args;
    // Before OLD teardown / NEW initialize.
    remint::validate_resume_place(place)?;
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
        place,
        mcp_servers,
        init_rpc_id,
        new_next_rpc_id,
    )?;
    Ok(HostRemintWires {
        flush,
        live_caps: Some(live_caps),
    })
}

/// Inputs for [`host_remint_full_path`] (session + rpc ids; runners stay
/// separate args so OLD/NEW wires stay explicit).
pub struct HostRemintFullArgs<'a> {
    pub session: &'a RemintSession,
    /// Session's LocalDir Place → ACP `cwd` on `session/resume` (absolute).
    pub place: Option<&'a hearth::Place>,
    /// Session config `mcpServers` (`[]` if none) → ACP `mcpServers`.
    pub mcp_servers: &'a [serde_json::Value],
    pub init_rpc_id: &'a RpcId,
    pub old_next_rpc_id: &'a mut u64,
    pub new_next_rpc_id: &'a mut u64,
}

/// Result of [`host_remint_full_path`]: Binding remint outcome plus the
/// OLD/NEW wire steps from [`host_remint_two_step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostRemintFullPath {
    pub outcome: RemintOutcome,
    pub wires: HostRemintWires,
}

/// Host remint **full** production path: one entry that composes
/// [`RemintSession::remint_and_attach`] then [`host_remint_two_step`].
///
/// Production must not call `remint_and_attach` alone — this composer owns
/// the order:
/// 1. `remint_and_attach` (Binding swap + outcome; hard refuse with
///    [`RemintError::BindingConflict`] when Binding already live / ownership
///    changed — no silent steal)
/// 2. OLD `flush_and_teardown_after_remint`
/// 3. when `needs_attach_resume` → NEW `attach_resume_on_new_wire`
///
/// Never `session/load` or `session/new`. [`ClientCapabilities::HOST`] stays
/// all false. SoftExpiring / Flush / Stage / Evidence / EffectId parked.
pub fn host_remint_full_path(
    old: &mut AttachRunner,
    new: &mut AttachRunner,
    new_transport: &mut RunnerTransport,
    args: HostRemintFullArgs<'_>,
) -> Result<HostRemintFullPath, RemintError> {
    let HostRemintFullArgs {
        session,
        place,
        mcp_servers,
        init_rpc_id,
        old_next_rpc_id,
        new_next_rpc_id,
    } = args;
    // Before any side effect (mint, cancel, teardown, initialize).
    remint::validate_resume_place(place)?;
    let outcome = session.remint_and_attach()?;
    let wires = host_remint_two_step(
        old,
        new,
        new_transport,
        HostRemintArgs {
            session,
            place,
            mcp_servers,
            outcome: outcome.clone(),
            init_rpc_id,
            old_next_rpc_id,
            new_next_rpc_id,
        },
    )?;
    Ok(HostRemintFullPath { outcome, wires })
}

/// Outcome of one [`AttachRunner::read_stdout`] attempt.
///
/// Real `Ok(0)` after poll-readable (including POLLHUP with no bytes) is
/// [`StdoutRead::Eof`] — proven wire loss for [`OldWire::StdoutEof`].
/// Timeout / WouldBlock / Interrupted stay [`StdoutRead::Empty`] so idle
/// never remints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StdoutRead {
    /// Some bytes arrived.
    Bytes(Vec<u8>),
    /// Timeout / WouldBlock / Interrupted — NOT proof of loss.
    Empty,
    /// `read` returned `Ok(0)` after poll reported readable (or POLLHUP).
    Eof,
}

/// Map a stdout read to an [`OldWire`] hint for [`host_wake`].
///
/// - [`StdoutRead::Eof`] → [`OldWire::StdoutEof`] (proven lost)
/// - [`StdoutRead::Empty`] → [`OldWire::Unknown`] (timeout must not remint)
/// - [`StdoutRead::Bytes`] → [`OldWire::LiveIdle`] (caller may upgrade to
///   [`OldWire::LiveInTurn`] when a turn is open)
pub fn old_wire_from_stdout(read: &StdoutRead) -> OldWire {
    match read {
        StdoutRead::Eof => OldWire::StdoutEof,
        StdoutRead::Empty => OldWire::Unknown,
        StdoutRead::Bytes(_) => OldWire::LiveIdle,
    }
}

/// What the Host knows about the OLD wire at wake time. Only `Exited`,
/// `StdoutEof` and `TransportError` prove the wire lost; a running child
/// alone is not proof of anything (`Unknown`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OldWire {
    /// Child `try_wait` returned `Some(code)`.
    Exited(i32),
    /// Child stdout hit EOF (`read` Ok(0) / POLLHUP with no bytes).
    /// Produced by [`old_wire_from_stdout`] / [`AttachRunner::probe_stdout`]
    /// from Real I/O — never from a read timeout.
    StdoutEof,
    /// Real IO error on the old wire (read/write failure, EPIPE). A read
    /// timeout is NOT this: [`AttachRunner::read_stdout`] returns
    /// [`StdoutRead::Empty`] on timeout / WouldBlock (never an error), and
    /// [`old_wire_from_stdout`] maps that to [`OldWire::Unknown`].
    TransportError(String),
    /// Wire alive, no turn open.
    LiveIdle,
    /// Wire alive, turn open.
    LiveInTurn,
    /// No evidence either way.
    Unknown,
}

impl OldWire {
    pub fn is_lost(&self) -> bool {
        matches!(
            self,
            Self::Exited(_) | Self::StdoutEof | Self::TransportError(_)
        )
    }
}

impl AttachRunner {
    /// Probe the child via `try_wait`:
    /// - `Ok(Some(code))` → [`OldWire::Exited`] (observed exit: proof of loss)
    /// - `Ok(None)` → [`OldWire::Unknown`] (running is not proof of a live,
    ///   answering wire)
    /// - `Err(_)` → [`OldWire::Unknown`]: a failed probe is not an observed
    ///   exit, so it never mints. Only observed exit, stdout EOF, or a real
    ///   wire IO error ([`OldWire::TransportError`], reported by the caller
    ///   from a read/write) prove loss.
    pub fn probe_exit(&mut self) -> OldWire {
        old_wire_from_try_wait(self.try_wait())
    }
}

fn old_wire_from_try_wait(r: Result<Option<i32>, BusError>) -> OldWire {
    match r {
        Ok(Some(code)) => OldWire::Exited(code),
        Ok(None) | Err(_) => OldWire::Unknown,
    }
}

/// Result of [`host_wake`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostWake {
    /// Old wire proven lost: one Binding mint, new wire resumed.
    Reminted(Box<HostRemintFullPath>),
    /// Old wire alive: existing Binding reused, no wire action.
    ReuseLive,
    /// No evidence: zero wire actions, zero mints.
    Waiting(hearth::WaitReason),
}

/// Host wake driver — the production caller of [`host_remint_full_path`].
///
/// - Old wire proven lost → kill + reap the old child, then
///   [`host_remint_full_path`] (cancel-before-reattach
///   if a turn is in flight, flush/teardown, new wire, `session/resume` with
///   the same session id).
/// - `LiveIdle` / `LiveInTurn` → reuse the Binding. No remint, no cancel.
/// - `Unknown` → `Waiting(LivenessUnknown)`, no wire action.
///
/// Exactly one Binding mint per wake: the only mint is the rebind inside
/// [`RemintSession::remint_and_attach`]. Callers must not also claim a
/// Binding (e.g. `Runtime::wake` / `claim_binding`) for the same wake — that
/// rebind(None) would move ownership off the snapshotted old Binding and the
/// remint would refuse with [`RemintError::BindingConflict`].
pub fn host_wake(
    old_wire: OldWire,
    old: &mut AttachRunner,
    new: &mut AttachRunner,
    new_transport: &mut RunnerTransport,
    args: HostRemintFullArgs<'_>,
) -> Result<HostWake, RemintError> {
    match old_wire {
        OldWire::LiveIdle | OldWire::LiveInTurn => Ok(HostWake::ReuseLive),
        OldWire::Unknown => Ok(HostWake::Waiting(hearth::WaitReason::LivenessUnknown)),
        OldWire::Exited(_) | OldWire::StdoutEof | OldWire::TransportError(_) => {
            // Place/cwd first: a resume we could never write must not kill
            // the old child, mint, cancel or touch either wire.
            remint::validate_resume_place(args.place)?;
            // EOF / EPIPE can happen while the child still runs: kill and
            // reap it BEFORE the new Binding is minted so two live agents
            // never share a session. Teardown writes then drop (dead wire).
            old.kill()
                .map_err(|e| RemintError::HostLog(format!("kill old: {e}")))?;
            host_remint_full_path(old, new, new_transport, args)
                .map(|full| HostWake::Reminted(Box::new(full)))
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

    fn ws_place() -> hearth::Place {
        hearth::Place::local_dir("/work/repo", hearth::PlaceAttach::RecreateFromGit)
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
        let rpc = encode_remint_rpc(
            RemintWireMethod::SessionResume,
            "S-keep",
            1,
            Some(&ws_place()),
            &[],
        )
        .unwrap();
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

        let resumed = encode_remint_rpc(
            RemintWireMethod::SessionResume,
            &out.agent_session_id,
            2,
            Some(&ws_place()),
            &[],
        )
        .unwrap();
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
        let allowed = encode_remint_rpc(
            RemintWireMethod::SessionResume,
            "S-orig",
            1,
            Some(&ws_place()),
            &[],
        )
        .unwrap();
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

    // ---- stdout EOF vs timeout (Runner-stdout-eof-as-lost) ----

    /// Real child closes stdout but stays alive: Ok(0)/POLLHUP → StdoutEof.
    #[test]
    fn real_stdout_eof_surfaces_as_old_wire_stdout_eof() {
        let spec = LaunchSpec {
            program: "sh".into(),
            // Close fd 1 then sleep — half-dead: stdout EOF, child still running.
            args: vec!["-c".into(), "exec 1>&-; sleep 30".into()],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        };
        let mut runner = AttachRunner::spawn(spec).expect("spawn");
        // Child may need a beat to close fd 1 after spawn.
        let mut read = StdoutRead::Empty;
        for _ in 0..25 {
            read = runner.read_stdout().expect("read");
            if read == StdoutRead::Eof {
                break;
            }
        }
        assert_eq!(read, StdoutRead::Eof, "Ok(0) after POLLHUP must be Eof");
        assert_eq!(old_wire_from_stdout(&read), OldWire::StdoutEof);
        assert!(old_wire_from_stdout(&read).is_lost());
        // Child still running — try_wait alone would be Unknown; EOF is the proof.
        assert_eq!(runner.try_wait().unwrap(), None, "child still alive");
        assert_eq!(
            runner.probe_exit(),
            OldWire::Unknown,
            "try_wait Exited path unchanged / not fired"
        );
        let _ = runner.kill();
    }

    /// Silent live child: poll timeout → Empty → Unknown (never remint).
    #[test]
    fn real_stdout_timeout_stays_unknown_not_eof() {
        let spec = LaunchSpec {
            program: "sleep".into(),
            args: vec!["30".into()],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        };
        let mut runner = AttachRunner::spawn(spec).expect("spawn sleep");
        let read = runner.read_stdout().expect("read");
        assert_eq!(read, StdoutRead::Empty, "timeout must be Empty, not Eof");
        assert_eq!(old_wire_from_stdout(&read), OldWire::Unknown);
        assert!(!old_wire_from_stdout(&read).is_lost());
        assert_eq!(runner.probe_stdout().unwrap(), OldWire::Unknown);
        let _ = runner.kill();
    }

    /// Correct mapping + naives as contrasts (toy parity).
    #[test]
    fn stdout_read_mapping_contrasts_naive_eof_as_empty_and_timeout_as_eof() {
        // Correct: Eof → StdoutEof (remint); Empty → Unknown (wait).
        assert_eq!(old_wire_from_stdout(&StdoutRead::Eof), OldWire::StdoutEof);
        assert_eq!(old_wire_from_stdout(&StdoutRead::Empty), OldWire::Unknown);
        assert_eq!(
            old_wire_from_stdout(&StdoutRead::Bytes(b"x".to_vec())),
            OldWire::LiveIdle
        );

        // NaiveEofAsEmpty: Ok(0) collapsed into empty → StuckUnknown.
        fn naive_eof_as_empty(r: &StdoutRead) -> OldWire {
            match r {
                StdoutRead::Eof | StdoutRead::Empty => OldWire::Unknown,
                StdoutRead::Bytes(_) => OldWire::LiveIdle,
            }
        }
        let stuck = naive_eof_as_empty(&StdoutRead::Eof);
        assert_eq!(stuck, OldWire::Unknown, "NaiveEofAsEmpty → StuckUnknown");
        assert!(!stuck.is_lost());

        // NaiveTimeoutAsEof: any empty → StdoutEof → FalseRemint.
        fn naive_timeout_as_eof(r: &StdoutRead) -> OldWire {
            match r {
                StdoutRead::Eof | StdoutRead::Empty => OldWire::StdoutEof,
                StdoutRead::Bytes(_) => OldWire::LiveIdle,
            }
        }
        let false_remint = naive_timeout_as_eof(&StdoutRead::Empty);
        assert_eq!(
            false_remint,
            OldWire::StdoutEof,
            "NaiveTimeoutAsEof → FalseRemint"
        );
        assert!(false_remint.is_lost());
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
                Some(&ws_place()),
                &[],
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
                Some(&ws_place()),
                &[],
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
                Some(&ws_place()),
                &[],
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
            Some(&ws_place()),
            &[],
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
        assert_eq!(written[0]["params"]["cwd"], "/work/repo");
        assert_eq!(written[0]["params"]["mcpServers"], serde_json::json!([]));
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
                place: Some(&ws_place()),
                mcp_servers: &[],
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
                place: Some(&ws_place()),
                mcp_servers: &[],
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
                place: Some(&ws_place()),
                mcp_servers: &[],
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

    /// Host full path: remint_and_attach then host_remint_two_step in one
    /// entry — happy path; never load/new; HOST caps all false.
    #[test]
    fn host_remint_full_path_composes_remint_then_two_step() {
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
        let agent_sid_before = session.agent_session_id();

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

        let full = host_remint_full_path(
            &mut old_runner,
            &mut new_runner,
            &mut transport,
            HostRemintFullArgs {
                session: &session,
                place: Some(&ws_place()),
                mcp_servers: &[],
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
        .expect("host remint full path");

        assert!(
            full.outcome
                .wire_actions
                .contains(&WireAction::SessionClose)
                || full
                    .outcome
                    .wire_actions
                    .contains(&WireAction::SessionCancel)
        );
        assert!(full
            .outcome
            .wire_actions
            .contains(&WireAction::AttachResume));
        assert_eq!(full.outcome.agent_session_id, agent_sid_before);
        assert!(full.wires.flush.needs_attach_resume);
        let caps = full.wires.live_caps.expect("attach resume owed");
        assert!(caps.resume);
        assert!(session.live_caps().resume);

        let old_raw = std::str::from_utf8(old_runner.stdin_log().unwrap()).unwrap();
        assert!(
            old_raw.contains("session/close") || old_raw.contains("session/cancel"),
            "old wire must teardown: {old_raw}"
        );
        assert!(!old_raw.contains("session/resume"));
        assert!(!old_raw.contains("session/load"));
        assert!(!old_raw.contains("session/new"));

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
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            serde_json::json!({
                "fs": {"readTextFile": false, "writeTextFile": false},
                "terminal": false
            })
        );
        assert_eq!(frames[1]["method"], "session/resume");
        assert_eq!(frames[1]["params"]["sessionId"], agent_sid_before);
        assert!(
            frames
                .iter()
                .all(|f| f["method"].as_str() != Some("session/new")
                    && f["method"].as_str() != Some("session/load")),
            "never session/new or session/load: {frames:?}"
        );
        assert_eq!(new_next, 3);
    }

    /// Host full path hard-refuse: competing Binding holder → typed
    /// [`RemintError::BindingConflict`]; no wire writes (no silent steal,
    /// no load/new).
    #[test]
    fn host_remint_full_path_refuses_binding_conflict() {
        let (session, _, bid) = RemintSession::open(
            "sess-1",
            "S-stable",
            LiveCaps {
                resume: true,
                cancel: true,
                close: true,
                load_session: false,
            },
        );
        // Competing rebind wins between snapshot and claim (same as
        // remint_claim_rechecks_owner_after_competing_rebind).
        let old = session.host_binding_of(&bid);
        let agent = session.host_agent_id();
        session.inject_before_claim(move |host, snap| {
            assert_eq!(snap, old);
            let _ = host
                .rebind(snap, agent, hearth::Host::from_bind("acp", None, None))
                .unwrap();
        });
        session.begin_turn("mid");

        let mut old_runner = initialized_fake(Vec::new());
        let mut new_runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, Vec::new());
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let mut old_next = 1u64;
        let mut new_next = 2u64;

        let err = host_remint_full_path(
            &mut old_runner,
            &mut new_runner,
            &mut transport,
            HostRemintFullArgs {
                session: &session,
                place: Some(&ws_place()),
                mcp_servers: &[],
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
        .unwrap_err();
        assert_eq!(err, RemintError::BindingConflict);
        assert!(session
            .observe()
            .iter()
            .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "BindingConflict"));

        // Remint state unchanged; no OLD/NEW wire traffic from the composer
        // (old runner may only have its prior initialize from initialized_fake).
        assert_eq!(session.binding_id(), bid);
        let old_raw = std::str::from_utf8(old_runner.stdin_log().unwrap()).unwrap();
        let old_after_init = frames_after_initialize(&old_runner);
        assert!(
            old_after_init.is_empty(),
            "no flush/teardown on conflict: {old_after_init:?}"
        );
        assert!(!old_raw.contains("session/resume"));
        assert!(!old_raw.contains("session/load"));
        assert!(!old_raw.contains("session/new"));
        assert!(
            new_runner.stdin_log().unwrap().is_empty(),
            "NEW wire must stay silent on conflict"
        );
        assert_eq!(new_next, 2);
    }
    // ---- host_wake driver ----

    fn wake_session() -> RemintSession {
        RemintSession::open(
            "sess-1",
            "S-stable",
            LiveCaps {
                resume: true,
                cancel: true,
                close: true,
                load_session: false,
            },
        )
        .0
    }

    fn mints(session: &RemintSession) -> usize {
        session
            .host_events()
            .iter()
            .filter(|e| matches!(e.body, hearth::EventBody::BindingAttached { .. }))
            .count()
    }

    fn new_wire_fake() -> AttachRunner {
        let init = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "protocolVersion": 1,
                "agentCapabilities": { "session": { "resume": true, "cancel": true, "close": true } } }
        });
        AttachRunner::spawn_fake(
            WireKind::JsonlRpc,
            JsonlRpcTransport::encode_jsonl(&init).unwrap(),
        )
    }

    fn run_wake(
        session: &RemintSession,
        old_wire: OldWire,
        old: &mut AttachRunner,
        new: &mut AttachRunner,
    ) -> Result<HostWake, RemintError> {
        let mut transport = transport_for_wire(WireKind::JsonlRpc);
        let (mut old_next, mut new_next) = (10u64, 2u64);
        host_wake(
            old_wire,
            old,
            new,
            &mut transport,
            HostRemintFullArgs {
                session,
                place: Some(&ws_place()),
                mcp_servers: &[],
                init_rpc_id: &RpcId::Num(1),
                old_next_rpc_id: &mut old_next,
                new_next_rpc_id: &mut new_next,
            },
        )
    }

    /// Lost wire (each proof kind): exactly one mint, cancel-before-reattach
    /// mid-turn, new wire initialize (HOST false) + resume same session id.
    #[test]
    fn host_wake_lost_wire_remints_once_and_resumes() {
        for kind in ["exit", "eof", "transport"] {
            let session = wake_session();
            session.begin_turn("mid");
            let sid = session.agent_session_id();
            let bid = session.binding_id();
            let mut old = initialized_fake(Vec::new());
            let old_wire = match kind {
                // Fake stdout exhausted → try_wait reports exit.
                "exit" => old.probe_exit(),
                "eof" => OldWire::StdoutEof,
                _ => OldWire::TransportError("broken pipe".into()),
            };
            assert!(old_wire.is_lost(), "{kind}: {old_wire:?}");
            let mut new = new_wire_fake();
            let before = mints(&session);

            let HostWake::Reminted(full) = run_wake(&session, old_wire, &mut old, &mut new)
                .unwrap_or_else(|e| panic!("{kind}: {e:?}"))
            else {
                panic!("{kind}: expected Reminted");
            };

            assert_eq!(mints(&session), before + 1, "{kind}: exactly one mint");
            assert_ne!(session.binding_id(), bid);
            let w = &full.outcome.wire_actions;
            assert!(
                w.contains(&WireAction::SessionCancel) || w.contains(&WireAction::SessionClose),
                "{kind}: cancel-before-reattach: {w:?}"
            );
            assert!(!session.turn_in_flight());
            let frames: Vec<serde_json::Value> = std::str::from_utf8(new.stdin_log().unwrap())
                .unwrap()
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert_eq!(frames.len(), 2, "{kind}: {frames:?}");
            assert_eq!(frames[0]["method"], "initialize");
            assert_eq!(
                frames[0]["params"]["clientCapabilities"],
                ClientCapabilities::HOST.to_json()
            );
            assert_eq!(frames[1]["method"], "session/resume");
            assert_eq!(frames[1]["params"]["sessionId"], sid);
            for raw in [old.stdin_log().unwrap(), new.stdin_log().unwrap()] {
                let raw = std::str::from_utf8(raw).unwrap();
                assert!(!raw.contains("session/load") && !raw.contains("session/new"));
            }
        }
    }

    /// Live wire (idle or in turn): reuse Binding, zero wire writes, zero
    /// mints, healthy turn not cancelled.
    #[test]
    fn host_wake_live_wire_reuses_binding_without_wire_writes() {
        for (old_wire, in_turn) in [(OldWire::LiveIdle, false), (OldWire::LiveInTurn, true)] {
            let session = wake_session();
            if in_turn {
                session.begin_turn("mid");
            }
            let bid = session.binding_id();
            let before = mints(&session);
            let mut old = initialized_fake(Vec::new());
            let old_log = old.stdin_log().unwrap().len();
            let mut new = new_wire_fake();

            let r = run_wake(&session, old_wire.clone(), &mut old, &mut new).unwrap();
            assert_eq!(r, HostWake::ReuseLive, "{old_wire:?}");
            assert_eq!(session.binding_id(), bid);
            assert_eq!(mints(&session), before);
            assert_eq!(session.turn_in_flight(), in_turn);
            assert_eq!(
                old.stdin_log().unwrap().len(),
                old_log,
                "no old-wire writes"
            );
            assert!(new.stdin_log().unwrap().is_empty(), "no new-wire writes");
        }
    }

    /// No evidence: typed Waiting(LivenessUnknown), zero wire writes/mints.
    #[test]
    fn host_wake_unknown_waits_without_wire_action() {
        let session = wake_session();
        session.begin_turn("mid");
        let bid = session.binding_id();
        let before = mints(&session);
        let mut old = initialized_fake(Vec::new());
        let old_log = old.stdin_log().unwrap().len();
        let mut new = new_wire_fake();

        let r = run_wake(&session, OldWire::Unknown, &mut old, &mut new).unwrap();
        assert_eq!(r, HostWake::Waiting(hearth::WaitReason::LivenessUnknown));
        assert_eq!(session.binding_id(), bid);
        assert_eq!(mints(&session), before);
        assert!(session.turn_in_flight());
        assert_eq!(old.stdin_log().unwrap().len(), old_log);
        assert!(new.stdin_log().unwrap().is_empty());
    }

    /// One-mint guard: a second claim on the same wake (Runtime::wake-style
    /// `rebind(None)` / `claim_binding`) while the old Binding is still owned
    /// is refused (`BindingOwnerChanged` → `AlreadyLive`), so the driver's
    /// rebind inside `remint_and_attach` is the only mint.
    #[test]
    fn host_wake_second_claim_refused_single_mint() {
        let session = wake_session();
        let agent = session.host_agent_id();
        let claimed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let c = claimed.clone();
        session.inject_before_claim(move |host, _snap| {
            let r = host.rebind(None, agent, hearth::Host::from_bind("acp", None, None));
            *c.lock().unwrap() = Some(r.is_err());
        });
        let before = mints(&session);
        let mut old = initialized_fake(Vec::new());
        let mut new = new_wire_fake();
        let r = run_wake(&session, OldWire::StdoutEof, &mut old, &mut new).unwrap();
        assert!(matches!(r, HostWake::Reminted(_)));
        assert_eq!(*claimed.lock().unwrap(), Some(true), "second claim refused");
        assert_eq!(mints(&session), before + 1, "exactly one mint");
    }
    /// try_wait error is not proof of loss: Unknown → Waiting, zero
    /// mints, zero wire writes.
    #[test]
    fn host_wake_probe_error_waits_without_wire_action() {
        let old_wire = old_wire_from_try_wait(Err(BusError::Transport("try_wait: EINVAL".into())));
        assert_eq!(old_wire, OldWire::Unknown);
        assert!(!old_wire.is_lost());
        let session = wake_session();
        let bid = session.binding_id();
        let before = mints(&session);
        let mut old = initialized_fake(Vec::new());
        let old_log = old.stdin_log().unwrap().len();
        let mut new = new_wire_fake();
        let r = run_wake(&session, old_wire, &mut old, &mut new).unwrap();
        assert_eq!(r, HostWake::Waiting(hearth::WaitReason::LivenessUnknown));
        assert_eq!(session.binding_id(), bid);
        assert_eq!(mints(&session), before);
        assert_eq!(old.stdin_log().unwrap().len(), old_log);
        assert!(new.stdin_log().unwrap().is_empty());
    }
    /// Lost via StdoutEof while the child still runs: old child is killed
    /// and reaped before the mint — no teardown frame reaches it (kill
    /// precedes remint_and_attach), exactly one mint.
    #[test]
    fn host_wake_kills_running_old_child_before_mint() {
        let session = wake_session();
        session.begin_turn("mid");
        let before = mints(&session);
        // Unread stdout keeps the fake child running.
        let mut old = initialized_fake(b"{}\n".to_vec());
        let old_log = old.stdin_log().unwrap().len();
        let mut new = new_wire_fake();
        let r = run_wake(&session, OldWire::StdoutEof, &mut old, &mut new).unwrap();
        assert!(matches!(r, HostWake::Reminted(_)));
        assert_eq!(
            old.try_wait().unwrap(),
            Some(9),
            "old child killed + reaped"
        );
        assert_eq!(
            old.stdin_log().unwrap().len(),
            old_log,
            "killed before mint: teardown writes dropped"
        );
        assert_eq!(mints(&session), before + 1);
    }

    /// Bad Place on a lost wire: typed fail-closed BEFORE kill / mint /
    /// cancel / either wire write; no rpc id consumed (Pep #32 Must).
    fn assert_wake_bad_place_zero_side_effects(place: Option<&hearth::Place>, want: RemintError) {
        {
            let session = wake_session();
            session.begin_turn("mid");
            let bid = session.binding_id();
            let before = mints(&session);
            // Unread stdout keeps the fake old child running.
            let mut old = initialized_fake(b"{}\n".to_vec());
            let old_log = old.stdin_log().unwrap().len();
            let mut new = new_wire_fake();
            let mut transport = transport_for_wire(WireKind::JsonlRpc);
            let (mut old_next, mut new_next) = (10u64, 2u64);
            let err = host_wake(
                OldWire::StdoutEof,
                &mut old,
                &mut new,
                &mut transport,
                HostRemintFullArgs {
                    session: &session,
                    place,
                    mcp_servers: &[],
                    init_rpc_id: &RpcId::Num(1),
                    old_next_rpc_id: &mut old_next,
                    new_next_rpc_id: &mut new_next,
                },
            )
            .unwrap_err();
            assert_eq!(err, want);
            assert_eq!(mints(&session), before, "{want:?}: no mint");
            assert_eq!(session.binding_id(), bid, "{want:?}: Binding kept");
            assert!(session.turn_in_flight(), "{want:?}: turn not cancelled");
            // Fake child may exit on its own (0); killed+reaped would be 9.
            assert_ne!(
                old.try_wait().unwrap(),
                Some(9),
                "{want:?}: old child not killed"
            );
            assert_eq!(
                old.stdin_log().unwrap().len(),
                old_log,
                "{want:?}: no old writes"
            );
            assert!(
                new.stdin_log().unwrap().is_empty(),
                "{want:?}: zero new frames"
            );
            assert_eq!(
                (old_next, new_next),
                (10, 2),
                "{want:?}: no rpc id consumed"
            );
        }
    }

    #[test]
    fn host_wake_no_place_zero_side_effects() {
        assert_wake_bad_place_zero_side_effects(None, RemintError::ResumeNoPlace);
    }

    #[test]
    fn host_wake_non_local_dir_place_zero_side_effects() {
        let mut aws = ws_place();
        aws.provider = hearth::PlaceProvider::Aws;
        assert_wake_bad_place_zero_side_effects(Some(&aws), RemintError::ResumePlaceNotLocalDir);
    }

    #[test]
    fn host_wake_relative_cwd_zero_side_effects() {
        let rel = hearth::Place::local_dir("work/repo", hearth::PlaceAttach::RecreateFromGit);
        assert_wake_bad_place_zero_side_effects(Some(&rel), RemintError::ResumeCwdNotAbsolute);
    }

    /// host_wake on Real-produced StdoutEof: one mint + session/resume.
    #[test]
    fn host_wake_on_probed_real_stdout_eof_remints_and_resumes() {
        let spec = LaunchSpec {
            program: "sh".into(),
            args: vec!["-c".into(), "exec 1>&-; sleep 30".into()],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        };
        let mut old = AttachRunner::spawn(spec).expect("spawn");
        let mut old_wire = OldWire::Unknown;
        for _ in 0..25 {
            old_wire = old.probe_stdout().expect("probe");
            if old_wire == OldWire::StdoutEof {
                break;
            }
        }
        assert_eq!(old_wire, OldWire::StdoutEof);

        let session = wake_session();
        session.begin_turn("mid");
        let sid = session.agent_session_id();
        let before = mints(&session);
        let mut new = new_wire_fake();

        let HostWake::Reminted(full) =
            run_wake(&session, old_wire, &mut old, &mut new).expect("wake")
        else {
            panic!("expected Reminted on real StdoutEof");
        };
        assert_eq!(mints(&session), before + 1);
        assert!(!session.turn_in_flight());
        let frames: Vec<serde_json::Value> = std::str::from_utf8(new.stdin_log().unwrap())
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(frames[0]["method"], "initialize");
        assert_eq!(
            frames[0]["params"]["clientCapabilities"],
            ClientCapabilities::HOST.to_json()
        );
        assert_eq!(frames[1]["method"], "session/resume");
        assert_eq!(frames[1]["params"]["sessionId"], sid);
        let w = &full.outcome.wire_actions;
        assert!(
            w.contains(&WireAction::SessionCancel) || w.contains(&WireAction::SessionClose),
            "cancel-before-reattach: {w:?}"
        );
        // Real runner has no stdin_log; only check new wire.
        let raw = std::str::from_utf8(new.stdin_log().unwrap()).unwrap();
        assert!(!raw.contains("session/load") && !raw.contains("session/new"));
    }
}
