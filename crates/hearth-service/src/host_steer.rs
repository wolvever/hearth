//! Service Host-wake splice: HTTP/WS steer → [`wake_each_kept_through_host_wake`].
//!
//! Closes the production hole between #46–#68 kernel/wire proofs and the
//! HTTP-visible orch path. Keep recipe stays a Host ticket per Agent; wires
//! are a HostAttach / RemintSession map keyed by Agent (Fake / Loopback / Real OK).
//! SoftExpiring / fleet / ToolResult parent / v2 / durable EventLog parked.

use std::collections::HashMap;
use std::time::Duration;

use hearth::{AgentId, Event, Place, SessionId, Wake};
use hearth_agent_wire::{
    complete_host_tool, transport_for_wire, wake_each_kept_through_host_wake, AttachRunner,
    CompleteHostTool, CompleteHostToolArgs, CompleteHostToolError, HostRemintFullArgs, HostWake,
    KeptWakeWire, LaunchSpec, OldWire, RemintSession, RpcId, RunnerTransport,
    WakeEachKeptThroughHost, WakeEachKeptThroughHostArgs, WakeThroughError, WireKind,
};

use crate::AppState;

/// One kept Agent's Host wire held by the service for steer / remint.
///
/// Host supplies Fake or Real [`AttachRunner`]s. LiveIdle / quick remint Must
/// floors use Fake; Real Exited ([`Self::real_exited`]) and Real StdoutEof
/// ([`Self::real_stdout_eof`]) concurrent remint Musts probe a Real CLI —
/// no Fake Exited / StdoutEof injection.
pub struct AgentHostWire {
    pub remint: RemintSession,
    pub old: AttachRunner,
    pub new: AttachRunner,
    pub new_transport: RunnerTransport,
    pub old_wire: OldWire,
    pub place: Option<Place>,
    pub old_next_rpc_id: u64,
    pub new_next_rpc_id: u64,
    pub init_rpc_id: RpcId,
}

impl AgentHostWire {
    /// Build a Fake LiveIdle wire (ReuseLive on steer; zero mint).
    pub fn fake_live(remint: RemintSession, place: Option<Place>) -> Self {
        Self {
            remint,
            old: initialized_fake(Vec::new()),
            new: new_wire_fake(),
            new_transport: transport_for_wire(WireKind::JsonlRpc),
            old_wire: OldWire::LiveIdle,
            place,
            old_next_rpc_id: 10,
            new_next_rpc_id: 2,
            init_rpc_id: RpcId::Num(1),
        }
    }

    /// Build a Fake wire whose next steer remints (`StdoutEof` / `Exited`).
    pub fn fake_lost(remint: RemintSession, place: Option<Place>, lost: OldWire) -> Self {
        assert!(
            lost.is_lost(),
            "fake_lost requires proven-lost OldWire (Exited/StdoutEof/TransportError)"
        );
        Self {
            remint,
            old: initialized_fake(Vec::new()),
            new: new_wire_fake(),
            new_transport: transport_for_wire(WireKind::JsonlRpc),
            old_wire: lost,
            place,
            old_next_rpc_id: 10,
            new_next_rpc_id: 2,
            init_rpc_id: RpcId::Num(1),
        }
    }

    /// Build a Real-old / Fake-new wire after probing `sh -c 'exit {code}'` to
    /// [`OldWire::Exited`]. Service-real-cli-concurrent-remint Must path —
    /// reuses wire `spawn_and_probe_real_exit` (#64); never Fake-injects Exited.
    pub fn real_exited(remint: RemintSession, place: Option<Place>, code: i32) -> Self {
        let (old, old_wire) = spawn_and_probe_real_exit(code);
        Self {
            remint,
            old,
            new: new_wire_fake(),
            new_transport: transport_for_wire(WireKind::JsonlRpc),
            old_wire,
            place,
            old_next_rpc_id: 10,
            new_next_rpc_id: 2,
            init_rpc_id: RpcId::Num(1),
        }
    }

    /// Build a Real-old / Fake-new wire after probing
    /// `sh -c 'exec 1>&-; sleep 30'` to [`OldWire::StdoutEof`].
    /// Service-real-stdout-eof-concurrent-remint Must path (Soft twin of
    /// [`Self::real_exited`] / #72) — reuses wire
    /// `spawn_and_probe_real_stdout_eof` (#65); never Fake-injects StdoutEof.
    pub fn real_stdout_eof(remint: RemintSession, place: Option<Place>) -> Self {
        let (old, old_wire) = spawn_and_probe_real_stdout_eof();
        Self {
            remint,
            old,
            new: new_wire_fake(),
            new_transport: transport_for_wire(WireKind::JsonlRpc),
            old_wire,
            place,
            old_next_rpc_id: 10,
            new_next_rpc_id: 2,
            init_rpc_id: RpcId::Num(1),
        }
    }
}

/// Registry of Host wires keyed by `(Session, Agent)`.
#[derive(Default)]
pub struct HostWireMap {
    agents: HashMap<(SessionId, AgentId), AgentHostWire>,
}

impl HostWireMap {
    pub fn insert(&mut self, session: SessionId, agent: AgentId, wire: AgentHostWire) {
        self.agents.insert((session, agent), wire);
    }

    pub fn get_mut(&mut self, session: SessionId, agent: AgentId) -> Option<&mut AgentHostWire> {
        self.agents.get_mut(&(session, agent))
    }

    pub fn has_any_for_session(&self, session: SessionId) -> bool {
        self.agents.keys().any(|(s, _)| *s == session)
    }

    pub fn get(&self, session: SessionId, agent: AgentId) -> Option<&AgentHostWire> {
        self.agents.get(&(session, agent))
    }

    /// Single-Agent Host tool completion (not fan-out).
    ///
    /// Takes the wire out, runs [`complete_host_tool`], puts it back. Updates
    /// `old_wire` to LiveIdle after a successful remint/reuse so a follow-up
    /// completion reuses the Binding.
    pub fn complete_host_tool_for(
        &mut self,
        runtime: &hearth::Runtime,
        session_id: SessionId,
        agent: AgentId,
        tool_call_id: &str,
        output: &str,
    ) -> Result<CompleteHostTool, CompleteHostToolError> {
        let mut wire = self
            .agents
            .remove(&(session_id, agent))
            .ok_or(CompleteHostToolError::Orphan)?;
        let result = complete_host_tool(
            &mut wire.old,
            &mut wire.new,
            &mut wire.new_transport,
            CompleteHostToolArgs {
                runtime,
                session_id,
                agent,
                tool_call_id,
                output,
                old_wire: wire.old_wire.clone(),
                remint: HostRemintFullArgs {
                    session: &wire.remint,
                    place: wire.place.as_ref(),
                    mcp_servers: &[],
                    init_rpc_id: &wire.init_rpc_id,
                    old_next_rpc_id: &mut wire.old_next_rpc_id,
                    new_next_rpc_id: &mut wire.new_next_rpc_id,
                },
            },
        );
        // After remint/reuse, subsequent wakes see LiveIdle (same as steer path).
        if let Ok(CompleteHostTool::Applied { host, .. }) = &result {
            match host {
                HostWake::Reminted(_) | HostWake::ReuseLive => {
                    wire.old_wire = OldWire::LiveIdle;
                }
                HostWake::Waiting(_) => {}
            }
        }
        self.agents.insert((session_id, agent), wire);
        result
    }

    /// Drain this Session's wires, run `wake_each_kept_through_host_wake`, put back.
    pub fn wake_each_kept(
        &mut self,
        runtime: &hearth::Runtime,
        session_id: SessionId,
        wake: Wake,
    ) -> Result<WakeEachKeptThroughHost, WakeThroughError> {
        let keys: Vec<AgentId> = self
            .agents
            .keys()
            .filter_map(|(s, a)| (*s == session_id).then_some(*a))
            .collect();
        let mut owned: Vec<(AgentId, AgentHostWire)> = Vec::with_capacity(keys.len());
        for a in keys {
            if let Some(w) = self.agents.remove(&(session_id, a)) {
                owned.push((a, w));
            }
        }
        let result = {
            let wires: HashMap<AgentId, KeptWakeWire<'_>> = owned
                .iter_mut()
                .map(|(a, w)| {
                    (
                        *a,
                        KeptWakeWire {
                            old: &mut w.old,
                            new: &mut w.new,
                            new_transport: &mut w.new_transport,
                            old_wire: w.old_wire.clone(),
                            remint: HostRemintFullArgs {
                                session: &w.remint,
                                place: w.place.as_ref(),
                                mcp_servers: &[],
                                init_rpc_id: &w.init_rpc_id,
                                old_next_rpc_id: &mut w.old_next_rpc_id,
                                new_next_rpc_id: &mut w.new_next_rpc_id,
                            },
                        },
                    )
                })
                .collect();
            wake_each_kept_through_host_wake(
                WakeEachKeptThroughHostArgs {
                    runtime,
                    session_id,
                    wake,
                },
                wires,
            )
        };
        for (a, w) in owned {
            self.agents.insert((session_id, a), w);
        }
        result
    }
}

/// Thin service wrapper: Host path when wires are installed for the Session;
/// otherwise Goose stand-in [`hearth::Runtime::wake_each_kept`] (ClaimBinding).
pub fn steer_each_kept(
    st: &AppState,
    session_id: SessionId,
    wake: Wake,
) -> Result<SteerOutcome, SteerError> {
    let mut wires = st.host_wires.lock().map_err(|_| SteerError::Lock)?;
    if wires.has_any_for_session(session_id) {
        match wires.wake_each_kept(&st.runtime, session_id, wake) {
            Ok(WakeEachKeptThroughHost::Woke { hosts, woke }) => {
                Ok(SteerOutcome::Host { hosts, woke })
            }
            Ok(WakeEachKeptThroughHost::Waiting(reason)) => Err(SteerError::Waiting(reason)),
            Err(WakeThroughError::Runtime(hearth::Error::Waiting(reason))) => {
                Err(SteerError::Waiting(reason))
            }
            Err(WakeThroughError::Runtime(e)) => Err(SteerError::Runtime(e)),
            Err(WakeThroughError::Remint(e)) => Err(SteerError::Remint(e)),
        }
    } else {
        match st.runtime.wake_each_kept(session_id, wake) {
            Ok(woke) => Ok(SteerOutcome::Kernel { woke }),
            Err(hearth::Error::Waiting(reason)) => Err(SteerError::Waiting(reason)),
            Err(e) => Err(SteerError::Runtime(e)),
        }
    }
}

/// Result of [`steer_each_kept`].
pub enum SteerOutcome {
    /// Wired Host path: per-Agent `host_wake` + Runtime fan-out.
    Host {
        hosts: Vec<(AgentId, hearth_agent_wire::HostWake)>,
        woke: Vec<(AgentId, Vec<Event>)>,
    },
    /// Unwired Goose stand-in: ClaimBinding `wake_each_kept`.
    Kernel { woke: Vec<(AgentId, Vec<Event>)> },
}

impl SteerOutcome {
    pub fn woke(&self) -> &[(AgentId, Vec<Event>)] {
        match self {
            Self::Host { woke, .. } | Self::Kernel { woke } => woke,
        }
    }
}

#[derive(Debug)]
pub enum SteerError {
    Lock,
    Waiting(hearth::WaitReason),
    Runtime(hearth::Error),
    Remint(hearth_agent_wire::RemintError),
}

/// Outcome of [`complete_tool_result`].
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum CompleteToolOutcome {
    /// Fresh ToolResult + Trigger wake applied.
    Applied {
        tool_result: Event,
        wake_events: Vec<Event>,
    },
    /// Duplicate / late-after-Interrupted — zero new Result/Wake.
    NotApplied,
}

/// Error from [`complete_tool_result`].
#[derive(Debug)]
pub enum CompleteToolError {
    Lock,
    /// No prior unmatched Host ToolCall for this Agent + id.
    Orphan,
    /// Wake Waiting after Result applied (or before on kernel path).
    Waiting(hearth::WaitReason),
    Runtime(hearth::Error),
    Remint(hearth_agent_wire::RemintError),
    /// Agent has no Host wire and kernel path refused.
    NoWire,
}

/// Service Host-owned tool completion → ToolResult + single-Agent Trigger wake.
///
/// Wired Sessions: [`complete_host_tool`] via that Agent's wire only (never
/// `wake_each_kept`). Unwired Goose stand-in: Session append + `Runtime::wake`
/// Trigger. Orphan → Orphan; terminal → NotApplied; wake Waiting → Waiting
/// (Result may already be on the log — M8).
pub fn complete_tool_result(
    st: &AppState,
    session_id: SessionId,
    agent: AgentId,
    tool_call_id: &str,
    output: &str,
) -> Result<CompleteToolOutcome, CompleteToolError> {
    let mut wires = st.host_wires.lock().map_err(|_| CompleteToolError::Lock)?;
    if wires.get(session_id, agent).is_some() {
        match wires.complete_host_tool_for(&st.runtime, session_id, agent, tool_call_id, output) {
            Ok(CompleteHostTool::Applied {
                tool_result,
                wake_events,
                ..
            }) => Ok(CompleteToolOutcome::Applied {
                tool_result,
                wake_events,
            }),
            Ok(CompleteHostTool::NotApplied) => Ok(CompleteToolOutcome::NotApplied),
            Ok(CompleteHostTool::AppliedWakeWaiting { reason, .. }) => {
                Err(CompleteToolError::Waiting(reason))
            }
            Err(CompleteHostToolError::Orphan) => Err(CompleteToolError::Orphan),
            Err(CompleteHostToolError::Remint(e)) => Err(CompleteToolError::Remint(e)),
            Err(CompleteHostToolError::Runtime(hearth::Error::Waiting(r))) => {
                Err(CompleteToolError::Waiting(r))
            }
            Err(CompleteHostToolError::Runtime(e)) => Err(CompleteToolError::Runtime(e)),
        }
    } else {
        // Goose stand-in: append then Runtime::wake (ClaimBinding).
        drop(wires);
        let sess = st
            .runtime
            .store()
            .session(session_id)
            .map_err(CompleteToolError::Runtime)?;
        let append = match sess.append_host_tool_result(agent, tool_call_id, output) {
            Ok(a) => a,
            Err(hearth::HostToolResultError::Orphan) => return Err(CompleteToolError::Orphan),
        };
        let tool_result = match append {
            hearth::HostToolResultAppend::AlreadyTerminal => {
                return Ok(CompleteToolOutcome::NotApplied);
            }
            hearth::HostToolResultAppend::Applied(ev) => ev,
        };
        match st.runtime.wake(
            session_id,
            agent,
            Wake::Trigger {
                name: "tool_result".into(),
            },
        ) {
            Ok(wake_events) => Ok(CompleteToolOutcome::Applied {
                tool_result,
                wake_events,
            }),
            Err(hearth::Error::Waiting(reason)) => Err(CompleteToolError::Waiting(reason)),
            Err(e) => Err(CompleteToolError::Runtime(e)),
        }
    }
}

/// Spawn a Real CLI that exits with `code` and probe until [`OldWire::Exited`].
/// Mirrors wire `spawn_and_probe_real_exit` (#64) for the service Host-wire map.
fn spawn_and_probe_real_exit(code: i32) -> (AttachRunner, OldWire) {
    let spec = LaunchSpec {
        program: "sh".into(),
        args: vec!["-c".into(), format!("exit {code}")],
        cwd: None,
        env: Vec::new(),
        wire: WireKind::JsonlRpc,
    };
    let mut old = AttachRunner::spawn(spec).expect("spawn real CLI");
    let mut old_wire = OldWire::Unknown;
    for _ in 0..50 {
        old_wire = old.probe_exit();
        if matches!(old_wire, OldWire::Exited(_)) {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        old_wire,
        OldWire::Exited(code),
        "Real probe must observe Exited (no Fake Exited injection)"
    );
    (old, old_wire)
}

/// Spawn a Real CLI that closes stdout but stays alive, then probe until
/// [`OldWire::StdoutEof`]. Mirrors wire `spawn_and_probe_real_stdout_eof`
/// (#65) for the service Host-wire map — no Fake StdoutEof injection.
fn spawn_and_probe_real_stdout_eof() -> (AttachRunner, OldWire) {
    let spec = LaunchSpec {
        program: "sh".into(),
        args: vec!["-c".into(), "exec 1>&-; sleep 30".into()],
        cwd: None,
        env: Vec::new(),
        wire: WireKind::JsonlRpc,
    };
    let mut old = AttachRunner::spawn(spec).expect("spawn real CLI stdout-eof");
    let mut old_wire = OldWire::Unknown;
    for _ in 0..50 {
        old_wire = old.probe_stdout().expect("probe stdout");
        if old_wire == OldWire::StdoutEof {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        old_wire,
        OldWire::StdoutEof,
        "Real probe must observe StdoutEof (no Fake StdoutEof injection)"
    );
    (old, old_wire)
}

/// Fake JSONL child that answered `initialize` (id 0).
fn initialized_fake(rest: Vec<u8>) -> AttachRunner {
    let mut stdout = hearth_agent_wire::JsonlRpcTransport::encode_jsonl(&serde_json::json!({
        "jsonrpc": "2.0", "id": 0, "result": { "protocolVersion": 1 }
    }))
    .expect("encode init");
    stdout.extend(rest);
    let mut runner = AttachRunner::spawn_fake(WireKind::JsonlRpc, stdout);
    let mut t = transport_for_wire(WireKind::JsonlRpc);
    runner
        .initialize_host(&mut t, &RpcId::Num(0))
        .expect("initialize fake");
    runner
}

/// New wire Fake that answers initialize with resume caps.
fn new_wire_fake() -> AttachRunner {
    let init = serde_json::json!({
        "jsonrpc": "2.0", "id": 1,
        "result": { "protocolVersion": 1,
            "agentCapabilities": { "session": { "resume": true, "cancel": true, "close": true } } }
    });
    AttachRunner::spawn_fake(
        WireKind::JsonlRpc,
        hearth_agent_wire::JsonlRpcTransport::encode_jsonl(&init).expect("encode"),
    )
}
