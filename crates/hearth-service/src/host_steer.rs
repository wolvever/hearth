//! Service Host-wake splice: HTTP/WS steer → [`wake_each_kept_through_host_wake`].
//!
//! Closes the production hole between #46–#68 kernel/wire proofs and the
//! HTTP-visible orch path. Keep recipe stays a Host ticket per Agent; wires
//! are a HostAttach / RemintSession map keyed by Agent (Fake / Loopback OK).
//! SoftExpiring / fleet / ToolResult parent / v2 / durable EventLog parked.

use std::collections::HashMap;

use hearth::{AgentId, Event, Place, SessionId, Wake};
use hearth_agent_wire::{
    transport_for_wire, wake_each_kept_through_host_wake, AttachRunner, HostRemintFullArgs,
    KeptWakeWire, OldWire, RemintSession, RpcId, RunnerTransport, WakeEachKeptThroughHost,
    WakeEachKeptThroughHostArgs, WakeThroughError, WireKind,
};

use crate::AppState;

/// One kept Agent's Host wire held by the service for steer / remint.
///
/// Host supplies Fake or Real [`AttachRunner`]s; Must floors use Fake.
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
