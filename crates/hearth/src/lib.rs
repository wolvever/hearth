//! Session-first kernel: hosts map onto six names — [`User`], [`Agent`],
//! [`Session`], [`Binding`], [`Event`], [`Place`] — with [`Store`]/[`InMemory`] as persistence,
//! [`Runtime`] as the keeper that remints Bindings, [`Session::surface`] as a view
//! of the same log (compacted ranges omitted), and [`Provisioner`] to record a
//! binding without starting a CLI. Occupancy is session membership; ask/decide
//! and config live as events or identity maps, not extra types. Channel, Thread,
//! Issue, and Squad stay out of the kernel.
//! A Session may hold many [`Place`] locators (not Environment). Bindings may share `sandbox_id`. Claude Tag channels and Multica issues are sessions in adapters. [`FakeSandbox`] is host-side demo state. [`PlaceMemory`] is Place-backed durable memory (files under a Place claim; not EventLog compaction). [`Runtime`] is the same class as Store / Provisioner / Host / FakeSandbox / PlaceMemory — not a seventh name.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;
use uuid::Uuid;

mod sandbox;
mod place;
mod place_memory;
mod host;
mod runtime;
pub use place::{Place, PlaceAttach, PlaceOs, PlaceProvider};
pub use place_memory::{
    cross_agent_write, skill_path, topic_path, InjectedContext, MemoryFiles, MemoryPolicy,
    PlaceMemory, WorkingState, AGENTS_MD, HANDOFF_MD, MEMORY_MD, SUMMARY_BYTE_CAP, USER_MD,
};
pub use sandbox::FakeSandbox;
pub use host::{
    grok_api_key, Host, HostKind, HostTicket, ClaudeCode as ClaudeCodeHost, Codex as CodexHost,
    Dsh as DshHost, Fx as FxHost, Pi as PiHost, OpenCode as OpenCodeHost, Goose as GooseHost,
    Grok as GrokHost, GrokBuild as GrokBuildHost,
};
pub use runtime::{HolderId, KeepSpec, Liveness, Runtime, SessionTurnLease, StepOutcome, Wake};

// --- identities ---

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        pub struct $name(pub Uuid);

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}

id_type!(UserId);
id_type!(AgentId);
id_type!(SessionId);
id_type!(BindingId);
id_type!(EventId);
id_type!(PlaceId);

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Member {
    User(UserId),
    Agent(AgentId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct User {
    pub id: UserId,
    pub name: String,
    /// User-owned defaults. Session overrides stay on the session log.
    pub config: HashMap<String, String>,
}

/// Long-lived identity. Not a process or a run.
/// Agent-owned defaults. Session overrides are `ConfigSet` events on the log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Agent {
    pub id: AgentId,
    pub name: String,
    pub instructions: String,
    pub config: HashMap<String, String>,
}

/// Disposable runtime attachment. Native resume ids live here, never on `Session`.
/// Optional [`Binding::agent`] names whose runtime this is so [`Session::leave`]
/// can unbind only that agent's Binding. Shared place is `sandbox_id`, not a
/// sixth type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Binding {
    pub id: BindingId,
    pub kind: String,
    pub native_resume_id: Option<String>,
    pub sandbox_id: Option<String>,
    pub agent: Option<AgentId>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub id: EventId,
    /// Optional last-read / compact position. NOT assigned on append.
    pub seq: Option<u64>,
    /// Turn this event was appended under, if a turn is open.
    pub turn: Option<u64>,
    pub ts: u64,
    pub body: EventBody,
}

/// Why a kept session woke. Not model-visible. Prefer Timer/Trigger on the log;
/// UserQuery is recorded as [`EventBody::UserMessage`] alone.
#[derive(Clone, Debug, PartialEq)]
pub enum WakeSource {
    UserQuery,
    Timer,
    Trigger { name: String },
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventBody {
    MemberJoin { member: Member },
    MemberLeave { member: Member },
    ConfigSet { key: String, value: String },
    UserMessage { user: UserId, text: String },
    AgentMessage { agent: AgentId, text: String },
    AgentThink { agent: AgentId, text: String },
    ToolCall { agent: AgentId, name: String, input: String },
    ToolResult { agent: AgentId, name: String, output: String },
    AskUser { agent: AgentId, prompt: String },
    PermissionAsked { agent: AgentId, request: String },
    PermissionDecided { request: String, allowed: bool, by: UserId },
    TurnStart { agent: AgentId },
    TurnEnd { agent: AgentId },
    /// Intra-turn durable step checkpoint. Not model-visible.
    StepCompleted {
        turn_id: u64,
        step_id: String,
        result: String,
    },
    BindingAttached { binding: BindingId },
    BindingReleased { binding: BindingId },
    /// Replaces `[start, end]` (inclusive EventId range in log order) in [`Session::surface`]. Full log keeps both.
    /// Product compact path: [`Session::compact_with_handoff`] (PreCompactHandoff).
    Compact { start: EventId, end: EventId, summary: String },
    /// Runtime wake marker (Timer / Trigger). Not model-visible.
    Wake { source: WakeSource },
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("unknown user {0:?}")]
    UnknownUser(UserId),
    #[error("unknown agent {0:?}")]
    UnknownAgent(AgentId),
    #[error("unknown session {0:?}")]
    UnknownSession(SessionId),
    #[error("unknown binding {0:?}")]
    UnknownBinding(BindingId),
    #[error("already a member")]
    AlreadyMember,
    #[error("not a member")]
    NotMember,
    #[error("store lock poisoned")]
    Poisoned,
    #[error("place path missing: {0}")]
    PlaceMissing(String),
    #[error("place provider mismatch")]
    PlaceProviderMismatch,
    #[error("cannot migrate place provider")]
    PlaceProviderSwap,
    #[error("unknown place {0:?}")]
    UnknownPlace(PlaceId),
    /// Place-backed memory write without a claim (keep-as-claim).
    #[error("no place claim")]
    NoPlaceClaim,
    /// Another Binding already holds the Place memory claim.
    #[error("place claim split-brain")]
    SplitBrain,
    /// Compact path ran without flushing `memory/handoff.md`.
    #[error("compact lost working state")]
    CompactLostWorkingState,
    /// On-demand topic/skill is not in the Place memory files.
    #[error("memory topic missing: {0}")]
    TopicMissing(String),
    #[error("session {0:?} is not kept")]
    NotKept(SessionId),
    #[error("prompt waiting ({0:?})")]
    Waiting(WaitReason),
    #[error("turn lease lost")]
    TurnLeaseLost,
    #[error("session not owned by this holder")]
    SessionNotOwned,
    #[error("live turn is still open")]
    LiveTurnOpen,
    #[error("no interrupted turn to resume")]
    TurnNotOpen,
}

/// Why [`Error::Waiting`] — return value, not a Queue type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitReason {
    NoBinding,
    TurnOpen,
    LivenessUnknown,
    AlreadyLive,
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Model-visible bodies: messages and tool I/O. Think / permission / binding stay in the log.
pub fn is_model_visible(body: &EventBody) -> bool {
    matches!(
        body,
        EventBody::UserMessage { .. }
            | EventBody::AgentMessage { .. }
            | EventBody::ToolCall { .. }
            | EventBody::ToolResult { .. }
            | EventBody::Compact { .. }
    )
}

/// Project the model-visible view. Compacted EventId ranges (log order) are omitted; the Compact event remains.
pub fn surface_of(events: &[Event]) -> Vec<Event> {
    let mut hidden: HashSet<EventId> = HashSet::new();
    for e in events {
        if let EventBody::Compact { start, end, .. } = e.body {
            let start_idx = events.iter().position(|x| x.id == start);
            let end_idx = events.iter().position(|x| x.id == end);
            match (start_idx, end_idx) {
                (Some(s), Some(en)) if s <= en => {
                    for ev in &events[s..=en] {
                        hidden.insert(ev.id);
                    }
                }
                _ => {}
            }
        }
    }
    events
        .iter()
        .filter(|e| !hidden.contains(&e.id) && is_model_visible(&e.body))
        .cloned()
        .collect()
}

/// Last `TurnStart` without a later `TurnEnd`, walking the EventLog in order.
pub fn open_turn_of(events: &[Event]) -> Option<(u64, AgentId)> {
    let mut open = None;
    for e in events {
        match &e.body {
            EventBody::TurnStart { agent } => {
                open = e.turn.map(|t| (t, *agent));
            }
            EventBody::TurnEnd { .. } => {
                open = None;
            }
            _ => {}
        }
    }
    open
}

/// Memoized payload of `StepCompleted` for `(turn_id, step_id)`, if present.
pub fn step_result_of(events: &[Event], turn_id: u64, step_id: &str) -> Option<String> {
    events.iter().find_map(|e| match &e.body {
        EventBody::StepCompleted {
            turn_id: t,
            step_id: s,
            result,
        } if *t == turn_id && s == step_id => Some(result.clone()),
        _ => None,
    })
}

// --- store ---

struct SessionData {
    members: HashSet<Member>,
    events: Vec<Event>,
    bindings: HashMap<BindingId, Binding>,
    places: HashMap<PlaceId, Place>,
    next_turn_id: u64,
    next_seq_id: u64,
    last_read: Option<u64>,
    current_turn: Option<u64>,
}

impl SessionData {
    fn new() -> Self {
        Self {
            members: HashSet::new(),
            events: Vec::new(),
            bindings: HashMap::new(),
            places: HashMap::new(),
            next_turn_id: 1,
            next_seq_id: 1,
            last_read: None,
            current_turn: None,
        }
    }

    fn push(&mut self, body: EventBody) -> Event {
        let event = Event {
            id: EventId::new(),
            seq: None,
            turn: self.current_turn,
            ts: now_ts(),
            body,
        };
        self.events.push(event.clone());
        event
    }
}

struct Inner {
    users: HashMap<UserId, User>,
    agents: HashMap<AgentId, Agent>,
    sessions: HashMap<SessionId, SessionData>,
}

/// Persistence for the five product concepts. Not itself a product concept.
pub trait Store {
    fn create_user(&self, name: String) -> User;
    fn create_agent(&self, name: String, instructions: String) -> Agent;
    fn create_session(&self) -> Session;
    fn session(&self, id: SessionId) -> Result<Session>;
    fn user(&self, id: UserId) -> Result<User>;
    fn agent(&self, id: AgentId) -> Result<Agent>;
}

/// In-memory kernel store.
#[derive(Clone)]
pub struct InMemory {
    inner: Arc<Mutex<Inner>>,
}

impl Default for InMemory {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemory {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                users: HashMap::new(),
                agents: HashMap::new(),
                sessions: HashMap::new(),
            })),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>> {
        self.inner.lock().map_err(|_| Error::Poisoned)
    }

    pub fn create_user(&self, name: impl Into<String>) -> User {
        let user = User {
            id: UserId::new(),
            name: name.into(),
            config: HashMap::new(),
        };
        let mut g = self.inner.lock().expect("store lock");
        g.users.insert(user.id, user.clone());
        user
    }

    pub fn create_agent(&self, name: impl Into<String>, instructions: impl Into<String>) -> Agent {
        let agent = Agent {
            id: AgentId::new(),
            name: name.into(),
            instructions: instructions.into(),
            config: HashMap::new(),
        };
        let mut g = self.inner.lock().expect("store lock");
        g.agents.insert(agent.id, agent.clone());
        agent
    }

    /// A session is a joinable room. It exists with zero bindings.
    pub fn create_session(&self) -> Session {
        let id = SessionId::new();
        let mut g = self.inner.lock().expect("store lock");
        g.sessions.insert(id, SessionData::new());
        Session {
            id,
            store: self.clone(),
        }
    }

    pub fn session(&self, id: SessionId) -> Result<Session> {
        let g = self.lock()?;
        if g.sessions.contains_key(&id) {
            Ok(Session {
                id,
                store: self.clone(),
            })
        } else {
            Err(Error::UnknownSession(id))
        }
    }

    pub fn user(&self, id: UserId) -> Result<User> {
        self.lock()?
            .users
            .get(&id)
            .cloned()
            .ok_or(Error::UnknownUser(id))
    }

    pub fn agent(&self, id: AgentId) -> Result<Agent> {
        self.lock()?
            .agents
            .get(&id)
            .cloned()
            .ok_or(Error::UnknownAgent(id))
    }

    pub fn set_user_config(&self, id: UserId, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let mut g = self.lock()?;
        let user = g.users.get_mut(&id).ok_or(Error::UnknownUser(id))?;
        user.config.insert(key.into(), value.into());
        Ok(())
    }

    pub fn set_agent_config(&self, id: AgentId, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let mut g = self.lock()?;
        let agent = g.agents.get_mut(&id).ok_or(Error::UnknownAgent(id))?;
        agent.config.insert(key.into(), value.into());
        Ok(())
    }
}

impl Store for InMemory {
    fn create_user(&self, name: String) -> User {
        InMemory::create_user(self, name)
    }

    fn create_agent(&self, name: String, instructions: String) -> Agent {
        InMemory::create_agent(self, name, instructions)
    }

    fn create_session(&self) -> Session {
        InMemory::create_session(self)
    }

    fn session(&self, id: SessionId) -> Result<Session> {
        InMemory::session(self, id)
    }

    fn user(&self, id: UserId) -> Result<User> {
        InMemory::user(self, id)
    }

    fn agent(&self, id: AgentId) -> Result<Agent> {
        InMemory::agent(self, id)
    }
}

/// Creates a [`Binding`] without starting a host CLI. Not a product concept.
pub trait Provisioner {
    fn provision(
        &self,
        session: &Session,
        kind: &str,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    ) -> Result<Binding>;
}

/// Records a Binding on the session only — no process spawn.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullProvisioner;

impl Provisioner for NullProvisioner {
    fn provision(
        &self,
        session: &Session,
        kind: &str,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    ) -> Result<Binding> {
        session.bind(kind, native_resume_id, sandbox_id)
    }
}

/// Handle to a joinable room: membership + append-only event log + bindings.
#[derive(Clone)]
pub struct Session {
    id: SessionId,
    store: InMemory,
}

impl Session {
    pub fn id(&self) -> SessionId {
        self.id
    }

    fn read<T>(&self, f: impl FnOnce(&SessionData) -> T) -> Result<T> {
        let g = self.store.lock()?;
        let data = g
            .sessions
            .get(&self.id)
            .ok_or(Error::UnknownSession(self.id))?;
        Ok(f(data))
    }

    fn write<T>(&self, f: impl FnOnce(&mut SessionData) -> Result<T>) -> Result<T> {
        let mut g = self.store.lock()?;
        let data = g
            .sessions
            .get_mut(&self.id)
            .ok_or(Error::UnknownSession(self.id))?;
        f(data)
    }

    pub fn join(&self, member: Member) -> Result<Event> {
        let mut g = self.store.lock()?;
        match &member {
            Member::User(id) if !g.users.contains_key(id) => return Err(Error::UnknownUser(*id)),
            Member::Agent(id) if !g.agents.contains_key(id) => return Err(Error::UnknownAgent(*id)),
            _ => {}
        }
        let data = g
            .sessions
            .get_mut(&self.id)
            .ok_or(Error::UnknownSession(self.id))?;
        if !data.members.insert(member.clone()) {
            return Err(Error::AlreadyMember);
        }
        Ok(data.push(EventBody::MemberJoin { member }))
    }

    pub fn leave(&self, member: Member) -> Result<Event> {
        self.write(|data| {
            if !data.members.remove(&member) {
                return Err(Error::NotMember);
            }
            if let Member::Agent(agent) = &member {
                let agent = *agent;
                let drop: Vec<BindingId> = data
                    .bindings
                    .values()
                    .filter(|b| b.agent == Some(agent))
                    .map(|b| b.id)
                    .collect();
                for bid in drop {
                    data.bindings.remove(&bid);
                    data.push(EventBody::BindingReleased { binding: bid });
                }
            }
            Ok(data.push(EventBody::MemberLeave { member }))
        })
    }

    pub fn append(&self, body: EventBody) -> Result<Event> {
        self.write(|data| Ok(data.push(body)))
    }

    pub fn bind(
        &self,
        kind: impl Into<String>,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    ) -> Result<Binding> {
        let host = Host::from_bind(kind, native_resume_id, sandbox_id);
        self.attach_binding(host.into_binding(None))
    }

    /// Bind a host runtime for one agent. Join does not call this.
    pub fn bind_agent(
        &self,
        agent: AgentId,
        kind: impl Into<String>,
        native_resume_id: Option<String>,
        sandbox_id: Option<String>,
    ) -> Result<Binding> {
        let host = Host::from_bind(kind, native_resume_id, sandbox_id);
        self.attach_binding(host.into_binding(Some(agent)))
    }

    /// Typed path: `session.bind_host(Some(id), HostKind::Goose.host(None, Some("box".into())))`.
    pub fn bind_host(&self, agent: Option<AgentId>, host: Host) -> Result<Binding> {
        self.attach_binding(host.into_binding(agent))
    }

    fn attach_binding(&self, binding: Binding) -> Result<Binding> {
        self.write(|data| {
            data.bindings.insert(binding.id, binding.clone());
            data.push(EventBody::BindingAttached {
                binding: binding.id,
            });
            Ok(binding)
        })
    }

    pub fn ask_user(&self, agent: AgentId, prompt: impl Into<String>) -> Result<Event> {
        self.append(EventBody::AskUser {
            agent,
            prompt: prompt.into(),
        })
    }

    pub fn ask_permission(&self, agent: AgentId, request: impl Into<String>) -> Result<Event> {
        self.append(EventBody::PermissionAsked {
            agent,
            request: request.into(),
        })
    }

    /// Record a decision. The deciding user must currently occupy this session.
    pub fn decide_permission(
        &self,
        request: impl Into<String>,
        allowed: bool,
        by: UserId,
    ) -> Result<Event> {
        self.require_user(by)?;
        self.append(EventBody::PermissionDecided {
            request: request.into(),
            allowed,
            by,
        })
    }

    /// Occupancy: only a joined User may steer with a UserMessage.
    pub fn user_message(&self, user: UserId, text: impl Into<String>) -> Result<Event> {
        self.require_user(user)?;
        self.append(EventBody::UserMessage {
            user,
            text: text.into(),
        })
    }

    fn require_user(&self, user: UserId) -> Result<()> {
        let g = self.store.lock()?;
        if !g.users.contains_key(&user) {
            return Err(Error::UnknownUser(user));
        }
        let data = g
            .sessions
            .get(&self.id)
            .ok_or(Error::UnknownSession(self.id))?;
        if !data.members.contains(&Member::User(user)) {
            return Err(Error::NotMember);
        }
        Ok(())
    }

    /// Session override. Last write wins per key when reduced via [`Self::config`].
    pub fn set_config(&self, key: impl Into<String>, value: impl Into<String>) -> Result<Event> {
        self.append(EventBody::ConfigSet {
            key: key.into(),
            value: value.into(),
        })
    }

    /// Agent defaults, then last-write-wins `ConfigSet` from this session log.
    pub fn config(&self) -> Result<HashMap<String, String>> {
        let g = self.store.lock()?;
        let data = g
            .sessions
            .get(&self.id)
            .ok_or(Error::UnknownSession(self.id))?;
        let mut out = HashMap::new();
        for member in &data.members {
            if let Member::Agent(id) = member {
                if let Some(agent) = g.agents.get(id) {
                    for (k, v) in &agent.config {
                        out.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        for e in &data.events {
            if let EventBody::ConfigSet { key, value } = &e.body {
                out.insert(key.clone(), value.clone());
            }
        }
        Ok(out)
    }

    /// Release a binding. The session and its log remain.
    pub fn unbind(&self, binding_id: BindingId) -> Result<Event> {
        self.write(|data| {
            if data.bindings.remove(&binding_id).is_none() {
                return Err(Error::UnknownBinding(binding_id));
            }
            Ok(data.push(EventBody::BindingReleased {
                binding: binding_id,
            }))
        })
    }

    /// Append a compaction marker. Replaced EventIds stay in [`Self::events`], drop from [`Self::surface`].
    ///
    /// This is the EventLog primitive. The product path is
    /// [`Self::compact_with_handoff`]: flush Place `handoff.md` first, then
    /// store a summary that *references* Place paths (see `NaiveCompactWithoutHandoff`).
    pub fn compact(&self, start: EventId, end: EventId, summary: impl Into<String>) -> Result<Event> {
        self.append(EventBody::Compact {
            start,
            end,
            summary: summary.into(),
        })
    }

    /// PreCompactHandoff then EventLog compact. Working state is written to
    /// Place `memory/handoff.md`; the Compact summary points at those paths.
    /// Does not remint Binding. Place must already be attached to this session.
    pub fn compact_with_handoff(
        &self,
        memory: &PlaceMemory,
        binding: BindingId,
        start: EventId,
        end: EventId,
        working: Option<WorkingState>,
    ) -> Result<Event> {
        crate::place_memory::pre_compact_handoff(self, memory, binding, start, end, working)
    }

    pub fn turn_start(&self, agent: AgentId) -> Result<Event> {
        self.write(|data| {
            let turn = data.next_turn_id;
            data.next_turn_id += 1;
            data.current_turn = Some(turn);
            Ok(data.push(EventBody::TurnStart { agent }))
        })
    }

    pub fn turn_end(&self, agent: AgentId) -> Result<Event> {
        self.write(|data| {
            let event = data.push(EventBody::TurnEnd { agent });
            data.current_turn = None;
            Ok(event)
        })
    }

    pub fn mark_read(&self) -> Result<u64> {
        self.write(|data| {
            let seq = data.next_seq_id;
            data.next_seq_id += 1;
            data.last_read = Some(seq);
            Ok(seq)
        })
    }

    pub fn last_read(&self) -> Result<Option<u64>> {
        self.read(|data| data.last_read)
    }

    pub fn next_turn_id(&self) -> Result<u64> {
        self.read(|data| data.next_turn_id)
    }

    pub fn next_seq_id(&self) -> Result<u64> {
        self.read(|data| data.next_seq_id)
    }

    pub fn current_turn(&self) -> Result<Option<u64>> {
        self.read(|data| data.current_turn)
    }

    /// Last `TurnStart` without a later `TurnEnd` on this session's EventLog.
    pub fn open_turn(&self) -> Result<Option<u64>> {
        self.read(|data| open_turn_of(&data.events).map(|(t, _)| t))
    }

    /// Set [`Self::current_turn`] from the EventLog (`TurnStart` without `TurnEnd`).
    pub fn restore_open_turn(&self) -> Result<Option<u64>> {
        self.write(|data| {
            data.current_turn = open_turn_of(&data.events).map(|(t, _)| t);
            Ok(data.current_turn)
        })
    }

    /// Memoized `StepCompleted` payload for `(turn_id, step_id)`, if present.
    pub fn step_result(&self, turn_id: u64, step_id: &str) -> Result<Option<String>> {
        self.read(|data| step_result_of(&data.events, turn_id, step_id))
    }

    pub fn events(&self) -> Result<Vec<Event>> {
        self.read(|data| data.events.clone())
    }

    /// Model-visible view of the log (not a second store).
    pub fn surface(&self) -> Result<Vec<Event>> {
        Ok(surface_of(&self.events()?))
    }

    pub fn members(&self) -> Result<Vec<Member>> {
        self.read(|data| data.members.iter().cloned().collect())
    }

    pub fn bindings(&self) -> Result<Vec<Binding>> {
        self.read(|data| data.bindings.values().cloned().collect())
    }

    /// All attached [`Place`] locators. Survive unbind of all Bindings.
    pub fn places(&self) -> Result<Vec<Place>> {
        self.read(|data| data.places.values().cloned().collect())
    }

    /// Lookup one attached [`Place`] by id.
    pub fn place(&self, id: PlaceId) -> Result<Option<Place>> {
        self.read(|data| data.places.get(&id).cloned())
    }

    /// Fail-closed attach, keyed by [`PlaceId`]. Same provider+instance reuses that row.
    /// Provider swap is only when rewriting an existing id. New ids may differ in provider.
    pub fn attach_place(&self, place: Place) -> Result<Place> {
        place.validate()?;
        let mut place = place;
        self.write(|data| {
            if let Some(existing) = data.places.get(&place.id) {
                if existing.provider != place.provider {
                    return Err(Error::PlaceProviderSwap);
                }
                data.places.insert(place.id, place.clone());
                return Ok(place);
            }
            if let Some((&eid, _)) = data.places.iter().find(|(_, p)| {
                p.provider == place.provider && p.instance == place.instance
            }) {
                place.id = eid;
                data.places.insert(eid, place.clone());
                return Ok(place);
            }
            data.places.insert(place.id, place.clone());
            Ok(place)
        })
    }

    /// Remove one locator. Session, log, and other places remain.
    pub fn detach_place(&self, id: PlaceId) -> Result<Place> {
        self.write(|data| data.places.remove(&id).ok_or(Error::UnknownPlace(id)))
    }

    /// Distinct live sandbox ids on current Bindings. Empty after last unbind
    /// of those Bindings — the host place (see [`FakeSandbox`]) may still exist.
    pub fn live_sandbox_ids(&self) -> Result<Vec<String>> {
        let mut ids: Vec<String> = self
            .bindings()?
            .into_iter()
            .filter_map(|b| b.sandbox_id)
            .collect();
        ids.sort();
        ids.dedup();
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_survives_all_bindings_released() {
        let store = InMemory::new();
        let session = store.create_session();
        let id = session.id();
        let a = session.bind("cli", Some("resume-1".into()), None).unwrap();
        let b = session.bind("sandbox", None, Some("box-1".into())).unwrap();
        session.unbind(a.id).unwrap();
        session.unbind(b.id).unwrap();
        assert!(session.bindings().unwrap().is_empty());
        assert!(store.session(id).is_ok());
        assert!(session
            .events()
            .unwrap()
            .iter()
            .any(|e| matches!(e.body, EventBody::BindingReleased { .. })));
    }

    #[test]
    fn same_agent_in_two_sessions() {
        let store = InMemory::new();
        let agent = store.create_agent("hearth", "stay on kernel names");
        let s1 = store.create_session();
        let s2 = store.create_session();
        s1.join(Member::Agent(agent.id)).unwrap();
        s2.join(Member::Agent(agent.id)).unwrap();
        assert!(s1.members().unwrap().contains(&Member::Agent(agent.id)));
        assert!(s2.members().unwrap().contains(&Member::Agent(agent.id)));
        assert_ne!(s1.id(), s2.id());
    }

    #[test]
    fn two_users_one_agent_late_joiner_sees_log() {
        let store = InMemory::new();
        let u1 = store.create_user("cheng");
        let u2 = store.create_user("guest");
        let agent = store.create_agent("scribe", "note everything");
        let session = store.create_session();
        session.join(Member::User(u1.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        session
            .append(EventBody::UserMessage {
                user: u1.id,
                text: "hello".into(),
            })
            .unwrap();
        session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "hi".into(),
            })
            .unwrap();
        session.join(Member::User(u2.id)).unwrap();
        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "hello"
        )));
        assert_eq!(session.members().unwrap().len(), 3);
    }

    #[test]
    fn permission_asked_survives_unbind() {
        let store = InMemory::new();
        let agent = store.create_agent("cli", "ask first");
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        let binding = session
            .bind("paseo-cli", Some("native-abc".into()), None)
            .unwrap();
        session
            .append(EventBody::PermissionAsked {
                agent: agent.id,
                request: "git push".into(),
            })
            .unwrap();
        session.unbind(binding.id).unwrap();
        assert!(session.bindings().unwrap().is_empty());
        assert!(session.events().unwrap().iter().any(|e| matches!(
            &e.body,
            EventBody::PermissionAsked { request, .. } if request == "git push"
        )));
    }

    #[test]
    fn native_resume_id_lives_on_binding_not_session() {
        let store = InMemory::new();
        let session = store.create_session();
        let binding = session
            .bind("claude-code", Some("resume-xyz".into()), None)
            .unwrap();
        assert_eq!(binding.native_resume_id.as_deref(), Some("resume-xyz"));
        // Session.id is a hearth identity; it is not the native resume token.
        assert_ne!(format!("{:?}", session.id().0), "resume-xyz");
        assert!(session.bindings().unwrap()[0].native_resume_id.is_some());
    }

    #[test]
    fn surface_is_model_visible_view() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("scribe", "");
        let session = store.create_session();
        session.join(Member::User(user.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        let bind = session.bind("cli", Some("r1".into()), None).unwrap();
        session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "go".into(),
            })
            .unwrap();
        session
            .append(EventBody::AgentThink {
                agent: agent.id,
                text: "hmm".into(),
            })
            .unwrap();
        session
            .append(EventBody::PermissionAsked {
                agent: agent.id,
                request: "rm".into(),
            })
            .unwrap();
        session
            .append(EventBody::ToolCall {
                agent: agent.id,
                name: "ls".into(),
                input: "{}".into(),
            })
            .unwrap();
        session
            .append(EventBody::ToolResult {
                agent: agent.id,
                name: "ls".into(),
                output: "ok".into(),
            })
            .unwrap();
        session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "done".into(),
            })
            .unwrap();
        session.unbind(bind.id).unwrap();

        let surface = session.surface().unwrap();
        assert!(surface.iter().all(|e| is_model_visible(&e.body)));
        assert_eq!(surface.len(), 4);
        assert!(matches!(surface[0].body, EventBody::UserMessage { .. }));
        assert!(matches!(surface[1].body, EventBody::ToolCall { .. }));
        assert!(matches!(surface[2].body, EventBody::ToolResult { .. }));
        assert!(matches!(surface[3].body, EventBody::AgentMessage { .. }));
    }

    #[test]
    fn compact_hides_replaced_range_on_surface() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("scribe", "");
        let session = store.create_session();
        let m1 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "old-a".into(),
            })
            .unwrap();
        let m2 = session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "old-b".into(),
            })
            .unwrap();
        let m3 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "keep".into(),
            })
            .unwrap();
        session.compact(m1.id, m2.id, "earlier chat").unwrap();

        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "old-a"
        )));
        assert!(log
            .iter()
            .any(|e| matches!(e.body, EventBody::Compact { .. })));

        let surface = session.surface().unwrap();
        assert!(!surface.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "old-a"
        )));
        assert!(!surface.iter().any(|e| matches!(
            &e.body,
            EventBody::AgentMessage { text, .. } if text == "old-b"
        )));
        assert!(surface.iter().any(|e| matches!(
            &e.body,
            EventBody::Compact { summary, .. } if summary == "earlier chat"
        )));
        assert!(surface.iter().any(|e| matches!(
            &e.body,
            EventBody::UserMessage { text, .. } if text == "keep"
        )));
        assert_eq!(m3.seq, None);
    }

    #[test]
    fn store_trait_is_implemented_by_in_memory() {
        fn accepts<S: Store>(s: &S) -> Session {
            s.create_session()
        }
        let store = InMemory::new();
        let _ = accepts(&store);
    }

    #[test]
    fn provisioner_creates_binding_without_cli() {
        let store = InMemory::new();
        let session = store.create_session();
        let p = NullProvisioner;
        let b = p
            .provision(&session, HostKind::Codex.as_str(), Some("r".into()), None)
            .unwrap();
        assert_eq!(b.kind, "codex");
        assert_eq!(b.native_resume_id.as_deref(), Some("r"));
        assert_eq!(session.bindings().unwrap().len(), 1);
    }

    #[test]
    fn permission_decide_survives_unbind_rebind() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("cli", "ask first");
        let session = store.create_session();
        session.join(Member::User(user.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        let first = session.bind(HostKind::ClaudeCode, None, None).unwrap();
        session.ask_user(agent.id, "ship it?").unwrap();
        session.ask_permission(agent.id, "git push").unwrap();
        session.unbind(first.id).unwrap();
        let second = NullProvisioner
            .provision(&session, HostKind::ClaudeCode.as_str(), None, None)
            .unwrap();
        let decided = session
            .decide_permission("git push", true, user.id)
            .unwrap();
        assert!(matches!(
            decided.body,
            EventBody::PermissionDecided {
                allowed: true,
                by,
                ..
            } if by == user.id
        ));
        let log = session.events().unwrap();
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::PermissionAsked { request, .. } if request == "git push"
        )));
        assert!(log.iter().any(|e| matches!(
            &e.body,
            EventBody::AskUser { prompt, .. } if prompt == "ship it?"
        )));
        assert_eq!(second.kind, "claude_code");
        assert_eq!(session.bindings().unwrap().len(), 1);
    }

    #[test]
    fn session_config_survives_unbind() {
        let store = InMemory::new();
        let agent = store.create_agent("scribe", "");
        store.set_agent_config(agent.id, "model", "sonnet").unwrap();
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        let binding = session.bind(HostKind::Pi, None, None).unwrap();
        assert_eq!(session.config().unwrap().get("model").map(String::as_str), Some("sonnet"));
        session.set_config("model", "opus").unwrap();
        session.set_config("temp", "0").unwrap();
        session.set_config("temp", "1").unwrap();
        session.unbind(binding.id).unwrap();
        let cfg = session.config().unwrap();
        assert_eq!(cfg.get("model").map(String::as_str), Some("opus"));
        assert_eq!(cfg.get("temp").map(String::as_str), Some("1"));
        assert!(session.bindings().unwrap().is_empty());
        assert!(store.session(session.id()).is_ok());
    }

    #[test]
    fn two_users_steer_one_session() {
        // Claude Tag "channel" is this Session — not a sixth concept.
        let store = InMemory::new();
        let a = store.create_user("cheng");
        let b = store.create_user("guest");
        let outsider = store.create_user("lurk");
        let agent = store.create_agent("tag", "");
        let session = store.create_session();
        session.join(Member::User(a.id)).unwrap();
        session.join(Member::User(b.id)).unwrap();
        session.join(Member::Agent(agent.id)).unwrap();
        session.user_message(a.id, "from cheng").unwrap();
        session.user_message(b.id, "from guest").unwrap();
        session.ask_permission(agent.id, "rm").unwrap();
        session.decide_permission("rm", false, a.id).unwrap();
        session.decide_permission("rm", true, b.id).unwrap();
        assert!(matches!(
            session.user_message(outsider.id, "nope").unwrap_err(),
            Error::NotMember
        ));
        assert!(matches!(
            session.decide_permission("rm", true, outsider.id).unwrap_err(),
            Error::NotMember
        ));
        let log = session.events().unwrap();
        assert_eq!(
            log.iter()
                .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn append_does_not_bump_next_seq_id() {
        let store = InMemory::new();
        let session = store.create_session();
        assert_eq!(session.next_seq_id().unwrap(), 1);
        let a = session
            .append(EventBody::UserMessage {
                user: UserId::new(),
                text: "one".into(),
            })
            .unwrap();
        let b = session
            .append(EventBody::UserMessage {
                user: UserId::new(),
                text: "two".into(),
            })
            .unwrap();
        assert_eq!(session.next_seq_id().unwrap(), 1);
        assert_eq!(a.seq, None);
        assert_eq!(b.seq, None);
        assert_ne!(a.id, b.id);
    }

    #[test]
    fn turn_start_stamps_and_turn_end_clears() {
        let store = InMemory::new();
        let agent = store.create_agent("cli", "");
        let session = store.create_session();
        assert_eq!(session.next_turn_id().unwrap(), 1);
        let start = session.turn_start(agent.id).unwrap();
        assert_eq!(session.next_turn_id().unwrap(), 2);
        assert_eq!(start.turn, Some(1));
        assert!(matches!(start.body, EventBody::TurnStart { .. }));
        let mid = session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "during".into(),
            })
            .unwrap();
        assert_eq!(mid.turn, Some(1));
        let end = session.turn_end(agent.id).unwrap();
        assert_eq!(end.turn, Some(1));
        assert!(matches!(end.body, EventBody::TurnEnd { .. }));
        let later = session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "after".into(),
            })
            .unwrap();
        assert_eq!(later.turn, None);
        assert_eq!(session.next_turn_id().unwrap(), 2);
    }

    #[test]
    fn mark_read_only_bumps_seq_cursor() {
        let store = InMemory::new();
        let session = store.create_session();
        session
            .append(EventBody::UserMessage {
                user: UserId::new(),
                text: "x".into(),
            })
            .unwrap();
        assert_eq!(session.next_seq_id().unwrap(), 1);
        assert_eq!(session.last_read().unwrap(), None);
        let seq = session.mark_read().unwrap();
        assert_eq!(seq, 1);
        assert_eq!(session.last_read().unwrap(), Some(1));
        assert_eq!(session.next_seq_id().unwrap(), 2);
        assert_eq!(session.next_turn_id().unwrap(), 1);
        let seq2 = session.mark_read().unwrap();
        assert_eq!(seq2, 2);
        assert_eq!(session.last_read().unwrap(), Some(2));
        assert_eq!(session.next_seq_id().unwrap(), 3);
    }

    #[test]
    fn compact_by_event_id_hides_range_on_surface() {
        let store = InMemory::new();
        let user = store.create_user("cheng");
        let agent = store.create_agent("scribe", "");
        let session = store.create_session();
        let m1 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "old-a".into(),
            })
            .unwrap();
        let m2 = session
            .append(EventBody::AgentMessage {
                agent: agent.id,
                text: "old-b".into(),
            })
            .unwrap();
        let m3 = session
            .append(EventBody::UserMessage {
                user: user.id,
                text: "keep".into(),
            })
            .unwrap();
        session.compact(m1.id, m2.id, "earlier chat").unwrap();
        let surface = session.surface().unwrap();
        assert!(!surface.iter().any(|e| e.id == m1.id || e.id == m2.id));
        assert!(surface.iter().any(|e| e.id == m3.id));
        assert!(surface.iter().any(|e| matches!(
            &e.body,
            EventBody::Compact { summary, .. } if summary == "earlier chat"
        )));
    }

    #[test]
    fn host_kind_goose_is_typed_host() {
        let host = HostKind::Goose.host(Some("resume".into()), Some("box".into()));
        assert!(matches!(host, Host::Goose(_)));
        assert_eq!(host.kind_str(), "goose");
        assert_eq!(host.native_resume_id(), Some("resume"));
        assert_eq!(host.sandbox_id(), Some("box"));
    }

    #[test]
    fn host_kind_grok_is_typed_host() {
        let host = HostKind::Grok.host(Some("resume".into()), Some("box".into()));
        assert!(matches!(host, Host::Grok(_)));
        assert_eq!(host.kind_str(), "grok");
        assert_eq!(host.native_resume_id(), Some("resume"));
        assert_eq!(host.sandbox_id(), Some("box"));
        let through = Host::from_bind("grok", None, None);
        assert!(matches!(through, Host::Grok(_)));
    }

    #[test]
    fn host_kind_grok_build_is_typed_host() {
        let host = HostKind::GrokBuild.host(Some("resume".into()), Some("box".into()));
        assert!(matches!(host, Host::GrokBuild(_)));
        assert_eq!(host.kind_str(), "grok_build");
        assert_eq!(host.native_resume_id(), Some("resume"));
        assert_eq!(host.sandbox_id(), Some("box"));
        let through = Host::from_bind("grok_build", None, None);
        assert!(matches!(through, Host::GrokBuild(_)));
        assert!(!matches!(through, Host::Grok(_)));
    }

    #[test]
    fn bind_agent_goose_kind_string_goes_through_host() {
        let store = InMemory::new();
        let agent = store.create_agent("multi", "");
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        let through = Host::from_bind(HostKind::Goose, None, Some("sb".into()));
        assert!(matches!(through, Host::Goose(_)));
        let binding = session
            .bind_agent(agent.id, HostKind::Goose, None, Some("sb".into()))
            .unwrap();
        assert_eq!(binding.kind, "goose");
        assert_eq!(binding.sandbox_id.as_deref(), Some("sb"));
        assert_eq!(binding.agent, Some(agent.id));
    }

    #[test]
    fn bind_host_opencode_works() {
        let store = InMemory::new();
        let agent = store.create_agent("oc", "");
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        let binding = session
            .bind_host(
                Some(agent.id),
                Host::OpenCode(OpenCodeHost {
                    native_resume_id: None,
                    sandbox_id: Some("box".into()),
                }),
            )
            .unwrap();
        assert_eq!(binding.kind, "opencode");
        assert_eq!(binding.sandbox_id.as_deref(), Some("box"));
        assert_eq!(binding.agent, Some(agent.id));
    }

    #[test]
    fn unknown_string_becomes_host_other() {
        let host = Host::from_bind("paseo-cli", Some("n".into()), None);
        assert!(matches!(
            &host,
            Host::Other { kind, native_resume_id, sandbox_id: None }
                if kind == "paseo-cli" && native_resume_id.as_deref() == Some("n")
        ));
        assert_eq!(host.kind_str(), "paseo-cli");
        let binding = host.into_binding(None);
        assert_eq!(binding.kind, "paseo-cli");
        assert_eq!(binding.native_resume_id.as_deref(), Some("n"));
    }
}


/// Shared-place tests (option A / B, join/leave).
///
/// JUDGE — Environment as a sixth type? **No.** These tests express a place
/// that is shared and outlives agents without lying:
/// - share = two Bindings, same `sandbox_id` (option A co-tenant);
/// - outlive = [`FakeSandbox`] files keyed by that id after leave/unbind;
/// - a later Binding with the same id sees the files (option B proxy).
/// Adding `Environment` would only be required if we had to fake a live Binding
/// or store files on Session. We do neither. Host maps (Paseo folder, MA/Tag
/// sandbox, Cursor VM/pool) stay behind the id string.
#[cfg(test)]
mod environment {
    use super::*;

    #[test]
    fn option_a_two_bindings_same_sandbox() {
        let store = InMemory::new();
        let claude = store.create_agent("claude", "");
        let codex = store.create_agent("codex", "");
        let session = store.create_session();
        session.join(Member::Agent(claude.id)).unwrap();
        session.join(Member::Agent(codex.id)).unwrap();
        let place = Some("sb-shared".to_string());
        let b_cc = session
            .bind_agent(claude.id, HostKind::ClaudeCode, Some("cc-resume".into()), place.clone())
            .unwrap();
        let b_cx = session
            .bind_agent(codex.id, HostKind::Codex, Some("cx-resume".into()), place.clone())
            .unwrap();
        assert_eq!(b_cc.sandbox_id, b_cx.sandbox_id);
        assert_eq!(b_cc.kind, "claude_code");
        assert_eq!(b_cx.kind, "codex");
        assert_eq!(session.bindings().unwrap().len(), 2);
        assert_eq!(session.live_sandbox_ids().unwrap(), vec!["sb-shared".to_string()]);

        session.leave(Member::Agent(claude.id)).unwrap();
        let left = session.bindings().unwrap();
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].id, b_cx.id);
        assert_eq!(left[0].sandbox_id.as_deref(), Some("sb-shared"));
        assert!(store.session(session.id()).is_ok());
        assert!(session.members().unwrap().contains(&Member::Agent(codex.id)));
        assert!(!session.members().unwrap().contains(&Member::Agent(claude.id)));
    }

    #[test]
    fn option_b_tool_proxy_same_sandbox_different_resume() {
        let store = InMemory::new();
        let claude = store.create_agent("claude", "");
        let codex = store.create_agent("codex", "");
        let session = store.create_session();
        session.join(Member::Agent(claude.id)).unwrap();
        session.join(Member::Agent(codex.id)).unwrap();
        let sid = "sb-proxy";
        let b_cc = session
            .bind_agent(claude.id, HostKind::ClaudeCode, Some("native-cc".into()), Some(sid.into()))
            .unwrap();
        let b_cx = session
            .bind_agent(codex.id, HostKind::Codex, Some("native-cx".into()), Some(sid.into()))
            .unwrap();
        assert_ne!(b_cc.native_resume_id, b_cx.native_resume_id);
        assert_eq!(b_cc.sandbox_id, b_cx.sandbox_id);

        let mut place = FakeSandbox::new();
        session
            .append(EventBody::ToolCall {
                agent: claude.id,
                name: "write".into(),
                input: "path=/note.txt\nbody=from-claude".into(),
            })
            .unwrap();
        session
            .append(EventBody::ToolCall {
                agent: codex.id,
                name: "write".into(),
                input: "path=/other.txt\nbody=from-codex".into(),
            })
            .unwrap();

        for e in session.events().unwrap() {
            if let EventBody::ToolCall { agent, name, input } = e.body {
                if name != "write" {
                    continue;
                }
                let bid = session
                    .bindings()
                    .unwrap()
                    .into_iter()
                    .find(|b| b.agent == Some(agent))
                    .unwrap();
                let out = place.apply_write(bid.sandbox_id.as_deref().unwrap(), &input);
                session
                    .append(EventBody::ToolResult {
                        agent,
                        name: "write".into(),
                        output: out,
                    })
                    .unwrap();
            }
        }
        assert_eq!(place.read(sid, "/note.txt"), Some("from-claude"));
        assert_eq!(place.read(sid, "/other.txt"), Some("from-codex"));

        session.leave(Member::Agent(claude.id)).unwrap();
        assert_eq!(session.bindings().unwrap().len(), 1);
        assert_eq!(place.read(sid, "/note.txt"), Some("from-claude"));

        session.unbind(b_cx.id).unwrap();
        assert!(session.bindings().unwrap().is_empty());
        // Host place remains after last Binding; Session remains.
        assert_eq!(place.read(sid, "/note.txt"), Some("from-claude"));
        assert!(store.session(session.id()).is_ok());

        let again = session
            .bind_agent(codex.id, HostKind::Codex, Some("native-cx-2".into()), Some(sid.into()))
            .unwrap();
        assert_eq!(again.sandbox_id.as_deref(), Some(sid));
        assert_eq!(place.read(sid, "/note.txt"), Some("from-claude"));
        assert_eq!(place.read(sid, "/other.txt"), Some("from-codex"));
    }

    #[test]
    fn join_does_not_create_sandbox_leave_does_not_destroy_session() {
        let store = InMemory::new();
        let a1 = store.create_agent("one", "");
        let a2 = store.create_agent("two", "");
        let session = store.create_session();
        assert!(session.bindings().unwrap().is_empty());
        assert!(session.live_sandbox_ids().unwrap().is_empty());

        session.join(Member::Agent(a1.id)).unwrap();
        session.join(Member::Agent(a2.id)).unwrap();
        assert!(session.bindings().unwrap().is_empty());
        assert!(session.live_sandbox_ids().unwrap().is_empty());

        let sid = "sb-later";
        let mut place = FakeSandbox::new();
        let b1 = session
            .bind_agent(a1.id, HostKind::ClaudeCode, None, Some(sid.into()))
            .unwrap();
        let b2 = session
            .bind_agent(a2.id, HostKind::Codex, None, Some(sid.into()))
            .unwrap();
        place.write(sid, "/keep.txt", "stay");

        session.leave(Member::Agent(a1.id)).unwrap();
        assert_eq!(session.bindings().unwrap().len(), 1);
        assert_eq!(session.bindings().unwrap()[0].id, b2.id);
        assert!(store.session(session.id()).is_ok());
        assert_eq!(place.read(sid, "/keep.txt"), Some("stay"));

        session.leave(Member::Agent(a2.id)).unwrap();
        assert!(session.members().unwrap().is_empty());
        assert!(session.bindings().unwrap().is_empty());
        assert!(store.session(session.id()).is_ok());
        // Last agent leave is not a sandbox release.
        assert_eq!(place.read(sid, "/keep.txt"), Some("stay"));

        // Explicit unbind of last live Binding already happened via leave.
        // Dedicated release is host-side:
        place.release(sid);
        assert!(place.read(sid, "/keep.txt").is_none());
        assert!(store.session(session.id()).is_ok());
        let _ = (b1, b2);
    }

    #[test]
    fn new_hosts_are_binding_kinds_not_agent_types() {
        let store = InMemory::new();
        let agent = store.create_agent("multi", "one identity");
        let session = store.create_session();
        session.join(Member::Agent(agent.id)).unwrap();
        let dsh = session
            .bind_agent(agent.id, HostKind::Dsh, None, None)
            .unwrap();
        let oc = session
            .bind_agent(agent.id, HostKind::OpenCode, None, None)
            .unwrap();
        let goose = session
            .bind_agent(agent.id, HostKind::Goose, None, None)
            .unwrap();
        let grok = session
            .bind_agent(agent.id, HostKind::Grok, None, None)
            .unwrap();
        let grok_build = session
            .bind_agent(agent.id, HostKind::GrokBuild, None, None)
            .unwrap();
        assert_eq!(dsh.kind, "dsh");
        assert_eq!(oc.kind, "opencode");
        assert_eq!(goose.kind, "goose");
        assert_eq!(grok.kind, "grok");
        assert_eq!(grok_build.kind, "grok_build");
        assert_eq!(dsh.agent, Some(agent.id));
    }
}
