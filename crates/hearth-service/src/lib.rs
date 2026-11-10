//! Local HTTP + WebSocket for hearth. Bind 0.0.0.0:8787. No public URL is claimed.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use hearth::{
    Event, EventBody, HostKind, InMemory, Member, Place, PlaceAttach, Runtime, SessionId, Wake,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

mod host_steer;
mod probe;
pub use host_steer::{
    AgentHostWire, CompleteToolError, CompleteToolOutcome, HostWireMap, SessionWires, SteerError,
    SteerOutcome,
};
pub use probe::{probe_local, CliStatus};

/// Doorbell only (stream-tails-log): "Session `session`'s EventLog may have
/// grown". No frame rides the channel; the WS stream tails the log by index,
/// so a lagged or cross-Session-flooded receiver just re-reads.
#[derive(Clone)]
struct LiveEvent {
    session: SessionId,
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) runtime: Runtime,
    names: Arc<Mutex<NameMap>>,
    clis: Vec<CliStatus>,
    live: broadcast::Sender<LiveEvent>,
    /// Host wires indexed by Session → per-Session lock
    /// (Host-wire-session-isolation) for
    /// [`wake_each_kept_through_host_wake`] steer (Service-host-wake-each).
    pub(crate) host_wires: Arc<Mutex<HostWireMap>>,
}

#[derive(Default)]
struct NameMap {
    users: HashMap<String, hearth::UserId>,
    agents: HashMap<String, hearth::AgentId>,
}

impl AppState {
    pub fn new(clis: Vec<CliStatus>) -> Self {
        let (live, _) = broadcast::channel(256);
        Self {
            runtime: Runtime::new(),
            names: Arc::new(Mutex::new(NameMap::default())),
            clis,
            live,
            host_wires: Arc::new(Mutex::new(HostWireMap::default())),
        }
    }

    /// Install a Host wire for `(session, agent)` so HTTP/WS steer routes
    /// through [`hearth_agent_wire::wake_each_kept_through_host_wake`].
    pub fn install_host_wire(
        &self,
        session: SessionId,
        agent: hearth::AgentId,
        wire: AgentHostWire,
    ) -> Result<(), StatusCode> {
        // Clone the Session Arc under the outer index lock, drop the outer
        // lock, then take the Session lock: the outer index must never wait
        // on a Session lock (which may be held across `host_wake`).
        let arc = self
            .host_wires
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .session_arc(session);
        arc.lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .insert(agent, wire);
        Ok(())
    }

    /// Override `OldWire` evidence on an installed Host wire (Must remint floors).
    pub fn set_host_old_wire(
        &self,
        session: SessionId,
        agent: hearth::AgentId,
        old_wire: hearth_agent_wire::OldWire,
    ) -> Result<(), StatusCode> {
        let arc = {
            let g = self
                .host_wires
                .lock()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            g.try_session_arc(session).ok_or(StatusCode::NOT_FOUND)?
        };
        let mut sw = arc.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let w = sw.get_mut(agent).ok_or(StatusCode::NOT_FOUND)?;
        w.old_wire = old_wire;
        Ok(())
    }

    fn store(&self) -> &InMemory {
        self.runtime.store()
    }

    /// Ring the Session's doorbell. Called on every exit of every
    /// log-writing handler (Ok and Err): a refused or failed request can
    /// still have appended events.
    fn ring(&self, session: SessionId) {
        let _ = self.live.send(LiveEvent { session });
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/sessions", post(create_session))
        .route("/sessions/{id}", get(get_session))
        .route("/sessions/{id}/join", post(join_session))
        .route("/sessions/{id}/events", get(list_events).post(post_event))
        .route(
            "/sessions/{id}/agents/{agent}/tool-call",
            post(post_tool_call),
        )
        .route(
            "/sessions/{id}/agents/{agent}/tool-result",
            post(post_tool_result),
        )
        .route(
            "/sessions/{id}/agents/{agent}/end-turn",
            post(post_end_turn),
        )
        .route("/sessions/{id}/stream", get(stream_session))
        .route("/agents/local", get(agents_local))
        .with_state(state)
}

#[derive(Deserialize)]
struct CreateBody {
    folder: Option<String>,
}

#[derive(Serialize)]
struct SessionView {
    id: String,
    places: Vec<PlaceView>,
    members: usize,
}

#[derive(Serialize)]
struct PlaceView {
    id: String,
    provider: String,
    instance: String,
    os: String,
    attach: String,
}

fn place_view(p: &Place) -> PlaceView {
    PlaceView {
        id: p.id.0.to_string(),
        provider: match &p.provider {
            hearth::PlaceProvider::LocalDir => "LocalDir".into(),
            hearth::PlaceProvider::Aws => "Aws".into(),
            hearth::PlaceProvider::Azure => "Azure".into(),
            hearth::PlaceProvider::Gcp => "Gcp".into(),
            hearth::PlaceProvider::CursorVm => "CursorVm".into(),
            hearth::PlaceProvider::GrokBox => "GrokBox".into(),
            hearth::PlaceProvider::Other(s) => format!("Other:{s}"),
        },
        instance: p.instance.clone(),
        os: format!("{:?}", p.os),
        attach: format!("{:?}", p.attach),
    }
}

fn sid(id: &str) -> Result<SessionId, StatusCode> {
    Uuid::parse_str(id)
        .map(SessionId)
        .map_err(|_| StatusCode::BAD_REQUEST)
}

async fn create_session(
    State(st): State<AppState>,
    Json(body): Json<CreateBody>,
) -> Result<Json<SessionView>, StatusCode> {
    let session = st.store().create_session();
    if let Some(folder) = body.folder {
        if Path::new(&folder).exists() {
            let _ = session.attach_place(Place::local_dir(folder, PlaceAttach::MustExist));
        }
    }
    Ok(Json(view(&session)))
}

fn view(session: &hearth::Session) -> SessionView {
    SessionView {
        id: session.id().0.to_string(),
        places: session
            .places()
            .ok()
            .unwrap_or_default()
            .iter()
            .map(place_view)
            .collect(),
        members: session.members().map(|m| m.len()).unwrap_or(0),
    }
}

async fn get_session(
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<SessionView>, StatusCode> {
    let id = sid(&id)?;
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(view(&session)))
}

#[derive(Deserialize)]
struct JoinBody {
    user: Option<String>,
    agent: Option<String>,
    /// Optional ParallelSafe path list for an Agent join/keep. Omit/`None`
    /// keeps fail-closed serialize (Service-parallel-safe-declare). Never
    /// inferred from tools.
    parallel_safe: Option<Vec<String>>,
}

/// Production keep stand-in. Grok is a test-only HostKind; do not replace this.
fn keep_standin(st: &AppState, id: SessionId, agent: hearth::AgentId) -> Result<(), StatusCode> {
    st.runtime
        .keep(id, agent, HostKind::Goose.host(None, None))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

/// Shared join used by HTTP POST /join and WS `{"type":"join",...}`.
///
/// `parallel_safe` (optional path list) is applied only on Agent keep via
/// [`Runtime::set_parallel_safe`]. Omit/`None` → fail-closed serialize.
fn apply_join(
    st: &AppState,
    id: SessionId,
    user: Option<String>,
    agent: Option<String>,
    parallel_safe: Option<Vec<String>>,
) -> Result<EventOut, StatusCode> {
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let mut names = st
        .names
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let (ev, keep_agent) = if let Some(name) = user {
        let uid = *names
            .users
            .entry(name.clone())
            .or_insert_with(|| st.store().create_user(name).id);
        (
            session
                .join(Member::User(uid))
                .map_err(|_| StatusCode::CONFLICT)?,
            None,
        )
    } else if let Some(name) = agent {
        let aid = *names
            .agents
            .entry(name.clone())
            .or_insert_with(|| st.store().create_agent(name, String::new()).id);
        (
            session
                .join(Member::Agent(aid))
                .map_err(|_| StatusCode::CONFLICT)?,
            Some(aid),
        )
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };
    drop(names);
    // The join is on the log: ring on every exit from here (Ok and Err).
    let kept: Result<(), StatusCode> = keep_agent.map_or(Ok(()), |aid| {
        keep_standin(st, id, aid)?;
        // Explicit declare only — never inferred from tools (DESIGN).
        if let Some(paths) = parallel_safe {
            st.runtime
                .set_parallel_safe(id, aid, Some(hearth::ParallelSafe::paths(paths)))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
        Ok(())
    });
    st.ring(id);
    kept?;
    Ok(event_out(&ev))
}

async fn join_session(
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<JoinBody>,
) -> Result<Json<SessionView>, StatusCode> {
    let id = sid(&id)?;
    apply_join(&st, id, body.user, body.agent, body.parallel_safe)?;
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(view(&session)))
}

#[derive(Deserialize)]
struct EventIn {
    user: Option<String>,
    message: Option<String>,
    text: Option<String>,
}

#[derive(Clone, Serialize)]
struct EventOut {
    seq: Option<u64>,
    body: String,
}

fn event_out(e: &Event) -> EventOut {
    EventOut {
        seq: e.seq,
        body: match &e.body {
            EventBody::UserMessage { text, .. } => format!("user:{text}"),
            EventBody::MemberJoin { .. } => "join".into(),
            other => format!("{other:?}"),
        },
    }
}

/// Ensure the Session has at least one kept Agent (bootstrap: keep the
/// first Agent member). Returns every kept Agent in keep order so a user
/// message can wake each — not first-only (#47 residual / Wake-each-kept).
fn ensure_kept_agents(st: &AppState, id: SessionId) -> Result<Vec<hearth::AgentId>, StatusCode> {
    let kept = st.runtime.kept_agents(id);
    if !kept.is_empty() {
        return Ok(kept);
    }
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let agent = session
        .members()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .find_map(|m| match m {
            Member::Agent(a) => Some(a),
            Member::User(_) => None,
        });
    let Some(agent) = agent else {
        return Err(StatusCode::CONFLICT);
    };
    keep_standin(st, id, agent)?;
    Ok(vec![agent])
}

/// Shared steer used by HTTP POST /events and WS `{"type":"message",...}`.
///
/// When Host wires are installed for the Session, admit through
/// [`hearth_agent_wire::wake_each_kept_through_host_wake`] (one shared
/// UserMessage; per-Agent `host_wake`; Waiting skips peers). Otherwise the
/// Goose stand-in uses [`Runtime::wake_each_kept`] (ClaimBinding). Wait is
/// 409. Do not append on wait.
fn apply_user_message(
    st: &AppState,
    id: SessionId,
    name: String,
    text: String,
) -> Result<EventOut, StatusCode> {
    let _session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let names = st
        .names
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let uid = *names.users.get(&name).ok_or(StatusCode::NOT_FOUND)?;
    drop(names);
    let _agents = ensure_kept_agents(st, id)?;
    let res = host_steer::steer_each_kept(st, id, Wake::UserQuery { user: uid, text });
    // Ring before matching: a 409/500 steer may have appended (remint
    // Binding events, FailClosed unbinds).
    st.ring(id);
    match res {
        Ok(outcome) => {
            let per_agent = outcome.woke();
            let mut um: Option<EventOut> = None;
            for (_agent, evs) in per_agent {
                for e in evs {
                    if matches!(e.body, EventBody::UserMessage { .. }) {
                        um = Some(event_out(e));
                    }
                }
            }
            um.or_else(|| {
                per_agent
                    .last()
                    .and_then(|(_, evs)| evs.last())
                    .map(event_out)
            })
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)
        }
        Err(SteerError::Waiting(_)) => Err(StatusCode::CONFLICT),
        Err(SteerError::Runtime(hearth::Error::Waiting(_)))
        | Err(SteerError::Runtime(hearth::Error::NotKept(_)))
        | Err(SteerError::Runtime(hearth::Error::AgentNotKept(..))) => Err(StatusCode::CONFLICT),
        Err(SteerError::Runtime(hearth::Error::NotMember))
        | Err(SteerError::Runtime(hearth::Error::UnknownUser(_))) => Err(StatusCode::FORBIDDEN),
        Err(SteerError::Lock) | Err(SteerError::Runtime(_)) | Err(SteerError::Remint(_)) => {
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn post_event(
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
    Json(body): Json<EventIn>,
) -> Result<Json<EventOut>, StatusCode> {
    let id = sid(&id)?;
    let text = body.message.or(body.text).ok_or(StatusCode::BAD_REQUEST)?;
    let name = body.user.ok_or(StatusCode::BAD_REQUEST)?;
    // Host-wire-session-isolation: run blocking host_wake off the async worker.
    let out = tokio::task::spawn_blocking(move || apply_user_message(&st, id, name, text))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;
    Ok(Json(out))
}

#[derive(Deserialize)]
struct ToolCallIn {
    name: String,
    #[serde(default)]
    input: Option<String>,
    /// Client-supplied id when present; else server mints a stable id.
    #[serde(default)]
    tool_call_id: Option<String>,
}

#[derive(Serialize)]
struct ToolCallOut {
    tool_call_id: String,
    /// Opaque path for existing `POST …/tool-result` (no auth Soft).
    callback_url: String,
}

/// POST /sessions/{id}/agents/{agent}/tool-call — Host-owned ToolCall dispatch
/// (RAP fire-and-forget). No wake. No auto turn_end. Completion is
/// [`post_tool_result`].
async fn post_tool_call(
    State(st): State<AppState>,
    AxPath((id, agent_name)): AxPath<(String, String)>,
    Json(body): Json<ToolCallIn>,
) -> Result<Json<ToolCallOut>, StatusCode> {
    let id = sid(&id)?;
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let agent = aid(&st, &agent_name)?;
    // M5: unknown handled by aid; not-kept → 4xx, zero ToolCall.
    if !st.runtime.kept_agents(id).contains(&agent) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tool_call_id = body
        .tool_call_id
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let input = body.input.unwrap_or_else(|| "{}".into());
    match session.append_host_tool_call(agent, &tool_call_id, &body.name, &input) {
        Ok(_ev) => {
            st.ring(id);
            let callback_url = format!("/sessions/{}/agents/{}/tool-result", id.0, agent_name);
            Ok(Json(ToolCallOut {
                tool_call_id,
                callback_url,
            }))
        }
        Err(hearth::HostToolCallError::AlreadyExists) => Err(StatusCode::CONFLICT),
        Err(hearth::HostToolCallError::NotOnSession) => Err(StatusCode::BAD_REQUEST),
    }
}

#[derive(Serialize)]
struct EndTurnOut {
    ended: bool,
}

/// POST /sessions/{id}/agents/{agent}/end-turn — explicit Host yield (RAP).
/// Empty or `{}` body. LiveInTurn → LiveIdle + TurnEnd; LiveIdle/no-open → 409;
/// unknown/not-kept → 4xx. Does not finalize unmatched ToolCalls; does not wake.
async fn post_end_turn(
    State(st): State<AppState>,
    AxPath((id, agent_name)): AxPath<(String, String)>,
    body: Option<Json<serde_json::Value>>,
) -> Result<Json<EndTurnOut>, StatusCode> {
    let _ = body; // empty or {} accepted; no fields required
    let id = sid(&id)?;
    let _session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let agent = aid(&st, &agent_name)?;
    // M5: unknown handled by aid; not-kept → 4xx, zero TurnEnd.
    if !st.runtime.kept_agents(id).contains(&agent) {
        return Err(StatusCode::BAD_REQUEST);
    }
    match st.runtime.end_host_turn(id, agent) {
        Ok(_ev) => {
            st.ring(id);
            Ok(Json(EndTurnOut { ended: true }))
        }
        Err(hearth::Error::TurnNotOpen) => Err(StatusCode::CONFLICT),
        Err(hearth::Error::Waiting(_)) => Err(StatusCode::CONFLICT),
        Err(hearth::Error::NotKept(_)) | Err(hearth::Error::AgentNotKept(..)) => {
            Err(StatusCode::BAD_REQUEST)
        }
        Err(hearth::Error::NotMember) | Err(hearth::Error::UnknownUser(_)) => {
            Err(StatusCode::FORBIDDEN)
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

#[derive(Deserialize)]
struct ToolResultIn {
    tool_call_id: String,
    output: String,
    /// Optional RAP-shaped error flag. Folded into output when true (EventBody
    /// ToolResult has no is_error field — Soft; no schema change).
    #[serde(default)]
    is_error: Option<bool>,
}

#[derive(Serialize)]
struct ToolResultOut {
    applied: bool,
    /// Debug body of the ToolResult Event when applied; empty when not.
    body: Option<String>,
}

/// Resolve Agent by join name (same NameMap as /join).
fn aid(st: &AppState, name: &str) -> Result<hearth::AgentId, StatusCode> {
    let names = st
        .names
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    names.agents.get(name).copied().ok_or(StatusCode::NOT_FOUND)
}

/// POST /sessions/{id}/agents/{agent}/tool-result — Host-owned ToolResult +
/// single-Agent Wake(Trigger { name: "tool_result" }). Never fan-out.
async fn post_tool_result(
    State(st): State<AppState>,
    AxPath((id, agent_name)): AxPath<(String, String)>,
    Json(body): Json<ToolResultIn>,
) -> Result<Json<ToolResultOut>, StatusCode> {
    let id = sid(&id)?;
    let _session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let agent = aid(&st, &agent_name)?;
    let output = if body.is_error.unwrap_or(false) {
        format!("[error] {}", body.output)
    } else {
        body.output
    };
    let tool_call_id = body.tool_call_id.clone();
    // Host-wire-session-isolation: run blocking host_wake off the async worker.
    let st_block = st.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        host_steer::complete_tool_result(&st_block, id, agent, &tool_call_id, &output)
    })
    .await;
    // Ring before matching: a 409/500 RAP may have appended (the ToolResult
    // is appended before the wake, plus any remint Binding events).
    st.ring(id);
    match outcome.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)? {
        Ok(CompleteToolOutcome::Applied { tool_result, .. }) => {
            let out = event_out(&tool_result);
            Ok(Json(ToolResultOut {
                applied: true,
                body: Some(out.body),
            }))
        }
        Ok(CompleteToolOutcome::NotApplied) => Ok(Json(ToolResultOut {
            applied: false,
            body: None,
        })),
        Err(CompleteToolError::Orphan) => Err(StatusCode::BAD_REQUEST),
        Err(CompleteToolError::Waiting(_)) => Err(StatusCode::CONFLICT),
        Err(CompleteToolError::Runtime(hearth::Error::Waiting(_)))
        | Err(CompleteToolError::Runtime(hearth::Error::NotKept(_)))
        | Err(CompleteToolError::Runtime(hearth::Error::AgentNotKept(..))) => {
            Err(StatusCode::CONFLICT)
        }
        Err(CompleteToolError::Lock)
        | Err(CompleteToolError::Runtime(_))
        | Err(CompleteToolError::Remint(_)) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn list_events(
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<Json<Vec<EventOut>>, StatusCode> {
    let id = sid(&id)?;
    let session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let out = session
        .events()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .iter()
        .map(event_out)
        .collect();
    Ok(Json(out))
}

/// Incoming WS JSON text. `type` is `join` or `message` (aliases: `user`, `steer`).
/// HTTP-shaped bodies also work: `{"user":".."}` joins; with `message`/`text`, steers.
#[derive(Deserialize)]
struct WsIn {
    #[serde(rename = "type")]
    kind: Option<String>,
    user: Option<String>,
    agent: Option<String>,
    /// Optional ParallelSafe path list on Agent join (same as HTTP JoinBody).
    parallel_safe: Option<Vec<String>>,
    message: Option<String>,
    text: Option<String>,
}

fn apply_ws_text(st: &AppState, id: SessionId, txt: &str) -> Result<EventOut, StatusCode> {
    let body: WsIn = serde_json::from_str(txt).map_err(|_| StatusCode::BAD_REQUEST)?;
    let kind = body.kind.as_deref().unwrap_or("");
    let text = body.message.or(body.text);
    let steer = matches!(kind, "message" | "user" | "steer") || (kind.is_empty() && text.is_some());
    if steer {
        let name = body.user.ok_or(StatusCode::BAD_REQUEST)?;
        let text = text.ok_or(StatusCode::BAD_REQUEST)?;
        apply_user_message(st, id, name, text)
    } else if kind == "join" || kind.is_empty() {
        apply_join(st, id, body.user, body.agent, body.parallel_safe)
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

async fn stream_session(
    ws: WebSocketUpgrade,
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let id = sid(&id)?;
    let _session = st.store().session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(ws.on_upgrade(move |socket| push_stream(socket, st, id)))
}

async fn push_stream(mut socket: WebSocket, st: AppState, id: SessionId) {
    // Subscribe before the first read, so no ring is missed; replay is
    // just the first tail.
    let mut rx = st.live.subscribe();
    let mut cursor = 0usize;
    if tail_log(&mut socket, &st, id, &mut cursor).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(txt))) => {
                        let st2 = st.clone();
                        let txt2 = txt.to_string();
                        match tokio::task::spawn_blocking(move || apply_ws_text(&st2, id, &txt2)).await {
                            Ok(Ok(_ev)) => {}
                            Ok(Err(code)) => {
                                let err = serde_json::json!({ "error": code.as_u16() }).to_string();
                                if socket.send(Message::Text(err.into())).await.is_err() {
                                    return;
                                }
                            }
                            Err(_) => {
                                let err = serde_json::json!({ "error": 500 }).to_string();
                                if socket.send(Message::Text(err.into())).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if socket.send(Message::Pong(p)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => return,
                }
            }
            live = rx.recv() => {
                match live {
                    Ok(live) if live.session == id => {
                        if tail_log(&mut socket, &st, id, &mut cursor).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    // A lag may have swallowed this Session's ring (own burst
                    // or another Session's flood): re-read, never skip.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if tail_log(&mut socket, &st, id, &mut cursor).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

/// Send `events[cursor..]` in log order and advance `cursor` per frame sent.
/// The EventLog is append-only, so an index is a complete, duplicate-free
/// cursor (`seq` is untouched: it stays `mark_read`-only).
async fn tail_log(
    socket: &mut WebSocket,
    st: &AppState,
    id: SessionId,
    cursor: &mut usize,
) -> Result<(), ()> {
    let Ok(session) = st.store().session(id) else {
        return Ok(());
    };
    let Ok(events) = session.events() else {
        return Ok(());
    };
    for e in events.iter().skip(*cursor) {
        let txt = serde_json::to_string(&event_out(e)).unwrap_or_default();
        socket
            .send(Message::Text(txt.into()))
            .await
            .map_err(|_| ())?;
        *cursor += 1;
    }
    Ok(())
}

async fn agents_local(State(st): State<AppState>) -> Json<Vec<CliStatus>> {
    Json(st.clis.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use futures_util::{SinkExt, StreamExt};
    use std::future::IntoFuture;
    use tokio_tungstenite::tungstenite::Message as WsMsg;
    use tower::ServiceExt;

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, String) {
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn session_roundtrip_and_local_agents() {
        let app = router(AppState::new(probe_local()));
        let dir = std::env::temp_dir();
        let body = serde_json::json!({ "folder": dir.to_string_lossy() }).to_string();
        let (st, txt) = call(
            app.clone(),
            Request::post("/sessions")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
        let id = v["id"].as_str().unwrap().to_string();
        assert_eq!(v["places"][0]["provider"], "LocalDir");

        let (st, txt) = call(
            app.clone(),
            Request::get(format!("/sessions/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(txt.contains(&id));

        let (st, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/join"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let (st, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/join"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"agent":"scribe"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);

        let (st, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"hello"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);

        let (st, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"again"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::CONFLICT);

        let (st, txt) = call(
            app.clone(),
            Request::get(format!("/sessions/{id}/events"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(txt.contains("hello"));
        assert!(!txt.contains("again"));

        let (st, txt) = call(
            app,
            Request::get("/agents/local").body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(txt.contains("rustc"));
        assert!(txt.contains("cargo"));
        assert!(txt.contains("claude"));
        assert!(txt.contains("codex"));
        assert!(txt.contains("cursor-agent"));
        assert!(txt.contains("agent"));
    }

    /// Wake-each-kept service floor: two kept ParallelSafe Agents both
    /// receive a user message (TurnStart each; one UserMessage). A
    /// first-only `wake` mutant fails this.
    #[tokio::test]
    async fn user_message_wakes_every_kept_agent() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();

        apply_join(&st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            &st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            &st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2, "join keeps each Agent");

        let app = router(st.clone());
        let (code, body) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"hi-both"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert!(body.contains("hi-both"));

        let session = st.store().session(sid).unwrap();
        for a in &agents {
            assert!(
                session.current_turn_of(*a).unwrap().is_some(),
                "kept Agent {a:?} must receive wake (first-only mutant fails)"
            );
        }
        let ums = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count();
        assert_eq!(ums, 1, "one shared UserMessage");
    }

    #[tokio::test]
    async fn missing_folder_is_session_without_place() {
        let app = router(AppState::new(vec![]));
        let body = serde_json::json!({ "folder": "/no/such/hearth/folder" }).to_string();
        let (st, txt) = call(
            app,
            Request::post("/sessions")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
        assert_eq!(v["places"].as_array().map(|a| a.len()), Some(0));
    }

    #[tokio::test]
    async fn two_clients_live_stream_and_http_still_works() {
        let app = router(AppState::new(vec![]));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());

        let create = http(addr, "POST", "/sessions", Some(r#"{"folder":null}"#)).await;
        assert_eq!(create.0, 200);
        let v: serde_json::Value = serde_json::from_str(&create.1).unwrap();
        let id = v["id"].as_str().unwrap().to_string();

        let j = http(
            addr,
            "POST",
            &format!("/sessions/{id}/join"),
            Some(r#"{"user":"cheng"}"#),
        )
        .await;
        assert_eq!(j.0, 200);
        let j2 = http(
            addr,
            "POST",
            &format!("/sessions/{id}/join"),
            Some(r#"{"user":"guest"}"#),
        )
        .await;
        assert_eq!(j2.0, 200);
        let ja = http(
            addr,
            "POST",
            &format!("/sessions/{id}/join"),
            Some(r#"{"agent":"scribe"}"#),
        )
        .await;
        assert_eq!(ja.0, 200);

        let url = format!("ws://{addr}/sessions/{id}/stream");
        let (mut a, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut b, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        let _ = a.next().await;
        let _ = a.next().await;
        let _ = b.next().await;
        let _ = b.next().await;

        let posted = http(
            addr,
            "POST",
            &format!("/sessions/{id}/events"),
            Some(r#"{"user":"guest","message":"steer-from-second"}"#),
        )
        .await;
        assert_eq!(posted.0, 200);

        expect_text(&mut a, "steer-from-second").await;
        expect_text(&mut b, "steer-from-second").await;

        let listed = http(addr, "GET", &format!("/sessions/{id}/events"), None).await;
        assert_eq!(listed.0, 200);
        assert!(listed.1.contains("steer-from-second"));

        let _ = a.close(None).await;
        let _ = b.close(None).await;
    }

    #[tokio::test]
    async fn two_ws_clients_steer_each_other() {
        let app = router(AppState::new(vec![]));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());

        let create = http(addr, "POST", "/sessions", Some(r#"{"folder":null}"#)).await;
        assert_eq!(create.0, 200);
        let v: serde_json::Value = serde_json::from_str(&create.1).unwrap();
        let id = v["id"].as_str().unwrap().to_string();

        let url = format!("ws://{addr}/sessions/{id}/stream");
        let (mut a, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut b, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

        a.send(WsMsg::Text(r#"{"type":"join","user":"cheng"}"#.into()))
            .await
            .unwrap();
        expect_text(&mut a, "join").await;
        expect_text(&mut b, "join").await;

        b.send(WsMsg::Text(r#"{"type":"join","user":"guest"}"#.into()))
            .await
            .unwrap();
        expect_text(&mut a, "join").await;
        expect_text(&mut b, "join").await;

        a.send(WsMsg::Text(r#"{"type":"join","agent":"scribe"}"#.into()))
            .await
            .unwrap();
        expect_text(&mut a, "join").await;
        expect_text(&mut b, "join").await;

        a.send(WsMsg::Text(
            r#"{"type":"message","user":"cheng","message":"steer-from-a"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut a, "steer-from-a").await;
        expect_text(&mut b, "steer-from-a").await;

        b.send(WsMsg::Text(
            r#"{"user":"guest","message":"steer-from-b"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut b, "409").await;

        let listed = http(addr, "GET", &format!("/sessions/{id}/events"), None).await;
        assert_eq!(listed.0, 200);
        assert!(listed.1.contains("steer-from-a"));
        assert!(!listed.1.contains("steer-from-b"));

        let posted = http(
            addr,
            "POST",
            &format!("/sessions/{id}/events"),
            Some(r#"{"user":"cheng","message":"http-still"}"#),
        )
        .await;
        assert_eq!(posted.0, 409);

        let _ = a.close(None).await;
        let _ = b.close(None).await;
    }

    /// Host-wire-session-isolation: short outer lookup, then Session lock.
    fn session_wires_arc(
        st: &AppState,
        sid: SessionId,
    ) -> std::sync::Arc<std::sync::Mutex<SessionWires>> {
        st.host_wires.lock().unwrap().session_arc(sid)
    }

    /// Resume-capable LiveCaps seed used by Host-wire Soft floors (#69+).
    fn resume_capable_caps() -> hearth_agent_wire::LiveCaps {
        hearth_agent_wire::LiveCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        }
    }

    /// Join two Agents with optional ParallelSafe path lists (product join
    /// path), then install Host Bindings + RemintSession + Fake wires.
    /// `None` paths → undeclared (fail-closed serialize).
    /// `caps` seeds Binding.live_caps via RemintSession::on_host_session
    /// (Service-livecaps-refuse can pass no-resume / load-only).
    fn install_two_host_wires_with_declare(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
        paths_a: Option<Vec<String>>,
        paths_b: Option<Vec<String>>,
        caps: hearth_agent_wire::LiveCaps,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        install_two_host_wires_with_declare_caps(st, sid, lost, paths_a, paths_b, caps, caps)
    }

    /// Per-Agent Binding.live_caps (Service-wake-fanout-skip-retry mixed floor).
    fn install_two_host_wires_with_declare_caps(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
        paths_a: Option<Vec<String>>,
        paths_b: Option<Vec<String>>,
        caps_a: hearth_agent_wire::LiveCaps,
        caps_b: hearth_agent_wire::LiveCaps,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::RemintSession;

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        // Service-parallel-safe-declare: product path declares via join body,
        // not Runtime::set_parallel_safe from the test.
        apply_join(st, sid, None, Some("alpha".into()), paths_a).unwrap();
        apply_join(st, sid, None, Some("beta".into()), paths_b).unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();

        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);

        for (ag, resume_id, caps) in [(a, "S-a", caps_a), (b, "S-b", caps_b)] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            assert_eq!(
                remint.binding_live_caps(),
                Some(caps),
                "Binding.live_caps must carry install seed"
            );
            let wire = match &lost {
                Some(w) => AgentHostWire::fake_lost(remint, Some(place.clone()), w.clone()),
                None => AgentHostWire::fake_live(remint, Some(place.clone())),
            };
            st.install_host_wire(sid, ag, wire).unwrap();
        }
        (a, b, place)
    }

    /// Service-host-wake-each Must floor helper: two ParallelSafe Agents with
    /// Host Bindings + RemintSession + Fake wires installed on AppState.
    /// Declares disjoint paths via the join product path. Resume-capable caps.
    fn install_two_parallel_safe_host_wires(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        install_two_parallel_safe_host_wires_with_caps(st, sid, lost, resume_capable_caps())
    }

    /// Service-livecaps-refuse: same as [`install_two_parallel_safe_host_wires`]
    /// but with caller-chosen Binding.live_caps (no-resume / load-only Softs).
    fn install_two_parallel_safe_host_wires_with_caps(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
        caps: hearth_agent_wire::LiveCaps,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        install_two_parallel_safe_host_wires_with_per_agent_caps(st, sid, lost, caps, caps)
    }

    /// Service-wake-fanout-skip-retry: per-Agent caps (A no-resume, B resume).
    fn install_two_parallel_safe_host_wires_with_per_agent_caps(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
        caps_a: hearth_agent_wire::LiveCaps,
        caps_b: hearth_agent_wire::LiveCaps,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        install_two_host_wires_with_declare_caps(
            st,
            sid,
            lost,
            Some(vec!["agent-0".into()]),
            Some(vec!["agent-1".into()]),
            caps_a,
            caps_b,
        )
    }

    /// Must: HTTP steer through host_wake fan-out → both ParallelSafe Agents
    /// LiveInTurn; one shared UserMessage. First-only mutant fails.
    #[tokio::test]
    async fn http_steer_through_host_wake_both_parallel_safe_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"hi-host"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert!(body.contains("hi-host"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Agent A LiveInTurn (first-only mutant fails)"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "Agent B LiveInTurn (first-only mutant fails)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let ums = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count();
        assert_eq!(ums, 1, "one shared UserMessage");
    }

    /// Must: Fake proven-lost wires → HTTP steer remints both Agents
    /// (mint+1 + session/resume each); no load/new; independent lease/fence;
    /// no cross-Agent Binding steal.
    #[tokio::test]
    async fn http_steer_through_host_wake_remints_both_independent_leases_fences() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );

        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"remint-both"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("remint-both"));

        let session = st.store().session(sid).unwrap();
        assert!(session.current_turn_of(a).unwrap().is_some());
        assert!(session.current_turn_of(b).unwrap().is_some());
        let after_mints = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints,
            before_mints + 2,
            "exactly one mint per Agent (remint)"
        );

        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_ne!(bid_a, bid_a_before, "A reminted");
        assert_ne!(bid_b, bid_b_before, "B reminted");
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");

        // Attach per-Agent leases onto wake-opened turns (compose begin_turn
        // after wake — same as Concurrent-*-remint floors).
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("ha"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("hb"))
            .expect("lease B");
        assert_ne!(ta, tb);
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "ha", ta, fa), (b, "hb", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("live lease");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }
        // A's fence raise must not steal B's lease/fence.
        st.runtime.end_turn(sid, "ha".into(), fa, true).unwrap();
        let (ta2, fa2, _) = st
            .runtime
            .resume_interrupted_turn(sid, a, "ha2".into())
            .unwrap();
        assert_eq!(ta2, ta);
        assert!(fa2 > fa, "A's own fence must rise on resume");
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap().expect("B live");
        assert_eq!(
            (
                lease_b.agent,
                lease_b.holder,
                lease_b.turn_id,
                lease_b.fence
            ),
            (b, hearth::HolderId::new("hb"), tb, fb),
            "turn_lease_of(B).fence pinned after A's raise — no steal"
        );

        // session/resume each; never load/new
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for agent in [a, b] {
            let w = wires.get_mut(agent).unwrap();
            // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
            let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            assert!(
                raw.contains("session/resume"),
                "Agent {agent:?} must session/resume; got {raw}"
            );
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "no load/new on {agent:?}: {raw}"
            );
        }
    }

    /// Must (Service-livecaps-refuse + Service-wake-fanout-skip-retry): both
    /// Agents Binding.live_caps no resume; Fake proven-lost → all-refuse typed
    /// ResumeNotSupported (SteerError::Remint / HTTP 500); each Agent
    /// FailClosed-skip (no mint/spawn/load/new/resume).
    #[tokio::test]
    async fn http_steer_livecaps_no_resume_refuses_without_spawn() {
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintError, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let no_resume = WireCaps {
            resume: false,
            load_session: false,
            cancel: true,
            close: true,
        };
        let (a, b, _) = install_two_parallel_safe_host_wires_with_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
            no_resume,
        );
        assert!(!no_resume.allows_resume());

        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();

        // Typed refuse on the same path HTTP uses (steer_each_kept).
        let uid = {
            let names = st.names.lock().unwrap();
            *names.users.get("cheng").unwrap()
        };
        let typed = host_steer::steer_each_kept(
            &st,
            sid,
            Wake::UserQuery {
                user: uid,
                text: "refuse-no-resume".into(),
            },
        );
        assert!(
            matches!(
                typed,
                Err(SteerError::Remint(RemintError::ResumeNotSupported))
            ),
            "typed ResumeNotSupported; got Ok/other Err"
        );

        // HTTP mapping: Remint → 500 (StdoutEof twin seed; product POST path).
        let st2 = AppState::new(vec![]);
        let session2 = st2.store().create_session();
        let sid2 = session2.id();
        let id2 = sid2.0.to_string();
        let _ = install_two_parallel_safe_host_wires_with_caps(
            &st2,
            sid2,
            Some(hearth_agent_wire::OldWire::StdoutEof),
            no_resume,
        );
        let app = router(st2.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id2}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"refuse-http"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(
            code,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Remint maps to HTTP 500; body={body}"
        );

        // Side effects on the typed-refuse Session: no mint, empty new stdin,
        // never load/new/resume; FailClosed observe pin.
        let after_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(after_mints, before_mints, "no Binding mint on refuse");
        // FailClosed-skip unbinds so AlreadyBound does not turn_start on a
        // dead Binding (Service-wake-fanout-skip-retry).
        let _ = (bid_a_before, bid_b_before);
        assert!(
            st.runtime.binding_for(sid, a).is_err() && st.runtime.binding_for(sid, b).is_err(),
            "all-refuse unbinds both Agents"
        );

        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        // Fan-out continues across caps-refuse: every kept Agent is host_wake'd
        // and FailClosed-skipped (tip abort-on-first mutant fails).
        for agent in [a, b] {
            let w = wires.get_mut(agent).unwrap();
            assert!(
                w.new.stdin_log().unwrap().is_empty(),
                "no spawn/initialize on new wire for {agent:?}: {:?}",
                std::str::from_utf8(w.new.stdin_log().unwrap())
            );
            let old_raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            assert!(
                !old_raw.contains("session/load")
                    && !old_raw.contains("session/new")
                    && !old_raw.contains("session/resume"),
                "never load/new/resume on {agent:?}: {old_raw}"
            );
            assert!(
                w.remint.observe().iter().any(
                    |e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported"
                ),
                "{agent:?} must FailClosed ResumeNotSupported"
            );
        }
    }

    /// Must (Service-livecaps-refuse): load-only Binding (resume:false,
    /// load_session:true) → typed LoadFallbackBlocked; never session/load;
    /// no mint / new-wire spawn. HTTP Remint → 500.
    #[tokio::test]
    async fn http_steer_livecaps_load_only_refuses_never_session_load() {
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintError, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let load_only = WireCaps {
            resume: false,
            load_session: true,
            cancel: false,
            close: false,
        };
        let (a, b, _) = install_two_parallel_safe_host_wires_with_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(0)),
            load_only,
        );
        assert!(!load_only.allows_resume());

        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        let uid = {
            let names = st.names.lock().unwrap();
            *names.users.get("cheng").unwrap()
        };
        let typed = host_steer::steer_each_kept(
            &st,
            sid,
            Wake::UserQuery {
                user: uid,
                text: "refuse-load-only".into(),
            },
        );
        assert!(
            matches!(
                typed,
                Err(SteerError::Remint(RemintError::LoadFallbackBlocked))
            ),
            "typed LoadFallbackBlocked; got Ok/other Err"
        );

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"user":"cheng","message":"refuse-load-http"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "body={body}");

        let after_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints, before_mints,
            "no Binding mint on load-only refuse"
        );

        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for agent in [a, b] {
            let w = wires.get_mut(agent).unwrap();
            assert!(
                w.new.stdin_log().unwrap().is_empty(),
                "no spawn/initialize on new wire for {agent:?}"
            );
            let old_raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            assert!(
                !old_raw.contains("session/load"),
                "never session/load on {agent:?}: {old_raw}"
            );
            assert!(
                !old_raw.contains("session/new") && !old_raw.contains("session/resume"),
                "never new/resume on {agent:?}: {old_raw}"
            );
            assert!(
                w.remint.observe().iter().any(|e| {
                    e.kind == RemintEventKind::FailClosed && e.text == "LoadFallbackBlocked"
                }),
                "{agent:?} must FailClosed LoadFallbackBlocked"
            );
        }
    }

    /// Must (Service-wake-fanout-skip-retry): A no-resume + Fake lost; B
    /// resume-capable + Fake lost; one HTTP steer → A FailClosed (no mint /
    /// spawn/load); B remints mint+1 + session/resume; HTTP 200 (not
    /// Session-wide Remint→500); tip abort-on-first mutant fails.
    #[tokio::test]
    async fn http_steer_caps_refuse_skips_peer_still_remints() {
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let no_resume = WireCaps {
            resume: false,
            load_session: false,
            cancel: true,
            close: true,
        };
        let resume = resume_capable_caps();
        let (a, b, _) = install_two_parallel_safe_host_wires_with_per_agent_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
            no_resume,
            resume,
        );
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"skip-peer"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(
            code,
            StatusCode::OK,
            "peer remint must not Session-wide Remint→500; body={body}"
        );
        assert!(body.contains("skip-peer"), "body={body}");

        let after_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(after_mints, before_mints + 1, "exactly one mint (B)");
        assert!(
            st.runtime.binding_for(sid, a).is_err(),
            "A unbound after FailClosed-skip"
        );
        assert_ne!(
            st.runtime.binding_for(sid, b).unwrap(),
            bid_b_before,
            "B reminted"
        );

        let session = st.store().session(sid).unwrap();
        assert!(session.current_turn_of(a).unwrap().is_none());
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "B LiveInTurn; tip abort-on-first fails"
        );
        let ums = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count();
        assert_eq!(ums, 1, "one shared UserMessage");

        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        let w_a = wires.get_mut(a).unwrap();
        assert!(
            w_a.remint
                .observe()
                .iter()
                .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported"),
            "A FailClosed"
        );
        assert!(w_a.new.stdin_log().unwrap().is_empty(), "A no spawn");
        let w_b = wires.get_mut(b).unwrap();
        // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
        let raw_b = std::str::from_utf8(w_b.old.stdin_log().unwrap()).unwrap();
        assert!(raw_b.contains("session/resume"), "B resume: {raw_b}");
        assert!(!raw_b.contains("session/load") && !raw_b.contains("session/new"));
    }

    /// Service-real-cli-concurrent-remint: two ParallelSafe Agents with Real
    /// old wires probed to Exited (no Fake Exited injection), mid-turn so
    /// cancel-before-reattach fires, Fake new wires for resume capture.
    fn install_two_parallel_safe_real_exited_host_wires(
        st: &AppState,
        sid: SessionId,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintSession};

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();

        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);

        let caps = WireCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        };
        // Distinct exit codes mirror wire #64 (7 / 8).
        for (ag, resume_id, code) in [(a, "S-a", 7), (b, "S-b", 8)] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            // Mid-turn so cancel-before-reattach fires (#43 / #64).
            remint.begin_turn(&format!("mid-{resume_id}"));
            let wire = AgentHostWire::real_exited(remint, Some(place.clone()), code);
            assert!(
                matches!(wire.old_wire, hearth_agent_wire::OldWire::Exited(c) if c == code),
                "Real probe must yield Exited({code}), got {:?}",
                wire.old_wire
            );
            st.install_host_wire(sid, ag, wire).unwrap();
        }
        (a, b, place)
    }

    /// Must (Service-real-cli-concurrent-remint): Real CLI Exited on both
    /// ParallelSafe Agents → one HTTP steer remints both (mint+1 +
    /// session/resume each, stable sessionId); cancel-before-reattach; no
    /// load/new; independent lease/fence; no cross-Agent Binding steal; one
    /// shared UserMessage. No Fake Exited injection.
    #[tokio::test]
    async fn http_steer_through_host_wake_real_cli_exit_remints_both_independent_leases_fences() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_real_exited_host_wires(&st, sid);

        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            assert!(
                wires.get_mut(a).unwrap().remint.turn_in_flight(),
                "A mid-turn before steer"
            );
            assert!(
                wires.get_mut(b).unwrap().remint.turn_in_flight(),
                "B mid-turn before steer"
            );
        }

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"hi-both-real"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("hi-both-real"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Agent A LiveInTurn (first-only mutant fails)"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "Agent B LiveInTurn (first-only mutant fails)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let after_mints = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints,
            before_mints + 2,
            "exactly one mint per Agent (remint)"
        );

        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_ne!(bid_a, bid_a_before, "A reminted");
        assert_ne!(bid_b, bid_b_before, "B reminted");
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");

        let events = session.events().unwrap();
        let ums: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .collect();
        assert_eq!(ums.len(), 1, "one shared UserMessage");
        assert!(matches!(
            &ums[0].body,
            EventBody::UserMessage { text, .. } if text == "hi-both-real"
        ));

        // Attach per-Agent leases onto wake-opened turns; raw pins (not is_none).
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("ha"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("hb"))
            .expect("lease B");
        assert_ne!(ta, tb);
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "ha", ta, fa), (b, "hb", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("live lease");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }
        // A's fence raise must not steal B's lease/fence.
        st.runtime.end_turn(sid, "ha".into(), fa, true).unwrap();
        let (ta2, fa2, _) = st
            .runtime
            .resume_interrupted_turn(sid, a, "ha2".into())
            .unwrap();
        assert_eq!(ta2, ta);
        assert!(fa2 > fa, "A's own fence must rise on resume");
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap().expect("B live");
        assert_eq!(
            (
                lease_b.agent,
                lease_b.holder,
                lease_b.turn_id,
                lease_b.fence
            ),
            (b, hearth::HolderId::new("hb"), tb, fb),
            "turn_lease_of(B).fence pinned after A's raise — no steal"
        );

        // New wires: session/resume stable id; never load/new; mid-turn cleared
        // (cancel-before-reattach path under remint). Resume ids from install.
        let expected = [(a, "S-a"), (b, "S-b")];
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(agent).unwrap();
            assert!(
                !w.remint.turn_in_flight(),
                "Agent {agent:?} turn cleared after remint (cancel-before-reattach)"
            );
            // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
            let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            let frames: Vec<serde_json::Value> = raw
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert!(
                frames.len() >= 2,
                "Agent {agent:?} expected initialize+resume; got {raw}"
            );
            assert_eq!(frames[0]["method"], "initialize", "Agent {agent:?}");
            assert_eq!(
                frames[1]["method"], "session/resume",
                "Agent {agent:?} must session/resume; got {raw}"
            );
            assert_eq!(
                frames[1]["params"]["sessionId"], expected_sid,
                "Agent {agent:?} stable sessionId"
            );
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "no load/new on {agent:?}: {raw}"
            );
        }
    }

    /// Service-real-cli-concurrent-permission-remint: extend #72 Real Exited
    /// install with mid-turn pending asks (A controller unparented; B
    /// `parent_tool_call_id` via Session/Host ask helpers — not Real CLI
    /// emitting asks). Real probe Exited each (no Fake Exited); Fake new
    /// wires for resume capture. Prefer extending #72 scaffolding.
    fn install_two_parallel_safe_real_exited_permission_host_wires(
        st: &AppState,
        sid: SessionId,
    ) -> (
        hearth::AgentId,
        hearth::AgentId,
        hearth::Place,
        hearth_agent_wire::runner::PermissionRef,
        hearth_agent_wire::runner::PermissionRef,
    ) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, PermissionOption, RemintSession, RpcId};

        fn perm_opts() -> Vec<PermissionOption> {
            vec![
                PermissionOption {
                    option_id: "allow-once".into(),
                    name: "Allow".into(),
                    kind: "allow_once".into(),
                },
                PermissionOption {
                    option_id: "reject-once".into(),
                    name: "Reject".into(),
                    kind: "reject_once".into(),
                },
            ]
        }

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();

        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);

        let caps = WireCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        };
        // Distinct exit codes mirror wire #64 / #66 / #72 (7 / 8).
        let mut ask_a = None;
        let mut ask_b = None;
        for (ag, resume_id, code) in [(a, "S-a", 7), (b, "S-b", 8)] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            // Mid-turn + pending asks (Session/Host helpers — not Real CLI).
            remint.begin_turn(&format!("mid-{resume_id}"));
            if ag == a {
                let ask = remint
                    .request_permission(RpcId::Num(1), Some("call_a"), "git log", perm_opts())
                    .unwrap();
                assert_eq!(ask.parent_tool_call_id, None);
                assert_eq!(remint.pending_permissions().len(), 1);
                ask_a = Some(ask);
            } else {
                let ask = remint
                    .request_permission_under(
                        RpcId::Num(1),
                        Some("toolu_sub"),
                        Some("toolu_task"),
                        "npm test",
                        perm_opts(),
                    )
                    .unwrap();
                assert_eq!(ask.parent_tool_call_id.as_deref(), Some("toolu_task"));
                assert_eq!(remint.pending_permissions().len(), 1);
                ask_b = Some(ask);
            }
            let wire = AgentHostWire::real_exited(remint, Some(place.clone()), code);
            assert!(
                matches!(wire.old_wire, hearth_agent_wire::OldWire::Exited(c) if c == code),
                "Real probe must yield Exited({code}), got {:?}",
                wire.old_wire
            );
            st.install_host_wire(sid, ag, wire).unwrap();
        }
        (a, b, place, ask_a.expect("ask A"), ask_b.expect("ask B"))
    }

    /// Must (Service-real-cli-concurrent-permission-remint): twin of wire #66 /
    /// extend #72. Two ParallelSafe Agents; mid-turn pending asks (B parented);
    /// Real Exited each (no Fake); one HTTP steer → PermissionResurface both;
    /// B parent preserved; A's ask survives B remint; mint+1 + session/resume
    /// each; healthy HITL skips cancel-before-reattach (rehydrate-pending-
    /// permission; orphan cancel floor stays #72); no load/new; independent
    /// lease/fence; no cross-Agent Binding / ask steal.
    #[tokio::test]
    async fn http_steer_through_host_wake_real_cli_exit_resurfaces_asks_independent_leases_fences()
    {
        use hearth_agent_wire::{RemintError, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _, ask_a, ask_b) =
            install_two_parallel_safe_real_exited_permission_host_wires(&st, sid);

        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            {
                let wa = wires.get_mut(a).unwrap();
                assert!(wa.remint.turn_in_flight(), "A mid-turn before steer");
                assert_eq!(wa.remint.pending_permissions().len(), 1);
            }
            {
                let wb = wires.get_mut(b).unwrap();
                assert!(wb.remint.turn_in_flight(), "B mid-turn before steer");
                assert_eq!(wb.remint.pending_permissions().len(), 1);
            }
        }

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"user":"cheng","message":"hi-both-real-perm"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("hi-both-real-perm"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Agent A LiveInTurn (first-only mutant fails)"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "Agent B LiveInTurn (first-only mutant fails)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let after_mints = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints,
            before_mints + 2,
            "exactly one mint per Agent (remint)"
        );

        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_ne!(bid_a, bid_a_before, "A reminted");
        assert_ne!(bid_b, bid_b_before, "B reminted");
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");

        let events = session.events().unwrap();
        let ums: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .collect();
        assert_eq!(ums.len(), 1, "one shared UserMessage");
        assert!(matches!(
            &ums[0].body,
            EventBody::UserMessage { text, .. } if text == "hi-both-real-perm"
        ));

        // PermissionResurface both; B parent preserved; A's ask survives B;
        // healthy HITL: turn stays in-flight (rehydrate-pending-permission;
        // orphan cancel-before-reattach floor remains #72).
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            {
                let remint_a = &wires.get_mut(a).unwrap().remint;
                assert!(
                    remint_a.turn_in_flight(),
                    "A turn stays in-flight under healthy HITL"
                );
                assert_eq!(
                    remint_a
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_a)
                );
                assert!(remint_a.observe().iter().any(|e| matches!(
                    &e.kind,
                    RemintEventKind::PermissionResurface(r)
                        if r == &ask_a && r.parent_tool_call_id.is_none()
                )));
                assert!(remint_a.pending_permissions().is_empty());
                assert_eq!(
                    remint_a.resolve_permission(&ask_a.binding, &ask_a.rpc_id, "allow-once"),
                    Err(RemintError::DeadBinding)
                );
            }
            {
                let remint_b = &wires.get_mut(b).unwrap().remint;
                assert!(
                    remint_b.turn_in_flight(),
                    "B turn stays in-flight under healthy HITL"
                );
                assert_eq!(
                    remint_b
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_b),
                    "B resurfaced with parent"
                );
                assert!(remint_b.observe().iter().any(|e| matches!(
                    &e.kind,
                    RemintEventKind::PermissionResurface(r)
                        if r == &ask_b
                            && r.parent_tool_call_id.as_deref() == Some("toolu_task")
                )));
                assert!(remint_b.pending_permissions().is_empty());
                assert_eq!(
                    remint_b.resolve_permission(&ask_b.binding, &ask_b.rpc_id, "allow-once"),
                    Err(RemintError::DeadBinding)
                );
            }
            // A's resurfaced ask survives B's remint (re-check after B asserts).
            {
                let remint_a = &wires.get_mut(a).unwrap().remint;
                assert_eq!(
                    remint_a
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_a),
                    "A's resurfaced ask survives B's remint"
                );
            }
        }

        // Attach per-Agent leases onto wake-opened turns; raw pins (not is_none).
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("ha"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("hb"))
            .expect("lease B");
        assert_ne!(ta, tb);
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "ha", ta, fa), (b, "hb", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("live lease");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }

        // Parent pins survive lease attach / fence raise.
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let parents_a: Vec<Option<String>> = wires
                .get_mut(a)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            let parents_b: Vec<Option<String>> = wires
                .get_mut(b)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            assert_eq!(parents_a, [None]);
            assert_eq!(parents_b, [Some("toolu_task".into())]);
        }

        // A's fence raise must not steal B's lease/fence.
        st.runtime.end_turn(sid, "ha".into(), fa, true).unwrap();
        let (ta2, fa2, _) = st
            .runtime
            .resume_interrupted_turn(sid, a, "ha2".into())
            .unwrap();
        assert_eq!(ta2, ta);
        assert!(fa2 > fa, "A's own fence must rise on resume");
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap().expect("B live");
        assert_eq!(
            (
                lease_b.agent,
                lease_b.holder,
                lease_b.turn_id,
                lease_b.fence
            ),
            (b, hearth::HolderId::new("hb"), tb, fb),
            "turn_lease_of(B).fence pinned after A's raise — no steal"
        );
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        st.runtime
            .durable_step(sid, "hb".into(), fb, tb, "b-ok", || Ok("b".into()))
            .unwrap();
        assert!(matches!(
            st.runtime
                .durable_step(sid, "ha2".into(), fa, ta, "zombie", || {
                    panic!("zombie must not run")
                }),
            Err(hearth::Error::TurnLeaseLost)
        ));
        st.runtime
            .durable_step(sid, "ha2".into(), fa2, ta, "a-ok", || Ok("a".into()))
            .unwrap();

        // New wires: session/resume stable id; never load/new. Resume ids from install.
        let expected = [(a, "S-a"), (b, "S-b")];
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(agent).unwrap();
            // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
            let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            let frames: Vec<serde_json::Value> = raw
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert!(
                frames.len() >= 2,
                "Agent {agent:?} expected initialize+resume; got {raw}"
            );
            assert_eq!(frames[0]["method"], "initialize", "Agent {agent:?}");
            assert_eq!(
                frames[1]["method"], "session/resume",
                "Agent {agent:?} must session/resume; got {raw}"
            );
            assert_eq!(
                frames[1]["params"]["sessionId"], expected_sid,
                "Agent {agent:?} stable sessionId"
            );
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "no load/new on {agent:?}: {raw}"
            );
        }
    }

    /// Service-real-stdout-eof-concurrent-remint: Soft twin of #72. Two
    /// ParallelSafe Agents with Real old wires probed to StdoutEof (no Fake
    /// StdoutEof injection), mid-turn so cancel-before-reattach fires, Fake
    /// new wires for resume capture. Extends #72 install scaffolding.
    fn install_two_parallel_safe_real_stdout_eof_host_wires(
        st: &AppState,
        sid: SessionId,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintSession};

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();

        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);

        let caps = WireCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        };
        for (ag, resume_id) in [(a, "S-a"), (b, "S-b")] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            // Mid-turn so cancel-before-reattach fires (#43 / #65 / #72).
            remint.begin_turn(&format!("mid-{resume_id}"));
            let wire = AgentHostWire::real_stdout_eof(remint, Some(place.clone()));
            assert!(
                matches!(wire.old_wire, hearth_agent_wire::OldWire::StdoutEof),
                "Real probe must yield StdoutEof, got {:?}",
                wire.old_wire
            );
            st.install_host_wire(sid, ag, wire).unwrap();
        }
        (a, b, place)
    }

    /// Must (Service-real-stdout-eof-concurrent-remint): Soft twin of #72.
    /// Real StdoutEof on both ParallelSafe Agents → one HTTP steer remints
    /// both (mint+1 + session/resume each, stable sessionId);
    /// cancel-before-reattach; no load/new; independent lease/fence; no
    /// cross-Agent Binding steal; one shared UserMessage. No Fake StdoutEof
    /// injection.
    #[tokio::test]
    async fn http_steer_through_host_wake_real_stdout_eof_remints_both_independent_leases_fences() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_real_stdout_eof_host_wires(&st, sid);

        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            assert!(
                wires.get_mut(a).unwrap().remint.turn_in_flight(),
                "A mid-turn before steer"
            );
            assert!(
                wires.get_mut(b).unwrap().remint.turn_in_flight(),
                "B mid-turn before steer"
            );
        }

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"user":"cheng","message":"hi-both-real-eof"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("hi-both-real-eof"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Agent A LiveInTurn (first-only mutant fails)"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "Agent B LiveInTurn (first-only mutant fails)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let after_mints = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints,
            before_mints + 2,
            "exactly one mint per Agent (remint)"
        );

        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_ne!(bid_a, bid_a_before, "A reminted");
        assert_ne!(bid_b, bid_b_before, "B reminted");
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");

        let events = session.events().unwrap();
        let ums: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .collect();
        assert_eq!(ums.len(), 1, "one shared UserMessage");
        assert!(matches!(
            &ums[0].body,
            EventBody::UserMessage { text, .. } if text == "hi-both-real-eof"
        ));

        // Attach per-Agent leases onto wake-opened turns; raw pins (not is_none).
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("ha"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("hb"))
            .expect("lease B");
        assert_ne!(ta, tb);
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "ha", ta, fa), (b, "hb", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("live lease");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }
        // A's fence raise must not steal B's lease/fence.
        st.runtime.end_turn(sid, "ha".into(), fa, true).unwrap();
        let (ta2, fa2, _) = st
            .runtime
            .resume_interrupted_turn(sid, a, "ha2".into())
            .unwrap();
        assert_eq!(ta2, ta);
        assert!(fa2 > fa, "A's own fence must rise on resume");
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap().expect("B live");
        assert_eq!(
            (
                lease_b.agent,
                lease_b.holder,
                lease_b.turn_id,
                lease_b.fence
            ),
            (b, hearth::HolderId::new("hb"), tb, fb),
            "turn_lease_of(B).fence pinned after A's raise — no steal"
        );

        // New wires: session/resume stable id; never load/new; mid-turn cleared
        // (cancel-before-reattach path under remint). Resume ids from install.
        let expected = [(a, "S-a"), (b, "S-b")];
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(agent).unwrap();
            assert!(
                !w.remint.turn_in_flight(),
                "Agent {agent:?} turn cleared after remint (cancel-before-reattach)"
            );
            // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
            let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            let frames: Vec<serde_json::Value> = raw
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert!(
                frames.len() >= 2,
                "Agent {agent:?} expected initialize+resume; got {raw}"
            );
            assert_eq!(frames[0]["method"], "initialize", "Agent {agent:?}");
            assert_eq!(
                frames[1]["method"], "session/resume",
                "Agent {agent:?} must session/resume; got {raw}"
            );
            assert_eq!(
                frames[1]["params"]["sessionId"], expected_sid,
                "Agent {agent:?} stable sessionId"
            );
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "no load/new on {agent:?}: {raw}"
            );
        }
    }

    /// Service-real-stdout-eof-concurrent-permission-remint: Soft twin of #74 /
    /// Exited→StdoutEof swap like #72→#73. Two ParallelSafe Agents with Real
    /// old wires probed to StdoutEof (no Fake StdoutEof); mid-turn pending asks
    /// (A controller unparented; B `parent_tool_call_id` via Session/Host ask
    /// helpers — not Real CLI emitting asks); Fake new wires for resume capture.
    /// Prefer extending #74 scaffolding.
    fn install_two_parallel_safe_real_stdout_eof_permission_host_wires(
        st: &AppState,
        sid: SessionId,
    ) -> (
        hearth::AgentId,
        hearth::AgentId,
        hearth::Place,
        hearth_agent_wire::runner::PermissionRef,
        hearth_agent_wire::runner::PermissionRef,
    ) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, PermissionOption, RemintSession, RpcId};

        fn perm_opts() -> Vec<PermissionOption> {
            vec![
                PermissionOption {
                    option_id: "allow-once".into(),
                    name: "Allow".into(),
                    kind: "allow_once".into(),
                },
                PermissionOption {
                    option_id: "reject-once".into(),
                    name: "Reject".into(),
                    kind: "reject_once".into(),
                },
            ]
        }

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();

        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);

        let caps = WireCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        };
        let mut ask_a = None;
        let mut ask_b = None;
        for (ag, resume_id) in [(a, "S-a"), (b, "S-b")] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            // Mid-turn + pending asks (Session/Host helpers — not Real CLI).
            remint.begin_turn(&format!("mid-{resume_id}"));
            if ag == a {
                let ask = remint
                    .request_permission(RpcId::Num(1), Some("call_a"), "git log", perm_opts())
                    .unwrap();
                assert_eq!(ask.parent_tool_call_id, None);
                assert_eq!(remint.pending_permissions().len(), 1);
                ask_a = Some(ask);
            } else {
                let ask = remint
                    .request_permission_under(
                        RpcId::Num(1),
                        Some("toolu_sub"),
                        Some("toolu_task"),
                        "npm test",
                        perm_opts(),
                    )
                    .unwrap();
                assert_eq!(ask.parent_tool_call_id.as_deref(), Some("toolu_task"));
                assert_eq!(remint.pending_permissions().len(), 1);
                ask_b = Some(ask);
            }
            let wire = AgentHostWire::real_stdout_eof(remint, Some(place.clone()));
            assert!(
                matches!(wire.old_wire, hearth_agent_wire::OldWire::StdoutEof),
                "Real probe must yield StdoutEof, got {:?}",
                wire.old_wire
            );
            st.install_host_wire(sid, ag, wire).unwrap();
        }
        (a, b, place, ask_a.expect("ask A"), ask_b.expect("ask B"))
    }

    /// Must (Service-real-stdout-eof-concurrent-permission-remint): Soft twin
    /// of #74 / wire #67 pattern. Two ParallelSafe Agents; mid-turn pending
    /// asks (B parented); Real StdoutEof each (no Fake); one HTTP steer →
    /// PermissionResurface both; B parent preserved; A's ask survives B remint;
    /// mint+1 + session/resume each; healthy HITL skips cancel-before-reattach
    /// (rehydrate-pending-permission; orphan cancel floor stays #72/#73); no
    /// load/new; independent lease/fence; no cross-Agent Binding / ask steal.
    #[tokio::test]
    async fn http_steer_through_host_wake_real_stdout_eof_resurfaces_asks_independent_leases_fences(
    ) {
        use hearth_agent_wire::{RemintError, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _, ask_a, ask_b) =
            install_two_parallel_safe_real_stdout_eof_permission_host_wires(&st, sid);

        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            {
                let wa = wires.get_mut(a).unwrap();
                assert!(wa.remint.turn_in_flight(), "A mid-turn before steer");
                assert_eq!(wa.remint.pending_permissions().len(), 1);
            }
            {
                let wb = wires.get_mut(b).unwrap();
                assert!(wb.remint.turn_in_flight(), "B mid-turn before steer");
                assert_eq!(wb.remint.pending_permissions().len(), 1);
            }
        }

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"user":"cheng","message":"hi-both-real-eof-perm"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("hi-both-real-eof-perm"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Agent A LiveInTurn (first-only mutant fails)"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "Agent B LiveInTurn (first-only mutant fails)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let after_mints = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(
            after_mints,
            before_mints + 2,
            "exactly one mint per Agent (remint)"
        );

        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_ne!(bid_a, bid_a_before, "A reminted");
        assert_ne!(bid_b, bid_b_before, "B reminted");
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");

        let events = session.events().unwrap();
        let ums: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .collect();
        assert_eq!(ums.len(), 1, "one shared UserMessage");
        assert!(matches!(
            &ums[0].body,
            EventBody::UserMessage { text, .. } if text == "hi-both-real-eof-perm"
        ));

        // PermissionResurface both; B parent preserved; A's ask survives B;
        // healthy HITL: turn stays in-flight (rehydrate-pending-permission;
        // orphan cancel-before-reattach floor remains #72/#73).
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            {
                let remint_a = &wires.get_mut(a).unwrap().remint;
                assert!(
                    remint_a.turn_in_flight(),
                    "A turn stays in-flight under healthy HITL"
                );
                assert_eq!(
                    remint_a
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_a)
                );
                assert!(remint_a.observe().iter().any(|e| matches!(
                    &e.kind,
                    RemintEventKind::PermissionResurface(r)
                        if r == &ask_a && r.parent_tool_call_id.is_none()
                )));
                assert!(remint_a.pending_permissions().is_empty());
                assert_eq!(
                    remint_a.resolve_permission(&ask_a.binding, &ask_a.rpc_id, "allow-once"),
                    Err(RemintError::DeadBinding)
                );
            }
            {
                let remint_b = &wires.get_mut(b).unwrap().remint;
                assert!(
                    remint_b.turn_in_flight(),
                    "B turn stays in-flight under healthy HITL"
                );
                assert_eq!(
                    remint_b
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_b),
                    "B resurfaced with parent"
                );
                assert!(remint_b.observe().iter().any(|e| matches!(
                    &e.kind,
                    RemintEventKind::PermissionResurface(r)
                        if r == &ask_b
                            && r.parent_tool_call_id.as_deref() == Some("toolu_task")
                )));
                assert!(remint_b.pending_permissions().is_empty());
                assert_eq!(
                    remint_b.resolve_permission(&ask_b.binding, &ask_b.rpc_id, "allow-once"),
                    Err(RemintError::DeadBinding)
                );
            }
            // A's resurfaced ask survives B's remint (re-check after B asserts).
            {
                let remint_a = &wires.get_mut(a).unwrap().remint;
                assert_eq!(
                    remint_a
                        .resurfaced_permissions()
                        .iter()
                        .map(|p| p.as_ref())
                        .collect::<Vec<_>>(),
                    std::slice::from_ref(&ask_a),
                    "A's resurfaced ask survives B's remint"
                );
            }
        }

        // Attach per-Agent leases onto wake-opened turns; raw pins (not is_none).
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("ha"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("hb"))
            .expect("lease B");
        assert_ne!(ta, tb);
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "ha", ta, fa), (b, "hb", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("live lease");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }

        // Parent pins survive lease attach / fence raise.
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let parents_a: Vec<Option<String>> = wires
                .get_mut(a)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            let parents_b: Vec<Option<String>> = wires
                .get_mut(b)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            assert_eq!(parents_a, [None]);
            assert_eq!(parents_b, [Some("toolu_task".into())]);
        }

        // A's fence raise must not steal B's lease/fence.
        st.runtime.end_turn(sid, "ha".into(), fa, true).unwrap();
        let (ta2, fa2, _) = st
            .runtime
            .resume_interrupted_turn(sid, a, "ha2".into())
            .unwrap();
        assert_eq!(ta2, ta);
        assert!(fa2 > fa, "A's own fence must rise on resume");
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap().expect("B live");
        assert_eq!(
            (
                lease_b.agent,
                lease_b.holder,
                lease_b.turn_id,
                lease_b.fence
            ),
            (b, hearth::HolderId::new("hb"), tb, fb),
            "turn_lease_of(B).fence pinned after A's raise — no steal"
        );
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        st.runtime
            .durable_step(sid, "hb".into(), fb, tb, "b-ok", || Ok("b".into()))
            .unwrap();
        assert!(matches!(
            st.runtime
                .durable_step(sid, "ha2".into(), fa, ta, "zombie", || {
                    panic!("zombie must not run")
                }),
            Err(hearth::Error::TurnLeaseLost)
        ));
        st.runtime
            .durable_step(sid, "ha2".into(), fa2, ta, "a-ok", || Ok("a".into()))
            .unwrap();

        // New wires: session/resume stable id; never load/new. Resume ids from install.
        let expected = [(a, "S-a"), (b, "S-b")];
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(agent).unwrap();
            // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
            let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
            let frames: Vec<serde_json::Value> = raw
                .lines()
                .filter(|l| !l.is_empty())
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert!(
                frames.len() >= 2,
                "Agent {agent:?} expected initialize+resume; got {raw}"
            );
            assert_eq!(frames[0]["method"], "initialize", "Agent {agent:?}");
            assert_eq!(
                frames[1]["method"], "session/resume",
                "Agent {agent:?} must session/resume; got {raw}"
            );
            assert_eq!(
                frames[1]["params"]["sessionId"], expected_sid,
                "Agent {agent:?} stable sessionId"
            );
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "no load/new on {agent:?}: {raw}"
            );
        }
    }

    /// Must: Waiting / Unknown peer skips without aborting the other Agent
    /// (tick semantics). Claim-before-record preserved (shared wake only for
    /// the Agent that host_wake'd).
    #[tokio::test]
    async fn http_steer_through_host_wake_unknown_peer_skips_without_abort() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        // B: release Binding so classify needs claim; Unknown host_wake skips
        // → AlreadyBound Waiting(NoBinding). A still ReuseLive + TurnStart.
        let session = st.store().session(sid).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        session.unbind(bid_b).unwrap();
        st.set_host_old_wire(sid, b, hearth_agent_wire::OldWire::Unknown)
            .unwrap();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"skip-b"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("skip-b"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "A still wakes"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_none(),
            "B skipped (Unknown / NoBinding)"
        );
        let ums = session
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count();
        assert_eq!(ums, 1, "Claim-before-record: one shared wake for A");
    }

    /// Must (Service-parallel-safe-declare): HTTP POST /join with optional
    /// `parallel_safe` path list → set_parallel_safe; two Agents with disjoint
    /// paths + one HTTP steer through host_wake → both LiveInTurn (composes #69).
    #[tokio::test]
    async fn http_join_parallel_safe_declare_both_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let app = router(st.clone());

        let (code, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/join"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (code, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/join"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"agent":"alpha","parallel_safe":["agent-0"]}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        let (code, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/join"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"agent":"beta","parallel_safe":["agent-1"]}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK);

        let agents = st.runtime.kept_agents(sid);
        assert_eq!(agents.len(), 2);
        let (a, b) = (agents[0], agents[1]);
        // Wire HostAttach so steer composes #69 host_wake fan-out.
        install_host_wires_for_kept(&st, sid, a, b, None);

        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"declare-both"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("declare-both"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "A LiveInTurn via HTTP parallel_safe declare"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "B LiveInTurn via HTTP parallel_safe declare"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// Must: omit parallel_safe on join → fail-closed serialize; one steer
    /// through host_wake admits only the first ready Agent (Waiting/TurnOpen
    /// skips the peer). Conflict gate unchanged.
    #[tokio::test]
    async fn http_join_missing_parallel_safe_serializes() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) =
            install_two_host_wires_with_declare(&st, sid, None, None, None, resume_capable_caps());

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"no-declare"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("no-declare"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "first Agent still wakes"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_none(),
            "undeclared peer serializes (Waiting TurnOpen skip)"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// Must: overlapping ParallelSafe path lists still serialize — conflict
    /// gate unchanged (B Waiting/skip while A LiveInTurn).
    #[tokio::test]
    async fn http_join_overlapping_parallel_safe_serializes() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_host_wires_with_declare(
            &st,
            sid,
            None,
            Some(vec!["src".into()]),
            Some(vec!["src/lib.rs".into()]),
            resume_capable_caps(),
        );

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"overlap"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("overlap"));

        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "first Agent wakes"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_none(),
            "overlapping peer serializes (gate skip)"
        );
    }

    /// Install Fake Host wires for already-kept Agents (HTTP-join floors).
    fn install_host_wires_for_kept(
        st: &AppState,
        sid: SessionId,
        a: hearth::AgentId,
        b: hearth::AgentId,
        lost: Option<hearth_agent_wire::OldWire>,
    ) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintSession};

        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();
        let names = st.names.lock().unwrap();
        let uid = *names.users.get("cheng").unwrap();
        drop(names);
        let caps = WireCaps {
            resume: true,
            cancel: true,
            close: true,
            load_session: false,
        };
        for (ag, resume_id) in [(a, "S-a"), (b, "S-b")] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(session.clone(), ag, uid, resume_id, caps)
                .expect("remint session");
            let wire = match &lost {
                Some(w) => AgentHostWire::fake_lost(remint, Some(place.clone()), w.clone()),
                None => AgentHostWire::fake_live(remint, Some(place.clone())),
            };
            st.install_host_wire(sid, ag, wire).unwrap();
        }
    }

    /// Record a Host-owned ToolCall on the Session EventLog (precondition for
    /// Service-tool-completion-trigger Must floors).
    fn record_host_tool_call(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
        id: &str,
        name: &str,
    ) {
        st.store()
            .session(sid)
            .unwrap()
            .append(EventBody::ToolCall {
                agent,
                tool_call_id: id.into(),
                name: name.into(),
                input: "{}".into(),
                parent_tool_call_id: None,
            })
            .unwrap();
    }

    /// Count this Session's EventLog entries whose body matches `pred`.
    fn count_events(st: &AppState, sid: SessionId, pred: impl Fn(&EventBody) -> bool) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| pred(&e.body))
            .count()
    }

    fn count_tool_calls(st: &AppState, sid: SessionId, tool_call_id: &str) -> usize {
        count_events(
            st,
            sid,
            |b| matches!(b, EventBody::ToolCall { tool_call_id: id, .. } if id == tool_call_id),
        )
    }

    fn count_tool_results(st: &AppState, sid: SessionId, tool_call_id: &str) -> usize {
        count_events(
            st,
            sid,
            |b| matches!(b, EventBody::ToolResult { tool_call_id: id, .. } if id == tool_call_id),
        )
    }

    fn count_trigger_wakes(st: &AppState, sid: SessionId) -> usize {
        count_events(
            st,
            sid,
            |b| matches!(b, EventBody::Wake { source: hearth::WakeSource::Trigger { name } } if name == "tool_result"),
        )
    }

    /// M1: Prior Host ToolCall + POST → one ToolResult + Wake Trigger + that
    /// Agent TurnStart / LiveInTurn.
    #[tokio::test]
    async fn http_tool_result_trigger_wakes_agent_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-m1", "host_tool");

        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-m1","output":"done-m1"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_tool_results(&st, sid, "tc-m1"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert!(
            st.store()
                .session(sid)
                .unwrap()
                .current_turn_of(a)
                .unwrap()
                .is_some(),
            "Agent A TurnStart / LiveInTurn"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let evs = st.store().session(sid).unwrap().events().unwrap();
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "tc-m1" && name == "host_tool" && output == "done-m1"
        )));
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::TurnStart { agent } if *agent == a
        )));
    }

    /// M2: Duplicate POST → still one ToolResult; applied:false.
    #[tokio::test]
    async fn http_tool_result_duplicate_is_noop() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-m2", "dup_tool");

        let app = router(st.clone());
        let req = || {
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-m2","output":"once"}"#))
                .unwrap()
        };
        let (c1, b1) = call(app.clone(), req()).await;
        assert_eq!(c1, StatusCode::OK);
        assert!(b1.contains(r#""applied":true"#));

        // End turn so second POST is not 409 Waiting(TurnOpen) — terminal
        // check runs before wake; NotApplied must not wake.
        let session = st.store().session(sid).unwrap();
        session.turn_end(a).unwrap();

        let (c2, b2) = call(app, req()).await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert!(b2.contains(r#""applied":false"#), "body={b2}");
        assert_eq!(count_tool_results(&st, sid, "tc-m2"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
    }

    /// M3: No unmatched ToolCall → 4xx, zero Result / Wake.
    #[tokio::test]
    async fn http_tool_result_orphan_refused() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (code, _body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"never-existed","output":"x"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert_eq!(count_tool_results(&st, sid, "never-existed"), 0);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    /// M4: After remint (Fake lost → new Binding): POST still works; remint /
    /// reuse; no session/load / session/new.
    #[tokio::test]
    async fn http_tool_result_remints_lost_wire_no_load_new() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        record_host_tool_call(&st, sid, a, "tc-m4", "remint_tool");
        let bid_before = st.runtime.binding_for(sid, a).unwrap();
        let before_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-m4","output":"after-remint"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#));
        assert_eq!(count_tool_results(&st, sid, "tc-m4"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let bid_after = st.runtime.binding_for(sid, a).unwrap();
        assert_ne!(bid_before, bid_after, "remint must mint new Binding");
        let after_mints = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count();
        assert_eq!(after_mints, before_mints + 1);
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        let w = wires.get_mut(a).unwrap();
        // Host-wire-remint-settle: Reminted swaps runners; resumed child is `old`.
        let raw = std::str::from_utf8(w.old.stdin_log().unwrap()).unwrap();
        assert!(
            raw.contains("session/resume"),
            "must session/resume; got {raw}"
        );
        assert!(
            !raw.contains("session/load") && !raw.contains("session/new"),
            "no load/new: {raw}"
        );
    }

    /// M5: Late POST after ToolCallInterrupted → not applied.
    #[tokio::test]
    async fn http_tool_result_late_after_interrupted_not_applied() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-m5", "late_tool");
        let n = st
            .store()
            .session(sid)
            .unwrap()
            .finalize_unmatched_tool_calls_of(a)
            .unwrap();
        assert_eq!(n, 1);
        assert!(hearth::tool_call_is_terminal(
            &st.store().session(sid).unwrap().events().unwrap(),
            "tc-m5"
        ));

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-m5","output":"late-ok"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":false"#), "body={body}");
        assert_eq!(count_tool_results(&st, sid, "tc-m5"), 0);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    /// M6: Peer ParallelSafe Agent untouched; completion is not fan-out.
    #[tokio::test]
    async fn http_tool_result_peer_parallel_safe_untouched_not_fanout() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-m6", "solo_tool");
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let lease_b_before = st.runtime.turn_lease_of(sid, b).unwrap();
        let fence_b_before = st.store().session(sid).unwrap().turn_fence_of(b).unwrap();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-m6","output":"solo"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "peer must not be woken (fan-out mutant fails)"
        );
        assert!(
            st.store()
                .session(sid)
                .unwrap()
                .current_turn_of(b)
                .unwrap()
                .is_none(),
            "peer no TurnStart"
        );
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(st.runtime.turn_lease_of(sid, b).unwrap(), lease_b_before);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fence_b_before
        );
        // Only one Trigger wake (not one per kept Agent).
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        let turn_starts: Vec<_> = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::TurnStart { agent } => Some(*agent),
                _ => None,
            })
            .collect();
        assert_eq!(turn_starts, vec![a], "only completing Agent TurnStart");
    }

    /// M7: ACP HostAttach drain ToolCall/ToolResult path unused by this
    /// ingress stays byte-identical (regression pin — drain code untouched).
    #[tokio::test]
    async fn http_tool_result_leaves_acp_drain_path_unused() {
        // Service-tool-completion-trigger never routes through HostAttach::drain.
        // Pin: a Loopback ACP drain of ToolCall+ToolResult still appends the
        // same Event bodies (name from ToolCall id on wire ToolResult).
        use hearth_agent_wire::{event_bodies, AgentEvent, CapabilityFlags, ToolStatus};
        let agent = hearth::AgentId(uuid::Uuid::nil());
        let call = AgentEvent::ToolCall {
            session_id: "s".into(),
            item_id: "drain-1".into(),
            name: "Bash".into(),
            arguments: serde_json::json!({}),
            status: ToolStatus::Completed,
            parent: None,
            subagent: false,
        };
        let result = AgentEvent::ToolResult {
            session_id: "s".into(),
            item_id: "drain-1".into(),
            output: serde_json::json!("ok"),
            status: ToolStatus::Completed,
        };
        let bodies_call = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &call,
            CapabilityFlags::ACP,
        );
        let bodies_res = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &result,
            CapabilityFlags::ACP,
        );
        assert!(matches!(
            &bodies_call[0],
            EventBody::ToolCall {
                tool_call_id,
                name,
                ..
            } if tool_call_id == "drain-1" && name == "Bash"
        ));
        assert!(matches!(
            &bodies_res[0],
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "drain-1" && name == "drain-1" && output.contains("ok")
        ));
    }

    /// M8: Claim-before-record — append Result then wake. When wake Waiting
    /// (LiveInTurn), Result stays on log; re-POST → NotApplied.
    #[tokio::test]
    async fn http_tool_result_append_before_wake_waiting_preserves_result() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        // Open a turn so Trigger wake hits Waiting(TurnOpen) after Result append.
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("m8"))
            .expect("begin_turn opens LiveInTurn");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        record_host_tool_call(&st, sid, a, "tc-m8", "wait_tool");

        let app = router(st.clone());
        let (code, _body) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-m8","output":"while-open"}"#,
                ))
                .unwrap(),
        )
        .await;
        // Result appended then wake Waiting → 409; Result remains.
        assert_eq!(code, StatusCode::CONFLICT);
        assert_eq!(count_tool_results(&st, sid, "tc-m8"), 1);
        // No tool_result Trigger (wake waited).
        assert_eq!(count_trigger_wakes(&st, sid), 0);

        // End turn; re-POST → terminal NotApplied (M2 covers).
        if let Some(lease) = st.runtime.turn_lease_of(sid, a).unwrap() {
            st.runtime
                .end_turn(sid, lease.holder, lease.fence, false)
                .unwrap();
        } else {
            st.store().session(sid).unwrap().turn_end(a).unwrap();
        }
        let (c2, b2) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-m8","output":"retry"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert!(b2.contains(r#""applied":false"#));
        assert_eq!(count_tool_results(&st, sid, "tc-m8"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    // -------------------------------------------------------------------------
    // Host-owned-tool-yield Must floors (H-I1 Soft under #78). Tip already
    // allows: Session::turn_end does not finalize unmatched ToolCalls.
    // -------------------------------------------------------------------------

    /// Yield helper: open turn, record Host ToolCall, end turn → LiveIdle.
    fn yield_host_tool_to_live_idle(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
        tool_call_id: &str,
        name: &str,
    ) -> hearth::BindingId {
        st.runtime
            .begin_turn(sid, agent, hearth::HolderId::new("yield"))
            .expect("begin_turn opens LiveInTurn");
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        record_host_tool_call(st, sid, agent, tool_call_id, name);
        let bid = st.runtime.binding_for(sid, agent).unwrap();
        if let Some(lease) = st.runtime.turn_lease_of(sid, agent).unwrap() {
            st.runtime
                .end_turn(sid, lease.holder, lease.fence, false)
                .unwrap();
        } else {
            st.store().session(sid).unwrap().turn_end(agent).unwrap();
        }
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::LiveIdle,
            "yield must leave LiveIdle"
        );
        bid
    }

    fn count_tool_interrupted(st: &AppState, sid: SessionId, tool_call_id: &str) -> usize {
        count_events(
            st,
            sid,
            |b| matches!(b, EventBody::ToolCallInterrupted { tool_call_id: id, .. } if id == tool_call_id),
        )
    }

    /// M1: Mid-turn Host ToolCall + turn_end → LiveIdle; tool_call_id unmatched
    /// (no Interrupt); Binding stays.
    #[tokio::test]
    async fn http_host_owned_yield_live_idle_unmatched_no_interrupt() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_before = yield_host_tool_to_live_idle(&st, sid, a, "tc-y1", "yield_tool");

        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_before);
        assert!(
            !hearth::tool_call_is_terminal(
                &st.store().session(sid).unwrap().events().unwrap(),
                "tc-y1"
            ),
            "yield must not finalize unmatched Host ToolCall"
        );
        assert_eq!(count_tool_interrupted(&st, sid, "tc-y1"), 0);
        assert_eq!(count_tool_results(&st, sid, "tc-y1"), 0);
        let unmatched =
            hearth::unmatched_tool_calls(&st.store().session(sid).unwrap().events().unwrap());
        assert!(
            unmatched
                .iter()
                .any(|(ag, id, _)| *ag == a && id == "tc-y1"),
            "tool_call_id must stay unmatched after yield: {unmatched:?}"
        );
    }

    /// M2: After yield → POST tool-result → one ToolResult + Wake(Trigger) +
    /// LiveInTurn (compose #78 M1; closes M8 Trigger trap as happy path).
    #[tokio::test]
    async fn http_host_owned_yield_later_complete_triggers_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let _bid = yield_host_tool_to_live_idle(&st, sid, a, "tc-y2", "yield_complete");

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-y2","output":"after-yield"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_tool_results(&st, sid, "tc-y2"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_some());
        let evs = st.store().session(sid).unwrap().events().unwrap();
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "tc-y2" && name == "yield_complete" && output == "after-yield"
        )));
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::Wake {
                source: hearth::WakeSource::Trigger { name }
            } if name == "tool_result"
        )));
    }

    /// M3: Peer ParallelSafe can begin_turn / HTTP steer during yielded
    /// LiveIdle; lease/fence/Binding pins; completion wake not fan-out.
    #[tokio::test]
    async fn http_host_owned_yield_peer_parallel_safe_runs_completion_not_fanout() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a = yield_host_tool_to_live_idle(&st, sid, a, "tc-y3", "yield_peer");
        let lease_a_before = st.runtime.turn_lease_of(sid, a).unwrap();
        let fence_a_before = st.store().session(sid).unwrap().turn_fence_of(a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();
        let lease_b_before = st.runtime.turn_lease_of(sid, b).unwrap();
        let fence_b_before = st.store().session(sid).unwrap().turn_fence_of(b).unwrap();

        // Peer begins its own turn while A is yielded LiveIdle.
        st.runtime
            .begin_turn(sid, b, hearth::HolderId::new("peer-y3"))
            .expect("peer ParallelSafe begin_turn during yield");
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle,
            "yielded Agent stays LiveIdle while peer runs"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.turn_lease_of(sid, a).unwrap(), lease_a_before);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(a).unwrap(),
            fence_a_before
        );
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b_before);
        // Peer's lease is new; fence may raise — pin Binding identity only for
        // cross-Agent steal (B's Binding unchanged).
        assert_ne!(
            st.runtime.turn_lease_of(sid, b).unwrap(),
            lease_b_before,
            "peer should hold its own lease"
        );
        let _ = fence_b_before;

        // End peer turn so completion wake of A is not Waiting(TurnOpen) via gate.
        if let Some(lease) = st.runtime.turn_lease_of(sid, b).unwrap() {
            st.runtime
                .end_turn(sid, lease.holder, lease.fence, false)
                .unwrap();
        }

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-y3","output":"solo-y3"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "completion must not fan-out wake peer"
        );
        assert!(
            st.store()
                .session(sid)
                .unwrap()
                .current_turn_of(b)
                .unwrap()
                .is_none(),
            "peer no TurnStart from completion"
        );
        let turn_starts: Vec<_> = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::TurnStart { agent } => Some(*agent),
                _ => None,
            })
            .collect();
        // Peer began+ended one TurnStart; completion adds A's TurnStart only
        // as the LiveInTurn wake — assert the last TurnStart is A and peer
        // is not in-turn now.
        assert!(
            turn_starts.contains(&a) && turn_starts.contains(&b),
            "both Agents opened a turn at some point: {turn_starts:?}"
        );
        assert!(
            st.store()
                .session(sid)
                .unwrap()
                .current_turn_of(a)
                .unwrap()
                .is_some(),
            "completing Agent must be LiveInTurn"
        );
    }

    /// M4: remint while LiveIdle + unmatched Host ToolCall → AlreadyLive;
    /// no finalize_unmatched_tool_calls_of (waiter stays open).
    #[tokio::test]
    async fn http_host_owned_yield_live_idle_remint_already_live_no_finalize() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_before = yield_host_tool_to_live_idle(&st, sid, a, "tc-y4", "yield_remint");

        let remint_err = st.runtime.remint(sid, a).expect_err("LiveIdle remint");
        assert!(
            matches!(
                remint_err,
                hearth::Error::Waiting(hearth::WaitReason::AlreadyLive)
            ),
            "expected AlreadyLive, got {remint_err:?}"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_before);
        assert_eq!(count_tool_interrupted(&st, sid, "tc-y4"), 0);
        assert!(
            !hearth::tool_call_is_terminal(
                &st.store().session(sid).unwrap().events().unwrap(),
                "tc-y4"
            ),
            "LiveIdle remint must not finalize Host tool waiter"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
    }

    /// M5: PositivelyDead / mid-turn remint still finalizes unmatched →
    /// Interrupted; late complete → NotApplied (#78 M5 regression).
    #[tokio::test]
    async fn http_host_owned_yield_positively_dead_remint_finalizes_late_not_applied() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        // Mid-turn Host ToolCall on RemintSession → remint_and_attach finalizes
        // (PositivelyDead-class recovery: old Binding reminted away).
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            w.remint.begin_turn("mid-y5");
            w.remint.start_tool("tc-y5", "remint_finalize");
            let out = w.remint.remint_and_attach().expect("mid-turn remint");
            assert!(out.turn_interrupted);
            assert_eq!(out.tools_finalized, 1);
        }
        assert_eq!(count_tool_interrupted(&st, sid, "tc-y5"), 1);
        assert!(hearth::tool_call_is_terminal(
            &st.store().session(sid).unwrap().events().unwrap(),
            "tc-y5"
        ));

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-y5","output":"late-y5"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":false"#), "body={body}");
        assert_eq!(count_tool_results(&st, sid, "tc-y5"), 0);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    /// M6: ACP HostAttach drain ToolCall/ToolResult unused path stays
    /// byte-identical under Host-owned yield (no mid-ACP auto turn_end).
    #[tokio::test]
    async fn http_host_owned_yield_leaves_acp_drain_path_byte_identical() {
        use hearth_agent_wire::{event_bodies, AgentEvent, CapabilityFlags, ToolStatus};
        let agent = hearth::AgentId(uuid::Uuid::nil());
        let call = AgentEvent::ToolCall {
            session_id: "s".into(),
            item_id: "drain-y6".into(),
            name: "Bash".into(),
            arguments: serde_json::json!({}),
            status: ToolStatus::Completed,
            parent: None,
            subagent: false,
        };
        let result = AgentEvent::ToolResult {
            session_id: "s".into(),
            item_id: "drain-y6".into(),
            output: serde_json::json!("ok"),
            status: ToolStatus::Completed,
        };
        let bodies_call = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &call,
            CapabilityFlags::ACP,
        );
        let bodies_res = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &result,
            CapabilityFlags::ACP,
        );
        assert!(matches!(
            &bodies_call[0],
            EventBody::ToolCall {
                tool_call_id,
                name,
                ..
            } if tool_call_id == "drain-y6" && name == "Bash"
        ));
        assert!(matches!(
            &bodies_res[0],
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "drain-y6" && name == "drain-y6" && output.contains("ok")
        ));
        // Drain path emits Event bodies only — never TurnEnd (no mid-ACP auto yield).
        assert!(
            !bodies_call
                .iter()
                .chain(bodies_res.iter())
                .any(|b| matches!(b, EventBody::TurnEnd { .. })),
            "ACP drain must not smuggle turn_end"
        );
    }

    // -------------------------------------------------------------------------
    // Service-host-tool-call (#80) — RAP dispatch half under #78/#79 Softs.
    // -------------------------------------------------------------------------

    /// M1: LiveIdle kept Agent + POST tool-call → one EventBody::ToolCall; 200
    /// with tool_call_id.
    #[tokio::test]
    async fn http_tool_call_live_idle_appends_one_tool_call() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"host_dispatch","tool_call_id":"tc-d1","input":"{\"x\":1}"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["tool_call_id"], "tc-d1");
        assert_eq!(count_tool_calls(&st, sid, "tc-d1"), 1);
        let evs = st.store().session(sid).unwrap().events().unwrap();
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::ToolCall {
                tool_call_id,
                name,
                input,
                ..
            } if tool_call_id == "tc-d1" && name == "host_dispatch" && input.contains("x")
        )));
        // No wake / no auto turn_end — still LiveIdle.
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(count_trigger_wakes(&st, sid), 0);
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_none());
    }

    /// M2: callback_url path targets that session/agent's existing tool-result
    /// route (string match; no auth).
    #[tokio::test]
    async fn http_tool_call_callback_url_targets_tool_result_route() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"cb","tool_call_id":"tc-d2"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let expected = format!("/sessions/{id}/agents/alpha/tool-result");
        assert_eq!(
            v["callback_url"].as_str().unwrap(),
            expected,
            "callback_url must be opaque path to existing tool-result route"
        );
        assert_eq!(v["tool_call_id"], "tc-d2");
    }

    /// M3: Later POST tool-result with that id → one ToolResult + Trigger +
    /// LiveInTurn (#78 compose).
    #[tokio::test]
    async fn http_tool_call_then_tool_result_triggers_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"compose","tool_call_id":"tc-d3"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let v: serde_json::Value = serde_json::from_str(&b1).unwrap();
        let tc_id = v["tool_call_id"].as_str().unwrap();
        let callback = v["callback_url"].as_str().unwrap();
        assert_eq!(callback, format!("/sessions/{id}/agents/alpha/tool-result"));

        let (c2, b2) = call(
            app,
            Request::post(callback)
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"tool_call_id":"{tc_id}","output":"done-d3"}}"#
                )))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert!(b2.contains(r#""applied":true"#), "body={b2}");
        assert_eq!(count_tool_calls(&st, sid, "tc-d3"), 1);
        assert_eq!(count_tool_results(&st, sid, "tc-d3"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M4: Duplicate POST same unmatched tool_call_id → 4xx; still one ToolCall.
    #[tokio::test]
    async fn http_tool_call_duplicate_unmatched_refused() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let req = || {
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"dup","tool_call_id":"tc-d4"}"#))
                .unwrap()
        };
        let (c1, b1) = call(app.clone(), req()).await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(app, req()).await;
        assert!(
            c2.is_client_error(),
            "duplicate unmatched must be 4xx, got {c2} body={b2}"
        );
        assert_eq!(count_tool_calls(&st, sid, "tc-d4"), 1);
    }

    /// M5: Unknown / not-kept Agent → 4xx; zero ToolCall.
    #[tokio::test]
    async fn http_tool_call_unknown_or_not_kept_refused() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        // NameMap entry for an Agent that is not kept on this Session.
        let stranger = st.store().create_agent("stranger", "");
        st.names
            .lock()
            .unwrap()
            .agents
            .insert("stranger".into(), stranger.id);

        let app = router(st.clone());
        let (c_unk, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/nope/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"x","tool_call_id":"tc-unk"}"#))
                .unwrap(),
        )
        .await;
        assert!(c_unk.is_client_error(), "unknown agent → 4xx, got {c_unk}");
        assert_eq!(count_tool_calls(&st, sid, "tc-unk"), 0);

        let (c_nk, _) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/stranger/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"x","tool_call_id":"tc-nk"}"#))
                .unwrap(),
        )
        .await;
        assert!(c_nk.is_client_error(), "not-kept agent → 4xx, got {c_nk}");
        assert_eq!(count_tool_calls(&st, sid, "tc-nk"), 0);

        // Member-but-not-kept: Host membership passes, so only the service
        // `kept_agents` guard refuses (load-bearing for that guard).
        let member = st.store().create_agent("member", "");
        st.names
            .lock()
            .unwrap()
            .agents
            .insert("member".into(), member.id);
        st.store()
            .session(sid)
            .unwrap()
            .join(hearth::Member::Agent(member.id))
            .unwrap();
        assert!(!st.runtime.kept_agents(sid).contains(&member.id));
        let (c_mnk, b_mnk) = call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/member/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"x","tool_call_id":"tc-mnk"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(
            c_mnk,
            StatusCode::BAD_REQUEST,
            "member-but-not-kept → 400, body={b_mnk}"
        );
        assert_eq!(count_tool_calls(&st, sid, "tc-mnk"), 0);
        let total_calls = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::ToolCall { .. }))
            .count();
        assert_eq!(total_calls, 0, "refused dispatch must append zero ToolCall");
    }

    /// Fix-1: re-dispatch of a terminal tool_call_id → 409; ToolCall count
    /// stays 1 (terminal keyed by id alone — reuse could never complete).
    #[tokio::test]
    async fn http_tool_call_redispatch_after_terminal_refused() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let dispatch = || {
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"re","tool_call_id":"tc-re"}"#))
                .unwrap()
        };
        let (c1, b1) = call(app.clone(), dispatch()).await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-re","output":"done-re"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert!(b2.contains(r#""applied":true"#), "body={b2}");
        assert!(hearth::tool_call_is_terminal(
            &st.store().session(sid).unwrap().events().unwrap(),
            "tc-re"
        ));

        let (c3, b3) = call(app, dispatch()).await;
        assert_eq!(
            c3,
            StatusCode::CONFLICT,
            "re-dispatch after terminal → 409, body={b3}"
        );
        assert_eq!(count_tool_calls(&st, sid, "tc-re"), 1);
        assert_eq!(count_tool_results(&st, sid, "tc-re"), 1);
    }

    /// Fix-1: cross-Agent same tool_call_id → 409; peer's original ToolCall
    /// still completes (Trigger + LiveInTurn), no cross-Agent steal.
    #[tokio::test]
    async fn http_tool_call_cross_agent_same_id_refused_peer_completes() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"peer","tool_call_id":"tc-shared"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"steal","tool_call_id":"tc-shared"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(
            c2,
            StatusCode::CONFLICT,
            "cross-Agent same id → 409, body={b2}"
        );
        assert_eq!(count_tool_calls(&st, sid, "tc-shared"), 1);

        // alpha cannot complete beta's id (orphan for alpha).
        let (c3, b3) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-shared","output":"stolen"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert!(
            !(c3 == StatusCode::OK && b3.contains(r#""applied":true"#)),
            "alpha must not complete beta's ToolCall: {c3} {b3}"
        );
        assert_eq!(count_tool_results(&st, sid, "tc-shared"), 0);

        let (c4, b4) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-shared","output":"ok"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c4, StatusCode::OK, "body={b4}");
        assert!(b4.contains(r#""applied":true"#), "body={b4}");
        assert_eq!(count_tool_results(&st, sid, "tc-shared"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle,
            "no fan-out to alpha"
        );
    }

    /// M6: Peer ParallelSafe Binding / lease / fence untouched; dispatch does
    /// not fan-out wake.
    #[tokio::test]
    async fn http_tool_call_peer_parallel_safe_untouched_no_fanout() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let lease_b_before = st.runtime.turn_lease_of(sid, b).unwrap();
        let fence_b_before = st.store().session(sid).unwrap().turn_fence_of(b).unwrap();

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"solo","tool_call_id":"tc-d6"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(count_tool_calls(&st, sid, "tc-d6"), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "peer must not be woken by dispatch"
        );
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_none());
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_none());
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(st.runtime.turn_lease_of(sid, b).unwrap(), lease_b_before);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fence_b_before
        );
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    /// M7: ACP HostAttach drain ToolCall/ToolResult unused path byte-identical.
    #[tokio::test]
    async fn http_tool_call_leaves_acp_drain_path_unused() {
        use hearth_agent_wire::{event_bodies, AgentEvent, CapabilityFlags, ToolStatus};
        let agent = hearth::AgentId(uuid::Uuid::nil());
        let call = AgentEvent::ToolCall {
            session_id: "s".into(),
            item_id: "drain-d7".into(),
            name: "Bash".into(),
            arguments: serde_json::json!({}),
            status: ToolStatus::Completed,
            parent: None,
            subagent: false,
        };
        let result = AgentEvent::ToolResult {
            session_id: "s".into(),
            item_id: "drain-d7".into(),
            output: serde_json::json!("ok"),
            status: ToolStatus::Completed,
        };
        let bodies_call = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &call,
            CapabilityFlags::ACP,
        );
        let bodies_res = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &result,
            CapabilityFlags::ACP,
        );
        assert!(matches!(
            &bodies_call[0],
            EventBody::ToolCall {
                tool_call_id,
                name,
                ..
            } if tool_call_id == "drain-d7" && name == "Bash"
        ));
        assert!(matches!(
            &bodies_res[0],
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "drain-d7" && name == "drain-d7" && output.contains("ok")
        ));
    }

    /// Server-mint when tool_call_id omitted (compose with M1 client-supplied).
    #[tokio::test]
    async fn http_tool_call_server_mints_id_when_absent() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (_a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"minted"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let tc_id = v["tool_call_id"].as_str().unwrap();
        assert!(!tc_id.is_empty(), "server must mint non-empty id");
        assert_eq!(count_tool_calls(&st, sid, tc_id), 1);
        assert_eq!(
            v["callback_url"].as_str().unwrap(),
            format!("/sessions/{id}/agents/alpha/tool-result")
        );
    }

    // -------------------------------------------------------------------------
    // Service-host-end-turn — RAP yield ingress under #79 (+ compose #78/#80).
    // -------------------------------------------------------------------------

    fn count_turn_ends(st: &AppState, sid: SessionId, agent: hearth::AgentId) -> usize {
        count_events(
            st,
            sid,
            |b| matches!(b, EventBody::TurnEnd { agent: a } if *a == agent),
        )
    }

    /// M1: LiveInTurn kept Agent + POST end-turn → LiveIdle; one TurnEnd;
    /// Binding identity unchanged.
    #[tokio::test]
    async fn http_end_turn_live_in_turn_yields_live_idle() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("et-m1"))
            .expect("begin_turn");
        let bid_before = st.runtime.binding_for(sid, a).unwrap();
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(count_turn_ends(&st, sid, a), 0);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""ended":true"#), "body={body}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(count_turn_ends(&st, sid, a), 1);
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_before);
        assert!(st.runtime.turn_lease_of(sid, a).unwrap().is_none());
    }

    /// M2: Mid-turn Host ToolCall + POST end-turn → LiveIdle; tool_call_id
    /// unmatched (no ToolCallInterrupted).
    #[tokio::test]
    async fn http_end_turn_mid_tool_call_unmatched_no_interrupt() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("et-m2"))
            .unwrap();
        let bid_before = st.runtime.binding_for(sid, a).unwrap();

        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"yield_tool","tool_call_id":"tc-et2"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");

        let (c2, b2) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_before);
        assert_eq!(count_tool_interrupted(&st, sid, "tc-et2"), 0);
        assert!(
            !hearth::tool_call_is_terminal(
                &st.store().session(sid).unwrap().events().unwrap(),
                "tc-et2"
            ),
            "yield must not finalize unmatched Host ToolCall"
        );
        let unmatched =
            hearth::unmatched_tool_calls(&st.store().session(sid).unwrap().events().unwrap());
        assert!(
            unmatched
                .iter()
                .any(|(ag, tid, _)| *ag == a && tid == "tc-et2"),
            "tool_call_id must stay unmatched: {unmatched:?}"
        );
    }

    /// M3: After M2 yield → POST tool-result → one ToolResult + Trigger +
    /// LiveInTurn (#78 compose).
    #[tokio::test]
    async fn http_end_turn_then_tool_result_triggers_live_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("et-m3"))
            .unwrap();

        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"compose_et","tool_call_id":"tc-et3"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );

        let (c3, b3) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-et3","output":"after-et"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c3, StatusCode::OK, "body={b3}");
        assert!(b3.contains(r#""applied":true"#), "body={b3}");
        assert_eq!(count_tool_results(&st, sid, "tc-et3"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_some());
        let evs = st.store().session(sid).unwrap().events().unwrap();
        assert!(evs.iter().any(|e| matches!(
            &e.body,
            EventBody::Wake {
                source: hearth::WakeSource::Trigger { name }
            } if name == "tool_result"
        )));
    }

    /// M4: LiveIdle / no open turn → 409; zero new TurnEnd.
    #[tokio::test]
    async fn http_end_turn_live_idle_conflict() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(count_turn_ends(&st, sid, a), 0);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "body={body}");
        assert_eq!(count_turn_ends(&st, sid, a), 0);
    }

    /// M5: Unknown / not-kept Agent → 4xx; zero TurnEnd.
    #[tokio::test]
    async fn http_end_turn_unknown_or_not_kept_refused() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, _, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("et-m5"))
            .unwrap();
        let ends_before = count_turn_ends(&st, sid, a);

        let stranger = st.store().create_agent("stranger", "");
        st.names
            .lock()
            .unwrap()
            .agents
            .insert("stranger".into(), stranger.id);
        // Member-but-not-kept: join without keep.
        let member = st.store().create_agent("member", "");
        st.store()
            .session(sid)
            .unwrap()
            .join(hearth::Member::Agent(member.id))
            .unwrap();
        st.names
            .lock()
            .unwrap()
            .agents
            .insert("member".into(), member.id);

        let app = router(st.clone());
        let (c_unk, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/nope/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert!(c_unk.is_client_error(), "unknown → 4xx, got {c_unk}");

        let (c_nk, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/stranger/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert!(c_nk.is_client_error(), "not-kept → 4xx, got {c_nk}");

        let (c_mem, _) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/member/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert!(
            c_mem.is_client_error(),
            "member-but-not-kept → 4xx, got {c_mem}"
        );
        assert_eq!(count_turn_ends(&st, sid, a), ends_before);
        assert_eq!(count_turn_ends(&st, sid, stranger.id), 0);
        assert_eq!(count_turn_ends(&st, sid, member.id), 0);
        // Alpha still LiveInTurn (refused peers did not end it).
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M6: Peer ParallelSafe Binding/lease/fence untouched; end-turn does not
    /// fan-out wake / does not end peer turn.
    #[tokio::test]
    async fn http_end_turn_peer_parallel_safe_untouched_no_fanout() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("et-m6a"))
            .unwrap();
        st.runtime
            .begin_turn(sid, b, hearth::HolderId::new("et-m6b"))
            .unwrap();
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let lease_b = st.runtime.turn_lease_of(sid, b).unwrap();
        let fence_b = st.store().session(sid).unwrap().turn_fence_of(b).unwrap();
        let wakes_before = count_trigger_wakes(&st, sid);
        let ends_b_before = count_turn_ends(&st, sid, b);

        let app = router(st.clone());
        let (code, body) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn,
            "peer turn must stay open"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(st.runtime.turn_lease_of(sid, b).unwrap(), lease_b);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fence_b
        );
        assert_eq!(count_turn_ends(&st, sid, b), ends_b_before);
        assert_eq!(count_trigger_wakes(&st, sid), wakes_before);
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_some());
    }

    /// M7: ACP HostAttach drain ToolCall/ToolResult unused path byte-identical
    /// (no mid-ACP auto turn_end smuggle).
    #[tokio::test]
    async fn http_end_turn_leaves_acp_drain_path_byte_identical() {
        use hearth_agent_wire::{event_bodies, AgentEvent, CapabilityFlags, ToolStatus};
        let agent = hearth::AgentId(uuid::Uuid::nil());
        let call = AgentEvent::ToolCall {
            session_id: "s".into(),
            item_id: "drain-et7".into(),
            name: "Bash".into(),
            arguments: serde_json::json!({}),
            status: ToolStatus::Completed,
            parent: None,
            subagent: false,
        };
        let result = AgentEvent::ToolResult {
            session_id: "s".into(),
            item_id: "drain-et7".into(),
            output: serde_json::json!("ok"),
            status: ToolStatus::Completed,
        };
        let bodies_call = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &call,
            CapabilityFlags::ACP,
        );
        let bodies_res = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &result,
            CapabilityFlags::ACP,
        );
        assert!(matches!(
            &bodies_call[0],
            EventBody::ToolCall {
                tool_call_id,
                name,
                ..
            } if tool_call_id == "drain-et7" && name == "Bash"
        ));
        assert!(matches!(
            &bodies_res[0],
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "drain-et7" && name == "drain-et7" && output.contains("ok")
        ));
        assert!(
            !bodies_call
                .iter()
                .chain(bodies_res.iter())
                .any(|b| matches!(b, EventBody::TurnEnd { .. })),
            "ACP drain must not smuggle turn_end"
        );
    }

    // ── Concurrent-host-tool-rap (#81 Soft lock floor) ─────────────────────
    // Two ParallelSafe Host-wired Agents; each HTTP RAP tool-call → (optional)
    // end-turn → tool-result → Trigger + LiveInTurn. Pin raw lease/fence/Binding;
    // Trigger wake stays single-Agent (never wake_each_kept). Tests-only.

    /// M1: A and B each POST …/tool-call (distinct ids) → one ToolCall each;
    /// both stay LiveIdle; zero Trigger (dispatch does not fan-out / steal).
    #[tokio::test]
    async fn http_concurrent_rap_both_tool_call_live_idle_no_trigger() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let lease_a_before = st.runtime.turn_lease_of(sid, a).unwrap();
        let lease_b_before = st.runtime.turn_lease_of(sid, b).unwrap();
        let fence_a_before = st.store().session(sid).unwrap().turn_fence_of(a).unwrap();
        let fence_b_before = st.store().session(sid).unwrap().turn_fence_of(b).unwrap();

        let app = router(st.clone());
        let (c_a, b_a) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"rap_a","tool_call_id":"tc-rap-a1"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c_a, StatusCode::OK, "body={b_a}");
        let (c_b, b_b) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"rap_b","tool_call_id":"tc-rap-b1"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c_b, StatusCode::OK, "body={b_b}");

        assert_eq!(count_tool_calls(&st, sid, "tc-rap-a1"), 1);
        assert_eq!(count_tool_calls(&st, sid, "tc-rap-b1"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_none());
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_none());
        // Raw Binding / lease / fence pins — not is_none() alone.
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_ne!(bid_a, bid_b, "no cross-Agent Binding steal");
        assert_eq!(st.runtime.turn_lease_of(sid, a).unwrap(), lease_a_before);
        assert_eq!(st.runtime.turn_lease_of(sid, b).unwrap(), lease_b_before);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(a).unwrap(),
            fence_a_before
        );
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fence_b_before
        );
    }

    /// M2: A and B each POST …/tool-result → one ToolResult + one Trigger each;
    /// both LiveInTurn; raw lease/fence/Binding independent; no cross-Agent
    /// TurnStart from peer complete (Trigger never wake_each_kept).
    #[tokio::test]
    async fn http_concurrent_rap_both_tool_result_live_in_turn_independent_pins() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a_before = st.runtime.binding_for(sid, a).unwrap();
        let bid_b_before = st.runtime.binding_for(sid, b).unwrap();

        let app = router(st.clone());
        for (agent, tc) in [("alpha", "tc-rap-a2"), ("beta", "tc-rap-b2")] {
            let (c, body) = call(
                app.clone(),
                Request::post(format!("/sessions/{id}/agents/{agent}/tool-call"))
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"name":"rap_{agent}","tool_call_id":"{tc}"}}"#
                    )))
                    .unwrap(),
            )
            .await;
            assert_eq!(c, StatusCode::OK, "tool-call {agent}: {body}");
        }

        // A completes first — only A TurnStart / LiveInTurn; B stays LiveIdle.
        let (c_a, b_a) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-a2","output":"done-a2"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c_a, StatusCode::OK, "body={b_a}");
        assert!(b_a.contains(r#""applied":true"#), "body={b_a}");
        assert_eq!(count_tool_results(&st, sid, "tc-rap-a2"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "A's Trigger must not wake peer B"
        );
        let turn_starts_after_a: Vec<_> = st
            .store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::TurnStart { agent } => Some(*agent),
                _ => None,
            })
            .collect();
        assert_eq!(
            turn_starts_after_a,
            vec![a],
            "only completing Agent TurnStart after A's result"
        );

        // B completes — both LiveInTurn; independent raw lease/fence/Binding.
        let (c_b, b_b) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-b2","output":"done-b2"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c_b, StatusCode::OK, "body={b_b}");
        assert!(b_b.contains(r#""applied":true"#), "body={b_b}");
        assert_eq!(count_tool_results(&st, sid, "tc-rap-b2"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 2, "one Trigger per Agent");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn,
            "A stays LiveInTurn across B's complete"
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let session = st.store().session(sid).unwrap();
        assert!(session.current_turn_of(a).unwrap().is_some());
        assert!(session.current_turn_of(b).unwrap().is_some());
        let turn_starts: Vec<_> = session
            .events()
            .unwrap()
            .iter()
            .filter_map(|e| match &e.body {
                EventBody::TurnStart { agent } => Some(*agent),
                _ => None,
            })
            .collect();
        assert_eq!(
            turn_starts,
            vec![a, b],
            "each Agent one TurnStart from its own Trigger; no fan-out"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a_before);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b_before);
        assert_ne!(bid_a_before, bid_b_before);

        // Compose begin_turn after Trigger wake (Concurrent-*-proof style) so
        // raw turn_lease_of / turn_fence_of pins are live — not is_none() alone.
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("rap-m2-a"))
            .expect("lease A on wake-opened turn");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("rap-m2-b"))
            .expect("lease B on wake-opened turn");
        assert_ne!(ta, tb, "independent turn ids");
        let session = st.store().session(sid).unwrap();
        assert_eq!(session.turn_fence_of(a).unwrap(), fa);
        assert_eq!(session.turn_fence_of(b).unwrap(), fb);
        for (agent, holder, turn, fence) in [(a, "rap-m2-a", ta, fa), (b, "rap-m2-b", tb, fb)] {
            let lease = st
                .runtime
                .turn_lease_of(sid, agent)
                .unwrap()
                .expect("raw turn_lease_of pin (not is_none)");
            assert_eq!(
                (lease.agent, lease.holder, lease.turn_id, lease.fence),
                (agent, hearth::HolderId::new(holder), turn, fence),
                "raw turn_lease_of pin (not is_none)"
            );
        }
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a_before);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b_before);
    }

    /// M3: Interleave — A tool-call + end-turn → LiveIdle unmatched; B full
    /// tool-call→tool-result→LiveInTurn; then A tool-result→Trigger+LiveInTurn.
    /// A's unmatched survives B's loop (no Interrupt); B Binding/lease/fence
    /// pins across A's yield+complete; A's Trigger does not re-wake or end B.
    #[tokio::test]
    async fn http_concurrent_rap_interleave_yield_peer_complete_then_resume() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();

        // Open A's turn so end-turn can yield (HTTP RAP yield under #81).
        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("rap-m3-a"))
            .expect("begin_turn A");
        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"rap_yield","tool_call_id":"tc-rap-a3"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(count_tool_interrupted(&st, sid, "tc-rap-a3"), 0);
        assert!(
            !hearth::tool_call_is_terminal(
                &st.store().session(sid).unwrap().events().unwrap(),
                "tc-rap-a3"
            ),
            "A unmatched must stay open after yield"
        );

        // B full RAP while A is yielded LiveIdle unmatched.
        let (c3, b3) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"rap_peer","tool_call_id":"tc-rap-b3"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c3, StatusCode::OK, "body={b3}");
        let (c4, b4) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-b3","output":"peer-done"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c4, StatusCode::OK, "body={b4}");
        assert!(b4.contains(r#""applied":true"#), "body={b4}");
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle,
            "B's Trigger must not wake yielded A"
        );
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(count_tool_interrupted(&st, sid, "tc-rap-a3"), 0);
        let unmatched =
            hearth::unmatched_tool_calls(&st.store().session(sid).unwrap().events().unwrap());
        assert!(
            unmatched
                .iter()
                .any(|(ag, tid, _)| *ag == a && tid == "tc-rap-a3"),
            "A's unmatched survived B's RAP loop: {unmatched:?}"
        );

        // Attach B's lease onto Trigger-opened turn; pin across A's complete.
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("rap-m3-b"))
            .expect("lease B after RAP");
        let lease_b = st
            .runtime
            .turn_lease_of(sid, b)
            .unwrap()
            .expect("raw turn_lease_of(B) after B RAP");
        assert_eq!(lease_b.agent, b);
        assert_eq!(lease_b.holder, hearth::HolderId::new("rap-m3-b"));
        assert_eq!(lease_b.turn_id, tb);
        assert_eq!(lease_b.fence, fb);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fb
        );
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        let ends_b_before = count_turn_ends(&st, sid, b);

        // A completes → Trigger + LiveInTurn; B pins hold; no re-wake / end B.
        let (c5, b5) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-a3","output":"after-yield"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c5, StatusCode::OK, "body={b5}");
        assert!(b5.contains(r#""applied":true"#), "body={b5}");
        assert_eq!(count_tool_results(&st, sid, "tc-rap-a3"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 2);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn,
            "A's Trigger must not end peer B"
        );
        assert_eq!(
            st.runtime
                .turn_lease_of(sid, b)
                .unwrap()
                .expect("B still leased"),
            lease_b,
            "raw turn_lease_of(B) pinned across A's yield+complete"
        );
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fb,
            "raw turn_fence_of(B) pinned across A's yield+complete"
        );
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(count_turn_ends(&st, sid, b), ends_b_before);
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_some());
        // A is LiveInTurn via Trigger (unleased until begin_turn); pin Binding
        // + open turn, then compose lease for independent raw pin.
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_some());
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("rap-m3-a2"))
            .expect("lease A after complete");
        let lease_a = st
            .runtime
            .turn_lease_of(sid, a)
            .unwrap()
            .expect("raw turn_lease_of(A) after complete");
        assert_eq!(lease_a.agent, a);
        assert_eq!(lease_a.turn_id, ta);
        assert_eq!(lease_a.fence, fa);
        assert_ne!(lease_a.turn_id, lease_b.turn_id);
        // B pin still holds after A's lease attach.
        assert_eq!(
            st.runtime.turn_lease_of(sid, b).unwrap().expect("B leased"),
            lease_b
        );
    }

    /// M4: Cross-Agent same tool_call_id → 409 (#80 Fix-1); peer may still
    /// complete its own distinct id under concurrent RAP.
    #[tokio::test]
    async fn http_concurrent_rap_cross_agent_same_id_409_peer_own_id_completes() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();

        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"rap_a","tool_call_id":"tc-rap-shared"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");

        // B tries A's id → 409; A's ToolCall count stays 1.
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"steal","tool_call_id":"tc-rap-shared"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(
            c2,
            StatusCode::CONFLICT,
            "cross-Agent same id → 409, body={b2}"
        );
        assert_eq!(count_tool_calls(&st, sid, "tc-rap-shared"), 1);

        // B uses its own id and completes; A completes its own — both LiveInTurn.
        let (c3, b3) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"name":"rap_b","tool_call_id":"tc-rap-b4"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(c3, StatusCode::OK, "body={b3}");
        let (c4, b4) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-b4","output":"b-own"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c4, StatusCode::OK, "body={b4}");
        assert!(b4.contains(r#""applied":true"#), "body={b4}");
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle,
            "B complete must not fan-out to A"
        );

        let (c5, b5) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-shared","output":"a-own"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c5, StatusCode::OK, "body={b5}");
        assert!(b5.contains(r#""applied":true"#), "body={b5}");
        assert_eq!(count_tool_results(&st, sid, "tc-rap-shared"), 1);
        assert_eq!(count_tool_results(&st, sid, "tc-rap-b4"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 2);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(a)
            .unwrap()
            .is_some());
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_some());
        let (ta, fa, _) = st
            .runtime
            .begin_turn(sid, a, hearth::HolderId::new("rap-m4-a"))
            .expect("lease A");
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("rap-m4-b"))
            .expect("lease B");
        assert_ne!(ta, tb);
        let lease_a = st
            .runtime
            .turn_lease_of(sid, a)
            .unwrap()
            .expect("raw turn_lease_of(A)");
        let lease_b = st
            .runtime
            .turn_lease_of(sid, b)
            .unwrap()
            .expect("raw turn_lease_of(B)");
        assert_eq!((lease_a.agent, lease_a.turn_id, lease_a.fence), (a, ta, fa));
        assert_eq!((lease_b.agent, lease_b.turn_id, lease_b.fence), (b, tb, fb));
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(a).unwrap(),
            fa
        );
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fb
        );
    }

    /// M5: Mid-loop LiveIdle remint on yielded A → AlreadyLive; unmatched stays
    /// open (#79 M4 under concurrent peer LiveInTurn).
    #[tokio::test]
    async fn http_concurrent_rap_yielded_remint_already_live_peer_in_turn() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();

        st.runtime
            .begin_turn(sid, a, hearth::HolderId::new("rap-m5-a"))
            .unwrap();
        let app = router(st.clone());
        let (c1, b1) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"rap_remint","tool_call_id":"tc-rap-a5"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");

        // Peer B full RAP → LiveInTurn while A is yielded unmatched.
        let (c3, _) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/beta/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"rap_peer","tool_call_id":"tc-rap-b5"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c3, StatusCode::OK);
        let (c4, b4) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"tool_call_id":"tc-rap-b5","output":"peer"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c4, StatusCode::OK, "body={b4}");
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let (tb, fb, _) = st
            .runtime
            .begin_turn(sid, b, hearth::HolderId::new("rap-m5-b"))
            .expect("lease B while A yielded");
        let lease_b = st
            .runtime
            .turn_lease_of(sid, b)
            .unwrap()
            .expect("raw turn_lease_of(B)");
        assert_eq!(lease_b.agent, b);
        assert_eq!(lease_b.holder, hearth::HolderId::new("rap-m5-b"));
        assert_eq!(lease_b.turn_id, tb);
        assert_eq!(lease_b.fence, fb);
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fb
        );

        let remint_err = st.runtime.remint(sid, a).expect_err("LiveIdle remint");
        assert!(
            matches!(
                remint_err,
                hearth::Error::Waiting(hearth::WaitReason::AlreadyLive)
            ),
            "expected AlreadyLive under concurrent peer LiveInTurn, got {remint_err:?}"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(count_tool_interrupted(&st, sid, "tc-rap-a5"), 0);
        assert!(
            !hearth::tool_call_is_terminal(
                &st.store().session(sid).unwrap().events().unwrap(),
                "tc-rap-a5"
            ),
            "LiveIdle remint must not finalize Host tool waiter"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn,
            "remint of A must not disturb peer B"
        );
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(
            st.runtime.turn_lease_of(sid, b).unwrap().expect("B leased"),
            lease_b,
            "raw turn_lease_of(B) pinned across A's AlreadyLive remint"
        );
        assert_eq!(
            st.store().session(sid).unwrap().turn_fence_of(b).unwrap(),
            fb,
            "raw turn_fence_of(B) pinned across A's AlreadyLive remint"
        );
    }

    /// M6: ACP HostAttach drain ToolCall/ToolResult unused path byte-identical
    /// under Concurrent-host-tool-rap (no ACP yield / RAP-replace smuggle).
    #[tokio::test]
    async fn http_concurrent_rap_leaves_acp_drain_path_byte_identical() {
        use hearth_agent_wire::{event_bodies, AgentEvent, CapabilityFlags, ToolStatus};
        let agent = hearth::AgentId(uuid::Uuid::nil());
        let call = AgentEvent::ToolCall {
            session_id: "s".into(),
            item_id: "drain-rap6".into(),
            name: "Bash".into(),
            arguments: serde_json::json!({}),
            status: ToolStatus::Completed,
            parent: None,
            subagent: false,
        };
        let result = AgentEvent::ToolResult {
            session_id: "s".into(),
            item_id: "drain-rap6".into(),
            output: serde_json::json!("ok"),
            status: ToolStatus::Completed,
        };
        let bodies_call = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &call,
            CapabilityFlags::ACP,
        );
        let bodies_res = event_bodies(
            hearth::BindingId(uuid::Uuid::nil()),
            agent,
            &result,
            CapabilityFlags::ACP,
        );
        assert!(matches!(
            &bodies_call[0],
            EventBody::ToolCall {
                tool_call_id,
                name,
                ..
            } if tool_call_id == "drain-rap6" && name == "Bash"
        ));
        assert!(matches!(
            &bodies_res[0],
            EventBody::ToolResult {
                tool_call_id,
                name,
                output,
                ..
            } if tool_call_id == "drain-rap6" && name == "drain-rap6" && output.contains("ok")
        ));
        assert!(
            !bodies_call
                .iter()
                .chain(bodies_res.iter())
                .any(|b| matches!(b, EventBody::TurnEnd { .. })),
            "ACP drain must not smuggle turn_end"
        );
        assert!(
            !bodies_call
                .iter()
                .chain(bodies_res.iter())
                .any(|b| matches!(
                    b,
                    EventBody::Wake {
                        source: hearth::WakeSource::Trigger { .. }
                    }
                )),
            "ACP drain must not smuggle Trigger wake"
        );
    }

    // ---- Host-wire-remint-settle (M1–M5) ----

    fn count_mints(st: &AppState, sid: SessionId) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count()
    }

    fn count_user_messages(st: &AppState, sid: SessionId) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count()
    }

    fn count_fail_closed(st: &AppState, sid: SessionId, agent: hearth::AgentId) -> usize {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        wires
            .get(agent)
            .unwrap()
            .remint
            .observe()
            .iter()
            .filter(|e| e.kind == hearth_agent_wire::RemintEventKind::FailClosed)
            .count()
    }

    fn host_old_wire(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
    ) -> hearth_agent_wire::OldWire {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        wires.get(agent).unwrap().old_wire.clone()
    }

    async fn steer(st: &AppState, sid: SessionId, msg: &str) -> (StatusCode, String) {
        let id = sid.0.to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"user":"cheng","message":"{msg}"}}"#
                )))
                .unwrap(),
        )
        .await
    }

    /// Attach a lease to the wake-opened turn, then end it (→ LiveIdle).
    fn end_wake_turn(st: &AppState, sid: SessionId, agent: hearth::AgentId, holder: &str) {
        let (_t, fence, _) = st
            .runtime
            .begin_turn(sid, agent, hearth::HolderId::new(holder))
            .expect("lease wake-opened turn");
        st.runtime
            .end_turn(sid, holder.into(), fence, false)
            .unwrap();
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::LiveIdle
        );
    }

    /// M1 (Host-wire-remint-settle): steer #1 remints both lost ParallelSafe
    /// Agents (+2); after both turns end, steer #2 reuses the live Bindings —
    /// 200, zero new BindingAttached, Binding ids unchanged, both LiveInTurn,
    /// exactly one new UserMessage, no FailClosed / unbind.
    /// Tip mutant (no steer-path settle): 500, +6 mints, both unbound.
    #[tokio::test]
    async fn http_steer_after_remint_second_steer_reuses_live_no_mint() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        let m0 = count_mints(&st, sid);
        let (c1, b1) = steer(&st, sid, "one").await;
        assert_eq!(c1, StatusCode::OK, "steer #1 body={b1}");
        assert_eq!(count_mints(&st, sid), m0 + 2, "steer #1 remints both");
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        end_wake_turn(&st, sid, a, "ha");
        end_wake_turn(&st, sid, b, "hb");
        let fc_a = count_fail_closed(&st, sid, a);
        let fc_b = count_fail_closed(&st, sid, b);
        let m1 = count_mints(&st, sid);
        let ums1 = count_user_messages(&st, sid);

        let (c2, b2) = steer(&st, sid, "two").await;
        assert_eq!(
            c2,
            StatusCode::OK,
            "steer #2 must reuse live; body={b2} mints+={} bind_a={:?} bind_b={:?}",
            count_mints(&st, sid) - m1,
            st.runtime.binding_for(sid, a).map(|x| x == bid_a),
            st.runtime.binding_for(sid, b).map(|x| x == bid_b),
        );
        assert!(b2.contains("two"), "body={b2}");
        assert_eq!(
            count_mints(&st, sid),
            m1,
            "steer #2: zero new BindingAttached"
        );
        assert_eq!(
            st.runtime.binding_for(sid, a).unwrap(),
            bid_a,
            "A Binding kept"
        );
        assert_eq!(
            st.runtime.binding_for(sid, b).unwrap(),
            bid_b,
            "B Binding kept"
        );
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
        }
        assert_eq!(
            count_user_messages(&st, sid),
            ums1 + 1,
            "one new UserMessage"
        );
        assert_eq!(count_fail_closed(&st, sid, a), fc_a, "no FailClosed A");
        assert_eq!(count_fail_closed(&st, sid, b), fc_b, "no FailClosed B");
    }

    /// M2 (Host-wire-remint-settle): steer remints A; A ends its turn; RAP
    /// tool-call → tool-result → 200 Applied + single-Agent Trigger + A
    /// LiveInTurn with zero mints and A's Binding unchanged; peer B untouched.
    /// Tip mutant: 500, +1 mint, A's live Binding replaced.
    #[tokio::test]
    async fn http_rap_after_steer_remint_tool_result_reuses_live_no_mint() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        let m0 = count_mints(&st, sid);
        let (c1, b1) = steer(&st, sid, "one").await;
        assert_eq!(c1, StatusCode::OK, "steer body={b1}");
        assert_eq!(count_mints(&st, sid), m0 + 2);
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let turn_b = session.current_turn_of(b).unwrap();
        assert!(turn_b.is_some(), "B LiveInTurn from steer");
        end_wake_turn(&st, sid, a, "ha");
        let m1 = count_mints(&st, sid);
        let triggers0 = count_trigger_wakes(&st, sid);

        let app = router(st.clone());
        let (c2, b2) = call(
            app.clone(),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-call"))
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"settle","tool_call_id":"tc-settle"}"#,
                ))
                .unwrap(),
        )
        .await;
        assert_eq!(c2, StatusCode::OK, "tool-call body={b2}");
        let (c3, b3) = call(
            app,
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-settle","output":"ok"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(
            c3,
            StatusCode::OK,
            "tool-result must reuse live; body={b3} mints+={} bind_a_kept={:?}",
            count_mints(&st, sid) - m1,
            st.runtime.binding_for(sid, a).map(|x| x == bid_a),
        );
        assert!(b3.contains(r#""applied":true"#), "body={b3}");
        assert_eq!(
            count_trigger_wakes(&st, sid),
            triggers0 + 1,
            "single Trigger"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(count_mints(&st, sid), m1, "RAP after steer remint: 0 mints");
        assert_eq!(
            st.runtime.binding_for(sid, a).unwrap(),
            bid_a,
            "A Binding kept"
        );
        // Peer B untouched.
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(session.current_turn_of(b).unwrap(), turn_b);
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M3 (Host-wire-remint-settle): settle only Agents that Reminted /
    /// ReuseLive. A no-resume lost → caps-refuse FailClosed-skip; B resume
    /// lost → reminted. After steer A's map evidence stays lost, B LiveIdle.
    /// Follow-up steer: A typed-refused again (no mint for A), B ReuseLive
    /// (0 mints). Mutant "settle every wire in the Session" fails.
    #[tokio::test]
    async fn http_steer_remint_settle_only_reminted_agent_skip_keeps_evidence() {
        use hearth_agent_wire::{LiveCaps as WireCaps, OldWire, RemintEventKind};

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let no_resume = WireCaps {
            resume: false,
            load_session: false,
            cancel: true,
            close: true,
        };
        let (a, b, _) = install_two_parallel_safe_host_wires_with_per_agent_caps(
            &st,
            sid,
            Some(OldWire::Exited(1)),
            no_resume,
            resume_capable_caps(),
        );
        let m0 = count_mints(&st, sid);
        let (c1, b1) = steer(&st, sid, "one").await;
        assert_eq!(c1, StatusCode::OK, "B remint keeps steer 200; body={b1}");
        assert_eq!(count_mints(&st, sid), m0 + 1, "only B minted");
        assert_eq!(
            host_old_wire(&st, sid, a),
            OldWire::Exited(1),
            "A skipped (caps-refuse FailClosed): evidence unchanged, not LiveIdle"
        );
        assert_eq!(host_old_wire(&st, sid, b), OldWire::LiveIdle, "B settled");
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        end_wake_turn(&st, sid, b, "hb");
        let refuse_a = |st: &AppState| {
            let sw = session_wires_arc(st, sid);
            let wires = sw.lock().unwrap();
            wires
                .get(a)
                .unwrap()
                .remint
                .observe()
                .iter()
                .filter(|e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported")
                .count()
        };
        let fc_a = refuse_a(&st);
        assert!(fc_a >= 1, "A FailClosed on steer #1");
        let fc_b = count_fail_closed(&st, sid, b);
        let m1 = count_mints(&st, sid);

        let (c2, b2) = steer(&st, sid, "two").await;
        assert_eq!(c2, StatusCode::OK, "B ReuseLive keeps steer 200; body={b2}");
        assert_eq!(
            count_mints(&st, sid),
            m1,
            "steer #2: 0 mints (A refused, B reuse)"
        );
        assert_eq!(
            refuse_a(&st),
            fc_a + 1,
            "A typed-refused again (lost evidence kept)"
        );
        assert_eq!(host_old_wire(&st, sid, a), OldWire::Exited(1));
        {
            let sw = session_wires_arc(&st, sid);
            let wires = sw.lock().unwrap();
            assert!(
                wires.get(a).unwrap().new.stdin_log().unwrap().is_empty(),
                "A never spawned / initialized a new wire"
            );
        }
        assert!(st.runtime.binding_for(sid, a).is_err(), "A still unbound");
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b, "B reused");
        assert_eq!(count_fail_closed(&st, sid, b), fc_b, "B no FailClosed");
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M4 (Host-wire-remint-settle): settle survives a peer abort. A lost +
    /// Place, B lost with no Place (ResumeNoPlace still aborts the fan-out —
    /// abort semantics unchanged). Steer #1 → 500 with A minted exactly once
    /// and A's evidence LiveIdle. Install B's Place; steer #2 → A ReuseLive
    /// (0 mints for A), B remints once. Mutant "settle from the Woke return"
    /// fails (lost on Err).
    #[tokio::test]
    async fn http_steer_remint_settle_survives_peer_abort() {
        use hearth_agent_wire::OldWire;

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, place) =
            install_two_parallel_safe_host_wires(&st, sid, Some(OldWire::Exited(1)));
        assert_eq!(st.runtime.kept_agents(sid), vec![a, b], "A runs before B");
        session_wires_arc(&st, sid)
            .lock()
            .unwrap()
            .get_mut(b)
            .unwrap()
            .place = None;
        let bid_b0 = st.runtime.binding_for(sid, b).unwrap();
        let m0 = count_mints(&st, sid);

        let (c1, b1) = steer(&st, sid, "one").await;
        assert_eq!(
            c1,
            StatusCode::INTERNAL_SERVER_ERROR,
            "B ResumeNoPlace aborts fan-out (unchanged); body={b1}"
        );
        assert_eq!(count_mints(&st, sid), m0 + 1, "A minted exactly once");
        assert_eq!(
            host_old_wire(&st, sid, a),
            OldWire::LiveIdle,
            "A settled at remint time despite peer abort"
        );
        assert_eq!(
            host_old_wire(&st, sid, b),
            OldWire::Exited(1),
            "B unsettled"
        );
        assert_eq!(
            st.runtime.binding_for(sid, b).unwrap(),
            bid_b0,
            "B untouched"
        );
        let bid_a = st.runtime.binding_for(sid, a).unwrap();

        session_wires_arc(&st, sid)
            .lock()
            .unwrap()
            .get_mut(b)
            .unwrap()
            .place = Some(place);
        let (c2, b2) = steer(&st, sid, "two").await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert_eq!(
            count_mints(&st, sid),
            m0 + 2,
            "steer #2: B remints once; A 0 mints"
        );
        assert_eq!(
            st.runtime.binding_for(sid, a).unwrap(),
            bid_a,
            "A ReuseLive"
        );
        assert_ne!(
            st.runtime.binding_for(sid, b).unwrap(),
            bid_b0,
            "B reminted"
        );
        for ag in [a, b] {
            assert_eq!(host_old_wire(&st, sid, ag), OldWire::LiveIdle);
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
        }
    }

    /// Runner-swap pins for M5: after a Reminted settle the map's `old` is the
    /// resumed Fake child (session/resume on its stdin, never Exited(code)),
    /// `new` is the reaped Real child (Exited(code)).
    fn assert_runner_swapped(st: &AppState, sid: SessionId, agent: hearth::AgentId, code: i32) {
        let sw = session_wires_arc(st, sid);
        let mut wires = sw.lock().unwrap();
        let w = wires.get_mut(agent).unwrap();
        assert_eq!(w.old_wire, hearth_agent_wire::OldWire::LiveIdle);
        assert_ne!(
            w.old.probe_exit(),
            hearth_agent_wire::OldWire::Exited(code),
            "map old must be the resumed runner, not the killed Real child"
        );
        let raw = std::str::from_utf8(w.old.stdin_log().expect("old is the Fake resumed runner"))
            .unwrap()
            .to_string();
        assert_eq!(
            raw.matches("session/resume").count(),
            1,
            "old resumed once: {raw}"
        );
        assert!(!raw.contains("session/load") && !raw.contains("session/new"));
        assert!(w.new.stdin_log().is_none(), "new is the reaped Real child");
        assert_eq!(
            w.new.probe_exit(),
            hearth_agent_wire::OldWire::Exited(code),
            "reaped Real child parked as new"
        );
    }

    /// M5 (Host-wire-remint-settle): Real-exited steer remint swaps runners.
    /// Map `old` is the resumed child; a later forced loss acts on it (kills
    /// the resumed runner) and never re-initializes it as `new`.
    /// Mutant "settle old_wire without swapping runners" fails.
    #[tokio::test]
    async fn http_steer_remint_swaps_runner_old_is_resumed_child() {
        use hearth_agent_wire::OldWire;

        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_real_exited_host_wires(&st, sid);
        let (c1, b1) = steer(&st, sid, "one").await;
        assert_eq!(c1, StatusCode::OK, "body={b1}");
        assert_runner_swapped(&st, sid, a, 7);
        assert_runner_swapped(&st, sid, b, 8);

        // Later forced loss on A: host_wake must kill the resumed runner
        // (one live child per Agent) and must not re-initialize it as `new`.
        end_wake_turn(&st, sid, a, "ha");
        end_wake_turn(&st, sid, b, "hb");
        st.set_host_old_wire(sid, a, OldWire::Exited(7)).unwrap();
        let (_c2, _b2) = steer(&st, sid, "two").await;
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        let w = wires.get_mut(a).unwrap();
        // Soft (parked): `new` is the reaped child, so the remint itself fails
        // honestly; this floor only pins which runner the loss acted on.
        let (old_exit, old_raw) = (
            w.old.try_wait().unwrap(),
            std::str::from_utf8(w.old.stdin_log().unwrap())
                .unwrap()
                .to_string(),
        );
        assert_eq!(old_exit, Some(9), "forced loss killed the resumed runner");
        assert_eq!(
            old_raw.matches("\"initialize\"").count(),
            1,
            "resumed runner never re-initialized as new: {old_raw}"
        );
        assert_eq!(old_raw.matches("session/resume").count(), 1, "{old_raw}");
    }

    /// M5 (tool-result twin): `complete_host_tool_for` Reminted settle swaps
    /// runners too. Mutant "drop the complete_host_tool_for swap" fails.
    #[tokio::test]
    async fn http_tool_result_remint_swaps_runner_old_is_resumed_child() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_real_exited_host_wires(&st, sid);
        record_host_tool_call(&st, sid, a, "tc-swap", "swap_tool");
        let m0 = count_mints(&st, sid);
        let (code, body) = call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"tc-swap","output":"ok"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_mints(&st, sid), m0 + 1, "A reminted once");
        assert_runner_swapped(&st, sid, a, 7);
        // Peer B untouched: still the Real lost child, evidence unchanged.
        assert_eq!(
            host_old_wire(&st, sid, b),
            hearth_agent_wire::OldWire::Exited(8)
        );
    }

    // ---- Host-wire-session-isolation (M1–M5) ----

    /// Replace Session X Agent A's `new` with a Real CLI that never answers
    /// `initialize` (`sh -c 'sleep 5'`), and mark old evidence lost so the
    /// next steer remints through the hung child. B stays LiveIdle.
    fn install_hung_remint_on_a(st: &AppState, sx: SessionId, ax: hearth::AgentId) {
        let sw = session_wires_arc(st, sx);
        let mut wires = sw.lock().unwrap();
        let w = wires.get_mut(ax).unwrap();
        w.old_wire = hearth_agent_wire::OldWire::Exited(1);
        w.new = hearth_agent_wire::AttachRunner::spawn(hearth_agent_wire::LaunchSpec {
            program: "sh".into(),
            args: vec!["-c".into(), "sleep 5".into()],
            cwd: None,
            env: Vec::new(),
            wire: hearth_agent_wire::WireKind::JsonlRpc,
        })
        .expect("spawn hung new wire");
    }

    /// M1: Cross-Session steer isolation. Session X remints A through a hung
    /// Real CLI; Session Y is fully LiveIdle. Y steer must return 200 in
    /// <1.5s with 0 mints while X is still in flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_steer_other_session_not_blocked_by_hung_remint() {
        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let sy = st.store().create_session().id();
        let (ax, _bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        let (ay, by, _) = install_two_parallel_safe_host_wires(&st, sy, None);
        install_hung_remint_on_a(&st, sx, ax);

        let m0y = count_mints(&st, sy);
        let stx = st.clone();
        let hx = tokio::spawn(async move { steer(&stx, sx, "x").await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !hx.is_finished(),
            "X remint must still be in flight before Y"
        );

        let t_y = std::time::Instant::now();
        let sty = st.clone();
        let hy = tokio::spawn(async move { steer(&sty, sy, "y").await });
        let (cy, by_body) = hy.await.unwrap();
        let y_elapsed = t_y.elapsed();

        assert_eq!(cy, StatusCode::OK, "Y steer body={by_body}");
        assert!(
            y_elapsed < std::time::Duration::from_millis(1500),
            "Y steer blocked behind X remint: {y_elapsed:?} x_finished={}",
            hx.is_finished()
        );
        assert!(
            !hx.is_finished(),
            "X must still be in flight after Y returns (isolation); y_elapsed={y_elapsed:?}"
        );
        assert_eq!(count_mints(&st, sy), m0y, "Y: 0 new BindingAttached");
        assert!(by_body.contains("y"), "body={by_body}");
        for ag in [ay, by] {
            assert_eq!(
                st.runtime.classify(sy, ag).unwrap(),
                hearth::Liveness::LiveInTurn,
                "Y agent {ag:?}"
            );
        }
        assert_eq!(count_user_messages(&st, sy), 1);

        let (cx, bx_body) = hx.await.unwrap();
        assert_eq!(cx, StatusCode::OK, "X eventually completes; body={bx_body}");
    }

    /// M2: Cross-Session RAP isolation. During X's hung remint, Y tool-result
    /// → 200 applied in <1.5s, Trigger, LiveInTurn, 0 mints.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_tool_result_other_session_not_blocked_by_hung_remint() {
        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let sy = st.store().create_session().id();
        let (ax, _bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        let (ay, _by, _) = install_two_parallel_safe_host_wires(&st, sy, None);
        record_host_tool_call(&st, sy, ay, "tc-y", "ytool");
        install_hung_remint_on_a(&st, sx, ax);

        let m0y = count_mints(&st, sy);
        let triggers0 = count_trigger_wakes(&st, sy);
        let stx = st.clone();
        let hx = tokio::spawn(async move { steer(&stx, sx, "x").await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !hx.is_finished(),
            "X remint must still be in flight before Y"
        );

        let t_y = std::time::Instant::now();
        let sty = st.clone();
        let idy = sy.0.to_string();
        let hy = tokio::spawn(async move {
            call(
                router(sty),
                Request::post(format!("/sessions/{idy}/agents/alpha/tool-result"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"tool_call_id":"tc-y","output":"ok"}"#))
                    .unwrap(),
            )
            .await
        });
        let (cy, by_body) = hy.await.unwrap();
        let y_elapsed = t_y.elapsed();

        assert_eq!(cy, StatusCode::OK, "Y tool-result body={by_body}");
        assert!(
            y_elapsed < std::time::Duration::from_millis(1500),
            "Y tool-result blocked behind X remint: {y_elapsed:?} x_finished={}",
            hx.is_finished()
        );
        assert!(
            !hx.is_finished(),
            "X must still be in flight after Y tool-result returns; y_elapsed={y_elapsed:?}"
        );
        assert!(by_body.contains(r#""applied":true"#), "body={by_body}");
        assert_eq!(count_mints(&st, sy), m0y, "Y: 0 mints");
        assert_eq!(
            count_trigger_wakes(&st, sy),
            triggers0 + 1,
            "single-Agent Trigger"
        );
        assert_eq!(
            st.runtime.classify(sy, ay).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let (cx, bx_body) = hx.await.unwrap();
        assert_eq!(cx, StatusCode::OK, "X eventually completes; body={bx_body}");
    }

    /// M3: Same-Session serialize, no Goose fallback. During X remint, peer
    /// beta tool-result waits on the Session lock. After fan-out B is
    /// LiveInTurn, so tool-result is pinned as typed **409** (Waiting) — not
    /// Goose ClaimBinding. Binding unchanged; ToolResult still applied (M8
    /// append-before-wake). Concurrent 2nd X steer stays blocked on the same
    /// Session lock (does not Goose-fast finish) while the first remint is in
    /// flight.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_same_session_tool_result_during_remint_serializes_no_goose_fallback() {
        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let id = sx.0.to_string();
        let (ax, bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        record_host_tool_call(&st, sx, bx, "tc-beta", "beta_tool");
        install_hung_remint_on_a(&st, sx, ax);

        let bid_b0 = st.runtime.binding_for(sx, bx).unwrap();
        let mints_b0 = st
            .store()
            .session(sx)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::BindingAttached { agent: Some(a), .. } if *a == bx
                )
            })
            .count();
        let stx = st.clone();
        let hx = tokio::spawn(async move { steer(&stx, sx, "x1").await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!hx.is_finished(), "X remint in flight");

        let stx2 = st.clone();
        let hx2 = tokio::spawn(async move { steer(&stx2, sx, "x2").await });
        let sttr = st.clone();
        let idtr = id.clone();
        let htr = tokio::spawn(async move {
            call(
                router(sttr),
                Request::post(format!("/sessions/{idtr}/agents/beta/tool-result"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"tool_call_id":"tc-beta","output":"ok"}"#))
                    .unwrap(),
            )
            .await
        });

        // Same-Session serialize: neither peer request finishes via Goose while
        // the hung remint still holds the Session lock.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !hx.is_finished(),
            "X remint still in flight during serialize window"
        );
        assert!(
            !hx2.is_finished(),
            "2nd X steer must serialize behind remint (not Goose-fast)"
        );
        assert!(
            !htr.is_finished(),
            "beta tool-result must serialize behind remint (not Goose-fast)"
        );

        let (c1, b1) = hx.await.unwrap();
        assert_eq!(c1, StatusCode::OK, "X steer #1 body={b1}");

        let (ctr, btr) = htr.await.unwrap();
        // Pin: typed 409 Waiting (B LiveInTurn after steer fan-out). Soft
        // alternative was serialized 200; LiveInTurn makes Waiting the honest
        // Host-wire outcome — never Goose ClaimBinding.
        assert_eq!(
            ctr,
            StatusCode::CONFLICT,
            "pinned: typed 409 Waiting after fan-out; body={btr}"
        );
        assert_eq!(
            st.runtime.binding_for(sx, bx).unwrap(),
            bid_b0,
            "B Binding unchanged — no Goose ClaimBinding mint"
        );
        let mints_b1 = st
            .store()
            .session(sx)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::BindingAttached { agent: Some(a), .. } if *a == bx
                )
            })
            .count();
        assert_eq!(mints_b1, mints_b0, "no ClaimBinding mint for B");
        let trs = st
            .store()
            .session(sx)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::ToolResult { tool_call_id, .. } if tool_call_id == "tc-beta"
                )
            })
            .count();
        assert_eq!(trs, 1, "exactly one ToolResult");
        // The 2nd X steer may take the Session lock right after the beta
        // tool-result (std Mutex is not FIFO) and run its own serialized
        // remint of A (transient Binding per attempt). Join it before reading
        // A's Binding so the assert never races that remint.
        let (_c2, _b2) = hx2.await.unwrap();
        // A FailClosed-unbound after the (serialized) budget; Binding for B kept.
        assert!(
            st.runtime.binding_for(sx, ax).is_err(),
            "A unbound after budget"
        );
    }

    /// M4: X outcome unchanged + wires reinserted. Hung remint → A FailClosed
    /// unbound after #77 budget; B ReuseLive (Binding unchanged); get(X,A) /
    /// get(X,B) present; B evidence LiveIdle, A still lost. ResumeNoPlace
    /// abort path also reinserts (compose with #84 M4).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_steer_hung_remint_session_wires_returned_outcome_unchanged() {
        use hearth_agent_wire::OldWire;

        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let (ax, bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        install_hung_remint_on_a(&st, sx, ax);
        let bid_b0 = st.runtime.binding_for(sx, bx).unwrap();
        let mints_b0 = st
            .store()
            .session(sx)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::BindingAttached { agent: Some(a), .. } if *a == bx
                )
            })
            .count();

        let (cx, bx_body) = steer(&st, sx, "x").await;
        assert_eq!(cx, StatusCode::OK, "body={bx_body}");
        assert!(
            st.runtime.binding_for(sx, ax).is_err(),
            "A FailClosed unbound"
        );
        assert_eq!(
            st.runtime.binding_for(sx, bx).unwrap(),
            bid_b0,
            "B ReuseLive — Binding unchanged"
        );
        let mints_b1 = st
            .store()
            .session(sx)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::BindingAttached { agent: Some(a), .. } if *a == bx
                )
            })
            .count();
        assert_eq!(mints_b1, mints_b0, "B ReuseLive: 0 mints for B");
        assert!(
            count_fail_closed(&st, sx, ax) >= 1,
            "A FailClosed after budget"
        );
        // Wires reinserted for both Agents.
        {
            let sw = session_wires_arc(&st, sx);
            let wires = sw.lock().unwrap();
            assert!(wires.get(ax).is_some(), "get(X,A) present after return");
            assert!(wires.get(bx).is_some(), "get(X,B) present after return");
        }
        assert_eq!(host_old_wire(&st, sx, bx), OldWire::LiveIdle, "B settled");
        assert_eq!(
            host_old_wire(&st, sx, ax),
            OldWire::Exited(1),
            "A still lost (#84 settle: FailClosed keeps evidence)"
        );
        end_wake_turn(&st, sx, bx, "hb");
        assert_eq!(
            st.runtime.classify(sx, bx).unwrap(),
            hearth::Liveness::LiveIdle
        );

        // ResumeNoPlace abort path reinserts wires (#84 M4 compose).
        let sy = st.store().create_session().id();
        let (ay, by, place) =
            install_two_parallel_safe_host_wires(&st, sy, Some(OldWire::Exited(1)));
        assert_eq!(st.runtime.kept_agents(sy), vec![ay, by]);
        session_wires_arc(&st, sy)
            .lock()
            .unwrap()
            .get_mut(by)
            .unwrap()
            .place = None;
        let (c_abort, _) = steer(&st, sy, "abort").await;
        assert_eq!(
            c_abort,
            StatusCode::INTERNAL_SERVER_ERROR,
            "B ResumeNoPlace aborts"
        );
        {
            let sw = session_wires_arc(&st, sy);
            let wires = sw.lock().unwrap();
            assert!(wires.get(ay).is_some(), "A wire reinserted on Err");
            assert!(wires.get(by).is_some(), "B wire reinserted on Err");
        }
        assert_eq!(host_old_wire(&st, sy, ay), OldWire::LiveIdle);
        // Restore Place so we do not leave a poisoned Session hanging in the map.
        session_wires_arc(&st, sy)
            .lock()
            .unwrap()
            .get_mut(by)
            .unwrap()
            .place = Some(place);
    }

    /// M5: While X remint in flight, install_host_wire for Z and
    /// set_host_old_wire on Y each return in <500ms; following Z steer → 200.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_wire_install_other_session_not_blocked_by_hung_remint() {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{OldWire, RemintSession};

        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let sy = st.store().create_session().id();
        let sz = st.store().create_session().id();
        let (ax, _bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        let (_ay, by, _) = install_two_parallel_safe_host_wires(&st, sy, None);
        install_hung_remint_on_a(&st, sx, ax);

        let stx = st.clone();
        let hx = tokio::spawn(async move { steer(&stx, sx, "x").await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!hx.is_finished(), "X remint in flight");

        // set_host_old_wire on Y while X held.
        let t_set = std::time::Instant::now();
        st.set_host_old_wire(sy, by, OldWire::LiveIdle).unwrap();
        let set_elapsed = t_set.elapsed();
        assert!(
            set_elapsed < std::time::Duration::from_millis(500),
            "set_host_old_wire blocked behind X: {set_elapsed:?}"
        );

        // install_host_wire for new Session Z while X held.
        apply_join(&st, sz, Some("cheng".into()), None, None).unwrap();
        apply_join(
            &st,
            sz,
            None,
            Some("alpha".into()),
            Some(vec!["z/a".into()]),
        )
        .unwrap();
        apply_join(&st, sz, None, Some("beta".into()), Some(vec!["z/b".into()])).unwrap();
        let agents = st.runtime.kept_agents(sz);
        let (az, bz) = (agents[0], agents[1]);
        let session = st.store().session(sz).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();
        let uid = {
            let names = st.names.lock().unwrap();
            *names.users.get("cheng").unwrap()
        };
        let t_inst = std::time::Instant::now();
        for (ag, resume_id) in [(az, "S-za"), (bz, "S-zb")] {
            let host = Host::from_bind("acp", Some(resume_id.into()), None);
            session.rebind(None, ag, host).unwrap();
            let remint = RemintSession::on_host_session(
                session.clone(),
                ag,
                uid,
                resume_id,
                resume_capable_caps(),
            )
            .expect("remint");
            let wire = AgentHostWire::fake_live(remint, Some(place.clone()));
            st.install_host_wire(sz, ag, wire).unwrap();
        }
        let inst_elapsed = t_inst.elapsed();
        assert!(
            inst_elapsed < std::time::Duration::from_millis(500),
            "install_host_wire blocked behind X: {inst_elapsed:?}"
        );

        let (cz, bz_body) = steer(&st, sz, "z").await;
        assert_eq!(cz, StatusCode::OK, "Z steer body={bz_body}");
        for ag in [az, bz] {
            assert_eq!(
                st.runtime.classify(sz, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
        }

        let (cx, _) = hx.await.unwrap();
        assert_eq!(cx, StatusCode::OK);
    }

    /// Fix 1 floor: `install_host_wire` on the SAME Session X during X's hung
    /// remint must not pin the cross-Session index (Y steer stays fast).
    /// The outer index lock must never wait on a Session lock.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_wire_install_same_session_during_remint_does_not_block_other_session() {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::RemintSession;
        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let sy = st.store().create_session().id();
        let sz = st.store().create_session().id();
        let (ax, _bx, _) = install_two_parallel_safe_host_wires(&st, sx, None);
        let (_ay, _by, _) = install_two_parallel_safe_host_wires(&st, sy, None);
        install_hung_remint_on_a(&st, sx, ax);
        apply_join(&st, sz, Some("cheng".into()), None, None).unwrap();
        apply_join(
            &st,
            sz,
            None,
            Some("gamma".into()),
            Some(vec!["z/g".into()]),
        )
        .unwrap();
        let gz = st.runtime.kept_agents(sz)[0];
        let session = st.store().session(sz).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();
        let uid = {
            let names = st.names.lock().unwrap();
            *names.users.get("cheng").unwrap()
        };
        let host = Host::from_bind("acp", Some("S-zg".into()), None);
        session.rebind(None, gz, host).unwrap();
        let remint =
            RemintSession::on_host_session(session.clone(), gz, uid, "S-zg", resume_capable_caps())
                .expect("remint");
        let wire = AgentHostWire::fake_live(remint, Some(place));

        let stx = st.clone();
        let hx = tokio::spawn(async move { steer(&stx, sx, "x").await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(!hx.is_finished(), "X remint in flight");
        // Embedder installs a third wire on X while X's remint runs.
        let sti = st.clone();
        let hi = std::thread::spawn(move || sti.install_host_wire(sx, gz, wire).unwrap());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let t_y = std::time::Instant::now();
        let (cy, by_body) = steer(&st, sy, "y").await;
        let y_elapsed = t_y.elapsed();
        let x_fin = hx.is_finished();
        assert_eq!(cy, StatusCode::OK, "body={by_body}");
        let _ = hx.await;
        hi.join().unwrap();
        assert!(
            y_elapsed < std::time::Duration::from_millis(1500),
            "Y steer blocked behind same-Session install on X: {y_elapsed:?} x_finished={x_fin}"
        );
    }

    #[allow(clippy::result_large_err)] // tungstenite::Error is upstream; test-only helper
    async fn expect_text(
        ws: &mut (impl StreamExt<Item = Result<WsMsg, tokio_tungstenite::tungstenite::Error>> + Unpin),
        needle: &str,
    ) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            let msg = tokio::time::timeout(left, ws.next())
                .await
                .unwrap_or_else(|_| panic!("timeout waiting for {needle}"))
                .unwrap_or_else(|| panic!("ws closed waiting for {needle}"))
                .unwrap();
            match msg {
                WsMsg::Text(t) if t.as_str().contains(needle) => return,
                WsMsg::Text(_) => continue,
                other => panic!("unexpected {other:?} waiting for {needle}"),
            }
        }
    }

    async fn http(
        addr: std::net::SocketAddr,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> (u16, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let payload = body.unwrap_or("");
        let req = format!(
            "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len()
        );
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let raw = String::from_utf8_lossy(&buf);
        let (head, rest) = raw.split_once("\r\n\r\n").unwrap_or(("HTTP/1.1 000", ""));
        let code = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (code, rest.to_string())
    }

    // ── Host-fanout-wire-wins ───────────────────────────────────────────
    // The Host steer fan-out turn_starts only Agents admitted by host_wake
    // THIS call (Reminted / ReuseLive). Skipped Agents (no wire, mismatch,
    // classify / gate skip, host_wake Waiting) get zero Runtime writes even
    // when their kernel Binding is live. Never unbind on Waiting.

    fn fww_mints(st: &AppState, sid: SessionId) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::BindingAttached { .. }))
            .count()
    }

    fn fww_user_messages(st: &AppState, sid: SessionId) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| matches!(e.body, EventBody::UserMessage { .. }))
            .count()
    }

    /// Agents of every TurnStart on the log, in log order.
    fn fww_turn_starts(st: &AppState, sid: SessionId) -> Vec<hearth::AgentId> {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter_map(|e| match e.body {
                EventBody::TurnStart { agent } => Some(agent),
                _ => None,
            })
            .collect()
    }

    async fn fww_steer(st: &AppState, sid: SessionId, msg: &str) -> (StatusCode, String) {
        let id = sid.0.to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "user": "cheng", "message": msg }).to_string(),
                ))
                .unwrap(),
        )
        .await
    }

    /// Two ParallelSafe Agents, both Host-bound (kernel LiveIdle), but only
    /// A gets its own wire installed. Returns (a, b, place, cheng).
    fn fww_install_two_bound_only_a_wired(
        st: &AppState,
        sid: SessionId,
    ) -> (
        hearth::AgentId,
        hearth::AgentId,
        hearth::Place,
        hearth::UserId,
    ) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::RemintSession;

        apply_join(st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("alpha".into()),
            Some(vec!["agent-0".into()]),
        )
        .unwrap();
        apply_join(
            st,
            sid,
            None,
            Some("beta".into()),
            Some(vec!["agent-1".into()]),
        )
        .unwrap();
        let agents = st.runtime.kept_agents(sid);
        let (a, b) = (agents[0], agents[1]);
        let session = st.store().session(sid).unwrap();
        let place = hearth::Place::local_dir(
            std::env::temp_dir().to_string_lossy().as_ref(),
            PlaceAttach::MustExist,
        );
        session.attach_place(place.clone()).unwrap();
        let uid = *st.names.lock().unwrap().users.get("cheng").unwrap();
        for (ag, resume_id) in [(a, "S-a"), (b, "S-b")] {
            session
                .rebind(
                    None,
                    ag,
                    Host::from_bind("acp", Some(resume_id.into()), None),
                )
                .unwrap();
        }
        let remint_a =
            RemintSession::on_host_session(session.clone(), a, uid, "S-a", resume_capable_caps())
                .expect("remint A");
        st.install_host_wire(
            sid,
            a,
            AgentHostWire::fake_live(remint_a, Some(place.clone())),
        )
        .unwrap();
        (a, b, place, uid)
    }

    /// M1: ParallelSafe A LiveIdle wire; B **bound** LiveIdle + wire Unknown
    /// (no unbind). Steer → 200; A turns; B no turn, still LiveIdle, Binding
    /// id unchanged; one UserMessage; 0 mints; TurnStart agents == [A]
    /// (woke ⊆ hosts). Tip mutant: B ghost-turns. "Unbind Waiting" mutant:
    /// B Binding changes / not LiveIdle.
    #[tokio::test]
    async fn http_steer_unknown_bound_peer_no_ghost_turn() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.set_host_old_wire(sid, b, hearth_agent_wire::OldWire::Unknown)
            .unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "precondition: B kernel Binding live (no unbind)"
        );
        let (m0, u0) = (fww_mints(&st, sid), fww_user_messages(&st, sid));

        let (code, body) = fww_steer(&st, sid, "m1").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains("m1"));

        let session = st.store().session(sid).unwrap();
        assert!(session.current_turn_of(a).unwrap().is_some(), "A turns");
        assert!(
            session.current_turn_of(b).unwrap().is_none(),
            "B Unknown: no ghost TurnStart"
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveIdle,
            "B stays LiveIdle (never unbind on Waiting)"
        );
        assert_eq!(
            st.runtime.binding_for(sid, b).unwrap(),
            bid_b,
            "B Binding id unchanged"
        );
        assert_eq!(fww_user_messages(&st, sid), u0 + 1, "one UserMessage");
        assert_eq!(fww_mints(&st, sid), m0, "0 new BindingAttached");
        assert_eq!(fww_turn_starts(&st, sid), vec![a], "woke == hosts == [A]");
    }

    /// M2: serialize (no parallel_safe). A first in keep order, bound, wire
    /// Unknown; B LiveIdle wire. Steer → 200, B turns, A none. B end-turn,
    /// steer again → B turns again, A still none; never 409-wedged.
    #[tokio::test]
    async fn http_steer_unknown_first_agent_does_not_steal_gate() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let id = sid.0.to_string();
        let (a, b, _) =
            install_two_host_wires_with_declare(&st, sid, None, None, None, resume_capable_caps());
        st.set_host_old_wire(sid, a, hearth_agent_wire::OldWire::Unknown)
            .unwrap();
        let bid_a = st.runtime.binding_for(sid, a).unwrap();

        let (code, body) = fww_steer(&st, sid, "m2-0").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_none(),
            "Unknown A must not take the turn / gate"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_some(),
            "healthy ReuseLive B gets the turn"
        );

        let (code, body) = call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/beta/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::OK, "body={body}");

        let (code, body) = fww_steer(&st, sid, "m2-1").await;
        assert_eq!(code, StatusCode::OK, "not 409-wedged; body={body}");
        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_none(),
            "A still none"
        );
        assert!(session.current_turn_of(b).unwrap().is_some(), "B again");
        assert_eq!(fww_turn_starts(&st, sid), vec![b, b]);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
    }

    /// M3: wired Session; kept B bound but (i) no wire of its own, or (ii) its
    /// slot holds a wire whose host_agent_id is A (mismatch). Steer → A turns;
    /// B no turn, no claim, no mint.
    #[tokio::test]
    async fn http_steer_unwired_or_mismatched_bound_peer_no_turn() {
        for mismatched in [false, true] {
            let st = AppState::new(vec![]);
            let sid = st.store().create_session().id();
            let (a, b, place, uid) = fww_install_two_bound_only_a_wired(&st, sid);
            if mismatched {
                let session = st.store().session(sid).unwrap();
                let remint_for_a = hearth_agent_wire::RemintSession::on_host_session(
                    session,
                    a,
                    uid,
                    "S-a",
                    resume_capable_caps(),
                )
                .expect("remint for A");
                st.install_host_wire(sid, b, AgentHostWire::fake_live(remint_for_a, Some(place)))
                    .unwrap();
            }
            let bid_b = st.runtime.binding_for(sid, b).unwrap();
            assert_eq!(
                st.runtime.classify(sid, b).unwrap(),
                hearth::Liveness::LiveIdle
            );
            let m0 = fww_mints(&st, sid);

            let (code, body) = fww_steer(&st, sid, "m3").await;
            assert_eq!(code, StatusCode::OK, "mismatched={mismatched} body={body}");
            let session = st.store().session(sid).unwrap();
            assert!(session.current_turn_of(a).unwrap().is_some());
            assert!(
                session.current_turn_of(b).unwrap().is_none(),
                "mismatched={mismatched}: B has no wire evidence → no turn"
            );
            assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b, "no claim");
            assert_eq!(fww_mints(&st, sid), m0, "no mint");
            assert_eq!(fww_turn_starts(&st, sid), vec![a]);
        }
    }

    /// M4 (HTTP): both bound, both wires Unknown → every host_wake Waiting →
    /// 409, 0 UserMessage, 0 TurnStart, 0 mints; Bindings unchanged.
    #[tokio::test]
    async fn http_steer_all_host_wake_waiting_409_no_append() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        for ag in [a, b] {
            st.set_host_old_wire(sid, ag, hearth_agent_wire::OldWire::Unknown)
                .unwrap();
        }
        let (bid_a, bid_b) = (
            st.runtime.binding_for(sid, a).unwrap(),
            st.runtime.binding_for(sid, b).unwrap(),
        );
        let (m0, u0, len0) = (
            fww_mints(&st, sid),
            fww_user_messages(&st, sid),
            st.store().session(sid).unwrap().events().unwrap().len(),
        );

        let (code, body) = fww_steer(&st, sid, "m4").await;
        assert_eq!(code, StatusCode::CONFLICT, "body={body}");
        assert_eq!(fww_user_messages(&st, sid), u0, "0 UserMessage");
        assert!(fww_turn_starts(&st, sid).is_empty(), "0 TurnStart");
        assert_eq!(fww_mints(&st, sid), m0, "0 mints");
        assert_eq!(
            st.store().session(sid).unwrap().events().unwrap().len(),
            len0,
            "nothing appended"
        );
        assert_eq!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
    }

    /// M4 (WS twin): same all-Waiting setup over the WS stream → {"error":409}
    /// and nothing appended.
    #[tokio::test]
    async fn ws_steer_all_host_wake_waiting_409_no_append() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        for ag in [a, b] {
            st.set_host_old_wire(sid, ag, hearth_agent_wire::OldWire::Unknown)
                .unwrap();
        }
        let len0 = st.store().session(sid).unwrap().events().unwrap().len();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, router(st.clone())).into_future());
        let url = format!("ws://{addr}/sessions/{id}/stream");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws.send(WsMsg::Text(
            r#"{"type":"message","user":"cheng","message":"m4-ws"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut ws, r#"{"error":409}"#).await;

        assert_eq!(fww_user_messages(&st, sid), 0, "0 UserMessage");
        assert!(fww_turn_starts(&st, sid).is_empty(), "0 TurnStart");
        assert_eq!(
            st.store().session(sid).unwrap().events().unwrap().len(),
            len0,
            "nothing appended"
        );
        let _ = ws.close(None).await;
    }

    /// M6 compose: lost A Reminted + Unknown bound B → A turns once (+1 mint),
    /// B none; RAP tool-result on the same Unknown B → 409 (steer and RAP
    /// agree); an unwired Session still wakes every kept Agent via the kernel
    /// path (not filtered).
    #[tokio::test]
    async fn http_steer_reminted_plus_unknown_peer_admits_reminted_only() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::StdoutEof),
        );
        st.set_host_old_wire(sid, b, hearth_agent_wire::OldWire::Unknown)
            .unwrap();
        let bid_a = st.runtime.binding_for(sid, a).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let m0 = fww_mints(&st, sid);

        let (code, body) = fww_steer(&st, sid, "m6").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let session = st.store().session(sid).unwrap();
        assert!(
            session.current_turn_of(a).unwrap().is_some(),
            "Reminted A turns"
        );
        assert!(
            session.current_turn_of(b).unwrap().is_none(),
            "Unknown B none"
        );
        assert_eq!(fww_mints(&st, sid), m0 + 1, "exactly one mint (A remint)");
        assert_ne!(st.runtime.binding_for(sid, a).unwrap(), bid_a);
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(fww_turn_starts(&st, sid), vec![a]);
        assert_eq!(fww_user_messages(&st, sid), 1);

        // RAP on the same Unknown evidence → 409, no turn (paths agree).
        record_host_tool_call(&st, sid, b, "fww-t1", "x");
        let (code, body) = call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/beta/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"tool_call_id":"fww-t1","output":"ok"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(code, StatusCode::CONFLICT, "body={body}");
        assert!(st
            .store()
            .session(sid)
            .unwrap()
            .current_turn_of(b)
            .unwrap()
            .is_none());

        // Kernel path (unwired Session) unchanged: every kept Agent wakes.
        let ksid = st.store().create_session().id();
        apply_join(&st, ksid, Some("cheng".into()), None, None).unwrap();
        apply_join(
            &st,
            ksid,
            None,
            Some("alpha".into()),
            Some(vec!["k-0".into()]),
        )
        .unwrap();
        apply_join(
            &st,
            ksid,
            None,
            Some("beta".into()),
            Some(vec!["k-1".into()]),
        )
        .unwrap();
        let ks = st.runtime.kept_agents(ksid);
        let (code, body) = fww_steer(&st, ksid, "m6-kernel").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let ksession = st.store().session(ksid).unwrap();
        for ag in &ks {
            assert!(
                ksession.current_turn_of(*ag).unwrap().is_some(),
                "kernel fan-out not filtered"
            );
        }
        assert_eq!(fww_turn_starts(&st, ksid), ks);
    }

    // ---- RAP-remint-retry-budget (M1–M5) ----

    /// Resume-capable Fake new wire whose first `fail` stdin writes fail
    /// (transient `RemintError::NewWire`).
    fn rrb_flaky_new_wire(fail: usize) -> hearth_agent_wire::AttachRunner {
        let init = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "protocolVersion": 1,
                "agentCapabilities": { "session": { "resume": true, "cancel": true, "close": true } } }
        });
        hearth_agent_wire::AttachRunner::spawn_fake_with_stdin_failures(
            hearth_agent_wire::WireKind::JsonlRpc,
            hearth_agent_wire::encode_fake_new_wire_stdout(&init, 2, "native"),
            fail,
        )
    }

    fn rrb_set_new_wire(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
        new: hearth_agent_wire::AttachRunner,
    ) {
        let sw = session_wires_arc(st, sid);
        let mut wires = sw.lock().unwrap();
        wires.get_mut(agent).unwrap().new = new;
    }

    async fn rrb_tool_result(st: &AppState, sid: SessionId, tc: &str) -> (StatusCode, String) {
        let id = sid.0.to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/alpha/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"tool_call_id":"{tc}","output":"o"}}"#
                )))
                .unwrap(),
        )
        .await
    }

    fn rrb_runner_log(st: &AppState, sid: SessionId, agent: hearth::AgentId, old: bool) -> String {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        let w = wires.get(agent).unwrap();
        let r = if old { &w.old } else { &w.new };
        String::from_utf8_lossy(r.stdin_log().unwrap_or_default()).into_owned()
    }

    fn rrb_fail_closed_texts(st: &AppState, sid: SessionId, agent: hearth::AgentId) -> Vec<String> {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        wires
            .get(agent)
            .unwrap()
            .remint
            .observe()
            .iter()
            .filter(|e| e.kind == hearth_agent_wire::RemintEventKind::FailClosed)
            .map(|e| e.text.clone())
            .collect()
    }

    /// M1 (RAP-remint-retry-budget): A's wire `Exited(1)`, resume-capable;
    /// A's new wire fails its first stdin write once. POST tool-result →
    /// 200 applied:true, 1 ToolResult, 1 Trigger, A LiveInTurn, evidence
    /// settled LiveIdle, resumed child (`old` after swap) has session/resume
    /// (no load/new). Re-POST → applied:false, still 1 Trigger.
    /// Tip (single-shot host_wake): 500 with the ToolResult appended,
    /// Trigger 0; every RAP-spec retry applied:false → Trigger lost.
    #[tokio::test]
    async fn http_tool_result_transient_new_wire_retries_then_triggers() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        rrb_set_new_wire(&st, sid, a, rrb_flaky_new_wire(1));
        record_host_tool_call(&st, sid, a, "tc-rrb1", "t");
        let m0 = count_mints(&st, sid);

        let (code, body) = rrb_tool_result(&st, sid, "tc-rrb1").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_tool_results(&st, sid, "tc-rrb1"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1, "Trigger recorded");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert_eq!(
            host_old_wire(&st, sid, a),
            hearth_agent_wire::OldWire::LiveIdle,
            "#84 settle on Applied"
        );
        assert!(count_mints(&st, sid) > m0, "A reminted");
        let raw = rrb_runner_log(&st, sid, a, true);
        assert!(raw.contains("session/resume"), "resumed child: {raw}");
        assert!(
            !raw.contains("session/load") && !raw.contains("session/new"),
            "no load/new: {raw}"
        );
        assert!(
            rrb_fail_closed_texts(&st, sid, a).is_empty(),
            "within budget: no FailClosed"
        );
        // Peer B untouched (single-Agent path, never fan-out).
        assert_eq!(
            host_old_wire(&st, sid, b),
            hearth_agent_wire::OldWire::Exited(1)
        );

        let (code2, body2) = rrb_tool_result(&st, sid, "tc-rrb1").await;
        assert_eq!(code2, StatusCode::OK, "body={body2}");
        assert!(body2.contains(r#""applied":false"#), "body={body2}");
        assert_eq!(count_tool_results(&st, sid, "tc-rrb1"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 1, "still 1 Trigger");
    }

    /// M3 (RAP-remint-retry-budget): new wire fails
    /// `HOST_WAKE_REMINT_RETRY_BUDGET + 1` writes → typed Remint(NewWire) /
    /// HTTP 500; exactly one FailClosed `NewWire`; no successful resume;
    /// ToolResult 1 / Trigger 0 (honest M8); A **not** unbound by the RAP
    /// path; peer B Binding / evidence / lease / fence unchanged.
    /// Kills: unbounded retry, FailClosed per attempt, reused fan-out unbind.
    #[tokio::test]
    async fn http_tool_result_new_wire_retry_exhausted_fail_closed_once() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        rrb_set_new_wire(
            &st,
            sid,
            a,
            rrb_flaky_new_wire(hearth_agent_wire::HOST_WAKE_REMINT_RETRY_BUDGET as usize + 1),
        );
        record_host_tool_call(&st, sid, a, "tc-rrb3", "t");
        let session = st.store().session(sid).unwrap();
        let bid_b = st.runtime.binding_for(sid, b).unwrap();
        let lease_b = format!("{:?}", st.runtime.turn_lease_of(sid, b));
        let fence_b = format!("{:?}", session.turn_fence_of(b));

        let (code, body) = rrb_tool_result(&st, sid, "tc-rrb3").await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "body={body}");
        assert_eq!(
            rrb_fail_closed_texts(&st, sid, a),
            vec!["NewWire".to_string()],
            "exactly one FailClosed NewWire on exhaust"
        );
        let new_raw = rrb_runner_log(&st, sid, a, false);
        assert!(
            !new_raw.contains("session/resume"),
            "exhausted: no successful resume: {new_raw}"
        );
        assert_eq!(count_tool_results(&st, sid, "tc-rrb3"), 1, "M8: appended");
        assert_eq!(count_trigger_wakes(&st, sid), 0, "M8: no Trigger");
        assert!(
            st.runtime.binding_for(sid, a).is_ok(),
            "RAP exhaust must not unbind A (no fan-out unbind)"
        );
        assert!(session.current_turn_of(a).unwrap().is_none());
        assert_eq!(st.runtime.binding_for(sid, b).unwrap(), bid_b);
        assert_eq!(format!("{:?}", st.runtime.turn_lease_of(sid, b)), lease_b);
        assert_eq!(format!("{:?}", session.turn_fence_of(b)), fence_b);
        assert_eq!(
            host_old_wire(&st, sid, b),
            hearth_agent_wire::OldWire::Exited(1)
        );
        assert!(session.current_turn_of(b).unwrap().is_none());
    }

    /// M4 (RAP-remint-retry-budget): caps refuse and no-place never retry.
    /// (1) resume:false caps + Exited → typed 500, FailClosed once, no new
    /// wire writes / spawn / mint. (2) ResumeNoPlace → typed 500, no FailClosed
    /// added, old child not killed (place check before kill), no mint.
    /// Kills "retry every RemintError".
    #[tokio::test]
    async fn http_tool_result_caps_refuse_and_no_place_never_retry() {
        // (1) caps no-resume.
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let caps = hearth_agent_wire::LiveCaps {
            resume: false,
            load_session: false,
            cancel: true,
            close: true,
        };
        let (a, _b, _) = install_two_parallel_safe_host_wires_with_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
            caps,
        );
        record_host_tool_call(&st, sid, a, "tc-rrb4c", "t");
        let m0 = count_mints(&st, sid);
        let (code, body) = rrb_tool_result(&st, sid, "tc-rrb4c").await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "body={body}");
        assert_eq!(
            rrb_fail_closed_texts(&st, sid, a),
            vec!["ResumeNotSupported".to_string()],
            "caps refuse: one attempt, FailClosed once"
        );
        assert!(
            rrb_runner_log(&st, sid, a, false).is_empty(),
            "no new-wire writes after caps refuse"
        );
        assert_eq!(count_mints(&st, sid), m0, "no mint on caps refuse");
        assert_eq!(count_tool_results(&st, sid, "tc-rrb4c"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 0);

        // (2) ResumeNoPlace.
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            wires.get_mut(a).unwrap().place = None;
        }
        record_host_tool_call(&st, sid, a, "tc-rrb4p", "t");
        let m0 = count_mints(&st, sid);
        let fc0 = rrb_fail_closed_texts(&st, sid, a);
        let (code, body) = rrb_tool_result(&st, sid, "tc-rrb4p").await;
        assert_eq!(code, StatusCode::INTERNAL_SERVER_ERROR, "body={body}");
        assert_eq!(
            rrb_fail_closed_texts(&st, sid, a),
            fc0,
            "no-place: no retry-exhaust FailClosed"
        );
        assert!(rrb_runner_log(&st, sid, a, false).is_empty(), "no new wire");
        assert_eq!(count_mints(&st, sid), m0, "no mint on no-place");
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            // Fake child may exit on its own (0); killed+reaped would be 9.
            assert_ne!(w.old.try_wait().unwrap(), Some(9), "old child not killed");
        }
        assert_eq!(count_tool_results(&st, sid, "tc-rrb4p"), 1);
        assert_eq!(count_trigger_wakes(&st, sid), 0);
    }

    /// M5 (RAP-remint-retry-budget): steer and RAP agree on the same wire
    /// evidence + one-time transient NewWire: both end Woke/Applied, with the
    /// same attempt footprint on A's resumed wire (initialize + resume) and
    /// no FailClosed. Tip: steer 200, RAP 500 (parity broken).
    #[tokio::test]
    async fn http_steer_and_rap_same_transient_both_remint() {
        // Steer.
        let st_s = AppState::new(vec![]);
        let sid_s = st_s.store().create_session().id();
        let (a_s, _, _) = install_two_parallel_safe_host_wires(
            &st_s,
            sid_s,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        rrb_set_new_wire(&st_s, sid_s, a_s, rrb_flaky_new_wire(1));
        let (cs, bs) = steer(&st_s, sid_s, "rrb-steer").await;
        assert_eq!(cs, StatusCode::OK, "steer body={bs}");
        assert_eq!(
            st_s.runtime.classify(sid_s, a_s).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        // RAP.
        let st_r = AppState::new(vec![]);
        let sid_r = st_r.store().create_session().id();
        let (a_r, _, _) = install_two_parallel_safe_host_wires(
            &st_r,
            sid_r,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        rrb_set_new_wire(&st_r, sid_r, a_r, rrb_flaky_new_wire(1));
        record_host_tool_call(&st_r, sid_r, a_r, "tc-rrb5", "t");
        let (cr, br) = rrb_tool_result(&st_r, sid_r, "tc-rrb5").await;
        assert_eq!(cr, StatusCode::OK, "RAP body={br}");
        assert!(br.contains(r#""applied":true"#), "RAP body={br}");
        assert_eq!(count_trigger_wakes(&st_r, sid_r), 1);
        assert_eq!(
            st_r.runtime.classify(sid_r, a_r).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        // Same footprint on A's resumed (settled → `old`) wire.
        let ls = rrb_runner_log(&st_s, sid_s, a_s, true);
        let lr = rrb_runner_log(&st_r, sid_r, a_r, true);
        for m in ["\"initialize\"", "session/resume"] {
            assert_eq!(
                ls.matches(m).count(),
                lr.matches(m).count(),
                "{m} parity: steer={ls} rap={lr}"
            );
        }
        assert_eq!(lr.matches("session/resume").count(), 1, "{lr}");
        assert_eq!(
            host_old_wire(&st_s, sid_s, a_s),
            host_old_wire(&st_r, sid_r, a_r)
        );
        assert!(rrb_fail_closed_texts(&st_s, sid_s, a_s).is_empty());
        assert!(rrb_fail_closed_texts(&st_r, sid_r, a_r).is_empty());
    }

    // ---- RAP-classify-before-remint (M1–M5) ----

    /// A joins with `parallel_safe: []` (documented: parallel-safe, touches
    /// no paths — never self-conflicts at the gate), B with `["agent-1"]`.
    fn rcb_setup(paths_a: Vec<String>) -> (AppState, SessionId, hearth::AgentId) {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, _b, _) = install_two_host_wires_with_declare_caps(
            &st,
            sid,
            None,
            Some(paths_a),
            Some(vec!["agent-1".into()]),
            resume_capable_caps(),
            resume_capable_caps(),
        );
        (st, sid, a)
    }

    async fn rcb_post(st: &AppState, path: String, body: &str) -> (StatusCode, String) {
        call(
            router(st.clone()),
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
    }

    async fn rcb_tool_call(st: &AppState, sid: SessionId, tc: &str) {
        let id = sid.0.to_string();
        let (c, b) = rcb_post(
            st,
            format!("/sessions/{id}/agents/alpha/tool-call"),
            &format!(r#"{{"name":"t","tool_call_id":"{tc}"}}"#),
        )
        .await;
        assert_eq!(c, StatusCode::OK, "{b}");
    }

    async fn rcb_tool_result(st: &AppState, sid: SessionId, tc: &str) -> (StatusCode, String) {
        let id = sid.0.to_string();
        rcb_post(
            st,
            format!("/sessions/{id}/agents/alpha/tool-result"),
            &format!(r#"{{"tool_call_id":"{tc}","output":"o"}}"#),
        )
        .await
    }

    async fn rcb_end_turn(st: &AppState, sid: SessionId) {
        let id = sid.0.to_string();
        let (c, b) = rcb_post(st, format!("/sessions/{id}/agents/alpha/end-turn"), "{}").await;
        assert_eq!(c, StatusCode::OK, "{b}");
    }

    /// HTTP steer → A LiveInTurn; Host tool-call tc1 mid-turn; A's CLI dies
    /// (evidence `Exited(1)`, same injection as #84–#87 floors).
    async fn rcb_open_turn_lost(st: &AppState, sid: SessionId, a: hearth::AgentId) {
        let (c, b) = steer(st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        rcb_tool_call(st, sid, "tc1").await;
        st.set_host_old_wire(sid, a, hearth_agent_wire::OldWire::Exited(1))
            .unwrap();
    }

    /// Zero-wire footprint of A: (mints, A Binding, A turn, old stdin,
    /// new stdin, evidence, FailClosed texts). Old-child exit checked apart
    /// (a Fake child may exit 0 on its own; killed+reaped is 9).
    #[allow(clippy::type_complexity)]
    fn rcb_footprint(
        st: &AppState,
        sid: SessionId,
        a: hearth::AgentId,
    ) -> (
        usize,
        Option<hearth::BindingId>,
        Option<u64>,
        String,
        String,
        hearth_agent_wire::OldWire,
        Vec<String>,
    ) {
        (
            count_mints(st, sid),
            st.runtime.binding_for(sid, a).ok(),
            st.store().session(sid).unwrap().current_turn_of(a).unwrap(),
            rrb_runner_log(st, sid, a, true),
            rrb_runner_log(st, sid, a, false),
            host_old_wire(st, sid, a),
            rrb_fail_closed_texts(st, sid, a),
        )
    }

    fn rcb_old_exit(st: &AppState, sid: SessionId, a: hearth::AgentId) -> Option<i32> {
        let sw = session_wires_arc(st, sid);
        let mut wires = sw.lock().unwrap();
        wires.get_mut(a).unwrap().old.try_wait().ok().flatten()
    }

    /// M1 (RAP-classify-before-remint): A (`parallel_safe: []`) under its own
    /// open turn with a lost wire. POST tool-result tc1 → 409, exactly one
    /// ToolResult (M8: append before wake), Trigger 0, and **zero wire
    /// action**: no mint, A Binding + turn unchanged, old child not killed,
    /// new wire stdin empty, evidence still `Exited(1)`, no FailClosed.
    /// Tip: gate passes (`[]` never self-conflicts) → remint under the turn
    /// (+1 mint, old exit 9, Binding swapped, session/resume on new) → 409.
    #[tokio::test]
    async fn http_tool_result_open_turn_empty_parallel_safe_no_remint() {
        let (st, sid, a) = rcb_setup(vec![]);
        rcb_open_turn_lost(&st, sid, a).await;
        let before = rcb_footprint(&st, sid, a);
        let (mints0, bid0, turn0, ..) = before.clone();
        assert!(bid0.is_some() && turn0.is_some());

        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::CONFLICT, "{b}");
        assert_eq!(count_tool_results(&st, sid, "tc1"), 1, "M8: Result lands");
        assert_eq!(count_trigger_wakes(&st, sid), 0);
        let after = rcb_footprint(&st, sid, a);
        assert_eq!(after.0, mints0, "0 mints under an open turn");
        assert_eq!(after.1, bid0, "A Binding unchanged");
        assert_eq!(after.2, turn0, "A's turn still open on the same Binding");
        assert!(after.4.is_empty(), "new wire untouched: {}", after.4);
        assert_eq!(after.5, hearth_agent_wire::OldWire::Exited(1));
        assert!(after.6.is_empty(), "no FailClosed: {:?}", after.6);
        assert_eq!(after, before, "zero wire action");
        assert_ne!(rcb_old_exit(&st, sid, a), Some(9), "old child not killed");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M2 (RAP-classify-before-remint): after M1, Host `end-turn` A → 200;
    /// user steer → 200, A reminted **exactly once**, LiveInTurn, resumed
    /// child settled on `old` (session/resume), evidence LiveIdle, 0
    /// FailClosed, A still bound, +1 UserMessage. Tip: stale map → steer
    /// 500, +3 mints, FailClosed NewWire, A unbound (PositivelyDead).
    #[tokio::test]
    async fn http_open_turn_rap_then_end_turn_steer_remints_once() {
        let (st, sid, a) = rcb_setup(vec![]);
        rcb_open_turn_lost(&st, sid, a).await;
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::CONFLICT, "{b}");
        rcb_end_turn(&st, sid).await;
        let m0 = count_mints(&st, sid);
        let u0 = count_user_messages(&st, sid);

        let (c, b) = steer(&st, sid, "next").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        assert_eq!(count_mints(&st, sid), m0 + 1, "reminted exactly once");
        assert_eq!(count_user_messages(&st, sid), u0 + 1);
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        assert!(st.runtime.binding_for(sid, a).is_ok(), "A not unbound");
        let old_log = rrb_runner_log(&st, sid, a, true);
        assert!(
            old_log.contains("session/resume"),
            "resumed child: {old_log}"
        );
        assert!(!old_log.contains("session/load") && !old_log.contains("session/new"));
        assert_eq!(
            host_old_wire(&st, sid, a),
            hearth_agent_wire::OldWire::LiveIdle
        );
        assert!(rrb_fail_closed_texts(&st, sid, a).is_empty());
    }

    /// M3 (RAP-classify-before-remint): after M1 + `end-turn`, a new RAP
    /// tool-call tc2 → tool-result tc2 → 200 applied:true, Trigger 1, +1
    /// mint, evidence LiveIdle, 0 FailClosed. tc1 stays at one ToolResult
    /// (no redelivery: parked G3). Tip: 500, Trigger 0, +3 mints.
    #[tokio::test]
    async fn http_open_turn_rap_then_end_turn_rap_remints_once() {
        let (st, sid, a) = rcb_setup(vec![]);
        rcb_open_turn_lost(&st, sid, a).await;
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::CONFLICT, "{b}");
        rcb_end_turn(&st, sid).await;
        rcb_tool_call(&st, sid, "tc2").await;
        let m0 = count_mints(&st, sid);

        let (c, b) = rcb_tool_result(&st, sid, "tc2").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        let v: serde_json::Value = serde_json::from_str(&b).unwrap();
        assert_eq!(v["applied"], true, "{b}");
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(count_mints(&st, sid), m0 + 1, "one remint");
        assert_eq!(
            host_old_wire(&st, sid, a),
            hearth_agent_wire::OldWire::LiveIdle
        );
        assert!(rrb_fail_closed_texts(&st, sid, a).is_empty());
        assert_eq!(count_tool_results(&st, sid, "tc1"), 1);
        assert_eq!(count_tool_results(&st, sid, "tc2"), 1);
    }

    /// M5 (RAP-classify-before-remint): steer and RAP on the same M1
    /// evidence both → 409 with identical zero-wire footprints. Contrast
    /// (non-empty `["agent-0"]` declaration: own-turn gate) unchanged: 409,
    /// zero wire action. Tip: steer zero-wire but RAP remints → disagree.
    #[tokio::test]
    async fn http_steer_and_rap_open_turn_same_zero_wire_footprint() {
        for paths_a in [vec![], vec!["agent-0".to_string()]] {
            let (st, sid, a) = rcb_setup(paths_a.clone());
            rcb_open_turn_lost(&st, sid, a).await;
            let fp0 = rcb_footprint(&st, sid, a);
            let u0 = count_user_messages(&st, sid);

            let (cs, bs) = steer(&st, sid, "contrast").await;
            assert_eq!(cs, StatusCode::CONFLICT, "{paths_a:?} steer: {bs}");
            let fp_steer = rcb_footprint(&st, sid, a);
            assert_eq!(fp_steer, fp0, "{paths_a:?}: steer zero wire action");
            assert_eq!(count_user_messages(&st, sid), u0);

            let (cr, br) = rcb_tool_result(&st, sid, "tc1").await;
            assert_eq!(cr, StatusCode::CONFLICT, "{paths_a:?} rap: {br}");
            let fp_rap = rcb_footprint(&st, sid, a);
            assert_eq!(fp_rap, fp_steer, "{paths_a:?}: RAP footprint == steer");
            assert_ne!(rcb_old_exit(&st, sid, a), Some(9), "{paths_a:?}");
            assert_eq!(count_tool_results(&st, sid, "tc1"), 1);
            assert_eq!(count_trigger_wakes(&st, sid), 0);
        }
    }

    // ---- Waker-admit-before-wire (M1–M6) ----

    /// `mallory` joins only another Session (user names are global).
    fn wab_join_mallory_elsewhere(st: &AppState) {
        let other = st.store().create_session().id();
        apply_join(st, other, Some("mallory".into()), None, None).unwrap();
    }

    async fn wab_steer_as(
        st: &AppState,
        sid: SessionId,
        user: &str,
        msg: &str,
    ) -> (StatusCode, String) {
        let id = sid.0.to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(format!(
                    r#"{{"user":"{user}","message":"{msg}"}}"#
                )))
                .unwrap(),
        )
        .await
    }

    fn wab_log_len(st: &AppState, sid: SessionId) -> usize {
        st.store().session(sid).unwrap().events().unwrap().len()
    }

    /// Per-Agent zero-wire footprint: (Binding, old stdin, new stdin,
    /// evidence, FailClosed texts).
    #[allow(clippy::type_complexity)]
    fn wab_agent_footprint(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
    ) -> (
        Option<hearth::BindingId>,
        String,
        String,
        hearth_agent_wire::OldWire,
        Vec<String>,
    ) {
        (
            st.runtime.binding_for(sid, agent).ok(),
            rrb_runner_log(st, sid, agent, true),
            rrb_runner_log(st, sid, agent, false),
            host_old_wire(st, sid, agent),
            rrb_fail_closed_texts(st, sid, agent),
        )
    }

    /// Assert a refused steer left zero wire / Runtime action on both Agents.
    #[allow(clippy::type_complexity)]
    fn wab_assert_zero_wire(
        st: &AppState,
        sid: SessionId,
        agents: [hearth::AgentId; 2],
        mints0: usize,
        len0: usize,
        fps0: &[(
            Option<hearth::BindingId>,
            String,
            String,
            hearth_agent_wire::OldWire,
            Vec<String>,
        )],
    ) {
        assert_eq!(count_mints(st, sid), mints0, "0 mints on a refused steer");
        assert_eq!(wab_log_len(st, sid), len0, "log length unchanged");
        assert_eq!(count_user_messages(st, sid), 0, "0 UserMessage");
        for (ag, fp0) in agents.iter().zip(fps0) {
            let fp = wab_agent_footprint(st, sid, *ag);
            assert!(fp.0.is_some(), "Agent {ag:?} still bound (no unbind)");
            assert_eq!(
                fp.3,
                hearth_agent_wire::OldWire::Exited(1),
                "Agent {ag:?} evidence unchanged"
            );
            assert!(fp.4.is_empty(), "Agent {ag:?} 0 FailClosed: {:?}", fp.4);
            assert!(fp.2.is_empty(), "Agent {ag:?} new wire untouched: {}", fp.2);
            assert_eq!(&fp, fp0, "Agent {ag:?} zero wire action");
            assert_ne!(
                rcb_old_exit(st, sid, *ag),
                Some(9),
                "Agent {ag:?} old child not killed"
            );
            assert!(
                st.store()
                    .session(sid)
                    .unwrap()
                    .current_turn_of(*ag)
                    .unwrap()
                    .is_none(),
                "Agent {ag:?} no TurnStart"
            );
        }
    }

    /// M1 (Waker-admit-before-wire): `mallory` (member of another Session
    /// only) steers X over HTTP; both kept Agents have lost resume-capable
    /// Host wires (`Exited(1)`). → 403 and **zero wire action**: 0 mints,
    /// old children not killed, evidence still `Exited(1)`, Bindings
    /// unchanged, 0 FailClosed, log length unchanged. Tip: membership is
    /// checked only in `record_wake` → both CLIs killed + reminted (2 → 4
    /// mints), evidence LiveIdle, then 403.
    #[tokio::test]
    async fn http_non_member_steer_lost_wires_403_zero_wire_action() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        wab_join_mallory_elsewhere(&st);
        let m0 = count_mints(&st, sid);
        let len0 = wab_log_len(&st, sid);
        let fps0 = [
            wab_agent_footprint(&st, sid, a),
            wab_agent_footprint(&st, sid, b),
        ];

        let (c, body) = wab_steer_as(&st, sid, "mallory", "x").await;
        assert_eq!(c, StatusCode::FORBIDDEN, "{body}");
        wab_assert_zero_wire(&st, sid, [a, b], m0, len0, &fps0);
    }

    /// M2 (Waker-admit-before-wire): same as M1 but the Bindings advertise no
    /// resume (`LiveCaps::default()`). → **403** (not 500), both Agents still
    /// bound, 0 FailClosed, no unbind, zero wire action. Tip: caps-refuse
    /// FailClosed + unbind on both Agents, every peer skipped → 500.
    #[tokio::test]
    async fn http_non_member_steer_no_resume_caps_403_no_unbind() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires_with_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
            hearth_agent_wire::LiveCaps::default(),
        );
        wab_join_mallory_elsewhere(&st);
        let m0 = count_mints(&st, sid);
        let len0 = wab_log_len(&st, sid);
        let fps0 = [
            wab_agent_footprint(&st, sid, a),
            wab_agent_footprint(&st, sid, b),
        ];

        let (c, body) = wab_steer_as(&st, sid, "mallory", "x").await;
        assert_eq!(c, StatusCode::FORBIDDEN, "403 not 500: {body}");
        assert_eq!(count_fail_closed(&st, sid, a), 0);
        assert_eq!(count_fail_closed(&st, sid, b), 0);
        wab_assert_zero_wire(&st, sid, [a, b], m0, len0, &fps0);
    }

    /// M3 twin (Waker-admit-before-wire): unwired Session (kernel / Goose
    /// path), never-bound kept Agent. Non-member steer → 403, nothing
    /// appended (0 BindingAttached). Tip: `claim` appends a BindingAttached
    /// before `record_wake` refuses NotMember.
    #[tokio::test]
    async fn http_non_member_steer_unwired_session_no_binding() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        apply_join(&st, sid, Some("cheng".into()), None, None).unwrap();
        apply_join(&st, sid, None, Some("scribe".into()), None).unwrap();
        wab_join_mallory_elsewhere(&st);
        let agent = st.runtime.kept_agents(sid)[0];
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::NeverBound
        );
        let len0 = wab_log_len(&st, sid);

        let (c, body) = wab_steer_as(&st, sid, "mallory", "x").await;
        assert_eq!(c, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(count_mints(&st, sid), 0, "0 BindingAttached");
        assert_eq!(wab_log_len(&st, sid), len0, "nothing appended");
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::NeverBound
        );

        // A member steer still claims + wakes on the kernel path.
        let (c, body) = steer(&st, sid, "hi").await;
        assert_eq!(c, StatusCode::OK, "{body}");
        assert_eq!(count_mints(&st, sid), 1);
        assert_eq!(
            st.runtime.classify(sid, agent).unwrap(),
            hearth::Liveness::LiveInTurn
        );
    }

    /// M5 (Waker-admit-before-wire): WS twin of M1 —
    /// `{"type":"message","user":"mallory"}` → `{"error":403}` frame, 0
    /// mints, evidence unchanged, zero wire action. Tip: 403 frame after
    /// 2 → 4 mints.
    #[tokio::test]
    async fn ws_non_member_steer_403_zero_wire_action() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        wab_join_mallory_elsewhere(&st);
        let m0 = count_mints(&st, sid);
        let len0 = wab_log_len(&st, sid);
        let fps0 = [
            wab_agent_footprint(&st, sid, a),
            wab_agent_footprint(&st, sid, b),
        ];

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, router(st.clone())).into_future());
        let url = format!("ws://{addr}/sessions/{id}/stream");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws.send(WsMsg::Text(
            r#"{"type":"message","user":"mallory","message":"m5-ws"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut ws, r#"{"error":403}"#).await;

        wab_assert_zero_wire(&st, sid, [a, b], m0, len0, &fps0);
        let _ = ws.close(None).await;
    }

    /// M6 (Waker-admit-before-wire) compose / contrast: after the M1
    /// refusal (and an unknown-name 404, still zero wire action), a RAP
    /// tool-result (Trigger waker) on lost A → 200 Applied, A reminted once
    /// (+1 mint, `session/resume`): Trigger is not subject to the user
    /// admit. Then a **member** steer → 200: lost B reminted exactly once,
    /// A (in its Trigger turn) skipped. Net: each lost Agent reminted
    /// **exactly once**, both LiveInTurn. (Ordered so no Agent remints twice:
    /// remint-after-remint stays a parked Soft.) Tip: the refused steer
    /// already reminted both → RAP / steer reuse live, and the remint lands
    /// on the refused request.
    #[tokio::test]
    async fn http_non_member_then_member_steer_remints_once() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        wab_join_mallory_elsewhere(&st);
        let m0 = count_mints(&st, sid);

        let (c, body) = wab_steer_as(&st, sid, "mallory", "x").await;
        assert_eq!(c, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(count_mints(&st, sid), m0);

        // Unknown name: 404, zero wire action.
        let len_g = wab_log_len(&st, sid);
        let (c, body) = wab_steer_as(&st, sid, "ghost-never-joined", "x").await;
        assert_eq!(c, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(wab_log_len(&st, sid), len_g);
        assert_eq!(count_mints(&st, sid), m0);

        // RAP Trigger on lost A: Applied, +1 mint (no user admit).
        rcb_tool_call(&st, sid, "tc-wab").await;
        let (c, body) = rcb_tool_result(&st, sid, "tc-wab").await;
        assert_eq!(c, StatusCode::OK, "{body}");
        assert!(body.contains(r#""applied":true"#), "{body}");
        assert_eq!(count_trigger_wakes(&st, sid), 1);
        assert_eq!(count_mints(&st, sid), m0 + 1, "RAP remints A once");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        // Member steer: lost B reminted exactly once; A (in turn) skipped.
        let (c, body) = steer(&st, sid, "member").await;
        assert_eq!(c, StatusCode::OK, "{body}");
        assert_eq!(count_mints(&st, sid), m0 + 2, "each Agent reminted once");
        assert_eq!(count_user_messages(&st, sid), 1);
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn,
                "{ag:?}"
            );
            let raw = rrb_runner_log(&st, sid, ag, true);
            assert_eq!(raw.matches("session/resume").count(), 1, "{ag:?}: {raw}");
            assert!(
                !raw.contains("session/load") && !raw.contains("session/new"),
                "{ag:?} no load/new: {raw}"
            );
            assert!(rrb_fail_closed_texts(&st, sid, ag).is_empty());
            assert_eq!(
                host_old_wire(&st, sid, ag),
                hearth_agent_wire::OldWire::LiveIdle
            );
        }
    }

    // ---- Stream-tails-log (M1–M8) ----
    // Each floor drains the WS *before* the action under test, then asserts
    // the live frames are exactly `log[len0..]` (order, no dup, no miss).

    type StlWs = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn stl_ws(st: &AppState, sid: SessionId) -> StlWs {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, router(st.clone())).into_future());
        let url = format!("ws://{addr}/sessions/{}/stream", sid.0);
        let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws
    }

    /// Every WS text frame body until `quiet_ms` of silence.
    async fn stl_drain(ws: &mut StlWs, quiet_ms: u64) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            let next =
                tokio::time::timeout(std::time::Duration::from_millis(quiet_ms), ws.next()).await;
            match next {
                Ok(Some(Ok(WsMsg::Text(t)))) => {
                    let v: serde_json::Value = serde_json::from_str(t.as_str()).unwrap();
                    out.push(v["body"].as_str().unwrap_or("").to_string());
                }
                Ok(Some(Ok(_))) => continue,
                _ => return out,
            }
        }
    }

    fn stl_log(st: &AppState, sid: SessionId) -> Vec<String> {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .map(|e| event_out(e).body)
            .collect()
    }

    /// Live frames == `log[len0..]` exactly, and the log grew.
    fn stl_assert_exact(tag: &str, st: &AppState, sid: SessionId, len0: usize, live: &[String]) {
        let log = stl_log(st, sid);
        assert!(log.len() > len0, "{tag}: expected appends after {len0}");
        assert_eq!(live, &log[len0..], "{tag}: live != log tail");
    }

    /// M1: RAP tool-result while the Agent is mid-turn → 409; the appended
    /// ToolResult is streamed live exactly once (re-POST adds nothing).
    #[tokio::test]
    async fn ws_rap_409_tool_result_streamed() {
        let (st, sid, a) = rcb_setup(vec!["agent-0".into()]);
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        rcb_tool_call(&st, sid, "tc1").await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::CONFLICT, "{b}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let live = stl_drain(&mut ws, 500).await;
        stl_assert_exact("M1", &st, sid, len0, &live);
        assert_eq!(live.len(), 1, "{live:?}");
        assert!(live[0].contains("ToolResult"), "{live:?}");
        let len1 = stl_log(&st, sid).len();
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        assert!(b.contains(r#""applied":false"#), "{b}");
        let live2 = stl_drain(&mut ws, 400).await;
        assert_eq!(live2, &stl_log(&st, sid)[len1..], "M1 re-POST");
    }

    /// M2: RAP tool-result → 500 after append (NewWire exhaust): every
    /// appended event is live.
    #[tokio::test]
    async fn ws_rap_500_events_streamed() {
        let (st, sid, a) = rcb_setup(vec!["agent-0".into()]);
        rcb_tool_call(&st, sid, "tc1").await;
        st.set_host_old_wire(sid, a, hearth_agent_wire::OldWire::Exited(1))
            .unwrap();
        rrb_set_new_wire(&st, sid, a, rrb_flaky_new_wire(99));
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::INTERNAL_SERVER_ERROR, "{b}");
        let live = stl_drain(&mut ws, 500).await;
        stl_assert_exact("M2", &st, sid, len0, &live);
        assert!(live.iter().any(|b| b.contains("ToolResult")), "{live:?}");
    }

    /// M3: steer that remints both lost Agents → 200; Binding events,
    /// UserMessage and TurnStarts are all live, exact.
    #[tokio::test]
    async fn ws_steer_remint_binding_events_streamed() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let _ = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        let live = stl_drain(&mut ws, 500).await;
        stl_assert_exact("M3", &st, sid, len0, &live);
        assert!(
            live.iter().any(|b| b.contains("BindingAttached")),
            "{live:?}"
        );
        assert!(
            live.iter().any(|b| b.contains("BindingReleased")),
            "{live:?}"
        );
    }

    /// M3 twin: RAP Applied with a remint (yielded A, lost wire).
    #[tokio::test]
    async fn ws_rap_applied_remint_events_streamed() {
        let (st, sid, a) = rcb_setup(vec!["agent-0".into()]);
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        rcb_tool_call(&st, sid, "tc1").await;
        rcb_end_turn(&st, sid).await;
        st.set_host_old_wire(sid, a, hearth_agent_wire::OldWire::Exited(1))
            .unwrap();
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        assert!(b.contains(r#""applied":true"#), "{b}");
        let live = stl_drain(&mut ws, 500).await;
        stl_assert_exact("M3 twin", &st, sid, len0, &live);
        assert!(
            live.iter().any(|b| b.contains("BindingAttached")),
            "{live:?}"
        );
    }

    /// M4: member steer, two lost no-resume Agents → 500 after the
    /// FailClosed unbinds; the `BindingReleased` events are still live.
    #[tokio::test]
    async fn ws_steer_500_failclosed_streamed() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let _ = install_two_parallel_safe_host_wires_with_caps(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
            hearth_agent_wire::LiveCaps::default(),
        );
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::INTERNAL_SERVER_ERROR, "{b}");
        let live = stl_drain(&mut ws, 500).await;
        stl_assert_exact("M4", &st, sid, len0, &live);
        assert_eq!(
            live.iter()
                .filter(|b| b.contains("BindingReleased"))
                .count(),
            2,
            "{live:?}"
        );
    }

    /// M5: one event on Session X, then a 300-join flood on Session Y: X's
    /// client still gets X's own event (cross-Session isolation).
    #[tokio::test]
    async fn ws_other_session_flood_keeps_own_event() {
        let st = AppState::new(vec![]);
        let sx = st.store().create_session().id();
        let sy = st.store().create_session().id();
        let mut ws = stl_ws(&st, sx).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sx).len();
        apply_join(&st, sx, Some("x0".into()), None, None).unwrap();
        for i in 0..300 {
            apply_join(&st, sy, Some(format!("y{i}")), None, None).unwrap();
        }
        let live = stl_drain(&mut ws, 800).await;
        stl_assert_exact("M5", &st, sx, len0, &live);
        assert_eq!(live.len(), 1, "{live:?}");
    }

    /// M6: 300 own-Session events in one burst with no yield (over the
    /// 256-slot doorbell channel): all 300 are live, exact.
    #[tokio::test]
    async fn ws_burst_over_capacity_all_streamed() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        for i in 0..300 {
            apply_join(&st, sid, Some(format!("u{i}")), None, None).unwrap();
        }
        let live = stl_drain(&mut ws, 800).await;
        stl_assert_exact("M6", &st, sid, len0, &live);
        assert_eq!(live.len(), 300);
    }

    /// M7: two clients, the second connects mid-Session; each client's
    /// replay + live == the full log, exact, no duplicates.
    #[tokio::test]
    async fn ws_two_clients_replay_plus_live_exact() {
        let (st, sid, _a) = rcb_setup(vec!["agent-0".into()]);
        let mut ws1 = stl_ws(&st, sid).await;
        let r1 = stl_drain(&mut ws1, 300).await;
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        let mut ws2 = stl_ws(&st, sid).await;
        let r2 = stl_drain(&mut ws2, 300).await;
        rcb_tool_call(&st, sid, "tc1").await;
        let (c, b) = rcb_tool_result(&st, sid, "tc1").await;
        assert_eq!(c, StatusCode::CONFLICT, "{b}");
        let l1 = stl_drain(&mut ws1, 500).await;
        let l2 = stl_drain(&mut ws2, 500).await;
        let all1: Vec<String> = r1.into_iter().chain(l1).collect();
        let all2: Vec<String> = r2.into_iter().chain(l2).collect();
        stl_assert_exact("M7 c1", &st, sid, 0, &all1);
        stl_assert_exact("M7 c2", &st, sid, 0, &all2);
    }

    /// M8 (contrast, green on tip): tool-call alone, then end-turn alone,
    /// each streamed exactly, drained before any later ring.
    #[tokio::test]
    async fn ws_tool_call_and_end_turn_streamed_alone() {
        let (st, sid, _a) = rcb_setup(vec!["agent-0".into()]);
        let (c, b) = steer(&st, sid, "go").await;
        assert_eq!(c, StatusCode::OK, "{b}");
        let mut ws = stl_ws(&st, sid).await;
        let _ = stl_drain(&mut ws, 300).await;
        let len0 = stl_log(&st, sid).len();
        rcb_tool_call(&st, sid, "tc1").await;
        let l1 = stl_drain(&mut ws, 400).await;
        stl_assert_exact("M8 tool-call", &st, sid, len0, &l1);
        let len1 = stl_log(&st, sid).len();
        rcb_end_turn(&st, sid).await;
        let l2 = stl_drain(&mut ws, 400).await;
        stl_assert_exact("M8 end-turn", &st, sid, len1, &l2);
    }

    // ---- Steer-delivers-prompt (A1 deliver-only, M1–M6) ----
    //
    // A Host-wired steer / RAP Trigger that opens a kernel turn must also
    // write exactly one `session/prompt` on that Agent's settled `old`
    // runner. No drain: turns still close only via POST end-turn.

    /// Parse a Fake runner's stdin JSONL into values (`None` → empty).
    fn sdp_lines(log: Option<&[u8]>) -> Vec<serde_json::Value> {
        std::str::from_utf8(log.unwrap_or(&[]))
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// `(old lines, new lines, old byte len, new byte len)` for one Agent.
    fn sdp_wire(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
    ) -> (Vec<serde_json::Value>, Vec<serde_json::Value>, usize, usize) {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        let w = wires.get(agent).unwrap();
        (
            sdp_lines(w.old.stdin_log()),
            sdp_lines(w.new.stdin_log()),
            w.old.stdin_log().map_or(0, <[u8]>::len),
            w.new.stdin_log().map_or(0, <[u8]>::len),
        )
    }

    fn sdp_methods(lines: &[serde_json::Value]) -> Vec<String> {
        lines
            .iter()
            .map(|v| v["method"].as_str().unwrap_or("").to_string())
            .collect()
    }

    fn sdp_prompts(lines: &[serde_json::Value]) -> Vec<serde_json::Value> {
        lines
            .iter()
            .filter(|v| v["method"] == "session/prompt")
            .cloned()
            .collect()
    }

    fn sdp_prompt_text(p: &serde_json::Value) -> String {
        let blocks = p["params"]["prompt"].as_array().expect("prompt blocks");
        assert_eq!(blocks.len(), 1, "one text block: {p}");
        assert_eq!(blocks[0]["type"], "text", "{p}");
        blocks[0]["text"].as_str().expect("text").to_string()
    }

    fn sdp_agent_session_id(st: &AppState, sid: SessionId, agent: hearth::AgentId) -> String {
        let sw = session_wires_arc(st, sid);
        let wires = sw.lock().unwrap();
        wires
            .get(agent)
            .unwrap()
            .remint
            .agent_session_id()
            .to_string()
    }

    fn sdp_prompt_count(st: &AppState, sid: SessionId, agent: hearth::AgentId) -> (usize, usize) {
        let (old, new, _, _) = sdp_wire(st, sid, agent);
        (sdp_prompts(&old).len(), sdp_prompts(&new).len())
    }

    fn sdp_count(st: &AppState, sid: SessionId, f: impl Fn(&EventBody) -> bool) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| f(&e.body))
            .count()
    }

    async fn sdp_steer(
        st: &AppState,
        sid: SessionId,
        user: &str,
        text: &str,
    ) -> (StatusCode, String) {
        let id = sid.0.to_string();
        let body = serde_json::json!({ "user": user, "message": text }).to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }

    async fn sdp_tool_result(
        st: &AppState,
        sid: SessionId,
        agent_name: &str,
        tool_call_id: &str,
        output: &str,
    ) -> (StatusCode, String) {
        let id = sid.0.to_string();
        let body =
            serde_json::json!({ "tool_call_id": tool_call_id, "output": output }).to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/{agent_name}/tool-result"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }

    async fn sdp_end_turn(st: &AppState, sid: SessionId, agent_name: &str) -> (StatusCode, String) {
        let id = sid.0.to_string();
        call(
            router(st.clone()),
            Request::post(format!("/sessions/{id}/agents/{agent_name}/end-turn"))
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
    }

    /// The documented ToolResult-shaped prompt text (DESIGN
    /// Steer-delivers-prompt): one JSON object
    /// `{"type":"tool_result","tool_call_id":..,"output":..}`.
    fn sdp_assert_tool_result_prompt(p: &serde_json::Value, tool_call_id: &str, output: &str) {
        let text = sdp_prompt_text(p);
        let v: serde_json::Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("ToolResult shape JSON: {e}: {text}"));
        assert_eq!(
            v,
            serde_json::json!({
                "type": "tool_result",
                "tool_call_id": tool_call_id,
                "output": output,
            }),
            "ToolResult-shaped prompt"
        );
    }

    /// M1: ReuseLive steer on two ParallelSafe Host wires → 200, both
    /// LiveInTurn; each Agent's `old` stdin has exactly one `session/prompt`
    /// with text == T, after `initialize`; `new` stays empty; A's prompt is
    /// addressed to A's agent session, never B's.
    /// Kills: no send (tip); first-woke-only; double send; send on `new`.
    #[tokio::test]
    async fn http_steer_reuse_live_delivers_prompt_once_each() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let t = "sdp-m1 deliver me";
        let (code, body) = sdp_steer(&st, sid, "cheng", t).await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
        }
        let (sa, sb) = (
            sdp_agent_session_id(&st, sid, a),
            sdp_agent_session_id(&st, sid, b),
        );
        for (ag, own, peer) in [(a, &sa, &sb), (b, &sb, &sa)] {
            let (old, new, _, new_len) = sdp_wire(&st, sid, ag);
            assert_eq!(
                sdp_methods(&old),
                vec!["initialize".to_string(), "session/prompt".to_string()],
                "Agent {ag:?}: initialize then exactly one prompt: {old:?}"
            );
            let prompts = sdp_prompts(&old);
            assert_eq!(prompts.len(), 1, "Agent {ag:?} exactly one prompt");
            assert_eq!(sdp_prompt_text(&prompts[0]), t, "text == T");
            assert_eq!(prompts[0]["params"]["sessionId"], own.as_str());
            assert!(
                old.iter()
                    .all(|l| l["params"]["sessionId"] != peer.as_str()),
                "Agent {ag:?} never gets the peer's prompt line"
            );
            assert!(new.is_empty() && new_len == 0, "new stdin empty: {new:?}");
        }
    }

    /// M2: Reminted steer (both lost `Exited(1)`) → 200, both LiveInTurn,
    /// +1 mint each. On each settled `old` (= resumed runner) the method
    /// order is exactly `initialize`, `session/resume`, `session/prompt`.
    /// The killed child (now `new`) has zero `session/prompt`.
    /// Kills: send pre-settle / before resume; send on both runners; extra remint.
    #[tokio::test]
    async fn http_steer_reminted_prompt_after_resume_once() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        let m0 = count_mints(&st, sid);
        let t = "sdp-m2 after resume";
        let (code, body) = sdp_steer(&st, sid, "cheng", t).await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(count_mints(&st, sid), m0 + 2, "one mint each");
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
            assert_eq!(
                host_old_wire(&st, sid, ag),
                hearth_agent_wire::OldWire::LiveIdle
            );
            let (old, new, _, _) = sdp_wire(&st, sid, ag);
            assert_eq!(
                sdp_methods(&old),
                vec![
                    "initialize".to_string(),
                    "session/resume".to_string(),
                    "session/prompt".to_string()
                ],
                "Agent {ag:?}: prompt strictly after resume on the resumed runner"
            );
            assert_eq!(sdp_prompt_text(&sdp_prompts(&old)[0]), t);
            assert!(
                sdp_prompts(&new).is_empty(),
                "Agent {ag:?}: killed child got no prompt: {new:?}"
            );
        }
    }

    /// M3 (ReuseLive): yielded Host-wired A with an unmatched Host ToolCall →
    /// POST tool-result → Applied, A LiveInTurn; A's `old` has exactly one
    /// ToolResult-shaped `session/prompt` (tool_call_id + output). B none.
    /// A re-POST (NotApplied) sends no second prompt.
    /// Kills: RAP path not wired; prompt on NotApplied; plain user-text shape.
    #[tokio::test]
    async fn http_rap_trigger_delivers_tool_result_prompt_once() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-sdp", "sdp_tool");
        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-sdp", "result-payload").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let (old, new, len_a, _) = sdp_wire(&st, sid, a);
        assert_eq!(
            sdp_methods(&old),
            vec!["initialize".to_string(), "session/prompt".to_string()],
            "{old:?}"
        );
        let prompts = sdp_prompts(&old);
        sdp_assert_tool_result_prompt(&prompts[0], "tc-sdp", "result-payload");
        assert_eq!(
            prompts[0]["params"]["sessionId"],
            sdp_agent_session_id(&st, sid, a).as_str()
        );
        assert!(new.is_empty(), "A new untouched");
        assert_eq!(sdp_prompt_count(&st, sid, b), (0, 0), "peer B no prompt");

        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-sdp", "result-payload").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":false"#), "body={body}");
        let (_, _, len_a2, _) = sdp_wire(&st, sid, a);
        assert_eq!(len_a2, len_a, "NotApplied re-POST: zero bytes written");
        assert_eq!(sdp_prompt_count(&st, sid, a), (1, 0));
    }

    /// M3 (Reminted twin): lost A → POST tool-result → Applied, +1 mint; the
    /// resumed `old` reads `initialize`, `session/resume`, then one
    /// ToolResult-shaped `session/prompt`. Killed child zero. Re-POST zero.
    #[tokio::test]
    async fn http_rap_trigger_delivers_tool_result_prompt_once_reminted() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(
            &st,
            sid,
            Some(hearth_agent_wire::OldWire::Exited(1)),
        );
        record_host_tool_call(&st, sid, a, "tc-sdp-r", "sdp_tool");
        let m0 = count_mints(&st, sid);
        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-sdp-r", "r-out").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert_eq!(count_mints(&st, sid), m0 + 1, "A reminted once");
        let (old, new, len_a, _) = sdp_wire(&st, sid, a);
        assert_eq!(
            sdp_methods(&old),
            vec![
                "initialize".to_string(),
                "session/resume".to_string(),
                "session/prompt".to_string()
            ],
            "{old:?}"
        );
        sdp_assert_tool_result_prompt(&sdp_prompts(&old)[0], "tc-sdp-r", "r-out");
        assert!(sdp_prompts(&new).is_empty(), "killed child no prompt");
        assert_eq!(sdp_prompt_count(&st, sid, b), (0, 0), "peer B no prompt");

        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-sdp-r", "r-out").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":false"#), "body={body}");
        assert_eq!(sdp_wire(&st, sid, a).2, len_a, "re-POST zero bytes");
    }

    /// M4 (a)+(b): zero prompt on every non-woke Agent.
    /// (a) B `Unknown` → Waiting: A gets one, B zero on old and new. Then
    /// with B LiveIdle, a second steer skips A at classify (LiveInTurn): A
    /// still one, B one.
    /// (b) FailClosed-skip peer (no-resume caps, lost): zero on its old and
    /// new; the resume-capable peer gets exactly one, after resume.
    /// Kills: send to every wired Agent; send on Waiting / skipped.
    #[tokio::test]
    async fn http_steer_no_prompt_on_skipped_waiting_failclosed() {
        use hearth_agent_wire::{LiveCaps as WireCaps, OldWire};

        // (a) Unknown → Waiting, then classify-skip LiveInTurn.
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        st.set_host_old_wire(sid, b, OldWire::Unknown).unwrap();
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m4a-1").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(sdp_prompt_count(&st, sid, a), (1, 0), "A woke: one");
        assert_eq!(sdp_prompt_count(&st, sid, b), (0, 0), "Unknown B: zero");
        let (_, _, ob, nb) = sdp_wire(&st, sid, b);
        assert_eq!(nb, 0, "B new untouched");

        st.set_host_old_wire(sid, b, OldWire::LiveIdle).unwrap();
        let (_, _, oa, _) = sdp_wire(&st, sid, a);
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m4a-2").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(sdp_wire(&st, sid, a).2, oa, "LiveInTurn A skipped: 0 bytes");
        assert_eq!(sdp_prompt_count(&st, sid, a), (1, 0));
        let (old_b, _, ob2, _) = sdp_wire(&st, sid, b);
        assert!(ob2 > ob);
        let pb = sdp_prompts(&old_b);
        assert_eq!(pb.len(), 1, "B woke on steer #2: one");
        assert_eq!(sdp_prompt_text(&pb[0]), "sdp-m4a-2");

        // (b) FailClosed-skip peer.
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let no_resume = WireCaps {
            resume: false,
            load_session: false,
            cancel: true,
            close: true,
        };
        let (a, b, _) = install_two_parallel_safe_host_wires_with_per_agent_caps(
            &st,
            sid,
            Some(OldWire::Exited(1)),
            no_resume,
            resume_capable_caps(),
        );
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m4b").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(count_fail_closed(&st, sid, a) >= 1, "A FailClosed-skipped");
        assert_eq!(sdp_prompt_count(&st, sid, a), (0, 0), "FailClosed A: zero");
        let (old_b, new_b, _, _) = sdp_wire(&st, sid, b);
        assert_eq!(
            sdp_methods(&old_b),
            vec![
                "initialize".to_string(),
                "session/resume".to_string(),
                "session/prompt".to_string()
            ]
        );
        assert!(sdp_prompts(&new_b).is_empty());
    }

    /// M4 (c) hosts ⊋ woke: two non-ParallelSafe idle Agents both pass the
    /// wire-loop gate and `host_wake`, but the kernel re-check Waitings the
    /// second. Only the kernel-woke Agent gets a prompt.
    /// Kills: iterate `hosts` instead of `woke`.
    #[tokio::test]
    async fn http_steer_prompt_only_kernel_woke_not_hosts() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let caps = resume_capable_caps();
        let (a, b, _) =
            install_two_host_wires_with_declare_caps(&st, sid, None, None, None, caps, caps);
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m4c").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let in_turn: Vec<_> = [a, b]
            .into_iter()
            .filter(|ag| session.current_turn_of(*ag).unwrap().is_some())
            .collect();
        assert_eq!(
            in_turn.len(),
            1,
            "kernel woke exactly one non-ParallelSafe Agent"
        );
        for ag in [a, b] {
            let want = usize::from(in_turn.contains(&ag));
            assert_eq!(
                sdp_prompt_count(&st, sid, ag),
                (want, 0),
                "Agent {ag:?}: prompt iff kernel-woke"
            );
        }
    }

    /// M4 (d): RAP `AppliedWakeWaiting` → zero prompt. Both Agents in their
    /// steer turn; a Host ToolCall on A then POST tool-result → 409 (ToolResult
    /// appended, wake Waiting). A's stdin unchanged; re-POST (NotApplied) too.
    /// Kills: RAP send on AppliedWakeWaiting.
    #[tokio::test]
    async fn http_rap_applied_wake_waiting_no_prompt() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m4d").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let (len_a, len_b) = (sdp_wire(&st, sid, a).2, sdp_wire(&st, sid, b).2);
        record_host_tool_call(&st, sid, a, "tc-wait", "x");
        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-wait", "out").await;
        assert_eq!(
            code,
            StatusCode::CONFLICT,
            "AppliedWakeWaiting; body={body}"
        );
        assert_eq!(
            count_tool_results(&st, sid, "tc-wait"),
            1,
            "Result appended"
        );
        assert_eq!(sdp_wire(&st, sid, a).2, len_a, "A: zero bytes on Waiting");
        assert_eq!(sdp_wire(&st, sid, b).2, len_b, "B untouched");
        let (code, body) = sdp_tool_result(&st, sid, "alpha", "tc-wait", "out").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":false"#), "body={body}");
        assert_eq!(
            sdp_wire(&st, sid, a).2,
            len_a,
            "A: zero bytes on NotApplied"
        );
        assert_eq!(
            sdp_prompt_count(&st, sid, a),
            (1, 0),
            "only the steer prompt"
        );
    }

    /// M5: refused steer = zero prompt anywhere. Non-member (member of
    /// another Session) → 403; all-Waiting gate (both Unknown) → 409; both
    /// already LiveInTurn → 409. Every wire's old and new byte length is
    /// unchanged by each refused request.
    /// Kills: send before admit/gate; send on Err / Waiting.
    #[tokio::test]
    async fn http_refused_steer_403_409_zero_prompt() {
        use hearth_agent_wire::OldWire;

        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        wab_join_mallory_elsewhere(&st);
        let lens = |st: &AppState| {
            [a, b].map(|ag| {
                let (_, _, o, n) = sdp_wire(st, sid, ag);
                (o, n)
            })
        };
        let l0 = lens(&st);
        let (code, body) = sdp_steer(&st, sid, "mallory", "sdp-m5-403").await;
        assert_eq!(code, StatusCode::FORBIDDEN, "body={body}");
        assert_eq!(lens(&st), l0, "403: zero bytes on any wire");
        for ag in [a, b] {
            assert_eq!(sdp_prompt_count(&st, sid, ag), (0, 0));
        }

        for ag in [a, b] {
            st.set_host_old_wire(sid, ag, OldWire::Unknown).unwrap();
        }
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m5-409u").await;
        assert_eq!(code, StatusCode::CONFLICT, "body={body}");
        assert_eq!(lens(&st), l0, "409 (all Waiting): zero bytes");
        for ag in [a, b] {
            assert_eq!(sdp_prompt_count(&st, sid, ag), (0, 0));
            st.set_host_old_wire(sid, ag, OldWire::LiveIdle).unwrap();
        }

        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m5-ok").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let l1 = lens(&st);
        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m5-409t").await;
        assert_eq!(code, StatusCode::CONFLICT, "both LiveInTurn; body={body}");
        assert_eq!(lens(&st), l1, "409 (TurnOpen): zero bytes");
        let (code, body) = sdp_steer(&st, sid, "mallory", "sdp-m5-403b").await;
        assert_eq!(code, StatusCode::FORBIDDEN, "body={body}");
        assert_eq!(lens(&st), l1, "403 mid-turn: zero bytes");
    }

    /// M5 (WS twin): non-member `{"type":"message","user":"mallory"}` →
    /// `{"error":403}` and zero bytes on every wire.
    #[tokio::test]
    async fn ws_non_member_steer_zero_prompt() {
        let st = AppState::new(vec![]);
        let sid = st.store().create_session().id();
        let id = sid.0.to_string();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        wab_join_mallory_elsewhere(&st);
        let l0 = [a, b].map(|ag| sdp_wire(&st, sid, ag).2 + sdp_wire(&st, sid, ag).3);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, router(st.clone())).into_future());
        let url = format!("ws://{addr}/sessions/{id}/stream");
        let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws.send(WsMsg::Text(
            r#"{"type":"message","user":"mallory","message":"sdp-ws"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut ws, r#"{"error":403}"#).await;
        let l1 = [a, b].map(|ag| sdp_wire(&st, sid, ag).2 + sdp_wire(&st, sid, ag).3);
        assert_eq!(l1, l0, "WS 403: zero bytes on any wire");
        for ag in [a, b] {
            assert_eq!(sdp_prompt_count(&st, sid, ag), (0, 0));
        }
        let _ = ws.close(None).await;
    }

    /// M6: identity + no auto-close. Each prompt's `params.sessionId` is THAT
    /// wire's agent session id (A ≠ B, never the hearth SessionId); text is
    /// byte-exact; the JSON-RPC id is present and unique on the wire. After
    /// delivery both stay LiveInTurn with zero AgentMessage / TurnEnd. POST
    /// end-turn A → A yields, B still LiveInTurn; a second steer → A exactly
    /// one more prompt (total 2), B none.
    /// Kills: hearth/peer session id; mangled text; rpc id not bumped;
    /// auto-TurnEnd / drain (A2).
    #[tokio::test]
    async fn http_steer_prompt_per_agent_session_id_and_end_turn_still_required() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let (sa, sb) = (
            sdp_agent_session_id(&st, sid, a),
            sdp_agent_session_id(&st, sid, b),
        );
        assert_ne!(sa, sb, "helper mints distinct agent session ids");
        let hearth_sid = sid.0.to_string();
        let am0 = sdp_count(&st, sid, |b| matches!(b, EventBody::AgentMessage { .. }));
        let te0 = sdp_count(&st, sid, |b| matches!(b, EventBody::TurnEnd { .. }));

        let t1 = "sdp-m6 \"quoted\" ünïcode\ttab\nnewline {json}";
        let (code, body) = sdp_steer(&st, sid, "cheng", t1).await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        for (ag, own) in [(a, &sa), (b, &sb)] {
            let (old, _, _, _) = sdp_wire(&st, sid, ag);
            let p = sdp_prompts(&old);
            assert_eq!(p.len(), 1);
            assert_eq!(
                p[0]["params"]["sessionId"],
                own.as_str(),
                "own agent session"
            );
            assert_ne!(p[0]["params"]["sessionId"], hearth_sid.as_str());
            assert_eq!(sdp_prompt_text(&p[0]), t1, "byte-exact text");
            assert!(
                p[0]["id"].is_u64(),
                "prompt is a JSON-RPC request: {}",
                p[0]
            );
            let ids: Vec<_> = old.iter().filter_map(|l| l.get("id")).collect();
            let uniq: std::collections::HashSet<String> =
                ids.iter().map(|v| v.to_string()).collect();
            assert_eq!(uniq.len(), ids.len(), "rpc ids unique on the wire: {ids:?}");
        }
        assert_eq!(
            sdp_count(&st, sid, |b| matches!(b, EventBody::AgentMessage { .. })),
            am0,
            "no drain: zero AgentMessage"
        );
        assert_eq!(
            sdp_count(&st, sid, |b| matches!(b, EventBody::TurnEnd { .. })),
            te0,
            "no auto-TurnEnd"
        );
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn,
                "turn stays open until end-turn"
            );
        }

        let (code, body) = sdp_end_turn(&st, sid, "alpha").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveIdle
        );
        assert_eq!(
            st.runtime.classify(sid, b).unwrap(),
            hearth::Liveness::LiveInTurn
        );

        let (code, body) = sdp_steer(&st, sid, "cheng", "sdp-m6-two").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let pa = sdp_prompts(&old_a);
        assert_eq!(pa.len(), 2, "A: exactly one more prompt");
        assert_eq!(sdp_prompt_text(&pa[1]), "sdp-m6-two");
        assert_ne!(pa[0]["id"], pa[1]["id"], "rpc id bumped");
        assert_eq!(sdp_prompt_count(&st, sid, b), (1, 0), "B (in turn) none");
    }

    // ---- deliver-prompt-bounded-write (M1–M5) ----
    //
    // One deaf Real CLI (answered `initialize`, never reads stdin again) must
    // not wedge its Session: the agent-bound stdin write is bounded, peers
    // still get their prompt, and a failed delivery marks THAT wire lost so
    // the next wake (after end-turn) remints. Real `sh -c` children only:
    // the Fake arm has no pipe and cannot show the wedge.

    /// Floor bound: `STDIN_WRITE_TIMEOUT` + 3 s slack (5 s literal in the
    /// floors-only RED commit, before the const existed).
    const DPBW_BOUND: std::time::Duration =
        hearth_agent_wire::STDIN_WRITE_TIMEOUT.saturating_add(std::time::Duration::from_secs(3));

    /// Real JSONL child that answers `initialize` (id 0), then runs `tail`.
    fn dpbw_real_initialized(tail: &str) -> hearth_agent_wire::AttachRunner {
        use hearth_agent_wire::{transport_for_wire, AttachRunner, LaunchSpec, RpcId, WireKind};
        let script = format!(
            "IFS= read -r l; printf '%s\\n' '{{\"jsonrpc\":\"2.0\",\"id\":0,\"result\":{{\"protocolVersion\":1}}}}'; {tail}"
        );
        let mut r = AttachRunner::spawn(LaunchSpec {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        })
        .expect("spawn Real CLI");
        r.initialize_host(&mut transport_for_wire(WireKind::JsonlRpc), &RpcId::Num(0))
            .expect("initialize Real CLI");
        r
    }

    /// Deaf CLI: initialized, then never reads stdin; lives `secs` (finite so
    /// a RED run on the blocking write still ends).
    fn dpbw_deaf(secs: u32) -> hearth_agent_wire::AttachRunner {
        dpbw_real_initialized(&format!("exec sleep {secs}"))
    }

    fn dpbw_swap_old(
        st: &AppState,
        sid: SessionId,
        agent: hearth::AgentId,
        r: hearth_agent_wire::AttachRunner,
    ) {
        let sw = session_wires_arc(st, sid);
        sw.lock().unwrap().get_mut(agent).unwrap().old = r;
    }

    fn dpbw_lock_free(st: &AppState, sid: SessionId) -> bool {
        session_wires_arc(st, sid).try_lock().is_ok()
    }

    /// HTTP steer on a spawned task, bounded by `DPBW_BOUND`. `None` = wedged.
    async fn dpbw_steer(
        st: &AppState,
        sid: SessionId,
        text: String,
    ) -> (Option<(StatusCode, String)>, std::time::Duration) {
        let st2 = st.clone();
        let t0 = std::time::Instant::now();
        let h = tokio::spawn(async move { sdp_steer(&st2, sid, "cheng", &text).await });
        let r = tokio::time::timeout(DPBW_BOUND, h).await;
        (r.ok().map(|j| j.unwrap()), t0.elapsed())
    }

    async fn dpbw_tool_result(
        st: &AppState,
        sid: SessionId,
        agent_name: &'static str,
        tool_call_id: &'static str,
        output: String,
    ) -> (Option<(StatusCode, String)>, std::time::Duration) {
        let st2 = st.clone();
        let t0 = std::time::Instant::now();
        let h = tokio::spawn(async move {
            sdp_tool_result(&st2, sid, agent_name, tool_call_id, &output).await
        });
        let r = tokio::time::timeout(DPBW_BOUND, h).await;
        (r.ok().map(|j| j.unwrap()), t0.elapsed())
    }

    async fn dpbw_end_turn(
        st: &AppState,
        sid: SessionId,
        agent_name: &'static str,
    ) -> Option<(StatusCode, String)> {
        let st2 = st.clone();
        let h = tokio::spawn(async move { sdp_end_turn(&st2, sid, agent_name).await });
        tokio::time::timeout(DPBW_BOUND, h)
            .await
            .ok()
            .map(|j| j.unwrap())
    }

    /// M1: deaf Real A + Fake live B (ParallelSafe). A ≥256 KiB steer returns
    /// 200 within the bound; both LiveInTurn; B's `old` has exactly one
    /// `session/prompt` with the exact text; Session wire lock free after.
    /// Kills: (a) blocking write_all; (b) stop peers on first send error;
    /// (g) send error → 500.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_steer_deaf_cli_bounded_peer_still_prompted() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        dpbw_swap_old(&st, sid, a, dpbw_deaf(10));
        let big = "x".repeat(256 * 1024);
        let (r, elapsed) = dpbw_steer(&st, sid, big.clone()).await;
        let (code, body) =
            r.unwrap_or_else(|| panic!("steer wedged > {DPBW_BOUND:?} on a deaf CLI"));
        assert_eq!(code, StatusCode::OK, "body len={}", body.len());
        assert!(elapsed <= DPBW_BOUND, "elapsed={elapsed:?}");
        assert!(dpbw_lock_free(&st, sid), "Session wire lock free");
        for ag in [a, b] {
            assert_eq!(
                st.runtime.classify(sid, ag).unwrap(),
                hearth::Liveness::LiveInTurn
            );
        }
        let (old_b, new_b, _, _) = sdp_wire(&st, sid, b);
        let pb = sdp_prompts(&old_b);
        assert_eq!(pb.len(), 1, "peer B gets exactly one prompt");
        assert_eq!(sdp_prompt_text(&pb[0]), big, "B prompt text exact");
        assert!(new_b.is_empty());
    }

    /// M2: RAP with a deaf Real A and an unmatched Host ToolCall; a ≥200 KiB
    /// tool-result output returns 200 applied:true within the bound;
    /// ToolResult + TurnStart on the log; lock free; A's evidence
    /// TransportError. Kills: (a); bounding only the steer path; RAP send
    /// error not marking the wire lost.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_rap_deaf_cli_large_output_bounded() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        record_host_tool_call(&st, sid, a, "tc-dpbw", "read_file");
        dpbw_swap_old(&st, sid, a, dpbw_deaf(10));
        let turns0 = sdp_count(
            &st,
            sid,
            |e| matches!(e, EventBody::TurnStart { agent, .. } if *agent == a),
        );
        let out = "y".repeat(200 * 1024);
        let (r, elapsed) = dpbw_tool_result(&st, sid, "alpha", "tc-dpbw", out).await;
        let (code, body) =
            r.unwrap_or_else(|| panic!("RAP tool-result wedged > {DPBW_BOUND:?} on a deaf CLI"));
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(body.contains(r#""applied":true"#), "body={body}");
        assert!(elapsed <= DPBW_BOUND, "elapsed={elapsed:?}");
        assert!(dpbw_lock_free(&st, sid), "Session wire lock free");
        assert_eq!(
            sdp_count(&st, sid, |e| matches!(
                e,
                EventBody::ToolResult { tool_call_id, .. } if tool_call_id == "tc-dpbw"
            )),
            1,
            "ToolResult on the log"
        );
        assert_eq!(
            sdp_count(
                &st,
                sid,
                |e| matches!(e, EventBody::TurnStart { agent, .. } if *agent == a)
            ),
            turns0 + 1,
            "Trigger TurnStart on the log"
        );
        assert_eq!(
            st.runtime.classify(sid, a).unwrap(),
            hearth::Liveness::LiveInTurn
        );
        let ev = host_old_wire(&st, sid, a);
        assert!(
            matches!(ev, hearth_agent_wire::OldWire::TransportError(_)),
            "RAP send timeout marks A lost, got {ev:?}"
        );
    }

    /// M3: while A's CLI stays deaf (child alive), the same Session is not
    /// wedged: after the big steer, `end-turn` B then a steer that reaches
    /// only B (A LiveInTurn → skipped) both finish within the bound; B has 2
    /// prompts. A cross-Session steer also finishes. Kills: (a); an
    /// HTTP-handler-only timeout (spawn_blocking keeps holding the lock).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_deaf_cli_does_not_block_same_session_followups() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        dpbw_swap_old(&st, sid, a, dpbw_deaf(15));
        // Cross-Session control: a second Session with its own Fake wires.
        let other = st.store().create_session().id();
        let _ = install_two_parallel_safe_host_wires(&st, other, None);

        let big = "x".repeat(256 * 1024);
        let (first, _) = dpbw_steer(&st, sid, big).await;
        let end_b = dpbw_end_turn(&st, sid, "beta").await;
        let (second, el2) = dpbw_steer(&st, sid, "dpbw-m3-two".into()).await;
        let (ctrl, _) = dpbw_steer(&st, other, "dpbw-m3-other".into()).await;

        assert!(
            ctrl.as_ref().is_some_and(|(c, _)| *c == StatusCode::OK),
            "cross-Session steer finishes"
        );
        let (ce, be) = end_b.expect("end-turn B wedged");
        assert_eq!(ce, StatusCode::OK, "end-turn B body={be}");
        let (c2, b2) = second.unwrap_or_else(|| {
            panic!("same-Session follow-up steer wedged > {DPBW_BOUND:?} behind deaf A")
        });
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        assert!(el2 <= DPBW_BOUND, "follow-up elapsed={el2:?}");
        assert!(
            first.as_ref().is_some_and(|(c, _)| *c == StatusCode::OK),
            "first steer wedged on a deaf CLI"
        );
        assert!(
            st.runtime.classify(sid, a).unwrap() == hearth::Liveness::LiveInTurn,
            "A still in its open turn (skipped, never reminted under it)"
        );
        let (old_b, _, _, _) = sdp_wire(&st, sid, b);
        let pb = sdp_prompts(&old_b);
        assert_eq!(pb.len(), 2, "B: big prompt + follow-up");
        assert_eq!(sdp_prompt_text(&pb[1]), "dpbw-m3-two");
        // A's child is still alive (deaf, not dead) through the follow-ups.
        let sw = session_wires_arc(&st, sid);
        let mut wires = sw.lock().unwrap();
        assert_eq!(wires.get_mut(a).unwrap().old.try_wait().unwrap(), None);
    }

    /// M4: failed delivery ⇒ `OldWire::TransportError` on THAT Agent only.
    /// (i) EPIPE: A's Real CLI exited after init while evidence says
    /// LiveIdle. (ii) timeout: deaf A + big steer. In both, B stays LiveIdle;
    /// `end-turn` A → next steer remints A (+1 mint), the resumed runner gets
    /// `session/resume` then exactly one `session/prompt`; never ReuseLive
    /// on the poisoned wire. Kills: (c) no mark; (d) mark peer / all;
    /// (f) mark Unknown; immediate remint under the open turn.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_send_error_marks_wire_lost_then_remints_after_end_turn() {
        use hearth_agent_wire::OldWire;
        for case in ["epipe", "timeout"] {
            let st = AppState::new(vec![]);
            let session = st.store().create_session();
            let sid = session.id();
            let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
            let (deaf, text) = match case {
                "epipe" => (dpbw_deaf(0), "dpbw-m4-one".to_string()),
                _ => (dpbw_deaf(10), "x".repeat(256 * 1024)),
            };
            dpbw_swap_old(&st, sid, a, deaf);
            if case == "epipe" {
                std::thread::sleep(std::time::Duration::from_millis(300));
            }
            assert_eq!(host_old_wire(&st, sid, a), OldWire::LiveIdle, "{case}");
            let m0 = count_mints(&st, sid);

            let (r1, _) = dpbw_steer(&st, sid, text).await;
            let (c1, _) = r1.unwrap_or_else(|| panic!("{case}: steer 1 wedged"));
            assert_eq!(c1, StatusCode::OK, "{case}");
            let ev_a = host_old_wire(&st, sid, a);
            assert!(
                matches!(ev_a, OldWire::TransportError(_)),
                "{case}: failed delivery marks A TransportError, got {ev_a:?}"
            );
            assert_eq!(
                host_old_wire(&st, sid, b),
                OldWire::LiveIdle,
                "{case}: peer B untouched"
            );
            assert_eq!(
                count_mints(&st, sid),
                m0,
                "{case}: no remint under the open turn"
            );
            assert_eq!(
                st.runtime.classify(sid, a).unwrap(),
                hearth::Liveness::LiveInTurn,
                "{case}: A's turn stays open until end-turn"
            );

            let (ce, be) = dpbw_end_turn(&st, sid, "alpha")
                .await
                .unwrap_or_else(|| panic!("{case}: end-turn wedged"));
            assert_eq!(ce, StatusCode::OK, "{case}: end-turn body={be}");
            let (r2, _) = dpbw_steer(&st, sid, "dpbw-m4-two".into()).await;
            let (c2, b2) = r2.unwrap_or_else(|| panic!("{case}: steer 2 wedged"));
            assert_eq!(c2, StatusCode::OK, "{case}: body={b2}");
            assert_eq!(
                count_mints(&st, sid),
                m0 + 1,
                "{case}: A reminted once (never ReuseLive on the poisoned wire)"
            );
            assert_eq!(host_old_wire(&st, sid, a), OldWire::LiveIdle, "{case}");
            let (old_a, _, _, _) = sdp_wire(&st, sid, a);
            assert_eq!(
                sdp_methods(&old_a),
                vec![
                    "initialize".to_string(),
                    "session/resume".to_string(),
                    "session/prompt".to_string()
                ],
                "{case}: resumed runner gets resume then exactly one prompt"
            );
            assert_eq!(sdp_prompt_text(&sdp_prompts(&old_a)[0]), "dpbw-m4-two");
        }
    }

    /// M5: no false timeout. A slow-but-reading Real CLI (reads after
    /// ~300 ms) gets the full ≥256 KiB prompt byte for byte; evidence stays
    /// LiveIdle. Kills: (e) first WouldBlock = error / zero deadline;
    /// partial-write loss.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_slow_reader_gets_full_prompt_no_false_timeout() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let out =
            std::env::temp_dir().join(format!("hearth-dpbw-slow-{}-{}", std::process::id(), sid.0));
        let _ = std::fs::remove_file(&out);
        dpbw_swap_old(
            &st,
            sid,
            a,
            dpbw_real_initialized(&format!(
                "sleep 0.3; IFS= read -r p; printf '%s' \"$p\" > {}; exec sleep 5",
                out.display()
            )),
        );
        let big = "z".repeat(256 * 1024);
        let (r, _) = dpbw_steer(&st, sid, big.clone()).await;
        let (c, body) = r.expect("slow-reader steer bounded");
        assert_eq!(c, StatusCode::OK, "body len={}", body.len());
        let mut got = String::new();
        for _ in 0..100 {
            got = std::fs::read_to_string(&out).unwrap_or_default();
            if serde_json::from_str::<serde_json::Value>(&got).is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let _ = std::fs::remove_file(&out);
        let v: serde_json::Value = serde_json::from_str(&got)
            .unwrap_or_else(|e| panic!("full prompt frame ({} bytes): {e}", got.len()));
        assert_eq!(v["method"], "session/prompt");
        assert_eq!(sdp_prompt_text(&v), big, "full prompt byte for byte");
        assert_eq!(
            host_old_wire(&st, sid, a),
            hearth_agent_wire::OldWire::LiveIdle,
            "no false timeout"
        );
    }

    // ---- A2 bounded-stdout-reader + resume state_update (floors-only RED) ----

    /// Real CLI that answers `initialize` then floods stdout with many
    /// `session/update` frames (>> 64 KiB pipe) without reading stdin again.
    fn a2_flood_after_init(secs: u32) -> hearth_agent_wire::AttachRunner {
        use hearth_agent_wire::{transport_for_wire, AttachRunner, LaunchSpec, RpcId, WireKind};
        let chunk = "x".repeat(360);
        let frame = format!(
            r#"{{"jsonrpc":"2.0","method":"session/update","params":{{"sessionId":"s","update":{{"sessionUpdate":"agent_message_chunk","content":{{"type":"text","text":"{chunk}"}}}}}}}}"#
        );
        let mut script = String::from(
            r#"IFS= read -r l; printf '%s\n' '{"jsonrpc":"2.0","id":0,"result":{"protocolVersion":1}}'; "#,
        );
        script.push_str("i=0; while [ \"$i\" -lt 220 ]; do printf '%s\\n' '");
        script.push_str(&frame);
        script.push_str("'; i=$((i+1)); done; exec sleep ");
        script.push_str(&secs.to_string());
        let mut r = AttachRunner::spawn(LaunchSpec {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            cwd: None,
            env: Vec::new(),
            wire: WireKind::JsonlRpc,
        })
        .expect("spawn flood CLI");
        r.initialize_host(&mut transport_for_wire(WireKind::JsonlRpc), &RpcId::Num(0))
            .expect("initialize flood CLI");
        std::thread::sleep(std::time::Duration::from_millis(200));
        r
    }

    /// M1: Real CLI floods unread-scale session/update after init; with the
    /// reader a small follow-up steer returns 200 without TransportError
    /// while under queue capacity; drained frames observed (spy > 0 in GREEN).
    /// Tip: flood → stdin timeout TransportError (RED).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_stdout_reader_drains_under_bound_no_false_lost() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        dpbw_swap_old(&st, sid, a, a2_flood_after_init(12));
        // Small steer: fits the stdin pipe buffer even if the CLI never reads
        // again (flood child ends in `sleep`). Tip without a reader still
        // accepts the small write but learns nothing (spy depth 0 / no queue).
        // With the reader, drain under capacity leaves evidence live and
        // queue depth > 0. (Large-write false-lost is covered by M2 overflow
        // and #92 deaf floors.)
        let (r, elapsed) = dpbw_steer(&st, sid, "a2-m1-small".into()).await;
        let (code, body) =
            r.unwrap_or_else(|| panic!("steer wedged > {DPBW_BOUND:?} behind flooded stdout"));
        assert_eq!(code, StatusCode::OK, "body len={}", body.len());
        let ev = host_old_wire(&st, sid, a);
        eprintln!(
            "A2-M1: elapsed={elapsed:?} A_evidence={ev:?} B_prompts={:?}",
            sdp_prompt_count(&st, sid, b)
        );
        assert!(
            !matches!(ev, hearth_agent_wire::OldWire::TransportError(_)),
            "A2 M1: reader must drain under bound — no false TransportError; tip got {ev:?}"
        );
        assert!(
            matches!(
                ev,
                hearth_agent_wire::OldWire::LiveIdle | hearth_agent_wire::OldWire::LiveInTurn
            ),
            "A2 M1: wire stays live under bound; got {ev:?}"
        );
        let depth = {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            wires.get_mut(a).unwrap().old.stdout_notif_len()
        };
        assert!(
            depth > 0,
            "A2 M1: reader must observe drained frames; depth={depth}"
        );
    }

    /// M2: Queue overflow ⇒ THAT Agent's old_wire = TransportError (overflow);
    /// peers continue; Session lock free; 200. Tip: no queue — flood forces
    /// stdin-timeout TransportError instead; assert overflow-specific mark.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_stdout_queue_overflow_marks_wire_lost_peers_ok() {
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let mut flooded = a2_flood_after_init(12);
        // Tiny bound so the ~220-frame flood overflows the notification queue
        // during deliver-time drain (marks TransportError; peers continue).
        flooded.set_stdout_notif_bound(16);
        dpbw_swap_old(&st, sid, a, flooded);
        let (r, _) = dpbw_steer(&st, sid, "a2-m2".into()).await;
        let (code, body) = r.unwrap_or_else(|| panic!("steer wedged > {DPBW_BOUND:?}"));
        assert_eq!(code, StatusCode::OK, "body len={}", body.len());
        assert!(dpbw_lock_free(&st, sid), "Session wire lock free");
        let ev = host_old_wire(&st, sid, a);
        eprintln!(
            "A2-M2: A_evidence={ev:?} B_prompts={:?}",
            sdp_prompt_count(&st, sid, b)
        );
        match &ev {
            hearth_agent_wire::OldWire::TransportError(msg) => {
                assert!(
                    msg.contains("overflow") || msg.contains("notification queue"),
                    "A2 M2: overflow must mark TransportError with overflow/queue text; tip got {msg:?}"
                );
            }
            other => panic!("A2 M2: overflow ⇒ TransportError on A; got {other:?}"),
        }
        // Peer B still prompted (same as #92 peers-continue).
        assert_eq!(sdp_prompt_count(&st, sid, b).0, 1, "peer B still prompted");
    }

    /// Build Fake `new` stdout: init + state_update{sessionState} + optional
    /// tool_call_update + resume result (id 2).
    fn a2_fake_new_with_state(
        session_state: &str,
        with_tool: bool,
    ) -> hearth_agent_wire::AttachRunner {
        use hearth_agent_wire::{AttachRunner, JsonlRpcTransport, WireKind};
        let init = serde_json::json!({
            "jsonrpc": "2.0", "id": 1,
            "result": { "protocolVersion": 1,
                "agentCapabilities": { "session": { "resume": true, "cancel": true, "close": true } } }
        });
        let state = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "native-a",
                "update": {
                    "sessionUpdate": "state_update",
                    "sessionState": session_state,
                    "background": []
                }
            }
        });
        let resume_ok = serde_json::json!({
            "jsonrpc": "2.0", "id": 2,
            "result": { "sessionId": "native-a" }
        });
        let mut bytes = JsonlRpcTransport::encode_jsonl(&init).unwrap();
        bytes.extend(JsonlRpcTransport::encode_jsonl(&state).unwrap());
        if with_tool {
            let tool = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "sessionId": "native-a",
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "tc-inflight",
                        "status": "in_progress",
                        "title": "read"
                    }
                }
            });
            bytes.extend(JsonlRpcTransport::encode_jsonl(&tool).unwrap());
        }
        bytes.extend(JsonlRpcTransport::encode_jsonl(&resume_ok).unwrap());
        AttachRunner::spawn_fake(WireKind::JsonlRpc, bytes)
    }

    /// M3: Remint with state_update{running} (+ in-flight tool) → not idle;
    /// zero fresh user session/prompt. Tip: LiveIdle + 1 prompt (RED).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_remint_running_state_update_no_idle_prompt() {
        use hearth_agent_wire::OldWire;
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            w.new = a2_fake_new_with_state("running", true);
            w.new_next_rpc_id = 2;
            w.old_wire = OldWire::StdoutEof;
        }
        let m0 = count_mints(&st, sid);
        let (code, body) = sdp_steer(&st, sid, "cheng", "a2-m3").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(count_mints(&st, sid) > m0, "A reminted");
        let ev = host_old_wire(&st, sid, a);
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let prompts = sdp_prompts(&old_a);
        eprintln!(
            "A2-M3: evidence={ev:?} methods={:?} prompts={}",
            sdp_methods(&old_a),
            prompts.len()
        );
        assert!(
            !matches!(ev, OldWire::LiveIdle),
            "A2 M3: remint settle must use resume state_update (running), not hardcode LiveIdle; got {ev:?}"
        );
        assert!(
            prompts.is_empty(),
            "A2 M3: do not deliver a fresh session/prompt when state_update=running; got {} prompts",
            prompts.len()
        );
    }

    /// M4: Remint with state_update{idle} → idle-compatible settle; a
    /// subsequent/this steer may deliver one prompt (contrast to M3).
    /// Tip already LiveIdle+prompt → contrast green on tip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_remint_idle_state_update_allows_prompt() {
        use hearth_agent_wire::OldWire;
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            w.new = a2_fake_new_with_state("idle", false);
            w.new_next_rpc_id = 2;
            w.old_wire = OldWire::StdoutEof;
        }
        let m0 = count_mints(&st, sid);
        let (code, body) = sdp_steer(&st, sid, "cheng", "a2-m4").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(count_mints(&st, sid) > m0, "A reminted");
        let ev = host_old_wire(&st, sid, a);
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let prompts = sdp_prompts(&old_a);
        eprintln!(
            "A2-M4: evidence={ev:?} methods={:?} prompts={}",
            sdp_methods(&old_a),
            prompts.len()
        );
        assert_eq!(ev, OldWire::LiveIdle, "A2 M4: idle state_update ⇒ LiveIdle");
        assert_eq!(prompts.len(), 1, "A2 M4: idle ⇒ one prompt allowed");
        assert_eq!(sdp_prompt_text(&prompts[0]), "a2-m4");
    }

    /// M5: Remint with state_update{requires_action} → non-idle; no fresh
    /// user prompt. Tip: LiveIdle + 1 prompt (RED).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_remint_requires_action_state_update_no_idle_prompt() {
        use hearth_agent_wire::OldWire;
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            w.new = a2_fake_new_with_state("requires_action", true);
            w.new_next_rpc_id = 2;
            w.old_wire = OldWire::StdoutEof;
        }
        let m0 = count_mints(&st, sid);
        let (code, body) = sdp_steer(&st, sid, "cheng", "a2-m5").await;
        assert_eq!(code, StatusCode::OK, "body={body}");
        assert!(count_mints(&st, sid) > m0, "A reminted");
        let ev = host_old_wire(&st, sid, a);
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let prompts = sdp_prompts(&old_a);
        eprintln!(
            "A2-M5: evidence={ev:?} methods={:?} prompts={}",
            sdp_methods(&old_a),
            prompts.len()
        );
        assert!(
            !matches!(ev, OldWire::LiveIdle),
            "A2 M5: requires_action must not settle LiveIdle; got {ev:?}"
        );
        assert!(
            prompts.is_empty(),
            "A2 M5: no fresh user prompt under requires_action; got {} prompts",
            prompts.len()
        );
    }

    // ---- ReuseLive leftover resume_session_state (post-A2) ----

    /// Fake old: answered initialize; unread state_update{running} still on stdout.
    fn reuse_live_leftover_old_with_unread_running() -> hearth_agent_wire::AttachRunner {
        use hearth_agent_wire::{
            transport_for_wire, AttachRunner, JsonlRpcTransport, RpcId, WireKind,
        };
        let init = serde_json::json!({
            "jsonrpc": "2.0", "id": 0, "result": { "protocolVersion": 1 }
        });
        let state = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "S-a",
                "update": {
                    "sessionUpdate": "state_update",
                    "sessionState": "running",
                    "background": []
                }
            }
        });
        let mut bytes = JsonlRpcTransport::encode_jsonl(&init).unwrap();
        bytes.extend(JsonlRpcTransport::encode_jsonl(&state).unwrap());
        let mut r = AttachRunner::spawn_fake(WireKind::JsonlRpc, bytes);
        r.initialize_host(&mut transport_for_wire(WireKind::JsonlRpc), &RpcId::Num(0))
            .expect("initialize");
        r
    }

    /// M1: poll-on-deliver drain captures state_update{running} into
    /// resume_session_state; after Host end-turn the next steer ReuseLive-settle
    /// must not apply that leftover → must settle LiveIdle and deliver the
    /// fresh user prompt. Tip applies leftover → LiveInTurn and skips prompt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_reuse_live_leftover_running_still_prompts_after_end_turn() {
        use hearth_agent_wire::{OldWire, ResumeSessionState};
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        dpbw_swap_old(&st, sid, a, reuse_live_leftover_old_with_unread_running());

        let (c1, b1) = sdp_steer(&st, sid, "cheng", "probe-p1").await;
        assert_eq!(c1, StatusCode::OK, "steer1 body={b1}");
        let (pa1, _) = sdp_prompt_count(&st, sid, a);
        assert_eq!(
            pa1, 1,
            "first steer delivers prompt (drain happens after settle)"
        );
        // Drain during deliver must have captured running into resume_session_state.
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let stt = wires.get_mut(a).unwrap().old.resume_session_state();
            assert_eq!(
                stt,
                Some(ResumeSessionState::Running),
                "drain leftover must sit on resume_session_state; got {stt:?}"
            );
        }
        let (ce, be) = sdp_end_turn(&st, sid, "alpha").await;
        assert_eq!(ce, StatusCode::OK, "end-turn alpha body={be}");
        let (ce, be) = sdp_end_turn(&st, sid, "beta").await;
        assert_eq!(ce, StatusCode::OK, "end-turn beta body={be}");

        let (c2, b2) = sdp_steer(&st, sid, "cheng", "probe-p2").await;
        assert_eq!(c2, StatusCode::OK, "steer2 body={b2}");
        let ev = host_old_wire(&st, sid, a);
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let prompts = sdp_prompts(&old_a);
        let texts: Vec<String> = prompts.iter().map(sdp_prompt_text).collect();
        eprintln!(
            "M1: evidence={ev:?} prompts={texts:?} B={:?}",
            sdp_prompt_count(&st, sid, b)
        );
        assert_eq!(
            ev,
            OldWire::LiveIdle,
            "Must: ReuseLive after end-turn must settle LiveIdle (not leftover running); tip got {ev:?}"
        );
        assert!(
            texts.iter().any(|t| t == "probe-p2"),
            "Must: second steer must deliver probe-p2; tip skipped. prompts={texts:?}"
        );
    }

    /// M2: after end-turn, inject leftover Running on LiveIdle old; next
    /// steer must still prompt (same Must as M1 without Fake stdout dependency).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn http_reuse_live_injected_leftover_running_still_prompts() {
        use hearth_agent_wire::{OldWire, ResumeSessionState};
        let st = AppState::new(vec![]);
        let session = st.store().create_session();
        let sid = session.id();
        let (a, _b, _) = install_two_parallel_safe_host_wires(&st, sid, None);
        let (c1, _) = sdp_steer(&st, sid, "cheng", "inj-p1").await;
        assert_eq!(c1, StatusCode::OK);
        let (ce, _) = sdp_end_turn(&st, sid, "alpha").await;
        assert_eq!(ce, StatusCode::OK);
        let (ce, _) = sdp_end_turn(&st, sid, "beta").await;
        assert_eq!(ce, StatusCode::OK);
        {
            let sw = session_wires_arc(&st, sid);
            let mut wires = sw.lock().unwrap();
            let w = wires.get_mut(a).unwrap();
            assert_eq!(w.old_wire, OldWire::LiveIdle);
            w.old
                .set_resume_session_state(Some(ResumeSessionState::Running));
        }
        let (c2, b2) = sdp_steer(&st, sid, "cheng", "inj-p2").await;
        assert_eq!(c2, StatusCode::OK, "body={b2}");
        let ev = host_old_wire(&st, sid, a);
        let (old_a, _, _, _) = sdp_wire(&st, sid, a);
        let texts: Vec<String> = sdp_prompts(&old_a).iter().map(sdp_prompt_text).collect();
        eprintln!("M2: evidence={ev:?} prompts={texts:?}");
        assert_eq!(
            ev,
            OldWire::LiveIdle,
            "tip leftover ⇒ LiveInTurn; Must LiveIdle; got {ev:?}"
        );
        assert!(
            texts.iter().any(|t| t == "inj-p2"),
            "Must deliver inj-p2; tip skipped. prompts={texts:?}"
        );
    }
}
