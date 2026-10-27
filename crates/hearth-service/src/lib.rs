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
    AgentHostWire, CompleteToolError, CompleteToolOutcome, HostWireMap, SteerError, SteerOutcome,
};
pub use probe::{probe_local, CliStatus};

#[derive(Clone)]
struct LiveEvent {
    session: SessionId,
    event: EventOut,
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) runtime: Runtime,
    names: Arc<Mutex<NameMap>>,
    clis: Vec<CliStatus>,
    live: broadcast::Sender<LiveEvent>,
    /// Host wires keyed by `(Session, Agent)` for
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
        self.host_wires
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .insert(session, agent, wire);
        Ok(())
    }

    /// Override `OldWire` evidence on an installed Host wire (Must remint floors).
    pub fn set_host_old_wire(
        &self,
        session: SessionId,
        agent: hearth::AgentId,
        old_wire: hearth_agent_wire::OldWire,
    ) -> Result<(), StatusCode> {
        let mut g = self
            .host_wires
            .lock()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let w = g.get_mut(session, agent).ok_or(StatusCode::NOT_FOUND)?;
        w.old_wire = old_wire;
        Ok(())
    }

    fn store(&self) -> &InMemory {
        self.runtime.store()
    }

    fn publish(&self, session: SessionId, event: EventOut) {
        let _ = self.live.send(LiveEvent { session, event });
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/sessions", post(create_session))
        .route("/sessions/{id}", get(get_session))
        .route("/sessions/{id}/join", post(join_session))
        .route("/sessions/{id}/events", get(list_events).post(post_event))
        .route(
            "/sessions/{id}/agents/{agent}/tool-result",
            post(post_tool_result),
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
    if let Some(aid) = keep_agent {
        keep_standin(st, id, aid)?;
        // Explicit declare only — never inferred from tools (DESIGN).
        if let Some(paths) = parallel_safe {
            st.runtime
                .set_parallel_safe(id, aid, Some(hearth::ParallelSafe::paths(paths)))
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
    }
    let out = event_out(&ev);
    st.publish(id, out.clone());
    Ok(out)
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
    match host_steer::steer_each_kept(st, id, Wake::UserQuery { user: uid, text }) {
        Ok(outcome) => {
            let per_agent = outcome.woke();
            let mut um: Option<EventOut> = None;
            for (_agent, evs) in per_agent {
                for e in evs {
                    let out = event_out(e);
                    if matches!(e.body, EventBody::UserMessage { .. }) {
                        um = Some(out.clone());
                    }
                    st.publish(id, out);
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
    Ok(Json(apply_user_message(&st, id, name, text)?))
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
    match host_steer::complete_tool_result(&st, id, agent, &body.tool_call_id, &output) {
        Ok(CompleteToolOutcome::Applied {
            tool_result,
            wake_events,
            ..
        }) => {
            let out = event_out(&tool_result);
            st.publish(id, out.clone());
            for e in &wake_events {
                st.publish(id, event_out(e));
            }
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
        | Err(CompleteToolError::NoWire)
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
    let mut rx = st.live.subscribe();
    if let Ok(session) = st.store().session(id) {
        if let Ok(events) = session.events() {
            for e in events {
                let txt = serde_json::to_string(&event_out(&e)).unwrap_or_default();
                if socket.send(Message::Text(txt.into())).await.is_err() {
                    return;
                }
            }
        }
    }
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(txt))) => {
                        if let Err(code) = apply_ws_text(&st, id, txt.as_str()) {
                            let err = serde_json::json!({ "error": code.as_u16() }).to_string();
                            if socket.send(Message::Text(err.into())).await.is_err() {
                                return;
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
                        let txt = serde_json::to_string(&live.event).unwrap_or_default();
                        if socket.send(Message::Text(txt.into())).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
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
        let mut wires = st.host_wires.lock().unwrap();
        for agent in [a, b] {
            let w = wires.get_mut(sid, agent).unwrap();
            let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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

        let mut wires = st.host_wires.lock().unwrap();
        // Fan-out continues across caps-refuse: every kept Agent is host_wake'd
        // and FailClosed-skipped (tip abort-on-first mutant fails).
        for agent in [a, b] {
            let w = wires.get_mut(sid, agent).unwrap();
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

        let mut wires = st.host_wires.lock().unwrap();
        for agent in [a, b] {
            let w = wires.get_mut(sid, agent).unwrap();
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

        let mut wires = st.host_wires.lock().unwrap();
        let w_a = wires.get_mut(sid, a).unwrap();
        assert!(
            w_a.remint
                .observe()
                .iter()
                .any(|e| e.kind == RemintEventKind::FailClosed && e.text == "ResumeNotSupported"),
            "A FailClosed"
        );
        assert!(w_a.new.stdin_log().unwrap().is_empty(), "A no spawn");
        let w_b = wires.get_mut(sid, b).unwrap();
        let raw_b = std::str::from_utf8(w_b.new.stdin_log().unwrap()).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            assert!(
                wires.get_mut(sid, a).unwrap().remint.turn_in_flight(),
                "A mid-turn before steer"
            );
            assert!(
                wires.get_mut(sid, b).unwrap().remint.turn_in_flight(),
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
        let mut wires = st.host_wires.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(sid, agent).unwrap();
            assert!(
                !w.remint.turn_in_flight(),
                "Agent {agent:?} turn cleared after remint (cancel-before-reattach)"
            );
            let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            {
                let wa = wires.get_mut(sid, a).unwrap();
                assert!(wa.remint.turn_in_flight(), "A mid-turn before steer");
                assert_eq!(wa.remint.pending_permissions().len(), 1);
            }
            {
                let wb = wires.get_mut(sid, b).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            {
                let remint_a = &wires.get_mut(sid, a).unwrap().remint;
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
                let remint_b = &wires.get_mut(sid, b).unwrap().remint;
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
                let remint_a = &wires.get_mut(sid, a).unwrap().remint;
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
            let mut wires = st.host_wires.lock().unwrap();
            let parents_a: Vec<Option<String>> = wires
                .get_mut(sid, a)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            let parents_b: Vec<Option<String>> = wires
                .get_mut(sid, b)
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
        let mut wires = st.host_wires.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(sid, agent).unwrap();
            let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            assert!(
                wires.get_mut(sid, a).unwrap().remint.turn_in_flight(),
                "A mid-turn before steer"
            );
            assert!(
                wires.get_mut(sid, b).unwrap().remint.turn_in_flight(),
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
        let mut wires = st.host_wires.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(sid, agent).unwrap();
            assert!(
                !w.remint.turn_in_flight(),
                "Agent {agent:?} turn cleared after remint (cancel-before-reattach)"
            );
            let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            {
                let wa = wires.get_mut(sid, a).unwrap();
                assert!(wa.remint.turn_in_flight(), "A mid-turn before steer");
                assert_eq!(wa.remint.pending_permissions().len(), 1);
            }
            {
                let wb = wires.get_mut(sid, b).unwrap();
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
            let mut wires = st.host_wires.lock().unwrap();
            {
                let remint_a = &wires.get_mut(sid, a).unwrap().remint;
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
                let remint_b = &wires.get_mut(sid, b).unwrap().remint;
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
                let remint_a = &wires.get_mut(sid, a).unwrap().remint;
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
            let mut wires = st.host_wires.lock().unwrap();
            let parents_a: Vec<Option<String>> = wires
                .get_mut(sid, a)
                .unwrap()
                .remint
                .resurfaced_permissions()
                .iter()
                .map(|p| p.parent_tool_call_id.clone())
                .collect();
            let parents_b: Vec<Option<String>> = wires
                .get_mut(sid, b)
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
        let mut wires = st.host_wires.lock().unwrap();
        for (agent, expected_sid) in expected {
            let w = wires.get_mut(sid, agent).unwrap();
            let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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

    fn count_tool_results(st: &AppState, sid: SessionId, tool_call_id: &str) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::ToolResult { tool_call_id: id, .. } if id == tool_call_id
                )
            })
            .count()
    }

    fn count_trigger_wakes(st: &AppState, sid: SessionId) -> usize {
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::Wake {
                        source: hearth::WakeSource::Trigger { name }
                    } if name == "tool_result"
                )
            })
            .count()
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
        let mut wires = st.host_wires.lock().unwrap();
        let w = wires.get_mut(sid, a).unwrap();
        let raw = std::str::from_utf8(w.new.stdin_log().unwrap()).unwrap();
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
        st.store()
            .session(sid)
            .unwrap()
            .events()
            .unwrap()
            .iter()
            .filter(|e| {
                matches!(
                    &e.body,
                    EventBody::ToolCallInterrupted { tool_call_id: id, .. } if id == tool_call_id
                )
            })
            .count()
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
            let mut wires = st.host_wires.lock().unwrap();
            let w = wires.get_mut(sid, a).unwrap();
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
}
