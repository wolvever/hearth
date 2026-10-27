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
pub use host_steer::{AgentHostWire, HostWireMap, SteerError, SteerOutcome};
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

    /// Join two Agents with optional ParallelSafe path lists (product join
    /// path), then install Host Bindings + RemintSession + Fake wires.
    /// `None` paths → undeclared (fail-closed serialize).
    fn install_two_host_wires_with_declare(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
        paths_a: Option<Vec<String>>,
        paths_b: Option<Vec<String>>,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        use hearth::{Host, PlaceAttach};
        use hearth_agent_wire::{LiveCaps as WireCaps, RemintSession};

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
        (a, b, place)
    }

    /// Service-host-wake-each Must floor helper: two ParallelSafe Agents with
    /// Host Bindings + RemintSession + Fake wires installed on AppState.
    /// Declares disjoint paths via the join product path.
    fn install_two_parallel_safe_host_wires(
        st: &AppState,
        sid: SessionId,
        lost: Option<hearth_agent_wire::OldWire>,
    ) -> (hearth::AgentId, hearth::AgentId, hearth::Place) {
        install_two_host_wires_with_declare(
            st,
            sid,
            lost,
            Some(vec!["agent-0".into()]),
            Some(vec!["agent-1".into()]),
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
        let (a, b, _) = install_two_host_wires_with_declare(&st, sid, None, None, None);

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
