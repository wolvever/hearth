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
    Event, EventBody, InMemory, Member, Place, PlaceAttach, SessionId,
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

mod probe;
pub use probe::{probe_local, CliStatus};

#[derive(Clone)]
struct LiveEvent {
    session: SessionId,
    event: EventOut,
}

#[derive(Clone)]
pub struct AppState {
    store: InMemory,
    names: Arc<Mutex<NameMap>>,
    clis: Vec<CliStatus>,
    live: broadcast::Sender<LiveEvent>,
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
            store: InMemory::new(),
            names: Arc::new(Mutex::new(NameMap::default())),
            clis,
            live,
        }
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
    let session = st.store.create_session();
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
    let session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(Json(view(&session)))
}

#[derive(Deserialize)]
struct JoinBody {
    user: Option<String>,
    agent: Option<String>,
}

/// Shared join used by HTTP POST /join and WS `{"type":"join",...}`.
fn apply_join(
    st: &AppState,
    id: SessionId,
    user: Option<String>,
    agent: Option<String>,
) -> Result<EventOut, StatusCode> {
    let session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let mut names = st.names.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let ev = if let Some(name) = user {
        let uid = *names
            .users
            .entry(name.clone())
            .or_insert_with(|| st.store.create_user(name).id);
        session.join(Member::User(uid)).map_err(|_| StatusCode::CONFLICT)?
    } else if let Some(name) = agent {
        let aid = *names
            .agents
            .entry(name.clone())
            .or_insert_with(|| st.store.create_agent(name, String::new()).id);
        session.join(Member::Agent(aid)).map_err(|_| StatusCode::CONFLICT)?
    } else {
        return Err(StatusCode::BAD_REQUEST);
    };
    drop(names);
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
    apply_join(&st, id, body.user, body.agent)?;
    let session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
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

/// Shared steer used by HTTP POST /events and WS `{"type":"message",...}`.
fn apply_user_message(
    st: &AppState,
    id: SessionId,
    name: String,
    text: String,
) -> Result<EventOut, StatusCode> {
    let session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    let names = st.names.lock().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let uid = *names.users.get(&name).ok_or(StatusCode::NOT_FOUND)?;
    drop(names);
    let ev = session
        .user_message(uid, text)
        .map_err(|_| StatusCode::FORBIDDEN)?;
    let out = event_out(&ev);
    st.publish(id, out.clone());
    Ok(out)
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
    let session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
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
    message: Option<String>,
    text: Option<String>,
}

fn apply_ws_text(st: &AppState, id: SessionId, txt: &str) -> Result<EventOut, StatusCode> {
    let body: WsIn = serde_json::from_str(txt).map_err(|_| StatusCode::BAD_REQUEST)?;
    let kind = body.kind.as_deref().unwrap_or("");
    let has_text = body.message.is_some() || body.text.is_some();
    match kind {
        "join" => apply_join(st, id, body.user, body.agent),
        "message" | "user" | "steer" => {
            let name = body.user.ok_or(StatusCode::BAD_REQUEST)?;
            let text = body.message.or(body.text).ok_or(StatusCode::BAD_REQUEST)?;
            apply_user_message(st, id, name, text)
        }
        "" if has_text => {
            let name = body.user.ok_or(StatusCode::BAD_REQUEST)?;
            let text = body.message.or(body.text).ok_or(StatusCode::BAD_REQUEST)?;
            apply_user_message(st, id, name, text)
        }
        "" => apply_join(st, id, body.user, body.agent),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

async fn stream_session(
    ws: WebSocketUpgrade,
    State(st): State<AppState>,
    AxPath(id): AxPath<String>,
) -> Result<impl IntoResponse, StatusCode> {
    let id = sid(&id)?;
    let _session = st.store.session(id).map_err(|_| StatusCode::NOT_FOUND)?;
    Ok(ws.on_upgrade(move |socket| push_stream(socket, st, id)))
}

async fn push_stream(mut socket: WebSocket, st: AppState, id: SessionId) {
    let mut rx = st.live.subscribe();
    if let Ok(session) = st.store.session(id) {
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
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20).await.unwrap();
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
            Request::get(format!("/sessions/{id}")).body(Body::empty()).unwrap(),
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
            Request::post(format!("/sessions/{id}/events"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"user":"cheng","message":"hello"}"#))
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);

        let (st, txt) = call(
            app.clone(),
            Request::get(format!("/sessions/{id}/events"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(txt.contains("hello"));

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

        let j = http(addr, "POST", &format!("/sessions/{id}/join"), Some(r#"{"user":"cheng"}"#)).await;
        assert_eq!(j.0, 200);
        let j2 = http(addr, "POST", &format!("/sessions/{id}/join"), Some(r#"{"user":"guest"}"#)).await;
        assert_eq!(j2.0, 200);

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

        a.send(WsMsg::Text(
            r#"{"type":"message","user":"cheng","message":"steer-from-a"}"#.into(),
        ))
        .await
        .unwrap();
        expect_text(&mut a, "steer-from-a").await;
        expect_text(&mut b, "steer-from-a").await;

        b.send(WsMsg::Text(r#"{"user":"guest","message":"steer-from-b"}"#.into()))
            .await
            .unwrap();
        expect_text(&mut a, "steer-from-b").await;
        expect_text(&mut b, "steer-from-b").await;

        let listed = http(addr, "GET", &format!("/sessions/{id}/events"), None).await;
        assert_eq!(listed.0, 200);
        assert!(listed.1.contains("steer-from-a"));
        assert!(listed.1.contains("steer-from-b"));

        let posted = http(
            addr,
            "POST",
            &format!("/sessions/{id}/events"),
            Some(r#"{"user":"cheng","message":"http-still"}"#),
        )
        .await;
        assert_eq!(posted.0, 200);
        expect_text(&mut a, "http-still").await;
        expect_text(&mut b, "http-still").await;

        let _ = a.close(None).await;
        let _ = b.close(None).await;
    }

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

    async fn http(addr: std::net::SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
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
