use hearth_service::{probe_local, router, AppState};

#[tokio::main]
async fn main() {
    let state = AppState::new(probe_local());
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8787")
        .await
        .expect("bind 0.0.0.0:8787");
    axum::serve(listener, app).await.expect("serve");
}
