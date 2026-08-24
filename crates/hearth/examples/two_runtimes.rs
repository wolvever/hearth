//! Option A (co-tenant) + option B (tool proxy) on one Session.
//! Place is `sandbox_id` + FakeSandbox — not a sixth Environment type.

use hearth::{EventBody, FakeSandbox, HostKind, InMemory, Member};

fn main() {
    let store = InMemory::new();
    let claude = store.create_agent("claude", "co-tenant");
    let codex = store.create_agent("codex", "co-tenant");
    let session = store.create_session();
    session.join(Member::Agent(claude.id)).unwrap();
    session.join(Member::Agent(codex.id)).unwrap();

    let sid = "sb-demo";
    let cc = session
        .bind_agent(
            claude.id,
            HostKind::ClaudeCode,
            Some("resume-cc".into()),
            Some(sid.into()),
        )
        .unwrap();
    let cx = session
        .bind_agent(
            codex.id,
            HostKind::Codex,
            Some("resume-cx".into()),
            Some(sid.into()),
        )
        .unwrap();
    assert_eq!(cc.sandbox_id, cx.sandbox_id);
    assert_ne!(cc.native_resume_id, cx.native_resume_id);

    let mut place = FakeSandbox::new();
    session
        .append(EventBody::ToolCall {
            agent: claude.id,
            name: "write".into(),
            input: "path=/a.txt\nbody=hello".into(),
        })
        .unwrap();
    place.apply_write(sid, "path=/a.txt\nbody=hello");

    session.leave(Member::Agent(claude.id)).unwrap();
    println!(
        "after claude leave: bindings={} sandbox_ids={:?} file={:?}",
        session.bindings().unwrap().len(),
        session.live_sandbox_ids().unwrap(),
        place.read(sid, "/a.txt")
    );
    println!("session still {} members={}", session.id().0, session.members().unwrap().len());
}
