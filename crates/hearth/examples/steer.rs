//! Two users, one agent, one room: bind → ask → unbind → bind → decide → print surface.

use hearth::{InMemory, Member, NullProvisioner, Provisioner};

fn main() {
    let store = InMemory::new();
    let cheng = store.create_user("cheng");
    let guest = store.create_user("guest");
    let agent = store.create_agent("scribe", "ask before you push");
    let session = store.create_session();

    session.join(Member::User(cheng.id)).unwrap();
    session.join(Member::User(guest.id)).unwrap();
    session.join(Member::Agent(agent.id)).unwrap();

    let first = session
        .bind("claude_code", Some("resume-1".into()), None)
        .unwrap();
    session
        .user_message(cheng.id, "please ship the kernel notes")
        .unwrap();
    session.ask_permission(agent.id, "git push").unwrap();
    session.unbind(first.id).unwrap();

    let second = NullProvisioner
        .provision(&session, "claude_code", Some("resume-2".into()), None)
        .unwrap();
    session
        .decide_permission("git push", true, guest.id)
        .unwrap();
    session
        .append(hearth::EventBody::AgentMessage {
            agent: agent.id,
            text: "pushed".into(),
        })
        .unwrap();

    println!(
        "session {} bindings={} members={}",
        session.id().0,
        session.bindings().unwrap().len(),
        session.members().unwrap().len()
    );
    println!(
        "live binding kind={} resume={:?}",
        second.kind, second.native_resume_id
    );
    println!("surface:");
    for e in session.surface().unwrap() {
        println!("  id={:?} seq={:?} {:?}", e.id, e.seq, e.body);
    }
}
