//! Demo: map sample native payloads from each harness into AgentEvent.

use hearth_agent_wire::adapters::{codex, grok_build, opencode, pi};
use hearth_agent_wire::*;

fn check(name: &str, ok: bool) {
    if ok {
        println!("[PASS] {name}");
    } else {
        println!("[FAIL] {name}");
        std::process::exit(1);
    }
}

fn main() {
    let mut lb = LoopbackAgent::new(AgentKind::GrokBuild);
    lb.send(AgentCommand::OpenSession {
        project_id: "p".into(),
        cwd: Some("/tmp".into()),
    })
    .unwrap();
    lb.send(AgentCommand::UserMessage {
        session_id: "sess-loop".into(),
        text: "ping".into(),
    })
    .unwrap();
    lb.send(AgentCommand::Compact {
        session_id: "sess-loop".into(),
    })
    .unwrap();
    let mut n = 0;
    while lb.try_recv().unwrap().is_some() {
        n += 1;
    }
    check("loopback emits session+turn+compact", n >= 5);

    let grok = grok_build::map_notification(&serde_json::json!({
        "method": "session/update",
        "params": {
            "sessionId": "s",
            "update": {
                "sessionUpdate": "tool_call_update",
                "toolCallId": "c1",
                "title": "bash",
                "status": "pending"
            }
        }
    }))
    .unwrap();
    check(
        "grok-build ACP tool_call → ToolCall",
        matches!(grok, AgentEvent::ToolCall { .. }),
    );

    let cx = codex::map_notification(&serde_json::json!({
        "method": "item/completed",
        "params": {
            "threadId": "th",
            "item": { "type": "reasoning", "id": "r", "text": "think" }
        }
    }))
    .unwrap();
    check(
        "codex reasoning → Thinking",
        matches!(cx, AgentEvent::Thinking { .. }),
    );

    let p = pi::map_event(&serde_json::json!({
        "type": "run.start",
        "sessionId": "s",
        "runId": "r1"
    }))
    .unwrap();
    check("pi run.start → TurnStarted", matches!(p, AgentEvent::TurnStarted { .. }));

    let oc = opencode::map_event(&serde_json::json!({
        "type": "permission.asked",
        "properties": {
            "sessionID": "s",
            "id": "p1",
            "title": "run?",
            "options": []
        }
    }))
    .unwrap();
    check(
        "opencode permission.asked → PermissionAsk",
        matches!(oc, AgentEvent::PermissionAsk { .. }),
    );

    check(
        "AgentCommand Compact serde",
        serde_json::to_string(&AgentCommand::Compact {
            session_id: "s".into(),
        })
        .unwrap()
        .contains("compact"),
    );

    println!("summary: 6 passed — unified wire ready for Hearth");
}
