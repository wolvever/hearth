//! Optional Grok (xAI) runner against [`hearth::Runtime::keep`] + [`hearth::Runtime::wake`].
//!
//! Without `XAI_API_KEY` or `GROK_API_KEY` this test returns (skip, not fail).
//! With a key it remints a `grok` Binding, records one user turn, and calls
//! `POST https://api.x.ai/v1/chat/completions` once. Override the model with
//! `XAI_MODEL` or `GROK_MODEL` (default `grok-3-mini`).
//!
//! Sandbox stays `Binding.sandbox_id` — this is not an Environment type.

use hearth::{grok_api_key, EventBody, HostKind, Member, Runtime, Wake};

#[test]
fn keep_wake_then_optional_grok_chat_completion() {
    let rt = Runtime::new();
    let user = rt.store().create_user("cheng");
    let agent = rt.store().create_agent("grok", "reply in one short sentence");
    let session = rt.create_session();
    session.join(Member::User(user.id)).unwrap();

    rt.keep(session.id(), agent.id, HostKind::Grok.host(None, None))
        .unwrap();
    assert!(session.bindings().unwrap().is_empty());

    let prompt = "Reply with the single word pong.";
    let evs = rt
        .wake(
            session.id(),
            Wake::UserQuery {
                user: user.id,
                text: prompt.into(),
            },
        )
        .unwrap();

    let binds = session.bindings().unwrap();
    assert_eq!(binds.len(), 1);
    assert_eq!(binds[0].kind, "grok");
    assert_eq!(binds[0].agent, Some(agent.id));
    assert!(evs.iter().any(|e| matches!(
        &e.body,
        EventBody::UserMessage { text, .. } if text == prompt
    )));
    assert!(evs
        .iter()
        .any(|e| matches!(e.body, EventBody::TurnStart { .. })));
    assert!(evs
        .iter()
        .any(|e| matches!(e.body, EventBody::BindingAttached { .. })));

    let Some(key) = grok_api_key() else {
        eprintln!("skipping Grok chat completion: set XAI_API_KEY or GROK_API_KEY to run");
        return;
    };

    let reply = grok_chat_completion(&key, prompt).expect("xAI chat completion");
    assert!(
        !reply.trim().is_empty(),
        "Grok returned an empty completion"
    );

    session
        .append(EventBody::AgentMessage {
            agent: agent.id,
            text: reply,
        })
        .unwrap();
    session.turn_end(agent.id).unwrap();
    assert!(session.current_turn().unwrap().is_none());
    assert!(session
        .surface()
        .unwrap()
        .iter()
        .any(|e| matches!(e.body, EventBody::AgentMessage { .. })));
}

fn grok_chat_completion(api_key: &str, user_text: &str) -> Result<String, String> {
    let model = std::env::var("XAI_MODEL")
        .or_else(|_| std::env::var("GROK_MODEL"))
        .unwrap_or_else(|_| "grok-3-mini".into());
    if !model
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
    {
        return Err("invalid XAI_MODEL / GROK_MODEL".into());
    }
    let user_json = json_escape(user_text);
    let body = format!(
        r#"{{"model":"{model}","messages":[{{"role":"user","content":"{user_json}"}}],"max_tokens":32,"stream":false}}"#
    );
    let output = std::process::Command::new("curl")
        .args([
            "-sS",
            "-m",
            "90",
            "-H",
            &format!("Authorization: Bearer {api_key}"),
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
            "https://api.x.ai/v1/chat/completions",
        ])
        .output()
        .map_err(|e| format!("curl: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "curl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let raw = String::from_utf8_lossy(&output.stdout);
    json_string_after(&raw, "choices", "content")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("unexpected xAI response: {raw}"))
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

/// First JSON string named `field` that appears after `after`.
fn json_string_after(raw: &str, after: &str, field: &str) -> Option<String> {
    let start = raw.find(&format!("\"{after}\""))?;
    let rest = &raw[start..];
    let key = format!("\"{field}\":");
    let rest = rest.split(&key).nth(1)?.trim_start();
    let rest = rest.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next()? {
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                other => out.push(other),
            },
            '"' => return Some(out),
            other => out.push(other),
        }
    }
    None
}
