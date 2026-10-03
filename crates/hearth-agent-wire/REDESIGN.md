# Redesign: Transport vs Codec (contrib-friendly)

**Date:** 2026-09-26 (Asia/Shanghai)  
**Scope:** `crates/hearth-agent-wire` (mirrored from the Agent Research redesign toy)  
**Not in scope:** SoftExpiring, Flush-before-dispatch, DualGate, AdmitCommit

## Cheng feedback (quoted intent)

1. Design the lib so developers love contributing more agent-type integrations.
2. Avoid stdio parsing — not error-resistant.

## Design rules

| Rule | Detail |
|------|--------|
| Split **Transport** vs **Codec/Adapter** | Transport frames bytes ↔ `WireFrame`. Codec maps `WireFrame` ↔ `AgentEvent` / `AgentCommand`. |
| Ban unstructured scrape | No NDJSON-of-stdout, no regex-of-logs, no generic `map_wire_line(kind, &str)` that strips `data:` and parses “a line”. |
| Allowed wires | (1) Content-Length or length-prefixed JSON-RPC (LSP/ACP), (2) HTTP+SSE typed EventSource, (3) WebSocket JSON, (4) JSONL RPC (`jsonl-rpc`, newline frames + `rpc_chunk`) — each a `Transport` impl. |
| Contrib shape | `adapters/<name>/{mod.rs,README.md,fixtures/*.json}` + `map_*` tests + `registry()` row + `CAPABILITIES.md` column. |
| Host surface | Prefer `FramedAgent<T, C>`; LoopbackAgent stays for Host unit tests. |

### What “already-decoded” means

```text
process/socket bytes
    → Transport::try_decode_*  (Content-Length / EventSource / WS / JSONL+rpc_chunk)
    → WireFrame::Json | WireFrame::Sse { event, data }
    → AdapterCodec::decode_event
    → AgentEvent
```

Adapters **never** see raw stdout buffers or human log lines.

## PR #5 implications for Codin (`wolvever/hearth` #5)

PR title: *Add hearth-agent-bus and thin Host attach*. HostAttach under
Place/Session/Binding is the right composition class (same as PlaceMemory /
FakeSandbox). The problem is the **wire helper**, not HostAttach itself.

### What PR #5 does today (problem quotes)

`adapters::map_wire_line` treats “one stdio JSON line **or** OpenCode SSE
`data:` line” as the same string API:

```rust
/// One stdio JSON line or OpenCode SSE `data:` line → optional AgentEvent.
pub fn map_wire_line(kind: AgentKind, line: &str) -> Result<Option<AgentEvent>, BusError> {
    let line = line.trim();
    // ...
    let payload = if let Some(rest) = line.strip_prefix("data:") {
        let rest = rest.trim();
        if rest.is_empty() || rest == "[DONE]" { return Ok(None); }
        rest
    } else {
        line
    };
    let value: Value = serde_json::from_str(payload)?;
    map_native(kind, &value).map(Some)
}
```

`WireAgent::push_line` feeds that helper:

```rust
/// Stdio / SSE attach helper: push native JSON (or `data:`) lines, recv events.
pub fn push_line(&mut self, line: &str) -> AttachResult<()> {
    if let Some(ev) = adapters::map_wire_line(self.kind, line)? {
        self.inbound.push(ev);
    }
    Ok(())
}
```

Tests encode the anti-pattern: `wire_line_maps_stdio_json_and_sse`,
`wire_agent_stdio_line_appends_tool_call` with a raw stdout-shaped string.

**Why Cheng rejected this:** mixed stdio NDJSON + SSE prefix stripping is
brittle (partial lines, banner text, log noise, multi-line JSON, missing
Content-Length). It also teaches contributors the wrong extension point.

### What to change in HostAttach / wire (exact guidance)

Keep:

- `HostAttach<A: CodingAgent>` — bind / attach / resume / drain / compact_with_handoff
- Binding id stability; EventLog mapping; no seventh noun; no Queue
- LoopbackAgent for Host tests
- Pure `map_notification` / `map_event` mappers

Delete / replace:

| Remove | Replace with |
|--------|----------------|
| `adapters::map_wire_line` | `Transport` decode + `AdapterCodec::decode_event` / `adapters::decode_frame` |
| `WireAgent::push_line` | `JsonRpcTransport::push_decoded` / `try_decode_content_length`, or `SseTransport::push_frame` / `parse_event_block` |
| `WireAgent` as NDJSON/SSE combo | `FramedAgent<JsonRpcTransport, GrokBuildCodec>` (ACP/Codex) or `FramedAgent<SseTransport, OpenCodeCodec>` |
| Tests that pass `"data: {...}"` strings into a shared line helper | Fixture JSON → codec; SSE tests use `SseFrame` / `parse_event_block` |

HostAttach API change is small: still `HostAttach::attach(binding, agent_id, coding)` where `coding` is any `CodingAgent` — preferably `FramedAgent<_, _>`. No need to remint Binding or change drain/EventLog mapping.

For live stdio ACP later: read bytes into a buffer, call
`JsonRpcTransport::try_decode_content_length`, then `push_decoded` / codec.
Do **not** `BufRead::lines()` as the event stream (headers are CRLF; bodies
may contain newlines inside JSON strings when using Content-Length).

### Suggested PR #5 follow-up commit message

`wire: split Transport/Codec; drop map_wire_line stdio scrape`

## How a new agent lands in &lt;1 afternoon

See `CONTRIBUTING.md`. Short path:

1. `cp -r src/adapters/grok_build src/adapters/my_agent` (or `opencode` if SSE).
2. Capture 2 fixtures → `fixtures/*.json`.
3. Implement `map_*` + `AdapterCodec`.
4. Add `registry()` row + `CAPABILITIES.md` column + README.
5. `cargo test`.

Discovery: `registry()` / `lookup(kind)` / `CAPABILITIES.md`.

## Toy crate status after this redesign

- Transports: `JsonRpcTransport`, `SseTransport`, `WebSocketJsonTransport`, `JsonlRpcTransport`
- Codecs: `GrokBuildCodec`, `CodexCodec`, `PiCodec`, `OpenCodeCodec`
- `FramedAgent<T, C>: CodingAgent`
- Docs: `CONTRIBUTING.md`, `CAPABILITIES.md`, this file; `BRIEF.md` updated
- SoftExpiring / flush: untouched (parked)
