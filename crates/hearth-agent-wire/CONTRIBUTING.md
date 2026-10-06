# Contributing an agent adapter

Goal: land a new agent type in **under one afternoon**, with fixtures and tests,
without touching Host/Session/Binding or scraping logs.

## Cheng rules (non-negotiable)

1. **Transport ≠ Codec.** Framing lives in `src/transport/`. Semantics live in
   `src/adapters/<name>/`. Adapters map typed protocol frames ↔
   `AgentEvent` / `AgentCommand` only.
2. **No stdio line scraping.** Do not split stdout/stderr on newlines and hope
   each line is JSON. Do not regex human CLI logs. Do not add a
   `map_wire_line(kind, line: &str)` that strips `data:` and parses NDJSON.
3. **Allowed wires only** (each behind `Transport` / [`WireKind`](src/transport/mod.rs)):
   - Length-prefixed or **Content-Length JSON-RPC** (LSP / ACP)
   - **HTTP + SSE** with typed EventSource frames (`event` / `data` / `id`)
   - **WebSocket JSON** (one JSON value per WS message)
   - **JSONL RPC** (`jsonl-rpc`) — newline is the frame delimiter, with
     optional `rpc_chunk` reassembly. Not NDJSON-of-logs.
4. SoftExpiring / Flush-before-dispatch stay **parked** — do not land them here.

## Afternoon checklist

```text
src/adapters/acp/     # generic ACP map — base for catalog agents
src/adapters/<name>/
  mod.rs              # map_* + AdapterCodec impl
  README.md           # wire, capabilities, fixture table
  fixtures/*.json     # captured native frames
```

1. **Pick a wire** from the allowed list. Reuse `JsonRpcTransport`,
   `SseTransport`, `WebSocketJsonTransport`, or `JsonlRpcTransport`. Add a
   new transport only if none fit — still framed, never log-scraping.
2. **Copy a sibling folder.** Start from `adapters/acp` for catalog / generic
   ACP agents. Copilot and Cursor are rows in `src/catalog/builtin.toml`
   (`id`, `label`, `command`, `extends = "acp"`) that resolve to `LaunchSpec`
   and share `AcpCodec` — do not fork a codec. Use `grok_build` for a
   Grok-specific ACP profile, `opencode` for SSE, `pi` for JSONL RPC (Paseo
   `rpc-types` + harness snapshot/watch).
3. **Drop 2–3 fixtures** under `fixtures/` from a real session capture
   (already-decoded JSON objects, not raw process dumps).
4. **Implement** `map_*` / `map_notification` and `AdapterCodec`
   (`decode_event`, `encode_command` stubs for the commands you support).
5. **Register** a row in `adapters::registry()` with typed
   `CapabilityFlags` (via `CapabilityFlags::for_agent` / a new associated
   const) and matching cells in `CAPABILITIES.md`. The
   `capabilities_matrix_matches_registry` test fails if they drift.
6. **Tests:** `map_*` tests that `include`/`read` fixtures; optionally a
   `FramedAgent<YourTransport, YourCodec>` round-trip.
7. `cargo test` — keep green.

## What HostAttach should call

```rust
let transport = JsonRpcTransport::new(); // or Sse / WebSocket / JsonlRpcTransport
// I/O boundary: Content-Length / EventSource / WS message / JSONL frames → Value
transport.push_decoded(already_framed_value);
// JSONL: transport.push_bytes(stdout_chunk) — newline frames + rpc_chunk assembly

let mut agent = FramedAgent::new(transport, GrokBuildCodec);
while let Some(ev) = agent.try_recv()? { /* EventLog */ }
agent.send(AgentCommand::UserMessage { .. })?;
```

Never: `wire.push_line(stdout_line)` / `map_wire_line`. Banner text is a
transport decode problem, not an event.

## Review bar

- Fixture-backed `map_*` tests
- No new NDJSON / `data:` strip helpers in adapters
- Registry + CAPABILITIES.md updated
- README in the adapter folder
