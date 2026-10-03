# pi adapter

- **Source:** Pi RPC dialect (Paseo `rpc-types` / `pi --mode rpc`) plus the
  earendil-works/pi durable harness snapshot + watch events
- **Wire:** `jsonl-rpc` via [`JsonlRpcTransport`](../../transport/jsonl.rs)
  (newline = frame delimiter, optional `rpc_chunk` reassembly)
- **Codec:** `PiCodec` — `map_event` on **typed** JSON frames after the
  transport has framed them

The same objects can arrive over WebSocket JSON; the registered wire is
`jsonl-rpc`. Do **not** scrape stdout / NDJSON logs. Banner text is a
transport problem, not an `AgentEvent`.

Snapshot + watch; events are not replayed on reconnect (native semantics).

## Caps

[`CapabilityFlags::PI`](../../capabilities.rs): session · message · tool ·
thinking · compact · subagent. Encode for `SetMode` / MCP / `Revert*` /
`Compact` stays `Unsupported` (flags for those reserved commands stay false).

## Fixtures

| File | Dialect | Maps to |
|------|---------|---------|
| `fixtures/run_start.json` | harness | `TurnStarted` |
| `fixtures/tool_start.json` | harness | `ToolCall` |
| `fixtures/turn_start.json` | RPC | `TurnStarted` |
| `fixtures/tool_execution_start.json` | RPC | `ToolCall` |
| `fixtures/message_update.json` | RPC | `MessageDelta` |

```bash
cargo test -p hearth-agent-wire pi::
```
