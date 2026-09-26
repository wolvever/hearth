# codex adapter

- **Source:** `openai/codex` App Server
- **Wire:** Content-Length JSON-RPC stdio via `JsonRpcTransport`
- **Codec:** `CodexCodec` — `map_notification` on decoded JSON-RPC notifications

## Fixtures

| File | Maps to |
|------|---------|
| `fixtures/item_reasoning.json` | `AgentEvent::Thinking` |
| `fixtures/turn_started.json` | `AgentEvent::TurnStarted` |

```bash
cargo test -p hearth-agent-wire codex::
```
