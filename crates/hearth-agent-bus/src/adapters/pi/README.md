# pi adapter

- **Source:** `earendil-works/pi` durable harness
- **Wire:** WebSocket JSON (or length-prefixed JSON) via `WebSocketJsonTransport`
- **Codec:** `PiCodec` — `map_event` on decoded JSON objects

Snapshot + watch; events are not replayed on reconnect (native semantics).

```bash
cargo test -p hearth_agent_bus pi::
```
