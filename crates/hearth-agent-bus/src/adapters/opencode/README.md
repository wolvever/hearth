# opencode adapter

- **Source:** `anomalyco/opencode` EventV2 bus
- **Wire:** HTTP + SSE via `SseTransport` (typed EventSource frames)
- **Codec:** `OpenCodeCodec` — `map_event` on the JSON `data` payload

Do **not** call a generic "stdio line" helper that strips `data:`. Feed
`SseFrame { event, data }` from the SSE transport instead.

```bash
cargo test -p hearth_agent_bus opencode::
```
