# Pi adapter

[earendil-works/pi](https://github.com/earendil-works/pi) durable harness events. Snapshot + watch; events are not replayed on reconnect.

| Layer | Type |
| --- | --- |
| Transport | `WsJsonFramer` (one watch message = one JSON value) |
| Adapter | `map_event(&Value) -> AgentEvent` |

## Mapped types

- `run.start` / `run.started` → `TurnStarted`
- `run.end` / `run.ended` → `TurnCompleted`
- `message` / `entry.message` → `Message`
- `tool.start` / `tool.end` → `ToolCall` / `ToolResult`
- `lane.start` → `SubagentStarted`

## Fixtures

- `fixtures/run_start.json` — `run.start` → `TurnStarted`
