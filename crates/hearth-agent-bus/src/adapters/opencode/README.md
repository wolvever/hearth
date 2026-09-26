# OpenCode adapter

[anomalyco/opencode](https://github.com/anomalyco/opencode) EventV2 over SSE.

| Layer | Type |
| --- | --- |
| Transport | `SseFramer` (WHATWG EventSource: blank-line dispatch, joined `data:` lines) |
| Adapter | `map_event(&Value) -> AgentEvent` |

Do not strip a single `data:` prefix and parse. Feed the byte stream to `SseFramer`. `data: [DONE]` is a stream terminator, not JSON.

## Mapped types

- `session.created` / `session.status` / `session.error`
- `message.part.updated` / `message.part.delta` → thinking / tool / message / compact
- `permission.asked` → `PermissionAsk`
- `question.asked` → `QuestionAsk`

## Fixtures

- `fixtures/permission_asked.json` — `permission.asked` → `PermissionAsk`
