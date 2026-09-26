# Codex adapter

OpenAI Codex [app-server](https://github.com/openai/codex) JSON-RPC (`thread/*`, `turn/*`, `item/*`, approvals, compact).

| Layer | Type |
| --- | --- |
| Transport | `JsonRpcFramer` (`Content-Length` JSON-RPC) |
| Adapter | `map_notification(&Value) -> AgentEvent` |

## Mapped methods

- `thread/started` → `SessionStarted`
- `turn/started` / `turn/completed` → `TurnStarted` / `TurnCompleted`
- `item/started` / `item/completed` → `Message` / `Thinking` / `ToolCall` / `ToolResult`
- `item/agentMessage/delta` → `MessageDelta`
- reasoning text deltas → `ThinkingDelta`
- compact methods → `CompactStarted` / `Compacted`

## Fixtures

- `fixtures/item_reasoning.json` — `item/completed` reasoning → `Thinking`
