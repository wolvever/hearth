# Grok Build adapter

ACP JSON-RPC from [xai-org/grok-build](https://github.com/xai-org/grok-build) (`grok agent stdio` / `serve`).

| Layer | Type |
| --- | --- |
| Transport | `JsonRpcFramer` (`Content-Length` JSON-RPC, same as LSP) |
| Adapter | `map_notification(&Value) -> AgentEvent` |

Do not feed NDJSON lines. Encode with `transport::encode_jsonrpc` or a real ACP stdio stream.

## Mapped methods

- `session/update` → `MessageDelta` / `ToolCall` / `Plan` / `ThinkingDelta` / `Native`
- `session/request_permission` → `PermissionAsk`
- anything else → `Native`

## Fixtures

- `fixtures/session_update_tool_call.json` — `tool_call_update` → `ToolCall`

## Adding coverage

Drop a captured ACP notification JSON in `fixtures/` and assert `map_notification`.
