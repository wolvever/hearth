# grok_build adapter

- **Source:** `xai-org/grok-build` (`grok agent stdio` / `serve`)
- **Wire:** Content-Length JSON-RPC (ACP) via `JsonRpcTransport`
- **Codec:** `GrokBuildCodec` — `map_notification` delegates to `acp::map_*` and keeps `AgentKind::GrokBuild`

## Rules

Adapters map **typed frames** only. Do not scrape stdout lines or strip `data:` here.
Use `transport::JsonRpcTransport::try_decode_content_length` at the I/O boundary.

## Fixtures

| File | Maps to |
|------|---------|
| `fixtures/session_update_tool_call.json` | `AgentEvent::ToolCall` |
| `fixtures/permission_request.json` | `AgentEvent::PermissionAsk` |

```bash
cargo test -p hearth-agent-wire grok_build::
```
