# acp adapter (generic ACP)

- **Source:** Agent Client Protocol — shared `session/update` + `session/request_permission` map
- **Wire:** `jsonrpc-content-length` via `JsonRpcTransport`
- **Codec:** `AcpCodec` — generic / catalog profile (`AgentKind::Acp`)
- **Profile:** `adapters/grok_build` calls `acp::map_*` and keeps `GrokBuildCodec` / `AgentKind::GrokBuild`

Catalog agents (Cursor, Copilot, …) should start here. Per-binary launch argv
is a later catalog slice — this folder is map + codec only.

## Caps defaults

[`CapabilityFlags::ACP`](../../capabilities.rs): session · message · tool ·
thinking · plan · permission.

Compact encode is a stub (`compact: false`); question / subagent / Paseo
reserved flags stay unset until a profile or later slice overrides. See
`CAPABILITIES.md`.

## Rules

Adapters map **typed frames** only. Do not scrape stdout lines or strip `data:` here.
Use `transport::JsonRpcTransport::try_decode_content_length` at the I/O boundary.

## Fixtures

| File | Maps to |
|------|---------|
| `fixtures/session_update_tool_call.json` | `AgentEvent::ToolCall` |
| `fixtures/permission_request.json` | `AgentEvent::PermissionAsk` |

```bash
cargo test -p hearth-agent-wire acp::
```
