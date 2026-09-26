# Adapter capabilities matrix

Wires: `jsonrpc-content-length` | `http-sse` | `websocket-json`

| Agent | Folder | Wire | session | message | tool | thinking | plan | permission | question | compact | subagent/task |
|-------|--------|------|:-------:|:-------:|:----:|:--------:|:----:|:----------:|:--------:|:-------:|:-------------:|
| Grok Build | `adapters/grok_build` | jsonrpc-content-length | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | | stub | |
| Codex | `adapters/codex` | jsonrpc-content-length | ✓ | ✓ | ✓ | ✓ | | stub | | ✓ | |
| Pi | `adapters/pi` | websocket-json | ✓ | ✓ | ✓ | | | | | | ✓ |
| OpenCode | `adapters/opencode` | http-sse | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ | ✓ | |

Discovery API: `hearth_agent_bus::registry()` / `lookup(AgentKind)`.

Update this table in the same PR that adds `adapters/<name>/`.
