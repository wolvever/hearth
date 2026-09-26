# Adapter capabilities matrix

Wires: `jsonrpc-content-length` | `http-sse` | `websocket-json`

Flags are typed [`CapabilityFlags`](src/capabilities.rs) on each `registry()`
[`AdapterInfo`](src/adapters/mod.rs). Host / catalog read the struct; this file
is the human matrix.

| Cell | Meaning |
|------|---------|
| `✓` | flag is `true` — event or command is mapped |
| empty | flag is `false` |
| `stub` | encode exists or is reserved; **flag stays `false`** until a later slice |

`cargo test -p hearth-agent-wire capabilities::` fails if a registry row
drifts from these tables (or if a `CapabilityFlags` field has no column).

## Landed (event / command map)

| Agent | Folder | Wire | session | message | tool | thinking | plan | permission | question | compact | subagent/task |
|-------|--------|------|:-------:|:-------:|:----:|:--------:|:----:|:----------:|:--------:|:-------:|:-------------:|
| ACP (generic) | adapters/acp | jsonrpc-content-length | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | | stub | |
| Grok Build | adapters/grok_build | jsonrpc-content-length | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | | stub | |
| Codex | adapters/codex | jsonrpc-content-length | ✓ | ✓ | ✓ | ✓ | | stub | | ✓ | |
| Pi | adapters/pi | websocket-json | ✓ | ✓ | ✓ | | | | | | ✓ |
| OpenCode | adapters/opencode | http-sse | ✓ | ✓ | ✓ | ✓ | | ✓ | ✓ | ✓ | |

Column aliases: `tool` → `tool_invocations`, `thinking` → `reasoning_stream`,
`subagent/task` → `subagent`. `session` / `message` are wire event-map flags,
not Paseo session persistence / token streaming.

## Reserved (later slices — all false until they land)

`SetMode` / `SetFeature` / `ConfigureMcp` / `Revert*` and `ModeChanged` /
`Rewound` exist as append-only stubs. Encoders return `Unsupported`; these
flags stay `false` until a later encode-depth slice. Catalog and session
listing remain later. Columns exist so the matrix test stays closed when
those flags flip.

| Agent | Folder | streaming | session_persistence | session_listing | dynamic_modes | mcp_servers | rewind_conversation | rewind_files | rewind_both |
|-------|--------|:---------:|:-------------------:|:---------------:|:-------------:|:-----------:|:-------------------:|:------------:|:-----------:|
| ACP (generic) | adapters/acp | | | | | | | | |
| Grok Build | adapters/grok_build | | | | | | | | |
| Codex | adapters/codex | | | | | | | | |
| Pi | adapters/pi | | | | | | | | |
| OpenCode | adapters/opencode | | | | | | | | |

Discovery API: `hearth_agent_wire::registry()` / `lookup(AgentKind)` /
`AdapterInfo.flags`.

Update both tables in the same PR that adds `adapters/<name>/`.
