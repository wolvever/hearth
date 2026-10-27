# acp adapter (generic ACP)

- **Source:** Agent Client Protocol — shared `session/update` + `session/request_permission` map
- **Wire:** `jsonl-rpc` (newline-delimited JSON-RPC). ACP stdio is not Content-Length. Do not enable Pi `rpc_chunk` reassembly (`JsonlRpcTransport::without_rpc_chunks`).
- **Codec:** `AcpCodec` — generic / catalog profile (`AgentKind::Acp`)
- **Profile:** `adapters/grok_build` calls `acp::map_*` and keeps `GrokBuildCodec` / `AgentKind::GrokBuild`

Catalog agents (Cursor, Copilot) start here and share `AcpCodec`. Launch
argv is data in `src/catalog/builtin.toml` (`profile` → `LaunchSpec`), not a
second codec and not a process spawn.

## Caps defaults

[`CapabilityFlags::ACP`](../../capabilities.rs): session · message · tool ·
thinking · plan · permission.

Compact encode is a stub (`compact: false`); question / subagent / Paseo
reserved flags stay unset until a profile or later slice overrides. See
`CAPABILITIES.md`.

## Rules

Adapters map **typed frames** only. Do not scrape stdout lines or strip `data:` here.
Use `JsonlRpcTransport::without_rpc_chunks` at the I/O boundary (one line, one JSON value). Do not frame ACP stdio with Content-Length.

## Permission asks are JSON-RPC requests

`session/request_permission` carries a JSON-RPC `id` and the agent blocks
until the client answers **that id** (ACP v1 tool-calls):

- decode keeps the id as `PermissionAsk::rpc_id` (`permission_id` is its JSON
  text; ACP params have no `id`). `toolCallId` is read from v1 `/toolCall`
  or v2 `/subject/toolCall`. Missing id / sessionId / toolCallId / options,
  or a bad / duplicate `optionId`, is a typed `BusError::Decode`.
- `ReplyPermission { rpc_id, option_id }` encodes a JSON-RPC **response**:
  `result.outcome = {outcome:"selected", optionId}` or `{outcome:"cancelled"}`
  when `option_id` is `None`. No `rpc_id` → `BusError::Encode`. There is no
  `session/request_permission/result` notification.
- `Abort` encodes the `session/cancel` notification; answer every pending
  ask `cancelled` first (`HostAttach::cancel`, `RemintSession::cancel_turn`).
- Route answers by `(Binding, rpc id)`, never by `toolCallId` (copilot-cli
  #989 reuses `"shell-permission"`) and never by a single "expected" id
  (claude-agent-acp #851).

## Agent→client requests (fs / terminal)

ACP agents must check `clientCapabilities` from `initialize` before calling
`fs/read_text_file`, `fs/write_text_file` or `terminal/*`; the client owns
the reply. Earlier these requests decoded as `AgentEvent::Native` and got
**no reply**, so an agent could hang mid-turn. Now:

- `ClientCapabilities::HOST` is what Host truly implements: **nothing** today,
  so `initialize` advertises `fs.readTextFile: false`, `fs.writeTextFile:
  false`, `terminal: false` (`encode_initialize`). No stub handlers. The same
  struct is the only gate for routing below.
- An agent→client request (has `id` + `method`) for a method not advertised,
  or an unknown method, gets an immediate JSON-RPC error on the same id:
  `-32601` method not found (`AgentCommand::ReplyError`). Notifications (no
  id) and responses get no reply. Handled requests
  (`HANDLED_AGENT_REQUESTS`: `session/request_permission`) never fall
  through to `-32601` — they still go to the pending-ask map.
- Requests for advertised methods are tracked in flight by `(Binding, rpc
  id)`. On session cancel each gets exactly one `-32800` before
  `session/cancel`; on Binding remint / teardown each gets one `-32800`
  for the OLD wire before teardown (`old_wire_replies`,
  `flush_old_wire_replies`). If the old wire is already dead the reply is
  dropped — never retried on the new Binding, never answered twice, and
  remint / teardown still succeed.

## Fixtures

| File | Maps to |
|------|---------|
| `fixtures/session_update_tool_call.json` | `AgentEvent::ToolCall` |
| `fixtures/permission_request.json` | `AgentEvent::PermissionAsk` (v1 `/toolCall`, numeric id) |
| `fixtures/permission_request_v2.json` | `AgentEvent::PermissionAsk` (v2 `/subject/toolCall`, string id) |

```bash
cargo test -p hearth-agent-wire acp::
```

## Subagent nesting (Claude Code `_meta`)

`tool_call` / `tool_call_update` / `agent_message_chunk` read
`update._meta.claudeCode.parentToolUseId` into `parent` and (tool calls only)
`update._meta.claudeCode.subagent == true` into `subagent`. Missing or
malformed meta decodes as `None` / `false`; no SubagentStarted and no child
session are emitted. Host maps `parent` to `parent_tool_call_id` on the
EventLog. See `crates/hearth/DESIGN.md` → **ACP parent**.

`session/request_permission` reads `toolCall._meta.claudeCode.parentToolUseId`
(v1 `/toolCall`, v2 `/subject/toolCall`) into `PermissionAsk.parent`, same
fail-closed rule; Host maps it to `PermissionAsked.parent_tool_call_id`.
An ask without it stays `None` even if a later update names a parent
(claude-agent-acp #708; DESIGN.md → **Permission parent**).

