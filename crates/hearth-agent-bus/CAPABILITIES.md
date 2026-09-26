# hearth-agent-bus capabilities

Unified `AgentEvent` / `AgentCommand` for external coding agents. **Not a seventh Hearth noun.** Host↔Goose stays the ypipe child.

## Transport vs Adapter

```
bytes  --Framer-->  Frame { value: serde_json::Value }
Frame / Value  --map_*-->  AgentEvent
AgentCommand  --HostAttach.send-->  outbound
```

Codec = adapter `map_*` (`Value`/`Frame` → `AgentEvent`). Transport never maps events.

| Transport | When | Fail closed |
| --- | --- | --- |
| `JsonRpcFramer` | ACP / LSP / Codex app-server stdio | Missing `Content-Length`, NDJSON, invalid JSON, leftover bytes on `finish` |
| `SseFramer` | OpenCode EventSource | Dispatch only on a blank line; non-JSON `data` (except `[DONE]`); leftover event on `finish` |
| `WsJsonFramer` | Pi watch / one WS text message | One message = one JSON value; trailing tokens |

There is **no** public `map_wire_line` / `push_line`. Do not split on newline and hope.

`FramedAgent::push_frame(Value)` is the decoded path. `FramedAgent::push_bytes(&mut Framer, &[u8])` is the byte path.

## Shipped adapters

| Agent | `AgentKind` | Binding kind | Transport | Mapper | Folder |
| --- | --- | --- | --- | --- | --- |
| Grok Build | `GrokBuild` | `grok_build` | JSON-RPC | `map_notification` | `src/adapters/grok_build/` |
| Codex | `Codex` | `codex` | JSON-RPC | `map_notification` | `src/adapters/codex/` |
| Pi | `Pi` | `pi` | WS JSON | `map_event` | `src/adapters/pi/` |
| OpenCode | `OpenCode` | `opencode` | SSE | `map_event` | `src/adapters/opencode/` |

Wanted next (contrib): Claude Code, Cursor, Aider, … — add a folder, not a kernel noun.

## Mapped surface (all adapters)

**Outbound** `AgentCommand`: CreateProject, OpenSession, CloseSession, UserMessage, Steer, Abort, ReplyPermission, ReplyQuestion, Compact, SpawnTask, CancelTask.

**Inbound** `AgentEvent`: session/project, message + delta, thinking + delta, tool call/result, plan, permission/question, task/subagent, compact start/done, status, error, `Native`.

Unmapped native payloads stay `AgentEvent::Native` (forward-compat). Do not drop them.

## Host composition (unchanged)

- `HostAttach` under Place / Session / Binding. Attach/resume **do not remint** `Binding.id`.
- `compact_with_handoff` = PreCompactHandoff then `AgentCommand::Compact`.
- SoftExpiring / DualGate / AdmitCommit / Flush-before-dispatch stay **parked**.

## Add an adapter in an afternoon

See [CONTRIBUTING.md](CONTRIBUTING.md). Shape:

```
src/adapters/<name>/
  README.md
  fixtures/*.json
  mod.rs          # map_notification or map_event
```
