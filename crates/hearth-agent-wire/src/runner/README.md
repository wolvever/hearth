# runner — AttachRunner

Host-adjacent spawn / pump / remint. **Not a kernel noun.**

| Piece | Role |
|-------|------|
| `AttachRunner` | Spawn `LaunchSpec` child; pump framed bytes into a Transport |
| `transport_for_wire` | ACP `jsonl-rpc` → `JsonlRpcTransport::without_rpc_chunks()` |
| `remint::RemintSession` | Binding remint policy (09-27…10-02 + 10-06 fail-closed) |
| `remint::LiveCaps` | Gate resume on live `initialize` caps |

## Remint (fail-closed)

Orphan tool calls are finalized on the **Host EventLog** as
`EventBody::ToolCallInterrupted { tool_call_id, status: Indeterminate }`
(keyed by `tool_call_id`, idempotent across a second remint; late
`ToolResult` after interrupt is dropped). Not cancelled — fate is unknown.

On an existing Session, remint outcomes are only:

1. `session/resume` with the **same** agent session id, or
2. a typed fail-closed Event (`ResumeNotSupported`, `LoadFallbackBlocked`,
   `SilentForkBlocked`, `CancelNotSupported`, `PromptResubmitBlocked`).

Never `session/new` under that Session. Never `session/load` as fallback
(Copilot load-only → fail closed). Never resubmit an in-flight prompt.

## Out of scope

SoftExpiring, Flush-before-dispatch, Stage, Evidence, EffectId, Queue,
seventh noun, `map_wire_line`, AgentBridge queued-run, Gemini `session/load`.

```bash
cargo test -p hearth-agent-wire runner::
```
