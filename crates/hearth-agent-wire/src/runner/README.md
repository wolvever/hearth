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

## Permission asks across remint (2026-10-08)

`RemintSession` keeps live asks in a map keyed by `(Binding, JSON-RPC id)`
(no single slot — a second in-flight ask never overwrites the first).
`resolve_permission(binding, rpc_id, option_id)` returns the
`ReplyPermission` to write on that same id; an `optionId` the agent did not
offer → `OptionNotOffered`. `cancel_turn` answers every live ask
`cancelled`, then `session/cancel`; the turn stays in flight until
`turn_ended(turn)` (the prompt response), so a remint after a lost cancel
still runs Cancel-before-reattach, and a late response for an older turn
never clears a newer one.

On remint the old Binding's asks move to *resurfaced* (still healthy HITL —
no cancel / finalize, also on a second remint). A dead Binding's rpc id is
never answered (`DeadBinding`; the resumed process restarts ids). A re-ask
from the resumed agent adopts a resurfaced ask only when its `toolCallId`
is unique; shared ids (copilot-cli #989) are never routed by `toolCallId`.
Host `PermissionAsked` / `PermissionDecided` carry a typed
`PermissionRpc { binding, rpc_id, tool_call_id }`.

## Out of scope

SoftExpiring, Flush-before-dispatch, Stage, Evidence, EffectId, Queue,
seventh noun, `map_wire_line`, AgentBridge queued-run, Gemini `session/load`.

```bash
cargo test -p hearth-agent-wire runner::
```
