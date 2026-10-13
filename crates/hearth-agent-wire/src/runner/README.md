# runner — AttachRunner

Host-adjacent spawn / pump / remint. **Not a kernel noun.**

| Piece | Role |
|-------|------|
| `AttachRunner` | Spawn `LaunchSpec` child; pump framed bytes into a Transport |
| `transport_for_wire` | ACP `jsonl-rpc` → `JsonlRpcTransport::without_rpc_chunks()` |
| `remint::RemintSession` | Binding remint policy (09-27…10-02 + 10-06 fail-closed) |
| `remint::LiveCaps` | Gate resume on live `initialize` caps |
| `AttachRunner::initialize_host` | Live attach: send ACP `initialize` with `ClientCapabilities::HOST` (all false), wait for result |
| `AttachRunner::flush_and_teardown_after_remint` | Runner adapter: `flush_old_wire_replies` then `write_old_wire_teardown` on the OLD wire |
| `AttachRunner::attach_resume_on_new_wire` | After remint: `initialize_host` on NEW wire → `set_live_caps` → `session/resume` (fail-closed if not advertised) |

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

## Live attach initialize + remint flush (2026-10-12)

Live AttachRunner must send ACP `initialize` with `ClientCapabilities::HOST`
(all false — no stub fs/terminal) and wait for the result before any
agent-bound RPC (`session/new`, prompt, …). Remint writes
`old_wire_replies` via `flush_old_wire_replies` **before**
`write_old_wire_teardown` (SessionClose / SessionCancel) on the old wire;
a dead old wire drops replies and never blocks remint or retries on the
new Binding. `flush_and_teardown_old_wire` and
`AttachRunner::flush_and_teardown_after_remint` share those two helpers.

## New-wire AttachResume after remint (2026-10-14)

When `flush_and_teardown_after_remint` returns `needs_attach_resume`, the
production path lands AttachResume on the **NEW** Binding/wire via
`AttachRunner::attach_resume_on_new_wire` / `write_new_wire_attach_resume`:

1. `initialize_host` on the NEW wire (`ClientCapabilities::HOST` all false)
2. `RemintSession::set_live_caps` from that live result
3. If LiveCaps advertise resume → write ACP `session/resume` with the **same**
   agent session id; else fail-closed (`ResumeNotSupported` /
   `LoadFallbackBlocked`) — never silent skip, never `session/load` /
   `session/new`.

AttachResume is never written on the old wire (already torn down).

Init gate: until `initialize_host` returns Ok, every agent-bound write
(`write_command`, `write_value`, `write_jsonl`, `write_jsonrpc`,
`write_bytes`) fails with `BusError::NotInitialized`. Agent->client
requests that arrive while waiting for the initialize reply are answered
`-32601`. Old-wire SessionCancel / SessionClose write failures are reported
in `OldWireFlush::teardown_failed` (never retried). The wait is bounded by
`WAIT_RPC_RESULT_TIMEOUT` (~51s).

Test fake: exhausting the fake child's stdout no longer kills it on read
(EOF is not death) — only `try_wait` / `kill` mark it exited, so live
attach can still write after `initialize` returns.

## Out of scope

SoftExpiring, Flush-before-dispatch, Stage, Evidence, EffectId, Queue,
seventh noun, `map_wire_line`, AgentBridge queued-run, Gemini `session/load`.

```bash
cargo test -p hearth-agent-wire runner::
```
