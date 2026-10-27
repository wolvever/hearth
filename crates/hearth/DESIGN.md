# hearth

Session-first kernel. Six product names: **User**, **Agent**, **Session**, **Binding**, **Event**, **Place**. `Store` / `InMemory` persist them; `Provisioner` records a `Binding` without spawning a CLI; `FakeSandbox` is a host-side file map keyed by `sandbox_id`; `PlaceMemory` is durable memory files under a Place **claim**. None of those is a seventh concept. `Runtime` keeps Sessions so an Agent can wake on user queries, timers, and triggers — same class as Store / Provisioner / Host / FakeSandbox / PlaceMemory, not a seventh name. `hearth-agent-wire` is an **adapter layer** (unified `AgentEvent` / `AgentCommand` for Grok Build, Codex, Pi, OpenCode) plus a thin `HostAttach` under Place / Session / Binding — not a seventh noun and not a Queue.

## Six concepts

1. **User** — `id`, `name`, owned `config`. Occupies a session as `Member::User`.
2. **Agent** — long-lived `id`, `name`, `instructions`, owned `config`. Not a process, run, or worktree.
3. **Session** — joinable room: membership + one append-only `Event` log + current bindings + a map of `Place`s keyed by `PlaceId`. Exists with zero bindings and zero places. `unbind` never deletes the room, the log, or any place.
4. **Binding** — disposable runtime: `id`, `kind` (string; `HostKind` is only a helper, not a type), optional `native_resume_id`, optional `sandbox_id`, optional `agent`. Native resume tokens live here, never on `Session.id`. `agent` names whose runtime this is so `leave` can unbind only that agent's Bindings.
5. **Event** — `{ id, seq?, turn?, ts, body }`. `Event.id` (`EventId`) is identity. `seq` is an optional last-read cursor, allocated only by `mark_read`, never on append. `turn` is the open turn at append time. One log per Session. High-traffic cloud appends do not serialize on an integer counter.
6. **Place** — locator attached to a Session (zero or many): `id` (`PlaceId`), `provider`, `instance`, `os`, `attach`. One laptop, many folders (many `LocalDir` places) plus maybe a VM. Same `provider`+`instance` is one row. Not an Environment recipe. Durable agent memory is files under a Place claim (`PlaceMemory`), not a field on this locator and not a seventh noun.

A member is `UserId` or `AgentId`. One agent may sit in many sessions; one session may hold many users and agents. Only a joined user may `user_message` or `decide_permission`.

### Binding fields

| Field | Role |
| --- | --- |
| `id` | `BindingId` |
| `kind` | Host string (`claude_code`, `codex`, `dsh`, `fx`, `pi`, `opencode`, `goose`, `grok`, `grok_build`, or adapter strings such as `paseo-cli` / `claude-managed`) |
| `native_resume_id` | Provider resume token |
| `sandbox_id` | Shared host place key (string). Not `Place`. |
| `agent` | Optional `AgentId` for leave-scoped unbind |

### Place fields

| Field | Values |
| --- | --- |
| `id` | `PlaceId` (minted by `Place::local_dir` / constructors) |
| `provider` | `LocalDir`, `Aws`, `Azure`, `Gcp`, `CursorVm`, `GrokBox`, `Other(String)` |
| `instance` | Path or provider instance id |
| `os` | `Linux`, `Windows`, `Macos`, `Unknown` |
| `attach` | `MustExist`, `RecreateFromGit`, `CopyThenMount` (stored; only `MustExist` is enforced for `LocalDir`) |

`Place::local_dir(path, attach)` sets `PlaceId::new()`, `LocalDir`, and `current_os()`.

### EventBody

- `MemberJoin { member }`
- `MemberLeave { member }`
- `ConfigSet { key, value }`
- `UserMessage { user, text }`
- `AgentMessage { agent, text, parent_tool_call_id }` — `parent_tool_call_id: Option<String>` is the tool call this message ran under (subagent nesting; see **ACP parent**). `None` = top level.
- `AgentThink { agent, text, parent_tool_call_id }` — `parent_tool_call_id` as on `AgentMessage` (Thinking-chunk-parent)
- `ToolCall { agent, tool_call_id, name, input, parent_tool_call_id }` — `parent_tool_call_id` as on `AgentMessage`
- `ToolResult { agent, tool_call_id, name, output }`
- `ToolCallInterrupted { agent, tool_call_id, name, status }` — remint terminal for an orphan; `status` is `Indeterminate` (not Cancelled)
- `AskUser { agent, prompt }`
- `PermissionAsked { agent, request, rpc, parent_tool_call_id }` — `rpc: Option<PermissionRpc { binding, rpc_id, tool_call_id }>` is the JSON-RPC address (key `(binding, rpc_id)`); `parent_tool_call_id` is the subagent launch the gated tool ran under, read from the ask itself (see **Permission parent**), outside `PermissionRpc`
- `PermissionDecided { request, allowed, by }`
- `TurnStart { agent }` / `TurnEnd { agent }`
- `StepCompleted { turn_id, step_id, result }` — intra-turn durable-step checkpoint (string payload). Not model-visible.
- `TurnFenced { turn_id, fence, agent }` — per-Agent turn lease fence raise. Not model-visible. `Session::turn_fence_of(agent)` / `turn_fence_of_agent` restore; `Session::turn_fence()` is the max across Agents.
- `BindingAttached { binding, agent }` / `BindingReleased { binding }` (`agent` is the owning Agent, `None` for agentless `bind`, so Runtime classifies each kept Agent from the log alone)
- `Compact { start, end, summary }` — replaces `[start, end]` (inclusive `EventId` range in log order) in `surface`; full log keeps originals and the marker. Product path (`compact_with_handoff`) sets `summary` to a Place-path bridge after flushing `memory/handoff.md`; `compact` alone is the EventLog primitive and does not write memory.
- `Wake { source }` — Runtime timer/trigger marker (`WakeSource`: `UserQuery` / `Timer` / `Trigger { name }`). Not model-visible. User queries are `UserMessage` alone so they are not doubled.

`events()` is the full log. `surface()` / `surface_of` / `is_model_visible` keep only `UserMessage`, `AgentMessage`, `ToolCall`, `ToolResult`, `ToolCallInterrupted`, `Compact`, and hide events whose `id` lies in any Compact's inclusive log-order range (`start`/`end` EventIds; missing ids hide nothing). Think, permission, bind, membership, turn, `StepCompleted`, and `Wake` stay in `events()` only. `SessionData` holds `next_turn_id` and `next_seq_id` (next unused, start at 1), plus `last_read` and `current_turn`. `current_turn` is a **per-Binding map** (`BindingId → (turn, agent)`): many Bindings may each hold one open turn on a Session (concurrent LiveInTurn). Keyed by Binding, not Agent: `rebind(Some(old), ..)` (remint) moves the entry to the new Binding so an open turn is never orphaned; an entry outlives an `unbind` until `turn_end` or a superseding `turn_start` by that Agent. `push`/`append` assign a new `EventId`, `seq: None`, and the **owner's** turn without bumping those counters: Agent-bearing bodies take that Agent's turn (never another Agent's), RPC permission bodies the RPC Binding's turn, `StepCompleted`/`TurnFenced` their `turn_id`, owner-less bodies (user message, wake, bind, config, compact) the sole open turn or `None` when 0 or ≥2 are open. `open_turns_of` walks the log per Agent (`TurnEnd { agent }` closes only that Agent's turn; the log carries Agent not Binding, so replay assumes ≤1 open turn per Agent, as `rebind` keeps ≤1 live Binding per Agent); `open_turn_of` (most recent) / `open_turn_of_agent` / `Session::open_turn` / `restore_open_turn` (rebuilds the map, keyed by each Agent's live Binding) recover open turns from the log. Each map entry also carries the turn's parallel-safe declaration (map-only; see **Parallel-safe conflict gate** below). `step_result_of` / `Session::step_result` look up a memoized step payload.

There is no Place attach/detach event. `attach_place` / `detach_place` mutate session state only.

## Session methods

| Method | Behavior |
| --- | --- |
| `id` | `SessionId` |
| `join(member)` | Occupancy. Appends `MemberJoin`. Does **not** create Place or Binding. |
| `leave(member)` | Occupancy. For `Member::Agent`, also unbinds Bindings whose `agent` is that id (`BindingReleased` then `MemberLeave`). Does not drop Session, log, Place, or `FakeSandbox` files. User leave does not unbind. |
| `bind(kind, native_resume_id, sandbox_id)` | Binding with `agent: None`. Goes through `Host::from_bind` then `into_binding`. |
| `bind_agent(agent, kind, native_resume_id, sandbox_id)` | Binding with `agent: Some`. Join does not call this. Goes through `Host::from_bind` then `into_binding`. |
| `bind_host(agent, host)` | Typed path: attach a constructed `Host` (e.g. `HostKind::Goose.host(...)`). |
| `unbind(binding_id)` | Drops that Binding. Session, log, and Places remain. |
| `attach_place(place)` | Fail-closed (see below). Keyed by `PlaceId`. Same `provider`+`instance` overwrites that row (reuses its id). New id may use a different provider than other places. Provider swap only when rewriting an existing `PlaceId`. |
| `places()` | `Vec<Place>`. Survive unbind of all Bindings. |
| `place(id)` | `Option<Place>` lookup of one locator. |
| `detach_place(id)` | Remove that locator only. Unknown id → `UnknownPlace`. Session/log/other places remain. |
| `user_message(user, text)` | Occupied User only (`NotMember` / `UnknownUser`). |
| `ask_user` / `ask_permission` | Append events. No protocol. |
| `decide_permission(request, allowed, by)` | Decider must occupy the room. |
| `set_config` / `config` | Session `ConfigSet` on the log. `config()` is all joined agents' maps, then last-write-wins session keys. User config is not folded in. |
| `compact(start, end, summary)` | EventLog primitive: marker on the log (`EventId` range). Does **not** flush Place memory. |
| `turn_start(agent)` / `turn_start_on(binding)` | Open a turn owned by the Agent's live Binding (none → `Waiting(NoBinding)`) / by `binding` (must carry `agent`). Allocates `next_turn_id`. Same Binding already in a turn → `Waiting(TurnOpen)`. Not parallel-safe: any other open turn → `Waiting(TurnOpen)` (conflict gate). |
| `turn_start_with(agent, decl)` / `turn_start_on_with(binding, decl)` | Same, with an explicit `Option<ParallelSafe>` declaration; opens alongside other open turns only when the gate admits it. |
| `check_turn_gate(agent, decl)` | Read-only gate check (same rule) so callers refuse before side effects. Not a reservation; `turn_start_*` rechecks under the store lock. |
| `parallel_safe_on(binding)` | Declaration of the turn owned by `binding` (`None` if no turn or not parallel-safe). |
| `turn_end(agent)` / `turn_end_on(binding)` | Close that owner's turn; `TurnEnd` stamped with it, then the map entry is removed. `turn_end` with no open turn appends an unstamped `TurnEnd`; `turn_end_on` → `TurnNotOpen`. |
| `current_turn_on(binding)` / `current_turn_of(agent)` / `current_turns()` | Per-owner open turn / all `(BindingId, turn)`. `current_turn()` is a compat view: oldest open turn of any Binding. |
| `compact_with_handoff(memory, binding, start, end, working)` | **PreCompactHandoff**: require the Place is attached; flush `working` into `memory/handoff.md` under the Binding's Place claim; append `Compact` whose summary *references* Place paths. Does not remint Binding. |
| `step_result(turn_id, step_id)` | Memoized `StepCompleted` payload, if any. |
| `mark_read` | Only allocator for seq ids; bumps `next_seq_id` and `last_read`. |
| `last_read` / `next_turn_id` / `next_seq_id` | Getters (next unused values). |
| `surface` / `events` | View vs full log. |
| `members` / `bindings` | Current sets. |
| `live_sandbox_ids` | Distinct `sandbox_id` on **current** Bindings. Empty after last such unbind even if Place / `FakeSandbox` still exist. |
| `append` | Raw log write (used by adapters). |

`InMemory` also: `create_user` / `create_agent` / `create_session` / `session` / `user` / `agent` / `set_user_config` / `set_agent_config`. `Store` trait is the create/get subset.

`NullProvisioner::provision` → `session.bind`. No process.

## Parallel-safe conflict gate

Same-path serialize + fail-closed opt-in, **above** `turn_start` (in `SessionData::start_turn`, under the store lock). Many `LiveInTurn` per Session are allowed only when every live turn explicitly opted in.

- **Declaration.** `ParallelSafe { paths: Vec<String> }` — a declaration value (like `KeepSpec`), not a product noun, not a Queue. `None` = not parallel-safe. `ParallelSafe::default()` (empty set) is parallel-safe touching no paths. Stored on the per-Binding `current_turn` entry; no new EventBody field.
- **Rule.** A new turn of Agent X declaring `d` opens iff, for every other open turn `o` that would stay open (all entries except X's own released-Binding entries, which a start supersedes): `d` is `Some`, `o`'s declaration is `Some`, and their path sets do not overlap. Otherwise `Error::Waiting(WaitReason::TurnOpen)` — today's wait, no new reason — and nothing is written. A lone turn (no other open turn) never needs a declaration.
- **Explicit only.** Effect / tool kind never substitutes (a read-only `ToolCall` on an undeclared turn still blocks). The kernel never infers a declaration.
- **Paths.** Lexical, by component, no filesystem access; meant Place-relative. `a` overlaps `a`, `a/b`, `a/b/c.rs`; `a/b` does not overlap `a/c` or `a/bc`. `.` components ignored; empty path / `.` = whole tree; any `..` component overlaps everything (fail-closed). Absolute vs relative spellings of the same file are not unified (caller declares one convention).
- **Restore.** `restore_open_turn` keeps an existing entry's declaration; a turn rebuilt from the EventLog alone (fresh map) is not parallel-safe → fail-closed until it ends.
- **Runtime.** `Runtime::set_parallel_safe(session, agent, Option<ParallelSafe>)` sets the kept Agent's declaration (default `None`, survives re-`keep`, `NotKept` / `AgentNotKept`). `wake` / `begin_turn` gate **before** claim / Wake record / UserMessage (`Runtime::check_turn_gate`), then open with `turn_start_with`. `tick` skips a gated Agent (still due). Re-keep adopt / `resume_interrupted_turn` open no new turn and are not gated. Turn leases and fences are **per Agent**; the conflict gate (not a Session-wide lease slot) serializes undeclared / overlapping turns. Two ParallelSafe Agents may both hold leases without cross-Agent `TurnLeaseLost`.
- **Wire.** `wake_through_host_wake` / `begin_turn_through_host_wake` (new-turn path) call `Runtime::check_turn_gate` before `host_wake`: refused → `Waiting(TurnOpen)`, zero wire action. `HostAttach::with_parallel_safe(decl)` declares turns that attach opens: `user_message` gates first when its Agent has no open turn (refused → nothing appended or sent); a wire `TurnStarted` echo is absorbed only when the Agent already has an open turn, otherwise it is `turn_start_with(decl)` and a gate refusal surfaces as `Waiting(TurnOpen)` from `drain` (never silently absorbed).

## Runtime

`Runtime` is the long-lived keeper that holds Sessions so an Agent can keep waking on user queries, timers, and triggers. Same class as `Store` / `Provisioner` / `Host` / `FakeSandbox` — **not** a seventh product name. Binding remains disposable.

- **Keep-by-(Session, Agent).** The kept map holds, per Session, one recipe per Agent (keep order) plus a **per-Agent** turn lease map. Many Agents may be kept on one Session; every Agent-addressed call takes the Agent, and a recipe is never borrowed from another Agent.
- `keep(session, agent, host)` records that Agent's Host recipe and occupancy (`Member::Agent` if needed). Does **not** bind. Re-keep of the same `(session, agent)` updates only that Agent's host; other Agents' recipes, timers, and leases are untouched. `kept()` lists Sessions; `kept_agents(session)` lists Agents in keep order.
- `release(session)` stops keeping every Agent and drops every lease; `release_agent(session, agent)` drops one recipe and that Agent's lease slot (last Agent out un-keeps the Session). Session, Agent, Places, and the log survive. Bindings are not deleted. Unknown Session → `NotKept`; kept Session, unkept Agent → `AgentNotKept(session, agent)`.
- `classify(session, agent)` is find_server on that Agent's Binding: `LiveIdle` / `LiveInTurn` / `PositivelyDead` / `Unknown` / `NeverBound`. `LiveInTurn` means **this Agent's** turn is open (`current_turn_of(agent)`); another Agent's open turn on the same Session does not count. Re-keep adopt / `resume_interrupted_turn` / `durable_step` likewise look only at the kept Agent's turn (`open_turn_of_agent`). The no-live-Binding log scan only counts `BindingAttached` with `agent == Some(agent)` (or agentless, fail-closed): another Agent's Binding never makes a never-bound Agent `Unknown` / `PositivelyDead`. Missing heartbeat is `Unknown`, not death. Wait is `Error::Waiting(WaitReason)`, not a Queue type.
- `remint(session, agent)` is the idle path, minting with that Agent's recipe. Refused on `LiveIdle` / `LiveInTurn` (`AlreadyLive`) and on `Unknown`. Allowed on `NeverBound` / `PositivelyDead` (`BindingReleased` on the log, no live Binding).
- `wake(session, agent, wake)` requires a kept `(session, agent)` (`NotKept` / `AgentNotKept`). Ensures the agent is a member, then:
  - `LiveInTurn` / `Unknown` → `Waiting`. No remint. No `turn_start`.
  - `NeverBound` / `PositivelyDead` → remint from the stored Host (`session.bind_host`).
  - Conflict gate first (`check_turn_gate` with the Agent's `set_parallel_safe` declaration): refused → `Waiting(TurnOpen)`, no remint, nothing written.
  - `LiveIdle` (or after remint) → `UserQuery` is `user_message` (user must occupy the room). `Timer` / `Trigger` append `EventBody::Wake`. Then `turn_start_with(agent, decl)`.
  - Does **not** `turn_end` — the host runner ends the turn later. Does not spawn a CLI or invent `AgentMessage`.
- **Wake-each-kept.** `wake_each_kept(session, wake)` / `wake_each_kept_with` fans one wake out to **every** kept Agent on the Session (keep order): one shared wake record (one `UserMessage` / one `Wake`), then per-Agent classify / gate / claim / `turn_start_with`. A `Waiting` Agent is skipped (like `tick`); claim `Waiting` (Host `AlreadyBound` with no Binding) likewise skips without aborting. If every Agent waits, returns `Waiting` and appends nothing. Closes the #47 service residual (first-only). `hearth-service` steers through this path. SoftExpiring / fleet stay parked.
- **Claim-before-record.** Closes #56 Soft Should M7. Tip order is claim-then-`record_wake` (shared wake record only after a successful claim, or when `LiveIdle` needs none). Must floor: Host `AlreadyBound` + `NoBinding` on every ready Agent → `Waiting`, zero `UserMessage` / `Wake` appended (extends the busy classify/gate "wait appends nothing" floor). Mutant claim-after-`record_wake` KILL. Tests-only + DESIGN; no production API change. SoftExpiring / fleet stay parked.
- **Host-wake-each-kept.** Wire `wake_each_kept_through_host_wake`: for each kept Agent (keep order) run `host_wake` (one mint max per Agent; Waiting / no wire / gate / LiveInTurn / Unknown skip like tick), then once `Runtime::wake_each_kept_with(AlreadyBound)` — one shared wake record, never `claim_binding`. Closes the #56 Soft residual (Host was per-Agent `wake_through_host_wake` / first-only). Non-ParallelSafe still first-ready only; remint mid-fan-out does not steal a peer Binding. SoftExpiring / fleet stay parked.
- `schedule(session, agent, every_ms)` sets that Agent's `next_due_ms = now + every_ms`. `tick(now)` wakes each due `(session, agent)` (`next_due_ms <= now`; Sessions by id, Agents in keep order) and returns `(SessionId, AgentId, events)`; a `Waiting` Agent is skipped and stays due while other Agents on the Session still fire. `tick_with` passes `(session, agent, wake)`; wire `tick_through_host_wake` takes wires keyed by `(SessionId, AgentId)` and skips a wire whose Host Agent differs. Wire `wake/begin_turn/remint_through_host_wake` address `RemintSession::host_agent_id()`. Pass `now` (epoch ms, same clock as `Event.ts`) so tests drive the loop without sleeping.
- `Wake` is not model-visible (`surface` omits it, like membership / turn / bind / `StepCompleted`).
- `begin_turn(session, agent, holder)` opens (or attaches to) a turn under a `SessionTurnLease` (holder + fence) in **that Agent's** lease slot. Remints Binding only on `NeverBound` / `PositivelyDead`. Leases and fences are **per Agent** (`Session::turn_fence_of(agent)`; `EventBody::TurnFenced { agent, .. }`). An Agent's `begin_turn` only refuses on **its own** slot (`LiveTurnOpen` live holder, `Waiting(TurnOpen)` interrupted). Another Agent's lease / fence raise does not block or `TurnLeaseLost` at this layer — the ParallelSafe conflict gate still serializes undeclared / overlapping turns. Un-leased `wake` turns stay per Agent. `SessionTurnLease.agent` names the lease Agent; `turn_lease_of(session, agent)` reads one slot; `end_turn` / `durable_step` address the holder's Agent and that Agent's fence. Open-turn re-keep adopt (release + keep + begin_turn): restore the open turn, `Session::finalize_unmatched_tool_calls_of(agent)` (`ToolCallInterrupted { Indeterminate }` per unmatched id of the **kept Agent only** — another Agent's live ToolCall under a concurrent turn is untouched; same helper as resume / wire remint; idempotent), then install **this Agent's** lease under a raised **per-Agent** fence. An interrupted lease (holder dropped while still kept) must `resume_interrupted_turn`.
- `end_turn(session, holder, fence, interrupted)` — `false` appends `TurnEnd` and clears that Agent's lease slot; `true` drops the live holder and leaves the turn open on the EventLog (host crash).
- `durable_step(session, holder, fence, turn_id, step_id, f)` — requires a live lease (fence match) and an open turn. If `StepCompleted` for `(turn_id, step_id)` is already on the Session EventLog → `Memoized` (do not run `f`). Else run `f`, append `StepCompleted`, return `Executed`. `(session, turn_id, step_id)` has at most one successful side-effect.
- `resume_interrupted_turn(session, agent, new_holder)` — after host crash: refuse if **this Agent's** holder is still live (`LiveTurnOpen`); restore the open turn from the EventLog; `Session::finalize_unmatched_tool_calls_of(agent)` (kept Agent only) writes `ToolCallInterrupted { Indeterminate }` for each unmatched `tool_call_id` (same helper as wire remint / re-keep adopt; idempotent); then reclaim **this Agent's** lease slot with a new **per-Agent** fence. Return `(turn_id, fence, binding_id)`. Does **not** remint Binding. Re-enter the turn from the top; memoization skips completed steps.
- **Per-Agent-lease** (#52). Replaces the single Session lease slot with a lease map keyed by Agent (within Session). Two ParallelSafe Agents can both `begin_turn` / hold leases without `Waiting(TurnOpen)` / `LiveTurnOpen` from each other. Composes with #46–#51: resume/adopt still write only the kept Agent's slot (no cross-Agent steal); a refuse on one Agent leaves the other Agent's interrupted lease intact in **its** slot (pin with raw per-Agent slot asserts, not `turn_lease().is_none()` alone). SoftExpiring / fleet stay parked.
- **Fence-by-Agent.** Closes #52 Soft. Turn fence is keyed per Agent on the EventLog (`EventBody::TurnFenced { agent, turn_id, fence }`; `Session::turn_fence_of(agent)`; independent per-Agent counters). Raising one Agent's fence (install / resume / re-keep adopt) does not `TurnLeaseLost` another Agent's live lease under concurrent ParallelSafe turns. Same-Agent raise still gates that Agent's old fence. `Session::turn_fence()` remains the max across Agents (overview / refuse-no-write pins). SoftExpiring / fleet stay parked.
- **Concurrent-wake-remint-proof.** Lock floor for #46–#57: extend Concurrent-remint-proof so the concurrent path is driven by wake fan-out (`wake_each_kept` / `wake_each_kept_through_host_wake`), not only manual `begin_turn`. Two ParallelSafe Agents on disjoint paths; one shared UserMessage wake fans out so both become LiveInTurn (first-only wake mutant fails); both mid-turn with pending permission asks (B subagent-parented); Binding remint / AttachResume resurfaces both with parents; raw per-Agent `turn_lease_of` / `turn_fence_of` pins stay independent (not `is_none()` alone); no cross-Agent lease / fence / Binding steal. SoftExpiring / fleet stay parked.
- **Concurrent-nesting-proof.** Lock floor for #46–#61: extend Concurrent-wake-remint-proof so at least one kept Agent drains nested CLAUDE_ACP frames mid-turn under wake fan-out (`wake_each_kept_through_host_wake`); EventLog retains the axis-4 `parent_tool_call_id` quartet (ToolCall + AgentMessage + PermissionAsked + AgentThink) under `CLAUDE_ACP` / Pi; remint resurfaces Permission with parent while the other three parents stay on the log; peer generic-ACP Agent (or `caps.subagent == false`) strips all four — no cross-Agent leak. Reuses Nesting-proof Host-drain + Concurrent-wake-remint scaffolding. Drop-any-one / always-None mutants KILL. Tests-only + DESIGN; no production API; no `ToolResult` parent; no child Session / Binding; no v2 `subagent_update`. SoftExpiring / fleet stay parked.
- **Concurrent-remint-proof.** Lock floor for #46–#53: two kept Agents on ParallelSafe disjoint paths, both mid-turn with pending permission asks (one subagent-parented), Binding remint / AttachResume resurfaces both asks with parents preserved; each Agent keeps its own lease + fence (`turn_lease_of` / `turn_fence_of` raw pins — not `is_none()` alone); no cross-Agent lease or fence steal. Soft Should from Fence-by-Agent (pin `turn_lease_of(B).fence` after A's raise) absorbed into the wire e2e. SoftExpiring / fleet stay parked.
- **Resume-lease-by-Agent** (#51). Resume / adopt is per Agent: `resume_interrupted_turn(session, agent, …)` adopts only **that Agent's** interrupted lease (or, with no slot for that Agent, that Agent's own log-open turn into its slot). Another Agent's interrupted lease sits in **its** slot and is never overwritten. Live holder on **this** Agent's slot stays `LiveTurnOpen`. Composes with `finalize_unmatched_tool_calls_of(agent)`; no new noun, no new `WaitReason`.
- Holder / fence / `SessionTurnLease` are Runtime bookkeeping, not a seventh product name. No Queue type.
- InMemory EventLog is append-then-return (no fsync). A parked durable EventLog should fsync `StepCompleted` before `Executed` is returned.

### Grok test path

`HostKind::Grok` (`"grok"`) is a Binding kind like Goose — not a seventh kernel name, not an Environment. Production `hearth-service` keep stays Goose. `HostKind::GrokBuild` (`"grok_build"`) is the ACP coding-agent ticket used by `hearth-agent-wire`, distinct from the xAI chat ticket.

```
# skip (exit 0): neither key set
cargo test -p hearth --test grok_keep_wake

# one real chat completion against the kept turn
XAI_API_KEY=... cargo test -p hearth --test grok_keep_wake -- --nocapture
# GROK_API_KEY also works. Optional: XAI_MODEL / GROK_MODEL (default grok-3-mini).
```

`hearth::grok_api_key()` reads those env vars (or a local `.env` — gitignored; see `.env.example`). The test always exercises `Runtime::keep` + `wake` with a `grok` ticket; the HTTP call (`curl` to `https://api.x.ai/v1/chat/completions`) runs only when a key is present. Keys are not stored in the repo.

## Fail-closed Place

`Place::validate` then attach:

- **`PlaceMissing`** — `LocalDir` + `MustExist` and `instance` path does not exist. That locator is not inserted. Other places unchanged.
- **`PlaceProviderMismatch`** — non-`LocalDir` whose `instance` is an absolute path that exists (cannot claim a real local dir as Aws/etc.).
- **`PlaceProviderSwap`** — attaching a Place whose `id` already exists with a different `provider`. Existing row kept. A *new* `PlaceId` with another provider is allowed (fail-closed is per locator).
- **`UnknownPlace`** — `detach_place` / lookup of an id that is not on the session.

Same `provider`+`instance` re-attach overwrites that row (reuses its `PlaceId`). Different folders on one laptop are many `LocalDir` places. Cloud/`RecreateFromGit`/`CopyThenMount` are stored labels; this crate does not provision VMs or clone repos.

## Place-backed memory

Durable memory is **files under a Place claim**, not Session EventLog compaction and not a seventh noun. `PlaceMemory` composes with `Place` / `Session` / `Binding` the way `FakeSandbox` composes with `sandbox_id`. No Queue. SoftExpiring pile and Flush-before-dispatch stay parked.

| Layer | Path (under Place) | Load policy |
| --- | --- | --- |
| Instructions | `AGENTS.md` | Always inject (human-authored) |
| Index | `memory/MEMORY.md` | Always inject, **byte-capped** (`SUMMARY_BYTE_CAP`) |
| User slice | `memory/USER.md` | Always inject when present, capped |
| Topics | `memory/topics/*.md` | On demand via `read_topic` |
| Handoff | `memory/handoff.md` | Flush **before** EventLog compact; bridge points here |
| Skills | `skills/*/SKILL.md` | Procedural; load when relevant (`read_skill`) |

### Invariants

1. **Inject index only** — `inject(session_cwd)` returns `AGENTS.md` + capped `MEMORY.md` + optional capped `USER.md`. Never dump topics/skills every turn.
2. **Writes require Place claim** (keep-as-claim). `acquire_claim(binding)` / `release_claim` / `transfer_claim`. No claim → `NoPlaceClaim`. A second Binding without transfer → `SplitBrain`.
3. **PreCompactHandoff** — `compact_with_handoff` / `PlaceMemory::compact_bridge` flush `WorkingState` into `handoff.md` before the EventLog `Compact` marker. The compact summary *references* `place://{PlaceId}/memory/handoff.md` + `MEMORY.md`. It does not inline the handbook.
4. **Cross-agent** = same Place + claim transfer. Session and Binding ids stay (no remint).
5. **Scope** — optional `applies_to` cwd. Foreign `session_cwd` still gets `AGENTS.md`, not the memory index / user slice.

`Session::compact` remains the EventLog primitive (used by tests that only hide a range). The product path is `compact_with_handoff`.

### Documented holes

These `MemoryPolicy` variants exist so the holes stay testable. They are not production defaults.

| Hole | What goes wrong |
| --- | --- |
| `NaiveInjectFullHandbook` | Every turn dumps `MEMORY.md` + all topics → context blowup |
| `NaiveCompactWithoutHandoff` | Compact summary is transcript-only; `handoff.md` is never written → working state lost |
| `NaiveWriteWithoutClaim` | Writes succeed without the Place claim → split-brain concurrent edits |

`MemoryPolicy::PlaceBacked` is the correct path.

### API

```
PlaceMemory::new(place_id, MemoryPolicy::PlaceBacked)
PlaceMemory::for_place(&place, policy)
acquire_claim / release_claim / transfer_claim / claim_holder
set_agents_md / set_memory_md / set_user_md / put_topic / put_skill / set_applies_to_cwd
inject(session_cwd) -> InjectedContext
flush_handoff(WorkingState) / compact_bridge(...) / handoff()
Session::compact_with_handoff(memory, binding, start, end, working)
```

Writes take the claiming `BindingId`. `transfer_claim(from, to)` moves the claim; it does not remint Binding or Session.

## Agent wire (`hearth-agent-wire`)

Adapter layer so Host can talk to external coding agents (Grok Build ACP, Codex App Server, Pi JSONL RPC, OpenCode SSE) through one `AgentEvent` / `AgentCommand` surface. **Not a seventh kernel noun.** Host↔Goose remains the runtime child over ypipe. This crate normalizes *external* agents so Session/EventLog can record and steer them uniformly. No Queue type — buffers stay transport-local only (`LoopbackAgent`, `FramedAgent`).

Same composition class as `PlaceMemory` / `FakeSandbox`: `HostAttach` holds an existing `Binding` plus a `CodingAgent`. Bind once (`HostAttach::bind` → `session.bind_host`); attach/resume reuse that `Binding.id` (no remint). Native resume tokens stay on `Binding.native_resume_id`. Subagent/task ids correlate on the EventLog; they do not mint child Sessions or Bindings.

### Crate layout (contrib)

Transport and Adapter are split. Transport frames **bytes ↔ `WireFrame`**. `AdapterCodec` maps **`WireFrame` ↔ `AgentEvent` / `AgentCommand`**. There is no public newline `push_line` / `map_wire_line`.

```
crates/hearth-agent-wire/
  CAPABILITIES.md      # shipped kinds + how to add Claude Code / Cursor
  CONTRIBUTING.md
  REDESIGN.md          # Transport vs Codec rules (stdio scrape banned)
  src/lib.rs           # AgentEvent, AgentCommand, CodingAgent, FramedAgent, LoopbackAgent
  src/transport/       # WireFrame, WireKind, Transport, jsonrpc / sse / websocket / jsonl
  src/catalog/         # builtin.toml → LaunchSpec data (Copilot, Cursor; shared AcpCodec; no spawn)
  src/host.rs          # HostAttach, event_bodies
  src/adapters/<name>/
    README.md
    fixtures/*.json
    mod.rs             # map_* + AdapterCodec
```

| Transport | Wire | Rejects |
| --- | --- | --- |
| `JsonRpcTransport` | LSP/ACP `Content-Length` or u32 length-prefix | NDJSON, missing length, invalid JSON, SSE frames |
| `SseTransport` | WHATWG EventSource (`event` / `data` / `id`) | Single-line `data:` strip inside adapters, non-JSON data (except `[DONE]`) |
| `WebSocketJsonTransport` | one WS text message | concatenated JSON values, SSE frames |
| `JsonlRpcTransport` | `jsonl-rpc` — newline frame delimiter + `rpc_chunk` reassembly | banner / invalid JSON as events, NDJSON log scrape, SSE frames |

`FramedAgent<T, C>` is `Transport` + `AdapterCodec`. Decoded path: `JsonRpcTransport::push_decoded` / `SseTransport::push_frame` / `JsonlRpcTransport::push_decoded`. Byte path: `try_decode_content_length` / `parse_event_block` / `decode_text` / `JsonlRpcTransport::push_bytes`, then codec. Live process spawn is optional and not required for the kernel bar.

`catalog/` is profile data only. Builtin Copilot (`copilot --acp`) and Cursor (`cursor-agent acp`) resolve to a `LaunchSpec` and share `AcpCodec`. Reattach is `HostAttach::resume` with `Binding.native_resume_id`, not Gemini `session/load`. `AttachRunner::spawn` exists (`hearth-agent-wire` `runner/mod.rs`); the missing link is that `Runtime::wake` does not route through `host_wake` yet. Remint `session/resume` carries `sessionId`, `cwd` (absolute `instance` of the Session's LocalDir Place) and `mcpServers` (Session config, `[]` if none); no Place / non-LocalDir / relative path fails closed with a typed `RemintError` and writes nothing.

### Outbound `AgentCommand` (Hearth → agent)

`CreateProject` / `OpenSession` / `CloseSession` / `UserMessage` / `Steer` / `Abort` / `ReplyPermission` / `ReplyQuestion` / `Compact` / `SpawnTask` / `CancelTask`.

### Inbound `AgentEvent` (agent → Hearth)

Session/project lifecycle, message and thinking (plus deltas), tool call upsert + result, plan, `PermissionAsk` / `QuestionAsk`, task / subagent lifecycle, `CompactStarted` / `Compacted`, status, error, `Native` (unmapped payload).

### EventLog mapping

`HostAttach::drain` appends through existing Session methods. Binding is not reminted.

| AgentEvent | EventLog |
| --- | --- |
| `TurnStarted` / `TurnCompleted` | `turn_start_with(attach decl)` (absorbed if the Agent's turn is already open; gate refusal → `Waiting(TurnOpen)`) / `turn_end` |
| assistant `Message` / `MessageDelta` | `AgentMessage` (wire `parent` → `parent_tool_call_id`) |
| `Thinking` / `ThinkingDelta` | `AgentThink` (wire `parent` → `parent_tool_call_id` when `caps.subagent`; log only; not `surface`) |
| `ToolCall` / `ToolResult` | `ToolCall` (wire `parent` → `parent_tool_call_id`; `subagent` not recorded) / `ToolResult` |
| `Plan` | `AgentMessage` (joined entries) |
| `PermissionAsk` | `PermissionAsked` |
| `QuestionAsk` | `AskUser` |
| `TaskCompleted` | `AgentMessage` (`task {id} ok\|failed`) |
| `SessionStarted` | remember native session id on the attach; no Event, no remint |
| `CompactStarted` / `Compacted` | ignored on drain (must not append a second `Compact`) |
| lifecycle / `Status` / `Error` / `Native` / subagent start | not appended |

`HostAttach::user_message` is occupancy-checked `Session::user_message` then outbound `UserMessage`. Permission reply is `decide_permission` then `ReplyPermission`.

### ACP parent (subagent nesting)

Claude Code over ACP (claude-agent-acp, v1 / Zed form) runs subagents **inside the root ACP session**. The launching Task / Agent tool call carries `update._meta.claudeCode.subagent: true`; every message chunk and tool call the subagent makes carries `update._meta.claudeCode.parentToolUseId: <launch toolCallId>`. Nested launches chain: an inner Agent call is both a child (`parentToolUseId: outer`) and a launch (`subagent: true`).

Hearth records that correlation and nothing more:

- **Wire.** `AgentEvent::ToolCall`, `Message`, `MessageDelta`, `Thinking`, `ThinkingDelta` gain `parent: Option<ItemId>`; `ToolCall` also gains `subagent: bool`. `AcpCodec` decodes them from `_meta.claudeCode` per frame (Grok Build shares the map; `agent_thought_chunk` → `ThinkingDelta.parent`). Fields are serde-additive (`default`, skipped when empty), so old JSON round-trips unchanged.
- **Fail-closed.** A missing `_meta`, a missing / non-object `claudeCode`, or a `parentToolUseId` that is not a non-empty string is `None`; a call never names itself as parent. `subagent` is `true` only for JSON `true`. Nothing is inferred across frames: when the wire drops the meta on a later update (the Task's own completion frames drop `subagent`), that frame decodes without it. Consumers key by `tool_call_id`.
- **Kernel.** `EventBody::ToolCall`, `EventBody::AgentMessage`, and `EventBody::AgentThink` gain `parent_tool_call_id: Option<String>`, copied from the wire `parent` by `event_bodies` **when** `caps.subagent` is true (see **Host-caps-consume**). It is an Event field. It is not a seventh noun, and it mints **no child Session and no child Binding**: every child frame appends to the same Session under the same Binding, the native session id is not touched, and nothing is reminted. `surface()` / `is_model_visible` are unchanged (child activity stays visible; nesting is metadata; Think stays log-only).
- **No `SubagentStarted` from ACP.** The launch already lands as a `ToolCall` whose `tool_call_id` is what children name, so that ToolCall *is* the parent marker. Turning the launch frame into `SubagentStarted { child_session_id: toolCallId, .. }` would drop the launch ToolCall (one frame decodes to one event) and pretend a tool-call id is a session id. `SubagentStarted` / `SubagentEnded` keep the Pi shape and stay "not appended" (table above). The wire `subagent` marker is not copied onto the kernel: nesting needs only the child → parent link.
- **Not in this cut.** `ToolResult` gets no parent (ACP never decodes one; completion is a `tool_call_update`). Permission asks landed next (see **Permission parent**). Thinking chunks landed as **Thinking-chunk-parent** (below). The v2 / RFD native-subagent-session form (`subagent_update`, child `sessionId`s) is left for later slices.
- **Claude-acp catalog (capability-honest).** Builtin profile `claude-acp` (`claude-agent-acp` argv, shared `AcpCodec`) advertises [`CapabilityFlags::CLAUDE_ACP`] (`subagent: true`) via `CatalogProfile::caps` / `caps_override`. Generic [`CapabilityFlags::ACP`].subagent stays `false`; Copilot / Cursor inherit ACP defaults. No new AgentKind, no child Session/Binding.
- **Host-caps-consume.** Host consults `CapabilityFlags` beyond catalog discovery: `HostAttach` carries `agent_caps` (default `CapabilityFlags::for_agent(kind)`; catalog profiles pin via `with_agent_caps(profile.caps())`). On drain, `event_bodies` retains wire `parent` → `parent_tool_call_id` only when `caps.subagent` is true (`CLAUDE_ACP` / Pi). When `caps.subagent` is false (generic ACP / Copilot / Cursor / Codex / OpenCode / GrokBuild), parent is stripped to `None` even if wire `_meta` carries `parentToolUseId` — fail-closed so advertise-only is not enough. No new noun; toy Loopback + caps pin.
- **Thinking-chunk-parent.** Closes the residual under ACP-parent / Host-caps-consume: `AgentEvent::Thinking` / `ThinkingDelta` gain serde-additive `parent: Option<ItemId>`; ACP `agent_thought_chunk` decodes `_meta.claudeCode.parentToolUseId` via the same fail-closed `claude_parent` (missing / non-string / self → `None`). Kernel `EventBody::AgentThink` gains `parent_tool_call_id`; `event_bodies` copies it only when `caps.subagent` (reuse Host-caps-consume gate; strip under generic ACP / Copilot / Cursor). Other adapters / `Thinking` constructors pass `None`. No new noun; no child Session / Binding; no `ToolResult` parent; no v2 `subagent_update`.
- **Nesting-proof.** Joined Host-drain lock-floor for axis-4: `ToolCall` + `AgentMessage` + `PermissionAsked` + `AgentThink` retain `parent_tool_call_id` under `CLAUDE_ACP` / Pi; all four strip to `None` under generic ACP / Copilot / Cursor even when wire `_meta` carries `parentToolUseId`. Reuses `claude_subagent_nested` / task fixtures + Host-caps-consume / Thinking-chunk-parent floors (extend, not unit-only duplicate). Concurrent-*-remint only pins Permission on remint — this locks the full quartet on drain. Tests-only + DESIGN; no production API; no `ToolResult` parent; no child Session / Binding; no v2 `subagent_update`. SoftExpiring / fleet stay parked.
- **Concurrent-nesting-proof.** Joins Concurrent-wake-remint-proof with Nesting-proof under wake fan-out: two ParallelSafe Agents → both LiveInTurn; CLAUDE_ACP Agent Host-drains nested frames mid-turn so the axis-4 quartet retains `parent_tool_call_id`; remint resurfaces Permission with parent and EventLog still holds the other three; peer generic ACP strips (no cross-Agent leak). Drop-any-one / always-None mutants KILL. Tests-only + DESIGN; SoftExpiring / fleet stay parked.

#### Permission parent

A subagent's `session/request_permission` names its launch the same way: claude-agent-acp stamps `params.toolCall._meta.claudeCode.parentToolUseId` (v2: `params.subject.toolCall._meta…`) on the ask itself.

- **Wire.** `AgentEvent::PermissionAsk` gains `parent: Option<ItemId>` (serde-additive), decoded from that toolCall's `_meta` with the update rule (missing / malformed / self → `None`; params-level `_meta` is not read).
- **Kernel.** `EventBody::PermissionAsked` gains `parent_tool_call_id: Option<String>`; `Session::ask_permission_rpc(agent, request, rpc, parent_tool_call_id)`. It sits **beside** `PermissionRpc`, not in it: the pending key stays `(binding, rpc_id)`, `PermissionDecided` / routing / re-ask adoption are unchanged. No child Session or Binding.
- **Remint.** `PendingPermission` and `PermissionRef` carry `parent_tool_call_id`; `PermissionResurface` (every remint, including a second one), `PermissionReasked` and `PermissionResolved` show it. The parent never makes a dead Binding's ask answerable or changes which resurfaced ask a re-ask adopts (still unique `toolCallId` only).
- **Ordering (claude-agent-acp #708): ask before update — fail-closed, no patching.** The ask's parent comes from the ask frame only. If the ask states none (the #708 frame as first reported, or a non-Claude agent), `PermissionAsked` / the pending ask record `None` = "unattributed", and Host does **not** borrow a parent from an earlier `ToolCall` or patch one in when a later update arrives. Reasons: the EventLog is append-only (a patch could not reach the recorded `PermissionAsked`, so log and pending state would disagree); `toolCallId` is not unique across asks (copilot-cli #989), so cross-frame joins can misattribute; current claude-agent-acp (`0ce42f2`) already stamps the ask, which is what closes #708. A consumer that wants a best-effort join may still read the later `ToolCall { parent_tool_call_id }` by `rpc.tool_call_id`; Host does not.

Fixtures: `adapters/acp/fixtures/claude_subagent_task.json` (claude-agent-acp origin-main Zed snapshot `subagent-task-legacy`, JSON-RPC wrapped, bulky `content` / `toolResponse` elided) and `claude_subagent_nested.json` (Agent → Agent → Read, hand-built in the same v1 shape from the upstream v2 `subagent-nested` scenario).

### Compact

`AgentCommand::Compact` **composes with** PreCompactHandoff. `HostAttach::compact_with_handoff` calls `Session::compact_with_handoff` (flush `memory/handoff.md` under the Binding's Place claim, EventLog `Compact` whose summary *references* Place paths) and only then sends `AgentCommand::Compact`. It does not replace `Session::compact` (EventLog primitive) and does not write memory itself. Place-backed `compact_with_handoff` / `handoff.md` stay intact.

### Host kinds

`AgentKind::{GrokBuild, Codex, Pi, OpenCode}` map to Binding kinds `grok_build` / `codex` / `pi` / `opencode` via `host_for` / `Host::from_bind`. `grok_build` is ACP; `grok` remains the xAI chat ticket.

### Parked (do not land here)

SoftExpiring pile, DualGate, AdmitCommit, Flush-before-dispatch, EffectId upsert, Evidence-before-next-inference, Stage-before-cutover (beyond what is already on main), host heartbeat, Binding fence, turn-lease renew. Runtime already has a lease fence for durable steps; `hearth-agent-wire` does not add another.

## Join / leave / unbind / place

- `join` is occupancy only — no Binding, no Place.
- Agent `leave` unbinds **that agent's** Bindings only (`Binding.agent == Some(id)`). Bindings with `agent: None` stay.
- Last agent leave does **not** delete Session, log, Places, or host files.
- `unbind` drops a Binding pointer. Places survive.
- `FakeSandbox::release(sandbox_id)` is the dedicated host-file drop. Not implied by leave or unbind.

Shared host folder/VM is still `Binding.sandbox_id` (option A: two Bindings, same id, different `kind`; option B: same id, distinct `native_resume_id`, tools applied to `FakeSandbox`). `Place` is the session locator (`provider`/`instance`), not those files.

## Environment vs Place

**Environment** (Claude Managed Agents / Tag recipe: isolation, runtime image, recycle) is **not** a kernel type. Adapters map it to `Binding.sandbox_id` + `kind`. Recycle = unbind old Binding, bind a new one on the same Session, replay `surface`.

**Place** is the sixth kernel type: attach locator on the Session. It outlives Bindings. It is not an Environment recipe. The locator struct does not store files.

`PlaceMemory` is the Place-claim file map (`AGENTS.md`, `memory/*`, `skills/*`) — same class as `FakeSandbox`, not a seventh name. `FakeSandbox` is host map (`sandbox_id → path → text`), not a Binding and not Place.

## Refused kernel types

Channel, Thread, Issue, Squad, **Environment**.

Claude Tag **channel** and Multica **Issue** are Sessions in adapters. MA **Session** is Binding plus a turn, not `hearth::Session`.

## Layout

```
crates/hearth/              kernel (lib.rs, host.rs, place.rs, place_memory.rs, sandbox.rs, runtime.rs)
crates/hearth-agent-wire/    unified AgentEvent / AgentCommand + HostAttach (not a seventh noun)
crates/hearth-paseo/        Paseo name map (no daemon / worktree supervisor)
crates/hearth-managed/      MA / Tag name map
crates/hearth-service/      local HTTP + WebSocket
```

Examples: `steer.rs`, `two_runtimes.rs`.

## Host mappings

### Paseo (`hearth-paseo`)

| Paseo | hearth |
| --- | --- |
| client / UI Session | `Session` (the room) |
| ManagedAgent | `Agent` |
| worktree + spawned CLI | `Binding` (`kind` `paseo-cli` / `worktree`) |
| native Claude/Codex resume id | `Binding.native_resume_id` |
| worktree id | often `Binding.sandbox_id` via `binding_from_worktree` |
| human operator | `User` |
| permission_requested / permission_resolved | `PermissionAsked` / `PermissionDecided` |
| provider hydrate | `hydrate_from_provider`: bind + import into the **same** log |

Paseo collapses chat and running CLI. hearth splits them: the room outlives the process. Timeline RAM + provider rehydrate is resume id + imported events, not a second log. Crate does not run Paseo.

### Managed Agents (`hearth-managed`)

| Managed Agents | hearth |
| --- | --- |
| Agent | `Agent` |
| Environment / sandbox | `Binding.sandbox_id` (and `kind`); **not** `Place`; not a kernel Environment |
| MA Session (run handle) | **not** `hearth::Session` — Binding plus a turn (`kind` `claude-managed`) |
| MA Events | `Event` / `EventBody` |
| durable multi-party room | `hearth::Session` |
| Tag channel | `Session` (two humans = two `User` members) |
| interrupt | `UserMessage` + optional `unbind` |
| env recycle | unbind / bind / `surface` unchanged |

### Claude Tag / Multica

A Tag **channel** is a `Session`. Not a Channel type. A Multica **Issue** is a `Session` in the adapter. Not Issue/Thread/Squad in the kernel.

### Agent wire (`hearth-agent-wire`)

| Wire idea | hearth |
| --- | --- |
| `AgentEvent` / `AgentCommand` | adapter vocabulary — not a kernel type |
| `HostAttach` | PlaceMemory-shaped composer over one Binding |
| attach / resume | same `Binding.id` (no remint) |
| `AgentEvent` drain | `Session` EventLog append (`turn_start` / `append` / …) |
| `AgentCommand::Compact` | after `compact_with_handoff` (Place `handoff.md`) |
| bytes → WireFrame | `JsonRpcTransport` / `SseTransport` / `WebSocketJsonTransport` |
| WireFrame → AgentEvent | `AdapterCodec::decode_event` / `adapters::decode_frame` / `map_native` |
| `FramedAgent<T, C>` | `transport_mut().push_decoded(...)` (not `push_line`) |
| Goose ypipe child | unchanged; agent-wire is for *external* coding agents |

### Two runtimes / host chrome

| Host idea | hearth |
| --- | --- |
| Co-tenant (Claude Code + Codex in one folder/VM) | two Bindings, same `sandbox_id` |
| Tool proxy (each CLI process, tools on one remote) | two Bindings, same `sandbox_id`, distinct `native_resume_id`; tools → `FakeSandbox` |
| Cursor default VM / pool / named machine / saved Environment | `sandbox_id` string and/or `Place` (`CursorVm`, …). Host owns env vars, egress, install. |
| Grok Bot shared machine vs per-agent desktop | machine = `sandbox_id` / `PlaceProvider::GrokBox`; desktop is host chrome, not a kernel type |
| Tag idle release | `unbind` / host recycle; Session + log + Place remain |

## Local service (`hearth-service`)

Bind `0.0.0.0:8787`. No public URL.

| Route | Role |
| --- | --- |
| `POST /sessions` `{"folder":"/optional/existing/path"}` | Create Session. If folder exists, `attach_place` `LocalDir` + `MustExist`. Missing folder → Session with `places: []` (no error). |
| `GET /sessions/:id` | `{ id, places, members }` |
| `POST /sessions/:id/join` `{"user"}` or `{"agent"}` | Occupancy. Creates named User/Agent on first use. Does not bind. |
| `POST /sessions/:id/events` `{"user","message"|"text"}` | `user_message` |
| `GET /sessions/:id/events` | Full log as `{ seq, body }` (`user:…`, `join`, or Debug) |
| `GET /sessions/:id/stream` | WebSocket: replay then live. Inbound JSON `type=join` / `type=message` (`user`/`steer` aliases). HTTP-shaped bodies work. Two clients see each other's join and steer. HTTP steer still works. |
| `GET /agents/local` | PATH probe only (`probe_local`). No session CLI spawn. |

CLI probe does not start a host CLI. Prefix-install shims (`claude`/`codex` under `node_modules/.bin`) need those dirs on PATH; Node 22 is not required. See `crates/hearth-service/README.md`.

No HTTP for bind, unbind, compact, ask/decide, or Place swap.

## Honest not-implemented

- No process spawn, PTY, or CLI supervisor. `Provisioner` / service probe never start a session CLI.
- No durable store (in-memory only). `StepCompleted` follows the same append-then-return path; fsync-before-`Executed` waits on a durable EventLog.
- No public ingress, auth, or multi-node replication.
- `PlaceAttach::RecreateFromGit` / `CopyThenMount` are enums only.
- Cloud providers are labels + fail-closed checks, not provisioners.
- `TurnStart` / `TurnEnd` exist on the enum and `turn_start` / `turn_end` allocate turn ids. `Runtime::begin_turn` / `end_turn` add a lease fence for durable steps; `Runtime::wake` still only `turn_start`s. No host turn runner.
- `Runtime::tick` is a deterministic timer pump; no background thread.
- `AskUser` is an event, not a blocking RPC.
- `FakeSandbox` is an in-crate hashmap, not isolation.
- `PlaceMemory` is an in-crate file map (not fsync'd onto `LocalDir`). A durable Place would persist the same paths.
- SoftExpiring pile / DualGate / AdmitCommit and Flush-before-dispatch stay **parked**. Also parked on the agent-wire: EffectId upsert, Evidence-before-next-inference, Stage-before-cutover (beyond main), host heartbeat, Binding fence, turn-lease renew.
- Adapters (`hearth-paseo`, `hearth-managed`, `hearth-agent-wire`) convert names / wire formats; they do not embed those products. `hearth-agent-wire` is not a seventh noun and not a Queue.
- `hearth-agent-wire` mappers + `LoopbackAgent` / `FramedAgent` are in-process. Transport is Content-Length JSON-RPC / EventSource / WS JSON — not newline-split stdio. Live CLI spawn/PTY is still Host follow-on.


## Three parts of a host bind

Adding OpenCode, Goose, Grok Build, Dsh, and the others is three pieces. **Host is not a seventh kernel name.** External coding-agent I/O goes through `hearth-agent-wire` (`HostAttach`); Goose over ypipe is unchanged.

1. **User params** — what `bind` / `bind_agent` already take: optional `agent`, `kind`, `native_resume_id`, `sandbox_id`.
2. **Host type** — a real Rust type constructed when `HostKind::Goose` (etc.) is passed: `Host::Goose(ticket)`. Known hosts share `HostTicket` (`native_resume_id`, `sandbox_id`). `HostKind::host` and `Host::from_bind` build it. Adapter strings (`paseo-cli`, …) become `Host::Other`. Not an Agent subclass.
3. **Binding** — the record stored on the Session. Product `kind` stays a string (`Goose` → `"goose"`, `Grok` → `"grok"`). `Host::into_binding` mints the `BindingId` and copies fields.

`bind` / `bind_agent` keep the same public signatures and go `(1) → (2) → (3)`. The typed path is `session.bind_host(Some(id), HostKind::Goose.host(None, Some("box".into())))`.

## Adding a host

OpenCode, Goose, Grok, DeepSeek Harness, and the others are **Binding kinds** (via a typed `Host` ticket), not new Agent types. One `Agent` identity can bind as `opencode` in one session and `grok` in another.

1. Add one line to the `hosts!` list in `host.rs` (`HostKind` + `Host` + `as_str`). Or pass a raw string → `Host::Other`. `dsh` is DeepSeek Harness. `grok` is the xAI chat ticket (see Grok test path). `grok_build` is Grok Build ACP via `hearth-agent-wire`.
2. `store.create_agent(...)` then `join` then `bind_agent(id, HostKind::OpenCode, resume, sandbox)` or `bind_host(Some(id), HostKind::OpenCode.host(resume, sandbox))`.
3. Spawn and resume stay in a host crate / `Provisioner`. The kernel only records the Binding.
4. Do not add an Agent subclass or a seventh kernel name.
