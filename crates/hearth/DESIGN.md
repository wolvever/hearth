# hearth

Session-first kernel. Six product names: **User**, **Agent**, **Session**, **Binding**, **Event**, **Place**. `Store` / `InMemory` persist them; `Provisioner` records a `Binding` without spawning a CLI; `FakeSandbox` is a host-side file map keyed by `sandbox_id`. None of those is a seventh concept. `Runtime` keeps Sessions so an Agent can wake on user queries, timers, and triggers — same class as Store / Provisioner / Host / FakeSandbox, not a seventh name.

## Six concepts

1. **User** — `id`, `name`, owned `config`. Occupies a session as `Member::User`.
2. **Agent** — long-lived `id`, `name`, `instructions`, owned `config`. Not a process, run, or worktree.
3. **Session** — joinable room: membership + one append-only `Event` log + current bindings + a map of `Place`s keyed by `PlaceId`. Exists with zero bindings and zero places. `unbind` never deletes the room, the log, or any place.
4. **Binding** — disposable runtime: `id`, `kind` (string; `HostKind` is only a helper, not a type), optional `native_resume_id`, optional `sandbox_id`, optional `agent`. Native resume tokens live here, never on `Session.id`. `agent` names whose runtime this is so `leave` can unbind only that agent's Bindings.
5. **Event** — `{ id, seq?, turn?, ts, body }`. `Event.id` (`EventId`) is identity. `seq` is an optional last-read cursor, allocated only by `mark_read`, never on append. `turn` is the open turn at append time. One log per Session. High-traffic cloud appends do not serialize on an integer counter.
6. **Place** — locator attached to a Session (zero or many): `id` (`PlaceId`), `provider`, `instance`, `os`, `attach`. One laptop, many folders (many `LocalDir` places) plus maybe a VM. Same `provider`+`instance` is one row. Not an Environment recipe.

A member is `UserId` or `AgentId`. One agent may sit in many sessions; one session may hold many users and agents. Only a joined user may `user_message` or `decide_permission`.

### Binding fields

| Field | Role |
| --- | --- |
| `id` | `BindingId` |
| `kind` | Host string (`claude_code`, `codex`, `dsh`, `fx`, `pi`, `opencode`, `goose`, `grok`, or adapter strings such as `paseo-cli` / `claude-managed`) |
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
- `AgentMessage { agent, text }`
- `AgentThink { agent, text }`
- `ToolCall { agent, name, input }`
- `ToolResult { agent, name, output }`
- `AskUser { agent, prompt }`
- `PermissionAsked { agent, request }`
- `PermissionDecided { request, allowed, by }`
- `TurnStart { agent }` / `TurnEnd { agent }`
- `StepCompleted { turn_id, step_id, result }` — intra-turn durable-step checkpoint (string payload). Not model-visible.
- `BindingAttached { binding }` / `BindingReleased { binding }`
- `Compact { start, end, summary }` — replaces `[start, end]` (inclusive `EventId` range in log order) in `surface`; full log keeps originals and the marker
- `Wake { source }` — Runtime timer/trigger marker (`WakeSource`: `UserQuery` / `Timer` / `Trigger { name }`). Not model-visible. User queries are `UserMessage` alone so they are not doubled.

`events()` is the full log. `surface()` / `surface_of` / `is_model_visible` keep only `UserMessage`, `AgentMessage`, `ToolCall`, `ToolResult`, `Compact`, and hide events whose `id` lies in any Compact's inclusive log-order range (`start`/`end` EventIds; missing ids hide nothing). Think, permission, bind, membership, turn, `StepCompleted`, and `Wake` stay in `events()` only. `SessionData` holds `next_turn_id` and `next_seq_id` (next unused, start at 1), plus `last_read` and `current_turn`. `push`/`append` assign a new `EventId`, `seq: None`, and `turn: current_turn` without bumping those counters. `open_turn_of` / `Session::open_turn` / `restore_open_turn` recover a `TurnStart` without `TurnEnd` from the log. `step_result_of` / `Session::step_result` look up a memoized step payload.

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
| `compact(start, end, summary)` | Marker on the log (`EventId` range). |
| `turn_start(agent)` / `turn_end(agent)` | Allocate `next_turn_id` and set `current_turn`; `TurnEnd` still stamped with that turn, then `current_turn` cleared. |
| `open_turn` / `restore_open_turn` | EventLog view / restore of an interrupted turn (`TurnStart` without `TurnEnd`). |
| `step_result(turn_id, step_id)` | Memoized `StepCompleted` payload, if any. |
| `mark_read` | Only allocator for seq ids; bumps `next_seq_id` and `last_read`. |
| `last_read` / `next_turn_id` / `next_seq_id` | Getters (next unused values). |
| `surface` / `events` | View vs full log. |
| `members` / `bindings` | Current sets. |
| `live_sandbox_ids` | Distinct `sandbox_id` on **current** Bindings. Empty after last such unbind even if Place / `FakeSandbox` still exist. |
| `append` | Raw log write (used by adapters). |

`InMemory` also: `create_user` / `create_agent` / `create_session` / `session` / `user` / `agent` / `set_user_config` / `set_agent_config`. `Store` trait is the create/get subset.

`NullProvisioner::provision` → `session.bind`. No process.

## Runtime

`Runtime` is the long-lived keeper that holds Sessions so an Agent can keep waking on user queries, timers, and triggers. Same class as `Store` / `Provisioner` / `Host` / `FakeSandbox` — **not** a seventh product name. Binding remains disposable.

- `keep(session, agent, host)` records a Host recipe and occupancy (`Member::Agent` if needed). Does **not** bind. Overwrite updates the recipe (host/agent).
- `release` stops keeping. Session, Agent, Places, and the log survive. Bindings are not deleted.
- `classify` is find_server on the kept agent's Binding: `LiveIdle` / `LiveInTurn` / `PositivelyDead` / `Unknown` / `NeverBound`. Missing heartbeat is `Unknown`, not death. Wait is `Error::Waiting(WaitReason)`, not a Queue type.
- `remint` is the idle path. Refused on `LiveIdle` / `LiveInTurn` (`AlreadyLive`) and on `Unknown`. Allowed on `NeverBound` / `PositivelyDead` (`BindingReleased` on the log, no live Binding).
- `wake` requires a kept session (`Error::NotKept`). Ensures the agent is a member, then:
  - `LiveInTurn` / `Unknown` → `Waiting`. No remint. No `turn_start`.
  - `NeverBound` / `PositivelyDead` → remint from the stored Host (`session.bind_host`).
  - `LiveIdle` (or after remint) → `UserQuery` is `user_message` (user must occupy the room). `Timer` / `Trigger` append `EventBody::Wake`. Then `turn_start(agent)`.
  - Does **not** `turn_end` — the host runner ends the turn later. Does not spawn a CLI or invent `AgentMessage`.
- `schedule(every_ms)` sets `next_due_ms = now + every_ms`. `tick(now)` wakes due sessions (`next_due_ms <= now`) then reschedules `now + every_ms`. Pass `now` (epoch ms, same clock as `Event.ts`) so tests drive the loop without sleeping.
- `Wake` is not model-visible (`surface` omits it, like membership / turn / bind / `StepCompleted`).
- `begin_turn(session, holder)` opens (or attaches to) a turn under a `SessionTurnLease` (holder + fence). Remints Binding only on `NeverBound` / `PositivelyDead`. Live holder → `LiveTurnOpen`. An interrupted lease must `resume_interrupted_turn`.
- `end_turn(session, holder, fence, interrupted)` — `false` appends `TurnEnd` and clears the lease; `true` drops the live holder and leaves the turn open on the EventLog (host crash).
- `durable_step(session, holder, fence, turn_id, step_id, f)` — requires a live lease (fence match) and an open turn. If `StepCompleted` for `(turn_id, step_id)` is already on the Session EventLog → `Memoized` (do not run `f`). Else run `f`, append `StepCompleted`, return `Executed`. `(session, turn_id, step_id)` has at most one successful side-effect.
- `resume_interrupted_turn(session, new_holder)` — after host crash: refuse if a live holder (`LiveTurnOpen`); restore the open turn from the EventLog; reclaim the lease with a new fence; return `(turn_id, fence, binding_id)`. Does **not** remint Binding. Re-enter the turn from the top; memoization skips completed steps.
- Holder / fence / `SessionTurnLease` are Runtime bookkeeping, not a seventh product name. No Queue type.
- InMemory EventLog is append-then-return (no fsync). A parked durable EventLog should fsync `StepCompleted` before `Executed` is returned.

### Grok test path

`HostKind::Grok` (`"grok"`) is a Binding kind like Goose — not a seventh kernel name, not an Environment. Production `hearth-service` keep stays Goose.

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

## Join / leave / unbind / place

- `join` is occupancy only — no Binding, no Place.
- Agent `leave` unbinds **that agent's** Bindings only (`Binding.agent == Some(id)`). Bindings with `agent: None` stay.
- Last agent leave does **not** delete Session, log, Places, or host files.
- `unbind` drops a Binding pointer. Places survive.
- `FakeSandbox::release(sandbox_id)` is the dedicated host-file drop. Not implied by leave or unbind.

Shared host folder/VM is still `Binding.sandbox_id` (option A: two Bindings, same id, different `kind`; option B: same id, distinct `native_resume_id`, tools applied to `FakeSandbox`). `Place` is the session locator (`provider`/`instance`), not those files.

## Environment vs Place

**Environment** (Claude Managed Agents / Tag recipe: isolation, runtime image, recycle) is **not** a kernel type. Adapters map it to `Binding.sandbox_id` + `kind`. Recycle = unbind old Binding, bind a new one on the same Session, replay `surface`.

**Place** is the sixth kernel type: attach locator on the Session. It outlives Bindings. It is not an Environment recipe and does not store files.

`FakeSandbox` is host map (`sandbox_id → path → text`), not a Binding and not Place.

## Refused kernel types

Channel, Thread, Issue, Squad, **Environment**.

Claude Tag **channel** and Multica **Issue** are Sessions in adapters. MA **Session** is Binding plus a turn, not `hearth::Session`.

## Layout

```
crates/hearth/           kernel (lib.rs, host.rs, place.rs, sandbox.rs, runtime.rs)
crates/hearth-paseo/     Paseo name map (no daemon / worktree supervisor)
crates/hearth-managed/   MA / Tag name map
crates/hearth-service/   local HTTP + WebSocket
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
- Adapters (`hearth-paseo`, `hearth-managed`) convert names; they do not embed those products.


## Three parts of a host bind

Adding OpenCode, Goose, Dsh, and the others is three pieces. **Host is not a seventh kernel name.**

1. **User params** — what `bind` / `bind_agent` already take: optional `agent`, `kind`, `native_resume_id`, `sandbox_id`.
2. **Host type** — a real Rust type constructed when `HostKind::Goose` (etc.) is passed: `Host::Goose(ticket)`. Known hosts share `HostTicket` (`native_resume_id`, `sandbox_id`). `HostKind::host` and `Host::from_bind` build it. Adapter strings (`paseo-cli`, …) become `Host::Other`. Not an Agent subclass.
3. **Binding** — the record stored on the Session. Product `kind` stays a string (`Goose` → `"goose"`, `Grok` → `"grok"`). `Host::into_binding` mints the `BindingId` and copies fields.

`bind` / `bind_agent` keep the same public signatures and go `(1) → (2) → (3)`. The typed path is `session.bind_host(Some(id), HostKind::Goose.host(None, Some("box".into())))`.

## Adding a host

OpenCode, Goose, Grok, DeepSeek Harness, and the others are **Binding kinds** (via a typed `Host` ticket), not new Agent types. One `Agent` identity can bind as `opencode` in one session and `grok` in another.

1. Add one line to the `hosts!` list in `host.rs` (`HostKind` + `Host` + `as_str`). Or pass a raw string → `Host::Other`. `dsh` is DeepSeek Harness. `grok` is the xAI ticket (see Grok test path).
2. `store.create_agent(...)` then `join` then `bind_agent(id, HostKind::OpenCode, resume, sandbox)` or `bind_host(Some(id), HostKind::OpenCode.host(resume, sandbox))`.
3. Spawn and resume stay in a host crate / `Provisioner`. The kernel only records the Binding.
4. Do not add an Agent subclass or a seventh kernel name.
