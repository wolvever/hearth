# catalog

ACP profile list. **Data only** — a profile resolves to a `LaunchSpec`
(program, args, env, `WireKind`). Nothing here spawns a process.
`AttachRunner` lives in `src/runner/` (slice 5).

| id | command | extends | codec | wire | caps note |
|----|---------|---------|-------|------|-----------|
| `copilot` | `copilot --acp` | `acp` | `AcpCodec` | `jsonl-rpc` | ACP defaults (`subagent: false`) |
| `cursor` | `cursor-agent acp` | `acp` | `AcpCodec` | `jsonl-rpc` | ACP defaults (`subagent: false`) |
| `claude-acp` | `claude-agent-acp` | `acp` | `AcpCodec` | `jsonl-rpc` | `CLAUDE_ACP` (`subagent: true`) |

Cursor, Copilot, and Claude-acp are not separate adapters. Add a row to
`builtin.toml` (`id`, `label`, `command`, `extends = "acp"`). Do not fork
`AcpCodec`. Capability overrides (today: Claude-acp `subagent`) are applied
in Rust via `apply_builtin_caps` / `CatalogProfile::caps` — not from TOML —
so Host fast-mode toggles and the generic ACP registry row stay separate.

ACP stdio is newline-delimited JSON-RPC (`jsonl-rpc`: one line, one JSON
value). Profiles do **not** use `jsonrpc-content-length`, and they do not
enable Pi `rpc_chunk` reassembly (`JsonlRpcTransport::without_rpc_chunks`).

Resume reuses `HostAttach::resume` and `Binding.native_resume_id`. This
catalog does not encode Gemini-style `session/load`. Live capability gating
(Copilot advertises `loadSession` but not `resume`) is `runner::LiveCaps` +
fail-closed remint, not this catalog.

```bash
cargo test -p hearth-agent-wire catalog::
```
