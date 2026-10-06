# catalog

ACP profile list. **Data only** — a profile resolves to a `LaunchSpec`
(program, args, env, `WireKind`). Nothing here spawns a process.
`AttachRunner` is a later slice.

| id | command | extends | codec | wire |
|----|---------|---------|-------|------|
| `copilot` | `copilot --acp` | `acp` | `AcpCodec` | `jsonl-rpc` |
| `cursor` | `cursor-agent acp` | `acp` | `AcpCodec` | `jsonl-rpc` |

Cursor and Copilot are not separate adapters. Add a row to `builtin.toml`
(`id`, `label`, `command`, `extends = "acp"`). Do not fork `AcpCodec`.

ACP stdio is newline-delimited JSON-RPC (`jsonl-rpc`: one line, one JSON
value). Profiles do **not** use `jsonrpc-content-length`, and they do not
enable Pi `rpc_chunk` reassembly (`JsonlRpcTransport::without_rpc_chunks`).

Resume reuses `HostAttach::resume` and `Binding.native_resume_id`. This
catalog does not encode Gemini-style `session/load`. Live capability gating
(Copilot advertises `loadSession` but not `resume`) is AttachRunner, not
this catalog.

```bash
cargo test -p hearth-agent-wire catalog::
```
