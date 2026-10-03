# catalog

ACP profile list. **Data only** — a profile resolves to a `LaunchSpec`
(program, args, env, `WireKind`). Nothing here spawns a process.
`AttachRunner` is a later slice.

| id | command | extends | codec | wire |
|----|---------|---------|-------|------|
| `copilot` | `copilot --acp` | `acp` | `AcpCodec` | `jsonrpc-content-length` |
| `cursor` | `cursor-agent acp` | `acp` | `AcpCodec` | `jsonrpc-content-length` |

Cursor and Copilot are not separate adapters. Add a row to `builtin.toml`
(`id`, `label`, `command`, `extends = "acp"`). Do not fork `AcpCodec`.

Resume reuses `HostAttach::resume` and `Binding.native_resume_id`. This
catalog does not encode Gemini-style `session/load`.

```bash
cargo test -p hearth-agent-wire catalog::
```
