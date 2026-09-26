# Contributing an adapter

Goal: add Claude Code / Cursor / etc. in an afternoon without a seventh Hearth noun.

1. **Pick a transport** (do not invent a line splitter):
   - stdio ACP / LSP / app-server → `JsonRpcFramer`
   - EventSource / SSE → `SseFramer`
   - one WebSocket text message → `WsJsonFramer`
2. **Add** `src/adapters/<name>/` with `README.md`, `fixtures/*.json`, and `map_notification` or `map_event` (`Value` → `AgentEvent`).
3. **Wire** `AgentKind`, `adapters::map_native`, `host::binding_kind` (Binding kind string; no remint).
4. **Tests**: one fixture per mapped event you care about; plus a `FramedAgent::push_bytes` through the framer if the wire is easy to encode (`encode_jsonrpc` / `encode_sse`).
5. **Unknown methods** → `AgentEvent::Native`. Do not fail the stream.
6. **Compact** stays Host-side `compact_with_handoff` (Place `handoff.md`) then `AgentCommand::Compact`.

Fail closed in Transport. Keep Adapter pure JSON. No Queue type. SoftExpiring and Flush-before-dispatch stay parked.
