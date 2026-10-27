# hearth-service

Local HTTP + WebSocket on 0.0.0.0:8787. Bind address only; no public URL is claimed.

## HTTP

- POST /sessions {"folder":"/optional/existing/path"}
- GET /sessions/:id
- POST /sessions/:id/join {"user":"cheng"} or {"agent":"tag"} or {"agent":"tag","parallel_safe":["path/a"]} (optional path list → Runtime::set_parallel_safe after keep; omit keeps fail-closed serialize)
- POST /sessions/:id/events {"user":"cheng","message":"..."} (text is an alias). When Host wires are installed for the Session, steers through wake_each_kept_through_host_wake (per-Agent host_wake + one shared UserMessage; Service-host-wake-each). Binding.live_caps that advertise no resume (Service-livecaps-refuse under #71+#69) typed-refuse ResumeNotSupported / LoadFallbackBlocked on Fake proven-lost remint (FailClosed-skip that Agent; Service-wake-fanout-skip-retry continues peers — HTTP 200 when a peer remints; all-refuse still SteerError::Remint / HTTP 500; no mint / spawn / session/load on the refusing Agent; limited remint retry budget 3 on resume-capable Bindings). Real AttachRunner remint floors install via AgentHostWire::real_exited (Service-real-cli-concurrent-remint / Service-real-cli-concurrent-permission-remint with mid-turn Session/Host pending asks) or AgentHostWire::real_stdout_eof (Service-real-stdout-eof-concurrent-remint Soft twin / Service-real-stdout-eof-concurrent-permission-remint Soft twin under #74 with mid-turn Session/Host pending asks) — no Fake Exited / StdoutEof injection. Otherwise Goose stand-in uses Runtime::wake_each_kept. 409 if every Agent waits; the log is unchanged.
- GET /sessions/:id/events
- GET /agents/local PATH lookup only. --version only if the binary exists. Missing CLIs have path null. No session CLI is spawned.

## WebSocket steer

GET /sessions/:id/stream (upgrade). Log replay, then live events. Incoming JSON text:

  {"type":"join","user":"cheng"}
  {"type":"join","agent":"tag"}
  {"type":"join","agent":"tag","parallel_safe":["path/a"]}
  {"type":"message","user":"cheng","message":"hello"}

HTTP-shaped bodies also work: user-only joins; user plus message or text steers. type aliases: user, steer (same as message).

Outgoing JSON matches HTTP events: seq plus body (join or user:<text>). Join an agent so the session is kept. HTTP and WS steer share admit. A second prompt while the turn is open is 409 and is not appended.

## Local CLI discovery
GET /agents/local looks up names on PATH and runs --version only when a file is found.
Shims from a prefix install live in that folder under node_modules .bin (claude or codex). Put those .bin dirs on PATH so the probe can see them.
Node 22 is not required to build or run this service. Missing tools stay listed without a path; nothing is started for a session.
