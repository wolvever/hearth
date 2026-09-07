# hearth-service

Local HTTP + WebSocket on 0.0.0.0:8787. Bind address only; no public URL is claimed.

## HTTP

- POST /sessions {"folder":"/optional/existing/path"}
- GET /sessions/:id
- POST /sessions/:id/join {"user":"cheng"} or {"agent":"tag"}
- POST /sessions/:id/events {"user":"cheng","message":"..."} (text is an alias). Goes through Runtime::wake. 409 if the turn is open; the log is unchanged.
- GET /sessions/:id/events
- GET /agents/local PATH lookup only. --version only if the binary exists. Missing CLIs have path null. No session CLI is spawned.

## WebSocket steer

GET /sessions/:id/stream (upgrade). Log replay, then live events. Incoming JSON text:

  {"type":"join","user":"cheng"}
  {"type":"join","agent":"tag"}
  {"type":"message","user":"cheng","message":"hello"}

HTTP-shaped bodies also work: user-only joins; user plus message or text steers. type aliases: user, steer (same as message).

Outgoing JSON matches HTTP events: seq plus body (join or user:<text>). Join an agent so the session is kept. HTTP and WS steer share admit. A second prompt while the turn is open is 409 and is not appended.

## Local CLI discovery
GET /agents/local looks up names on PATH and runs --version only when a file is found.
Shims from a prefix install live in that folder under node_modules .bin (claude or codex). Put those .bin dirs on PATH so the probe can see them.
Node 22 is not required to build or run this service. Missing tools stay listed without a path; nothing is started for a session.
