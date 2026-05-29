# claude-004 — review of `raft ui`

Reviewer: claude (planner/auditor). Subject: codex's `raft ui` feature (embedded
HTTP server + single-page app on 127.0.0.1:7420). Method: full static read of
`cmd_ui` / `handle_ui_request` / `handle_ui_post` / `api_*` / `UI_HTML`, plus a
live smoke test against a throwaway bus on port 7421 with curl probes.

## Verdict

Strong feature. The architecture is right: one hand-rolled HTTP server, no new
deps, all writes funnel through the existing `send_message` core so locking,
turn enforcement, and rate limits are reused rather than reimplemented. The
client is XSS-safe by construction. **One critical security gap must be fixed
before this is safe to leave running: there is no Host/Origin validation, so any
website the user visits can drive the local bus (CSRF / DNS-rebinding).**

## CRITICAL — CSRF / DNS-rebinding: unauthenticated cross-origin writes

Binding to 127.0.0.1 stops other *hosts* from connecting, but it does **not**
stop other *origins in the user's own browser*. Any page the user visits can
`fetch('http://127.0.0.1:7420/api/send', {method:'POST', ...})`. With a DNS-
rebinding attack the Host header can even be an attacker domain. There is no
Origin check, no Host allowlist, and no CSRF token, so these requests are
honored.

Proven on the live server (port 7421, spoofed headers):

```
POST /api/send   Host: evil.example.com   Origin: http://evil.example.com
body: {"agent":"bob","conversation":"general","to":"alice","body":"csrf-write-from-evil-origin"}
=> 200 {"ok":true,"message_id":"m-...18b3e71ff503"}   # message landed on the bus
```

`GET /api/snapshot` with the same spoofed Host also returns 200 with full bus
contents — so a malicious page can both **read** every conversation and **write**
as any agent. The only thing that blocked an earlier probe was the turn model
(alice didn't hold the turn) — that is incidental application logic, not a
security control.

Fix (small, no new deps):
1. On every request, reject unless the `Host` header is in an allowlist
   (`127.0.0.1:<port>`, `localhost:<port>`). This kills DNS-rebinding.
2. On every `POST /api/*`, require `Origin` (or `Referer`) to match the bound
   origin; reject otherwise. This kills cross-origin CSRF.
3. Optional hardening: a per-process random token printed at startup and
   required as a header on writes.

## HIGH — TCP exposure weakens the 0700 same-user privacy model

The on-disk bus is protected by `0700` dirs: only the same OS user can read it.
`raft ui` opens a TCP socket that serves that same content to anything that can
reach the port. Even with the Host/Origin fixes above, any *other local process*
running as a different user that can reach the loopback port, or any local
software with a browser engine, widens the trust boundary beyond "same Unix
user." Worth a docs note that `raft ui` is a localhost-only dev convenience and
should not run unattended, plus the token in (3) above to restore a real
authn boundary.

## MEDIUM — `claim --workspace` error message is unhelpful

`raft claim --workspace /tmp/a` fails with bare `No such file or directory (os
error 2)` when the workspace path doesn't exist. It should say which path and
which flag, e.g. `--workspace path does not exist: /tmp/a`. Hit this during
setup; pure ergonomics, not UI-specific, but worth folding in.

## What's solid (keep as-is)

- **Write boundary**: `api_send` deserializes a `UiSendRequest` and calls the
  same `send_message(root, input)` the CLI uses — `DirLock`, turn enforcement,
  rate limit, atomic write, turn-pass all reused. No bypass path. `api_open` /
  `api_channel` / `api_join` likewise delegate to the existing helpers. This is
  exactly right.
- **XSS-safe rendering**: zero `innerHTML` in `UI_HTML`. Message bodies render
  via `<pre>` + `textContent`; agent/conversation labels via `textContent`.
  A stored `<img onerror>`/`<script>` payload is data, never markup. (I seeded
  one in the smoke bus; static analysis confirms it cannot execute. Could not do
  the live visual confirmation — the browser extension wasn't connected — but the
  textContent-only path makes execution impossible regardless.)
- **Robust request reader**: 2s read timeout, 1MB body cap, content-length
  parsing — won't hang the single-threaded accept loop on a slow/oversized
  client.
- **Responsive + accessible**: 3-col grid with sensible 1080px/760px
  breakpoints, real `<button type="button">` elements (keyboard-focusable),
  visible focus styles.

## Minor / nits

- Error surfacing uses `alert(error.message)` — fine for a dev tool, but a small
  inline toast/banner would be less disruptive and easier to read.
- `setInterval(loadSnapshot, 5000)` polls every 5s with no backoff; if the
  server dies the client will alert-spam. Consider pausing polling after N
  consecutive failures.

## Bottom line

Ship the architecture as designed. Block on the **CRITICAL Host/Origin fix**
before this runs unattended; fold in the token for a real authn boundary, and
the `claim` error-message polish if cheap. Everything else is a nice-to-have.
