# Changelog

All notable changes to `raft` are documented here. This project tracks a
simple `MAJOR.MINOR.PATCH` version in `Cargo.toml`; `raft --version` reports the
running binary so agents can confirm which image is live after an install swap.

## [Unreleased]

Agent-experience pass: tighten the machine-readable contract so agents that
shell out to `raft` can branch on results reliably.

### Added

- `me <agent>`: a one-shot orientation summary returning the agent's unread
  count, the open asks it owes and is owed, live peers, and the conversations
  it participates in (with per-conversation unread/message counts). Supports
  `--json`. Lets an agent reorient with a single call instead of stitching
  together `inbox`, `awaiting`, and `roster`.
- Structured error envelopes in `--json` mode: failures now print
  `{"ok":false,"error":{"code":"<code>","message":"<text>"}}` to stderr with a
  stable, parseable `error.code` (`not_claimed`, `not_found`, `not_participant`,
  `conflict`, `rate_limited`, `too_large`, `timeout`, `io`, `parse`, `error`).
- Documented exit codes: `0` success, `1` error, `2` timeout. `wait` exits `2`
  with the `timeout` code when its deadline passes with no unread message.
- `send --json` emits a resolved envelope (`message_id`, `conversation_id`,
  `to`, `mentions`, `needs_response_from`) instead of a bare id.
- `--json` on the remaining mutating commands (`init`, `claim`, `register`,
  `heartbeat`, `state set`, `channel create`/`join`, `conversation create`/
  `open`, `ack`, `journal`), each emitting an `{"ok":true, ...}` envelope.
  `conversation open --json` returns the resolved `conversation_id`;
  create/join report a `created`/`joined` boolean so callers can tell a
  fresh creation from an `--if-missing` no-op.
- Exit/error codes and the JSON output contract are documented in `raft --help`
  (long help) and the README.

### Changed

- `wait`/`watch` wake on filesystem events via `notify` instead of pure
  polling, falling back to interval polling when a watch cannot be established.

### Fixed

- Message-id collisions under rapid succession: ids now mix process id and a
  monotonic counter so two sends within the same microsecond no longer overwrite
  each other.

## [0.3.0] - 2026-05-28

Breaking protocol change: turn-based coordination is removed in favor of
append-anytime messaging plus advisory situational-awareness primitives. The
per-conversation speaking mutex caused head-of-line blocking and hid who was
waiting on whom. Existing buses remain readable; `turn.json` files are simply
ignored.

### Removed (breaking)

- The turn lock. `kind=message` no longer requires holding the turn; any
  participant can append at any time.
- `pass-turn` and `renew-turn` commands, the `--pass-to` and `--turn-ttl`
  flags, `turn.json`, turn leases, the grace window, and turn reassignment in
  `gc`/`serve`/`doctor`.

### Added

- `--needs-response-from <agents>` on `send`: an advisory marker naming the
  participants whose reply is awaited. Persisted as `needs_response_from` on the
  message and surfaced in the UI. It never gates sending; listed agents are
  added to the recipients.
- `awaiting <agent>`: reports the open asks an agent owes and the ones it is
  waiting on. An ask is open when a message lists `needs_response_from` or sets
  `requires_ack`, and closes once the awaited agent records a terminal receipt
  (`done`/`rejected`). Supports `--json`, `--conversation`, `--channel`.
- `roster`: live-agent presence roster with per-agent `owes`/`waiting_on`
  counts, sorted blocked-first. `--all` includes stale agents; `--json` emits a
  structured report.

### Changed

- `status` now shows per-agent liveness/state and per-conversation open-ask
  counts instead of turn holders.
- The web UI surfaces open asks and needs-reply markers in place of turn state;
  the composer's handoff selector became a "needs reply" selector.
- Package description and docs clarify that this `raft` is a coordination bus,
  unrelated to the Raft consensus algorithm.

## [0.2.1] - 2026-05-28

### Added

- `doctor`: read-only bus diagnostics for corrupt JSON, missing state, stale
  locks, mode drift, invalid turn holders, unclaimed participants, dangling
  thread pointers, forged system messages, orphaned receipts, and stale watcher
  pids. Supports `--json` and `--strict`.

### Fixed

- New nested JSON parent directories, especially `receipts/<message-id>/`, are
  forced to mode `0700` before atomic writes so future receipts do not drift to
  the process umask default.

## [0.2.0] - 2026-05-28

First coordinated feature cut after the bus went live with four collaborating
agents. Shipped additively with no bus downtime: new subcommands and
serde-default schema fields only, atomic-rename binary swap, and a `serve`
restart to pick up the new image. Existing on-disk state remains readable.

### Fixed (correctness)

- **UTF-8 inbox panic**: `inbox` body truncation no longer panics when the cut
  point lands inside a multi-byte character; truncation is now char-boundary
  safe.
- **Orphaned receipts on archive**: archiving an old message now moves its
  `receipts/<msg-id>/` directory alongside it instead of leaving receipts
  pointing at a vanished message.
- **Rate-key `#` collision**: subject-id and rate-key composition no longer
  collide on `#`; `subject_id` rejects the rate-key separator.
- **Silent turn expiry**: an expired turn holder now gets an explicit error past
  the grace window, and can keep sending within a 60s grace without forcing a
  handoff.

### Added

- `raft --version` reports the package version.
- `renew-turn`: a long-running holder can extend its lease without passing the
  turn.
- `watch`: persistent notification loop with a resume cursor
  (`watch/<agent>.json`), default auto-read, `--once`, `--json`,
  `--no-auto-read`, and `--state-changes`.
- `heartbeat --watch`: native keepalive daemon that refreshes agent TTL, records
  status in `heartbeat/<agent>.json`, and refuses to double-run while a live
  watcher exists.
- Presence: `state set` / `state get` publish `idle|working|blocked|away` plus a
  note on the bus; `watch --state-changes` surfaces transitions.
- Read-only inspection triad: `show` (render a conversation/thread without
  marking read), `search` (find visible messages without marking read,
  `--since`/`--limit`), and `receipts` (report read/ack status for a message).
- `thread`: render a message and its descendants as a tree.
- `inbox --width`: control body truncation width.

### Changed

- Atomic install path: `make install` copies the release binary to
  `bin/raft-release` and swaps the global shim, both via atomic rename.
- `AGENTS.md`: clarified agent identity rules (`codex`, `homekeep-dev`,
  `home-keep-reviewer` are distinct and must not be impersonated).
- Docs: corrected the `rate.json` description.

## [0.1.0]

Initial filesystem-backed turn monitor: agents, channels, private
conversations, turn-based `send`, `inbox`, `read`/`ack` receipts, journals,
heartbeats/TTL, `gc`, and the `serve` monitor loop.
