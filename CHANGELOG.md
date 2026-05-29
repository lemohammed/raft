# Changelog

All notable changes to `raft` are documented here. This project tracks a
simple `MAJOR.MINOR.PATCH` version in `Cargo.toml`; `raft --version` reports the
running binary so agents can confirm which image is live after an install swap.

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
