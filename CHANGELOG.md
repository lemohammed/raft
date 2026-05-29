# Changelog

All notable changes to `raft` are documented here. This project tracks a
simple `MAJOR.MINOR.PATCH` version in `Cargo.toml`; `raft --version` reports the
running binary so agents can confirm which image is live after an install swap.

## [Unreleased]

Agent-experience pass: tighten the machine-readable contract so agents that
shell out to `raft` can branch on results reliably.

### Added

- Open asks now carry `awaited_live`, the awaited agent's heartbeat liveness,
  everywhere they surface (`awaiting`, `me`'s `you_owe`/`owed_to_you`, and the
  `wait --owed`/`--resolved` resolution envelope). An agent blocked waiting on a
  delegate previously had to join its `owed_to_you` list against `me`'s separate
  `live_peers` in the shell to answer the one question that decides whether to
  escalate: "is my delegate dead, or just slow?" The signal is now inline — a
  `done` that will never come because the worker's process exited reads as
  `awaited_live: false`, and text output flags it as `@agent (offline)`.
- `search` gained structured filters — `--from`, `--kind`, and `--mentions` —
  that combine conjunctively with the existing substring pattern and
  `--since`/`--conversation`/`--channel`/`--limit`. The pattern is now optional,
  but at least one criterion is required, so `search --agent x` with no filters
  is rejected rather than silently dumping the whole bus. `--mentions` matches
  both `@mention` tokens and `to[]` recipients. This closes the last read-path
  hole that forced an agent to pull a broad result set and post-process it in
  the shell just to answer "what did bob send me" or "show me the asks."
- `ack` now reports whether it actually closed an ask, the worker-side
  counterpart to `wait --owed`. The success envelope carries `was_awaited` (the
  acking agent is in the message's awaited set) and `closed_ask` (this ack just
  transitioned an open ask to closed: a terminal `done`/`rejected` by an awaited
  agent that had not already recorded a terminal receipt). Previously `ack done`
  exited `0` and printed `done <id>` even when it closed nothing — a `done` that
  landed on the wrong message id, on a non-ask, or from a non-awaited agent
  silently left the asker's `wait --owed`/`awaiting` blocked forever, the worst
  failure mode for an autonomous loop. A new `ack --require-open` flag turns that
  silent no-op into a hard `not_awaited` error (carrying the message's `awaited`
  set), so an agent can guarantee its acknowledgement discharged a real
  obligation. Plain (non-strict) acks stay permissive so recording a receipt on
  any visible message still works. Text mode appends ` (closed ask)` when an ask
  closes.
- `wait --owed` and `wait --resolved <message-id>`: block until an ask the agent
  *sent* closes, the asker-side counterpart to waiting for an unread message.
  Acks are receipts, not messages, so a plain `wait` never wakes when an awaited
  agent records a terminal `done`/`rejected` — an agent that delegated work and
  needed to block on the result had to busy-poll `awaiting`/`me` in a shell loop.
  `--owed` watches every open ask the agent owns and wakes on the first to close;
  `--resolved <message-id>` watches one specific ask and reports it immediately
  if already closed (erroring `not_found` if the id is not an ask the agent
  owns). Both report the resolved ask (`message_id`, `conversation_id`,
  `awaited`, `status`, `note`, `subject`), emit `{"ok":true,"resolved":…}` under
  `--json` (`resolved` is `null` when nothing is open), and exit `2` on timeout.
- `inbox`/`show`/`wait`/`watch` (`--json`) now decorate each message with three
  viewer-relative fields — `unread`, `awaiting_me`, and `my_status` — so an agent
  can answer "what's new?" and "what do I still owe a reply to?" from a single
  call. Previously the JSON path emitted the raw message and dropped the unread
  bit the text path already computed, forcing the agent into an `awaiting`
  bus-scan plus a `receipts` call per message to reconstruct the same signals.
  `awaiting_me` is true when the viewer is in the message's still-open awaited
  set (it set `requires_ack` or named the viewer in `needs_response_from`) and
  the viewer has not recorded a terminal `done`/`rejected` receipt; `my_status`
  is the viewer's current ack status or `null`. A new `inbox --needs-action`
  filter narrows the inbox to messages that are `unread` or `awaiting_me` — the
  agent's actionable queue. (`read` keeps the raw message shape.)
- `conversation remove <id> --agent <name>` and `channel leave <ch> --agent
  <name>`: the lifecycle counterpart to `conversation add`/`channel join`.
  Until now a participant set could only grow — an agent that finished its part
  of a room stayed listed forever, kept appearing as a valid recipient, and
  could still send. Both commands are idempotent (`removed`/`left` is `false`
  on a repeat), refuse to remove the last participant (which would orphan the
  room), and reject cross-type usage (`channel leave` on a private conversation
  or `conversation remove` on a channel points the caller at the right
  command). A removed agent can no longer `send`/`reply` until re-added. They
  write a system message recording the departure and support `--json`
  (`conversation remove` returns `removed` + `participants`; `channel leave`
  returns `left` + `members`).
- `not_found` errors for a mistyped conversation or channel id now carry
  nearest-match `suggestions` (by edit distance, closest first, capped at three
  and omitted when nothing is close), so a typo'd `send`, `channel join`, or
  `conversation add` hints at the intended id without a `channel list`/`me`
  round-trip. Read commands that treat an empty result as success (`show`,
  `search`) are unchanged.
- `not_participant` error envelopes now carry the conversation's valid
  `participants` alongside `code`/`message`, so a rejected `send`/`reply` tells
  the agent who *can* be addressed (and who to `conversation add`) without a
  second `show`/`status` round-trip. Error envelopes gained an optional
  structured-detail channel; unrelated errors stay lean.
- `conversation add <id> --agent <name>`: add a participant to a conversation
  that already exists. Channels had `channel join`, but a private/group
  conversation's participant set was frozen at creation — looping in a third
  agent (e.g. a reviewer) meant recreating the conversation under a new id and
  losing its thread history. The command is idempotent (`added` is `false` on a
  repeat) and reports the resulting `participants`; it rejects channels
  (pointing the caller at `channel join`) and returns `not_found` for a missing
  conversation. Supports `--json`.
- `raft --help` (long help) now opens with a TYPICAL AGENT FLOW section
  (claim → me → reply --ack → awaiting → roster → channel list), orienting a
  first-time agent toward the high-level commands instead of the raw subcommand
  list. The `conflict` error-code description was also broadened to match
  reality (it covers channel/conversation conflicts, not just claimed names).
- `reply <message-id>`: respond to a message without restating its context.
  It inherits the parent's conversation, threads the response (`after` points at
  the parent), inherits the subject, and defaults the recipient to the original
  sender — overridable via `--to`/`--subject`. Replaces the three-flag
  `send --conversation … --to … --after …` dance that replying previously
  required. Supports `--requires-ack`, `--needs-response-from`, and `--json`
  (the envelope adds `after`). `--ack <status>` (with optional `--ack-note`)
  also records an acknowledgement receipt on the parent in the same call, so
  `--ack done` answers and closes an ask at once; an invalid status is rejected
  before the reply is sent.
- `roster` now reports each agent's advertised `capabilities`, and a
  `--capability <tag>` filter narrows the roster to agents offering a given
  skill — so an agent can discover a live peer to delegate to without dumping
  full `status`. Capabilities also appear in the text roster as `{tag,tag}`.
- `channel list`: enumerate the channels on the bus so an agent can discover
  rooms to join instead of having to learn channel names out of band. Reports
  each channel's members and message count; with `--agent`, annotates whether
  that agent has joined and its unread count. Supports `--json` (bare array).
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
- README documents the success output shape of every `--json` command: which
  commands return the `{"ok":true, ...}` mutating envelope versus bare read data
  (arrays of messages, single messages, NDJSON streams, or structured objects),
  so a caller can pick the right parse path per command.

### Changed

- `wait`/`watch` wake on filesystem events via `notify` instead of pure
  polling, falling back to interval polling when a watch cannot be established.
- `ack --status` now validates against a fixed set (`received`, `accepted`,
  `working`, `blocked`, `done`, `rejected`) and rejects anything else. Previously
  any id-shaped string was accepted, but only `done`/`rejected` close an open
  ask, so a typo like `--status finished` silently left the ask open. The
  recognized statuses and which ones are terminal are documented in `ack --help`.

### Fixed

- `reply --ack` is now atomic. It previously sent the reply under one
  conversation lock, then re-acquired the lock to write the ack receipt — so a
  lock-acquisition failure (or a crash) between the two left the reply
  delivered but the ask still open, while the command exited non-zero. An agent
  retrying on that non-zero exit would send a duplicate reply. The reply-send
  and the receipt now run under a single lock, so a lock failure aborts before
  anything is written (no half-sent reply) and no other writer can interleave.
- Message-id collisions under rapid succession: ids now mix process id and a
  monotonic counter so two sends within the same microsecond no longer overwrite
  each other.
- Orphaned atomic-write temp files: a crash between an atomic write's create and
  rename left a dot-prefixed `.tmp` sibling behind forever. `gc` (and therefore
  `serve`) now reaps `.tmp` files older than 5 minutes — old enough to never
  touch an in-flight write — and reports the count as `orphan_temp_files`.
  `doctor` warns about them under the `orphan_temp_file` code.
- Heartbeat-watch startup signal race: `heartbeat --watch` published its pid and
  released the startup lock before installing its `SIGTERM`/`SIGINT` handlers, so
  a stop signal arriving during init hit the default disposition and killed the
  watcher mid-startup (no `shutdown_at` recorded, non-zero exit). Handlers are now
  registered before the pid is published, so an early stop is always a graceful
  shutdown.
- Lock-reap race: `gc` and lock acquisition judged a lock stale and then deleted
  it by path, so a lock that was refreshed or released-and-reacquired in the gap
  could be reaped out from under a live holder. Reaping now re-reads the owner
  token immediately before deleting and skips the lock if it was refreshed (now
  unexpired) or replaced (different token), so a lock within its lease is never
  reclaimed.
- Error-code accuracy: looking up a missing message (used by `ack`, `read`,
  `thread`, `receipts`) returned the generic `error` code despite a "not found"
  message; it now returns `not_found`. A message hidden from the caller now
  returns `not_participant` — including the `thread` visibility check, which had
  kept the generic code. Re-creating an existing channel or conversation without
  `--if-missing` now returns `conflict` instead of `error`, as does refusing to
  start a second `heartbeat --watch` while one is already live.

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
