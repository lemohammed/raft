# Changelog

All notable changes to `raft` are documented here. This project tracks a
simple `MAJOR.MINOR.PATCH` version in `Cargo.toml`; `raft --version` reports the
running binary so agents can confirm which image is live after an install swap.

## [Unreleased]

Agent-experience pass: tighten the machine-readable contract so agents that
shell out to `raft` can branch on results reliably.

### Fixed

- `status` no longer over-counts a conversation's `messages`. It counted every
  entry in `messages/`, including a crash-orphaned `.tmp` sibling left by an
  interrupted `atomic_write_json`, so `status` could report more messages than
  `me`, `channel list`, or the `ui` snapshot for the same room (every other path
  filters to `*.json`). `status` now filters by extension too, so its count
  agrees with the rest.
- `withdraw` no longer lists — or notifies — recipients who already discharged
  the ask. `released[]` was computed from `message_awaited`, which returns every
  awaited agent without consulting receipts, so an agent that already acked
  `done`/`rejected` appeared in `released[]` and received an "ask withdrawn, stop
  in-flight work" notice for work it had already completed (contradicting its own
  receipt and inviting it to discard finished output). `released` now filters out
  terminal-receipt agents, mirroring `gather_open_asks`; if every recipient has
  already responded, withdraw reports `not_found` (nothing left to withdraw).
- Rejoining a room (`channel join` / `conversation add` after a `leave`/`remove`)
  no longer overwrites the agent's `joined_at` membership baseline. `remove`
  preserves `joined_at`, but the join handlers re-inserted it as `now`, pushing
  the baseline past any ask created during the prior membership — so
  `predates_membership` hid the still-unanswered obligation and `message_awaited`
  dropped the agent. An agent that simply disconnected and reconnected silently
  discharged owed work: the ask vanished from the asker's `owed_to_you`, the
  agent's `you_owe`, and `wait --owed`, with no terminal receipt or withdrawal.
  The baseline is now stamped only on first join and preserved across rejoins,
  so an open ask correctly reopens when the agent returns.
- `wait <asker> --resolved <id>` no longer reports a multi-recipient ask as
  resolved the instant the *first* awaited agent answers. For an ask delegated
  to several agents (`--needs-response-from a,b` or a broadcast `--requires-ack`),
  the blocking loop walked a snapshot of the open recipients and returned on the
  first to record a terminal receipt — so an asker waiting on work owed by
  `a,b` got back `{"resolved":{"awaited":"a","status":"done"}}` while `b` still
  owed a reply. `--resolved <id>` now blocks until *every* awaited agent is
  terminal and reports an aggregate status (`rejected` if any recipient rejected,
  otherwise `done`); `wait --owed` keeps its first-to-close behavior.
- `send --kind event` (and `receipt`) no longer accepts `--requires-ack` or
  `--needs-response-from`. Only `message` carries obligation semantics
  (protocol: "Only this kind may use `--needs-response-from`"), but the flags
  were stored verbatim on any kind and `gather_open_asks` let `event` fall
  through into ask accounting — so an IM bridge relaying a human as an `event`
  fabricated a real open ask (`awaiting`/`me`/`roster`/`status`/`wait --owed`
  all reported it) that the bridge agent, which never runs `ack`, could never
  close. `send` now rejects these flags on non-`message` kinds, and
  `message_awaited` (the chokepoint for every ask-accounting path) treats any
  non-`message` row as opening no ask, disarming legacy events already on disk.
- Removing an agent from a room (`conversation remove` / `channel leave`) now
  releases any open ask still awaiting them. A removed agent cannot ack or reply
  (`write_receipt`/`send` reject non-participants), but the ask stayed open and
  unresolvable: the asker's `owed_to_you` reported the removed agent with a
  false `awaited_live:true`, `roster` kept counting the owes/waiting, the
  removed agent couldn't even see the obligation (`gather_open_asks` skips rooms
  it left), and `wait --owed` blocked until timeout on a reply that could never
  come. `message_awaited` now drops awaited agents who are no longer
  participants, so the obligation resolves the moment they leave (the removal
  already posts a `participant removed` system notice). When several agents were
  awaited, only the departed one is released; the rest stay open.
- `inbox --limit` now keeps the globally newest messages. `visible_messages`
  concatenates each conversation's messages in conversation-id order, so the
  merged list was sorted only *within* a room; `inbox` then kept the trailing
  `--limit` rows without a global sort, so it retained the newest messages of
  the last-sorting room and silently truncated genuinely newer messages from
  earlier-sorting rooms (and `--unread`/`--needs-action` inherited the same
  skew). `inbox` now sorts by message id before applying the limit, matching
  `show`, `search`, and `thread`.
- `search --mentions <id>` now matches `*` broadcasts. The filter compared the
  target only against literal `mentions[]`/`to[]` entries, but a message sent to
  `*` reaches every room member (and `message_visible_to` treats it that way),
  so an agent running `search --mentions me` silently missed every broadcast it
  had actually received — the opposite of the documented "matches both
  @mentions and to[] recipients". A `*` recipient now counts as reaching the
  target when the target is a participant of that conversation (membership is
  resolved per room, so a non-member is not spuriously matched).
- `thread --limit` now keeps the *newest* messages instead of the oldest. The
  renderer walked the `after` tree depth-first decrementing a shared budget, so
  once the limit was hit it dropped the highest-id (most recent) replies and
  kept the earliest — the opposite of `show`/`inbox`/`search`, which all retain
  the newest. An agent paging a long thread saw stale leading messages and
  silently lost the latest activity, with no signal anything was missing. The
  window now keeps the root plus the newest reachable replies (a dropped reply
  re-parents onto its nearest surviving ancestor so the tree stays connected),
  and the `--json` form reports `truncated`/`omitted` (text prints an
  omitted-count footer).
- `gc --archive` (and `serve --archive`) no longer archives an unresolved open
  ask out of every obligation view. Archival moved any message older than
  `retention_days` (default 14) into `archive/`, filtering on age alone — but
  the obligation views (`awaiting`/`me`/`roster`/`wait`) and the `ack`/
  `withdraw` mutators scan only the live `messages/` dir. So an ask that aged
  out before its awaited agents acked silently vanished: the worker's queue
  cleared, the asker's `owed_to_you`/`wait --owed` reported nothing owed (a
  false "resolved" signal), and `ack`/`withdraw` returned `not_found`. Archival
  now retains a message while it is still an open ask (any awaited agent lacks a
  terminal receipt); `withdraw` or a terminal ack lets it age out normally.
- A message that set both `--needs-response-from` and `--requires-ack` silently
  dropped the ack requirement: `message_awaited` picked exactly one source via
  `if/else if`, so a non-empty `needs_response_from` suppressed `requires_ack`
  entirely. An ask like "@b please reply, everyone ack" awaited only `b`; every
  other recipient owed nothing, was absent from `awaiting`/`me`/`wait --owed`,
  and the asker's `wait --owed` closed the moment `b` replied even though no one
  else had acked. The send envelope still echoed both flags as set, so the
  asker had no signal the requirement was dropped. The two obligation sources
  now union (deduped, same self/`*`/membership filters), and `await_kind` is
  computed per awaited agent (`needs_response` wins for an agent named in both,
  since a reply subsumes an ack).
- `watch` no longer silently drops an unread message whose id sorts below its
  resume cursor. Message ids are not monotonic across processes within a
  millisecond, but the watch loop used a scalar high-water cursor as its sole
  dedup and skipped anything `id <= cursor` — so a still-unread message that
  became visible after the cursor advanced (e.g. a second agent writing into the
  same channel in the same millisecond) was skipped forever. Under the default
  (auto-read) the dedup is now the read receipt, and an in-session set guards
  exact-once delivery regardless of id ordering; the persisted cursor is demoted
  to a soft resume floor that applies only to state-change notices and
  `--no-auto-read` (where no receipt exists). An explicit `--since` stays a hard
  floor.
- `read` (and `watch` auto-read) no longer downgrades an explicit ack. A `read`
  receipt is the weakest status, but `write_receipt` previously overwrote the
  current status unconditionally — so re-reading (or auto-reading via `watch`) a
  message you had already marked `done`/`rejected` silently reverted the receipt
  to `read`, reopening the closed ask and un-resolving the asker's `wait --owed`.
  A `read` now preserves any stronger existing status (and its note) while still
  recording the read in `read_at` and the receipt history.

### Added

- `state get` now reports liveness alongside the published state: `live`
  (heartbeat lease not expired, same check as `roster`/`me`) plus
  `last_seen_at` and `expires_at` in `--json`, and a `(stale)` marker in text.
  A crashed or exited agent leaves its last `current_state` on disk, so a bare
  `state get` previously presented e.g. `working` as authoritative with no way
  to tell it apart from a live agent's current state.
- Channels and conversations now record a per-member `joined_at` baseline, so a
  late joiner is no longer flooded with the room's entire pre-join history. A
  broadcast (`--to "*"`) is visible to every current member, and membership was
  checked only against the *current* participant list — so on `channel join`,
  every prior `*` message instantly became unread, and a pre-join `*` ask sent
  with `--requires-ack` even made the new member `awaiting_me` on work it never
  saw happen. `message_is_unread` and `message_awaited` now treat any message
  created before a member's `joined_at` as backlog: not unread, not owed. Rooms
  created before this field existed leave it empty, so their founding members
  are treated as present from the start (no behavior change for old buses).
- `read --json` now emits the viewer-relative message view (`unread`,
  `awaiting_me`, `my_status`) like `inbox`/`show`/`wait`/`watch`, instead of the
  raw message. Because a `read` receipt is non-terminal, `awaiting_me` still
  flags an ask the reader owes — so an agent learns from the `read` call itself
  that reading did not discharge its obligation, with no extra `awaiting` round
  trip. Text mode prints `awaiting: you still owe a reply/ack`.
- Every open ask (`awaiting`, `me`, `wait --owed`/`--resolved`) now carries
  `await_kind`: `"needs_response"` (the ask came from `--needs-response-from`, so
  the sender wants a substantive reply) or `"requires_ack"` (from
  `--requires-ack`, so a bare acknowledgement suffices). Previously the awaited
  agent could see only that it owed *something*, not whether to compose a reply
  or just ack, forcing a `show`/`receipts` round-trip to disambiguate. Text mode
  shows it as `wants reply` / `wants ack`.
- `reply` now reports `omitted_recipients[]`: the group/channel participants who
  were on the parent thread but are not reached by a bare reply (which defaults
  its audience to the parent's sender alone). Previously a reply in a multi-party
  thread silently answered only one person; the field — and a stderr warning in
  text mode — surfaces who was dropped so the sender can re-address with `--to`.
  Stays empty when `--to` is given explicitly, since that is a deliberate choice.
- `wait` (both the unread form and `--owed`/`--resolved`) now fails fast with
  `not_claimed` — carrying the same nearest-id `suggestions` — when the named
  agent has not been claimed, instead of blocking for the full `--timeout` and
  then exiting `2` (`timeout`). A typo'd id used to look exactly like a genuine
  wait that nothing answered; it now surfaces the mistake immediately and
  distinguishably.
- `not_claimed` errors now carry nearest-match agent-id `suggestions`, the same
  recovery affordance `not_found` already gives for conversation/channel ids. A
  mistyped agent id on `me`, `heartbeat`, `state set`/`get`, or `register`
  previously returned a bare "not claimed" string, leaving a brand-new agent
  that fat-fingered its own name to guess; it now gets `{"suggestions":[…]}`
  (closest first, omitted when nothing is close) and recovers in one shot.
- `me` now reports the agent's own heartbeat liveness as `live` (plus
  `expires_at`), and text mode prints a `STALE: … run 'raft heartbeat <id>'`
  banner when it has lapsed. raft computes liveness everywhere else only for
  *peers* (`offline_recipients`, `roster`'s `active`, `me`'s `live_peers`), so
  an agent whose heartbeat expired during a long tool call could orient with
  `me` and see nothing wrong — while every peer that asked it something saw
  `awaited_live: false` and blocked on a `wait --owed` reply the agent didn't
  know it looked too dead to be expected to send. Surfacing self-liveness at the
  documented orientation chokepoint closes that silent cross-agent deadlock.
- `ack` now carries `withdrawn` in its success envelope and its `not_awaited`
  error details: `null` normally, or the withdrawal record (`by`, `at`,
  `reason`) when the sender has retracted the ask. Because a withdrawn ask
  collapses the awaited set to empty, it reads as `was_awaited: false` — the
  same as a message the agent was never asked to answer. A worker that raced the
  sender's withdrawal therefore got a signal indistinguishable from
  never-awaited (and an opaque `--require-open` failure). The new field lets it
  tell "too late, it was withdrawn" — and why — from "this was never mine".
- `withdraw <message-id> --from <sender>` retracts an open ask the sender no
  longer needs answered. It stamps the message with a `withdrawn` marker so the
  ask drops out of every `awaited` view at once — the awaited agents' `you_owe`,
  the sender's `owed_to_you`, the roster owes/waiting-on counts, and any
  `wait --owed` blocked on it. Previously a sender who opened an ask had no way
  to take it back: a question that went moot, got solved another way, or was
  re-routed stayed pinned to everyone's obligation lists forever, with no path
  short of the recipient acking a reply that no longer made sense. Only the
  original sender may withdraw (a non-sender gets `not_found`, mirroring
  `wait --resolved`); withdrawing is idempotent; and the `--json` envelope
  returns `released[]` (the agents whose obligation was lifted), `withdrawn`,
  and `already_withdrawn`. Each released worker also receives a discoverable
  `ask withdrawn` system notice (surfaced through `inbox`/`show`/`thread`, like
  the existing `participant removed`/`channel left` notices) that names the ask
  and carries the withdrawal reason. This closes the asymmetry where the sender
  got `released[]` back but a worker who had already acked `working` only saw
  the ask vanish silently from its `you_owe`, unable to tell a withdrawal from a
  done-by-someone-else or a bug.
- `rate_limited` and `too_large` send errors now carry structured `details` in
  the `--json` error envelope. `rate_limited` adds `retry_after_seconds` (until
  the sender's window resets), `window_seconds`, `max_messages_per_sender`, and
  `count`; `too_large` adds `size` and `limit`. Previously an agent that hit
  either limit got only a human-readable message string and had to either
  regex it or busy-retry; it can now compute a precise backoff or trim to the
  exact byte bound without a second round-trip.
- `send`/`reply` now return `offline_recipients[]` — resolved recipients whose
  heartbeat has expired (a `*` recipient expands to participants; the sender is
  excluded). Previously a send to a crashed or expired peer returned a plain
  success envelope, so an agent that delegated an ask only discovered the peer
  was down later, by blocking on `wait` for a reply that would never come. The
  signal is now at send time, letting the sender reroute or escalate
  immediately. Text mode prints the same warning to stderr without disturbing
  the message id on stdout.
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

### Fixed

- `--help` now lists the complete set of valid values for every enumerated
  argument, so an agent can discover them without trial-and-error: the `away`
  agent state was missing from `state`/`state set` help, and the `ack` summary
  and `send --kind` help truncated their value lists with "...". A regression
  test asserts each subcommand's help enumerates the full set its validator
  accepts.

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
