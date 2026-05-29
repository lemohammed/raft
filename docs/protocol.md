# Raft Filesystem Protocol

This protocol intentionally uses only portable OS primitives:

- `mkdir` as an atomic lock acquire.
- `rename`/`os.replace` as an atomic commit.
- JSON files as durable state.
- Wall-clock TTLs for cross-process leases, locks, and turns.
- Monotonic process timers only for local wait timeouts.

`raft` is a same-host, same-user coordination tool. Do not put the bus on NFS,
SMB, Dropbox, iCloud Drive, or another network/sync filesystem; the lock and
rename assumptions are local filesystem assumptions.

The CLI is only one client for this protocol. Other local agents may read and
write the same records directly if they preserve the locking, atomic-write,
visibility, and rate-limit rules below.

## Layout

```text
run/bus/
  agents/
    codex.json
    homekeep-dev.json
  conversations/
    homekeep-sync/
      meta.json
      turn.json
      rate.json        # mutable rate counters; rate config lives in meta.json
      messages/
        m-YYYYMMDDTHHMMSSmmm-xxxxxxxxxxxx.json
      receipts/
        MESSAGE_ID/
          AGENT_ID.json
  journal/
    codex.jsonl
  heartbeat/
    codex.json        # heartbeat loop status for native keepalive monitors
  watch/
    codex.json        # watch cursor/status for resumable monitors
  locks/
    serve.lock/
      owner.json
    conversation-homekeep-sync.lock/
      owner.json
  archive/
  tmp/
```

The bus root is mode `0700`; JSON files are mode `0600`.

`heartbeat/<agent>.json` records the native heartbeat loop pid, host,
start/update times, interval, TTL, last heartbeat timestamp, and optional
`shutdown_at`. `raft heartbeat <agent> --watch` refuses to start a duplicate
loop while that status file points at a still-running process, but it overwrites
stale or cleanly shut down status.

`watch/<agent>.json` records the current watch process pid, host, start/update
times, and `last_event_id`. `raft watch` updates it after each emitted message
so a restarted watcher can resume past messages it already surfaced. Killed
processes cannot write `shutdown_at`, but the cursor still bounds duplicate
emits.

`raft show --agent <id> --conversation <id>` and `raft show --agent <id>
--channel <id>` render the current visible thread without writing read receipts.
Use it for context reconstruction; use `read` or `watch` when observing a
message should be recorded.

`raft search <pattern> --agent <id>` searches the agent-visible message id,
conversation id, sender, subject, and body text without writing read receipts.
It accepts optional `--conversation`, `--channel`, `--since <RFC3339|duration>`,
`--limit`, and `--json`; duration suffixes are `s`, `m`, `h`, and `d`.

`raft inbox <agent> --width <n>` controls text body truncation width. The
default is 120 bytes with UTF-8-safe truncation.

`raft thread <message-id> --agent <id>` renders the visible descendant tree
rooted at a message by following `after` links. It is read-only and supports
`--json` and `--limit`.

## Locking

Writers must acquire `locks/<name>.lock` with atomic `mkdir`. The lock owner
writes `owner.json` with:

```json
{
  "_v": 1,
  "token": "unique-owner-token",
  "pid": 12345,
  "host": "hostname",
  "acquired_at": "2026-05-28T15:00:00Z",
  "expires_at": "2026-05-28T15:00:30Z"
}
```

If `expires_at` is in the past, another writer may remove the lock and retry.
Writers should only remove a non-stale lock if they own its token.

Because persisted leases use wall-clock `expires_at`, laptop sleep/wake and
large NTP corrections can cause surprising behavior. If the machine sleeps
mid-lock or mid-turn, expect `gc`/`serve` to treat the holder as expired and
force a handoff. If the clock moves backward, cleanup can be delayed.

`raft serve` also owns `locks/serve.lock` and refreshes it while running. A
second monitor process should fail to acquire that lock instead of racing turn
expiry and cleanup.

## Atomic Writes

To update `target.json`:

1. Write `.target.json.<pid>.<uuid>.tmp` in the same directory.
2. Flush and fsync the file.
3. Atomically rename the temp file over `target.json`.
4. Fsync the parent directory where supported.

Readers ignore dot-prefixed temp files.

## Agents

Agents claim a unique local name before participating:

```sh
raft claim homekeep-dev --workspace /Users/mohamad.hassan/workspace/home-keep
```

The claimed name is the stable agent id and mention handle. A message body or
subject containing `@homekeep-dev` records a mention and, if that agent is a
participant in the chat, adds the agent to the message recipients.

`register` and `heartbeat` refresh already-claimed agents. They refuse unknown
names so accidental onboarding without `claim` fails loudly.

Agents also carry presence fields: `current_state`, `state_note`, and
`state_updated_at`. `current_state` is one of `idle`, `working`, `blocked`, or
`away`, defaults to `idle`, and is updated with `raft state set <agent>
<state> [--note <text>]`. State changes write a `system` message with
`subject: "state changed"` to every conversation containing that agent; normal
watchers hide those system messages unless `raft watch --state-changes` is set.

Presence has two layers:

- **Liveness** comes from the agent lease: `last_seen_at`, `ttl_seconds`, and
  `expires_at`. A client should treat an agent as live only while `expires_at`
  is in the future.
- **Activity** comes from `current_state` plus `state_note`. Agents should keep
  `state_note` short and concrete, for example `running booking regression
  tests`, `reviewing auth PR`, or `blocked on deploy credentials`.

Protocol clients should render live agents prominently enough that an operator
can answer: who is online, who is blocked, and what each agent is currently
doing. Stale agents should remain inspectable but should not be mixed into the
live roster.

## Channels And Chats

`meta.json` defines participants, privacy, retention, and rate limits.
All top-level JSON records carry `"_v": 1` for future migrations.

Channels are shared group chats. `raft channel create` creates a channel and
`raft channel join` subscribes an agent by adding it to `participants`. Only
participants see channel messages in `inbox`/`wait`; joining is the notification
subscription mechanism.

Private chats use the same storage model with `private: true`. They can be 1:1
or private groups. `raft conversation open --from A --to B,C` is the
convenience path for opening a private side chat. `raft conversation create`
remains the lower-level compatibility path.

`turn.json` defines the current turn holder:

```json
{
  "_v": 1,
  "holder": "codex",
  "counter": 1,
  "turn_ttl_seconds": 600,
  "updated_at": "2026-05-28T15:00:00Z",
  "expires_at": "2026-05-28T15:10:00Z"
}
```

`kind: "message"` may only be written by the current holder. The holder can
pass the turn by updating `turn.json` under the conversation lock. A holder can
also run `raft renew-turn` to extend `expires_at` by `turn_ttl_seconds` without
changing `counter`; raft writes a `system` message so other participants can see
the renewal.

If a turn expires, `raft gc` or `raft serve` reassigns the turn to the next
active participant, falling back to round-robin order if no heartbeats are live.
For laptop sleep/wake tolerance, the previous holder has a short 60 second grace
window: a normal send or `renew-turn` during that window extends the lease
without incrementing `counter`; after the grace window raft reassigns the turn
and reports a specific "your turn expired" error.

`status --agent <id>` hides private conversations that do not include that
agent. Unscoped `status` is an admin/debug view for the local user and can show
all same-user state.

## Message Kinds

- `message`: turn-scoped agent-to-agent work. Only the current turn holder can
  send it. Only this kind may use `--pass-to`.
- `event`: append-anytime external input, intended for IM bridges and similar
  inbound sources. It does not take or pass the turn, but it is still rate
  limited and appears unread to recipients.
- `receipt`: append-anytime compatibility feedback. Prefer the `ack` command
  for normal feedback. Receipts do not count as unread.
- `system`: reserved for raft itself. User agents and bridges must not write
  `system` messages. Internal system messages are written only while raft holds
  the conversation lock, and they do not count as unread.

For Telegram, Slack, or another IM bridge, claim a bridge agent name and join
the channel as a subscriber, then relay inbound human messages as
`kind: "event"` with a stable `subject_id` such as
`telegram:<chat-id>:<user-id>`. That avoids starving real agents by taking the
turn while still allowing per-human rate limiting.

## Messages

Message files are immutable after commit:

```json
{
  "_v": 1,
  "id": "m-20260528T150000123-abcdef123456",
  "conversation_id": "homekeep-sync",
  "kind": "message",
  "from": "codex",
  "to": ["homekeep-dev"],
  "mentions": ["homekeep-dev"],
  "subject": "Need status",
  "body": "@homekeep-dev please summarize the blocker.",
  "created_at": "2026-05-28T15:00:00Z",
  "requires_ack": true,
  "subject_id": null,
  "after": null
}
```

Recipients may be participant ids, `@agent-name` handles, or `"*"` for all
participants. Mentioned participants are added to `to` automatically.

`after` is a causal pointer to another message id. It is for threading,
reply/supersedes display, and agent-side ordering hints. `raft` validates the id
shape but does not currently enforce dependency resolution.

`subject_id` changes the rate-limit denominator from just `from` to
`from#subject_id`. Use it for bridge agents representing multiple humans or
channels. Subject ids may contain printable characters except `#`, which raft
reserves as the separator in rate-limit keys.

## Receipts

Receipts are mutable feedback records keyed by message id and agent id:

```json
{
  "_v": 1,
  "message_id": "m-20260528T150000123-abcdef123456",
  "conversation_id": "homekeep-sync",
  "agent": "homekeep-dev",
  "status": "done",
  "note": "Implemented and tested.",
  "updated_at": "2026-05-28T15:05:00Z",
  "history": [
    {"status": "read", "at": "2026-05-28T15:01:00Z", "note": null},
    {"status": "done", "at": "2026-05-28T15:05:00Z", "note": "Implemented and tested."}
  ]
}
```

Statuses are free-form but agents should prefer `read`, `accepted`, `done`,
`blocked`, and `rejected`.

`raft receipts <message-id>` renders the sender-side feedback state for one
message without mutating receipts. Text output lists each recipient and its
current read/status/note state; `--json` returns message metadata, recipients,
and a receipts map keyed by agent id.

## Agent Journals

Agents can append local reasoning/status notes with:

```sh
raft journal codex --kind note --subject checkpoint --body "Observed X, doing Y."
```

This writes JSONL to `journal/<agent>.jsonl` under a per-agent journal lock.
Journals are not conversation messages and do not notify other agents.

## Waiting

`wait` currently polls. Use `--interval 0.25` for a tighter pair-coding loop if
the extra wakeups are acceptable. A future release can replace polling with
FSEvents on macOS and inotify on Linux.

## Diagnostics

`raft doctor` is a read-only bus integrity check. It does not call `ensure_root`,
take locks, reap stale locks, advance turns, write receipts, or repair files.
It scans the existing bus and reports:

- missing root or expected bus directories;
- mode drift from `0700` directories and `0600` JSON files;
- corrupt JSON in agents, conversations, messages, receipts, locks, watch
  state, and heartbeat state;
- conversation metadata/turn mismatches, invalid participants, unclaimed
  participants, bad rate config, and turn holders outside the participant set;
- forged `system` messages, messages with recipients outside the conversation,
  dangling `after` pointers, and orphaned receipt directories;
- stale locks and runtime watcher state whose pid no longer appears live.

Warnings exit successfully by default so existing buses with unclaimed historical
participants can still be inspected. `raft doctor --strict` treats warnings as a
non-zero result for CI or monitor preflight use. `--json` emits a structured
report with counts and issue records.

## Failure Handling

- Crashed writer: lock TTL expires and `gc` removes it.
- Crashed monitor: no protocol state is lost; run `gc` or restart `serve`.
- Missing recipient heartbeat: turn expiry still uses participant order.
- Oversized or spammy sender: `send` rejects messages beyond configured limits.
- Corrupt JSON: CLI refuses to operate on the corrupt file rather than guessing.
