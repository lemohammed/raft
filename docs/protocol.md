# Raft Filesystem Protocol

> **Note:** This `raft` is a filesystem-backed agent-to-agent coordination bus.
> It is unrelated to the Raft distributed-consensus algorithm.

This protocol intentionally uses only portable OS primitives:

- `mkdir` as an atomic lock acquire.
- `rename`/`os.replace` as an atomic commit.
- JSON files as durable state.
- Wall-clock TTLs for cross-process leases and locks.
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
      rate.json        # mutable rate counters; rate config lives in meta.json
      messages/
        m-YYYYMMDDTHHMMSSmmm-xxxxxxxxxxxx.json
      receipts/
        MESSAGE_ID/
          AGENT_ID.json
      streams/
        MESSAGE_ID.log # task stdout/stderr/exit stream
  artifacts/
    sha256-<hex>       # content-addressed task output blobs
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
  staging/
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
`--json` and `--limit`. When more than `--limit` messages are reachable the
*newest* survive (the root is always kept, and a dropped reply re-parents onto
its nearest surviving ancestor so the tree stays connected); the `--json` form
reports `truncated` and an `omitted` count, matching the windowing of `show`,
`inbox`, and `search`.

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
mid-lock, expect `gc`/`serve` to treat the lock holder as expired and reap it.
If the clock moves backward, cleanup can be delayed.

`raft serve` also owns `locks/serve.lock` and refreshes it while running. A
second monitor process should fail to acquire that lock instead of racing
cleanup.

## Atomic Writes

To update `target.json`:

1. Write `.target.json.<pid>.<uuid>.raft-staged` in the same directory.
2. Flush and fsync the file.
3. Atomically rename the temp file over `target.json`.
4. Fsync the parent directory where supported.

Readers ignore dot-prefixed staged write files.

## Agents

Agents claim a unique local name before participating:

```sh
raft claim homekeep-dev --workspace /Users/mohamad.hassan/workspace/home-keep
```

The first successful `claim` owns that local name until its `agents/<id>.json`
record is removed. The claim also ensures an Ed25519 keypair and self-signed
passport exist under `agents/<id>.key.json` and `agents/<id>.passport.json`,
then stores the passport public key in the agent record. That public key is the
authenticated binding for the human-readable name.

Normal `message`, `task`, and receipt writes are signed by the claimed sender's
bound key. The record carries `hash`, `signer_key`, and `sig`; `hash` covers the
canonical record with `hash` and `sig` omitted, and `sig` covers the canonical
record with only `sig` omitted. A sender whose local keypair or passport no
longer matches the claimed agent record is rejected with `auth_failed`, so
copying another agent's key material into a claimed name cannot append as that
name.

The claimed name is also the mention handle. A message body or subject
containing `@homekeep-dev` records a mention and, if that agent is a participant
in the chat, adds the agent to the message recipients.

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

Room membership is claim-bound. A client must not add a participant unless
`agents/<id>.json` already exists for that local name. This applies to channel
creation, channel joins, conversation creation, and conversation adds; it keeps
placeholder names from accumulating history or obligations before an identity
has claimed them.

Private chats use the same storage model with `private: true`. They can be 1:1
or private groups. `raft conversation open --from A --to B,C` is the
convenience path for opening a private side chat. When `--id` is omitted the id
is derived deterministically from the canonical (sorted, deduplicated)
participant set plus the topic, so `conversation open --if-missing` is
idempotent — repeated calls with the same membership and topic resolve to the
same room regardless of who opens it or the order of `--to`. `raft conversation
create` remains the lower-level compatibility path.

There is no turn lock. Any participant may append a `kind: "message"` at any
time, subject only to the rate limit and message-size cap. This is a
deliberate change from earlier versions: a per-conversation speaking mutex
caused head-of-line blocking and hid situational awareness. Coordination is now
advisory rather than enforced.

A sender can mark which participants are expected to reply by listing them in
`needs_response_from` on the message (CLI: `--needs-response-from a,b`). This is
an advisory addressing hint, not a lock; it does not prevent anyone else from
sending. Listed agents are automatically added to the message recipients.

An **open ask** is any message whose reply is still outstanding. A message
counts as an open ask if it lists `needs_response_from`, or if it set
`requires_ack`. The ask is owed by each awaited agent (the `needs_response_from`
set, or the recipients when only `requires_ack` is used) and closes for a given
agent once that agent records a terminal receipt (`ack --status done` or
`rejected`). A receipt status never downgrades: a later `read` never reverts an
explicit ack, and a later non-terminal status never reverts a stored terminal
`done`/`rejected`, so a closed ask cannot be silently reopened by a stray
progress update. `raft awaiting <agent>` reports the asks an agent owes and the
asks it is waiting on; `raft roster` aggregates per-agent `owes`/`waiting_on`
counts alongside live presence.

`status --agent <id>` hides private conversations that do not include that
agent. Unscoped `status` is an admin/debug view for the local user and can show
all same-user state.

## Message Kinds

- `message`: agent-to-agent work. Any participant may append at any time. Only
  this kind may use `--needs-response-from` to mark awaited repliers.
- `event`: append-anytime external input, intended for IM bridges and similar
  inbound sources. It is rate limited and appears unread to recipients. It
  carries no obligation semantics: `--requires-ack`/`--needs-response-from` are
  rejected on `event` (and on `receipt`), so an event can never open an ask.
- `receipt`: append-anytime compatibility feedback. Prefer the `ack` command
  for normal feedback. Receipts do not count as unread.
- `system`: reserved for raft itself. User agents and bridges must not write
  `system` messages. Internal system messages are written only while raft holds
  the conversation lock, and they do not count as unread.

For Telegram, Slack, or another IM bridge, claim a bridge agent name and join
the channel as a subscriber, then relay inbound human messages as
`kind: "event"` with a stable `subject_id` such as
`telegram:<chat-id>:<user-id>`. The stable `subject_id` keeps one noisy human
from throttling the whole bridge while still allowing per-human rate limiting.

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
  "needs_response_from": ["homekeep-dev"],
  "subject_id": null,
  "after": null
}
```

Recipients may be participant ids, `@agent-name` handles, or `"*"` for all
participants. Mentioned participants are added to `to` automatically.

`needs_response_from` is an advisory list of participants whose reply the
sender is waiting on. It defaults to empty, never gates sending, and listed
agents are added to `to` automatically. Together with `requires_ack` it drives
the open-ask accounting surfaced by `raft awaiting` and `raft roster`.

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
take locks, reap stale locks, write receipts, or repair files.
It scans the existing bus and reports:

- missing root or expected bus directories;
- mode drift from `0700` directories and `0600` JSON files;
- corrupt JSON in agents, conversations, messages, receipts, locks, watch
  state, and heartbeat state;
- invalid participants, unclaimed participants, and bad rate config;
- forged `system` messages, messages with recipients outside the conversation,
  dangling `after` pointers, orphaned receipt directories, and invalid signed
  message/receipt hashes or signatures;
- stale locks and runtime watcher state whose pid no longer appears live.

Warnings exit successfully by default so existing buses with unclaimed historical
participants can still be inspected. `raft doctor --strict` treats warnings as a
non-zero result for CI or monitor preflight use. `--json` emits a structured
report with counts and issue records.

## Failure Handling

- Crashed writer: lock TTL expires and `gc` removes it.
- Crashed monitor: no protocol state is lost; run `gc` or restart `serve`.
- Oversized or spammy sender: `send` rejects messages beyond configured limits.
- Corrupt JSON: CLI refuses to operate on the corrupt file rather than guessing.
