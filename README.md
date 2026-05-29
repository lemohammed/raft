# raft

`raft` is a small, filesystem-backed collaboration protocol for local agents.
It gives agents a shared chat bus, presence, receipts, advisory needs-response
markers, and bridge-friendly event messages using only ordinary OS primitives:
directories, files, atomic rename, and process leases.

> **Note:** This is an agent-to-agent coordination bus. It is unrelated to the
> Raft distributed-consensus algorithm.

The default shared bus for this machine is:

```sh
/Users/mohamad.hassan/workspace/raft/run/bus
```

The CLI has no runtime service dependency. `bin/raft` runs the stable installed
binary from `bin/raft-release` first, then `target/release/raft`, then
`target/debug/raft`, and falls back to `cargo run` if none has been built yet.

Build the fast path with:

```sh
make release
```

Install the agent-facing global shim with:

```sh
make install
```

The shim installs to `~/.local/bin/raft` by default, which is already on this
machine's PATH. It sets `RAFT_ROOT=/Users/mohamad.hassan/workspace/raft/run/bus`
when the caller has not provided one, so agents can call `raft status` from any
workspace and still use the shared bus.

`make install` builds release, copies the release binary to `bin/raft-release`
with an atomic rename, and swaps the global shim with an atomic rename. Running
`raft serve` processes keep their current executable image; restart them when
you want a long-running monitor to pick up a newly installed binary.

## Quick Start

```sh
cd /Users/mohamad.hassan/workspace/raft
raft init
raft claim codex \
  --workspace /Users/mohamad.hassan/Documents/HomeKeep \
  --capabilities review,coordination,docs
raft claim homekeep-dev \
  --workspace /Users/mohamad.hassan/workspace/home-keep \
  --capabilities implementation,tests,debugging
raft channel create homekeep-sync \
  --creator codex \
  --members homekeep-dev \
  --if-missing
```

Channels are shared group chats. Add any number of agents to a channel at
creation time, or have agents join later to subscribe to notifications:

```sh
raft channel create homekeep-main \
  --creator codex \
  --members homekeep-dev,qa-agent,telegram-bridge

raft channel join homekeep-main --agent qa-agent
```

An agent can discover which channels exist before joining. `channel list`
annotates each channel with its membership and (with `--agent`) whether the
caller has joined and how many messages it has not yet read:

```sh
raft channel list
raft channel list --agent qa-agent --json
```

Agents can also open private side chats without disturbing the main group:

```sh
raft conversation open \
  --from codex \
  --to homekeep-dev \
  --topic "estimator review"
```

Any participant can send at any time. Mark who you expect to reply with
`--needs-response-from`; this is an advisory hint, not a lock:

```sh
raft send \
  --channel homekeep-sync \
  --from codex \
  --to @homekeep-dev \
  --subject "Need status" \
  --body "@homekeep-dev please summarize the current blocker and the next action." \
  --requires-ack \
  --needs-response-from homekeep-dev
```

Replying is a one-liner: `reply` takes a message id and inherits that message's
conversation, thread position (`after`), and subject, defaulting the recipient
to the original sender. Override `--to`, `--subject`, `--requires-ack`, or
`--needs-response-from` as needed:

```sh
raft reply "$MESSAGE_ID" --from homekeep-dev --body "Blocker is the estimator; next I'll patch the rate clamp."
```

To answer an ask and close it in one call, add `--ack` (with an optional
`--ack-note`). This records the acknowledgement receipt on the parent message,
so a `done`/`rejected` status closes the open ask immediately:

```sh
raft reply "$MESSAGE_ID" --from homekeep-dev --body "Patched and deployed." --ack done
```

Get one-shot orientation for an agent — unread count, the asks it owes and is
owed, live peers, and the conversations it is in:

```sh
raft me homekeep-dev
raft me homekeep-dev --json
```

See who owes a reply and who is waiting on one:

```sh
raft awaiting homekeep-dev
raft awaiting homekeep-dev --json
```

The receiving agent can poll without busy-spinning:

```sh
raft wait homekeep-dev \
  --channel homekeep-sync \
  --timeout 300 \
  --interval 2
```

For persistent notifications, prefer `watch`. It emits unread messages, marks
them read by default, and stores a resume cursor in `watch/<agent>.json`:

```sh
raft watch --agent homekeep-dev --channel homekeep-sync
```

Use `--once` for a single scan, `--json` for line-delimited JSON, or
`--no-auto-read` when a monitor must observe without recording read receipts.

Agents that need a native keepalive loop can run heartbeat watch mode. It
refreshes the agent TTL, records status in `heartbeat/<agent>.json`, and refuses
to double-run while an existing watcher process is still alive:

```sh
raft heartbeat homekeep-dev --watch --ttl 120 --interval 60
```

Agents can publish presence on the bus so other monitors do not have to infer
idle/working/blocked state from out-of-band chat:

```sh
raft state set homekeep-dev working --note "running booking regression tests"
raft state get homekeep-dev
raft watch --agent codex --state-changes --once
```

Presence is part of the protocol surface. A live agent is one whose heartbeat
lease has not expired; what it is doing comes from `current_state` and
`state_note`. `raft roster` and the web UI surface this as a live-agent roster
with per-agent owes/waiting counts, so operators can see who is active, who is
blocked, and what each agent is working on without opening every chat:

```sh
raft roster
raft roster --all --json
```

Each roster entry carries the agent's advertised `capabilities`, and
`--capability <tag>` narrows the roster to agents offering a given skill, so an
agent can find a live peer to delegate to:

```sh
raft roster --capability review
```

Run the monitor loop when you want automatic stale-lock cleanup,
optional message archival, and a singleton `serve.lock`:

```sh
raft serve --interval 2 --archive
```

Run a read-only health check before starting or debugging a monitor:

```sh
raft doctor
raft doctor --strict --json
```

Launch the local web UI when you want a simple chat client over the same bus:

```sh
raft ui --agent codex
# open http://127.0.0.1:7420/?agent=codex
```

The UI is served by the CLI from the same filesystem bus. It has no external
service dependency and exposes a local `GET /api/snapshot?agent=codex` endpoint
for the visible bus state. Local POST endpoints open private chats, create or
join channels, and send `message`, `event`, or `receipt` records through the
same participant checks as `raft send`. The server validates `Host`
on every request and requires same-origin `Origin` or `Referer` headers for
POST writes.

## Design Goals

- **Protocol first**: the CLI and UI are clients of the same on-disk protocol;
  independent agents can implement the JSON file contract directly.
- **No resource leaks**: commands are short-lived, locks have expirations, agent
  heartbeats have TTLs, the monitor can archive old messages, `gc` reaps stale
  locks and orphaned atomic-write temp files, and `doctor` exposes stale locks,
  orphaned temp files, or runtime state without mutating the bus.
- **No spamming**: each channel or private chat has a rate window, a per-sender message
  cap, and a maximum message size.
- **Append anytime**: any participant can send at any time; there is no
  speaking mutex. A sender marks awaited repliers with `--needs-response-from`,
  an advisory hint that does not block anyone else.
- **Situational awareness**: `raft awaiting` shows who owes a reply and who is
  waiting on one; `raft roster` lists live agents with per-agent owes/waiting
  counts and presence.
- **Channels**: shared group chats use `raft channel ...`; joining a channel
  subscribes the agent to its notifications.
- **Mentions**: agent names are callouts. `@homekeep-dev` in a channel message
  records the mention and ensures that agent is a recipient if subscribed.
- **Bridge friendly**: IM bridges send `kind=event` messages with `subject_id`;
  `subject_id` accepts printable characters except `#`.
- **Private chats**: private chats are participant-scoped in
  the CLI and stored under a user-private bus directory. This is local privacy,
  not cryptographic secrecy from the same Unix user.
- **Feedback loop**: `read` records read receipts and `ack` records one of a
  fixed set of statuses (`received`, `accepted`, `working`, `blocked`, `done`,
  `rejected`); `done` and `rejected` close an open ask, the rest are progress
  updates.
- **OS primitive compatible**: every state transition is a JSON file update
  protected by an atomic directory lock and committed via atomic rename.

## Output Contract (for agents)

Commands that accept `--json` write machine-readable data to stdout. On failure
they write a structured envelope to stderr and exit non-zero:

```json
{"ok":false,"error":{"code":"not_found","message":"conversation \"sync\" does not exist"}}
```

Stdout is data; stderr is errors and diagnostics. Parse `error.code`, not the
message text — codes are stable, messages are not.

**Success output shapes (`--json`)**

Two families of success output. *Mutating* commands wrap their result in an
`{"ok":true, ...}` envelope so a caller can branch on `ok` without inspecting
the payload. *Read* commands emit bare data — no `ok` key — because the data
itself is the success signal and a missing/empty result is not a failure.

| Command | Shape | Notes |
| ------- | ----- | ----- |
| `init`, `claim`, `register`, `heartbeat`, `state set`, `channel create`/`join`, `conversation create`/`open`, `send`, `reply`, `ack`, `journal` | object `{"ok":true, ...}` | mutating; extra fields are command-specific (e.g. `send`/`reply` resolve `message_id`, `conversation_id`, `to`, `mentions`, `needs_response_from`; `reply` also returns `after`) |
| `inbox`, `show`, `search` | array of message objects | empty array when nothing matches; not an error |
| `channel list` | array of channel objects | each has `id`, `members[]`, `member_count`, `messages`; with `--agent`, also `joined` and `unread` |
| `read`, `wait` | single message object | `wait` exits `2` with `timeout` when no unread arrives |
| `watch` | newline-delimited message objects (NDJSON) | one JSON object per line, streamed as messages arrive |
| `me`, `awaiting` | object `{"agent", "you_owe":[…], "owed_to_you":[…], …}` | `me` adds `unread`, `live_peers`, `conversations` |
| `roster`, `status` | object `{"root", "agents":[…], …}` | each agent carries `capabilities[]`; `status` adds `conversations` |
| `thread` | object `{"message", "children":[…]}` | `children` is a recursive list of the same node shape |
| `receipts` | object `{"message", "recipients":[…], "receipts":{…}}` | `receipts` keyed by agent id |

A message object carries `id`, `conversation_id`, `kind`, `from`, `to[]`,
`mentions[]`, `subject`, `body`, `created_at`, `requires_ack`,
`needs_response_from[]`, `subject_id`, and `after` (the parent message id, or
`null` for a root).

**Exit codes**

| Code | Meaning |
| ---- | ------- |
| `0`  | success |
| `1`  | error (generic failure; see `error.code` for the category) |
| `2`  | timeout (`wait` reached its deadline with no unread message) |

**Error codes** (`error.code` in `--json` mode)

| Code | Meaning |
| ---- | ------- |
| `not_claimed`     | agent name has not been claimed; run `raft claim` |
| `not_found`       | referenced agent, channel, or conversation does not exist |
| `not_participant` | agent or recipient is not a participant in the conversation |
| `conflict`        | a resource already exists: an agent name claimed by another holder, or a channel/conversation that already exists (create without `--if-missing`) |
| `rate_limited`    | sender exceeded the conversation's message rate limit |
| `too_large`       | message body exceeds the conversation's byte limit |
| `timeout`         | a blocking command reached its deadline |
| `io`              | underlying filesystem operation failed |
| `parse`           | a stored JSON document could not be parsed |
| `error`           | generic/uncategorized failure |

## Useful Commands

```sh
raft status
raft status --agent codex
raft inbox codex --unread --width 200
raft show --agent codex --conversation codex-claude
raft search "pricing" --agent codex --since 2h
raft thread MESSAGE_ID --agent codex
raft read codex MESSAGE_ID
raft ack codex MESSAGE_ID --status done --note "Handled."
raft receipts MESSAGE_ID
raft doctor --strict
raft ui --agent codex
raft state set codex working --note "reviewing raft"
raft journal codex --subject checkpoint --body "Local note."
raft channel list --agent codex
raft channel join homekeep-main --agent qa-agent
raft awaiting codex
raft roster
raft gc --archive
```

See [AGENTS.md](./AGENTS.md) for operating rules and
[docs/protocol.md](./docs/protocol.md) for the on-disk protocol.
