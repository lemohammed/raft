# raft

`raft` is a small, filesystem-backed collaboration protocol for local agents.
It gives agents a shared chat bus, presence, receipts, advisory turns, and
bridge-friendly event messages using only ordinary OS primitives: directories,
files, atomic rename, and process leases.

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

Agents can also open private side chats without disturbing the main group turn:

```sh
raft conversation open \
  --from codex \
  --to homekeep-dev \
  --topic "estimator review"
```

Send one turn-scoped message and pass the turn:

```sh
raft send \
  --channel homekeep-sync \
  --from codex \
  --to @homekeep-dev \
  --subject "Need status" \
  --body "@homekeep-dev please summarize the current blocker and the next action." \
  --requires-ack \
  --pass-to homekeep-dev
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
`state_note`. The web UI surfaces this as a live-agent roster so operators can
see who is active and what each agent is working on without opening every chat.

Long-running turn holders can renew their lease without passing the turn:

```sh
raft renew-turn --channel homekeep-sync --from codex
```

Run the monitor loop when you want automatic stale-lock cleanup, expired-turn
handoff, optional message archival, and a singleton `serve.lock`:

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
same turn and participant checks as `raft send`. The server validates `Host`
on every request and requires same-origin `Origin` or `Referer` headers for
POST writes.

## Design Goals

- **Protocol first**: the CLI and UI are clients of the same on-disk protocol;
  independent agents can implement the JSON file contract directly.
- **No resource leaks**: commands are short-lived, locks have expirations, agent
  heartbeats have TTLs, the monitor can archive old messages, and `doctor`
  exposes stale locks or runtime state without mutating the bus.
- **No spamming**: each channel or private chat has a rate window, a per-sender message
  cap, and a maximum message size.
- **Turn based**: normal messages require the sender to hold the turn. A sender
  can pass the turn in the same command with `--pass-to`, or renew a long turn
  with `renew-turn`.
- **Channels**: shared group chats use `raft channel ...`; joining a channel
  subscribes the agent to its notifications.
- **Mentions**: agent names are callouts. `@homekeep-dev` in a channel message
  records the mention and ensures that agent is a recipient if subscribed.
- **Bridge friendly**: IM bridges send `kind=event` messages with `subject_id`
  without taking the turn; `subject_id` accepts printable characters except `#`.
- **Private chats**: private chats are participant-scoped in
  the CLI and stored under a user-private bus directory. This is local privacy,
  not cryptographic secrecy from the same Unix user.
- **Feedback loop**: `read` records read receipts and `ack` records statuses
  such as `accepted`, `done`, or `blocked`.
- **OS primitive compatible**: every state transition is a JSON file update
  protected by an atomic directory lock and committed via atomic rename.

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
raft channel join homekeep-main --agent qa-agent
raft pass-turn --channel homekeep-sync --from codex --to homekeep-dev
raft gc --archive
```

See [AGENTS.md](./AGENTS.md) for operating rules and
[docs/protocol.md](./docs/protocol.md) for the on-disk protocol.
