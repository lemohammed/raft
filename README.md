# raft

[![CI](https://github.com/lemohammed/raft/actions/workflows/ci.yml/badge.svg)](https://github.com/lemohammed/raft/actions/workflows/ci.yml)

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

`raft` uses Rust 1.88+ (`rust-version = "1.88"`). The repository pins
`rust-toolchain.toml` to 1.88.0 with `clippy` and `rustfmt`, so rustup-backed
Cargo commands automatically select the toolchain needed for the 2024-edition
syntax used by the source and tests. The Makefile also invokes
`rustup run 1.88.0 cargo ...`, so `make check`/`make install` use the pinned
toolchain even on machines where another `cargo` appears earlier on `PATH`.

Build the fast path with:

```sh
make toolchain
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

## Swarm automation

For multi-agent work, use `raft` as the local coordination backplane:

1. Agents claim stable ids with capability tags (`tests`, `review`, `deploy`,
   `echo`) and keep heartbeats fresh.
2. A coordinator posts actionable asks with `--needs-response-from`, or uses
   `swarm assign` to pick the lowest-load live channel members for human/LLM work.
3. For executable automation, use `swarm dispatch`: it ranks live channel members
   by capability match, state, and open-ask load, selects the best worker, and
   enqueues a `kind:"task"` message for that worker.
4. Workers run `raft run <agent> --tool name=/path --trust <issuer>`; task status,
   results, artifacts, and logs flow back through the same obligation lifecycle.

Example:

```sh
raft swarm dispatch \
  --from coord \
  --channel homekeep-sync \
  --capability tests \
  --tool pytest \
  --args '{"path":"tests/booking"}' \
  --cap run/caps/pytest-cap.json \
  --json
```

Use `swarm candidates --json` first when you want to inspect the routing decision
before opening work.

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

Joining records a membership baseline: an agent owes nothing for activity that
predates its `joined_at`, so a late joiner sees the channel's prior history as
read backlog rather than a wall of unread messages, and a broadcast ask sent
before it joined never lands in its `awaiting` list. Catch-up is still
available on demand via plain `inbox` (without `--unread`) or `show`. The
baseline is set once and survives a leave/rejoin: an agent that reconnects
keeps its original `joined_at`, so an ask it was already owed reopens on rejoin
rather than being silently discharged by a fresh baseline.

Room membership is claim-bound: `channel create`, `channel join`,
`conversation create`, and `conversation add` reject unclaimed agent names.
Claim the handle first so no later process can inherit a placeholder name and
its pending obligations. Sends also refuse direct or wildcard recipients whose
claim record is missing from a legacy room.

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

When `--id` is omitted the conversation id is derived deterministically from the
participants and topic, so re-running `conversation open --if-missing` with the
same agents and topic reuses the existing room (`created: false`) instead of
forking a new one. The derived id is independent of who opens the chat and the
order of `--to`, so both peers opening from their own side land in the same
room; a different `--topic` is a different room.

To pull another agent into a side chat already in progress — without losing
its history by recreating it — add them as a participant. `conversation add`
is idempotent and reports the resulting member set:

```sh
raft conversation add codex-homekeep-dev --agent qa-agent --json
```

When an agent is done in a room, drop them with `conversation remove` (or
`channel leave` for a channel). Both are idempotent (`removed`/`left` is
`false` on a repeat), refuse to remove the last participant, and reject
cross-type usage (a channel points you at `channel leave`, a private
conversation at `conversation remove`). A removed agent can no longer send
until re-added; any open ask still awaiting them is released (they could
never ack or reply once removed), so the asker's `owed_to_you`/`wait --owed`
stops blocking on a response that can no longer arrive:

```sh
raft conversation remove codex-homekeep-dev --agent qa-agent --json
raft channel leave homekeep-main --agent qa-agent --json
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

The two obligation flags are independent and compose: `--requires-ack` makes
every recipient owe an acknowledgement, while each `--needs-response-from` name
additionally owes a substantive reply. A message can carry both — "everyone
ack, and @homekeep-dev specifically reply" — and each awaited agent reports its
own `await_kind` (`requires_ack` or `needs_response`); the ask stays open until
*all* of them are discharged. Both flags require `--kind message` (the default)
— they are rejected on `event`/`receipt`, which carry no obligation semantics,
so an inbound bridge `event` can never fabricate an ask nobody can close.

Replying is a one-liner: `reply` takes a message id and inherits that message's
conversation, thread position (`after`), and subject, defaulting the recipient
to the original sender. Override `--to`, `--subject`, `--requires-ack`, or
`--needs-response-from` as needed:

```sh
raft reply "$MESSAGE_ID" --from homekeep-dev --body "Blocker is the estimator; next I'll patch the rate clamp."
```

Because a bare reply addresses only the original sender, replying in a group or
channel thread silently drops everyone else who was on the parent. The
envelope's `omitted_recipients[]` names those left-out participants (text mode
warns on stderr) so you can re-address with `--to` if you meant to answer the
whole thread; it stays empty when you pass `--to` explicitly, since that is a
deliberate choice.

To answer an ask and close it in one call, add `--ack` (with an optional
`--ack-note`). This records the acknowledgement receipt on the parent message,
so a `done`/`rejected` status closes the open ask immediately:

```sh
raft reply "$MESSAGE_ID" --from homekeep-dev --body "Patched and deployed." --ack done
```

An `ack` (whether standalone or via `reply --ack`) reports whether it actually
discharged an obligation. The `--json` envelope carries `was_awaited` (the agent
is in the message's awaited set) and `closed_ask` (this ack just transitioned an
open ask to closed — terminal status, awaited, and not already terminal). An
agent should branch on `closed_ask` rather than assuming exit 0 means progress:
a `done` that lands on the wrong message id closes nothing and would otherwise
leave the asker's `wait --owed` blocked forever. Pass `--require-open` to turn
that mistake into a hard `not_awaited` error instead of a silent no-op:

```sh
raft ack homekeep-dev "$MESSAGE_ID" --status done --require-open
```

Receipt status never downgrades. A bare `read` marker never reverts an explicit
ack, and a non-terminal status (`received`/`accepted`/`working`/`blocked`) never
reverts a stored terminal `done`/`rejected` — so an `ack working` (or
`reply --ack working`) recorded after a `done` does not reopen the closed ask.
When the guard preserves the stronger status, `ack --json` reports the `status`
that actually stuck alongside `requested_status` and `downgrade_ignored: true`,
so a caller is never told a downgrade took effect. A deliberate terminal change
(`done`→`rejected`) and any upgrade still apply.

The envelope (and the `not_awaited` error details) also carries `withdrawn`:
`null` normally, or the withdrawal record (`by`, `at`, `reason`) when the sender
has retracted the ask. A withdrawn ask reads as `was_awaited: false` — the same
as a message you were never on the hook for — so without this field a worker
that raced the withdrawal could not tell "too late, it was withdrawn" from "this
was never mine". `withdrawn` disambiguates the two and surfaces the reason.

If you opened an ask and no longer need the reply (the question went moot, you
solved it yourself, you re-routed it elsewhere), withdraw it so it stops
counting against everyone. Only the original sender can withdraw, and the ask
drops out of every `awaited` view at once — the awaited agents' `you_owe`, your
own `owed_to_you`, the roster counts, and any `wait --owed` blocked on it.
Withdrawing is idempotent, and the `--json` envelope returns `released[]` (the
agents whose obligation was lifted). `released[]` lists only the genuinely-open
recipients: an agent that already recorded a terminal `done`/`rejected` ack owes
nothing, so it is omitted and is not notified — withdraw never tells a worker to
stop work it already finished. (If every recipient has already responded there
is nothing left to withdraw, and the command reports `not_found`.) Each released
worker gets a discoverable `ask withdrawn` system notice (visible in
`inbox`/`show`/`thread`, like the `participant removed`/`channel left` notices)
that names the ask and carries the reason — so a worker who already acked
`working` learns why the ask vanished from its `you_owe` instead of seeing it
disappear silently:

```sh
raft withdraw "$MESSAGE_ID" --from homekeep-dev --reason "fixed it myself"
```

Get one-shot orientation for an agent — unread count, the asks it owes and is
owed, live peers, and the conversations it is in:

```sh
raft me homekeep-dev
raft me homekeep-dev --json
```

`me` also reports the agent's **own** heartbeat liveness as `live` (with
`expires_at`). raft computes liveness everywhere else only for *peers*, so a
stale agent — one whose heartbeat lapsed during a long tool call — would
otherwise orient with `me` and see nothing wrong, while every peer that asks it
something gets `awaited_live: false` and blocks on a `wait --owed` reply it
doesn't know it looks too dead to send. When `live` is false, text mode prints a
`STALE: … run 'raft heartbeat <id>'` banner so the agent can revive itself.

See who owes a reply and who is waiting on one:

```sh
raft awaiting homekeep-dev
raft awaiting homekeep-dev --json
```

For swarm-style orchestration, let `raft` rank candidates before assigning work.
`swarm candidates` scores live agents by capability match, published state, and
current open-ask load; `swarm assign` picks the best channel members and sends a
normal ask with `needs_response_from` set to the selected agents, so existing
`awaiting`, `reply --ack`, and `wait --resolved` automation keeps working:

```sh
raft swarm candidates \
  --capability review \
  --capability tests \
  --exclude codex \
  --json

raft swarm assign \
  --from codex \
  --channel homekeep-sync \
  --capability review \
  --count 1 \
  --subject "Review estimator patch" \
  --body "Please review the diff and reply with blockers." \
  --json
```

Every candidate row includes `matching_capabilities`, `missing_capabilities`,
`owes`, `waiting_on`, `score`, and human-readable `reasons`, making it suitable
for higher-level collaboration algorithms that want deterministic, explainable
worker selection. Use `swarm assign --dry-run --json` to preview a routing
decision without mutating the bus.

Every open ask reported by `awaiting`, `me`, and `wait --owed`/`--resolved`
carries `awaited_live`: whether the awaited agent's heartbeat is still active.
A blocked asker can branch on it directly — an ask whose delegate is offline
(`awaited_live: false`) is a candidate to re-route or escalate rather than keep
waiting on. Text output flags it as `@agent (offline)`.

Each open ask also carries `await_kind`, derived per awaited agent (one message
that both names a responder and requires acks yields a `needs_response` ask for
the named agent and `requires_ack` asks for the rest): `"needs_response"` means
that agent was named in `--needs-response-from` (the sender wants a substantive
reply), `"requires_ack"` means it owes only the bare `--requires-ack`
acknowledgement. An agent triaging its `you_owe` list can branch on this to decide
whether to compose a `reply` or just `ack` — either way the ask closes when a
terminal `done`/`rejected` receipt is recorded (use `reply --ack` to do both at
once). Text output shows it as `wants reply` / `wants ack`.

The receiving agent can poll without busy-spinning:

```sh
raft wait homekeep-dev \
  --channel homekeep-sync \
  --timeout 300 \
  --interval 2
```

The *asking* side has a symmetric primitive. After delegating work with
`--requires-ack`/`--needs-response-from`, block until the awaited agent records a
terminal `done`/`rejected` ack — acks are receipts, not messages, so plain
`wait` never wakes on them. `wait --owed` blocks until *any* open ask the agent
owns closes (first-to-finish wins); `wait --resolved <message-id>` blocks on one
specific ask and only resolves once *every* awaited agent on it is terminal — so
an ask delegated to several agents (`--needs-response-from a,b`) does not report
done until both `a` and `b` answer, and its aggregate `status` is `rejected` if
any recipient rejected, otherwise `done`. `--resolved` reports immediately if the
ask has already closed. Both forms report the resolved ask (`message_id`,
`conversation_id`, `awaited`, `awaited_live`, `status`, `note`, `subject`) and
exit `2` on timeout.

Either form of `wait` fails fast with `not_claimed` (carrying nearest-id
`suggestions`) when the named agent has not been claimed, rather than blocking
for the whole `--timeout` and then exiting `2`. A typo'd agent id is a mistake
to surface immediately, not a deadline to wait out:

```sh
raft send --conversation c --from codex --to homekeep-dev \
  --subject "ship it" --body "deploy when green" --requires-ack
raft wait codex --owed --timeout 600 --json
```

For persistent notifications, prefer `watch`. It emits unread messages, marks
them read by default, and stores a resume cursor in `watch/<agent>.json`:

```sh
raft watch --agent homekeep-dev --channel homekeep-sync
```

Use `--once` for a single scan, `--json` for line-delimited JSON, or
`--no-auto-read` when a monitor must observe without recording read receipts.

Under the default (auto-read), `watch` dedups on read receipts, not on the id
cursor, so it never silently drops a message: message ids are not totally
ordered across concurrent writers, and a still-unread message can surface with
an id that sorts *below* one already emitted — auto-read delivers it anyway. The
persisted cursor is a soft resume hint (it suppresses re-emission only for
state-change notices and under `--no-auto-read`, where no receipt exists to
dedup on). An explicit `--since <id>` remains a hard floor for every message
kind.

Agents that need a native keepalive loop can run heartbeat watch mode. It
refreshes the agent TTL, records status in `heartbeat/<agent>.json`, and refuses
to double-run while an existing watcher process is still alive:

```sh
raft heartbeat homekeep-dev --watch --ttl 120 --interval 60
```

Agents can publish presence on the bus so other monitors do not have to infer
state from out-of-band chat. The published state is one of `idle`, `working`,
`blocked`, or `away`:

```sh
raft state set homekeep-dev working --note "running booking regression tests"
raft state get homekeep-dev
raft watch --agent codex --state-changes --once
```

Presence is part of the protocol surface. A live agent is one whose heartbeat
lease has not expired; what it is doing comes from `current_state` and
`state_note`. `raft state get` joins that liveness onto the published state
(`live` plus `last_seen_at`/`expires_at` in `--json`, a `(stale)` marker in
text) so a crashed agent's leftover `working` is not mistaken for the current
truth. `raft roster` and the web UI surface this as a live-agent roster
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

Archival (`gc --archive` or `serve --archive`) moves messages older than a
conversation's `retention_days` into `archive/`, where the obligation views do
not look. An unresolved open ask is therefore *retained* past its window rather
than archived into invisibility — otherwise an ask that aged out would silently
vanish from `awaiting`/`me`/`roster`, falsely clearing the asker and the
worker's queue. `withdraw` (or a terminal `done`/`rejected` ack) resolves the
ask so it can age out normally.

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

## Mesh (experimental): cryptographic identity

raft is growing a **mesh** layer that extends the local bus into a peer-to-peer
agent network — cryptographic identity, capability tokens, and remote task
delegation over an untrusted network. The full architecture (benchmarked against
Letta and the Nous Hermes tool-call format) lives in
[`docs/superpowers/specs/2026-05-29-raft-mesh-remote-execution-design.md`](docs/superpowers/specs/2026-05-29-raft-mesh-remote-execution-design.md).

The first piece is **identity**. Each agent can mint an Ed25519 keypair and a
self-signed *passport* that binds its human-readable id to its public key:

```sh
raft id new codex --capabilities plan,code   # writes agents/codex.key.json (0600) + passport
raft id show codex                            # the shareable public passport
raft id verify codex                          # checks the self-signature
raft id fingerprint codex                     # short, human-comparable key fingerprint
```

The secret seed never leaves the host. The passport is what other agents trust:
a tampered passport (for example, a forged broader capability set) fails
`raft id verify`. On the mesh the public key is the true identity and the id is a
convenience label. Every wire/disk format — `ed25519:<hex>` keys,
`sha256:<hex>` hashes, canonical sorted-key JSON for signing — is specified so
other languages can implement it. Identity is opt-in and additive: a local-only
bus keeps working unsigned.

### Capability tokens

The second piece is **authority**. A capability token is a chain of signed
blocks: a root block issued by one agent's key, then zero or more *attenuation*
blocks, each signed by the previous holder and each only able to *narrow* scope.
Anyone can verify a token offline against the root issuer's public key.

```sh
# Alice grants Bob the right to run the `deploy` tool in staging for 1h:
raft grant new --issuer alice --to bob \
  --action tool.run --tool deploy --env staging --ttl 1h \
  --out cap.json

# Bob narrows it and re-delegates to Carol (same action, shorter scope):
raft grant attenuate --holder bob --to carol --token-file cap.json \
  --action tool.run --out cap-carol.json

# Anyone verifies an action offline, pinning the trusted root:
raft grant verify --token-file cap-carol.json --root alice \
  --action tool.run --tool deploy --env staging   # -> authorized

raft grant inspect --token-file cap-carol.json          # chain + effective scope
```

Effective authority is the *intersection* of every block's caveats, so
attenuation cannot broaden: a later block listing a wider tool set still
intersects down, a later expiry takes the earlier `min`. Verification is
fail-closed — a token that does not constrain `action` authorizes nothing, and a
denied check returns the stable `not_authorized` code. This is the opposite of
the ambient, all-or-nothing credentials most agent runtimes inject into tool
code; here authority is explicit, scoped, time-boxed, and delegable.

### Remote tasks

The third piece is **delegation**. A task is an obligation-bearing message whose
body is a Hermes-style tool call plus the capability that authorizes the worker
to run it. Task status is the normal receipt lifecycle, so `awaiting`,
`wait --owed`, and `wait --resolved` work without a separate scheduler.

```sh
raft task dispatch --from alice --to bob --conversation deploy-room \
  --tool deploy --args '{"service":"api","env":"staging"}' --cap cap.json

raft run bob --tool deploy=/usr/local/bin/deploy-tool --trust alice --once
raft task status m-abc123
raft task cancel m-abc123 --from alice --reason superseded
```

When `--cap` is supplied, `task dispatch` fails before writing the task unless
the token's current holder matches the selected worker's claimed public key.
That keeps a coordinator from accidentally assigning a valid token to the wrong
agent and leaving the executor to reject it later.
Dispatch also checks the token's effective scope against the requested
conversation, tool, expiry, runtime limit, and output limit so an impossible
assignment never enters the worker's queue.
The `task` message kind is reserved for this dispatch path; `send`, `reply`,
and the UI endpoint reject manual `kind=task` writes so they cannot bypass the
Hermes body and worker/capability checks.

`raft run` is the v1 executor loop. It verifies the embedded capability against
the trusted root, runs registered tools with JSON arguments on stdin, and returns
the result as a reply before writing a terminal `done` or `rejected` receipt.
The built-in sandbox uses a scrubbed environment, a per-task scratch directory,
a wall-clock timeout, and an output cap; it is not yet an OS-enforced container
or microVM boundary. Captured stdout/stderr are also persisted as
content-addressed artifacts under `artifacts/sha256-...`, and the executor writes
a task log under `conversations/<id>/streams/<task-id>.log`. `task status --json`
includes both the artifact metadata and the log path in the result body.

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

Some errors carry extra structured fields alongside `code`/`message` so an
agent can self-correct in one shot. A `not_participant` failure includes the
conversation's valid `participants`, so a rejected `send`/`reply` immediately
reveals who can be addressed (and who to `conversation add`):

```json
{"ok":false,"error":{"code":"not_participant","message":"recipient \"qa\" is not a participant in \"proj\"","participants":["codex","homekeep-dev"]}}
```

A `not_found` failure on a mistyped conversation or channel id includes
nearest-match `suggestions` (closest first; omitted when nothing is close), so
a typo'd `send`, `channel join`, or `conversation add` hints at the right id
without a `channel list` / `me` round-trip:

```json
{"ok":false,"error":{"code":"not_found","message":"channel \"homekeep-man\" does not exist","suggestions":["homekeep-main"]}}
```

`not_claimed` failures (a mistyped agent id on `me`, `heartbeat`, `state`, or
`register`) carry the same nearest-match `suggestions`, so an agent that fat-
fingers its own name recovers in one shot instead of guessing:

```json
{"ok":false,"error":{"code":"not_claimed","message":"agent @alise is not claimed; use raft claim","suggestions":["alice"]}}
```

**Success output shapes (`--json`)**

Two families of success output. *Mutating* commands wrap their result in an
`{"ok":true, ...}` envelope so a caller can branch on `ok` without inspecting
the payload. *Read* commands emit bare data — no `ok` key — because the data
itself is the success signal and a missing/empty result is not a failure.

| Command | Shape | Notes |
| ------- | ----- | ----- |
| `init`, `claim`, `register`, `heartbeat`, `state set`, `channel create`/`join`/`leave`, `conversation create`/`open`/`add`/`remove`, `send`, `reply`, `withdraw`, `ack`, `journal` | object `{"ok":true, ...}` | mutating; extra fields are command-specific (e.g. `send`/`reply` resolve `message_id`, `conversation_id`, `to`, `mentions`, `needs_response_from`, and `offline_recipients`; `reply` also returns `after` and `omitted_recipients` (group/channel participants a bare reply did not reach); `conversation add` returns `participants[]` and `added`; `conversation remove` returns `participants[]` and `removed`; `channel leave` returns `members[]` and `left`; `ack` returns the effective `status`, `requested_status`, `downgrade_ignored`, `was_awaited`, `closed_ask`, and `withdrawn` (the withdrawal record or `null`); `withdraw` returns `released[]`, `withdrawn`, and `already_withdrawn`) |
| `inbox`, `show` | array of viewer-relative message objects | each message plus `unread`, `awaiting_me`, `my_status` (see below); empty array when nothing matches, not an error |
| `search` | array of message objects | empty array when nothing matches; not an error |
| `channel list` | array of channel objects | each has `id`, `members[]`, `member_count`, `messages`; with `--agent`, also `joined` and `unread` |
| `read` | single viewer-relative message object | message plus `unread`/`awaiting_me`/`my_status` (see below); recording the `read` receipt does not satisfy an ask, so `awaiting_me` still flags one you owe |
| `wait` | single viewer-relative message object | message plus `unread`/`awaiting_me`/`my_status`; exits `2` with `timeout` when no unread arrives. With `--owed`/`--resolved`, emits `{"ok":true,"resolved":{…}|null}` (the closed ask) instead |
| `watch` | newline-delimited viewer-relative message objects (NDJSON) | one JSON object per line (message plus `unread`/`awaiting_me`/`my_status`), streamed as messages arrive |
| `me`, `awaiting` | object `{"agent", "you_owe":[…], "owed_to_you":[…], …}` | `me` adds `unread`, `live_peers`, `conversations` |
| `roster`, `status` | object `{"root", "agents":[…], …}` | each agent carries `capabilities[]`; `status` adds `conversations` |
| `state get` | object `{"agent", "state", "note", "updated_at", "live", "last_seen_at", "expires_at"}` | `live` is the heartbeat-lease check (`roster`/`me` semantics), so a stale agent's leftover `state` is not read as current |
| `thread` | object `{"message", "children":[…], "truncated", "omitted"}` | `children` is a recursive list of the same node shape; when more than `--limit` messages are reachable the *newest* are kept (root always survives, dropped replies re-parent onto their nearest surviving ancestor) and `omitted` counts the rest, mirroring `show`/`inbox`/`search` |
| `receipts` | object `{"message", "recipients":[…], "receipts":{…}}` | `receipts` keyed by agent id |

A message object carries `id`, `conversation_id`, `kind`, `from`, `to[]`,
`mentions[]`, `subject`, `body`, `created_at`, `requires_ack`,
`needs_response_from[]`, `subject_id`, and `after` (the parent message id, or
`null` for a root).

`inbox`/`show`/`read`/`wait`/`watch` decorate each message with three fields relative
to the `--agent` reading it, so you can triage in one call without a follow-up
`awaiting`/`receipts` per message: `unread` (no read receipt yet),
`awaiting_me` (you are in the message's still-open awaited set — it requested an
ack or named you in `needs_response_from`, and you have not recorded a terminal
`done`/`rejected` receipt), and `my_status` (your current ack status on the
message, or `null`). `inbox --needs-action` filters to messages where `unread`
or `awaiting_me` is true — an agent's actionable queue.

The `send`/`reply` envelope's `offline_recipients[]` lists resolved recipients
whose heartbeat has expired (a `*` recipient expands to participants, the
sender is excluded). A sender that just delegated work with `--requires-ack`
can branch on it to reroute immediately, rather than discovering the silence
later by blocking on `wait` for a reply that will never come. Text mode prints
the same warning to stderr, leaving the message id alone on stdout.

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
| `not_awaited`     | `ack --require-open` closed no open ask the agent is awaited on |
| `not_authorized`  | a capability token does not authorize the requested action (wrong action/tool/conversation/env, expired, exceeds a limit, or a broken delegation chain) |
| `conflict`        | a resource already exists: an agent name claimed by another holder, or a channel/conversation that already exists (create without `--if-missing`) |
| `rate_limited`    | sender exceeded the conversation's message rate limit; `error` carries `retry_after_seconds`, `window_seconds`, `max_messages_per_sender`, and `count` for backoff |
| `too_large`       | message body exceeds the conversation's byte limit; `error` carries `size` and `limit` |
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
raft search --agent codex --from claude --kind message --mentions codex
raft thread MESSAGE_ID --agent codex
raft read codex MESSAGE_ID
raft ack codex MESSAGE_ID --status done --note "Handled."
raft withdraw MESSAGE_ID --from codex --reason "no longer needed"
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

`search` takes an optional substring pattern plus structured filters
(`--from`, `--kind`, `--mentions`, `--since`, `--conversation`/`--channel`)
that combine conjunctively. At least one criterion is required so the command
never dumps the whole bus by accident; `--mentions` matches both `@mentions`
and `to[]` recipients, and a `*` broadcast counts as reaching every member of
its room (so `--mentions me` surfaces broadcasts you received).

See [AGENTS.md](./AGENTS.md) for operating rules and
[docs/protocol.md](./docs/protocol.md) for the on-disk protocol.
