# Agent Instructions

Use the Rust `raft` CLI for concise, turn-based local A2A communication. The shared bus is:

```sh
export RAFT_ROOT=/Users/mohamad.hassan/workspace/raft/run/bus
export RAFT_BIN=raft
```

`raft` should be installed at `~/.local/bin/raft`. That directory is on PATH on
this machine, and the installed shim defaults `RAFT_ROOT` to the shared bus when
the caller has not set it. `make install` updates the stable wrapper target and
global shim with atomic renames; restart long-running `raft serve` processes to
pick up a newly installed binary.

## Identity

- Codex in this thread should use agent id `codex`.
- `home-keep-reviewer` is a separate reviewer identity and must not be
  impersonated by Codex.
- The developer agent working in `/Users/mohamad.hassan/workspace/home-keep`
  should use agent id `homekeep-dev`.
- Other agents must claim a stable, unique, personable name matching
  `[A-Za-z0-9_.-]`. Prefer names that read well as callouts, such as
  `@qa-scout`, `@build-sentinel`, or `@homekeep-dev`.
- Claim once during onboarding. Refresh with `heartbeat` while active.

```sh
$RAFT_BIN claim homekeep-dev \
  --workspace /Users/mohamad.hassan/workspace/home-keep \
  --capabilities implementation,tests,debugging
```

The claimed name is also the mention handle. Other agents can call you out as
`@agent-name` inside a channel or private chat.

## Channel Rules

- Prefer one channel per shared workstream. For HomeKeep coordination use
  `homekeep-sync`.
- Channels are group chats. Join a channel to subscribe the agent to
  notifications for that channel.
- Private side chats are for smaller subsets and should stay scoped.
- Normal `kind=message` messages require the sender to hold the turn.
- `kind=system` is reserved for raft internals. Do not send it manually.
- Include a specific request, current context, and expected response shape.
- Pass the turn when another agent should respond:

```sh
$RAFT_BIN send \
  --channel homekeep-sync \
  --from homekeep-dev \
  --to @codex \
  --subject "Estimator fix status" \
  --body "@codex implemented X. Blocked on Y. Please review Z." \
  --requires-ack \
  --pass-to codex
```

Join a channel:

```sh
$RAFT_BIN channel join homekeep-sync --agent homekeep-dev
```

Open a private side chat:

```sh
$RAFT_BIN conversation open \
  --from homekeep-dev \
  --to codex \
  --topic "implementation review"
```

Open a private group side chat:

```sh
$RAFT_BIN conversation open \
  --from codex \
  --to homekeep-dev,qa-agent \
  --topic "booking regression"
```

Use scoped status when working as an agent:

```sh
$RAFT_BIN status --agent homekeep-dev
```

## Bridge Rules

- Telegram, Slack, or browser bridge agents should claim a name, then join the
  channel as normal subscribers.
- Inbound human messages should use `--kind event` and a stable `--subject-id`.
- Bridge events do not take or pass the turn, so they should not use `--pass-to`.

```sh
$RAFT_BIN send \
  --channel homekeep-sync \
  --from telegram-bridge \
  --to codex,homekeep-dev \
  --kind event \
  --subject-id telegram:CHAT_ID:USER_ID \
  --subject "Telegram inbound" \
  --body "User asked for the current deployment status."
```

## Anti-Spam Rules

- Batch related notes into one message.
- Prefer `wait --interval 2` for background loops. Use a lower interval such as
  `0.25` only for an active pair-coding loop.
- Do not send repeated status pings. Send one message, pass the turn, and wait.
- Bridge agents must use `--subject-id` so one noisy human does not throttle the
  whole bridge.
- Keep messages under the default 32 KiB limit. Link to files for large logs.

## Feedback Loop

- Always `read` a message before acting on it.
- Always `ack` with a useful terminal status:
  - `accepted`: you took ownership.
  - `done`: the requested action is complete.
  - `blocked`: you cannot proceed without input or an external change.
  - `rejected`: the request is invalid or outside scope.
- Include a short `--note` when the status is not self-explanatory.

```sh
$RAFT_BIN --root "$RAFT_ROOT" read homekeep-dev MESSAGE_ID
$RAFT_BIN --root "$RAFT_ROOT" ack homekeep-dev MESSAGE_ID --status accepted --note "Starting now."
```

## Journals

- Use `journal` for private per-agent notes that should be discoverable but
  should not notify other agents.

```sh
$RAFT_BIN --root "$RAFT_ROOT" journal homekeep-dev --subject checkpoint --body "Ran tests; next is deploy check."
```

## Privacy

- Use private chats for messages that should only be visible through the CLI
  to named participants.
- The bus directory is chmod `0700`, and files are chmod `0600`.
- This is same-user local privacy. Do not put credentials or secrets in raft
  messages.

## Robustness

- If a command exits non-zero, do not retry in a loop. Read the error and fix
  the state or wait for the monitor.
- If a turn holder disappears, run:

```sh
$RAFT_BIN --root "$RAFT_ROOT" gc
```

- If continuous cleanup is needed, run:

```sh
$RAFT_BIN --root "$RAFT_ROOT" serve --interval 2 --archive
```

- Only one `serve` process should run per bus. It owns `locks/serve.lock`.
- The bus is same-host only. Do not share it over NFS/SMB/cloud-sync folders.
- Lease expiry uses wall-clock timestamps. If the laptop sleeps mid-turn or
  mid-lock, expect a forced handoff after wake.
- Do not edit JSON state by hand unless the CLI is unavailable. If manual edits
  are required, acquire the corresponding lock directory first and commit by
  writing a temp file in the target directory, fsyncing it, and atomically
  renaming it into place.
