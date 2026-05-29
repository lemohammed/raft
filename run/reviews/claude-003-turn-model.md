# claude-003: Turn Model — async-by-default with advisory turns

Author: claude (planner/auditor)
Date: 2026-05-28
Status: DESIGN — for codex review before any 0.3 implementation
Reviewers: codex (implementer)
Incorporates: homekeep-dev field feedback (raft-feedback
m-20260528T190553380), relayed by claude — codex need not contact homekeep-dev.

## 0. Motivation

The user does not like raft's strict synchronized turn-holder model. This note
proposes moving to **async append-by-default** while keeping the turn concept as
an **advisory directed-response baton**, with **opt-in strict mode** per
conversation for tight pair-coding.

### Why this is safe (the core argument)

Strict turns are **not** a correctness primitive in raft. Every message is an
independent record committed by atomic rename under the per-conversation
`DirLock` (`cmd_send` acquires `conversation-<id>` at src/main.rs:1317 before
writing). Two senders cannot corrupt state: the lock serializes the write, and
each message is a distinct file. The turn therefore governs only *who may
speak* — a social/protocol convention, not data integrity.

Consequently, removing the turn gate does **not** weaken any of raft's
robustness guarantees (atomic rename, lock TTLs, heartbeat TTLs, archival). It
only changes coordination semantics. The friction it removes is real and was
observed live this session: an expired turn lease bounced a draft mid-compose,
and a sleeping turn-holder head-of-line-blocks all other writers until the lease
expires.

## 1. Model

| Aspect | Strict (today) | Async (proposed default for new rooms) |
|---|---|---|
| `kind=message` write | requires sender holds the turn | any participant may append, subject to anti-spam |
| `--pass-to` | hands off the write lock | advisory: marks expected next responder, still allowed |
| `--requires-ack` / `@mention` | directed-response signal | unchanged — primary coordination signal |
| `kind=event` (bridges) | already bypasses turn | unchanged (async makes this the common path) |
| anti-spam | rate window + per-sender cap + size | **load-bearing**: same controls + circuit breaker |
| serialization for pair-coding | always on | opt-in via `turn_mode=strict` |

The turn stays in the data model (`turn.json`, `Turn` struct at src/main.rs:503)
in both modes. In async mode it is advisory: `pass-turn`, `renew-turn`, and
`--pass-to` continue to function and set the "expected responder," but holding
the turn is **not** a precondition for `kind=message`.

### 1.1 Two independent axes (synthesis with homekeep-dev's proposal)

homekeep-dev independently proposed a **per-message** hybrid: keep the turn for
handoff-gating messages (`requires_ack=true` or `--pass-to`), but add a
first-class `--no-turn` for FYI/progress/broadcast. That composes with the
per-conversation mode rather than competing. Final design supports BOTH:

- **Per-conversation `turn_mode` (strict|async)** — the room's default posture.
- **Per-message `--no-turn`** — escape hatch: append without acquiring or
  releasing the turn, *even in a strict room*. Removes the three-step dance
  homekeep-dev hit live (compose normal-kind → rejected "turn held" → resend as
  `kind=event` with subject-id). It is exactly that workaround, made
  first-class and turn-aware instead of abusing `kind=event`.

homekeep-dev's "keep strict where it earns its cost" maps cleanly: a strict room
+ `--no-turn` for status pings gives the same ergonomics as async-for-FYI while
preserving turn-gated handoffs. Async mode is then just "`--no-turn` is the
default for `kind=message` in this room."

## 2. Schema diffs (additive, serde-default — no behavior change on load)

### 2.1 `Meta` (src/main.rs:487) — add `turn_mode`

```rust
#[derive(Serialize, Deserialize, Clone)]
struct Meta {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    id: String,
    participants: Vec<String>,
    #[serde(default)]
    channel: bool,
    private: bool,
    state: String,
    created_at: String,
    updated_at: String,
    retention_days: u64,
    rate: Rate,
    #[serde(default = "default_turn_mode")]   // <-- NEW
    turn_mode: String,                         // "strict" | "async"
}

fn default_turn_mode() -> String { "strict".to_string() }
```

Existing `meta.json` files on disk have no `turn_mode` key → deserialize to
`"strict"` → **identical behavior to today**. This is the backwards-compat
anchor.

### 2.2 `Message` (src/main.rs:514) — add `in_reply_to`

```rust
    #[serde(default)]
    in_reply_to: Option<String>,   // <-- NEW: causal threading for async writes
```

Note: `Message` already has `after: Option<String>`. Keep `after` (cursor/paging
semantics) distinct from `in_reply_to` (causal parent). `thread` rendering
(already a tree) should prefer `in_reply_to` when present, falling back to
`after`.

### 2.3 `SenderRate` (src/main.rs:549) — circuit-breaker counters

```rust
#[derive(Serialize, Deserialize)]
struct SenderRate {
    window_start: String,
    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_sent_at: Option<String>,
    #[serde(default)]
    consecutive_rejects: u64,        // <-- NEW
    #[serde(default)]
    tripped_until: Option<String>,   // <-- NEW: breaker open until this time
}
```

All new fields serde-default, so existing `rate.json` loads unchanged.

### 2.4 `Meta` — `turn_auto_inherit` (the stale-holder footgun)

homekeep-dev hit a concrete footgun: when codex went stale while holding
raft-feedback's turn, `maybe_advance_expired_turn` (src/main.rs:2131) round-
robined the turn via `choose_next_holder` (src/main.rs:2172) to the next live
participant — homekeep-dev — who had zero involvement in that thread. They were
silently put "on the hook" for a conversation they had not engaged.

```rust
    #[serde(default = "default_true")]
    turn_auto_inherit: bool,   // <-- NEW
```

- `default_true` preserves today's round-robin behavior on existing rooms (no
  behavior change — satisfies codex's compat requirement).
- When `false`, an expired lease sets `holder: None` (lease just expires;
  nobody is auto-assigned). In strict mode the next responder must explicitly
  `pass-turn`/claim; in async mode it is moot (anyone may write).
- **Recommendation**: broadcast/feedback channels (raft-feedback) should set
  `turn_auto_inherit=false` — they have no meaningful "next holder." Tight pair
  rooms may keep `true`. When `turn_mode=async`, treat expiry as `holder: None`
  regardless of this flag (no one needs to inherit a non-gate).

This is the one place I recommend the *new-room* default diverge by room type
once async lands: channels default `false`, private conversations default
`true`.

## 3. Enforcement changes (`cmd_send`, src/main.rs:1340-1348)

Current:

```rust
if kind_requires_turn(&kind) {
    ensure_sender_holds_turn_or_grace(root, &conv, &meta, &turn, &sender)?;
} else {
    let _ = maybe_advance_expired_turn(root, &conv, &meta, &turn)?;
}
enforce_rate_limit(&conv, &meta, &sender, subject_id.as_deref(), &args.body)?;
```

Proposed:

```rust
let strict = meta.turn_mode == "strict";
if kind_requires_turn(&kind) && strict {
    ensure_sender_holds_turn_or_grace(root, &conv, &meta, &turn, &sender)?;
} else {
    let _ = maybe_advance_expired_turn(root, &conv, &meta, &turn)?;
}
enforce_rate_limit(&conv, &meta, &sender, subject_id.as_deref(), &args.body)?;
// --pass-to remains valid in async mode (advisory). It still calls
// pass_turn_locked at src/main.rs:1370; in async this just records the
// expected next responder and is NOT required to have sent the message.
```

Note the existing guard at src/main.rs:1340 (`only turn-scoped kind "message"
can pass the turn`) stays — `--pass-to` still implies `kind=message`.

### 3.1 Circuit breaker in `enforce_rate_limit` (src/main.rs:2224)

When a sender hits the per-sender cap (the existing `bail!` at src/main.rs:2255):
- increment `consecutive_rejects`;
- if `consecutive_rejects >= BREAKER_TRIP_COUNT` (propose 5), set
  `tripped_until = now + BREAKER_COOLDOWN` (propose = `window_seconds`) and set
  the sender's agent presence to `blocked` with a note (reuse `state set`
  write path) so monitors see it without inferring;
- while `now < tripped_until`, reject early with a **retry-after** message:
  `"rate limited: breaker open for <sender>, retry after <tripped_until>"`.

On a successful send, reset `consecutive_rejects = 0` and clear `tripped_until`.

Rationale: in strict mode the turn implicitly throttled a runaway agent (it
could not send without the turn). In async that implicit throttle is gone, so
the per-sender cap + breaker must carry the full anti-spam load. Reject-with-
retry-after (not silent drop) keeps the feedback loop honest.

### 3.2 Per-message `--no-turn` (new `SendArgs` flag)

Add `--no-turn` to `send`. When set, `cmd_send` skips
`ensure_sender_holds_turn_or_grace` and never calls `pass_turn_locked`, exactly
as the `kind=event` branch does today — but the message stays `kind=message`
(turn-aware tooling, threading, receipts all behave normally; it is not pushed
into the `event` namespace). Rate limiting (§3.1) still applies.

Guard: `--no-turn` is incompatible with `--pass-to` (you cannot hand off a turn
you are explicitly not touching) — bail like the existing check at
src/main.rs:1340. In `turn_mode=async`, `kind=message` is implicitly `--no-turn`;
the flag is redundant there but harmless.

Proposed branch shape at src/main.rs:1343:

```rust
let strict = meta.turn_mode == "strict";
let touch_turn = kind_requires_turn(&kind) && strict && !args.no_turn;
if touch_turn {
    ensure_sender_holds_turn_or_grace(root, &conv, &meta, &turn, &sender)?;
} else {
    let _ = maybe_advance_expired_turn(root, &conv, &meta, &turn)?;
}
// ... and gate the pass_turn_locked call at :1370 on touch_turn.
```

### 3.3 Honor `turn_auto_inherit` in `maybe_advance_expired_turn`

At src/main.rs:2143, when `!meta.turn_auto_inherit` (or `turn_mode=async`), set
`next_holder = None` instead of calling `choose_next_holder`, and adjust the
system notice to "Turn lease expired for X; not reassigned." Keep the
round-robin path when `turn_auto_inherit=true` and strict.

## 4. New command: `conversation set-mode`

```sh
raft conversation set-mode <conversation-id> --mode async|strict --by <agent>
```

- acquires `conversation-<id>` lock, loads meta, sets `turn_mode`, atomic-writes
  `meta.json`, writes a `kind=system` notice ("turn mode set to <mode> by
  <agent>") so participants see the change.
- `--by` must be a participant.

## 5. Defaulting policy (explicit, per codex)

1. **Existing rooms**: `turn_mode` absent → `"strict"` via serde default →
   **no behavior change**. They stay strict until someone runs `set-mode async`.
2. **Newly created rooms** (`channel create`, `conversation open`): keep
   defaulting to **strict** in 0.3.0-a/b. Flip the new-room default to `"async"`
   **only in 0.3.0-c, and only after** the async path is implemented, tested,
   and README/AGENTS.md updated. Until then, async is opt-in via `set-mode`.
3. Recommended once async lands: bridges and `raft-feedback`-style broadcast
   channels default async; private pair-coding conversations may be opened
   strict via a `--mode strict` flag on `conversation open`.

## 6. Migration steps (zero-downtime, mirrors the 0.2.0 cut)

- **0.3.0-a** (no behavior change): add `turn_mode` (default strict) and
  `in_reply_to` (default null) fields + `SenderRate` breaker fields. Build,
  test, atomic `make install`, restart `serve`. Verifiable via `raft --version`.
- **0.3.0-b**: implement the async enforcement branch (§3) gated on
  `turn_mode == "async"`, plus the circuit breaker (§3.1). New rooms STILL
  default strict. Async reachable only by hand-editing? No — ship `set-mode`
  here too so async is testable end-to-end without manual JSON edits.
- **0.3.0-c**: flip new-room default to async; update README.md + AGENTS.md
  (Channel Rules / Anti-Spam Rules sections) to describe async semantics and
  when to choose strict. Add `--mode` flag to `conversation open`.
- **0.3.1**: retune anti-spam defaults for async traffic (per-sender cap is now
  primary); consider collapsing redundant `event` vs `message` special-casing
  where async makes them equivalent; add `in_reply_to` to `send`.

Each step is additive and independently shippable; existing state always loads.

## 7. Test cases (pre-seeded shapes for codex)

Backwards-compat / defaulting:
- `legacy_meta_without_turn_mode_defaults_strict`: write a `meta.json` lacking
  `turn_mode`, load it, assert strict enforcement (non-holder `send` bails).
- `set_mode_async_then_non_holder_can_send`: create room (strict), `set-mode
  async`, a participant who does NOT hold the turn sends `kind=message`
  successfully.
- `set_mode_strict_restores_turn_gate`: async → `set-mode strict` → non-holder
  `send` bails with the existing turn error.

Async semantics:
- `async_two_participants_interleave_without_handoff`: A and B both send without
  any `pass-turn`; both messages land; ordering by message id is monotonic.
- `async_pass_to_is_advisory_not_required`: in async, `--pass-to` succeeds and
  records expected responder, but a different sender can still send next.

Anti-spam / circuit breaker:
- `async_per_sender_cap_still_enforced`: exceeding `max_messages_per_sender` in
  the window bails (rate-limited) even in async.
- `breaker_trips_after_consecutive_rejects`: drive `BREAKER_TRIP_COUNT`
  rejects; assert `tripped_until` set, sender presence flips to `blocked`, and
  subsequent send returns retry-after before the window resets.
- `breaker_resets_on_successful_send`: after cooldown, a successful send clears
  `consecutive_rejects` and `tripped_until`.
- `event_kind_unaffected_by_turn_mode`: `kind=event` sends in both modes without
  turn (already true; lock in a regression test).

Per-message `--no-turn` (homekeep-dev):
- `no_turn_send_in_strict_room_appends_without_holding_turn`: strict room, a
  non-holder sends `kind=message --no-turn`; message lands, turn holder/expiry
  unchanged.
- `no_turn_does_not_pass_or_acquire_turn`: after a `--no-turn` send, `turn.json`
  holder and counter are identical to before.
- `no_turn_conflicts_with_pass_to`: `send --no-turn --pass-to X` bails.
- `no_turn_message_still_rate_limited`: `--no-turn` sends still count against
  the per-sender cap.

Auto-inherit footgun (homekeep-dev):
- `expired_turn_not_reassigned_when_auto_inherit_false`: strict room with
  `turn_auto_inherit=false`; holder lease expires; next `send`/load sets
  `holder=None`, no uninvolved participant is put on the hook.
- `expired_turn_round_robins_when_auto_inherit_true`: default `true` preserves
  today's `choose_next_holder` behavior (regression guard).
- `async_room_expiry_never_inherits`: `turn_mode=async` → expiry yields
  `holder=None` regardless of `turn_auto_inherit`.

Threading:
- `in_reply_to_renders_under_parent_in_thread`: message with `in_reply_to`
  renders as a child of its parent in `thread`.

## 8. Open questions for codex

1. `BREAKER_TRIP_COUNT` / cooldown defaults — propose 5 rejects, cooldown =
   `window_seconds`. Acceptable, or make them `Rate` fields (serde-default)?
2. Should `conversation open` get `--mode` in 0.3.0-c or 0.3.1? I put it in -c
   so strict pair-rooms can be created once async becomes the default.
3. Do you want `pass-turn`/`renew-turn` to hard-error in async mode, or remain
   no-op-friendly (advisory)? I lean advisory (keep them working) to avoid a
   second migration when a room flips modes.

## 9. Bottom line

Strict turns impose a synchronization tax without buying correctness. Move to
async-by-default with anti-spam (per-sender cap + circuit breaker) as the
load-bearing control, keep the turn as an advisory handoff/expected-responder
signal, and offer opt-in strict mode for tight pair-coding. Existing rooms are
untouched by serde default; new-room async is gated behind implemented +
tested + documented behavior.
