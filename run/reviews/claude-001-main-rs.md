# raft code review — src/main.rs + tests/cli.rs

Reviewer: @claude (read-only pass)
Target: src/main.rs (2160 LOC), tests/cli.rs (255 LOC)
Date: 2026-05-28

## Headline

Much more rigorous than the protocol doc suggests. Five things I griped about in my earlier "watching, not touching" critique are already done in code:

- `_v` schema field on every record (line 24, plus on Agent, Meta, Turn, Message, Receipt, LockOwner, RateState, JournalEntry)
- Per-agent reasoning journal (`raft journal`, line 1183; `journal/<id>.jsonl`)
- IM-bridge contract: `kind="event"` bypasses turn enforcement (line 1010-1018)
- `subject_id` adds a per-subject dimension to rate-keys, so a bridge agent doesn't throttle N humans on one cap (line 1567)
- `@mention` parsing auto-adds mentioned participants as recipients (line 1992-2030)
- `serve.lock` prevents two concurrent `raft serve` instances (line 1389)

Striking those from the earlier critique.

## P0 — correctness bugs

### 1. UTF-8 panic in `cmd_inbox` (line 1097)

```rust
if body.len() > 120 {
    body.truncate(117);
    body.push_str("...");
}
```

`String::truncate` panics if the offset is not a char boundary. Any non-ASCII body whose first 117 bytes end mid-codepoint crashes `raft inbox`. The first agent that sends a message containing an emoji, a non-Latin character, or curly quotes lands on this.

Patch:

```rust
if body.len() > 120 {
    let mut end = 117.min(body.len());
    while !body.is_char_boundary(end) { end -= 1; }
    body.truncate(end);
    body.push_str("...");
}
```

### 2. `archive_old_messages` orphans receipts (line 1712)

Moving `messages/<id>.json` to `archive/<conv>/` does NOT move `receipts/<id>/`. Consequences:

- `receipts/` grows unboundedly even with `serve --archive` running
- `find_message` (line 1649) only scans `conversations/<id>/messages/`, so reading an archived message id returns 404
- `receipt_path_for` (line 1824) still computes a valid path that points at an abandoned receipt directory

Fix:
- Either `fs::rename` `receipts/<id>/` next to the message, or `fs::remove_dir_all` it after archive
- Optional: extend `find_message` to also search `archive/<conv>/<id>.json` so historical `read`/`ack` still work; archive search is bounded by retention_days * conversations.

### 3. Rate-key separator collision (line 1567, 2097)

```rust
fn rate_key(sender: &str, subject_id: Option<&str>) -> String {
    match subject_id {
        Some(subject_id) => format!("{sender}#{subject_id}"),
        None => sender.to_string(),
    }
}
```

`validate_subject_id` (line 2087) permits `#` in subject_ids. So `(sender="a", subject="b#c")` and `(sender="a#b", subject="c")` collide on the same rate bucket. A bridge that accepts user-controlled subject_ids can be tricked into bypassing per-subject rate limits, or into starving another sender's bucket.

Fix: forbid `#` in `validate_subject_id`, or use a separator that's banned in both ids (e.g., a control character).

### 4. Turn expiry on send is silently destructive (line 993)

```rust
let (conv, meta, mut turn) = load_conversation(root, &conversation_id)?;
turn = maybe_advance_expired_turn(root, &conv, &meta, &turn)?.0;
ensure_participant(&meta, &sender)?;
...
if kind_requires_turn(&kind) && turn.holder.as_deref() != Some(&sender) {
    bail!("turn is held by {:?}; ...", turn.holder);
}
```

`cmd_send` runs `maybe_advance_expired_turn` BEFORE the holder check. If your turn lease quietly expired while you were composing a message (slow review, agent thinking), your send fails with "turn is held by <other-participant>" — even though you were the holder a moment ago. The reassignment is silent from the sender's perspective.

Options:
- Grace period: if previous holder == sender and the turn expired within last N seconds, extend the lease and accept the send
- Clearer error: "your turn (held since T) expired at T+TTL and was reassigned to X; retry with `pass-turn --force` or wait"

## P1 — quality / UX

### 5. `cmd_wait` uses `process::exit(2)` on timeout (line 1134)

Inconsistent with the rest of the code path (typed `RaftError` → stderr → main's centralized exit). `process::exit` skips Drop for any in-scope guards. Wrap in a typed timeout error or at minimum document the exit code in `--help`.

### 6. `cmd_serve` lock refresh gap (line 1395-1405)

`serve_lock.refresh(...)` is called before and after `cmd_gc`, but not during. On a bus with many conversations, gc can exceed `SERVE_LOCK_TTL_SECONDS=30` (each conversation acquires its own lock for archive scans). If serve_lock expires mid-gc, a competing `raft serve` wins the lock and you have two reapers racing.

Mitigations: refresh from inside `cmd_gc` per-conversation, lengthen `SERVE_LOCK_TTL_SECONDS`, or run gc in a worker thread with serve_lock heartbeats on the main thread.

### 7. `DirLock::drop` swallows read errors (line 549)

```rust
let owner: Option<LockOwner> = read_json(&self.path.join("owner.json")).ok().flatten();
if owner.map(|item| item.token == self.token).unwrap_or(false) {
    let _ = fs::remove_dir_all(&self.path);
    ...
}
self.acquired = false;
```

If `owner.json` read fails for any reason other than `ENOENT` (truncated, permission, etc.), the lock is silently left in place. gc cleans up eventually but it's a stealth leak. Either log via `eprintln!` on the error path or bubble it up explicitly.

### 8. Fixed-window rate limiter (line 1577)

```rust
if (now - window_start).num_seconds() >= meta.rate.window_seconds as i64 {
    entry.window_start = iso_now();
    entry.count = 0;
}
```

Classic fixed-window bypass: sender can ship `max` at T-1s and another `max` at T+1s for 2*max in ~2 seconds. Acceptable for anti-spam; document the bound, or move to a sliding-window ring buffer if a TG bridge with adversarial users ever ships.

### 9. `--interval` panic on negative/NaN (line 1404, 1136)

`Duration::from_secs_f64` panics on negative, NaN, or > u64::MAX seconds. Clap accepts any `f64`. `raft serve --interval -1` crashes. Validate at parse time:

```rust
fn parse_positive_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e| format!("{e}"))?;
    if !v.is_finite() || v <= 0.0 { return Err(format!("must be > 0")); }
    Ok(v)
}
```

### 10. `extract_mentions` over-matches (line 2009)

`"email@example.com"` extracts `example`. Backtick code blocks containing `@agent-name` also match. Only fires if the matched name is a participant, so the cost is just adding a participant who's already in the conversation to the recipient list — harmless functionally, surprising semantically. Acceptable, but worth a `// NOTE:` so the next author doesn't try to "fix" it without context.

## P2 — docs and hygiene

- `docs/protocol.md` shows `rate.json` in the layout (line 21) but rate config is stored in `meta.json.rate`. Doc and code disagree.
- `docs/protocol.md` does not document: `kind="event"`, `subject_id`, `mentions`, the `journal/` directory, `claim` vs `register` distinction, `serve.lock`. These are exactly the primitives a new agent needs to know about before extending the bus.
- `README.md` does not mention `raft journal` or the `raft channel create|join` workflow.
- `Cargo.toml`: edition = "2024" implies rustc ≥ 1.85 — pin via `rust-toolchain.toml` for reproducible builds. No `[lints]` section.
- No `LICENSE` file. With this much rigor, ship one.

## Ship order

1. **P0 #1** (UTF-8 panic) — single agent message with a 🚀 in it crashes `raft inbox` for everyone. Fix today.
2. **P0 #3** (rate-key collision) — must land before any TG/Slack bridge with user-controlled `subject_id` ships.
3. **P0 #2** (orphaned receipts) — fix within the week. Currently a slow leak.
4. **P0 #4** (silent turn expiry) — annoyance not crash; fix when you next touch `cmd_send`.
5. P1 items as time permits.
6. P2 docs in the next docs pass.

