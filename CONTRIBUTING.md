# Contributing to raft

raft is a filesystem-backed agent-to-agent coordination bus, growing a
peer-to-peer mesh layer (cryptographic identity, capability tokens, remote task
delegation). Its primary users are autonomous agents that parse its JSON output,
so **correctness and a stable contract matter more than features**.

## Ground rules

1. **The JSON contract is load-bearing.** Mutating commands emit
   `{"ok":true,...}`; read commands emit bare data; failures emit
   `{"ok":false,"error":{"code","message"}}` on stderr with a *stable* error
   code. Don't change a field's meaning or an error code without a deliberate,
   documented reason — agents depend on these.
2. **New fields are additive and optional.** Every record carries a schema
   version (`_v`) and deserializes with `#[serde(default)]` for new fields. A new
   build must read every record an older build wrote, and vice versa where
   possible. Mesh/integrity fields (signatures, capability tokens) are opt-in:
   an unsigned local-only bus keeps working.
3. **Wire formats are re-implementable.** Like the filesystem protocol, every
   on-the-wire and on-disk format is specified so another language can implement
   it: canonical JSON (sorted keys, no insignificant whitespace), Ed25519,
   SHA-256, hex. No format may depend on a Rust crate's internal representation.
4. **Tests are the spec in executable form.** Every behavior change ships with a
   test in `tests/`. Bug fixes ship with a regression test that fails before the
   fix. `cargo build --release` and `cargo test --release` must be green.
5. **Security posture.** The mesh assumes an untrusted network: authenticity is
   per-record (signed), authority is explicit (capability tokens), and isolation
   is enforced (sandbox), not optional. If a change weakens any of these,
   document the threat model honestly.

## Workflow

- Read `docs/protocol.md` (local filesystem protocol) and, for mesh work,
  `docs/superpowers/specs/2026-05-29-raft-mesh-remote-execution-design.md`.
- Branch from `main`. Keep changes focused — one concern per PR.
- Run `cargo build --release && cargo test --release` before pushing.
- Update `README.md`, `CHANGELOG.md` (`[Unreleased]`), and `docs/` when behavior
  or the contract changes.
- Commit messages: imperative subject, a body that explains *why*, and follow the
  repo's existing style.

## Project layout

- `src/main.rs` — command dispatch and most command implementations.
- `src/cli.rs` — clap argument definitions.
- `src/types.rs` — on-disk/serialized data model.
- `src/error.rs` — `RaftError` and the stable error codes.
- `src/storage.rs`, `src/util.rs` — atomic writes, locking, time, ids.
- `src/crypto.rs`, `src/identity.rs`, `src/capability.rs`, `src/mesh/` — the
  mesh layers (identity, signing, capabilities, federation).
- `tests/cli.rs` — end-to-end integration tests driving the built binary.

## License

By contributing you agree your contributions are licensed under the project's
dual license, **MIT OR Apache-2.0** (`LICENSE-MIT`, `LICENSE-APACHE`).
