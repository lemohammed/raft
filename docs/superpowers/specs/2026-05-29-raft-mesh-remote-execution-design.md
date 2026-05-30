# raft mesh — remote execution & agent-org protocol (design)

> Status: **design, self-approved for implementation** (operator delegated full
> autonomy on 2026-05-29). Open source (Apache-2.0).
>
> This document extends `raft` — a same-host, same-user, filesystem-backed
> agent-to-agent coordination bus — into a **peer-to-peer agent mesh** with
> **remote task delegation**, **cryptographic identity**, **capability-scoped
> authority**, **sandboxed execution**, and **organizational structure**, over an
> **untrusted network**. It is benchmarked against **Letta** (stateful agents
> server) and the **Nous Hermes** structured tool-call format, and is informed by
> a parallel deep-research gap analysis (see `## Prior art & gaps`, reconciled as
> the research lands).

## 0. Why this shape

raft already has the rare, valuable part that most agent frameworks lack: a
**first-class obligation primitive**. A message can *obligate* named recipients
(`needs_response_from` / `requires_ack`), an obligation is *closed* only by a
*terminal receipt* (`done`/`rejected`) with a tamper-resistant `history`, and
the whole thing is observable (`awaiting`, `wait --owed/--resolved`, `withdraw`).
Letta, CrewAI, AutoGen, LangGraph, and OpenAI Swarm orchestrate *calls*; raft
tracks *commitments*. Remote execution is, fundamentally, a commitment made
across a trust boundary. So the design does **not** bolt an RPC system onto raft;
it **lifts raft's obligation engine across the network** and adds exactly the
machinery a trust boundary forces: verifiable identity, scoped authority,
replicated tamper-evident logs, and isolated execution.

Design principles (inherited from raft, extended):

1. **Local-first, federation second.** Each host keeps running its own local bus.
   The mesh *federates* buses; it never requires a central server. A partition
   degrades to "local still works, remote catches up on reconnect."
2. **Portable, re-implementable protocol.** Like the filesystem protocol, every
   wire format is specified in terms other languages can implement (canonical
   JSON, Ed25519, SHA-256, hex). No format depends on a Rust crate's internals.
3. **Authenticity is end-to-end, not transport-deep.** Every record is signed by
   its author. A relay, a cache, or a malicious peer can move bytes but cannot
   forge, reorder undetectably, or silently drop without it being provable.
4. **Authority is explicit and attenuable.** Nothing is permitted because "you
   reached the port." Every remote action presents a capability token that some
   key authorized, narrowed at each delegation hop.
5. **Reuse before invention.** Remote delegation is an *ask* carrying a *tool
   call*; task status *is* the receipt lifecycle; cancel *is* withdraw. We extend
   the vocabulary, we do not fork the model.

## 1. Layered architecture

```
L6  Organization     roles, delegation trees, accountability        (raft org)
L5  Execution        sandboxed tool runner, artifacts, log stream    (raft run)
L4  Delegation       task = capability-gated Hermes tool-call ask     (raft task)
L3  Federation       node daemon, peer registry, log replication     (raft node/peer)
L2  Authority        attenuable capability tokens                     (raft grant)
L1  Integrity        signed, hash-chained message & receipt logs      (transparent)
L0  Identity         Ed25519 keypairs + agent passports               (raft id)
----------------------------------------------------------------------------------
     raft core        conversations, messages, asks, receipts, liveness (unchanged)
```

Each layer is independently useful and independently testable. L0–L1 add value
**even on a single host** (verifiable authorship, tamper-evident history). L2
adds value the moment two agents don't fully trust each other. L3+ light up the
network. We build bottom-up; every layer ships green with tests before the next.

## 2. L0 — Identity

**Problem it closes.** Letta agents are rows in a server DB; their "identity" is
a server-assigned id with no cryptographic basis and no portability between
hosts. There is no way for agent B to prove to agent C that a message "from A"
was actually authored by A. On an untrusted mesh this is fatal.

**Design.**

- Each agent owns an **Ed25519 keypair**. The private key lives at
  `agents/<id>.key` (mode `0600`, never leaves the host). Ed25519 chosen for
  small keys/sigs (32B/64B), fast verification, deterministic signatures, and
  universal language support.
- An **agent passport** is a small signed document binding the human-readable
  `id` to the public key:

  ```json
  {
    "_v": 1,
    "id": "codex",
    "pubkey": "ed25519:9f86d081...",   // hex
    "capabilities": ["plan", "code"],
    "issued_at": "2026-05-29T12:00:00Z",
    "sig": "hex(ed25519(canonical(passport_without_sig)))"
  }
  ```

  The passport is **self-signed** by default (sovereign identity). An org may
  additionally counter-sign passports (see L6) to vouch for membership; that is
  an *additional* signature, never a replacement — identity is always rooted in
  the agent's own key.
- **Mesh address** = `(id, home_node)`. `id` is unique within a node; across the
  mesh, the `pubkey` is the true identity and `id` is a convenience label. A
  receiver always trusts the *key*, displays the *id*, and flags collisions
  (same id, different key) loudly.

**Signing model (L1 depends on this).** Define **canonical bytes** of a JSON
object as: UTF-8, object keys sorted lexicographically by Unicode code point, no
insignificant whitespace, numbers in shortest round-trippable form (RFC 8785 /
JCS subset — we implement the subset raft emits, which is strings, ints, bools,
arrays, nested objects). A signature is `ed25519(sk, canonical(fields))` over the
record's fields **excluding** the signature fields themselves, hex-encoded.

**CLI.** `raft id new <id>`, `raft id show <id>`, `raft id verify <passport.json>`,
`raft id fingerprint <id>`.

**GAP closed.** Portable, sovereign, verifiable agent identity — absent from
Letta/Hermes/CrewAI/AutoGen.

## 3. L1 — Integrity: signed, hash-chained logs

**Problem it closes.** raft records are plain JSON on a trusted FS. Across hosts,
a relay could tamper, reorder, replay, or drop. Letta's history is a server DB
with no client-verifiable integrity; the Hermes format signs nothing.

**Design.**

- Every `Message` and `Receipt` gains optional integrity fields (back-compatible;
  absent on legacy/local-only records):

  ```jsonc
  {
    // ...existing message fields...
    "signer": "codex",                 // agent id
    "signer_key": "ed25519:9f86...",   // pubkey that must verify
    "author_seq": 42,                  // per-(author,conversation) counter
    "author_prev": "sha256:...",       // hash of this author's previous record
    "hash": "sha256:...",              // sha256(canonical(record_without_hash_sig))
    "sig": "hex(...)"                  // ed25519 over canonical(record_without_sig)
  }
  ```

- **Per-author hash chain.** A linear conversation-wide chain is impossible with
  concurrent multi-writers without consensus — and we explicitly reject
  consensus (it kills local-first/partition tolerance). Instead each *author*
  maintains a hash chain of *their own* records within a conversation
  (`author_seq` + `author_prev`). This is enough to: detect dropped/withheld
  messages from an author (gap in `author_seq`), detect tampering (broken hash),
  and prevent silent reordering of an author's own stream. Conversation ordering
  remains `created_at` then `id`, exactly as today.
- **Verification on ingest.** A node accepts a remote record only if: the `sig`
  verifies against `signer_key`; the `signer_key` matches the sender's known
  passport; `author_prev`/`author_seq` continue that author's known chain (or
  open it at seq 0); and the author is a participant per the conversation's
  (replicated, signed) `meta`. Otherwise the record is rejected and logged.
- **No consensus, deliberately.** The home node of a conversation (L3) is the
  tie-breaker for the *mutable* `meta` (participant set, rate config). Immutable
  signed records (messages, receipts) need no tie-breaker — they are
  append-only and self-authenticating. This is CRDT-flavored, not Paxos/Raft.

**GAP closed.** Client-verifiable, tamper-evident, replayable history with
gap detection — the substrate for accountability (L6) and the thing every
surveyed framework lacks.

## 4. L2 — Authority: attenuable capability tokens

**Problem it closes.** "Delegation of authority" is the single biggest hole in
the field. Letta has API keys (all-or-nothing, per server). CrewAI/AutoGen pass
Python objects in one trusted process. None can express "agent A lets agent B,
but only B, run *only* the `deploy` tool, *only* against staging, *only* for the
next hour, and B may sub-delegate a strictly narrower slice to C." That sentence
is the whole game for autonomous orgs.

**Design — biscuit/macaroon-inspired, specified for re-implementation.**

A **capability token** is a chain of signed blocks:

```jsonc
{
  "_v": 1,
  "root_issuer": "ed25519:<alice_pubkey>",
  "blocks": [
    { "issuer": "ed25519:<alice_pubkey>",
      "holder": "ed25519:<bob_pubkey>",
      "caveats": { "conversation": "deploy-room", "action": ["task.dispatch","task.result"],
                   "tool": ["deploy"], "env": ["staging"], "expires_at": "2026-05-29T13:00:00Z" },
      "sig": "hex(ed25519(alice_sk, canonical(block_without_sig)))" },
    { "issuer": "ed25519:<bob_pubkey>",          // attenuation hop: Bob -> Carol
      "holder": "ed25519:<carol_pubkey>",
      "caveats": { "tool": ["deploy"], "env": ["staging"], "max_runtime_s": 60,
                   "expires_at": "2026-05-29T12:30:00Z" },
      "sig": "hex(ed25519(bob_sk, canonical(block_without_sig)))" }
  ]
}
```

Rules:

- **Attenuation only.** Each block is signed by the previous block's `holder`
  key, and may only *narrow* authority. Verification computes the *intersection*
  of all blocks' caveats; a later block can never broaden a set, extend an
  expiry, or raise a limit. (Caveat semantics are defined per key: set caveats
  intersect; scalar limits take the min; expiry takes the earliest.)
- **Offline-verifiable.** A verifier needs only the token and the `root_issuer`
  public key. It checks every block's signature against its issuer, that the
  signing chain is contiguous (`block[i].issuer == block[i-1].holder`), that the
  root issuer actually holds the authority being claimed (verified against the
  conversation/org policy — see L6), and that the *intersected* caveats permit
  the requested action **now**.
- **Bound to use.** A token authorizes an *action verb* in raft's vocabulary
  (`task.dispatch`, `task.result`, `conversation.post`, `tool.run:<name>`,
  `org.grant`, …). The verb set is closed and documented.
- **Revocation.** Short expiries are the primary control (no global CRL needed
  for autonomy). For long-lived grants, an org publishes a signed revocation
  list in its (replicated) record; verifiers consult it. Revocation is
  best-effort and explicitly so — we document the window.

**CLI.** `raft grant new --to <holder> --action ... --caveat k=v --ttl 1h`,
`raft grant attenuate <token> --to <holder> --caveat ...`,
`raft grant verify <token> --action <verb> --context k=v`,
`raft grant inspect <token>`.

**GAP closed.** Scoped, attenuable, offline-verifiable delegation-of-authority
chains — the field's largest gap.

## 5. L3 — Federation: node daemon & log replication

**Problem it closes.** Every surveyed framework is hub-and-spoke (a server, or a
single Python process). True P2P autonomy needs hosts that peer directly,
replicate state, and survive partitions.

**Design.**

- **Node identity.** A host runs `raft node serve` with its own node Ed25519 key
  (distinct from agent keys; a node hosts many agents). Peers are listed in
  `peers/<node_id>.json` = `{ node_id, pubkey, address, agents_hint }`.
- **Transport: signed envelopes over HTTP(S).** Every wire message is an
  **envelope** `{ from_node, to_node, nonce, issued_at, payload, sig }`, signed
  by the sending node key. HTTPS provides confidentiality; the envelope provides
  authenticity and replay protection (nonce + `issued_at` window). Because
  authenticity is in the envelope and records are individually signed,
  **relays are safe** — the transport can be swapped (QUIC, a store-and-forward
  relay for NAT'd nodes) without weakening guarantees.
- **Replication: home node + pull.** Each conversation has a **home node**
  (where it was created, recorded in signed `meta`). The home node is
  authoritative for mutable `meta`. Message/receipt logs replicate by **pull**:
  a participant's node periodically (or on a push *hint*) asks peers "give me
  records in conversation X with `author_seq` > my high-water mark per author."
  Pulled records are verified (L1) before they touch the local bus. This is
  eventually consistent, partition-tolerant, and needs no quorum.
- **Mapping to the FS.** Federated conversations live under the same
  `conversations/<id>/` tree; remote-authored records are written with the same
  atomic-write/visibility rules. A `federation.json` per conversation records
  the home node, participant→node routing, and per-author high-water marks.

**CLI.** `raft node serve`, `raft node status`, `raft peer add/list/remove`,
`raft conversation federate <id> --with <node>`.

**GAP closed.** Decentralized, partition-tolerant replication of the obligation
log — no central agent server.

## 6. L4 — Remote task delegation (the payoff layer)

**Problem it closes.** Letta has *no* native agent-to-agent delegation/obligation
model; multi-agent is "one agent calls another's API and hopes." The Hermes
format standardizes *what a tool call looks like* but says nothing about *who may
invoke it remotely, how its status is tracked, how results stream back, or how
it's cancelled.*

**Design — delegation is an ask carrying a Hermes tool-call.**

- A **task** is a `Message` with `kind: "task"` whose `body` is a Hermes-format
  structured tool call:

  ```json
  { "tool_call": { "name": "deploy",
                   "arguments": { "service": "api", "env": "staging" } },
    "capability": "<capability-token>",
    "limits": { "max_runtime_s": 60, "max_output_bytes": 1048576 },
    "result_schema": { /* optional JSON schema the result must satisfy */ } }
  ```

  It is addressed with `needs_response_from: [assignee]` — so **the entire
  existing obligation engine applies for free**: it shows in `awaiting`, blocks
  `wait --owed/--resolved`, and is auditable.
- **Authority check.** The assignee's node verifies the embedded `capability`
  authorizes `tool.run:deploy` with the requested args under the caveats, *before*
  accepting the task. Unauthorized → terminal `rejected` receipt with reason
  `unauthorized` (closes the ask honestly rather than hanging).
- **Status = receipt lifecycle.** `working` (accepted, running, with progress
  notes), then terminal `done`/`rejected`. The iteration-38 downgrade guard
  already prevents a late `working` from reopening a finished task — that
  invariant is *load-bearing* here.
- **Result.** Returned as a `reply` (kind `message`) whose body is the structured
  result `{ "result": ..., "artifacts": [<content-address>...] }`, with
  `--ack done`. The result is signed (L1), so the delegator can prove what the
  worker returned.
- **Streaming.** Long tasks emit incremental `working` receipts carrying progress
  (`history` already stores them) and may append log lines to a result stream
  (L5). `raft task status <id> --follow` tails them; `wait --resolved` blocks for
  the terminal state.
- **Cancel = withdraw.** `raft task cancel` is `withdraw` under the hood — it
  releases the obligation and notifies the worker to stop (the iteration-36
  fix ensures an already-finished worker isn't told to stop).

**CLI.** `raft task dispatch --to <agent> --tool <name> --args <json> --cap <token>`,
`raft task status <id> [--follow]`, `raft task result <id>`,
`raft task cancel <id>`.

**GAP closed.** A complete, audited, capability-gated, cancellable remote
delegation lifecycle with status tracking and verifiable results — built from
primitives raft already proved out locally.

## 7. L5 — Sandboxed execution

**Problem it closes.** Letta executes tools in-process or in an e2b/Docker
sandbox it manages centrally; there's no portable, self-hostable, capability-
gated worker. Hermes describes the call, never the runtime.

**Design.**

- `raft run --agent <worker>` is an **executor loop**: it `watch`es for `task`
  asks addressed to the worker, verifies the capability (L2), and for each
  authorized task runs the named tool in a **sandbox**.
- **Default sandbox: subprocess with limits.** A registered tool maps to an
  executable; it runs as a child process with: a wall-clock timeout
  (`max_runtime_s`), output cap (`max_output_bytes`), a scratch working dir under
  `run/sandbox/<task-id>/`, scrubbed env, and OS resource limits (`rlimit` on
  Unix: CPU, address space, file size, nofile). Network off by default unless the
  capability grants `net`.
- **Pluggable isolation.** The sandbox is an interface; stronger backends
  (containers, Firecracker microVMs, `bwrap`) plug in behind the same contract.
  v1 ships the subprocess backend and documents the interface.
- **Artifacts.** Outputs are content-addressed (`sha256`) and stored under
  `artifacts/<hash>`; the result reply references hashes, so artifacts are
  dedup'd, verifiable, and replicate like any blob.
- **Log stream.** stdout/stderr stream to `conversations/<id>/streams/<task>.log`
  and surface via `task status --follow`; truncation respects the output cap and
  says so (no silent truncation — consistent with raft's existing discipline).

**GAP closed.** A portable, capability-gated, self-hostable sandboxed worker with
verifiable artifacts and streamed logs.

## 8. L6 — Organization & accountability

**Problem it closes.** CrewAI/AutoGen/LangGraph encode roles/hierarchies in code
at authoring time; there's no *runtime*, *verifiable*, *dynamic* org structure,
and no accountability trail that survives across hosts.

**Design.**

- An **org** is a signed record: `{ id, root_key, roles, members, policy }`,
  replicated like a conversation. A **role** is a named capability bundle (the
  caveats an org will grant a holder of that role). **Membership** binds an agent
  passport to roles, counter-signed by an org admin key.
- **Delegation tree.** Authority flows from the org root key down via capability
  attenuation (L2): a supervisor role can `org.grant` narrower capabilities to
  worker roles; the chain is the org chart, and it is *verifiable*.
- **Dynamic assignment.** Assigning/revoking a role = issuing/revoking a
  capability (signed record + revocation list). No redeploy.
- **Accountability.** Because every message, receipt, task, and grant is signed
  and hash-chained (L1), "who authorized what, who did what, and what was
  returned" is a verifiable replay of the logs — not a best-effort trace. Failure
  attribution points at a signed record, not a log line that could be fabricated.

**CLI.** `raft org new`, `raft org role`, `raft org add-member`, `raft org grant`,
`raft org audit <task-or-agent>`.

**GAP closed.** Runtime, verifiable, dynamic organizational structure with
cryptographic accountability and failure attribution.

## 9. Prior art & gaps (reconciled with deep-research)

This section is seeded from first-principles analysis and **reconciled with the
deep-research gap report** (`wf_19fa669a-946`) as it lands; each row ends in the
"GAP" the layer above closes.

| Dimension | Letta | Hermes format | A2A / MCP | raft mesh closes it at |
|---|---|---|---|---|
| Agent identity | server-assigned id | n/a | A2A "agent card" (no required crypto) | **L0** Ed25519 passports |
| History integrity | server DB | unsigned | transport TLS only | **L1** signed hash-chained logs |
| Delegated authority | API key (coarse) | n/a | OAuth-ish, no attenuation | **L2** attenuable capabilities |
| Decentralization | central server | n/a | client-server | **L3** home-node + pull federation |
| A2A delegation lifecycle | none | n/a | task states, no obligation/withdraw | **L4** ask-carrying tool-call |
| Sandbox | central (e2b/docker) | n/a | unspecified | **L5** portable sandbox + artifacts |
| Org structure | none | n/a | none | **L6** signed roles + delegation tree |
| Accountability | server logs | n/a | partial trace | **L1+L6** verifiable replay |

## 10. Build order & milestones

1. **M0** L0 identity + L1 signing (single host, back-compatible, opt-in). *Value
   now: verifiable authorship locally.*
2. **M1** L2 capability tokens (offline verify + attenuation).
3. **M2** L3 federation between two nodes (pull replication, ingest verification).
4. **M3** L4 remote task delegation end-to-end (dispatch→work→result→audit).
5. **M4** L5 sandbox executor + artifacts.
6. **M5** L6 org/roles + `audit`, normative `docs/mesh-protocol.md`.

Each milestone: `cargo build --release` + `cargo test --release` green, README +
CHANGELOG updated, committed. New crypto deps kept minimal: `ed25519-dalek`,
`sha2`, `rand` (CSPRNG for keygen/nonces). All wire formats documented for
re-implementation. Back-compat: every new field is additive and optional;
existing local-only buses keep working unsigned until an operator opts in.

## 11. Explicit non-goals (v1)

- No global consensus / total order (local-first + per-author chains instead).
- No PKI/CA hierarchy (sovereign keys + optional org counter-signing).
- No built-in NAT traversal beyond a documented relay-envelope path (relays are
  safe by construction; a concrete relay is future work).
- No GC/retention changes beyond making them integrity-aware.
- No attempt to sandbox-escape-proof against a determined local root; the v1
  subprocess sandbox raises the bar and documents its threat model honestly.
