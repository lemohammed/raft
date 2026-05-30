# raft mesh M4 artifacts and logs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the useful M4 slice by making `raft run` persist verifiable task outputs as artifacts and task logs, then surface them from `raft task status`.

**Architecture:** Keep the existing executor flow in `src/main.rs` and focused data shapes in `src/task.rs`. Store stdout/stderr under content-addressed `artifacts/sha256-<hex>` files and write a per-task stream file at `conversations/<id>/streams/<task-id>.log`, all inside the bus root. Extend `TaskResult` additively with `artifacts[]` and `log`, preserving existing result JSON fields.

**Tech Stack:** Rust 2024, serde JSON, existing SHA-256 helpers in `src/crypto.rs`, CLI integration tests in `tests/cli.rs`.

---

### Task 1: Prove executor outputs are persisted

**Files:**
- Modify: `tests/cli.rs`
- Later modify: `src/task.rs`, `src/main.rs`

- [ ] **Step 1: Write the failing integration test**

Add a test near the existing executor tests:

```rust
#[test]
fn executor_persists_artifacts_and_task_log() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "worker", "--workspace", "."]);
    run(&bus, &["id", "new", "alice", "--json"]);
    run(&bus, &["id", "new", "worker", "--json"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "alice,worker",
            "--starter",
            "alice",
        ],
    );

    let cap = bus.join("cap.json");
    run(
        &bus,
        &[
            "grant",
            "new",
            "--issuer",
            "alice",
            "--to",
            "worker",
            "--action",
            "tool.run",
            "--tool",
            "echo",
            "--ttl",
            "1h",
            "--out",
            cap.to_str().unwrap(),
            "--json",
        ],
    );
    let dispatch = run(
        &bus,
        &[
            "task",
            "dispatch",
            "--from",
            "alice",
            "--to",
            "worker",
            "--conversation",
            "c",
            "--tool",
            "echo",
            "--args",
            "{\"artifact\":\"hello\"}",
            "--cap",
            cap.to_str().unwrap(),
            "--json",
        ],
    );
    let task_id = serde_json::from_slice::<serde_json::Value>(&dispatch.stdout).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();

    run(
        &bus,
        &[
            "run",
            "worker",
            "--once",
            "--tool",
            "echo=/bin/cat",
            "--trust",
            "alice",
            "--json",
        ],
    );

    let status = run(&bus, &["task", "status", &task_id, "--json"]);
    let status_json: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    let result = &status_json["result"];
    assert_eq!(result["ok"], true);
    assert!(result["log"].as_str().unwrap().ends_with(&format!("streams/{task_id}.log")));
    let artifacts = result["artifacts"].as_array().unwrap();
    assert!(artifacts.iter().any(|artifact| artifact["name"] == "stdout"));

    let stdout_artifact = artifacts
        .iter()
        .find(|artifact| artifact["name"] == "stdout")
        .unwrap();
    let artifact_path = bus.join(stdout_artifact["path"].as_str().unwrap());
    assert_eq!(
        fs::read_to_string(&artifact_path).unwrap(),
        "{\"artifact\":\"hello\"}"
    );

    let log_path = bus.join(result["log"].as_str().unwrap());
    let log = fs::read_to_string(log_path).unwrap();
    assert!(log.contains("[stdout] {\"artifact\":\"hello\"}"));
    assert!(log.contains("[exit] 0"));
}
```

- [ ] **Step 2: Run the failing test**

Run:

```sh
cargo test --test cli executor_persists_artifacts_and_task_log
```

Expected: FAIL because `TaskResult` has no `artifacts` or `log` fields yet.

### Task 2: Add task artifact and log data shapes

**Files:**
- Modify: `src/task.rs`

- [ ] **Step 1: Extend result structs**

Add:

```rust
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TaskArtifact {
    pub(crate) name: String,
    pub(crate) hash: String,
    pub(crate) path: String,
    pub(crate) bytes: u64,
}
```

Then add to `TaskResult`:

```rust
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub(crate) artifacts: Vec<TaskArtifact>,
#[serde(default, skip_serializing_if = "Option::is_none")]
pub(crate) log: Option<String>,
```

- [ ] **Step 2: Run the focused unit tests**

Run:

```sh
cargo test task::tests
```

Expected: PASS after updating any `TaskResult` construction sites.

### Task 3: Persist outputs under the bus root

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Implement artifact storage helper**

Add a helper near the task executor functions:

```rust
fn persist_task_artifact(root: &Path, name: &str, bytes: &[u8]) -> Result<Option<task::TaskArtifact>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let hash = crypto::sha256_hex(bytes);
    let relative = format!("artifacts/sha256-{hash}");
    let path = root.join(&relative);
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, bytes)?;
    }
    Ok(Some(task::TaskArtifact {
        name: name.to_string(),
        hash: format!("sha256:{hash}"),
        path: relative,
        bytes: bytes.len() as u64,
    }))
}
```

- [ ] **Step 2: Implement task log helper**

Add:

```rust
fn persist_task_log(
    root: &Path,
    task_message: &Message,
    outcome: &task::ToolOutcome,
) -> Result<String> {
    let relative = format!(
        "conversations/{}/streams/{}.log",
        task_message.conversation_id, task_message.id
    );
    let path = root.join(&relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut log = String::new();
    if !outcome.stdout.is_empty() {
        log.push_str("[stdout] ");
        log.push_str(&outcome.stdout);
        if !outcome.stdout.ends_with('\n') {
            log.push('\n');
        }
    }
    if !outcome.stderr.is_empty() {
        log.push_str("[stderr] ");
        log.push_str(&outcome.stderr);
        if !outcome.stderr.ends_with('\n') {
            log.push('\n');
        }
    }
    if outcome.timed_out {
        log.push_str("[timeout]\n");
    }
    log.push_str(&format!(
        "[exit] {}\n",
        outcome
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "killed".to_string())
    ));
    fs::write(&path, log)?;
    Ok(relative)
}
```

- [ ] **Step 3: Thread artifacts and log into task results**

Change `record_task_outcome` to accept `artifacts: Vec<task::TaskArtifact>` and `log: Option<String>`, then populate the new `TaskResult` fields. In the successful tool-run path, persist stdout/stderr artifacts and the log before calling `record_task_outcome`; in authorization/launch failure paths, pass empty artifacts and no log.

- [ ] **Step 4: Run the focused integration test**

Run:

```sh
cargo test --test cli executor_persists_artifacts_and_task_log
```

Expected: PASS.

### Task 4: Document M4 behavior

**Files:**
- Modify: `README.md`
- Modify: `CHANGELOG.md`
- Modify: `docs/protocol.md`

- [ ] **Step 1: Update README remote task docs**

Explain that `raft run` writes content-addressed stdout/stderr artifacts under `artifacts/` and a stream log under `conversations/<id>/streams/<task-id>.log`; `task status --json` exposes both.

- [ ] **Step 2: Update protocol layout**

Add `artifacts/` and `conversations/<id>/streams/` to the filesystem layout section.

- [ ] **Step 3: Update changelog**

Add an Unreleased M4 bullet for sandbox artifacts and task logs.

- [ ] **Step 4: Run full verification**

Run:

```sh
cargo fmt --check
cargo test
```

Expected: both PASS.
