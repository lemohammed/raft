//! Remote task delegation (L4) and the sandboxed executor (L5).
//!
//! A **task** is an obligation-bearing message (`kind: "task"`) whose body is a
//! Hermes-format tool call plus the capability that authorizes it. Because a task
//! is an ask, the entire obligation engine applies for free: it shows in
//! `awaiting`, blocks `wait --owed/--resolved`, and its receipt lifecycle
//! (`working` → `done`/`rejected`) *is* the task status. The result is returned
//! as a signed reply; cancelling is `withdraw`.
//!
//! The body shape adopts the Nous Hermes function-calling format verbatim
//! (`{"name", "arguments"}`) — the format standardizes only the *call*, so raft
//! supplies the remote layer around it: addressing (the message envelope),
//! authority (the embedded capability), status (the receipt), and result
//! transport (the reply).

use crate::capability::Token;
use crate::error::{RaftError, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Hermes function-calling shape: `{name, arguments}`.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ToolCall {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: serde_json::Value,
}

/// Resource limits the executor enforces for a task.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct TaskLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_runtime_s: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) max_output_bytes: Option<u64>,
}

/// The body of a `kind:"task"` message.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TaskBody {
    pub(crate) tool_call: ToolCall,
    /// The capability authorizing the assignee to run this tool. Optional only so
    /// a fully-trusted local bus can dispatch without one; the executor refuses
    /// to run a capability-bearing task it cannot authorize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) capability: Option<Token>,
    #[serde(default)]
    pub(crate) limits: TaskLimits,
}

/// The body of a task result reply.
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct TaskResult {
    pub(crate) ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub(crate) output_truncated: bool,
}

impl TaskBody {
    pub(crate) fn to_body_string(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    pub(crate) fn parse(body: &str) -> Result<Self> {
        serde_json::from_str(body)
            .map_err(|err| RaftError::coded("parse", format!("invalid task body: {err}")))
    }
}

impl TaskResult {
    pub(crate) fn to_body_string(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

/// The outcome of running a tool in the sandbox.
pub(crate) struct ToolOutcome {
    pub(crate) exit_code: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
    pub(crate) truncated: bool,
}

/// Run `executable` as a child process with the task's `arguments` written to its
/// stdin as JSON, capturing stdout/stderr.
///
/// v1 sandbox boundary (honest threat model): the child runs with a **scrubbed
/// environment** (only `PATH`), a dedicated **scratch working directory**, a
/// **wall-clock timeout** (killed on expiry), and an **output cap** (drained but
/// truncated past the cap, never deadlocking the child). It does **not** yet
/// enforce OS resource limits (CPU/memory via `rlimit`) or network isolation;
/// those are the documented next hardening step and a stronger backend
/// (container/microVM) plugs in behind this same function's contract. Do not run
/// untrusted code under the v1 backend on a host you are unwilling to expose to
/// arbitrary same-user actions.
pub(crate) fn run_tool(
    executable: &Path,
    arguments: &serde_json::Value,
    scratch: &Path,
    timeout: Duration,
    max_output_bytes: usize,
) -> Result<ToolOutcome> {
    std::fs::create_dir_all(scratch)?;
    let mut child = Command::new(executable)
        .current_dir(scratch)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("RAFT_SANDBOX", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| RaftError::coded("io", format!("failed to launch tool: {err}")))?;

    // Feed arguments as JSON on stdin, then close it so the tool sees EOF.
    if let Some(mut stdin) = child.stdin.take() {
        let payload = serde_json::to_vec(arguments)?;
        let _ = stdin.write_all(&payload);
        // Dropping stdin closes the pipe.
    }

    // Drain stdout/stderr on threads so a chatty tool can never block on a full
    // pipe while we are enforcing the timeout. Each reader caps what it retains.
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let (out_rx, out_handle) = spawn_capped_reader(stdout, max_output_bytes);
    let (err_rx, err_handle) = spawn_capped_reader(stderr, max_output_bytes);

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let exit_code = loop {
        match child.try_wait()? {
            Some(status) => break status.code(),
            None => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break None;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    };

    let (stdout, out_truncated) = out_handle.join().unwrap_or_default();
    let (stderr, err_truncated) = err_handle.join().unwrap_or_default();
    drop(out_rx);
    drop(err_rx);

    Ok(ToolOutcome {
        exit_code,
        timed_out,
        stdout,
        stderr,
        truncated: out_truncated || err_truncated,
    })
}

/// Spawn a thread that reads a child stream into a UTF-8-lossy string, retaining
/// at most `cap` bytes but continuing to drain (so the child never blocks).
/// Returns the join handle yielding `(text, truncated)`.
fn spawn_capped_reader(
    stream: Option<impl Read + Send + 'static>,
    cap: usize,
) -> (mpsc::Receiver<()>, std::thread::JoinHandle<(String, bool)>) {
    let (tx, rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let _tx = tx;
        let mut kept: Vec<u8> = Vec::new();
        let mut truncated = false;
        if let Some(mut stream) = stream {
            let mut buf = [0u8; 8192];
            loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if kept.len() < cap {
                            let room = cap - kept.len();
                            kept.extend_from_slice(&buf[..n.min(room)]);
                            if n > room {
                                truncated = true;
                            }
                        } else {
                            truncated = true;
                        }
                    }
                }
            }
        }
        (String::from_utf8_lossy(&kept).into_owned(), truncated)
    });
    (rx, handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_body_roundtrips_in_hermes_shape() {
        let body = TaskBody {
            tool_call: ToolCall {
                name: "deploy".into(),
                arguments: serde_json::json!({ "service": "api", "env": "staging" }),
            },
            capability: None,
            limits: TaskLimits {
                max_runtime_s: Some(60),
                max_output_bytes: Some(1024),
            },
        };
        let string = body.to_body_string().unwrap();
        // The tool call uses the Hermes {name, arguments} shape verbatim.
        let value: serde_json::Value = serde_json::from_str(&string).unwrap();
        assert_eq!(value["tool_call"]["name"], "deploy");
        assert_eq!(value["tool_call"]["arguments"]["service"], "api");
        let parsed = TaskBody::parse(&string).unwrap();
        assert_eq!(parsed.tool_call.name, "deploy");
        assert_eq!(parsed.limits.max_runtime_s, Some(60));
    }

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        // Project-local scratch only (never the system temp dir).
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tmp")
            .join(format!("raft-{tag}-{}", std::process::id()))
    }

    #[test]
    fn run_tool_captures_stdout_and_exit_code() {
        let dir = scratch_dir("tasktest");
        // `cat` echoes the JSON arguments we write to stdin straight back out.
        let outcome = run_tool(
            Path::new("/bin/cat"),
            &serde_json::json!({ "hello": "mesh" }),
            &dir,
            Duration::from_secs(5),
            1_000_000,
        )
        .unwrap();
        assert_eq!(outcome.exit_code, Some(0));
        assert!(!outcome.timed_out);
        let echoed: serde_json::Value = serde_json::from_str(outcome.stdout.trim()).unwrap();
        assert_eq!(echoed["hello"], "mesh");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_tool_enforces_a_timeout() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch_dir("tasktimeout");
        std::fs::create_dir_all(&dir).unwrap();
        // A tool that ignores stdin and runs longer than its budget. (The
        // sandbox feeds arguments on stdin, so a long-runner must not depend on
        // an argv operand the way `/bin/sleep` does.)
        let script = dir.join("sleeper.sh");
        std::fs::write(&script, b"#!/bin/sh\nsleep 5\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let outcome = run_tool(
            &script,
            &serde_json::json!({}),
            &dir,
            Duration::from_secs(1),
            1_000,
        )
        .unwrap();
        assert!(
            outcome.timed_out,
            "a tool exceeding its budget must be killed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
