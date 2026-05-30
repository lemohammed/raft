use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_raft"))
}

fn temp_bus() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "raft-test-{}-{nanos}-{counter}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path.join("bus")
}

fn run(root: &PathBuf, args: &[&str]) -> std::process::Output {
    let output = Command::new(bin())
        .arg("--root")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    if !output.status.success() {
        panic!(
            "command failed: {:?}\nstdout={}\nstderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

fn run_fail(root: &PathBuf, args: &[&str]) -> std::process::Output {
    let output = Command::new(bin())
        .arg("--root")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "command unexpectedly succeeded: {args:?}"
    );
    output
}

#[test]
fn send_json_returns_resolved_envelope() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "codex", "--workspace", "."]);
    run(&bus, &["claim", "homekeep-dev", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "sync",
            "--participants",
            "codex,homekeep-dev",
            "--starter",
            "codex",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "sync",
            "--from",
            "codex",
            "--to",
            "homekeep-dev",
            "--body",
            "ping",
            "--needs-response-from",
            "homekeep-dev",
            "--json",
        ],
    );
    let envelope: serde_json::Value = serde_json::from_slice(&sent.stdout).unwrap();
    assert_eq!(envelope["ok"], serde_json::json!(true));
    assert!(
        envelope["message_id"]
            .as_str()
            .unwrap()
            .starts_with("m-"),
        "message_id should be present and prefixed"
    );
    assert_eq!(envelope["conversation_id"], serde_json::json!("sync"));
    let to: Vec<String> = envelope["to"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect();
    assert!(to.contains(&"homekeep-dev".to_string()));
    assert_eq!(
        envelope["needs_response_from"],
        serde_json::json!(["homekeep-dev"])
    );
}

#[test]
fn mutating_commands_emit_ok_envelopes() {
    let bus = temp_bus();

    let init: serde_json::Value =
        serde_json::from_slice(&run(&bus, &["init", "--json"]).stdout).unwrap();
    assert_eq!(init["ok"], serde_json::json!(true));

    let claim: serde_json::Value =
        serde_json::from_slice(&run(&bus, &["claim", "codex", "--json"]).stdout).unwrap();
    assert_eq!(claim["ok"], serde_json::json!(true));
    assert_eq!(claim["agent"], serde_json::json!("codex"));
    assert_eq!(claim["mention"], serde_json::json!("@codex"));

    run(&bus, &["claim", "homekeep-dev"]);

    // conversation open --json must return the resolved conversation id.
    let opened: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &[
                "conversation",
                "open",
                "--from",
                "codex",
                "--to",
                "homekeep-dev",
                "--json",
            ],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(opened["ok"], serde_json::json!(true));
    assert_eq!(opened["created"], serde_json::json!(true));
    let conversation_id = opened["conversation_id"].as_str().unwrap().to_string();
    assert!(!conversation_id.is_empty());

    // `conversation create` with an explicit id is the idempotent path:
    // creating the same id again with --if-missing reports created=false.
    let created: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &[
                "conversation",
                "create",
                "proj",
                "--participants",
                "codex,homekeep-dev",
                "--json",
            ],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(created["created"], serde_json::json!(true));
    assert_eq!(created["conversation_id"], serde_json::json!("proj"));

    let recreated: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &[
                "conversation",
                "create",
                "proj",
                "--participants",
                "codex,homekeep-dev",
                "--if-missing",
                "--json",
            ],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(recreated["created"], serde_json::json!(false));
    assert_eq!(recreated["conversation_id"], serde_json::json!("proj"));

    let state: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &["state", "set", "codex", "working", "--note", "busy", "--json"],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(state["changed"], serde_json::json!(true));
    assert_eq!(state["state"], serde_json::json!("working"));

    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            &conversation_id,
            "--from",
            "codex",
            "--to",
            "homekeep-dev",
            "--body",
            "hi",
        ],
    );
    let message_id = String::from_utf8(sent.stdout).unwrap().trim().to_string();

    let ack: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &["ack", "homekeep-dev", &message_id, "--status", "done", "--json"],
        )
        .stdout,
    )
    .unwrap();
    assert_eq!(ack["ok"], serde_json::json!(true));
    assert_eq!(ack["status"], serde_json::json!("done"));
    assert_eq!(ack["message_id"], serde_json::json!(message_id));
}

#[test]
fn me_summarizes_unread_asks_peers_and_conversations() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice"]);
    run(&bus, &["claim", "bob"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "sync",
            "--participants",
            "alice,bob",
            "--starter",
            "alice",
        ],
    );
    // bob asks alice for a response → alice owes bob.
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "sync",
            "--from",
            "bob",
            "--to",
            "alice",
            "--subject",
            "q",
            "--body",
            "need input",
            "--needs-response-from",
            "alice",
        ],
    );
    // alice asks bob for a response → bob owes alice.
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "sync",
            "--from",
            "alice",
            "--to",
            "bob",
            "--subject",
            "ask",
            "--body",
            "you handle it?",
            "--needs-response-from",
            "bob",
        ],
    );

    let summary: serde_json::Value =
        serde_json::from_slice(&run(&bus, &["me", "alice", "--json"]).stdout).unwrap();

    assert_eq!(summary["agent"], serde_json::json!("alice"));
    // alice has one unread (bob's message); her own message is not unread to her.
    assert_eq!(summary["unread"], serde_json::json!(1));
    assert_eq!(summary["you_owe"].as_array().unwrap().len(), 1);
    assert_eq!(summary["you_owe"][0]["from"], serde_json::json!("bob"));
    assert_eq!(summary["owed_to_you"].as_array().unwrap().len(), 1);
    assert_eq!(
        summary["owed_to_you"][0]["awaited"],
        serde_json::json!("bob")
    );
    // bob is a live peer.
    let peers: Vec<String> = summary["live_peers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|peer| peer["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(peers, vec!["bob".to_string()]);
    // the sync conversation is listed with one unread.
    let sync = summary["conversations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|conv| conv["id"] == serde_json::json!("sync"))
        .expect("sync conversation present");
    assert_eq!(sync["unread"], serde_json::json!(1));
}

#[test]
fn me_rejects_unclaimed_agent() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    let output = run_fail(&bus, &["me", "ghost", "--json"]);
    let envelope: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(envelope["ok"], serde_json::json!(false));
    assert_eq!(envelope["error"]["code"], serde_json::json!("not_claimed"));
}

#[test]
fn error_codes_are_stable_for_common_failures() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);

    // A missing message must surface not_found, not the generic error code —
    // ack/read/thread/receipts all route through find_message.
    let missing = run_fail(&bus, &["ack", "bob", "m-nope", "--status", "done", "--json"]);
    let envelope: serde_json::Value = serde_json::from_slice(&missing.stderr).unwrap();
    assert_eq!(envelope["error"]["code"], serde_json::json!("not_found"));

    // Re-creating an existing channel/conversation without --if-missing is a conflict.
    run(&bus, &["channel", "create", "team", "--creator", "alice", "--json"]);
    let dup_channel = run_fail(&bus, &["channel", "create", "team", "--creator", "alice", "--json"]);
    let dup_channel_json: serde_json::Value =
        serde_json::from_slice(&dup_channel.stderr).unwrap();
    assert_eq!(dup_channel_json["error"]["code"], serde_json::json!("conflict"));

    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob", "--starter", "alice",
        ],
    );
    let dup_conv = run_fail(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob", "--starter", "alice",
            "--json",
        ],
    );
    let dup_conv_json: serde_json::Value = serde_json::from_slice(&dup_conv.stderr).unwrap();
    assert_eq!(dup_conv_json["error"]["code"], serde_json::json!("conflict"));

    // Sending from an id outside the conversation's participants is not_participant.
    run(
        &bus,
        &[
            "conversation", "create", "room", "--participants", "alice,bob", "--starter", "alice",
        ],
    );
    let outsider = run_fail(
        &bus,
        &["send", "--conversation", "room", "--from", "carol", "--to", "bob", "--subject", "x", "--body", "y", "--json"],
    );
    let outsider_json: serde_json::Value = serde_json::from_slice(&outsider.stderr).unwrap();
    assert_eq!(outsider_json["error"]["code"], serde_json::json!("not_participant"));

    // A participant who cannot see a message (it was not addressed to them) gets
    // not_participant from `thread`, matching read/ack/show visibility checks.
    run(&bus, &["claim", "dave", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "trio", "--participants", "alice,bob,dave", "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &["send", "--conversation", "trio", "--from", "alice", "--to", "bob", "--subject", "s", "--body", "hidden from dave"],
    );
    let mid = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let hidden = run_fail(&bus, &["thread", &mid, "--agent", "dave", "--json"]);
    let hidden_json: serde_json::Value = serde_json::from_slice(&hidden.stderr).unwrap();
    assert_eq!(hidden_json["error"]["code"], serde_json::json!("not_participant"));
}

#[test]
fn private_message_ack_flow() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "codex", "--workspace", "."]);
    run(&bus, &["claim", "homekeep-dev", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "homekeep-sync",
            "--participants",
            "codex,homekeep-dev",
            "--starter",
            "codex",
            "--private",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "homekeep-sync",
            "--from",
            "codex",
            "--to",
            "homekeep-dev",
            "--subject",
            "status",
            "--body",
            "please report status",
            "--requires-ack",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let inbox = run(&bus, &["inbox", "homekeep-dev", "--unread"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains(&message_id));
    let read = run(&bus, &["read", "homekeep-dev", &message_id, "--json"]);
    assert!(String::from_utf8_lossy(&read.stdout).contains("please report status"));
    let ack = run(
        &bus,
        &[
            "ack",
            "homekeep-dev",
            &message_id,
            "--status",
            "done",
            "--note",
            "ok",
        ],
    );
    assert!(String::from_utf8_lossy(&ack.stdout).contains("done"));
    let unread = run(&bus, &["inbox", "homekeep-dev", "--unread"]);
    assert!(String::from_utf8_lossy(&unread.stdout).contains("no messages"));
}

#[test]
fn status_message_count_ignores_orphan_tmp_files() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "hi",
        ],
    );

    let count = |out: &std::process::Output| -> u64 {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["id"] == "c")
            .unwrap()["messages"]
            .as_u64()
            .unwrap()
    };

    let before = count(&run(&bus, &["status", "--json"]));

    // An interrupted atomic write leaves a `.tmp` sibling in messages/. It must
    // not inflate the count, which must agree with every other count path.
    fs::write(bus.join("conversations/c/messages/.orphan.1.abc.tmp"), b"{}").unwrap();

    let after = count(&run(&bus, &["status", "--json"]));
    assert_eq!(
        before, after,
        "status must count only .json message files, not orphan .tmp siblings"
    );
}

#[test]
fn heartbeat_watch_keeps_agent_active_and_records_shutdown() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &["claim", "agent-a", "--workspace", ".", "--ttl", "2"],
    );
    let mut child = Command::new(bin())
        .arg("--root")
        .arg(&bus)
        .args([
            "heartbeat",
            "agent-a",
            "--watch",
            "--ttl",
            "2",
            "--interval",
            "0.5",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    thread::sleep(Duration::from_millis(2600));
    let status = run(&bus, &["status", "--agent", "agent-a", "--json"]);
    let status_json: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status_json["agents"][0]["id"], "agent-a");
    assert_eq!(status_json["agents"][0]["active"], true);

    Command::new("kill")
        .arg("-TERM")
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(child.wait().unwrap().success());
    let state_path = bus.join("heartbeat/agent-a.json");
    let state: serde_json::Value = serde_json::from_slice(&fs::read(&state_path).unwrap()).unwrap();
    assert_eq!(state["agent"], "agent-a");
    assert!(state["shutdown_at"].as_str().is_some());

    let mut restarted = Command::new(bin())
        .arg("--root")
        .arg(&bus)
        .args([
            "heartbeat",
            "agent-a",
            "--watch",
            "--ttl",
            "2",
            "--interval",
            "0.5",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    // Wait until the restarted watcher has published its own pid and cleared the
    // previous shutdown marker, which means it is past startup and has installed
    // its SIGTERM handler. Killing before that races the default disposition and
    // terminates it non-zero under load.
    let restarted_pid = restarted.id();
    let mut ready = false;
    for _ in 0..200 {
        if let Ok(bytes) = fs::read(&state_path)
            && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
            && value["pid"].as_u64() == Some(restarted_pid as u64)
            && value["shutdown_at"].is_null()
        {
            ready = true;
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(ready, "restarted heartbeat watcher did not come up in time");
    Command::new("kill")
        .arg("-TERM")
        .arg(restarted_pid.to_string())
        .status()
        .unwrap();
    assert!(restarted.wait().unwrap().success());
}

#[test]
fn state_set_get_and_watch_state_changes_work() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alpha-agent", "--workspace", "."]);
    run(&bus, &["claim", "beta-agent", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "alpha-agent,beta-agent",
            "--starter",
            "alpha-agent",
        ],
    );

    let default_state = run(&bus, &["state", "get", "alpha-agent", "--json"]);
    let default_json: serde_json::Value = serde_json::from_slice(&default_state.stdout).unwrap();
    assert_eq!(default_json["state"], "idle");

    run(
        &bus,
        &[
            "state",
            "set",
            "alpha-agent",
            "working",
            "--note",
            "reviewing PR",
        ],
    );
    let agent: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("agents/alpha-agent.json")).unwrap()).unwrap();
    assert_eq!(agent["current_state"], "working");
    assert_eq!(agent["state_note"], "reviewing PR");

    let first_watch = run(
        &bus,
        &[
            "watch",
            "--agent",
            "beta-agent",
            "--conversation",
            "c",
            "--state-changes",
            "--once",
        ],
    );
    assert!(String::from_utf8_lossy(&first_watch.stdout).contains("@alpha-agent is now working"));

    run(&bus, &["state", "set", "alpha-agent", "idle"]);
    let second_watch = run(
        &bus,
        &[
            "watch",
            "--agent",
            "beta-agent",
            "--conversation",
            "c",
            "--state-changes",
            "--once",
        ],
    );
    assert!(String::from_utf8_lossy(&second_watch.stdout).contains("@alpha-agent is now idle"));
}

#[test]
fn state_get_flags_a_stale_agents_state_as_not_live() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "worker", "--workspace", "."]);
    run(&bus, &["state", "set", "worker", "working", "--note", "on it"]);

    // A freshly-claimed, heartbeating agent reads as live.
    let fresh = run(&bus, &["state", "get", "worker", "--json"]);
    let fresh_json: serde_json::Value = serde_json::from_slice(&fresh.stdout).unwrap();
    assert_eq!(fresh_json["state"], "working");
    assert_eq!(fresh_json["live"], true);

    // Expire the heartbeat: the on-disk `current_state` is unchanged, but the
    // agent is gone, so `state get` must report it as not live (text marks it
    // `(stale)`) rather than presenting `working` as authoritative.
    let path = bus.join("agents").join("worker.json");
    let mut agent: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    agent["expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    fs::write(&path, serde_json::to_vec(&agent).unwrap()).unwrap();

    let stale = run(&bus, &["state", "get", "worker", "--json"]);
    let stale_json: serde_json::Value = serde_json::from_slice(&stale.stdout).unwrap();
    assert_eq!(stale_json["state"], "working");
    assert_eq!(stale_json["live"], false);

    let stale_text = run(&bus, &["state", "get", "worker"]);
    assert!(String::from_utf8_lossy(&stale_text.stdout).contains("(stale)"));
}

#[test]
fn any_participant_can_send_without_turn() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    // No turn gate: a non-starter participant can append at any time.
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "b",
            "--to",
            "a",
            "--body",
            "append anytime",
        ],
    );
    let inbox = run(&bus, &["inbox", "a", "--unread"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("append anytime"));
}

#[test]
fn rate_limit_is_enforced() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
            "--rate-max",
            "1",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "one",
        ],
    );
    let denied = run_fail(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "two",
        ],
    );
    assert!(String::from_utf8_lossy(&denied.stderr).contains("rate limited"));
}

#[test]
fn wildcard_recipient_is_allowed() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "*",
            "--body",
            "broadcast",
        ],
    );
    let inbox = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("broadcast"));
}

#[test]
fn inbox_truncates_utf8_without_panic() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let body = format!("{}🚀{}", "a".repeat(116), "b".repeat(20));
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            &body,
        ],
    );
    let inbox = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("..."));
}

#[test]
fn inbox_limit_keeps_the_globally_newest_message_across_rooms() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    // Two rooms; `aaa-room` sorts before `zzz-room` by conversation id, so
    // visible_messages concatenates aaa's messages first. The OLDER message
    // lives in the later-sorting room to expose the missing global sort.
    run(
        &bus,
        &[
            "conversation", "create", "aaa-room", "--participants", "a,viewer",
            "--starter", "a",
        ],
    );
    run(
        &bus,
        &[
            "conversation", "create", "zzz-room", "--participants", "a,viewer",
            "--starter", "a",
        ],
    );
    let old = run(
        &bus,
        &[
            "send", "--conversation", "zzz-room", "--from", "a", "--to", "viewer",
            "--body", "older from zzz",
        ],
    );
    let old_id = String::from_utf8_lossy(&old.stdout).trim().to_string();
    let new = run(
        &bus,
        &[
            "send", "--conversation", "aaa-room", "--from", "a", "--to", "viewer",
            "--body", "newer from aaa",
        ],
    );
    let new_id = String::from_utf8_lossy(&new.stdout).trim().to_string();
    assert!(new_id > old_id, "ids must be time-ordered for this test");

    // `--limit 1` must surface the globally newest message, regardless of which
    // room sorts last.
    let inbox = run(&bus, &["inbox", "viewer", "--limit", "1", "--json"]);
    let views: Vec<serde_json::Value> = serde_json::from_slice(&inbox.stdout).unwrap();
    assert_eq!(views.len(), 1);
    assert_eq!(views[0]["id"].as_str().unwrap(), new_id);
}

#[test]
fn inbox_width_controls_body_truncation() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let body = format!("{} tail-marker", "a".repeat(140));
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            &body,
        ],
    );
    let default_inbox = run(&bus, &["inbox", "b", "--unread"]);
    assert!(!String::from_utf8_lossy(&default_inbox.stdout).contains("tail-marker"));
    let wide_inbox = run(&bus, &["inbox", "b", "--unread", "--width", "200"]);
    assert!(String::from_utf8_lossy(&wide_inbox.stdout).contains("tail-marker"));
}

#[test]
fn ui_snapshot_endpoint_serves_bus_state() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alpha-agent", "--workspace", "."]);
    run(&bus, &["claim", "beta-agent", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "alpha-agent,beta-agent",
            "--starter",
            "alpha-agent",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "alpha-agent",
            "--to",
            "beta-agent",
            "--subject",
            "hello",
            "--body",
            "ui visible message",
        ],
    );

    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let port_string = port.to_string();
    let mut child = Command::new(bin())
        .arg("--root")
        .arg(&bus)
        .args([
            "ui",
            "--agent",
            "beta-agent",
            "--host",
            "127.0.0.1",
            "--port",
            &port_string,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let mut response = String::new();
    for _ in 0..40 {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(mut stream) => {
                stream
                    .write_all(
                        format!(
                            "GET /api/snapshot?agent=beta-agent HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .unwrap();
                stream.read_to_string(&mut response).unwrap();
                break;
            }
            Err(_) => thread::sleep(Duration::from_millis(50)),
        }
    }

    let post_body = r#"{"agent":"alpha-agent","conversation":"c","to":"beta-agent","subject":"from ui","body":"sent from ui endpoint"}"#;
    let mut send_response = String::new();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(
            format!(
                "POST /api/send HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                post_body.len(),
                post_body
            )
            .as_bytes(),
        )
        .unwrap();
    stream.read_to_string(&mut send_response).unwrap();
    child.kill().unwrap();
    let _ = child.wait();
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.contains("\"agent\": \"beta-agent\""));
    assert!(response.contains("ui visible message"));
    assert!(response.contains("\"unread\": true"));
    assert!(send_response.starts_with("HTTP/1.1 200 OK"));
    assert!(send_response.contains("\"message_id\""));
    let inbox = run(&bus, &["inbox", "beta-agent", "--unread", "--width", "200"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("sent from ui endpoint"));
}

#[test]
fn ui_rejects_cross_origin_writes() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alpha-agent", "--workspace", "."]);
    run(&bus, &["claim", "beta-agent", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "alpha-agent,beta-agent",
            "--starter",
            "alpha-agent",
        ],
    );

    let probe = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let port_string = port.to_string();
    let mut child = Command::new(bin())
        .arg("--root")
        .arg(&bus)
        .args([
            "ui",
            "--agent",
            "beta-agent",
            "--host",
            "127.0.0.1",
            "--port",
            &port_string,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    for _ in 0..40 {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let post_body = r#"{"agent":"alpha-agent","conversation":"c","to":"beta-agent","body":"evil origin write"}"#;
    let mut send_response = String::new();
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .write_all(
            format!(
                "POST /api/send HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nOrigin: http://evil.example.com\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                post_body.len(),
                post_body
            )
            .as_bytes(),
        )
        .unwrap();
    stream.read_to_string(&mut send_response).unwrap();
    child.kill().unwrap();
    let _ = child.wait();

    assert!(send_response.starts_with("HTTP/1.1 403 Forbidden"));
    assert!(send_response.contains("blocked cross-origin UI write"));
    let inbox = run(&bus, &["inbox", "beta-agent", "--unread", "--width", "200"]);
    assert!(!String::from_utf8_lossy(&inbox.stdout).contains("evil origin write"));
}

#[test]
fn watch_emits_and_auto_marks_read() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "watch me",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let watched = run(
        &bus,
        &["watch", "--agent", "b", "--conversation", "c", "--once"],
    );
    assert!(String::from_utf8_lossy(&watched.stdout).contains(&message_id));
    let unread = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&unread.stdout).contains("no messages"));
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("watch/b.json")).unwrap()).unwrap();
    assert_eq!(state["last_event_id"], message_id);
}

#[test]
fn watch_no_auto_read_still_resumes_past_emitted_id() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "still unread",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let first = run(
        &bus,
        &[
            "watch",
            "--agent",
            "b",
            "--conversation",
            "c",
            "--once",
            "--no-auto-read",
        ],
    );
    assert!(String::from_utf8_lossy(&first.stdout).contains(&message_id));
    let unread = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&unread.stdout).contains(&message_id));
    let second = run(
        &bus,
        &[
            "watch",
            "--agent",
            "b",
            "--conversation",
            "c",
            "--once",
            "--no-auto-read",
        ],
    );
    assert!(String::from_utf8_lossy(&second.stdout).is_empty());
}

#[test]
fn show_renders_thread_without_marking_read() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let first = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--subject",
            "one",
            "--body",
            "first message",
        ],
    );
    let first_id = String::from_utf8_lossy(&first.stdout).trim().to_string();
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--subject",
            "two",
            "--body",
            "second message",
        ],
    );
    let shown = run(&bus, &["show", "--agent", "b", "--conversation", "c"]);
    let stdout = String::from_utf8_lossy(&shown.stdout);
    assert!(stdout.contains("Subject: one"));
    assert!(stdout.contains("second message"));
    let unread = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&unread.stdout).contains(&first_id));
}

#[test]
fn show_json_honors_limit() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "first",
        ],
    );
    let second = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "second",
        ],
    );
    let second_id = String::from_utf8_lossy(&second.stdout).trim().to_string();
    let shown = run(
        &bus,
        &[
            "show",
            "--agent",
            "b",
            "--conversation",
            "c",
            "--limit",
            "1",
            "--json",
        ],
    );
    let messages: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
    assert_eq!(messages.as_array().unwrap().len(), 1);
    assert_eq!(messages[0]["id"], second_id);
}

#[test]
fn search_finds_visible_messages_without_marking_read() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--subject",
            "Need Audit",
            "--body",
            "please check the pricing path",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "unrelated",
        ],
    );

    let found = run(
        &bus,
        &["search", "pricing", "--agent", "b", "--conversation", "c"],
    );
    let stdout = String::from_utf8_lossy(&found.stdout);
    assert!(stdout.contains(&message_id));
    assert!(!stdout.contains("unrelated"));
    let unread = run(&bus, &["inbox", "b", "--unread"]);
    assert!(String::from_utf8_lossy(&unread.stdout).contains(&message_id));
}

#[test]
fn search_json_honors_since_and_limit() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let old = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "needle old",
        ],
    );
    let old_id = String::from_utf8_lossy(&old.stdout).trim().to_string();
    let old_path = bus.join(format!("conversations/c/messages/{old_id}.json"));
    let mut old_message: serde_json::Value =
        serde_json::from_slice(&fs::read(&old_path).unwrap()).unwrap();
    old_message["created_at"] = serde_json::Value::String("2000-01-01T00:00:00Z".to_string());
    fs::write(&old_path, serde_json::to_vec(&old_message).unwrap()).unwrap();
    let fresh = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "needle fresh",
        ],
    );
    let fresh_id = String::from_utf8_lossy(&fresh.stdout).trim().to_string();

    let found = run(
        &bus,
        &[
            "search",
            "needle",
            "--agent",
            "b",
            "--conversation",
            "c",
            "--since",
            "1h",
            "--limit",
            "1",
            "--json",
        ],
    );
    let messages: serde_json::Value = serde_json::from_slice(&found.stdout).unwrap();
    assert_eq!(messages.as_array().unwrap().len(), 1);
    assert_eq!(messages[0]["id"], fresh_id);
}

#[test]
fn thread_renders_after_descendants_as_tree() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let root = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "root topic",
        ],
    );
    let root_id = String::from_utf8_lossy(&root.stdout).trim().to_string();
    let child = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--after",
            &root_id,
            "--body",
            "child branch",
        ],
    );
    let child_id = String::from_utf8_lossy(&child.stdout).trim().to_string();
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--after",
            &root_id,
            "--body",
            "sibling branch",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--after",
            &child_id,
            "--body",
            "grandchild branch",
        ],
    );

    let thread = run(&bus, &["thread", &root_id, "--agent", "b"]);
    let stdout = String::from_utf8_lossy(&thread.stdout);
    assert!(stdout.contains("root topic"));
    assert!(stdout.contains("  child branch"));
    assert!(stdout.contains("  sibling branch"));
    assert!(stdout.contains("    grandchild branch"));

    let json = run(&bus, &["thread", &root_id, "--agent", "b", "--json"]);
    let tree: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(tree["message"]["id"], root_id);
    assert_eq!(tree["children"].as_array().unwrap().len(), 2);
}

#[test]
fn thread_limit_keeps_the_newest_replies_not_the_oldest() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let root = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "a", "--to", "b", "--body", "root topic",
        ],
    );
    let root_id = String::from_utf8_lossy(&root.stdout).trim().to_string();

    // Five direct replies; ids increase with send order so the last ones sent
    // are the newest.
    let mut child_ids = Vec::new();
    for index in 0..5 {
        let reply = run(
            &bus,
            &[
                "send",
                "--conversation",
                "c",
                "--from",
                "a",
                "--to",
                "b",
                "--after",
                &root_id,
                "--body",
                &format!("reply {index}"),
            ],
        );
        child_ids.push(String::from_utf8_lossy(&reply.stdout).trim().to_string());
    }

    // --limit 3 keeps the root plus the 2 newest replies; the 3 oldest drop.
    let json = run(
        &bus,
        &["thread", &root_id, "--agent", "b", "--limit", "3", "--json"],
    );
    let view: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(view["message"]["id"], root_id);
    assert_eq!(view["truncated"], true);
    assert_eq!(view["omitted"], 3);
    let kept: Vec<String> = view["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|child| child["message"]["id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(kept, vec![child_ids[3].clone(), child_ids[4].clone()]);

    // Without a binding limit nothing is omitted.
    let full = run(
        &bus,
        &["thread", &root_id, "--agent", "b", "--limit", "100", "--json"],
    );
    let full_view: serde_json::Value = serde_json::from_slice(&full.stdout).unwrap();
    assert_eq!(full_view["truncated"], false);
    assert_eq!(full_view["omitted"], 0);
    assert_eq!(full_view["children"].as_array().unwrap().len(), 5);
}

#[test]
fn receipts_report_read_and_ack_statuses() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b,c",
            "--starter",
            "a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b,c",
            "--body",
            "needs feedback",
            "--requires-ack",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(&bus, &["read", "b", &message_id]);
    run(
        &bus,
        &["ack", "b", &message_id, "--status", "done", "--note", "ok"],
    );
    run(&bus, &["read", "c", &message_id]);

    let json = run(&bus, &["receipts", &message_id, "--json"]);
    let report: serde_json::Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(report["message"]["id"], message_id);
    assert_eq!(report["recipients"][0], "b");
    assert_eq!(report["recipients"][1], "c");
    assert_eq!(report["receipts"]["b"]["status"], "done");
    assert_eq!(report["receipts"]["b"]["note"], "ok");
    assert_eq!(report["receipts"]["c"]["status"], "read");

    let text = run(&bus, &["receipts", &message_id]);
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(stdout.contains("b: read="));
    assert!(stdout.contains("status=done"));
    assert!(stdout.contains("c: read="));
}

#[test]
fn agents_claim_names_and_mentions_notify_channel_subscribers() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "claim",
            "atlas-reviewer",
            "--workspace",
            ".",
            "--capabilities",
            "review,coordination",
        ],
    );
    let duplicate = run_fail(&bus, &["claim", "@atlas-reviewer", "--workspace", "."]);
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("already claimed"));
    run(&bus, &["claim", "builder-agent", "--workspace", "."]);
    run(
        &bus,
        &[
            "channel",
            "create",
            "homekeep",
            "--creator",
            "atlas-reviewer",
        ],
    );
    run(
        &bus,
        &["channel", "join", "homekeep", "--agent", "builder-agent"],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--channel",
            "homekeep",
            "--from",
            "atlas-reviewer",
            "--to",
            "@atlas-reviewer",
            "--body",
            "Please check this @builder-agent",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let inbox = run(
        &bus,
        &[
            "inbox",
            "builder-agent",
            "--channel",
            "homekeep",
            "--unread",
        ],
    );
    assert!(String::from_utf8_lossy(&inbox.stdout).contains(&message_id));
    let read = run(&bus, &["read", "builder-agent", &message_id, "--json"]);
    let message: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    assert_eq!(message["mentions"][0], "builder-agent");
}

#[test]
fn group_conversation_and_private_side_chat_work() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "group",
            "--participants",
            "a,b,c",
            "--starter",
            "a",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "group",
            "--from",
            "a",
            "--to",
            "b,c",
            "--body",
            "group message",
        ],
    );
    let c_inbox = run(&bus, &["inbox", "c", "--unread"]);
    assert!(String::from_utf8_lossy(&c_inbox.stdout).contains("group message"));

    run(
        &bus,
        &[
            "conversation",
            "open",
            "--id",
            "side-a-b",
            "--from",
            "a",
            "--to",
            "b",
            "--topic",
            "side channel",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "side-a-b",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "private note",
        ],
    );
    let b_status = run(&bus, &["status", "--agent", "b"]);
    assert!(String::from_utf8_lossy(&b_status.stdout).contains("side-a-b"));
    let c_status = run(&bus, &["status", "--agent", "c"]);
    assert!(!String::from_utf8_lossy(&c_status.stdout).contains("side-a-b"));
    let c_private_inbox = run(&bus, &["inbox", "c", "--conversation", "side-a-b"]);
    assert!(String::from_utf8_lossy(&c_private_inbox.stdout).contains("no messages"));
}

#[test]
fn conversation_open_if_missing_is_idempotent_for_a_derived_id() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);

    let open = |from: &str, to: &str| -> serde_json::Value {
        serde_json::from_slice(
            &run(
                &bus,
                &[
                    "conversation", "open", "--from", from, "--to", to,
                    "--topic", "deploy", "--if-missing", "--json",
                ],
            )
            .stdout,
        )
        .unwrap()
    };

    // First open derives an id and creates the room.
    let first = open("alice", "bob");
    assert_eq!(first["created"], true);
    let id = first["conversation_id"].as_str().unwrap().to_string();

    // Re-opening the same membership+topic must reuse that room, not fork a new
    // one — the whole point of --if-missing. The derived id is deterministic.
    let second = open("alice", "bob");
    assert_eq!(second["conversation_id"].as_str().unwrap(), id);
    assert_eq!(second["created"], false);

    // The derived id is independent of who opens it and the order of --to, so a
    // peer opening from the other side lands in the same room.
    let reverse = open("bob", "alice");
    assert_eq!(reverse["conversation_id"].as_str().unwrap(), id);
    assert_eq!(reverse["created"], false);

    // A different topic is a different room.
    let other_topic: serde_json::Value = serde_json::from_slice(
        &run(
            &bus,
            &[
                "conversation", "open", "--from", "alice", "--to", "bob",
                "--topic", "incident", "--if-missing", "--json",
            ],
        )
        .stdout,
    )
    .unwrap();
    assert_ne!(other_topic["conversation_id"].as_str().unwrap(), id);
    assert_eq!(other_topic["created"], true);
}

#[test]
fn event_kind_cannot_open_an_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "worker", "--workspace", "."]);
    run(&bus, &["claim", "bridge", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "worker,bridge",
            "--starter", "bridge",
        ],
    );

    // An event must not carry obligation flags: a bridge relaying a human is
    // not asking a peer for a reply, and the bridge rarely runs `ack`, so an
    // honored flag would strand a permanently-open ask.
    let ack = run_fail(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "bridge", "--to", "worker",
            "--kind", "event", "--body", "human says hi", "--requires-ack", "--json",
        ],
    );
    let ack_err: serde_json::Value = serde_json::from_slice(&ack.stderr).unwrap();
    assert!(
        ack_err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only valid on kind")
    );

    let needs = run_fail(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "bridge", "--to", "worker",
            "--kind", "event", "--body", "human says hi",
            "--needs-response-from", "worker", "--json",
        ],
    );
    let needs_err: serde_json::Value = serde_json::from_slice(&needs.stderr).unwrap();
    assert!(
        needs_err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only valid on kind")
    );

    // A plain event still sends and never shows up as an owed ask.
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "bridge", "--to", "worker",
            "--kind", "event", "--body", "human says hi", "--json",
        ],
    );
    let owed = run(&bus, &["awaiting", "worker", "--json"]);
    let owed_json: serde_json::Value = serde_json::from_slice(&owed.stdout).unwrap();
    assert!(owed_json["you_owe"].as_array().unwrap().is_empty());
}

#[test]
fn bridge_event_rates_by_subject_id() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b,tg-bridge",
            "--starter",
            "a",
            "--rate-max",
            "1",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "tg-bridge",
            "--to",
            "b",
            "--kind",
            "event",
            "--subject-id",
            "telegram:1",
            "--body",
            "first user",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "tg-bridge",
            "--to",
            "b",
            "--kind",
            "event",
            "--subject-id",
            "telegram:2",
            "--body",
            "second user",
        ],
    );
    let denied = run_fail(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "tg-bridge",
            "--to",
            "b",
            "--kind",
            "event",
            "--subject-id",
            "telegram:1",
            "--body",
            "first user again",
        ],
    );
    assert!(String::from_utf8_lossy(&denied.stderr).contains("rate limited"));
}

#[test]
fn awaiting_tracks_open_asks_until_resolved() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "sender-x", "--workspace", "."]);
    run(&bus, &["claim", "ower-y", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "sender-x,ower-y",
            "--starter",
            "sender-x",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "sender-x",
            "--to",
            "ower-y",
            "--subject",
            "need answer",
            "--body",
            "please respond",
            "--needs-response-from",
            "ower-y",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // The awaited agent owes a response; the sender is waiting on one.
    let owes = run(&bus, &["awaiting", "ower-y", "--json"]);
    let owes_json: serde_json::Value = serde_json::from_slice(&owes.stdout).unwrap();
    assert_eq!(owes_json["you_owe"][0]["message_id"], message_id);
    assert_eq!(owes_json["you_owe"][0]["from"], "sender-x");
    assert!(owes_json["owed_to_you"].as_array().unwrap().is_empty());

    let waiting = run(&bus, &["awaiting", "sender-x", "--json"]);
    let waiting_json: serde_json::Value = serde_json::from_slice(&waiting.stdout).unwrap();
    assert_eq!(waiting_json["owed_to_you"][0]["message_id"], message_id);
    assert!(waiting_json["you_owe"].as_array().unwrap().is_empty());

    // A terminal ack from the awaited agent closes the ask for both sides.
    run(&bus, &["read", "ower-y", &message_id]);
    run(&bus, &["ack", "ower-y", &message_id, "--status", "done"]);

    let owes_after = run(&bus, &["awaiting", "ower-y", "--json"]);
    let owes_after_json: serde_json::Value = serde_json::from_slice(&owes_after.stdout).unwrap();
    assert!(owes_after_json["you_owe"].as_array().unwrap().is_empty());

    let waiting_after = run(&bus, &["awaiting", "sender-x", "--json"]);
    let waiting_after_json: serde_json::Value =
        serde_json::from_slice(&waiting_after.stdout).unwrap();
    assert!(waiting_after_json["owed_to_you"].as_array().unwrap().is_empty());
}

#[test]
fn ack_rejects_unknown_status_and_nonterminal_keeps_ask_open() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "sender-x", "--workspace", "."]);
    run(&bus, &["claim", "ower-y", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "sender-x,ower-y",
            "--starter",
            "sender-x",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "sender-x",
            "--to",
            "ower-y",
            "--subject",
            "need answer",
            "--body",
            "please respond",
            "--needs-response-from",
            "ower-y",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(&bus, &["read", "ower-y", &message_id]);

    // An unrecognized status is rejected with a stable error code.
    let denied = run_fail(
        &bus,
        &["ack", "ower-y", &message_id, "--status", "finished", "--json"],
    );
    let envelope: serde_json::Value = serde_json::from_slice(&denied.stderr).unwrap();
    assert_eq!(envelope["ok"], serde_json::json!(false));
    assert_eq!(envelope["error"]["code"], serde_json::json!("error"));
    assert!(
        String::from_utf8_lossy(&denied.stderr).contains("finished"),
        "error should echo the rejected status"
    );

    // A valid but non-terminal status records the receipt yet keeps the ask open.
    run(&bus, &["ack", "ower-y", &message_id, "--status", "working"]);
    let owes = run(&bus, &["awaiting", "ower-y", "--json"]);
    let owes_json: serde_json::Value = serde_json::from_slice(&owes.stdout).unwrap();
    assert_eq!(
        owes_json["you_owe"][0]["message_id"], message_id,
        "non-terminal ack must not close the ask"
    );

    // A terminal status then closes it.
    run(&bus, &["ack", "ower-y", &message_id, "--status", "done"]);
    let owes_after = run(&bus, &["awaiting", "ower-y", "--json"]);
    let owes_after_json: serde_json::Value = serde_json::from_slice(&owes_after.stdout).unwrap();
    assert!(owes_after_json["you_owe"].as_array().unwrap().is_empty());
}

#[test]
fn requires_ack_creates_open_ask_for_recipient() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "sender-x", "--workspace", "."]);
    run(&bus, &["claim", "ower-y", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "sender-x,ower-y",
            "--starter",
            "sender-x",
        ],
    );
    // No explicit needs-response-from, but --requires-ack implies an open ask.
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "sender-x",
            "--to",
            "ower-y",
            "--subject",
            "ack me",
            "--body",
            "please ack",
            "--requires-ack",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let owes = run(&bus, &["awaiting", "ower-y", "--json"]);
    let owes_json: serde_json::Value = serde_json::from_slice(&owes.stdout).unwrap();
    assert_eq!(owes_json["you_owe"][0]["message_id"], message_id);
}

#[test]
fn roster_lists_live_agents_with_presence_and_ask_counts() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alpha-agent", "--workspace", "."]);
    run(&bus, &["claim", "bravo-agent", "--workspace", "."]);
    run(
        &bus,
        &["state", "set", "bravo-agent", "blocked", "--note", "stuck"],
    );
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "alpha-agent,bravo-agent",
            "--starter",
            "alpha-agent",
        ],
    );
    run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "alpha-agent",
            "--to",
            "bravo-agent",
            "--subject",
            "ping",
            "--body",
            "respond",
            "--needs-response-from",
            "bravo-agent",
        ],
    );

    let roster = run(&bus, &["roster", "--json"]);
    let roster_json: serde_json::Value = serde_json::from_slice(&roster.stdout).unwrap();
    let agents = roster_json["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    // blocked sorts before idle.
    assert_eq!(agents[0]["id"], "bravo-agent");
    assert_eq!(agents[0]["current_state"], "blocked");
    assert_eq!(agents[0]["owes"], 1);
    assert_eq!(agents[0]["waiting_on"], 0);
    let alpha = agents
        .iter()
        .find(|agent| agent["id"] == "alpha-agent")
        .unwrap();
    assert_eq!(alpha["owes"], 0);
    assert_eq!(alpha["waiting_on"], 1);
}

#[test]
fn subject_id_rejects_rate_key_separator() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b,tg-bridge",
            "--starter",
            "a",
        ],
    );
    let denied = run_fail(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "tg-bridge",
            "--to",
            "b",
            "--kind",
            "event",
            "--subject-id",
            "telegram:chat#user",
            "--body",
            "cannot collide rate keys",
        ],
    );
    assert!(String::from_utf8_lossy(&denied.stderr).contains("reserved"));
}

#[test]
fn system_kind_is_reserved_for_raft() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
        ],
    );
    let denied = run_fail(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--kind",
            "system",
            "--body",
            "fake system event",
        ],
    );
    assert!(String::from_utf8_lossy(&denied.stderr).contains("reserved"));
}

#[test]
fn records_include_schema_versions_and_journal_entries() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "agent-a", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "agent-a,b",
            "--starter",
            "agent-a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "agent-a",
            "--to",
            "b",
            "--body",
            "versioned",
        ],
    );
    run(
        &bus,
        &[
            "journal",
            "agent-a",
            "--kind",
            "note",
            "--subject",
            "checkpoint",
            "--body",
            "local reasoning note",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let meta: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("conversations/c/meta.json")).unwrap()).unwrap();
    let message: serde_json::Value = serde_json::from_slice(
        &fs::read(bus.join(format!("conversations/c/messages/{message_id}.json"))).unwrap(),
    )
    .unwrap();
    let agent: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("agents/agent-a.json")).unwrap()).unwrap();
    let journal = fs::read_to_string(bus.join("journal/agent-a.jsonl")).unwrap();
    assert_eq!(meta["_v"], 1);
    assert_eq!(message["_v"], 1);
    assert_eq!(agent["_v"], 1);
    assert!(journal.contains("\"_v\":1"));
}

#[test]
fn gc_archive_moves_receipts_with_message() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "a,b",
            "--starter",
            "a",
            "--retention-days",
            "1",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "a",
            "--to",
            "b",
            "--body",
            "archive me",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(&bus, &["read", "b", &message_id]);
    run(&bus, &["ack", "b", &message_id, "--status", "done"]);

    let message_path = bus.join(format!("conversations/c/messages/{message_id}.json"));
    let mut message: serde_json::Value =
        serde_json::from_slice(&fs::read(&message_path).unwrap()).unwrap();
    message["created_at"] = serde_json::Value::String("2000-01-01T00:00:00Z".to_string());
    fs::write(&message_path, serde_json::to_vec(&message).unwrap()).unwrap();

    run(&bus, &["gc", "--archive"]);

    assert!(!message_path.exists());
    assert!(bus.join(format!("archive/c/{message_id}.json")).exists());
    assert!(
        !bus.join(format!("conversations/c/receipts/{message_id}"))
            .exists()
    );
    assert!(
        bus.join(format!("archive/c/receipts/{message_id}/b.json"))
            .exists()
    );
}

#[test]
fn gc_archive_retains_an_unresolved_open_ask_until_it_is_discharged() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "a,b", "--starter", "a",
            "--retention-days", "1",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "a", "--to", "b",
            "--body", "deploy?", "--requires-ack",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // Age the ask well past its retention window without resolving it. The join
    // baseline is pushed even earlier so the ask still post-dates b's
    // membership (otherwise the membership filter would drop b on its own).
    let meta_path = bus.join("conversations/c/meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    meta["joined_at"]["a"] = serde_json::json!("1999-01-01T00:00:00Z");
    meta["joined_at"]["b"] = serde_json::json!("1999-01-01T00:00:00Z");
    fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

    let message_path = bus.join(format!("conversations/c/messages/{message_id}.json"));
    let mut message: serde_json::Value =
        serde_json::from_slice(&fs::read(&message_path).unwrap()).unwrap();
    message["created_at"] = serde_json::Value::String("2000-01-01T00:00:00Z".to_string());
    fs::write(&message_path, serde_json::to_vec(&message).unwrap()).unwrap();

    // Archival must not vanish a live obligation: the message stays put and the
    // worker still owes it.
    run(&bus, &["gc", "--archive"]);
    assert!(
        message_path.exists(),
        "an unresolved open ask must not be archived out of every obligation view"
    );
    assert!(!bus.join(format!("archive/c/{message_id}.json")).exists());
    let owes = run(&bus, &["awaiting", "b", "--json"]);
    let owes_json: serde_json::Value = serde_json::from_slice(&owes.stdout).unwrap();
    assert_eq!(owes_json["you_owe"][0]["message_id"], message_id);

    // Once b discharges it with a terminal ack, the same gc run archives it.
    run(&bus, &["read", "b", &message_id]);
    run(&bus, &["ack", "b", &message_id, "--status", "done"]);
    run(&bus, &["gc", "--archive"]);
    assert!(!message_path.exists(), "a resolved ask should archive normally");
    assert!(bus.join(format!("archive/c/{message_id}.json")).exists());
}

#[test]
fn doctor_reports_healthy_bus_as_ok() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "agent-a", "--workspace", "."]);
    run(&bus, &["claim", "agent-b", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "agent-a,agent-b",
            "--starter",
            "agent-a",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send",
            "--conversation",
            "c",
            "--from",
            "agent-a",
            "--to",
            "agent-b",
            "--body",
            "health check",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(&bus, &["read", "agent-b", &message_id]);
    run(&bus, &["ack", "agent-b", &message_id, "--status", "done"]);

    let doctor = run(&bus, &["doctor", "--json"]);
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["ok"], true);
    assert_eq!(report["error_count"], 0);
    assert_eq!(report["warning_count"], 0);
    assert_eq!(report["counts"]["agents"], 2);
    assert_eq!(report["counts"]["conversations"], 1);
    assert_eq!(report["counts"]["receipts"], 1);
}

#[test]
fn doctor_reports_corrupt_json_without_mutating() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "agent-a", "--workspace", "."]);
    run(&bus, &["claim", "agent-b", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "agent-a,agent-b",
            "--starter",
            "agent-a",
        ],
    );
    let meta_path = bus.join("conversations/c/meta.json");
    fs::write(&meta_path, b"{not json").unwrap();

    let doctor = run_fail(&bus, &["doctor", "--json"]);
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["ok"], false);
    assert!(report["error_count"].as_u64().unwrap() >= 1);
    assert!(report["issues"].as_array().unwrap().iter().any(|issue| {
        issue["code"] == "invalid_json" && issue["path"] == "conversations/c/meta.json"
    }));
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("doctor found"));
    assert_eq!(fs::read(&meta_path).unwrap(), b"{not json");
}

#[test]
fn doctor_strict_fails_on_warnings() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &[
            "conversation",
            "create",
            "c",
            "--participants",
            "agent-a,agent-b",
            "--starter",
            "agent-a",
        ],
    );

    let normal = run(&bus, &["doctor", "--json"]);
    let normal_report: serde_json::Value = serde_json::from_slice(&normal.stdout).unwrap();
    assert_eq!(normal_report["ok"], true);
    assert_eq!(normal_report["error_count"], 0);
    assert!(normal_report["warning_count"].as_u64().unwrap() >= 1);

    let strict = run_fail(&bus, &["doctor", "--strict", "--json"]);
    let strict_report: serde_json::Value = serde_json::from_slice(&strict.stdout).unwrap();
    assert_eq!(strict_report["ok"], false);
    assert_eq!(strict_report["error_count"], 0);
    assert!(strict_report["warning_count"].as_u64().unwrap() >= 1);
}

fn backdate(path: &PathBuf) {
    let status = Command::new("touch")
        .args(["-t", "200001010000"])
        .arg(path)
        .status()
        .unwrap();
    assert!(status.success(), "touch should backdate {path:?}");
}

#[test]
fn gc_reaps_stale_orphan_temp_files_but_keeps_fresh_ones() {
    let bus = temp_bus();
    run(&bus, &["init"]);

    // Two interrupted atomic writes: dot-prefixed ".tmp" siblings.
    let stale = bus.join("agents").join(".atlas.json.999.deadbeef.tmp");
    fs::write(&stale, b"{}\n").unwrap();
    let fresh = bus.join("agents").join(".atlas.json.998.cafef00d.tmp");
    fs::write(&fresh, b"{}\n").unwrap();
    backdate(&stale);

    let out = run(&bus, &["gc"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("orphan_temp_files=1"),
        "gc should reap exactly the stale temp file; got: {stdout}"
    );
    assert!(!stale.exists(), "stale temp file should be reaped");
    assert!(
        fresh.exists(),
        "a recently written temp file must be preserved (may be an in-flight write)"
    );
}

fn write_lock(bus: &PathBuf, name: &str, token: &str, expires_at: &str) -> PathBuf {
    let dir = bus.join("locks").join(format!("{name}.lock"));
    fs::create_dir_all(&dir).unwrap();
    let owner = format!(
        "{{\"v\":1,\"token\":\"{token}\",\"pid\":4321,\"host\":\"test\",\
         \"acquired_at\":\"2000-01-01T00:00:00Z\",\"expires_at\":\"{expires_at}\"}}\n"
    );
    fs::write(dir.join("owner.json"), owner).unwrap();
    dir
}

#[test]
fn gc_reaps_expired_lock_but_keeps_a_live_one() {
    let bus = temp_bus();
    run(&bus, &["init"]);

    let expired = write_lock(&bus, "conversation-c", "tok-old", "2000-01-01T00:00:00Z");
    let live = write_lock(&bus, "conversation-d", "tok-live", "2999-01-01T00:00:00Z");

    let out = run(&bus, &["gc"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("stale_locks=1"),
        "exactly the expired lock should be reaped; got: {stdout}"
    );
    assert!(!expired.exists(), "expired lock should be reaped");
    assert!(
        live.exists(),
        "a lock whose lease is still in the future must never be reaped"
    );
}

#[test]
fn doctor_reports_stale_orphan_temp_files() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    let stale = bus.join("conversations").join(".meta.json.1.abc.tmp");
    fs::create_dir_all(stale.parent().unwrap()).unwrap();
    fs::write(&stale, b"{}\n").unwrap();
    backdate(&stale);

    let out = run(&bus, &["doctor", "--json"]);
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["ok"], true, "orphan temp files are warnings, not errors");
    assert!(
        report["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|issue| issue["code"] == "orphan_temp_file"),
        "doctor should warn about the stale orphan temp file"
    );
}

#[test]
fn channel_list_reports_membership_and_unread() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &["channel", "create", "eng", "--creator", "alice", "--members", "bob"],
    );
    run(&bus, &["channel", "create", "ops", "--creator", "alice"]);
    run(
        &bus,
        &[
            "send", "--channel", "eng", "--from", "alice", "--to", "*", "--subject",
            "hi", "--body", "hello team",
        ],
    );

    // Bare listing is an array covering every channel regardless of membership.
    let listed = run(&bus, &["channel", "list", "--json"]);
    let channels: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let arr = channels.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let ops = arr.iter().find(|c| c["id"] == "ops").unwrap();
    assert_eq!(ops["member_count"], 1);
    assert!(ops.get("joined").is_none());

    // With --agent, each channel is annotated with membership and unread.
    let scoped = run(&bus, &["channel", "list", "--agent", "bob", "--json"]);
    let scoped_channels: serde_json::Value = serde_json::from_slice(&scoped.stdout).unwrap();
    let scoped_arr = scoped_channels.as_array().unwrap();
    let eng = scoped_arr.iter().find(|c| c["id"] == "eng").unwrap();
    assert_eq!(eng["joined"], true);
    assert_eq!(eng["unread"], 1);
    let ops = scoped_arr.iter().find(|c| c["id"] == "ops").unwrap();
    assert_eq!(ops["joined"], false);
    assert_eq!(ops["unread"], 0);
}

#[test]
fn roster_exposes_capabilities_and_filters_by_them() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &["claim", "alice", "--workspace", ".", "--capabilities", "review,docs"],
    );
    run(
        &bus,
        &[
            "claim", "bob", "--workspace", ".", "--capabilities", "implementation,tests",
        ],
    );

    // Every agent carries its capability tags in the roster.
    let all = run(&bus, &["roster", "--json"]);
    let all_json: serde_json::Value = serde_json::from_slice(&all.stdout).unwrap();
    let agents = all_json["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    let alice = agents.iter().find(|a| a["id"] == "alice").unwrap();
    assert_eq!(alice["capabilities"], serde_json::json!(["review", "docs"]));

    // --capability narrows to agents advertising the tag.
    let filtered = run(&bus, &["roster", "--capability", "tests", "--json"]);
    let filtered_json: serde_json::Value = serde_json::from_slice(&filtered.stdout).unwrap();
    let filtered_agents = filtered_json["agents"].as_array().unwrap();
    assert_eq!(filtered_agents.len(), 1);
    assert_eq!(filtered_agents[0]["id"], "bob");
}

#[test]
fn reply_inherits_conversation_thread_and_subject() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let parent = run(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "alice", "--to", "bob",
            "--subject", "task", "--body", "please do X",
        ],
    );
    let parent_id = String::from_utf8(parent.stdout).unwrap().trim().to_string();

    let reply = run(
        &bus,
        &["reply", &parent_id, "--from", "bob", "--body", "on it", "--json"],
    );
    let envelope: serde_json::Value = serde_json::from_slice(&reply.stdout).unwrap();
    assert_eq!(envelope["ok"], true);
    assert_eq!(envelope["conversation_id"], "proj");
    assert_eq!(envelope["after"], parent_id);
    // Recipient defaults to the original sender.
    assert_eq!(envelope["to"], serde_json::json!(["alice"]));

    // The reply inherits the parent subject and is threaded beneath it.
    let thread = run(&bus, &["thread", &parent_id, "--agent", "alice", "--json"]);
    let tree: serde_json::Value = serde_json::from_slice(&thread.stdout).unwrap();
    assert_eq!(tree["children"].as_array().unwrap().len(), 1);
    assert_eq!(tree["children"][0]["message"]["subject"], "task");

    // A non-participant cannot reply.
    let denied = run_fail(
        &bus,
        &["reply", &parent_id, "--from", "carol", "--body", "hi", "--json"],
    );
    let err: serde_json::Value = serde_json::from_slice(&denied.stderr).unwrap();
    assert_eq!(err["error"]["code"], "not_participant");
}

#[test]
fn a_non_terminal_ack_cannot_downgrade_a_closed_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ship it", "--needs-response-from", "bob",
        ],
    );
    let mid = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    let owed_count = |out: &std::process::Output| -> usize {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["owed_to_you"].as_array().unwrap().len()
    };

    // bob closes the ask with a terminal ack.
    run(&bus, &["ack", "bob", &mid, "--status", "done", "--note", "shipped"]);
    assert_eq!(owed_count(&run(&bus, &["awaiting", "alice", "--json"])), 0);

    // A later non-terminal ack must NOT downgrade the stored `done`, which would
    // silently reopen the ask. The response reports the effective status.
    let downgrade = run(&bus, &["ack", "bob", &mid, "--status", "working", "--json"]);
    let dj: serde_json::Value = serde_json::from_slice(&downgrade.stdout).unwrap();
    assert_eq!(dj["ok"], true);
    assert_eq!(dj["status"], "done", "the stored terminal status must be preserved");
    assert_eq!(dj["requested_status"], "working");
    assert_eq!(dj["downgrade_ignored"], true);

    // The ask stays closed and the receipt still reads `done` with its note.
    assert_eq!(owed_count(&run(&bus, &["awaiting", "alice", "--json"])), 0);
    let receipts = run(&bus, &["receipts", &mid, "--json"]);
    let rj: serde_json::Value = serde_json::from_slice(&receipts.stdout).unwrap();
    assert_eq!(rj["receipts"]["bob"]["status"], "done");
    assert_eq!(rj["receipts"]["bob"]["note"], "shipped");
}

#[test]
fn reply_with_ack_closes_the_open_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let parent = run(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "alice", "--to", "bob",
            "--subject", "task", "--body", "do X", "--requires-ack",
            "--needs-response-from", "bob",
        ],
    );
    let parent_id = String::from_utf8(parent.stdout).unwrap().trim().to_string();

    // Bob owes a response before replying.
    let before = run(&bus, &["awaiting", "bob", "--json"]);
    let before_json: serde_json::Value = serde_json::from_slice(&before.stdout).unwrap();
    assert_eq!(before_json["you_owe"].as_array().unwrap().len(), 1);

    // reply --ack done both sends a reply and records a terminal receipt.
    let reply = run(
        &bus,
        &[
            "reply", &parent_id, "--from", "bob", "--body", "done it",
            "--ack", "done", "--ack-note", "shipped", "--json",
        ],
    );
    let reply_json: serde_json::Value = serde_json::from_slice(&reply.stdout).unwrap();
    assert_eq!(reply_json["ack"], "done");

    // The ask is now closed.
    let after = run(&bus, &["awaiting", "bob", "--json"]);
    let after_json: serde_json::Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after_json["you_owe"].as_array().unwrap().len(), 0);

    // The receipt landed on the parent with the note.
    let receipts = run(&bus, &["receipts", &parent_id, "--json"]);
    let receipts_json: serde_json::Value = serde_json::from_slice(&receipts.stdout).unwrap();
    assert_eq!(receipts_json["receipts"]["bob"]["status"], "done");
    assert_eq!(receipts_json["receipts"]["bob"]["note"], "shipped");

    // An invalid ack status is rejected before any reply is sent.
    let before_count = std::fs::read_dir(bus.join("conversations/proj/messages"))
        .unwrap()
        .count();
    run_fail(
        &bus,
        &["reply", &parent_id, "--from", "bob", "--body", "x", "--ack", "finished"],
    );
    let after_count = std::fs::read_dir(bus.join("conversations/proj/messages"))
        .unwrap()
        .count();
    assert_eq!(before_count, after_count, "bad ack status must not send a reply");
}

#[test]
fn long_help_documents_the_agent_flow() {
    let output = Command::new(bin()).arg("--help").output().unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(
        help.contains("TYPICAL AGENT FLOW"),
        "long help should orient an agent with a typical flow"
    );
    // The conflict code description must cover channels/conversations, not just
    // a claimed agent name.
    assert!(
        help.contains("channel/conversation"),
        "conflict code help should mention channel/conversation conflicts"
    );
}

#[test]
fn awaiting_closes_each_recipient_independently() {
    // A single ask naming two awaited agents must close per recipient: one
    // agent recording a terminal receipt closes its own owed ask without
    // touching the other's, and the sender stays owed until both answer.
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "alice",
            "--to", "bob,carol", "--subject", "need both",
            "--body", "please respond", "--needs-response-from", "bob,carol",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // Both awaited agents owe; the sender is owed two responses.
    let bob_before = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_before_json: serde_json::Value = serde_json::from_slice(&bob_before.stdout).unwrap();
    assert_eq!(bob_before_json["you_owe"].as_array().unwrap().len(), 1);
    let carol_before = run(&bus, &["awaiting", "carol", "--json"]);
    let carol_before_json: serde_json::Value =
        serde_json::from_slice(&carol_before.stdout).unwrap();
    assert_eq!(carol_before_json["you_owe"].as_array().unwrap().len(), 1);
    // The sender is owed one response per awaited recipient, and each entry
    // names which agent is awaited so the sender can tell them apart.
    let alice_before = run(&bus, &["awaiting", "alice", "--json"]);
    let alice_before_json: serde_json::Value =
        serde_json::from_slice(&alice_before.stdout).unwrap();
    let owed_before = alice_before_json["owed_to_you"].as_array().unwrap();
    assert_eq!(owed_before.len(), 2);
    let mut awaited_before: Vec<&str> = owed_before
        .iter()
        .map(|e| e["awaited"].as_str().unwrap())
        .collect();
    awaited_before.sort();
    assert_eq!(awaited_before, vec!["bob", "carol"]);

    // Bob records a terminal receipt; carol does not.
    run(&bus, &["read", "bob", &message_id]);
    run(&bus, &["ack", "bob", &message_id, "--status", "done"]);

    // Bob's ask is closed; carol's remains open; the sender is still owed.
    let bob_after = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_after_json: serde_json::Value = serde_json::from_slice(&bob_after.stdout).unwrap();
    assert!(bob_after_json["you_owe"].as_array().unwrap().is_empty());
    let carol_after = run(&bus, &["awaiting", "carol", "--json"]);
    let carol_after_json: serde_json::Value =
        serde_json::from_slice(&carol_after.stdout).unwrap();
    assert_eq!(carol_after_json["you_owe"].as_array().unwrap().len(), 1);
    let alice_after = run(&bus, &["awaiting", "alice", "--json"]);
    let alice_after_json: serde_json::Value =
        serde_json::from_slice(&alice_after.stdout).unwrap();
    let owed_after = alice_after_json["owed_to_you"].as_array().unwrap();
    assert_eq!(owed_after.len(), 1);
    assert_eq!(owed_after[0]["awaited"], "carol");
}

#[test]
fn conversation_add_lets_a_new_participant_join_an_existing_chat() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );

    // Carol cannot send before being added.
    let blocked = run_fail(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "carol", "--to", "alice",
            "--subject", "hi", "--body", "let me in", "--json",
        ],
    );
    let blocked_json: serde_json::Value = serde_json::from_slice(&blocked.stderr).unwrap();
    assert_eq!(blocked_json["error"]["code"], "not_participant");

    // Adding her reports the new participant set.
    let added = run(&bus, &["conversation", "add", "proj", "--agent", "carol", "--json"]);
    let added_json: serde_json::Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(added_json["ok"], true);
    assert_eq!(added_json["added"], true);
    let participants: Vec<&str> = added_json["participants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    assert!(participants.contains(&"carol"));

    // Re-adding is idempotent: added=false, no duplicate participant.
    let again = run(&bus, &["conversation", "add", "proj", "--agent", "carol", "--json"]);
    let again_json: serde_json::Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again_json["added"], false);
    assert_eq!(again_json["participants"].as_array().unwrap().len(), 3);

    // Now carol can send.
    run(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "carol", "--to", "alice",
            "--subject", "hi", "--body", "thanks",
        ],
    );

    // Adding to a channel is rejected; the agent must use `channel join`.
    run(&bus, &["channel", "create", "room", "--creator", "alice"]);
    let chan = run_fail(&bus, &["conversation", "add", "room", "--agent", "bob", "--json"]);
    let chan_json: serde_json::Value = serde_json::from_slice(&chan.stderr).unwrap();
    assert!(
        chan_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("channel join")
    );

    // Adding to a missing conversation reports not_found.
    let missing = run_fail(&bus, &["conversation", "add", "nope", "--agent", "bob", "--json"]);
    let missing_json: serde_json::Value = serde_json::from_slice(&missing.stderr).unwrap();
    assert_eq!(missing_json["error"]["code"], "not_found");
}

#[test]
fn not_participant_error_lists_the_valid_participants() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );

    // A non-participant sender's error carries the valid participant set so the
    // agent can self-correct (e.g. ask to be added) without a second lookup.
    let from_outsider = run_fail(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "carol", "--to", "alice",
            "--subject", "hi", "--body", "x", "--json",
        ],
    );
    let from_json: serde_json::Value = serde_json::from_slice(&from_outsider.stderr).unwrap();
    assert_eq!(from_json["error"]["code"], "not_participant");
    let mut participants: Vec<&str> = from_json["error"]["participants"]
        .as_array()
        .expect("not_participant error should list participants")
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    participants.sort();
    assert_eq!(participants, vec!["alice", "bob"]);

    // The same enrichment applies when the bad name is the recipient.
    let to_outsider = run_fail(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "alice", "--to", "carol",
            "--subject", "hi", "--body", "x", "--json",
        ],
    );
    let to_json: serde_json::Value = serde_json::from_slice(&to_outsider.stderr).unwrap();
    assert_eq!(to_json["error"]["code"], "not_participant");
    assert_eq!(
        to_json["error"]["participants"].as_array().unwrap().len(),
        2
    );

    // Unrelated errors stay lean — no participants key bleeds in.
    let not_found = run_fail(
        &bus,
        &[
            "send", "--conversation", "nope", "--from", "alice", "--to", "bob",
            "--subject", "hi", "--body", "x", "--json",
        ],
    );
    let nf_json: serde_json::Value = serde_json::from_slice(&not_found.stderr).unwrap();
    assert_eq!(nf_json["error"]["code"], "not_found");
    assert!(nf_json["error"]["participants"].is_null());
}

#[test]
fn not_found_suggests_nearest_conversation_id() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "homekeep-sync", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    run(&bus, &["channel", "create", "homekeep-main", "--creator", "alice"]);

    // A near-miss conversation id on send yields a "did you mean" suggestion.
    let typo = run_fail(
        &bus,
        &[
            "send", "--conversation", "homekep-sync", "--from", "alice", "--to", "bob",
            "--subject", "x", "--body", "y", "--json",
        ],
    );
    let typo_json: serde_json::Value = serde_json::from_slice(&typo.stderr).unwrap();
    assert_eq!(typo_json["error"]["code"], "not_found");
    let suggestions: Vec<&str> = typo_json["error"]["suggestions"]
        .as_array()
        .expect("not_found should suggest near-matches")
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert!(suggestions.contains(&"homekeep-sync"));

    // The closest match is ranked first.
    assert_eq!(suggestions[0], "homekeep-sync");

    // channel join enriches the same way, naming the channel.
    let chan = run_fail(&bus, &["channel", "join", "homekeep-man", "--agent", "bob", "--json"]);
    let chan_json: serde_json::Value = serde_json::from_slice(&chan.stderr).unwrap();
    let chan_suggestions: Vec<&str> = chan_json["error"]["suggestions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap())
        .collect();
    assert_eq!(chan_suggestions[0], "homekeep-main");

    // A wildly different id offers no suggestions rather than noise.
    let far = run_fail(
        &bus,
        &[
            "send", "--conversation", "zzzzzzzzzz", "--from", "alice", "--to", "bob",
            "--subject", "x", "--body", "y", "--json",
        ],
    );
    let far_json: serde_json::Value = serde_json::from_slice(&far.stderr).unwrap();
    assert_eq!(far_json["error"]["code"], "not_found");
    assert!(far_json["error"]["suggestions"].is_null());
}

#[test]
fn conversation_remove_drops_a_participant_and_blocks_their_sends() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );

    // Removing carol reports the shrunken participant set.
    let removed = run(&bus, &["conversation", "remove", "proj", "--agent", "carol", "--json"]);
    let removed_json: serde_json::Value = serde_json::from_slice(&removed.stdout).unwrap();
    assert_eq!(removed_json["ok"], true);
    assert_eq!(removed_json["removed"], true);
    let participants: Vec<&str> = removed_json["participants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    assert!(!participants.contains(&"carol"));

    // Carol can no longer send to the conversation.
    let blocked = run_fail(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "carol", "--to", "alice",
            "--subject", "x", "--body", "y", "--json",
        ],
    );
    let blocked_json: serde_json::Value = serde_json::from_slice(&blocked.stderr).unwrap();
    assert_eq!(blocked_json["error"]["code"], "not_participant");

    // Re-removing is idempotent.
    let again = run(&bus, &["conversation", "remove", "proj", "--agent", "carol", "--json"]);
    let again_json: serde_json::Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again_json["removed"], false);

    // Removing the last remaining participant is refused.
    run(&bus, &["conversation", "remove", "proj", "--agent", "bob", "--json"]);
    let last = run_fail(&bus, &["conversation", "remove", "proj", "--agent", "alice", "--json"]);
    let last_json: serde_json::Value = serde_json::from_slice(&last.stderr).unwrap();
    assert!(
        last_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("last participant")
    );
}

#[test]
fn removing_an_awaited_participant_releases_the_open_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "proj", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    // Alice asks both bob and carol for a response.
    run(
        &bus,
        &[
            "send", "--conversation", "proj", "--from", "alice", "--to", "bob,carol",
            "--subject", "Q", "--body", "please respond",
            "--needs-response-from", "bob,carol",
        ],
    );

    let owed = |out: &std::process::Output| -> Vec<String> {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["owed_to_you"]
            .as_array()
            .unwrap()
            .iter()
            .map(|ask| ask["awaited"].as_str().unwrap().to_string())
            .collect()
    };

    // Both bob and carol are owed.
    let before = run(&bus, &["awaiting", "alice", "--json"]);
    let mut before_awaited = owed(&before);
    before_awaited.sort();
    assert_eq!(before_awaited, vec!["bob".to_string(), "carol".to_string()]);

    // Removing carol releases only carol's obligation; bob's stays open.
    run(&bus, &["conversation", "remove", "proj", "--agent", "carol", "--json"]);
    let after = run(&bus, &["awaiting", "alice", "--json"]);
    assert_eq!(owed(&after), vec!["bob".to_string()]);

    // Removing bob too leaves no open ask, so `wait --owed` resolves instead of
    // blocking forever on a reply that can never come.
    run(&bus, &["conversation", "remove", "proj", "--agent", "bob", "--json"]);
    let drained = run(&bus, &["awaiting", "alice", "--json"]);
    assert!(owed(&drained).is_empty());
    let waited = run(
        &bus,
        &["wait", "alice", "--owed", "--timeout", "2", "--json"],
    );
    let waited_json: serde_json::Value = serde_json::from_slice(&waited.stdout).unwrap();
    assert_eq!(waited_json["ok"], true);
    assert!(waited_json["resolved"].is_null());
}

#[test]
fn rejoining_a_channel_preserves_an_open_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &["channel", "create", "room", "--creator", "alice", "--members", "bob"],
    );
    run(
        &bus,
        &[
            "send", "--channel", "room", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "please respond", "--needs-response-from", "bob",
        ],
    );

    let owed = |out: &std::process::Output| -> Vec<String> {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["owed_to_you"]
            .as_array()
            .unwrap()
            .iter()
            .map(|ask| ask["awaited"].as_str().unwrap().to_string())
            .collect()
    };

    assert_eq!(owed(&run(&bus, &["awaiting", "alice", "--json"])), vec!["bob".to_string()]);

    // Bob reconnects: leave then rejoin must NOT clobber the join baseline and
    // silently discharge the still-unanswered ask.
    run(&bus, &["channel", "leave", "room", "--agent", "bob", "--json"]);
    run(&bus, &["channel", "join", "room", "--agent", "bob", "--json"]);

    assert_eq!(
        owed(&run(&bus, &["awaiting", "alice", "--json"])),
        vec!["bob".to_string()],
        "alice's open ask must survive bob's leave/rejoin"
    );
    let bob_view = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_json: serde_json::Value = serde_json::from_slice(&bob_view.stdout).unwrap();
    assert_eq!(
        bob_json["you_owe"].as_array().unwrap().len(),
        1,
        "bob must still see the ack he owes after rejoining"
    );
}

#[test]
fn channel_leave_unsubscribes_an_agent() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &["channel", "create", "room", "--creator", "alice", "--members", "bob"],
    );

    let left = run(&bus, &["channel", "leave", "room", "--agent", "bob", "--json"]);
    let left_json: serde_json::Value = serde_json::from_slice(&left.stdout).unwrap();
    assert_eq!(left_json["left"], true);
    let members: Vec<&str> = left_json["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m.as_str().unwrap())
        .collect();
    assert_eq!(members, vec!["alice"]);

    // channel list reflects the smaller membership.
    let list = run(&bus, &["channel", "list", "--json"]);
    let list_json: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    let room = list_json
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == "room")
        .unwrap();
    assert_eq!(room["member_count"], 1);

    // Leaving a conversation via `channel leave` is rejected.
    run(
        &bus,
        &[
            "conversation", "create", "side", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let wrong = run_fail(&bus, &["channel", "leave", "side", "--agent", "bob", "--json"]);
    let wrong_json: serde_json::Value = serde_json::from_slice(&wrong.stderr).unwrap();
    assert!(
        wrong_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("conversation remove")
    );
}

#[test]
fn inbox_json_carries_viewer_relative_action_signals() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "sync", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "sync", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "please ack", "--requires-ack", "--json",
        ],
    );
    let mid = serde_json::from_slice::<serde_json::Value>(&sent.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Bob has an unread message that awaits his ack.
    let inbox = run(&bus, &["inbox", "bob", "--json"]);
    let rows: serde_json::Value = serde_json::from_slice(&inbox.stdout).unwrap();
    let msg = &rows.as_array().unwrap()[0];
    assert_eq!(msg["unread"], true);
    assert_eq!(msg["awaiting_me"], true);
    assert_eq!(msg["my_status"], serde_json::Value::Null);
    // The raw Message fields are still flattened in alongside the derived ones.
    assert_eq!(msg["id"].as_str().unwrap(), mid);
    assert_eq!(msg["requires_ack"], true);

    // --needs-action surfaces it.
    let needs = run(&bus, &["inbox", "bob", "--needs-action", "--json"]);
    let needs_rows: serde_json::Value = serde_json::from_slice(&needs.stdout).unwrap();
    assert_eq!(needs_rows.as_array().unwrap().len(), 1);

    // A non-terminal ack leaves the ask open: read, but still awaiting_me.
    run(&bus, &["ack", "bob", &mid, "--status", "working"]);
    let inbox = run(&bus, &["inbox", "bob", "--json"]);
    let msg = serde_json::from_slice::<serde_json::Value>(&inbox.stdout).unwrap()
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(msg["unread"], false);
    assert_eq!(msg["awaiting_me"], true);
    assert_eq!(msg["my_status"], "working");

    // A terminal ack closes the ask: awaiting_me clears.
    run(&bus, &["ack", "bob", &mid, "--status", "done"]);
    let inbox = run(&bus, &["inbox", "bob", "--json"]);
    let msg = serde_json::from_slice::<serde_json::Value>(&inbox.stdout).unwrap()
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(msg["awaiting_me"], false);
    assert_eq!(msg["my_status"], "done");

    // Nothing left to act on.
    let needs = run(&bus, &["inbox", "bob", "--needs-action", "--json"]);
    let needs_rows: serde_json::Value = serde_json::from_slice(&needs.stdout).unwrap();
    assert!(needs_rows.as_array().unwrap().is_empty());

    // The sender never awaits their own message and has no status on it.
    let inbox = run(&bus, &["inbox", "alice", "--json"]);
    let msg = serde_json::from_slice::<serde_json::Value>(&inbox.stdout).unwrap()
        .as_array()
        .unwrap()[0]
        .clone();
    assert_eq!(msg["awaiting_me"], false);
    assert_eq!(msg["my_status"], serde_json::Value::Null);
}

#[test]
fn wait_owed_blocks_until_an_owed_ask_closes() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ack pls", "--requires-ack", "--json",
        ],
    );
    let mid = serde_json::from_slice::<serde_json::Value>(&sent.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // bob records a terminal ack shortly after alice starts blocking.
    let bus_thread = bus.clone();
    let mid_thread = mid.clone();
    let acker = thread::spawn(move || {
        thread::sleep(Duration::from_millis(300));
        run(
            &bus_thread,
            &["ack", "bob", &mid_thread, "--status", "done", "--note", "ok"],
        );
    });

    let owed = run(&bus, &["wait", "alice", "--owed", "--timeout", "10", "--json"]);
    acker.join().unwrap();
    let owed_json: serde_json::Value = serde_json::from_slice(&owed.stdout).unwrap();
    assert_eq!(owed_json["ok"], true);
    assert_eq!(owed_json["resolved"]["message_id"], mid);
    assert_eq!(owed_json["resolved"]["awaited"], "bob");
    assert_eq!(owed_json["resolved"]["status"], "done");
    assert_eq!(owed_json["resolved"]["note"], "ok");

    // --resolved on the now-closed ask reports it immediately.
    let resolved = run(&bus, &["wait", "alice", "--resolved", &mid, "--json"]);
    let resolved_json: serde_json::Value = serde_json::from_slice(&resolved.stdout).unwrap();
    assert_eq!(resolved_json["resolved"]["status"], "done");

    // With nothing else open, --owed returns null without blocking.
    let none = run(&bus, &["wait", "alice", "--owed", "--timeout", "2", "--json"]);
    let none_json: serde_json::Value = serde_json::from_slice(&none.stdout).unwrap();
    assert_eq!(none_json["resolved"], serde_json::Value::Null);
}

#[test]
fn wait_resolved_blocks_until_every_recipient_answers() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob,carol",
            "--subject", "Q", "--body", "both please", "--needs-response-from", "bob,carol",
            "--json",
        ],
    );
    let mid = serde_json::from_slice::<serde_json::Value>(&sent.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Only bob has answered. `--resolved <id>` must NOT report the ask done
    // while carol still owes a reply; it blocks and then times out.
    run(&bus, &["ack", "bob", &mid, "--status", "done", "--note", "bob ok"]);
    let partial = run_fail(&bus, &["wait", "alice", "--resolved", &mid, "--timeout", "1", "--json"]);
    let partial_json: serde_json::Value = serde_json::from_slice(&partial.stderr).unwrap();
    assert_eq!(
        partial_json["error"]["code"], "timeout",
        "wait --resolved must keep blocking until every awaited agent is terminal"
    );

    // Once carol also answers, the ask resolves.
    run(&bus, &["ack", "carol", &mid, "--status", "done", "--note", "carol ok"]);
    let done = run(&bus, &["wait", "alice", "--resolved", &mid, "--timeout", "2", "--json"]);
    let done_json: serde_json::Value = serde_json::from_slice(&done.stdout).unwrap();
    assert_eq!(done_json["ok"], true);
    assert_eq!(done_json["resolved"]["status"], "done");
}

#[test]
fn wait_resolved_on_a_closed_ask_surfaces_a_rejection() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob,carol",
            "--subject", "Q", "--body", "both please", "--needs-response-from", "bob,carol",
            "--json",
        ],
    );
    let mid = serde_json::from_slice::<serde_json::Value>(&sent.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Both recipients answer terminally BEFORE the asker calls wait, so the ask
    // is already closed. bob accepts but carol rejects — the aggregate must be
    // `rejected`, not the alphabetically-first `done`. (bob sorts before carol.)
    run(&bus, &["ack", "bob", &mid, "--status", "done", "--note", "bob ok"]);
    run(&bus, &["ack", "carol", &mid, "--status", "rejected", "--note", "carol no"]);

    let resolved = run(&bus, &["wait", "alice", "--resolved", &mid, "--timeout", "2", "--json"]);
    let resolved_json: serde_json::Value = serde_json::from_slice(&resolved.stdout).unwrap();
    assert_eq!(resolved_json["ok"], true);
    assert_eq!(
        resolved_json["resolved"]["status"], "rejected",
        "a rejection by any recipient must not be hidden behind an earlier-sorting done"
    );
}

#[test]
fn wait_resolution_rejects_unknown_and_unowned_asks() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ack pls", "--requires-ack", "--json",
        ],
    );
    let mid = serde_json::from_slice::<serde_json::Value>(&sent.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // An unknown message id is not found.
    let unknown = run_fail(&bus, &["wait", "alice", "--resolved", "m-nope-000000", "--json"]);
    let unknown_json: serde_json::Value = serde_json::from_slice(&unknown.stderr).unwrap();
    assert_eq!(unknown_json["error"]["code"], "not_found");

    // bob does not own this ask (alice sent it), so bob cannot wait on it.
    let unowned = run_fail(&bus, &["wait", "bob", "--resolved", &mid, "--json"]);
    let unowned_json: serde_json::Value = serde_json::from_slice(&unowned.stderr).unwrap();
    assert_eq!(unowned_json["error"]["code"], "not_found");

    // An open ask that never closes times out with exit code 2.
    let timed_out = Command::new(bin())
        .arg("--root")
        .arg(&bus)
        .args(["wait", "alice", "--owed", "--timeout", "1", "--json"])
        .output()
        .unwrap();
    assert_eq!(timed_out.status.code(), Some(2));
    let timeout_json: serde_json::Value = serde_json::from_slice(&timed_out.stderr).unwrap();
    assert_eq!(timeout_json["error"]["code"], "timeout");
}

#[test]
fn ack_reports_whether_it_closed_an_open_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let ask = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ack pls", "--requires-ack", "--json",
        ],
    );
    let ask_id = serde_json::from_slice::<serde_json::Value>(&ask.stdout).unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    let plain = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "FYI", "--body", "info", "--json",
        ],
    );
    let plain_id = serde_json::from_slice::<serde_json::Value>(&plain.stdout).unwrap()
        ["message_id"]
        .as_str()
        .unwrap()
        .to_string();

    // A terminal ack from the awaited agent closes the ask.
    let closed = run(&bus, &["ack", "bob", &ask_id, "--status", "done", "--json"]);
    let closed_json: serde_json::Value = serde_json::from_slice(&closed.stdout).unwrap();
    assert_eq!(closed_json["was_awaited"], true);
    assert_eq!(closed_json["closed_ask"], true);

    // Re-acking is idempotent: still succeeds, but closes nothing new.
    let again = run(&bus, &["ack", "bob", &ask_id, "--status", "done", "--json"]);
    let again_json: serde_json::Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again_json["closed_ask"], false);

    // Acking a plain (non-ask) message still works, but closes no ask.
    let plain_ack = run(&bus, &["ack", "bob", &plain_id, "--status", "done", "--json"]);
    let plain_ack_json: serde_json::Value = serde_json::from_slice(&plain_ack.stdout).unwrap();
    assert_eq!(plain_ack_json["was_awaited"], false);
    assert_eq!(plain_ack_json["closed_ask"], false);

    // --require-open rejects a terminal ack that closes nothing.
    let rejected = run_fail(
        &bus,
        &["ack", "bob", &plain_id, "--status", "done", "--require-open", "--json"],
    );
    let rejected_json: serde_json::Value = serde_json::from_slice(&rejected.stderr).unwrap();
    assert_eq!(rejected_json["error"]["code"], "not_awaited");
    assert_eq!(rejected_json["error"]["was_awaited"], false);

    // An agent that was never awaited cannot satisfy --require-open either.
    let ask2 = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q2", "--body", "x", "--requires-ack", "--json",
        ],
    );
    let ask2_id = serde_json::from_slice::<serde_json::Value>(&ask2.stdout).unwrap()
        ["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    // alice (the sender) is not in the awaited set for her own ask.
    let self_ack = run_fail(
        &bus,
        &["ack", "alice", &ask2_id, "--status", "done", "--require-open", "--json"],
    );
    let self_json: serde_json::Value = serde_json::from_slice(&self_ack.stderr).unwrap();
    assert_eq!(self_json["error"]["code"], "not_awaited");
}

#[test]
fn search_filters_by_from_kind_and_mentions() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "deploy", "--body", "ship it @carol", "--requires-ack",
        ],
    );
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "bob", "--to", "alice",
            "--subject", "status", "--body", "deploy is green",
        ],
    );

    let ids = |out: &std::process::Output| -> Vec<String> {
        serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout)
            .unwrap()
            .into_iter()
            .map(|m| m["from"].as_str().unwrap().to_string())
            .collect()
    };

    // --from filters to a single sender.
    let from_bob = run(&bus, &["search", "--agent", "alice", "--from", "bob", "--json"]);
    assert_eq!(ids(&from_bob), vec!["bob".to_string()]);

    // --mentions matches the @carol mention as well as the to[] recipient.
    let mentions_carol =
        run(&bus, &["search", "--agent", "alice", "--mentions", "carol", "--json"]);
    assert_eq!(ids(&mentions_carol), vec!["alice".to_string()]);
    let mentions_bob =
        run(&bus, &["search", "--agent", "alice", "--mentions", "bob", "--json"]);
    assert_eq!(ids(&mentions_bob), vec!["alice".to_string()]);

    // Pattern + filter combine conjunctively.
    let combo = run(
        &bus,
        &["search", "deploy", "--agent", "alice", "--from", "alice", "--json"],
    );
    assert_eq!(ids(&combo), vec!["alice".to_string()]);
    let no_combo = run(
        &bus,
        &["search", "deploy", "--agent", "alice", "--from", "carol", "--json"],
    );
    assert!(ids(&no_combo).is_empty());

    // --kind selects message-kind rows.
    let kind_message =
        run(&bus, &["search", "--agent", "alice", "--kind", "message", "--json"]);
    assert_eq!(ids(&kind_message).len(), 2);

    // No criteria at all is rejected.
    let no_criteria = run_fail(&bus, &["search", "--agent", "alice", "--json"]);
    let err: serde_json::Value = serde_json::from_slice(&no_criteria.stderr).unwrap();
    assert_eq!(err["error"]["code"], "error");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--from/--kind/--mentions")
    );
}

#[test]
fn search_mentions_matches_wildcard_broadcasts_to_room_members() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    // Alice broadcasts to the whole room with `*`; bob is never named literally.
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "*",
            "--subject", "all-hands", "--body", "standup in 5",
        ],
    );
    let broadcast_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    let ids = |out: &std::process::Output| -> Vec<String> {
        serde_json::from_slice::<Vec<serde_json::Value>>(&out.stdout)
            .unwrap()
            .into_iter()
            .map(|m| m["id"].as_str().unwrap().to_string())
            .collect()
    };

    // `--mentions bob` surfaces the broadcast even though bob is only reached
    // via `*`, because bob is a participant of the room.
    let for_bob = run(&bus, &["search", "--agent", "alice", "--mentions", "bob", "--json"]);
    assert_eq!(ids(&for_bob), vec![broadcast_id.clone()]);

    // A non-member is not surfaced by the wildcard expansion.
    let for_ghost =
        run(&bus, &["search", "--agent", "alice", "--mentions", "ghost", "--json"]);
    assert!(ids(&for_ghost).is_empty());
}

#[test]
fn open_asks_report_whether_the_awaited_agent_is_live() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ship it", "--requires-ack",
        ],
    );

    let awaited_live = |out: &std::process::Output| -> bool {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["owed_to_you"][0]["awaited_live"].as_bool().unwrap()
    };

    // Bob just claimed, so the ask alice is owed shows a live delegate.
    let fresh = run(&bus, &["awaiting", "alice", "--json"]);
    assert!(awaited_live(&fresh), "freshly-claimed bob should be live");

    // Expire bob's heartbeat: now the same ask reports a dead delegate, the
    // signal an asker needs to decide whether to re-route or escalate.
    let bob = bus.join("agents").join("bob.json");
    let mut agent: serde_json::Value =
        serde_json::from_slice(&fs::read(&bob).unwrap()).unwrap();
    agent["expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    fs::write(&bob, serde_json::to_vec(&agent).unwrap()).unwrap();

    let stale = run(&bus, &["awaiting", "alice", "--json"]);
    assert!(!awaited_live(&stale), "expired bob should read as offline");

    // Text mode flags the offline delegate inline.
    let text = run(&bus, &["awaiting", "alice"]);
    assert!(
        String::from_utf8_lossy(&text.stdout).contains("@bob (offline)"),
        "text output should mark the offline delegate"
    );
}

#[test]
fn send_envelope_flags_offline_recipients() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );

    // Expire bob's heartbeat so he reads as offline; carol stays live.
    let bob = bus.join("agents").join("bob.json");
    let mut agent: serde_json::Value =
        serde_json::from_slice(&fs::read(&bob).unwrap()).unwrap();
    agent["expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    fs::write(&bob, serde_json::to_vec(&agent).unwrap()).unwrap();

    let offline = |out: &std::process::Output| -> Vec<String> {
        let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        v["offline_recipients"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect()
    };

    // A send to a downed peer surfaces it so the sender can reroute now,
    // instead of discovering the silence later via a blocked `wait`.
    let mixed = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob,carol",
            "--subject", "Q", "--body", "ship it", "--requires-ack", "--json",
        ],
    );
    assert_eq!(offline(&mixed), vec!["bob".to_string()]);

    // All-live recipients yield an empty list, never a missing field.
    let live_only = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "carol",
            "--subject", "hi", "--body", "yo", "--json",
        ],
    );
    assert!(offline(&live_only).is_empty());

    // A `*` broadcast expands to participants (minus the sender) and still
    // reports the offline member.
    let broadcast = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "*",
            "--subject", "all", "--body", "hey", "--json",
        ],
    );
    assert_eq!(offline(&broadcast), vec!["bob".to_string()]);

    // Text mode warns on stderr without polluting the stdout id.
    let text = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "t", "--body", "x",
        ],
    );
    assert!(
        String::from_utf8_lossy(&text.stderr).contains("offline recipient(s): @bob"),
        "text mode should warn about the offline recipient on stderr"
    );
    assert!(
        !String::from_utf8_lossy(&text.stdout).contains("offline"),
        "stdout should carry only the message id"
    );
}

#[test]
fn rate_limit_and_size_errors_carry_recovery_details() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice", "--rate-max", "2", "--rate-window", "60",
            "--max-message-bytes", "10",
        ],
    );

    // An over-size message reports the exact size and limit so a caller can
    // trim and retry without guessing the bound.
    let too_large = run_fail(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "x", "--body", "this body is way too long", "--json",
        ],
    );
    let big: serde_json::Value = serde_json::from_slice(&too_large.stderr).unwrap();
    assert_eq!(big["error"]["code"], "too_large");
    assert_eq!(big["error"]["size"], 25);
    assert_eq!(big["error"]["limit"], 10);

    // Exhaust the window, then the third small send is rate-limited and carries
    // a retry_after_seconds an agent can back off on instead of busy-retrying.
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "a", "--body", "hi", "--json",
        ],
    );
    run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "b", "--body", "yo", "--json",
        ],
    );
    let limited = run_fail(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "c", "--body", "no", "--json",
        ],
    );
    let err: serde_json::Value = serde_json::from_slice(&limited.stderr).unwrap();
    assert_eq!(err["error"]["code"], "rate_limited");
    assert_eq!(err["error"]["max_messages_per_sender"], 2);
    assert_eq!(err["error"]["window_seconds"], 60);
    assert_eq!(err["error"]["count"], 2);
    let retry = err["error"]["retry_after_seconds"].as_i64().unwrap();
    assert!(
        (0..=60).contains(&retry),
        "retry_after_seconds should fall within the window, got {retry}"
    );
}

#[test]
fn help_lists_every_valid_enumeration_value() {
    // An agent reading --help should discover every legal string for the
    // enumerated arguments without trial-and-error or reading the source.
    let help_of = |args: &[&str]| -> String {
        let out = Command::new(bin()).args(args).output().unwrap();
        assert!(out.status.success(), "{args:?} --help should exit 0");
        String::from_utf8(out.stdout).unwrap()
    };

    // Agent states: `away` was previously omitted from the help.
    let state_help = help_of(&["state", "set", "--help"]);
    for value in ["idle", "working", "blocked", "away"] {
        assert!(
            state_help.contains(value),
            "state set --help should list the {value:?} state"
        );
    }

    // Ack statuses: the summary previously truncated the set with "...".
    let ack_help = help_of(&["ack", "--help"]);
    for value in ["received", "accepted", "working", "blocked", "done", "rejected"] {
        assert!(
            ack_help.contains(value),
            "ack --help should list the {value:?} status"
        );
    }

    // Send kinds: `receipt` was previously hidden behind a "...".
    let send_help = help_of(&["send", "--help"]);
    for value in ["message", "event", "receipt"] {
        assert!(
            send_help.contains(value),
            "send --help should list the {value:?} kind"
        );
    }
}

#[test]
fn withdraw_closes_an_open_ask_the_sender_no_longer_needs() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ship it", "--needs-response-from", "bob",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // Pre-state: bob owes a response and alice is owed one.
    let bob_before = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_before_json: serde_json::Value = serde_json::from_slice(&bob_before.stdout).unwrap();
    assert_eq!(bob_before_json["you_owe"].as_array().unwrap().len(), 1);

    // A non-sender cannot withdraw someone else's ask: it isn't theirs.
    let denied = run_fail(&bus, &["withdraw", &message_id, "--from", "bob", "--json"]);
    let denied_json: serde_json::Value = serde_json::from_slice(&denied.stderr).unwrap();
    assert_eq!(denied_json["error"]["code"], "not_found");

    // The sender withdraws; the response set it releases names bob.
    let withdrawn = run(
        &bus,
        &["withdraw", &message_id, "--from", "alice", "--reason", "moot", "--json"],
    );
    let withdrawn_json: serde_json::Value = serde_json::from_slice(&withdrawn.stdout).unwrap();
    assert_eq!(withdrawn_json["ok"], true);
    assert_eq!(withdrawn_json["withdrawn"], true);
    assert_eq!(withdrawn_json["already_withdrawn"], false);
    assert_eq!(
        withdrawn_json["released"].as_array().unwrap(),
        &vec![serde_json::json!("bob")]
    );

    // The ask drops out of both sides: bob no longer owes, alice is no longer owed.
    let bob_after = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_after_json: serde_json::Value = serde_json::from_slice(&bob_after.stdout).unwrap();
    assert!(bob_after_json["you_owe"].as_array().unwrap().is_empty());
    let alice_after = run(&bus, &["awaiting", "alice", "--json"]);
    let alice_after_json: serde_json::Value = serde_json::from_slice(&alice_after.stdout).unwrap();
    assert!(alice_after_json["owed_to_you"].as_array().unwrap().is_empty());

    // The released worker gets a discoverable lifecycle notice naming the ask
    // and carrying the reason, so the silent disappearance from `you_owe` has an
    // explanation. Like other system notices it is not a new unread item or an
    // open ask for bob.
    let bob_view = run(&bus, &["show", "--agent", "bob", "--conversation", "c", "--json"]);
    let bob_view_json: serde_json::Value = serde_json::from_slice(&bob_view.stdout).unwrap();
    let notice = bob_view_json
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["subject"] == "ask withdrawn")
        .expect("released worker should see an `ask withdrawn` notice");
    assert_eq!(notice["kind"], "system");
    assert_eq!(notice["from"], "raft");
    assert_eq!(notice["unread"], false);
    assert_eq!(notice["awaiting_me"], false);
    let body = notice["body"].as_str().unwrap();
    assert!(body.contains(&message_id), "notice should name the ask id");
    assert!(body.contains("moot"), "notice should carry the reason");

    // Withdrawing again is an idempotent no-op success.
    let again = run(&bus, &["withdraw", &message_id, "--from", "alice", "--json"]);
    let again_json: serde_json::Value = serde_json::from_slice(&again.stdout).unwrap();
    assert_eq!(again_json["ok"], true);
    assert_eq!(again_json["already_withdrawn"], true);
    assert!(again_json["released"].as_array().unwrap().is_empty());
}

#[test]
fn withdraw_excludes_recipients_who_already_responded() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob,carol",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob,carol",
            "--subject", "Q", "--body", "both please", "--needs-response-from", "bob,carol",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // bob finishes and reports done; only carol still owes a reply.
    run(&bus, &["ack", "bob", &message_id, "--status", "done", "--note", "ok"]);

    // Withdrawing must release only the still-open obligation (carol), not bob,
    // who already discharged the ask.
    let withdrawn = run(
        &bus,
        &["withdraw", &message_id, "--from", "alice", "--reason", "moot", "--json"],
    );
    let withdrawn_json: serde_json::Value = serde_json::from_slice(&withdrawn.stdout).unwrap();
    assert_eq!(
        withdrawn_json["released"].as_array().unwrap(),
        &vec![serde_json::json!("carol")],
        "withdraw must not release a recipient who already responded"
    );

    // bob, who already finished, must NOT get an `ask withdrawn` notice telling
    // him to stop work he already completed.
    let bob_view = run(&bus, &["show", "--agent", "bob", "--conversation", "c", "--json"]);
    let bob_view_json: serde_json::Value = serde_json::from_slice(&bob_view.stdout).unwrap();
    assert!(
        bob_view_json
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["subject"] != "ask withdrawn"),
        "a recipient who already responded must not receive a withdrawal notice"
    );

    // carol, the genuinely-released worker, does get the notice.
    let carol_view = run(&bus, &["show", "--agent", "carol", "--conversation", "c", "--json"]);
    let carol_view_json: serde_json::Value = serde_json::from_slice(&carol_view.stdout).unwrap();
    assert!(
        carol_view_json
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["subject"] == "ask withdrawn"),
        "the released worker should still see the withdrawal notice"
    );
}

#[test]
fn withdraw_rejects_a_message_that_is_not_an_open_ask() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    // A plain message with no ack expectation is not an open ask.
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "FYI", "--body", "no reply needed",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    let denied = run_fail(&bus, &["withdraw", &message_id, "--from", "alice", "--json"]);
    let denied_json: serde_json::Value = serde_json::from_slice(&denied.stderr).unwrap();
    assert_eq!(denied_json["error"]["code"], "not_found");
}

#[test]
fn ack_of_a_withdrawn_ask_is_distinguishable_from_never_awaited() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "c", "--participants", "alice,bob",
            "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "Q", "--body", "ship it", "--needs-response-from", "bob",
        ],
    );
    let message_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();
    run(
        &bus,
        &["withdraw", &message_id, "--from", "alice", "--reason", "moot", "--json"],
    );

    // Bob raced the withdrawal and acks anyway. The envelope must carry the
    // withdrawal so bob can tell "too late, withdrawn" (and why) from a message
    // it was never on the hook for.
    let acked = run(
        &bus,
        &["ack", "bob", &message_id, "--status", "done", "--json"],
    );
    let acked_json: serde_json::Value = serde_json::from_slice(&acked.stdout).unwrap();
    assert_eq!(acked_json["was_awaited"], false);
    assert_eq!(acked_json["closed_ask"], false);
    assert_eq!(acked_json["withdrawn"]["by"], "alice");
    assert_eq!(acked_json["withdrawn"]["reason"], "moot");

    // Text mode flags it inline.
    let text = run(&bus, &["ack", "bob", &message_id, "--status", "done"]);
    assert!(
        String::from_utf8_lossy(&text.stdout).contains("(ask withdrawn)"),
        "text ack should mark the withdrawn ask"
    );

    // `--require-open` still fails (no open ask), but the error details now name
    // the withdrawal rather than looking identical to never-awaited.
    let strict = run_fail(
        &bus,
        &["ack", "bob", &message_id, "--status", "done", "--require-open", "--json"],
    );
    let strict_json: serde_json::Value = serde_json::from_slice(&strict.stderr).unwrap();
    assert_eq!(strict_json["error"]["code"], "not_awaited");
    assert_eq!(strict_json["error"]["withdrawn"]["by"], "alice");

    // A message bob was genuinely never awaited on carries no withdrawal,
    // distinguishing the two cases.
    let fyi = run(
        &bus,
        &[
            "send", "--conversation", "c", "--from", "alice", "--to", "bob",
            "--subject", "FYI", "--body", "no reply needed",
        ],
    );
    let fyi_id = String::from_utf8_lossy(&fyi.stdout).trim().to_string();
    let fyi_ack = run(&bus, &["ack", "bob", &fyi_id, "--status", "done", "--json"]);
    let fyi_json: serde_json::Value = serde_json::from_slice(&fyi_ack.stdout).unwrap();
    assert_eq!(fyi_json["was_awaited"], false);
    assert!(fyi_json["withdrawn"].is_null());
}

#[test]
fn me_reports_the_agents_own_heartbeat_liveness() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);

    // Fresh claim: alice's own heartbeat is live.
    let fresh: serde_json::Value =
        serde_json::from_slice(&run(&bus, &["me", "alice", "--json"]).stdout).unwrap();
    assert_eq!(fresh["live"], true);
    assert!(fresh["expires_at"].is_string());

    // Expire alice's heartbeat: liveness is computed for the agent itself, not
    // just peers, so `me` must now report the agent as stale.
    let alice = bus.join("agents").join("alice.json");
    let mut agent: serde_json::Value =
        serde_json::from_slice(&fs::read(&alice).unwrap()).unwrap();
    agent["expires_at"] = serde_json::json!("2000-01-01T00:00:00Z");
    fs::write(&alice, serde_json::to_vec(&agent).unwrap()).unwrap();

    let stale: serde_json::Value =
        serde_json::from_slice(&run(&bus, &["me", "alice", "--json"]).stdout).unwrap();
    assert_eq!(stale["live"], false);

    // Text mode surfaces an actionable banner pointing at `heartbeat`.
    let text = run(&bus, &["me", "alice"]);
    let out = String::from_utf8_lossy(&text.stdout);
    assert!(out.contains("STALE"), "text `me` should flag a stale self");
    assert!(
        out.contains("raft heartbeat alice"),
        "text `me` should tell the agent how to recover"
    );
}

#[test]
fn not_claimed_errors_suggest_nearest_agent_ids() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);

    // A mistyped id gets a recovery path, mirroring the conversation-not-found
    // suggestions, rather than a bare "not claimed" string.
    let typo = run_fail(&bus, &["me", "alise", "--json"]);
    let err: serde_json::Value = serde_json::from_slice(&typo.stderr).unwrap();
    assert_eq!(err["error"]["code"], "not_claimed");
    let suggestions = err["error"]["suggestions"].as_array().unwrap();
    assert!(
        suggestions.iter().any(|s| s == "alice"),
        "expected `alice` among suggestions, got {suggestions:?}"
    );

    // A wholly unrelated id has no near match, so no suggestions key is forced.
    let miss = run_fail(&bus, &["state", "get", "zzzzzzzz", "--json"]);
    let miss_err: serde_json::Value = serde_json::from_slice(&miss.stderr).unwrap();
    assert_eq!(miss_err["error"]["code"], "not_claimed");
    assert!(miss_err["error"].get("suggestions").is_none());
}

#[test]
fn wait_fails_fast_for_an_unclaimed_agent() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);

    // An unclaimed waiter would otherwise block for the full --timeout and exit
    // 2 (timeout). A generous timeout proves the failure is immediate: if the
    // check regressed the test would observe `timeout`, not `not_claimed`.
    let unread = run_fail(&bus, &["wait", "alise", "--timeout", "30", "--json"]);
    let unread_err: serde_json::Value = serde_json::from_slice(&unread.stderr).unwrap();
    assert_eq!(unread_err["error"]["code"], "not_claimed");
    assert!(
        unread_err["error"]["suggestions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s == "alice"),
        "fast-fail should still carry id suggestions"
    );

    // The same guard covers the resolution path (`--owed`/`--resolved`).
    let owed = run_fail(&bus, &["wait", "alise", "--owed", "--timeout", "30", "--json"]);
    let owed_err: serde_json::Value = serde_json::from_slice(&owed.stderr).unwrap();
    assert_eq!(owed_err["error"]["code"], "not_claimed");
}

#[test]
fn bare_reply_reports_thread_participants_it_did_not_reach() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "trio", "--participants", "alice,bob,carol", "--starter",
            "alice",
        ],
    );

    // Alice opens a thread addressed to the whole group.
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "trio", "--from", "alice", "--to", "bob,carol", "--subject",
            "plan", "--body", "thoughts?", "--json",
        ],
    );
    let sent_json: serde_json::Value = serde_json::from_slice(&sent.stdout).unwrap();
    let parent_id = sent_json["message_id"].as_str().unwrap();

    // A bare reply from bob defaults its audience to the parent's sender (alice),
    // silently dropping carol. The envelope names carol so bob can re-address.
    let reply = run(
        &bus,
        &["reply", parent_id, "--from", "bob", "--body", "ack", "--json"],
    );
    let reply_json: serde_json::Value = serde_json::from_slice(&reply.stdout).unwrap();
    assert_eq!(reply_json["to"], serde_json::json!(["alice"]));
    let omitted = reply_json["omitted_recipients"].as_array().unwrap();
    assert_eq!(
        omitted,
        &vec![serde_json::json!("carol")],
        "bare reply should flag the dropped group participant"
    );

    // An explicit --to that covers the thread is a deliberate choice: no warning.
    let full = run(
        &bus,
        &[
            "reply", parent_id, "--from", "bob", "--to", "alice,carol", "--body", "ack all",
            "--json",
        ],
    );
    let full_json: serde_json::Value = serde_json::from_slice(&full.stdout).unwrap();
    assert!(
        full_json["omitted_recipients"].as_array().unwrap().is_empty(),
        "explicit --to covering the thread should omit nobody"
    );
}

#[test]
fn open_asks_label_whether_a_reply_or_a_bare_ack_is_expected() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "room", "--participants", "alice,bob", "--starter", "alice",
        ],
    );

    // A --requires-ack ask: a bare acknowledgement closes it.
    run(
        &bus,
        &[
            "send", "--conversation", "room", "--from", "alice", "--to", "bob", "--subject",
            "deploy", "--body", "ship it", "--requires-ack",
        ],
    );
    // A --needs-response-from ask: the sender wants a substantive reply.
    run(
        &bus,
        &[
            "send", "--conversation", "room", "--from", "alice", "--to", "bob", "--subject",
            "design", "--body", "thoughts?", "--needs-response-from", "bob",
        ],
    );

    let awaiting = run(&bus, &["awaiting", "bob", "--json"]);
    let json: serde_json::Value = serde_json::from_slice(&awaiting.stdout).unwrap();
    let owed = json["you_owe"].as_array().unwrap();
    assert_eq!(owed.len(), 2, "bob owes two responses");

    let kinds: std::collections::HashMap<&str, &str> = owed
        .iter()
        .map(|ask| {
            (
                ask["subject"].as_str().unwrap(),
                ask["await_kind"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(kinds["deploy"], "requires_ack");
    assert_eq!(kinds["design"], "needs_response");
}

#[test]
fn read_reports_that_an_ask_is_still_owed_after_reading() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "room", "--participants", "alice,bob", "--starter", "alice",
        ],
    );
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "room", "--from", "alice", "--to", "bob", "--subject",
            "deploy", "--body", "ship it", "--requires-ack", "--json",
        ],
    );
    let sent_json: serde_json::Value = serde_json::from_slice(&sent.stdout).unwrap();
    let message_id = sent_json["message_id"].as_str().unwrap();

    // Reading the ask records a non-terminal `read` receipt, which must NOT
    // satisfy the obligation: the view still flags the ask as owed.
    let read = run(&bus, &["read", "bob", message_id, "--json"]);
    let view: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    assert_eq!(view["my_status"], "read", "read receipt is recorded");
    assert_eq!(view["unread"], false, "the reader just read it");
    assert_eq!(
        view["awaiting_me"], true,
        "a non-terminal read must not discharge the ask"
    );
    // The raw message fields remain present (the view is a superset).
    assert_eq!(view["id"], message_id);
    assert_eq!(view["from"], "alice");

    // A terminal ack closes it; a fresh read then reports the ask as no longer owed.
    run(&bus, &["ack", "bob", message_id, "--status", "done"]);
    let after = run(&bus, &["read", "bob", message_id, "--json"]);
    let after_view: serde_json::Value = serde_json::from_slice(&after.stdout).unwrap();
    assert_eq!(after_view["awaiting_me"], false);
    assert_eq!(after_view["my_status"], "done");
}

#[test]
fn watch_emits_a_late_message_whose_id_sorts_below_the_cursor() {
    // Message ids are not monotonic across processes within a millisecond, so a
    // genuinely unread message can surface with an id lexically lower than one
    // watch already emitted. Under the default (auto-read), read receipts — not
    // the id cursor — must be the dedup, so such a message is never lost.
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(
        &bus,
        &["conversation", "create", "c", "--participants", "a,b", "--starter", "a"],
    );
    let sent = run(
        &bus,
        &["send", "--conversation", "c", "--from", "a", "--to", "b", "--body", "high id"],
    );
    let high_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // First watch emits the high-id message and advances the cursor to it.
    let first = run(&bus, &["watch", "--agent", "b", "--conversation", "c", "--once"]);
    assert!(String::from_utf8_lossy(&first.stdout).contains(&high_id));
    let state: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("watch/b.json")).unwrap()).unwrap();
    assert_eq!(state["last_event_id"], high_id);

    // Craft a still-unread message addressed to b whose id sorts BELOW the
    // cursor, as a second concurrent writer could have produced in the same ms.
    let messages_dir = bus.join("conversations/c/messages");
    let template: serde_json::Value =
        serde_json::from_slice(&fs::read(messages_dir.join(format!("{high_id}.json"))).unwrap())
            .unwrap();
    let low_id = "m-20000101T000000000-0000";
    let mut crafted = template.clone();
    crafted["id"] = serde_json::json!(low_id);
    crafted["body"] = serde_json::json!("low id but newly arrived");
    fs::write(
        messages_dir.join(format!("{low_id}.json")),
        serde_json::to_vec(&crafted).unwrap(),
    )
    .unwrap();

    // The low-id message is below the persisted cursor but unread; auto-read
    // watch must still deliver it (the old scalar-cursor logic dropped it).
    let second = run(&bus, &["watch", "--agent", "b", "--conversation", "c", "--once"]);
    let out = String::from_utf8_lossy(&second.stdout);
    assert!(
        out.contains(low_id),
        "watch must not skip an unread message whose id sorts below the cursor; got: {out:?}"
    );
    // And it does not re-emit the already-read high-id message.
    assert!(!out.contains(&high_id), "already-read message must not re-emit");
}

#[test]
fn channel_joiner_is_not_flooded_with_pre_join_broadcasts() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "alice", "--workspace", "."]);
    run(&bus, &["claim", "bob", "--workspace", "."]);
    run(&bus, &["claim", "carol", "--workspace", "."]);
    run(&bus, &["channel", "create", "general", "--creator", "alice"]);
    run(&bus, &["channel", "join", "general", "--agent", "bob"]);
    run(&bus, &["channel", "join", "general", "--agent", "carol"]);

    // A broadcast ask to every subscriber.
    let old = run(
        &bus,
        &[
            "send", "--channel", "general", "--from", "alice", "--to", "*",
            "--subject", "history", "--body", "ancient broadcast", "--requires-ack",
        ],
    );
    let old_id = String::from_utf8_lossy(&old.stdout).trim().to_string();

    // Pin the timeline on disk so membership ordering is exact rather than at
    // the mercy of whole-second wall-clock resolution: the broadcast lands
    // after bob joined but before carol did.
    let meta_path = bus.join("conversations/general/meta.json");
    let mut meta: serde_json::Value =
        serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    meta["joined_at"]["alice"] = serde_json::json!("2023-01-01T00:00:00Z");
    meta["joined_at"]["bob"] = serde_json::json!("2023-01-01T00:00:00Z");
    meta["joined_at"]["carol"] = serde_json::json!("2024-01-01T00:00:00Z");
    fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

    let old_path = bus.join(format!("conversations/general/messages/{old_id}.json"));
    let mut old_msg: serde_json::Value =
        serde_json::from_slice(&fs::read(&old_path).unwrap()).unwrap();
    old_msg["created_at"] = serde_json::json!("2023-06-01T00:00:00Z");
    fs::write(&old_path, serde_json::to_vec(&old_msg).unwrap()).unwrap();

    // The pre-join broadcast is backlog for carol: not unread, not owed.
    let carol_inbox = run(&bus, &["inbox", "carol", "--channel", "general", "--unread"]);
    assert!(
        !String::from_utf8_lossy(&carol_inbox.stdout).contains("ancient broadcast"),
        "a late joiner must not see pre-join broadcasts as unread"
    );
    let carol_owes = run(&bus, &["awaiting", "carol", "--json"]);
    let carol_owes_json: serde_json::Value = serde_json::from_slice(&carol_owes.stdout).unwrap();
    assert!(
        carol_owes_json["you_owe"].as_array().unwrap().is_empty(),
        "a late joiner must owe nothing on a pre-join ask"
    );

    // Bob, present when it was sent, still sees it as unread and owed.
    let bob_inbox = run(&bus, &["inbox", "bob", "--channel", "general", "--unread"]);
    assert!(String::from_utf8_lossy(&bob_inbox.stdout).contains("ancient broadcast"));
    let bob_owes = run(&bus, &["awaiting", "bob", "--json"]);
    let bob_owes_json: serde_json::Value = serde_json::from_slice(&bob_owes.stdout).unwrap();
    assert_eq!(bob_owes_json["you_owe"][0]["message_id"], old_id);

    // A broadcast sent now (after carol's 2024 join) does reach her.
    let fresh = run(
        &bus,
        &[
            "send", "--channel", "general", "--from", "alice", "--to", "*",
            "--subject", "now", "--body", "fresh broadcast", "--requires-ack",
        ],
    );
    let fresh_id = String::from_utf8_lossy(&fresh.stdout).trim().to_string();
    let carol_inbox2 = run(&bus, &["inbox", "carol", "--channel", "general", "--unread"]);
    assert!(
        String::from_utf8_lossy(&carol_inbox2.stdout).contains("fresh broadcast"),
        "a post-join broadcast must reach the joiner as unread"
    );
    let carol_owes2 = run(&bus, &["awaiting", "carol", "--json"]);
    let carol_owes2_json: serde_json::Value =
        serde_json::from_slice(&carol_owes2.stdout).unwrap();
    assert_eq!(carol_owes2_json["you_owe"][0]["message_id"], fresh_id);
}

#[test]
fn an_ask_can_require_a_reply_from_one_agent_and_an_ack_from_everyone() {
    let bus = temp_bus();
    run(&bus, &["init"]);
    run(&bus, &["claim", "asker", "--workspace", "."]);
    run(&bus, &["claim", "lead", "--workspace", "."]);
    run(&bus, &["claim", "crew", "--workspace", "."]);
    run(
        &bus,
        &[
            "conversation", "create", "room", "--participants", "asker,lead,crew",
            "--starter", "asker",
        ],
    );

    // One message that names `lead` for a substantive reply AND asks everyone to
    // ack. Before unioning the two obligation sources, the non-empty
    // needs-response-from silently suppressed requires-ack, dropping crew.
    let sent = run(
        &bus,
        &[
            "send", "--conversation", "room", "--from", "asker", "--to", "*",
            "--subject", "ship", "--body", "lead drives; everyone ack",
            "--needs-response-from", "lead", "--requires-ack",
        ],
    );
    let msg_id = String::from_utf8_lossy(&sent.stdout).trim().to_string();

    // Both lead and crew are awaited, each with the right obligation kind.
    let waiting = run(&bus, &["awaiting", "asker", "--json"]);
    let waiting_json: serde_json::Value = serde_json::from_slice(&waiting.stdout).unwrap();
    let owed = waiting_json["owed_to_you"].as_array().unwrap();
    let mut by_agent: std::collections::BTreeMap<String, String> = Default::default();
    for ask in owed {
        assert_eq!(ask["message_id"], serde_json::json!(msg_id));
        by_agent.insert(
            ask["awaited"].as_str().unwrap().to_string(),
            ask["await_kind"].as_str().unwrap().to_string(),
        );
    }
    assert_eq!(by_agent.get("lead").map(String::as_str), Some("needs_response"));
    assert_eq!(by_agent.get("crew").map(String::as_str), Some("requires_ack"));

    // crew's bare ack closes only crew's obligation; lead still owes a reply, so
    // the asker is not yet fully satisfied.
    run(&bus, &["read", "crew", &msg_id]);
    run(&bus, &["ack", "crew", &msg_id, "--status", "done"]);
    let after_crew = run(&bus, &["awaiting", "asker", "--json"]);
    let after_crew_json: serde_json::Value = serde_json::from_slice(&after_crew.stdout).unwrap();
    let still_owed: Vec<&str> = after_crew_json["owed_to_you"]
        .as_array()
        .unwrap()
        .iter()
        .map(|ask| ask["awaited"].as_str().unwrap())
        .collect();
    assert_eq!(still_owed, vec!["lead"], "crew's ack must not close lead's reply");

    // lead's terminal reply finally clears the asker.
    run(&bus, &["read", "lead", &msg_id]);
    run(&bus, &["ack", "lead", &msg_id, "--status", "done"]);
    let done = run(&bus, &["awaiting", "asker", "--json"]);
    let done_json: serde_json::Value = serde_json::from_slice(&done.stdout).unwrap();
    assert!(done_json["owed_to_you"].as_array().unwrap().is_empty());
}
