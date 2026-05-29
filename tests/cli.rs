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
