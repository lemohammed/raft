use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
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

fn iso_test_after(seconds: i64) -> String {
    (Utc::now() + TimeDelta::seconds(seconds)).to_rfc3339_opts(SecondsFormat::Secs, true)
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
fn private_turn_message_ack_flow() {
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
            "--pass-to",
            "homekeep-dev",
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
    thread::sleep(Duration::from_millis(200));
    Command::new("kill")
        .arg("-TERM")
        .arg(restarted.id().to_string())
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
fn turn_is_enforced() {
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
            "b",
            "--to",
            "a",
            "--body",
            "not my turn",
        ],
    );
    assert!(String::from_utf8_lossy(&denied.stderr).contains("turn is held"));
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
            "--pass-to",
            "b",
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
fn bridge_event_bypasses_turn_and_rates_by_subject_id() {
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
    let status = run(&bus, &["status"]);
    assert!(String::from_utf8_lossy(&status.stdout).contains("turn=a"));
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
    let turn: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("conversations/c/turn.json")).unwrap()).unwrap();
    let message: serde_json::Value = serde_json::from_slice(
        &fs::read(bus.join(format!("conversations/c/messages/{message_id}.json"))).unwrap(),
    )
    .unwrap();
    let agent: serde_json::Value =
        serde_json::from_slice(&fs::read(bus.join("agents/agent-a.json")).unwrap()).unwrap();
    let journal = fs::read_to_string(bus.join("journal/agent-a.jsonl")).unwrap();
    assert_eq!(meta["_v"], 1);
    assert_eq!(turn["_v"], 1);
    assert_eq!(message["_v"], 1);
    assert_eq!(agent["_v"], 1);
    assert!(journal.contains("\"_v\":1"));
}

#[test]
fn gc_reassigns_expired_turn() {
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
            "--turn-ttl",
            "1",
        ],
    );
    let turn_path = bus.join("conversations").join("c").join("turn.json");
    let mut turn: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    turn["expires_at"] = serde_json::Value::String("2000-01-01T00:00:00Z".to_string());
    fs::write(&turn_path, serde_json::to_vec(&turn).unwrap()).unwrap();
    run(&bus, &["gc"]);
    let updated: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    assert_eq!(updated["holder"], "b");
}

#[test]
fn expired_holder_can_send_within_grace_without_handoff() {
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
            "--turn-ttl",
            "60",
        ],
    );
    let turn_path = bus.join("conversations").join("c").join("turn.json");
    let mut turn: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    turn["expires_at"] = serde_json::Value::String(iso_test_after(-1));
    fs::write(&turn_path, serde_json::to_vec(&turn).unwrap()).unwrap();

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
            "inside grace",
        ],
    );

    let updated: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    assert_eq!(updated["holder"], "a");
    assert_eq!(updated["counter"], 1);
}

#[test]
fn expired_holder_after_grace_gets_explicit_error() {
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
            "--turn-ttl",
            "60",
        ],
    );
    let turn_path = bus.join("conversations").join("c").join("turn.json");
    let mut turn: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    turn["expires_at"] = serde_json::Value::String(iso_test_after(-120));
    fs::write(&turn_path, serde_json::to_vec(&turn).unwrap()).unwrap();

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
            "after grace",
        ],
    );
    let stderr = String::from_utf8_lossy(&denied.stderr);
    assert!(stderr.contains("your turn expired"));
    assert!(stderr.contains("reassigned to b"));
    let updated: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    assert_eq!(updated["holder"], "b");
    assert_eq!(updated["counter"], 2);
}

#[test]
fn renew_turn_extends_current_holder_without_handoff() {
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
            "--turn-ttl",
            "60",
        ],
    );
    let turn_path = bus.join("conversations").join("c").join("turn.json");
    let mut turn: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    turn["expires_at"] = serde_json::Value::String(iso_test_after(5));
    fs::write(&turn_path, serde_json::to_vec(&turn).unwrap()).unwrap();
    let previous = DateTime::parse_from_rfc3339(turn["expires_at"].as_str().unwrap()).unwrap();

    run(&bus, &["renew-turn", "--conversation", "c", "--from", "a"]);

    let updated: serde_json::Value =
        serde_json::from_slice(&fs::read(&turn_path).unwrap()).unwrap();
    let renewed = DateTime::parse_from_rfc3339(updated["expires_at"].as_str().unwrap()).unwrap();
    assert_eq!(updated["holder"], "a");
    assert_eq!(updated["counter"], 1);
    assert!(renewed > previous);
    let denied = run_fail(&bus, &["renew-turn", "--conversation", "c", "--from", "b"]);
    assert!(String::from_utf8_lossy(&denied.stderr).contains("turn is held by"));
    let inbox = run(&bus, &["inbox", "b"]);
    assert!(String::from_utf8_lossy(&inbox.stdout).contains("Turn lease renewed by a."));
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
    let turn_path = bus.join("conversations/c/turn.json");
    fs::write(&turn_path, b"{not json").unwrap();

    let doctor = run_fail(&bus, &["doctor", "--json"]);
    let report: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(report["ok"], false);
    assert!(report["error_count"].as_u64().unwrap() >= 1);
    assert!(report["issues"].as_array().unwrap().iter().any(|issue| {
        issue["code"] == "invalid_json" && issue["path"] == "conversations/c/turn.json"
    }));
    assert!(String::from_utf8_lossy(&doctor.stderr).contains("doctor found"));
    assert_eq!(fs::read(&turn_path).unwrap(), b"{not json");
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
