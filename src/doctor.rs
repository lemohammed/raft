use crate::SCHEMA_VERSION;
use crate::cli::DoctorArgs;
use crate::error::Result;
use crate::receipt_recipients;
use crate::storage::{collect_orphan_temp_files, is_agent_record_file};
use crate::types::{Agent, HeartbeatState, LockOwner, Message, Meta, Receipt, WatchState};
use crate::util::{parse_time, process_is_alive, validate_agent_state, validate_id};
use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[derive(Serialize)]
struct DoctorReport {
    root: String,
    ok: bool,
    strict: bool,
    error_count: usize,
    warning_count: usize,
    counts: DoctorCounts,
    issues: Vec<DoctorIssue>,
}

impl DoctorReport {
    fn new(root: &Path, strict: bool) -> Self {
        Self {
            root: root.display().to_string(),
            ok: true,
            strict,
            error_count: 0,
            warning_count: 0,
            counts: DoctorCounts::default(),
            issues: Vec::new(),
        }
    }

    fn error(&mut self, root: &Path, path: &Path, code: &str, message: impl Into<String>) {
        self.issues.push(DoctorIssue {
            level: "error".to_string(),
            code: code.to_string(),
            path: doctor_display_path(root, path),
            message: message.into(),
        });
    }

    fn warn(&mut self, root: &Path, path: &Path, code: &str, message: impl Into<String>) {
        self.issues.push(DoctorIssue {
            level: "warning".to_string(),
            code: code.to_string(),
            path: doctor_display_path(root, path),
            message: message.into(),
        });
    }

    fn finalize(&mut self) {
        self.error_count = self
            .issues
            .iter()
            .filter(|issue| issue.level == "error")
            .count();
        self.warning_count = self
            .issues
            .iter()
            .filter(|issue| issue.level == "warning")
            .count();
        self.ok = self.error_count == 0 && (!self.strict || self.warning_count == 0);
    }
}

#[derive(Default, Serialize)]
struct DoctorCounts {
    agents: usize,
    conversations: usize,
    messages: usize,
    receipts: usize,
    locks: usize,
    heartbeat_watchers: usize,
    watch_cursors: usize,
}

#[derive(Serialize)]
struct DoctorIssue {
    level: String,
    code: String,
    path: String,
    message: String,
}

pub(crate) fn cmd_doctor(root: &Path, args: DoctorArgs) -> Result<()> {
    let report = build_doctor_report(root, args.strict);
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        if report.ok {
            println!(
                "doctor ok: errors={} warnings={} agents={} conversations={} messages={} receipts={} locks={}",
                report.error_count,
                report.warning_count,
                report.counts.agents,
                report.counts.conversations,
                report.counts.messages,
                report.counts.receipts,
                report.counts.locks
            );
        } else {
            println!(
                "doctor found issues: errors={} warnings={}",
                report.error_count, report.warning_count
            );
        }
        for issue in &report.issues {
            println!(
                "[{}] {} {}: {}",
                issue.level, issue.code, issue.path, issue.message
            );
        }
    }
    if !report.ok {
        bail!(
            "doctor found {} error(s), {} warning(s)",
            report.error_count,
            report.warning_count
        );
    }
    Ok(())
}

fn build_doctor_report(root: &Path, strict: bool) -> DoctorReport {
    let mut report = DoctorReport::new(root, strict);
    if !root.exists() {
        report.error(root, root, "missing_root", "bus root does not exist");
        report.finalize();
        return report;
    }
    if !root.is_dir() {
        report.error(root, root, "invalid_root", "bus root is not a directory");
        report.finalize();
        return report;
    }
    doctor_check_dir_mode(root, root, &mut report);
    for child in [
        "agents",
        "archive",
        "conversations",
        "heartbeat",
        "journal",
        "locks",
        "tmp",
        "watch",
    ] {
        doctor_check_expected_dir(root, &mut report, child);
    }

    let claimed_agents = doctor_scan_agents(root, &mut report);
    doctor_scan_runtime_states(root, &mut report, &claimed_agents);
    doctor_scan_locks(root, &mut report);
    doctor_scan_conversations(root, &mut report, &claimed_agents);
    doctor_scan_orphan_temp_files(root, &mut report);
    report.finalize();
    report
}

fn doctor_scan_orphan_temp_files(root: &Path, report: &mut DoctorReport) {
    let orphans = match collect_orphan_temp_files(root) {
        Ok(orphans) => orphans,
        Err(err) => {
            report.error(root, root, "tmp_scan_failed", err.to_string());
            return;
        }
    };
    for path in orphans {
        report.warn(
            root,
            &path,
            "orphan_temp_file",
            "stale atomic-write temp file; run raft gc to reap it",
        );
    }
}

fn doctor_scan_agents(root: &Path, report: &mut DoctorReport) -> BTreeSet<String> {
    let mut claimed = BTreeSet::new();
    for entry in doctor_sorted_read_dir(root, &root.join("agents"), report) {
        let path = entry.path();
        if !is_agent_record_file(&path) {
            continue;
        }
        report.counts.agents += 1;
        doctor_check_file_mode(root, &path, report);
        let Some(agent) = doctor_read_json::<Agent>(root, &path, report) else {
            continue;
        };
        doctor_check_schema(root, &path, report, agent.v, "agent");
        if let Some(stem) = doctor_file_stem(&path)
            && stem != agent.id
        {
            report.error(
                root,
                &path,
                "agent_id_mismatch",
                format!("file name is {stem:?} but payload id is {:?}", agent.id),
            );
        }
        if let Err(err) = validate_id(&agent.id, "agent id") {
            report.error(root, &path, "invalid_agent_id", err.to_string());
        }
        let expected_mention = format!("@{}", agent.id);
        if agent.mention != expected_mention {
            report.warn(
                root,
                &path,
                "agent_mention_mismatch",
                format!(
                    "mention is {:?}; expected {expected_mention:?}",
                    agent.mention
                ),
            );
        }
        doctor_check_time(root, &path, report, "last_seen_at", &agent.last_seen_at);
        doctor_check_time(root, &path, report, "expires_at", &agent.expires_at);
        doctor_check_time(
            root,
            &path,
            report,
            "state_updated_at",
            &agent.state_updated_at,
        );
        if let Err(err) = validate_agent_state(&agent.current_state) {
            report.error(root, &path, "invalid_agent_state", err.to_string());
        }
        claimed.insert(agent.id);
    }
    claimed
}

fn doctor_scan_runtime_states(
    root: &Path,
    report: &mut DoctorReport,
    claimed_agents: &BTreeSet<String>,
) {
    for entry in doctor_sorted_read_dir(root, &root.join("heartbeat"), report) {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("json")) {
            continue;
        }
        report.counts.heartbeat_watchers += 1;
        doctor_check_file_mode(root, &path, report);
        let Some(state) = doctor_read_json::<HeartbeatState>(root, &path, report) else {
            continue;
        };
        doctor_check_schema(root, &path, report, state.v, "heartbeat state");
        doctor_check_runtime_agent(
            root,
            &path,
            report,
            claimed_agents,
            &state.agent,
            state.pid,
            state.shutdown_at.as_deref(),
        );
        doctor_check_time(root, &path, report, "started_at", &state.started_at);
        doctor_check_time(root, &path, report, "updated_at", &state.updated_at);
        doctor_check_time(
            root,
            &path,
            report,
            "last_heartbeat_at",
            &state.last_heartbeat_at,
        );
    }

    for entry in doctor_sorted_read_dir(root, &root.join("watch"), report) {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("json")) {
            continue;
        }
        report.counts.watch_cursors += 1;
        doctor_check_file_mode(root, &path, report);
        let Some(state) = doctor_read_json::<WatchState>(root, &path, report) else {
            continue;
        };
        doctor_check_schema(root, &path, report, state.v, "watch state");
        doctor_check_runtime_agent(
            root,
            &path,
            report,
            claimed_agents,
            &state.agent,
            state.pid,
            state.shutdown_at.as_deref(),
        );
        doctor_check_time(root, &path, report, "started_at", &state.started_at);
        doctor_check_time(root, &path, report, "updated_at", &state.updated_at);
    }
}

fn doctor_scan_locks(root: &Path, report: &mut DoctorReport) {
    for entry in doctor_sorted_read_dir(root, &root.join("locks"), report) {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("lock")) {
            continue;
        }
        report.counts.locks += 1;
        if !path.is_dir() {
            report.error(root, &path, "invalid_lock", "lock path is not a directory");
            continue;
        }
        doctor_check_dir_mode(root, &path, report);
        let owner_path = path.join("owner.json");
        if !owner_path.exists() {
            report.warn(root, &path, "missing_lock_owner", "lock has no owner.json");
            continue;
        }
        doctor_check_file_mode(root, &owner_path, report);
        let Some(owner) = doctor_read_json::<LockOwner>(root, &owner_path, report) else {
            continue;
        };
        doctor_check_schema(root, &owner_path, report, owner.v, "lock owner");
        if owner.token.is_empty() {
            report.error(root, &owner_path, "empty_lock_token", "lock token is empty");
        }
        doctor_check_time(root, &owner_path, report, "acquired_at", &owner.acquired_at);
        match parse_time(&owner.expires_at) {
            Ok(expires_at) if expires_at < Utc::now() => {
                report.warn(root, &path, "stale_lock", "lock owner is expired")
            }
            Ok(_) => {}
            Err(_) => report.error(
                root,
                &owner_path,
                "invalid_time",
                format!("expires_at is not RFC3339: {:?}", owner.expires_at),
            ),
        }
    }
}

fn doctor_scan_conversations(
    root: &Path,
    report: &mut DoctorReport,
    claimed_agents: &BTreeSet<String>,
) {
    for entry in doctor_sorted_read_dir(root, &root.join("conversations"), report) {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        report.counts.conversations += 1;
        doctor_check_dir_mode(root, &conv, report);
        let meta_path = conv.join("meta.json");
        if !meta_path.exists() {
            report.error(root, &conv, "missing_meta", "conversation has no meta.json");
            continue;
        }
        doctor_check_file_mode(root, &meta_path, report);
        let Some(meta) = doctor_read_json::<Meta>(root, &meta_path, report) else {
            continue;
        };
        doctor_check_meta(root, report, claimed_agents, &conv, &meta);
        for child in ["messages", "receipts"] {
            let path = conv.join(child);
            if !path.exists() {
                report.error(
                    root,
                    &path,
                    "missing_conversation_dir",
                    format!("conversation is missing {child}/"),
                );
            } else if !path.is_dir() {
                report.error(
                    root,
                    &path,
                    "invalid_conversation_dir",
                    format!("{child}/ is not a directory"),
                );
            } else {
                doctor_check_dir_mode(root, &path, report);
            }
        }
        let message_ids = doctor_scan_messages(root, report, &conv, &meta);
        doctor_scan_receipts(root, report, &conv, &meta, &message_ids);
    }
}

fn doctor_check_meta(
    root: &Path,
    report: &mut DoctorReport,
    claimed_agents: &BTreeSet<String>,
    conv: &Path,
    meta: &Meta,
) {
    let meta_path = conv.join("meta.json");
    doctor_check_schema(root, &meta_path, report, meta.v, "conversation meta");
    if let Some(dir_name) = conv.file_name().and_then(OsStr::to_str)
        && dir_name != meta.id
    {
        report.error(
            root,
            &meta_path,
            "conversation_id_mismatch",
            format!("directory is {dir_name:?} but payload id is {:?}", meta.id),
        );
    }
    if let Err(err) = validate_id(&meta.id, "conversation id") {
        report.error(root, &meta_path, "invalid_conversation_id", err.to_string());
    }
    if meta.participants.is_empty() {
        report.error(
            root,
            &meta_path,
            "empty_participants",
            "conversation has no participants",
        );
    }
    let mut seen = BTreeSet::new();
    for participant in &meta.participants {
        if let Err(err) = validate_id(participant, "participant") {
            report.error(root, &meta_path, "invalid_participant", err.to_string());
        }
        if !seen.insert(participant) {
            report.error(
                root,
                &meta_path,
                "duplicate_participant",
                format!("participant {participant:?} appears more than once"),
            );
        }
        if !claimed_agents.contains(participant) {
            report.warn(
                root,
                &meta_path,
                "unclaimed_participant",
                format!("participant @{participant} has no agent claim"),
            );
        }
    }
    if meta.channel && meta.private {
        report.error(
            root,
            &meta_path,
            "invalid_privacy",
            "a channel cannot also be private",
        );
    }
    doctor_check_time(root, &meta_path, report, "created_at", &meta.created_at);
    doctor_check_time(root, &meta_path, report, "updated_at", &meta.updated_at);
    if meta.rate.window_seconds == 0 {
        report.error(
            root,
            &meta_path,
            "invalid_rate",
            "rate.window_seconds must be positive",
        );
    }
    if meta.rate.max_messages_per_sender == 0 {
        report.error(
            root,
            &meta_path,
            "invalid_rate",
            "rate.max_messages_per_sender must be positive",
        );
    }
    if meta.rate.max_message_bytes == 0 {
        report.error(
            root,
            &meta_path,
            "invalid_rate",
            "rate.max_message_bytes must be positive",
        );
    }
}

fn doctor_scan_messages(
    root: &Path,
    report: &mut DoctorReport,
    conv: &Path,
    meta: &Meta,
) -> BTreeSet<String> {
    let mut message_ids = BTreeSet::new();
    let mut messages = Vec::new();
    for entry in doctor_sorted_read_dir(root, &conv.join("messages"), report) {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("json")) {
            continue;
        }
        report.counts.messages += 1;
        doctor_check_file_mode(root, &path, report);
        let Some(message) = doctor_read_json::<Message>(root, &path, report) else {
            continue;
        };
        doctor_check_message(root, report, &path, meta, &message);
        message_ids.insert(message.id.clone());
        messages.push((path, message));
    }
    for (path, message) in messages {
        if let Some(after) = message.after.as_deref()
            && !message_ids.contains(after)
        {
            report.warn(
                root,
                &path,
                "dangling_after",
                format!("after points to missing message {after:?}"),
            );
        }
    }
    message_ids
}

fn doctor_check_message(
    root: &Path,
    report: &mut DoctorReport,
    path: &Path,
    meta: &Meta,
    message: &Message,
) {
    doctor_check_schema(root, path, report, message.v, "message");
    if let Some(stem) = doctor_file_stem(path)
        && stem != message.id
    {
        report.error(
            root,
            path,
            "message_id_mismatch",
            format!("file name is {stem:?} but payload id is {:?}", message.id),
        );
    }
    if let Err(err) = validate_id(&message.id, "message id") {
        report.error(root, path, "invalid_message_id", err.to_string());
    }
    if message.conversation_id != meta.id {
        report.error(
            root,
            path,
            "message_conversation_mismatch",
            format!(
                "message belongs to {:?}, expected {:?}",
                message.conversation_id, meta.id
            ),
        );
    }
    if !matches!(
        message.kind.as_str(),
        "message" | "event" | "receipt" | "system"
    ) {
        report.error(
            root,
            path,
            "invalid_message_kind",
            format!("unsupported kind {:?}", message.kind),
        );
    }
    if message.kind == "system" {
        if message.from != "raft" {
            report.error(
                root,
                path,
                "forged_system_message",
                "system messages must be from raft",
            );
        }
    } else if !meta
        .participants
        .iter()
        .any(|participant| participant == &message.from)
    {
        report.error(
            root,
            path,
            "sender_not_participant",
            format!(
                "sender @{sender} is not a participant",
                sender = message.from.as_str()
            ),
        );
    }
    for recipient in &message.to {
        if recipient != "*" && !meta.participants.iter().any(|item| item == recipient) {
            report.error(
                root,
                path,
                "recipient_not_participant",
                format!("recipient @{recipient} is not a participant"),
            );
        }
    }
    for mention in &message.mentions {
        if !meta.participants.iter().any(|item| item == mention) {
            report.warn(
                root,
                path,
                "mention_not_participant",
                format!("mention @{mention} is not a participant"),
            );
        }
    }
    doctor_check_time(root, path, report, "created_at", &message.created_at);
    if message.requires_ack && receipt_recipients(message, meta).is_empty() {
        report.warn(
            root,
            path,
            "ack_without_recipients",
            "message requires ack but has no recipient other than sender",
        );
    }
}

fn doctor_scan_receipts(
    root: &Path,
    report: &mut DoctorReport,
    conv: &Path,
    meta: &Meta,
    message_ids: &BTreeSet<String>,
) {
    for message_receipts in doctor_sorted_read_dir(root, &conv.join("receipts"), report) {
        let receipt_dir = message_receipts.path();
        if !receipt_dir.is_dir() {
            report.warn(
                root,
                &receipt_dir,
                "invalid_receipt_dir",
                "receipt entry is not a directory",
            );
            continue;
        }
        doctor_check_dir_mode(root, &receipt_dir, report);
        let message_id = receipt_dir
            .file_name()
            .and_then(OsStr::to_str)
            .unwrap_or_default()
            .to_string();
        if !message_ids.contains(&message_id) {
            report.warn(
                root,
                &receipt_dir,
                "orphan_receipts",
                format!("receipt directory references missing message {message_id:?}"),
            );
        }
        for entry in doctor_sorted_read_dir(root, &receipt_dir, report) {
            let path = entry.path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }
            report.counts.receipts += 1;
            doctor_check_file_mode(root, &path, report);
            let Some(receipt) = doctor_read_json::<Receipt>(root, &path, report) else {
                continue;
            };
            doctor_check_receipt(root, report, &path, meta, &message_id, &receipt);
        }
    }
}

fn doctor_check_receipt(
    root: &Path,
    report: &mut DoctorReport,
    path: &Path,
    meta: &Meta,
    message_id: &str,
    receipt: &Receipt,
) {
    doctor_check_schema(root, path, report, receipt.v, "receipt");
    if receipt.message_id != message_id {
        report.error(
            root,
            path,
            "receipt_message_mismatch",
            format!(
                "receipt is for {:?}, expected directory message {:?}",
                receipt.message_id, message_id
            ),
        );
    }
    if receipt.conversation_id != meta.id {
        report.error(
            root,
            path,
            "receipt_conversation_mismatch",
            format!(
                "receipt belongs to {:?}, expected {:?}",
                receipt.conversation_id, meta.id
            ),
        );
    }
    if let Some(stem) = doctor_file_stem(path)
        && stem != receipt.agent
    {
        report.error(
            root,
            path,
            "receipt_agent_mismatch",
            format!(
                "file name is {stem:?} but payload agent is {:?}",
                receipt.agent.as_str()
            ),
        );
    }
    if !meta
        .participants
        .iter()
        .any(|participant| participant == &receipt.agent)
    {
        report.warn(
            root,
            path,
            "receipt_agent_not_participant",
            format!(
                "receipt agent @{agent} is not a participant",
                agent = receipt.agent.as_str()
            ),
        );
    }
    if receipt.status.trim().is_empty() {
        report.error(
            root,
            path,
            "empty_receipt_status",
            "receipt status is empty",
        );
    }
    doctor_check_time(root, path, report, "updated_at", &receipt.updated_at);
    if let Some(read_at) = receipt.read_at.as_deref() {
        doctor_check_time(root, path, report, "read_at", read_at);
    }
    for event in &receipt.history {
        doctor_check_time(root, path, report, "history.at", &event.at);
    }
}

fn doctor_read_json<T: DeserializeOwned>(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
) -> Option<T> {
    match File::open(path) {
        Ok(file) => match serde_json::from_reader(file) {
            Ok(value) => Some(value),
            Err(err) => {
                report.error(
                    root,
                    path,
                    "invalid_json",
                    format!("failed to parse JSON: {err}"),
                );
                None
            }
        },
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => {
            report.error(root, path, "read_failed", err.to_string());
            None
        }
    }
}

fn doctor_check_expected_dir(root: &Path, report: &mut DoctorReport, child: &str) {
    let path = root.join(child);
    if !path.exists() {
        report.warn(
            root,
            &path,
            "missing_directory",
            format!("expected bus directory {child}/ is missing"),
        );
    } else if !path.is_dir() {
        report.error(
            root,
            &path,
            "invalid_directory",
            format!("expected {child}/ to be a directory"),
        );
    } else {
        doctor_check_dir_mode(root, &path, report);
    }
}

fn doctor_check_dir_mode(root: &Path, path: &Path, report: &mut DoctorReport) {
    doctor_check_mode(root, path, report, 0o700, "directory_mode");
}

fn doctor_check_file_mode(root: &Path, path: &Path, report: &mut DoctorReport) {
    doctor_check_mode(root, path, report, 0o600, "file_mode");
}

fn doctor_check_mode(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
    expected: u32,
    code: &str,
) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    let actual = metadata.permissions().mode() & 0o777;
    if actual != expected {
        report.warn(
            root,
            path,
            code,
            format!("mode is {actual:o}; expected {expected:o}"),
        );
    }
}

fn doctor_check_schema(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
    version: u16,
    label: &str,
) {
    if version != SCHEMA_VERSION {
        report.warn(
            root,
            path,
            "schema_version",
            format!("{label} has _v={version}; current schema is {SCHEMA_VERSION}"),
        );
    }
}

fn doctor_check_time(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
    field: &str,
    value: &str,
) {
    if parse_time(value).is_err() {
        report.error(
            root,
            path,
            "invalid_time",
            format!("{field} is not RFC3339: {value:?}"),
        );
    }
}

fn doctor_check_runtime_agent(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
    claimed_agents: &BTreeSet<String>,
    agent_id: &str,
    pid: u32,
    shutdown_at: Option<&str>,
) {
    if !claimed_agents.contains(agent_id) {
        report.warn(
            root,
            path,
            "runtime_unclaimed_agent",
            format!("runtime state references unclaimed agent @{agent_id}"),
        );
    }
    if shutdown_at.is_none() && !process_is_alive(pid) {
        report.warn(
            root,
            path,
            "stale_runtime_state",
            format!("pid {pid} is no longer running"),
        );
    }
}

fn doctor_sorted_read_dir(
    root: &Path,
    path: &Path,
    report: &mut DoctorReport,
) -> Vec<fs::DirEntry> {
    let mut entries = match fs::read_dir(path) {
        Ok(entries) => {
            let mut output = Vec::new();
            for entry in entries {
                match entry {
                    Ok(entry) => output.push(entry),
                    Err(err) => report.error(root, path, "read_dir_failed", err.to_string()),
                }
            }
            output
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(err) => {
            report.error(root, path, "read_dir_failed", err.to_string());
            Vec::new()
        }
    };
    entries.sort_by_key(|entry| entry.path());
    entries
}

fn doctor_display_path(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(stripped) if stripped.as_os_str().is_empty() => ".".to_string(),
        Ok(stripped) => stripped.display().to_string(),
        Err(_) => path.display().to_string(),
    }
}

fn doctor_file_stem(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .map(ToString::to_string)
}
