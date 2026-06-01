use crate::cli::DoctorArgs;
use crate::crypto;
use crate::error::Result;
use crate::receipt_recipients;
use crate::storage::{collect_orphan_temp_files, is_agent_record_file};
use crate::types::{Agent, HeartbeatState, LockOwner, Message, Meta, Receipt, WatchState};
use crate::util::{
    parse_time, process_is_alive, validate_agent_state, validate_id, validate_subject_id,
};
use crate::{MAX_SUMMARY_BYTES, SCHEMA_VERSION};
use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
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
        "staging",
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
        match agent.pubkey.as_deref() {
            Some(pubkey) => {
                if let Err(err) = crypto::parse_pubkey(pubkey) {
                    report.error(root, &path, "invalid_agent_pubkey", err.to_string());
                }
            }
            None => report.warn(
                root,
                &path,
                "agent_missing_pubkey",
                "claimed agent is not bound to a passport public key",
            ),
        }
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
    let task_ids: BTreeSet<String> = messages
        .iter()
        .filter(|(_, message)| message.kind == "task")
        .map(|(_, message)| message.id.clone())
        .collect();
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
        if let Some(after) = message.after.as_deref()
            && task_ids.contains(after)
            && message.kind == "message"
            && let Err(err) = serde_json::from_str::<crate::task::TaskResult>(&message.body)
        {
            report.error(
                root,
                &path,
                "invalid_task_result",
                format!("reply to task {after:?} is not a valid task result: {err}"),
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
    if let Err(err) = validate_id(&message.from, "sender") {
        report.error(root, path, "invalid_sender_id", err.to_string());
    }
    if let Err(err) = validate_id(&message.conversation_id, "conversation id") {
        report.error(
            root,
            path,
            "invalid_message_conversation_id",
            err.to_string(),
        );
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
        "message" | "event" | "receipt" | "system" | "task" | "summary"
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
    let mut recipient_seen = BTreeSet::new();
    for recipient in &message.to {
        if !recipient_seen.insert(recipient) {
            report.error(
                root,
                path,
                "duplicate_recipient",
                format!("recipient @{recipient} is listed more than once"),
            );
        }
        if recipient != "*"
            && let Err(err) = validate_id(recipient, "recipient")
        {
            report.error(root, path, "invalid_recipient_id", err.to_string());
        }
        if recipient != "*" && !meta.participants.iter().any(|item| item == recipient) {
            report.error(
                root,
                path,
                "recipient_not_participant",
                format!("recipient @{recipient} is not a participant"),
            );
        }
    }
    let mut mention_seen = BTreeSet::new();
    for mention in &message.mentions {
        if !mention_seen.insert(mention) {
            report.error(
                root,
                path,
                "duplicate_mention",
                format!("mention @{mention} is listed more than once"),
            );
        }
        if let Err(err) = validate_id(mention, "mention") {
            report.error(root, path, "invalid_mention_id", err.to_string());
        }
        if !meta.participants.iter().any(|item| item == mention) {
            report.warn(
                root,
                path,
                "mention_not_participant",
                format!("mention @{mention} is not a participant"),
            );
        }
    }
    let mut awaited_seen = BTreeSet::new();
    for awaited in &message.needs_response_from {
        if let Err(err) = validate_id(awaited, "awaited agent") {
            report.error(root, path, "invalid_awaited_id", err.to_string());
        }
        if !awaited_seen.insert(awaited) {
            report.error(
                root,
                path,
                "duplicate_awaited_agent",
                format!("awaited agent @{awaited} is listed more than once"),
            );
        }
        if !meta.participants.iter().any(|item| item == awaited) {
            report.error(
                root,
                path,
                "awaited_not_participant",
                format!("awaited agent @{awaited} is not a participant"),
            );
        }
    }
    if let Some(subject_id) = message.subject_id.as_deref()
        && let Err(err) = validate_subject_id(subject_id)
    {
        report.error(
            root,
            path,
            "invalid_subject_id",
            format!("message subject_id is invalid: {}", err.message),
        );
    }
    if let Some(after) = message.after.as_deref()
        && let Err(err) = validate_id(after, "after message id")
    {
        report.error(root, path, "invalid_after_id", err.to_string());
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
    if !matches!(message.kind.as_str(), "message" | "task")
        && (message.requires_ack || !message.needs_response_from.is_empty())
    {
        report.error(
            root,
            path,
            "non_obligation_kind_has_ask",
            format!(
                "kind {:?} must not carry requires_ack or needs_response_from",
                message.kind
            ),
        );
    }
    if message.kind == "summary" && message.body.len() > MAX_SUMMARY_BYTES {
        report.error(
            root,
            path,
            "summary_too_large",
            format!(
                "summary is {} bytes; limit is {}",
                message.body.len(),
                MAX_SUMMARY_BYTES
            ),
        );
    }
    if message.kind == "task" {
        let mut worker_pubkey = None;
        if message.requires_ack {
            report.error(
                root,
                path,
                "task_requires_ack",
                "task messages must not use requires_ack",
            );
        }
        if message.needs_response_from.len() != 1 {
            report.error(
                root,
                path,
                "invalid_task_worker_count",
                format!(
                    "task messages must name exactly one awaited worker, found {}",
                    message.needs_response_from.len()
                ),
            );
        } else if let Some(worker) = message.needs_response_from.first()
            && !message.to.iter().any(|recipient| recipient == worker)
        {
            report.error(
                root,
                path,
                "task_worker_not_recipient",
                format!("task worker @{worker} is not an explicit recipient"),
            );
        }
        if let Some(worker) = message.needs_response_from.first() {
            match crate::identity::load_passport(root, worker) {
                Ok(Some(passport)) => worker_pubkey = Some(passport.pubkey),
                Ok(None) => report.error(
                    root,
                    path,
                    "task_worker_identity_missing",
                    format!("task worker @{worker} has no passport"),
                ),
                Err(err) => report.error(
                    root,
                    path,
                    "task_worker_identity_invalid",
                    format!("task worker @{worker} passport is invalid: {}", err.message),
                ),
            }
        }
        match crate::task::TaskBody::parse(&message.body) {
            Ok(body) => {
                if let Some(token) = &body.capability {
                    if let Err(err) = crate::capability::verify_chain(token, None) {
                        report.error(
                            root,
                            path,
                            "invalid_task_capability",
                            format!("task capability chain is invalid: {}", err.message),
                        );
                    }
                    if let Some(worker_pubkey) = worker_pubkey.as_deref()
                        && token.blocks.last().map(|block| block.holder.as_str())
                            != Some(worker_pubkey)
                    {
                        report.error(
                            root,
                            path,
                            "task_capability_holder_mismatch",
                            "task capability holder does not match the awaited worker",
                        );
                    }
                }
            }
            Err(err) => report.error(
                root,
                path,
                "invalid_task_body",
                format!("task body is not valid Hermes JSON: {}", err.message),
            ),
        }
    }
    if message.kind != "system" {
        doctor_check_signed_record(
            root,
            report,
            path,
            SignedRecordCheck {
                record_type: "message",
                author: &message.from,
                signer_key: message.signer_key.as_deref(),
                hash: message.hash.as_deref(),
                sig: message.sig.as_deref(),
                record: message,
            },
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
    doctor_check_signed_record(
        root,
        report,
        path,
        SignedRecordCheck {
            record_type: "receipt",
            author: &receipt.agent,
            signer_key: receipt.signer_key.as_deref(),
            hash: receipt.hash.as_deref(),
            sig: receipt.sig.as_deref(),
            record: receipt,
        },
    );
}

struct SignedRecordCheck<'a, T> {
    record_type: &'a str,
    author: &'a str,
    signer_key: Option<&'a str>,
    hash: Option<&'a str>,
    sig: Option<&'a str>,
    record: &'a T,
}

fn doctor_check_signed_record<T: Serialize>(
    root: &Path,
    report: &mut DoctorReport,
    path: &Path,
    signed: SignedRecordCheck<'_, T>,
) {
    let SignedRecordCheck {
        record_type,
        author,
        signer_key,
        hash,
        sig,
        record,
    } = signed;
    let Some(author_pubkey) = doctor_agent_pubkey(root, report, path, author) else {
        if signer_key.is_some() || hash.is_some() || sig.is_some() {
            report.warn(
                root,
                path,
                &format!("unverified_{record_type}_signature"),
                format!(
                    "cannot verify {record_type} signature because @{author} has no claimed pubkey"
                ),
            );
        }
        return;
    };
    let Some(signer_key) = signer_key else {
        report.error(
            root,
            path,
            &format!("missing_{record_type}_signer"),
            format!("{record_type} by @{author} is missing signer_key"),
        );
        return;
    };
    if signer_key != author_pubkey {
        report.error(
            root,
            path,
            &format!("{record_type}_signer_mismatch"),
            format!("{record_type} signer key does not match @{author}'s claimed pubkey"),
        );
        return;
    }
    let Some(hash) = hash else {
        report.error(
            root,
            path,
            &format!("missing_{record_type}_hash"),
            format!("{record_type} by @{author} is missing hash"),
        );
        return;
    };
    let Some(sig) = sig else {
        report.error(
            root,
            path,
            &format!("missing_{record_type}_signature"),
            format!("{record_type} by @{author} is missing sig"),
        );
        return;
    };
    let value = match serde_json::to_value(record) {
        Ok(value) => value,
        Err(err) => {
            report.error(
                root,
                path,
                &format!("{record_type}_signature_encode_failed"),
                err.to_string(),
            );
            return;
        }
    };
    doctor_check_record_hash(root, report, path, record_type, hash, &value);
    let signing_bytes = match crypto::canonical_omitting(&value, &["sig"]) {
        Ok(bytes) => bytes,
        Err(err) => {
            report.error(
                root,
                path,
                &format!("{record_type}_signature_encode_failed"),
                err.to_string(),
            );
            return;
        }
    };
    if let Err(err) = crypto::verify(signer_key, &signing_bytes, sig) {
        report.error(
            root,
            path,
            &format!("invalid_{record_type}_signature"),
            err.to_string(),
        );
    }
}

fn doctor_check_record_hash(
    root: &Path,
    report: &mut DoctorReport,
    path: &Path,
    record_type: &str,
    hash: &str,
    value: &Value,
) {
    let hash_bytes = match crypto::canonical_omitting(value, &["hash", "sig"]) {
        Ok(bytes) => bytes,
        Err(err) => {
            report.error(
                root,
                path,
                &format!("{record_type}_hash_encode_failed"),
                err.to_string(),
            );
            return;
        }
    };
    let expected = crypto::sha256_hex(&hash_bytes);
    if hash != expected {
        report.error(
            root,
            path,
            &format!("invalid_{record_type}_hash"),
            format!("stored hash {hash:?} does not match canonical {expected:?}"),
        );
    }
}

fn doctor_agent_pubkey(
    root: &Path,
    report: &mut DoctorReport,
    signed_record_path: &Path,
    agent_id: &str,
) -> Option<String> {
    let agent_path = root.join("agents").join(format!("{agent_id}.json"));
    let agent = doctor_read_json::<Agent>(root, &agent_path, report)?;
    match agent.pubkey {
        Some(pubkey) => {
            if let Err(err) = crypto::parse_pubkey(&pubkey) {
                report.error(
                    root,
                    signed_record_path,
                    "invalid_agent_pubkey",
                    format!("cannot verify @{agent_id}: {err}"),
                );
                None
            } else {
                Some(pubkey)
            }
        }
        None => None,
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
