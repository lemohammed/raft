#[macro_use]
mod error;
mod cli;
mod storage;
mod types;
mod ui_html;
mod util;

use crate::cli::*;
use crate::error::{RaftError, Result};
use crate::storage::*;
use crate::types::*;
use crate::ui_html::UI_HTML;
use crate::util::*;
use chrono::{DateTime, TimeDelta, Utc};
use clap::Parser;
use serde::de::DeserializeOwned;
use serde::Serialize;
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::flag as signal_flag;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub(crate) const DEFAULT_RATE_WINDOW_SECONDS: u64 = 60;
pub(crate) const DEFAULT_RATE_MAX_MESSAGES: u64 = 10;
pub(crate) const DEFAULT_MAX_MESSAGE_BYTES: usize = 32_768;
pub(crate) const DEFAULT_AGENT_TTL_SECONDS: u64 = 120;
pub(crate) const LOCK_TTL_SECONDS: u64 = 30;
const LOCK_TIMEOUT_SECONDS: u64 = 5;
const SERVE_LOCK_TTL_SECONDS: u64 = 30;
pub(crate) const SCHEMA_VERSION: u16 = 1;

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

fn main() {
    let cli = Cli::parse();
    let root = match root_path(cli.root) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("raft: {err}");
            process::exit(1);
        }
    };

    if let Err(err) = run(root, cli.command) {
        eprintln!("raft: {err}");
        process::exit(1);
    }
}

fn run(root: PathBuf, command: Commands) -> Result<()> {
    match command {
        Commands::Init => cmd_init(&root),
        Commands::Claim(args) => cmd_claim(&root, args),
        Commands::Register(args) => cmd_register(&root, args),
        Commands::Heartbeat(args) => cmd_heartbeat(&root, args),
        Commands::State { command } => match command {
            StateCommand::Set(args) => cmd_state_set(&root, args),
            StateCommand::Get(args) => cmd_state_get(&root, args),
        },
        Commands::Channel { command } => match command {
            ChannelCommand::Create(args) => cmd_channel_create(&root, args),
            ChannelCommand::Join(args) => cmd_channel_join(&root, args),
        },
        Commands::Conversation { command } => match command {
            ConversationCommand::Create(args) => cmd_conversation_create(&root, args),
            ConversationCommand::Open(args) => cmd_conversation_open(&root, args),
        },
        Commands::Send(args) => cmd_send(&root, args),
        Commands::Awaiting(args) => cmd_awaiting(&root, args),
        Commands::Roster(args) => cmd_roster(&root, args),
        Commands::Inbox(args) => cmd_inbox(&root, args),
        Commands::Wait(args) => cmd_wait(&root, args),
        Commands::Watch(args) => cmd_watch(&root, args),
        Commands::Show(args) => cmd_show(&root, args),
        Commands::Search(args) => cmd_search(&root, args),
        Commands::Thread(args) => cmd_thread(&root, args),
        Commands::Read(args) => cmd_read(&root, args),
        Commands::Ack(args) => cmd_ack(&root, args),
        Commands::Receipts(args) => cmd_receipts(&root, args),
        Commands::Journal(args) => cmd_journal(&root, args),
        Commands::Status(args) => cmd_status(&root, args),
        Commands::Doctor(args) => cmd_doctor(&root, args),
        Commands::Gc(args) => cmd_gc(&root, args),
        Commands::Serve(args) => cmd_serve(&root, args),
        Commands::Ui(args) => cmd_ui(&root, args),
    }
}

fn root_path(root: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = root {
        return Ok(path);
    }
    if let Ok(value) = env::var("RAFT_ROOT") {
        return Ok(PathBuf::from(value));
    }
    Ok(env::current_dir()?.join("run").join("bus"))
}

fn cmd_init(root: &Path) -> Result<()> {
    ensure_root(root)?;
    println!("initialized raft bus at {}", root.display());
    Ok(())
}

fn migrate_conversation_records(conv: &Path) -> Result<()> {
    if let Some(meta) = read_json::<Meta>(&conv.join("meta.json"))? {
        atomic_write_json(&conv.join("meta.json"), &meta)?;
    }
    if let Some(rate) = read_json::<RateState>(&conv.join("rate.json"))? {
        atomic_write_json(&conv.join("rate.json"), &rate)?;
    }
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        if let Some(message) = read_json::<Message>(&entry.path())? {
            atomic_write_json(&entry.path(), &message)?;
        }
    }
    for message_receipts in sorted_read_dir(&conv.join("receipts"))? {
        if !message_receipts.path().is_dir() {
            continue;
        }
        for entry in sorted_read_dir(&message_receipts.path())? {
            if entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            if let Some(receipt) = read_json::<Receipt>(&entry.path())? {
                atomic_write_json(&entry.path(), &receipt)?;
            }
        }
    }
    Ok(())
}

fn cmd_claim(root: &Path, args: ClaimArgs) -> Result<()> {
    let agent_id = validate_claim_name(&args.agent)?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let path = agent_path(root, &agent_id);
    if path.exists() {
        bail!("agent name @{agent_id} is already claimed");
    }
    let workspace = args.workspace.map(|path| resolve_path(&path)).transpose()?;
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace,
        capabilities: split_csv(&args.capabilities)?,
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: args.ttl,
        expires_at: iso_after(args.ttl),
        current_state: default_agent_state(),
        state_note: None,
        state_updated_at: iso_now(),
    };
    atomic_write_json(&path, &payload)?;
    println!("claimed @{agent_id} at {}", root.display());
    Ok(())
}

fn cmd_register(root: &Path, args: RegisterArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent = read_json(&agent_path(root, &agent_id))?
        .ok_or_else(|| RaftError(format!("agent @{agent_id} is not claimed; use raft claim")))?;
    let workspace = args
        .workspace
        .map(|path| resolve_path(&path))
        .transpose()?
        .or_else(|| previous.workspace.clone());
    let capabilities = {
        let parsed = split_csv(&args.capabilities)?;
        if parsed.is_empty() {
            previous.capabilities.clone()
        } else {
            parsed
        }
    };
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace,
        capabilities,
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: args.ttl,
        expires_at: iso_after(args.ttl),
        current_state: previous.current_state,
        state_note: previous.state_note,
        state_updated_at: previous.state_updated_at,
    };
    atomic_write_json(&agent_path(root, &agent_id), &payload)?;
    println!("registered {agent_id} at {}", root.display());
    Ok(())
}

fn cmd_heartbeat(root: &Path, args: HeartbeatArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    if args.watch {
        return cmd_heartbeat_watch(root, &agent_id, args.ttl, args.interval);
    }
    heartbeat_once(root, &agent_id, args.ttl, true)?;
    Ok(())
}

fn heartbeat_once(
    root: &Path,
    agent_id: &str,
    ttl_override: Option<u64>,
    print: bool,
) -> Result<Agent> {
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent = read_json(&agent_path(root, agent_id))?
        .ok_or_else(|| RaftError(format!("agent @{agent_id} is not claimed; use raft claim")))?;
    let ttl = ttl_override.unwrap_or(previous.ttl_seconds);
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.to_string(),
        mention: format!("@{agent_id}"),
        workspace: previous.workspace,
        capabilities: previous.capabilities,
        pid: process::id(),
        host: hostname(),
        last_seen_at: iso_now(),
        ttl_seconds: ttl,
        expires_at: iso_after(ttl),
        current_state: previous.current_state,
        state_note: previous.state_note,
        state_updated_at: previous.state_updated_at,
    };
    atomic_write_json(&agent_path(root, agent_id), &payload)?;
    if print {
        println!("heartbeat {agent_id}");
    }
    Ok(payload)
}

fn cmd_heartbeat_watch(
    root: &Path,
    agent_id: &str,
    ttl_override: Option<u64>,
    interval_override: Option<f64>,
) -> Result<()> {
    let previous: Agent = read_json(&agent_path(root, agent_id))?
        .ok_or_else(|| RaftError(format!("agent @{agent_id} is not claimed; use raft claim")))?;
    let ttl = ttl_override.unwrap_or(previous.ttl_seconds);
    let interval = interval_override.unwrap_or_else(|| (ttl as f64 / 2.0).max(1.0));
    if !interval.is_finite() || interval <= 0.0 {
        bail!("--interval must be a positive finite number");
    }
    let state_path = heartbeat_state_path(root, agent_id);
    let _lock = DirLock::acquire(
        root,
        &format!("heartbeat-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if let Some(existing) = read_json::<HeartbeatState>(&state_path)?
        && existing.shutdown_at.is_none()
        && existing.pid != process::id()
        && process_is_alive(existing.pid)
    {
        bail!(
            "heartbeat watcher for @{agent_id} already appears active with pid {}",
            existing.pid
        );
    }
    let started_at = iso_now();
    let mut state = HeartbeatState {
        v: SCHEMA_VERSION,
        agent: agent_id.to_string(),
        pid: process::id(),
        host: hostname(),
        started_at: started_at.clone(),
        updated_at: started_at.clone(),
        last_heartbeat_at: started_at,
        interval_seconds: interval,
        ttl_seconds: ttl,
        shutdown_at: None,
    };
    atomic_write_json(&state_path, &state)?;
    drop(_lock);

    let shutdown = Arc::new(AtomicBool::new(false));
    signal_flag::register(SIGTERM, Arc::clone(&shutdown))?;
    signal_flag::register(SIGINT, Arc::clone(&shutdown))?;

    loop {
        let agent = heartbeat_once(root, agent_id, Some(ttl), false)?;
        state.last_heartbeat_at = agent.last_seen_at;
        state.updated_at = iso_now();
        atomic_write_json(&state_path, &state)?;
        sleep_interruptibly(Duration::from_secs_f64(interval), &shutdown);
        if shutdown.load(Ordering::Relaxed) {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&state_path, &state)?;
            return Ok(());
        }
    }
}

fn cmd_state_set(root: &Path, args: StateSetArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let state = validate_agent_state(&args.state)?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("agent-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let previous: Agent = read_json(&agent_path(root, &agent_id))?
        .ok_or_else(|| RaftError(format!("agent @{agent_id} is not claimed; use raft claim")))?;
    let changed = previous.current_state != state || previous.state_note != args.note;
    let now = iso_now();
    let payload = Agent {
        v: SCHEMA_VERSION,
        id: agent_id.clone(),
        mention: format!("@{agent_id}"),
        workspace: previous.workspace,
        capabilities: previous.capabilities,
        pid: process::id(),
        host: hostname(),
        last_seen_at: now.clone(),
        ttl_seconds: previous.ttl_seconds,
        expires_at: iso_after(previous.ttl_seconds),
        current_state: state.clone(),
        state_note: args.note.clone(),
        state_updated_at: now,
    };
    atomic_write_json(&agent_path(root, &agent_id), &payload)?;
    if changed {
        write_state_change_messages(root, &agent_id, &state, args.note.as_deref())?;
    }
    println!("@{agent_id} {state}");
    Ok(())
}

fn cmd_state_get(root: &Path, args: StateGetArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let agent: Agent = read_json(&agent_path(root, &agent_id))?
        .ok_or_else(|| RaftError(format!("agent @{agent_id} is not claimed; use raft claim")))?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent.id,
                "state": agent.current_state,
                "note": agent.state_note,
                "updated_at": agent.state_updated_at
            }))?
        );
    } else if let Some(note) = agent.state_note {
        println!(
            "@{} {} since {} — {}",
            agent.id, agent.current_state, agent.state_updated_at, note
        );
    } else {
        println!(
            "@{} {} since {}",
            agent.id, agent.current_state, agent.state_updated_at
        );
    }
    Ok(())
}

fn cmd_channel_create(root: &Path, args: ChannelCreateArgs) -> Result<()> {
    let channel_id = validate_id(&args.channel, "channel id")?;
    let creator = validate_id(&args.creator, "creator")?;
    let mut participants = vec![creator.clone()];
    participants.extend(split_csv(&args.members)?);
    let participants = unique(participants);
    ensure_root(root)?;
    let conv = conversation_path(root, &channel_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{channel_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if conv.join("meta.json").exists() {
        if args.if_missing {
            migrate_conversation_records(&conv)?;
            let meta: Meta = read_json(&conv.join("meta.json"))?
                .ok_or_else(|| RaftError(format!("channel {channel_id:?} has no metadata")))?;
            if !meta.channel {
                bail!("{channel_id:?} already exists but is not a channel");
            }
            println!("channel {channel_id} ready; root={}", root.display());
            return Ok(());
        }
        bail!("channel {channel_id:?} already exists");
    }
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    set_dir_private(&conv)?;
    set_dir_private(&conv.join("messages"))?;
    set_dir_private(&conv.join("receipts"))?;

    let meta = Meta {
        v: SCHEMA_VERSION,
        id: channel_id.clone(),
        participants,
        channel: true,
        private: false,
        state: "open".to_string(),
        created_at: iso_now(),
        updated_at: iso_now(),
        retention_days: args.retention_days,
        rate: Rate {
            window_seconds: args.rate_window,
            max_messages_per_sender: args.rate_max,
            max_message_bytes: args.max_message_bytes,
        },
    };
    atomic_write_json(&conv.join("meta.json"), &meta)?;
    write_system_message(
        &conv,
        &channel_id,
        vec![creator],
        format!(
            "Channel opened. Subscribers: {}.",
            meta.participants.join(",")
        ),
        "channel opened",
    )?;
    println!("channel {channel_id} ready; root={}", root.display());
    Ok(())
}

fn cmd_channel_join(root: &Path, args: ChannelJoinArgs) -> Result<()> {
    let channel_id = validate_id(&args.channel, "channel id")?;
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{channel_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &channel_id)?;
    let mut meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| RaftError(format!("channel {channel_id:?} does not exist")))?;
    if !meta.channel {
        bail!("{channel_id:?} is not a channel");
    }
    if !meta
        .participants
        .iter()
        .any(|participant| participant == &agent_id)
    {
        meta.participants.push(agent_id.clone());
        meta.updated_at = iso_now();
        atomic_write_json(&conv.join("meta.json"), &meta)?;
        write_system_message(
            &conv,
            &channel_id,
            vec!["*".to_string()],
            format!("@{agent_id} joined channel {channel_id}."),
            "channel joined",
        )?;
    }
    println!("@{agent_id} subscribed to channel {channel_id}");
    Ok(())
}

fn cmd_conversation_create(root: &Path, args: ConversationCreateArgs) -> Result<()> {
    let conversation_id = validate_id(&args.conversation, "conversation id")?;
    let participants = unique(split_csv(&args.participants)?);
    if participants.len() < 2 {
        bail!("a conversation needs at least two participants");
    }
    for participant in &participants {
        validate_id(participant, "participant")?;
    }
    let starter = match args.starter {
        Some(starter) => validate_id(&starter, "starter")?,
        None => participants[0].clone(),
    };
    if !participants.contains(&starter) {
        bail!("starter must be one of the participants");
    }

    ensure_root(root)?;
    let conv = conversation_path(root, &conversation_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if conv.join("meta.json").exists() {
        if args.if_missing {
            migrate_conversation_records(&conv)?;
            println!(
                "conversation {conversation_id} ready; root={}",
                root.display()
            );
            return Ok(());
        }
        bail!("conversation {conversation_id:?} already exists");
    }
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    set_dir_private(&conv)?;
    set_dir_private(&conv.join("messages"))?;
    set_dir_private(&conv.join("receipts"))?;

    let meta = Meta {
        v: SCHEMA_VERSION,
        id: conversation_id.clone(),
        participants,
        channel: false,
        private: args.private,
        state: "open".to_string(),
        created_at: iso_now(),
        updated_at: iso_now(),
        retention_days: args.retention_days,
        rate: Rate {
            window_seconds: args.rate_window,
            max_messages_per_sender: args.rate_max,
            max_message_bytes: args.max_message_bytes,
        },
    };
    atomic_write_json(&conv.join("meta.json"), &meta)?;
    write_system_message(
        &conv,
        &conversation_id,
        vec![starter.clone()],
        format!(
            "Conversation opened by {starter}. Participants: {}.",
            meta.participants.join(",")
        ),
        "conversation opened",
    )?;
    println!(
        "conversation {conversation_id} ready; root={}",
        root.display()
    );
    Ok(())
}

fn cmd_conversation_open(root: &Path, args: ConversationOpenArgs) -> Result<()> {
    let opener = validate_id(&args.opener, "opener")?;
    let mut participants = vec![opener.clone()];
    participants.extend(split_csv(&args.to)?);
    let participants = unique(participants);
    if participants.len() < 2 {
        bail!("a private chat needs at least two unique participants");
    }
    let conversation_id = match args.conversation {
        Some(id) => validate_id(&id, "conversation id")?,
        None => generated_private_conversation_id(&participants, &args.topic),
    };

    cmd_conversation_create(
        root,
        ConversationCreateArgs {
            conversation: conversation_id,
            participants: participants.join(","),
            starter: Some(opener),
            private: true,
            if_missing: args.if_missing,
            retention_days: args.retention_days,
            rate_window: args.rate_window,
            rate_max: args.rate_max,
            max_message_bytes: args.max_message_bytes,
        },
    )
}

fn cmd_send(root: &Path, args: SendArgs) -> Result<()> {
    let conversation_id = target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let message_id = send_message(
        root,
        SendMessageInput {
            conversation_id,
            sender: args.sender,
            to: args.to,
            subject: args.subject,
            body: args.body,
            kind: args.kind,
            after: args.after,
            subject_id: args.subject_id,
            requires_ack: args.requires_ack,
            needs_response_from: args.needs_response_from,
        },
    )?;
    println!("{message_id}");
    Ok(())
}

fn send_message(root: &Path, input: SendMessageInput) -> Result<String> {
    let conversation_id = input.conversation_id;
    let sender = validate_id(&input.sender, "sender")?;
    let mut recipients = unique(split_recipients(&input.to)?);
    if recipients.is_empty() {
        bail!("--to needs at least one recipient");
    }
    for recipient in &recipients {
        if recipient != "*" {
            validate_id(recipient, "recipient")?;
        }
    }
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let (conv, meta) = load_conversation(root, &conversation_id)?;
    ensure_participant(&meta, &sender)?;
    let mentions = mentioned_participants(&meta, &input.subject, &input.body);
    recipients.extend(mentions.iter().cloned());
    recipients = unique(recipients);
    ensure_recipients(&meta, &recipients)?;
    let needs_response_from = unique(split_recipients(&input.needs_response_from)?);
    for awaited in &needs_response_from {
        ensure_participant(&meta, awaited)?;
        if !recipients.iter().any(|recipient| recipient == awaited || recipient == "*") {
            recipients.push(awaited.clone());
        }
    }
    recipients = unique(recipients);
    let kind = normalize_send_kind(&input.kind)?;
    let subject_id = input
        .subject_id
        .as_deref()
        .map(validate_subject_id)
        .transpose()?;
    let after = input
        .after
        .as_deref()
        .map(|value| validate_id(value, "after message id"))
        .transpose()?;
    enforce_rate_limit(&conv, &meta, &sender, subject_id.as_deref(), &input.body)?;

    let message_id = new_message_id();
    let message = Message {
        v: SCHEMA_VERSION,
        id: message_id.clone(),
        conversation_id: meta.id.clone(),
        kind,
        from: sender.clone(),
        to: recipients,
        mentions,
        subject: input.subject,
        body: input.body,
        created_at: iso_now(),
        requires_ack: input.requires_ack,
        needs_response_from,
        subject_id,
        after,
    };
    atomic_write_json(
        &conv.join("messages").join(format!("{message_id}.json")),
        &message,
    )?;
    Ok(message_id)
}


fn ask_is_terminal(status: &str) -> bool {
    matches!(status, "done" | "rejected")
}

fn message_awaited(message: &Message, meta: &Meta) -> Vec<String> {
    let awaited = if !message.needs_response_from.is_empty() {
        message.needs_response_from.clone()
    } else if message.requires_ack {
        receipt_recipients(message, meta)
    } else {
        return Vec::new();
    };
    awaited
        .into_iter()
        .filter(|agent| agent != &message.from && agent != "*")
        .collect()
}

fn gather_open_asks(
    root: &Path,
    only_conversation: Option<&str>,
    participant: Option<&str>,
) -> Result<Vec<OpenAsk>> {
    let mut asks = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if let Some(id) = only_conversation
            && meta.id != id
        {
            continue;
        }
        if let Some(agent) = participant
            && !meta.participants.iter().any(|item| item == agent)
        {
            continue;
        }
        for message_entry in sorted_read_dir(&conv.join("messages"))? {
            if message_entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            let Some(message): Option<Message> = read_json(&message_entry.path())? else {
                continue;
            };
            if message.kind == "system" || message.kind == "receipt" {
                continue;
            }
            let awaited = message_awaited(&message, &meta);
            if awaited.is_empty() {
                continue;
            }
            let receipts = load_message_receipts(root, &message)?;
            for who in awaited {
                let status = receipts.get(&who).map(|receipt| receipt.status.clone());
                if status.as_deref().map(ask_is_terminal).unwrap_or(false) {
                    continue;
                }
                asks.push(OpenAsk {
                    conversation_id: meta.id.clone(),
                    message_id: message.id.clone(),
                    from: message.from.clone(),
                    awaited: who,
                    subject: message.subject.clone(),
                    created_at: message.created_at.clone(),
                    status: status.unwrap_or_else(|| "none".to_string()),
                });
            }
        }
    }
    asks.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
    Ok(asks)
}

fn cmd_awaiting(root: &Path, args: AwaitingArgs) -> Result<()> {
    ensure_root(root)?;
    let agent = validate_id(&args.agent, "agent id")?;
    let only = optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let asks = gather_open_asks(root, only.as_deref(), Some(&agent))?;
    let incoming: Vec<&OpenAsk> = asks.iter().filter(|ask| ask.awaited == agent).collect();
    let outgoing: Vec<&OpenAsk> = asks
        .iter()
        .filter(|ask| ask.from == agent && ask.awaited != agent)
        .collect();
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent,
                "you_owe": incoming,
                "owed_to_you": outgoing
            }))?
        );
        return Ok(());
    }
    println!("awaiting for @{agent}");
    println!("you owe a response to:");
    if incoming.is_empty() {
        println!("  nothing");
    }
    for ask in &incoming {
        println!(
            "  {} in {} from @{} [{}]: {}",
            ask.message_id, ask.conversation_id, ask.from, ask.status, ask.subject
        );
    }
    println!("waiting on a response from others:");
    if outgoing.is_empty() {
        println!("  nothing");
    }
    for ask in &outgoing {
        println!(
            "  {} in {} -> @{} [{}]: {}",
            ask.message_id, ask.conversation_id, ask.awaited, ask.status, ask.subject
        );
    }
    Ok(())
}

fn state_priority(state: &str) -> u8 {
    match state {
        "blocked" => 0,
        "working" => 1,
        "idle" => 2,
        "away" => 3,
        _ => 4,
    }
}

fn cmd_roster(root: &Path, args: RosterArgs) -> Result<()> {
    ensure_root(root)?;
    let asks = gather_open_asks(root, None, None)?;
    let mut owes: BTreeMap<String, usize> = BTreeMap::new();
    let mut waiting_on: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *owes.entry(ask.awaited.clone()).or_default() += 1;
        *waiting_on.entry(ask.from.clone()).or_default() += 1;
    }
    let mut entries = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(agent): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= Utc::now())
            .unwrap_or(false);
        if !active && !args.all {
            continue;
        }
        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        entries.push(serde_json::json!({
            "id": agent.id,
            "mention": mention,
            "active": active,
            "current_state": agent.current_state,
            "state_note": agent.state_note,
            "last_seen_at": agent.last_seen_at,
            "expires_at": agent.expires_at,
            "owes": owes.get(&agent.id).copied().unwrap_or(0),
            "waiting_on": waiting_on.get(&agent.id).copied().unwrap_or(0)
        }));
    }
    entries.sort_by(|left, right| {
        let lp = state_priority(left["current_state"].as_str().unwrap_or(""));
        let rp = state_priority(right["current_state"].as_str().unwrap_or(""));
        lp.cmp(&rp).then_with(|| {
            left["id"]
                .as_str()
                .unwrap_or("")
                .cmp(right["id"].as_str().unwrap_or(""))
        })
    });
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root": root,
                "agents": entries
            }))?
        );
        return Ok(());
    }
    println!("roster ({} shown):", entries.len());
    if entries.is_empty() {
        println!("  none");
    }
    for entry in &entries {
        let liveness = if entry["active"].as_bool().unwrap_or(false) {
            "live"
        } else {
            "stale"
        };
        let note = entry["state_note"].as_str().unwrap_or("");
        let note_suffix = if note.is_empty() {
            String::new()
        } else {
            format!(" — {note}")
        };
        println!(
            "  {} [{}/{}] owes={} waiting={}{}",
            entry["id"].as_str().unwrap_or("unknown"),
            liveness,
            entry["current_state"].as_str().unwrap_or("idle"),
            entry["owes"].as_u64().unwrap_or(0),
            entry["waiting_on"].as_u64().unwrap_or(0),
            note_suffix
        );
    }
    Ok(())
}

fn cmd_inbox(root: &Path, args: InboxArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    ensure_root(root)?;
    let mut rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
    if args.unread {
        rows.retain(|message| message_is_unread(root, message, &agent_id));
    }
    if rows.len() > args.limit {
        rows = rows.split_off(rows.len() - args.limit);
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no messages");
        return Ok(());
    }
    for message in rows {
        let unread = if message_is_unread(root, &message, &agent_id) {
            "*"
        } else {
            " "
        };
        let body = truncated_body(&message.body, args.width);
        println!(
            "{unread} {} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            body
        );
    }
    Ok(())
}

fn cmd_wait(root: &Path, args: WaitArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let deadline = Instant::now() + Duration::from_secs(args.timeout);
    loop {
        ensure_root(root)?;
        let rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
        if let Some(message) = rows
            .into_iter()
            .find(|message| message_is_unread(root, message, &agent_id))
        {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&message)?);
            } else {
                println!("{}", message.id);
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            process::exit(2);
        }
        thread::sleep(Duration::from_secs_f64(args.interval));
    }
}

fn cmd_watch(root: &Path, args: WatchArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let since = args
        .since
        .as_deref()
        .map(|id| validate_id(id, "message id"))
        .transpose()?;
    ensure_root(root)?;
    let mut state = start_watch_state(root, &agent_id, since)?;
    let deadline = if args.timeout == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_secs(args.timeout))
    };

    loop {
        let mut rows = visible_messages(root, &agent_id, conversation_id.as_deref())?;
        rows.sort_by(|left, right| left.id.cmp(&right.id));
        let mut emitted = false;
        for message in rows {
            if let Some(last_event_id) = state.last_event_id.as_deref()
                && message.id.as_str() <= last_event_id
            {
                continue;
            }
            let should_emit = message_is_unread(root, &message, &agent_id)
                || args.state_changes && is_state_change_message(&message);
            if !should_emit {
                continue;
            }
            emit_watch_message(&message, args.json)?;
            if !args.no_auto_read && !is_state_change_message(&message) {
                let _lock = DirLock::acquire(
                    root,
                    &format!("conversation-{}", message.conversation_id),
                    LOCK_TTL_SECONDS,
                    LOCK_TIMEOUT_SECONDS,
                )?;
                write_receipt(root, &agent_id, &message, "read", None)?;
            }
            state.last_event_id = Some(message.id.clone());
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            emitted = true;
        }
        if args.once {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            return Ok(());
        }
        if let Some(deadline) = deadline
            && Instant::now() >= deadline
        {
            state.shutdown_at = Some(iso_now());
            state.updated_at = iso_now();
            atomic_write_json(&watch_state_path(root, &agent_id), &state)?;
            return Ok(());
        }
        if !emitted {
            thread::sleep(Duration::from_secs_f64(args.interval));
        }
    }
}

fn cmd_show(root: &Path, args: ShowArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    let conversation_id = target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    ensure_root(root)?;
    let mut rows = visible_messages(root, &agent_id, Some(&conversation_id))?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if rows.len() > args.limit {
        rows = rows.split_off(rows.len() - args.limit);
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no visible messages");
        return Ok(());
    }
    for (index, message) in rows.iter().enumerate() {
        if index > 0 {
            println!();
        }
        println!(
            "{} {} {} -> {} [{}]",
            message.id,
            message.created_at,
            message.from,
            message.to.join(","),
            message.kind
        );
        if !message.subject.is_empty() {
            println!("Subject: {}", message.subject);
        }
        if !message.mentions.is_empty() {
            println!("Mentions: {}", message.mentions.join(","));
        }
        if let Some(after) = message.after.as_deref() {
            println!("After: {after}");
        }
        println!("{}", message.body);
    }
    Ok(())
}

fn cmd_search(root: &Path, args: SearchArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    if args.pattern.trim().is_empty() {
        bail!("search pattern cannot be empty");
    }
    let conversation_id =
        optional_target_room(args.conversation.as_deref(), args.channel.as_deref())?;
    let cutoff = args.since.as_deref().map(parse_since_cutoff).transpose()?;
    ensure_root(root)?;
    let pattern = args.pattern.to_lowercase();
    let mut rows = visible_messages(root, &agent_id, conversation_id.as_deref())?
        .into_iter()
        .filter(|message| {
            cutoff
                .map(|cutoff| {
                    parse_time(&message.created_at)
                        .map(|created_at| created_at >= cutoff)
                        .unwrap_or(false)
                })
                .unwrap_or(true)
        })
        .filter(|message| message_matches_pattern(message, &pattern))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    if rows.len() > args.limit {
        rows = rows.split_off(rows.len() - args.limit);
    }
    if args.json {
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("no matches");
        return Ok(());
    }
    for message in rows {
        println!(
            "{} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            truncated_body(&message.body, 120)
        );
    }
    Ok(())
}

fn cmd_thread(root: &Path, args: ThreadArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let (_, root_message) = find_message(root, &args.message_id)?;
    if !message_visible_to(&root_message, &agent_id) {
        bail!(
            "message {:?} is not visible to {agent_id:?}",
            root_message.id
        );
    }
    let mut rows = visible_messages(root, &agent_id, Some(&root_message.conversation_id))?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    let mut remaining = args.limit.max(1);
    let mut visited = BTreeSet::new();
    let tree = build_thread_node(&root_message.id, &rows, &mut remaining, &mut visited)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&tree)?);
    } else {
        print_thread_node(&tree, 0);
    }
    Ok(())
}

fn cmd_read(root: &Path, args: ReadArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let (path, message) = find_message(root, &args.message_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", message.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    write_receipt(root, &agent_id, &message, "read", None)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&message)?);
    } else {
        println!(
            "{} {} {} -> {:?}",
            message.id, message.conversation_id, message.from, message.to
        );
        if !message.subject.is_empty() {
            println!("Subject: {}", message.subject);
        }
        println!("\n{}", message.body);
        println!("\nsource={}", path.display());
    }
    Ok(())
}

fn cmd_ack(root: &Path, args: AckArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    validate_id(&args.status, "status")?;
    ensure_root(root)?;
    let (_, message) = find_message(root, &args.message_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{}", message.conversation_id),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    write_receipt(root, &agent_id, &message, &args.status, args.note)?;
    println!("{} {}", args.status, args.message_id);
    Ok(())
}

fn cmd_receipts(root: &Path, args: ReceiptsArgs) -> Result<()> {
    ensure_root(root)?;
    let (_, message) = find_message(root, &args.message_id)?;
    let conv = conversation_path(root, &message.conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?.ok_or_else(|| {
        RaftError(format!(
            "conversation {:?} does not exist",
            message.conversation_id
        ))
    })?;
    let recipients = receipt_recipients(&message, &meta);
    let receipts = load_message_receipts(root, &message)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "message": {
                    "id": message.id,
                    "conversation_id": message.conversation_id,
                    "from": message.from,
                    "to": message.to,
                    "created_at": message.created_at,
                    "requires_ack": message.requires_ack
                },
                "recipients": recipients,
                "receipts": receipts
            }))?
        );
        return Ok(());
    }
    println!(
        "{} in {} from={} created={}",
        message.id, message.conversation_id, message.from, message.created_at
    );
    println!("Recipients: {}", recipients.join(","));
    println!("Receipts:");
    for recipient in recipients {
        if let Some(receipt) = receipts.get(&recipient) {
            println!(
                "  {}: read={} last={} status={} note={}",
                recipient,
                receipt.read_at.as_deref().unwrap_or("null"),
                receipt.updated_at,
                receipt.status,
                receipt
                    .note
                    .as_ref()
                    .map(|note| format!("{note:?}"))
                    .unwrap_or_else(|| "null".to_string())
            );
        } else {
            println!("  {recipient}: <none>");
        }
    }
    Ok(())
}

fn cmd_journal(root: &Path, args: JournalArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    validate_id(&args.kind, "journal kind")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("journal-{agent_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let entry = JournalEntry {
        v: SCHEMA_VERSION,
        id: format!("j-{}", unique_token()),
        agent: agent_id.clone(),
        kind: args.kind,
        subject: args.subject,
        body: args.body,
        created_at: iso_now(),
    };
    append_jsonl(
        &root.join("journal").join(format!("{agent_id}.jsonl")),
        &entry,
    )?;
    println!("{}", entry.id);
    Ok(())
}

fn cmd_status(root: &Path, args: StatusArgs) -> Result<()> {
    ensure_root(root)?;
    let scoped_agent = args
        .agent
        .as_deref()
        .map(|agent| validate_id(agent, "agent id"))
        .transpose()?;
    let mut agents = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let agent: Agent = read_json(&entry.path())?.ok_or_else(|| {
            RaftError(format!(
                "agent file disappeared: {}",
                entry.path().display()
            ))
        })?;
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= Utc::now())
            .unwrap_or(false);
        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        agents.push(serde_json::json!({
            "id": agent.id,
            "mention": mention,
            "workspace": agent.workspace,
            "capabilities": agent.capabilities,
            "last_seen_at": agent.last_seen_at,
            "expires_at": agent.expires_at,
            "current_state": agent.current_state,
            "state_note": agent.state_note,
            "state_updated_at": agent.state_updated_at,
            "active": active
        }));
    }

    let asks = gather_open_asks(root, None, scoped_agent.as_deref())?;
    let mut open_asks_by_conv: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *open_asks_by_conv.entry(ask.conversation_id.clone()).or_default() += 1;
    }

    let mut conversations = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if let Some(agent_id) = scoped_agent.as_deref()
            && meta.private
            && !meta
                .participants
                .iter()
                .any(|participant| participant == agent_id)
        {
            continue;
        }
        let messages = sorted_read_dir(&conv.join("messages"))
            .map(|items| items.len())
            .unwrap_or(0);
        conversations.push(serde_json::json!({
            "id": meta.id,
            "participants": meta.participants,
            "channel": meta.channel,
            "private": meta.private,
            "messages": messages,
            "open_asks": open_asks_by_conv.get(&meta.id).copied().unwrap_or(0)
        }));
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "root": root,
                "agents": agents,
                "conversations": conversations
            }))?
        );
        return Ok(());
    }

    println!("root: {}", root.display());
    println!("agents:");
    if agents.is_empty() {
        println!("  none");
    }
    for agent in agents {
        let liveness = if agent["active"].as_bool().unwrap_or(false) {
            "live"
        } else {
            "stale"
        };
        let note = agent["state_note"].as_str().unwrap_or("");
        let note_suffix = if note.is_empty() {
            String::new()
        } else {
            format!(" — {note}")
        };
        println!(
            "  {} ({}): {liveness}/{}{}",
            agent["id"].as_str().unwrap_or("unknown"),
            agent["mention"].as_str().unwrap_or(""),
            agent["current_state"].as_str().unwrap_or("idle"),
            note_suffix
        );
    }
    println!("conversations:");
    if conversations.is_empty() {
        println!("  none");
    }
    for conversation in conversations {
        let room_kind = if conversation["channel"].as_bool().unwrap_or(false) {
            "channel"
        } else if conversation["private"].as_bool().unwrap_or(false) {
            "private"
        } else {
            "chat"
        };
        println!(
            "  {} [{}]: messages={} open_asks={}",
            conversation["id"].as_str().unwrap_or("unknown"),
            room_kind,
            conversation["messages"].as_u64().unwrap_or(0),
            conversation["open_asks"].as_u64().unwrap_or(0)
        );
    }
    Ok(())
}

fn cmd_doctor(root: &Path, args: DoctorArgs) -> Result<()> {
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
    report.finalize();
    report
}

fn doctor_scan_agents(root: &Path, report: &mut DoctorReport) -> BTreeSet<String> {
    let mut claimed = BTreeSet::new();
    for entry in doctor_sorted_read_dir(root, &root.join("agents"), report) {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("json")) {
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

fn cmd_gc(root: &Path, args: GcArgs) -> Result<()> {
    ensure_root(root)?;
    let mut stale_locks = 0;
    let mut archived_messages = 0;

    for entry in sorted_read_dir(&root.join("locks"))? {
        let path = entry.path();
        if path.extension() != Some(OsStr::new("lock")) {
            continue;
        }
        if lock_is_stale(&path)? {
            let _ = fs::remove_dir_all(&path);
            stale_locks += 1;
        }
    }

    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if args.archive {
            let _lock = DirLock::acquire(
                root,
                &format!("conversation-{}", meta.id),
                LOCK_TTL_SECONDS,
                LOCK_TIMEOUT_SECONDS,
            )?;
            archived_messages += archive_old_messages(root, &conv, &meta)?;
        }
    }

    println!(
        "gc complete: stale_locks={stale_locks} archived_messages={archived_messages}"
    );
    Ok(())
}

fn cmd_serve(root: &Path, args: ServeArgs) -> Result<()> {
    ensure_root(root)?;
    let serve_lock = DirLock::acquire(root, "serve", SERVE_LOCK_TTL_SECONDS, LOCK_TIMEOUT_SECONDS)?;
    println!(
        "raft monitor serving {}; interval={}s",
        root.display(),
        args.interval
    );
    loop {
        serve_lock.refresh(SERVE_LOCK_TTL_SECONDS)?;
        cmd_gc(
            root,
            GcArgs {
                archive: args.archive,
            },
        )?;
        serve_lock.refresh(SERVE_LOCK_TTL_SECONDS)?;
        thread::sleep(Duration::from_secs_f64(args.interval));
    }
}

fn cmd_ui(root: &Path, args: UiArgs) -> Result<()> {
    let agent_id = validate_id(&args.agent, "agent id")?;
    ensure_root(root)?;
    let listener = TcpListener::bind((args.host.as_str(), args.port))?;
    let address = listener.local_addr()?;
    println!(
        "raft ui serving http://{}/?agent={} root={}",
        address,
        agent_id,
        root.display()
    );
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                if let Err(err) = handle_ui_request(
                    root,
                    &agent_id,
                    args.host.as_str(),
                    args.port,
                    args.limit,
                    stream,
                ) {
                    eprintln!("raft ui: {err}");
                }
            }
            Err(err) => eprintln!("raft ui: failed connection: {err}"),
        }
    }
    Ok(())
}

fn handle_ui_request(
    root: &Path,
    default_agent: &str,
    bind_host: &str,
    bind_port: u16,
    default_limit: usize,
    mut stream: TcpStream,
) -> Result<()> {
    let request = read_http_request(&mut stream)?;
    if request.method.is_empty() {
        return Ok(());
    }
    let method = request.method.as_str();
    let target = request.target.as_str();
    let (path, query) = split_http_target(target);
    if let Err(err) = validate_ui_request_security(&request, bind_host, bind_port) {
        return write_http_json(
            &mut stream,
            403,
            &serde_json::json!({
                "ok": false,
                "error": err.to_string()
            }),
        );
    }
    if method == "GET" {
        return match path {
            "/" | "/index.html" => write_http_response(
                &mut stream,
                200,
                "text/html; charset=utf-8",
                UI_HTML.as_bytes(),
            ),
            "/api/snapshot" => {
                let agent =
                    query_param(query, "agent").unwrap_or_else(|| default_agent.to_string());
                let agent = validate_id(&agent, "agent id")?;
                let limit = query_param(query, "limit")
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(default_limit)
                    .clamp(1, 500);
                let snapshot = build_ui_snapshot(root, &agent, limit)?;
                write_http_json(&mut stream, 200, &snapshot)
            }
            "/health" => write_http_text(&mut stream, 200, "ok"),
            "/favicon.ico" => {
                write_http_response(&mut stream, 204, "text/plain; charset=utf-8", b"")
            }
            _ => write_http_text(&mut stream, 404, "Not found"),
        };
    }
    if method == "POST" {
        return handle_ui_post(root, path, &request.body, &mut stream);
    }
    write_http_text(&mut stream, 405, "Method not allowed")
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let count = match stream.read(&mut buffer) {
            Ok(count) => count,
            Err(err)
                if err.kind() == io::ErrorKind::WouldBlock
                    || err.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(err) => return Err(err.into()),
        };
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..count]);
        if bytes.len() > 1_048_576 {
            bail!("request body is too large");
        }
        if let Some(header_end) = http_header_end(&bytes) {
            let header_text = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = http_content_length(&header_text)?;
            let body_start = header_end + 4;
            if bytes.len() >= body_start + content_length {
                break;
            }
        }
    }
    if bytes.is_empty() {
        return Ok(HttpRequest {
            method: String::new(),
            target: String::new(),
            headers: Vec::new(),
            body: Vec::new(),
        });
    }
    let header_end =
        http_header_end(&bytes).ok_or_else(|| RaftError("invalid HTTP request".to_string()))?;
    let header_text = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = header_text.lines();
    let request_line = lines
        .next()
        .ok_or_else(|| RaftError("missing HTTP request line".to_string()))?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("/").to_string();
    let content_length = http_content_length(&header_text)?;
    let headers = parse_http_headers(&header_text);
    let body_start = header_end + 4;
    let body_end = (body_start + content_length).min(bytes.len());
    Ok(HttpRequest {
        method,
        target,
        headers,
        body: bytes[body_start..body_end].to_vec(),
    })
}

fn handle_ui_post(root: &Path, path: &str, body: &[u8], stream: &mut TcpStream) -> Result<()> {
    let response = match path {
        "/api/send" => api_send(root, body),
        "/api/open" => api_open_private(root, body),
        "/api/channel" => api_create_channel(root, body),
        "/api/join" => api_join_channel(root, body),
        _ => return write_http_text(stream, 404, "Not found"),
    };
    match response {
        Ok(payload) => write_http_json(stream, 200, &payload),
        Err(err) => write_http_json(
            stream,
            400,
            &serde_json::json!({
                "ok": false,
                "error": err.to_string()
            }),
        ),
    }
}

fn api_send(root: &Path, body: &[u8]) -> Result<serde_json::Value> {
    let request: UiSendRequest = serde_json::from_slice(body)?;
    let conversation_id = target_room(request.conversation.as_deref(), request.channel.as_deref())?;
    let message_id = send_message(
        root,
        SendMessageInput {
            conversation_id,
            sender: request.agent,
            to: request.to,
            subject: request.subject,
            body: request.body,
            kind: request.kind,
            after: request.after,
            subject_id: request.subject_id,
            requires_ack: request.requires_ack,
            needs_response_from: request.needs_response_from.join(","),
        },
    )?;
    Ok(serde_json::json!({
        "ok": true,
        "message_id": message_id
    }))
}

fn api_open_private(root: &Path, body: &[u8]) -> Result<serde_json::Value> {
    let request: UiOpenRequest = serde_json::from_slice(body)?;
    let conversation_id = open_private_chat(root, &request.agent, &request.to, &request.topic)?;
    Ok(serde_json::json!({
        "ok": true,
        "conversation_id": conversation_id
    }))
}

fn api_create_channel(root: &Path, body: &[u8]) -> Result<serde_json::Value> {
    let request: UiChannelRequest = serde_json::from_slice(body)?;
    let channel_id = create_ui_channel(root, &request.agent, &request.channel, &request.members)?;
    Ok(serde_json::json!({
        "ok": true,
        "conversation_id": channel_id
    }))
}

fn api_join_channel(root: &Path, body: &[u8]) -> Result<serde_json::Value> {
    let request: UiJoinRequest = serde_json::from_slice(body)?;
    join_channel(root, &request.agent, &request.channel)?;
    Ok(serde_json::json!({
        "ok": true,
        "channel": request.channel
    }))
}

fn open_private_chat(root: &Path, opener: &str, to: &str, topic: &str) -> Result<String> {
    let opener = validate_id(opener, "agent id")?;
    let mut participants = vec![opener.clone()];
    participants.extend(split_csv(to)?);
    let participants = unique(participants);
    if participants.len() < 2 {
        bail!("a private chat needs at least two unique participants");
    }
    ensure_root(root)?;
    let participant_set = participants.iter().cloned().collect::<BTreeSet<_>>();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta.private || meta.channel {
            continue;
        }
        let existing_set = meta.participants.iter().cloned().collect::<BTreeSet<_>>();
        if existing_set == participant_set {
            return Ok(meta.id);
        }
    }
    let conversation_id = generated_private_conversation_id(&participants, topic);
    create_conversation_record(
        root,
        &conversation_id,
        participants,
        opener.clone(),
        true,
        false,
    )?;
    Ok(conversation_id)
}

fn create_ui_channel(root: &Path, creator: &str, channel: &str, members: &str) -> Result<String> {
    let creator = validate_id(creator, "agent id")?;
    let channel_id = validate_id(channel, "channel id")?;
    let mut participants = vec![creator.clone()];
    participants.extend(split_csv(members)?);
    let participants = unique(participants);
    ensure_root(root)?;
    let conv = conversation_path(root, &channel_id)?;
    if conv.join("meta.json").exists() {
        let meta: Meta = read_json(&conv.join("meta.json"))?
            .ok_or_else(|| RaftError(format!("channel {channel_id:?} has no metadata")))?;
        if !meta.channel {
            bail!("{channel_id:?} already exists but is not a channel");
        }
        join_channel(root, &creator, &channel_id)?;
        return Ok(channel_id);
    }
    create_conversation_record(root, &channel_id, participants, creator, false, true)?;
    Ok(channel_id)
}

fn join_channel(root: &Path, agent: &str, channel: &str) -> Result<()> {
    let agent_id = validate_id(agent, "agent id")?;
    let channel_id = validate_id(channel, "channel id")?;
    ensure_root(root)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{channel_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &channel_id)?;
    let mut meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| RaftError(format!("channel {channel_id:?} does not exist")))?;
    if !meta.channel {
        bail!("{channel_id:?} is not a channel");
    }
    if !meta
        .participants
        .iter()
        .any(|participant| participant == &agent_id)
    {
        meta.participants.push(agent_id.clone());
        meta.updated_at = iso_now();
        atomic_write_json(&conv.join("meta.json"), &meta)?;
        write_system_message(
            &conv,
            &channel_id,
            vec!["*".to_string()],
            format!("@{agent_id} joined channel {channel_id}."),
            "channel joined",
        )?;
    }
    Ok(())
}

fn create_conversation_record(
    root: &Path,
    conversation_id: &str,
    participants: Vec<String>,
    starter: String,
    private: bool,
    channel: bool,
) -> Result<()> {
    let conversation_id = validate_id(conversation_id, "conversation id")?;
    if participants.len() < 2 {
        bail!("a conversation needs at least two participants");
    }
    for participant in &participants {
        validate_id(participant, "participant")?;
    }
    if !participants.contains(&starter) {
        bail!("starter must be one of the participants");
    }
    let conv = conversation_path(root, &conversation_id)?;
    let _lock = DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    if conv.join("meta.json").exists() {
        return Ok(());
    }
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    set_dir_private(&conv)?;
    set_dir_private(&conv.join("messages"))?;
    set_dir_private(&conv.join("receipts"))?;

    let meta = Meta {
        v: SCHEMA_VERSION,
        id: conversation_id.clone(),
        participants,
        channel,
        private,
        state: "open".to_string(),
        created_at: iso_now(),
        updated_at: iso_now(),
        retention_days: 14,
        rate: Rate {
            window_seconds: DEFAULT_RATE_WINDOW_SECONDS,
            max_messages_per_sender: DEFAULT_RATE_MAX_MESSAGES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        },
    };
    atomic_write_json(&conv.join("meta.json"), &meta)?;
    let subject = if channel {
        "channel opened"
    } else {
        "conversation opened"
    };
    let body = if channel {
        format!(
            "Channel opened. Subscribers: {}.",
            meta.participants.join(",")
        )
    } else {
        format!(
            "Conversation opened by {starter}. Participants: {}.",
            meta.participants.join(",")
        )
    };
    write_system_message(&conv, &conversation_id, vec![starter], body, subject)?;
    Ok(())
}

fn http_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn http_content_length(headers: &str) -> Result<usize> {
    for line in headers.lines().skip(1) {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse::<usize>()
                .map_err(|_| RaftError("invalid Content-Length".to_string()));
        }
    }
    Ok(0)
}

fn parse_http_headers(headers: &str) -> Vec<(String, String)> {
    headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            Some((key.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

fn http_header<'a>(request: &'a HttpRequest, name: &str) -> Option<&'a str> {
    let name = name.to_ascii_lowercase();
    request
        .headers
        .iter()
        .find(|(key, _)| key == &name)
        .map(|(_, value)| value.as_str())
}

fn validate_ui_request_security(
    request: &HttpRequest,
    bind_host: &str,
    bind_port: u16,
) -> Result<()> {
    let host =
        http_header(request, "host").ok_or_else(|| RaftError("missing Host header".to_string()))?;
    let allowed_hosts = ui_allowed_hosts(bind_host, bind_port);
    if !allowed_hosts
        .iter()
        .any(|allowed| host.eq_ignore_ascii_case(allowed))
    {
        bail!("blocked request Host {host:?}");
    }
    if request.method.eq_ignore_ascii_case("POST") {
        let allowed_origins = ui_allowed_origins(&allowed_hosts);
        let origin_ok = http_header(request, "origin")
            .map(|origin| {
                allowed_origins
                    .iter()
                    .any(|allowed| origin.eq_ignore_ascii_case(allowed))
            })
            .unwrap_or(false);
        let referer_ok = http_header(request, "referer")
            .map(|referer| {
                allowed_origins.iter().any(|allowed| {
                    referer.eq_ignore_ascii_case(allowed)
                        || referer
                            .to_ascii_lowercase()
                            .starts_with(&format!("{}/", allowed.to_ascii_lowercase()))
                })
            })
            .unwrap_or(false);
        if !origin_ok && !referer_ok {
            bail!("blocked cross-origin UI write");
        }
    }
    Ok(())
}

fn ui_allowed_hosts(bind_host: &str, bind_port: u16) -> Vec<String> {
    let mut hosts = vec![
        format!("127.0.0.1:{bind_port}"),
        format!("localhost:{bind_port}"),
    ];
    let bind_host = bind_host.trim();
    if !bind_host.is_empty()
        && bind_host != "127.0.0.1"
        && bind_host != "localhost"
        && bind_host != "0.0.0.0"
        && bind_host != "::"
    {
        hosts.push(format!("{bind_host}:{bind_port}"));
    }
    hosts.sort();
    hosts.dedup();
    hosts
}

fn ui_allowed_origins(hosts: &[String]) -> Vec<String> {
    hosts.iter().map(|host| format!("http://{host}")).collect()
}

fn build_ui_snapshot(root: &Path, agent_id: &str, limit: usize) -> Result<UiSnapshot> {
    let mut agents = Vec::new();
    for entry in sorted_read_dir(&root.join("agents"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(agent): Option<Agent> = read_json(&entry.path())? else {
            continue;
        };
        let active = parse_time(&agent.expires_at)
            .map(|expires_at| expires_at >= Utc::now())
            .unwrap_or(false);
        let mention = if agent.mention.is_empty() {
            format!("@{}", agent.id)
        } else {
            agent.mention.clone()
        };
        agents.push(UiAgent {
            id: agent.id,
            mention,
            workspace: agent.workspace,
            capabilities: agent.capabilities,
            current_state: agent.current_state,
            state_note: agent.state_note,
            state_updated_at: agent.state_updated_at,
            last_seen_at: agent.last_seen_at,
            expires_at: agent.expires_at,
            active,
        });
    }

    let asks = gather_open_asks(root, None, Some(agent_id))?;
    let mut open_asks_by_conv: BTreeMap<String, usize> = BTreeMap::new();
    for ask in &asks {
        *open_asks_by_conv.entry(ask.conversation_id.clone()).or_default() += 1;
    }

    let mut conversations = Vec::new();
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        let joined = meta
            .participants
            .iter()
            .any(|participant| participant == agent_id);
        if !joined && (!meta.channel || meta.private) {
            continue;
        }
        let mut messages = Vec::new();
        if joined {
            for message_entry in sorted_read_dir(&conv.join("messages"))? {
                if message_entry.path().extension() != Some(OsStr::new("json")) {
                    continue;
                }
                let Some(message): Option<Message> = read_json(&message_entry.path())? else {
                    continue;
                };
                if !message_visible_to(&message, agent_id) {
                    continue;
                }
                let unread = message_is_unread(root, &message, agent_id);
                messages.push(UiMessage {
                    id: message.id,
                    kind: message.kind,
                    from: message.from,
                    to: message.to,
                    mentions: message.mentions,
                    subject: message.subject,
                    body: message.body,
                    created_at: message.created_at,
                    requires_ack: message.requires_ack,
                    needs_response_from: message.needs_response_from,
                    unread,
                    after: message.after,
                });
            }
        }
        messages.sort_by(|left, right| left.id.cmp(&right.id));
        let message_count = messages.len();
        let unread_count = messages.iter().filter(|message| message.unread).count();
        let latest_at = messages.last().map(|message| message.created_at.clone());
        if messages.len() > limit {
            messages = messages.split_off(messages.len() - limit);
        }
        conversations.push(UiConversation {
            id: meta.id.clone(),
            participants: meta.participants,
            channel: meta.channel,
            private: meta.private,
            joined,
            message_count,
            unread_count,
            open_asks: open_asks_by_conv.get(&meta.id).copied().unwrap_or(0),
            latest_at,
            messages,
        });
    }
    conversations.sort_by(|left, right| {
        right
            .latest_at
            .cmp(&left.latest_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    let active_agents = agents.iter().filter(|agent| agent.active).count();
    let unread_messages = conversations
        .iter()
        .map(|conversation| conversation.unread_count)
        .sum();
    let message_total = conversations
        .iter()
        .map(|conversation| conversation.message_count)
        .sum();
    Ok(UiSnapshot {
        root: root.display().to_string(),
        agent: agent_id.to_string(),
        generated_at: iso_now(),
        totals: UiTotals {
            active_agents,
            stale_agents: agents.len().saturating_sub(active_agents),
            conversations: conversations.len(),
            unread_messages,
            messages: message_total,
        },
        agents,
        conversations,
    })
}

fn split_http_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (target, None),
    }
}

fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|part| {
        let (key, value) = part.split_once('=').unwrap_or((part, ""));
        if percent_decode(key) == name {
            Some(percent_decode(value))
        } else {
            None
        }
    })
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = String::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'%' if index + 2 < bytes.len() => {
                let hex = &value[index + 1..index + 3];
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    output.push(byte as char);
                    index += 3;
                    continue;
                }
                output.push('%');
                index += 1;
            }
            b'+' => {
                output.push(' ');
                index += 1;
            }
            byte => {
                output.push(byte as char);
                index += 1;
            }
        }
    }
    output
}

fn write_http_text(stream: &mut TcpStream, status: u16, body: &str) -> Result<()> {
    write_http_response(stream, status, "text/plain; charset=utf-8", body.as_bytes())
}

fn write_http_json<T: Serialize>(stream: &mut TcpStream, status: u16, payload: &T) -> Result<()> {
    let body = serde_json::to_vec_pretty(payload)?;
    write_http_response(stream, status, "application/json; charset=utf-8", &body)
}

fn write_http_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };
    let headers = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}


fn load_conversation(root: &Path, conversation_id: &str) -> Result<(PathBuf, Meta)> {
    let conv = conversation_path(root, conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?
        .ok_or_else(|| RaftError(format!("conversation {conversation_id:?} does not exist")))?;
    Ok((conv, meta))
}

fn enforce_rate_limit(
    conv: &Path,
    meta: &Meta,
    sender: &str,
    subject_id: Option<&str>,
    body: &str,
) -> Result<()> {
    let size = body.len();
    if size > meta.rate.max_message_bytes {
        bail!(
            "message is {size} bytes; limit is {}",
            meta.rate.max_message_bytes
        );
    }
    let path = conv.join("rate.json");
    let mut state: RateState = read_json(&path)?.unwrap_or_default();
    let now = Utc::now();
    let rate_key = rate_key(sender, subject_id);
    let entry = state
        .senders
        .entry(rate_key.clone())
        .or_insert_with(|| SenderRate {
            window_start: iso_now(),
            count: 0,
            last_sent_at: None,
        });
    let window_start = parse_time(&entry.window_start).unwrap_or(now);
    if (now - window_start).num_seconds() >= meta.rate.window_seconds as i64 {
        entry.window_start = iso_now();
        entry.count = 0;
    }
    if entry.count >= meta.rate.max_messages_per_sender {
        bail!(
            "rate limited: {rate_key:?} already sent {} messages in {}s for {:?}",
            meta.rate.max_messages_per_sender,
            meta.rate.window_seconds,
            meta.id
        );
    }
    entry.count += 1;
    entry.last_sent_at = Some(iso_now());
    atomic_write_json(&path, &state)?;
    Ok(())
}

fn visible_messages(
    root: &Path,
    agent_id: &str,
    conversation_id: Option<&str>,
) -> Result<Vec<Message>> {
    let mut messages = Vec::new();
    let conversation_dirs = if let Some(conversation_id) = conversation_id {
        vec![conversation_path(root, conversation_id)?]
    } else {
        sorted_read_dir(&root.join("conversations"))?
            .into_iter()
            .map(|entry| entry.path())
            .collect()
    };
    for conv in conversation_dirs {
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta.participants.iter().any(|item| item == agent_id) {
            continue;
        }
        for entry in sorted_read_dir(&conv.join("messages"))? {
            if entry.path().extension() != Some(OsStr::new("json")) {
                continue;
            }
            let Some(message): Option<Message> = read_json(&entry.path())? else {
                continue;
            };
            if message_visible_to(&message, agent_id) {
                messages.push(message);
            }
        }
    }
    Ok(messages)
}

fn message_visible_to(message: &Message, agent_id: &str) -> bool {
    message.from == agent_id
        || message
            .to
            .iter()
            .any(|item| item == "*" || item == agent_id)
}

fn message_is_unread(root: &Path, message: &Message, agent_id: &str) -> bool {
    if message.kind == "system" || message.kind == "receipt" {
        return false;
    }
    if message.from == agent_id {
        return false;
    }
    !receipt_path_for(root, message, agent_id).exists()
}

fn is_state_change_message(message: &Message) -> bool {
    message.kind == "system" && message.subject == "state changed"
}

fn start_watch_state(root: &Path, agent_id: &str, since: Option<String>) -> Result<WatchState> {
    let previous: Option<WatchState> = read_json(&watch_state_path(root, agent_id))?;
    let now = iso_now();
    let last_event_id = since.or_else(|| previous.and_then(|state| state.last_event_id));
    let state = WatchState {
        v: SCHEMA_VERSION,
        agent: agent_id.to_string(),
        pid: process::id(),
        host: hostname(),
        started_at: now.clone(),
        updated_at: now,
        last_event_id,
        shutdown_at: None,
    };
    atomic_write_json(&watch_state_path(root, agent_id), &state)?;
    Ok(state)
}

fn emit_watch_message(message: &Message, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(message)?);
    } else {
        println!(
            "{} {} {} -> {} [{}] {} {}",
            message.id,
            message.conversation_id,
            message.from,
            message.to.join(","),
            message.kind,
            message.subject,
            truncated_body(&message.body, 120)
        );
    }
    io::stdout().flush()?;
    Ok(())
}

fn truncated_body(body: &str, width: usize) -> String {
    let mut body = body.split_whitespace().collect::<Vec<_>>().join(" ");
    if body.len() > width {
        if width <= 3 {
            let mut end = width.min(body.len());
            while !body.is_char_boundary(end) {
                end -= 1;
            }
            body.truncate(end);
            return body;
        }
        let mut end = (width - 3).min(body.len());
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        body.push_str("...");
    }
    body
}

fn build_thread_node(
    message_id: &str,
    messages: &[Message],
    remaining: &mut usize,
    visited: &mut BTreeSet<String>,
) -> Result<ThreadNode> {
    let message = messages
        .iter()
        .find(|message| message.id == message_id)
        .ok_or_else(|| {
            RaftError(format!(
                "message {message_id:?} was not found in visible thread"
            ))
        })?;
    if *remaining == 0 || !visited.insert(message.id.clone()) {
        return Ok(ThreadNode {
            message: message.clone(),
            children: Vec::new(),
        });
    }
    *remaining -= 1;
    let mut children = Vec::new();
    for child in messages
        .iter()
        .filter(|candidate| candidate.after.as_deref() == Some(message_id))
    {
        if *remaining == 0 {
            break;
        }
        if visited.contains(&child.id) {
            continue;
        }
        children.push(build_thread_node(&child.id, messages, remaining, visited)?);
    }
    Ok(ThreadNode {
        message: message.clone(),
        children,
    })
}

fn print_thread_node(node: &ThreadNode, depth: usize) {
    let indent = "  ".repeat(depth);
    println!(
        "{}{} {} -> {} [{}] {}",
        indent,
        node.message.id,
        node.message.from,
        node.message.to.join(","),
        node.message.kind,
        node.message.subject
    );
    println!("{}  {}", indent, truncated_body(&node.message.body, 160));
    for child in &node.children {
        print_thread_node(child, depth + 1);
    }
}

fn message_matches_pattern(message: &Message, pattern: &str) -> bool {
    let haystack = format!(
        "{}\n{}\n{}\n{}\n{}",
        message.id, message.conversation_id, message.from, message.subject, message.body
    )
    .to_lowercase();
    haystack.contains(pattern)
}

fn parse_since_cutoff(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(time) = parse_time(value) {
        return Ok(time);
    }
    let Some((number, unit)) = value.split_at_checked(value.len().saturating_sub(1)) else {
        bail!("invalid --since {value:?}; use RFC3339 or a duration like 30m, 2h, 7d");
    };
    let amount: i64 = number.parse().map_err(|_| {
        RaftError(format!(
            "invalid --since {value:?}; duration must be numeric"
        ))
    })?;
    if amount < 0 {
        bail!("invalid --since {value:?}; duration must be non-negative");
    }
    let delta = match unit {
        "s" => TimeDelta::seconds(amount),
        "m" => TimeDelta::minutes(amount),
        "h" => TimeDelta::hours(amount),
        "d" => TimeDelta::days(amount),
        _ => bail!("invalid --since {value:?}; use s, m, h, or d duration suffix"),
    };
    Ok(Utc::now() - delta)
}

fn find_message(root: &Path, message_id: &str) -> Result<(PathBuf, Message)> {
    let message_id = validate_id(message_id, "message id")?;
    for conv_entry in sorted_read_dir(&root.join("conversations"))? {
        let path = conv_entry
            .path()
            .join("messages")
            .join(format!("{message_id}.json"));
        if path.exists() {
            let message: Message = read_json(&path)?.ok_or_else(|| {
                RaftError(format!("message file disappeared: {}", path.display()))
            })?;
            return Ok((path, message));
        }
    }
    bail!("message {message_id:?} was not found");
}

fn receipt_recipients(message: &Message, meta: &Meta) -> Vec<String> {
    if message.to.iter().any(|recipient| recipient == "*") {
        return meta
            .participants
            .iter()
            .filter(|participant| *participant != &message.from)
            .cloned()
            .collect();
    }
    message
        .to
        .iter()
        .filter(|recipient| *recipient != &message.from)
        .cloned()
        .collect()
}

fn load_message_receipts(root: &Path, message: &Message) -> Result<BTreeMap<String, Receipt>> {
    let mut receipts = BTreeMap::new();
    let dir = root
        .join("conversations")
        .join(&message.conversation_id)
        .join("receipts")
        .join(&message.id);
    for entry in sorted_read_dir(&dir)? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(receipt): Option<Receipt> = read_json(&entry.path())? else {
            continue;
        };
        receipts.insert(receipt.agent.clone(), receipt);
    }
    Ok(receipts)
}

fn write_receipt(
    root: &Path,
    agent_id: &str,
    message: &Message,
    status: &str,
    note: Option<String>,
) -> Result<()> {
    let conv = conversation_path(root, &message.conversation_id)?;
    let meta: Meta = read_json(&conv.join("meta.json"))?.ok_or_else(|| {
        RaftError(format!(
            "conversation {:?} does not exist",
            message.conversation_id
        ))
    })?;
    ensure_participant(&meta, agent_id)?;
    if !message_visible_to(message, agent_id) {
        bail!("message {:?} is not visible to {agent_id:?}", message.id);
    }
    let path = receipt_path_for(root, message, agent_id);
    let existing: Option<Receipt> = read_json(&path)?;
    let mut receipt = existing.unwrap_or_else(|| Receipt {
        v: SCHEMA_VERSION,
        message_id: message.id.clone(),
        conversation_id: message.conversation_id.clone(),
        agent: agent_id.to_string(),
        status: status.to_string(),
        updated_at: iso_now(),
        note: note.clone(),
        read_at: None,
        history: Vec::new(),
    });
    receipt.history.push(ReceiptEvent {
        status: status.to_string(),
        at: iso_now(),
        note: note.clone(),
    });
    receipt.status = status.to_string();
    receipt.updated_at = iso_now();
    receipt.note = note;
    if status == "read" && receipt.read_at.is_none() {
        receipt.read_at = Some(iso_now());
    }
    atomic_write_json(&path, &receipt)?;
    Ok(())
}

fn archive_old_messages(_root: &Path, conv: &Path, meta: &Meta) -> Result<usize> {
    let cutoff = Utc::now() - TimeDelta::days(meta.retention_days as i64);
    let archive_dir = conv
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| RaftError("invalid conversation path".to_string()))?
        .join("archive")
        .join(&meta.id);
    let mut archived = 0;
    for entry in sorted_read_dir(&conv.join("messages"))? {
        if entry.path().extension() != Some(OsStr::new("json")) {
            continue;
        }
        let Some(message): Option<Message> = read_json(&entry.path())? else {
            continue;
        };
        let created_at = parse_time(&message.created_at).unwrap_or_else(|_| Utc::now());
        if created_at >= cutoff {
            continue;
        }
        fs::create_dir_all(&archive_dir)?;
        set_dir_private(&archive_dir)?;
        let mut target = archive_dir.join(entry.file_name());
        if target.exists() {
            target = archive_dir.join(format!("{}-{}.json", message.id, unique_token()));
        }
        fs::rename(entry.path(), &target)?;
        archive_message_receipts(conv, &archive_dir, &message, &target)?;
        archived += 1;
    }
    Ok(archived)
}

fn archive_message_receipts(
    conv: &Path,
    archive_dir: &Path,
    message: &Message,
    archived_message_path: &Path,
) -> Result<()> {
    let source = conv.join("receipts").join(&message.id);
    if !source.exists() {
        return Ok(());
    }
    let receipts_dir = archive_dir.join("receipts");
    fs::create_dir_all(&receipts_dir)?;
    set_dir_private(&receipts_dir)?;
    let stem = archived_message_path
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| RaftError("invalid archived message path".to_string()))?;
    let mut target = receipts_dir.join(stem);
    if target.exists() {
        target = receipts_dir.join(format!("{stem}-{}", unique_token()));
    }
    fs::rename(source, target)?;
    Ok(())
}

fn write_state_change_messages(
    root: &Path,
    agent_id: &str,
    state: &str,
    note: Option<&str>,
) -> Result<()> {
    let body = match note {
        Some(note) if !note.is_empty() => format!("@{agent_id} is now {state}: {note}"),
        _ => format!("@{agent_id} is now {state}"),
    };
    for entry in sorted_read_dir(&root.join("conversations"))? {
        let conv = entry.path();
        if !conv.is_dir() {
            continue;
        }
        let Some(meta): Option<Meta> = read_json(&conv.join("meta.json"))? else {
            continue;
        };
        if !meta
            .participants
            .iter()
            .any(|participant| participant == agent_id)
        {
            continue;
        }
        let _lock = DirLock::acquire(
            root,
            &format!("conversation-{}", meta.id),
            LOCK_TTL_SECONDS,
            LOCK_TIMEOUT_SECONDS,
        )?;
        write_system_message(
            &conv,
            &meta.id,
            vec!["*".to_string()],
            body.clone(),
            "state changed",
        )?;
    }
    Ok(())
}

fn write_system_message(
    conv: &Path,
    conversation_id: &str,
    to: Vec<String>,
    body: String,
    subject: &str,
) -> Result<String> {
    let message_id = new_message_id();
    let message = Message {
        v: SCHEMA_VERSION,
        id: message_id.clone(),
        conversation_id: conversation_id.to_string(),
        kind: "system".to_string(),
        from: "raft".to_string(),
        to,
        mentions: Vec::new(),
        subject: subject.to_string(),
        body,
        created_at: iso_now(),
        requires_ack: false,
        needs_response_from: Vec::new(),
        subject_id: None,
        after: None,
    };
    atomic_write_json(
        &conv.join("messages").join(format!("{message_id}.json")),
        &message,
    )?;
    Ok(message_id)
}

fn ensure_participant(meta: &Meta, agent_id: &str) -> Result<()> {
    if !meta.participants.iter().any(|item| item == agent_id) {
        bail!("agent {agent_id:?} is not a participant in {:?}", meta.id);
    }
    Ok(())
}

fn ensure_recipients(meta: &Meta, recipients: &[String]) -> Result<()> {
    for recipient in recipients {
        if recipient != "*" && !meta.participants.iter().any(|item| item == recipient) {
            bail!(
                "recipient {recipient:?} is not a participant in {:?}",
                meta.id
            );
        }
    }
    Ok(())
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

fn mentioned_participants(meta: &Meta, subject: &str, body: &str) -> Vec<String> {
    let mut mentions = BTreeSet::new();
    for mention in extract_mentions(subject)
        .into_iter()
        .chain(extract_mentions(body))
    {
        if meta
            .participants
            .iter()
            .any(|participant| participant == &mention)
        {
            mentions.insert(mention);
        }
    }
    mentions.into_iter().collect()
}
