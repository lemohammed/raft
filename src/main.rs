use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use clap::{Args, Parser, Subcommand};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::flag as signal_flag;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_RATE_WINDOW_SECONDS: u64 = 60;
const DEFAULT_RATE_MAX_MESSAGES: u64 = 10;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 32_768;
const DEFAULT_AGENT_TTL_SECONDS: u64 = 120;
const LOCK_TTL_SECONDS: u64 = 30;
const LOCK_TIMEOUT_SECONDS: u64 = 5;
const SERVE_LOCK_TTL_SECONDS: u64 = 30;
const SCHEMA_VERSION: u16 = 1;

type Result<T> = std::result::Result<T, RaftError>;

#[derive(Debug)]
struct RaftError(String);

impl std::fmt::Display for RaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RaftError {}

impl From<io::Error> for RaftError {
    fn from(value: io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<serde_json::Error> for RaftError {
    fn from(value: serde_json::Error) -> Self {
        Self(value.to_string())
    }
}

macro_rules! bail {
    ($($arg:tt)*) => {
        return Err(RaftError(format!($($arg)*)))
    };
}

#[derive(Parser)]
#[command(name = "raft")]
#[command(version)]
#[command(about = "Filesystem-backed agent-to-agent coordination bus.")]
struct Cli {
    #[arg(long)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Claim(ClaimArgs),
    Register(RegisterArgs),
    Heartbeat(HeartbeatArgs),
    State {
        #[command(subcommand)]
        command: StateCommand,
    },
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    Conversation {
        #[command(subcommand)]
        command: ConversationCommand,
    },
    Send(SendArgs),
    Awaiting(AwaitingArgs),
    Roster(RosterArgs),
    Inbox(InboxArgs),
    Wait(WaitArgs),
    Watch(WatchArgs),
    Show(ShowArgs),
    Search(SearchArgs),
    Thread(ThreadArgs),
    Read(ReadArgs),
    Ack(AckArgs),
    Receipts(ReceiptsArgs),
    Journal(JournalArgs),
    Status(StatusArgs),
    Doctor(DoctorArgs),
    Gc(GcArgs),
    Serve(ServeArgs),
    Ui(UiArgs),
}

#[derive(Args)]
struct ClaimArgs {
    agent: String,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long, default_value = "")]
    capabilities: String,
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    ttl: u64,
}

#[derive(Args)]
struct RegisterArgs {
    agent: String,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long, default_value = "")]
    capabilities: String,
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    ttl: u64,
}

#[derive(Args)]
struct HeartbeatArgs {
    agent: String,
    #[arg(long)]
    ttl: Option<u64>,
    #[arg(long)]
    watch: bool,
    #[arg(long)]
    interval: Option<f64>,
}

#[derive(Subcommand)]
enum StateCommand {
    Set(StateSetArgs),
    Get(StateGetArgs),
}

#[derive(Args)]
struct StateSetArgs {
    agent: String,
    state: String,
    #[arg(long)]
    note: Option<String>,
}

#[derive(Args)]
struct StateGetArgs {
    agent: String,
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
enum ChannelCommand {
    Create(ChannelCreateArgs),
    Join(ChannelJoinArgs),
}

#[derive(Args)]
struct ChannelCreateArgs {
    channel: String,
    #[arg(long)]
    creator: String,
    #[arg(long, default_value = "")]
    members: String,
    #[arg(long = "if-missing")]
    if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,
}

#[derive(Args)]
struct ChannelJoinArgs {
    channel: String,
    #[arg(long)]
    agent: String,
}

#[derive(Subcommand)]
enum ConversationCommand {
    Create(ConversationCreateArgs),
    Open(ConversationOpenArgs),
}

#[derive(Args)]
struct ConversationCreateArgs {
    conversation: String,
    #[arg(long)]
    participants: String,
    #[arg(long)]
    starter: Option<String>,
    #[arg(long)]
    private: bool,
    #[arg(long = "if-missing")]
    if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,
}

#[derive(Args)]
struct ConversationOpenArgs {
    #[arg(long = "id")]
    conversation: Option<String>,
    #[arg(long = "from")]
    opener: String,
    #[arg(long)]
    to: String,
    #[arg(long, default_value = "")]
    topic: String,
    #[arg(long = "if-missing")]
    if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    max_message_bytes: usize,
}

#[derive(Args)]
struct SendArgs {
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long = "from")]
    sender: String,
    #[arg(long)]
    to: String,
    #[arg(long, default_value = "")]
    subject: String,
    #[arg(long)]
    body: String,
    #[arg(long, default_value = "message")]
    kind: String,
    #[arg(long)]
    after: Option<String>,
    #[arg(long = "subject-id")]
    subject_id: Option<String>,
    #[arg(long = "requires-ack")]
    requires_ack: bool,
    #[arg(long = "needs-response-from", default_value = "")]
    needs_response_from: String,
}

#[derive(Args)]
struct AwaitingArgs {
    agent: String,
    #[arg(long, conflicts_with = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct RosterArgs {
    #[arg(long)]
    all: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct InboxArgs {
    agent: String,
    #[arg(long, conflicts_with = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long)]
    unread: bool,
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long, default_value_t = 120)]
    width: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct WaitArgs {
    agent: String,
    #[arg(long, conflicts_with = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long, default_value_t = 300)]
    timeout: u64,
    #[arg(long, default_value_t = 2.0)]
    interval: f64,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct WatchArgs {
    #[arg(long)]
    agent: String,
    #[arg(long, conflicts_with = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long)]
    since: Option<String>,
    #[arg(long, default_value_t = 0)]
    timeout: u64,
    #[arg(long, default_value_t = 1.0)]
    interval: f64,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    json: bool,
    #[arg(long = "no-auto-read")]
    no_auto_read: bool,
    #[arg(long = "state-changes")]
    state_changes: bool,
}

#[derive(Args)]
struct ShowArgs {
    #[arg(long)]
    agent: String,
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long, default_value_t = 50)]
    limit: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct SearchArgs {
    pattern: String,
    #[arg(long)]
    agent: String,
    #[arg(long, conflicts_with = "channel")]
    conversation: Option<String>,
    #[arg(long)]
    channel: Option<String>,
    #[arg(long)]
    since: Option<String>,
    #[arg(long, default_value_t = 20)]
    limit: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct ThreadArgs {
    message_id: String,
    #[arg(long)]
    agent: String,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct ReadArgs {
    agent: String,
    message_id: String,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct AckArgs {
    agent: String,
    message_id: String,
    #[arg(long, default_value = "done")]
    status: String,
    #[arg(long)]
    note: Option<String>,
}

#[derive(Args)]
struct ReceiptsArgs {
    message_id: String,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct JournalArgs {
    agent: String,
    #[arg(long, default_value = "note")]
    kind: String,
    #[arg(long, default_value = "")]
    subject: String,
    #[arg(long)]
    body: String,
}

#[derive(Args)]
struct StatusArgs {
    #[arg(long)]
    agent: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct DoctorArgs {
    #[arg(long)]
    json: bool,
    #[arg(long)]
    strict: bool,
}

#[derive(Args, Clone, Copy)]
struct GcArgs {
    #[arg(long)]
    archive: bool,
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, default_value_t = 2.0)]
    interval: f64,
    #[arg(long)]
    archive: bool,
}

#[derive(Args)]
struct UiArgs {
    #[arg(long, default_value = "codex")]
    agent: String,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 7420)]
    port: u16,
    #[arg(long, default_value_t = 80)]
    limit: usize,
}

#[derive(Serialize, Deserialize, Clone)]
struct Agent {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    id: String,
    #[serde(default)]
    mention: String,
    workspace: Option<String>,
    capabilities: Vec<String>,
    pid: u32,
    host: String,
    last_seen_at: String,
    ttl_seconds: u64,
    expires_at: String,
    #[serde(default = "default_agent_state")]
    current_state: String,
    #[serde(default)]
    state_note: Option<String>,
    #[serde(default = "iso_now")]
    state_updated_at: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Rate {
    window_seconds: u64,
    max_messages_per_sender: u64,
    max_message_bytes: usize,
}

#[derive(Serialize, Deserialize, Clone)]
struct Meta {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    id: String,
    participants: Vec<String>,
    #[serde(default)]
    channel: bool,
    private: bool,
    state: String,
    created_at: String,
    updated_at: String,
    retention_days: u64,
    rate: Rate,
}

#[derive(Serialize, Deserialize, Clone)]
struct Message {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    id: String,
    conversation_id: String,
    kind: String,
    from: String,
    to: Vec<String>,
    #[serde(default)]
    mentions: Vec<String>,
    subject: String,
    body: String,
    created_at: String,
    requires_ack: bool,
    #[serde(default)]
    needs_response_from: Vec<String>,
    #[serde(default)]
    subject_id: Option<String>,
    after: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct RateState {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    senders: BTreeMap<String, SenderRate>,
}

impl Default for RateState {
    fn default() -> Self {
        Self {
            v: SCHEMA_VERSION,
            senders: BTreeMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SenderRate {
    window_start: String,
    count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_sent_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct ReceiptEvent {
    status: String,
    at: String,
    note: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Receipt {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    message_id: String,
    conversation_id: String,
    agent: String,
    status: String,
    updated_at: String,
    note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_at: Option<String>,
    history: Vec<ReceiptEvent>,
}

#[derive(Serialize, Deserialize, Clone)]
struct WatchState {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    agent: String,
    pid: u32,
    host: String,
    started_at: String,
    updated_at: String,
    last_event_id: Option<String>,
    shutdown_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct HeartbeatState {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    agent: String,
    pid: u32,
    host: String,
    started_at: String,
    updated_at: String,
    last_heartbeat_at: String,
    interval_seconds: f64,
    ttl_seconds: u64,
    shutdown_at: Option<String>,
}

#[derive(Serialize)]
struct ThreadNode {
    message: Message,
    children: Vec<ThreadNode>,
}

#[derive(Serialize, Deserialize)]
struct LockOwner {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    token: String,
    pid: u32,
    host: String,
    acquired_at: String,
    expires_at: String,
}

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

#[derive(Serialize)]
struct UiSnapshot {
    root: String,
    agent: String,
    generated_at: String,
    totals: UiTotals,
    agents: Vec<UiAgent>,
    conversations: Vec<UiConversation>,
}

#[derive(Serialize)]
struct UiTotals {
    active_agents: usize,
    stale_agents: usize,
    conversations: usize,
    unread_messages: usize,
    messages: usize,
}

#[derive(Serialize)]
struct UiAgent {
    id: String,
    mention: String,
    workspace: Option<String>,
    capabilities: Vec<String>,
    current_state: String,
    state_note: Option<String>,
    state_updated_at: String,
    last_seen_at: String,
    expires_at: String,
    active: bool,
}

#[derive(Serialize)]
struct UiConversation {
    id: String,
    participants: Vec<String>,
    channel: bool,
    private: bool,
    joined: bool,
    message_count: usize,
    unread_count: usize,
    open_asks: usize,
    latest_at: Option<String>,
    messages: Vec<UiMessage>,
}

#[derive(Serialize)]
struct UiMessage {
    id: String,
    kind: String,
    from: String,
    to: Vec<String>,
    mentions: Vec<String>,
    subject: String,
    body: String,
    created_at: String,
    requires_ack: bool,
    needs_response_from: Vec<String>,
    unread: bool,
    after: Option<String>,
}

#[derive(Deserialize)]
struct UiSendRequest {
    agent: String,
    conversation: Option<String>,
    channel: Option<String>,
    to: String,
    #[serde(default)]
    subject: String,
    body: String,
    #[serde(default = "default_message_kind")]
    kind: String,
    #[serde(default)]
    requires_ack: bool,
    #[serde(default)]
    needs_response_from: Vec<String>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    subject_id: Option<String>,
}

#[derive(Deserialize)]
struct UiOpenRequest {
    agent: String,
    to: String,
    #[serde(default)]
    topic: String,
}

#[derive(Deserialize)]
struct UiChannelRequest {
    agent: String,
    channel: String,
    #[serde(default)]
    members: String,
}

#[derive(Deserialize)]
struct UiJoinRequest {
    agent: String,
    channel: String,
}

struct SendMessageInput {
    conversation_id: String,
    sender: String,
    to: String,
    subject: String,
    body: String,
    kind: String,
    after: Option<String>,
    subject_id: Option<String>,
    requires_ack: bool,
    needs_response_from: String,
}

struct HttpRequest {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

#[derive(Serialize)]
struct JournalEntry {
    #[serde(rename = "_v")]
    v: u16,
    id: String,
    agent: String,
    kind: String,
    subject: String,
    body: String,
    created_at: String,
}

struct DirLock {
    root: PathBuf,
    path: PathBuf,
    token: String,
    acquired: bool,
}

impl DirLock {
    fn acquire(root: &Path, name: &str, ttl_seconds: u64, timeout_seconds: u64) -> Result<Self> {
        ensure_root(root)?;
        validate_id(name, "lock name")?;
        let path = root.join("locks").join(format!("{name}.lock"));
        let token = unique_token();
        let deadline = Instant::now() + Duration::from_secs(timeout_seconds);

        loop {
            match fs::create_dir(&path) {
                Ok(()) => {
                    set_dir_private(&path)?;
                    let owner = LockOwner {
                        v: SCHEMA_VERSION,
                        token: token.clone(),
                        pid: process::id(),
                        host: hostname(),
                        acquired_at: iso_now(),
                        expires_at: iso_after(ttl_seconds),
                    };
                    atomic_write_json(&path.join("owner.json"), &owner)?;
                    return Ok(Self {
                        root: root.to_path_buf(),
                        path,
                        token,
                        acquired: true,
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                    if lock_is_stale(&path)? {
                        let _ = fs::remove_dir_all(&path);
                        let _ = fsync_dir(&root.join("locks"));
                        continue;
                    }
                    if Instant::now() >= deadline {
                        bail!("timed out waiting for lock {name:?}");
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    fn refresh(&self, ttl_seconds: u64) -> Result<()> {
        let owner: Option<LockOwner> = read_json(&self.path.join("owner.json"))?;
        let Some(owner) = owner else {
            bail!("lock {} disappeared", self.path.display());
        };
        if owner.token != self.token {
            bail!(
                "lock {} is no longer owned by this process",
                self.path.display()
            );
        }
        let refreshed = LockOwner {
            v: SCHEMA_VERSION,
            token: self.token.clone(),
            pid: process::id(),
            host: hostname(),
            acquired_at: owner.acquired_at,
            expires_at: iso_after(ttl_seconds),
        };
        atomic_write_json(&self.path.join("owner.json"), &refreshed)
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        if !self.acquired {
            return;
        }
        let owner: Option<LockOwner> = read_json(&self.path.join("owner.json")).ok().flatten();
        if owner.map(|item| item.token == self.token).unwrap_or(false) {
            let _ = fs::remove_dir_all(&self.path);
            let _ = fsync_dir(&self.root.join("locks"));
        }
        self.acquired = false;
    }
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

#[derive(Serialize, Clone)]
struct OpenAsk {
    conversation_id: String,
    message_id: String,
    from: String,
    awaited: String,
    subject: String,
    created_at: String,
    status: String,
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

const UI_HTML: &str = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>raft</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --panel-soft: #f0f6f4;
      --ink: #171a1f;
      --muted: #69727e;
      --line: #dce1e7;
      --accent: #0b6f6b;
      --accent-ink: #ffffff;
      --warn: #9a5a00;
      --warn-bg: #fff5de;
      --event: #5d4b9c;
      --error: #b23b3b;
      --shadow: 0 12px 30px rgba(23, 26, 31, 0.08);
    }

    * { box-sizing: border-box; }
    [hidden] { display: none !important; }
    body {
      margin: 0;
      height: 100vh;
      overflow: hidden;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    button, input, textarea, select { font: inherit; }
    button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      color: var(--ink);
      cursor: pointer;
      font-weight: 700;
    }
    button:hover { border-color: #a8b2bd; }
    button:focus-visible, input:focus-visible, textarea:focus-visible, select:focus-visible {
      outline: 3px solid rgba(11, 111, 107, 0.18);
      outline-offset: 1px;
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      color: var(--ink);
      padding: 9px 10px;
      outline: 0;
    }
    textarea {
      resize: none;
      min-height: 52px;
      max-height: 160px;
      line-height: 1.45;
    }
    .app {
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr);
      height: 100vh;
      min-width: 0;
    }
    .rooms {
      display: flex;
      flex-direction: column;
      min-width: 0;
      border-right: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.78);
    }
    .rooms-head {
      padding: 16px;
      border-bottom: 1px solid var(--line);
      display: grid;
      gap: 12px;
    }
    .brand {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
    }
    .brand h1 {
      margin: 0;
      font-size: 22px;
      line-height: 1.05;
    }
    .brand p {
      margin: 3px 0 0;
      color: var(--muted);
      font-size: 12px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 8px;
      background: var(--panel-soft);
      color: var(--muted);
      font-size: 11px;
      font-weight: 750;
      white-space: nowrap;
    }
    .pill.unread {
      border-color: rgba(154, 90, 0, 0.28);
      background: var(--warn-bg);
      color: var(--warn);
    }
    .pill.error {
      border-color: rgba(178, 59, 59, 0.28);
      background: #fff0f0;
      color: var(--error);
    }
    .field {
      display: grid;
      gap: 5px;
    }
    .field label {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
    }
    .row {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
    }
    .btn {
      padding: 7px 11px;
      font-size: 13px;
    }
    .btn.primary {
      border-color: var(--accent);
      background: var(--accent);
      color: var(--accent-ink);
    }
    .btn.ghost {
      background: transparent;
    }
    details {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
    }
    summary {
      cursor: pointer;
      padding: 9px 11px;
      font-size: 13px;
      font-weight: 750;
    }
    .new-room {
      padding: 0 11px 11px;
      display: grid;
      gap: 8px;
    }
    .presence-panel {
      border-bottom: 1px solid var(--line);
      padding: 10px 8px;
      display: grid;
      gap: 8px;
    }
    .presence-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 0 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
    }
    .presence-list {
      display: grid;
      gap: 6px;
    }
    .presence-agent {
      width: 100%;
      display: grid;
      gap: 4px;
      padding: 9px 10px;
      text-align: left;
      background: var(--panel);
    }
    .presence-agent:disabled {
      cursor: default;
      opacity: 1;
    }
    .presence-main {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
      font-weight: 800;
    }
    .presence-name {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .presence-note {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .room-list {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 8px;
      display: grid;
      align-content: start;
      gap: 6px;
    }
    .room {
      width: 100%;
      display: grid;
      gap: 5px;
      padding: 10px;
      text-align: left;
      background: transparent;
    }
    .room.active {
      border-color: var(--accent);
      background: var(--panel);
      box-shadow: 0 0 0 3px rgba(11, 111, 107, 0.12);
    }
    .room-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      min-width: 0;
      font-weight: 800;
    }
    .room-title span:first-child {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .muted, .meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .clip {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .chat {
      min-width: 0;
      height: 100vh;
      display: flex;
      flex-direction: column;
      background: var(--bg);
    }
    .chat-head {
      min-height: 72px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.9);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
    }
    .chat-title {
      min-width: 0;
    }
    .chat-title h2 {
      margin: 0;
      font-size: 19px;
      line-height: 1.15;
      overflow-wrap: anywhere;
    }
    .messages {
      flex: 1;
      min-height: 0;
      overflow: auto;
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .message-row {
      display: flex;
      align-items: flex-end;
      gap: 8px;
    }
    .message-row.mine { justify-content: flex-end; }
    .message-row.system { justify-content: center; }
    .bubble {
      max-width: min(760px, 74%);
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--panel);
      padding: 9px 11px;
      box-shadow: var(--shadow);
    }
    .mine .bubble {
      border-color: var(--accent);
      background: var(--accent);
      color: var(--accent-ink);
      box-shadow: none;
    }
    .system .bubble {
      max-width: min(680px, 90%);
      background: var(--warn-bg);
      color: #5c4100;
      box-shadow: none;
    }
    .event .bubble {
      border-color: rgba(93, 75, 156, 0.28);
      background: #f5f2ff;
    }
    .bubble-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 4px;
      font-size: 12px;
      font-weight: 800;
    }
    .mine .bubble-head, .mine .meta {
      color: rgba(255, 255, 255, 0.78);
    }
    .subject {
      margin: 0 0 4px;
      font-weight: 800;
      overflow-wrap: anywhere;
    }
    .body {
      margin: 0;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      line-height: 1.55;
    }
    .empty {
      margin: auto;
      color: var(--muted);
      text-align: center;
      border: 1px dashed #b7c0c9;
      border-radius: 8px;
      padding: 24px;
      background: rgba(255, 255, 255, 0.65);
    }
    .composer {
      border-top: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.92);
      padding: 12px;
      display: grid;
      gap: 8px;
    }
    .composer-main {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: end;
      gap: 8px;
    }
    .composer-options {
      display: grid;
      grid-template-columns: minmax(120px, 1fr) minmax(110px, 0.7fr) minmax(130px, 0.9fr) auto;
      align-items: end;
      gap: 8px;
    }
    .check {
      display: inline-flex;
      align-items: center;
      gap: 7px;
      min-height: 36px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 750;
      white-space: nowrap;
    }
    .check input {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
    }
    .details-panel {
      position: fixed;
      inset: 0 0 0 auto;
      z-index: 20;
      width: min(360px, 92vw);
      border-left: 1px solid var(--line);
      background: var(--panel);
      box-shadow: -18px 0 40px rgba(23, 26, 31, 0.14);
      padding: 16px;
      overflow: auto;
    }
    .details-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 12px;
    }
    .details-head h3 {
      margin: 0;
      font-size: 16px;
    }
    .section {
      border-top: 1px solid var(--line);
      padding-top: 12px;
      margin-top: 12px;
      display: grid;
      gap: 8px;
    }
    .agent {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 9px;
      display: grid;
      gap: 4px;
    }
    .toast {
      position: fixed;
      left: 50%;
      bottom: 18px;
      transform: translateX(-50%);
      z-index: 30;
      max-width: min(560px, calc(100vw - 24px));
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--ink);
      color: white;
      padding: 9px 12px;
      box-shadow: var(--shadow);
      font-size: 13px;
    }
    @media (max-width: 860px) {
      body { overflow: auto; }
      .app {
        display: block;
        height: auto;
        min-height: 100vh;
      }
      .rooms {
        height: auto;
        max-height: 46vh;
        border-right: 0;
        border-bottom: 1px solid var(--line);
      }
      .chat {
        min-height: 54vh;
        height: auto;
      }
      .messages {
        min-height: 360px;
      }
      .composer-options {
        grid-template-columns: 1fr 1fr;
      }
    }
    @media (max-width: 560px) {
      .rooms-head, .chat-head, .messages {
        padding-left: 12px;
        padding-right: 12px;
      }
      .chat-head {
        align-items: flex-start;
        display: grid;
      }
      .bubble {
        max-width: 88%;
      }
      .composer-main {
        grid-template-columns: 1fr;
      }
      .composer-options {
        grid-template-columns: 1fr;
      }
      .composer .btn.primary {
        width: 100%;
      }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="rooms">
      <div class="rooms-head">
        <div class="brand">
          <div>
            <h1>raft</h1>
            <p>agent collaboration protocol</p>
          </div>
          <span id="status-pill" class="pill">loading</span>
        </div>
        <div class="field">
          <label for="agent-input">Agent</label>
          <input id="agent-input" autocomplete="off" spellcheck="false">
        </div>
        <div class="field">
          <label for="search-input">Search</label>
          <input id="search-input" autocomplete="off" spellcheck="false">
        </div>
        <div class="row">
          <button id="refresh-button" class="btn primary" type="button">Refresh</button>
          <button id="unread-button" class="btn" type="button">Unread</button>
        </div>
        <details>
          <summary>New chat</summary>
          <div class="new-room">
            <input id="private-to-input" autocomplete="off" spellcheck="false" placeholder="agent ids">
            <input id="private-topic-input" autocomplete="off" spellcheck="false" placeholder="topic">
            <button id="open-private-button" class="btn" type="button">Open</button>
          </div>
        </details>
        <details>
          <summary>New channel</summary>
          <div class="new-room">
            <input id="channel-id-input" autocomplete="off" spellcheck="false" placeholder="channel id">
            <input id="channel-members-input" autocomplete="off" spellcheck="false" placeholder="members">
            <button id="create-channel-button" class="btn" type="button">Create</button>
          </div>
        </details>
      </div>
      <section class="presence-panel" aria-label="Live agents">
        <div class="presence-head">
          <span>Live agents</span>
          <span id="presence-count" class="pill">0</span>
        </div>
        <div id="presence-list" class="presence-list"></div>
      </section>
      <nav id="conversation-list" class="room-list" aria-label="Conversations"></nav>
    </aside>
    <main class="chat">
      <header class="chat-head">
        <div class="chat-title">
          <h2 id="room-title">No chat selected</h2>
          <div id="room-subtitle" class="meta"></div>
        </div>
        <div class="row">
          <button id="join-button" class="btn primary" type="button" hidden>Join</button>
          <button id="details-button" class="btn ghost" type="button">Info</button>
        </div>
      </header>
      <section id="message-list" class="messages" aria-live="polite"></section>
      <form id="composer" class="composer" hidden>
        <div class="composer-main">
          <textarea id="body-input" name="body" placeholder="Message"></textarea>
          <button class="btn primary" type="submit">Send</button>
        </div>
        <div class="composer-options">
          <div class="field">
            <label for="to-input">To</label>
            <input id="to-input" name="to" autocomplete="off" spellcheck="false">
          </div>
          <div class="field">
            <label for="kind-input">Kind</label>
            <select id="kind-input" name="kind">
              <option value="message">message</option>
              <option value="event">event</option>
              <option value="receipt">receipt</option>
            </select>
          </div>
          <div class="field">
            <label for="needs-input">Needs reply</label>
            <select id="needs-input" name="needs_response_from"></select>
          </div>
          <label class="check">
            <input id="ack-input" name="requires_ack" type="checkbox">
            Ack
          </label>
        </div>
      </form>
    </main>
    <aside id="details-panel" class="details-panel" hidden>
      <div class="details-head">
        <h3>Info</h3>
        <button id="details-close" class="btn" type="button">Close</button>
      </div>
      <div id="details-content"></div>
    </aside>
  </div>
  <div id="toast" class="toast" hidden></div>
  <script>
    const state = {
      agent: new URLSearchParams(location.search).get("agent") || "codex",
      selected: null,
      snapshot: null,
      unreadOnly: false,
      query: "",
      detailsOpen: false,
      renderedRoom: null,
      forceScrollBottom: false
    };

    const $ = (id) => document.getElementById(id);
    $("agent-input").value = state.agent;

    function fmtTime(value) {
      if (!value) return "never";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
    }

    function roomKind(conversation) {
      if (conversation.channel) return "channel";
      if (conversation.private) return "private";
      return "chat";
    }

    function latestMessage(conversation) {
      return conversation.messages[conversation.messages.length - 1] || null;
    }

    function roomPreview(conversation) {
      const message = latestMessage(conversation);
      if (!message) return "No messages";
      return message.subject || message.body || message.kind;
    }

    async function loadSnapshot({ quiet = false } = {}) {
      if (!quiet) setStatus("loading");
      const agent = $("agent-input").value.trim() || "codex";
      state.agent = agent;
      const response = await fetch(`/api/snapshot?agent=${encodeURIComponent(agent)}&limit=160`, { cache: "no-store" });
      if (!response.ok) {
        setStatus("error", "error");
        throw new Error(await response.text());
      }
      state.snapshot = await response.json();
      if (!state.selected || !state.snapshot.conversations.some((item) => item.id === state.selected)) {
        state.selected = state.snapshot.conversations[0]?.id || null;
      }
      history.replaceState(null, "", `?agent=${encodeURIComponent(agent)}`);
      render();
      setStatus("live");
    }

    function filteredConversations() {
      if (!state.snapshot) return [];
      const query = state.query.toLowerCase();
      return state.snapshot.conversations.filter((conversation) => {
        if (state.unreadOnly && conversation.unread_count === 0) return false;
        if (!query) return true;
        const haystack = [
          conversation.id,
          conversation.participants.join(" "),
          roomPreview(conversation)
        ].join(" ").toLowerCase();
        return haystack.includes(query);
      });
    }

    function selectedConversation() {
      return state.snapshot?.conversations.find((item) => item.id === state.selected) || null;
    }

    function render() {
      if (!state.snapshot) return;
      renderPresence(state.snapshot.agents);
      renderRooms(filteredConversations());
      renderChat(selectedConversation());
      renderDetails();
    }

    function renderPresence(agents) {
      const list = $("presence-list");
      list.textContent = "";
      const liveAgents = agents
        .filter((agent) => agent.active)
        .sort((left, right) => {
          if (left.id === state.agent) return -1;
          if (right.id === state.agent) return 1;
          return activityRank(left.current_state) - activityRank(right.current_state) || left.id.localeCompare(right.id);
        });
      $("presence-count").textContent = liveAgents.length;
      if (liveAgents.length === 0) {
        const empty = document.createElement("div");
        empty.className = "muted";
        empty.textContent = "No live agents";
        list.append(empty);
        return;
      }
      for (const agent of liveAgents) {
        const node = document.createElement("button");
        node.type = "button";
        node.className = "presence-agent";
        node.disabled = agent.id === state.agent;
        node.title = agent.id === state.agent ? "This is you" : `Open chat with ${agent.mention}`;
        node.addEventListener("click", () => openPrivateChat(agent.id, ""));
        const top = document.createElement("div");
        top.className = "presence-main";
        const name = document.createElement("span");
        name.className = "presence-name";
        name.textContent = agent.mention;
        const status = document.createElement("span");
        status.className = `pill${agent.current_state === "blocked" ? " error" : ""}`;
        status.textContent = agent.current_state;
        top.append(name, status);
        const note = document.createElement("div");
        note.className = "presence-note";
        note.textContent = activityText(agent);
        node.append(top, note);
        list.append(node);
      }
    }

    function activityRank(value) {
      return { blocked: 0, working: 1, idle: 2, away: 3 }[value] ?? 4;
    }

    function activityText(agent) {
      if (agent.state_note) return agent.state_note;
      const workspace = agent.workspace ? agent.workspace.split("/").filter(Boolean).pop() : "";
      if (workspace) return `${workspace} | seen ${fmtTime(agent.last_seen_at)}`;
      return `seen ${fmtTime(agent.last_seen_at)}`;
    }

    function asksLabel(conversation) {
      const open = conversation.open_asks || 0;
      if (open === 0) return "no open asks";
      return open === 1 ? "1 open ask" : `${open} open asks`;
    }

    function renderRooms(conversations) {
      const list = $("conversation-list");
      list.textContent = "";
      if (conversations.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No chats";
        list.append(empty);
        return;
      }
      for (const conversation of conversations) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `room${conversation.id === state.selected ? " active" : ""}`;
        button.addEventListener("click", () => {
          state.selected = conversation.id;
          render();
        });

        const title = document.createElement("div");
        title.className = "room-title";
        const name = document.createElement("span");
        name.textContent = conversation.id;
        title.append(name);
        if (conversation.unread_count > 0) {
          const unread = document.createElement("span");
          unread.className = "pill unread";
          unread.textContent = conversation.unread_count;
          title.append(unread);
        }

        const meta = document.createElement("div");
        meta.className = "muted";
        meta.textContent = `${roomKind(conversation)} | ${asksLabel(conversation)}`;
        const preview = document.createElement("div");
        preview.className = "muted clip";
        preview.textContent = roomPreview(conversation);
        button.append(title, meta, preview);
        list.append(button);
      }
    }

    function renderChat(conversation) {
      const list = $("message-list");
      const previousRoom = state.renderedRoom;
      const nextRoom = conversation ? conversation.id : null;
      const roomChanged = previousRoom !== nextRoom;
      const previousScrollTop = list.scrollTop;
      const previousScrollHeight = list.scrollHeight;
      const shouldStickToBottom = roomChanged || state.forceScrollBottom || isNearBottom(list);
      list.textContent = "";
      const composer = $("composer");
      const join = $("join-button");
      join.hidden = true;
      join.onclick = null;
      composer.hidden = true;
      state.renderedRoom = nextRoom;
      state.forceScrollBottom = false;

      if (!conversation) {
        $("room-title").textContent = "No chat selected";
        $("room-subtitle").textContent = "";
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No chats";
        list.append(empty);
        return;
      }

      $("room-title").textContent = conversation.id;
      $("room-subtitle").textContent = `${conversation.participants.join(", ")} | ${roomKind(conversation)} | ${asksLabel(conversation)}`;

      if (conversation.channel && !conversation.joined) {
        join.hidden = false;
        join.onclick = () => joinChannel(conversation.id);
      }

      if (conversation.messages.length === 0) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = conversation.joined ? "No messages" : "Join to read";
        list.append(empty);
      } else {
        for (const message of conversation.messages) {
          list.append(renderMessage(message));
        }
      }

      if (conversation.joined) {
        updateComposer(conversation);
        composer.hidden = false;
      }
      requestAnimationFrame(() => {
        if (shouldStickToBottom) {
          list.scrollTop = list.scrollHeight;
        } else {
          const heightDelta = list.scrollHeight - previousScrollHeight;
          list.scrollTop = Math.max(0, previousScrollTop + Math.min(0, heightDelta));
        }
      });
    }

    function isNearBottom(node) {
      return node.scrollHeight - node.scrollTop - node.clientHeight < 72;
    }

    function renderMessage(message) {
      const row = document.createElement("article");
      const mine = message.from === state.agent;
      const system = message.kind === "system";
      row.className = `message-row${mine ? " mine" : ""}${system ? " system" : ""}${message.kind === "event" ? " event" : ""}`;

      const bubble = document.createElement("div");
      bubble.className = "bubble";
      const head = document.createElement("div");
      head.className = "bubble-head";
      const from = document.createElement("span");
      from.textContent = system ? "raft" : message.from;
      const at = document.createElement("span");
      at.textContent = fmtTime(message.created_at);
      head.append(from, at);
      bubble.append(head);

      if (message.subject) {
        const subject = document.createElement("p");
        subject.className = "subject";
        subject.textContent = message.subject;
        bubble.append(subject);
      }

      const body = document.createElement("pre");
      body.className = "body";
      body.textContent = message.body || "(empty)";
      bubble.append(body);

      const metaBits = [];
      if (!system) metaBits.push(`to ${message.to.join(", ")}`);
      if (message.kind !== "message" && !system) metaBits.push(message.kind);
      if (message.requires_ack) metaBits.push("ack");
      if (message.needs_response_from && message.needs_response_from.length > 0) {
        metaBits.push(`needs reply: ${message.needs_response_from.join(", ")}`);
      }
      if (message.unread) metaBits.push("unread");
      if (metaBits.length > 0) {
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = metaBits.join(" | ");
        bubble.append(meta);
      }
      row.append(bubble);
      return row;
    }

    function updateComposer(conversation) {
      const defaultTo = conversation.channel
        ? "*"
        : conversation.participants.filter((participant) => participant !== state.agent).join(",") || "*";
      if (!$("to-input").value || $("to-input").dataset.room !== conversation.id) {
        $("to-input").value = defaultTo;
        $("to-input").dataset.room = conversation.id;
      }
      const needs = $("needs-input");
      const previous = needs.value;
      needs.textContent = "";
      const blank = document.createElement("option");
      blank.value = "";
      blank.textContent = "No reply needed";
      needs.append(blank);
      for (const participant of conversation.participants) {
        if (participant === state.agent) continue;
        const option = document.createElement("option");
        option.value = participant;
        option.textContent = participant;
        needs.append(option);
      }
      needs.value = [...needs.options].some((option) => option.value === previous) ? previous : "";
    }

    function renderDetails() {
      $("details-panel").hidden = !state.detailsOpen;
      if (!state.detailsOpen || !state.snapshot) return;
      const content = $("details-content");
      content.textContent = "";
      const conversation = selectedConversation();
      content.append(detailBlock("Bus", [
        `root: ${state.snapshot.root}`,
        `updated: ${fmtTime(state.snapshot.generated_at)}`,
        `active agents: ${state.snapshot.totals.active_agents}`,
        `visible chats: ${state.snapshot.totals.conversations}`
      ]));
      if (conversation) {
        content.append(detailBlock("Chat", [
          `id: ${conversation.id}`,
          `kind: ${roomKind(conversation)}`,
          `joined: ${conversation.joined ? "yes" : "no"}`,
          `open asks: ${conversation.open_asks || 0}`,
          `messages: ${conversation.message_count}`
        ]));
      }
      const section = document.createElement("div");
      section.className = "section";
      const title = document.createElement("strong");
      title.textContent = "Agents";
      section.append(title);
      for (const agent of state.snapshot.agents) {
        const node = document.createElement("div");
        node.className = "agent";
        const top = document.createElement("div");
        top.className = "row";
        const name = document.createElement("strong");
        name.textContent = agent.mention;
        const status = document.createElement("span");
        status.className = `pill${agent.active ? "" : " error"}`;
        status.textContent = agent.active ? agent.current_state : "stale";
        top.append(name, status);
        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = `${agent.capabilities.join(", ") || "no capabilities"} | seen ${fmtTime(agent.last_seen_at)}`;
        node.append(top, meta);
        if (agent.id !== state.agent) {
          const chat = document.createElement("button");
          chat.className = "btn";
          chat.type = "button";
          chat.textContent = "Chat";
          chat.addEventListener("click", () => openPrivateChat(agent.id, ""));
          node.append(chat);
        }
        section.append(node);
      }
      content.append(section);
    }

    function detailBlock(titleText, lines) {
      const section = document.createElement("div");
      section.className = "section";
      const title = document.createElement("strong");
      title.textContent = titleText;
      section.append(title);
      for (const line of lines) {
        const node = document.createElement("div");
        node.className = "meta";
        node.textContent = line;
        section.append(node);
      }
      return section;
    }

    function setStatus(value, tone = "") {
      const pill = $("status-pill");
      pill.textContent = value;
      pill.className = `pill${tone === "error" ? " error" : ""}`;
    }

    function toast(message) {
      const node = $("toast");
      node.textContent = message;
      node.hidden = false;
      clearTimeout(toast.timer);
      toast.timer = setTimeout(() => {
        node.hidden = true;
      }, 2200);
    }

    async function apiPost(path, payload) {
      setStatus("sending");
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const text = await response.text();
      let result = {};
      if (text) {
        try {
          result = JSON.parse(text);
        } catch {
          result = { ok: false, error: text };
        }
      }
      if (!response.ok || result.ok === false) {
        throw new Error(result.error || `request failed with ${response.status}`);
      }
      return result;
    }

    async function openPrivateChat(to, topic) {
      const target = (to || "").trim();
      if (!target) {
        toast("Agent required");
        return;
      }
      try {
        const result = await apiPost("/api/open", {
          agent: state.agent,
          to: target,
          topic: (topic || "").trim()
        });
        state.selected = result.conversation_id;
        state.forceScrollBottom = true;
        $("private-to-input").value = "";
        $("private-topic-input").value = "";
        await loadSnapshot();
        toast("Chat ready");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function createChannel() {
      const channel = $("channel-id-input").value.trim();
      if (!channel) {
        toast("Channel required");
        return;
      }
      try {
        const result = await apiPost("/api/channel", {
          agent: state.agent,
          channel,
          members: $("channel-members-input").value.trim()
        });
        state.selected = result.conversation_id;
        state.forceScrollBottom = true;
        $("channel-id-input").value = "";
        $("channel-members-input").value = "";
        await loadSnapshot();
        toast("Channel ready");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function joinChannel(channel) {
      try {
        await apiPost("/api/join", { agent: state.agent, channel });
        state.selected = channel;
        state.forceScrollBottom = true;
        await loadSnapshot();
        toast("Joined");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    async function sendCurrentMessage(event) {
      event.preventDefault();
      const conversation = selectedConversation();
      if (!conversation) return;
      const body = $("body-input").value.trim();
      if (!body) {
        toast("Message required");
        return;
      }
      const payload = {
        agent: state.agent,
        conversation: conversation.channel ? null : conversation.id,
        channel: conversation.channel ? conversation.id : null,
        to: $("to-input").value.trim() || "*",
        subject: "",
        body,
        kind: $("kind-input").value || "message",
        requires_ack: $("ack-input").checked,
        needs_response_from: $("needs-input").value ? [$("needs-input").value] : []
      };
      if (payload.kind !== "message") payload.needs_response_from = [];
      try {
        state.forceScrollBottom = true;
        await apiPost("/api/send", payload);
        $("body-input").value = "";
        $("ack-input").checked = false;
        $("needs-input").value = "";
        await loadSnapshot();
        toast("Sent");
      } catch (error) {
        setStatus("error", "error");
        toast(error.message);
      }
    }

    $("refresh-button").addEventListener("click", () => loadSnapshot().catch((error) => toast(error.message)));
    $("open-private-button").addEventListener("click", () => openPrivateChat($("private-to-input").value, $("private-topic-input").value));
    $("create-channel-button").addEventListener("click", () => createChannel());
    $("unread-button").addEventListener("click", () => {
      state.unreadOnly = !state.unreadOnly;
      $("unread-button").classList.toggle("primary", state.unreadOnly);
      render();
    });
    $("agent-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter") loadSnapshot().catch((error) => toast(error.message));
    });
    $("search-input").addEventListener("input", (event) => {
      state.query = event.target.value;
      render();
    });
    $("details-button").addEventListener("click", () => {
      state.detailsOpen = !state.detailsOpen;
      renderDetails();
    });
    $("details-close").addEventListener("click", () => {
      state.detailsOpen = false;
      renderDetails();
    });
    $("composer").addEventListener("submit", sendCurrentMessage);

    loadSnapshot().catch((error) => {
      setStatus("error", "error");
      const list = $("message-list");
      list.textContent = "";
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = error.message;
      list.append(empty);
    });
    setInterval(() => loadSnapshot({ quiet: true }).catch(() => setStatus("offline", "error")), 5000);
  </script>
</body>
</html>
"##;

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

fn conversation_path(root: &Path, conversation_id: &str) -> Result<PathBuf> {
    Ok(root
        .join("conversations")
        .join(validate_id(conversation_id, "conversation id")?))
}

fn target_room(conversation: Option<&str>, channel: Option<&str>) -> Result<String> {
    match (conversation, channel) {
        (Some(conversation), None) => validate_id(conversation, "conversation id"),
        (None, Some(channel)) => validate_id(channel, "channel id"),
        (None, None) => bail!("provide --conversation or --channel"),
        (Some(_), Some(_)) => bail!("provide only one of --conversation or --channel"),
    }
}

fn optional_target_room(
    conversation: Option<&str>,
    channel: Option<&str>,
) -> Result<Option<String>> {
    match (conversation, channel) {
        (Some(conversation), None) => Ok(Some(validate_id(conversation, "conversation id")?)),
        (None, Some(channel)) => Ok(Some(validate_id(channel, "channel id")?)),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => bail!("provide only one of --conversation or --channel"),
    }
}

fn agent_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("agents").join(format!("{agent_id}.json"))
}

fn receipt_path_for(root: &Path, message: &Message, agent_id: &str) -> PathBuf {
    root.join("conversations")
        .join(&message.conversation_id)
        .join("receipts")
        .join(&message.id)
        .join(format!("{agent_id}.json"))
}

fn watch_state_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("watch").join(format!("{agent_id}.json"))
}

fn heartbeat_state_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("heartbeat").join(format!("{agent_id}.json"))
}

fn ensure_root(root: &Path) -> Result<()> {
    fs::create_dir_all(root)?;
    set_dir_private(root)?;
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
        let path = root.join(child);
        fs::create_dir_all(&path)?;
        set_dir_private(&path)?;
    }
    Ok(())
}

fn atomic_write_json<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        set_dir_private(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| RaftError(format!("invalid target file: {}", path.display())))?;
    let tmp = path.with_file_name(format!(
        ".{file_name}.{}.{}.tmp",
        process::id(),
        unique_token()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)?;
    serde_json::to_writer_pretty(&mut file, payload)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    if let Some(parent) = path.parent() {
        let _ = fsync_dir(parent);
    }
    Ok(())
}

fn append_jsonl<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        set_dir_private(parent)?;
    }
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer(&mut file, payload)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    if let Some(parent) = path.parent() {
        let _ = fsync_dir(parent);
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match File::open(path) {
        Ok(file) => Ok(Some(serde_json::from_reader(file)?)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
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

fn fsync_dir(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    file.sync_all()
}

fn lock_is_stale(path: &Path) -> Result<bool> {
    let owner: Option<LockOwner> = read_json(&path.join("owner.json"))?;
    let Some(owner) = owner else {
        let Ok(metadata) = fs::metadata(path) else {
            return Ok(true);
        };
        let Ok(modified) = metadata.modified() else {
            return Ok(false);
        };
        return Ok(modified
            .elapsed()
            .map(|elapsed| elapsed > Duration::from_secs(LOCK_TTL_SECONDS))
            .unwrap_or(false));
    };
    Ok(parse_time(&owner.expires_at)
        .map(|expires_at| expires_at < Utc::now())
        .unwrap_or(true))
}

fn set_dir_private(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
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

fn sorted_read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = match fs::read_dir(path) {
        Ok(entries) => entries.collect::<std::result::Result<Vec<_>, _>>()?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };
    entries.sort_by_key(|entry| entry.path());
    Ok(entries)
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

fn process_is_alive(pid: u32) -> bool {
    process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn sleep_interruptibly(duration: Duration, shutdown: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

fn validate_agent_state(value: &str) -> Result<String> {
    match value {
        "idle" | "working" | "blocked" | "away" => Ok(value.to_string()),
        _ => bail!("invalid state {value:?}; use idle, working, blocked, or away"),
    }
}

fn validate_id(value: &str, label: &str) -> Result<String> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 80 || !bytes[0].is_ascii_alphanumeric() {
        bail!("invalid {label} {value:?}; use 1-80 letters, digits, dots, dashes, or underscores");
    }
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("invalid {label} {value:?}; use 1-80 letters, digits, dots, dashes, or underscores");
    }
    Ok(value.to_string())
}

fn validate_claim_name(value: &str) -> Result<String> {
    let agent_id = validate_id(value.trim_start_matches('@'), "agent name")?;
    if agent_id.len() < 3 {
        bail!("agent name @{agent_id} is too short; choose a unique, personable name");
    }
    Ok(agent_id)
}

fn split_csv(value: &str) -> Result<Vec<String>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| validate_id(item, "id"))
        .collect()
}

fn split_recipients(value: &str) -> Result<Vec<String>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            if item == "*" {
                Ok(item.to_string())
            } else {
                validate_id(item.trim_start_matches('@'), "recipient")
            }
        })
        .collect()
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

fn extract_mentions(value: &str) -> Vec<String> {
    let mut mentions = Vec::new();
    let mut chars = value.char_indices().peekable();
    while let Some((_index, ch)) = chars.next() {
        if ch != '@' {
            continue;
        }
        let mut mention = String::new();
        while let Some((_next_index, next)) = chars.peek().copied() {
            if next.is_ascii_alphanumeric() || matches!(next, '.' | '_' | '-') {
                mention.push(next);
                chars.next();
            } else {
                break;
            }
        }
        if validate_id(&mention, "mention").is_ok() {
            mentions.push(mention);
        }
    }
    unique(mentions)
}

fn generated_private_conversation_id(participants: &[String], topic: &str) -> String {
    let topic_slug = slugify_id_segment(topic);
    let participant_slug = participants
        .iter()
        .take(3)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("-");
    let base = if topic_slug.is_empty() {
        format!("p-{participant_slug}")
    } else {
        format!("p-{topic_slug}-{participant_slug}")
    };
    let suffix = unique_token_short();
    let max_base_len = 79usize.saturating_sub(suffix.len());
    let mut trimmed = base.chars().take(max_base_len).collect::<String>();
    while trimmed.ends_with('-') || trimmed.ends_with('.') || trimmed.ends_with('_') {
        trimmed.pop();
    }
    if trimmed.is_empty() {
        trimmed.push('p');
    }
    format!("{trimmed}-{suffix}")
}

fn slugify_id_segment(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_dash = false;
        } else if !previous_dash && !slug.is_empty() {
            slug.push('-');
            previous_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    slug
}

fn normalize_send_kind(kind: &str) -> Result<String> {
    match kind {
        "message" | "event" | "receipt" => Ok(kind.to_string()),
        "system" => bail!("kind \"system\" is reserved for raft internals"),
        _ => bail!("unsupported kind {kind:?}; use message, event, or receipt"),
    }
}

fn validate_subject_id(value: &str) -> Result<String> {
    if value.is_empty() || value.len() > 160 {
        bail!("invalid subject id: use 1-160 printable characters");
    }
    if value.chars().any(|ch| ch.is_control()) {
        bail!("invalid subject id: control characters are not allowed");
    }
    if value.contains('#') {
        bail!("invalid subject id: '#' is reserved for raft rate-limit keys");
    }
    Ok(value.to_string())
}

fn rate_key(sender: &str, subject_id: Option<&str>) -> String {
    match subject_id {
        Some(subject_id) => format!("{sender}#{subject_id}"),
        None => sender.to_string(),
    }
}

fn schema_v1() -> u16 {
    SCHEMA_VERSION
}

fn default_agent_state() -> String {
    "idle".to_string()
}

fn default_message_kind() -> String {
    "message".to_string()
}

fn unique(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value);
        }
    }
    output
}

fn resolve_path(path: &Path) -> Result<String> {
    Ok(path.canonicalize()?.display().to_string())
}

fn hostname() -> String {
    env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

fn iso_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn iso_after(seconds: u64) -> String {
    (Utc::now() + TimeDelta::seconds(seconds as i64)).to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_time(value: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|value| value.with_timezone(&Utc))
}

fn new_message_id() -> String {
    let now = Utc::now();
    let stamp = format!(
        "{}{:03}",
        now.format("%Y%m%dT%H%M%S"),
        now.timestamp_subsec_millis()
    );
    format!("m-{stamp}-{}", unique_token_short())
}

fn unique_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:x}{:x}", process::id())
}

fn unique_token_short() -> String {
    let token = unique_token();
    token.chars().take(12).collect()
}
