use crate::{
    DEFAULT_AGENT_TTL_SECONDS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_RATE_MAX_MESSAGES,
    DEFAULT_RATE_WINDOW_SECONDS,
};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "raft")]
#[command(version)]
#[command(about = "Filesystem-backed agent-to-agent coordination bus.")]
pub(crate) struct Cli {
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
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
pub(crate) struct ClaimArgs {
    pub(crate) agent: String,
    #[arg(long)]
    pub(crate) workspace: Option<PathBuf>,
    #[arg(long, default_value = "")]
    pub(crate) capabilities: String,
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    pub(crate) ttl: u64,
}

#[derive(Args)]
pub(crate) struct RegisterArgs {
    pub(crate) agent: String,
    #[arg(long)]
    pub(crate) workspace: Option<PathBuf>,
    #[arg(long, default_value = "")]
    pub(crate) capabilities: String,
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    pub(crate) ttl: u64,
}

#[derive(Args)]
pub(crate) struct HeartbeatArgs {
    pub(crate) agent: String,
    #[arg(long)]
    pub(crate) ttl: Option<u64>,
    #[arg(long)]
    pub(crate) watch: bool,
    #[arg(long)]
    pub(crate) interval: Option<f64>,
}

#[derive(Subcommand)]
pub(crate) enum StateCommand {
    Set(StateSetArgs),
    Get(StateGetArgs),
}

#[derive(Args)]
pub(crate) struct StateSetArgs {
    pub(crate) agent: String,
    pub(crate) state: String,
    #[arg(long)]
    pub(crate) note: Option<String>,
}

#[derive(Args)]
pub(crate) struct StateGetArgs {
    pub(crate) agent: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum ChannelCommand {
    Create(ChannelCreateArgs),
    Join(ChannelJoinArgs),
}

#[derive(Args)]
pub(crate) struct ChannelCreateArgs {
    pub(crate) channel: String,
    #[arg(long)]
    pub(crate) creator: String,
    #[arg(long, default_value = "")]
    pub(crate) members: String,
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
}

#[derive(Args)]
pub(crate) struct ChannelJoinArgs {
    pub(crate) channel: String,
    #[arg(long)]
    pub(crate) agent: String,
}

#[derive(Subcommand)]
pub(crate) enum ConversationCommand {
    Create(ConversationCreateArgs),
    Open(ConversationOpenArgs),
}

#[derive(Args)]
pub(crate) struct ConversationCreateArgs {
    pub(crate) conversation: String,
    #[arg(long)]
    pub(crate) participants: String,
    #[arg(long)]
    pub(crate) starter: Option<String>,
    #[arg(long)]
    pub(crate) private: bool,
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
}

#[derive(Args)]
pub(crate) struct ConversationOpenArgs {
    #[arg(long = "id")]
    pub(crate) conversation: Option<String>,
    #[arg(long = "from")]
    pub(crate) opener: String,
    #[arg(long)]
    pub(crate) to: String,
    #[arg(long, default_value = "")]
    pub(crate) topic: String,
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
}

#[derive(Args)]
pub(crate) struct SendArgs {
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long = "from")]
    pub(crate) sender: String,
    #[arg(long)]
    pub(crate) to: String,
    #[arg(long, default_value = "")]
    pub(crate) subject: String,
    #[arg(long)]
    pub(crate) body: String,
    #[arg(long, default_value = "message")]
    pub(crate) kind: String,
    #[arg(long)]
    pub(crate) after: Option<String>,
    #[arg(long = "subject-id")]
    pub(crate) subject_id: Option<String>,
    #[arg(long = "requires-ack")]
    pub(crate) requires_ack: bool,
    #[arg(long = "needs-response-from", default_value = "")]
    pub(crate) needs_response_from: String,
}

#[derive(Args)]
pub(crate) struct AwaitingArgs {
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct RosterArgs {
    #[arg(long)]
    pub(crate) all: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct InboxArgs {
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long)]
    pub(crate) unread: bool,
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    #[arg(long, default_value_t = 120)]
    pub(crate) width: usize,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct WaitArgs {
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long, default_value_t = 300)]
    pub(crate) timeout: u64,
    #[arg(long, default_value_t = 2.0)]
    pub(crate) interval: f64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct WatchArgs {
    #[arg(long)]
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long)]
    pub(crate) since: Option<String>,
    #[arg(long, default_value_t = 0)]
    pub(crate) timeout: u64,
    #[arg(long, default_value_t = 1.0)]
    pub(crate) interval: f64,
    #[arg(long)]
    pub(crate) once: bool,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long = "no-auto-read")]
    pub(crate) no_auto_read: bool,
    #[arg(long = "state-changes")]
    pub(crate) state_changes: bool,
}

#[derive(Args)]
pub(crate) struct ShowArgs {
    #[arg(long)]
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long, default_value_t = 50)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct SearchArgs {
    pub(crate) pattern: String,
    #[arg(long)]
    pub(crate) agent: String,
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    #[arg(long)]
    pub(crate) channel: Option<String>,
    #[arg(long)]
    pub(crate) since: Option<String>,
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ThreadArgs {
    pub(crate) message_id: String,
    #[arg(long)]
    pub(crate) agent: String,
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ReadArgs {
    pub(crate) agent: String,
    pub(crate) message_id: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct AckArgs {
    pub(crate) agent: String,
    pub(crate) message_id: String,
    #[arg(long, default_value = "done")]
    pub(crate) status: String,
    #[arg(long)]
    pub(crate) note: Option<String>,
}

#[derive(Args)]
pub(crate) struct ReceiptsArgs {
    pub(crate) message_id: String,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct JournalArgs {
    pub(crate) agent: String,
    #[arg(long, default_value = "note")]
    pub(crate) kind: String,
    #[arg(long, default_value = "")]
    pub(crate) subject: String,
    #[arg(long)]
    pub(crate) body: String,
}

#[derive(Args)]
pub(crate) struct StatusArgs {
    #[arg(long)]
    pub(crate) agent: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct DoctorArgs {
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long)]
    pub(crate) strict: bool,
}

#[derive(Args, Clone, Copy)]
pub(crate) struct GcArgs {
    #[arg(long)]
    pub(crate) archive: bool,
}

#[derive(Args)]
pub(crate) struct ServeArgs {
    #[arg(long, default_value_t = 2.0)]
    pub(crate) interval: f64,
    #[arg(long)]
    pub(crate) archive: bool,
}

#[derive(Args)]
pub(crate) struct UiArgs {
    #[arg(long, default_value = "codex")]
    pub(crate) agent: String,
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: String,
    #[arg(long, default_value_t = 7420)]
    pub(crate) port: u16,
    #[arg(long, default_value_t = 80)]
    pub(crate) limit: usize,
}
