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
#[command(long_about = "\
Filesystem-backed agent-to-agent coordination bus.

Commands that accept --json emit machine-readable output on stdout and, on \
failure, a structured error envelope on stderr:
  {\"ok\":false,\"error\":{\"code\":\"<code>\",\"message\":\"<text>\"}}

Stdout carries data; stderr carries errors and diagnostics.

TYPICAL AGENT FLOW
  raft claim <me> --capabilities ...   take a name and start a heartbeat TTL
  raft me <me>                         one-shot orientation: unread, asks, peers
  raft reply <message-id> --from <me> --body ... --ack done
                                       answer an ask and close it in one call
  raft awaiting <me>                   see what you owe and are owed
  raft roster --capability <tag>       find a live peer with a given skill
  raft channel list --agent <me>       discover channels you can join

EXIT CODES
  0  success
  1  error (generic failure; see error code for the specific category)
  2  timeout (e.g. `wait` reached its deadline with no unread message)

ERROR CODES (stable; surfaced as error.code in --json mode)
  not_claimed       agent name has not been claimed; run `raft claim`
  not_found         referenced agent, channel, or conversation does not exist
  not_participant   agent or recipient is not a participant in the conversation
  conflict          a resource already exists: an agent name claimed by \
another holder, or a channel/conversation created without --if-missing
  rate_limited      sender exceeded the conversation's message rate limit
  too_large         message body exceeds the conversation's byte limit
  timeout           a blocking command reached its deadline
  io                underlying filesystem operation failed
  parse             a stored JSON document could not be parsed
  error             generic/uncategorized failure")]
pub(crate) struct Cli {
    /// Bus root directory (defaults to $RAFT_ROOT or ./run/bus).
    #[arg(long)]
    pub(crate) root: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Initialize a new bus directory at the resolved root.
    Init(InitArgs),
    /// Claim an agent name, taking ownership and starting its heartbeat TTL.
    Claim(ClaimArgs),
    /// Register an agent name without claiming exclusive ownership.
    Register(RegisterArgs),
    /// Refresh an agent's heartbeat so it stays live (optionally in a loop).
    Heartbeat(HeartbeatArgs),
    /// Get or set an agent's published state (e.g. working, blocked, idle).
    State {
        #[command(subcommand)]
        command: StateCommand,
    },
    /// Create or join a shared group channel.
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    /// Create or open a conversation between specific participants.
    Conversation {
        #[command(subcommand)]
        command: ConversationCommand,
    },
    /// Send a message to a conversation or channel.
    Send(SendArgs),
    /// Reply to a message, inheriting its conversation, thread, and subject.
    Reply(ReplyArgs),
    /// One-shot orientation summary for an agent (unread, asks, peers, rooms).
    Me(MeArgs),
    /// Show which replies an agent owes and which it is waiting on.
    Awaiting(AwaitingArgs),
    /// List live agents with presence and per-agent open-ask counts.
    Roster(RosterArgs),
    /// List an agent's recent messages without marking them read.
    Inbox(InboxArgs),
    /// Block until the agent has an unread message, or the timeout elapses.
    Wait(WaitArgs),
    /// Stream new messages for an agent as they arrive (resumable).
    Watch(WatchArgs),
    /// Render a conversation thread for an agent without marking it read.
    Show(ShowArgs),
    /// Search an agent's visible messages by substring.
    Search(SearchArgs),
    /// Render a single message and its reply tree.
    Thread(ThreadArgs),
    /// Mark a message read for an agent, recording a read receipt.
    Read(ReadArgs),
    /// Record an acknowledgement receipt (done, accepted, blocked, ...).
    Ack(AckArgs),
    /// Show the receipts recorded against a message.
    Receipts(ReceiptsArgs),
    /// Append a private journal entry for an agent.
    Journal(JournalArgs),
    /// Summarize bus liveness, agent state, and open asks.
    Status(StatusArgs),
    /// Run read-only diagnostics against the bus.
    Doctor(DoctorArgs),
    /// Garbage-collect stale locks and optionally archive old messages.
    Gc(GcArgs),
    /// Run the background maintenance loop (locks, TTLs, archival).
    Serve(ServeArgs),
    /// Serve the local web UI for an agent.
    Ui(UiArgs),
}

#[derive(Args)]
pub(crate) struct InitArgs {
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ClaimArgs {
    /// Agent name to claim.
    pub(crate) agent: String,
    /// Filesystem path of the agent's workspace.
    #[arg(long)]
    pub(crate) workspace: Option<PathBuf>,
    /// Comma-separated capability tags advertised to peers.
    #[arg(long, default_value = "")]
    pub(crate) capabilities: String,
    /// Seconds before the agent is considered stale without a heartbeat.
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    pub(crate) ttl: u64,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct RegisterArgs {
    /// Agent name to register (non-exclusive).
    pub(crate) agent: String,
    /// Filesystem path of the agent's workspace.
    #[arg(long)]
    pub(crate) workspace: Option<PathBuf>,
    /// Comma-separated capability tags advertised to peers.
    #[arg(long, default_value = "")]
    pub(crate) capabilities: String,
    /// Seconds before the agent is considered stale without a heartbeat.
    #[arg(long, default_value_t = DEFAULT_AGENT_TTL_SECONDS)]
    pub(crate) ttl: u64,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct HeartbeatArgs {
    /// Agent name to keep alive.
    pub(crate) agent: String,
    /// Override the agent's TTL in seconds for this heartbeat.
    #[arg(long)]
    pub(crate) ttl: Option<u64>,
    /// Keep heartbeating in a loop until interrupted.
    #[arg(long)]
    pub(crate) watch: bool,
    /// Seconds between heartbeats when --watch is set.
    #[arg(long)]
    pub(crate) interval: Option<f64>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum StateCommand {
    /// Publish an agent's state.
    Set(StateSetArgs),
    /// Read an agent's published state.
    Get(StateGetArgs),
}

#[derive(Args)]
pub(crate) struct StateSetArgs {
    /// Agent whose state to set.
    pub(crate) agent: String,
    /// State label to publish (e.g. working, blocked, idle).
    pub(crate) state: String,
    /// Optional human-readable note attached to the state.
    #[arg(long)]
    pub(crate) note: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct StateGetArgs {
    /// Agent whose state to read.
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum ChannelCommand {
    /// Create a new channel.
    Create(ChannelCreateArgs),
    /// Subscribe an agent to an existing channel.
    Join(ChannelJoinArgs),
    /// Unsubscribe an agent from a channel.
    Leave(ChannelLeaveArgs),
    /// List channels on the bus so an agent can discover ones to join.
    List(ChannelListArgs),
}

#[derive(Args)]
pub(crate) struct ChannelLeaveArgs {
    /// Channel to leave.
    pub(crate) channel: String,
    /// Agent leaving the channel.
    #[arg(long)]
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ChannelListArgs {
    /// Annotate each channel with this agent's membership and unread count.
    #[arg(long)]
    pub(crate) agent: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ChannelCreateArgs {
    /// Channel name to create.
    pub(crate) channel: String,
    /// Agent creating the channel (becomes the first subscriber).
    #[arg(long)]
    pub(crate) creator: String,
    /// Comma-separated agent names to subscribe at creation.
    #[arg(long, default_value = "")]
    pub(crate) members: String,
    /// Succeed without error if the channel already exists.
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    /// Days to retain messages before they become eligible for archival.
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    /// Rate-limit window in seconds.
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    /// Maximum messages per sender within the rate window.
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    /// Maximum message body size in bytes.
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ChannelJoinArgs {
    /// Channel to join.
    pub(crate) channel: String,
    /// Agent joining the channel.
    #[arg(long)]
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum ConversationCommand {
    /// Create a conversation with an explicit id and participants.
    Create(ConversationCreateArgs),
    /// Open (or reuse) a conversation between the given agents.
    Open(ConversationOpenArgs),
    /// Add a participant to an existing conversation.
    Add(ConversationAddArgs),
    /// Remove a participant from an existing conversation.
    Remove(ConversationRemoveArgs),
}

#[derive(Args)]
pub(crate) struct ConversationAddArgs {
    /// Conversation to add the agent to.
    pub(crate) conversation: String,
    /// Agent to add as a participant.
    #[arg(long)]
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ConversationRemoveArgs {
    /// Conversation to remove the agent from.
    pub(crate) conversation: String,
    /// Agent to remove as a participant.
    #[arg(long)]
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ConversationCreateArgs {
    /// Conversation id to create.
    pub(crate) conversation: String,
    /// Comma-separated participant agent names.
    #[arg(long)]
    pub(crate) participants: String,
    /// Agent recorded as the conversation starter.
    #[arg(long)]
    pub(crate) starter: Option<String>,
    /// Mark the conversation private (participant-scoped).
    #[arg(long)]
    pub(crate) private: bool,
    /// Succeed without error if the conversation already exists.
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    /// Days to retain messages before they become eligible for archival.
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    /// Rate-limit window in seconds.
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    /// Maximum messages per sender within the rate window.
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    /// Maximum message body size in bytes.
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ConversationOpenArgs {
    /// Explicit conversation id; if omitted, derived from the participants.
    #[arg(long = "id")]
    pub(crate) conversation: Option<String>,
    /// Agent opening the conversation.
    #[arg(long = "from")]
    pub(crate) opener: String,
    /// Comma-separated agent names to open the conversation with.
    #[arg(long)]
    pub(crate) to: String,
    /// Optional conversation topic.
    #[arg(long, default_value = "")]
    pub(crate) topic: String,
    /// Succeed without error if the conversation already exists.
    #[arg(long = "if-missing")]
    pub(crate) if_missing: bool,
    /// Days to retain messages before they become eligible for archival.
    #[arg(long = "retention-days", default_value_t = 14)]
    pub(crate) retention_days: u64,
    /// Rate-limit window in seconds.
    #[arg(long = "rate-window", default_value_t = DEFAULT_RATE_WINDOW_SECONDS)]
    pub(crate) rate_window: u64,
    /// Maximum messages per sender within the rate window.
    #[arg(long = "rate-max", default_value_t = DEFAULT_RATE_MAX_MESSAGES)]
    pub(crate) rate_max: u64,
    /// Maximum message body size in bytes.
    #[arg(long = "max-message-bytes", default_value_t = DEFAULT_MAX_MESSAGE_BYTES)]
    pub(crate) max_message_bytes: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct SendArgs {
    /// Target conversation id (mutually exclusive with --channel).
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    pub(crate) conversation: Option<String>,
    /// Target channel name (mutually exclusive with --conversation).
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Sending agent name.
    #[arg(long = "from")]
    pub(crate) sender: String,
    /// Comma-separated recipients; use `*` for everyone in the room.
    #[arg(long)]
    pub(crate) to: String,
    /// Optional subject line.
    #[arg(long, default_value = "")]
    pub(crate) subject: String,
    /// Message body.
    #[arg(long)]
    pub(crate) body: String,
    /// Message kind (message, event, ...).
    #[arg(long, default_value = "message")]
    pub(crate) kind: String,
    /// Id of the message this one replies to (threads the reply).
    #[arg(long)]
    pub(crate) after: Option<String>,
    /// External correlation id for bridged events.
    #[arg(long = "subject-id")]
    pub(crate) subject_id: Option<String>,
    /// Require recipients to record an acknowledgement receipt.
    #[arg(long = "requires-ack")]
    pub(crate) requires_ack: bool,
    /// Comma-separated agents whose reply is awaited (advisory).
    #[arg(long = "needs-response-from", default_value = "")]
    pub(crate) needs_response_from: String,
    /// Emit a machine-readable JSON envelope instead of the message id.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ReplyArgs {
    /// Id of the message being replied to.
    pub(crate) message: String,
    /// Replying agent name.
    #[arg(long = "from")]
    pub(crate) sender: String,
    /// Reply body.
    #[arg(long)]
    pub(crate) body: String,
    /// Recipients; defaults to the original sender. Use `*` for the whole room.
    #[arg(long)]
    pub(crate) to: Option<String>,
    /// Subject line; defaults to the parent message's subject.
    #[arg(long)]
    pub(crate) subject: Option<String>,
    /// Require recipients to record an acknowledgement receipt.
    #[arg(long = "requires-ack")]
    pub(crate) requires_ack: bool,
    /// Comma-separated agents whose reply is awaited (advisory).
    #[arg(long = "needs-response-from", default_value = "")]
    pub(crate) needs_response_from: String,
    /// Also record this acknowledgement status on the parent message, e.g.
    /// `done` to close the ask in the same call. One of: received, accepted,
    /// working, blocked, done, rejected.
    #[arg(long)]
    pub(crate) ack: Option<String>,
    /// Note to attach to the --ack receipt.
    #[arg(long = "ack-note")]
    pub(crate) ack_note: Option<String>,
    /// Emit a machine-readable JSON envelope instead of the message id.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct MeArgs {
    /// Agent to summarize.
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct AwaitingArgs {
    /// Agent whose open asks to report.
    pub(crate) agent: String,
    /// Limit to a single conversation.
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    /// Limit to a single channel.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct RosterArgs {
    /// Include stale (non-live) agents.
    #[arg(long)]
    pub(crate) all: bool,
    /// Only list agents advertising this capability tag.
    #[arg(long)]
    pub(crate) capability: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct InboxArgs {
    /// Agent whose inbox to list.
    pub(crate) agent: String,
    /// Limit to a single conversation.
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    /// Limit to a single channel.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Show only unread messages.
    #[arg(long)]
    pub(crate) unread: bool,
    /// Show only messages needing action: unread, or an open ask awaiting you.
    #[arg(long)]
    pub(crate) needs_action: bool,
    /// Maximum number of messages to list.
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    /// Truncate each message body to this width.
    #[arg(long, default_value_t = 120)]
    pub(crate) width: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct WaitArgs {
    /// Agent to wait for an unread message on.
    pub(crate) agent: String,
    /// Limit to a single conversation.
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    /// Limit to a single channel.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Seconds to wait before giving up (exits 2 on timeout).
    #[arg(long, default_value_t = 300)]
    pub(crate) timeout: u64,
    /// Fallback poll interval in seconds when events are unavailable.
    #[arg(long, default_value_t = 2.0)]
    pub(crate) interval: f64,
    /// Emit the matched message as JSON instead of its id.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct WatchArgs {
    /// Agent to stream messages for.
    #[arg(long)]
    pub(crate) agent: String,
    /// Limit to a single conversation.
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    /// Limit to a single channel.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Resume after this message id (overrides the saved cursor).
    #[arg(long)]
    pub(crate) since: Option<String>,
    /// Seconds to stream before exiting; 0 streams until interrupted.
    #[arg(long, default_value_t = 0)]
    pub(crate) timeout: u64,
    /// Fallback poll interval in seconds when events are unavailable.
    #[arg(long, default_value_t = 1.0)]
    pub(crate) interval: f64,
    /// Scan once, emit any new messages, then exit.
    #[arg(long)]
    pub(crate) once: bool,
    /// Emit line-delimited JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
    /// Do not record read receipts for emitted messages.
    #[arg(long = "no-auto-read")]
    pub(crate) no_auto_read: bool,
    /// Also emit agent state-change events.
    #[arg(long = "state-changes")]
    pub(crate) state_changes: bool,
}

#[derive(Args)]
pub(crate) struct ShowArgs {
    /// Agent viewing the thread (for visibility filtering).
    #[arg(long)]
    pub(crate) agent: String,
    /// Conversation to render (mutually exclusive with --channel).
    #[arg(long, conflicts_with = "channel", required_unless_present = "channel")]
    pub(crate) conversation: Option<String>,
    /// Channel to render (mutually exclusive with --conversation).
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Maximum number of messages to render.
    #[arg(long, default_value_t = 50)]
    pub(crate) limit: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct SearchArgs {
    /// Substring to search for in message bodies.
    pub(crate) pattern: String,
    /// Agent whose visible messages to search.
    #[arg(long)]
    pub(crate) agent: String,
    /// Limit to a single conversation.
    #[arg(long, conflicts_with = "channel")]
    pub(crate) conversation: Option<String>,
    /// Limit to a single channel.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Only match messages newer than this (RFC3339 or a duration like 2h).
    #[arg(long)]
    pub(crate) since: Option<String>,
    /// Maximum number of matches to return.
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ThreadArgs {
    /// Root message id of the thread to render.
    pub(crate) message_id: String,
    /// Agent viewing the thread (for visibility filtering).
    #[arg(long)]
    pub(crate) agent: String,
    /// Maximum number of messages to render.
    #[arg(long, default_value_t = 100)]
    pub(crate) limit: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ReadArgs {
    /// Agent recording the read receipt.
    pub(crate) agent: String,
    /// Message id to mark read.
    pub(crate) message_id: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct AckArgs {
    /// Agent recording the acknowledgement.
    pub(crate) agent: String,
    /// Message id to acknowledge.
    pub(crate) message_id: String,
    /// Acknowledgement status: received, accepted, working, blocked, done, or
    /// rejected. `done` and `rejected` close an open ask; the rest are progress
    /// updates.
    #[arg(long, default_value = "done")]
    pub(crate) status: String,
    /// Optional note attached to the acknowledgement.
    #[arg(long)]
    pub(crate) note: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct ReceiptsArgs {
    /// Message id whose receipts to show.
    pub(crate) message_id: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct JournalArgs {
    /// Agent the journal entry belongs to.
    pub(crate) agent: String,
    /// Entry kind (note, checkpoint, ...).
    #[arg(long, default_value = "note")]
    pub(crate) kind: String,
    /// Optional entry subject.
    #[arg(long, default_value = "")]
    pub(crate) subject: String,
    /// Entry body.
    #[arg(long)]
    pub(crate) body: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct StatusArgs {
    /// Limit the summary to a single agent.
    #[arg(long)]
    pub(crate) agent: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct DoctorArgs {
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
    /// Exit non-zero if any warning (not just error) is found.
    #[arg(long)]
    pub(crate) strict: bool,
}

#[derive(Args, Clone, Copy)]
pub(crate) struct GcArgs {
    /// Also archive messages past their retention window.
    #[arg(long)]
    pub(crate) archive: bool,
}

#[derive(Args)]
pub(crate) struct ServeArgs {
    /// Seconds between maintenance passes.
    #[arg(long, default_value_t = 2.0)]
    pub(crate) interval: f64,
    /// Archive old messages during each pass.
    #[arg(long)]
    pub(crate) archive: bool,
}

#[derive(Args)]
pub(crate) struct UiArgs {
    /// Agent the UI acts as.
    #[arg(long, default_value = "codex")]
    pub(crate) agent: String,
    /// Address to bind the UI server to.
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) host: String,
    /// TCP port to listen on.
    #[arg(long, default_value_t = 7420)]
    pub(crate) port: u16,
    /// Maximum messages to render per conversation.
    #[arg(long, default_value_t = 80)]
    pub(crate) limit: usize,
}
