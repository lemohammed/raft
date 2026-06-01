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
  raft swarm candidates --capability <tag> --json
                                       rank workers by skill, state, and load
  raft swarm assign --from <me> --channel <room> --capability <tag> ...
                                       pick workers and open an actionable ask
  raft swarm dispatch --from <me> --channel <room> --capability <tag> --tool <tool>
                                       pick one worker and enqueue an executable task
  raft channel list --agent <me>       discover channels you can join

EXIT CODES
  0  success
  1  error (generic failure; see error code for the specific category)
  2  timeout (e.g. `wait` reached its deadline with no unread message)

ERROR CODES (stable; surfaced as error.code in --json mode)
  not_claimed       agent name has not been claimed; run `raft claim`
  not_found         referenced agent, channel, or conversation does not exist
  not_participant   agent or recipient is not a participant in the conversation
  not_awaited       `ack --require-open` closed no open ask you are awaited on
  auth_failed       a signature, passport, signed record, or local key binding \
failed verification
  not_authorized    a capability token does not authorize the requested action
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
    /// Get or set an agent's published state (one of: idle, working, blocked, away).
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
    /// Withdraw an ask you sent so it stops counting as open (you no longer need the reply).
    Withdraw(WithdrawArgs),
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
    /// Record an acknowledgement receipt (received, accepted, working, blocked, done, rejected).
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
    /// Manage an agent's cryptographic identity (Ed25519 keypair + passport).
    Id {
        #[command(subcommand)]
        command: IdCommand,
    },
    /// Issue, attenuate, and verify capability tokens (scoped, delegable authority).
    Grant {
        #[command(subcommand)]
        command: GrantCommand,
    },
    /// Delegate, track, and cancel remote tasks (capability-gated tool calls).
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// Export and import signed mesh packets between bus roots.
    Mesh {
        #[command(subcommand)]
        command: MeshCommand,
    },
    /// Rank and assign agents for swarm-style collaboration.
    Swarm {
        #[command(subcommand)]
        command: SwarmCommand,
    },
    /// Run the executor loop: claim authorized task asks and run their tools sandboxed.
    Run(RunArgs),
}

#[derive(Subcommand)]
pub(crate) enum TaskCommand {
    /// Dispatch a tool call to a worker as a capability-gated task ask.
    Dispatch(TaskDispatchArgs),
    /// Show a task's status (worker receipt lifecycle) and result, if any.
    Status(TaskStatusArgs),
    /// Cancel a task you dispatched (withdraws the obligation).
    Cancel(TaskCancelArgs),
}

#[derive(Args)]
pub(crate) struct TaskDispatchArgs {
    /// Dispatching agent id.
    #[arg(long)]
    pub(crate) from: String,
    /// Worker agent id to assign the task to.
    #[arg(long)]
    pub(crate) to: String,
    /// Conversation id the task belongs to.
    #[arg(long)]
    pub(crate) conversation: Option<String>,
    /// Channel id the task belongs to.
    #[arg(long)]
    pub(crate) channel: Option<String>,
    /// Tool name to invoke (the Hermes tool_call name).
    #[arg(long)]
    pub(crate) tool: String,
    /// Tool arguments as a JSON object (default `{}`).
    #[arg(long, default_value = "{}")]
    pub(crate) args: String,
    /// Capability token file authorizing the worker to run this tool.
    #[arg(long)]
    pub(crate) cap: Option<PathBuf>,
    /// Maximum runtime, seconds (also bounded by any capability limit).
    #[arg(long = "max-runtime-s")]
    pub(crate) max_runtime_s: Option<u64>,
    /// Maximum captured output, bytes.
    #[arg(long = "max-output-bytes")]
    pub(crate) max_output_bytes: Option<u64>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct TaskStatusArgs {
    /// Task id (the dispatched message id).
    pub(crate) task: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct TaskCancelArgs {
    /// Task id (the dispatched message id).
    pub(crate) task: String,
    /// Dispatching agent id (must be the task's sender).
    #[arg(long)]
    pub(crate) from: String,
    /// Optional reason recorded on the cancellation notice.
    #[arg(long)]
    pub(crate) reason: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum MeshCommand {
    /// Export a signed message packet that another bus can import.
    ExportMessage(MeshExportMessageArgs),
    /// Export a signed receipt packet that another bus can import.
    ExportReceipt(MeshExportReceiptArgs),
    /// Import and verify a signed mesh packet.
    Import(MeshImportArgs),
}

#[derive(Args)]
pub(crate) struct MeshExportMessageArgs {
    /// Message id to export.
    pub(crate) message: String,
    /// Directory where the packet JSON should be written.
    #[arg(long)]
    pub(crate) out: PathBuf,
    /// Sender-side node id recorded in the packet.
    #[arg(long = "from-node", default_value = "local")]
    pub(crate) from_node: String,
    /// Optional intended recipient node id.
    #[arg(long = "to-node")]
    pub(crate) to_node: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct MeshExportReceiptArgs {
    /// Message id whose receipt should be exported.
    pub(crate) message: String,
    /// Agent id that authored the receipt.
    #[arg(long)]
    pub(crate) agent: String,
    /// Directory where the packet JSON should be written.
    #[arg(long)]
    pub(crate) out: PathBuf,
    /// Sender-side node id recorded in the packet.
    #[arg(long = "from-node", default_value = "local")]
    pub(crate) from_node: String,
    /// Optional intended recipient node id.
    #[arg(long = "to-node")]
    pub(crate) to_node: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct MeshImportArgs {
    /// Packet JSON produced by `mesh export-message` or `mesh export-receipt`.
    pub(crate) packet: PathBuf,
    /// Reject packets addressed to a different node.
    #[arg(long = "to-node")]
    pub(crate) to_node: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum SwarmCommand {
    /// Rank live agents by capability match, availability, and current open-ask load.
    Candidates(SwarmCandidatesArgs),
    /// Pick the best matching agents in a channel and send them an actionable ask.
    Assign(SwarmAssignArgs),
    /// Pick the best matching agent in a channel and dispatch an executable task.
    Dispatch(SwarmDispatchArgs),
}

#[derive(Args)]
pub(crate) struct SwarmCandidatesArgs {
    /// Required capability tag. Repeat, or pass comma-separated tags.
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
    /// Exclude an agent id from consideration. Repeat, or pass comma-separated ids.
    #[arg(long = "exclude")]
    pub(crate) exclude: Vec<String>,
    /// Include partial matches instead of requiring every requested capability.
    #[arg(long = "allow-partial")]
    pub(crate) allow_partial: bool,
    /// Include stale agents with a large score penalty.
    #[arg(long)]
    pub(crate) all: bool,
    /// Maximum number of candidates to return.
    #[arg(long, default_value_t = 10)]
    pub(crate) limit: usize,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct SwarmAssignArgs {
    /// Coordinator/asker agent id.
    #[arg(long = "from")]
    pub(crate) sender: String,
    /// Existing channel where the ask should be sent.
    #[arg(long)]
    pub(crate) channel: String,
    /// Required capability tag. Repeat, or pass comma-separated tags.
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
    /// Exclude an agent id from consideration. Repeat, or pass comma-separated ids.
    #[arg(long = "exclude")]
    pub(crate) exclude: Vec<String>,
    /// Number of agents to select.
    #[arg(long, default_value_t = 1)]
    pub(crate) count: usize,
    /// Subject line for the assignment ask.
    #[arg(long)]
    pub(crate) subject: String,
    /// Body for the assignment ask.
    #[arg(long)]
    pub(crate) body: String,
    /// Preview the selected agents without sending a message.
    #[arg(long = "dry-run")]
    pub(crate) dry_run: bool,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct SwarmDispatchArgs {
    /// Coordinator/dispatcher agent id.
    #[arg(long = "from")]
    pub(crate) sender: String,
    /// Existing channel whose live members should be considered as workers.
    #[arg(long)]
    pub(crate) channel: String,
    /// Required worker capability tag. Repeat, or pass comma-separated tags.
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
    /// Exclude an agent id from consideration. Repeat, or pass comma-separated ids.
    #[arg(long = "exclude")]
    pub(crate) exclude: Vec<String>,
    /// Tool name to invoke on the selected worker.
    #[arg(long)]
    pub(crate) tool: String,
    /// Tool arguments as a JSON object (default `{}`).
    #[arg(long, default_value = "{}")]
    pub(crate) args: String,
    /// Capability token file authorizing the selected worker to run this tool.
    #[arg(long)]
    pub(crate) cap: Option<PathBuf>,
    /// Maximum runtime, seconds (also bounded by any capability limit).
    #[arg(long = "max-runtime-s")]
    pub(crate) max_runtime_s: Option<u64>,
    /// Maximum captured output, bytes.
    #[arg(long = "max-output-bytes")]
    pub(crate) max_output_bytes: Option<u64>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct RunArgs {
    /// Worker agent id whose authorized tasks this executor will run.
    pub(crate) agent: String,
    /// Tool registration as `name=/path/to/executable` (repeatable).
    #[arg(long = "tool")]
    pub(crate) tool: Vec<String>,
    /// Pin the trusted capability root: an agent id or `ed25519:<hex>` key.
    /// When set, a task whose capability is not rooted here is rejected.
    #[arg(long)]
    pub(crate) trust: Option<String>,
    /// Process the currently-pending tasks once and exit (good for cron/tests).
    #[arg(long)]
    pub(crate) once: bool,
    /// Poll interval in seconds when looping (ignored with --once).
    #[arg(long, default_value_t = 1.0)]
    pub(crate) interval: f64,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum GrantCommand {
    /// Issue a new root capability from an issuer to a holder.
    New(GrantNewArgs),
    /// Attenuate (narrow) an existing capability and re-delegate it.
    Attenuate(GrantAttenuateArgs),
    /// Verify a capability authorizes a specific action.
    Verify(GrantVerifyArgs),
    /// Verify a capability's signatures and print its chain and effective scope.
    Inspect(GrantInspectArgs),
}

/// Caveats shared by `grant new` and `grant attenuate`. Each narrows authority;
/// omitted dimensions are left unconstrained by the block being created.
#[derive(Args)]
pub(crate) struct CaveatArgs {
    /// Comma-separated allowed action verbs (e.g. tool.run,task.dispatch).
    #[arg(long)]
    pub(crate) action: Option<String>,
    /// Comma-separated allowed tool names.
    #[arg(long)]
    pub(crate) tool: Option<String>,
    /// Restrict to a single conversation id.
    #[arg(long)]
    pub(crate) conversation: Option<String>,
    /// Comma-separated allowed execution environments.
    #[arg(long)]
    pub(crate) env: Option<String>,
    /// Expiry as a duration from now (e.g. 30m, 2h, 7d).
    #[arg(long)]
    pub(crate) ttl: Option<String>,
    /// Maximum task runtime in seconds.
    #[arg(long = "max-runtime-s")]
    pub(crate) max_runtime_s: Option<u64>,
    /// Maximum captured output in bytes.
    #[arg(long = "max-output-bytes")]
    pub(crate) max_output_bytes: Option<u64>,
}

#[derive(Args)]
pub(crate) struct GrantNewArgs {
    /// Issuing agent id (must have a local keypair from `raft id new`).
    #[arg(long)]
    pub(crate) issuer: String,
    /// Holder: an agent id with a passport on this bus, or an `ed25519:<hex>` key.
    #[arg(long)]
    pub(crate) to: String,
    #[command(flatten)]
    pub(crate) caveats: CaveatArgs,
    /// Write the token to this file instead of stdout.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
    /// Emit machine-readable JSON instead of text (when writing to --out).
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct GrantAttenuateArgs {
    /// Current holder agent id (must hold the token and have a local keypair).
    #[arg(long)]
    pub(crate) holder: String,
    /// New holder: an agent id with a passport, or an `ed25519:<hex>` key.
    #[arg(long)]
    pub(crate) to: String,
    /// Path to the token to attenuate.
    #[arg(long = "token-file")]
    pub(crate) token_file: PathBuf,
    #[command(flatten)]
    pub(crate) caveats: CaveatArgs,
    /// Write the attenuated token to this file instead of stdout.
    #[arg(long)]
    pub(crate) out: Option<PathBuf>,
    /// Emit machine-readable JSON instead of text (when writing to --out).
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct GrantVerifyArgs {
    /// Path to the token to verify.
    #[arg(long = "token-file")]
    pub(crate) token_file: PathBuf,
    /// Expected root issuer: an agent id or `ed25519:<hex>` key. Pins trust.
    #[arg(long)]
    pub(crate) root: Option<String>,
    /// Action verb to authorize (required).
    #[arg(long)]
    pub(crate) action: String,
    /// Conversation context for the request.
    #[arg(long)]
    pub(crate) conversation: Option<String>,
    /// Tool context for the request.
    #[arg(long)]
    pub(crate) tool: Option<String>,
    /// Environment context for the request.
    #[arg(long)]
    pub(crate) env: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct GrantInspectArgs {
    /// Path to the token to inspect.
    #[arg(long = "token-file")]
    pub(crate) token_file: PathBuf,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum IdCommand {
    /// Generate a new keypair and self-signed passport for an agent.
    New(IdNewArgs),
    /// Print an agent's passport (its shareable public identity).
    Show(IdShowArgs),
    /// Verify a passport's self-signature (by id on this bus, or a file).
    Verify(IdVerifyArgs),
    /// Print an agent's short public-key fingerprint.
    Fingerprint(IdFingerprintArgs),
}

#[derive(Args)]
pub(crate) struct IdNewArgs {
    /// Agent id to mint an identity for.
    pub(crate) agent: String,
    /// Comma-separated capability tags to record in the passport.
    #[arg(long)]
    pub(crate) capabilities: Option<String>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct IdShowArgs {
    /// Agent id whose passport to print.
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct IdVerifyArgs {
    /// Agent id whose stored passport to verify.
    pub(crate) agent: Option<String>,
    /// Verify a passport JSON file instead of a stored agent passport.
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Args)]
pub(crate) struct IdFingerprintArgs {
    /// Agent id whose fingerprint to print.
    pub(crate) agent: String,
    /// Emit machine-readable JSON instead of text.
    #[arg(long)]
    pub(crate) json: bool,
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
    /// State label to publish. One of: idle, working, blocked, away.
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
    /// Message kind. One of: message, event, receipt, task, summary (`system` is reserved for raft).
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
    /// Message kind for this reply. Use `summary` to publish the next rolling conversation memory.
    #[arg(long, default_value = "message")]
    pub(crate) kind: String,
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
pub(crate) struct WithdrawArgs {
    /// Agent withdrawing the ask. Must be the sender of the message.
    #[arg(long)]
    pub(crate) from: String,
    /// Id of the ask to withdraw.
    pub(crate) message_id: String,
    /// Optional human-readable reason recorded with the withdrawal.
    #[arg(long)]
    pub(crate) reason: Option<String>,
    /// Emit machine-readable JSON instead of text.
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
    /// Block until an open ask you are owed closes (a terminal ack), instead of
    /// waiting for an unread message. Reports the resolved ask.
    #[arg(long, conflicts_with_all = ["conversation", "channel", "resolved"])]
    pub(crate) owed: bool,
    /// Block until this specific ask you own closes (a terminal ack). Errors
    /// `not_found` if the id is not an ask you are owed.
    #[arg(long, conflicts_with_all = ["conversation", "channel"])]
    pub(crate) resolved: Option<String>,
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
    /// Substring to match across id/conversation/from/subject/body. Optional
    /// when at least one of --from/--kind/--mentions is given.
    pub(crate) pattern: Option<String>,
    /// Agent whose visible messages to search.
    #[arg(long)]
    pub(crate) agent: String,
    /// Only match messages sent by this agent.
    #[arg(long)]
    pub(crate) from: Option<String>,
    /// Only match messages of this kind (message, event, system, receipt).
    #[arg(long)]
    pub(crate) kind: Option<String>,
    /// Only match messages that mention or are addressed to this agent.
    #[arg(long)]
    pub(crate) mentions: Option<String>,
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
    /// Fail with `not_awaited` unless this ack actually closes an open ask you
    /// are awaited on, guarding against a `done`/`rejected` that silently lands
    /// on the wrong message and leaves the asker blocked forever.
    #[arg(long)]
    pub(crate) require_open: bool,
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
