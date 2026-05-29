use crate::SCHEMA_VERSION;
use crate::util::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Agent {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) mention: String,
    pub(crate) workspace: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) pid: u32,
    pub(crate) host: String,
    pub(crate) last_seen_at: String,
    pub(crate) ttl_seconds: u64,
    pub(crate) expires_at: String,
    #[serde(default = "default_agent_state")]
    pub(crate) current_state: String,
    #[serde(default)]
    pub(crate) state_note: Option<String>,
    #[serde(default = "iso_now")]
    pub(crate) state_updated_at: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Rate {
    pub(crate) window_seconds: u64,
    pub(crate) max_messages_per_sender: u64,
    pub(crate) max_message_bytes: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Meta {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) id: String,
    pub(crate) participants: Vec<String>,
    #[serde(default)]
    pub(crate) channel: bool,
    pub(crate) private: bool,
    pub(crate) state: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) retention_days: u64,
    pub(crate) rate: Rate,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Message {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) id: String,
    pub(crate) conversation_id: String,
    pub(crate) kind: String,
    pub(crate) from: String,
    pub(crate) to: Vec<String>,
    #[serde(default)]
    pub(crate) mentions: Vec<String>,
    pub(crate) subject: String,
    pub(crate) body: String,
    pub(crate) created_at: String,
    pub(crate) requires_ack: bool,
    #[serde(default)]
    pub(crate) needs_response_from: Vec<String>,
    #[serde(default)]
    pub(crate) subject_id: Option<String>,
    pub(crate) after: Option<String>,
    /// Set when the sender retracts an ask it opened. Once present, the message
    /// is no longer an open ask: it drops out of every `awaited` computation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) withdrawn: Option<Withdrawal>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Withdrawal {
    pub(crate) by: String,
    pub(crate) at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

/// A `Message` decorated with fields relative to the agent reading it, so a
/// `--json` consumer can answer "is this new?" and "do I still owe a reply?"
/// without a follow-up `awaiting`/`receipts` round-trip per message. The
/// `Message` fields are flattened in, so this is a superset of the raw shape.
#[derive(Serialize)]
pub(crate) struct ViewMessage {
    #[serde(flatten)]
    pub(crate) message: Message,
    /// The viewer has no read receipt on this message yet.
    pub(crate) unread: bool,
    /// The viewer is in this message's still-open awaited set (it requested an
    /// ack or named the viewer in `needs_response_from`) and the viewer has not
    /// yet recorded a terminal receipt.
    pub(crate) awaiting_me: bool,
    /// The viewer's current ack status on this message, or `null` if none.
    pub(crate) my_status: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct RateState {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) senders: BTreeMap<String, SenderRate>,
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
pub(crate) struct SenderRate {
    pub(crate) window_start: String,
    pub(crate) count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_sent_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct ReceiptEvent {
    pub(crate) status: String,
    pub(crate) at: String,
    pub(crate) note: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Receipt {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) message_id: String,
    pub(crate) conversation_id: String,
    pub(crate) agent: String,
    pub(crate) status: String,
    pub(crate) updated_at: String,
    pub(crate) note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) read_at: Option<String>,
    pub(crate) history: Vec<ReceiptEvent>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct WatchState {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) agent: String,
    pub(crate) pid: u32,
    pub(crate) host: String,
    pub(crate) started_at: String,
    pub(crate) updated_at: String,
    pub(crate) last_event_id: Option<String>,
    pub(crate) shutdown_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct HeartbeatState {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) agent: String,
    pub(crate) pid: u32,
    pub(crate) host: String,
    pub(crate) started_at: String,
    pub(crate) updated_at: String,
    pub(crate) last_heartbeat_at: String,
    pub(crate) interval_seconds: f64,
    pub(crate) ttl_seconds: u64,
    pub(crate) shutdown_at: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct ThreadNode {
    pub(crate) message: Message,
    pub(crate) children: Vec<ThreadNode>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct LockOwner {
    #[serde(rename = "_v", default = "schema_v1")]
    pub(crate) v: u16,
    pub(crate) token: String,
    pub(crate) pid: u32,
    pub(crate) host: String,
    pub(crate) acquired_at: String,
    pub(crate) expires_at: String,
}

#[derive(Serialize)]
pub(crate) struct UiSnapshot {
    pub(crate) root: String,
    pub(crate) agent: String,
    pub(crate) generated_at: String,
    pub(crate) totals: UiTotals,
    pub(crate) agents: Vec<UiAgent>,
    pub(crate) conversations: Vec<UiConversation>,
}

#[derive(Serialize)]
pub(crate) struct UiTotals {
    pub(crate) active_agents: usize,
    pub(crate) stale_agents: usize,
    pub(crate) conversations: usize,
    pub(crate) unread_messages: usize,
    pub(crate) messages: usize,
}

#[derive(Serialize)]
pub(crate) struct UiAgent {
    pub(crate) id: String,
    pub(crate) mention: String,
    pub(crate) workspace: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) current_state: String,
    pub(crate) state_note: Option<String>,
    pub(crate) state_updated_at: String,
    pub(crate) last_seen_at: String,
    pub(crate) expires_at: String,
    pub(crate) active: bool,
}

#[derive(Serialize)]
pub(crate) struct UiConversation {
    pub(crate) id: String,
    pub(crate) participants: Vec<String>,
    pub(crate) channel: bool,
    pub(crate) private: bool,
    pub(crate) joined: bool,
    pub(crate) message_count: usize,
    pub(crate) unread_count: usize,
    pub(crate) open_asks: usize,
    pub(crate) latest_at: Option<String>,
    pub(crate) messages: Vec<UiMessage>,
}

#[derive(Serialize)]
pub(crate) struct UiMessage {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) from: String,
    pub(crate) to: Vec<String>,
    pub(crate) mentions: Vec<String>,
    pub(crate) subject: String,
    pub(crate) body: String,
    pub(crate) created_at: String,
    pub(crate) requires_ack: bool,
    pub(crate) needs_response_from: Vec<String>,
    pub(crate) unread: bool,
    pub(crate) after: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct UiSendRequest {
    pub(crate) agent: String,
    pub(crate) conversation: Option<String>,
    pub(crate) channel: Option<String>,
    pub(crate) to: String,
    #[serde(default)]
    pub(crate) subject: String,
    pub(crate) body: String,
    #[serde(default = "default_message_kind")]
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) requires_ack: bool,
    #[serde(default)]
    pub(crate) needs_response_from: Vec<String>,
    #[serde(default)]
    pub(crate) after: Option<String>,
    #[serde(default)]
    pub(crate) subject_id: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct UiOpenRequest {
    pub(crate) agent: String,
    pub(crate) to: String,
    #[serde(default)]
    pub(crate) topic: String,
}

#[derive(Deserialize)]
pub(crate) struct UiChannelRequest {
    pub(crate) agent: String,
    pub(crate) channel: String,
    #[serde(default)]
    pub(crate) members: String,
}

#[derive(Deserialize)]
pub(crate) struct UiJoinRequest {
    pub(crate) agent: String,
    pub(crate) channel: String,
}

pub(crate) struct SendMessageInput {
    pub(crate) conversation_id: String,
    pub(crate) sender: String,
    pub(crate) to: String,
    pub(crate) subject: String,
    pub(crate) body: String,
    pub(crate) kind: String,
    pub(crate) after: Option<String>,
    pub(crate) subject_id: Option<String>,
    pub(crate) requires_ack: bool,
    pub(crate) needs_response_from: String,
}

pub(crate) struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

#[derive(Serialize)]
pub(crate) struct JournalEntry {
    #[serde(rename = "_v")]
    pub(crate) v: u16,
    pub(crate) id: String,
    pub(crate) agent: String,
    pub(crate) kind: String,
    pub(crate) subject: String,
    pub(crate) body: String,
    pub(crate) created_at: String,
}

#[derive(Serialize, Clone)]
pub(crate) struct OpenAsk {
    pub(crate) conversation_id: String,
    pub(crate) message_id: String,
    pub(crate) from: String,
    pub(crate) awaited: String,
    pub(crate) subject: String,
    pub(crate) created_at: String,
    pub(crate) status: String,
    pub(crate) awaited_live: bool,
    pub(crate) await_kind: &'static str,
}
