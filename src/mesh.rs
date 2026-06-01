use crate::cli::{MeshExportMessageArgs, MeshExportReceiptArgs, MeshImportArgs};
use crate::crypto;
use crate::error::{RaftError, Result};
use crate::identity::{self, Passport};
use crate::storage::{
    agent_path, atomic_write_json, conversation_path, ensure_root, read_json, receipt_path_for,
    set_dir_private, sorted_read_dir,
};
use crate::types::{Agent, Message, Meta, Rate, Receipt};
use crate::util::{iso_now, parse_time, schema_v1, unique, unique_token, validate_id};
use crate::{
    DEFAULT_AGENT_TTL_SECONDS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_RATE_MAX_MESSAGES,
    DEFAULT_RATE_WINDOW_SECONDS, LOCK_TIMEOUT_SECONDS, LOCK_TTL_SECONDS, SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

const RECORD_MESSAGE: &str = "message";
const RECORD_RECEIPT: &str = "receipt";
const MESH_AGENT_EXPIRY: &str = "1970-01-01T00:00:00.000Z";

#[derive(Serialize, Deserialize, Clone)]
struct MeshConversation {
    id: String,
    participants: Vec<String>,
    channel: bool,
    private: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct MeshPacket {
    #[serde(rename = "_v", default = "schema_v1")]
    v: u16,
    packet_id: String,
    from_node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_node: Option<String>,
    issued_at: String,
    nonce: String,
    record_type: String,
    conversation: MeshConversation,
    author: String,
    author_pubkey: String,
    record_id: String,
    record_hash: String,
    passport: Passport,
    record: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sig: Option<String>,
}

struct PacketRoute {
    from_node: String,
    to_node: Option<String>,
}

pub(crate) fn cmd_mesh_export_message(root: &Path, args: MeshExportMessageArgs) -> Result<()> {
    let message_id = validate_id(&args.message, "message id")?;
    let from_node = validate_id(&args.from_node, "node id")?;
    let to_node = args
        .to_node
        .as_deref()
        .map(|value| validate_id(value, "node id"))
        .transpose()?;
    ensure_root(root)?;

    let (_path, message) = find_message(root, &message_id)?;
    let meta = load_conversation(root, &message.conversation_id)?;
    let packet = build_packet(
        root,
        &message.from,
        RECORD_MESSAGE,
        &message.id,
        &message,
        &meta,
        PacketRoute { from_node, to_node },
    )?;
    write_packet(&args.out, &packet, args.json)
}

pub(crate) fn cmd_mesh_export_receipt(root: &Path, args: MeshExportReceiptArgs) -> Result<()> {
    let message_id = validate_id(&args.message, "message id")?;
    let agent_id = validate_id(&args.agent, "agent id")?;
    let from_node = validate_id(&args.from_node, "node id")?;
    let to_node = args
        .to_node
        .as_deref()
        .map(|value| validate_id(value, "node id"))
        .transpose()?;
    ensure_root(root)?;

    let (_message_path, message) = find_message(root, &message_id)?;
    let receipt_path = receipt_path_for(root, &message, &agent_id);
    let receipt: Receipt = read_json(&receipt_path)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("no receipt for message {message_id:?} by @{agent_id}"),
        )
    })?;
    let meta = load_conversation(root, &message.conversation_id)?;
    let record_id = format!("{}-{}", receipt.message_id, receipt.agent);
    let packet = build_packet(
        root,
        &receipt.agent,
        RECORD_RECEIPT,
        &record_id,
        &receipt,
        &meta,
        PacketRoute { from_node, to_node },
    )?;
    write_packet(&args.out, &packet, args.json)
}

pub(crate) fn cmd_mesh_import(root: &Path, args: MeshImportArgs) -> Result<()> {
    let expected_node = args
        .to_node
        .as_deref()
        .map(|value| validate_id(value, "node id"))
        .transpose()?;
    ensure_root(root)?;
    let packet: MeshPacket = read_json(&args.packet)?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("mesh packet {} was not found", args.packet.display()),
        )
    })?;
    verify_packet_address(&packet, expected_node.as_deref())?;
    verify_packet(root, &packet)?;

    let imported = match packet.record_type.as_str() {
        RECORD_MESSAGE => {
            let message: Message = serde_json::from_value(packet.record.clone())?;
            ensure_import_conversation(root, &packet.conversation, &packet.issued_at)?;
            ensure_remote_agent(root, &packet)?;
            import_message(root, &packet, &message)?
        }
        RECORD_RECEIPT => {
            let receipt: Receipt = serde_json::from_value(packet.record.clone())?;
            let (_message_path, message) = find_message(root, &receipt.message_id)?;
            if message.conversation_id != receipt.conversation_id {
                return Err(RaftError::coded(
                    "auth_failed",
                    "receipt conversation does not match imported message",
                ));
            }
            ensure_import_conversation(root, &packet.conversation, &packet.issued_at)?;
            ensure_remote_agent(root, &packet)?;
            import_receipt(root, &message, &receipt)?
        }
        other => {
            return Err(RaftError::coded(
                "parse",
                format!("unknown mesh record type {other:?}"),
            ));
        }
    };

    if args.json {
        emit_ok(serde_json::json!({
            "packet_id": packet.packet_id,
            "record_type": packet.record_type,
            "record_id": packet.record_id,
            "imported": imported.imported,
            "duplicate": imported.duplicate,
            "conversation_id": packet.conversation.id,
            "author": packet.author,
        }))?;
    } else if imported.duplicate {
        println!(
            "mesh packet {} already present ({}/{})",
            packet.packet_id, packet.record_type, packet.record_id
        );
    } else {
        println!(
            "imported mesh packet {} ({}/{})",
            packet.packet_id, packet.record_type, packet.record_id
        );
    }
    Ok(())
}

fn build_packet<T: Serialize>(
    root: &Path,
    author: &str,
    record_type: &str,
    record_id: &str,
    record: &T,
    meta: &Meta,
    route: PacketRoute,
) -> Result<MeshPacket> {
    let author = validate_id(author, "author")?;
    let (_pubkey, keypair) = agent_signing_key(root, &author)?;
    let passport = identity::load_passport(root, &author)?.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!("no passport for @{author}; run `raft id new {author}`"),
        )
    })?;
    passport.verify().map_err(|err| {
        RaftError::coded(
            "auth_failed",
            format!(
                "passport for @{author} failed verification: {}",
                err.message
            ),
        )
    })?;
    let record_value = serde_json::to_value(record)?;
    let record_hash = verify_local_record(&record_value, &passport.pubkey)?;
    let nonce = unique_token();
    let packet_id = packet_id(&route.from_node, &author, record_type, record_id, &nonce);
    let mut packet = MeshPacket {
        v: SCHEMA_VERSION,
        packet_id,
        from_node: route.from_node,
        to_node: route.to_node,
        issued_at: iso_now(),
        nonce,
        record_type: record_type.to_string(),
        conversation: MeshConversation {
            id: meta.id.clone(),
            participants: meta.participants.clone(),
            channel: meta.channel,
            private: meta.private,
        },
        author,
        author_pubkey: passport.pubkey.clone(),
        record_id: record_id.to_string(),
        record_hash,
        passport,
        record: record_value,
        sig: None,
    };
    packet.sig = Some(keypair.sign(&packet_signing_bytes(&packet)?));
    Ok(packet)
}

fn write_packet(out: &Path, packet: &MeshPacket, json: bool) -> Result<()> {
    fs::create_dir_all(out)?;
    set_dir_private(out)?;
    let path = out.join(format!("mesh-{}.json", packet.packet_id));
    atomic_write_json(&path, packet)?;
    if json {
        emit_ok(serde_json::json!({
            "packet": path.display().to_string(),
            "packet_id": packet.packet_id,
            "record_type": packet.record_type,
            "record_id": packet.record_id,
            "conversation_id": packet.conversation.id,
            "author": packet.author,
        }))?;
    } else {
        println!(
            "wrote mesh packet {} ({}/{}) to {}",
            packet.packet_id,
            packet.record_type,
            packet.record_id,
            path.display()
        );
    }
    Ok(())
}

fn verify_packet_address(packet: &MeshPacket, expected_node: Option<&str>) -> Result<()> {
    if let (Some(expected), Some(actual)) = (expected_node, packet.to_node.as_deref())
        && actual != expected
    {
        return Err(RaftError::coded(
            "not_authorized",
            format!("packet is addressed to node {actual:?}, not {expected:?}"),
        ));
    }
    Ok(())
}

fn verify_packet(root: &Path, packet: &MeshPacket) -> Result<()> {
    validate_id(&packet.packet_id, "packet id")?;
    validate_id(&packet.from_node, "node id")?;
    if let Some(to_node) = &packet.to_node {
        validate_id(to_node, "node id")?;
    }
    parse_time(&packet.issued_at)
        .map_err(|_| RaftError::coded("parse", "packet issued_at is not an RFC3339 timestamp"))?;
    validate_id(&packet.conversation.id, "conversation id")?;
    for participant in &packet.conversation.participants {
        validate_id(participant, "participant")?;
    }
    if !packet
        .conversation
        .participants
        .iter()
        .any(|participant| participant == &packet.author)
    {
        return Err(RaftError::coded(
            "auth_failed",
            format!(
                "packet author @{} is not a conversation participant",
                packet.author
            ),
        ));
    }
    verify_passport(packet)?;
    let packet_sig = packet
        .sig
        .as_deref()
        .ok_or_else(|| RaftError::coded("auth_failed", "mesh packet is unsigned"))?;
    crypto::verify(
        &packet.author_pubkey,
        &packet_signing_bytes(packet)?,
        packet_sig,
    )
    .map_err(|err| {
        RaftError::coded(
            "auth_failed",
            format!("mesh packet signature failed verification: {}", err.message),
        )
    })?;

    match packet.record_type.as_str() {
        RECORD_MESSAGE => {
            let message: Message = serde_json::from_value(packet.record.clone())?;
            if message.id != packet.record_id {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet message id does not match record_id",
                ));
            }
            if message.from != packet.author {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet message author does not match passport",
                ));
            }
            if message.conversation_id != packet.conversation.id {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet message conversation does not match manifest",
                ));
            }
            verify_message_routing(packet, &message)?;
            verify_record_value(
                &packet.record,
                &packet.author_pubkey,
                &packet.record_hash,
                "message",
            )?;
        }
        RECORD_RECEIPT => {
            let receipt: Receipt = serde_json::from_value(packet.record.clone())?;
            if format!("{}-{}", receipt.message_id, receipt.agent) != packet.record_id {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet receipt id does not match record_id",
                ));
            }
            if receipt.agent != packet.author {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet receipt author does not match passport",
                ));
            }
            if receipt.conversation_id != packet.conversation.id {
                return Err(RaftError::coded(
                    "auth_failed",
                    "mesh packet receipt conversation does not match manifest",
                ));
            }
            verify_record_value(
                &packet.record,
                &packet.author_pubkey,
                &packet.record_hash,
                "receipt",
            )?;
        }
        other => {
            return Err(RaftError::coded(
                "parse",
                format!("unknown mesh record type {other:?}"),
            ));
        }
    }
    verify_name_binding(root, packet)?;
    Ok(())
}

fn verify_message_routing(packet: &MeshPacket, message: &Message) -> Result<()> {
    let participants = &packet.conversation.participants;
    for recipient in &message.to {
        if recipient != "*" && !participants.iter().any(|item| item == recipient) {
            return Err(RaftError::coded(
                "auth_failed",
                format!("message recipient @{recipient} is not in the mesh conversation"),
            ));
        }
    }
    for mention in &message.mentions {
        if !participants.iter().any(|item| item == mention) {
            return Err(RaftError::coded(
                "auth_failed",
                format!("message mention @{mention} is not in the mesh conversation"),
            ));
        }
    }
    for awaited in &message.needs_response_from {
        if !participants.iter().any(|item| item == awaited) {
            return Err(RaftError::coded(
                "auth_failed",
                format!("message awaits @{awaited}, who is not in the mesh conversation"),
            ));
        }
    }
    Ok(())
}

fn verify_passport(packet: &MeshPacket) -> Result<()> {
    packet.passport.verify().map_err(|err| {
        RaftError::coded(
            "auth_failed",
            format!("passport failed verification: {}", err.message),
        )
    })?;
    if packet.passport.id != packet.author {
        return Err(RaftError::coded(
            "auth_failed",
            "passport id does not match packet author",
        ));
    }
    if packet.passport.pubkey != packet.author_pubkey {
        return Err(RaftError::coded(
            "auth_failed",
            "passport pubkey does not match packet author key",
        ));
    }
    Ok(())
}

fn verify_name_binding(root: &Path, packet: &MeshPacket) -> Result<()> {
    let passport_path = passport_path(root, &packet.author);
    if let Some(existing) = read_json::<Passport>(&passport_path)? {
        existing.verify().map_err(|err| {
            RaftError::coded(
                "auth_failed",
                format!(
                    "stored passport for @{} is invalid: {}",
                    packet.author, err.message
                ),
            )
        })?;
        if existing.pubkey != packet.author_pubkey {
            return Err(RaftError::coded(
                "conflict",
                format!(
                    "agent name @{} is already bound to a different passport key",
                    packet.author
                ),
            ));
        }
    }
    if let Some(existing) = read_json::<Agent>(&agent_path(root, &packet.author))? {
        match existing.pubkey.as_deref() {
            Some(pubkey) if pubkey == packet.author_pubkey => {}
            Some(_) => {
                return Err(RaftError::coded(
                    "conflict",
                    format!(
                        "agent name @{} is already claimed by a different key",
                        packet.author
                    ),
                ));
            }
            None => {
                return Err(RaftError::coded(
                    "conflict",
                    format!(
                        "agent name @{} exists locally without a passport binding",
                        packet.author
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn ensure_remote_agent(root: &Path, packet: &MeshPacket) -> Result<()> {
    let passport_path = passport_path(root, &packet.author);
    if read_json::<Passport>(&passport_path)?.is_none() {
        atomic_write_json(&passport_path, &packet.passport)?;
    }
    if read_json::<Agent>(&agent_path(root, &packet.author))?.is_none() {
        let agent = Agent {
            v: SCHEMA_VERSION,
            id: packet.author.clone(),
            mention: format!("@{}", packet.author),
            workspace: None,
            capabilities: packet.passport.capabilities.clone(),
            pubkey: Some(packet.author_pubkey.clone()),
            pid: 0,
            host: format!("mesh:{}", packet.from_node),
            last_seen_at: packet.issued_at.clone(),
            ttl_seconds: DEFAULT_AGENT_TTL_SECONDS,
            expires_at: MESH_AGENT_EXPIRY.to_string(),
            current_state: "away".to_string(),
            state_note: Some("imported from mesh packet".to_string()),
            state_updated_at: packet.issued_at.clone(),
        };
        atomic_write_json(&agent_path(root, &packet.author), &agent)?;
    }
    Ok(())
}

fn ensure_import_conversation(
    root: &Path,
    conversation: &MeshConversation,
    issued_at: &str,
) -> Result<()> {
    let conversation_id = validate_id(&conversation.id, "conversation id")?;
    let participants = unique(conversation.participants.clone());
    if participants.len() < 2 {
        return Err(RaftError::coded(
            "auth_failed",
            "mesh conversation must have at least two participants",
        ));
    }
    for participant in &participants {
        validate_id(participant, "participant")?;
    }

    let _lock = crate::storage::DirLock::acquire(
        root,
        &format!("conversation-{conversation_id}"),
        LOCK_TTL_SECONDS,
        LOCK_TIMEOUT_SECONDS,
    )?;
    let conv = conversation_path(root, &conversation_id)?;
    fs::create_dir_all(conv.join("messages"))?;
    fs::create_dir_all(conv.join("receipts"))?;
    fs::create_dir_all(conv.join("executions"))?;
    fs::create_dir_all(conv.join("streams"))?;
    set_dir_private(&conv)?;
    for child in ["messages", "receipts", "executions", "streams"] {
        set_dir_private(&conv.join(child))?;
    }

    if let Some(mut meta) = read_json::<Meta>(&conv.join("meta.json"))? {
        if meta.channel != conversation.channel || meta.private != conversation.private {
            return Err(RaftError::coded(
                "conflict",
                format!("conversation {conversation_id:?} already exists with different privacy"),
            ));
        }
        let mut changed = false;
        for participant in participants {
            if !meta.participants.iter().any(|item| item == &participant) {
                meta.participants.push(participant.clone());
                meta.joined_at.insert(participant, issued_at.to_string());
                changed = true;
            }
        }
        if changed {
            meta.updated_at = issued_at.to_string();
            atomic_write_json(&conv.join("meta.json"), &meta)?;
        }
        return Ok(());
    }

    let joined_at = participants
        .iter()
        .map(|participant| (participant.clone(), issued_at.to_string()))
        .collect::<BTreeMap<_, _>>();
    let meta = Meta {
        v: SCHEMA_VERSION,
        id: conversation_id,
        participants,
        channel: conversation.channel,
        private: conversation.private,
        state: "open".to_string(),
        created_at: issued_at.to_string(),
        updated_at: issued_at.to_string(),
        retention_days: 14,
        rate: Rate {
            window_seconds: DEFAULT_RATE_WINDOW_SECONDS,
            max_messages_per_sender: DEFAULT_RATE_MAX_MESSAGES,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        },
        joined_at,
    };
    atomic_write_json(&conv.join("meta.json"), &meta)
}

#[derive(Clone, Copy)]
struct ImportOutcome {
    imported: bool,
    duplicate: bool,
}

fn import_message(root: &Path, _packet: &MeshPacket, message: &Message) -> Result<ImportOutcome> {
    let path = conversation_path(root, &message.conversation_id)?
        .join("messages")
        .join(format!("{}.json", message.id));
    if let Some(existing) = read_json::<Message>(&path)? {
        if existing.hash == message.hash && existing.sig == message.sig {
            return Ok(ImportOutcome {
                imported: false,
                duplicate: true,
            });
        }
        return Err(RaftError::coded(
            "conflict",
            format!(
                "message {:?} already exists with different content",
                message.id
            ),
        ));
    }
    atomic_write_json(&path, message)?;
    Ok(ImportOutcome {
        imported: true,
        duplicate: false,
    })
}

fn import_receipt(root: &Path, message: &Message, receipt: &Receipt) -> Result<ImportOutcome> {
    let path = receipt_path_for(root, message, &receipt.agent);
    if let Some(existing) = read_json::<Receipt>(&path)? {
        if existing.hash == receipt.hash && existing.sig == receipt.sig {
            return Ok(ImportOutcome {
                imported: false,
                duplicate: true,
            });
        }
        let existing_time = parse_time(&existing.updated_at).map_err(|_| {
            RaftError::coded(
                "parse",
                "stored receipt updated_at is not an RFC3339 timestamp",
            )
        })?;
        let incoming_time = parse_time(&receipt.updated_at).map_err(|_| {
            RaftError::coded(
                "parse",
                "incoming receipt updated_at is not an RFC3339 timestamp",
            )
        })?;
        if incoming_time <= existing_time {
            return Ok(ImportOutcome {
                imported: false,
                duplicate: true,
            });
        }
    }
    atomic_write_json(&path, receipt)?;
    Ok(ImportOutcome {
        imported: true,
        duplicate: false,
    })
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
                RaftError::new(format!("message file disappeared: {}", path.display()))
            })?;
            return Ok((path, message));
        }
    }
    Err(RaftError::coded(
        "not_found",
        format!("message {message_id:?} was not found"),
    ))
}

fn load_conversation(root: &Path, conversation_id: &str) -> Result<Meta> {
    let conversation_id = validate_id(conversation_id, "conversation id")?;
    read_json(&conversation_path(root, &conversation_id)?.join("meta.json"))?.ok_or_else(|| {
        RaftError::coded(
            "not_found",
            format!("conversation {conversation_id:?} does not exist"),
        )
    })
}

fn agent_signing_key(root: &Path, agent_id: &str) -> Result<(String, crypto::Keypair)> {
    let agent: Agent = read_json(&agent_path(root, agent_id))?.ok_or_else(|| {
        RaftError::coded(
            "not_claimed",
            format!("agent {agent_id:?} has not been claimed"),
        )
    })?;
    let pubkey = agent.pubkey.ok_or_else(|| {
        RaftError::coded(
            "auth_failed",
            format!("agent @{agent_id} is not bound to a passport key"),
        )
    })?;
    let keypair = identity::load_bound_keypair(root, agent_id, &pubkey)?;
    Ok((pubkey, keypair))
}

fn verify_local_record(record: &serde_json::Value, expected_pubkey: &str) -> Result<String> {
    verify_record_value(record, expected_pubkey, "", "record")
}

fn verify_record_value(
    record: &serde_json::Value,
    expected_pubkey: &str,
    expected_hash: &str,
    label: &str,
) -> Result<String> {
    let signer = record
        .get("signer_key")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RaftError::coded("auth_failed", format!("{label} is unsigned")))?;
    if signer != expected_pubkey {
        return Err(RaftError::coded(
            "auth_failed",
            format!("{label} signer key does not match passport"),
        ));
    }
    let stored_hash = record
        .get("hash")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RaftError::coded("auth_failed", format!("{label} has no content hash")))?;
    let computed_hash = signed_record_hash(record)?;
    if stored_hash != computed_hash {
        return Err(RaftError::coded(
            "auth_failed",
            format!("{label} content hash failed verification"),
        ));
    }
    if !expected_hash.is_empty() && computed_hash != expected_hash {
        return Err(RaftError::coded(
            "auth_failed",
            format!("{label} hash does not match packet manifest"),
        ));
    }
    let sig = record
        .get("sig")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| RaftError::coded("auth_failed", format!("{label} is unsigned")))?;
    crypto::verify(
        expected_pubkey,
        &crypto::canonical_omitting(record, &["sig"])?,
        sig,
    )
    .map_err(|err| {
        RaftError::coded(
            "auth_failed",
            format!("{label} signature failed verification: {}", err.message),
        )
    })?;
    Ok(computed_hash)
}

fn signed_record_hash(record: &serde_json::Value) -> Result<String> {
    Ok(crypto::sha256_hex(&crypto::canonical_omitting(
        record,
        &["hash", "sig"],
    )?))
}

fn packet_signing_bytes(packet: &MeshPacket) -> Result<Vec<u8>> {
    let value = serde_json::to_value(packet)?;
    crypto::canonical_omitting(&value, &["sig"])
}

fn packet_id(
    from_node: &str,
    author: &str,
    record_type: &str,
    record_id: &str,
    nonce: &str,
) -> String {
    let digest = crypto::sha256_hex(
        format!("{from_node}:{author}:{record_type}:{record_id}:{nonce}").as_bytes(),
    );
    let hex = digest.strip_prefix(crypto::HASH_PREFIX).unwrap_or(&digest);
    format!("pkt-{}", &hex[..32])
}

fn passport_path(root: &Path, id: &str) -> PathBuf {
    root.join("agents").join(format!("{id}.passport.json"))
}

fn emit_ok(fields: serde_json::Value) -> Result<()> {
    let mut map = serde_json::Map::new();
    map.insert("ok".to_string(), serde_json::Value::Bool(true));
    if let serde_json::Value::Object(extra) = fields {
        map.extend(extra);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Object(map))?
    );
    Ok(())
}
