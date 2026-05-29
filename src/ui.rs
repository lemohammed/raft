use crate::cli::UiArgs;
use crate::error::{RaftError, Result};
use crate::storage::{
    DirLock, atomic_write_json, conversation_path, ensure_root, read_json, set_dir_private,
    sorted_read_dir, target_room,
};
use crate::types::{
    Agent, HttpRequest, Message, Meta, Rate, SendMessageInput, UiAgent, UiChannelRequest,
    UiConversation, UiJoinRequest, UiMessage, UiOpenRequest, UiSendRequest, UiSnapshot, UiTotals,
};
use crate::ui_html::UI_HTML;
use crate::util::{
    generated_private_conversation_id, iso_now, parse_time, split_csv, unique, validate_id,
};
use crate::{
    DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_RATE_MAX_MESSAGES, DEFAULT_RATE_WINDOW_SECONDS,
    LOCK_TIMEOUT_SECONDS, LOCK_TTL_SECONDS, SCHEMA_VERSION,
};
use crate::{
    gather_open_asks, message_is_unread, message_visible_to, send_message, write_system_message,
};
use chrono::Utc;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::time::Duration;

pub(crate) fn cmd_ui(root: &Path, args: UiArgs) -> Result<()> {
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
    )?
    .id;
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
