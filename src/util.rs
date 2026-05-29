use crate::SCHEMA_VERSION;
use crate::error::Result;
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use std::collections::BTreeSet;
use std::env;
use std::path::Path;
use std::process;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) fn process_is_alive(pid: u32) -> bool {
    process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

pub(crate) fn sleep_interruptibly(duration: Duration, shutdown: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(100)));
    }
}

pub(crate) fn validate_agent_state(value: &str) -> Result<String> {
    match value {
        "idle" | "working" | "blocked" | "away" => Ok(value.to_string()),
        _ => bail!("invalid state {value:?}; use idle, working, blocked, or away"),
    }
}

pub(crate) fn validate_id(value: &str, label: &str) -> Result<String> {
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

pub(crate) fn validate_claim_name(value: &str) -> Result<String> {
    let agent_id = validate_id(value.trim_start_matches('@'), "agent name")?;
    if agent_id.len() < 3 {
        bail!("agent name @{agent_id} is too short; choose a unique, personable name");
    }
    Ok(agent_id)
}

pub(crate) fn split_csv(value: &str) -> Result<Vec<String>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| validate_id(item, "id"))
        .collect()
}

pub(crate) fn split_recipients(value: &str) -> Result<Vec<String>> {
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

pub(crate) fn extract_mentions(value: &str) -> Vec<String> {
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

pub(crate) fn generated_private_conversation_id(participants: &[String], topic: &str) -> String {
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

pub(crate) fn slugify_id_segment(value: &str) -> String {
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

pub(crate) fn normalize_send_kind(kind: &str) -> Result<String> {
    match kind {
        "message" | "event" | "receipt" => Ok(kind.to_string()),
        "system" => bail!("kind \"system\" is reserved for raft internals"),
        _ => bail!("unsupported kind {kind:?}; use message, event, or receipt"),
    }
}

pub(crate) fn validate_subject_id(value: &str) -> Result<String> {
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

pub(crate) fn rate_key(sender: &str, subject_id: Option<&str>) -> String {
    match subject_id {
        Some(subject_id) => format!("{sender}#{subject_id}"),
        None => sender.to_string(),
    }
}

pub(crate) fn schema_v1() -> u16 {
    SCHEMA_VERSION
}

pub(crate) fn default_agent_state() -> String {
    "idle".to_string()
}

pub(crate) fn default_message_kind() -> String {
    "message".to_string()
}

pub(crate) fn unique(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for value in values {
        if seen.insert(value.clone()) {
            output.push(value);
        }
    }
    output
}

pub(crate) fn resolve_path(path: &Path) -> Result<String> {
    Ok(path.canonicalize()?.display().to_string())
}

pub(crate) fn hostname() -> String {
    env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string())
}

pub(crate) fn iso_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn iso_after(seconds: u64) -> String {
    (Utc::now() + TimeDelta::seconds(seconds as i64)).to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn parse_time(value: &str) -> std::result::Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339(value).map(|value| value.with_timezone(&Utc))
}

pub(crate) fn new_message_id() -> String {
    let now = Utc::now();
    let stamp = format!(
        "{}{:03}",
        now.format("%Y%m%dT%H%M%S"),
        now.timestamp_subsec_millis()
    );
    format!("m-{stamp}-{}", unique_token_short())
}

pub(crate) fn unique_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:x}{:x}", process::id())
}

pub(crate) fn unique_token_short() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let count = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:08x}{:x}{:x}", nanos as u32, process::id(), count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn message_ids_are_unique_under_rapid_succession() {
        let mut ids = HashSet::new();
        for _ in 0..50_000 {
            assert!(
                ids.insert(new_message_id()),
                "duplicate message id generated within the same process"
            );
        }
    }
}
