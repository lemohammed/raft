use crate::error::{RaftError, Result};
use crate::types::{LockOwner, Message};
use crate::util::{hostname, iso_after, iso_now, parse_time, unique_token, validate_id};
use crate::{LOCK_TTL_SECONDS, SCHEMA_VERSION};
use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, Instant};

pub(crate) struct DirLock {
    root: PathBuf,
    path: PathBuf,
    token: String,
    acquired: bool,
}

impl DirLock {
    pub(crate) fn acquire(
        root: &Path,
        name: &str,
        ttl_seconds: u64,
        timeout_seconds: u64,
    ) -> Result<Self> {
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

    pub(crate) fn refresh(&self, ttl_seconds: u64) -> Result<()> {
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

pub(crate) fn conversation_path(root: &Path, conversation_id: &str) -> Result<PathBuf> {
    Ok(root
        .join("conversations")
        .join(validate_id(conversation_id, "conversation id")?))
}

pub(crate) fn target_room(conversation: Option<&str>, channel: Option<&str>) -> Result<String> {
    match (conversation, channel) {
        (Some(conversation), None) => validate_id(conversation, "conversation id"),
        (None, Some(channel)) => validate_id(channel, "channel id"),
        (None, None) => bail!("provide --conversation or --channel"),
        (Some(_), Some(_)) => bail!("provide only one of --conversation or --channel"),
    }
}

pub(crate) fn optional_target_room(
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

pub(crate) fn agent_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("agents").join(format!("{agent_id}.json"))
}

pub(crate) fn receipt_path_for(root: &Path, message: &Message, agent_id: &str) -> PathBuf {
    root.join("conversations")
        .join(&message.conversation_id)
        .join("receipts")
        .join(&message.id)
        .join(format!("{agent_id}.json"))
}

pub(crate) fn watch_state_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("watch").join(format!("{agent_id}.json"))
}

pub(crate) fn heartbeat_state_path(root: &Path, agent_id: &str) -> PathBuf {
    root.join("heartbeat").join(format!("{agent_id}.json"))
}

pub(crate) fn ensure_root(root: &Path) -> Result<()> {
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

pub(crate) fn atomic_write_json<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        set_dir_private(parent)?;
    }
    let file_name = path
        .file_name()
        .and_then(OsStr::to_str)
        .ok_or_else(|| RaftError::new(format!("invalid target file: {}", path.display())))?;
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

pub(crate) fn append_jsonl<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
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

pub(crate) fn read_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match File::open(path) {
        Ok(file) => Ok(Some(serde_json::from_reader(file)?)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub(crate) fn fsync_dir(path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    file.sync_all()
}

pub(crate) fn lock_is_stale(path: &Path) -> Result<bool> {
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

pub(crate) fn set_dir_private(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

pub(crate) fn sorted_read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let mut entries = match fs::read_dir(path) {
        Ok(entries) => entries.collect::<std::result::Result<Vec<_>, _>>()?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };
    entries.sort_by_key(|entry| entry.path());
    Ok(entries)
}
