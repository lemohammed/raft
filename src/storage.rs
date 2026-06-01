use crate::error::{RaftError, Result};
use crate::types::{LockOwner, Message};
use crate::util::{hostname, iso_after, iso_now, parse_time, unique_token, validate_id};
use crate::{LOCK_TTL_SECONDS, SCHEMA_VERSION};
use chrono::Utc;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::ffi::{OsStr, OsString};
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
                    if reap_stale_lock(root, &path)? {
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

pub(crate) fn is_agent_record_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(OsStr::to_str) else {
        return false;
    };
    path.extension() == Some(OsStr::new("json"))
        && !name.ends_with(".key.json")
        && !name.ends_with(".passport.json")
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
        "staging",
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
    let staged = path.with_file_name(format!(
        ".{file_name}.{}.{}.raft-staged",
        process::id(),
        unique_token()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&staged)?;
    serde_json::to_writer_pretty(&mut file, payload)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&staged, path)?;
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

/// The owner token of a lock instance, or `None` for an orphaned directory with
/// no readable owner.
fn lock_owner_token(path: &Path) -> Result<Option<String>> {
    let owner: Option<LockOwner> = read_json(&path.join("owner.json"))?;
    Ok(owner.map(|owner| owner.token))
}

/// Remove a lock directory only if it is still the *same* stale instance we
/// judged. Reading the owner, deciding it is stale, and deleting the directory
/// cannot be fused into one atomic step with directory primitives, so we re-read
/// the owner immediately before the destructive call: if the lock was refreshed
/// (still the same token but the expiry now lies in the future) or replaced by a
/// fresh holder (a different token) since we judged it, we leave it alone.
///
/// This keeps a lock that is within its lease from being reaped out from under a
/// live holder — the failure mode of a plain "if stale, remove" — while still
/// reclaiming genuinely abandoned locks. Returns whether the directory was
/// removed.
pub(crate) fn reap_stale_lock(root: &Path, path: &Path) -> Result<bool> {
    let token_when_judged = lock_owner_token(path)?;
    if !lock_is_stale(path)? {
        return Ok(false);
    }
    // Final guard, as close to the destructive call as possible.
    if !lock_is_stale(path)? || lock_owner_token(path)? != token_when_judged {
        return Ok(false);
    }
    let removed = fs::remove_dir_all(path).is_ok();
    if removed {
        let _ = fsync_dir(&root.join("locks"));
    }
    Ok(removed)
}

pub(crate) fn set_dir_private(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

/// `atomic_write_json` writes a dot-prefixed sibling ending in ".raft-staged"
/// and renames it into place. A crash between create and rename orphans one.
/// They are only ever transient, so anything older than this is safe to reap.
pub(crate) const ORPHAN_TMP_STALE_SECONDS: u64 = 300;

/// True for the transient sibling files written by [`atomic_write_json`].
pub(crate) fn is_temp_artifact(name: &str) -> bool {
    name.starts_with('.') && (name.ends_with(".raft-staged") || name.ends_with(".tmp"))
}

fn temp_file_is_stale(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    modified
        .elapsed()
        .map(|elapsed| elapsed > Duration::from_secs(ORPHAN_TMP_STALE_SECONDS))
        .unwrap_or(false)
}

/// Recursively collect orphaned temp files under `root` older than
/// [`ORPHAN_TMP_STALE_SECONDS`]. Symlinks are not followed, so a malicious or
/// stray symlink cannot redirect the sweep outside the bus tree.
pub(crate) fn collect_orphan_temp_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut found = Vec::new();
    collect_orphan_temp_files_into(root, &mut found)?;
    Ok(found)
}

fn collect_orphan_temp_files_into(dir: &Path, found: &mut Vec<PathBuf>) -> Result<()> {
    for entry in sorted_read_dir(dir)? {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            collect_orphan_temp_files_into(&path, found)?;
        } else if file_type.is_file() {
            let Some(name) = path.file_name().and_then(OsStr::to_str) else {
                continue;
            };
            if is_temp_artifact(name) && temp_file_is_stale(&path) {
                found.push(path);
            }
        }
    }
    Ok(())
}

pub(crate) fn sorted_read_dir(path: &Path) -> Result<Vec<fs::DirEntry>> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries
            .map(|entry| entry.map(|entry| (entry.file_name(), entry)))
            .collect::<std::result::Result<Vec<(OsString, fs::DirEntry)>, _>>()?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };
    Ok(entries_sorted_by_cached_name(entries))
}

fn entries_sorted_by_cached_name(mut entries: Vec<(OsString, fs::DirEntry)>) -> Vec<fs::DirEntry> {
    entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
    entries.into_iter().map(|(_, entry)| entry).collect()
}

#[cfg(test)]
mod tests {
    use super::sorted_read_dir;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn scratch_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("run")
            .join("test-buses")
            .join(format!(
                "raft-storage-{name}-{}-{nanos}",
                std::process::id()
            ))
    }

    #[test]
    fn sorted_read_dir_orders_by_file_name() {
        let dir = scratch_dir("ordered");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("zeta.json"), b"{}").unwrap();
        fs::create_dir(dir.join("middle.lock")).unwrap();
        fs::write(dir.join("alpha.json"), b"{}").unwrap();

        let names = sorted_read_dir(&dir)
            .unwrap()
            .into_iter()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert_eq!(names, vec!["alpha.json", "middle.lock", "zeta.json"]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sorted_read_dir_missing_dir_is_empty() {
        let dir = scratch_dir("missing");
        let _ = fs::remove_dir_all(&dir);

        let entries = sorted_read_dir(&dir).unwrap();

        assert!(entries.is_empty());
    }
}
