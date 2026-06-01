use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::Duration;

/// Blocks until the filesystem under the watched paths changes, or a timeout
/// elapses — whichever comes first. When `notify` cannot be set up the waker
/// degrades to plain timeout sleeps, so callers keep polling as a fallback.
pub(crate) struct Waker {
    _watcher: Option<RecommendedWatcher>,
    rx: Option<Receiver<()>>,
}

impl Waker {
    pub(crate) fn new(paths: &[&Path]) -> Self {
        let (tx, rx) = mpsc::channel();
        let watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
            if res.is_ok() {
                let _ = tx.send(());
            }
        });
        let mut watcher = match watcher {
            Ok(watcher) => watcher,
            Err(_) => return Self::polling_only(),
        };
        let mut watched_any = false;
        for path in paths {
            if path.exists() && watcher.watch(path, RecursiveMode::Recursive).is_ok() {
                watched_any = true;
            }
        }
        if watched_any {
            Self {
                _watcher: Some(watcher),
                rx: Some(rx),
            }
        } else {
            Self::polling_only()
        }
    }

    fn polling_only() -> Self {
        Self {
            _watcher: None,
            rx: None,
        }
    }

    /// True when event-driven wakeups are active; false when degraded to polling.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_event_driven(&self) -> bool {
        self.rx.is_some()
    }

    /// Wait for a change event, returning early on the first event and coalescing
    /// any others that arrived. Falls back to a plain sleep without a watcher.
    pub(crate) fn wait(&self, timeout: Duration) {
        let Some(rx) = &self.rx else {
            thread::sleep(timeout);
            return;
        };
        match rx.recv_timeout(timeout) {
            Ok(()) => while rx.try_recv().is_ok() {},
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => thread::sleep(timeout),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    fn scratch_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("run")
            .join("test-buses")
            .join(format!("raft-waker-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn wakes_before_timeout_on_filesystem_change() {
        let dir = scratch_dir();
        let waker = Waker::new(&[dir.as_path()]);
        if !waker.is_event_driven() {
            return; // platform without a working watcher: polling fallback, nothing to assert
        }
        let writer = dir.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            fs::write(writer.join("event.txt"), b"hi").unwrap();
        });
        let start = Instant::now();
        waker.wait(Duration::from_secs(10));
        let elapsed = start.elapsed();
        handle.join().unwrap();
        assert!(
            elapsed < Duration::from_secs(5),
            "waker should return promptly after an fs event, took {elapsed:?}"
        );
    }

    #[test]
    fn missing_paths_degrade_to_polling() {
        let waker = Waker::new(&[std::path::Path::new("/nonexistent/raft/path/xyz")]);
        assert!(!waker.is_event_driven());
        let start = Instant::now();
        waker.wait(Duration::from_millis(150));
        assert!(start.elapsed() >= Duration::from_millis(140));
    }
}
