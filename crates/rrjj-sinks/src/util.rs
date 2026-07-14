use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

use rrjj_schema::{Event, EventBody};

use crate::error::SinkError;
use crate::traits::SinkCursor;

pub(crate) fn event_op(event: &Event) -> Option<&str> {
    match &event.body {
        EventBody::SessionStart(value) => Some(&value.baseline_op),
        EventBody::Snapshot(value) => Some(&value.op),
        EventBody::SessionEnd(value) => Some(&value.final_op),
        EventBody::Flush(value) => Some(&value.op),
        _ => None,
    }
}

pub fn normalize_commit_oid(commit: &str) -> Result<String, SinkError> {
    let hex = commit.strip_prefix("c:").unwrap_or(commit);
    if hex.len() != 40 && hex.len() != 64 {
        return Err(SinkError::InvalidFlush(format!(
            "checkpoint commit OID must be 40 or 64 hex digits: {commit:?}"
        )));
    }
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(SinkError::InvalidFlush(format!(
            "checkpoint commit OID is not hex: {commit:?}"
        )));
    }
    Ok(hex.to_ascii_lowercase())
}

pub fn latest_snapshot_commit(events: &[Event]) -> Option<String> {
    events.iter().rev().find_map(|event| match &event.body {
        EventBody::Snapshot(snapshot) => Some(snapshot.commit.clone()),
        _ => None,
    })
}

pub fn parse_event_spool(
    bytes: &[u8],
    path: &Path,
    session_id: &str,
    schema_version: u8,
    sink_name: &str,
) -> Result<Vec<Event>, SinkError> {
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(SinkError::Failed(format!(
            "{sink_name} spool has an incomplete final line: {}",
            path.display()
        )));
    }
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .enumerate()
        .map(|(index, line)| {
            let event: Event = serde_json::from_slice(line)?;
            let expected = index as u64;
            if event.seq != expected {
                return Err(SinkError::Failed(format!(
                    "{sink_name} spool sequence mismatch on line {}: expected {}, got {}",
                    index + 1,
                    expected,
                    event.seq
                )));
            }
            if event.session_id != session_id {
                return Err(SinkError::Failed(format!(
                    "{sink_name} spool belongs to session {}, not {}",
                    event.session_id, session_id
                )));
            }
            if event.v != schema_version {
                return Err(SinkError::Failed(format!(
                    "{sink_name} spool schema {} is incompatible with configured schema {}",
                    event.v, schema_version
                )));
            }
            Ok(event)
        })
        .collect()
}

pub(crate) fn write_json_atomic<T: serde::Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), std::io::Error> {
    let temporary = temporary_path(path);
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?,
    )?;
    fs::File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(path.parent().expect("session file has a parent"))
}

pub(crate) fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".rrjj-sync-tmp");
    temporary.into()
}

pub(crate) fn remove_temporary(path: &Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    fs::File::open(path)?.sync_all()
}

pub(crate) fn write_cursor_atomic(path: &Path, cursor: &SinkCursor) -> Result<(), std::io::Error> {
    write_json_atomic(path, cursor)
}

pub(crate) fn redact_secrets(message: &str, secrets: &[&str]) -> String {
    let mut redacted = message.to_owned();
    for secret in secrets {
        if !secret.is_empty() {
            redacted = redacted.replace(secret, "***");
        }
    }
    redacted
}

#[cfg(unix)]
pub(crate) async fn set_private_file_permissions(path: &Path) -> Result<(), std::io::Error> {
    tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o600)).await
}

#[cfg(not(unix))]
pub(crate) async fn set_private_file_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
pub(crate) async fn set_private_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
    tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o700)).await
}

#[cfg(not(unix))]
pub(crate) async fn set_private_directory_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}
