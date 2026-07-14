use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use rrjj_schema::Event;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt as _, BufWriter};
use tokio::sync::Mutex;

use crate::error::SinkError;
use crate::traits::Sink;
use crate::util::{parse_event_spool, set_private_file_permissions};

/// Synced NDJSON event journal used as the local acceptance boundary.
pub struct NdjsonSink {
    path: PathBuf,
    state: Mutex<NdjsonState>,
    max_spool_bytes: u64,
    session_id: Option<String>,
    schema_version: Option<u8>,
    accepted: StdMutex<Vec<Event>>,
}

struct NdjsonState {
    writer: BufWriter<File>,
    bytes: u64,
}

impl NdjsonSink {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self, SinkError> {
        Self::create_bounded(path, u64::MAX).await
    }

    pub async fn create_bounded(
        path: impl AsRef<Path>,
        max_spool_bytes: u64,
    ) -> Result<Self, SinkError> {
        Self::create_for_session(path, max_spool_bytes, None, None).await
    }

    pub async fn create_for_session(
        path: impl AsRef<Path>,
        max_spool_bytes: u64,
        session_id: Option<String>,
        schema_version: Option<u8>,
    ) -> Result<Self, SinkError> {
        let path = path.as_ref().to_owned();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let existing = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let accepted = match (&session_id, schema_version) {
            (Some(session_id), Some(schema_version)) => {
                parse_event_spool(&existing, &path, session_id, schema_version, "journal")?
            }
            _ => Vec::new(),
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        set_private_file_permissions(&path).await?;
        let bytes = file.metadata().await?.len();
        if bytes > max_spool_bytes {
            return Err(SinkError::SpoolFull {
                used: bytes,
                attempted: 0,
                limit: max_spool_bytes,
            });
        }
        Ok(Self {
            path,
            state: Mutex::new(NdjsonState {
                writer: BufWriter::new(file),
                bytes,
            }),
            max_spool_bytes,
            session_id,
            schema_version,
            accepted: StdMutex::new(accepted),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn accepted_events(&self) -> Vec<Event> {
        self.accepted.lock().expect("journal accepted").clone()
    }

    pub fn next_seq(&self) -> u64 {
        self.accepted.lock().expect("journal accepted").len() as u64
    }

    pub async fn events_through(&self, last_seq: u64) -> Result<Vec<Event>, SinkError> {
        let events = self.accepted_events();
        if events.last().map(|event| event.seq) != Some(last_seq) {
            return Err(SinkError::InvalidFlush(format!(
                "journal does not end at requested sequence {last_seq}"
            )));
        }
        Ok(events)
    }
}

#[async_trait]
impl Sink for NdjsonSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        if let Some(session_id) = &self.session_id
            && event.session_id != *session_id
        {
            return Err(SinkError::Failed(format!(
                "journal belongs to session {session_id}, got {}",
                event.session_id
            )));
        }
        if let Some(schema_version) = self.schema_version
            && event.v != schema_version
        {
            return Err(SinkError::Failed(format!(
                "journal schema {schema_version} is incompatible with event schema {}",
                event.v
            )));
        }
        let expected = self.next_seq();
        if event.seq != expected {
            return Err(SinkError::Failed(format!(
                "journal sequence mismatch: expected {expected}, got {}",
                event.seq
            )));
        }
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        let mut state = self.state.lock().await;
        if state.bytes + line.len() as u64 > self.max_spool_bytes {
            return Err(SinkError::SpoolFull {
                used: state.bytes,
                attempted: line.len() as u64,
                limit: self.max_spool_bytes,
            });
        }
        state.writer.write_all(&line).await?;
        state.writer.flush().await?;
        state.writer.get_ref().sync_data().await?;
        state.bytes += line.len() as u64;
        drop(state);
        self.accepted
            .lock()
            .expect("journal accepted")
            .push(event.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        let mut state = self.state.lock().await;
        state.writer.flush().await?;
        state.writer.get_ref().sync_all().await?;
        Ok(())
    }
}
