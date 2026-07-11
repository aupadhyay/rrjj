use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufReader, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use rrjj_schema::{Event, FormatMetadata, SessionManifest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt as _, BufWriter};
use tokio::sync::{Mutex, Notify, broadcast};
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("serialize event: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("write event stream: {0}")]
    Io(#[from] std::io::Error),
    #[error("local spool is full ({used} + {attempted} > {limit} bytes)")]
    SpoolFull {
        used: u64,
        attempted: u64,
        limit: u64,
    },
    #[error("disk exhausted while writing {path}")]
    DiskExhausted { path: PathBuf },
    #[error("sink has failed permanently: {0}")]
    Failed(String),
    #[error("invalid flush request: {0}")]
    InvalidFlush(String),
}

#[derive(Clone, Debug)]
pub struct S3SinkConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub spool_path: PathBuf,
    pub max_spool_bytes: u64,
    pub session_id: String,
    pub format: FormatMetadata,
}

#[derive(Clone, Debug)]
pub struct FlushRequest {
    pub shadow_root: PathBuf,
    pub last_seq: u64,
    pub last_op: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SinkCursor {
    pub delivered_seq: Option<u64>,
    pub delivered_op: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DirectorySyncStats {
    pub files_linked: u64,
    pub files_copied: u64,
    pub files_replaced: u64,
    pub files_reused: u64,
    pub files_removed: u64,
    pub bytes_copied: u64,
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn emit(&self, event: &Event) -> Result<(), SinkError>;
    async fn flush(&self) -> Result<(), SinkError>;

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        let _ = request;
        self.flush().await
    }
}

impl BroadcastSink {
    pub fn new(durable: Arc<dyn Sink>, capacity: usize) -> (Self, broadcast::Sender<Event>) {
        let (events, _) = broadcast::channel(capacity.max(1));
        (
            Self {
                durable,
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl Sink for BroadcastSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        self.durable.emit(event).await?;
        let _ = self.events.send(event.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.durable.flush().await
    }

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        self.durable.flush_session(request).await
    }
}

impl S3SessionSink {
    pub async fn create(config: S3SinkConfig) -> Result<Self, SinkError> {
        let shared = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()))
            .load()
            .await;
        let mut builder = aws_sdk_s3::config::Builder::from(&shared);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint_url(endpoint).force_path_style(true);
        }
        Self::with_client(config, Client::from_conf(builder.build())).await
    }

    pub async fn with_client(config: S3SinkConfig, client: Client) -> Result<Self, SinkError> {
        if let Some(parent) = config.spool_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let existing = match tokio::fs::read(&config.spool_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
        let accepted = parse_spool(&existing, &config)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.spool_path)
            .await?;
        set_private_file_permissions(&config.spool_path).await?;
        let spool_bytes = file.metadata().await?.len();
        if spool_bytes > config.max_spool_bytes {
            return Err(SinkError::SpoolFull {
                used: spool_bytes,
                attempted: 0,
                limit: config.max_spool_bytes,
            });
        }
        let mut manifest = SessionManifest {
            session_id: config.session_id.clone(),
            format: config.format.clone(),
            last_seq: 0,
            last_op: String::new(),
            events_object: None,
            durable_seq: None,
            durable_op: None,
        };
        for event in &accepted {
            manifest.last_seq = event.seq;
            if let Some(op) = event_op(event) {
                manifest.last_op = op.to_owned();
            }
        }
        let cursor_path = upload_cursor_path(&config.spool_path);
        let uploaded_seq =
            read_upload_cursor(&cursor_path, accepted.last().map(|event| event.seq)).await?;
        let inner = Arc::new(S3Inner {
            client,
            config,
            spool: Mutex::new(BufWriter::new(file)),
            state: Mutex::new(S3State {
                manifest,
                spool_bytes,
                next_seq: accepted.len() as u64,
                accepted,
                uploaded_seq,
                store_hashes: BTreeMap::new(),
                failed: None,
            }),
            upload_wakeup: Notify::new(),
            upload_progress: Notify::new(),
            cursor_path,
        });
        spawn_uploader(&inner);
        inner.upload_wakeup.notify_one();
        Ok(Self { inner })
    }

    fn key(&self, suffix: &str) -> String {
        s3_key(&self.inner.config, suffix)
    }

    async fn put(&self, key: String, body: ByteStream) -> Result<(), SinkError> {
        put_object(&self.inner, key, body).await
    }

    async fn wait_uploaded(&self, through_seq: u64) {
        loop {
            let notified = self.inner.upload_progress.notified();
            if self
                .inner
                .state
                .lock()
                .await
                .uploaded_seq
                .is_some_and(|uploaded| uploaded >= through_seq)
            {
                return;
            }
            self.inner.upload_wakeup.notify_one();
            notified.await;
        }
    }
}

fn s3_key(config: &S3SinkConfig, suffix: &str) -> String {
    let prefix = config.prefix.trim_matches('/');
    if prefix.is_empty() {
        format!("{}/{}", config.session_id, suffix)
    } else {
        format!("{}/{}/{}", prefix, config.session_id, suffix)
    }
}

async fn put_object(inner: &S3Inner, key: String, body: ByteStream) -> Result<(), SinkError> {
    inner
        .client
        .put_object()
        .bucket(&inner.config.bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .map_err(|error| SinkError::Failed(format!("S3 put failed: {error}")))?;
    Ok(())
}

fn spawn_uploader(inner: &Arc<S3Inner>) {
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut backoff = Duration::from_millis(25);
        loop {
            let Some(inner) = weak.upgrade() else {
                return;
            };
            let next = {
                let state = inner.state.lock().await;
                let next_seq = state.uploaded_seq.map_or(0, |seq| seq + 1);
                state
                    .accepted
                    .get(next_seq as usize)
                    .map(|event| (next_seq, serde_json::to_vec(event)))
            };
            let Some((seq, Ok(bytes))) = next else {
                inner.upload_wakeup.notified().await;
                continue;
            };
            let key = s3_key(&inner.config, &format!("live/{seq:020}.json"));
            match put_object(&inner, key, ByteStream::from(bytes)).await {
                Ok(()) => {
                    let cursor = SinkCursor {
                        delivered_seq: Some(seq),
                        delivered_op: None,
                    };
                    if let Err(error) = write_cursor_atomic(&inner.cursor_path, &cursor).await {
                        eprintln!("rrjj S3 upload cursor persistence failed: {error}");
                    }
                    inner.state.lock().await.uploaded_seq = Some(seq);
                    inner.upload_progress.notify_waiters();
                    backoff = Duration::from_millis(25);
                }
                Err(error) => {
                    eprintln!("rrjj S3 live upload failed; retrying: {error}");
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
                }
            }
        }
    });
}

#[async_trait]
impl Sink for S3SessionSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        let mut state = self.inner.state.lock().await;
        if let Some(message) = &state.failed {
            return Err(SinkError::Failed(message.clone()));
        }
        if event.seq != state.next_seq {
            return Err(SinkError::Failed(format!(
                "S3 event sequence mismatch: expected {}, got {}",
                state.next_seq, event.seq
            )));
        }
        let object = serde_json::to_vec(event)?;
        let mut line = object.clone();
        line.push(b'\n');
        if state.spool_bytes + line.len() as u64 > self.inner.config.max_spool_bytes {
            return Err(SinkError::SpoolFull {
                used: state.spool_bytes,
                attempted: line.len() as u64,
                limit: self.inner.config.max_spool_bytes,
            });
        }
        let mut spool = self.inner.spool.lock().await;
        if let Err(error) = spool.write_all(&line).await {
            return Err(fail_s3_spool(
                &mut state,
                &self.inner.config.spool_path,
                error,
            ));
        }
        if let Err(error) = spool.flush().await {
            return Err(fail_s3_spool(
                &mut state,
                &self.inner.config.spool_path,
                error,
            ));
        }
        if let Err(error) = spool.get_ref().sync_data().await {
            return Err(fail_s3_spool(
                &mut state,
                &self.inner.config.spool_path,
                error,
            ));
        }
        state.spool_bytes += line.len() as u64;
        state.next_seq += 1;
        state.accepted.push(event.clone());
        state.manifest.last_seq = event.seq;
        if let Some(op) = event_op(event) {
            state.manifest.last_op = op.to_owned();
        }
        drop(state);
        self.inner.upload_wakeup.notify_one();
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        if let Some(message) = &self.inner.state.lock().await.failed {
            return Err(SinkError::Failed(message.clone()));
        }
        let mut spool = self.inner.spool.lock().await;
        spool.flush().await?;
        spool.get_ref().sync_all().await?;
        Ok(())
    }

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        self.flush().await?;
        let mut manifest = {
            let state = self.inner.state.lock().await;
            if request.last_seq != state.manifest.last_seq {
                return Err(SinkError::InvalidFlush(format!(
                    "coordinator seq {} does not match S3 spool seq {}",
                    request.last_seq, state.manifest.last_seq
                )));
            }
            state.manifest.clone()
        };
        self.wait_uploaded(request.last_seq).await;

        let known_hashes = self.inner.state.lock().await.store_hashes.clone();
        let mut files = WalkDir::new(&request.shadow_root)
            .follow_links(false)
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| SinkError::Failed(format!("walk shadow store: {error}")))?;
        files.retain(|entry| entry.file_type().is_file());
        files.sort_by_key(|entry| entry.path().to_owned());
        let mut uploaded_hashes = Vec::new();
        for entry in files {
            let relative = entry
                .path()
                .strip_prefix(&request.shadow_root)
                .map_err(|error| SinkError::Failed(error.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            let bytes = tokio::fs::read(entry.path()).await?;
            let hash: [u8; 32] = Sha256::digest(&bytes).into();
            if known_hashes.get(&relative) == Some(&hash) {
                continue;
            }
            self.put(
                self.key(&format!("store/{relative}")),
                ByteStream::from(bytes),
            )
            .await?;
            uploaded_hashes.push((relative, hash));
        }
        let events = {
            let state = self.inner.state.lock().await;
            state
                .accepted
                .iter()
                .take(request.last_seq as usize + 1)
                .map(|event| {
                    let mut line = serde_json::to_vec(event)?;
                    line.push(b'\n');
                    Ok(line)
                })
                .collect::<Result<Vec<_>, serde_json::Error>>()?
                .concat()
        };
        let events_object = format!("events/{:020}.ndjson", request.last_seq);
        self.put(self.key(&events_object), ByteStream::from(events))
            .await?;

        manifest.last_op = request.last_op.clone();
        manifest.events_object = Some(events_object);
        manifest.durable_seq = Some(request.last_seq);
        manifest.durable_op = Some(request.last_op.clone());
        self.put(
            self.key("manifest.json"),
            ByteStream::from(serde_json::to_vec_pretty(&manifest)?),
        )
        .await?;
        let mut state = self.inner.state.lock().await;
        state.manifest = manifest;
        state.store_hashes.extend(uploaded_hashes);
        Ok(())
    }
}

pub struct NdjsonSink {
    writer: Mutex<BufWriter<File>>,
}

pub struct DirectorySessionSink {
    spool: Mutex<BufWriter<File>>,
    spool_path: PathBuf,
    session_dir: PathBuf,
    max_spool_bytes: u64,
    state: StdMutex<DirectoryState>,
}

pub struct BroadcastSink {
    durable: Arc<dyn Sink>,
    events: broadcast::Sender<Event>,
}

pub struct S3SessionSink {
    inner: Arc<S3Inner>,
}

struct S3Inner {
    client: Client,
    config: S3SinkConfig,
    spool: Mutex<BufWriter<File>>,
    state: Mutex<S3State>,
    upload_wakeup: Notify,
    upload_progress: Notify,
    cursor_path: PathBuf,
}

#[derive(Clone, Debug)]
struct S3State {
    manifest: SessionManifest,
    spool_bytes: u64,
    next_seq: u64,
    accepted: Vec<Event>,
    uploaded_seq: Option<u64>,
    store_hashes: BTreeMap<String, [u8; 32]>,
    failed: Option<String>,
}

#[derive(Clone, Debug)]
struct DirectoryState {
    manifest: SessionManifest,
    spool_bytes: u64,
    failed: Option<String>,
    last_sync: Option<DirectorySyncStats>,
}

impl DirectorySessionSink {
    pub async fn create(
        spool_path: impl AsRef<Path>,
        session_dir: impl AsRef<Path>,
        session_id: String,
        format: FormatMetadata,
        max_spool_bytes: u64,
    ) -> Result<Self, SinkError> {
        let spool_path = spool_path.as_ref().to_owned();
        let session_dir = session_dir.as_ref().to_owned();
        if let Some(parent) = spool_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::create_dir_all(&session_dir).await?;
        set_private_directory_permissions(&session_dir).await?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&spool_path)
            .await?;
        set_private_file_permissions(&spool_path).await?;
        let spool_bytes = file.metadata().await?.len();
        if spool_bytes > max_spool_bytes {
            return Err(SinkError::SpoolFull {
                used: spool_bytes,
                attempted: 0,
                limit: max_spool_bytes,
            });
        }
        let manifest = SessionManifest {
            session_id,
            format,
            last_seq: 0,
            last_op: String::new(),
            events_object: None,
            durable_seq: None,
            durable_op: None,
        };
        Ok(Self {
            spool: Mutex::new(BufWriter::new(file)),
            spool_path,
            session_dir,
            max_spool_bytes,
            state: StdMutex::new(DirectoryState {
                manifest,
                spool_bytes,
                failed: None,
                last_sync: None,
            }),
        })
    }

    pub fn manifest(&self) -> SessionManifest {
        self.state
            .lock()
            .expect("directory sink state")
            .manifest
            .clone()
    }

    pub fn last_sync_stats(&self) -> Option<DirectorySyncStats> {
        self.state
            .lock()
            .expect("directory sink state")
            .last_sync
            .clone()
    }

    fn check_failed(&self) -> Result<(), SinkError> {
        match &self.state.lock().expect("directory sink state").failed {
            Some(message) => Err(SinkError::Failed(message.clone())),
            None => Ok(()),
        }
    }

    fn fail_io(&self, path: &Path, error: std::io::Error) -> SinkError {
        let sink_error = if error.raw_os_error() == Some(28) {
            SinkError::DiskExhausted {
                path: path.to_owned(),
            }
        } else {
            SinkError::Io(error)
        };
        self.state.lock().expect("directory sink state").failed = Some(sink_error.to_string());
        sink_error
    }
}

#[async_trait]
impl Sink for DirectorySessionSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        self.check_failed()?;
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        {
            let state = self.state.lock().expect("directory sink state");
            if state.spool_bytes + line.len() as u64 > self.max_spool_bytes {
                return Err(SinkError::SpoolFull {
                    used: state.spool_bytes,
                    attempted: line.len() as u64,
                    limit: self.max_spool_bytes,
                });
            }
        }
        let mut writer = self.spool.lock().await;
        if let Err(error) = writer.write_all(&line).await {
            return Err(self.fail_io(&self.spool_path, error));
        }
        if let Err(error) = writer.flush().await {
            return Err(self.fail_io(&self.spool_path, error));
        }
        if let Err(error) = writer.get_ref().sync_data().await {
            return Err(self.fail_io(&self.spool_path, error));
        }
        let manifest = {
            let mut state = self.state.lock().expect("directory sink state");
            state.spool_bytes += line.len() as u64;
            state.manifest.last_seq = event.seq;
            if let Some(op) = event_op(event) {
                state.manifest.last_op = op.to_owned();
            }
            state.manifest.clone()
        };
        let session_dir = self.session_dir.clone();
        let manifest_result =
            tokio::task::spawn_blocking(move || write_manifest_atomic(&session_dir, &manifest))
                .await;
        // The synced spool append is the acceptance boundary. Returning an
        // error here would make the coordinator retry an already-appended
        // event and duplicate its NDJSON line. Flush republishes the manifest.
        match manifest_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => eprintln!(
                "rrjj observed manifest update failed after local acceptance: {}: {error}",
                self.session_dir.join("manifest.json").display()
            ),
            Err(error) => {
                eprintln!(
                    "rrjj observed manifest update task failed after local acceptance: {error}"
                )
            }
        }
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.check_failed()?;
        let mut writer = self.spool.lock().await;
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        Ok(())
    }

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        self.flush().await?;
        let mut manifest = self.manifest();
        if request.last_seq != manifest.last_seq {
            return Err(SinkError::InvalidFlush(format!(
                "coordinator seq {} does not match spool seq {}",
                request.last_seq, manifest.last_seq
            )));
        }
        manifest.last_op = request.last_op.clone();
        let shadow_root = request.shadow_root.clone();
        let spool_path = self.spool_path.clone();
        let session_dir = self.session_dir.clone();
        let durable_manifest = manifest.clone();
        let stats = tokio::task::spawn_blocking(move || {
            sync_directory_session(&shadow_root, &spool_path, &session_dir, durable_manifest)
        })
        .await
        .map_err(|error| SinkError::Failed(error.to_string()))??;
        eprintln!(
            "rrjj local session sync: linked={}, copied={}, replaced={}, reused={}, removed={}, bytes_copied={}",
            stats.files_linked,
            stats.files_copied,
            stats.files_replaced,
            stats.files_reused,
            stats.files_removed,
            stats.bytes_copied
        );
        let mut state = self.state.lock().expect("directory sink state");
        state.manifest = manifest;
        state.manifest.durable_seq = Some(request.last_seq);
        state.manifest.durable_op = Some(request.last_op.clone());
        state.manifest.events_object = Some(format!("events/{:020}.ndjson", request.last_seq));
        state.last_sync = Some(stats);
        Ok(())
    }
}

fn fail_s3_spool(state: &mut S3State, path: &Path, error: std::io::Error) -> SinkError {
    let sink_error = if error.raw_os_error() == Some(28) {
        SinkError::DiskExhausted {
            path: path.to_owned(),
        }
    } else {
        SinkError::Io(error)
    };
    state.failed = Some(sink_error.to_string());
    sink_error
}

fn parse_spool(bytes: &[u8], config: &S3SinkConfig) -> Result<Vec<Event>, SinkError> {
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        return Err(SinkError::Failed(format!(
            "S3 spool has an incomplete final line: {}",
            config.spool_path.display()
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
                    "S3 spool sequence mismatch on line {}: expected {}, got {}",
                    index + 1,
                    expected,
                    event.seq
                )));
            }
            if event.session_id != config.session_id {
                return Err(SinkError::Failed(format!(
                    "S3 spool belongs to session {}, not {}",
                    event.session_id, config.session_id
                )));
            }
            if event.v != config.format.schema_version {
                return Err(SinkError::Failed(format!(
                    "S3 spool schema {} is incompatible with configured schema {}",
                    event.v, config.format.schema_version
                )));
            }
            Ok(event)
        })
        .collect()
}

fn upload_cursor_path(spool_path: &Path) -> PathBuf {
    let mut name = spool_path.as_os_str().to_owned();
    name.push(".upload-cursor.json");
    PathBuf::from(name)
}

async fn read_upload_cursor(path: &Path, last_seq: Option<u64>) -> Result<Option<u64>, SinkError> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let cursor: SinkCursor = serde_json::from_slice(&bytes)?;
    if cursor.delivered_seq > last_seq {
        return Err(SinkError::Failed(format!(
            "S3 upload cursor is beyond the local spool: {}",
            path.display()
        )));
    }
    Ok(cursor.delivered_seq)
}

async fn write_cursor_atomic(path: &Path, cursor: &SinkCursor) -> Result<(), std::io::Error> {
    let temporary = path.with_extension("tmp");
    tokio::fs::write(
        &temporary,
        serde_json::to_vec_pretty(cursor).map_err(std::io::Error::other)?,
    )
    .await?;
    let file = File::open(&temporary).await?;
    file.sync_all().await?;
    tokio::fs::rename(temporary, path).await
}

fn event_op(event: &Event) -> Option<&str> {
    match &event.body {
        rrjj_schema::EventBody::SessionStart(value) => Some(&value.baseline_op),
        rrjj_schema::EventBody::Snapshot(value) => Some(&value.op),
        rrjj_schema::EventBody::SessionEnd(value) => Some(&value.final_op),
        _ => None,
    }
}

fn sync_directory_session(
    shadow_root: &Path,
    spool_path: &Path,
    session_dir: &Path,
    mut manifest: SessionManifest,
) -> Result<DirectorySyncStats, SinkError> {
    let source_repo = shadow_root.join("repo");
    let store = session_dir.join("store");
    let destination_repo = store.join("repo");
    let mut stats = DirectorySyncStats::default();
    let mut dirty_directories = BTreeSet::new();
    if !store.exists() {
        fs::create_dir_all(&store)?;
        dirty_directories.insert(store.clone());
        dirty_directories.insert(session_dir.to_owned());
    }
    sync_repository_tree(
        &source_repo,
        &destination_repo,
        Path::new(""),
        &mut stats,
        &mut dirty_directories,
    )?;
    sync_directories(&dirty_directories)?;

    let events_dir = session_dir.join("events");
    fs::create_dir_all(&events_dir)?;
    let events_object = format!("events/{:020}.ndjson", manifest.last_seq);
    let events = session_dir.join(&events_object);
    if !events.exists() {
        copy_file_atomic(spool_path, &events)?;
        sync_directory(&events_dir)?;
        sync_directory(session_dir)?;
    }

    manifest.durable_seq = Some(manifest.last_seq);
    manifest.durable_op = Some(manifest.last_op.clone());
    manifest.events_object = Some(events_object);
    let cursor = SinkCursor {
        delivered_seq: manifest.durable_seq,
        delivered_op: manifest.durable_op.clone(),
    };
    write_json_atomic(&session_dir.join("cursor.json"), &cursor)?;
    write_manifest_atomic(session_dir, &manifest)?;
    Ok(stats)
}

fn write_manifest_atomic(
    session_dir: &Path,
    manifest: &SessionManifest,
) -> Result<(), std::io::Error> {
    fs::create_dir_all(session_dir)?;
    let manifest_tmp = session_dir.join("manifest.json.tmp");
    let manifest_path = session_dir.join("manifest.json");
    let bytes = serde_json::to_vec_pretty(manifest).map_err(std::io::Error::other)?;
    fs::write(&manifest_tmp, bytes)?;
    fs::File::open(&manifest_tmp)?.sync_all()?;
    fs::rename(&manifest_tmp, &manifest_path)?;
    fs::File::open(session_dir)?.sync_all()
}

fn write_json_atomic<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), std::io::Error> {
    let temporary = temporary_path(path);
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?,
    )?;
    fs::File::open(&temporary)?.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(path.parent().expect("session file has a parent"))
}

fn sync_repository_tree(
    source: &Path,
    destination: &Path,
    relative: &Path,
    stats: &mut DirectorySyncStats,
    dirty_directories: &mut BTreeSet<PathBuf>,
) -> Result<(), std::io::Error> {
    if !destination.exists() {
        fs::create_dir_all(destination)?;
        mark_directory_changed(destination, dirty_directories);
    }
    let mut source_names = BTreeSet::new();
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        source_names.insert(entry.file_name());
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let relative_path = relative.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_repository_tree(
                &source_path,
                &destination_path,
                &relative_path,
                stats,
                dirty_directories,
            )?;
        } else if file_type.is_file() {
            sync_repository_file(
                &source_path,
                &destination_path,
                &relative_path,
                stats,
                dirty_directories,
            )?;
        } else if file_type.is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "refusing symlink in repository state: {}",
                    source_path.display()
                ),
            ));
        }
    }
    for entry in fs::read_dir(destination)? {
        let entry = entry?;
        if source_names.contains(&entry.file_name()) {
            continue;
        }
        let relative_path = relative.join(entry.file_name());
        if entry.file_type()?.is_file() && !is_immutable_repository_file(&relative_path) {
            fs::remove_file(entry.path())?;
            stats.files_removed += 1;
            dirty_directories.insert(destination.to_owned());
        }
    }
    Ok(())
}

fn sync_repository_file(
    source: &Path,
    destination: &Path,
    relative: &Path,
    stats: &mut DirectorySyncStats,
    dirty_directories: &mut BTreeSet<PathBuf>,
) -> Result<(), std::io::Error> {
    if is_immutable_repository_file(relative) {
        if destination.exists() {
            stats.files_reused += 1;
            return Ok(());
        }
        let temporary = temporary_path(destination);
        remove_temporary(&temporary)?;
        match fs::hard_link(source, &temporary) {
            Ok(()) => stats.files_linked += 1,
            Err(_) => {
                stats.bytes_copied += fs::copy(source, &temporary)?;
                fs::File::open(&temporary)?.sync_all()?;
                stats.files_copied += 1;
            }
        }
        fs::rename(temporary, destination)?;
        dirty_directories.insert(
            destination
                .parent()
                .expect("repository file has a parent")
                .into(),
        );
        return Ok(());
    }

    if destination.exists() && files_equal(source, destination)? {
        stats.files_reused += 1;
        return Ok(());
    }
    let replacing = destination.exists();
    stats.bytes_copied += copy_file_atomic(source, destination)?;
    if replacing {
        stats.files_replaced += 1;
    } else {
        stats.files_copied += 1;
    }
    dirty_directories.insert(
        destination
            .parent()
            .expect("repository file has a parent")
            .into(),
    );
    Ok(())
}

fn is_immutable_repository_file(relative: &Path) -> bool {
    let components = relative
        .iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>();
    match components.as_slice() {
        [a, kind, name]
            if a == "op_store"
                && matches!(kind.as_ref(), "operations" | "views")
                && is_hex(name) =>
        {
            true
        }
        [a, kind, name]
            if a == "index"
                && matches!(kind.as_ref(), "segments" | "changed_paths")
                && is_hex(name) =>
        {
            true
        }
        [a, b, c, fanout, object]
            if a == "store"
                && b == "git"
                && c == "objects"
                && fanout.len() == 2
                && matches!(object.len(), 38 | 62)
                && is_hex(fanout)
                && is_hex(object) =>
        {
            true
        }
        [a, b, c, pack, name]
            if a == "store"
                && b == "git"
                && c == "objects"
                && pack == "pack"
                && is_immutable_git_pack_file(name) =>
        {
            true
        }
        _ => false,
    }
}

fn is_hex(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_immutable_git_pack_file(name: &str) -> bool {
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return false;
    };
    let Some(hash) = stem.strip_prefix("pack-") else {
        return false;
    };
    matches!(hash.len(), 40 | 64)
        && is_hex(hash)
        && matches!(
            extension,
            "pack" | "idx" | "rev" | "bitmap" | "promisor" | "mtimes"
        )
}

fn files_equal(left: &Path, right: &Path) -> Result<bool, std::io::Error> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = BufReader::new(fs::File::open(left)?);
    let mut right = BufReader::new(fs::File::open(right)?);
    let mut left_buffer = [0_u8; 8192];
    let mut right_buffer = [0_u8; 8192];
    loop {
        let left_read = left.read(&mut left_buffer)?;
        let right_read = right.read(&mut right_buffer)?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

fn copy_file_atomic(source: &Path, destination: &Path) -> Result<u64, std::io::Error> {
    let temporary = temporary_path(destination);
    remove_temporary(&temporary)?;
    let bytes = fs::copy(source, &temporary)?;
    fs::File::open(&temporary)?.sync_all()?;
    fs::rename(temporary, destination)?;
    Ok(bytes)
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".rrjj-sync-tmp");
    temporary.into()
}

fn remove_temporary(path: &Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn mark_directory_changed(path: &Path, dirty_directories: &mut BTreeSet<PathBuf>) {
    dirty_directories.insert(path.to_owned());
    if let Some(parent) = path.parent() {
        dirty_directories.insert(parent.to_owned());
    }
}

fn sync_directories(directories: &BTreeSet<PathBuf>) -> Result<(), std::io::Error> {
    let mut directories = directories.iter().collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
async fn set_private_file_permissions(path: &Path) -> Result<(), std::io::Error> {
    tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o600)).await
}

#[cfg(not(unix))]
async fn set_private_file_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(unix)]
async fn set_private_directory_permissions(path: &Path) -> Result<(), std::io::Error> {
    tokio::fs::set_permissions(path, fs::Permissions::from_mode(0o700)).await
}

#[cfg(not(unix))]
async fn set_private_directory_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

impl NdjsonSink {
    pub async fn create(path: impl AsRef<Path>) -> Result<Self, SinkError> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        set_private_file_permissions(path).await?;
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
        })
    }
}

#[async_trait]
impl Sink for NdjsonSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        let mut line = serde_json::to_vec(event)?;
        line.push(b'\n');
        let mut writer = self.writer.lock().await;
        writer.write_all(&line).await?;
        writer.flush().await?;
        writer.get_ref().sync_data().await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.writer.lock().await.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    use rrjj_schema::{
        EventBody, FormatMetadata, Overflow, OverflowRecovery, SCHEMA_VERSION,
        SESSION_FORMAT_VERSION,
    };

    use super::*;

    #[tokio::test]
    async fn writes_one_json_object_per_line() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("events.ndjson");
        let sink = NdjsonSink::create(&path).await.unwrap();
        sink.emit(&Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(text.lines().count(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap()["seq"],
            0
        );
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn syncs_spool_repository_and_advances_manifest_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let shadow = temp.path().join("shadow");
        fs::create_dir_all(shadow.join("repo/store")).unwrap();
        fs::create_dir_all(shadow.join("working-copy-000")).unwrap();
        fs::write(shadow.join("repo/store/object"), "jj state").unwrap();
        fs::write(shadow.join("working-copy-000/tree_state"), "not published").unwrap();
        let sink = DirectorySessionSink::create(
            temp.path().join("spool.ndjson"),
            temp.path().join("session"),
            "s".into(),
            format(),
            10_000,
        )
        .await
        .unwrap();
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(temp.path().join("spool.ndjson"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(temp.path().join("session"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        sink.emit(&Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        sink.flush_session(&FlushRequest {
            shadow_root: shadow,
            last_seq: 0,
            last_op: "op:abc".into(),
        })
        .await
        .unwrap();

        let manifest: SessionManifest =
            serde_json::from_slice(&fs::read(temp.path().join("session/manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest.durable_seq, Some(0));
        assert_eq!(manifest.durable_op.as_deref(), Some("op:abc"));
        assert_eq!(
            fs::read_to_string(temp.path().join("session/store/repo/store/object")).unwrap(),
            "jj state"
        );
        assert_eq!(
            fs::read_to_string(
                temp.path()
                    .join("session/events/00000000000000000000.ndjson")
            )
            .unwrap()
            .lines()
            .count(),
            1
        );
        assert!(!temp.path().join("session/store/working-copy-000").exists());

        sink.emit(&Event::new(
            "s",
            1,
            EventBody::Overflow(Overflow {
                source: "later".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        let pending: SessionManifest =
            serde_json::from_slice(&fs::read(temp.path().join("session/manifest.json")).unwrap())
                .unwrap();
        assert_eq!(pending.last_seq, 1);
        assert_eq!(pending.durable_seq, Some(0));
    }

    #[tokio::test]
    async fn incrementally_reuses_objects_and_replaces_mutable_files() {
        let temp = tempfile::tempdir().unwrap();
        let shadow = temp.path().join("shadow");
        let objects = shadow.join("repo/store/git/objects/aa");
        let heads = shadow.join("repo/op_heads/heads");
        fs::create_dir_all(&objects).unwrap();
        fs::create_dir_all(&heads).unwrap();
        let first_object = objects.join("11111111111111111111111111111111111111");
        let second_object = objects.join("22222222222222222222222222222222222222");
        let mutable = heads.join("current");
        fs::write(&first_object, "immutable-one").unwrap();
        fs::write(&mutable, "old").unwrap();
        let sink = DirectorySessionSink::create(
            temp.path().join("spool.ndjson"),
            temp.path().join("session"),
            "s".into(),
            format(),
            10_000,
        )
        .await
        .unwrap();
        sink.emit(&Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        let request = FlushRequest {
            shadow_root: shadow.clone(),
            last_seq: 0,
            last_op: "op:first".into(),
        };
        sink.flush_session(&request).await.unwrap();
        let first = sink.last_sync_stats().unwrap();
        assert_eq!(first.files_linked, 1);
        assert!(first.files_copied >= 1);

        sink.flush_session(&request).await.unwrap();
        let unchanged = sink.last_sync_stats().unwrap();
        assert_eq!(unchanged.files_linked, 0);
        assert_eq!(unchanged.files_copied, 0);
        assert_eq!(unchanged.files_replaced, 0);
        assert_eq!(unchanged.bytes_copied, 0);

        let published_mutable = temp
            .path()
            .join("session/store/repo/op_heads/heads/current");
        let published_before = fs::read(&published_mutable).unwrap();
        fs::write(&mutable, "new").unwrap();
        assert_eq!(fs::read(&published_mutable).unwrap(), published_before);
        fs::write(&second_object, "immutable-two").unwrap();
        sink.flush_session(&request).await.unwrap();
        let changed = sink.last_sync_stats().unwrap();
        assert_eq!(changed.files_linked, 1);
        assert_eq!(changed.files_replaced, 1);
        assert_eq!(fs::read_to_string(published_mutable).unwrap(), "new");
        assert_eq!(
            fs::read_to_string(temp.path().join(
                "session/store/repo/store/git/objects/aa/22222222222222222222222222222222222222"
            ))
            .unwrap(),
            "immutable-two"
        );
    }

    #[tokio::test]
    async fn keeps_versioned_events_readable_after_later_flush() {
        let temp = tempfile::tempdir().unwrap();
        let shadow = temp.path().join("shadow");
        fs::create_dir_all(shadow.join("repo")).unwrap();
        let sink = DirectorySessionSink::create(
            temp.path().join("spool.ndjson"),
            temp.path().join("session"),
            "s".into(),
            format(),
            10_000,
        )
        .await
        .unwrap();
        sink.emit(&Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "first".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        sink.flush_session(&FlushRequest {
            shadow_root: shadow.clone(),
            last_seq: 0,
            last_op: "op:first".into(),
        })
        .await
        .unwrap();
        let first_events = temp
            .path()
            .join("session/events/00000000000000000000.ndjson");
        let first_bytes = fs::read(&first_events).unwrap();

        sink.emit(&Event::new(
            "s",
            1,
            EventBody::Overflow(Overflow {
                source: "second".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        sink.flush_session(&FlushRequest {
            shadow_root: shadow,
            last_seq: 1,
            last_op: "op:second".into(),
        })
        .await
        .unwrap();
        assert_eq!(fs::read(first_events).unwrap(), first_bytes);
        assert_eq!(
            fs::read_to_string(
                temp.path()
                    .join("session/events/00000000000000000001.ndjson")
            )
            .unwrap()
            .lines()
            .count(),
            2
        );
    }

    #[tokio::test]
    async fn reports_spool_exhaustion_without_partial_append() {
        let temp = tempfile::tempdir().unwrap();
        let sink = DirectorySessionSink::create(
            temp.path().join("spool.ndjson"),
            temp.path().join("session"),
            "s".into(),
            format(),
            1,
        )
        .await
        .unwrap();
        let error = sink
            .emit(&Event::new(
                "s",
                0,
                EventBody::Overflow(Overflow {
                    source: "test".into(),
                    raw_events: 1,
                    recovery: OverflowRecovery::FullScanSnapshot,
                }),
            ))
            .await
            .unwrap_err();
        assert!(matches!(error, SinkError::SpoolFull { .. }));
        assert_eq!(
            fs::metadata(temp.path().join("spool.ndjson"))
                .unwrap()
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn broadcasts_only_after_durable_sink_accepts_event() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let durable: Arc<dyn Sink> = Arc::new(NdjsonSink::create(temp.path()).await.unwrap());
        let (sink, sender) = BroadcastSink::new(durable, 4);
        let mut receiver = sender.subscribe();
        let event = Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        );
        sink.emit(&event).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), event);
        assert_eq!(fs::read_to_string(temp.path()).unwrap().lines().count(), 1);
    }

    #[tokio::test]
    async fn s3_emit_is_nonblocking_and_uploads_in_order_with_retry_manifest_last() {
        use aws_config::retry::RetryConfig;
        use aws_sdk_s3::config::{Credentials, Region};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = requests.clone();
        let server = tokio::spawn(async move {
            for request_index in 0..6 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 64 * 1024];
                let read = stream.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..read]);
                captured
                    .lock()
                    .await
                    .push(request.lines().next().unwrap().to_owned());
                if request_index == 0 {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    stream
                        .write_all(
                            b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n",
                        )
                        .await
                        .unwrap();
                    continue;
                }
                stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await
                    .unwrap();
            }
        });
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "test"))
                .retry_config(RetryConfig::disabled())
                .endpoint_url(endpoint)
                .force_path_style(true)
                .build(),
        );
        let temp = tempfile::tempdir().unwrap();
        let shadow = temp.path().join("shadow");
        fs::create_dir_all(shadow.join("repo")).unwrap();
        fs::write(shadow.join("repo/object"), "state").unwrap();
        let sink = S3SessionSink::with_client(
            S3SinkConfig {
                bucket: "bucket".into(),
                prefix: "prefix".into(),
                region: "us-east-1".into(),
                endpoint: None,
                spool_path: temp.path().join("spool.ndjson"),
                max_spool_bytes: 10_000,
                session_id: "s".into(),
                format: format(),
            },
            client,
        )
        .await
        .unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            sink.emit(&Event::new(
                "s",
                0,
                EventBody::Overflow(Overflow {
                    source: "test".into(),
                    raw_events: 1,
                    recovery: OverflowRecovery::FullScanSnapshot,
                }),
            )),
        )
        .await
        .expect("local acceptance must not wait for S3")
        .unwrap();
        sink.emit(&Event::new(
            "s",
            1,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        sink.flush_session(&FlushRequest {
            shadow_root: shadow,
            last_seq: 1,
            last_op: "op:test".into(),
        })
        .await
        .unwrap();
        server.await.unwrap();
        let requests = requests.lock().await;
        assert!(requests[0].contains("/live/00000000000000000000.json"));
        assert!(requests[1].contains("/live/00000000000000000000.json"));
        assert!(requests[2].contains("/live/00000000000000000001.json"));
        assert!(requests[3].contains("/store/repo/object"));
        assert!(requests[4].contains("/events/00000000000000000001.ndjson"));
        assert!(requests[5].contains("manifest.json"));
        assert_eq!(
            fs::read_to_string(temp.path().join("spool.ndjson"))
                .unwrap()
                .lines()
                .count(),
            2
        );
        assert_eq!(
            sink.inner
                .state
                .lock()
                .await
                .manifest
                .events_object
                .as_deref(),
            Some("events/00000000000000000001.ndjson")
        );
    }

    #[tokio::test]
    async fn s3_restart_restores_sequence_and_rejects_incompatible_spools() {
        use aws_config::retry::RetryConfig;
        use aws_sdk_s3::config::{Credentials, Region};

        let temp = tempfile::tempdir().unwrap();
        let spool = temp.path().join("spool.ndjson");
        let event = Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "before-restart".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        );
        fs::write(
            &spool,
            format!("{}\n", serde_json::to_string(&event).unwrap()),
        )
        .unwrap();
        let client = Client::from_conf(
            aws_sdk_s3::config::Builder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "test"))
                .retry_config(RetryConfig::disabled())
                .endpoint_url("http://127.0.0.1:9")
                .force_path_style(true)
                .build(),
        );
        let config = S3SinkConfig {
            bucket: "bucket".into(),
            prefix: "prefix".into(),
            region: "us-east-1".into(),
            endpoint: None,
            spool_path: spool.clone(),
            max_spool_bytes: 10_000,
            session_id: "s".into(),
            format: format(),
        };
        let sink = S3SessionSink::with_client(config.clone(), client.clone())
            .await
            .unwrap();
        sink.emit(&Event::new(
            "s",
            1,
            EventBody::Overflow(Overflow {
                source: "after-restart".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        ))
        .await
        .unwrap();
        assert_eq!(fs::read_to_string(&spool).unwrap().lines().count(), 2);

        let incompatible = S3SinkConfig {
            session_id: "other".into(),
            ..config
        };
        assert!(matches!(
            S3SessionSink::with_client(incompatible, client).await,
            Err(SinkError::Failed(message)) if message.contains("belongs to session")
        ));
    }

    fn format() -> FormatMetadata {
        FormatMetadata {
            session_format: SESSION_FORMAT_VERSION,
            schema_version: SCHEMA_VERSION,
            rrjj_version: "test".into(),
            jj_lib_version: "0.43.0".into(),
            jj_store_version: "test".into(),
        }
    }
}
