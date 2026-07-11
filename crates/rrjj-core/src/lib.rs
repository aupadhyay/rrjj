use std::collections::{BTreeMap, BTreeSet};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use chrono::{SecondsFormat, Utc};
use futures::StreamExt as _;
use globset::{Glob, GlobSet, GlobSetBuilder};
use jj_lib::backend::TreeValue;
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::git_backend::GitBackend;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::local_working_copy::LocalWorkingCopy;
use jj_lib::matchers::{EverythingMatcher, NothingMatcher};
use jj_lib::merge::MergedTreeValue;
use jj_lib::merged_tree::MergedTree;
use jj_lib::object_id::ObjectId as _;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{BackendInitializer, ReadonlyRepo, Repo as _};
use jj_lib::repo_path::RepoPath;
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::working_copy::{SnapshotOptions, WorkingCopy};
use notify::event::{ModifyKind, RenameMode};
use notify::{EventKind, RecursiveMode, Watcher as _};
use rrjj_schema::{
    Change, ChangeKind, ChangeStats, Event, EventBody, Flush, Mark, Overflow, OverflowRecovery,
    SessionEnd, SessionStart, Snapshot, TouchOperation, TouchedPath, TouchedPaths,
};
use rrjj_sinks::{FlushRequest, Sink};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::sync::{mpsc, oneshot, watch};

const JJ_LIB_VERSION: &str = "0.43.0";
const JJ_STORE_VERSION: &str = "jj-lib-0.43.0/git";
const MAX_AUDIT_PATHS: usize = 100_000;
const MAX_OVERFLOW_REPORTS: usize = 64;

#[derive(Clone, Debug)]
pub struct Config {
    pub session_id: String,
    pub watched_root: PathBuf,
    pub shadow_root: PathBuf,
    pub ignore: Vec<String>,
    pub excluded_paths: Vec<PathBuf>,
    pub max_changes_per_event: usize,
    pub quiescence: Duration,
    pub max_delay: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Status {
    pub session_id: String,
    pub seq: u64,
    pub snapshots: u64,
    pub last_op: String,
    pub pending_snapshot: bool,
    pub paused: bool,
}

pub enum Command {
    Status {
        reply: oneshot::Sender<Status>,
    },
    Snap {
        reply: oneshot::Sender<Result<Snapshot>>,
    },
    Mark {
        label: String,
        meta: Map<String, Value>,
        reply: oneshot::Sender<Result<()>>,
    },
    Flush {
        reply: oneshot::Sender<Result<Status>>,
    },
    Pause {
        reply: oneshot::Sender<Status>,
    },
    Resume {
        reply: oneshot::Sender<Result<Status>>,
    },
    Shutdown {
        reason: String,
        reply: oneshot::Sender<Result<()>>,
    },
}

#[derive(Clone)]
struct OverflowNotice {
    source: String,
    raw_events: u64,
}

#[derive(Default)]
struct AuditState {
    paths: BTreeMap<String, BTreeSet<TouchOperation>>,
    raw_events: u64,
    started_at: Option<String>,
    first: Option<Instant>,
    last: Option<Instant>,
    overflows: Vec<OverflowNotice>,
    overflow_reports_limited: bool,
    path_limit_reported: bool,
}

#[derive(Default)]
struct AuditBatch {
    paths: BTreeMap<String, BTreeSet<TouchOperation>>,
    raw_events: u64,
    started_at: Option<String>,
    overflows: Vec<OverflowNotice>,
}

struct AuditAccumulator {
    state: Mutex<AuditState>,
    changed: watch::Sender<u64>,
}

impl AuditAccumulator {
    fn new() -> (Arc<Self>, watch::Receiver<u64>) {
        let (changed, receiver) = watch::channel(0);
        (
            Arc::new(Self {
                state: Mutex::new(AuditState::default()),
                changed,
            }),
            receiver,
        )
    }

    fn observe(&self, paths: Vec<TouchedPath>, raw_events: u64) {
        if paths.is_empty() {
            return;
        }
        let now_instant = Instant::now();
        let mut state = self.state.lock().expect("audit accumulator lock poisoned");
        state.started_at.get_or_insert_with(now);
        state.first.get_or_insert(now_instant);
        state.last = Some(now_instant);
        state.raw_events = state.raw_events.saturating_add(raw_events);
        for touched in paths {
            if let Some(operations) = state.paths.get_mut(&touched.path) {
                operations.extend(touched.operations);
            } else if state.paths.len() < MAX_AUDIT_PATHS {
                state
                    .paths
                    .insert(touched.path, touched.operations.into_iter().collect());
            } else if !state.path_limit_reported {
                state.path_limit_reported = true;
            }
        }
        drop(state);
        self.notify_changed();
    }

    fn report_overflow(&self, source: String, raw_events: u64) {
        let now_instant = Instant::now();
        let mut state = self.state.lock().expect("audit accumulator lock poisoned");
        state.first.get_or_insert(now_instant);
        state.last = Some(now_instant);
        Self::push_overflow(&mut state, source, raw_events);
        drop(state);
        self.notify_changed();
    }

    fn push_overflow(state: &mut AuditState, source: String, raw_events: u64) {
        if state.overflows.len() < MAX_OVERFLOW_REPORTS - 1 {
            state.overflows.push(OverflowNotice { source, raw_events });
        } else if !state.overflow_reports_limited {
            state.overflow_reports_limited = true;
            state.overflows.push(OverflowNotice {
                source: format!("rrjj:overflow_report_limit:{MAX_OVERFLOW_REPORTS}"),
                raw_events,
            });
        }
    }

    fn notify_changed(&self) {
        self.changed.send_modify(|generation| {
            *generation = generation.wrapping_add(1);
        });
    }

    fn deadline(&self, quiescence: Duration, max_delay: Duration) -> Option<Instant> {
        let state = self.state.lock().expect("audit accumulator lock poisoned");
        match (state.first, state.last) {
            (Some(first), Some(last)) => Some(std::cmp::min(first + max_delay, last + quiescence)),
            _ => None,
        }
    }

    fn drain(&self) -> AuditBatch {
        let mut state = self.state.lock().expect("audit accumulator lock poisoned");
        let drained = std::mem::take(&mut *state);
        let mut overflows = drained.overflows;
        if drained.path_limit_reported {
            overflows.push(OverflowNotice {
                source: format!("rrjj:audit_path_limit:{MAX_AUDIT_PATHS}"),
                raw_events: drained.raw_events,
            });
        }
        AuditBatch {
            paths: drained.paths,
            raw_events: drained.raw_events,
            started_at: drained.started_at,
            overflows,
        }
    }

    fn is_dirty(&self) -> bool {
        let state = self.state.lock().expect("audit accumulator lock poisoned");
        !state.paths.is_empty() || !state.overflows.is_empty()
    }
}

#[derive(Clone)]
pub struct CoordinatorHandle {
    tx: mpsc::Sender<Command>,
    paused: watch::Sender<bool>,
    audit: Arc<AuditAccumulator>,
}

impl CoordinatorHandle {
    pub async fn status(&self) -> Result<Status> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Status { reply }).await?;
        Ok(response.await?)
    }

    pub async fn snap(&self) -> Result<Snapshot> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Snap { reply }).await?;
        response.await?
    }

    pub async fn mark(&self, label: String, meta: Map<String, Value>) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Mark { label, meta, reply }).await?;
        response.await?
    }

    pub async fn touched(&self, paths: Vec<TouchedPath>, raw_events: u64) -> Result<()> {
        self.audit.observe(paths, raw_events);
        Ok(())
    }

    pub async fn pause(&self) -> Result<Status> {
        let (reply, response) = oneshot::channel();
        self.paused.send_replace(true);
        self.tx.send(Command::Pause { reply }).await?;
        Ok(response.await?)
    }

    pub async fn resume(&self) -> Result<Status> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Resume { reply }).await?;
        let status = response.await??;
        self.paused.send_replace(false);
        Ok(status)
    }

    pub async fn overflow(&self, source: String, raw_events: u64) -> Result<()> {
        self.audit.report_overflow(source, raw_events);
        Ok(())
    }

    pub async fn flush(&self) -> Result<Status> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Flush { reply }).await?;
        response.await?
    }

    pub async fn shutdown(&self, reason: String) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.tx.send(Command::Shutdown { reason, reply }).await?;
        response.await?
    }
}

pub async fn start(config: Config, sink: Arc<dyn Sink>) -> Result<CoordinatorHandle> {
    validate_roots(&config.watched_root, &config.shadow_root)?;
    let (tx, rx) = mpsc::channel(64);
    let (paused, paused_rx) = watch::channel(false);
    let (audit, audit_changed) = AuditAccumulator::new();
    let coordinator = Coordinator::initialize(config, sink, audit.clone()).await?;
    let watcher_config = coordinator.config.clone();
    std::thread::Builder::new()
        .name("rrjj-coordinator".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build rrjj coordinator runtime");
            runtime.block_on(async move {
                if let Err(error) = coordinator.run(rx).await {
                    eprintln!("rrjj coordinator stopped: {error:#}");
                }
            });
        })?;
    let handle = CoordinatorHandle { tx, paused, audit };
    start_watcher(watcher_config, handle.clone(), paused_rx, audit_changed)?;
    Ok(handle)
}

struct Coordinator {
    config: Config,
    sink: Arc<dyn Sink>,
    recorder: JjRecorder,
    seq: u64,
    snapshots: u64,
    touched_paths: BTreeMap<String, BTreeSet<TouchOperation>>,
    raw_events: u64,
    audit_started_at: Option<String>,
    pending_snapshot: bool,
    force_full_scan: bool,
    paused: bool,
    pending_snapshot_event: Option<Event>,
    audit: Arc<AuditAccumulator>,
    pending_overflows: Vec<OverflowNotice>,
    path_limit_reported: bool,
}

impl Coordinator {
    async fn initialize(
        config: Config,
        sink: Arc<dyn Sink>,
        audit: Arc<AuditAccumulator>,
    ) -> Result<Self> {
        let mut recorder =
            JjRecorder::init(&config.watched_root, &config.shadow_root, &config.ignore).await?;
        let baseline = recorder.capture(true, config.max_changes_per_event).await?;
        let mut coordinator = Self {
            config,
            sink,
            recorder,
            seq: 0,
            snapshots: 0,
            touched_paths: BTreeMap::new(),
            raw_events: 0,
            audit_started_at: None,
            pending_snapshot: false,
            force_full_scan: false,
            paused: false,
            pending_snapshot_event: None,
            audit,
            pending_overflows: Vec::new(),
            path_limit_reported: false,
        };
        coordinator
            .emit(EventBody::SessionStart(SessionStart {
                roots: vec![coordinator.config.watched_root.display().to_string()],
                baseline_op: baseline.op,
                baseline_tree: baseline.tree,
                ignore: coordinator.config.ignore.clone(),
                rrjj_version: env!("CARGO_PKG_VERSION").into(),
                jj_lib_version: JJ_LIB_VERSION.into(),
                jj_store_version: JJ_STORE_VERSION.into(),
            }))
            .await?;
        Ok(coordinator)
    }

    async fn run(mut self, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        while let Some(command) = rx.recv().await {
            match command {
                Command::Status { reply } => {
                    let _ = reply.send(self.status());
                }
                Command::Snap { reply } => {
                    let result = self.snapshot().await;
                    let _ = reply.send(result);
                }
                Command::Mark { label, meta, reply } => {
                    let result = self.mark(label, meta).await;
                    let _ = reply.send(result);
                }
                Command::Flush { reply } => {
                    let result = self.flush().await;
                    let _ = reply.send(result);
                }
                Command::Pause { reply } => {
                    self.paused = true;
                    let _ = reply.send(self.status());
                }
                Command::Resume { reply } => {
                    self.drain_audit();
                    self.paused = false;
                    let result = async {
                        while self.pending_snapshot {
                            self.snapshot().await?;
                            self.drain_audit();
                        }
                        Ok(self.status())
                    }
                    .await;
                    let _ = reply.send(result);
                }
                Command::Shutdown { reason, reply } => {
                    let result = self.finish(reason).await;
                    let _ = reply.send(result);
                    return Ok(());
                }
            }
        }
        self.finish("control_channel_closed".into()).await
    }

    fn status(&self) -> Status {
        Status {
            session_id: self.config.session_id.clone(),
            seq: self.seq,
            snapshots: self.snapshots,
            last_op: self.recorder.op_id(),
            pending_snapshot: self.pending_snapshot || self.audit.is_dirty(),
            paused: self.paused,
        }
    }

    fn drain_audit(&mut self) {
        let batch = self.audit.drain();
        if self.audit_started_at.is_none() {
            self.audit_started_at = batch.started_at;
        }
        for (path, operations) in batch.paths {
            if let Some(existing) = self.touched_paths.get_mut(&path) {
                existing.extend(operations);
            } else if self.touched_paths.len() < MAX_AUDIT_PATHS {
                self.touched_paths.insert(path, operations);
            } else if !self.path_limit_reported {
                self.path_limit_reported = true;
                self.pending_overflows.push(OverflowNotice {
                    source: format!("rrjj:audit_path_limit:{MAX_AUDIT_PATHS}"),
                    raw_events: self.raw_events.saturating_add(batch.raw_events),
                });
            }
        }
        self.raw_events = self.raw_events.saturating_add(batch.raw_events);
        if !batch.overflows.is_empty() {
            self.pending_overflows.extend(batch.overflows);
            self.force_full_scan = true;
        }
        if !self.touched_paths.is_empty() || !self.pending_overflows.is_empty() {
            self.pending_snapshot = true;
        }
    }

    async fn mark(&mut self, label: String, meta: Map<String, Value>) -> Result<()> {
        self.drain_audit();
        while self.pending_snapshot {
            self.snapshot().await?;
            self.drain_audit();
        }
        self.emit(EventBody::Mark(Mark {
            label,
            meta,
            ref_op: self.recorder.op_id(),
        }))
        .await
    }

    async fn snapshot(&mut self) -> Result<Snapshot> {
        if let Some(event) = self.pending_snapshot_event.clone() {
            let snapshot = match &event.body {
                EventBody::Snapshot(snapshot) => snapshot.clone(),
                _ => unreachable!("pending snapshot event must contain a snapshot"),
            };
            self.emit_exact(&event).await?;
            self.pending_snapshot_event = None;
            self.force_full_scan = !self.pending_overflows.is_empty();
            self.pending_snapshot =
                self.audit_started_at.is_some() || !self.pending_overflows.is_empty();
            self.snapshots += 1;
            return Ok(snapshot);
        }
        self.drain_audit();
        self.emit_overflows().await?;
        self.emit_audit().await?;
        let snapshot = self
            .recorder
            .capture(self.force_full_scan, self.config.max_changes_per_event)
            .await?;
        self.pending_snapshot_event = Some(Event::new(
            &self.config.session_id,
            self.seq,
            EventBody::Snapshot(snapshot.clone()),
        ));
        let event = self
            .pending_snapshot_event
            .clone()
            .expect("snapshot event prepared above");
        self.emit_exact(&event).await?;
        self.pending_snapshot_event = None;
        self.force_full_scan = false;
        self.pending_snapshot = false;
        self.snapshots += 1;
        Ok(snapshot)
    }

    async fn emit_audit(&mut self) -> Result<()> {
        if self.audit_started_at.is_none() {
            return Ok(());
        }
        let event = TouchedPaths {
            paths: self
                .touched_paths
                .iter()
                .map(|(path, operations)| TouchedPath {
                    path: path.clone(),
                    operations: operations.iter().copied().collect(),
                })
                .collect(),
            raw_events: self.raw_events,
            window_started_at: self.audit_started_at.clone().expect("checked above"),
            window_ended_at: now(),
        };
        self.emit(EventBody::TouchedPaths(event)).await?;
        self.touched_paths.clear();
        self.raw_events = 0;
        self.audit_started_at = None;
        self.path_limit_reported = false;
        Ok(())
    }

    async fn emit_overflows(&mut self) -> Result<()> {
        while let Some(notice) = self.pending_overflows.first().cloned() {
            self.emit(EventBody::Overflow(Overflow {
                source: notice.source,
                raw_events: notice.raw_events,
                recovery: OverflowRecovery::FullScanSnapshot,
            }))
            .await?;
            self.pending_overflows.remove(0);
        }
        Ok(())
    }

    async fn finish(&mut self, reason: String) -> Result<()> {
        self.drain_audit();
        while self.pending_snapshot {
            self.snapshot().await?;
            self.drain_audit();
        }
        let events = self.seq;
        self.emit(EventBody::SessionEnd(SessionEnd {
            final_op: self.recorder.op_id(),
            reason,
            snapshots: self.snapshots,
            events,
        }))
        .await?;
        self.flush_durable().await?;
        Ok(())
    }

    async fn flush(&mut self) -> Result<Status> {
        self.drain_audit();
        while self.pending_snapshot {
            self.snapshot().await?;
            self.drain_audit();
        }
        self.flush_durable().await?;
        Ok(self.status())
    }

    async fn flush_durable(&mut self) -> Result<()> {
        let op = self.recorder.op_id();
        let through_seq = self.seq;
        self.emit(EventBody::Flush(Flush {
            op: op.clone(),
            through_seq,
        }))
        .await?;
        self.sink
            .flush_session(&FlushRequest {
                shadow_root: self.config.shadow_root.clone(),
                last_seq: self.seq - 1,
                last_op: op,
            })
            .await?;
        Ok(())
    }

    async fn emit(&mut self, body: EventBody) -> Result<()> {
        let event = Event::new(&self.config.session_id, self.seq, body);
        self.emit_exact(&event).await
    }

    async fn emit_exact(&mut self, event: &Event) -> Result<()> {
        ensure!(event.seq == self.seq, "pending event sequence changed");
        self.sink.emit(event).await?;
        self.seq += 1;
        Ok(())
    }
}

fn start_watcher(
    config: Config,
    handle: CoordinatorHandle,
    mut paused: watch::Receiver<bool>,
    mut audit_changed: watch::Receiver<u64>,
) -> Result<()> {
    ensure!(
        !config.quiescence.is_zero() && !config.max_delay.is_zero(),
        "watch delays must be greater than zero"
    );
    let root = config.watched_root.canonicalize()?;
    let ignores = build_ignore_set(&config.ignore)?;
    let excluded = config
        .excluded_paths
        .iter()
        .filter_map(|path| absolute(path).ok())
        .collect::<Vec<_>>();
    let callback_root = root.clone();
    let callback_excluded = excluded.clone();
    let callback_ignores = ignores.clone();
    let callback_literal_ignores = config.ignore.clone();
    let callback_handle = handle.clone();
    let mut watcher =
        notify::recommended_watcher(move |result: notify::Result<notify::Event>| match result {
            Ok(event) => {
                let need_rescan = event.need_rescan();
                if let Some(operation) = touch_operation(&event.kind) {
                    let touched = event
                        .paths
                        .into_iter()
                        .filter_map(|path| {
                            watcher_path(
                                &callback_root,
                                &path,
                                &callback_excluded,
                                &callback_ignores,
                                &callback_literal_ignores,
                            )
                        })
                        .map(|path| TouchedPath {
                            path,
                            operations: vec![operation],
                        })
                        .collect::<Vec<_>>();
                    callback_handle.audit.observe(touched, 1);
                }
                if need_rescan {
                    callback_handle
                        .audit
                        .report_overflow("notify:rescan".into(), 1);
                }
            }
            Err(error) => callback_handle
                .audit
                .report_overflow(format!("notify:{error}"), 0),
        })?;
    watcher.watch(&root, RecursiveMode::Recursive)?;
    tokio::spawn(async move {
        let _watcher = watcher;
        let mut retry_due = None::<Instant>;
        loop {
            let audit_due = if *paused.borrow() {
                None
            } else {
                handle.audit.deadline(config.quiescence, config.max_delay)
            };
            let due = match (audit_due, retry_due) {
                (Some(audit), Some(retry)) => Some(std::cmp::min(audit, retry)),
                (Some(audit), None) => Some(audit),
                (None, Some(retry)) if !*paused.borrow() => Some(retry),
                _ => None,
            };
            let sleep = async {
                match due {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                changed = audit_changed.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                changed = paused.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = sleep => {
                    if let Err(error) = handle.snap().await {
                        if fatal_capture_error(&error) {
                            eprintln!("rrjj fatal local capture failure: {error:#}");
                            return;
                        }
                        eprintln!("rrjj watcher snapshot failed; will retry: {error:#}");
                        retry_due = Some(Instant::now() + config.quiescence);
                    } else {
                        retry_due = None;
                    }
                }
            }
        }
    });
    Ok(())
}

fn fatal_capture_error(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<rrjj_sinks::SinkError>(),
        Some(
            rrjj_sinks::SinkError::SpoolFull { .. }
                | rrjj_sinks::SinkError::DiskExhausted { .. }
                | rrjj_sinks::SinkError::Failed(_)
        )
    )
}

fn touch_operation(kind: &EventKind) -> Option<TouchOperation> {
    match kind {
        EventKind::Access(_) => None,
        EventKind::Create(_) => Some(TouchOperation::Create),
        EventKind::Remove(_) => Some(TouchOperation::Remove),
        EventKind::Modify(ModifyKind::Name(
            RenameMode::Any
            | RenameMode::From
            | RenameMode::To
            | RenameMode::Both
            | RenameMode::Other,
        )) => Some(TouchOperation::Rename),
        EventKind::Modify(_) => Some(TouchOperation::Modify),
        _ => Some(TouchOperation::Other),
    }
}

fn build_ignore_set(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let pattern = pattern.trim_start_matches('/');
        builder.add(Glob::new(pattern)?);
        builder.add(Glob::new(&format!("{pattern}/**"))?);
    }
    Ok(builder.build()?)
}

fn watcher_path(
    root: &Path,
    path: &Path,
    excluded: &[PathBuf],
    ignores: &GlobSet,
    literal_ignores: &[String],
) -> Option<String> {
    let path = absolute(path).ok()?;
    if excluded.iter().any(|excluded| path.starts_with(excluded)) {
        return None;
    }
    let relative = path.strip_prefix(root).ok()?;
    if relative.as_os_str().is_empty()
        || ignores.is_match(relative)
        || relative.components().any(|component| {
            literal_ignores
                .iter()
                .any(|ignore| component.as_os_str() == ignore.trim_matches('/'))
        })
    {
        return None;
    }
    Some(relative.to_string_lossy().replace('\\', "/"))
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

struct JjRecorder {
    repo: Arc<ReadonlyRepo>,
    working_copy: Box<dyn WorkingCopy>,
    watched_root: PathBuf,
    shadow_root: PathBuf,
    state_generation: u64,
    base_ignores: Arc<GitIgnoreFile>,
    previous_tree: MergedTree,
    previous_commit: Option<jj_lib::backend::CommitId>,
}

impl JjRecorder {
    async fn init(watched_root: &Path, shadow_root: &Path, ignore: &[String]) -> Result<Self> {
        fs::create_dir_all(shadow_root)?;
        #[cfg(unix)]
        fs::set_permissions(shadow_root, fs::Permissions::from_mode(0o700))?;
        let settings = settings()?;
        let repo_path = shadow_root.join("repo");
        ensure!(
            !repo_path.exists(),
            "shadow repository already exists: {}",
            repo_path.display()
        );
        fs::create_dir(&repo_path)?;
        let backend_initializer: &BackendInitializer =
            &|settings, store_path| Ok(Box::new(GitBackend::init_internal(settings, store_path)?));
        let signer = Signer::from_settings(&settings)?;
        let repo = ReadonlyRepo::init(
            &settings,
            &repo_path,
            backend_initializer,
            signer,
            ReadonlyRepo::default_op_store_initializer(),
            ReadonlyRepo::default_op_heads_store_initializer(),
            ReadonlyRepo::default_index_store_initializer(),
            ReadonlyRepo::default_submodule_store_initializer(),
        )
        .await?;
        let working_copy = new_working_copy(&repo, watched_root, shadow_root, 0)?;
        let previous_tree = repo.store().empty_merged_tree();
        let ignore_rules = ignore.join("\n");
        let base_ignores = GitIgnoreFile::empty().chain(
            RepoPath::root(),
            Path::new("<rrjj-ignore>"),
            ignore_rules.as_bytes(),
        )?;
        Ok(Self {
            repo,
            working_copy,
            watched_root: watched_root.to_owned(),
            shadow_root: shadow_root.to_owned(),
            state_generation: 0,
            base_ignores,
            previous_tree,
            previous_commit: None,
        })
    }

    fn op_id(&self) -> String {
        format!("op:{}", self.repo.op_id().hex())
    }

    async fn capture(&mut self, full_scan: bool, max_changes: usize) -> Result<Snapshot> {
        if full_scan {
            self.state_generation += 1;
            self.working_copy = new_working_copy(
                &self.repo,
                &self.watched_root,
                &self.shadow_root,
                self.state_generation,
            )?;
        }
        let tree = self.snapshot_tree().await?;
        let (changes, stats, truncated) =
            project_diff(&self.previous_tree, &tree, &self.watched_root, max_changes).await?;
        let parent_op = self.op_id();
        let mut tx = self.repo.start_transaction();
        let parent_commit = self
            .previous_commit
            .clone()
            .unwrap_or_else(|| self.repo.store().root_commit_id().clone());
        let commit = tx
            .repo_mut()
            .new_commit(vec![parent_commit], tree.clone())
            .set_description("rrjj filesystem snapshot")
            .write()
            .await?;
        tx.repo_mut()
            .set_wc_commit(WorkspaceName::DEFAULT.to_owned(), commit.id().clone())
            .map_err(|error| anyhow!("set rrjj working-copy commit: {error}"))?;
        let repo = tx.commit("rrjj filesystem snapshot").await?;
        let op = format!("op:{}", repo.op_id().hex());
        let tree_id = tree
            .tree_ids()
            .as_resolved()
            .ok_or_else(|| anyhow!("snapshot produced a conflicted root tree"))?
            .hex();
        let snapshot = Snapshot {
            op,
            parent_op,
            commit: format!("c:{}", commit.id().hex()),
            tree: format!("t:{tree_id}"),
            changes,
            stats,
            truncated,
        };
        self.repo = repo;
        let locked = self.working_copy.start_mutation().await?;
        self.working_copy = locked.finish(self.repo.op_id().clone()).await?;
        self.previous_tree = tree;
        self.previous_commit = Some(commit.id().clone());
        Ok(snapshot)
    }

    async fn snapshot_tree(&mut self) -> Result<MergedTree> {
        let everything = EverythingMatcher;
        let nothing = NothingMatcher;
        let options = SnapshotOptions {
            base_ignores: self.base_ignores.clone(),
            progress: None,
            start_tracking_matcher: &everything,
            force_tracking_matcher: &nothing,
            max_new_file_size: u64::MAX,
        };
        let mut locked = self.working_copy.start_mutation().await?;
        let (tree, _) = locked.snapshot(&options).await?;
        self.working_copy = locked.finish(self.repo.op_id().clone()).await?;
        Ok(tree)
    }
}

async fn project_diff(
    before: &MergedTree,
    after: &MergedTree,
    watched_root: &Path,
    max_changes: usize,
) -> Result<(Vec<Change>, ChangeStats, bool)> {
    let mut stream = before.diff_stream(after, &EverythingMatcher);
    let mut changes = Vec::new();
    let mut stats = ChangeStats::default();
    let mut total = 0usize;
    while let Some(entry) = stream.next().await {
        let values = entry.values?;
        let before_value = resolved_value(&values.before)?;
        let after_value = resolved_value(&values.after)?;
        let kind = classify(before_value, after_value);
        stats.record(kind);
        total += 1;
        if changes.len() < max_changes {
            let path = entry.path.as_internal_file_string().to_owned();
            changes.push(Change {
                blob: blob_id(after_value),
                prev_blob: blob_id(before_value),
                bytes: after_value
                    .and_then(|_| fs::metadata(watched_root.join(&path)).ok())
                    .map(|metadata| metadata.len()),
                path,
                kind,
                from: None,
            });
        }
    }
    Ok((changes, stats, total > max_changes))
}

fn resolved_value(value: &MergedTreeValue) -> Result<Option<&TreeValue>> {
    value
        .as_resolved()
        .map(Option::as_ref)
        .ok_or_else(|| anyhow!("conflicted tree value cannot be projected in schema v0"))
}

fn classify(before: Option<&TreeValue>, after: Option<&TreeValue>) -> ChangeKind {
    match (before, after) {
        (None, Some(_)) => ChangeKind::Added,
        (Some(_), None) => ChangeKind::Removed,
        (
            Some(TreeValue::File {
                id: before_id,
                executable: before_executable,
                ..
            }),
            Some(TreeValue::File {
                id: after_id,
                executable: after_executable,
                ..
            }),
        ) if before_id == after_id && before_executable != after_executable => {
            ChangeKind::ModeChanged
        }
        _ => ChangeKind::Modified,
    }
}

fn blob_id(value: Option<&TreeValue>) -> Option<String> {
    match value {
        Some(TreeValue::File { id, .. }) => Some(format!("b:{}", id.hex())),
        Some(TreeValue::Symlink(id)) => Some(format!("b:{}", id.hex())),
        _ => None,
    }
}

fn new_working_copy(
    repo: &Arc<ReadonlyRepo>,
    watched_root: &Path,
    shadow_root: &Path,
    generation: u64,
) -> Result<Box<dyn WorkingCopy>> {
    let state_path = shadow_root.join(format!("working-copy-{generation:03}"));
    fs::create_dir(&state_path)
        .with_context(|| format!("create working-copy state {}", state_path.display()))?;
    Ok(Box::new(LocalWorkingCopy::init(
        repo.store().clone(),
        watched_root.to_owned(),
        state_path,
        repo.op_id().clone(),
        WorkspaceName::DEFAULT.to_owned(),
        repo.settings(),
    )?))
}

fn settings() -> Result<UserSettings> {
    let mut config = StackedConfig::with_defaults();
    config.add_layer(ConfigLayer::parse(
        ConfigSource::User,
        r#"
[user]
name = "rrjj recorder"
email = "rrjj.invalid"
"#,
    )?);
    Ok(UserSettings::from_config(config)?)
}

fn validate_roots(watched: &Path, shadow: &Path) -> Result<()> {
    ensure!(
        watched.is_dir(),
        "watched root must be an existing directory"
    );
    if shadow.exists() {
        ensure!(
            shadow.read_dir()?.next().is_none(),
            "shadow root must be empty"
        );
    }
    let watched = watched.canonicalize()?;
    let shadow_parent = shadow
        .parent()
        .ok_or_else(|| anyhow!("shadow root has no parent"))?
        .canonicalize()?;
    let shadow = shadow_parent.join(
        shadow
            .file_name()
            .ok_or_else(|| anyhow!("shadow root has no name"))?,
    );
    ensure!(watched != shadow, "watched and shadow roots must differ");
    ensure!(
        !shadow.starts_with(&watched) && !watched.starts_with(&shadow),
        "watched and shadow roots must not contain each other"
    );
    Ok(())
}

fn now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use async_trait::async_trait;
    use rrjj_reader::Session;
    use rrjj_schema::{Event, FormatMetadata, SCHEMA_VERSION, SESSION_FORMAT_VERSION};
    use rrjj_sinks::{DirectorySessionSink, SinkError};

    use super::*;

    #[derive(Default)]
    struct MemorySink(Mutex<Vec<Event>>);

    #[async_trait]
    impl Sink for MemorySink {
        async fn emit(&self, event: &Event) -> Result<(), SinkError> {
            self.0.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn flush(&self) -> Result<(), SinkError> {
            Ok(())
        }
    }

    struct FailAuditOnceSink {
        events: Mutex<Vec<Event>>,
        fail_audit: AtomicBool,
    }

    #[async_trait]
    impl Sink for FailAuditOnceSink {
        async fn emit(&self, event: &Event) -> Result<(), SinkError> {
            if matches!(event.body, EventBody::TouchedPaths(_))
                && self.fail_audit.swap(false, Ordering::SeqCst)
            {
                return Err(SinkError::Io(std::io::Error::other(
                    "injected audit failure",
                )));
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn flush(&self) -> Result<(), SinkError> {
            Ok(())
        }
    }

    struct FailSnapshotOnceSink {
        events: Mutex<Vec<Event>>,
        fail_snapshot: AtomicBool,
    }

    #[async_trait]
    impl Sink for FailSnapshotOnceSink {
        async fn emit(&self, event: &Event) -> Result<(), SinkError> {
            if matches!(event.body, EventBody::Snapshot(_))
                && self.fail_snapshot.swap(false, Ordering::SeqCst)
            {
                return Err(SinkError::Io(std::io::Error::other(
                    "injected snapshot failure",
                )));
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn flush(&self) -> Result<(), SinkError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn snapshots_real_tree_and_orders_mark_after_pending_snapshot() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        fs::write(watched.path().join("a.txt"), "one").unwrap();
        fs::create_dir(watched.path().join(".git")).unwrap();
        fs::write(watched.path().join(".git/config"), "ignored").unwrap();
        let sink = Arc::new(MemorySink::default());
        let handle = start(
            Config {
                session_id: "test".into(),
                watched_root: watched.path().to_owned(),
                shadow_root: shadow.path().to_owned(),
                ignore: vec![".git".into()],
                excluded_paths: vec![watched.path().join("a.txt")],
                max_changes_per_event: 100,
                quiescence: Duration::from_secs(60),
                max_delay: Duration::from_secs(60),
            },
            sink.clone(),
        )
        .await
        .unwrap();
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(shadow.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        fs::write(watched.path().join("a.txt"), "two").unwrap();
        fs::write(watched.path().join(".git/config"), "still ignored").unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "a.txt".into(),
                    operations: vec![TouchOperation::Modify],
                }],
                3,
            )
            .await
            .unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "a.txt".into(),
                    operations: vec![TouchOperation::Rename],
                }],
                1,
            )
            .await
            .unwrap();
        handle.mark("after-edit".into(), Map::new()).await.unwrap();
        handle.shutdown("test".into()).await.unwrap();

        let events = sink.0.lock().unwrap();
        assert!(events.windows(2).all(|pair| pair[1].seq == pair[0].seq + 1));
        let snapshot = events
            .iter()
            .position(|event| matches!(event.body, EventBody::Snapshot(_)))
            .unwrap();
        let mark = events
            .iter()
            .position(|event| matches!(event.body, EventBody::Mark(_)))
            .unwrap();
        assert!(snapshot < mark);
        let EventBody::Snapshot(snapshot_event) = &events[snapshot].body else {
            unreachable!();
        };
        assert_eq!(snapshot_event.changes.len(), 1);
        assert_eq!(snapshot_event.changes[0].path, "a.txt");
        let EventBody::TouchedPaths(audit) = &events[snapshot - 1].body else {
            panic!("snapshot must follow touched-path audit");
        };
        assert!(
            audit.raw_events >= 4,
            "manual audit events must be retained alongside platform watcher events"
        );
        assert_eq!(
            audit.paths[0].operations,
            vec![TouchOperation::Modify, TouchOperation::Rename]
        );
        assert!(matches!(
            &events[events.len() - 2].body,
            EventBody::SessionEnd(_)
        ));
        let EventBody::Flush(flush) = &events[events.len() - 1].body else {
            panic!("graceful shutdown must durably flush session_end");
        };
        assert_eq!(flush.through_seq, events[events.len() - 1].seq);
        assert!(!watched.path().join(".jj").exists());
        assert!(shadow.path().join("repo").exists());
    }

    #[test]
    fn classifies_executable_bit_only_change() {
        use jj_lib::backend::{CopyId, FileId};

        let id = FileId::new(vec![1]);
        let before = TreeValue::File {
            id: id.clone(),
            executable: false,
            copy_id: CopyId::placeholder(),
        };
        let after = TreeValue::File {
            id,
            executable: true,
            copy_id: CopyId::placeholder(),
        };
        assert_eq!(
            classify(Some(&before), Some(&after)),
            ChangeKind::ModeChanged
        );
    }

    #[test]
    fn ignores_access_notifications_but_records_mutations() {
        assert_eq!(
            touch_operation(&EventKind::Access(notify::event::AccessKind::Read)),
            None
        );
        assert_eq!(
            touch_operation(&EventKind::Create(notify::event::CreateKind::File)),
            Some(TouchOperation::Create)
        );
        assert_eq!(
            touch_operation(&EventKind::Modify(ModifyKind::Any)),
            Some(TouchOperation::Modify)
        );
    }

    #[tokio::test]
    async fn flushes_and_materializes_a_durable_session() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        let output = tempfile::tempdir().unwrap();
        let materialized = tempfile::tempdir().unwrap();
        fs::write(watched.path().join("recorded.txt"), "before").unwrap();
        let sink = Arc::new(
            DirectorySessionSink::create(
                output.path().join("spool.ndjson"),
                output.path().join("session"),
                "durable".into(),
                FormatMetadata {
                    session_format: SESSION_FORMAT_VERSION,
                    schema_version: SCHEMA_VERSION,
                    rrjj_version: env!("CARGO_PKG_VERSION").into(),
                    jj_lib_version: JJ_LIB_VERSION.into(),
                    jj_store_version: JJ_STORE_VERSION.into(),
                },
                1_000_000,
            )
            .await
            .unwrap(),
        );
        let handle = start(
            Config {
                session_id: "durable".into(),
                watched_root: watched.path().to_owned(),
                shadow_root: shadow.path().to_owned(),
                ignore: vec![],
                excluded_paths: vec![],
                max_changes_per_event: 100,
                quiescence: Duration::from_secs(60),
                max_delay: Duration::from_secs(60),
            },
            sink,
        )
        .await
        .unwrap();
        fs::write(watched.path().join("recorded.txt"), "after").unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "recorded.txt".into(),
                    operations: vec![TouchOperation::Modify],
                }],
                1,
            )
            .await
            .unwrap();
        let status = handle.flush().await.unwrap();

        let session = Session::open(output.path().join("session")).unwrap();
        assert_eq!(
            session.manifest().durable_op.as_deref(),
            Some(status.last_op.as_str())
        );
        let baseline_op = session.index()[0].op.clone().unwrap();
        let diff = session.diff(&baseline_op, &status.last_op).await.unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].path, "recorded.txt");
        session
            .materialize(&status.last_op, materialized.path())
            .await
            .unwrap();
        assert_eq!(
            fs::read_to_string(materialized.path().join("recorded.txt")).unwrap(),
            "after"
        );
        assert!(!watched.path().join(".jj").exists());
        handle.shutdown("test".into()).await.unwrap();
    }

    #[test]
    fn watcher_filter_excludes_ignored_and_sink_paths() {
        let root = tempfile::tempdir().unwrap();
        let spool = root.path().join("spool/events.ndjson");
        let ignores = build_ignore_set(&[".git".into(), "*.tmp".into()]).unwrap();
        assert_eq!(
            watcher_path(
                root.path(),
                &root.path().join("src/lib.rs"),
                std::slice::from_ref(&spool),
                &ignores,
                &[".git".into(), "*.tmp".into()],
            )
            .as_deref(),
            Some("src/lib.rs")
        );
        assert!(
            watcher_path(
                root.path(),
                &root.path().join(".git/config"),
                std::slice::from_ref(&spool),
                &ignores,
                &[".git".into(), "*.tmp".into()],
            )
            .is_none()
        );
        assert!(
            watcher_path(
                root.path(),
                &spool.join("part"),
                std::slice::from_ref(&spool),
                &ignores,
                &[],
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn watcher_debounces_changes_and_aggregates_operations() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        let sink = Arc::new(MemorySink::default());
        let handle = start(
            Config {
                session_id: "watch".into(),
                watched_root: watched.path().to_owned(),
                shadow_root: shadow.path().to_owned(),
                ignore: vec!["ignored".into()],
                excluded_paths: vec![],
                max_changes_per_event: 100,
                quiescence: Duration::from_millis(100),
                max_delay: Duration::from_millis(500),
            },
            sink.clone(),
        )
        .await
        .unwrap();
        fs::write(watched.path().join("tracked.txt"), "one").unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        fs::write(watched.path().join("tracked.txt"), "two").unwrap();
        fs::create_dir(watched.path().join("ignored")).unwrap();
        fs::write(watched.path().join("ignored/no.txt"), "ignored").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !handle.status().await.unwrap().pending_snapshot {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if sink
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| matches!(event.body, EventBody::Snapshot(_)))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let events = sink.0.lock().unwrap().clone();
        let audits = events
            .iter()
            .filter_map(|event| match &event.body {
                EventBody::TouchedPaths(paths) => Some(paths),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].paths.len(), 1);
        assert_eq!(audits[0].paths[0].path, "tracked.txt");
        assert!(
            audits[0].paths[0]
                .operations
                .iter()
                .any(|operation| matches!(
                    operation,
                    TouchOperation::Create | TouchOperation::Modify
                ))
        );
        handle.shutdown("test".into()).await.unwrap();
    }

    #[tokio::test]
    async fn audit_state_survives_sink_failure_and_retries_exactly_once() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        fs::write(watched.path().join("tracked.txt"), "before").unwrap();
        let sink = Arc::new(FailAuditOnceSink {
            events: Mutex::new(Vec::new()),
            fail_audit: AtomicBool::new(true),
        });
        let handle = start(test_config("audit-retry", &watched, &shadow), sink.clone())
            .await
            .unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "tracked.txt".into(),
                    operations: vec![TouchOperation::Modify],
                }],
                1,
            )
            .await
            .unwrap();
        assert!(handle.snap().await.is_err());
        handle.snap().await.unwrap();
        handle.shutdown("test".into()).await.unwrap();
        let events = sink.events.lock().unwrap();
        let audits = events
            .iter()
            .filter_map(|event| match &event.body {
                EventBody::TouchedPaths(audit) => Some(audit),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(audits.len(), 1);
        assert_eq!(audits[0].raw_events, 1);
        assert_eq!(audits[0].paths[0].path, "tracked.txt");
    }

    #[tokio::test]
    async fn committed_snapshot_event_is_retried_without_recapturing() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        let sink = Arc::new(FailSnapshotOnceSink {
            events: Mutex::new(Vec::new()),
            fail_snapshot: AtomicBool::new(true),
        });
        let handle = start(
            test_config("snapshot-retry", &watched, &shadow),
            sink.clone(),
        )
        .await
        .unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "tracked.txt".into(),
                    operations: vec![TouchOperation::Create],
                }],
                1,
            )
            .await
            .unwrap();
        assert!(handle.snap().await.is_err());
        let committed_op = handle.status().await.unwrap().last_op;
        let retried = handle.snap().await.unwrap();
        assert_eq!(retried.op, committed_op);
        handle.shutdown("test".into()).await.unwrap();
        let events = sink.events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.body, EventBody::Snapshot(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn paused_writes_are_captured_by_flush_and_shutdown() {
        for shutdown in [false, true] {
            let watched = tempfile::tempdir().unwrap();
            let shadow = tempfile::tempdir().unwrap();
            fs::write(watched.path().join("tracked.txt"), "before").unwrap();
            let sink = Arc::new(MemorySink::default());
            let handle = start(
                test_config(
                    if shutdown {
                        "paused-end"
                    } else {
                        "paused-flush"
                    },
                    &watched,
                    &shadow,
                ),
                sink.clone(),
            )
            .await
            .unwrap();
            handle.pause().await.unwrap();
            fs::write(watched.path().join("tracked.txt"), "after").unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while !handle.status().await.unwrap().pending_snapshot {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            if shutdown {
                handle.shutdown("test".into()).await.unwrap();
            } else {
                handle.flush().await.unwrap();
                handle.shutdown("test".into()).await.unwrap();
            }
            let events = sink.0.lock().unwrap();
            assert!(events.iter().any(|event| matches!(
                &event.body,
                EventBody::TouchedPaths(audit)
                    if audit.paths.iter().any(|path| path.path == "tracked.txt")
            )));
            assert!(events.iter().any(|event| matches!(
                &event.body,
                EventBody::Snapshot(snapshot)
                    if snapshot.changes.iter().any(|change| change.path == "tracked.txt")
            )));
        }
    }

    #[tokio::test]
    async fn overflow_mid_window_preserves_all_handed_off_paths() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        let sink = Arc::new(MemorySink::default());
        let handle = start(
            test_config("overflow-window", &watched, &shadow),
            sink.clone(),
        )
        .await
        .unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "before.txt".into(),
                    operations: vec![TouchOperation::Create],
                }],
                1,
            )
            .await
            .unwrap();
        handle.overflow("test".into(), 1).await.unwrap();
        handle
            .touched(
                vec![TouchedPath {
                    path: "after.txt".into(),
                    operations: vec![TouchOperation::Modify],
                }],
                1,
            )
            .await
            .unwrap();
        handle.snap().await.unwrap();
        handle.shutdown("test".into()).await.unwrap();
        let events = sink.0.lock().unwrap();
        let audit = events
            .iter()
            .find_map(|event| match &event.body {
                EventBody::TouchedPaths(audit) => Some(audit),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            audit
                .paths
                .iter()
                .map(|path| path.path.as_str())
                .collect::<Vec<_>>(),
            vec!["after.txt", "before.txt"]
        );
    }

    #[test]
    fn accumulator_drain_does_not_clear_later_observations() {
        let (audit, _changed) = AuditAccumulator::new();
        audit.observe(
            vec![TouchedPath {
                path: "during-capture.txt".into(),
                operations: vec![TouchOperation::Create],
            }],
            1,
        );
        let before_capture = audit.drain();
        audit.observe(
            vec![TouchedPath {
                path: "during-capture.txt".into(),
                operations: vec![TouchOperation::Remove],
            }],
            1,
        );
        let during_capture = audit.drain();

        assert_eq!(before_capture.raw_events, 1);
        assert_eq!(
            before_capture.paths["during-capture.txt"],
            BTreeSet::from([TouchOperation::Create])
        );
        assert_eq!(during_capture.raw_events, 1);
        assert_eq!(
            during_capture.paths["during-capture.txt"],
            BTreeSet::from([TouchOperation::Remove])
        );
    }

    #[tokio::test]
    async fn synthetic_notification_storm_does_not_fill_coordinator_queue() {
        let watched = tempfile::tempdir().unwrap();
        let shadow = tempfile::tempdir().unwrap();
        let sink = Arc::new(MemorySink::default());
        let handle = start(
            test_config("notification-storm", &watched, &shadow),
            sink.clone(),
        )
        .await
        .unwrap();
        handle.pause().await.unwrap();

        tokio::time::timeout(Duration::from_secs(5), async {
            for index in 0..100_000 {
                handle
                    .touched(
                        vec![TouchedPath {
                            path: "transient.txt".into(),
                            operations: vec![if index % 2 == 0 {
                                TouchOperation::Create
                            } else {
                                TouchOperation::Remove
                            }],
                        }],
                        1,
                    )
                    .await
                    .unwrap();
            }
        })
        .await
        .expect("100k observations should aggregate without coordinator backpressure");

        let status = tokio::time::timeout(Duration::from_secs(1), handle.status())
            .await
            .expect("control queue should remain responsive")
            .unwrap();
        assert!(status.pending_snapshot);
        tokio::time::timeout(Duration::from_secs(5), handle.flush())
            .await
            .expect("flush should not wait behind one command per observation")
            .unwrap();
        handle.shutdown("test".into()).await.unwrap();

        let events = sink.0.lock().unwrap();
        let audit = events
            .iter()
            .find_map(|event| match &event.body {
                EventBody::TouchedPaths(audit) => Some(audit),
                _ => None,
            })
            .unwrap();
        assert_eq!(audit.raw_events, 100_000);
        assert_eq!(audit.paths.len(), 1);
        assert_eq!(
            audit.paths[0].operations,
            vec![TouchOperation::Create, TouchOperation::Remove]
        );
    }

    fn test_config(
        session_id: &str,
        watched: &tempfile::TempDir,
        shadow: &tempfile::TempDir,
    ) -> Config {
        Config {
            session_id: session_id.into(),
            watched_root: watched.path().to_owned(),
            shadow_root: shadow.path().to_owned(),
            ignore: vec![],
            excluded_paths: vec![],
            max_changes_per_event: 100,
            quiescence: Duration::from_secs(60),
            max_delay: Duration::from_secs(60),
        }
    }
}
