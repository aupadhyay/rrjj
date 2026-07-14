use std::collections::BTreeSet;
use std::fs;
use std::io::{BufReader, Read as _};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;

use async_trait::async_trait;
use rrjj_schema::{Event, FormatMetadata, SessionManifest};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt as _, BufWriter};
use tokio::sync::Mutex;

use crate::error::SinkError;
use crate::traits::{FlushRequest, Sink, SinkCursor};
use crate::util::{
    event_op, set_private_directory_permissions, set_private_file_permissions, temporary_path,
    write_json_atomic,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DirectorySyncStats {
    pub files_linked: u64,
    pub files_copied: u64,
    pub files_replaced: u64,
    pub files_reused: u64,
    pub files_removed: u64,
    pub bytes_copied: u64,
}

pub struct DirectorySessionSink {
    spool: Mutex<BufWriter<File>>,
    spool_path: PathBuf,
    session_dir: PathBuf,
    max_spool_bytes: u64,
    state: StdMutex<DirectoryState>,
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
            storage: None,
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
        // The synced spool append is the acceptance boundary.
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
        let checkpoint = request.checkpoint.clone();
        let stats = tokio::task::spawn_blocking(move || {
            sync_directory_session(
                &shadow_root,
                &spool_path,
                &session_dir,
                durable_manifest,
                checkpoint.as_deref(),
            )
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

fn sync_directory_session(
    shadow_root: &Path,
    spool_path: &Path,
    session_dir: &Path,
    mut manifest: SessionManifest,
    checkpoint: Option<&str>,
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
        checkpoint: checkpoint.map(str::to_owned),
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
        crate::util::remove_temporary(&temporary)?;
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
    crate::util::remove_temporary(&temporary)?;
    let bytes = fs::copy(source, &temporary)?;
    fs::File::open(&temporary)?.sync_all()?;
    fs::rename(temporary, destination)?;
    Ok(bytes)
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
