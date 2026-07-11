use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use clap::Parser;
use futures::{AsyncReadExt as _, StreamExt as _};
use jj_lib::backend::TreeValue;
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::git_backend::GitBackend;
use jj_lib::gitignore::GitIgnoreFile;
use jj_lib::local_working_copy::LocalWorkingCopy;
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merged_tree::MergedTree;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{BackendInitializer, ReadonlyRepo, Repo as _};
use jj_lib::settings::UserSettings;
use jj_lib::signing::Signer;
use jj_lib::working_copy::{SnapshotOptions, WorkingCopy};
use serde::Serialize;
use tempfile::TempDir;

const FILES_PER_DIRECTORY: usize = 1_000;

#[derive(Debug, Parser)]
#[command(about = "Isolated jj-lib external-working-copy scale harness")]
struct Args {
    /// Number of files in the cold baseline.
    #[arg(long, default_value_t = 1_000)]
    files: usize,

    /// Bytes written to each baseline file.
    #[arg(long, default_value_t = 64)]
    file_bytes: usize,

    /// New files created before the incremental snapshot.
    #[arg(long, default_value_t = 25)]
    add: usize,

    /// Existing files modified before the incremental snapshot.
    #[arg(long, default_value_t = 25)]
    modify: usize,

    /// Existing files removed before the incremental snapshot.
    #[arg(long, default_value_t = 25)]
    remove: usize,

    /// Existing files renamed before the incremental snapshot.
    #[arg(long, default_value_t = 25)]
    move_like: usize,

    /// Recreate private working-copy state for every measured snapshot.
    #[arg(long)]
    full_rescan: bool,

    /// Existing empty directory to use as the watched root.
    #[arg(long)]
    watched_root: Option<PathBuf>,

    /// Existing empty directory to use for all private jj state.
    #[arg(long)]
    shadow_root: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct RunConfig<'a> {
    kind: &'static str,
    jj_lib_version: &'static str,
    watched_root: &'a Path,
    shadow_root: &'a Path,
    files: usize,
    file_bytes: usize,
    add: usize,
    modify: usize,
    remove: usize,
    move_like: usize,
    full_rescan: bool,
}

#[derive(Debug, Serialize)]
struct PhaseMetric {
    kind: &'static str,
    phase: &'static str,
    elapsed_ms: f64,
    diff_ms: f64,
    diff_entries: usize,
    tree_entries: usize,
    untracked_paths: usize,
    store_bytes: u64,
    store_growth_bytes: u64,
    peak_rss_bytes: Option<u64>,
}

#[derive(Debug, Serialize)]
struct VerificationMetric {
    kind: &'static str,
    phase: &'static str,
    elapsed_ms: f64,
    files_verified: usize,
    bytes_verified: u64,
    expected_diff_entries: usize,
    actual_diff_entries: usize,
    ok: bool,
}

struct Roots {
    watched: PathBuf,
    shadow: PathBuf,
    _watched_temp: Option<TempDir>,
    _shadow_temp: Option<TempDir>,
}

struct Harness {
    repo: Arc<ReadonlyRepo>,
    working_copy: Box<dyn WorkingCopy>,
    watched_root: PathBuf,
    shadow_root: PathBuf,
    state_generation: usize,
}

struct SnapshotResult {
    tree: MergedTree,
    elapsed: Duration,
    untracked_paths: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    validate_args(&args)?;
    let roots = prepare_roots(&args)?;

    emit(&RunConfig {
        kind: "config",
        jj_lib_version: "0.43.0",
        watched_root: &roots.watched,
        shadow_root: &roots.shadow,
        files: args.files,
        file_bytes: args.file_bytes,
        add: args.add,
        modify: args.modify,
        remove: args.remove,
        move_like: args.move_like,
        full_rescan: args.full_rescan,
    })?;

    generate_baseline(&roots.watched, args.files, args.file_bytes)?;
    let mut harness = Harness::init(&roots.watched, &roots.shadow).await?;
    let mut previous_store_bytes = directory_size(&roots.shadow)?;

    let baseline = harness.snapshot(args.full_rescan).await?;
    let baseline_diff =
        timed_diff(&harness.repo.store().empty_merged_tree(), &baseline.tree).await?;
    let baseline_entries = count_tree_entries(&baseline.tree)?;
    let baseline_store_bytes = directory_size(&roots.shadow)?;
    emit_phase(
        "cold_baseline",
        &baseline,
        baseline_diff,
        baseline_entries,
        baseline_store_bytes,
        previous_store_bytes,
    )?;
    previous_store_bytes = baseline_store_bytes;

    let no_op = harness.snapshot(args.full_rescan).await?;
    let no_op_diff = timed_diff(&baseline.tree, &no_op.tree).await?;
    ensure!(
        no_op_diff.1 == 0,
        "no-op snapshot changed {} tree entries",
        no_op_diff.1
    );
    let no_op_entries = count_tree_entries(&no_op.tree)?;
    let no_op_store_bytes = directory_size(&roots.shadow)?;
    emit_phase(
        "no_op",
        &no_op,
        no_op_diff,
        no_op_entries,
        no_op_store_bytes,
        previous_store_bytes,
    )?;
    previous_store_bytes = no_op_store_bytes;

    apply_churn(&roots.watched, &args)?;
    let incremental = harness.snapshot(args.full_rescan).await?;
    let incremental_diff = timed_diff(&no_op.tree, &incremental.tree).await?;
    let incremental_entries = count_tree_entries(&incremental.tree)?;
    let incremental_store_bytes = directory_size(&roots.shadow)?;
    emit_phase(
        "incremental",
        &incremental,
        incremental_diff,
        incremental_entries,
        incremental_store_bytes,
        previous_store_bytes,
    )?;

    let expected_diff_entries = args.add + args.modify + args.remove + 2 * args.move_like;
    ensure!(
        incremental_diff.1 == expected_diff_entries,
        "incremental tree diff had {} entries; expected {}",
        incremental_diff.1,
        expected_diff_entries
    );

    let verify_start = Instant::now();
    let (files_verified, bytes_verified) =
        verify_tree_matches_filesystem(&incremental.tree, &roots.watched).await?;
    emit(&VerificationMetric {
        kind: "verification",
        phase: "final_tree",
        elapsed_ms: millis(verify_start.elapsed()),
        files_verified,
        bytes_verified,
        expected_diff_entries,
        actual_diff_entries: incremental_diff.1,
        ok: true,
    })?;

    ensure!(
        !roots.watched.join(".jj").exists(),
        "jj metadata leaked into watched root"
    );
    eprintln!("PASS: final jj tree exactly matches the watched filesystem");
    Ok(())
}

impl Harness {
    async fn init(watched_root: &Path, shadow_root: &Path) -> Result<Self> {
        let settings = settings()?;
        let repo_path = shadow_root.join("repo");
        fs::create_dir(&repo_path).with_context(|| format!("create {}", repo_path.display()))?;
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
        Ok(Self {
            repo,
            working_copy,
            watched_root: watched_root.to_owned(),
            shadow_root: shadow_root.to_owned(),
            state_generation: 0,
        })
    }

    async fn snapshot(&mut self, full_rescan: bool) -> Result<SnapshotResult> {
        let start = Instant::now();
        if full_rescan {
            self.state_generation += 1;
            self.working_copy = new_working_copy(
                &self.repo,
                &self.watched_root,
                &self.shadow_root,
                self.state_generation,
            )?;
        }

        let everything = EverythingMatcher;
        let options = SnapshotOptions {
            base_ignores: GitIgnoreFile::empty(),
            progress: None,
            start_tracking_matcher: &everything,
            force_tracking_matcher: &everything,
            max_new_file_size: u64::MAX,
        };
        let mut locked = self.working_copy.start_mutation().await?;
        let (tree, stats) = locked.snapshot(&options).await?;
        self.working_copy = locked.finish(self.repo.op_id().clone()).await?;
        let elapsed = start.elapsed();
        Ok(SnapshotResult {
            tree,
            elapsed,
            untracked_paths: stats.untracked_paths.len(),
        })
    }
}

fn new_working_copy(
    repo: &Arc<ReadonlyRepo>,
    watched_root: &Path,
    shadow_root: &Path,
    generation: usize,
) -> Result<Box<dyn WorkingCopy>> {
    let state_path = shadow_root.join(format!("working-copy-{generation:03}"));
    fs::create_dir(&state_path).with_context(|| format!("create {}", state_path.display()))?;
    let working_copy = LocalWorkingCopy::init(
        repo.store().clone(),
        watched_root.to_owned(),
        state_path,
        repo.op_id().clone(),
        WorkspaceName::DEFAULT.to_owned(),
        repo.settings(),
    )?;
    Ok(Box::new(working_copy))
}

fn settings() -> Result<UserSettings> {
    let mut config = StackedConfig::with_defaults();
    config.add_layer(ConfigLayer::parse(
        ConfigSource::User,
        r#"
[user]
name = "rrjj scale harness"
email = "rrjj-scale.invalid"
"#,
    )?);
    Ok(UserSettings::from_config(config)?)
}

fn validate_args(args: &Args) -> Result<()> {
    ensure!(args.files > 0, "--files must be greater than zero");
    ensure!(
        args.file_bytes > 0,
        "--file-bytes must be greater than zero"
    );
    ensure!(
        args.modify + args.remove + args.move_like <= args.files,
        "modify + remove + move-like must not exceed baseline file count"
    );
    Ok(())
}

fn prepare_roots(args: &Args) -> Result<Roots> {
    let watched_temp = if args.watched_root.is_none() {
        Some(tempfile::Builder::new().prefix("rrjj-watched-").tempdir()?)
    } else {
        None
    };
    let shadow_temp = if args.shadow_root.is_none() {
        Some(tempfile::Builder::new().prefix("rrjj-shadow-").tempdir()?)
    } else {
        None
    };
    let watched = match (&args.watched_root, &watched_temp) {
        (Some(path), _) => prepare_empty_root(path)?,
        (None, Some(temp)) => temp.path().to_owned(),
        (None, None) => unreachable!(),
    };
    let shadow = match (&args.shadow_root, &shadow_temp) {
        (Some(path), _) => prepare_empty_root(path)?,
        (None, Some(temp)) => temp.path().to_owned(),
        (None, None) => unreachable!(),
    };
    let watched = watched.canonicalize()?;
    let shadow = shadow.canonicalize()?;
    ensure!(watched != shadow, "watched and shadow roots must differ");
    ensure!(
        !shadow.starts_with(&watched),
        "shadow root must not be inside watched root"
    );
    ensure!(
        !watched.starts_with(&shadow),
        "watched root must not be inside shadow root"
    );
    Ok(Roots {
        watched,
        shadow,
        _watched_temp: watched_temp,
        _shadow_temp: shadow_temp,
    })
}

fn prepare_empty_root(path: &Path) -> Result<PathBuf> {
    fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    ensure!(
        fs::read_dir(path)?.next().is_none(),
        "{} must be empty",
        path.display()
    );
    Ok(path.to_owned())
}

fn generate_baseline(root: &Path, count: usize, file_bytes: usize) -> Result<()> {
    for index in 0..count {
        let path = baseline_path(root, index);
        if index % FILES_PER_DIRECTORY == 0 {
            fs::create_dir_all(path.parent().expect("baseline path has parent"))?;
        }
        fs::write(&path, content(index, 0, file_bytes))
            .with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

fn apply_churn(root: &Path, args: &Args) -> Result<()> {
    let mut cursor = 0;
    for index in cursor..cursor + args.modify {
        let path = baseline_path(root, index);
        let mut bytes = content(index, 1, args.file_bytes);
        bytes.push(b'\n');
        fs::write(&path, bytes).with_context(|| format!("modify {}", path.display()))?;
    }
    cursor += args.modify;

    for index in cursor..cursor + args.remove {
        let path = baseline_path(root, index);
        fs::remove_file(&path).with_context(|| format!("remove {}", path.display()))?;
    }
    cursor += args.remove;

    for index in cursor..cursor + args.move_like {
        let source = baseline_path(root, index);
        let destination = root.join("moved").join(format!("f{index:012}.bin"));
        fs::create_dir_all(destination.parent().expect("move path has parent"))?;
        fs::rename(&source, &destination)
            .with_context(|| format!("move {} to {}", source.display(), destination.display()))?;
    }

    for index in 0..args.add {
        let path = root.join("added").join(format!("f{index:012}.bin"));
        if index == 0 {
            fs::create_dir_all(path.parent().expect("added path has parent"))?;
        }
        fs::write(&path, content(index, 2, args.file_bytes))
            .with_context(|| format!("add {}", path.display()))?;
    }
    Ok(())
}

fn baseline_path(root: &Path, index: usize) -> PathBuf {
    root.join("baseline")
        .join(format!("d{:08}", index / FILES_PER_DIRECTORY))
        .join(format!("f{index:012}.bin"))
}

fn content(index: usize, generation: usize, bytes: usize) -> Vec<u8> {
    let prefix = format!("{index:012}:{generation}:");
    prefix.bytes().cycle().take(bytes).collect::<Vec<_>>()
}

async fn timed_diff(before: &MergedTree, after: &MergedTree) -> Result<(Duration, usize)> {
    let start = Instant::now();
    let mut stream = before.diff_stream(after, &EverythingMatcher);
    let mut count = 0;
    while let Some(entry) = stream.next().await {
        entry.values?;
        count += 1;
    }
    Ok((start.elapsed(), count))
}

fn count_tree_entries(tree: &MergedTree) -> Result<usize> {
    tree.entries().try_fold(0, |count, (_path, value)| {
        value.map(|_| count + 1).map_err(Into::into)
    })
}

async fn verify_tree_matches_filesystem(
    tree: &MergedTree,
    watched_root: &Path,
) -> Result<(usize, u64)> {
    let filesystem_count = count_files(watched_root)?;
    let mut tree_count = 0;
    let mut bytes_verified = 0;
    for (repo_path, merged_value) in tree.entries() {
        let merged_value = merged_value?;
        let Some(value) = merged_value.as_resolved() else {
            bail!(
                "conflicted tree value at {}",
                repo_path.as_internal_file_string()
            );
        };
        let Some(value) = value else {
            bail!(
                "absent tree value emitted at {}",
                repo_path.as_internal_file_string()
            );
        };
        let TreeValue::File { id, .. } = value else {
            bail!(
                "non-file tree value at {}",
                repo_path.as_internal_file_string()
            );
        };
        let disk_path = watched_root.join(repo_path.as_internal_file_string());
        let disk_bytes =
            fs::read(&disk_path).with_context(|| format!("read {}", disk_path.display()))?;
        let mut stored_bytes = Vec::with_capacity(disk_bytes.len());
        tree.store()
            .read_file(&repo_path, id)
            .await?
            .read_to_end(&mut stored_bytes)
            .await?;
        ensure!(
            disk_bytes == stored_bytes,
            "content mismatch for {}",
            repo_path.as_internal_file_string()
        );
        tree_count += 1;
        bytes_verified += disk_bytes.len() as u64;
    }
    ensure!(
        tree_count == filesystem_count,
        "tree has {tree_count} files but filesystem has {filesystem_count}"
    );
    Ok((tree_count, bytes_verified))
}

fn count_files(root: &Path) -> Result<usize> {
    let mut pending = vec![root.to_owned()];
    let mut count = 0;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                count += 1;
            } else {
                bail!("unsupported filesystem entry {}", entry.path().display());
            }
        }
    }
    Ok(count)
}

fn directory_size(root: &Path) -> Result<u64> {
    let mut pending = vec![root.to_owned()];
    let mut bytes = 0;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                bytes += entry.metadata()?.len();
            }
        }
    }
    Ok(bytes)
}

fn emit_phase(
    phase: &'static str,
    snapshot: &SnapshotResult,
    diff: (Duration, usize),
    tree_entries: usize,
    store_bytes: u64,
    previous_store_bytes: u64,
) -> Result<()> {
    emit(&PhaseMetric {
        kind: "phase",
        phase,
        elapsed_ms: millis(snapshot.elapsed),
        diff_ms: millis(diff.0),
        diff_entries: diff.1,
        tree_entries,
        untracked_paths: snapshot.untracked_paths,
        store_bytes,
        store_growth_bytes: store_bytes.saturating_sub(previous_store_bytes),
        peak_rss_bytes: peak_rss_bytes(),
    })
}

fn emit(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(unix)]
fn peak_rss_bytes() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage on success.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }
    // SAFETY: getrusage returned success, so usage is initialized.
    let max_rss = unsafe { usage.assume_init() }.ru_maxrss as u64;
    #[cfg(target_os = "macos")]
    {
        Some(max_rss)
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some(max_rss * 1_024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_content_has_requested_length_and_generation() {
        let first = content(7, 0, 128);
        let changed = content(7, 1, 128);
        assert_eq!(first.len(), 128);
        assert_eq!(changed.len(), 128);
        assert_ne!(first, changed);
    }

    #[test]
    fn baseline_layout_shards_large_file_sets() {
        let root = Path::new("/watch");
        assert_eq!(
            baseline_path(root, 999),
            root.join("baseline/d00000000/f000000000999.bin")
        );
        assert_eq!(
            baseline_path(root, 1_000),
            root.join("baseline/d00000001/f000000001000.bin")
        );
    }
}
