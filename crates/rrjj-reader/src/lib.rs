use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt as _;
use jj_lib::backend::TreeValue;
use jj_lib::config::{ConfigLayer, ConfigSource, StackedConfig};
use jj_lib::matchers::EverythingMatcher;
use jj_lib::merge::MergedTreeValue;
use jj_lib::object_id::ObjectId as _;
use jj_lib::op_store::OperationId;
use jj_lib::ref_name::WorkspaceName;
use jj_lib::repo::{ReadonlyRepo, Repo as _, RepoLoader, StoreFactories};
use jj_lib::settings::UserSettings;
use jj_lib::working_copy::WorkingCopyFactory;
use rrjj_schema::{Event, EventBody, SCHEMA_VERSION, SessionManifest};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("read event stream: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid JSON on line {line}: {source}")]
    Json {
        line: usize,
        source: serde_json::Error,
    },
    #[error("unsupported schema version {version} on line {line}")]
    Version { line: usize, version: u8 },
    #[error("sequence gap on line {line}: expected {expected}, found {actual}")]
    Sequence {
        line: usize,
        expected: u64,
        actual: u64,
    },
    #[error("session changed on line {line}: expected {expected}, found {actual}")]
    Session {
        line: usize,
        expected: String,
        actual: String,
    },
    #[error("session manifest is missing")]
    MissingManifest,
    #[error("incompatible session format {session_format} or schema {schema_version}")]
    Incompatible {
        session_format: u32,
        schema_version: u8,
    },
    #[error("session has no durable watermark")]
    NotDurable,
    #[error("sequence {requested} is beyond durable watermark {durable}")]
    BeyondWatermark { requested: u64, durable: u64 },
    #[error("operation {0} is not in the durable timeline")]
    UnknownOperation(String),
    #[error("tree {0} is not in the durable timeline")]
    UnknownTree(String),
    #[error("invalid object id {0}")]
    InvalidObjectId(String),
    #[error("repository error: {0}")]
    Repository(String),
    #[error("materialization destination must be outside the source session")]
    DestinationInsideSession,
    #[error("materialization destination is not empty: {0}")]
    DestinationNotEmpty(PathBuf),
}

#[derive(Clone, Debug)]
pub struct Timeline {
    events: Vec<Event>,
}

#[derive(Clone, Debug)]
pub struct Session {
    root: PathBuf,
    manifest: SessionManifest,
    timeline: Timeline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TimelineEntry {
    pub seq: u64,
    pub timestamp: String,
    pub kind: String,
    pub op: Option<String>,
    pub tree: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OperationInfo {
    pub seq: u64,
    pub op: String,
    pub parent_op: Option<String>,
    pub commit: Option<String>,
    pub tree: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TreeDiff {
    pub path: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

impl Session {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, ReadError> {
        let root = root.as_ref().to_owned();
        let manifest_path = root.join("manifest.json");
        let manifest_bytes = fs::read(&manifest_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                ReadError::MissingManifest
            } else {
                ReadError::Io(error)
            }
        })?;
        let manifest: SessionManifest = serde_json::from_slice(&manifest_bytes)
            .map_err(|source| ReadError::Json { line: 0, source })?;
        if !manifest.is_compatible() {
            return Err(ReadError::Incompatible {
                session_format: manifest.format.session_format,
                schema_version: manifest.format.schema_version,
            });
        }
        let durable = manifest.durable_seq.ok_or(ReadError::NotDurable)?;
        let events_path = manifest
            .events_object
            .as_deref()
            .unwrap_or("events/000000.ndjson");
        let events = fs::File::open(root.join(events_path))?;
        let timeline = Timeline::read(events)?;
        if timeline.events().last().map(|event| event.seq) != Some(durable) {
            return Err(ReadError::BeyondWatermark {
                requested: timeline.events().last().map_or(0, |event| event.seq),
                durable,
            });
        }
        Ok(Self {
            root,
            manifest,
            timeline,
        })
    }

    pub fn manifest(&self) -> &SessionManifest {
        &self.manifest
    }

    pub fn index(&self) -> Vec<TimelineEntry> {
        self.timeline
            .events()
            .iter()
            .map(|event| {
                let (kind, op, tree) = match &event.body {
                    EventBody::SessionStart(value) => (
                        "session_start",
                        Some(value.baseline_op.clone()),
                        Some(value.baseline_tree.clone()),
                    ),
                    EventBody::Snapshot(value) => {
                        ("snapshot", Some(value.op.clone()), Some(value.tree.clone()))
                    }
                    EventBody::TouchedPaths(_) => ("touched_paths", None, None),
                    EventBody::Mark(value) => ("mark", Some(value.ref_op.clone()), None),
                    EventBody::Flush(value) => ("flush", Some(value.op.clone()), None),
                    EventBody::SessionEnd(value) => {
                        ("session_end", Some(value.final_op.clone()), None)
                    }
                    EventBody::Error(_) => ("error", None, None),
                    EventBody::Overflow(_) => ("overflow", None, None),
                };
                TimelineEntry {
                    seq: event.seq,
                    timestamp: event.ts.clone(),
                    kind: kind.into(),
                    op,
                    tree,
                }
            })
            .collect()
    }

    pub fn event(&self, seq: u64) -> Result<&Event, ReadError> {
        let durable = self.manifest.durable_seq.ok_or(ReadError::NotDurable)?;
        if seq > durable {
            return Err(ReadError::BeyondWatermark {
                requested: seq,
                durable,
            });
        }
        self.timeline.event(seq).ok_or(ReadError::BeyondWatermark {
            requested: seq,
            durable,
        })
    }

    pub fn inspect_operation(&self, op: &str) -> Result<OperationInfo, ReadError> {
        self.timeline
            .events()
            .iter()
            .find_map(|event| match &event.body {
                EventBody::SessionStart(value) if value.baseline_op == op => Some(OperationInfo {
                    seq: event.seq,
                    op: value.baseline_op.clone(),
                    parent_op: None,
                    commit: None,
                    tree: value.baseline_tree.clone(),
                }),
                EventBody::Snapshot(value) if value.op == op => Some(OperationInfo {
                    seq: event.seq,
                    op: value.op.clone(),
                    parent_op: Some(value.parent_op.clone()),
                    commit: Some(value.commit.clone()),
                    tree: value.tree.clone(),
                }),
                _ => None,
            })
            .ok_or_else(|| ReadError::UnknownOperation(op.into()))
    }

    pub fn inspect_tree(&self, tree: &str) -> Result<OperationInfo, ReadError> {
        self.index()
            .into_iter()
            .find(|entry| entry.tree.as_deref() == Some(tree))
            .and_then(|entry| entry.op)
            .ok_or_else(|| ReadError::UnknownTree(tree.into()))
            .and_then(|op| self.inspect_operation(&op))
    }

    pub async fn diff(&self, before_op: &str, after_op: &str) -> Result<Vec<TreeDiff>, ReadError> {
        self.inspect_operation(before_op)?;
        self.inspect_operation(after_op)?;
        let loader = repo_loader(&self.root.join("store/repo"))?;
        let before = load_repo_at(&loader, before_op).await?;
        let after = load_repo_at(&loader, after_op).await?;
        let before_tree = wc_commit(&before).await?.tree();
        let after_tree = wc_commit(&after).await?.tree();
        let mut stream = before_tree.diff_stream(&after_tree, &EverythingMatcher);
        let mut changes = Vec::new();
        while let Some(entry) = stream.next().await {
            let values = entry.values.map_err(repository)?;
            changes.push(TreeDiff {
                path: entry.path.as_internal_file_string().to_owned(),
                before: value_id(resolved(&values.before)?),
                after: value_id(resolved(&values.after)?),
            });
        }
        Ok(changes)
    }

    pub async fn materialize(
        &self,
        op: &str,
        destination: impl AsRef<Path>,
    ) -> Result<(), ReadError> {
        self.inspect_operation(op)?;
        let destination = destination.as_ref();
        let session = self.root.canonicalize()?;
        let destination_absolute = absolute_path(destination)?;
        if destination_absolute.starts_with(&session) {
            return Err(ReadError::DestinationInsideSession);
        }
        if destination.exists() {
            if !destination.is_dir() || destination.read_dir()?.next().is_some() {
                return Err(ReadError::DestinationNotEmpty(destination.to_owned()));
            }
        } else {
            fs::create_dir_all(destination)?;
        }
        let repo = self.load_repo_at(op).await?;
        let commit = wc_commit(&repo).await?;
        let state = tempfile::tempdir()?;
        let factory = jj_lib::local_working_copy::LocalWorkingCopyFactory {};
        let working_copy = factory
            .init_working_copy(
                repo.store().clone(),
                destination.to_owned(),
                state.path().to_owned(),
                repo.op_id().clone(),
                WorkspaceName::DEFAULT.to_owned(),
                repo.settings(),
            )
            .map_err(repository)?;
        let mut locked = working_copy.start_mutation().await.map_err(repository)?;
        locked.check_out(&commit).await.map_err(repository)?;
        locked
            .finish(repo.op_id().clone())
            .await
            .map_err(repository)?;
        Ok(())
    }

    async fn load_repo_at(&self, op: &str) -> Result<Arc<ReadonlyRepo>, ReadError> {
        let loader = repo_loader(&self.root.join("store/repo"))?;
        load_repo_at(&loader, op).await
    }
}

async fn load_repo_at(loader: &RepoLoader, op: &str) -> Result<Arc<ReadonlyRepo>, ReadError> {
    let id = OperationId::new(decode_id(op)?);
    let operation = loader.load_operation(&id).await.map_err(repository)?;
    loader.load_at(&operation).await.map_err(repository)
}

async fn wc_commit(repo: &Arc<ReadonlyRepo>) -> Result<jj_lib::commit::Commit, ReadError> {
    let id = repo
        .view()
        .get_wc_commit_id(WorkspaceName::DEFAULT)
        .ok_or_else(|| ReadError::Repository("operation has no rrjj working-copy commit".into()))?;
    repo.store().get_commit_async(id).await.map_err(repository)
}

fn repo_loader(path: &Path) -> Result<RepoLoader, ReadError> {
    let settings = settings()?;
    RepoLoader::init_from_file_system(&settings, path, &StoreFactories::default())
        .map_err(repository)
}

fn settings() -> Result<UserSettings, ReadError> {
    let mut config = StackedConfig::with_defaults();
    config.add_layer(
        ConfigLayer::parse(
            ConfigSource::User,
            "[user]\nname = \"rrjj reader\"\nemail = \"rrjj.invalid\"\n",
        )
        .map_err(repository)?,
    );
    UserSettings::from_config(config).map_err(repository)
}

fn decode_id(value: &str) -> Result<Vec<u8>, ReadError> {
    let hex = value.split_once(':').map_or(value, |(_, hex)| hex);
    if !hex.len().is_multiple_of(2) {
        return Err(ReadError::InvalidObjectId(value.into()));
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16)
                .map_err(|_| ReadError::InvalidObjectId(value.into()))
        })
        .collect()
}

fn resolved(value: &MergedTreeValue) -> Result<Option<&TreeValue>, ReadError> {
    value
        .as_resolved()
        .map(Option::as_ref)
        .ok_or_else(|| ReadError::Repository("conflicted tree cannot be diffed".into()))
}

fn value_id(value: Option<&TreeValue>) -> Option<String> {
    match value {
        Some(TreeValue::File { id, .. }) => Some(id.hex()),
        Some(TreeValue::Symlink(id)) => Some(id.hex()),
        Some(TreeValue::Tree(id)) => Some(id.hex()),
        Some(TreeValue::GitSubmodule(id)) => Some(id.hex()),
        None => None,
    }
}

fn repository(error: impl std::fmt::Display) -> ReadError {
    ReadError::Repository(error.to_string())
}

fn absolute_path(path: &Path) -> Result<PathBuf, ReadError> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

impl Timeline {
    pub fn read(reader: impl Read) -> Result<Self, ReadError> {
        let mut events = Vec::new();
        let mut session_id: Option<String> = None;
        for (index, line) in BufReader::new(reader).lines().enumerate() {
            let line_number = index + 1;
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: Event = serde_json::from_str(&line).map_err(|source| ReadError::Json {
                line: line_number,
                source,
            })?;
            if event.v != SCHEMA_VERSION {
                return Err(ReadError::Version {
                    line: line_number,
                    version: event.v,
                });
            }
            let expected_seq = events.len() as u64;
            if event.seq != expected_seq {
                return Err(ReadError::Sequence {
                    line: line_number,
                    expected: expected_seq,
                    actual: event.seq,
                });
            }
            match &session_id {
                Some(expected) if expected != &event.session_id => {
                    return Err(ReadError::Session {
                        line: line_number,
                        expected: expected.clone(),
                        actual: event.session_id,
                    });
                }
                None => session_id = Some(event.session_id.clone()),
                _ => {}
            }
            events.push(event);
        }
        Ok(Self { events })
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub fn event(&self, seq: u64) -> Option<&Event> {
        self.events.get(seq as usize)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rrjj_schema::{FormatMetadata, SESSION_FORMAT_VERSION, SessionManifest};

    use super::*;

    #[test]
    fn rejects_sequence_gaps() {
        let input = concat!(
            r#"{"v":0,"seq":0,"session_id":"s","ts":"x","type":"mark","data":{"label":"a","meta":{},"ref_op":"op:a"}}"#,
            "\n",
            r#"{"v":0,"seq":2,"session_id":"s","ts":"x","type":"mark","data":{"label":"b","meta":{},"ref_op":"op:b"}}"#,
            "\n"
        );
        assert!(matches!(
            Timeline::read(input.as_bytes()),
            Err(ReadError::Sequence {
                expected: 1,
                actual: 2,
                ..
            })
        ));
    }

    #[test]
    fn refuses_incompatible_session_format() {
        let root = tempfile::tempdir().unwrap();
        write_manifest(
            root.path(),
            SessionManifest {
                session_id: "s".into(),
                format: FormatMetadata {
                    session_format: SESSION_FORMAT_VERSION + 1,
                    schema_version: SCHEMA_VERSION,
                    rrjj_version: "test".into(),
                    jj_lib_version: "0.43.0".into(),
                    jj_store_version: "test".into(),
                },
                last_seq: 0,
                last_op: "op:a".into(),
                events_object: None,
                durable_seq: Some(0),
                durable_op: Some("op:a".into()),
            },
        );
        assert!(matches!(
            Session::open(root.path()),
            Err(ReadError::Incompatible { .. })
        ));
    }

    #[test]
    fn refuses_events_beyond_durable_watermark() {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("events")).unwrap();
        fs::write(
            root.path().join("events/000000.ndjson"),
            concat!(
                r#"{"v":0,"seq":0,"session_id":"s","ts":"x","type":"mark","data":{"label":"a","meta":{},"ref_op":"op:a"}}"#,
                "\n",
                r#"{"v":0,"seq":1,"session_id":"s","ts":"x","type":"mark","data":{"label":"b","meta":{},"ref_op":"op:a"}}"#,
                "\n"
            ),
        )
        .unwrap();
        write_manifest(
            root.path(),
            SessionManifest {
                session_id: "s".into(),
                format: FormatMetadata {
                    session_format: SESSION_FORMAT_VERSION,
                    schema_version: SCHEMA_VERSION,
                    rrjj_version: "test".into(),
                    jj_lib_version: "0.43.0".into(),
                    jj_store_version: "test".into(),
                },
                last_seq: 1,
                last_op: "op:a".into(),
                events_object: None,
                durable_seq: Some(0),
                durable_op: Some("op:a".into()),
            },
        );
        assert!(matches!(
            Session::open(root.path()),
            Err(ReadError::BeyondWatermark {
                requested: 1,
                durable: 0
            })
        ));
    }

    fn write_manifest(root: &Path, manifest: SessionManifest) {
        fs::write(
            root.join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }
}
