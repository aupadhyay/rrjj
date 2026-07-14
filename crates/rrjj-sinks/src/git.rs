use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use git2::{Cred, FetchOptions, Oid, PushOptions, RemoteCallbacks, Repository};

use crate::error::SinkError;
use crate::traits::{CheckpointPublication, CheckpointRequest, CheckpointSink};
use crate::util::{normalize_commit_oid, redact_secrets};

#[derive(Clone, Debug)]
pub struct GitCheckpointConfig {
    pub remote_url: String,
    pub authorization: Option<String>,
    pub ref_prefix: String,
    pub session_id: String,
    pub cursor_path: PathBuf,
}

pub struct GitCheckpointSink {
    config: GitCheckpointConfig,
    last_pushed: Mutex<Option<String>>,
}

impl GitCheckpointSink {
    pub fn create(config: GitCheckpointConfig) -> Result<Self, SinkError> {
        validate_ref_prefix(&config.ref_prefix)?;
        validate_session_id(&config.session_id)?;
        let last_pushed = read_pushed_oid(&config.cursor_path)?;
        Ok(Self {
            config,
            last_pushed: Mutex::new(last_pushed),
        })
    }

    pub fn session_ref(&self) -> String {
        format!(
            "{}/{}",
            self.config.ref_prefix.trim_end_matches('/'),
            self.config.session_id
        )
    }

    fn sanitized(&self, message: impl Into<String>) -> String {
        let secrets = self
            .config
            .authorization
            .as_deref()
            .map(|value| vec![value])
            .unwrap_or_default();
        redact_secrets(&message.into(), &secrets)
    }
}

#[async_trait]
impl CheckpointSink for GitCheckpointSink {
    async fn publish_checkpoint(
        &self,
        request: &CheckpointRequest,
    ) -> Result<CheckpointPublication, SinkError> {
        let commit = normalize_commit_oid(&request.commit)?;
        let config = self.config.clone();
        let session_ref = self.session_ref();
        let expected = self.last_pushed.lock().expect("git cursor").clone();
        let shadow_root = request.shadow_root.clone();
        let result = tokio::task::spawn_blocking(move || {
            push_checkpoint_from_shadow(
                &config,
                &shadow_root,
                &session_ref,
                &commit,
                expected.as_deref(),
            )
        })
        .await
        .map_err(|error| SinkError::Failed(self.sanitized(error.to_string())))?
        .map_err(|error| match error {
            SinkError::CheckpointConflict(message) => {
                SinkError::CheckpointConflict(self.sanitized(message))
            }
            SinkError::Failed(message) => SinkError::Failed(self.sanitized(message)),
            SinkError::Transient(message) => SinkError::Transient(self.sanitized(message)),
            SinkError::InvalidConfig(message) => SinkError::InvalidConfig(self.sanitized(message)),
            SinkError::InvalidFlush(message) => SinkError::InvalidFlush(self.sanitized(message)),
            other => other,
        })?;
        *self.last_pushed.lock().expect("git cursor") = Some(result.checkpoint.clone());
        write_pushed_oid(&self.config.cursor_path, &result.checkpoint)?;
        Ok(result)
    }
}

pub(crate) fn push_checkpoint_from_shadow(
    config: &GitCheckpointConfig,
    shadow_root: &Path,
    session_ref: &str,
    commit: &str,
    expected_prior: Option<&str>,
) -> Result<CheckpointPublication, SinkError> {
    if session_ref.starts_with("refs/heads/") || !session_ref.starts_with("refs/") {
        return Err(SinkError::InvalidConfig(format!(
            "recorder ref must be fully qualified under refs/ and outside refs/heads: {session_ref}"
        )));
    }
    let git_dir = shadow_root.join("repo/store/git");
    let repo = Repository::open_bare(&git_dir).map_err(|error| {
        SinkError::Failed(format!(
            "open shadow git repository {}: {error}",
            git_dir.display()
        ))
    })?;
    let oid = Oid::from_str(commit).map_err(|error| {
        SinkError::InvalidFlush(format!("invalid commit OID {commit}: {error}"))
    })?;
    repo.find_commit(oid).map_err(|error| {
        SinkError::InvalidFlush(format!(
            "checkpoint commit {commit} is missing from the local Git object store: {error}"
        ))
    })?;

    repo.reference(session_ref, oid, true, "rrjj checkpoint")
        .map_err(|error| SinkError::Failed(format!("update local recorder ref: {error}")))?;

    let remote_tip = if let Ok(remote_repo) =
        Repository::open(&config.remote_url).or_else(|_| Repository::open_bare(&config.remote_url))
    {
        remote_repo
            .find_reference(session_ref)
            .ok()
            .and_then(|reference| reference.target())
            .map(|oid| oid.to_string())
    } else {
        let mut remote = repo
            .remote_anonymous(&config.remote_url)
            .map_err(|error| SinkError::Failed(format!("open git remote: {error}")))?;
        fetch_remote_tip(&repo, &mut remote, config, session_ref)?
    };

    match (remote_tip.as_deref(), expected_prior) {
        (Some(tip), _) if tip == commit => {
            return Ok(CheckpointPublication {
                checkpoint: commit.to_owned(),
            });
        }
        (None, _) => {}
        (Some(tip), Some(expected)) if tip == expected => {}
        (Some(tip), Some(expected)) => {
            return Err(SinkError::CheckpointConflict(format!(
                "remote {session_ref} is at {tip}, expected {expected} before advancing to {commit}"
            )));
        }
        (Some(tip), None) => {
            return Err(SinkError::CheckpointConflict(format!(
                "remote {session_ref} already exists at {tip}; refusing unconditional create of {commit}"
            )));
        }
    }

    let mut remote = repo
        .remote_anonymous(&config.remote_url)
        .map_err(|error| SinkError::Failed(format!("open git remote: {error}")))?;

    let authorization = config.authorization.clone();
    let rejected = Arc::new(Mutex::new(None::<String>));
    let rejected_cb = rejected.clone();
    let mut callbacks = RemoteCallbacks::new();
    if let Some(authorization) = authorization.clone() {
        callbacks.credentials(move |_url, _username, _allowed| {
            Cred::userpass_plaintext("x-access-token", &authorization)
        });
    }
    callbacks.push_update_reference(move |name, status| {
        if let Some(status) = status {
            *rejected_cb.lock().expect("push status") = Some(format!("{name}: {status}"));
        }
        Ok(())
    });

    let mut options = PushOptions::new();
    options.remote_callbacks(callbacks);
    if let Some(authorization) = &authorization {
        let header = format!("Authorization: {authorization}");
        options.custom_headers(&[&header]);
    }

    let refspec = format!("{session_ref}:{session_ref}");
    remote
        .push(&[refspec.as_str()], Some(&mut options))
        .map_err(|error| {
            let message = error.message().to_owned();
            if message.contains("non-fast-forward")
                || message.contains("cannot lock ref")
                || message.contains("rejected")
            {
                SinkError::CheckpointConflict(message)
            } else if message.contains("timed out")
                || message.contains("Could not connect")
                || message.contains("Failed to connect")
            {
                SinkError::Transient(message)
            } else {
                SinkError::Failed(message)
            }
        })?;
    if let Some(status) = rejected.lock().expect("push status").clone() {
        return Err(SinkError::CheckpointConflict(status));
    }
    Ok(CheckpointPublication {
        checkpoint: commit.to_owned(),
    })
}

fn fetch_remote_tip(
    repo: &Repository,
    remote: &mut git2::Remote<'_>,
    config: &GitCheckpointConfig,
    session_ref: &str,
) -> Result<Option<String>, SinkError> {
    let authorization = config.authorization.clone();
    let mut callbacks = RemoteCallbacks::new();
    if let Some(authorization) = authorization.clone() {
        callbacks.credentials(move |_url, _username, _allowed| {
            Cred::userpass_plaintext("x-access-token", &authorization)
        });
    }
    let mut fetch = FetchOptions::new();
    fetch.remote_callbacks(callbacks);
    if let Some(authorization) = &authorization {
        let header = format!("Authorization: {authorization}");
        fetch.custom_headers(&[&header]);
    }
    let cache_ref = format!("refs/rrjj/remote-cache/{}", config.session_id);
    let refspec = format!("+{session_ref}:{cache_ref}");
    match remote.fetch(&[refspec.as_str()], Some(&mut fetch), None) {
        Ok(()) => Ok(repo
            .find_reference(&cache_ref)
            .ok()
            .and_then(|reference| reference.target())
            .map(|oid| oid.to_string())),
        Err(error) => {
            let message = error.message().to_owned();
            if message.contains("not found")
                || message.contains("couldn't find remote ref")
                || message.contains("does not exist")
            {
                Ok(None)
            } else if message.contains("timed out")
                || message.contains("Could not connect")
                || message.contains("Failed to connect")
            {
                Err(SinkError::Transient(message))
            } else {
                Err(SinkError::Failed(message))
            }
        }
    }
}

fn validate_ref_prefix(prefix: &str) -> Result<(), SinkError> {
    if !prefix.starts_with("refs/") || prefix.starts_with("refs/heads") {
        return Err(SinkError::InvalidConfig(format!(
            "git ref prefix must be under refs/ and outside refs/heads: {prefix:?}"
        )));
    }
    if prefix.contains("//") || prefix.ends_with('/') {
        return Err(SinkError::InvalidConfig(format!(
            "git ref prefix must not end with '/' or contain '//': {prefix:?}"
        )));
    }
    Ok(())
}

fn validate_session_id(session_id: &str) -> Result<(), SinkError> {
    if session_id.is_empty()
        || session_id.contains('/')
        || session_id.contains('\\')
        || session_id.contains('\0')
        || session_id.contains("..")
    {
        return Err(SinkError::InvalidConfig(format!(
            "session id is not safe for a git ref name: {session_id:?}"
        )));
    }
    Ok(())
}

fn read_pushed_oid(path: &Path) -> Result<Option<String>, SinkError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            Ok(value
                .get("checkpoint")
                .and_then(|value| value.as_str())
                .map(str::to_owned))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn write_pushed_oid(path: &Path, checkpoint: &str) -> Result<(), SinkError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let value = serde_json::json!({ "checkpoint": checkpoint });
    crate::util::write_json_atomic(path, &value)?;
    Ok(())
}
