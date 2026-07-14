mod broadcast;
mod durable;
mod error;
mod git;
mod http;
mod journal;
mod local;
mod traits;
mod util;

pub use broadcast::BroadcastSink;
pub use durable::DurableSessionSink;
pub use error::SinkError;
pub use git::{GitCheckpointConfig, GitCheckpointSink};
pub use http::{HttpEventConfig, HttpEventSink};
pub use journal::NdjsonSink;
pub use local::{DirectorySessionSink, DirectorySyncStats};
pub use traits::{
    CheckpointPublication, CheckpointRequest, CheckpointSink, DurableWatermark,
    EventAcknowledgement, EventPublishRequest, EventSink, FlushRequest, Sink, SinkCursor,
};
pub use util::{latest_snapshot_commit, normalize_commit_oid, parse_event_spool};

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use rrjj_schema::{
        Event, EventBody, FormatMetadata, Overflow, OverflowRecovery, SCHEMA_VERSION,
        SESSION_FORMAT_VERSION, SessionManifest,
    };
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;

    use super::*;
    use crate::traits::{CheckpointPublication, CheckpointRequest, CheckpointSink};
    use crate::util::write_json_atomic;

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
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn syncs_spool_repository_and_advances_manifest_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let shadow = temp.path().join("shadow");
        std::fs::create_dir_all(shadow.join("repo/store")).unwrap();
        std::fs::create_dir_all(shadow.join("working-copy-000")).unwrap();
        std::fs::write(shadow.join("repo/store/object"), "jj state").unwrap();
        std::fs::write(shadow.join("working-copy-000/tree_state"), "not published").unwrap();
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
                std::fs::metadata(temp.path().join("spool.ndjson"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(temp.path().join("session"))
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
            checkpoint: Some("c:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
        })
        .await
        .unwrap();

        let manifest: SessionManifest = serde_json::from_slice(
            &std::fs::read(temp.path().join("session/manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(manifest.durable_seq, Some(0));
        assert_eq!(manifest.durable_op.as_deref(), Some("op:abc"));
        assert_eq!(
            std::fs::read_to_string(temp.path().join("session/store/repo/store/object")).unwrap(),
            "jj state"
        );
        assert!(!temp.path().join("session/store/working-copy-000").exists());
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
        assert_eq!(
            std::fs::read_to_string(temp.path())
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn durable_session_publishes_checkpoint_before_events_and_watermark() {
        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            NdjsonSink::create_for_session(
                temp.path().join("spool.ndjson"),
                10_000,
                Some("s".into()),
                Some(SCHEMA_VERSION),
            )
            .await
            .unwrap(),
        );
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let checkpoint = Arc::new(RecordingCheckpoint {
            order: order.clone(),
            oid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        });
        let events = Arc::new(RecordingEvents {
            order: order.clone(),
            fail_once: Mutex::new(false),
        });
        let sink = DurableSessionSink::new(journal.clone(), checkpoint, events, "s".into());
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
            shadow_root: temp.path().join("shadow"),
            last_seq: 0,
            last_op: "op:a".into(),
            checkpoint: Some("c:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
        })
        .await
        .unwrap();
        assert_eq!(
            *order.lock().unwrap(),
            vec!["checkpoint".to_owned(), "events".to_owned()]
        );
    }

    #[tokio::test]
    async fn durable_flush_does_not_advance_when_events_fail_after_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let journal = Arc::new(
            NdjsonSink::create_for_session(
                temp.path().join("spool.ndjson"),
                10_000,
                Some("s".into()),
                Some(SCHEMA_VERSION),
            )
            .await
            .unwrap(),
        );
        let order = Arc::new(Mutex::new(Vec::<String>::new()));
        let checkpoint = Arc::new(RecordingCheckpoint {
            order: order.clone(),
            oid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
        });
        let events = Arc::new(RecordingEvents {
            order: order.clone(),
            fail_once: Mutex::new(true),
        });
        let sink = DurableSessionSink::new(journal, checkpoint, events, "s".into());
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
        let error = sink
            .flush_session(&FlushRequest {
                shadow_root: temp.path().join("shadow"),
                last_seq: 0,
                last_op: "op:a".into(),
                checkpoint: Some("c:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into()),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, SinkError::Failed(_)));
        assert_eq!(
            *order.lock().unwrap(),
            vec!["checkpoint".to_owned(), "events".to_owned()]
        );
    }

    #[tokio::test]
    async fn http_event_sink_batches_retries_and_acknowledges_duplicates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::<String>::new()));
        let captured = seen.clone();
        let server = tokio::spawn(async move {
            for index in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 64 * 1024];
                let read = stream.read(&mut bytes).await.unwrap();
                let request = String::from_utf8_lossy(&bytes[..read]).into_owned();
                captured.lock().unwrap().push(request.clone());
                assert!(
                    request
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .starts_with("POST "),
                    "expected HTTP POST"
                );
                assert!(
                    !request.contains("\"authorization\""),
                    "authorization must not be copied into JSON body"
                );
                let status = if index == 0 {
                    "HTTP/1.1 500 Internal Server Error"
                } else {
                    "HTTP/1.1 200 OK"
                };
                let body = r#"{"accepted_through_seq":0,"durable_through_seq":0}"#;
                stream
                    .write_all(
                        format!(
                            "{status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        let temp = tempfile::tempdir().unwrap();
        let sink = HttpEventSink::create(HttpEventConfig {
            url: format!("http://{address}/events"),
            authorization: Some("Bearer secret-token".into()),
            max_events_per_batch: 10,
            max_bytes_per_batch: 10_000,
            cursor_path: temp.path().join("http-cursor.json"),
            max_retries: 3,
        })
        .unwrap();
        let event = Event::new(
            "s",
            0,
            EventBody::Overflow(Overflow {
                source: "test".into(),
                raw_events: 1,
                recovery: OverflowRecovery::FullScanSnapshot,
            }),
        );
        let ack = sink
            .publish_events(&EventPublishRequest {
                session_id: "s".into(),
                schema_version: SCHEMA_VERSION,
                events: vec![event.clone()],
                durable: Some(DurableWatermark {
                    seq: 0,
                    op: "op:a".into(),
                    checkpoint: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                }),
            })
            .await
            .unwrap();
        assert_eq!(ack.accepted_through_seq, 0);
        // Replay is success.
        let ack = sink
            .publish_events(&EventPublishRequest {
                session_id: "s".into(),
                schema_version: SCHEMA_VERSION,
                events: vec![event],
                durable: Some(DurableWatermark {
                    seq: 0,
                    op: "op:a".into(),
                    checkpoint: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
                }),
            })
            .await
            .unwrap();
        assert_eq!(ack.accepted_through_seq, 0);
        server.await.unwrap();
        assert!(seen.lock().unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn git_checkpoint_pushes_to_bare_remote_under_recorder_ref() {
        let temp = tempfile::tempdir().unwrap();
        let shadow_git = temp.path().join("shadow/repo/store/git");
        let remote = temp.path().join("remote.git");
        std::fs::create_dir_all(&shadow_git).unwrap();
        let source = git2::Repository::init_bare(&shadow_git).unwrap();
        let oid = write_test_commit(&source);
        git2::Repository::init_bare(&remote).unwrap();

        let sink = GitCheckpointSink::create(GitCheckpointConfig {
            remote_url: remote.to_string_lossy().into_owned(),
            authorization: None,
            ref_prefix: "refs/rrjj/sessions".into(),
            session_id: "session-1".into(),
            cursor_path: temp.path().join("git-cursor.json"),
        })
        .unwrap();
        let publication = sink
            .publish_checkpoint(&CheckpointRequest {
                shadow_root: temp.path().join("shadow"),
                session_id: "session-1".into(),
                commit: format!("c:{oid}"),
                last_seq: 0,
                last_op: "op:a".into(),
            })
            .await
            .unwrap();
        assert_eq!(publication.checkpoint, oid.to_string());

        let remote_repo = git2::Repository::open_bare(&remote).unwrap();
        let reference = remote_repo
            .find_reference("refs/rrjj/sessions/session-1")
            .unwrap();
        assert_eq!(reference.target().unwrap(), oid);
        assert!(remote_repo.find_reference("refs/heads/main").is_err());

        // Idempotent retry.
        let again = sink
            .publish_checkpoint(&CheckpointRequest {
                shadow_root: temp.path().join("shadow"),
                session_id: "session-1".into(),
                commit: format!("c:{oid}"),
                last_seq: 0,
                last_op: "op:a".into(),
            })
            .await
            .unwrap();
        assert_eq!(again.checkpoint, oid.to_string());
    }

    #[tokio::test]
    async fn git_checkpoint_rejects_cas_conflict() {
        let temp = tempfile::tempdir().unwrap();
        let shadow_git = temp.path().join("shadow/repo/store/git");
        let remote = temp.path().join("remote.git");
        std::fs::create_dir_all(&shadow_git).unwrap();
        let source = git2::Repository::init_bare(&shadow_git).unwrap();
        let first = write_test_commit(&source);
        let second = write_child_commit(&source, first);
        let _ = second;
        git2::Repository::init_bare(&remote).unwrap();
        let mut push_remote = source.remote_anonymous(remote.to_str().unwrap()).unwrap();
        push_remote
            .push(
                &[
                    "refs/rrjj/internal/seed:refs/rrjj/internal/seed",
                    "refs/rrjj/internal/child:refs/rrjj/sessions/session-1",
                ],
                None,
            )
            .unwrap();
        write_json_atomic(
            &temp.path().join("git-cursor.json"),
            &serde_json::json!({ "checkpoint": first.to_string() }),
        )
        .unwrap();

        let sink = GitCheckpointSink::create(GitCheckpointConfig {
            remote_url: remote.to_string_lossy().into_owned(),
            authorization: None,
            ref_prefix: "refs/rrjj/sessions".into(),
            session_id: "session-1".into(),
            cursor_path: temp.path().join("git-cursor.json"),
        })
        .unwrap();
        let error = sink
            .publish_checkpoint(&CheckpointRequest {
                shadow_root: temp.path().join("shadow"),
                session_id: "session-1".into(),
                commit: format!("c:{first}"),
                last_seq: 0,
                last_op: "op:a".into(),
            })
            .await
            .unwrap_err();
        assert!(matches!(error, SinkError::CheckpointConflict(_)), "{error}");
    }

    #[test]
    fn secrets_are_redacted_from_messages() {
        let message = crate::util::redact_secrets(
            "Authorization: Bearer super-secret failed",
            &["super-secret"],
        );
        assert!(!message.contains("super-secret"));
        assert!(message.contains("***"));
    }

    struct RecordingCheckpoint {
        order: Arc<Mutex<Vec<String>>>,
        oid: String,
    }

    #[async_trait]
    impl CheckpointSink for RecordingCheckpoint {
        async fn publish_checkpoint(
            &self,
            _request: &CheckpointRequest,
        ) -> Result<CheckpointPublication, SinkError> {
            self.order.lock().unwrap().push("checkpoint".into());
            Ok(CheckpointPublication {
                checkpoint: self.oid.clone(),
            })
        }
    }

    struct RecordingEvents {
        order: Arc<Mutex<Vec<String>>>,
        fail_once: Mutex<bool>,
    }

    #[async_trait]
    impl EventSink for RecordingEvents {
        async fn publish_events(
            &self,
            request: &EventPublishRequest,
        ) -> Result<EventAcknowledgement, SinkError> {
            self.order.lock().unwrap().push("events".into());
            if *self.fail_once.lock().unwrap() {
                *self.fail_once.lock().unwrap() = false;
                return Err(SinkError::Failed("injected event failure".into()));
            }
            Ok(EventAcknowledgement {
                accepted_through_seq: request.events.last().map(|event| event.seq).unwrap_or(0),
                durable_through_seq: request.durable.as_ref().map(|value| value.seq),
            })
        }
    }

    fn write_test_commit(repo: &git2::Repository) -> git2::Oid {
        let mut index = git2::Index::new().unwrap();
        let tree_id = index.write_tree_to(repo).unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let signature = git2::Signature::now("rrjj", "rrjj@example.com").unwrap();
        repo.commit(
            Some("refs/rrjj/internal/seed"),
            &signature,
            &signature,
            "seed",
            &tree,
            &[],
        )
        .unwrap()
    }

    fn write_child_commit(repo: &git2::Repository, parent: git2::Oid) -> git2::Oid {
        let parent_commit = repo.find_commit(parent).unwrap();
        let tree = parent_commit.tree().unwrap();
        let signature = git2::Signature::now("rrjj", "rrjj@example.com").unwrap();
        repo.commit(
            Some("refs/rrjj/internal/child"),
            &signature,
            &signature,
            "child",
            &tree,
            &[&parent_commit],
        )
        .unwrap()
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
