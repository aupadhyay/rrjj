use std::sync::Arc;

use async_trait::async_trait;
use rrjj_schema::SCHEMA_VERSION;

use crate::error::SinkError;
use crate::journal::NdjsonSink;
use crate::traits::{
    CheckpointRequest, CheckpointSink, DurableWatermark, EventPublishRequest, EventSink,
    FlushRequest, Sink,
};
use crate::util::{latest_snapshot_commit, normalize_commit_oid};

/// Coordinates local journal acceptance with checkpoint and event publication.
pub struct DurableSessionSink {
    journal: Arc<NdjsonSink>,
    checkpoint: Arc<dyn CheckpointSink>,
    events: Arc<dyn EventSink>,
    session_id: String,
}

impl DurableSessionSink {
    pub fn new(
        journal: Arc<NdjsonSink>,
        checkpoint: Arc<dyn CheckpointSink>,
        events: Arc<dyn EventSink>,
        session_id: String,
    ) -> Self {
        Self {
            journal,
            checkpoint,
            events,
            session_id,
        }
    }

    pub fn journal(&self) -> &NdjsonSink {
        &self.journal
    }
}

#[async_trait]
impl Sink for DurableSessionSink {
    async fn emit(&self, event: &rrjj_schema::Event) -> Result<(), SinkError> {
        self.journal.emit(event).await
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.journal.flush().await
    }

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        self.journal.flush().await?;
        let events = self.journal.events_through(request.last_seq).await?;
        let commit = request
            .checkpoint
            .clone()
            .or_else(|| latest_snapshot_commit(&events))
            .ok_or_else(|| {
                SinkError::InvalidFlush(
                    "flush requires a checkpoint commit OID from the recorder or a snapshot event"
                        .into(),
                )
            })?;
        let publication = self
            .checkpoint
            .publish_checkpoint(&CheckpointRequest {
                shadow_root: request.shadow_root.clone(),
                session_id: self.session_id.clone(),
                commit,
                last_seq: request.last_seq,
                last_op: request.last_op.clone(),
            })
            .await?;
        let checkpoint = normalize_commit_oid(&publication.checkpoint)?;
        let ack = self
            .events
            .publish_events(&EventPublishRequest {
                session_id: self.session_id.clone(),
                schema_version: SCHEMA_VERSION,
                events,
                durable: Some(DurableWatermark {
                    seq: request.last_seq,
                    op: request.last_op.clone(),
                    checkpoint: checkpoint.clone(),
                }),
            })
            .await?;
        if ack.accepted_through_seq < request.last_seq {
            return Err(SinkError::Failed(format!(
                "event sink acknowledged only through {}, flush required {}",
                ack.accepted_through_seq, request.last_seq
            )));
        }
        if ack
            .durable_through_seq
            .is_some_and(|seq| seq < request.last_seq)
        {
            return Err(SinkError::Failed(format!(
                "event sink durable watermark {:?} is before flush target {}",
                ack.durable_through_seq, request.last_seq
            )));
        }
        Ok(())
    }
}
