use std::path::PathBuf;

use async_trait::async_trait;
use rrjj_schema::Event;
use serde::{Deserialize, Serialize};

use crate::error::SinkError;

#[derive(Clone, Debug)]
pub struct FlushRequest {
    pub shadow_root: PathBuf,
    pub last_seq: u64,
    pub last_op: String,
    /// Optional checkpoint identity (`c:<oid>` or bare Git OID hex).
    pub checkpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkCursor {
    pub delivered_seq: Option<u64>,
    pub delivered_op: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CheckpointRequest {
    pub shadow_root: PathBuf,
    pub session_id: String,
    pub commit: String,
    pub last_seq: u64,
    pub last_op: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointPublication {
    /// Canonical Git commit OID hex (no `c:` prefix).
    pub checkpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableWatermark {
    pub seq: u64,
    pub op: String,
    pub checkpoint: String,
}

#[derive(Clone, Debug)]
pub struct EventPublishRequest {
    pub session_id: String,
    pub schema_version: u8,
    pub events: Vec<Event>,
    pub durable: Option<DurableWatermark>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventAcknowledgement {
    pub accepted_through_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_through_seq: Option<u64>,
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

#[async_trait]
pub trait CheckpointSink: Send + Sync {
    async fn publish_checkpoint(
        &self,
        request: &CheckpointRequest,
    ) -> Result<CheckpointPublication, SinkError>;
}

#[async_trait]
pub trait EventSink: Send + Sync {
    async fn publish_events(
        &self,
        request: &EventPublishRequest,
    ) -> Result<EventAcknowledgement, SinkError>;
}
