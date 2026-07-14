use std::path::PathBuf;

use thiserror::Error;

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
    #[error("invalid sink configuration: {0}")]
    InvalidConfig(String),
    #[error("checkpoint conflict: {0}")]
    CheckpointConflict(String),
    #[error("event publication conflict: {0}")]
    EventConflict(String),
    #[error("transient sink failure: {0}")]
    Transient(String),
}
