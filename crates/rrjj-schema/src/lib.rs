use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const SCHEMA_VERSION: u8 = 0;
pub const SESSION_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatMetadata {
    pub session_format: u32,
    pub schema_version: u8,
    pub rrjj_version: String,
    pub jj_lib_version: String,
    pub jj_store_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePointers {
    pub provider: String,
    pub session_uri: String,
    pub manifest_uri: String,
    pub repository_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_uri: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePointer {
    pub provider: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionManifest {
    pub session_id: String,
    pub format: FormatMetadata,
    pub last_seq: u64,
    pub last_op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_object: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durable_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durable_op: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<StoragePointers>,
}

impl SessionManifest {
    pub fn is_compatible(&self) -> bool {
        self.format.session_format == SESSION_FORMAT_VERSION
            && self.format.schema_version == SCHEMA_VERSION
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub v: u8,
    pub seq: u64,
    pub session_id: String,
    pub ts: String,
    #[serde(flatten)]
    pub body: EventBody,
}

impl Event {
    pub fn new(session_id: impl Into<String>, seq: u64, body: EventBody) -> Self {
        Self::at(session_id, seq, Utc::now(), body)
    }

    pub fn at(
        session_id: impl Into<String>,
        seq: u64,
        timestamp: DateTime<Utc>,
        body: EventBody,
    ) -> Self {
        Self {
            v: SCHEMA_VERSION,
            seq,
            session_id: session_id.into(),
            ts: timestamp.to_rfc3339_opts(SecondsFormat::Millis, true),
            body,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum EventBody {
    SessionStart(SessionStart),
    Snapshot(Snapshot),
    TouchedPaths(TouchedPaths),
    Mark(Mark),
    Flush(Flush),
    SessionEnd(SessionEnd),
    Error(ErrorEvent),
    Overflow(Overflow),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionStart {
    pub roots: Vec<String>,
    pub baseline_op: String,
    pub baseline_tree: String,
    pub ignore: Vec<String>,
    pub rrjj_version: String,
    pub jj_lib_version: String,
    pub jj_store_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub op: String,
    pub parent_op: String,
    pub commit: String,
    pub tree: String,
    pub changes: Vec<Change>,
    pub stats: ChangeStats,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub path: String,
    pub kind: ChangeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prev_blob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Removed,
    Modified,
    Renamed,
    ModeChanged,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeStats {
    pub added: u64,
    pub removed: u64,
    pub modified: u64,
    pub renamed: u64,
    pub mode_changed: u64,
}

impl ChangeStats {
    pub fn record(&mut self, kind: ChangeKind) {
        match kind {
            ChangeKind::Added => self.added += 1,
            ChangeKind::Removed => self.removed += 1,
            ChangeKind::Modified => self.modified += 1,
            ChangeKind::Renamed => self.renamed += 1,
            ChangeKind::ModeChanged => self.mode_changed += 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchedPaths {
    pub paths: Vec<TouchedPath>,
    pub raw_events: u64,
    pub window_started_at: String,
    pub window_ended_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TouchedPath {
    pub path: String,
    pub operations: Vec<TouchOperation>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TouchOperation {
    Create,
    Modify,
    Remove,
    Rename,
    Other,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Mark {
    pub label: String,
    #[serde(default)]
    pub meta: Map<String, Value>,
    pub ref_op: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flush {
    pub op: String,
    pub through_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionEnd {
    pub final_op: String,
    pub reason: String,
    pub snapshots: u64,
    pub events: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEvent {
    pub scope: String,
    pub message: String,
    pub fatal: bool,
    pub retrying: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Overflow {
    pub source: String,
    pub raw_events: u64,
    pub recovery: OverflowRecovery,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowRecovery {
    FullScanSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    #[test]
    fn serializes_normative_envelope() {
        let timestamp = Utc.with_ymd_and_hms(2026, 7, 10, 12, 30, 0).unwrap();
        let event = Event::at(
            "session",
            7,
            timestamp,
            EventBody::Mark(Mark {
                label: "step".into(),
                meta: Map::new(),
                ref_op: "op:abc".into(),
            }),
        );
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["v"], 0);
        assert_eq!(value["seq"], 7);
        assert_eq!(value["type"], "mark");
        assert_eq!(value["data"]["ref_op"], "op:abc");
        assert_eq!(value["ts"], "2026-07-10T12:30:00.000Z");
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let event: Event = serde_json::from_value(serde_json::json!({
            "v": 0, "seq": 0, "session_id": "s", "ts": "now",
            "type": "overflow",
            "data": {
                "source": "inotify", "raw_events": 3,
                "recovery": "full_scan_snapshot", "future": true
            },
            "future_envelope": true
        }))
        .unwrap();
        assert!(matches!(event.body, EventBody::Overflow(_)));
    }

    #[test]
    fn manifest_tracks_observed_and_durable_watermarks_separately() {
        let manifest = SessionManifest {
            session_id: "s".into(),
            format: FormatMetadata {
                session_format: SESSION_FORMAT_VERSION,
                schema_version: SCHEMA_VERSION,
                rrjj_version: "0.1.0".into(),
                jj_lib_version: "0.43.0".into(),
                jj_store_version: "jj-lib-0.43.0/git".into(),
            },
            last_seq: 9,
            last_op: "op:new".into(),
            events_object: Some("events/00000000000000000009.ndjson".into()),
            durable_seq: Some(7),
            durable_op: Some("op:old".into()),
            storage: Some(StoragePointers {
                provider: "s3".into(),
                session_uri: "s3://recordings/rrjj/s".into(),
                manifest_uri: "s3://recordings/rrjj/s/manifest.json".into(),
                repository_uri: "s3://recordings/rrjj/s/store/".into(),
                events_uri: Some(
                    "s3://recordings/rrjj/s/events/00000000000000000009.ndjson".into(),
                ),
            }),
        };
        assert!(manifest.is_compatible());
        let value = serde_json::to_value(&manifest).unwrap();
        assert_eq!(value["last_seq"], 9);
        assert_eq!(value["durable_seq"], 7);
        assert_ne!(value["last_op"], value["durable_op"]);
        assert_eq!(
            value["storage"]["manifest_uri"],
            "s3://recordings/rrjj/s/manifest.json"
        );
    }

    #[test]
    fn old_manifest_without_storage_remains_compatible() {
        let manifest: SessionManifest = serde_json::from_value(serde_json::json!({
            "session_id": "s",
            "format": {
                "session_format": SESSION_FORMAT_VERSION,
                "schema_version": SCHEMA_VERSION,
                "rrjj_version": "0.1.0",
                "jj_lib_version": "0.43.0",
                "jj_store_version": "jj-lib-0.43.0/git"
            },
            "last_seq": 0,
            "last_op": "op:a"
        }))
        .unwrap();
        assert!(manifest.storage.is_none());
        assert!(manifest.is_compatible());
    }
}
