use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use rrjj_schema::Event;
use serde::Serialize;

use crate::error::SinkError;
use crate::traits::{
    DurableWatermark, EventAcknowledgement, EventPublishRequest, EventSink, SinkCursor,
};
use crate::util::{redact_secrets, write_cursor_atomic};

#[derive(Clone, Debug)]
pub struct HttpEventConfig {
    pub url: String,
    pub authorization: Option<String>,
    pub max_events_per_batch: usize,
    pub max_bytes_per_batch: usize,
    pub cursor_path: PathBuf,
    pub max_retries: u32,
}

#[derive(Clone, Debug, Serialize)]
struct HttpEventBatch<'a> {
    schema_version: u8,
    session_id: &'a str,
    events: &'a [Event],
    #[serde(skip_serializing_if = "Option::is_none")]
    durable: Option<&'a DurableWatermark>,
}

pub struct HttpEventSink {
    config: HttpEventConfig,
    client: reqwest::Client,
    delivered_seq: Mutex<Option<u64>>,
}

impl HttpEventSink {
    pub fn create(config: HttpEventConfig) -> Result<Self, SinkError> {
        if config.max_events_per_batch == 0 {
            return Err(SinkError::InvalidConfig(
                "event HTTP max batch events must be greater than zero".into(),
            ));
        }
        if config.max_bytes_per_batch == 0 {
            return Err(SinkError::InvalidConfig(
                "event HTTP max batch bytes must be greater than zero".into(),
            ));
        }
        let delivered_seq = read_delivered_seq(&config.cursor_path)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| SinkError::InvalidConfig(format!("build HTTP client: {error}")))?;
        Ok(Self {
            config,
            client,
            delivered_seq: Mutex::new(delivered_seq),
        })
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

    async fn post_batch(
        &self,
        request: &EventPublishRequest,
        events: &[Event],
        durable: Option<&DurableWatermark>,
    ) -> Result<EventAcknowledgement, SinkError> {
        let body = HttpEventBatch {
            schema_version: request.schema_version,
            session_id: &request.session_id,
            events,
            durable,
        };
        let mut attempt = 0_u32;
        let mut backoff = Duration::from_millis(50);
        loop {
            attempt += 1;
            let mut builder = self.client.post(&self.config.url).json(&body);
            if let Some(authorization) = &self.config.authorization {
                builder = builder.header(reqwest::header::AUTHORIZATION, authorization);
            }
            let response = match builder.send().await {
                Ok(response) => response,
                Err(error) => {
                    if attempt > self.config.max_retries {
                        return Err(SinkError::Transient(self.sanitized(error.to_string())));
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
                    continue;
                }
            };
            let status = response.status();
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            let text = response
                .text()
                .await
                .map_err(|error| SinkError::Transient(self.sanitized(error.to_string())))?;
            match classify_http_status(status) {
                HttpClass::Success => {
                    let ack: EventAcknowledgement =
                        serde_json::from_str(&text).map_err(|error| {
                            SinkError::Failed(self.sanitized(format!(
                                "HTTP event sink returned malformed acknowledgement: {error}"
                            )))
                        })?;
                    validate_ack(&ack, events, durable)?;
                    return Ok(ack);
                }
                HttpClass::Conflict => {
                    let ack: EventAcknowledgement =
                        serde_json::from_str(&text).map_err(|error| {
                            SinkError::EventConflict(self.sanitized(format!(
                                "HTTP 409 without identical acknowledgement: {error}; body redacted"
                            )))
                        })?;
                    validate_ack(&ack, events, durable).map_err(|error| {
                        SinkError::EventConflict(self.sanitized(error.to_string()))
                    })?;
                    return Ok(ack);
                }
                HttpClass::Retryable => {
                    if attempt > self.config.max_retries {
                        return Err(SinkError::Transient(
                            self.sanitized(format!("HTTP event sink failed with {status}")),
                        ));
                    }
                    tokio::time::sleep(retry_after.unwrap_or(backoff)).await;
                    backoff = std::cmp::min(backoff * 2, Duration::from_secs(5));
                }
                HttpClass::Permanent => {
                    return Err(SinkError::Failed(self.sanitized(format!(
                        "HTTP event sink permanently rejected batch with {status}"
                    ))));
                }
            }
        }
    }
}

#[async_trait]
impl EventSink for HttpEventSink {
    async fn publish_events(
        &self,
        request: &EventPublishRequest,
    ) -> Result<EventAcknowledgement, SinkError> {
        if request.events.is_empty() {
            return Err(SinkError::InvalidFlush(
                "HTTP event sink requires a contiguous non-empty event batch".into(),
            ));
        }
        validate_contiguous(&request.events)?;
        let already = *self.delivered_seq.lock().expect("http cursor");
        let mut start = 0usize;
        if let Some(delivered) = already {
            while start < request.events.len() && request.events[start].seq <= delivered {
                start += 1;
            }
            if start == request.events.len() {
                return Ok(EventAcknowledgement {
                    accepted_through_seq: delivered,
                    durable_through_seq: request.durable.as_ref().map(|value| value.seq),
                });
            }
            if request.events[start].seq != delivered + 1 {
                return Err(SinkError::EventConflict(format!(
                    "HTTP event sink sequence gap after delivered {delivered}: next is {}",
                    request.events[start].seq
                )));
            }
        } else if request.events[0].seq != 0 {
            return Err(SinkError::EventConflict(format!(
                "HTTP event sink requires batches to start at seq 0, got {}",
                request.events[0].seq
            )));
        }

        let mut offset = start;
        let mut last_ack = EventAcknowledgement {
            accepted_through_seq: already.unwrap_or(0).saturating_sub(1),
            durable_through_seq: None,
        };
        while offset < request.events.len() {
            let remaining = &request.events[offset..];
            let batch = take_batch(
                remaining,
                self.config.max_events_per_batch,
                self.config.max_bytes_per_batch,
            )?;
            let end = offset + batch.len();
            let is_final = end == request.events.len();
            let durable = if is_final {
                request.durable.as_ref()
            } else {
                None
            };
            last_ack = self.post_batch(request, batch, durable).await?;
            *self.delivered_seq.lock().expect("http cursor") = Some(last_ack.accepted_through_seq);
            write_cursor_atomic(
                &self.config.cursor_path,
                &SinkCursor {
                    delivered_seq: Some(last_ack.accepted_through_seq),
                    delivered_op: None,
                    checkpoint: request
                        .durable
                        .as_ref()
                        .map(|value| value.checkpoint.clone()),
                },
            )?;
            offset = end;
        }
        Ok(last_ack)
    }
}

#[derive(Clone, Copy)]
enum HttpClass {
    Success,
    Conflict,
    Retryable,
    Permanent,
}

fn classify_http_status(status: StatusCode) -> HttpClass {
    if status.is_success() {
        HttpClass::Success
    } else if status == StatusCode::CONFLICT {
        HttpClass::Conflict
    } else if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        HttpClass::Retryable
    } else {
        HttpClass::Permanent
    }
}

fn validate_contiguous(events: &[Event]) -> Result<(), SinkError> {
    for window in events.windows(2) {
        if window[1].seq != window[0].seq + 1 {
            return Err(SinkError::EventConflict(format!(
                "HTTP event batch is not contiguous: {} then {}",
                window[0].seq, window[1].seq
            )));
        }
    }
    Ok(())
}

fn validate_ack(
    ack: &EventAcknowledgement,
    events: &[Event],
    durable: Option<&DurableWatermark>,
) -> Result<(), SinkError> {
    let last = events.last().expect("non-empty batch").seq;
    if ack.accepted_through_seq < last {
        return Err(SinkError::Failed(format!(
            "HTTP acknowledgement accepted_through_seq {} is before batch end {last}",
            ack.accepted_through_seq
        )));
    }
    if let Some(durable) = durable {
        match ack.durable_through_seq {
            Some(seq) if seq >= durable.seq => Ok(()),
            Some(seq) => Err(SinkError::Failed(format!(
                "HTTP acknowledgement durable_through_seq {seq} is before requested {}",
                durable.seq
            ))),
            None => Err(SinkError::Failed(
                "HTTP acknowledgement missing durable_through_seq for final durable batch".into(),
            )),
        }
    } else {
        Ok(())
    }
}

fn take_batch(
    events: &[Event],
    max_events: usize,
    max_bytes: usize,
) -> Result<&[Event], SinkError> {
    let mut bytes = 2usize; // []
    let mut count = 0usize;
    for event in events.iter().take(max_events) {
        let encoded = serde_json::to_vec(event)?;
        let extra = encoded.len() + if count == 0 { 0 } else { 1 };
        if count > 0 && bytes + extra > max_bytes {
            break;
        }
        if count == 0 && encoded.len() + 2 > max_bytes {
            return Err(SinkError::InvalidFlush(format!(
                "single event at seq {} exceeds HTTP batch byte limit {max_bytes}",
                event.seq
            )));
        }
        bytes += extra;
        count += 1;
    }
    Ok(&events[..count.max(1).min(events.len())])
}

fn read_delivered_seq(path: &PathBuf) -> Result<Option<u64>, SinkError> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let cursor: SinkCursor = serde_json::from_slice(&bytes)?;
            Ok(cursor.delivered_seq)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
