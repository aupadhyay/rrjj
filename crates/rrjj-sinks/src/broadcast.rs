use std::sync::Arc;

use async_trait::async_trait;
use rrjj_schema::Event;
use tokio::sync::broadcast;

use crate::error::SinkError;
use crate::traits::{FlushRequest, Sink};

pub struct BroadcastSink {
    durable: Arc<dyn Sink>,
    events: broadcast::Sender<Event>,
}

impl BroadcastSink {
    pub fn new(durable: Arc<dyn Sink>, capacity: usize) -> (Self, broadcast::Sender<Event>) {
        let (events, _) = broadcast::channel(capacity.max(1));
        (
            Self {
                durable,
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl Sink for BroadcastSink {
    async fn emit(&self, event: &Event) -> Result<(), SinkError> {
        self.durable.emit(event).await?;
        let _ = self.events.send(event.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<(), SinkError> {
        self.durable.flush().await
    }

    async fn flush_session(&self, request: &FlushRequest) -> Result<(), SinkError> {
        self.durable.flush_session(request).await
    }
}
