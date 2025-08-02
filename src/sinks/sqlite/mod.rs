use std::convert::TryFrom;
use std::time::{Duration};

use crate::codecs::{Framer, FramingConfig, TextSerializerConfig};
use crate::internal_event::{
    CountByteSize, EventsSent, InternalEventHandle as _, Output, Registered,
};
use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType, Transformer},
    config::{GenerateConfig, Input, SinkConfig, SinkContext},
    event::{Event, EventStatus},
    register,
    sinks::util::{timezone_to_offset, StreamSink},
    template::Template,
    core::sink::VectorSink,
};
pub use agent_lib::config::is_default;
use agent_lib::configurable::configurable_component;
use agent_lib::{

    TimeZone,
};
use async_trait::async_trait;
use bytes::{BytesMut};
use futures::stream::{BoxStream, StreamExt};
use serde_with::serde_as;
use tokio_util::codec::Encoder as _;
use tracing::{debug, error};

use crate::sinks::util::sqlite_service::SqliteService;

/// Configuration for the `sqlite` sink.
#[serde_as]
#[configurable_component(sink("sqlite", "Output observability events into SQLite database."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SqliteSinkConfig {
    /// Database file path.
    #[configurable(metadata(docs::examples = "/tmp/events.db"))]
    #[configurable(metadata(docs::examples = "./data/events.db"))]
    pub path: Template,

    /// Table name to write events to.
    #[configurable(metadata(docs::examples = "events"))]
    #[configurable(metadata(docs::examples = "logs"))]
    pub table: Template,

    /// The amount of time that a database connection can be idle and stay open.
    #[serde(default = "default_idle_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "idle_timeout_secs")]
    #[configurable(metadata(docs::examples = 600))]
    #[configurable(metadata(docs::human_name = "Idle Timeout"))]
    pub idle_timeout: Duration,

    #[serde(flatten)]
    pub encoding: EncodingConfigWithFraming,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,
}

impl GenerateConfig for SqliteSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            path: Template::try_from("/tmp/events.db").unwrap(),
            table: Template::try_from("events").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            timezone: Default::default(),
        })
        .unwrap()
    }
}

const fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}

#[async_trait::async_trait]
#[typetag::serde(name = "sqlite")]
impl SinkConfig for SqliteSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<VectorSink> {
        let sink = SqliteSink::new(self, cx)?;
        Ok(VectorSink::from_event_streamsink(sink))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type())
    }
}

pub struct SqliteSink {
    path: Template,
    table: Template,
    transformer: Transformer,
    encoder: Encoder<Framer>,
    services: std::collections::HashMap<String, SqliteService>,
    events_sent: Registered<EventsSent>,
}

impl SqliteSink {
    pub fn new(config: &SqliteSinkConfig, cx: SinkContext) -> crate::Result<Self> {
        let transformer = config.encoding.transformer();
        let (framer, serializer) = config.encoding.build(SinkType::StreamBased)?;
        let encoder = Encoder::<Framer>::new(framer, serializer);

        let offset = config
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        Ok(Self {
            path: config.path.clone().with_tz_offset(offset),
            table: config.table.clone().with_tz_offset(offset),
            transformer,
            encoder,
            services: std::collections::HashMap::new(),
            events_sent: register!(EventsSent::from(Output(None))),
        })
    }

    fn partition_event(&mut self, event: &Event) -> Option<(String, String)> {
        let path = match self.path.render(event) {
            Ok(path) => String::from_utf8_lossy(&path).to_string(),
            Err(_) => return None,
        };

        let table = match self.table.render(event) {
            Ok(table) => String::from_utf8_lossy(&table).to_string(),
            Err(_) => return None,
        };

        Some((path, table))
    }

    async fn get_or_create_service(&mut self, path: &str, table: &str) -> crate::Result<&mut SqliteService> {
        let key = format!("{}:{}", path, table);
        
        if !self.services.contains_key(&key) {
            let service = SqliteService::new(path, table)
                .await
                .map_err(|e| crate::Error::from(format!("Failed to create SQLite service: {}", e)))?;
            self.services.insert(key.clone(), service);
        }
        
        Ok(self.services.get_mut(&key).unwrap())
    }

    async fn process_event(&mut self, mut event: Event) {
        let (path, table) = match self.partition_event(&event) {
            Some(parts) => parts,
            None => {
                event.metadata().update_status(EventStatus::Errored);
                return;
            }
        };

        self.transformer.transform(&mut event);
        
        let mut buffer = BytesMut::new();
        if let Err(error) = self.encoder.encode(event.clone(), &mut buffer) {
            error!("Failed to encode event: {}", error);
            event.metadata().update_status(EventStatus::Errored);
            return;
        }

        let data = String::from_utf8_lossy(&buffer).to_string();
        let event_type = event.as_log().get("event_type").map(|v| v.to_string_lossy().to_string());
        let source = event.as_log().get("source").map(|v| v.to_string_lossy().to_string());

        match self.get_or_create_service(&path, &table).await {
            Ok(service) => {
                match service.write_event(&data, event_type.as_deref(), source.as_deref()).await {
                    Ok(rows) => {
                        self.events_sent.emit(CountByteSize(1, buffer.len().into()));
                        debug!("Wrote {} rows to SQLite table {} in {}", rows, table, path);
                        event.metadata().update_status(EventStatus::Delivered);
                    }
                    Err(error) => {
                        error!("Failed to write to SQLite: {}", error);
                        event.metadata().update_status(EventStatus::Errored);
                    }
                }
            }
            Err(error) => {
                error!("Failed to get SQLite service: {}", error);
                event.metadata().update_status(EventStatus::Errored);
            }
        }
    }

    async fn run(&mut self, mut input: BoxStream<'_, Event>) -> crate::Result<()> {
        loop {
            tokio::select! {
                event = input.next() => {
                    match event {
                        Some(event) => self.process_event(event).await,
                        None => {
                            debug!("Receiver exhausted, terminating the processing loop.");
                            
                            // Close all SQLite services
                            for (_, service) in &mut self.services {
                                if let Err(error) = service.close().await {
                                    error!("Failed to close SQLite service: {}", error);
                                }
                            }
                            
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl StreamSink<Event> for SqliteSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        SqliteSink::run(&mut self, input)
            .await
            .expect("sqlite sink error");
        Ok(())
    }
}
