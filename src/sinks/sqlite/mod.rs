use std::convert::TryFrom;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::stream::{BoxStream, StreamExt};
use serde_with::serde_as;
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite, Row};
use tokio::sync::Mutex;
use vector_lib::codecs::{
    encoding::{Framer, FramingConfig},
    TextSerializerConfig,
};
use vector_lib::configurable::configurable_component;
use vector_lib::{
    internal_event::{CountByteSize, EventsSent, InternalEventHandle as _, Output, Registered},
    EstimatedJsonEncodedSizeOf, TimeZone,
};

use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType, Transformer},
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    event::{Event, EventStatus, Finalizable, Value},
    expiring_hash_map::ExpiringHashMap,
    internal_events::{
        TemplateRenderingError,
    },
    sinks::util::{timezone_to_offset, StreamSink},
    template::Template,
};

mod bytes_path;
use bytes_path::BytesPath;

/// Configuration for the `sqlite` sink.
#[serde_as]
#[configurable_component(sink("sqlite", "Deliver log data to a SQLite database."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct SqliteSinkConfig {
    /// SQLite database file path.
    #[configurable(metadata(docs::examples = "/tmp/vector.db"))]
    #[configurable(metadata(docs::examples = "./data/logs.db"))]
    pub path: Template,

    /// Table name to write events to.
    #[configurable(metadata(docs::examples = "logs"))]
    #[configurable(metadata(docs::examples = "events"))]
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
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,
}

impl GenerateConfig for SqliteSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            path: Template::try_from("/tmp/vector.db").unwrap(),
            table: Template::try_from("logs").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            acknowledgements: Default::default(),
            timezone: Default::default(),
        })
        .unwrap()
    }
}

const fn default_idle_timeout() -> Duration {
    Duration::from_secs(300)
}

#[async_trait::async_trait]
#[typetag::serde(name = "sqlite")]
impl SinkConfig for SqliteSinkConfig {
    async fn build(
        &self,
        cx: SinkContext,
    ) -> crate::Result<super::VectorSink> {
        let sink = SqliteSink::new(self, cx)?;
        Ok(super::VectorSink::from_event_streamsink(sink))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type())
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

struct DatabaseConnection {
    pool: Pool<Sqlite>,
    table_name: String,
}

pub struct SqliteSink {
    path: Template,
    table: Template,
    transformer: Transformer,
    encoder: Encoder<Framer>,
    idle_timeout: Duration,
    connections: ExpiringHashMap<Bytes, Arc<Mutex<DatabaseConnection>>>,
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
            idle_timeout: config.idle_timeout,
            connections: ExpiringHashMap::default(),
            events_sent: register!(EventsSent::from(Output(None))),
        })
    }

    fn partition_event(&mut self, event: &Event) -> Option<(Bytes, Bytes)> {
        let path_bytes = match self.path.render(event) {
            Ok(b) => b,
            Err(error) => {
                emit!(TemplateRenderingError {
                    error,
                    field: Some("path"),
                    drop_event: true,
                });
                return None;
            }
        };

        let table_bytes = match self.table.render(event) {
            Ok(b) => b,
            Err(error) => {
                emit!(TemplateRenderingError {
                    error,
                    field: Some("table"),
                    drop_event: true,
                });
                return None;
            }
        };

        Some((path_bytes, table_bytes))
    }

    async fn run(&mut self, mut input: BoxStream<'_, Event>) -> crate::Result<()> {
        loop {
            tokio::select! {
                event = input.next() => {
                    match event {
                        Some(event) => self.process_event(event).await,
                        None => {
                            debug!(message = "Receiver exhausted, terminating the processing loop.");
                            break;
                        }
                    }
                }
                result = self.connections.next_expired(), if !self.connections.is_empty() => {
                    match result {
                        None => unreachable!(),
                        Some((_, _)) => {
                            // Connection expired and was automatically removed
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn process_event(&mut self, mut event: Event) {
        let (path_bytes, table_bytes) = match self.partition_event(&event) {
            Some(parts) => parts,
            None => {
                event.metadata().update_status(EventStatus::Errored);
                return;
            }
        };

        let next_deadline = tokio::time::Instant::now() + self.idle_timeout;
        
        let connection_key = {
            let mut combined = BytesMut::with_capacity(path_bytes.len() + table_bytes.len() + 1);
            combined.extend_from_slice(&path_bytes);
            combined.extend_from_slice(b"|");
            combined.extend_from_slice(&table_bytes);
            combined.freeze()
        };

        let conn_arc = if let Some(conn) = self.connections.reset_at(&connection_key, next_deadline) {
            conn
        } else {
            let path_str = String::from_utf8_lossy(&path_bytes);
            let table_str = String::from_utf8_lossy(&table_bytes);
            
            match self.create_connection(&path_str, &table_str).await {
                Ok(conn) => {
                    let conn_arc = Arc::new(Mutex::new(conn));
                    self.connections.insert_at(connection_key.clone(), conn_arc.clone(), next_deadline);
                    conn_arc
                }
                Err(error) => {
                    error!(message = "Failed to create SQLite connection", %error);
                    event.metadata().update_status(EventStatus::Errored);
                    return;
                }
            }
        };

        let event_size = event.estimated_json_encoded_size_of();
        let finalizers = event.take_finalizers();
        
        match self.write_event_to_sqlite(&conn_arc, event).await {
            Ok(_) => {
                finalizers.update_status(EventStatus::Delivered);
                self.events_sent.emit(CountByteSize(1, event_size));
            }
            Err(error) => {
                error!(message = "Failed to write event to SQLite", %error);
                finalizers.update_status(EventStatus::Errored);
            }
        }
    }

    async fn create_connection(&self, db_path: &str, table_name: &str) -> crate::Result<DatabaseConnection> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&format!("sqlite:{}", db_path))
            .await
            .map_err(|e| crate::Error::from(e))?;

        // Create table if not exists
        let create_table_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                message TEXT,
                level TEXT,
                fields TEXT
            )
            "#,
            table_name
        );

        sqlx::query(&create_table_sql)
            .execute(&pool)
            .await
            .map_err(|e| crate::Error::from(e))?;

        Ok(DatabaseConnection {
            pool,
            table_name: table_name.to_string(),
        })
    }

    async fn write_event_to_sqlite(
        &self,
        conn: &Arc<Mutex<DatabaseConnection>>,
        mut event: Event,
    ) -> crate::Result<()> {
        self.transformer.transform(&mut event);
        
        let mut buffer = BytesMut::new();
        self.encoder
            .encode(event.clone(), &mut buffer)
            .map_err(|e| crate::Error::from(e))?;

        let message = String::from_utf8_lossy(&buffer).to_string();
        let level = event
            .as_log()
            .get("level")
            .map(|v| v.to_string_lossy())
            .unwrap_or_else(|| "info".to_string());
        
        let fields = serde_json::to_string(event.as_log().all_fields())
            .unwrap_or_else(|_| "{}".to_string());

        let conn = conn.lock().await;
        let insert_sql = format!(
            "INSERT INTO {} (message, level, fields) VALUES (?, ?, ?)",
            conn.table_name
        );

        sqlx::query(&insert_sql)
            .bind(message)
            .bind(level)
            .bind(fields)
            .execute(&conn.pool)
            .await
            .map_err(|e| crate::Error::from(e))?;

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
