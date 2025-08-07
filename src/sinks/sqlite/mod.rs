use std::convert::TryFrom;
use std::time::Duration;

use crate::codecs::{Framer, FramingConfig, TextSerializerConfig};



use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType, Transformer},
    config::{GenerateConfig, Input, SinkConfig, SinkContext},
    core::sink::VectorSink,
    event::{Event},

    sinks::util::{timezone_to_offset, StreamSink},
    template::Template,
};
pub use agent_lib::config::is_default;
use agent_lib::configurable::configurable_component;
use agent_lib::TimeZone;
use async_trait::async_trait;
use bytes::BytesMut;
use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;
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
    /* 每个文件每个表对应一个sqlite service */
    services: std::collections::HashMap<String, SqliteService>,
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
        })
    }

    fn partition_event(&mut self, event: &Event) -> Option<(String, String)> {
        let path = match self.path.render(event) {
            Ok(path) => String::from_utf8_lossy(&path).to_string(),
            Err(_) => return None,
        };
        // println!("db path {}", path);
        let table = match self.table.render(event) {
            Ok(table) => String::from_utf8_lossy(&table).to_string(),
            Err(_) => return None,
        };
        // println!("event db table {}", table);
        Some((path, table))
    }
    // 新增方法：从JSON数据中提取event type和source
    fn extract_event_info(&self, data: &str) -> (Option<String>, Option<String>) {
        match serde_json::from_str::<Value>(data) {
            Ok(json) => {
                let event_type = json
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                let source = json
                    .get("namespace")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                (event_type, source)
            }
            Err(_) => {
                // 如果不是有效的JSON，返回None
                (None, None)
            }
        }
    }
    async fn get_or_create_service(
        &mut self,
        path: &str,
        table: &str,
    ) -> crate::Result<&mut SqliteService> {
        let key = format!("{}:{}", path, table);

        if !self.services.contains_key(&key) {
            let service = SqliteService::new(path, table).await.map_err(|e| {
                crate::Error::from(format!("Failed to create SQLite service: {}", e))
            })?;
            println!("new sqlite service created for {}:{}", path, table);
            self.services.insert(key.clone(), service);
        }

        Ok(self.services.get_mut(&key).unwrap())
    }

    async fn process_event(&mut self, mut event: Event) {
        let (path, table) = match self.partition_event(&event) {
            Some(parts) => parts,
            None => {

                return;
            }
        };
        // println!("event sink to table {}:{}", path, table);
        self.transformer.transform(&mut event);

        let mut buffer = BytesMut::new();
        if let Err(error) = self.encoder.encode(event.clone(), &mut buffer) {
            error!("Failed to encode event: {}", error);

            return;
        }

        let data = String::from_utf8_lossy(&buffer).to_string();
        // println!("data is {}",data);
        // 解析JSON并提取event type
        let (event_type, source) = self.extract_event_info(&data);

        match self.get_or_create_service(&path, &table).await {
            Ok(service) => match service.write_event(&data, event_type.as_deref(), source.as_deref()).await {
                Ok(rows) => {
                    debug!("Wrote {} rows to SQLite table {} in {}", rows, table, path);

                }
                Err(error) => {
                    error!("Failed to write to SQLite: {}", error);
           
                }
            },
            Err(error) => {
                println!("Failed to get SQLite service: {}", error);
                error!("Failed to get SQLite service: {}", error);
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

#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};

    // 只引入测试里真正用到的
    use super::*;
    use crate::sinks::{
        random_metrics_with_stream, random_metrics_with_stream_timestamp, run_assert_sqlite_sink,
        temp_dir, temp_file,
    };

    #[tokio::test]
    async fn sqlite_single_partition() {
        let db_path = temp_file();
        println!("测试数据库路径: {}", db_path.display());

        let config = SqliteSinkConfig {
            path: Template::try_from(db_path.to_str().unwrap()).unwrap(),
            table: Template::try_from("events").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            timezone: Default::default(),
        };
        println!("SqliteSinkConfig initialized");
        let (input, _events) = random_metrics_with_stream(10, None, None);
        println!("random_metrics_with_stream initialized");
        run_assert_sqlite_sink(&config, input.clone().into_iter()).await;
        println!("run_assert_sqlite_sink initialized");
        // 验证数据库文件已创建
        assert!(db_path.exists(), "数据库文件应已创建");
    }

    #[tokio::test]
    async fn sqlite_many_partitions() {
        let directory = temp_dir();

        // 修改这里：使用正确的时间戳格式，而不是{}
        let format = "%Y-%m-%d-%H-%M-%S";
        let mut template = directory.to_string_lossy().to_string();
        template.push_str(&format!("/{}.db", format));

        let config = SqliteSinkConfig {
            path: template.try_into().unwrap(),
            table: Template::try_from("events").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, TextSerializerConfig::default()).into(),
            timezone: Default::default(),
        };

        let metric_count = 3;
        let timestamp = Utc::now().trunc_subsecs(3);
        let timestamp_offset = Duration::from_secs(1);

        let (input, _events) = random_metrics_with_stream_timestamp(
            metric_count,
            None,
            None,
            timestamp,
            timestamp_offset,
        );

        run_assert_sqlite_sink(&config, input.clone().into_iter()).await;
        println!("=== run_assert_sqlite_sink returned ===");
        println!(
            "=== 开始验证数据库文件，期望创建 {} 个文件 ===",
            metric_count
        );
        // 验证多个数据库文件已创建
        for index in 0..metric_count {
            let expected_timestamp = timestamp + (timestamp_offset * index as u32);
            let expected_filename = directory.join(format!(
                "{}.db",
                expected_timestamp.format("%Y-%m-%d-%H-%M-%S")
            ));
            println!(
                "检查文件 {}: {} (时间戳: {:?})",
                index + 1,
                expected_filename.display(),
                expected_timestamp
            );
            assert!(
                expected_filename.exists(),
                "数据库文件应已创建: {}",
                expected_filename.display()
            );
            println!("文件 {} 存在", index + 1);
        }
    }
}
