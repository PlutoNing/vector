use std::convert::TryFrom;
use std::time::{Duration, Instant};

use crate::codecs::{Framer, FramingConfig, JsonSerializerConfig};

use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType},
    config::{GenerateConfig, Input, SinkConfig, SinkContext},
    core::sink::VectorSink,
    event::{Event},
    sinks::util::{expiring_hash_map::ExpiringHashMap, timezone_to_offset, StreamSink},
    template::Template,
};
pub use agent_lib::config::is_default;
use agent_lib::configurable::configurable_component;
use agent_lib::TimeZone;
use async_compression::tokio::write::GzipEncoder;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use serde_with::serde_as;
use tokio::{
    fs::{self, File},
    io::AsyncWriteExt,
};
use tokio_util::codec::Encoder as _;
use tracing::{debug, error, trace};

use std::path::Path;

#[derive(Debug, Clone)]
pub struct BytesPath {
    #[cfg(unix)]
    path: Bytes,
}

impl BytesPath {
    #[cfg(unix)]
    pub const fn new(path: Bytes) -> Self {
        Self { path }
    }
}

impl AsRef<Path> for BytesPath {
    #[cfg(unix)]
    fn as_ref(&self) -> &Path {
        use std::os::unix::ffi::OsStrExt;
        let os_str = std::ffi::OsStr::from_bytes(&self.path);
        Path::new(os_str)
    }
}
const fn default_max_file_size_bytes() -> u64 {
    104_857_600 // 100MB
}
/* 输出到文件的时候, 走到这里构建file sink config */
/// Configuration for the `file` sink.
#[serde_as]
#[configurable_component(sink("file", "Output observability events into files."))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    /// File path to write events to.
    ///
    /// Compression format extension must be explicit.
    #[configurable(metadata(docs::examples = "/tmp/vector-%Y-%m-%d.log"))]
    #[configurable(metadata(
        docs::examples = "/tmp/application-{{ application_id }}-%Y-%m-%d.log"
    ))]
    #[configurable(metadata(docs::examples = "/tmp/vector-%Y-%m-%d.log.zst"))]
    pub path: Template,

    /// The amount of time that a file can be idle and stay open.
    ///
    /// After not receiving any events in this amount of time, the file is flushed and closed.
    #[serde(default = "default_idle_timeout")]
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(rename = "idle_timeout_secs")]
    #[configurable(metadata(docs::examples = 600))]
    #[configurable(metadata(docs::human_name = "Idle Timeout"))]
    pub idle_timeout: Duration,
    /* file sink这个是必须的吗 */
    #[serde(flatten)]
    pub encoding: EncodingConfigWithFraming,

    /* yaml定义的压缩方式 */
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "is_default")]
    pub compression: Compression,

    #[configurable(derived)]
    #[serde(default)]
    pub timezone: Option<TimeZone>,

    /// Maximum file size in bytes before rotation.
    #[serde(default = "default_max_file_size_bytes")]
    #[configurable(metadata(docs::examples = 104857600))] // 100MB
    pub max_file_size_bytes: u64,
}
/* 生成默认配置 */
impl GenerateConfig for FileSinkConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            path: Template::try_from("/tmp/vector-%Y-%m-%d.log").unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, JsonSerializerConfig::default()).into(),
            compression: Default::default(),
            timezone: Default::default(),
            max_file_size_bytes: default_max_file_size_bytes(),
        })
        .unwrap()
    }
}

const fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}

/// 压缩方式
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    /// [Gzip][gzip] compression.
    Gzip,

    /// No compression.
    #[default]
    None,
}
/*
一个输出到文件的wrapper
内部是个文件
或者是个gzip encoder */
enum OutFile {
    Regular(File),
    Gzip(GzipEncoder<File>),
}

impl OutFile {
    fn new(file: File, compression: Compression) -> Self {
        match compression {
            Compression::None => OutFile::Regular(file),
            Compression::Gzip => OutFile::Gzip(GzipEncoder::new(file)),
        }
    }

    async fn sync_all(&mut self) -> Result<(), std::io::Error> {
        match self {
            OutFile::Regular(file) => file.sync_all().await,
            OutFile::Gzip(gzip) => gzip.get_mut().sync_all().await,
        }
    }

    async fn shutdown(&mut self) -> Result<(), std::io::Error> {
        match self {
            OutFile::Regular(file) => file.shutdown().await,
            OutFile::Gzip(gzip) => gzip.shutdown().await,
        }
    }

    /* 把src写入到outfile */
    async fn write_all(&mut self, src: &[u8]) -> Result<(), std::io::Error> {
        match self {
            OutFile::Regular(file) => file.write_all(src).await,
            OutFile::Gzip(gzip) => gzip.write_all(src).await,
        }
    }

    /// Shutdowns by flushing data, writing headers, and syncing all of that
    /// data and metadata to the filesystem.
    async fn close(&mut self) -> Result<(), std::io::Error> {
        self.shutdown().await?;
        self.sync_all().await
    }
}
/* 输出到文件的时候走到这里 */
#[async_trait::async_trait]
#[typetag::serde(name = "file")]
impl SinkConfig for FileSinkConfig {
    async fn build(&self, cx: SinkContext) -> crate::Result<VectorSink> {
        let sink = FileSink::new(self, cx)?; /* 这里构建filesink的各种成员实例 */
        Ok(
            VectorSink::from_event_streamsink(sink), /* 从FileSink到vectorSink */
        )
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type())
    }
}
/* 一个filesink的实现 */
pub struct FileSink {
    /// 文件路径模板,定义了输出文件的模式,如%Y-%m-%d和{{field}}
    path: Template,
    /// 编码器，将事件序列化为字节流（支持JSON、文本等格式）
    encoder: Encoder<Framer>,
    /// 文件空闲超时时间，超过此时间未收到事件则关闭文件?
    idle_timeout: Duration,
    /// 文件句柄缓存，按路径缓存打开的文件，自动过期清理
    files: ExpiringHashMap<Bytes, OutFile>,
    /// 压缩方式（None/Gzip），决定文件写入方式
    compression: Compression,
}

impl FileSink {
    /* 根据config新建一个file sink, 定义内部的encoder, transformer之类的成员 */
    pub fn new(config: &FileSinkConfig, cx: SinkContext) -> crate::Result<Self> {
        let (framer, serializer) = config.encoding.build(SinkType::StreamBased)?;
        let encoder = Encoder::<Framer>::new(framer, serializer);

        let offset = config
            .timezone
            .or(cx.globals.timezone)
            .and_then(timezone_to_offset);

        Ok(Self {
            path: config.path.clone().with_tz_offset(offset),
            encoder,
            idle_timeout: config.idle_timeout,
            files: ExpiringHashMap::default(),
            compression: config.compression,
        })
    }

    /// Uses pass the `event` to `self.path` template to obtain the file path
    /// to store the event as.
    fn partition_event(&mut self, event: &Event) -> Option<bytes::Bytes> {
        // 1. 原始模板字符串
        let _template_str = self.path.get_ref();
        // info!("原始模板: {}", template_str);
        // println!("原始模板: {}", template_str);
        // 3. 获取模板中的字段引用
        let _fields = self.path.get_fields();
        // info!("模板字段: {:?}", fields);
        // println!("模板字段: {:?}", fields);
        // 4. 渲染模板
        let render_result = self.path.render(event);

        match render_result {
            Ok(rendered_path) => {
                // 5. 成功渲染的路径
                let _path_str = String::from_utf8_lossy(&rendered_path);
                // info!("渲染成功: {}", path_str);
                // info!("渲染结果长度: {} bytes", rendered_path.len());
                // println!("事件输出到文件 {}", path_str);
                Some(rendered_path)
            }
            Err(_error) => None,
        }
    }

    fn deadline_at(&self) -> Instant {
        Instant::now()
            .checked_add(self.idle_timeout)
            .expect("unable to compute next deadline")
    }

    /* 是StreamSink<Event> 的run接口的input */
    async fn run(&mut self, mut input: BoxStream<'_, Event>) -> crate::Result<()> {
        loop {
            tokio::select! {
                /* 从input获取event */
                event = input.next() => {
                    match event {
                        Some(event) => self.process_event(event).await,
                        None => {
                            // If we got `None` - terminate the processing.
                            debug!(message = "Receiver exhausted, terminating the processing loop.");

                            // Close all the open files.
                            debug!(message = "Closing all the open files.");
                            for (path, file) in self.files.iter_mut() {
                                if let Err(error) = file.close().await {
                                    error!("Failed to close file: {:?}, error: {}", path, error);
                                } else{
                                    trace!(message = "Successfully closed file.", path = ?path);
                                }
                            }

                           debug!("All files closed, total open files: 0");


                            break;
                        }
                    }
                }
                /* 关闭过期文件 */
                result = self.files.next_expired(), if !self.files.is_empty() => {
                    match result {
                        // We do not poll map when it's empty, so we should
                        // never reach this branch.
                        None => unreachable!(),
                        Some((mut expired_file, path)) => {
                            // We got an expired file. All we really want is to
                            // flush and close it.
                            if let Err(error) = expired_file.close().await {
                                error!("Failed to close expired file: {:?}, error: {}", path, error);
                            }
                            drop(expired_file); // ignore close error
                            debug!("File operation completed, total open files: {}", self.files.len());

                        }
                    }
                }
            }
        }

        Ok(())
    }

    /* 处理从input读取的event */
    async fn process_event(&mut self, event: Event) {
        /* 要输出到的文件路径 */
        let path = match self.partition_event(&event) {
            Some(path) => path,
            None => {
                // We weren't able to find the path to use for the
                // file.
                // The error is already handled at `Partition_event`, so
                // here we just skip the event.
                return;
            }
        };

        let next_deadline = self.deadline_at();

        let file = if let Some(file) = self.files.reset_at(&path, next_deadline) {
            /* 如果文件已经存在了 */
            trace!(message = "Working with an already opened file.", path = ?path);
            file
        } else {
            /* 打开新日志文件 */
            trace!(message = "Opening new file.", ?path);
            let file = match open_file(BytesPath::new(path.clone())).await {
                Ok(file) => file,
                Err(error) => {
                    // We couldn't open the file for this event.
                    // Maybe other events will work though! Just log
                    // the error and skip this event.
                    error!("Failed to open file: {:?}, error: {}", path, error);
                    return;
                }
            };

            /* 新建一个输出到file的outfile */
            let outfile = OutFile::new(file, self.compression);

            /* outfile加入filesink的文件哈希表集合 */
            self.files.insert_at(path.clone(), outfile, next_deadline);
            debug!(
                "File operation completed, total open files: {}",
                self.files.len()
            );

            self.files.get_mut(&path).unwrap()
        };

        /* 现在把event写入刚刚获取的file */
        trace!(message = "Writing an event to file.", path = ?path);

        /* 写出到outfile */
        match write_event_to_file(file, event,&mut self.encoder).await {
            Ok(byte_size) => {
                debug!(
                    "Sent {} bytes to file: {}",
                    byte_size,
                    String::from_utf8_lossy(&path)
                );
            }
            Err(error) => {
                error!("Failed to write file: {:?}, error: {}", path, error);
            }
        }
    }
}

/* 用于打开作为指标输出文件的文件
或者打开项目的配置文件 */
async fn open_file(path: impl AsRef<std::path::Path>) -> std::io::Result<File> {
    let parent = path.as_ref().parent();

    if let Some(parent) = parent {
        fs::create_dir_all(parent).await?;
    }

    fs::OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .append(true)
        .open(path)
        .await
}
/* 把事件写入到outfile
先transform, 然后encoder */
async fn write_event_to_file(
    file: &mut OutFile,
    event: Event,
    encoder: &mut Encoder<Framer>,
) -> Result<usize, std::io::Error> {
    /* 转换 */
    let mut buffer = BytesMut::new();
    /* 编码 */
    encoder
        .encode(event, &mut buffer)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    /* 写入 */
    file.write_all(&buffer).await.map(|()| buffer.len())
}

/* 实现filesink的streamsink接口. 接受一个input, 处理内容 */
#[async_trait]
impl StreamSink<Event> for FileSink {
    async fn run(mut self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        FileSink::run(&mut self, input)
            .await
            .expect("file sink error");
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use chrono::{SubsecRound, Utc};

    // 只引入测试里真正用到的
    use super::*;
    use crate::sinks::{
        lines_from_file, random_metrics_with_stream, random_metrics_with_stream_timestamp,
        temp_dir, temp_file, util::run_sink_test,
    };

    #[tokio::test]
    async fn metric_single_partition() {
        let template = temp_file();
        println!("测试文件路径: {}", template.display());
        let config = FileSinkConfig {
            path: template.clone().try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, JsonSerializerConfig::default()).into(),
            compression: Compression::None,
            timezone: Default::default(),
            max_file_size_bytes: default_max_file_size_bytes(),
        };

        let (input, _events) = random_metrics_with_stream(100, None);

        run_sink_test(&config, input.clone().into_iter()).await;

        let output = lines_from_file(template);
        for (input, output) in input.into_iter().zip(output) {
            let metric_name = input.as_metric().name();
            assert!(output.contains(metric_name));
        }
    }

    #[tokio::test]
    async fn metric_many_partitions() {
        let directory = temp_dir();

        let format = "%Y-%m-%d-%H-%M-%S";
        let mut template = directory.to_string_lossy().to_string();
        template.push_str(&format!("/{}.log", format));

        let config = FileSinkConfig {
            path: template.try_into().unwrap(),
            idle_timeout: default_idle_timeout(),
            encoding: (None::<FramingConfig>, JsonSerializerConfig::default()).into(),
            compression: Compression::None,
            timezone: Default::default(),
            max_file_size_bytes: default_max_file_size_bytes(),
        };

        let metric_count = 3;
        let timestamp = Utc::now().trunc_subsecs(3);
        let timestamp_offset = Duration::from_secs(1);

        let (input, _events) = random_metrics_with_stream_timestamp(
            metric_count,
            None,
            timestamp,
            timestamp_offset,
        );

        run_sink_test(&config, input.clone().into_iter()).await;

        let output = (0..metric_count).map(|index| {
            let expected_timestamp = timestamp + (timestamp_offset * index as u32);
            let expected_filename =
                directory.join(format!("{}.log", expected_timestamp.format(format)));

            lines_from_file(expected_filename)
        });
        for (input, output) in input.iter().zip(output) {
            // The format will partition by second and metrics are a second apart.
            assert_eq!(
                output.len(),
                1,
                "Expected the output file to contain one metric"
            );
            let output = &output[0];

            let metric_name = input.as_metric().name();
            assert!(output.contains(metric_name));
        }
    }
}
