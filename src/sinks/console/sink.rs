use async_trait::async_trait;
use bytes::BytesMut;
use futures::{stream::BoxStream, StreamExt};
use tokio::{io, io::AsyncWriteExt};
use tokio_util::codec::Encoder as _;
use crate::codecs::Framer;
use crate::{
    codecs::{Encoder, Transformer},
    event::{Event, EventStatus, Finalizable},
    sinks::util::StreamSink,
};
/* 一个sink的具体实现 */
pub struct WriterSink<T> {
    pub output: T, /* 指定了输出到stdout还是stderr */
    pub transformer: Transformer, /* 涉及的transform */
    pub encoder: Encoder<Framer>, /* 使用的编码器 */
}

#[async_trait]
impl<T> StreamSink<Event> for WriterSink<T>
where
    T: io::AsyncWrite + Send + Sync + Unpin,
{
    /*  */
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        while let Some(mut event) = input.next().await {
            /* 转换读取的event? */
            self.transformer.transform(&mut event);

            let finalizers = event.take_finalizers();
            let mut bytes = BytesMut::new();
            /* 编码读取的event? */
            self.encoder.encode(event, &mut bytes).map_err(|_| {
                // Error is handled by `Encoder`.
                finalizers.update_status(EventStatus::Errored);
            })?;
/* */
            match self.output.write_all(&bytes).await {
                Err(error) => {
                    // Error when writing to stdout/stderr is likely irrecoverable,
                    // so stop the sink.
                    error!(message = "Error writing to output. Stopping sink.", %error);
                    finalizers.update_status(EventStatus::Errored);
                    return Err(());
                }
                Ok(()) => {
                    finalizers.update_status(EventStatus::Delivered);
                }
            }
        }

        Ok(())
    }
}