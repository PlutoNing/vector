use async_trait::async_trait;
use bytes::BytesMut;
use futures::{stream::BoxStream, StreamExt};
use tokio::{io, io::AsyncWriteExt};
use tokio_util::codec::Encoder as _;
use crate::codecs::Framer;
use vector_lib::{
    // internal_event::{
        // ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output, Protocol,
    // },
    EstimatedJsonEncodedSizeOf,
};
use crate::internal_event::{ ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle as _, Output, Protocol,};
use crate::{
    register,
    // registered_event,
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
        let bytes_sent = register!(BytesSent::from(Protocol("console".into(),)));
        let events_sent = register!(EventsSent::from(Output(None)));
        while let Some(mut event) = input.next().await {
            let event_byte_size = event.estimated_json_encoded_size_of();
            /* 转换读取的event? */
            self.transformer.transform(&mut event);

            let finalizers = event.take_finalizers();
            let mut bytes = BytesMut::new();
            /* 编码读取的event? */
            self.encoder.encode(event, &mut bytes).map_err(|_| {
                // Error is handled by `Encoder`.
                finalizers.update_status(EventStatus::Errored);
            })?;
/* write_all是什么 */
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

                    events_sent.emit(CountByteSize(1, event_byte_size));
                    bytes_sent.emit(ByteSize(bytes.len()));
                }
            }
        }

        Ok(())
    }
}