use crate::codecs::Framer;
use crate::{
    codecs::{Encoder, Transformer},
    event::{Event},
    sinks::util::StreamSink,
};
use async_trait::async_trait;
use bytes::BytesMut;
use futures::{stream::BoxStream, StreamExt};
use tokio::{io, io::AsyncWriteExt};
use tokio_util::codec::Encoder as _;
/* 一个sink的具体实现 */
pub struct WriterSink<T> {
    pub output: T,                /* 指定了输出到stdout还是stderr */
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
            self.transformer.transform(&mut event);

            let mut bytes = BytesMut::new();
            self.encoder.encode(event, &mut bytes).map_err(|_| ())?;

            match self.output.write_all(&bytes).await {
                Err(error) => {
                    error!(message = "Error writing to output. Stopping sink.", %error);
                    return Err(());
                }
                Ok(()) => {}
            }
        }

        Ok(())
    }
}
