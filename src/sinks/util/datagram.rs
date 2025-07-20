#[cfg(unix)]
use std::path::PathBuf;

use bytes::BytesMut;
use futures::{stream::BoxStream, StreamExt};
use futures_util::stream::Peekable;
#[cfg(unix)]
use tokio::net::UnixDatagram;
use tokio_util::codec::Encoder;
use vector_lib::internal_event::RegisterInternalEvent;
use vector_lib::internal_event::{ByteSize, BytesSent, InternalEventHandle};
use vector_lib::EstimatedJsonEncodedSizeOf;

use crate::{
    codecs::Transformer,
    event::{Event, EventStatus, Finalizable},
    internal_events::{SocketEventsSent, SocketMode},
};

pub enum DatagramSocket {
    #[cfg(unix)]
    Unix(UnixDatagram, PathBuf),
}

pub async fn send_datagrams<E: Encoder<Event, Error = vector_lib::codecs::encoding::Error>>(
    input: &mut Peekable<BoxStream<'_, Event>>,
    mut socket: DatagramSocket,
    transformer: &Transformer,
    encoder: &mut E,
    bytes_sent: &<BytesSent as RegisterInternalEvent>::Handle,
) {
    while let Some(mut event) = input.next().await {
        let byte_size = event.estimated_json_encoded_size_of();

        transformer.transform(&mut event);

        let finalizers = event.take_finalizers();
        let mut bytes = BytesMut::new();

        // Errors are handled by `Encoder`.
        if encoder.encode(event, &mut bytes).is_err() {
            continue;
        }

        match send_datagram(&mut socket, &bytes).await {
            Ok(()) => {
                emit!(SocketEventsSent {
                    mode: match socket {
                        #[cfg(unix)]
                        DatagramSocket::Unix(..) => SocketMode::Unix,
                    },
                    count: 1,
                    byte_size,
                });

                bytes_sent.emit(ByteSize(bytes.len()));
                finalizers.update_status(EventStatus::Delivered);
            }
            Err(_error) => {
                finalizers.update_status(EventStatus::Errored);
                return;
            }
        }
    }
}

async fn send_datagram(socket: &mut DatagramSocket, buf: &[u8]) -> tokio::io::Result<()> {
    let _sent = match socket {
        #[cfg(unix)]
        DatagramSocket::Unix(uds, _) => uds.send(buf).await,
    }?;
    Ok(())
}
