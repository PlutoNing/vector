use bytes::BytesMut;
use tokio_util::codec::Encoder as _;
use vector_lib::codecs::{
    encoding::{Error, Framer, Serializer},
    CharacterDelimitedEncoder, NewlineDelimitedEncoder, TextSerializerConfig,
};

use crate::{
    event::Event,
};

#[derive(Debug, Clone)]
/// An encoder that can encode structured events into byte frames.
pub struct Encoder<Framer>
where
    Framer: Clone,
{
    framer: Framer,
    serializer: Serializer,
}

impl Default for Encoder<Framer> {
    fn default() -> Self {
        Self {
            framer: NewlineDelimitedEncoder::default().into(),
            serializer: TextSerializerConfig::default().build().into(),
        }
    }
}

impl Default for Encoder<()> {
    fn default() -> Self {
        Self {
            framer: (),
            serializer: TextSerializerConfig::default().build().into(),
        }
    }
}

impl<Framer> Encoder<Framer>
where
    Framer: Clone,
{
    /// Serialize the event without applying framing.
    pub fn serialize(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Error> {
        let len = buffer.len();
        let mut payload = buffer.split_off(len);

        self.serialize_at_start(event, &mut payload)?;

        buffer.unsplit(payload);

        Ok(())
    }

    /// Serialize the event without applying framing, at the start of the provided buffer.
    fn serialize_at_start(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Error> {
        self.serializer.encode(event, buffer).map_err(|error| {
            error!("Failed to serialize event: {}", error);
            Error::SerializingError(error)
        })
    }
}

impl Encoder<Framer> {
    /// Creates a new `Encoder` with the specified `Serializer` to produce bytes
    /// from a structured event, and the `Framer` to wrap these into a byte
    /// frame. 把framer和序列化器组合成编码器
    pub const fn new(framer: Framer, serializer: Serializer) -> Self {
        Self { framer, serializer }
    }

    /// Get the framer.
    pub const fn framer(&self) -> &Framer {
        &self.framer
    }

    /// Get the serializer.
    pub const fn serializer(&self) -> &Serializer {
        &self.serializer
    }

    /// Get the prefix that encloses a batch of events.
    pub const fn batch_prefix(&self) -> &[u8] {
        match (&self.framer, &self.serializer) {
            (
                Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
                Serializer::Json(_),
            ) => b"[",
            _ => &[],
        }
    }

    /// Get the suffix that encloses a batch of events.
    pub const fn batch_suffix(&self) -> &[u8] {
        match (&self.framer, &self.serializer) {
            (
                Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
                Serializer::Json(_) ,
            ) => b"]",
            _ => &[],
        }
    }

    /// Get the HTTP content type.
    pub const fn content_type(&self) -> &'static str {
        match (&self.serializer, &self.framer) {
            (Serializer::Json(_), Framer::NewlineDelimited(_)) => {
                "application/x-ndjson"
            }
            (
                Serializer::Json(_),
                Framer::CharacterDelimited(CharacterDelimitedEncoder { delimiter: b',' }),
            ) => "application/json",
            (
                Serializer::Json(_)
                | Serializer::Text(_),
                _,
            ) => "text/plain",
        }
    }
}

impl Encoder<()> {
    /// Creates a new `Encoder` with the specified `Serializer` to produce bytes
    /// from a structured event.
    pub const fn new(serializer: Serializer) -> Self {
        Self {
            framer: (),
            serializer,
        }
    }

    /// Get the serializer.
    pub const fn serializer(&self) -> &Serializer {
        &self.serializer
    }
}

impl tokio_util::codec::Encoder<Event> for Encoder<Framer> {
    type Error = Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let len = buffer.len();
        let mut payload = buffer.split_off(len);

        self.serialize_at_start(event, &mut payload)?;

        // Frame the serialized event.
        self.framer.encode((), &mut payload).map_err(|error| {
            error!("Failed to Framing event: {}", error);
            Error::FramingError(error)
        })?;

        buffer.unsplit(payload);

        Ok(())
    }
}

impl tokio_util::codec::Encoder<Event> for Encoder<()> {
    type Error = Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        let len = buffer.len();
        let mut payload = buffer.split_off(len);

        self.serialize_at_start(event, &mut payload)?;

        buffer.unsplit(payload);

        Ok(())
    }
}
