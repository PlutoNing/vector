use std::{io, io::BufWriter};

use bytes::{BufMut, BytesMut};
use flate2::write::{GzEncoder};

use super::{Compression};

const GZIP_INPUT_BUFFER_CAPACITY: usize = 4_096;

const OUTPUT_BUFFER_CAPACITY: usize = 1_024;

enum Writer {
    Plain(bytes::buf::Writer<BytesMut>),
    Gzip(BufWriter<GzEncoder<bytes::buf::Writer<BytesMut>>>),
}

impl Writer {
    pub fn get_ref(&self) -> &BytesMut {
        match self {
            Writer::Plain(inner) => inner.get_ref(),
            Writer::Gzip(inner) => inner.get_ref().get_ref().get_ref(),
        }
    }

    pub fn into_inner(self) -> BytesMut {
        match self {
            Writer::Plain(writer) => writer,
            Writer::Gzip(writer) => writer
                .into_inner()
                .expect("BufWriter writer should not fail to finish")
                .finish()
                .expect("gzip writer should not fail to finish"),
        }
        .into_inner()
    }

    pub fn finish(self) -> io::Result<BytesMut> {
        let buf = match self {
            Writer::Plain(writer) => writer,
            Writer::Gzip(writer) => writer.into_inner()?.finish()?,
        }
        .into_inner();

        Ok(buf)
    }
}

impl From<Compression> for Writer {
    fn from(compression: Compression) -> Self {
        let writer = BytesMut::with_capacity(OUTPUT_BUFFER_CAPACITY).writer();
        match compression {
            Compression::None => Writer::Plain(writer),
            // Buffering writes to the underlying Encoder writer
            // to avoid Vec-trashing and expensive memset syscalls.
            // https://github.com/rust-lang/flate2-rs/issues/395#issuecomment-1975088152
            Compression::Gzip(level) => Writer::Gzip(BufWriter::with_capacity(
                GZIP_INPUT_BUFFER_CAPACITY,
                GzEncoder::new(writer, level.as_flate2()),
            )),
        }
    }
}

impl io::Write for Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[allow(clippy::disallowed_methods)] // Caller handles the result of `write`.
        match self {
            Writer::Plain(inner_buf) => inner_buf.write(buf),
            Writer::Gzip(writer) => writer.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Writer::Plain(writer) => writer.flush(),
            Writer::Gzip(writer) => writer.flush(),
        }
    }
}

/// Simple compressor implementation based on [`Compression`].
///
/// Users can acquire a `Compressor` via [`Compressor::from`] based on the desired compression scheme.
pub struct Compressor {
    compression: Compression,
    inner: Writer,
}

impl Compressor {
    /// Gets a mutable reference to the underlying buffer.
    pub fn get_ref(&self) -> &BytesMut {
        self.inner.get_ref()
    }

    /// Gets whether or not this compressor will actually compress the input.
    ///
    /// While it may be counterintuitive for "compression" to not compress, this is simply a
    /// consequence of designing a single type that may or may not compress so that we can avoid
    /// having to box writers at a higher-level.
    ///
    /// Some callers can benefit from knowing whether or not compression is actually taking place,
    /// as different size limitations may come into play.
    pub const fn is_compressed(&self) -> bool {
        self.compression.is_compressed()
    }

    /// Consumes the compressor, returning the internal buffer used by the compressor.
    ///
    /// # Errors
    ///
    /// If the compressor encounters an I/O error while finalizing the payload, an error
    /// variant will be returned.
    pub fn finish(self) -> io::Result<BytesMut> {
        self.inner.finish()
    }

    /// Consumes the compressor, returning the internal buffer used by the compressor.
    ///
    /// # Panics
    ///
    /// Panics if finalizing the compressor encounters an I/O error.  This should generally only be
    /// possible when the system is out of memory and allocations cannot be performed to write any
    /// footer/checksum data.
    ///
    /// Consider using `finish` if catching these scenarios is important.
    pub fn into_inner(self) -> BytesMut {
        self.inner.into_inner()
    }
}

impl io::Write for Compressor {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        #[allow(clippy::disallowed_methods)] // Caller handles the result of `write`.
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl From<Compression> for Compressor {
    fn from(compression: Compression) -> Self {
        Compressor {
            compression,
            inner: compression.into(),
        }
    }
}
