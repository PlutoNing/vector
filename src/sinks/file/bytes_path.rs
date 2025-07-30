//! Fun little hack around bytes and OsStr

use std::path::Path;

use bytes::Bytes;

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
