/// Converts back and forth between user-friendly metadata types and the on-disk integer representation.
pub trait AsMetadata: Sized {
    /// Converts this metadata value into its integer representation.
    fn into_u32(self) -> u32;

    /// Converts an integer representation of metadata into its real type, if possible.
    ///
    /// If the given integer does not represent a valid representation of the given metadata type,
    /// possibly due to including bits not valid for the type, and so on, then `None` will be
    /// returned.  Otherwise, `Some(Self)` will be returned.
    fn from_u32(value: u32) -> Option<Self>;
}

impl AsMetadata for () {
    fn into_u32(self) -> u32 {
        0
    }

    fn from_u32(_: u32) -> Option<Self> {
        Some(())
    }
}
