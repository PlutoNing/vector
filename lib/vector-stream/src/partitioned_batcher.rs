use std::{


    num::NonZeroUsize,


    time::Duration,
};





use vector_common::byte_size_of::ByteSizeOf;


use crate::batcher::{
    config::BatchConfigParts,
    data::BatchData,
    limiter::{ByteSizeOfItemSize, ItemBatchSize, SizeLimit},

};
/// Controls the behavior of the batcher in terms of batch size and flush interval.
///
/// This is a temporary solution for pushing in a fixed settings structure so we don't have to worry
/// about misordering parameters and what not.  At some point, we will pull
/// `BatchConfig`/`BatchSettings`/`BatchSize` out of `vector` and move them into `vector_core`, and
/// make it more generalized. We can't do that yet, though, until we've converted all of the sinks
/// with their various specialized batch buffers.
#[derive(Copy, Clone, Debug)]
pub struct BatcherSettings {
    pub timeout: Duration,
    pub size_limit: usize,
    pub item_limit: usize,
}

impl BatcherSettings {
    pub const fn new(
        timeout: Duration,
        size_limit: NonZeroUsize,
        item_limit: NonZeroUsize,
    ) -> Self {
        BatcherSettings {
            timeout,
            size_limit: size_limit.get(),
            item_limit: item_limit.get(),
        }
    }

    /// A batcher config using the `ByteSizeOf` trait to determine batch sizes.
    /// The output is a  `Vec<T>`.
    pub fn as_byte_size_config<T: ByteSizeOf>(
        &self,
    ) -> BatchConfigParts<SizeLimit<ByteSizeOfItemSize>, Vec<T>> {
        self.as_item_size_config(ByteSizeOfItemSize)
    }

    /// A batcher config using the `ItemBatchSize` trait to determine batch sizes.
    /// The output is a `Vec<T>`.
    pub fn as_item_size_config<T, I>(&self, item_size: I) -> BatchConfigParts<SizeLimit<I>, Vec<T>>
    where
        I: ItemBatchSize<T>,
    {
        BatchConfigParts {
            batch_limiter: SizeLimit {
                batch_size_limit: self.size_limit,
                batch_item_limit: self.item_limit,
                current_size: 0,
                item_size_calculator: item_size,
            },
            batch_data: vec![],
            timeout: self.timeout,
        }
    }

    /// A batcher config using the `ItemBatchSize` trait to determine batch sizes.
    /// The output is built with the supplied object implementing [`BatchData`].
    pub fn as_reducer_config<I, T, B>(
        &self,
        item_size: I,
        reducer: B,
    ) -> BatchConfigParts<SizeLimit<I>, B>
    where
        I: ItemBatchSize<T>,
        B: BatchData<T>,
    {
        BatchConfigParts {
            batch_limiter: SizeLimit {
                batch_size_limit: self.size_limit,
                batch_item_limit: self.item_limit,
                current_size: 0,
                item_size_calculator: item_size,
            },
            batch_data: reducer,
            timeout: self.timeout,
        }
    }
}