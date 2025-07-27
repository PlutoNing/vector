use vector_lib::{event::Event, partition::Partitioner};
use tracing::error;

use crate::template::Template;

/// Partitions items based on the generated key for the given event.
pub struct KeyPartitioner {
    key_prefix_template: Template,
    dead_letter_key_prefix: Option<String>,
}

impl KeyPartitioner {
    pub const fn new(
        key_prefix_template: Template,
        dead_letter_key_prefix: Option<String>,
    ) -> Self {
        Self {
            key_prefix_template,
            dead_letter_key_prefix,
        }
    }
}

impl Partitioner for KeyPartitioner {
    type Item = Event;
    type Key = Option<String>;

    fn partition(&self, item: &Self::Item) -> Self::Key {
        self.key_prefix_template
            .render_string(item)
            .or_else(|error| {
                if let Some(dead_letter_key_prefix) = &self.dead_letter_key_prefix {
                    error!("Failed to render key prefix template: {}, using dead letter key prefix", error);
                    Ok(dead_letter_key_prefix.clone())
                } else {
                    error!("Failed to render key prefix template: {}, dropping event", error);
                    Err(())
                }
            })
            .ok()
    }
}
