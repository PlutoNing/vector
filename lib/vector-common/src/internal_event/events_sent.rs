

use metrics::{counter, Counter};
use tracing::trace;



use super::{CountByteSize, Output, SharedString};

pub const DEFAULT_OUTPUT: &str = "_default";
/* 有调用 */
crate::registered_event!(
    EventsSent {
        output: Option<SharedString>,
    } => {
        events: Counter = if let Some(output) = &self.output {
            counter!("component_sent_events_total", "output" => output.clone())
        } else {
            counter!("component_sent_events_total")
        },
        event_bytes: Counter = if let Some(output) = &self.output {
            counter!("component_sent_event_bytes_total", "output" => output.clone())
        } else {
            counter!("component_sent_event_bytes_total")
        },
        output: Option<SharedString> = self.output,
    }

    fn emit(&self, data: CountByteSize) {
        let CountByteSize(count, byte_size) = data;

        match &self.output {
            Some(output) => {
                trace!(message = "Events sent.", count = %count, byte_size = %byte_size.get(), output = %output);
            }
            None => {
                trace!(message = "Events sent.", count = %count, byte_size = %byte_size.get());
            }
        }

        self.events.increment(count as u64);
        self.event_bytes.increment(byte_size.get() as u64);
    }
);
/* 有调用 */
impl From<Output> for EventsSent {
    fn from(output: Output) -> Self {
        Self { output: output.0 }
    }
}