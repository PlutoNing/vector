mod events_received;
mod events_sent;

use std::ops::{Add, AddAssign};

pub use metrics::SharedString;

pub use events_received::EventsReceived;
pub use events_sent::{EventsSent, DEFAULT_OUTPUT};

use metrics::{counter, Counter};

crate::registered_event!(
    BytesSent {
        protocol: SharedString,
    } => {
        bytes_sent: Counter = counter!("component_sent_bytes_total", "protocol" => self.protocol.clone()),
        _protocol: SharedString = self.protocol,
    }

    fn emit(&self, byte_size: ByteSize) {
        self.bytes_sent.increment(byte_size.0 as u64);
    }
);

impl From<Protocol> for BytesSent {
    fn from(protocol: Protocol) -> Self {
        Self {
            protocol: protocol.0,
        }
    }
}

/* 有调用 */
crate::registered_event!(
    BytesReceived {
        protocol: SharedString,
    } => {
        received_bytes: Counter = counter!("component_received_bytes_total", "protocol" => self.protocol.clone()),
        protocol: SharedString = self.protocol,
    }

    fn emit(&self, data: ByteSize) {
        self.received_bytes.increment(data.0 as u64);
        trace!(message = "Bytes received.", byte_size = %data.0, protocol = %self.protocol);
    }
);

impl From<Protocol> for BytesReceived {
    fn from(protocol: Protocol) -> Self {
        Self {
            protocol: protocol.0,
        }
    }
}

/// The user can configure whether a tag should be emitted. If they configure it to
/// be emitted, but the value doesn't exist - we should emit the tag but with a value
/// of `-`.
#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum OptionalTag<T> {
    Ignored,
    Specified(Option<T>),
}

impl<T> From<Option<T>> for OptionalTag<T> {
    fn from(value: Option<T>) -> Self {
        Self::Specified(value)
    }
}

// use crate::json_size::JsonSize;
use agent_lib::json_size::JsonSize;

pub trait InternalEvent: Sized {
    fn emit(self);

    // Optional for backwards compat until all events implement this
    fn name(&self) -> Option<&'static str> {
        None
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait RegisterInternalEvent: Sized {
    type Handle: InternalEventHandle;

    fn register(self) -> Self::Handle;

    fn name(&self) -> Option<&'static str> {
        None
    }
}

#[allow(clippy::module_name_repetitions)]
pub trait InternalEventHandle: Sized {
    type Data: Sized;
    fn emit(&self, data: Self::Data);
}

// Sets the name of an event if it doesn't have one
pub struct DefaultName<E> {
    pub name: &'static str,
    pub event: E,
}

impl<E: InternalEvent> InternalEvent for DefaultName<E> {
    fn emit(self) {
        self.event.emit();
    }

    fn name(&self) -> Option<&'static str> {
        Some(self.event.name().unwrap_or(self.name))
    }
}

impl<E: RegisterInternalEvent> RegisterInternalEvent for DefaultName<E> {
    type Handle = E::Handle;

    fn register(self) -> Self::Handle {
        self.event.register()
    }

    fn name(&self) -> Option<&'static str> {
        Some(self.event.name().unwrap_or(self.name))
    }
}

pub fn emit(event: impl InternalEvent) {
    event.emit();
}

pub fn register<E: RegisterInternalEvent>(event: E) -> E::Handle {
    event.register()
}

pub type Registered<T> = <T as RegisterInternalEvent>::Handle;

// Wrapper types used to hold data emitted by registered events

#[derive(Clone, Copy)]
pub struct ByteSize(pub usize);

#[derive(Clone, Copy)]
pub struct Count(pub usize);

/// Holds the tuple `(count_of_events, estimated_json_size_of_events)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CountByteSize(pub usize, pub JsonSize);

impl AddAssign for CountByteSize {
    fn add_assign(&mut self, rhs: Self) {
        self.0 += rhs.0;
        self.1 += rhs.1;
    }
}

impl Add<CountByteSize> for CountByteSize {
    type Output = CountByteSize;

    fn add(self, rhs: CountByteSize) -> Self::Output {
        CountByteSize(self.0 + rhs.0, self.1 + rhs.1)
    }
}

// Wrapper types used to hold parameters for registering events

pub struct Output(pub Option<SharedString>);

pub struct Protocol(pub SharedString);

impl Protocol {
    pub const HTTP: Protocol = Protocol(SharedString::const_str("http"));
    pub const HTTPS: Protocol = Protocol(SharedString::const_str("https"));
    pub const NONE: Protocol = Protocol(SharedString::const_str("none"));
    pub const TCP: Protocol = Protocol(SharedString::const_str("tcp"));
    pub const UDP: Protocol = Protocol(SharedString::const_str("udp"));
    pub const UNIX: Protocol = Protocol(SharedString::const_str("unix"));
    pub const INTERNAL: Protocol = Protocol(SharedString::const_str("internal"));
    pub const STATIC: Protocol = Protocol(SharedString::const_str("static"));
}

impl From<&'static str> for Protocol {
    fn from(s: &'static str) -> Self {
        Self(SharedString::const_str(s))
    }
}

impl From<Protocol> for SharedString {
    fn from(value: Protocol) -> Self {
        value.0
    }
}

/// Macro to take care of some of the repetitive boilerplate in implementing a registered event. See
/// the other events in this module for examples of how to use this.
///
/// ## Usage
///
/// ```ignore
/// registered_event!(
///     Event {
///         event_field: &'static str,
///     } => {
///         handle_field: Counter = counter!("name", "tag" => self.event_field),
///     }
///     fn emit(&self, data: DataType) {
///         self.handle_field.increment(data.0);
///     }
/// );
///
/// let handle = register!(Event { event_field: "message" });
///
/// handle.emit(DataType(123));
/// ```
///
/// In this example, the first set of fields describes the data required to register the event. This
/// is what would be used by the `register!` macro. For example, `register!(Event { event_field:
/// "something" })`. The second set of fields describes the data required to store the registered
/// handle, namely the `Counter`s and `Gauge`s that record the handle from `metrics` as well as any
/// associated data for emitting traces or debug messages, followed by an initialization assignment
/// value. The `emit` function is the code required to update the metrics and generate any log
/// messages.
#[macro_export]
macro_rules! registered_event {
    // A registered event struct with no fields (zero-sized type).
    ($event:ident => $($tail:tt)*) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $event;

        $crate::registered_event!(=> $event $($tail)*);
    };

    // A normal registered event struct.
    ($event:ident { $( $field:ident: $type:ty, )* } => $($tail:tt)*) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $event {
            $( pub $field: $type, )*
        }

        $crate::registered_event!(=> $event $($tail)*);
    };

    // Sub-matcher to implement the common bits in the above two cases.
    (
        => $event:ident {
            $( $field:ident: $type:ty = $value:expr, )*
        }

        fn emit(&$slf:ident, $data_name:ident: $data:ident)
            $emit_body:block

        $(fn register($fixed_name:ident: $fixed_tags:ty, $tags_name:ident: $tags:ty)
            $register_body:block)?
    ) => {
        paste::paste!{
            #[derive(Clone)]
            pub struct [<$event Handle>] {
                $( $field: $type, )*
            }

            impl $crate::internal_event::RegisterInternalEvent for $event {
                type Handle = [<$event Handle>];

                fn name(&self) -> Option<&'static str> {
                    Some(stringify!($event))
                }

                fn register($slf) -> Self::Handle {
                    Self::Handle {
                        $( $field: $value, )*
                    }
                }
            }

            impl $crate::internal_event::InternalEventHandle for [<$event Handle>] {
                type Data = $data;

                fn emit(&$slf, $data_name: $data)
                    $emit_body
            }

        }
    };
}

#[macro_export]
macro_rules! register {
    ($event:expr) => {
        crate::internal_event::register($event)
    };
}
