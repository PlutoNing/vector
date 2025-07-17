use crate::event::Event;

pub(crate) const fn check_is_trace(e: Event) -> (bool, Event) {
    (matches!(e, Event::Trace(_)), e)
}
