use crate::event::Event;

pub(crate) const fn check_is_log(e: Event) -> (bool, Event) {
    (matches!(e, Event::Log(_)), e)
}