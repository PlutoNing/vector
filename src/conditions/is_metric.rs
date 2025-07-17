use crate::event::Event;

pub(crate) const fn check_is_metric(e: Event) -> (bool, Event) {
    (matches!(e, Event::Metric(_)), e)
}
