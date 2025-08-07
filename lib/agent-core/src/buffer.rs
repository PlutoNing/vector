pub trait EventCount {
    fn event_count(&self) -> usize;
}
impl<T> EventCount for Vec<T>
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        self.iter().map(EventCount::event_count).sum()
    }
}

impl<T> EventCount for &T
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        (*self).event_count()
    }
}
