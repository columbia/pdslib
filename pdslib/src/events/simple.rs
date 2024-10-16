use crate::events::traits::{Event, Epoch};

// TODO: add enough things to run basic queries and filters
#[derive(Debug)]
pub struct SimpleEvent {
    pub id: usize,
    pub epoch_number: usize,
    pub value: usize,
}

impl Event for SimpleEvent {}

#[derive(Debug)]
pub struct SimpleEpoch {
    pub events: Vec<SimpleEvent>,
}

// TODO: add event to epoch, etc. Maybe epoch trait.
impl Epoch for SimpleEpoch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_event() {
        let event = SimpleEvent { id: 1, epoch_number: 1, value: 3};
        assert_eq!(event.id, 1);
    }
}