// TODO: maybe T: Budget with another trait

pub trait Filter<T> {
    fn new(capacity: T) -> Self;

    fn try_consume(&mut self, budget: T) -> Result<(), ()>;
}