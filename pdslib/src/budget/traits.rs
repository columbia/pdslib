// TODO: maybe Budget trait, and Filter<T: Budget> if we need?

pub type FilterResult = Result<(), ()>; // Continue or Halt

pub trait Filter<T> {
    fn new(capacity: T) -> Self;

    fn try_consume(&mut self, budget: T) -> FilterResult;
}

pub trait FilterStorage {
    type FilterId;
    type Budget;
    type Filter: Filter<Self::Budget>;
    // TODO: allow custom error type.

    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
        capacity: Self::Budget,
    ) -> Result<(), ()>;

    fn get_filter(
        &mut self,
        filter_id: Self::FilterId,
    ) -> Option<&Self::Filter>;

    fn try_consume(
        &mut self,
        filter_id: Self::FilterId,
        budget: Self::Budget,
    ) -> Result<FilterResult, ()>;
}
