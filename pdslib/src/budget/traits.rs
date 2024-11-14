// TODO: maybe Budget trait, and Filter<T: Budget> if we need?

use thiserror::Error;

#[derive(Error, Debug)]
pub enum FilterError {
    #[error("Out of budget")]
    OutOfBudget,
}

pub trait Filter<T> {
    fn new(capacity: T) -> Self;

    fn try_consume(&mut self, budget: &T) -> Result<(), FilterError>;
}

#[derive(Error, Debug)]
pub enum FilterStorageError {
    #[error(transparent)]
    FilterError(#[from] FilterError),
    #[error("Filter does not exist")]
    FilterDoesNotExist,
    #[error("Cannot initialize new filter")]
    CannotInitializeFilter,
}

pub trait FilterStorage {
    type FilterId;
    type Budget;

    fn new_filter(
        &mut self,
        filter_id: Self::FilterId,
        capacity: Self::Budget,
    ) -> Result<(), FilterStorageError>;

    fn is_initialized(&mut self, filter_id: &Self::FilterId) -> bool;

    fn try_consume(
        &mut self,
        filter_id: &Self::FilterId,
        budget: &Self::Budget,
    ) -> Result<(), FilterStorageError>;
}
