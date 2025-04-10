use std::{collections::HashMap, fmt::Debug, hash::Hash, vec};

use crate::{
    budget::{pure_dp_filter::PureDPBudget, traits::FilterStorage},
    events::traits::{
        EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
    },
    pds::epoch_pds::{EpochPrivateDataService, FilterId, PdsReport},
    queries::traits::EpochReportRequest,
};

/// [Experimental] Batch wrapper for private data service.
/// TODO: maybe we need a trait for EpochPDS in the end.
pub struct BatchPrivateDataService<
    FS: FilterStorage,
    ES: EventStorage,
    Q: EpochReportRequest,
    ERR: From<FS::Error> + From<ES::Error>,
> {
    /// Batch.
    pub pending_requests: Vec<Q>,

    /// Base private data service.
    /// Filters need to have functionality to unlock budget.
    pub pds: EpochPrivateDataService<FS, ES, Q, ERR>,
}
// TODO: time release. Maybe lives outside of pdslib.

impl<U, EI, E, EE, RES, FS, ES, Q, ERR> BatchPrivateDataService<FS, ES, Q, ERR>
where
    U: Clone + Eq + Hash + Debug,
    EI: EpochId,
    E: Event<EpochId = EI, Uri = U> + Clone,
    EE: EpochEvents,
    FS: FilterStorage<Budget = PureDPBudget, FilterId = FilterId<EI, U>>,
    RES: RelevantEventSelector<Event = E>,
    ES: EventStorage<
        Event = E,
        EpochEvents = EE,
        RelevantEventSelector = RES,
        Uri = U,
    >,
    Q: EpochReportRequest<
        EpochId = EI,
        EpochEvents = EE,
        RelevantEventSelector = RES,
        Uri = U,
    >,
    ERR: From<FS::Error> + From<ES::Error> + From<anyhow::Error>,
{
    /// Registers a new event.
    pub fn register_event(&mut self, event: E) -> Result<(), ERR> {
        self.pds.register_event(event)
    }

    /// TODO: Nice to take ownership of the request, should do that in pds too.
    pub fn register_report_request(&mut self, request: Q) -> Result<(), ERR> {
        self.pending_requests.push(request);
        Ok(())
    }

    pub fn schedule_batch(&mut self) -> Result<Vec<PdsReport<Q>>, ERR> {
        // TODO: keep track of queriers and intermediaries? Or maybe this lives in the report directly, metadata. Maybe wrap it.
        // TODO: keep pending requests by deadline.
        let mut reports = vec![];
        for request in self.pending_requests.iter() {
            let report = self.pds.compute_report(request)?;
            reports.push(report);
        }
        self.pending_requests.clear(); // We don't retry failed requests for now.
        Ok(reports)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        budget::{
            hashmap_filter_storage::HashMapFilterStorage,
            pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        },
        events::hashmap_event_storage::HashMapEventStorage,
        queries::{
            simple_last_touch_histogram::SimpleLastTouchHistogramRequest,
            traits::PassivePrivacyLossRequest,
        },
    };

    // #[test]
    // fn schedule_one_batch() -> Result<(), anyhow::Error> {}
}
