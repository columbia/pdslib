#[cfg(feature = "experimental")]
use std::fmt::Debug;

use log::debug;

use super::{core::PrivateDataServiceCore, quotas::FilterId};
use crate::{
    budget::{pure_dp_filter::PureDPBudget, traits::FilterStorage},
    events::{relevant_events::RelevantEvents, traits::EventStorage},
    queries::traits::EpochReportRequest,
};
#[cfg(feature = "experimental")]
use crate::{
    pds::quotas::PdsFilterStatus, queries::traits::PassivePrivacyLossRequest,
    util::hashmap::HashMap,
};

/// Epoch-based private data service, using generic filter
/// storage and event storage interfaces.
pub struct PrivateDataService<
    Q: EpochReportRequest,
    FS: FilterStorage<
        Budget = PureDPBudget,
        FilterId = FilterId<Q::EpochId, Q::Uri>,
    >,
    ES: EventStorage<Event = Q::Event>,
    ERR: From<FS::Error> + From<ES::Error>,
> {
    pub core: PrivateDataServiceCore<Q, FS, ERR>,

    /// Event storage interface.
    pub event_storage: ES,
}

/// Report returned by Pds, potentially augmented with debugging information
#[derive(Debug)]
pub struct PdsReport<Q: EpochReportRequest> {
    pub filtered_report: Q::Report,
    pub unfiltered_report: Q::Report,

    /// Store a list of the filter IDs that were out-of-budget in the atomic
    /// check for any epoch in the attribution window.
    pub oob_filters: Vec<FilterId<Q::EpochId, Q::Uri>>,
}

/// Default implementation for a null report
impl<Q: EpochReportRequest> Default for PdsReport<Q> {
    fn default() -> Self {
        Self {
            filtered_report: Q::Report::default(),
            unfiltered_report: Q::Report::default(),
            oob_filters: Vec::new(),
        }
    }
}

/// API for the epoch-based PDS.
impl<Q, FS, ES, ERR> PrivateDataService<Q, FS, ES, ERR>
where
    Q: EpochReportRequest<Report: Clone>,
    FS: FilterStorage<
        Budget = PureDPBudget,
        FilterId = FilterId<Q::EpochId, Q::Uri>,
    >,
    ES: EventStorage<Event = Q::Event>,
    ERR: From<FS::Error> + From<ES::Error>,
{
    pub fn new(filter_storage: FS, event_storage: ES) -> Self {
        Self {
            core: PrivateDataServiceCore::new(filter_storage),
            event_storage,
        }
    }

    /// Registers a new event.
    pub fn register_event(&mut self, event: Q::Event) -> Result<(), ERR> {
        debug!("Registering event {event:?}");
        self.event_storage.add_event(event)?;
        Ok(())
    }

    /// Computes a report for the given report request.
    pub fn compute_report(&mut self, request: &Q) -> Result<PdsReport<Q>, ERR> {
        let relevant_event_selector = request.relevant_event_selector();
        let relevant_events = RelevantEvents::from_event_storage(
            &mut self.event_storage,
            &request.epoch_ids(),
            relevant_event_selector,
        )?;

        self.core.compute_report(request, relevant_events)
    }

    /// [Experimental] Accounts for passive privacy loss. Can fail if the
    /// implementation has an error, but failure must not leak the state of
    /// the filters.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/16): what are the semantics of passive loss queries that go over the filter
    /// capacity?
    #[allow(clippy::type_complexity)]
    #[cfg(feature = "experimental")]
    pub fn account_for_passive_privacy_loss(
        &mut self,
        request: PassivePrivacyLossRequest<Q::EpochId, Q::Uri, PureDPBudget>,
    ) -> Result<PdsFilterStatus<FilterId<Q::EpochId, Q::Uri>>, ERR> {
        let source_losses = HashMap::new(); // Dummy.

        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            let filters_to_consume = self.core.filters_to_consume(
                epoch_id,
                &request.privacy_budget,
                &source_losses,
                &request.uris,
            );

            // Phase 1: dry run.
            let check_status = self.core.deduct_budget(
                &filters_to_consume,
                true, // dry run
            )?;
            if check_status != PdsFilterStatus::Continue {
                return Ok(check_status);
            }

            // Phase 2: Consume the budget
            let consume_status = self.core.deduct_budget(
                &filters_to_consume,
                false, // actually consume
            )?;

            assert_eq!(
                consume_status, PdsFilterStatus::Continue,
                "ERR: Phase 2 failed unexpectedly with status {consume_status:?} after Phase 1 succeeded",
            );

            // Semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(PdsFilterStatus::Continue)
    }
}
