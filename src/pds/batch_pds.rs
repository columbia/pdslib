use std::{collections::HashMap, fmt::Debug, hash::Hash, vec};

use anyhow::Error;

use crate::{
    budget::{pure_dp_filter::PureDPBudget, traits::FilterStorage},
    events::{
        ppa_event::PpaEvent,
        traits::{
            EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
        },
    },
    pds::{
        epoch_pds::{EpochPrivateDataService, FilterId, PdsReport},
        utils::PpaPds,
    },
    queries::{ppa_histogram::PpaHistogramRequest, traits::EpochReportRequest},
};

/// [Experimental] Batch wrapper for private data service.
/// TODO: maybe we need a trait for EpochPDS in the end. Make generic.
pub struct BatchPrivateDataService {
    /// Batch.
    pub pending_requests: Vec<PpaHistogramRequest>,

    /// Base private data service.
    /// Filters need to have functionality to unlock budget.
    pub pds: PpaPds,
}
// TODO: time release. Maybe lives outside of pdslib.

impl BatchPrivateDataService {
    /// Registers a new event, calls the existing pds transparently.
    pub fn register_event(&mut self, event: PpaEvent) -> Result<(), Error> {
        self.pds.register_event(event)
    }

    /// TODO: Nice to take ownership of the request, should do that in pds too.
    pub fn register_report_request(
        &mut self,
        request: PpaHistogramRequest,
    ) -> Result<(), Error> {
        self.pending_requests.push(request);
        Ok(())
    }

    pub fn schedule_batch(
        &mut self,
    ) -> Result<Vec<PdsReport<PpaHistogramRequest>>, Error> {
        // TODO(P1): first unlock some  fresh eps_C.
        // then go through requests one by one, try to allocate with regular quotas.
        //  next, reach out to the filters to deactivate qimp or set the capacity to infinity.
        // At the end, reset the quota filter capacities.
        // Let's keep a fixed qconv for now.

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
    // use common::logging;
    use log::info;
    use log4rs;

    use super::*;
    use crate::{
        budget::{
            hashmap_filter_storage::HashMapFilterStorage,
            pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        },
        events::{
            hashmap_event_storage::HashMapEventStorage, ppa_event::PpaEvent,
            traits::EventUris,
        },
        pds::{batch_pds, epoch_pds::StaticCapacities, utils::PpaPds},
        queries::{
            ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
            simple_last_touch_histogram::SimpleLastTouchHistogramRequest,
            traits::{PassivePrivacyLossRequest, ReportRequestUris},
        },
    };

    #[test]
    fn schedule_one_batch() -> Result<(), anyhow::Error> {
        log4rs::init_file("logging_config.yaml", Default::default())?;

        let capacities = StaticCapacities::mock();

        let pds = PpaPds::new(capacities)?;

        let mut batch_pds = BatchPrivateDataService {
            pending_requests: vec![],
            pds,
        };

        info!("Registering events");

        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0x559, // 0x559 = "campaignCounts".to_string() | 0x400
            uris: EventUris::mock(),
            filter_data: 1,
        };

        batch_pds.register_event(event1.clone())?;

        let request1 = PpaHistogramRequest::mock()?;
        batch_pds.register_report_request(request1)?;

        let request2 = PpaHistogramRequest::mock()?;
        batch_pds.register_report_request(request2)?;

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 2);

        info!("Reports: {:?}", reports);

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 0);
        info!("Reports after scheduling everyone: {:?}", reports);

        // TODO: check ull reports, etc.

        Ok(())
    }
}
