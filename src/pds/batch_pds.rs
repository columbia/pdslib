use std::{collections::HashMap, fmt::Debug, mem::take, vec};

use anyhow::{bail, Result};
use log::{debug, info, warn};

use super::{epoch_pds::FilterId, utils::PpaCapacities};
use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::{ppa_event::PpaEvent, traits::Event},
    pds::{epoch_pds::PdsReport, utils::PpaPds},
    queries::ppa_histogram::PpaHistogramRequest,
};

#[derive(Debug)]
pub struct BatchedRequest {
    /// Since reports are dissociated from the initial report request, we need
    /// to keep track of who asked for what.
    request_id: u64,

    /// Number of times we can try scheduling this request.
    /// E.g. if this is equal to 1, this request goes through only one
    /// `schedule_batch` call. It has to be answered by the end of the
    /// call. If it didn't get allocated in the initialization, online or batch
    /// phase then it is answered with a null report.
    n_remaining_scheduling_attempts: u64,

    /// The actual request.
    /// TODO: make generic.
    request: PpaHistogramRequest,
}

impl BatchedRequest {
    pub fn new(
        request_id: u64,
        n_scheduling_attemps: u64,
        request: PpaHistogramRequest,
    ) -> Result<Self> {
        if n_scheduling_attemps == 0 {
            // TODO(later): allow requests with 0 batch scheduling attempt for
            // real-time queries. But for now we only consider batched, where a
            // query goes through at least one batch phase.
            bail!("The request should have at least one scheduling attempt.");
        }

        Ok(BatchedRequest {
            request_id,
            n_remaining_scheduling_attempts: n_scheduling_attemps,
            request,
        })
    }
}

/// [Experimental] Batch wrapper for private data service.
pub struct BatchPrivateDataService {
    pub current_scheduling_interval: u64,

    /// Queries that arrived during the current interval.
    pub new_pending_requests: Vec<BatchedRequest>,

    /// Queries that are still waiting.
    pub batched_requests: Vec<BatchedRequest>,

    /// Reports for requests that have already been answered but need to wait
    /// for more scheduling intervals until they can be released.
    /// Grouped by scheduling interval at the end of which they will be
    /// released.
    pub delayed_reports: HashMap<u64, Vec<BatchedReport>>,

    /// Epochs present in the system
    /// Range of epochs from start to end (included).
    /// TODO: formalize a bit more the invariants, use  HashSet<u64>, or connect to time?
    pub epochs: Option<(usize, usize)>,

    /// Amount of c-filter budget to be released per scheduling interval.
    pub eps_c_per_release: f64,

    /// Base private data service.
    /// Filters need to have functionality to unlock budget.
    pub pds: PpaPds,
}
// TODO: time release. Maybe lives outside of pdslib.

/// Report for a batched request. Guaranteed to be returned after the number of
/// scheduling attempts the request specified.
#[derive(Debug)]
pub struct BatchedReport {
    /// The request that asked for this report, potentially a long time ago.
    pub request_id: u64,

    /// The report answering that request.
    pub report: PdsReport<PpaHistogramRequest>,
}

impl BatchPrivateDataService {
    /// Create a new batch private data service.
    pub fn new(capacities: PpaCapacities, n_releases: usize) -> Result<Self> {
        // Release the c-filter over T scheduling intervals.
        let eps_c_per_release = match capacities.c {
            PureDPBudget::Epsilon(eps_c) => eps_c / (n_releases as f64),
            PureDPBudget::Infinite => {
                warn!("C-filter has infinite capacity. Release is a no-op");
                0.0
            }
        };

        let pds = PpaPds::new(capacities)?;

        Ok(BatchPrivateDataService {
            current_scheduling_interval: 0,
            new_pending_requests: vec![],
            batched_requests: vec![],
            delayed_reports: HashMap::new(),
            eps_c_per_release,
            epochs: None,
            pds,
        })
    }

    /// Register a new event, calls the existing pds transparently.
    pub fn register_event(&mut self, event: PpaEvent) -> Result<()> {
        let epoch = event.epoch_id();

        // Update the range of epochs present in the system
        match self.epochs {
            Some((start, end)) => {
                if epoch < end {
                    bail!("Epochs should be monotonically increasing. Got epoch {}, but the current range is ({}, {})", epoch, start, end);
                }
                if epoch > end {
                    // Extend the range of epochs when needed.
                    // NOTE: queries don't extend the range.
                    self.epochs = Some((start, epoch));
                }
            }
            None => {
                self.epochs = Some((epoch, epoch));
            }
        }

        // TODO(P1): also keep track of impression sites, maybe per epoch, or just in total.

        self.pds.register_event(event)
    }

    pub fn register_report_request(
        &mut self,
        request: BatchedRequest,
    ) -> Result<()> {
        self.new_pending_requests.push(request);
        Ok(())
    }

    pub fn schedule_batch(&mut self) -> Result<Vec<BatchedReport>> {
        info!(
            "Scheduling batch for interval {}",
            self.current_scheduling_interval
        );

        info!(
            "Queries already in the batch before initialization: {:?}",
            self.batched_requests
        );

        // We are entering a new scheduling interval. Decrement the number
        // of remaining attempts for all requests.
        for request in &mut self.batched_requests {
            request.n_remaining_scheduling_attempts -= 1;
        }

        self.initialization_phase()?;

        info!(
            "Queries in the batch after initialization: {:?}",
            self.batched_requests
        );

        info!("New queries that arrived since the previous scheduling attempt: {:?}", self.new_pending_requests);

        self.online_phase()?;

        info!(
            "Queries in the batch after online phase: {:?}",
            self.batched_requests
        );

        assert!(
            self.new_pending_requests.is_empty(),
            "New requests should be empty after the online phase, since unallocated ones are moved to the batch."
        );

        // Any request with 0 remaining attempts will be answered here and
        // removed from the batch.
        self.batch_phase()?;

        info!(
            "Queries in the batch after batch phase: {:?}",
            self.batched_requests
        );

        // Take all the reports that are ready to be released.
        let reports = self
            .delayed_reports
            .remove(&self.current_scheduling_interval)
            .unwrap_or_default();

        info!(
            "Reports to be released at the end of scheduling interval {}: {:?}",
            self.current_scheduling_interval, reports
        );

        self.current_scheduling_interval += 1;

        Ok(reports) // TODO(P1): only answer by the deadline.
    }

    fn initialization_phase(&mut self) -> Result<()> {
        // TODO(P1): first unlock eps_C. Browse all epochs, or up to some max
        // number of iterations that we know are enough to unlock everything.
        // Shove this to another function then, and for now really just try all
        // the past epochs since our implementations just have a few. Use events
        // or reports to check whether an epoch exists, otherwise could expose
        // from filter storage.
        if let Some((start, end)) = self.epochs {
            for epoch in start..=end {
                self.release_budget(epoch)?;
            }
        }

        //TODO(P1): Fresh quotas, need to update
        // abstractions here too... TODO: what happens when some epochs
        // in the attribution have unlocked their whole budget but not
        // others? TODO(later): some basic caching to avoid checking
        // queries that have zero chance of being fair?

        let batched_requests = take(&mut self.batched_requests);
        let unallocated_requests = self.try_allocate(batched_requests)?;

        // Put unallocated requests back into the batch.
        self.batched_requests = unallocated_requests;

        Ok(())
    }

    fn online_phase(&mut self) -> Result<()> {
        // browse newly arrived requests one by one, try to allocate with
        // regular quotas.

        let new_pending_requests = take(&mut self.new_pending_requests);
        let unallocated_requests = self.try_allocate(new_pending_requests)?;

        // Put unallocated requests into the batch.
        self.batched_requests.extend(unallocated_requests);
        Ok(())
    }

    fn batch_phase(&mut self) -> Result<()> {
        //  next, reach out to the filters to deactivate qimp or set the
        // capacity to infinity. Let's keep a fixed qconv for now.
        // Sort and try to allocate.

        // TODO(P1): implement the actual logic here.
        let sorted_batched_requests = take(&mut self.batched_requests);

        // Try to allocate the requests. Requests with 0 remaining attemps will
        // be
        let unallocated_requests =
            self.try_allocate(sorted_batched_requests)?;

        // Put unallocated requests back into the batch.
        self.batched_requests = unallocated_requests;
        Ok(())
    }

    fn try_allocate(
        &mut self,
        requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        // Go through requests one by one and try to allocate them.
        let mut unallocated_requests = vec![];
        for request in requests {
            let report = self.pds.compute_report(&request.request)?;

            info!("Report for request {}: {:?}", request.request_id, report);

            // if (report.error_cause().is_none() || (end_of_scheduling_interval && request.n_remaining_scheduling_attempts == 0)) {
            //     if report.error_cause().is_none() {
            //     debug!(
            //         "Request {} was successfully allocated: {:?}",
            //         request.request_id, report
            //     );
            // }
            // else {
            //     debug!("Request was not allocated but ")
            // }
            //     // Request was successfully allocated.

            if report.error_cause().is_none() {
                // Keep the result for when
                // the time is right.
                let batched_report = BatchedReport {
                    request_id: request.request_id,
                    report,
                };

                // If n_remaining_scheduling_attempts is 0, we will release the
                // report right away, at the end of the current call to
                // `schedule_batch`.
                let target_scheduling_interval = self
                    .current_scheduling_interval
                    + request.n_remaining_scheduling_attempts;

                self.delayed_reports
                    .entry(target_scheduling_interval)
                    .or_default()
                    .push(batched_report);
            } else {
                // Keep the request for the batch phase.
                unallocated_requests.push(request);
            }
        }

        Ok(unallocated_requests)
    }

    /// Release budget for the given epoch. This is a no-op when the epoch's unlocked budget has reached the capacity.
    fn release_budget(&mut self, epoch: usize) -> Result<()> {
        let filter_id = FilterId::C(epoch);
        self.pds.initialize_filter_if_necessary(filter_id.clone())?;
        self.pds
            .filter_storage
            .release(&filter_id, self.eps_c_per_release)?;
        info!(
            "Released budget for epoch {}. Filter state: {:?}",
            epoch,
            self.pds.filter_storage.storage.filters.get(&filter_id)
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // use common::logging;
    use log::info;
    use log4rs;

    use super::*;
    use crate::{
        events::{ppa_event::PpaEvent, traits::EventUris},
        pds::epoch_pds::StaticCapacities,
        queries::{
            ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector},
            traits::ReportRequestUris,
        },
    };

    #[test]
    fn schedule_one_batch() -> Result<(), anyhow::Error> {
        log4rs::init_file("logging_config.yaml", Default::default())?;

        let capacities = StaticCapacities::new(
            PureDPBudget::Epsilon(10.0),
            PureDPBudget::Epsilon(4.0),
            PureDPBudget::Epsilon(10.0),
            PureDPBudget::Epsilon(10.0),
        );

        let mut batch_pds = BatchPrivateDataService::new(capacities, 2)?;

        info!("Registering events");

        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris::mock(),
            filter_data: 1,
        };

        batch_pds.register_event(event1.clone())?;

        // Request that will be answered in the first scheduling attempt.
        batch_pds.register_report_request(BatchedRequest::new(
            1,
            1,
            PpaHistogramRequest::new(
                1,
                1,
                1.0,
                1.0,
                1.0,
                5,
                PpaRelevantEventSelector {
                    report_request_uris: ReportRequestUris::mock(),
                    is_matching_event: Box::new(|_: u64| true),
                },
            )?,
        )?)?;

        // Another request with one scheduling attempt.
        batch_pds.register_report_request(BatchedRequest::new(
            2,
            1,
            PpaHistogramRequest::new(
                1,
                1,
                1.0,
                1.0,
                1.0,
                5,
                PpaRelevantEventSelector {
                    report_request_uris: ReportRequestUris::mock(),
                    is_matching_event: Box::new(|_: u64| true),
                },
            )?,
        )?)?;

        // A request that will try two scheduling attempts. It requests too much
        // so should wait for more budget to be released.
        batch_pds.register_report_request(BatchedRequest::new(
            3,
            2,
            PpaHistogramRequest::new(
                1,
                1,
                1.0,
                1.0,
                1.0,
                5,
                PpaRelevantEventSelector {
                    report_request_uris: ReportRequestUris::mock(),
                    is_matching_event: Box::new(|_: u64| true),
                },
            )?,
        )?)?;

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 2);

        info!("Reports: {:?}", reports);

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 1);
        info!("Reports again: {:?}", reports);

        // TODO: check ull reports, etc.

        Ok(())
    }
}
