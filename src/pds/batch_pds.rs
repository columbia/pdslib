use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem::take,
    vec,
};

use anyhow::{bail, Result};
use log::{debug, info, warn};

use super::{epoch_pds::FilterId, utils::PpaCapacities};
use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::{ppa_event::PpaEvent, traits::Event},
    pds::{epoch_pds::PdsReport, utils::PpaPds},
    queries::{ppa_histogram::PpaHistogramRequest, traits::EpochReportRequest},
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

    /// Cache the most recent result received for this request, even if it was
    /// a null report.
    report: Option<PdsReport<PpaHistogramRequest>>,
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
            report: None,
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
    /// TODO: formalize a bit more the invariants, use  HashSet<u64>, or
    /// connect to time?
    pub epochs: Option<(usize, usize)>,

    /// List of all the different sources that appear in each epoch.
    pub sources_per_epoch: HashMap<usize, HashSet<String>>,

    /// Amount of c-filter budget to be released per scheduling interval.
    pub eps_c_per_release: f64,

    /// Copy of the capacities passed to pds
    pub capacities: PpaCapacities,

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

        let pds = PpaPds::new(capacities.clone())?;

        Ok(BatchPrivateDataService {
            current_scheduling_interval: 0,
            new_pending_requests: vec![],
            batched_requests: vec![],
            delayed_reports: HashMap::new(),
            eps_c_per_release,
            epochs: None,
            sources_per_epoch: HashMap::new(),
            capacities,
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

        // Update the sources for that epoch
        let source = event.event_uris().source_uri.clone();
        self.sources_per_epoch
            .entry(epoch)
            .or_default()
            .insert(source);

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

        // We are entering a new scheduling interval. Decrement the number
        // of remaining attempts for all requests in the system..
        for request in &mut self.batched_requests {
            request.n_remaining_scheduling_attempts -= 1;
        }
        for request in &mut self.new_pending_requests {
            request.n_remaining_scheduling_attempts -= 1;
        }

        debug!(
            "\n\n 1. Starting initialization phase. Existing requests: {:?}",
            self.batched_requests
        );
        let batched_requests = take(&mut self.batched_requests);
        let unallocated_requests =
            self.initialization_phase(batched_requests)?;
        self.batched_requests.extend(unallocated_requests);

        debug!(
            "\n\n 2. Starting online phase. New requests: {:?}",
            self.new_pending_requests
        );
        let new_requests = take(&mut self.new_pending_requests);
        let unallocated_requests = self.online_phase(new_requests)?;
        self.batched_requests.extend(unallocated_requests);

        // Any request with 0 remaining attempts will be answered here and
        // removed from the batch.
        debug!(
            "\n\n 3. Starting batch phase. Batch: {:?}",
            self.batched_requests
        );
        let batched_requests = take(&mut self.batched_requests);
        let unallocated_requests = self.batch_phase(batched_requests)?;
        self.batched_requests.extend(unallocated_requests);

        // Take all the reports that are ready to be released.
        let reports = self
            .delayed_reports
            .remove(&self.current_scheduling_interval)
            .unwrap_or_default();

        self.current_scheduling_interval += 1;

        Ok(reports)
    }

    /// Unlock fresh eps_c, enable imp quota with fresh capacity, and try to
    /// allocate requests from the previous batch.
    fn initialization_phase(
        &mut self,
        batched_requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        // Just try all the past epochs since our experiments just have a
        // few. Could eventually discard epochs that have reached their
        // lifetime if that becomes a bottleneck.
        if let Some((start, end)) = self.epochs {
            for epoch in start..=end {
                self.release_budget(epoch)?;

                // Reset imp quota for all sources in the epoch
                if let Some(sources) = self.sources_per_epoch.get(&epoch) {
                    for source in sources {
                        let filter_id =
                            FilterId::QSource(epoch, source.clone());
                        self.pds.filter_storage.remove(&filter_id)?;
                        self.pds.initialize_filter_if_necessary(
                            filter_id.clone(),
                        )?;
                    }
                }
            }
        }

        let unallocated_requests = self.try_allocate(batched_requests)?;
        Ok(unallocated_requests)
    }

    /// Browse `new_requests` one by one, try to allocate under
    /// regular quotas. Stores allocated requests for delayed response. Returns
    /// a list of unallocated requests.
    fn online_phase(
        &mut self,
        new_requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        let unallocated_requests = self.try_allocate(new_requests)?;
        Ok(unallocated_requests)
    }

    /// Disable the imp quotas, sort the requests, and try to allocate them.
    /// Stores allocated requests for delayed response. Returns a list of
    /// unallocated requests.
    fn batch_phase(
        &mut self,
        batched_requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        //  next, reach out to the filters to deactivate qimp or set the
        // capacity to infinity. Let's keep a fixed qconv for now.

        if let Some((start, end)) = self.epochs {
            for epoch in start..=end {
                // Set all imp quotas to infinity for all sources in the epoch
                if let Some(sources) = self.sources_per_epoch.get(&epoch) {
                    for source in sources {
                        let filter_id =
                            FilterId::QSource(epoch, source.clone());

                        // TODO: initialize if necessary, but let's see if we
                        // get an error first.
                        self.pds
                            .filter_storage
                            .set_capacity_to_infinity(&filter_id)?;

                        debug!(
                            "Set filter {:?} to infinite capacity. Filter state: {:?}",
                            filter_id,
                            self.pds.filter_storage.storage.filters.get(&filter_id)
                        );
                    }
                }
            }
        }

        // TODO(P1): actually re-sort after every successful allocation? Sounds
        // pretty expensive, but can pass a param to try_allocate.
        let sorted_batched_requests = self.sort_batch(batched_requests)?;

        // Try to allocate the requests.
        let unallocated_requests =
            self.try_allocate(sorted_batched_requests)?;

        // Requests with 0 remaining attemps will be answered with a null.
        // Put other unallocated requests back into the batch.
        let mut remaining_unallocated_requests = vec![];
        for request in unallocated_requests {
            if request.n_remaining_scheduling_attempts == 0 {
                if let Some(report) = request.report {
                    let batched_report = BatchedReport {
                        request_id: request.request_id,
                        report,
                    };
                    self.delayed_reports
                        .entry(self.current_scheduling_interval)
                        .or_default()
                        .push(batched_report);
                } else {
                    bail!(
                        "Request {:?} was not allocated and has no report. This should not happen.",
                        request
                    );
                }
            } else {
                remaining_unallocated_requests.push(request);
            }
        }

        Ok(remaining_unallocated_requests)
    }

    /// Stores allocated requests for delayed response. Returns a list of
    /// unallocated requests.
    fn try_allocate(
        &mut self,
        requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        // Go through requests one by one and try to allocate them.
        let mut unallocated_requests = vec![];
        for mut request in requests {
            let report = self.pds.compute_report(&request.request)?;

            debug!("Report for request {}: {:?}", request.request_id, report);

            if report.error_cause().is_none() {
                debug!(
                    "Request {} was successfully allocated: {:?}",
                    request.request_id, report
                );

                // Keep the result for when the time is right.
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

                debug!(
                    "Target scheduling interval: {}",
                    target_scheduling_interval
                );

                self.delayed_reports
                    .entry(target_scheduling_interval)
                    .or_default()
                    .push(batched_report);
            } else {
                // Keep the request for later. Cache the report in case we need
                // it.
                request.report = Some(report);
                unallocated_requests.push(request);
            }
        }

        Ok(unallocated_requests)
    }

    /// Release budget for the given epoch. This is a no-op when the epoch's
    /// unlocked budget has reached the capacity.
    fn release_budget(&mut self, epoch: usize) -> Result<()> {
        let filter_id = FilterId::C(epoch);
        self.pds.initialize_filter_if_necessary(filter_id.clone())?;
        self.pds
            .filter_storage
            .release(&filter_id, self.eps_c_per_release)?;
        debug!(
            "Released budget for epoch {}. Filter state: {:?}",
            epoch,
            self.pds.filter_storage.storage.filters.get(&filter_id)
        );
        Ok(())
    }

    /// Sort the requests.
    fn sort_batch(
        &mut self,
        requests: Vec<BatchedRequest>,
    ) -> Result<Vec<BatchedRequest>> {
        // Problem: A request spans multiple epochs. So the total budget an
        // impression site received so far is not a scalar, it's a vector over
        // epochs. Simple heuristic: just add up all epochs so we reduce
        // to a scalar and can sort sources. Could also take the average. But
        // over which set of epochs? Maybe all the epochs covered by requests in
        // the batch. Could go beyond, like all epochs ever, to optimize for
        // fairness over time.

        let mut all_sources = HashSet::new();
        for request in &requests {
            let source_uris = request.request.report_uris().source_uris;
            all_sources.extend(source_uris);
        }
        debug!("Sources across all requests: {:?}", all_sources);

        let mut all_epochs = HashSet::new();
        for request in &requests {
            let epoch_ids = request.request.epoch_ids();
            all_epochs.extend(epoch_ids);
        }
        debug!("Epochs across all requests: {:?}", all_epochs);

        let mut budget_per_source: HashMap<String, f64> = HashMap::new();
        for source in all_sources {
            let mut source_total_budget = 0.0;
            for epoch in &all_epochs {
                let filter_id = FilterId::QSource(*epoch, source.clone());
                self.pds.initialize_filter_if_necessary(filter_id.clone())?;
                let consumed_budget =
                    self.pds.filter_storage.consumed_budget(&filter_id)?;
                source_total_budget += consumed_budget;
            }
            budget_per_source.insert(source.clone(), source_total_budget);
        }
        debug!("Budget per source: {:?}", budget_per_source);

        // Another problem: it sounds tighter to look at the actual individual
        // budget, instead of the requested budget. Because IDP optimizations
        // tell us that sometimes a request actually consumes zero budget, so it
        // should probably be ordered first. It's just a bit weird
        // because we would need to cache `source_losses` or call
        // `compute_epoch_source_losses`. Cheap optimization: use the
        // list of source IDs to approximate.

        let mut requests_by_source: HashMap<String, Vec<&BatchedRequest>> =
            HashMap::new();

        for request in &requests {
            let source_uris = &request.request.report_uris().source_uris;
            for source in source_uris {
                requests_by_source
                    .entry(source.clone())
                    .or_default()
                    .push(request);
            }
        }

        debug!("Requests by source: {:?}", requests_by_source);

        // Idea: if we allocated that request, what would be the new value for
        // `budget_per_source`?
        let sorted_requests = vec![];

        Ok(sorted_requests)
    }
}

#[cfg(test)]
mod tests {
    // use common::logging;

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
            PureDPBudget::Epsilon(5.0),
            PureDPBudget::Epsilon(10.0),
            PureDPBudget::Epsilon(2.0),
        );

        let mut batch_pds = BatchPrivateDataService::new(capacities, 2)?;

        debug!("Registering events");

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
                1.1,
                5,
                PpaRelevantEventSelector {
                    report_request_uris: ReportRequestUris::mock(),
                    is_matching_event: Box::new(|_: u64| true),
                },
            )?,
        )?)?;

        // Another request with one scheduling attempt. But it doesn't go
        // through the online phase because of qimp, instead it has to wait
        // until the batch phase for the quotas to be disabled.
        batch_pds.register_report_request(BatchedRequest::new(
            2,
            1,
            PpaHistogramRequest::new(
                1,
                1,
                1.0,
                1.0,
                1.2,
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
                1.3,
                5,
                PpaRelevantEventSelector {
                    report_request_uris: ReportRequestUris::mock(),
                    is_matching_event: Box::new(|_: u64| true),
                },
            )?,
        )?)?;

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 2);

        debug!("Reports: {:?}", reports);

        for report in reports {
            assert!(
                report.report.error_cause().is_none(),
                "Report should not have an error cause. Got: {:?}",
                report.report.error_cause()
            );
        }

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 1);
        debug!("Reports again: {:?}", reports);

        assert!(
            reports[0].report.error_cause().is_none(),
            "Report should not have an error cause. Got: {:?}",
            reports[0].report.error_cause()
        );

        Ok(())
    }
}
