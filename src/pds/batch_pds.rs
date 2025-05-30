use core::panic;
use std::{
    borrow::Borrow,
    cmp::Ordering::{Greater, Less},
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem::take,
    vec,
};

use anyhow::Result;
use log::debug;

use super::{
    private_data_service::{PdsReport, PrivateDataService},
    quotas::{PdsFilterStatus, StaticCapacities},
};
use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{Filter, FilterStatus, FilterStorage, ReleaseFilter},
    },
    events::traits::EventStorage,
    mechanisms::NoiseScale,
    pds::quotas::FilterId,
    queries::traits::EpochReportRequest,
};

#[derive(Debug)]
pub struct BatchedRequest<Q: EpochReportRequest> {
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
    request: Q,
}

impl<Q: EpochReportRequest> BatchedRequest<Q> {
    pub fn new(
        request_id: u64,
        n_scheduling_attempts: u64,
        request: Q,
    ) -> Self {
        if n_scheduling_attempts == 0 {
            // TODO(later): allow requests with 0 batch scheduling attempt for
            // real-time queries. But for now we only consider batched, where a
            // query goes through at least one batch phase.
            panic!("The request should have at least one scheduling attempt.");
        }

        BatchedRequest {
            request_id,
            n_remaining_scheduling_attempts: n_scheduling_attempts,
            request,
        }
    }
}

/// [Experimental] Batch wrapper for private data service.
// pub struct BatchPrivateDataService<U: Uri = String, B: Budget = PureDPBudget>
// {
pub struct BatchPrivateDataService<Q, FS, ES, ERR>
where
    Q: EpochReportRequest,
    Q::Report: Clone,
    FS: FilterStorage<
        Budget = PureDPBudget,
        FilterId = FilterId<Q::EpochId, Q::Uri>,
        Capacities = StaticCapacities<
            FilterId<Q::EpochId, Q::Uri>,
            PureDPBudget,
        >,
    >,
    FS::Filter: ReleaseFilter<FS::Budget, Error = FS::Error>,
    ES: EventStorage<Event = Q::Event>,
    ERR: From<FS::Error> + From<ES::Error>,
{
    /// Current scheduling interval.
    /// Used to release budget for the c-filter.
    pub current_scheduling_interval: u64,

    /// Queries that arrived during the current interval.
    pub new_pending_requests: Vec<BatchedRequest<Q>>,

    /// Queries that are still waiting.
    pub batched_requests: Vec<BatchedRequest<Q>>,

    /// Reports for requests that have already been answered but need to wait
    /// for more scheduling intervals until they can be released.
    /// Grouped by scheduling interval at the end of which they will be
    /// released.
    pub delayed_reports: HashMap<u64, Vec<BatchedReport<Q>>>,

    /// Epochs present in the system
    /// Range of epochs from start to end (included).
    /// TODO: formalize a bit more the invariants, use  HashSet<u64>, or
    /// connect to time?
    pub epochs: Option<(Q::EpochId, Q::EpochId)>,

    /// List of all the different sources that appear in each epoch.
    pub sources_per_epoch: HashMap<Q::EpochId, HashSet<Q::Uri>>,

    /// Amount of c-filter budget to be released per scheduling interval.
    pub eps_c_per_release: FS::Budget,

    /// NOTE: these filters are not actually directly visible to a querier,
    /// because of report identifiers, to clarify.
    pub public_filters: FS,

    /// Base private data service.
    /// Filters need to have functionality to unlock budget.
    pub pds: PrivateDataService<Q, FS, ES, ERR>,
}
// TODO: time release. Maybe lives outside of pdslib.

/// Report for a batched request. Guaranteed to be returned after the number of
/// scheduling attempts the request specified.
#[derive(Debug)]
pub struct BatchedReport<Q: EpochReportRequest> {
    /// The request that asked for this report, potentially a long time ago.
    pub request_id: u64,

    /// The report answering that request.
    pub report: PdsReport<Q>,
}

impl<Q, FS, ES, ERR> BatchPrivateDataService<Q, FS, ES, ERR>
where
    Q: EpochReportRequest,
    Q::Report: Clone,
    FS: FilterStorage<
        Budget = PureDPBudget,
        FilterId = FilterId<Q::EpochId, Q::Uri>,
        Capacities = StaticCapacities<
            FilterId<Q::EpochId, Q::Uri>,
            PureDPBudget,
        >,
    >,
    FS::Filter: ReleaseFilter<FS::Budget, Error = FS::Error>,
    ES: EventStorage<Event = Q::Event>,
    ERR: From<FS::Error> + From<ES::Error>,
{
    /// Create a new batch private data service.
    pub fn new(
        pds: PrivateDataService<Q, FS, ES, ERR>,
        n_releases: usize,
    ) -> Result<Self, ERR> {
        let capacities = pds.core.filter_storage.capacities().clone();

        // Release the c-filter over T scheduling intervals.
        let eps_c_per_release = match capacities.c {
            f64::INFINITY => {
                debug!("C-filter has infinite capacity. Release is a no-op");
                0.0
            }
            eps_c => eps_c / (n_releases as f64),
        };

        debug!(
            "BatchPDS: capacities after dividing by {n_releases} releases: {capacities:?}"
        );

        Ok(BatchPrivateDataService {
            pds,
            eps_c_per_release,
            public_filters: FS::new(capacities)?,

            current_scheduling_interval: 0,
            new_pending_requests: vec![],
            batched_requests: vec![],
            delayed_reports: HashMap::new(),
            epochs: None,
            sources_per_epoch: HashMap::new(),
        })
    }

    pub fn register_report_request(
        &mut self,
        request: BatchedRequest<Q>,
    ) -> Result<(), ERR> {
        // Update the sources for each epoch
        let sources = &request.request.report_uris().source_uris;
        for epoch in request.request.epoch_ids() {
            for source in sources {
                self.sources_per_epoch
                    .entry(epoch)
                    .or_default()
                    .insert(source.clone());
            }
        }

        self.new_pending_requests.push(request);
        Ok(())
    }

    pub fn schedule_batch(&mut self) -> Result<Vec<BatchedReport<Q>>, ERR> {
        debug!(
            "Scheduling batch for interval {}",
            self.current_scheduling_interval
        );

        let mut previous_batch = take(&mut self.batched_requests);
        let mut new_requests = take(&mut self.new_pending_requests);

        // We are starting a scheduling attempt. Decrement the number
        // of remaining attempts for all requests in the system.
        for request in &mut previous_batch {
            request.n_remaining_scheduling_attempts -= 1;
        }
        for request in &mut new_requests {
            request.n_remaining_scheduling_attempts -= 1;
        }

        // Previous batch gets the first shot.
        let unallocated_from_previous_batch =
            self.initialization_phase(previous_batch)?;

        // New online queries try next.
        let unallocated_new_requests = self.online_phase(new_requests)?;

        // Put all the unallocated requests into the batch.
        let mut batched_requests = vec![];
        batched_requests.extend(unallocated_from_previous_batch);
        batched_requests.extend(unallocated_new_requests);

        // Requests with 0 remaining attempts will be answered and removed from
        // the batch.
        let unallocated_requests = self.batch_phase(batched_requests)?;

        // Store the batch for next scheduling interval.
        self.batched_requests = unallocated_requests;

        // Take all the reports that are ready to be released.
        let reports = self
            .delayed_reports
            .remove(&self.current_scheduling_interval)
            .unwrap_or_default();

        self.current_scheduling_interval += 1;

        Ok(reports)
    }

    fn set_imp_quota_capacity(
        &mut self,
        epoch: Q::EpochId,
        capacity: PureDPBudget,
    ) -> Result<(), ERR> {
        if let Some(sources) = self.sources_per_epoch.get(&epoch) {
            let filter_ids = sources
                .iter()
                .map(|source| FilterId::QSource(epoch, source.clone()))
                .collect::<Vec<_>>();
            self.initialize_filters(filter_ids.iter())?;

            let pds_filter_storage = &mut self.pds.core.filter_storage;

            for filter_id in filter_ids {
                pds_filter_storage.edit_filter_or_new(&filter_id, |f| {
                    f.set_capacity(capacity)
                })?;

                self.public_filters.edit_filter_or_new(&filter_id, |f| {
                    f.set_capacity(capacity)
                })?;
            }
        }

        Ok(())
    }

    /// Unlock fresh eps_c, enable imp quota with fresh capacity, and try to
    /// allocate requests from the previous batch.
    fn initialization_phase(
        &mut self,
        batched_requests: Vec<BatchedRequest<Q>>,
    ) -> Result<Vec<BatchedRequest<Q>>, ERR> {
        let imp_capacity = self.pds.core.filter_storage.capacities().qsource;

        // Just try all the past epochs since our experiments just have a
        // few. Could eventually discard epochs that have reached their
        // lifetime if that becomes a bottleneck.
        let epoch_ids =
            self.sources_per_epoch.keys().copied().collect::<Vec<_>>();
        for epoch_id in epoch_ids {
            // Release (unlock) a fraction of this epoch's global budget
            self.release_budget(epoch_id)?;

            // Turn on the impression-site quota
            self.set_imp_quota_capacity(epoch_id, imp_capacity)?;
        }

        let unallocated_requests =
            self.try_allocate(batched_requests, false)?;
        Ok(unallocated_requests)
    }

    /// Browse `new_requests` one by one, try to allocate under
    /// regular quotas. Stores allocated requests for delayed response. Returns
    /// a list of unallocated requests.
    fn online_phase(
        &mut self,
        new_requests: Vec<BatchedRequest<Q>>,
    ) -> Result<Vec<BatchedRequest<Q>>, ERR> {
        let unallocated_requests = self.try_allocate(new_requests, false)?;
        Ok(unallocated_requests)
    }

    /// Disable the imp quotas, sort the requests, and try to allocate them.
    /// Stores allocated requests for delayed response. Returns a list of
    /// unallocated requests.
    fn batch_phase(
        &mut self,
        batched_requests: Vec<BatchedRequest<Q>>,
    ) -> Result<Vec<BatchedRequest<Q>>, ERR> {
        let epoch_ids =
            self.sources_per_epoch.keys().copied().collect::<Vec<_>>();
        for epoch_id in epoch_ids {
            // Turn the impression-site quotas off by setting their capacity
            // to infinity.
            self.set_imp_quota_capacity(epoch_id, f64::INFINITY)?;
        }

        // Repeatedly sort and try to allocate. Re-sort each time a request is
        // allocated. Exit the loop when no more request can be
        // allocated.
        let sorted_batched_requests = self.sort_batch(batched_requests)?;
        let mut k = sorted_batched_requests.len();
        let (mut unallocated_requests, mut allocated_index) =
            self.try_allocate_one(sorted_batched_requests, true)?;
        debug!("Tried allocating one request.");

        while let Some(i) = allocated_index {
            if i == k - 1 {
                // We allocated the last request. No need to sort again.
                debug!("Allocated all the requests we could.");
                break;
            }

            debug!(
                "Allocated request {i} from the unallocated ones. Sorting the remaining requests and trying to allocate again."
            );
            let sorted_batched_requests =
                self.sort_batch(unallocated_requests)?;
            k = sorted_batched_requests.len();
            (unallocated_requests, allocated_index) =
                self.try_allocate_one(sorted_batched_requests, true)?;
        }

        Ok(unallocated_requests)
    }

    /// Just mimics `deduct_budget` but with non-IDP filters.
    /// And also does it across all epochs.
    /// TODO(P2): Could do it on a single epoch if that helps (checking on all
    /// epochs might simply return OOB every time but old epochs don't actually
    /// matter too much).
    #[allow(clippy::type_complexity)]
    fn deduct_budget(
        &mut self,
        request: &Q,
        dry_run: bool,
    ) -> Result<PdsFilterStatus<FilterId<Q::EpochId, Q::Uri>>, ERR> {
        let uris = request.report_uris();

        // Case 3 from Cookie Monster only.
        let NoiseScale::Laplace(noise_scale) = request.noise_scale();
        let loss = request.report_global_sensitivity() / noise_scale;

        let mut filter_ids = vec![];
        for epoch_id in request.epoch_ids() {
            // Build the filter IDs for NC, C and QTrigger. Qsource has the same
            // loss here.
            for query_uri in &uris.querier_uris {
                filter_ids.push(FilterId::Nc(epoch_id, query_uri.clone()));
            }
            filter_ids
                .push(FilterId::QTrigger(epoch_id, uris.trigger_uri.clone()));
            filter_ids.push(FilterId::C(epoch_id));

            for source in &uris.source_uris {
                filter_ids.push(FilterId::QSource(epoch_id, source.clone()));
            }
        }

        self.initialize_filters(filter_ids.iter())?;

        // Try to consume the privacy loss from the filters
        let mut oob_filters = vec![];
        for fid in filter_ids {
            let filter_status = match dry_run {
                true => self.public_filters.can_consume(&fid, &loss)?,
                false => self.public_filters.try_consume(&fid, &loss)?,
            };

            if filter_status == FilterStatus::OutOfBudget {
                oob_filters.push(fid);
            }
        }

        // If any filter was out of budget, the whole operation is marked as out
        // of budget.
        if !oob_filters.is_empty() {
            return Ok(PdsFilterStatus::OutOfBudget(oob_filters));
        }
        Ok(PdsFilterStatus::Continue)
    }

    /// After sending a request for allocation by calling `compute_report`, keep
    /// track of public information that was in the request. We don't peek
    /// into the result of the report itself or the state of the filters. Maybe
    /// the request was not allocated after all.
    fn update_allocation_statistics(&mut self, request: &Q) -> Result<(), ERR> {
        self.deduct_budget(request, false)?;
        Ok(())
    }

    fn can_probably_allocate(&mut self, request: &Q) -> Result<bool, ERR> {
        let filter_status = self.deduct_budget(request, true)?;
        match filter_status {
            PdsFilterStatus::Continue => Ok(true),
            PdsFilterStatus::OutOfBudget(oob) => {
                debug!(
                    "Request {request:?} might be out of budget for filters {oob:?}, so we can't guarantee it will run.",
                );
                Ok(false)
            }
        }
    }

    fn send_report_for_release(
        &mut self,
        request: &BatchedRequest<Q>,
        report: PdsReport<Q>,
    ) {
        debug!("Request {} got report {:?}", request.request_id, report);

        // Keep the result for when the time is right.
        let batched_report = BatchedReport {
            request_id: request.request_id,
            report,
        };

        // If n_remaining_scheduling_attempts is 0, we will release the
        // report right away, at the end of the current call to
        // `schedule_batch`.
        let target_scheduling_interval = self.current_scheduling_interval
            + request.n_remaining_scheduling_attempts;
        debug!("Target scheduling interval: {target_scheduling_interval}");

        self.delayed_reports
            .entry(target_scheduling_interval)
            .or_default()
            .push(batched_report);
    }

    /// Stores allocated requests for delayed response. Returns a list of
    /// unallocated requests.
    fn try_allocate(
        &mut self,
        requests: Vec<BatchedRequest<Q>>,
        allocate_final_attempts: bool,
    ) -> Result<Vec<BatchedRequest<Q>>, ERR> {
        // Go through requests one by one and try to allocate them.
        let mut unallocated_requests = vec![];
        for request in requests {
            let querier_uri = &request.request.report_uris().querier_uris[0];

            if (allocate_final_attempts
                && request.n_remaining_scheduling_attempts == 0)
                || self.can_probably_allocate(&request.request)?
            {
                debug!(
                    "Request {} can probably be allocated: {request:?}",
                    request.request_id
                );

                // pre-initialize filters to unlock non-C filters
                self.initialize_filters_for_request(&request.request)?;

                // Compute the actual report. It might be null though.
                let mut report = self.pds.compute_report(&request.request)?;
                let report = report.remove(querier_uri).unwrap();

                if !report.oob_filters.is_empty() {
                    for filter_id in report.oob_filters.iter() {
                        if let FilterId::QSource(_, _) = filter_id {
                            // Qimp should never block a request if we have
                            // perfect upper bounds for the public filters.
                            panic!(
                                "Request {} was not allocated: {:?}. Final attempt? {}",
                                request.request_id, report.oob_filters, allocate_final_attempts
                            );
                        }
                    }
                }

                self.update_allocation_statistics(&request.request)?;

                // Keep the result for when the time is right.
                self.send_report_for_release(&request, report);
            } else {
                // TODO(P1): compute the report at the end if None?
                unallocated_requests.push(request);
            }
        }

        Ok(unallocated_requests)
    }

    /// Browse the requests one by one, try to allocate them. If we allocate a
    /// request, stop trying allocating and return the index of the allocated
    /// request. Otherwise, return None. Either way, also return all the
    /// unallocated requests.
    fn try_allocate_one(
        &mut self,
        mut requests: Vec<BatchedRequest<Q>>,
        allocate_final_attempts: bool,
    ) -> Result<(Vec<BatchedRequest<Q>>, Option<usize>), ERR> {
        let mut i = 0;
        let mut unallocated_requests = vec![];

        while !requests.is_empty() {
            let request = requests.remove(0);
            let unallocated_request =
                self.try_allocate(vec![request], allocate_final_attempts)?;
            if unallocated_request.is_empty() {
                // We successfully allocated the request.
                // Keep all the other requests as unallocated.
                unallocated_requests.extend(requests);
                return Ok((unallocated_requests, Some(i)));
            }
            unallocated_requests.extend(unallocated_request);
            i += 1;
        }

        Ok((unallocated_requests, None))
    }

    /// Release budget for the given epoch. This is a no-op when the epoch's
    /// unlocked budget has reached the capacity.
    fn release_budget(&mut self, epoch: Q::EpochId) -> Result<(), ERR> {
        let filter_id = FilterId::C(epoch);

        self.pds
            .core
            .filter_storage
            .edit_filter_or_new(&filter_id, |f| {
                f.release(&self.eps_c_per_release)
            })?;

        self.public_filters.edit_filter_or_new(&filter_id, |f| {
            f.release(&self.eps_c_per_release)
        })?;

        Ok(())
    }

    /// Sort the requests. Start with the request that has the smallest
    /// beneficiary, break ties by request budget.
    ///
    /// NOTE: this is just one possible heuristic.
    /// Other ideas: if we allocated that request, what would be the new value
    /// for `budget_per_source` for each source?
    /// Then, try to minimize the maximum value across all sources, and break
    /// ties by request epsilon. This is not perfect since it could allocate to
    /// sources that are already quite big, but not the biggest.
    /// But maximizing the minimum allocation doesn't look too great either. A
    /// large request can be allocated if it also boosts a small one? A request
    /// that asks for zero budget is not prioritized?
    /// Some problems with max min or min max: it sounds tighter to look at the
    /// actual individual budget, instead of the requested budget. Because
    /// IDP optimizations tell us that sometimes a request actually consumes
    /// zero budget, so it should probably be ordered first. It's just a bit
    /// weird because we would need to cache `source_losses` or call
    /// `compute_epoch_source_losses`. Cheap optimization: use the
    /// list of source IDs to approximate.
    fn sort_batch(
        &mut self,
        requests: Vec<BatchedRequest<Q>>,
    ) -> Result<Vec<BatchedRequest<Q>>, ERR> {
        // Problem: A request spans multiple epochs. So the total budget an
        // impression site received so far is not a scalar, it's a vector over
        // epochs. Simple heuristic: just add up all epochs so we reduce
        // to a scalar and can sort sources. Could also take the average. But
        // over which set of epochs? Maybe all the epochs covered by requests in
        // the batch. Could go beyond, like all epochs ever, to optimize for
        // fairness over time.
        // Actually max sounds better, a bit more like DPF.

        let mut all_sources = HashSet::new();
        for request in &requests {
            let source_uris = &request.request.report_uris().source_uris;
            all_sources.extend(source_uris);
        }
        debug!("Sources across all requests: {all_sources:?}");

        let mut all_epochs = HashSet::new();
        for request in &requests {
            let epoch_ids = request.request.epoch_ids();
            all_epochs.extend(epoch_ids);
        }
        debug!("Epochs across all requests: {all_epochs:?}");

        let mut budget_per_source: HashMap<Q::Uri, FS::Budget> = HashMap::new();
        for source in &all_sources {
            let source = (*source).clone();
            let mut source_total_budget: f64 = 0.0;
            for epoch in &all_epochs {
                let filter_id = FilterId::QSource(*epoch, source.clone());

                let filter =
                    self.public_filters.get_filter_or_new(&filter_id)?;
                let consumed_budget =
                    filter.get_capacity()? - filter.remaining_budget()?;

                source_total_budget = source_total_budget.max(consumed_budget);
            }
            budget_per_source.insert(source, source_total_budget);
        }
        debug!("Budget per source: {budget_per_source:?}");

        let mut weighted_requests: Vec<(BatchedRequest<Q>, f64, f64)> = vec![];

        // For each request, find the minimum source budget across all sources.
        // So it r appears in both q1's list of requests and q2's list, since
        // we'll go through q1's list first we don't need to even remember about
        // q2.
        for request in requests {
            let mut min_source_budget = f64::MAX;
            let source_uris = &request.request.report_uris().source_uris;

            let NoiseScale::Laplace(noise_scale) =
                request.request.noise_scale();
            let requested_budget =
                request.request.report_global_sensitivity() / noise_scale;

            for source in source_uris.iter() {
                let source_budget = *budget_per_source.get(source).unwrap();
                if source_budget < min_source_budget {
                    min_source_budget = source_budget;
                }
            }

            weighted_requests.push((
                request,
                min_source_budget,
                requested_budget,
            ));
        }

        // Sort by weight.
        weighted_requests.sort_by(|a, b| {
            let (a_min_source_budget, a_request_budget) = (a.1, a.2);
            let (b_min_source_budget, b_request_budget) = (b.1, b.2);

            if a_min_source_budget < b_min_source_budget {
                Less
            } else if a_min_source_budget > b_min_source_budget {
                Greater
            } else {
                // If the minimum source budget is the same, sort by request
                // budget
                if a_request_budget <= b_request_budget {
                    Less
                } else {
                    Greater
                }
            }
        });

        debug!(
            "Requests and budgets after sorting: {:?}",
            weighted_requests
                .iter()
                .map(|(r, b, c)| (r.request_id, b, c))
                .collect::<Vec<_>>()
        );

        let sorted_requests =
            weighted_requests.into_iter().map(|(r, _, _)| r).collect();

        Ok(sorted_requests)
    }

    /// Given a request, calculate the list of filters that will be deducted
    /// from by PDS core, and pre-initialize them, so that all non-C filters
    /// are unlocked and act as regular filters.
    fn initialize_filters_for_request(
        &mut self,
        request: &Q,
    ) -> Result<(), ERR> {
        let uris = request.report_uris();

        // We (mis-)use PDS's filters_to_consume() method to get a list of
        // filters that will be deducted for this request.

        for epoch_id in request.epoch_ids() {
            let mut source_losses = HashMap::new();
            for source in &uris.source_uris {
                source_losses.insert(source.clone(), 0.0);
            }

            let filter_ids = self.pds.core.filters_to_consume(
                epoch_id,
                &0.0, // just set to 0, we only care about the filter IDs
                &source_losses,
                request.report_uris(),
            );
            self.initialize_filters(filter_ids.keys())?;
        }
        Ok(())
    }

    /// Given a list of filter IDs, initialize them in the filter storage,
    /// such that non-C filters are unlocked and act as regular filters.
    fn initialize_filters<'f, FID>(
        &mut self,
        filters: impl Iterator<Item = FID>,
    ) -> Result<(), ERR>
    where
        // accept both owned and borrowed FilterIDs
        FID: Borrow<&'f FilterId<Q::EpochId, Q::Uri>> + 'f,
        Q::EpochId: 'f, // required by borrow checker
        Q::Uri: 'f,
    {
        for filter_id in filters {
            let filter_id = filter_id.borrow();

            // Only C-filters should act as release filters. Nc, q-conv and
            // q-imp act as regular PureDPBudget filters.
            // As FilterStorage only supports storing one type of filter,
            // we fully unlock all other filter types before consuming.
            // As such, they act as regular non-release filters.
            let should_unlock = !matches!(filter_id, FilterId::C(_));

            // if we don't need to unlock, we don't need to do anything
            if !should_unlock {
                continue;
            }

            for filter_storage in
                [&mut self.pds.core.filter_storage, &mut self.public_filters]
            {
                filter_storage.edit_filter_or_new(filter_id, |f| {
                    // unlock the filter so it acts as a regular filter
                    f.release(&f64::INFINITY)
                })?;
            }
        }

        Ok(())
    }

    #[allow(dead_code)] // used in tests
    fn collect_request_ids(&self, requests: &[BatchedRequest<Q>]) -> Vec<u64> {
        requests.iter().map(|r| r.request_id).collect::<Vec<_>>()
    }

    #[allow(dead_code)] // used in tests
    fn collect_report_ids(&self, reports: &[BatchedReport<Q>]) -> Vec<u64> {
        reports.iter().map(|r| r.request_id).collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use log::info;

    use super::*;
    use crate::{
        budget::{
            hashmap_filter_storage::HashMapFilterStorage,
            release_filter::PureDPBudgetReleaseFilter,
        },
        events::{
            hashmap_event_storage::HashMapEventStorage,
            ppa_event::PpaEvent,
            traits::{Event, EventUris},
        },
        queries::{
            ppa_histogram::{
                PpaHistogramConfig, PpaHistogramRequest,
                PpaRelevantEventSelector,
            },
            traits::ReportRequestUris,
        },
        util::tests::init_default_logging,
    };

    fn event_storage_with_events<E: Event>(
        events: Vec<E>,
    ) -> HashMapEventStorage<E> {
        let mut event_storage = HashMapEventStorage::new();
        for event in events {
            event_storage.add_event(event).unwrap();
        }
        event_storage
    }

    #[test]
    fn schedule_one_batch() -> Result<()> {
        init_default_logging();

        let capacities = StaticCapacities::new(10.0, 5.0, 10.0, 4.0);

        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris::mock(),
            filter_data: 1,
        };
        let event_storage = event_storage_with_events(vec![event1]);

        let filter_storage: HashMapFilterStorage<PureDPBudgetReleaseFilter, _> =
            HashMapFilterStorage::new(capacities)?;
        let pds: PrivateDataService<_, _, _, anyhow::Error> =
            PrivateDataService::new(filter_storage, event_storage);
        let mut batch_pds = BatchPrivateDataService::new(pds, 2)?;

        let mut request_config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 1,
            attributable_value: 1.0,
            max_attributable_value: 1.0,
            requested_epsilon: 1.1,
            histogram_size: 5,
        };

        let mut report_uris = ReportRequestUris::mock();
        let querier_uri = &report_uris.querier_uris[0];
        report_uris.intermediary_uris.push(querier_uri.clone());

        let always_relevant_selector = || PpaRelevantEventSelector {
            report_request_uris: report_uris.clone(),
            is_matching_event: Box::new(|_: u64| true),
            bucket_intermediary_mapping: HashMap::from([(
                0,
                querier_uri.clone(),
            )]),
        };

        // Request that will be answered in the first scheduling attempt.
        batch_pds.register_report_request(BatchedRequest::new(
            1,
            1,
            PpaHistogramRequest::new(
                &request_config,
                always_relevant_selector(),
            )?,
        ))?;

        // Another request with one scheduling attempt. But it doesn't go
        // through the online phase because of qimp, instead it has to wait
        // until the batch phase for the quotas to be disabled.
        request_config.requested_epsilon = 1.2;
        batch_pds.register_report_request(BatchedRequest::new(
            2,
            1,
            PpaHistogramRequest::new(
                &request_config,
                always_relevant_selector(),
            )?,
        ))?;

        // A request that will try two scheduling attempts. It requests too much
        // so should wait for more budget to be released.
        request_config.requested_epsilon = 1.3;
        batch_pds.register_report_request(BatchedRequest::new(
            3,
            2,
            PpaHistogramRequest::new(
                &request_config,
                always_relevant_selector(),
            )?,
        ))?;

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 2);

        debug!("Reports: {reports:?}");

        for report in reports {
            assert!(
                report.report.oob_filters.is_empty(),
                "Report should not have any OOB filters. Got: {:?}",
                report.report.oob_filters
            );
        }

        let reports = batch_pds.schedule_batch()?;
        assert_eq!(reports.len(), 1);
        debug!("Reports again: {reports:?}");

        assert!(
            reports[0].report.oob_filters.is_empty(),
            "Report should not have any OOB filters. Got: {:?}",
            reports[0].report.oob_filters
        );

        Ok(())
    }

    /// Test that mimics the example from the paper that motivates batching.
    #[test]
    fn utilization_example() -> Result<()> {
        init_default_logging();

        let capacities = StaticCapacities::new(1.0, 10.0, 1.0, 5.0);

        let mut trigger_uris = vec![];
        for i in 1..=9 {
            trigger_uris.push(format!("shoes-{i}.ex"));
        }

        // Event relevant to all the shoes websites. Could also register 10
        // different events, with one querier each.
        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris {
                source_uri: "news.ex".to_string(),
                trigger_uris: trigger_uris.clone(),
                querier_uris: trigger_uris.clone(),
                intermediary_uris: vec![],
            },
            filter_data: 1,
        };
        let event2 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris {
                source_uri: "blog.ex".to_string(),
                trigger_uris: vec!["hats-1.ex".to_string()],
                querier_uris: vec!["hats-1.ex".to_string()],
                intermediary_uris: vec![],
            },
            filter_data: 1,
        };

        let event_storage = event_storage_with_events(vec![event1, event2]);

        // Using a single release here.
        let filter_storage: HashMapFilterStorage<PureDPBudgetReleaseFilter, _> =
            HashMapFilterStorage::new(capacities)?;
        let pds: PrivateDataService<_, _, _, anyhow::Error> =
            PrivateDataService::new(filter_storage, event_storage);
        let mut batch_pds = BatchPrivateDataService::new(pds, 1)?;

        let mut request_config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 1,
            attributable_value: 1.0,
            max_attributable_value: 1.0,
            requested_epsilon: 99.9, // will be set per request
            histogram_size: 5,
        };

        let always_valid_selector =
            |uris: ReportRequestUris<String>| PpaRelevantEventSelector {
                report_request_uris: uris,
                is_matching_event: Box::new(|_: u64| true),
                bucket_intermediary_mapping: HashMap::new(),
            };

        // Every single conversion sites gets a conversion.
        for i in 1..=9 {
            request_config.requested_epsilon = 0.9 + 0.01 * i as f64;
            batch_pds.register_report_request(BatchedRequest::new(
                i,
                1,
                PpaHistogramRequest::new(
                    &request_config,
                    always_valid_selector(ReportRequestUris {
                        trigger_uri: format!("shoes-{i}.ex"),
                        source_uris: vec!["news.ex".to_string()],
                        querier_uris: vec![format!("shoes-{i}.ex")],
                        intermediary_uris: vec![],
                    }),
                )?,
            ))?;
        }

        request_config.requested_epsilon = 0.96;
        batch_pds.register_report_request(BatchedRequest::new(
            6,
            1,
            PpaHistogramRequest::new(
                &request_config,
                always_valid_selector(ReportRequestUris {
                    trigger_uri: "hats-1.ex".to_string(),
                    source_uris: vec!["blog.ex".to_string()],
                    querier_uris: vec!["hats-1.ex".to_string()],
                    intermediary_uris: vec![],
                }),
            )?,
        ))?;

        // We open up `schedule_batch` to check step by step.
        let mut previous_batch = take(&mut batch_pds.batched_requests);
        let mut new_requests = take(&mut batch_pds.new_pending_requests);
        for request in &mut previous_batch {
            request.n_remaining_scheduling_attempts -= 1;
        }
        for request in &mut new_requests {
            request.n_remaining_scheduling_attempts -= 1;
        }

        assert!(previous_batch.is_empty());

        let unallocated_from_previous_batch =
            batch_pds.initialization_phase(previous_batch)?;

        assert!(unallocated_from_previous_batch.is_empty());
        assert_eq!(new_requests.len(), 10);

        let unallocated_new_requests = batch_pds.online_phase(new_requests)?;

        // Because of qimp, news.ex can't accept all the queries. Also tried to
        // allocate in order.
        assert_eq!(
            batch_pds.collect_request_ids(&unallocated_new_requests),
            vec![6, 7, 8, 9]
        );

        let mut batched_requests = vec![];
        batched_requests.extend(unallocated_from_previous_batch);
        batched_requests.extend(unallocated_new_requests);

        let unallocated_requests = batch_pds.batch_phase(batched_requests)?;

        // Requests have to all allocated or answered with a null report.
        assert!(unallocated_requests.is_empty());

        // Take all the reports that are ready to be released.
        let reports = batch_pds
            .delayed_reports
            .remove(&batch_pds.current_scheduling_interval)
            .unwrap_or_default();

        batch_pds.current_scheduling_interval += 1;

        assert_eq!(reports.len(), 10);

        // No report should be null
        for report in &reports {
            assert!(
                report.report.oob_filters.is_empty(),
                "Report should not have an error cause. Got: {:?}",
                report.report.oob_filters
            );
        }

        debug!("Reports: {reports:?}");

        Ok(())
    }

    /// Start with the utilization example, but add more queries from different
    /// impsites. The system will not be able to allocate everyone. We want
    /// to verify that it is at least "fair" in the sense that it doesn't
    /// let a single site take all the budget.
    #[test]
    fn order_fairness() -> Result<()> {
        init_default_logging();

        let capacities = StaticCapacities::new(
            1.0,
            10.0, /* We'll do two releases, so not
                   * enough space for all the queries
                   * at the first attempt. */
            1.0,
            1.0, /* Also tighter quota for online phase. So the batch will
                  * have to decide what to do. Gotta be fair. */
        );

        // Event relevant to all the shoes websites. Could also register 10
        // different events, with one querier each.
        let mut trigger_uris = vec![];
        for i in 1..=10 {
            trigger_uris.push(format!("shoes-{i}.ex"));
        }

        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris {
                source_uri: "news.ex".to_string(),
                trigger_uris: trigger_uris.clone(),
                querier_uris: trigger_uris.clone(),
                intermediary_uris: vec![],
            },
            filter_data: 1,
        };

        // Site with a lot of requests, but not as many as news.ex.
        let mut trigger_uris = vec![];
        for i in 1..=10 {
            trigger_uris.push(format!("hats-{i}.ex"));
        }
        let event2 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: EventUris {
                source_uri: "blog.ex".to_string(),
                trigger_uris: trigger_uris.clone(),
                querier_uris: trigger_uris.clone(),
                intermediary_uris: vec![],
            },
            filter_data: 1,
        };

        let event_storage = event_storage_with_events(vec![event1, event2]);

        // Using a single release here.
        let filter_storage: HashMapFilterStorage<PureDPBudgetReleaseFilter, _> =
            HashMapFilterStorage::new(capacities)?;
        let pds: PrivateDataService<_, _, _, anyhow::Error> =
            PrivateDataService::new(filter_storage, event_storage);
        let mut batch_pds = BatchPrivateDataService::new(pds, 2)?;

        let mut request_config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 1,
            attributable_value: 1.0,
            max_attributable_value: 1.0,
            requested_epsilon: 99.9, // will be set per request
            histogram_size: 5,
        };

        // Every single conversion sites gets a conversion. But news.ex comes
        // first! If we only allocated online that could be terribly unfair for
        // blog.ex.
        for i in 1..=10 {
            let shoes_conv = format!("shoes-{i}.ex");

            request_config.requested_epsilon = if i == 3 {
                0.99 // We want this request to be smaller than the others in
                     // the tests.
            } else {
                0.99 + 0.0001 * i as f64
            };

            batch_pds.register_report_request(BatchedRequest::new(
                i,
                2, // Space for one more time. Easier to check the batch.
                PpaHistogramRequest::new(
                    &request_config,
                    PpaRelevantEventSelector {
                        report_request_uris: ReportRequestUris {
                            trigger_uri: shoes_conv.clone(),
                            source_uris: vec!["news.ex".to_string()],
                            querier_uris: vec![shoes_conv.clone()],
                            intermediary_uris: vec![],
                        },
                        is_matching_event: Box::new(|_: u64| true),
                        bucket_intermediary_mapping: HashMap::new(),
                    },
                )?,
            ))?;
        }

        for i in 1..=10 {
            let hats_conv = format!("hats-{i}.ex");

            request_config.requested_epsilon = 0.99 + 0.0001 * i as f64;

            batch_pds.register_report_request(BatchedRequest::new(
                10 + i,
                2,
                PpaHistogramRequest::new(
                    &request_config,
                    PpaRelevantEventSelector {
                        report_request_uris: ReportRequestUris {
                            trigger_uri: hats_conv.clone(),
                            source_uris: vec!["blog.ex".to_string()],
                            querier_uris: vec![hats_conv.clone()],
                            intermediary_uris: vec![],
                        },
                        is_matching_event: Box::new(|_: u64| true),
                        bucket_intermediary_mapping: HashMap::new(),
                    },
                )?,
            ))?;
        }

        let reports = batch_pds.schedule_batch()?;

        // No report should be released just yet
        assert_eq!(reports.len(), 0);

        info!(
            "Delayed reports after first scheduling: {:?}",
            batch_pds.delayed_reports
        );

        info!(
            "Batched requests after first scheduling: {:?}",
            batch_pds.collect_request_ids(&batch_pds.batched_requests)
        );

        // Only 5 reports should have been allocated from the released global
        // budget.
        assert_eq!(batch_pds.batched_requests.len(), 20 - 5);

        // Forcefully take the delayed reports to look at them.
        let allocated_reports = batch_pds
            .delayed_reports
            .remove(&batch_pds.current_scheduling_interval)
            .unwrap_or_default();

        // Requests should be balanced across both sources. For the 5th request,
        // break ties by smallest request.
        let mut report_ids = batch_pds.collect_report_ids(&allocated_reports);
        report_ids.sort();
        assert_eq!(report_ids, vec![1, 2, 3, 11, 12]);

        debug!("Reports: {:?}", reports);

        // Run a second batch.
        let reports = batch_pds.schedule_batch()?;

        // All the queries get answered. We already removed 5.
        assert_eq!(reports.len(), 15);

        // Only 5 reports should be non-null.
        let mut n_non_null_reports = 0;
        for report in &reports {
            if report.report.oob_filters.is_empty() {
                n_non_null_reports += 1;
            }
        }
        assert_eq!(n_non_null_reports, 5);
        debug!("Reports: {:?}", reports);

        Ok(())
    }
}
