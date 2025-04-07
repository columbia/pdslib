use std::{collections::HashMap, fmt::Debug, hash::Hash};

use log::debug;

use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{Budget, FilterCapacities, FilterStatus, FilterStorage},
    },
    events::traits::{
        EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector,
    },
    mechanisms::{NoiseScale, NormType},
    queries::traits::{
        EpochReportRequest, PassivePrivacyLossRequest, ReportRequestUris,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FilterId<
    E, // Epoch ID
    U, // URI
> {
    /// Non-collusion per-querier filter
    Nc(E, U /* querier URI */),
    /// Collusion filter (tracks overall privacy loss)
    C(E),
    /// Quota filter regulating c-filter consumption per trigger_uri
    QTrigger(E, U /* trigger URI */),
    /// Quota filter regulating c-filter consumption per source_uri
    QSource(E, U /* source URI */),
}

/// Struct containing the default capacity for each type of filter.
#[derive(Debug, Clone)]
pub struct StaticCapacities<FID, B> {
    pub nc: B,
    pub c: B,
    pub qtrigger: B,
    pub qsource: B,
    _phantom: std::marker::PhantomData<FID>,
}

impl<FID, B> StaticCapacities<FID, B> {
    pub fn new(nc: B, c: B, qtrigger: B, qsource: B) -> Self {
        Self {
            nc,
            c,
            qtrigger,
            qsource,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: Budget, E, U> FilterCapacities for StaticCapacities<FilterId<E, U>, B> {
    type FilterId = FilterId<E, U>;
    type Budget = B;
    type Error = anyhow::Error;

    fn capacity(
        &self,
        filter_id: &Self::FilterId,
    ) -> Result<Self::Budget, Self::Error> {
        match filter_id {
            FilterId::Nc(..) => Ok(self.nc.clone()),
            FilterId::C(..) => Ok(self.c.clone()),
            FilterId::QTrigger(..) => Ok(self.qtrigger.clone()),
            FilterId::QSource(..) => Ok(self.qsource.clone()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PdsFilterStatus<FID> {
    /// No filter was out budget, the atomic check passed for this epoch
    Continue,

    /// At least one filter was out of budget, the atomic check failed for this
    /// epoch. The ids of out-of-budget filters are stored in a vector if they
    /// are known. If an unspecified error causes the atomic check to fail,
    /// the vector can be empty.
    OutOfBudget(Vec<FID>),
}

impl<FID> Default for PdsFilterStatus<FID> {
    fn default() -> Self {
        Self::OutOfBudget(vec![])
    }
}

/// Epoch-based private data service, using generic filter
/// storage and event storage interfaces.
///
/// TODO(https://github.com/columbia/pdslib/issues/18): handle multiple queriers
/// instead of assuming that there is a single querier and using filter_id =
/// epoch_id
pub struct EpochPrivateDataService<
    FS: FilterStorage,
    ES: EventStorage,
    Q: EpochReportRequest,
    ERR: From<FS::Error> + From<ES::Error>,
> {
    /// Filter storage interface.
    pub filter_storage: FS,

    /// Event storage interface.
    pub event_storage: ES,

    /// Type of accepted queries.
    pub _phantom_request: std::marker::PhantomData<Q>,

    /// Type of errors.
    pub _phantom_error: std::marker::PhantomData<ERR>,
}

#[derive(Debug)]
pub enum PdsReportResult<Q: EpochReportRequest> {
    Regular(PdsReport<Q>),
    Optimization(HashMap<Q::Uri, PdsReport<Q>>),
}

/// Report returned by Pds, potentially augmented with debugging information
/// TODO: add more detailed information about which filters/quotas kicked in.
#[derive(Default, Debug)]
pub struct PdsReport<Q: EpochReportRequest> {
    pub filtered_report: Q::Report,
    pub unfiltered_report: Q::Report,

    /// Store a list of the filter IDs that were out-of-budget in the atomic
    /// check for any epoch in the attribution window.
    pub oob_filters: Vec<FilterId<Q::EpochId, Q::Uri>>,
}

/// API for the epoch-based PDS.
///
/// TODO(https://github.com/columbia/pdslib/issues/21): support more than PureDP
/// TODO(https://github.com/columbia/pdslib/issues/22): simplify trait bounds?
impl<U, EI, E, EE, RES, FS, ES, Q, ERR> EpochPrivateDataService<FS, ES, Q, ERR>
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
        Report: Clone,
    >,
    ERR: From<FS::Error> + From<ES::Error> + From<anyhow::Error>,
{
    /// Registers a new event.
    pub fn register_event(&mut self, event: E) -> Result<(), ERR> {
        debug!("Registering event {:?}", event);
        self.event_storage.add_event(event)?;
        Ok(())
    }

    /// Computes a report for the given report request.
    /// This function follows `compute_attribution_report` from the Cookie
    /// Monster Algorithm (https://arxiv.org/pdf/2405.16719, Code Listing 1)
    pub fn compute_report(&mut self, request: &Q) -> Result<PdsReportResult<Q>, ERR> {
        debug!("Computing report for request {:?}", request);

        // Collect events from event storage by epoch. If an epoch has no
        // relevant events, don't add it to the mapping.
        let mut relevant_events_per_epoch: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.relevant_event_selector();
        for epoch_id in request.epoch_ids() {
            let epoch_relevant_events = self
                .event_storage
                .relevant_epoch_events(&epoch_id, relevant_event_selector)?;

            if let Some(epoch_relevant_events) = epoch_relevant_events {
                relevant_events_per_epoch
                    .insert(epoch_id, epoch_relevant_events);
            }
        }

        // Collect events from event storage by epoch per source. If an
        // epoch-source has no relevant events, don't add it to the
        // mapping.
        let mut relevant_events_per_epoch_source: HashMap<EI, HashMap<U, EE>> =
            HashMap::new();
        for epoch_id in request.epoch_ids() {
            let epoch_source_relevant_events =
                self.event_storage.relevant_epoch_source_events(
                    &epoch_id,
                    relevant_event_selector,
                )?;

            if let Some(epoch_source_relevant_events) =
                epoch_source_relevant_events
            {
                relevant_events_per_epoch_source
                    .insert(epoch_id, epoch_source_relevant_events);
            }
        }

        // Compute the raw report, useful for debugging and accounting.
        let num_epochs: usize = relevant_events_per_epoch.len();
        // TODO(https://github.com/columbia/pdslib/issues/55): Support more than just last_touch otherwise there will ever only be one event to be filtered out once any NC filter runs out.
        let unfiltered_report =
            request.compute_report(&relevant_events_per_epoch);

        // Browse epochs in the attribution window
        let mut oob_filters = vec![];
        for epoch_id in request.epoch_ids() {
            // Step 1. Get relevant events for the current epoch `epoch_id`.
            let epoch_relevant_events =
                relevant_events_per_epoch.get(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
            let individual_privacy_loss = self.compute_epoch_loss(
                request,
                epoch_relevant_events,
                &unfiltered_report,
                num_epochs,
            );

            // Step 3. Get relevant events for the current epoch `epoch_id` per
            // source.
            let epoch_source_relevant_events =
                relevant_events_per_epoch_source.get(&epoch_id);

            // Step 4. Compute device-epoch-source losses.
            let source_losses = self.compute_epoch_source_losses(
                request,
                epoch_source_relevant_events,
                &unfiltered_report,
                num_epochs,
            );

            // Step 5. Try to consume budget from current epoch, drop events if
            // OOB. Two phase commit.

            // Phase 1: dry run.
            let check_status = self.deduct_budget(
                &epoch_id,
                &individual_privacy_loss,
                &source_losses,
                request.report_uris(),
                true, // dry run
            )?;

            match check_status {
                PdsFilterStatus::Continue => {
                    // Phase 2: Consume the budget
                    let consume_status = self.deduct_budget(
                        &epoch_id,
                        &individual_privacy_loss,
                        &source_losses,
                        request.report_uris(),
                        false, // actually consume
                    )?;

                    if consume_status != PdsFilterStatus::Continue {
                        return Err(anyhow::anyhow!(
                            "ERR: Phase 2 failed unexpectedly wtih status {:?} after Phase 1 succeeded", 
                            consume_status,
                        ).into());
                    }
                }
                PdsFilterStatus::OutOfBudget(filters) => {
                    // TODO(https://github.com/columbia/pdslib/issues/55): Maybe this is a better solution?
                    // Because when NC filter is oob we should really only drop the corresponding report
                    // on that querier in the optimization query case.
                    // Check if any essential filters (C, QTrigger, QSource) are OOB
                    let has_essential_oob = filters.iter().any(|f| match f {
                        FilterId::C(_) | FilterId::QTrigger(_, _) | FilterId::QSource(_, _) => true,
                        FilterId::Nc(_, _) => false,
                    } ) || !request.is_optimization_query();  // Should not drop on NC filter oob if this is an optimization query.
                    
                    if has_essential_oob {
                        // Essential filters are OOB, drop the epoch
                        relevant_events_per_epoch.remove(&epoch_id);
                    }

                    // Keep track of why we dropped this epoch
                    oob_filters.extend(filters);
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report =
            request.compute_report(&relevant_events_per_epoch);
        let main_report = PdsReport {
            filtered_report,
            unfiltered_report: unfiltered_report.clone(),
            oob_filters,
        };

        // Step 6 (Only happens for optimization queries).
        // It returns a hash map keyed by querier URIs with the filtered report for the corresponding querier as the value; if querier reports have no meaningful elements, returns the main report.
        if request.is_optimization_query() {
            if let Some(querier_bucket_mapping) = request.get_querier_bucket_mapping() {
                let querier_uris = request.report_uris().querier_uris.clone();
                let mut querier_reports = HashMap::new();
                
                // Calculate site-level privacy budgets needed for filtering
                let mut epoch_site_privacy_losses = HashMap::new();

                // Process each epoch individually
                for epoch_id in request.epoch_ids() {
                    // Skip epochs that were dropped due to OOB (or not relevant)
                    if !relevant_events_per_epoch.contains_key(&epoch_id) {
                        continue;
                    }

                    // Get the source events for this epoch (or skip if none)
                    if let Some(source_events) = relevant_events_per_epoch_source.get(&epoch_id) {
                        // Calculate losses for this epoch
                        let epoch_losses = self.compute_epoch_source_losses(
                            request,
                            Some(source_events),  // Pass the correct Option<&HashMap> type
                            &unfiltered_report,
                            num_epochs
                        );
                        
                        // Add to the site-level privacy losses map
                        for (site, loss) in epoch_losses {
                            epoch_site_privacy_losses.insert(site, loss);
                        }
                    }
                }
                
                // Get available site budgets (from privacy filters)
                // TODO(https://github.com/columbia/pdslib/issues/55): Can maybe deprecate this, since events oob are already filtered before this optimization query step, so no need to check across these sites again.
                let mut available_site_budgets = HashMap::new();
                for site in epoch_site_privacy_losses.keys() {
                    // Initialize with Infinite budget, to be reduced by the minimum across epochs
                    let mut min_budget = PureDPBudget::Infinite;
                    
                    // Check budget for all epochs in the request
                    for epoch_id in request.epoch_ids() {
                        // Create the QSource filter ID for this epoch-site pair
                        let filter_id = FilterId::QSource(epoch_id.clone(), site.clone());
                        
                        // Initialize the filter if necessary
                        self.initialize_filter_if_necessary(filter_id.clone())?;
                        
                        // Get the remaining budget for this epoch-site
                        match self.filter_storage.remaining_budget(&filter_id) {
                            Ok(budget) => {
                                // Update the minimum budget
                                min_budget = match (min_budget, budget) {
                                    (PureDPBudget::Infinite, other) => other,
                                    (current, PureDPBudget::Infinite) => current,
                                    (PureDPBudget::Epsilon(current), PureDPBudget::Epsilon(new)) => {
                                        if new < current {
                                            PureDPBudget::Epsilon(new)
                                        } else {
                                            PureDPBudget::Epsilon(current)
                                        }
                                    }
                                };
                            }
                            Err(_) => {
                                // If there's an error, assume no budget
                                min_budget = PureDPBudget::Epsilon(0.0);
                                break;
                            }
                        }
                    }

                    if min_budget == PureDPBudget::Infinite {
                        // If the minimum budget is still Infinite, set it to 0
                        min_budget = PureDPBudget::Epsilon(0.0);
                    }
                    
                    // Use the minimum budget across all epochs for this site
                    available_site_budgets.insert(site.clone(), min_budget);
                }
                
                for querier_uri in querier_uris {
                    // Create a querier-specific report if this querier has bucket mappings
                    if !querier_bucket_mapping.contains_key(&querier_uri) {
                        continue;
                    }
                    // Create a filtered report for this querier
                    if let Some(querier_filtered_report) = request.filter_report_for_querier(
                        &main_report.filtered_report, 
                        &querier_uri,
                        &relevant_events_per_epoch,
                        Some(&epoch_site_privacy_losses),
                        Some(&available_site_budgets)
                    ) {
                        // Flag to track if any epoch is out of budget for this querier
                        let mut has_oob_epoch = false;
                        
                        // Deduct privacy budget from this querier's NC filter
                        for epoch_id in request.epoch_ids() {
                            // Skip epochs that were dropped due to OOB
                            if !relevant_events_per_epoch.contains_key(&epoch_id) {
                                continue;
                            }
                            
                            // NC filter ID for this querier and epoch
                            let filter_id = FilterId::Nc(epoch_id.clone(), querier_uri.clone());
                            
                            // Initialize the filter if needed
                            self.initialize_filter_if_necessary(filter_id.clone())?;
                            
                            // Compute individual loss for this querier's view
                            let querier_individual_loss = self.compute_epoch_loss(
                                request,
                                relevant_events_per_epoch.get(&epoch_id),
                                &querier_filtered_report,  // Use the filtered report for this querier
                                num_epochs,
                            );
                            
                            // Deduct from this querier's NC filter only
                            if !self.filter_storage.can_consume(&filter_id, &querier_individual_loss)? {
                                has_oob_epoch = true;
                                break;
                            }
                            
                            // Actually consume the budget
                            let status = self.filter_storage.try_consume(&filter_id, &querier_individual_loss)?;
                            if status == FilterStatus::OutOfBudget {
                                has_oob_epoch = true;
                                break;
                            }
                        }
                        
                        // Only include this querier if all epochs had sufficient budget
                        if !has_oob_epoch {
                            let querier_pds_report = PdsReport {
                                filtered_report: querier_filtered_report.clone(),
                                unfiltered_report: querier_filtered_report,
                                oob_filters: main_report.oob_filters.clone(),
                            };
                            
                            querier_reports.insert(querier_uri, querier_pds_report);
                        }
                    }
                }
                
                // If all queriers are out of NC filter budget, then this `querier_reports` will be empty.
                return Ok(PdsReportResult::Optimization(querier_reports));
            }
        }

        // For regular requests or optimization queries without mappings, just return the main report
        Ok(PdsReportResult::Regular(main_report))
    }

    /// [Experimental] Accounts for passive privacy loss. Can fail if the
    /// implementation has an error, but failure must not leak the state of
    /// the filters.
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/16): what are the semantics of passive loss queries that go over the filter
    /// capacity?
    pub fn account_for_passive_privacy_loss(
        &mut self,
        request: PassivePrivacyLossRequest<EI, U, PureDPBudget>,
    ) -> Result<PdsFilterStatus<FilterId<EI, U>>, ERR> {
        let source_losses = HashMap::new(); // Dummy.

        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            // Phase 1: dry run.
            let check_status = self.deduct_budget(
                &epoch_id,
                &request.privacy_budget,
                &source_losses,
                request.uris.clone(),
                true, // dry run
            )?;
            if check_status != PdsFilterStatus::Continue {
                return Ok(check_status);
            }

            // Phase 2: Consume the budget
            let consume_status = self.deduct_budget(
                &epoch_id,
                &request.privacy_budget,
                &source_losses,
                request.uris.clone(),
                false, // actually consume
            )?;

            if consume_status != PdsFilterStatus::Continue {
                return Err(anyhow::anyhow!(
                    "ERR: Phase 2 failed unexpectedly wtih status {:?} after Phase 1 succeeded", 
                    consume_status,
                ).into());
            }

            // TODO(https://github.com/columbia/pdslib/issues/16): semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(PdsFilterStatus::Continue)
    }

    fn initialize_filter_if_necessary(
        &mut self,
        filter_id: FilterId<EI, U>,
    ) -> Result<(), ERR> {
        let filter_initialized =
            self.filter_storage.is_initialized(&filter_id)?;

        if !filter_initialized {
            let create_filter_result =
                self.filter_storage.new_filter(filter_id);

            if create_filter_result.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }

    /// Compute the privacy loss at the device-epoch-source level.
    fn compute_epoch_source_losses(
        &self,
        request: &Q,
        relevant_events_per_epoch_source: Option<&HashMap<U, EE>>,
        computed_attribution: &Q::Report,
        num_epochs: usize,
    ) -> HashMap<U, PureDPBudget> {
        let mut per_source_losses = HashMap::new();

        // Collect sources and noise scale from the request.
        let requested_sources = request.report_uris().source_uris;
        let NoiseScale::Laplace(noise_scale) = request.noise_scale();

        // Count requested sources for case analysis
        let num_requested_sources = requested_sources.len();

        for source in requested_sources {
            // No relevant events map, or no events for this source, or empty
            // events
            let has_no_relevant_events = match relevant_events_per_epoch_source
            {
                None => true,
                Some(map) => match map.get(&source) {
                    None => true,
                    Some(events) => events.is_empty(),
                },
            };

            let individual_sensitivity = if has_no_relevant_events {
                // Case 1: Epoch-source with no relevant events.
                0.0
            } else if num_epochs == 1 && num_requested_sources == 1 {
                // Case 2: Single epoch and single source with relevant events.
                // Use actual individual sensitivity for this specific
                // epoch-source.
                request.single_epoch_source_individual_sensitivity(
                    computed_attribution,
                    NormType::L1,
                )
            } else {
                // Case 3: Multiple epochs or multiple sources.
                // Use global sensitivity as an upper bound.
                request.report_global_sensitivity()
            };

            // Treat near-zero noise scales as non-private, i.e. requesting
            // infinite budget, which can only go through if filters
            // are also set to infinite capacity, e.g. for
            // debugging. The machine precision `f64::EPSILON` is
            // not related to privacy.
            if noise_scale.abs() < f64::EPSILON {
                per_source_losses.insert(source, PureDPBudget::Infinite);
            } else {
                // In Cookie Monster, we have `query_global_sensitivity` /
                // `requested_epsilon` instead of just `noise_scale`.
                per_source_losses.insert(
                    source,
                    PureDPBudget::Epsilon(individual_sensitivity / noise_scale),
                );
            }
        }

        per_source_losses
    }

    /// Deduct the privacy loss from the various filters.
    fn deduct_budget(
        &mut self,
        epoch_id: &EI,
        loss: &FS::Budget,
        source_losses: &HashMap<U, FS::Budget>,
        uris: ReportRequestUris<U>,
        dry_run: bool,
    ) -> Result<PdsFilterStatus<FilterId<EI, U>>, ERR> {
        // Build the filter IDs for NC, C and QTrigger
        let mut device_epoch_filter_ids = Vec::new();
        for query_uri in uris.querier_uris {
            device_epoch_filter_ids
                .push(FilterId::Nc(epoch_id.clone(), query_uri));
        }
        device_epoch_filter_ids
            .push(FilterId::QTrigger(epoch_id.clone(), uris.trigger_uri));
        device_epoch_filter_ids.push(FilterId::C(epoch_id.clone()));

        // NC, C and QTrigger all have the same device-epoch level loss
        let mut filters_to_consume = HashMap::new();
        for filter_id in device_epoch_filter_ids {
            filters_to_consume.insert(filter_id, loss);
        }

        // Add the QSource filters with their own device-epoch-source level loss
        for (source, loss) in source_losses {
            let fid = FilterId::QSource(epoch_id.clone(), source.clone());
            filters_to_consume.insert(fid, loss);
        }

        // Try to consume the privacy loss from the filters
        let mut oob_filters = vec![];
        for (fid, loss) in filters_to_consume {
            self.initialize_filter_if_necessary(fid.clone())?;
            let filter_status =
                self.filter_storage.maybe_consume(&fid, loss, dry_run)?;
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

    /// Pure DP individual privacy loss, following
    /// `compute_individual_privacy_loss` from Code Listing 1 in Cookie Monster (https://arxiv.org/pdf/2405.16719).
    ///
    /// TODO(https://github.com/columbia/pdslib/issues/21): generic budget.
    fn compute_epoch_loss(
        &self,
        request: &Q,
        epoch_relevant_events: Option<&EE>,
        computed_attribution: &Q::Report,
        num_epochs: usize,
    ) -> PureDPBudget {
        // Case 1: Epoch with no relevant events
        match epoch_relevant_events {
            None => {
                return PureDPBudget::Epsilon(0.0);
            }
            Some(epoch_events) => {
                if epoch_events.is_empty() {
                    return PureDPBudget::Epsilon(0.0);
                }
            }
        }

        let individual_sensitivity = match num_epochs {
            1 => {
                // Case 2: One epoch.
                request.single_epoch_individual_sensitivity(
                    computed_attribution,
                    NormType::L1,
                )
            }
            _ => {
                // Case 3: Multiple epochs.
                request.report_global_sensitivity()
            }
        };

        let NoiseScale::Laplace(noise_scale) = request.noise_scale();

        // Treat near-zero noise scales as non-private, i.e. requesting infinite
        // budget, which can only go through if filters are also set to
        // infinite capacity, e.g. for debugging. The machine precision
        // `f64::EPSILON` is not related to privacy.
        if noise_scale.abs() < f64::EPSILON {
            return PureDPBudget::Infinite;
        }

        // In Cookie Monster, we have `query_global_sensitivity` /
        // `requested_epsilon` instead of just `noise_scale`.
        PureDPBudget::Epsilon(individual_sensitivity / noise_scale)
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

    #[test]
    fn test_account_for_passive_privacy_loss() -> Result<(), anyhow::Error> {
        let capacities: StaticCapacities<
            FilterId<usize, String>,
            PureDPBudget,
        > = StaticCapacities::mock();
        let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
            HashMapFilterStorage::new(capacities)?;
        let events = HashMapEventStorage::new();

        let mut pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            _phantom_request: std::marker::PhantomData::<
                SimpleLastTouchHistogramRequest,
            >,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };

        let uris = ReportRequestUris::mock();

        // First request should succeed
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(0.2),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert_eq!(result, PdsFilterStatus::Continue);

        // Second request with same budget should succeed (2.0 total)
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(0.3),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert_eq!(result, PdsFilterStatus::Continue);

        // Verify remaining budgets
        for epoch_id in 1..=3 {
            // we consumed 0.5 so far
            let expected_budgets = vec![
                (FilterId::Nc(epoch_id, uris.querier_uris[0].clone()), 0.5),
                (FilterId::C(epoch_id), 19.5),
                (FilterId::QTrigger(epoch_id, uris.trigger_uri.clone()), 1.0),
            ];

            assert_remaining_budgets(&pds.filter_storage, &expected_budgets)?;
        }

        // Attempting to consume more should fail.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![2, 3],
            privacy_budget: PureDPBudget::Epsilon(2.0),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert!(matches!(result, PdsFilterStatus::OutOfBudget(_)));
        if let PdsFilterStatus::OutOfBudget(oob_filters) = result {
            assert!(oob_filters
                .contains(&FilterId::Nc(2, uris.querier_uris[0].clone())));
        }

        // Consume from just one epoch.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![3],
            privacy_budget: PureDPBudget::Epsilon(0.5),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert_eq!(result, PdsFilterStatus::Continue);

        // Verify remaining budgets
        use FilterId::*;
        for epoch_id in 1..=2 {
            let expected_budgets = vec![
                (Nc(epoch_id, uris.querier_uris[0].clone()), 0.5),
                (C(epoch_id), 19.5),
                (QTrigger(epoch_id, uris.trigger_uri.clone()), 1.0),
            ];

            assert_remaining_budgets(&pds.filter_storage, &expected_budgets)?;
        }

        // epoch 3's nc-filter and q-conv should be out of budget
        let remaining = pds
            .filter_storage
            .remaining_budget(&Nc(3, uris.querier_uris[0].clone()))?;
        assert_eq!(remaining, PureDPBudget::Epsilon(0.0));

        Ok(())
    }

    #[track_caller]
    fn assert_remaining_budgets<FS: FilterStorage<Budget = PureDPBudget>>(
        filter_storage: &FS,
        expected_budgets: &[(FS::FilterId, f64)],
    ) -> Result<(), FS::Error>
    where
        FS::FilterId: Debug,
    {
        for (filter_id, expected_budget) in expected_budgets {
            let remaining = filter_storage.remaining_budget(filter_id)?;
            assert_eq!(
                remaining,
                PureDPBudget::Epsilon(*expected_budget),
                "Remaining budget for {:?} is not as expected",
                filter_id
            );
        }
        Ok(())
    }

    /// TODO: test this on the real `compute_report`, not just passive privacy
    /// loss.
    #[test]
    fn test_budget_rollback_on_depletion() -> Result<(), anyhow::Error> {
        // PDS with several filters
        let capacities: StaticCapacities<
            FilterId<usize, String>,
            PureDPBudget,
        > = StaticCapacities::new(
            PureDPBudget::Epsilon(1.0),  // nc
            PureDPBudget::Epsilon(20.0), // c
            PureDPBudget::Epsilon(2.0),  // q-trigger
            PureDPBudget::Epsilon(5.0),  // q-source
        );

        let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
            HashMapFilterStorage::new(capacities)?;

        let events = HashMapEventStorage::new();

        let mut pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            _phantom_request: std::marker::PhantomData::<
                SimpleLastTouchHistogramRequest,
            >,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };

        // Create a sample request uris with multiple queriers
        let mut uris = ReportRequestUris::mock();
        uris.querier_uris = vec![
            "querier1.example.com".to_string(),
            "querier2.example.com".to_string(),
        ];

        // Initialize all filters for epoch 1
        let epoch_id = 1;
        let filter_ids = vec![
            FilterId::C(epoch_id),
            FilterId::Nc(epoch_id, uris.querier_uris[0].clone()),
            FilterId::Nc(epoch_id, uris.querier_uris[1].clone()),
            FilterId::QTrigger(epoch_id, uris.trigger_uri.clone()),
            FilterId::QSource(epoch_id, uris.source_uris[0].clone()),
        ];

        for filter_id in &filter_ids {
            pds.filter_storage.new_filter(filter_id.clone())?;
        }

        // Record initial budgets
        let mut initial_budgets = HashMap::new();
        for filter_id in &filter_ids {
            initial_budgets.insert(
                filter_id.clone(),
                pds.filter_storage.remaining_budget(filter_id)?,
            );
        }

        // Set up a request that will succeed for most filters but fail for one
        // Make the NC filter for querier1 have only 0.5 epsilon left
        pds.filter_storage.try_consume(
            &FilterId::Nc(epoch_id, uris.querier_uris[0].clone()),
            &PureDPBudget::Epsilon(0.5),
        )?;

        // Now attempt a deduction that requires 0.7 epsilon
        // This should fail because querier1's NC filter only has 0.5 left
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![epoch_id],
            privacy_budget: PureDPBudget::Epsilon(0.7),
            uris: uris.clone(),
        };

        let result = pds.account_for_passive_privacy_loss(request)?;
        assert!(matches!(result, PdsFilterStatus::OutOfBudget(_)));
        if let PdsFilterStatus::OutOfBudget(oob_filters) = result {
            assert!(oob_filters.contains(&FilterId::Nc(
                1,
                "querier1.example.com".to_string()
            )));
        }

        // Check that all other filters were not modified
        // First verify that querier1's NC filter still has 0.5 epsilon
        assert_eq!(
            pds.filter_storage.remaining_budget(&FilterId::Nc(
                epoch_id,
                uris.querier_uris[0].clone()
            ))?,
            PureDPBudget::Epsilon(0.5),
            "Filter that was insufficient should still have its partial budget"
        );

        // Then verify the other filters still have their original budgets
        for filter_id in &filter_ids {
            // Skip the querier1 NC filter we already checked
            if matches!(filter_id, FilterId::Nc(_, uri) if uri == &uris.querier_uris[0])
            {
                continue;
            }

            let current_budget =
                pds.filter_storage.remaining_budget(filter_id)?;
            let initial_budget = initial_budgets.get(filter_id).unwrap();

            assert_eq!(
                current_budget, *initial_budget,
                "Filter {:?} budget changed when it shouldn't have",
                filter_id
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod optimization_queries_tests {
    use super::*;
    use crate::{
        budget::{
            hashmap_filter_storage::HashMapFilterStorage,
            pure_dp_filter::{PureDPBudget, PureDPBudgetFilter},
        },
        events::{
            hashmap_event_storage::HashMapEventStorage,
            ppa_event::PpaEvent,
            traits::EventUris,
        },
        queries::{
            ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector, PpaHistogramConfig, create_querier_bucket_mapping},
            traits::ReportRequestUris,
        },
    };

    // Tests for the optimization query functionality in compute_report
    #[test]
    fn test_compute_report_optimization_query() -> Result<(), anyhow::Error> {
        // Create PDS with mock capacities
        let events = HashMapEventStorage::<PpaEvent, PpaRelevantEventSelector>::new();
        let capacities = StaticCapacities::mock();
        let filters: HashMapFilterStorage<_, PureDPBudgetFilter, _, _> =
            HashMapFilterStorage::new(capacities)?;

        let mut pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            _phantom_request: std::marker::PhantomData::<PpaHistogramRequest>,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };

        // Create test URIs
        let source_uri = "blog.example.com".to_string();
        let trigger_uri = "shoes.example.com".to_string();
        let querier_uri1 = "adtech1.example.com".to_string();
        let querier_uri2 = "adtech2.example.com".to_string();

        // Create event URIs with multiple queriers
        let event_uris = EventUris {
            source_uri: source_uri.clone(),
            trigger_uris: vec![trigger_uri.clone()],
            querier_uris: vec![querier_uri1.clone(), querier_uri2.clone()],
        };

        // Create report request URIs
        let report_request_uris = ReportRequestUris {
            trigger_uri: trigger_uri.clone(),
            source_uris: vec![source_uri.clone()],
            querier_uris: vec![querier_uri1.clone(), querier_uri2.clone()],
        };

        // Create and register test events with different histogram indices
        // Event 1 - bucket 0 (for querier1)
        let event1 = PpaEvent {
            id: 1,
            timestamp: 0,
            epoch_number: 1,
            histogram_index: 0,
            uris: event_uris.clone(),
            filter_data: 1,
        };

        // Event 2 - bucket 1 (for querier2)
        let event2 = PpaEvent {
            id: 2,
            timestamp: 1,
            epoch_number: 1,
            histogram_index: 1,
            uris: event_uris.clone(),
            filter_data: 1,
        };

        // Event 3 - bucket 2 (shared by both queriers)
        let event3 = PpaEvent {
            id: 3,
            timestamp: 2,
            epoch_number: 1,
            histogram_index: 2,
            uris: event_uris.clone(),
            filter_data: 1,
        };

        pds.register_event(event1.clone())?;
        pds.register_event(event2.clone())?;
        pds.register_event(event3.clone())?;

        // Create querier bucket mapping
        let querier_bucket_mapping = create_querier_bucket_mapping(vec![
            (querier_uri1.clone(), vec![0, 2]),  // querier1 gets buckets 0 and 2
            (querier_uri2.clone(), vec![1, 2]),  // querier2 gets buckets 1 and 2
        ]);

        // Create histogram request with optimization query flag set to true
        let config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 1,
            report_global_sensitivity: 100.0,
            query_global_sensitivity: 200.0,
            requested_epsilon: 1.0,
            histogram_size: 3,
            is_optimization_query: true,
        };
        let request = PpaHistogramRequest::new(
            config,
            PpaRelevantEventSelector {
                report_request_uris,
                is_matching_event: Box::new(|event_filter_data: u64| event_filter_data == 1),
                querier_bucket_mapping,
            },
        ).map_err(|_| anyhow::anyhow!("Failed to create request"))?;

        // Process the request
        let report_result = pds.compute_report(&request)?;

        // Verify the result is an Optimization report
        match report_result {
            PdsReportResult::Optimization(querier_reports) => {
                // Verify we have reports for both queriers
                assert_eq!(querier_reports.len(), 2, "Expected reports for 2 queriers");
                
                // TODO(https://github.com/columbia/pdslib/issues/55): Maybe we wouldn't want just
                // the last touch attribution logic anymore.
                // IMPORTANT: With LastTouch attribution logic, only the last event in each epoch
                // is considered for attribution. Since all three events are in epoch 1, and event3
                // (with histogram_index 2) is the last one, only bucket 2 should appear in reports.
                
                // Verify querier1's report has only bucket 2 (the last event's bucket)
                let querier1_report = querier_reports.get(&querier_uri1).expect("Missing report for querier1");
                let querier1_bins = &querier1_report.filtered_report.bin_values;
                assert_eq!(querier1_bins.len(), 1, "Expected 1 bucket for querier1 (LastTouch attribution)");
                assert!(querier1_bins.contains_key(&2), "Expected bucket 2 for querier1");
                assert!(!querier1_bins.contains_key(&0), "Unexpected bucket 0 for querier1 - only last event is used with LastTouch");
                
                // Verify querier2's report has only bucket 2 (the last event's bucket)
                let querier2_report = querier_reports.get(&querier_uri2).expect("Missing report for querier2");
                let querier2_bins = &querier2_report.filtered_report.bin_values;
                assert_eq!(querier2_bins.len(), 1, "Expected 1 bucket for querier2 (LastTouch attribution)");
                assert!(querier2_bins.contains_key(&2), "Expected bucket 2 for querier2");
                assert!(!querier2_bins.contains_key(&1), "Unexpected bucket 1 for querier2 - only last event is used with LastTouch");
                
                // Verify each bucket has the expected value
                assert_eq!(querier1_bins.get(&2), Some(&100.0), "Incorrect value for querier1 bucket 2");
                assert_eq!(querier2_bins.get(&2), Some(&100.0), "Incorrect value for querier2 bucket 2");
                
                // Verify NC filters for each querier have been consumed
                // Check remaining budget for querier1's NC filter
                let querier1_filter_id = FilterId::Nc(1, querier_uri1.clone());
                let querier1_remaining = pds.filter_storage.remaining_budget(&querier1_filter_id)?;
                
                // Check remaining budget for querier2's NC filter
                let querier2_filter_id = FilterId::Nc(1, querier_uri2.clone());
                let querier2_remaining = pds.filter_storage.remaining_budget(&querier2_filter_id)?;
                
                // Verify budget has been consumed from both queriers' NC filters
                match (querier1_remaining, querier2_remaining) {
                    (PureDPBudget::Epsilon(q1), PureDPBudget::Epsilon(q2)) => {
                        assert!(q1 < 1.0, "Expected some budget to be consumed from querier1's NC filter");
                        assert!(q2 < 1.0, "Expected some budget to be consumed from querier2's NC filter");
                    },
                    _ => panic!("Unexpected budget type")
                }
            },
            PdsReportResult::Regular(_) => {
                panic!("Expected Optimization report, got Regular report");
            }
        }

        Ok(())
    }

    #[test]
    fn test_one_querier_out_of_budget_one_in_budget() -> Result<(), anyhow::Error> {
        // NC filter capacities: NC=1.5 so that one querier has enough budget
        let capacities = StaticCapacities::new(
            PureDPBudget::Epsilon(1.5),  // NC capacity (increased TO 1.5)
            PureDPBudget::Epsilon(9999.),
            PureDPBudget::Epsilon(9999.),
            PureDPBudget::Epsilon(9999.),
        );
        let filters = HashMapFilterStorage::<_, PureDPBudgetFilter, _, _>::new(capacities)?;
        let events = HashMapEventStorage::new();
        
        let mut pds = EpochPrivateDataService {
            filter_storage: filters,
            event_storage: events,
            _phantom_request: std::marker::PhantomData::<PpaHistogramRequest>,
            _phantom_error: std::marker::PhantomData::<anyhow::Error>,
        };

        // Two queriers: querier1 will be in-budget, querier2 out-of-budget
        let querier1 = "adtech1.example".to_string();
        let querier2 = "adtech2.example".to_string();
        let source_uri = "source.example".to_string();
        let trigger_uri = "trigger.example".to_string();

        // Create base event URIs
        let event_uris = EventUris {
            source_uri: source_uri.clone(),
            trigger_uris: vec![trigger_uri.clone()],
            querier_uris: vec![querier1.clone(), querier2.clone()],
        };
        
        // Register events with particular histogram indices
        let event = PpaEvent {
            id: 123,
            timestamp: 1001, // Later timestamp so it gets precedence with LastTouch
            epoch_number: 1,
            histogram_index: 1,
            uris: event_uris.clone(),
            filter_data: 1,
        };
        pds.register_event(event)?;

        // Build a querier-bucket mapping
        let querier_bucket_mapping = create_querier_bucket_mapping(vec![
            (querier1.clone(), vec![1]),  // querier1 wants bucket 1
            (querier2.clone(), vec![1]),  // querier2 wants bucket 1 too
        ]);

        // Create request with report_global_sensitivity = 1
        let request_uris = ReportRequestUris {
            trigger_uri: trigger_uri.clone(),
            source_uris: vec![source_uri.clone()],
            querier_uris: vec![querier1.clone(), querier2.clone()],
        };

        let config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 1,
            report_global_sensitivity: 0.4,
            query_global_sensitivity: 0.4,
            requested_epsilon: 1.0, // NC filter capacity is 1.5
            histogram_size: 3,
            is_optimization_query: true,
        };
        let request = PpaHistogramRequest::new(
            config,
            PpaRelevantEventSelector {
                report_request_uris: request_uris,
                is_matching_event: Box::new(|val| val == 1),
                querier_bucket_mapping,
            },
        ).map_err(|_| anyhow::anyhow!("Failed to create request"))?;

        // Pre-consume 1.0 from querier2's NC filter to deplete it below what's needed
        let nc_filter_q2 = FilterId::Nc(1, querier2.clone());
        pds.filter_storage.new_filter(nc_filter_q2.clone())?;
        pds.filter_storage.try_consume(&nc_filter_q2, &PureDPBudget::Epsilon(1.0))?;

        // Compute report and check results
        let result = pds.compute_report(&request)?;
        match result {
            PdsReportResult::Optimization(filtered_result) => {
                // print!("\n==Check overall filtered result==\n");
                assert_eq!(
                    filtered_result.len(),
                    1,
                    "We should have exactly one querier subreport"
                );

                // print!("\n==Check querier1 report==\n");
                assert!(
                    filtered_result.contains_key(&querier1),
                    "querier1 should remain in-budget and appear in the result"
                );
                if let Some(report) = filtered_result.get(&querier1) {
                    assert_eq!(
                        report.filtered_report.bin_values.len(),
                        1,
                        "querier1 should have one bucket in the report"
                    );
                    assert!(
                        report.filtered_report.bin_values.get(&1) == Some(&0.4),
                        "querier1 should have one bucket with bin value 0.4 in the report"
                    );
                }

                // print!("\n==Ensure querier2 report is empty==\n");
                assert!(
                    !filtered_result.contains_key(&querier2),
                    "querier2 is out-of-budget after pre-consumption"
                );
            },
            other => panic!("Expected Optimization result, got {:?}", other),
        }

        Ok(())
    }
}
