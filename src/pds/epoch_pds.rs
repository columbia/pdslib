use std::{collections::HashMap, fmt::Debug, hash::Hash, vec};

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
    pub fn compute_report(&mut self, request: &Q) -> Result<HashMap<Q::Uri, PdsReport<Q>>, ERR> {
        debug!("Computing report for request {:?}", request);

        // Check if this is a multi-beneficiary query, which we don't support yet
        if request.report_uris().querier_uris.len() > 1 {
            todo!("Implement multi-beneficiary queries");
        }

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
                PdsFilterStatus::OutOfBudget(mut filters) => {
                    // Not enough budget, drop events without any filter
                    // consumption
                    relevant_events_per_epoch.remove(&epoch_id);

                    // Keep track of why we dropped this epoch
                    oob_filters.append(&mut filters);
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

        // Handle optimization queries when at least two intermediary URIs are in the request.
        if request.report_uris().intermediary_uris.len() >= 2 {
            let intermediary_uris = request.report_uris().intermediary_uris.clone();
            let mut intermediary_reports = HashMap::new();

            if request.get_intermediary_bucket_mapping().is_some() {
                // Process each intermediary
                for intermediary_uri in intermediary_uris {
                    // Get the bucket mapping for this intermediary
                    let histogram_indices_to_intermediary = request
                        .get_intermediary_bucket_mapping()
                        .unwrap()
                        .get(&intermediary_uri)
                        .unwrap();
                    print!("FKKK {:?}", histogram_indices_to_intermediary);

                    // TODO(https://github.com/columbia/pdslib/issues/55):
                    // The events should not be readable by any intermediary. In Fig 2 it seems that the first event is readable by r1.ex and r3.ex only,
                    // and the second event is readable by r2.ex and r3.ex. r3 is a special intermediary that can read all the events (maybe r3.ex = shoes.example).
                    // But feel free to keep this remark in a issue for later, because that would involve modifying the is_relevant_event logic too, to check that
                    // the intermediary_uris match. Your get_intermediary_bucket_mapping seems to serve the same purpose.
                    // Get the relevant events for this intermediary
                    let mut relevant_events_per_epoch_one_intermerdiary: HashMap<EI, EE> = HashMap::new();

                    for (epoch, events) in &relevant_events_per_epoch {
                        for histogram_index in histogram_indices_to_intermediary.iter() {
                            let mut filtered_events: EE = EpochEvents::new();
                            for event in events.iter() {
                                if event.histogram_index() ==  *histogram_index {
                                    // Clone if you want new, owned data
                                    filtered_events.push(event.clone());
                                }
                            }

                            if !filtered_events.is_empty() {
                                relevant_events_per_epoch_one_intermerdiary.insert(epoch.clone(), filtered_events);
                            }
                        }
                    }

                    // Filter report for this intermediary
                    if let Some(intermediary_filtered_report) = request.filter_report_for_intermediary(
                        &main_report.filtered_report, 
                        &intermediary_uri,
                        &relevant_events_per_epoch
                    ) {
                        // Create PdsReport for this intermediary
                        let intermediary_pds_report = PdsReport {
                            filtered_report: intermediary_filtered_report,
                            unfiltered_report: unfiltered_report.clone(),
                            oob_filters: main_report.oob_filters.clone(),
                        };
                        
                        // Add this code to deduct budget for the intermediary
                        // Create a modified request URIs with the intermediary as the querier
                        let mut intermediary_report_uris = request.report_uris().clone();
                        intermediary_report_uris.querier_uris = vec![intermediary_uri.clone()];

                        intermediary_reports.insert(intermediary_uri, intermediary_pds_report);
                    }
                }
            }
            // Return optimization result with all intermediary reports
            // If the querier needs to receive a report for itself too, need to add itself as an intermediary in the request
            return Ok(intermediary_reports);
        }

        // For regular requests or optimization queries without intermediary reports
        Ok(HashMap::from([(
            request.report_uris().querier_uris[0].clone(),
            main_report,
        )]))
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
mod cross_report_optimization_tests {
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
            ppa_histogram::{PpaHistogramRequest, PpaRelevantEventSelector, PpaHistogramConfig, create_intermediary_bucket_mapping},
            traits::ReportRequestUris,
        },
    };

    #[test]
    fn test_cross_report_optimization() -> Result<(), anyhow::Error> {
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
        let beneficiary_uri = "shoes.example.com".to_string();
        let trigger_uri = "shoes.example.com".to_string();
        let intermediary_uri1 = "r1.ex".to_string();
        let intermediary_uri2 = "r2.ex".to_string();
        let intermediary_uri3 = "r3.ex".to_string();

        // Create event URIs with appropriate intermediaries
        let event_uris = EventUris {
            source_uri: source_uri.clone(),
            trigger_uris: vec![trigger_uri.clone()],
            querier_uris: vec![beneficiary_uri.clone()],
            intermediary_uris: vec![
                intermediary_uri1.clone(),
                intermediary_uri2.clone(),
                intermediary_uri3.clone(),
            ],
        };

        // Create report request URIs
        let report_request_uris = ReportRequestUris {
            trigger_uri: trigger_uri.clone(),
            source_uris: vec![source_uri.clone()],
            querier_uris: vec![beneficiary_uri.clone()],
            intermediary_uris: vec![
                intermediary_uri1.clone(),
                intermediary_uri2.clone(),
                intermediary_uri3.clone(),
            ],
        };

        // Register an early event with bucket 1 - this should be overridden by last-touch attribution
        let early_event = PpaEvent {
            id: 1,
            timestamp: 100,
            epoch_number: 1,
            histogram_index: 1,  // r1.ex bucket
            uris: event_uris.clone(),
            filter_data: 1,
        };
        pds.register_event(early_event.clone())?;

        // The event that should be attributed (latest timestamp in epoch 1)
        // We'll use a histogram index that's covered by both intermediaries (3)
        let main_event = PpaEvent {
            id: 2,
            timestamp: 200,  // Later timestamp so this event is picked by last-touch
            epoch_number: 1,
            histogram_index: 2,  // A bucket that will be kept and read by r2.ex
            uris: event_uris.clone(),
            filter_data: 1,
        };
        pds.register_event(main_event.clone())?;
        // Create intermediary bucket mapping
        // Both intermediaries have access to bucket 3, so they'll both get data from the same event
        let intermediary_bucket_mapping = create_intermediary_bucket_mapping(vec![
            (intermediary_uri1.clone(), vec![1]),  // r1.ex gets buckets 1
            (intermediary_uri2.clone(), vec![2]),  // r2.ex gets buckets 2
            (intermediary_uri3.clone(), vec![3]),  // r3.ex gets buckets 3
        ]).map_err(anyhow::Error::msg)?;
        // Create histogram request with optimization query flag set to true
        let config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            report_global_sensitivity: 100.0,
            query_global_sensitivity: 200.0,
            requested_epsilon: 1.0,
            histogram_size: 4,  // Ensure we have space for bucket 3
        };

        let request = PpaHistogramRequest::new(
            config.clone(),
            PpaRelevantEventSelector {
                report_request_uris,
                is_matching_event: Box::new(|event_filter_data: u64| event_filter_data == 1),
                intermediary_bucket_mapping,
            },
        ).map_err(|e| anyhow::anyhow!("Failed to create request: {}", e))?;
        // Initialize and check the initial beneficiary's NC filter
        let beneficiary_filter_id = FilterId::Nc(1, beneficiary_uri.clone());
        pds.filter_storage.new_filter(beneficiary_filter_id.clone())?;
        let initial_budget = pds.filter_storage.remaining_budget(&beneficiary_filter_id)?;

        // Process the request
        let report_result = pds.compute_report(&request)?;

        // Verify the result is an Optimization report
        // Verify we have reports for both intermediaries
        assert_eq!(report_result.len(), 3, "Expected reports for 2 intermediaries");

        // Verify r1.ex's report has bucket 1
        let r1_report = report_result.get(&intermediary_uri1).expect("Missing report for r1.ex");
        let r1_bins = &r1_report.filtered_report.bin_values;
        assert!(r1_bins.is_empty(), "1 bucket for r1.ex should have been filtered out by last-touch attribution");

        // Verify r2.ex's report has bucket 2
        let r2_report = report_result.get(&intermediary_uri2).expect("Missing report for r2.ex");
        let r2_bins = &r2_report.filtered_report.bin_values;
        assert_eq!(r2_bins.len(), 1, "Expected 1 bucket for r2.ex");
        assert!(r2_bins.contains_key(&2), "Expected bucket 3 for r2.ex");

        // Intermediary r2 receives the value from the main event
        assert_eq!(r2_bins.get(&2), Some(&100.0), "Incorrect value for r2.ex bucket 3");

        // Verify r3.ex's report has bucket 3
        let r3_report = report_result.get(&intermediary_uri3).expect("Missing report for r3.ex");
        let r3_bins = &r3_report.filtered_report.bin_values;
        assert!(r3_bins.is_empty(), "1 bucket for r1.ex should have been filtered out by last-touch attribution");

        // Verify the privacy budget was deducted only once
        // Despite two reports being generated (one for each intermediary)
        let post_budget = pds.filter_storage.remaining_budget(&beneficiary_filter_id)?;

        match (initial_budget.clone(), post_budget) {
            (PureDPBudget::Epsilon(initial), PureDPBudget::Epsilon(remaining)) => {
                let deduction = initial - remaining;

                // Verify budget was actually deducted
                assert!(deduction == 0.5, "Expected budget deduction but none occurred");

                // Calculate what would be deducted with vs. without optimization
                let expected_single_deduction = config.report_global_sensitivity / config.query_global_sensitivity;

                // Verify deduction is close to single event (cross-report optimization working)
                assert!(deduction == expected_single_deduction, 
                        "Budget deduction indicates optimization is not working");
            },
            _ => {
                panic!("Expected finite budget deduction");
            }
        }
        Ok(())
    }


#[test]
fn test_bucket_overlap_validation() {
    // Test case 1: Valid disjoint buckets
    let valid_mapping = vec![
        ("r1.ex".to_string(), vec![1, 3, 5]),
        ("r2.ex".to_string(), vec![2, 4, 6]),
    ];
    
    let result = create_intermediary_bucket_mapping(valid_mapping);
    assert!(result.is_ok(), "Valid mapping should not produce an error");
    
    // Test case 2: Invalid overlapping buckets
    let invalid_mapping = vec![
        ("r1.ex".to_string(), vec![1, 3, 5]),
        ("r2.ex".to_string(), vec![2, 3, 6]), // Bucket 3 overlaps with r1.ex
    ];
    
    let result = create_intermediary_bucket_mapping(invalid_mapping);
    assert!(result.is_err(), "Overlapping buckets should produce an error");
}
}