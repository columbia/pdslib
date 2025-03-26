use std::{collections::HashMap, fmt::Debug, hash::Hash};

use log::info;

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

    /// Default capacity that will be used for all new epochs
    pub epoch_capacity: FS::Budget,

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
}

/// API for the epoch-based PDS.
///
/// TODO(https://github.com/columbia/pdslib/issues/21): support more than PureDP
/// TODO(https://github.com/columbia/pdslib/issues/22): simplify trait bounds?
impl<U, EI, E, EE, RES, FS, ES, Q, ERR> EpochPrivateDataService<FS, ES, Q, ERR>
where
    U: Clone + Eq + Hash,
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
    ERR: From<FS::Error> + From<ES::Error>,
{
    /// Registers a new event.
    pub fn register_event(&mut self, event: E) -> Result<(), ERR> {
        info!("Registering event {:?}", event);
        self.event_storage.add_event(event)?;
        Ok(())
    }

    /// Computes a report for the given report request.
    /// This function follows `compute_attribution_report` from the Cookie
    /// Monster Algorithm (https://arxiv.org/pdf/2405.16719, Code Listing 1)
    pub fn compute_report(&mut self, request: &Q) -> Result<PdsReport<Q>, ERR> {
        info!("Computing report for request {:?}", request);

        // Collect events from event storage by epoch. If an epoch has no
        // relevant events, don't add it to the mapping.
        let mut relevant_events_per_epoch: HashMap<EI, EE> = HashMap::new();
        let relevant_event_selector = request.relevant_event_selector();
        for epoch_id in request.epoch_ids() {
            let epoch_relevant_events = self
                .event_storage
                .relevant_epoch_events(&epoch_id, &relevant_event_selector)?;

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
                    &relevant_event_selector,
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

            // Step 3. Compute device-epoch-source losses.
            let source_losses = self.compute_epoch_source_losses(
                request,
                epoch_source_relevant_events,
                &unfiltered_report,
                num_epochs,
            );

            // Step 4. Try to consume budget from current epoch, drop events if
            // OOB.
            let deduct_res = self.deduct_budget(
                &epoch_id,
                &individual_privacy_loss,
                &source_losses,
                request.report_uris(),
            );
            match deduct_res {
                Ok(FilterStatus::Continue) => {
                    // The budget is not depleted, keep events.
                }
                Ok(FilterStatus::OutOfBudget) => {
                    // The budget is depleted, drop events.
                    relevant_events_per_epoch.remove(&epoch_id);
                }
                Err(_) => {
                    // Return default report if anything else goes wrong.
                    return Ok(PdsReport {
                        filtered_report: Default::default(),
                        unfiltered_report,
                    });
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report =
            request.compute_report(&relevant_events_per_epoch);
        Ok(PdsReport {
            filtered_report,
            unfiltered_report,
        })
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
    ) -> Result<FilterStatus, ERR> {
        let source_losses = HashMap::new(); // Dummy.

        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            // Try to consume budget from current epoch.
            let budget_res = self.deduct_budget(
                &epoch_id,
                &request.privacy_budget,
                &source_losses,
                request.uris.clone(),
            )?;
            if budget_res == FilterStatus::OutOfBudget {
                return Ok(FilterStatus::OutOfBudget);
            }

            // TODO(https://github.com/columbia/pdslib/issues/16): semantics are still unclear, for now we ignore the request if
            // it would exhaust the filter.
        }
        Ok(FilterStatus::Continue)
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
    ) -> Result<FilterStatus, ERR> {
        use FilterId::*;
        let mut filters_to_consume = vec![];

        for query_uri in uris.querier_uris {
            filters_to_consume.push(Nc(epoch_id.clone(), query_uri));
        }
        filters_to_consume.push(QTrigger(epoch_id.clone(), uris.trigger_uri));
        filters_to_consume.push(C(epoch_id.clone()));
        for filter_id in filters_to_consume {
            self.initialize_filter_if_necessary(filter_id.clone())?;

            match self.filter_storage.check_and_consume(&filter_id, loss)? {
                FilterStatus::Continue => {
                    // The budget is not depleted, keep going.
                }
                FilterStatus::OutOfBudget => {
                    // The budget is depleted, stop deducting from filters.
                    return Ok(FilterStatus::OutOfBudget);
                    // TODO(https://github.com/columbia/pdslib/issues/39)
                    // need to implement transaction rollbacks for previous
                    // filter deductions.
                }
            }
        }

        for (source, loss) in source_losses {
            let fid = FilterId::QSource(epoch_id.clone(), source.clone());
            self.initialize_filter_if_necessary(fid.clone())?;

            match self.filter_storage.check_and_consume(&fid, loss)? {
                FilterStatus::Continue => {
                    // The budget is not depleted, keep going.
                }
                FilterStatus::OutOfBudget => {
                    // The budget is depleted, stop deducting from filters.
                    return Ok(FilterStatus::OutOfBudget);
                }
            }
        }

        Ok(FilterStatus::Continue)
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
            epoch_capacity: PureDPBudget::Epsilon(3.0),
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
        assert_eq!(result, FilterStatus::Continue);

        // Second request with same budget should succeed (2.0 total)
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![1, 2, 3],
            privacy_budget: PureDPBudget::Epsilon(0.3),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert_eq!(result, FilterStatus::Continue);

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
        assert_eq!(result, FilterStatus::OutOfBudget);

        // Consume from just one epoch.
        let request = PassivePrivacyLossRequest {
            epoch_ids: vec![3],
            privacy_budget: PureDPBudget::Epsilon(0.5),
            uris: uris.clone(),
        };
        let result = pds.account_for_passive_privacy_loss(request)?;
        assert_eq!(result, FilterStatus::Continue);

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
}
