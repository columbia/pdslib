use log::info;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

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
        EpochReportRequest, PassivePrivacyLossRequest, ReportRequest,
        ReportRequestUris,
    },
    util::shared_types::Uri,
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
    // TODO(https://github.com/columbia/pdslib/issues/38) q-source
}

/// Struct containing the default capacity for each type of filter.
pub struct StaticCapacities<FID, B> {
    pub nc: B,
    pub c: B,
    pub qtrigger: B,
    _phantom: std::marker::PhantomData<FID>,
}

impl<FID, B> StaticCapacities<FID, B> {
    pub fn new(nc: B, c: B, qtrigger: B) -> Self {
        Self {
            nc,
            c,
            qtrigger,
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

/// API for the epoch-based PDS.
///
/// TODO(https://github.com/columbia/pdslib/issues/21): support more than PureDP
/// TODO(https://github.com/columbia/pdslib/issues/22): simplify trait bounds?
impl<U, EI, E, EE, RES, FS, ES, Q, ERR> EpochPrivateDataService<FS, ES, Q, ERR>
where
    U: Uri + Clone + Eq + Hash,
    EI: EpochId,
    E: Event<EpochId = EI, Uri = U>,
    EE: EpochEvents<E>,
    FS: FilterStorage<Budget = PureDPBudget, FilterId = FilterId<EI, U>>,
    RES: RelevantEventSelector<Event = E>,
    ES: EventStorage<Event = E, EpochEvents = EE, RelevantEventSelector = RES>,
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
    pub fn compute_report(
        &mut self,
        request: &Q,
    ) -> Result<<Q as ReportRequest>::Report, ERR> {
        info!("Computing report for request {:?}", request);

        // Collect events from event storage. If an epoch has no relevant
        // events, don't add it to the mapping.
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

        // Compute the raw report, useful for debugging and accounting.
        let num_epochs: usize = relevant_events_per_epoch.len();
        let unbiased_report =
            request.compute_report(&relevant_events_per_epoch);

        // Browse epochs in the attribution window
        for epoch_id in request.epoch_ids() {
            // Step 1. Get relevant events for the current epoch `epoch_id`.
            let epoch_relevant_events =
                relevant_events_per_epoch.get(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
            let individual_privacy_loss = self.compute_individual_privacy_loss(
                request,
                epoch_relevant_events,
                &unbiased_report,
                num_epochs,
            );

            // Step 3. Compute per-iimpression-site losses.
            let impression_site_losses = self.compute_impression_site_loss(
                epoch_relevant_events,
                &individual_privacy_loss,
            );

            // Step 4. Try to consume budget from current epoch, drop events if
            // OOB.
            let deduct_res = self.deduct_budget(
                &epoch_id,
                &individual_privacy_loss,
                request.report_uris(),
                &impression_site_losses,
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
                    return Ok(Default::default());
                }
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report =
            request.compute_report(&relevant_events_per_epoch);
        Ok(filtered_report)
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
        let impression_site_losses = HashMap::new();  // Dummy.

        // For each epoch, try to consume the privacy budget.
        for epoch_id in request.epoch_ids {
            // Try to consume budget from current epoch.
            let budget_res = self.deduct_budget(
                &epoch_id,
                &request.privacy_budget,
                request.uris.clone(),
                &impression_site_losses,
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

    // Compute the impression site loss that can be incurred at each site as a hashmap.
    fn compute_impression_site_loss(
        &self,
        epoch_events: Option<&EE>,
        epoch_budget: &PureDPBudget,
    ) -> HashMap<U, PureDPBudget> {
        let mut site_losses: HashMap<U, PureDPBudget> = HashMap::new();

        // If no events or undefined, then no impression sites map to events. Return already.
        let Some(events) = epoch_events else {
            return site_losses;
        };
        if events.is_empty() {
            return site_losses;
        }

        // Over events, find impression site URIs.
        let mut impression_counts: HashMap<U, usize> = HashMap::new();
        let mut total_impressions = 0_usize;
        for event in events.iter() {
            let uri = event.event_uris().source_uri.clone();
            // Increment the count fo rthe particular impression site URI.
            *impression_counts.entry(uri).or_insert(0) += 1;
            // Increment the total count of impressions.
            total_impressions += 1;
        }

        // Not impression is found.
        if total_impressions == 0 {
            return site_losses;
        }

        // Split the budget for each impression site.
        let eps_value = match epoch_budget {
            PureDPBudget::Epsilon(eps) => *eps,
            _ => {
                // If the budget is not a numerical value, then we cannot split it.
                return site_losses;
            }
        };
        for (site, count) in impression_counts {
            let fraction = (count as f64) / (total_impressions as f64);
            let one_site_loss = PureDPBudget::Epsilon(eps_value * fraction);
            site_losses.insert(site, one_site_loss);
        }
    
        site_losses
    }

    /// Deduct the privacy loss from the various filters.
    fn deduct_budget(
        &mut self,
        epoch_id: &EI,
        loss: &FS::Budget,
        uris: ReportRequestUris<U>,
        impression_site_losses: &HashMap<U, FS::Budget>,
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

        for (imp_site, imp_loss) in impression_site_losses {
            let fid = FilterId::QSource(epoch_id.clone(), imp_site.clone());
            self.initialize_filter_if_necessary(fid.clone())?;

            match self.filter_storage.check_and_consume(&fid, imp_loss)? {
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
    fn compute_individual_privacy_loss(
        &self,
        request: &Q,
        epoch_relevant_events: Option<&EE>,
        computed_attribution: &<Q as ReportRequest>::Report,
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
