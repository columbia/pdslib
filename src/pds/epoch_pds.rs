//! TODO(https://github.com/columbia/pdslib/issues/66): refactor this file

use std::{collections::HashMap, fmt::Debug, vec};

use log::debug;

use super::{
    accounting::{compute_epoch_loss, compute_epoch_source_losses},
    quotas::{FilterId, PdsFilterStatus},
};
use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStatus, FilterStorage},
    },
    events::traits::{
        EpochEvents, EpochId, Event, EventStorage, RelevantEventSelector, Uri,
    },
    queries::traits::{
        EpochReportRequest, PassivePrivacyLossRequest, ReportRequestUris,
    },
};

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
    U: Uri,
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
    pub fn compute_report(
        &mut self,
        request: &Q,
    ) -> Result<HashMap<Q::Uri, PdsReport<Q>>, ERR> {
        debug!("Computing report for request {:?}", request);

        // Check if this is a multi-beneficiary query, which we don't support
        // yet
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
        let num_epochs: usize = request.epoch_ids().len();
        let unfiltered_result =
            request.compute_report(&relevant_events_per_epoch);

        // Browse epochs in the attribution window
        let mut oob_filters = vec![];
        for epoch_id in request.epoch_ids() {
            // Step 1. Get relevant events for the current epoch `epoch_id`.
            let epoch_relevant_events =
                relevant_events_per_epoch.get(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
            let individual_privacy_loss = compute_epoch_loss(
                request,
                epoch_relevant_events,
                unfiltered_result
                    .uri_report_map
                    .get(&request.report_uris().querier_uris[0])
                    .unwrap(),
                num_epochs,
            );

            // Step 3. Get relevant events for the current epoch `epoch_id` per
            // source.
            let epoch_source_relevant_events =
                relevant_events_per_epoch_source.get(&epoch_id);

            // Step 4. Compute device-epoch-source losses.
            let source_losses = compute_epoch_source_losses(
                request,
                epoch_source_relevant_events,
                unfiltered_result
                    .uri_report_map
                    .get(&request.report_uris().querier_uris[0])
                    .unwrap(),
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
        let filtered_result =
            request.compute_report(&relevant_events_per_epoch);
        let main_report = PdsReport {
            filtered_report: filtered_result
                .uri_report_map
                .get(&request.report_uris().querier_uris[0])
                .unwrap()
                .clone(),
            unfiltered_report: unfiltered_result
                .uri_report_map
                .get(&request.report_uris().querier_uris[0])
                .unwrap()
                .clone(),
            oob_filters,
        };

        // Handle optimization queries when at least two intermediary URIs are
        // in the request.
        if self.is_optimization_query(filtered_result.uri_report_map) {
            let intermediary_uris =
                request.report_uris().intermediary_uris.clone();
            let mut intermediary_reports = HashMap::new();

            if filtered_result.bucket_uri_map.keys().len() > 0 {
                // Process each intermediary
                for intermediary_uri in intermediary_uris {
                    // TODO(https://github.com/columbia/pdslib/issues/55):
                    // The events should not be readable by any intermediary. In
                    // Fig 2 it seems that the first event is readable by r1.ex
                    // and r3.ex only, and the second event
                    // is readable by r2.ex and r3.ex. r3 is a special
                    // intermediary that can read all the events (maybe r3.ex =
                    // shoes.example). But feel free to keep
                    // this remark in a issue for later, because that would
                    // involve modifying the is_relevant_event logic too, to
                    // check that the intermediary_uris
                    // match. Your get_bucket_intermediary_mapping seems to
                    // serve the same purpose.
                    // Get the relevant events for this intermediary

                    // Filter report for this intermediary
                    if let Some(intermediary_filtered_report) =
                        unfiltered_result.uri_report_map.get(&intermediary_uri)
                    {
                        // Create PdsReport for this intermediary
                        let intermediary_pds_report = PdsReport {
                            filtered_report: intermediary_filtered_report
                                .clone(),
                            unfiltered_report: unfiltered_result
                                .uri_report_map
                                .get(&intermediary_uri)
                                .unwrap()
                                .clone(),
                            oob_filters: main_report.oob_filters.clone(),
                        };

                        // Add this code to deduct budget for the intermediary
                        // Create a modified request URIs with the intermediary
                        // as the querier
                        let mut intermediary_report_uris =
                            request.report_uris().clone();
                        intermediary_report_uris.querier_uris =
                            vec![intermediary_uri.clone()];

                        intermediary_reports
                            .insert(intermediary_uri, intermediary_pds_report);
                    }
                }
            }
            // Return optimization result with all intermediary reports
            // If the querier needs to receive a report for itself too, need to
            // add itself as an intermediary in the request
            return Ok(intermediary_reports);
        }

        // For regular requests or optimization queries without intermediary
        // reports
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
        filter_id: &FilterId<EI, U>,
    ) -> Result<(), ERR> {
        let filter_initialized =
            self.filter_storage.is_initialized(&filter_id)?;

        if !filter_initialized {
            let create_filter_result =
                self.filter_storage.new_filter(&filter_id);

            if create_filter_result.is_err() {
                return Ok(());
            }
        }
        Ok(())
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
                .push(FilterId::Nc(*epoch_id, query_uri));
        }
        device_epoch_filter_ids
            .push(FilterId::QTrigger(*epoch_id, uris.trigger_uri));
        device_epoch_filter_ids.push(FilterId::C(*epoch_id));

        // NC, C and QTrigger all have the same device-epoch level loss
        let mut filters_to_consume = HashMap::new();
        for filter_id in device_epoch_filter_ids {
            filters_to_consume.insert(filter_id, loss);
        }

        // Add the QSource filters with their own device-epoch-source level loss
        for (source, loss) in source_losses {
            let fid = FilterId::QSource(*epoch_id, source.clone());
            filters_to_consume.insert(fid, loss);
        }

        // Try to consume the privacy loss from the filters
        let mut oob_filters = vec![];
        for (fid, loss) in filters_to_consume {
            self.initialize_filter_if_necessary(&fid)?;
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

    fn is_optimization_query(
        &self,
        site_to_report_mapping: HashMap<U, Q::Report>,
    ) -> bool {
        // TODO: May need to change this based on assumption changes.
        // If the mapping has more then 3 keys, that means it has at least 2
        // intermediary sites (since we map the main report only to the first
        // querier URI), so this would be the case where the query optimization
        // can take place.
        if site_to_report_mapping.keys().len() >= 3 {
            return true;
        }

        false
    }
}
