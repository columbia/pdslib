use std::{collections::HashMap, vec};

use log::debug;

use super::{
    accounting::{compute_epoch_loss, compute_epoch_source_losses},
    private_data_service::PdsReport,
    quotas::{FilterId, PdsFilterStatus},
};
use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStatus, FilterStorage},
    },
    events::traits::{EpochEvents, Event},
    queries::traits::{
        EpochReportRequest, QueryComputeResult, Report, ReportRequestUris,
    },
};

pub struct PrivateDataServiceCore<Q, FS, ERR>
where
    Q: EpochReportRequest,
    FS: FilterStorage<
        FilterId = FilterId<Q::EpochId, Q::Uri>,
        // TODO(https://github.com/columbia/pdslib/issues/21): generic budget
        Budget = PureDPBudget,
    >,
    ERR: From<FS::Error>,
{
    /// Filter storage interface.
    pub filter_storage: FS,

    /// Defining these generics on each individual function causes much more
    /// boilerplate, compared to defining them once here on the struct.
    _phantom: std::marker::PhantomData<(Q, ERR)>,
}

impl<R, Q, FS, ERR> PrivateDataServiceCore<Q, FS, ERR>
where
    R: Report + Clone,
    Q: EpochReportRequest<Report = R>,
    FS: FilterStorage<
        FilterId = FilterId<Q::EpochId, Q::Uri>,
        // TODO(https://github.com/columbia/pdslib/issues/21): generic budget
        Budget = PureDPBudget,
    >,
    ERR: From<FS::Error>,
{
    pub fn new(filter_storage: FS) -> Self {
        Self {
            filter_storage,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Computes a report for the given report request.
    /// This function follows `compute_attribution_report` from the Cookie
    /// Monster Algorithm (https://arxiv.org/pdf/2405.16719, Code Listing 1)
    pub fn compute_report(
        &mut self,
        mut relevant_events_per_epoch: HashMap<Q::EpochId, Q::EpochEvents>,
        request: &Q,
    ) -> Result<HashMap<Q::Uri, PdsReport<Q>>, ERR> {
        debug!("Computing report for request {:?}", request);

        // Check if this is a multi-beneficiary query, which we don't support
        // yet
        if request.report_uris().querier_uris.len() > 1 {
            todo!("Implement multi-beneficiary queries");
        }

        // For every epoch, organize events into buckets, per event's source
        // URI.
        let mut relevant_events_per_epoch_source: HashMap<
            Q::EpochId,
            HashMap<Q::Uri, Q::EpochEvents>,
        > = HashMap::new();
        for epoch_id in request.epoch_ids() {
            let Some(epoch_events) = relevant_events_per_epoch.get(&epoch_id)
            else {
                continue;
            };

            // source URI => list of events
            let mut events_per_source = HashMap::new();

            let iter = epoch_events.iter();
            for event in iter {
                let source_uri = event.event_uris().source_uri;
                events_per_source
                    .entry(source_uri.clone())
                    .or_insert_with(Q::EpochEvents::new)
                    .push(event.clone());
            }

            if !events_per_source.is_empty() {
                relevant_events_per_epoch_source
                    .insert(epoch_id.clone(), events_per_source);
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

            let filters_to_consume = self.filters_to_consume(
                &epoch_id,
                &individual_privacy_loss,
                &source_losses,
                request.report_uris(),
            );

            // Phase 1: dry run.
            let check_status = self.deduct_budget(
                &filters_to_consume,
                true, // dry run
            )?;

            match check_status {
                PdsFilterStatus::Continue => {
                    // Phase 2: Consume the budget
                    let consume_status = self.deduct_budget(
                        &filters_to_consume,
                        false, // actually consume
                    )?;

                    if consume_status != PdsFilterStatus::Continue {
                        panic!("ERR: Phase 2 failed unexpectedly wtih status {consume_status:?} after Phase 1 succeeded");
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

        let querier_uri = &request.report_uris().querier_uris[0];
        let filtered_report =
            filtered_result.uri_report_map.get(querier_uri).unwrap();
        let unfiltered_report =
            unfiltered_result.uri_report_map.get(querier_uri).unwrap();

        let main_report = PdsReport {
            filtered_report: filtered_report.clone(),
            unfiltered_report: unfiltered_report.clone(),
            oob_filters,
        };

        // Handle optimization queries when at least two intermediary URIs are
        // in the request.
        if self.is_optimization_query(&filtered_result.uri_report_map) {
            let intermediate_reports = self.calculate_optimization_query(
                request,
                unfiltered_result,
                filtered_result,
                main_report.oob_filters,
            )?;
            return Ok(intermediate_reports);
        }

        // For regular requests or optimization queries without intermediary
        // reports
        Ok(HashMap::from([(querier_uri.clone(), main_report)]))
    }

    /// Calculate how much privacy to deduct from which filters,
    /// for the given epoch and losses.
    pub fn filters_to_consume<'a>(
        &self,
        epoch_id: &'a Q::EpochId,
        loss: &'a FS::Budget,
        source_losses: &'a HashMap<Q::Uri, FS::Budget>,
        uris: ReportRequestUris<Q::Uri>,
    ) -> HashMap<FilterId<Q::EpochId, Q::Uri>, &'a PureDPBudget> {
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

        filters_to_consume
    }

    /// Deduct the privacy loss from the various filters.
    #[allow(clippy::type_complexity)]
    pub fn deduct_budget(
        &mut self,
        filters_to_consume: &HashMap<
            FilterId<Q::EpochId, Q::Uri>,
            &PureDPBudget,
        >,
        dry_run: bool,
    ) -> Result<PdsFilterStatus<FilterId<Q::EpochId, Q::Uri>>, ERR> {
        // Try to consume the privacy loss from the filters
        let mut oob_filters = vec![];
        for (fid, loss) in filters_to_consume {
            self.filter_storage.ensure_filter(fid.clone())?;
            let filter_status =
                self.filter_storage.maybe_consume(fid, loss, dry_run)?;
            if filter_status == FilterStatus::OutOfBudget {
                oob_filters.push(fid.clone());
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
        site_to_report_mapping: &HashMap<Q::Uri, Q::Report>,
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

    fn calculate_optimization_query(
        &self,
        request: &Q,
        unfiltered_result: QueryComputeResult<Q::Uri, Q::Report>,
        filtered_result: QueryComputeResult<Q::Uri, Q::Report>,
        oob_filters: Vec<FilterId<Q::EpochId, Q::Uri>>,
    ) -> Result<HashMap<Q::Uri, PdsReport<Q>>, ERR> {
        let intermediary_uris = request.report_uris().intermediary_uris.clone();
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
                    let unfiltered_report = unfiltered_result
                        .uri_report_map
                        .get(&intermediary_uri)
                        .unwrap();

                    let intermediary_pds_report = PdsReport {
                        filtered_report: intermediary_filtered_report.clone(),
                        unfiltered_report: unfiltered_report.clone(),
                        oob_filters: oob_filters.clone(),
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
        Ok(intermediary_reports)
    }
}
