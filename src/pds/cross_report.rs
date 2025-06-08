use std::{
    collections::{HashMap, HashSet},
    vec,
};

use log::{debug, warn};

use super::{
    accounting::compute_epoch_loss,
    private_data_service::PdsReport,
    quotas::{FilterId, PdsFilterStatus},
};
use crate::{
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStatus, FilterStorage},
    },
    events::{
        ppa_event::PpaEvent, relevant_events::RelevantEvents, traits::Uri,
    },
    mechanisms::NoiseScale,
    pds::core::PrivateDataServiceCore,
    queries::{
        histogram::HistogramRequest,
        ppa_histogram::{
            PpaEpochId, PpaHistogramRequest, PpaRelevantEventSelector,
            RequestedBuckets,
        },
        traits::EpochReportRequest,
    },
};

/// The attribution object that can be used to compute distinct
/// reports for distinct queriers/beneficiaries, while sharing
/// global privacy loss.
pub struct AttributionObject<Q: HistogramRequest> {
    /// The report request object
    pub request: Q,

    /// The relevant events for this request
    pub events: RelevantEvents<Q::Event>,

    /// The attributed value for each event
    pub event_values: HashMap<Q::Event, f64>,

    /// The set of histogram buckets that have already been requested.
    /// A histogram bucket can only be requested once. If it is requested
    /// again, a null report will be generated instead.
    /// If None, all buckets have already been requested.
    pub already_requested_buckets: Option<HashSet<Q::BucketKey>>,
}

impl<U, FS, ERR> PrivateDataServiceCore<PpaHistogramRequest<U>, FS, ERR>
where
    U: Uri,
    FS: FilterStorage<
        FilterId = FilterId<PpaEpochId, U>,
        Budget = PureDPBudget,
    >,
    ERR: From<FS::Error>,
{
    /// Attributes conversion value to events and deduct privacy loss from global filter and quotas.
    /// Creates an `AttributionObject` that can queriers can use to generate reports, that will deduct
    /// per-querier privacy loss and map events to their respective histogram buckets.
    ///
    /// WARNING: This is an experimental API that only offers global DP guarantees.
    pub fn measure_conversion(
        &mut self,
        request: PpaHistogramRequest<U>,
        mut relevant_events: RelevantEvents<PpaEvent<U>>,
    ) -> Result<AttributionObject<PpaHistogramRequest<U>>, ERR> {
        let uris = request.report_uris();
        let epochs = request.epoch_ids();

        // TODO(later): optimize privacy loss accounting
        if epochs.len() <= 1 {
            warn!("Cross-report optimization only saves budget when requesting more than 1 epoch. We recommend using the regular API otherwise.")
        }

        let mut oob_filters = vec![];
        for epoch_id in epochs {
            // 2 * a^max / lambda
            let NoiseScale::Laplace(noise_scale) = request.noise_scale();
            let individual_privacy_loss = request
                .histogram_multi_epoch_report_global_sensitivity()
                / noise_scale;

            let source_losses = uris
                .source_uris
                .iter()
                .map(|source_uri| (source_uri.clone(), individual_privacy_loss))
                .collect::<HashMap<_, _>>();

            // Try to consume budget from current epoch, drop events if OOB.
            // Two phase commit.
            let mut filters_to_consume = self.filters_to_consume(
                epoch_id,
                &individual_privacy_loss,
                &source_losses,
                uris,
            );

            // Do not consume per-querier, that is done in get_report().
            for querier_uri in &uris.querier_uris {
                filters_to_consume.remove(&FilterId::PerQuerier(
                    epoch_id,
                    querier_uri.clone(),
                ));
            }

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
                    relevant_events.drop_epoch(&epoch_id);

                    // Keep track of why we dropped this epoch
                    oob_filters.append(&mut filters);
                }
            }
        }

        let event_values = request
            .event_values(&relevant_events)
            .into_iter()
            .map(|(event, value)| (event.clone(), value))
            .collect::<HashMap<_, _>>();

        let attribution_object = AttributionObject {
            request,
            event_values,
            events: relevant_events,
            already_requested_buckets: Some(HashSet::new()),
        };

        Ok(attribution_object)
    }
}

impl<U: Uri> AttributionObject<PpaHistogramRequest<U>> {
    /// Get the report for a specific querier/beneficiary URI.
    pub fn get_report<FS>(
        &mut self,
        beneficiary_uri: &U,
        relevant_event_selector: &PpaRelevantEventSelector<U>,
        filter_storage: &mut FS,
    ) -> Result<PdsReport<PpaHistogramRequest<U>>, FS::Error>
    where
        FS: FilterStorage<
            FilterId = FilterId<PpaEpochId, U>,
            Budget = PureDPBudget,
        >,
    {
        let epochs = self.request.epoch_ids();
        let num_epochs = epochs.len();

        // if already_requested_buckets is None, all buckets have already
        // been requested
        let Some(already_requested_buckets) =
            &mut self.already_requested_buckets
        else {
            debug!("All buckets have already been requested, returning null report");
            return Ok(PdsReport::default());
        };

        match &relevant_event_selector.requested_buckets {
            RequestedBuckets::SpecificBuckets(requested_buckets) => {
                // if any of the requested buckets have already been previously
                // requested, abort and return null report
                for bucket in requested_buckets {
                    if already_requested_buckets.contains(bucket) {
                        debug!("Bucket {bucket:?} already requested, returning null report");
                        return Ok(PdsReport::default());
                    }
                }

                // Add the requested buckets to the already requested set
                already_requested_buckets.extend(requested_buckets);
            }
            RequestedBuckets::AllBuckets => {
                // None means this request is for all buckets.
                // Also set our already_requested_buckets to None
                self.already_requested_buckets = None;
            }
        }

        // get the attributed values for the requested events
        // only keep the buckets that are requested
        let mut event_values = HashMap::new();
        for epoch in &epochs {
            for event in self.events.for_epoch(epoch) {
                if relevant_event_selector
                    .requested_buckets
                    .contains(&event.histogram_index)
                {
                    if let Some(value) = self.event_values.get(event) {
                        event_values.insert(event.clone(), *value);
                    }
                }
            }
        }

        // Per-querier report before filtering out epochs that are OOB for the per-querier filter.
        // `compute_attribution` already filtered epochs that were OOB for the other filters/quotas.
        let mut unfiltered_report =
            self.request.map_events_to_buckets(&event_values);

        let mut oob_filters = vec![];
        for epoch_id in epochs {
            let epoch_relevant_events = self.events.for_epoch(&epoch_id);

            // Compute per-querier individual loss for current epoch.
            let individual_privacy_loss = compute_epoch_loss(
                &self.request,
                epoch_relevant_events,
                &unfiltered_report,
                num_epochs,
            );

            let filter_id =
                FilterId::PerQuerier(epoch_id, beneficiary_uri.clone());
            let filter_status = filter_storage
                .try_consume(&filter_id, &individual_privacy_loss)?;

            if filter_status == FilterStatus::OutOfBudget {
                // Not enough budget, drop events without any filter
                // consumption
                for event in epoch_relevant_events {
                    event_values.remove(event);
                }

                // Keep track of why we dropped this epoch
                oob_filters.push(filter_id);
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report,
        // using the attributed event values precomputed by `measure_conversion`.
        let filtered_report = self.request.map_events_to_buckets(&event_values);

        let report = PdsReport {
            filtered_report,
            unfiltered_report,
            oob_filters,
        };
        Ok(report)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::*;
    use crate::{
        events::{ppa_event::PpaEvent, traits::EventUris},
        pds::{
            aliases::{PpaFilterStorage, PpaPdsCore},
            quotas::StaticCapacities,
        },
        queries::{
            ppa_histogram::{
                PpaHistogramConfig, PpaHistogramRequest,
                PpaRelevantEventSelector,
            },
            traits::ReportRequestUris,
        },
    };

    #[test]
    fn test_cross_report_optimization() -> Result<(), anyhow::Error> {
        // Create PDS with mock capacities
        let capacities = StaticCapacities::mock();
        let filters = PpaFilterStorage::new(capacities.clone())?;
        let mut pds = PpaPdsCore::<_>::new(filters);

        // Create test URIs
        let source_uri = "blog.example.com".to_string();
        let trigger_uri = "shoes.example.com".to_string();
        let querier_uris = vec![
            "r1.ex".to_string(), // bucket 1
            "r2.ex".to_string(), // bucket 2
            "r3.ex".to_string(), // also bucket 2
        ];

        // Create event URIs with appropriate intermediaries
        let event_uris = EventUris {
            source_uri: source_uri.clone(),
            trigger_uris: vec![trigger_uri.clone()],
            querier_uris: querier_uris.clone(),
        };

        // Create report request URIs
        let report_request_uris = ReportRequestUris {
            trigger_uri: trigger_uri.clone(),
            source_uris: vec![source_uri.clone()],
            querier_uris: querier_uris.clone(),
        };

        // Register an early event with bucket 1 - this should be overridden by
        // last-touch attribution
        let early_event = PpaEvent {
            id: 1,
            timestamp: 100,
            epoch_number: 1,
            histogram_index: 1, // r1.ex bucket
            uris: event_uris.clone(),
            filter_data: 1,
        };

        // The event that should be attributed (latest timestamp in epoch 1)
        // We'll use a histogram index that's covered by both intermediaries (3)
        let main_event = PpaEvent {
            id: 2,
            timestamp: 200, /* Later timestamp so this event is picked by
                             * last-touch */
            epoch_number: 1,
            histogram_index: 2, // A bucket that will be kept and read by r2.ex
            uris: event_uris.clone(),
            filter_data: 1,
        };

        let events = HashMap::from([(1, vec![early_event, main_event])]);
        let relevant_events = RelevantEvents::from_mapping(events);

        let config = PpaHistogramConfig {
            start_epoch: 1,
            end_epoch: 2,
            attributable_value: 100.0,
            max_attributable_value: 200.0,
            requested_epsilon: 1.0,
            histogram_size: 3,
        };

        let relevant_event_selector = |bucket: u64| PpaRelevantEventSelector {
            report_request_uris: report_request_uris.clone(),
            is_matching_event: Box::new(|_: u64| true),
            requested_buckets: vec![bucket].into(),
        };

        let request = PpaHistogramRequest::new(
            &config,
            PpaRelevantEventSelector {
                report_request_uris: report_request_uris.clone(),
                is_matching_event: Box::new(|_| true),
                requested_buckets: vec![1].into(),
            },
        )
        .expect("Failed to create request");

        let NoiseScale::Laplace(noise_scale) = request.noise_scale();

        // Process the request
        let mut attr_object =
            pds.measure_conversion(request, relevant_events.clone())?;

        // Verify r1.ex's report has bucket 1, which has zero attribution.
        let r1_report = attr_object
            .get_report(
                &querier_uris[0],
                &relevant_event_selector(1),
                &mut pds.filter_storage,
            )
            .unwrap();
        let r1_bins = &r1_report.filtered_report.bin_values;
        assert!(r1_bins.is_empty(), "1 bucket for r1.ex should have been filtered out by last-touch attribution");

        // Verify r2.ex's report has bucket 2
        let r2_report = attr_object
            .get_report(
                &querier_uris[1],
                &relevant_event_selector(2),
                &mut pds.filter_storage,
            )
            .unwrap();
        let r2_bins = &r2_report.filtered_report.bin_values;
        assert_eq!(
            r2_bins.len(),
            1,
            "Expected 1 bucket for r2.ex, got {r2_bins:?}"
        );
        assert!(r2_bins.contains_key(&2), "Expected bucket 3 for r2.ex");

        // Intermediary r2 receives the value from the main event
        assert_eq!(
            r2_bins.get(&2),
            Some(&config.attributable_value),
            "Incorrect value for r2.ex bucket 3"
        );

        // Verify r3.ex's report is empty, as bucket 2 was already requested by r2.ex
        let r3_report = attr_object
            .get_report(
                &querier_uris[2],
                &relevant_event_selector(2),
                &mut pds.filter_storage,
            )
            .unwrap();
        let r3_bins = &r3_report.filtered_report.bin_values;
        assert!(r3_bins.is_empty(), "Bucket 2 for r3.ex should be empty");

        // Verify the privacy budget was deducted only once from the global filter,
        // despite three reports being generated
        let initial_budget = capacities.global;
        let post_budget =
            pds.filter_storage.remaining_budget(&FilterId::Global(1))?;

        assert!(
            initial_budget.is_finite() && post_budget.is_finite(),
            "Expected finite budget deduction"
        );

        let deduction = initial_budget - post_budget;

        // Verify budget was actually deducted
        assert!(
            deduction > 0.0,
            "No budget was deducted from the global filter",
        );

        // Calculate what would be deducted with vs. without
        // optimization
        let expected_deduction = 2.0 * config.attributable_value / noise_scale;

        // Verify deduction is close to single event (cross-report
        // optimization working)
        assert_eq!(
            deduction, expected_deduction,
            "Budget deduction indicates optimization is not working"
        );

        Ok(())
    }

    fn test_cross_epoch_last_touch() -> Result<(), anyhow::Error> {
        let capacities = StaticCapacities::mock();
        let filters = PpaFilterStorage::new(capacities.clone())?;
        let mut pds = PpaPdsCore::<_>::new(filters);

        let event1 = PpaEvent {
            id: 1,
            timestamp: 100,
            epoch_number: 1,
            histogram_index: 1,
            uris: EventUris::mock(),
            filter_data: 1,
        };
        let event2 = PpaEvent {
            id: 2,
            timestamp: 200, // Later timestamp
            epoch_number: 2,
            histogram_index: 1, // Same bucket as event1
            uris: EventUris::mock(),
            filter_data: 1,
        };

        // set epoch 2 PerQuerier filter to be OOB
        let querier_uri = ReportRequestUris::mock().querier_uris[0].clone();
        let filter_id = FilterId::PerQuerier(2, querier_uri.clone());
        let filter_capacity = capacities.per_querier;
        pds.filter_storage
            .try_consume(&filter_id, &filter_capacity)?;

        // get attribution object
        let request = PpaHistogramRequest::new(
            &PpaHistogramConfig {
                start_epoch: 1,
                end_epoch: 2,
                attributable_value: 100.0,
                max_attributable_value: 200.0,
                requested_epsilon: 1.0,
                histogram_size: 3,
            },
            PpaRelevantEventSelector {
                report_request_uris: ReportRequestUris::mock(),
                is_matching_event: Box::new(|_| true),
                requested_buckets: vec![1].into(),
            },
        )
        .unwrap();
        let relevant_events = RelevantEvents::from_vec(vec![event1, event2]);
        let mut attr_object =
            pds.measure_conversion(request, relevant_events)?;

        let report = attr_object.get_report(
            &querier_uri,
            &PpaRelevantEventSelector {
                report_request_uris: ReportRequestUris::mock(),
                is_matching_event: Box::new(|_| true),
                requested_buckets: RequestedBuckets::AllBuckets,
            },
            &mut pds.filter_storage,
        )?;

        // the attribution should be empty, as the last-touch event's
        // epoch was OOB for that querier
        assert!(
            report.filtered_report.bin_values.is_empty(),
            "Expected empty report for querier {querier_uri}, but got: {:?}",
            report.filtered_report.bin_values
        );

        Ok(())
    }
}
