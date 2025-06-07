use std::{
    collections::{HashMap, HashSet},
    vec,
};

use log::debug;

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
        },
        traits::EpochReportRequest,
    },
};

impl<U, FS, ERR> PrivateDataServiceCore<PpaHistogramRequest<U>, FS, ERR>
where
    U: Uri,
    FS: FilterStorage<
        FilterId = FilterId<PpaEpochId, U>,
        Budget = PureDPBudget,
    >,
    ERR: From<FS::Error>,
{
    pub fn measure_conversion(
        &mut self,
        request: PpaHistogramRequest<U>,
        mut relevant_events: RelevantEvents<PpaEvent<U>>,
    ) -> Result<AttributionObject<PpaHistogramRequest<U>>, ERR> {
        let uris = request.report_uris();

        let epochs = request.epoch_ids();

        let mut oob_filters = vec![];
        for epoch_id in epochs {
            // 2 * a^max / lambda for every source URI
            let NoiseScale::Laplace(noise_scale) = request.noise_scale();
            let individual_privacy_loss = request
                .histogram_multi_epoch_report_global_sensitivity()
                / noise_scale;

            // 2 * a^max / lambda for every source URI
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
            // Change the per-querier budgets to 0 to still initialize the
            // filter.
            for querier_uri in &uris.querier_uris {
                filters_to_consume.remove(
                    &FilterId::PerQuerier(epoch_id, querier_uri.clone()),
                    // &0.0, // no budget consumption for per-querier filter
                );
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

        let attribution_object = AttributionObject {
            request,
            events: relevant_events,
            already_requested_buckets: HashSet::new(),
        };

        Ok(attribution_object)
    }
}

/// The attribution object that can be used to compute distinct
/// reports for distinct queriers/benificiaries.
pub struct AttributionObject<Q: HistogramRequest> {
    /// The report request object
    pub request: Q,

    /// The relevant events for this request
    pub events: RelevantEvents<Q::Event>,

    /// The set of histogram buckets that have already been requested.
    /// A histogram bucket can only be requested once. If it is requested
    /// again, a null report will be generated instead.
    pub already_requested_buckets: HashSet<Q::BucketKey>,
}

impl<U: Uri> AttributionObject<PpaHistogramRequest<U>> {
    /// Get the report for a specific querier/benificiary URI.
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

        // if any of the requested buckets have already been previously
        // requested, abort and return null report
        for bucket in &relevant_event_selector.requested_buckets {
            if self.already_requested_buckets.contains(bucket) {
                debug!("Bucket {bucket:?} already requested, returning null report");
                return Ok(PdsReport::default());
            }
        }

        // Add the requested buckets to the already requested set
        self.already_requested_buckets
            .extend(&relevant_event_selector.requested_buckets);

        let mut relevant_events = self.events.clone();
        let mut unfiltered_report =
            self.request.compute_report(&relevant_events);

        // only keep the buckets that are requested
        unfiltered_report.bin_values.retain(|bucket, _| {
            relevant_event_selector.requested_buckets.contains(bucket)
        });

        let mut oob_filters = vec![];
        for epoch_id in epochs {
            let epoch_relevant_events = self.events.for_epoch(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
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
                relevant_events.drop_epoch(&epoch_id);

                // Keep track of why we dropped this epoch
                oob_filters.push(filter_id);
            }
        }

        // Now that we've dropped OOB epochs, we can compute the final report.
        let mut filtered_report = self.request.compute_report(&relevant_events);

        // only keep the buckets that are requested
        filtered_report.bin_values.retain(|bucket, _| {
            relevant_event_selector.requested_buckets.contains(bucket)
        });

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

        // Create histogram request with optimization query flag set to true
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
            requested_buckets: vec![bucket],
        };

        let request = PpaHistogramRequest::new(
            &config,
            PpaRelevantEventSelector {
                report_request_uris: report_request_uris.clone(),
                is_matching_event: Box::new(|_| true),
                requested_buckets: vec![1],
            },
        )
        .expect("Failed to create request");

        let NoiseScale::Laplace(noise_scale) = request.noise_scale();

        // Process the request
        let mut attr_object =
            pds.measure_conversion(request, relevant_events.clone())?;

        // Verify r1.ex's report has bucket 1
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

        // Verify r3.ex's report has bucket 2, but should not receive any value
        // as bucket 2 was already requested by r2.ex
        let r3_report = attr_object
            .get_report(
                &querier_uris[2],
                &relevant_event_selector(2),
                &mut pds.filter_storage,
            )
            .unwrap();
        let r3_bins = &r3_report.filtered_report.bin_values;
        assert!(r3_bins.is_empty(), "Bucket 2 for r3.ex should be empty");

        // Verify the privacy budget was deducted only once
        // Despite three reports being generated
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
}
