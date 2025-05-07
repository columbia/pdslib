use core::f64;
use std::collections::HashMap;

use log::debug;

use crate::{
    budget::pure_dp_filter::PureDPBudget,
    events::traits::EpochEvents,
    mechanisms::{NoiseScale, NormType},
    queries::traits::EpochReportRequest,
};

/// Pure DP individual privacy loss, following
/// `compute_individual_privacy_loss` from Code Listing 1 in Cookie Monster (https://arxiv.org/pdf/2405.16719).
///
/// TODO(https://github.com/columbia/pdslib/issues/21): generic budget.
pub fn compute_epoch_loss<Q: EpochReportRequest>(
    request: &Q,
    epoch_relevant_events: Option<&Q::EpochEvents>,
    computed_attribution: &Q::Report,
    num_epochs: usize,
) -> PureDPBudget {
    // Case 1: Epoch with no relevant events
    match epoch_relevant_events {
        None => {
            return PureDPBudget::from(0.0);
        }
        Some(epoch_events) => {
            if epoch_events.is_empty() {
                return PureDPBudget::from(0.0);
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

    debug!("Individual sensitivity: {individual_sensitivity} for {num_epochs} epochs");

    let NoiseScale::Laplace(noise_scale) = request.noise_scale();

    // Treat near-zero noise scales as non-private, i.e. requesting infinite
    // budget, which can only go through if filters are also set to
    // infinite capacity, e.g. for debugging. The machine precision
    // `f64::EPSILON` is not related to privacy.
    if noise_scale.abs() < f64::EPSILON {
        return PureDPBudget::from(f64::INFINITY);
    }

    // In Cookie Monster, we have `query_global_sensitivity` /
    // `requested_epsilon` instead of just `noise_scale`.
    PureDPBudget::from(individual_sensitivity / noise_scale)
}

/// Compute the privacy loss at the device-epoch-source level.
/// From Big Bird, similar idea as Cookie Monster but at a finer granularity.
pub fn compute_epoch_source_losses<Q: EpochReportRequest>(
    request: &Q,
    relevant_events_per_epoch_source: Option<&HashMap<Q::Uri, Q::EpochEvents>>,
    computed_attribution: &Q::Report,
    num_epochs: usize,
) -> HashMap<Q::Uri, PureDPBudget> {
    let mut per_source_losses = HashMap::new();

    // Collect sources and noise scale from the request.
    let requested_sources = request.report_uris().source_uris;
    let NoiseScale::Laplace(noise_scale) = request.noise_scale();

    // Count requested sources for case analysis
    let num_requested_sources = requested_sources.len();

    for source in requested_sources {
        // No relevant events map, or no events for this source, or empty
        // events
        let has_no_relevant_events = match relevant_events_per_epoch_source {
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
            per_source_losses.insert(source, PureDPBudget::from(f64::INFINITY));
        } else {
            // In Cookie Monster, we have `query_global_sensitivity` /
            // `requested_epsilon` instead of just `noise_scale`.
            per_source_losses.insert(
                source,
                PureDPBudget::from(individual_sensitivity / noise_scale),
            );
        }
    }

    per_source_losses
}
