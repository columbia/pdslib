use std::{cell::Cell, marker::PhantomData, vec};

use log::debug;

use super::{
    accounting::{compute_epoch_loss, compute_epoch_source_losses},
    private_data_service::PdsReport,
    quotas::{FilterId, PdsFilterStatus},
};
use crate::{
    actions::traits::ActionStorage,
    budget::{
        pure_dp_filter::PureDPBudget,
        traits::{FilterStatus, FilterStorage},
    },
    events::relevant_events::RelevantEvents,
    queries::traits::{EpochReportRequest, Report, ReportRequestUris},
};

pub struct PrivateDataServiceCore<Q, FS, AS, ERR>
where
    Q: EpochReportRequest,
    FS: FilterStorage<
            FilterId = FilterId<Q::EpochId, Q::Uri>,
            Budget = PureDPBudget,
        >,
    AS: ActionStorage<EpochId = Q::EpochId, Uri = Q::Uri>,
    ERR: From<FS::Error> + From<AS::Error>,
{
    /// Filter storage interface.
    pub filter_storage: FS,

    /// Action storage interface.
    pub action_storage: AS,

    /// This PhantomData serves two purposes:
    /// 1. It Defines the Q and ERR generics on the struct instead of on each
    ///    individual function, reducing boilerplate
    /// 2. Cell<> ensures this struct is not Sync, thus not usable from
    ///    multiple multiple threads simultaneously
    _phantom: PhantomData<Cell<(Q, ERR)>>,
}

impl<R, Q, FS, AS, ERR> PrivateDataServiceCore<Q, FS, AS, ERR>
where
    R: Report + Clone,
    Q: EpochReportRequest<Report = R>,
    FS: FilterStorage<
            FilterId = FilterId<Q::EpochId, Q::Uri>,
            Budget = PureDPBudget,
        >,
    AS: ActionStorage<EpochId = Q::EpochId, Uri = Q::Uri>,
    ERR: From<FS::Error> + From<AS::Error>,
{
    pub fn new(filter_storage: FS, action_storage: AS) -> Self {
        Self {
            filter_storage,
            action_storage,
            _phantom: PhantomData,
        }
    }

    /// Computes a report for the given report request.
    /// This function follows Algorithm 2 from the Big Bird paper
    /// (https://arxiv.org/abs/2506.05290, Alg. 2)
    pub fn compute_report(
        &mut self,
        request: &Q,
        // mutable, as we will drop out-of-budget epochs from it
        mut relevant_events: RelevantEvents<Q::Event>,
        action_id: Option<AS::ActionId>,
    ) -> Result<PdsReport<Q>, ERR> {
        debug!("Computing report for request {request:?}");

        let uris = request.report_uris();

        // Check if this is a multi-beneficiary query, which we don't support
        // yet
        if uris.querier_uris.len() > 1 {
            unimplemented!("Multi-beneficiary queries");
        }

        let epochs = request.epoch_ids();
        let num_epochs = epochs.len();

        // Compute the raw report, useful for debugging and accounting.
        let unfiltered_report = request.compute_report(&relevant_events);

        let mut drop_epoch_reasons: Vec<DropEpochReason<_>> = vec![];

        // First, enforce action quotas
        if let Some(aid) = action_id {
            let conv_site = &uris.trigger_uri;

            for &epoch_id in &epochs {
                let allowed = self
                    .action_storage
                    .try_record_site(aid, epoch_id, conv_site)?;

                if !allowed {
                    // Oscar Paper: "If quota-count is exceeded in epoch e...
                    // nullifies only epoch e's data"
                    relevant_events.drop_epoch(&epoch_id);
                    drop_epoch_reasons
                        .push(DropEpochReason::CountQuotaExceeded);
                }
            }
        }

        // Browse epochs in the attribution window
        for epoch_id in epochs {
            // Step 1. Get relevant events for the current epoch `epoch_id`.
            let epoch_relevant_events = relevant_events.for_epoch(&epoch_id);

            // Step 2. Compute individual loss for current epoch.
            let individual_privacy_loss = compute_epoch_loss(
                request,
                epoch_relevant_events,
                &unfiltered_report,
                num_epochs,
            );

            // Step 3. Compute device-epoch-source losses.
            let source_losses = compute_epoch_source_losses(
                request,
                relevant_events.sources_for_epoch(&epoch_id),
                &unfiltered_report,
                num_epochs,
            );

            // Step 4. Try to consume budget from current epoch, drop events if
            // OOB. Two phase commit.
            let filters_to_consume = self.filters_to_consume(
                epoch_id,
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
                        panic!(
                            "ERR: Phase 2 failed unexpectedly wtih status {consume_status:?} after Phase 1 succeeded"
                        );
                    }
                }

                PdsFilterStatus::OutOfBudget(filters) => {
                    // Not enough budget, drop events without any filter
                    // consumption
                    relevant_events.drop_epoch(&epoch_id);

                    // Keep track of why we dropped this epoch
                    drop_epoch_reasons
                        .push(DropEpochReason::OutOfBudget(filters));
                }
            }
        }

        debug!(
            "Relevant events after filtering OOB epochs: {relevant_events:?}"
        );

        // Now that we've dropped OOB epochs, we can compute the final report.
        let filtered_report = request.compute_report(&relevant_events);
        debug!("Filtered report: {filtered_report:?}");

        #[cfg(feature = "experimental")]
        let report_with_metadata = PdsReport {
            filtered_report,
            unfiltered_report,
            drop_epoch_reasons,
        };
        #[cfg(not(feature = "experimental"))]
        let report_with_metadata = PdsReport {
            filtered_report,
            ..Default::default()
        };

        Ok(report_with_metadata)
    }

    /// Calculate how much privacy to deduct from which filters,
    /// for the given epoch and losses.
    #[allow(clippy::type_complexity)]
    pub fn filters_to_consume<'a>(
        &self,
        epoch_id: Q::EpochId,
        loss: &'a FS::Budget,
        source_losses: &'a Vec<(Q::Uri, FS::Budget)>,
        uris: &ReportRequestUris<Q::Uri>,
    ) -> Vec<(FilterId<Q::EpochId, Q::Uri>, &'a PureDPBudget)> {
        // Build the filter IDs for PerQuerier, Global and TriggerQuota
        let mut device_epoch_filter_ids = Vec::new();
        for query_uri in &uris.querier_uris {
            device_epoch_filter_ids
                .push(FilterId::PerQuerier(epoch_id, query_uri.clone()));
        }
        device_epoch_filter_ids
            .push(FilterId::TriggerQuota(epoch_id, uris.trigger_uri.clone()));
        device_epoch_filter_ids.push(FilterId::Global(epoch_id));

        let mut filters_to_consume = Vec::with_capacity(
            device_epoch_filter_ids.len() + source_losses.len(),
        );

        // PerQuerier, Global and TriggerQuota all have the same device-epoch
        // level loss
        for filter_id in device_epoch_filter_ids {
            filters_to_consume.push((filter_id, loss));
        }

        // Add the SourceQuota filters with their own device-epoch-source level
        // loss
        for (source, loss) in source_losses {
            let fid = FilterId::SourceQuota(epoch_id, source.clone());
            filters_to_consume.push((fid, loss));
        }

        filters_to_consume
    }

    /// Deduct the privacy loss from the various filters.
    #[allow(clippy::type_complexity)]
    pub fn deduct_budget(
        &mut self,
        filters_to_consume: &Vec<(FilterId<Q::EpochId, Q::Uri>, &PureDPBudget)>,
        dry_run: bool,
    ) -> Result<PdsFilterStatus<FilterId<Q::EpochId, Q::Uri>>, ERR> {
        // Try to consume the privacy loss from the filters
        let mut oob_filters = vec![];
        for (fid, loss) in filters_to_consume {
            let filter_status = match dry_run {
                true => self.filter_storage.can_consume(fid, loss)?,
                false => self.filter_storage.try_consume(fid, loss)?,
            };

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
}

#[derive(Debug)]
pub enum DropEpochReason<FID> {
    CountQuotaExceeded,
    OutOfBudget(Vec<FID>),
}
