#[cfg(feature = "experimental")]
pub mod debug_reports {
    use crate::pds::private_data_service::PdsReport;
    use crate::queries::traits::EpochReportRequest;
    
    /// Simple debugging helper to access unfiltered reports
    /// WARNING: Only for research/debugging - exposes unfiltered data
    pub fn log_unfiltered_report<Q: EpochReportRequest>(
        report: &PdsReport<Q>,
        query_id: &str,
    ) where
        Q::Report: std::fmt::Debug,
    {
        log::warn!(
            "[EXPERIMENTAL] Unfiltered report for query '{}': {:?}", 
            query_id, 
            report.unfiltered_report
        );
    }
    
    /// Access unfiltered report for bias analysis
    pub fn get_unfiltered_report<Q: EpochReportRequest>(
        report: &PdsReport<Q>
    ) -> &Q::Report {
        &report.unfiltered_report
    }
}

#[cfg(feature = "experimental")]
pub mod baselines {
    pub fn run_special_baseline_algorithm(input_data: &str, config_param: i32) -> String {
        format!("Baseline processed: {} with param {}", input_data, config_param)
    }
}