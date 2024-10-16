use crate::{events::simple::{SimpleEpoch, SimpleEvent}};
use crate::queries::simple_last_touch_histogram::{SimpleLastTouchHistogramQuery, SimpleLastTouchHistogramReport,SimpleLastTouchAttributionReportRequest};
use std::collections::HashMap;

use super::traits::PrivateDataService;

pub struct SimplePDS {
    epochs: HashMap<usize, SimpleEpoch>,
}

impl SimplePDS {
    pub fn new() -> SimplePDS {
        SimplePDS {
            epochs: HashMap::new(),
        }
    }
}

impl PrivateDataService<SimpleEvent, SimpleLastTouchHistogramQuery> for SimplePDS {

    fn register_event(&mut self, event: SimpleEvent) -> Result<(), ()> {
        let epoch_number = event.epoch_number;
        let epoch = self.epochs.entry(epoch_number).or_insert(SimpleEpoch { events: Vec::new() });
        epoch.events.push(event);
        Ok(())
    }

    fn compute_report(&mut self, request: SimpleLastTouchAttributionReportRequest) -> SimpleLastTouchHistogramReport {

        println!("Computing report for request: {:?}", request);
        println!("Current data: {:?}", self.epochs);

        // TODO: compute actual LTA 
        SimpleLastTouchHistogramReport {
            attributed_value: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_pds() {
        let mut pds = SimplePDS::new();
        let event = SimpleEvent { id: 1, epoch_number: 1, value: 3};
        pds.register_event(event).unwrap();
        let report_request = SimpleLastTouchAttributionReportRequest { attributable_value: 3.0 };
        let report = pds.compute_report(report_request);
        assert_eq!(report.attributed_value, None);
    }
}