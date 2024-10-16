use pdslib::pds::simple::SimplePDS;
use pdslib::events::simple::SimpleEvent;
use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchAttributionReportRequest;

use pdslib::pds::traits::PrivateDataService;

#[test]
fn main() {
    let mut pds = SimplePDS::new();
    let event = SimpleEvent { id: 1, epoch_number: 1, value: 3};
    pds.register_event(event).unwrap();
    let report_request = SimpleLastTouchAttributionReportRequest { attributable_value: 3.0 };
    let report = pds.compute_report(report_request);
    assert_eq!(report.attributed_value, None);
}