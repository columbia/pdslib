use pdslib::budget::hashmap_filter_storage::HashMapFilterStorage;
use pdslib::budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};
use pdslib::events::simple_events::{SimpleEvent, SimpleEventStorage};
use pdslib::pds::implem::PrivateDataServiceImpl;
use pdslib::pds::traits::PrivateDataService;
use pdslib::queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest;

#[test]
fn main() {
    let mut events = SimpleEventStorage::new();
    let mut filters: HashMapFilterStorage<
        usize,
        PureDPBudgetFilter,
        PureDPBudget,
    > = HashMapFilterStorage::new();

    let mut pds = PrivateDataServiceImpl {
        filter_storage: filters,
        event_storage: events,
        _phantom: std::marker::PhantomData::<SimpleLastTouchHistogramRequest>,
    };

    let event = SimpleEvent {
        id: 1,
        epoch_number: 1,
        value: 3,
    };
    pds.register_event(event).unwrap();
    let report_request = SimpleLastTouchHistogramRequest {
        epoch_start: 1,
        epoch_end: 1,
        attributable_value: 3.0,
    };
    let report = pds.compute_report(report_request);
    assert_eq!(report.attributed_value, None);
}
