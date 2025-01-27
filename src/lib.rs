pub mod budget;
pub mod events;
pub mod mechanisms;
pub mod pds;
pub mod queries;

use jni::objects::{JClass};
use jni::sys::{jdouble, jlong, jstring};
use jni::JNIEnv;

use once_cell::sync::Lazy; // or lazy_static::lazy_static
use std::collections::HashMap;
use std::sync::Mutex;
use rand::random;

use budget::hashmap_filter_storage::HashMapFilterStorage;
use budget::pure_dp_filter::{PureDPBudget, PureDPBudgetFilter};
use events::simple_event::SimpleEvent;
use events::hashmap_event_storage::HashMapEventStorage;
use events::traits::RelevantEventSelector;
use pds::implem::PrivateDataServiceImpl;
use pds::traits::PrivateDataService;
use queries::simple_last_touch_histogram::SimpleLastTouchHistogramRequest;

// Type aliases for simplicity
#[derive(Clone, Debug, Default)]
pub struct AlwaysRelevantSelectorInLib;

impl RelevantEventSelector for AlwaysRelevantSelectorInLib {
    type Event = SimpleEvent;

    fn is_relevant_event(&self, _event: &Self::Event) -> bool {
        true
    }
}

type MyFS = HashMapFilterStorage<usize, PureDPBudgetFilter, PureDPBudget>;
type MyES = HashMapEventStorage<SimpleEvent, AlwaysRelevantSelectorInLib>;
type MyQ  = SimpleLastTouchHistogramRequest;

// Global PDS map
static PDS_INSTANCES: Lazy<Mutex<HashMap<i64, PrivateDataServiceImpl<MyFS, MyES, MyQ>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// JNI: createPDS
#[no_mangle]
pub extern "C" fn Java_com_example_myapp_DemoJni_createPDS(
    _env: JNIEnv,
    _class: JClass,
    epsilon: jdouble,
) -> jlong {
    let filters: MyFS = HashMapFilterStorage::new();
    let events  = HashMapEventStorage::new();

    let mut pds = PrivateDataServiceImpl {
        filter_storage: filters,
        event_storage: events,
        epoch_capacity: PureDPBudget { epsilon },
        _phantom: std::marker::PhantomData,
    };

    let handle = random::<i64>();
    PDS_INSTANCES.lock().unwrap().insert(handle, pds);
    handle
}

// JNI: registerEvent
#[no_mangle]
pub extern "C" fn Java_com_example_myapp_DemoJni_registerEvent(
    _env: JNIEnv,
    _class: JClass,
    pds_handle: jlong,
    id: jlong,
    epoch_number: jlong,
    event_key: jlong,
) {
    let mut instances = PDS_INSTANCES.lock().unwrap();
    if let Some(pds) = instances.get_mut(&(pds_handle as i64)) {
        let event = SimpleEvent {
            id: id as usize,
            epoch_number: epoch_number as usize,
            event_key: event_key as usize,
        };
        pds.register_event(event).unwrap();
    }
}

// JNI: computeReport
#[no_mangle]
pub extern "C" fn Java_com_example_myapp_DemoJni_computeReport(
    env: JNIEnv,
    _class: JClass,
    pds_handle: jlong,
    epoch_start: jlong,
    epoch_end: jlong,
    attributable_value: jdouble,
    noise_scale: jdouble,
) -> jstring {
    let mut instances = PDS_INSTANCES.lock().unwrap();
    if let Some(pds) = instances.get_mut(&(pds_handle as i64)) {
        let report_request = SimpleLastTouchHistogramRequest {
            epoch_start: epoch_start as usize,
            epoch_end: epoch_end as usize,
            attributable_value,
            noise_scale,
            is_relevant_event: always_relevant_event,
        };

        let report = pds.compute_report(report_request);
        if let Some((epoch_number, event_key, value)) = report.attributed_value {
            let result = format!("{},{},{}", epoch_number, event_key, value);
            // Convert to JNI string, return raw pointer
            return env.new_string(result).unwrap().into_raw();
        }
    }
    env.new_string("None").unwrap().into_raw()
}

// Dummy reference to keep createPDS symbol from being stripped
#[allow(dead_code)]
#[no_mangle]
pub extern "C" fn Java_com_example_myapp_DemoJni_createPDS_dummy_reference() {
    let _ = Java_com_example_myapp_DemoJni_createPDS;
}

// (Optional) Rust tests if you want to test the underlying logic
#[cfg(test)]
mod tests {
    use super::*; // Bring in your PDS logic from the parent module

    #[test]
    fn test_demo_like_flow() {
        // 1) Create PDS (similar to your original code)
        println!("Creating FKKK instance");
        let mut filters: MyFS = HashMapFilterStorage::new();
        let events = HashMapEventStorage::new();
        // Feel free to print any debug here if that’s what your code does:
        let mut pds = PrivateDataServiceImpl {
            filter_storage: filters,
            event_storage: events,
            epoch_capacity: PureDPBudget { epsilon: 1.0 }, // or whatever
            _phantom: std::marker::PhantomData,
        };

        // 2) Register Event
        let demo_event = SimpleEvent {
            id: 1,
            epoch_number: 10,
            event_key: 123,
        };
        println!("Registering event {:?}", demo_event);
        pds.register_event(demo_event).unwrap();

        // 3) Compute Report
        let request = SimpleLastTouchHistogramRequest {
            epoch_start: 0,
            epoch_end: 100,
            attributable_value: 5.0,
            noise_scale: 0.0, // no noise for a stable test
            is_relevant_event: always_relevant_event,
        };
        println!("Computing report for request {:?}", request);
        let report = pds.compute_report(request);

        // 4) Check result
        if let Some((epoch_number, event_key, value)) = report.attributed_value {
            // For example: (10, 123, 5.0)
            assert_eq!(epoch_number, 10, "Wrong epoch in report!");
            assert_eq!(event_key, 123, "Wrong event_key in report!");
            assert!((value - 5.0).abs() < f64::EPSILON, "Wrong value in report!");
            println!("Got expected report: {}, {}, {}", epoch_number, event_key, value);
        } else {
            panic!("Expected a report with an attributed value, got None instead");
        }
    }

    #[test]
    fn test_demo_like_flow_with_half_epsilon() {
        // 1) Create PDS with a smaller epsilon
        println!("Creating PDS with epsilon=0.5");
        let filters: MyFS = HashMapFilterStorage::new();
        let events  = HashMapEventStorage::new();
        let mut pds = PrivateDataServiceImpl {
            filter_storage: filters,
            event_storage: events,
            epoch_capacity: PureDPBudget { epsilon: 0.5 },
            _phantom: std::marker::PhantomData,
        };

        // 2) Register an event
        let demo_event = SimpleEvent {
            id: 2,
            epoch_number: 20,
            event_key: 999,
        };
        println!("Registering event {:?}", demo_event);
        pds.register_event(demo_event).unwrap();

        // 3) Compute report
        let request = SimpleLastTouchHistogramRequest {
            epoch_start: 0,
            epoch_end: 100,
            attributable_value: 10.0,
            noise_scale: 0.0,
            is_relevant_event: always_relevant_event,
        };
        println!("Computing report for request {:?}", request);
        let report = pds.compute_report(request);

        // 4) Check result
        if let Some((epoch_number, event_key, value)) = report.attributed_value {
            println!("Got an attributed report: epoch={}, key={}, value={}", epoch_number, event_key, value);
            // In some code, you might log the current budget or do partial consumption.
            // If so, replicate the debug statements that show remaining budget, etc.

            // Example assertion:
            assert_eq!(epoch_number, 20);
            assert_eq!(event_key, 999);
            // Because your code might consume budget or add noise, your assertion can vary.
            // For a no-noise test with a relevant event, we might expect exactly 10.0
            assert!((value - 10.0).abs() < f64::EPSILON);
        } else {
            panic!("Expected an attributed value with an event, got None");
        }
    }
}

fn always_relevant_event(event: &SimpleEvent) -> bool {
    AlwaysRelevantSelectorInLib {}.is_relevant_event(event)
}
