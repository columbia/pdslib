# pdslib Python Bindings

Python bindings for [pdslib](../../README.md) - on-device differential privacy budgeting for privacy-preserving
attribution measurement APIs.

## Setup

```bash
cd bindings/python
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
maturin develop
```

## Usage

```python
import pdslib_python

# Create a PDS instance with budget parameters
pds = pdslib_python.Pds(
    per_querier_budget=1.0,
    global_budget=20.0,
    trigger_quota=1.5,
    source_quota=4.0,
)

# Register an event
trigger_uris = pdslib_python.UriSet(["shoes.com"])
querier_uris = pdslib_python.UriSet(["adtech.com"])
event_uris = pdslib_python.EventUris("blog.com", trigger_uris, querier_uris)

event = pdslib_python.PpaEvent(
    id=1,
    timestamp=0,
    epoch_number=1,
    histogram_index=0x559,
    uris=event_uris,
    filter_data=1,  # event type identifier (e.g., 1=purchase, 2=add_to_cart)
)
pds.register_event(event)

# Create a report request
source_uris = pdslib_python.UriSet(["blog.com"])
report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)

config = pdslib_python.PpaHistogramConfig(
    start_epoch=1,
    end_epoch=2,
    attributable_value=32768.0,
    max_attributable_value=65536.0,
    requested_epsilon=1.0,
    histogram_size=2048,
)

selector = pdslib_python.PpaRelevantEventSelector(
    report_request_uris=report_uris,
    filter_data=None,  # None matches all events (or use int to filter by type)
    requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
)

request = pdslib_python.PpaHistogramRequest.new(config, selector)

# Compute the report
report = pds.compute_report(request)

# Access results
for bucket, value in report.bin_values:
    print(f"Bucket {bucket}: {value}")
```

## Notes

- `PpaRelevantEventSelector.filter_data` is simplified from the Rust API. The Python bindings take
  `int | None`, where `int` matches events with that exact `filter_data` value, and `None` matches all events.
- Experimental feature from the Rust library (`oob_filters`) is not currently exposed in
  these bindings.

## Development

All commands below assume you're in `bindings/python/` with the venv activated:

```bash
cd bindings/python
source .venv/bin/activate
```

### Run tests

```bash
pytest tests/
```

Or from the repo root:

```bash
just python-test
```

### Rebuild after Rust changes

```bash
maturin develop
```
