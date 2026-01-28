import pytest

import pdslib_python


@pytest.fixture
def event_uris():
    trigger_uris = pdslib_python.UriSet(["shoes.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    return pdslib_python.EventUris("blog.com", trigger_uris, querier_uris)


@pytest.fixture
def report_uris():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    return pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)


@pytest.fixture
def direct_config():
    return pdslib_python.DirectPpaHistogramConfig(
        start_epoch=1,
        end_epoch=2,
        attributable_value=32768.0,
        laplace_noise_scale=131072.0,
        histogram_size=2048,
    )


@pytest.fixture
def pds():
    return pdslib_python.Pds(1.0, 20.0, 1.5, 4.0)


def test_compute_report(pds, event_uris, report_uris, direct_config):
    event = pdslib_python.PpaEvent(
        id=1,
        timestamp=0,
        epoch_number=1,
        histogram_index=0x559,
        uris=event_uris,
        filter_data=1,
    )
    pds.register_event(event)

    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=1,
        requested_buckets=pdslib_python.RequestedBuckets.specific_buckets([0x559]),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(direct_config, selector)

    report = pds.compute_report(request)

    assert len(report.bin_values) == 1
    bucket, _ = report.bin_values[0]
    assert bucket == 0x559


def test_filter_data_none_matches_all(pds, event_uris, report_uris, direct_config):
    event = pdslib_python.PpaEvent(
        id=1, timestamp=0, epoch_number=1, histogram_index=100,
        uris=event_uris, filter_data=1,
    )
    pds.register_event(event)

    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=None,
        requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(direct_config, selector)
    report = pds.compute_report(request)

    assert len(report.bin_values) == 1


def test_filter_data_matches_same_value(pds, event_uris, report_uris, direct_config):
    event = pdslib_python.PpaEvent(
        id=1, timestamp=0, epoch_number=1, histogram_index=100,
        uris=event_uris, filter_data=1,
    )
    pds.register_event(event)

    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=1,
        requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(direct_config, selector)
    report = pds.compute_report(request)

    assert len(report.bin_values) == 1


def test_filter_data_excludes_different_value(pds, event_uris, report_uris, direct_config):
    event = pdslib_python.PpaEvent(
        id=1, timestamp=0, epoch_number=1, histogram_index=100,
        uris=event_uris, filter_data=1,
    )
    pds.register_event(event)

    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=999,
        requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(direct_config, selector)
    report = pds.compute_report(request)

    assert len(report.bin_values) == 0
