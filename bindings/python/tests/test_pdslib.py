import pdslib_python


def test_version():
    assert pdslib_python.__version__ == "0.3.0"


def test_module_exists():
    assert pdslib_python is not None


def test_uri_set_create():
    uri_set = pdslib_python.UriSet(["shoes.com", "blog.com"])
    assert uri_set is not None


def test_event_uris_create():
    trigger_uris = pdslib_python.UriSet(["shoes.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    event_uris = pdslib_python.EventUris("blog.com", trigger_uris, querier_uris)
    assert event_uris is not None


def test_report_request_uris_create():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)
    assert report_uris is not None


def test_ppa_event_create():
    trigger_uris = pdslib_python.UriSet(["shoes.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    event_uris = pdslib_python.EventUris("blog.com", trigger_uris, querier_uris)
    event = pdslib_python.PpaEvent(
        id=1,
        timestamp=0,
        epoch_number=1,
        histogram_index=1369,
        uris=event_uris,
        filter_data=1,
    )
    assert event is not None


def test_ppa_histogram_config_create():
    config = pdslib_python.PpaHistogramConfig(
        start_epoch=1,
        end_epoch=2,
        attributable_value=32768.0,
        max_attributable_value=65536.0,
        requested_epsilon=1.0,
        histogram_size=2048,
    )
    assert config is not None


def test_direct_ppa_histogram_config_create():
    config = pdslib_python.DirectPpaHistogramConfig(
        start_epoch=1,
        end_epoch=2,
        attributable_value=32768.0,
        laplace_noise_scale=131072.0,
        histogram_size=2048,
    )
    assert config is not None


def test_requested_buckets_all():
    buckets = pdslib_python.RequestedBuckets.all_buckets()
    assert buckets is not None


def test_requested_buckets_specific():
    buckets = pdslib_python.RequestedBuckets.specific_buckets([0x559, 0x560])
    assert buckets is not None


def test_ppa_relevant_event_selector_create():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)
    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=1,
        requested_buckets=pdslib_python.RequestedBuckets.specific_buckets([0x559]),
    )
    assert selector is not None


def test_ppa_relevant_event_selector_match_all():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)
    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=None,  # match all events
        requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
    )
    assert selector is not None


def test_ppa_histogram_request_new_direct():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)
    config = pdslib_python.DirectPpaHistogramConfig(
        start_epoch=1,
        end_epoch=2,
        attributable_value=32768.0,
        laplace_noise_scale=131072.0,
        histogram_size=2048,
    )
    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=1,
        requested_buckets=pdslib_python.RequestedBuckets.specific_buckets([0x559]),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(config, selector)
    assert request is not None


def test_ppa_histogram_request_new():
    source_uris = pdslib_python.UriSet(["blog.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
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
        filter_data=None,
        requested_buckets=pdslib_python.RequestedBuckets.all_buckets(),
    )
    request = pdslib_python.PpaHistogramRequest.new(config, selector)
    assert request is not None


def test_pds_create():
    pds = pdslib_python.Pds(
        per_querier_budget=1.0,
        global_budget=20.0,
        trigger_quota=1.5,
        source_quota=4.0,
    )
    assert pds is not None


def test_pds_register_event():
    pds = pdslib_python.Pds(1.0, 20.0, 1.5, 4.0)
    trigger_uris = pdslib_python.UriSet(["shoes.com"])
    querier_uris = pdslib_python.UriSet(["adtech.com"])
    event_uris = pdslib_python.EventUris("blog.com", trigger_uris, querier_uris)
    event = pdslib_python.PpaEvent(
        id=1,
        timestamp=0,
        epoch_number=1,
        histogram_index=0x559,
        uris=event_uris,
        filter_data=1,
    )
    pds.register_event(event)


def test_pds_compute_report():
    pds = pdslib_python.Pds(1.0, 20.0, 1.5, 4.0)

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
        filter_data=1,
    )
    pds.register_event(event)

    # Create a report request
    source_uris = pdslib_python.UriSet(["blog.com"])
    report_uris = pdslib_python.ReportRequestUris("shoes.com", source_uris, querier_uris)
    config = pdslib_python.DirectPpaHistogramConfig(
        start_epoch=1,
        end_epoch=2,
        attributable_value=32768.0,
        laplace_noise_scale=131072.0,
        histogram_size=2048,
    )
    selector = pdslib_python.PpaRelevantEventSelector(
        report_request_uris=report_uris,
        filter_data=1,
        requested_buckets=pdslib_python.RequestedBuckets.specific_buckets([0x559]),
    )
    request = pdslib_python.PpaHistogramRequest.new_direct(config, selector)

    # Compute the report
    report = pds.compute_report(request)
    assert report is not None
    assert hasattr(report, "filtered_bin_values")
    assert hasattr(report, "unfiltered_bin_values")
    assert hasattr(report, "oob_filters")
    assert isinstance(report.filtered_bin_values, list)
    assert isinstance(report.unfiltered_bin_values, list)
    assert isinstance(report.oob_filters, list)
