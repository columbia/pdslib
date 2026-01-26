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
