import pdslib_python


def test_version():
    assert pdslib_python.__version__ == "0.3.0"


def test_module_exists():
    assert pdslib_python is not None


def test_uri_set_create():
    uri_set = pdslib_python.UriSet(["shoes.com", "blog.com"])
    assert uri_set is not None
