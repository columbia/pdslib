import pdslib_python


def test_version():
    assert pdslib_python.__version__ == "0.3.0"


def test_module_exists():
    assert pdslib_python is not None

