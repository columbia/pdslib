import pdslib

def test_version():
    assert pdslib.__version__ == "0.3.0"

def test_module_exists():
    assert pdslib is not None
