# pdslib Python Bindings

Python bindings for [pdslib](../../README.md) - on-device differential privacy budgeting for privacy-preserving attribution measurement APIs.

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
import pdslib

print(pdslib.__version__)
```

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

### Rebuild after Rust changes

```bash
maturin develop
```
