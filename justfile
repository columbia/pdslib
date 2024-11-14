set shell := ["zsh", "-uc"]

build:
    cd pdslib; cargo build

test:
    cd pdslib; cargo test

demo:
    cd pdslib; cargo test --package pdslib --test demo -- --nocapture 

install:
    source .venv/bin/activate; uv sync; cd pdslib; env -u CONDA_PREFIX maturin develop;

format:
    cd pdslib; cargo +nightly fmt