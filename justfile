set shell := ["zsh", "-uc"]

demo:
    cd pdslib; cargo test --package pdslib --test demo -- --nocapture 

install:
    source .venv/bin/activate; uv sync; cd pdslib; env -u CONDA_PREFIX maturin develop;