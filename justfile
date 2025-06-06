set shell := ["zsh", "-uc"]

build:
    cargo build

test:
    cargo test --features experimental

demo:
    cargo test --package pdslib --test simple_events_demo -- --nocapture 
    cargo test --package pdslib --test ppa_demo -- --nocapture 

format:
    cargo +nightly fmt
    cargo clippy --fix --allow-dirty
    cargo clippy --tests  -- -D warnings