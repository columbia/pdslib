set shell := ["zsh", "-uc"]

build:
    cargo build

test:
    cargo test

demo:
    cargo test --package pdslib --test simple_events_demo -- --nocapture 
    cargo test --package pdslib --test ara_demo -- --nocapture 

format:
    cargo +nightly fmt
    cargo clippy --tests  -- -D warnings