set shell := ["zsh", "-uc"]

build:
    cargo build

test:
    cargo test

demo:
    cargo test --package pdslib --test demo -- --nocapture 

format:
    cargo +nightly fmt