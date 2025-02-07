# Private Data Service Library

`pdslib` is a standalone Rust library (crate) containing core individual DP interfaces. It is intended to be used as a building block for DP systems like Cookie Monster that use on-device budgeting.

Repository structure:
- `src` contains the following main components: `budget`, `events`, `mechanisms` (no dependencies), `queries` (depends on `budget`, `events`, `mechanisms`) and `pds` (depends on the rest).
- `src/*/traits.rs` define interfaces. Other files in `src/*` implement these interfaces, with very simple in-memory datastructures for now. Other crates using `pdslib` in particular environments (e.g., Chromium or Android) can have implementations for the same traits using browser storage or SQLite databases.
- `src/pds` is structured to work with  `budget`, `events`, `queries` only through interfaces. So we should be able to swap the implementation for event storage or replace the type of query, and still obtain a working implementation of the `PrivateDataService` interface.
- `tests` contains integration tests. In particular, `tests/demo.rs` shows how an external application can use `pdslib` to register events and request reports on a device. 
