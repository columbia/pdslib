pub mod budget;
pub mod events;
pub mod pds;
pub mod queries;

fn add(a: u32, b: u32) -> u32 {
    a + b
}

uniffi::include_scaffolding!("example");
