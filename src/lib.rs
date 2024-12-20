pub mod budget;
pub mod events;
pub mod mechanisms;
pub mod pds;
pub mod queries;

// TODO: add more bindings here, if that's helpful for the Java or C++ interop
fn add(a: u32, b: u32) -> u32 {
    a + b
}

uniffi::include_scaffolding!("example");
