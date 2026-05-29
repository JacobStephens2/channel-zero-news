//! The Channel 0 News — realtime backend library.
//!
//! Split into a library crate so the protocol, state machine, room actor and
//! persistence layers can be unit-tested independently of the Axum binary.

pub mod protocol;
pub mod state;
