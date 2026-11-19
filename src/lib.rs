//! The Channel 0 News — realtime backend library.
//!
//! Split into a library crate so the protocol, state machine, room actor and
//! persistence layers can be unit-tested independently of the Axum binary.

pub mod app;
pub mod client_address;
pub mod connection;
pub mod db;
mod direct;
pub mod error;
pub mod protocol;
pub mod registry;
pub mod room;
pub mod room_game;
pub mod routes;
pub mod state;
pub mod ws;
