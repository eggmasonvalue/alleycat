//! `alleycat-agy-bridge` — Codex app-server façade over Google Antigravity CLI (`agy`).

pub mod bridge;
pub mod handlers;
pub mod index;
pub mod pool;
pub mod state;
pub mod translate;

pub use bridge::{AgyBridge, AgyBridgeBuilder};
pub use handlers::model::DiscoveredModel;
pub use index::AgySessionRef;
pub use pool::{AgyPool, PoolPolicy};
