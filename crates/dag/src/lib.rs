//! Layered DAG execution engine.
//!
//! Layer L3. Four fixed layers (preprocess → account_select → model_access →
//! post_process); nodes implement [`DagNode`] and run in declaration order.

pub mod context;
pub mod executor;
pub mod nodes;
pub mod token_estimate;

pub use context::DagContext;
pub use executor::{DagNode, Layer, run};
pub use nodes::{StreamDelivery, default_layers, settle_deferred_stream};
