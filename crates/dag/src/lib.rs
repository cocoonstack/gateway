//! Layered DAG execution engine.
//!
//! Layer L3. Four fixed layers (preprocess → account_select → model_access →
//! post_process); nodes implement [`DagNode`] and declare same-layer
//! dependencies. Topologies are code-declared for now — parsing external
//! topology definitions is a Phase-4 follow-up.

pub mod context;
pub mod executor;
pub mod nodes;

pub use context::DagContext;
pub use executor::{DagNode, Layer, Plan, run};
pub use nodes::{StreamDelivery, default_layers, settle_deferred_stream};
