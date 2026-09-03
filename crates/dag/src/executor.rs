//! DAG executor.
//!
//! Four fixed layers run in order; within a layer, nodes run sequentially in
//! declaration order. Layers are declared in code.

use gw_models::GResult;

use crate::context::DagContext;

#[async_trait::async_trait]
pub trait DagNode: Send + Sync {
    fn name(&self) -> &'static str;
    async fn execute(&self, ctx: &mut DagContext) -> GResult<()>;
}

pub struct Layer {
    pub name: &'static str,
    pub nodes: Vec<Box<dyn DagNode>>,
}

/// Run every layer's nodes in declaration order; a node error aborts the run
/// and a cache hit short-circuits every node after the one that set it.
pub async fn run(layers: &[Layer], ctx: &mut DagContext) -> GResult<()> {
    for layer in layers {
        for node in &layer.nodes {
            if ctx.cache_hit {
                return Ok(());
            }
            tracing::debug!(layer = layer.name, node = node.name(), "dag node start");
            let started = std::time::Instant::now();
            let result = node.execute(ctx).await;
            metrics::histogram!("gateway_node_duration_seconds", "node" => node.name())
                .record(started.elapsed().as_secs_f64());
            result?;
        }
    }
    Ok(())
}
