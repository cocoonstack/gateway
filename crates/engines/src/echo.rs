//! A reference `ModelEngine` used to prove the trait/factory/server wiring.
//!
//! It performs no upstream call — it echoes request metadata back as the message.
//! Real vendor engines (OpenAI, Azure, Claude, …) replace it per model type.
//! It is the concrete pattern new engines should follow:
//! hold the request + a recorder, implement `run()` and `recorder()`.

use gw_models::{GResult, GatewayRequest, GatewayResponse};

use crate::engine::{EngineOutcome, ModelEngine};

/// Placeholder engine that echoes the dispatched model type and request flags.
pub struct EchoEngine {
    request: GatewayRequest,
}

impl EchoEngine {
    pub fn new(request: GatewayRequest) -> Self {
        Self { request }
    }
}

#[async_trait::async_trait]
impl ModelEngine for EchoEngine {
    async fn run(&self) -> GResult<EngineOutcome> {
        let model = self
            .request
            .protocol()
            .map(|m| m.as_str())
            .unwrap_or("<none>");
        let message = format!(
            "echo-engine: model={model} online={} ak={}",
            self.request.is_online, self.request.ak
        );
        let prompt_tokens = message.len() as i64;
        let response = GatewayResponse {
            message,
            model: model.to_owned(),
            prompt_tokens,
            completion_tokens: 0,
            total_tokens: prompt_tokens,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::ok(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gw_consts::Protocol;
    use gw_models::ModelParamV2;

    #[tokio::test]
    async fn echoes_protocol() {
        let req = GatewayRequest {
            is_online: true,
            model_param_v2: Some(ModelParamV2::new(Protocol::OpenaiChat)),
            ..Default::default()
        };
        let engine = EchoEngine::new(req);
        let out = engine.run().await.unwrap();
        assert_eq!(out.http_code, 200);
        assert!(out.response.message.contains("model=openai-chat"));
        assert!(out.response.total_tokens > 0);
    }
}
