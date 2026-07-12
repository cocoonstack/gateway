//! Engine factory: one engine per [`Protocol`], matched exhaustively at
//! compile time. Realtime is not a chat-pipeline engine — it is served on the
//! /v1/realtime WebSocket surface, so the factory answers 501-with-pointer for
//! it on the chat path.

use ap_consts::{ErrCode, Protocol};
use ap_models::{GResult, GatewayError, GatewayRequest};

use crate::bespoke::{CohereEngine, DashScopeEngine, ErnieEngine, LlamaEngine, MinimaxV1Engine};
use crate::claude_engine::ClaudeEngine;
use crate::engine::ModelEngine;
use crate::families::{
    AudioEngine, AudioKind, CompletionsEngine, EmbeddingsEngine, ImageEngine, PassthroughEngine,
    ResponsesEngine, SearchEngine, VertexEngine, VideoEngine,
};
use crate::openai_engine::OpenAiEngine;
use crate::transport::SharedTransport;

/// Whether the gateway serves `p`. Every protocol is served: chat-pipeline
/// protocols through `get_engine`, Realtime on the /v1/realtime WebSocket
/// surface.
pub fn is_implemented(_p: Protocol) -> bool {
    true
}

/// Build the engine for a request.
pub fn get_engine(
    request: GatewayRequest,
    transport: SharedTransport,
) -> GResult<Box<dyn ModelEngine>> {
    let p = request
        .protocol()
        .ok_or_else(|| GatewayError::bad_request("request missing model_param_v2"))?;

    Ok(match p {
        Protocol::OpenaiChat => Box::new(OpenAiEngine::new(request, transport)),
        Protocol::Completions => Box::new(CompletionsEngine::new(request, transport)),
        Protocol::Responses => Box::new(ResponsesEngine::new(request, transport)),
        Protocol::AnthropicMessages => Box::new(ClaudeEngine::new(request, transport)),
        Protocol::Gemini => Box::new(VertexEngine::new(request, transport)),
        Protocol::Embeddings => Box::new(EmbeddingsEngine::new(request, transport)),
        Protocol::Image => Box::new(ImageEngine::new(request, transport)),
        Protocol::Tts => Box::new(AudioEngine::new(request, transport, AudioKind::Tts)),
        Protocol::Stt => Box::new(AudioEngine::new(request, transport, AudioKind::Stt)),
        Protocol::Audio => Box::new(AudioEngine::new(request, transport, AudioKind::Other)),
        Protocol::Video => Box::new(VideoEngine::new(request, transport)),
        Protocol::Search => Box::new(SearchEngine::new(request, transport)),
        Protocol::Passthrough => Box::new(PassthroughEngine::new(request, transport)),
        Protocol::Ernie => Box::new(ErnieEngine::new(request, transport)),
        Protocol::MinimaxV1 => Box::new(MinimaxV1Engine::new(request, transport)),
        Protocol::AwsCohere => Box::new(CohereEngine::new(request, transport)),
        Protocol::AwsLlama => Box::new(LlamaEngine::new(request, transport)),
        Protocol::Dashscope => Box::new(DashScopeEngine::new(request, transport)),
        Protocol::Realtime => {
            return Err(GatewayError::new(
                ErrCode::INTERNAL_UNKNOWN,
                501,
                format!(
                    "realtime model `{}` is served on the /v1/realtime websocket surface, not the chat surface",
                    p.as_str()
                ),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ap_models::ModelParamV2;

    use super::*;
    use crate::transport::MockTransport;

    fn req(p: Protocol) -> GatewayRequest {
        GatewayRequest {
            model_param_v2: Some(ModelParamV2::new(p)),
            ..Default::default()
        }
    }

    #[test]
    fn every_non_realtime_protocol_dispatches() {
        let t: SharedTransport = Arc::new(MockTransport);
        let mut dispatched = 0;
        for &p in Protocol::ALL {
            let got = get_engine(req(p), t.clone());
            if p == Protocol::Realtime {
                assert_eq!(got.err().map(|e| e.http_status), Some(501), "{p}");
            } else {
                assert!(got.is_ok(), "no engine for {p}");
                dispatched += 1;
            }
        }
        assert_eq!(dispatched, Protocol::ALL.len() - 1);
    }

    #[test]
    fn rejects_missing_param() {
        let t: SharedTransport = Arc::new(MockTransport);
        let err = get_engine(GatewayRequest::default(), t).err().unwrap();
        assert_eq!(err.http_status, 400);
    }
}
