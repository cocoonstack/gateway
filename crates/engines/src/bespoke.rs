//! Bespoke vendor wire shapes: these vendors do NOT speak the OpenAI protocol;
//! each engine builds the vendor's real request shape and parses its real
//! response shape (the mock answers in the same shapes). AWS engines compute a
//! real SigV4 Authorization header.

use gw_models::{GResult, GatewayError, GatewayResponse, TypedParams};
use gw_protocol::object;
use serde_json::{Map, Value, json};

use crate::base::{Base, base_engine};
use crate::bedrock::{
    bedrock_header_usage, bedrock_input_tokens, bedrock_invoke, bedrock_stream, invocation_metrics,
};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk, reject_minimax_error};
use crate::transport::Headers;

base_engine!(ErnieEngine);

#[async_trait::async_trait]
impl ModelEngine for ErnieEngine {
    /// Baidu Ernie: `/wenxinworkshop/chat/{model}`, a `bce-v3/…` key as Bearer
    /// or a legacy token as `?access_token=`; reply `{result, usage}`.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // ernie's system is a top-level field (system turns are filtered above)
        let system = self.base.system_text();
        let messages = simple_turns(&mut self.base, ("assistant", "user"), ("role", "content"));
        let mut body = json!({});
        body["messages"] = Value::Array(messages);
        if !system.is_empty() {
            body["system"] = system.into();
        }
        if let Some(p) = self.base.chat_params()
            && let Some(t) = p.temperature
        {
            body["temperature"] = json!(t);
        }
        let key = self.base.api_key();
        let mut url = format!(
            "{}/rpc/2.0/ai_custom/v1/wenxinworkshop/chat/{model}",
            self.base.base_url("mock://aip.baidubce.com"),
        );
        let mut headers = Vec::new();
        if key.starts_with("bce-v3/") {
            headers.push(("authorization", format!("Bearer {key}")));
        } else {
            url.push_str("?access_token=");
            url.push_str(&key);
        }
        let (status, mut v) = self.base.post_json(&url, headers, body).await?;
        let message = crate::engine::take_string(&mut v, "/result").unwrap_or_default();
        let usage = &v["usage"];
        let prompt_tokens = crate::engine::tok(&usage["prompt_tokens"]);
        let completion_tokens = crate::engine::tok(&usage["completion_tokens"]);
        let total_tokens = crate::engine::tok(&usage["total_tokens"]);
        let resp = GatewayResponse {
            message,
            model,
            finish_reason: if v["is_truncated"].as_bool().unwrap_or(false) {
                "length".into()
            } else {
                "stop".into()
            },
            prompt_tokens,
            completion_tokens,
            total_tokens,
            raw_usage: v.get_mut("usage").map(Value::take).filter(|u| !u.is_null()),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(MinimaxV1Engine);

#[async_trait::async_trait]
impl ModelEngine for MinimaxV1Engine {
    /// MiniMax v1: messages use sender_type USER/BOT + text;
    /// response {reply, usage{total_tokens}, base_resp{status_code,status_msg}}.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // v1 carries the system instruction as top-level `prompt` + role_meta
        let system = self.base.system_text();
        let messages = simple_turns(&mut self.base, ("BOT", "USER"), ("sender_type", "text"));
        let mut body = json!({"model": model});
        body["messages"] = Value::Array(messages);
        if !system.is_empty() {
            body["prompt"] = system.into();
            body["role_meta"] = json!({"user_name": "USER", "bot_name": "BOT"});
        }
        let url = format!(
            "{}/v1/text/chatcompletion",
            self.base.base_url("mock://api.minimax.chat")
        );
        let (status, mut v) = self
            .base
            .post_json(&url, self.base.bearer_headers(), body)
            .await?;
        reject_minimax_error(&v)?;
        let total = crate::engine::tok(&v["usage"]["total_tokens"]);
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/reply").unwrap_or_default(),
            model,
            finish_reason: "stop".into(),
            total_tokens: total,
            raw_usage: v.get_mut("usage").map(Value::take).filter(|u| !u.is_null()),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(AwsEmbedEngine);

#[async_trait::async_trait]
impl ModelEngine for AwsEmbedEngine {
    /// Bedrock embeddings over InvokeModel, answered in the OpenAI list shape:
    /// Titan takes exactly one `{inputText}`, Cohere batches `{texts}`; usage
    /// from the `x-amzn-bedrock-*` header, the body count as fallback.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let (texts, dimensions) = match self.base.take_typed() {
            Some(TypedParams::Embeddings(p)) if !p.input.is_empty() => (p.input, p.dimensions),
            _ => {
                return Err(GatewayError::bad_request(
                    "aws-embed serves embeddings input only",
                ));
            }
        };
        let mut data = Vec::with_capacity(texts.len());
        let (status, prompt_tokens) = if model.starts_with("cohere.") {
            // bedrock's cohere embed requires an input_type; the gateway embeds for retrieval storage
            let mut body = json!({"input_type": "search_document"});
            body["texts"] = Value::Array(texts.into_iter().map(Value::String).collect());
            let (st, mut v, headers) = bedrock_invoke(&mut self.base, &model, body).await?;
            if let Value::Array(rows) = v["embeddings"].take() {
                data.extend(rows.into_iter().enumerate().map(embedding_row));
            }
            (st, bedrock_input_tokens(&headers, 0))
        } else {
            let [text] = <[String; 1]>::try_from(texts).map_err(|_| {
                GatewayError::bad_request("titan embeddings require exactly one input")
            })?;
            let mut body = json!({});
            body["inputText"] = Value::String(text);
            if let Some(d) = dimensions {
                body["dimensions"] = json!(d);
            }
            let (st, mut v, headers) = bedrock_invoke(&mut self.base, &model, body).await?;
            let body_count = crate::engine::tok(&v["inputTextTokenCount"]);
            data.push(embedding_row((0, v["embedding"].take())));
            (st, bedrock_input_tokens(&headers, body_count))
        };
        let mut v2 = json!({"object": "list", "model": model,
            "usage": {"prompt_tokens": prompt_tokens, "total_tokens": prompt_tokens}});
        v2["data"] = Value::Array(data);
        let resp = GatewayResponse {
            model,
            prompt_tokens,
            total_tokens: prompt_tokens,
            raw_usage: Some(v2["usage"].clone()),
            response_v2: Some(v2),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

fn embedding_row((index, embedding): (usize, Value)) -> Value {
    let mut row = json!({"object": "embedding", "index": index});
    row["embedding"] = embedding;
    row
}

/// The non-system turns as `{role_key: ai|user, content_key: text}` objects,
/// moved out of the request (the vendors' flat two-role wires).
fn simple_turns(
    base: &mut Base,
    (ai, user): (&str, &str),
    (role_key, content_key): (&str, &str),
) -> Vec<Value> {
    std::mem::take(&mut base.request.message)
        .into_iter()
        .filter(|m| m.role != gw_consts::role::SYSTEM)
        .map(|m| {
            let mut turn = Map::with_capacity(2);
            let role = if m.role == gw_consts::role::AI {
                ai
            } else {
                user
            };
            turn.insert(role_key.to_owned(), role.into());
            turn.insert(content_key.to_owned(), Value::String(m.content));
            Value::Object(turn)
        })
        .collect()
}

/// The Llama 3/4 chat template for Bedrock's raw prompt; a bare `role: text`
/// prompt makes the model invent turns until max_gen_len.
fn llama_prompt(model: &str, messages: &[gw_models::ChatMsg]) -> String {
    let (start, end, eot) = if model.contains("llama4") {
        ("<|header_start|>", "<|header_end|>", "<|eot|>")
    } else {
        ("<|start_header_id|>", "<|end_header_id|>", "<|eot_id|>")
    };
    let mut prompt = String::from("<|begin_of_text|>");
    for m in messages {
        let role = match m.role.as_str() {
            gw_consts::role::SYSTEM => "system",
            gw_consts::role::AI => "assistant",
            _ => "user",
        };
        prompt.push_str(start);
        prompt.push_str(role);
        prompt.push_str(end);
        prompt.push_str("\n\n");
        prompt.push_str(&m.content);
        prompt.push_str(eot);
    }
    prompt.push_str(start);
    prompt.push_str("assistant");
    prompt.push_str(end);
    prompt.push_str("\n\n");
    prompt
}

base_engine!(LlamaEngine);

#[async_trait::async_trait]
impl ModelEngine for LlamaEngine {
    /// Bedrock Llama: `{prompt, max_gen_len, temperature}` → `{generation,
    /// prompt_token_count, generation_token_count, stop_reason}`, streamed per delta.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let prompt = llama_prompt(&model, &self.base.request.message);
        let mut body = json!({});
        body["prompt"] = prompt.into();
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                body["max_gen_len"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        if self.base.request.stream {
            let mut resp = GatewayResponse {
                model,
                ..Default::default()
            };
            let mut full = String::new();
            let (status, r) = bedrock_stream(&mut self.base, body, |mut v| {
                let mut chunks = Vec::new();
                if let Some(Value::String(t)) = v.get_mut("generation").map(Value::take)
                    && !t.is_empty()
                {
                    full.push_str(&t);
                    chunks.push(StreamChunk {
                        delta: t,
                        ..Default::default()
                    });
                }
                if let Some(n) = v["prompt_token_count"].as_i64() {
                    resp.prompt_tokens = n;
                }
                if let Some(n) = v["generation_token_count"].as_i64() {
                    resp.completion_tokens = n;
                }
                if let Some(sr) = v["stop_reason"].as_str() {
                    resp.finish_reason = sr.to_owned();
                    chunks.push(StreamChunk {
                        finish_reason: Some(sr.to_owned()),
                        ..Default::default()
                    });
                }
                invocation_metrics(&v, &mut resp);
                Ok(chunks)
            })
            .await?;
            resp.message = full;
            let total = resp.prompt_tokens.saturating_add(resp.completion_tokens);
            resp.total_tokens = total;
            resp.raw_usage = Some(json!({
                "prompt_tokens": resp.prompt_tokens, "completion_tokens": resp.completion_tokens,
                "total_tokens": total
            }));
            return Ok(EngineOutcome::from_pump(resp, status, r));
        }
        let (status, mut v, headers) = bedrock_invoke(&mut self.base, &model, body).await?;
        let (pt, ct) = bedrock_header_usage(
            &headers,
            (
                crate::engine::tok(&v["prompt_token_count"]),
                crate::engine::tok(&v["generation_token_count"]),
            ),
        );
        let total = pt.saturating_add(ct);
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/generation").unwrap_or_default(),
            model,
            finish_reason: crate::engine::take_string(&mut v, "/stop_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: total,
            raw_usage: Some(
                json!({"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": total}),
            ),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(DashScopeEngine);

impl DashScopeEngine {
    fn build_body(&mut self, stream: bool) -> GResult<Value> {
        let messages: Vec<Value> = std::mem::take(&mut self.base.request.message)
            .into_iter()
            .map(|m| {
                let role = if m.role == gw_consts::role::AI {
                    "assistant"
                } else if m.role == gw_consts::role::SYSTEM {
                    "system"
                } else {
                    "user"
                };
                object([("role", role.into()), ("content", m.content.into())])
            })
            .collect();
        let mut parameters = json!({"result_format": "message"});
        if stream {
            // deltas instead of the full-text-so-far in every frame
            parameters["incremental_output"] = json!(true);
        }
        if let Some(p) = self.base.chat_params() {
            if let Some(t) = p.temperature {
                parameters["temperature"] = json!(t);
            }
            if let Some(t) = p.top_p {
                parameters["top_p"] = json!(t);
            }
            if let Some(mt) = p.max_tokens {
                parameters["max_tokens"] = json!(mt);
            }
        }
        let input = object([("messages", Value::Array(messages))]);
        Ok(object([
            ("model", self.base.model_name()?.into()),
            ("input", input),
            ("parameters", parameters),
        ]))
    }

    fn url(&self) -> String {
        format!(
            "{}/api/v1/services/aigc/text-generation/generation",
            self.base.base_url("mock://dashscope.aliyuncs.com")
        )
    }

    fn headers(&self, stream: bool) -> Headers {
        let mut h = self.base.bearer_headers();
        if stream {
            // DashScope streams only when this header is present
            h.push(("X-DashScope-SSE", "enable".into()));
        }
        h
    }

    /// Native DashScope streaming: SSE frames decoded as they arrive and
    /// forwarded through `stream_tx` (the live-pump contract).
    async fn run_stream(&mut self) -> GResult<EngineOutcome> {
        let body = self.build_body(true)?;
        let reply = self
            .base
            .post_raw(&self.url(), self.headers(true), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.base.model_name()?.to_owned(),
            ..Default::default()
        };
        crate::pump::reject_json_error("dashscope", status, &reply.body)?;
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "dashscope",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| dashscope_apply_frame(&v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = dashscope_common_usage(&resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for DashScopeEngine {
    /// DashScope native wire: `{model, input:{messages}, parameters}` →
    /// `{output:{choices}, usage}`; streams via `X-DashScope-SSE` + `incremental_output`.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body(false)?;
        let (status, mut v) = self
            .base
            .post_json(&self.url(), self.headers(false), body)
            .await?;
        let mut resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/output/choices/0/message/content")
                .unwrap_or_default(),
            model: self.base.model_name()?.to_owned(),
            finish_reason: crate::engine::take_string(&mut v, "/output/choices/0/finish_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            ..Default::default()
        };
        dashscope_apply_usage(&v["usage"], &mut resp);
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = dashscope_common_usage(&resp);
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// Apply one DashScope SSE frame: running frames carry the literal "null"
/// finish_reason and cumulative usage (last frame wins).
fn dashscope_apply_frame(
    v: &Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    let choice = &v["output"]["choices"][0];
    if let Some(t) = choice["message"]["content"].as_str()
        && !t.is_empty()
    {
        full.push_str(t);
        chunks.push(StreamChunk {
            delta: t.to_owned(),
            ..Default::default()
        });
    }
    if let Some(fr) = choice["finish_reason"].as_str()
        && !fr.is_empty()
        && fr != "null"
    {
        resp.finish_reason = fr.to_owned();
        chunks.push(StreamChunk {
            finish_reason: Some(fr.to_owned()),
            ..Default::default()
        });
    }
    dashscope_apply_usage(&v["usage"], resp);
    Ok(chunks)
}

fn dashscope_apply_usage(usage: &Value, resp: &mut GatewayResponse) {
    if usage.is_null() {
        return;
    }
    if let Some(it) = usage["input_tokens"].as_i64() {
        resp.prompt_tokens = it.max(0);
    }
    if let Some(ot) = usage["output_tokens"].as_i64() {
        resp.completion_tokens = ot.max(0);
    }
    if let Some(tt) = usage["total_tokens"].as_i64() {
        resp.total_tokens = tt.max(0);
    }
    if let Some(cached) = usage["prompt_tokens_details"]["cached_tokens"].as_i64() {
        resp.read_cached_prompt_tokens = cached.max(0);
    }
}

fn dashscope_common_usage(resp: &GatewayResponse) -> Option<gw_models::CommonUsage> {
    Some(gw_models::CommonUsage::from_openai_parts(
        resp.prompt_tokens,
        resp.completion_tokens,
        resp.read_cached_prompt_tokens,
        0,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{ChatMsg, EmbeddingParams, GatewayRequest, ModelParamV2};

    use super::*;
    use crate::transport::{
        HeaderMap, MockTransport, SharedTransport, Transport, UpstreamBody, UpstreamRequest,
        UpstreamResponse,
    };

    #[derive(Debug)]
    struct BedrockReply(&'static str, i64, i64);

    #[async_trait::async_trait]
    impl Transport for BedrockReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-amzn-bedrock-input-token-count",
                self.1.to_string().parse().unwrap(),
            );
            headers.insert(
                "x-amzn-bedrock-output-token-count",
                self.2.to_string().parse().unwrap(),
            );
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Json(self.0.as_bytes().to_vec().into()),
                headers,
            })
        }
    }

    fn req(mt: Protocol, name: &str) -> GatewayRequest {
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hello bespoke")],
            model_param_v2: Some(ModelParamV2::with_name(mt, name)),
            ..Default::default()
        }
    }

    fn t() -> SharedTransport {
        Arc::new(MockTransport)
    }

    #[tokio::test]
    async fn ernie_wire_shape() {
        let mut e = ErnieEngine::new(req(Protocol::Ernie, "ernie-4.0"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-ernie] you said: hello bespoke")
        );
        assert!(out.response.total_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }

    #[tokio::test]
    async fn minimax_v1_wire_shape() {
        let mut e = MinimaxV1Engine::new(req(Protocol::MinimaxV1, "abab6.5"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-minimax] you said: hello bespoke")
        );
        assert!(out.response.total_tokens > 0);
    }

    fn embed_req(model: &str, inputs: &[&str], dimensions: Option<i64>) -> GatewayRequest {
        let mut r = req(Protocol::AwsEmbed, model);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::Embeddings(EmbeddingParams {
            input: inputs.iter().map(|s| (*s).to_owned()).collect(),
            dimensions,
        }));
        r
    }

    #[tokio::test]
    async fn titan_embeddings_invoke_once_and_bill_the_body_count() {
        let r = embed_req("amazon.titan-embed-text-v2:0", &["hello world"], Some(256));
        let out = AwsEmbedEngine::new(r, t()).run().await.unwrap();
        let v = out.response.response_v2.unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["data"][0]["index"], 0);
        assert!(v["data"][0]["embedding"].is_array());
        assert_eq!(out.response.prompt_tokens, 2);
        assert_eq!(out.response.completion_tokens, 0);
        assert_eq!(v["usage"]["total_tokens"], 2);
    }

    #[tokio::test]
    async fn cohere_embeddings_batch_once_and_bill_the_bedrock_headers() {
        let r = embed_req("cohere.embed-english-v3", &["a", "b", "c"], None);
        let out = AwsEmbedEngine::new(r, t()).run().await.unwrap();
        let v = out.response.response_v2.unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(out.response.prompt_tokens, 12);
        assert_eq!(v["usage"]["prompt_tokens"], 12);
    }

    #[tokio::test]
    async fn titan_rejects_a_batch_the_vendor_cannot_serve() {
        let r = embed_req("amazon.titan-embed-text-v2:0", &["one", "two"], None);
        assert!(AwsEmbedEngine::new(r, t()).run().await.is_err());
    }

    #[tokio::test]
    async fn aws_embed_rejects_a_chat_request() {
        let out = AwsEmbedEngine::new(req(Protocol::AwsEmbed, "amazon.titan-embed-text-v2:0"), t())
            .run()
            .await;
        assert!(out.is_err());
    }

    #[tokio::test]
    async fn bedrock_headers_bill_the_invoke() {
        let mut e = AwsEmbedEngine::new(
            embed_req("cohere.embed-english-v3", &["only text"], None),
            Arc::new(BedrockReply(r#"{"embeddings":[[0.5,0.25]]}"#, 57, 0)),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.prompt_tokens, 57);
        assert_eq!(out.response.total_tokens, 57);

        let mut e = LlamaEngine::new(
            req(Protocol::AwsLlama, "meta.llama3-1-8b-instruct-v1:0"),
            Arc::new(BedrockReply(
                r#"{"generation":"yo","prompt_token_count":1,"generation_token_count":1,"stop_reason":"stop"}"#,
                30,
                20,
            )),
        );
        let out = e.run().await.unwrap();
        assert_eq!(
            (out.response.prompt_tokens, out.response.completion_tokens),
            (30, 20)
        );
    }

    #[tokio::test]
    async fn bedrock_streams_reassemble_text_and_take_the_invocation_metrics() {
        let mut r = req(Protocol::AwsLlama, "meta.llama3-1-8b-instruct-v1:0");
        r.stream = true;
        let out = LlamaEngine::new(r, t()).run().await.unwrap();
        assert!(
            out.response.message.contains("[mock-llama]"),
            "{:?}",
            out.response
        );
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
        assert!(out.response.total_tokens > 0);
    }

    #[tokio::test]
    async fn llama_wire_shape() {
        let mut e = LlamaEngine::new(
            req(Protocol::AwsLlama, "meta.llama3-70b-instruct-v1:0"),
            t(),
        );
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("[mock-llama]"));
        assert!(out.response.total_tokens > 0);
    }
    #[tokio::test]
    async fn dashscope_stream_decodes_frames() {
        let mut r = req(Protocol::Dashscope, "qwen-max");
        r.stream = true;
        let mut e = DashScopeEngine::new(r, t());
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3, "chunks: {:?}", out.chunks);
        assert!(
            out.response
                .message
                .contains("[mock-dashscope] you said: hello bespoke")
        );
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
    }

    #[tokio::test]
    async fn dashscope_wire_shape() {
        let mut e = DashScopeEngine::new(req(Protocol::Dashscope, "qwen-max"), t());
        let out = e.run().await.unwrap();
        assert!(
            out.response
                .message
                .contains("[mock-dashscope] you said: hello bespoke")
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }
}
