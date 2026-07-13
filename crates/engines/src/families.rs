//! The non-chat protocol engines.
//! One engine per Protocol variant (AudioEngine covers tts/stt/audio via AudioKind):
//!   Vertex generateContent / Embeddings / Image / Audio(TTS·STT·other) /
//!   Video(async task) / Search / Passthrough(register+misc).
//! Each engine only does "build request → Transport → parse response" — nothing else
//! crosses that boundary.
//! The mock protocol flags byte-level vendor differences as deferred to a later
//! fidelity pass.

use gw_models::{GResult, GatewayError, GatewayRequest, GatewayResponse, Recorder, TypedParams};
use serde_json::{Value, json};

use crate::base::{Base, base_engine};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sse::SseDecoder;
use crate::transport::{SharedTransport, UpstreamBody};

/// Build Gemini `parts` from a unified message. Text → `{"text":…}`; data-URI
/// images → `{"inlineData":{"mimeType","data"}}` (Gemini's inline-image shape).
/// camelCase keys match this engine's existing `generationConfig`/`topP`
/// choice; exact casing is pinned against a real fixture during live
/// integration. Non-data image URLs can't be inlined offline (no fetch), so
/// they're skipped rather than forwarded as an unusable OpenAI block.
///
/// Without this, multimodal requests to Gemini silently drop every image and
/// only the flattened text reaches the vendor — the same class of bug as
/// dropping images in the OpenAI/Claude engines.
fn gemini_parts(m: &gw_models::ChatMsg) -> Vec<Value> {
    if let Some(Value::Array(parts)) = &m.parts {
        let mut out = Vec::new();
        for p in parts {
            match p["type"].as_str() {
                Some("text") => {
                    if let Some(t) = p["text"].as_str() {
                        out.push(json!({"text": t}));
                    }
                }
                Some("image_url") => {
                    let url = p["image_url"]["url"].as_str().unwrap_or_default();
                    if let Some((mime, data)) = parse_data_uri(url) {
                        out.push(json!({"inlineData": {"mimeType": mime, "data": data}}));
                    }
                    // remote (non-data) URL: cannot inline without a fetch → skip
                }
                _ => {}
            }
        }
        if !out.is_empty() {
            return out;
        }
    }
    vec![json!({"text": m.content})]
}

/// Parse a `data:<mime>;base64,<payload>` URI into `(mime, payload)`.
fn parse_data_uri(url: &str) -> Option<(&str, &str)> {
    let rest = url.strip_prefix("data:")?;
    let (meta, data) = rest.split_once(',')?;
    let mime = meta.strip_suffix(";base64").unwrap_or(meta);
    if mime.is_empty() || data.is_empty() {
        return None;
    }
    Some((mime, data))
}

base_engine!(VertexEngine);

impl VertexEngine {
    /// Gemini API auth: the x-goog-api-key header — an API key is not an OAuth
    /// Bearer token and Google rejects it as one.
    fn gemini_headers(&self) -> Vec<(String, String)> {
        vec![
            ("content-type".into(), "application/json".into()),
            ("x-goog-api-key".into(), self.base.api_key()),
        ]
    }

    fn build_body(&self) -> GResult<Value> {
        let contents: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .map(|m| {
                let role = if m.role == gw_consts::role::AI {
                    gw_consts::role::MODEL
                } else {
                    gw_consts::role::USER
                };
                json!({"role": role, "parts": gemini_parts(m)})
            })
            .collect();
        let mut body = json!({"contents": contents});
        // sampling params → generationConfig (Gemini's shape); without this the
        // params are silently dropped and Gemini uses defaults.
        if let Some(TypedParams::Chat(p)) = self.base.param()?.typed.as_ref() {
            let mut gen_cfg = json!({});
            if let Some(t) = p.temperature {
                gen_cfg["temperature"] = json!(t);
            }
            if let Some(t) = p.top_p {
                gen_cfg["topP"] = json!(t);
            }
            if let Some(mt) = p.max_tokens {
                gen_cfg["maxOutputTokens"] = json!(mt);
            }
            if gen_cfg.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
                body["generationConfig"] = gen_cfg;
            }
        }
        Ok(body)
    }

    /// Native Gemini streaming: `:streamGenerateContent?alt=sse` frames are
    /// decoded as they arrive and forwarded through `stream_tx` when the
    /// request carries one (the shared live-pump contract).
    async fn run_stream(&self) -> GResult<EngineOutcome> {
        let body = self.build_body()?;
        let url = format!(
            "{}/v1beta/models/{}:streamGenerateContent?alt=sse",
            self.base.base_url("mock://vertex.googleapis.com"),
            self.base.param()?.model_name
        );
        let reply = self
            .base
            .send_upstream_raw(&url, self.gemini_headers(), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.base.param()?.model_name.clone(),
            ..Default::default()
        };
        if let UpstreamBody::Json(b) = &reply.body {
            // a stream request answered with JSON is an error body
            let v: Value = serde_json::from_slice(b)
                .map_err(|e| GatewayError::internal("parse gemini reply").with_source(e))?;
            if let Some(err) = crate::engine::vendor_error(status, &v) {
                return Err(err);
            }
        }
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "gemini",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| vertex_apply_frame(v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        resp.aborted = r.aborted;
        if resp.total_tokens == 0 {
            resp.total_tokens = resp.prompt_tokens.saturating_add(resp.completion_tokens);
        }
        resp.raw_usage_json = vertex_raw_usage(&resp);
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks: r.chunks,
            streamed_live: r.streamed_live,
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl ModelEngine for VertexEngine {
    /// Gemini generateContent: contents/parts request, candidates/usageMetadata
    /// response; `:streamGenerateContent?alt=sse` when the request streams.
    async fn run(&self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body()?;
        let url = format!(
            "{}/v1beta/models/{}:generateContent",
            self.base.base_url("mock://vertex.googleapis.com"),
            self.base.param()?.model_name
        );
        let (status, v) = self
            .base
            .round_trip_with(&url, self.gemini_headers(), body)
            .await?;
        let text: String = v["candidates"][0]["content"]["parts"]
            .as_array()
            .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
            .unwrap_or_default();
        let mut resp = GatewayResponse {
            message: text,
            model: self.base.param()?.model_name.clone(),
            finish_reason: v["candidates"][0]["finishReason"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase(),
            ..Default::default()
        };
        vertex_apply_usage(&v["usageMetadata"], &mut resp);
        if resp.total_tokens == 0 {
            resp.total_tokens = resp.prompt_tokens.saturating_add(resp.completion_tokens);
        }
        resp.raw_usage_json = vertex_raw_usage(&resp);
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

/// Apply one `streamGenerateContent` frame to the accumulating response;
/// returns the chunks the frame yields. usageMetadata is cumulative — the
/// last frame's counts win.
fn vertex_apply_frame(
    v: &Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    let text: String = v["candidates"][0]["content"]["parts"]
        .as_array()
        .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
        .unwrap_or_default();
    if !text.is_empty() {
        full.push_str(&text);
        chunks.push(StreamChunk {
            delta: text,
            ..Default::default()
        });
    }
    if let Some(fr) = v["candidates"][0]["finishReason"].as_str() {
        resp.finish_reason = fr.to_lowercase();
        chunks.push(StreamChunk {
            finish_reason: Some(resp.finish_reason.clone()),
            ..Default::default()
        });
    }
    vertex_apply_usage(&v["usageMetadata"], resp);
    Ok(chunks)
}

/// Fold a `usageMetadata` object into the response. Cumulative — the last
/// frame's counts win. thinking models report `thoughtsTokenCount` outside
/// `candidatesTokenCount` (live-verified on generativelanguage.googleapis.com:
/// totalTokenCount == prompt + candidates + thoughts); OpenAI semantics fold
/// reasoning into completion, so map thoughts → reasoning ⊆ completion or
/// billing loses them.
fn vertex_apply_usage(um: &Value, resp: &mut GatewayResponse) {
    if um.is_null() {
        return;
    }
    if let Some(pt) = um["promptTokenCount"].as_i64() {
        resp.prompt_tokens = pt.max(0);
    }
    let thoughts = crate::engine::tok(&um["thoughtsTokenCount"]);
    if let Some(cand) = um["candidatesTokenCount"].as_i64() {
        resp.completion_tokens = cand.max(0).saturating_add(thoughts);
        resp.reasoning_tokens = thoughts;
    }
    if let Some(tt) = um["totalTokenCount"].as_i64() {
        resp.total_tokens = tt.max(0);
    }
}

/// usage dialect normalized to the openai shape at the engine boundary
/// (CommonUsage extraction follows the openai field table).
fn vertex_raw_usage(resp: &GatewayResponse) -> Vec<u8> {
    json!({
        "prompt_tokens": resp.prompt_tokens,
        "completion_tokens": resp.completion_tokens,
        "total_tokens": resp.total_tokens,
        "completion_tokens_details": {"reasoning_tokens": resp.reasoning_tokens},
    })
    .to_string()
    .into_bytes()
}

base_engine!(EmbeddingsEngine);

#[async_trait::async_trait]
impl ModelEngine for EmbeddingsEngine {
    /// Merges the openai/ali/vertex embedding engines to the openai shape.
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let input: Vec<String> = match &param.typed {
            Some(TypedParams::Embeddings(p)) => p.input.clone(),
            _ => self
                .base
                .request
                .message
                .iter()
                .map(|m| m.content.clone())
                .collect(),
        };
        if input.is_empty() {
            return Err(GatewayError::bad_request(
                "embeddings input must not be empty",
            ));
        }
        let mut body = json!({"model": param.model_name, "input": input});
        // send dimensions if the caller set it (else silently dropped).
        if let Some(TypedParams::Embeddings(p)) = &param.typed
            && let Some(d) = p.dimensions
        {
            body["dimensions"] = json!(d);
        }
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/embeddings",
                    self.base.base_url("mock://api.openai.com")
                ),
                body,
            )
            .await?;
        let first: Vec<f32> = v["data"][0]["embedding"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_f64())
                    .map(|x| x as f32)
                    .collect()
            })
            .unwrap_or_default();
        let pt = crate::engine::tok(&v["usage"]["prompt_tokens"]);
        let resp = GatewayResponse {
            embeddings: first,
            model: param.model_name.clone(),
            prompt_tokens: pt,
            total_tokens: pt,
            raw_usage_json: v["usage"].to_string().into_bytes(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(ImageEngine);

#[async_trait::async_trait]
impl ModelEngine for ImageEngine {
    /// Merges the dalle/wanx/flux/stability/... engines to the images/generations shape.
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let (prompt, n, size, image, mask) = match &param.typed {
            Some(TypedParams::Image(p)) => (
                p.prompt.clone(),
                p.n,
                p.size.clone(),
                p.image.clone(),
                p.mask.clone(),
            ),
            _ => (
                self.base
                    .request
                    .message
                    .last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default(),
                1,
                None,
                None,
                None,
            ),
        };
        if prompt.is_empty() {
            return Err(GatewayError::bad_request("image prompt must not be empty"));
        }
        let mut body = json!({"model": param.model_name, "prompt": prompt, "n": n, "size": size});
        // `image` present → edit endpoint (source image + optional mask); else generate.
        let (path, is_edit) = if let Some(img) = image {
            body["image"] = json!(img);
            if let Some(m) = mask {
                body["mask"] = json!(m);
            }
            ("/v1/images/edits", true)
        } else {
            ("/v1/images/generations", false)
        };
        let url = format!("{}{path}", self.base.base_url("mock://api.openai.com"));
        let (status, v) = self.base.round_trip(&url, body).await?;
        let count = v["data"].as_array().map(|a| a.len()).unwrap_or(0);
        let verb = if is_edit { "edited" } else { "generated" };
        let resp = GatewayResponse {
            message: format!("{count} image(s) {verb}"),
            model: param.model_name.clone(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

/// Which audio surface this engine serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioKind {
    Tts,
    Stt,
    Other,
}

pub struct AudioEngine {
    base: Base,
    kind: AudioKind,
}

impl AudioEngine {
    pub fn new(request: GatewayRequest, transport: SharedTransport, kind: AudioKind) -> Self {
        Self {
            base: Base::new(request, transport),
            kind,
        }
    }
}

#[async_trait::async_trait]
impl ModelEngine for AudioEngine {
    /// Merges the openai_tts/whisper/azure_asr/elevenlabs/cosyvoice/minimax_t2a etc. engines.
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let (path, body) = match self.kind {
            AudioKind::Tts => {
                let (input, voice, format) = match &param.typed {
                    Some(TypedParams::AudioTts(p)) => {
                        (p.input.clone(), p.voice.clone(), p.response_format.clone())
                    }
                    _ => (
                        self.base
                            .request
                            .message
                            .last()
                            .map(|m| m.content.clone())
                            .unwrap_or_default(),
                        None,
                        None,
                    ),
                };
                if input.is_empty() {
                    return Err(GatewayError::bad_request("tts input must not be empty"));
                }
                let mut b = json!({"model": param.model_name, "input": input, "voice": voice});
                // send response_format if set (mp3/wav/pcm) — else dropped.
                if let Some(f) = format {
                    b["response_format"] = json!(f);
                }
                ("/v1/audio/speech", b)
            }
            AudioKind::Stt => {
                let (audio, language) = match &param.typed {
                    Some(TypedParams::AudioStt(p)) => (p.audio_b64.clone(), p.language.clone()),
                    _ => (String::new(), None),
                };
                if audio.is_empty() {
                    return Err(GatewayError::bad_request("stt audio_b64 must not be empty"));
                }
                (
                    "/v1/audio/transcriptions",
                    json!({"model": param.model_name, "audio_b64": audio, "language": language}),
                )
            }
            AudioKind::Other => (
                "/v1/audio/other",
                json!({"model": param.model_name, "raw": param.raw}),
            ),
        };
        let url = format!("{}{path}", self.base.base_url("mock://api.openai.com"));
        let (status, v) = self.base.round_trip(&url, body).await?;
        let message = match self.kind {
            AudioKind::Stt => v["text"].as_str().unwrap_or_default().to_owned(),
            _ => format!(
                "audio payload ({} b64 bytes)",
                v["audio_b64"].as_str().map(str::len).unwrap_or(0)
            ),
        };
        let resp = GatewayResponse {
            message,
            model: param.model_name.clone(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(VideoEngine);

#[async_trait::async_trait]
impl ModelEngine for VideoEngine {
    /// Merges the sora/veo/kling/runway/vidu/minimax_video engines
    /// (async-task type; mock completes immediately — a real submit/poll flow
    /// would be split per vendor).
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let prompt = match &param.typed {
            Some(TypedParams::Video(p)) => p.prompt.clone(),
            _ => self
                .base
                .request
                .message
                .last()
                .map(|m| m.content.clone())
                .unwrap_or_default(),
        };
        if prompt.is_empty() {
            return Err(GatewayError::bad_request("video prompt must not be empty"));
        }
        let mut body = json!({"model": param.model_name, "prompt": prompt});
        // send duration/resolution if set (else dropped).
        if let Some(TypedParams::Video(p)) = &param.typed {
            if let Some(d) = p.duration_seconds {
                body["duration_seconds"] = json!(d);
            }
            if let Some(r) = &p.resolution {
                body["resolution"] = json!(r);
            }
        }
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/videos/generations",
                    self.base.base_url("mock://api.vendor.com")
                ),
                body,
            )
            .await?;
        let resp = GatewayResponse {
            message: v["video_url"].as_str().unwrap_or_default().to_owned(),
            model: param.model_name.clone(),
            step: v["status"].as_str().unwrap_or_default().to_owned(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(SearchEngine);

#[async_trait::async_trait]
impl ModelEngine for SearchEngine {
    /// Merges the bingsearch/brave/serp/google_custom_search engines.
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let (query, count) = match &param.typed {
            Some(TypedParams::Search(p)) => (p.query.clone(), p.count),
            _ => (
                self.base
                    .request
                    .message
                    .last()
                    .map(|m| m.content.clone())
                    .unwrap_or_default(),
                3,
            ),
        };
        if query.is_empty() {
            return Err(GatewayError::bad_request("search query must not be empty"));
        }
        let body = json!({"query": query, "count": count});
        let (status, v) = self
            .base
            .round_trip(
                &format!("{}/v1/search", self.base.base_url("mock://api.vendor.com")),
                body,
            )
            .await?;
        let titles: Vec<String> = v["results"]
            .as_array()
            .map(|rs| {
                rs.iter()
                    .filter_map(|r| r["title"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let resp = GatewayResponse {
            message: titles.join("; "),
            model: param.model_name.clone(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(PassthroughEngine);

#[async_trait::async_trait]
impl ModelEngine for PassthroughEngine {
    /// Dedicated integration surfaces: request body passed through as-is,
    /// placeholder protocol (byte-level alignment deferred).
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let body = json!({"model": param.model_name, "payload": param.raw});
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/passthrough",
                    self.base.base_url("mock://api.vendor.com")
                ),
                body,
            )
            .await?;
        let resp = GatewayResponse {
            message: if v["ok"].as_bool().unwrap_or(false) {
                "ok".into()
            } else {
                "error".into()
            },
            model: param.model_name.clone(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(CompletionsEngine);

#[async_trait::async_trait]
impl ModelEngine for CompletionsEngine {
    /// The legacy openai text-completions endpoint.
    /// Request `{model, prompt, ...}` (not chat's messages), response `{choices:[{text}]}`.
    /// prompt = concatenation of message contents (the view puts prompt into a single
    /// user message).
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let prompt: String = self
            .base
            .request
            .message
            .iter()
            .map(|m| m.content.as_str())
            .collect::<Vec<_>>()
            .join("");
        let mut body = json!({"model": param.model_name, "prompt": prompt});
        if let Some(TypedParams::Chat(p)) = param.typed.as_ref() {
            if let Some(mt) = p.max_tokens {
                body["max_tokens"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        let (status, v) = self
            .base
            .round_trip(
                &format!(
                    "{}/v1/completions",
                    self.base.base_url("mock://api.openai.com")
                ),
                body,
            )
            .await?;
        // response: choices[0].text (legacy shape), not choices[].message.content
        let text = v["choices"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let usage = &v["usage"];
        let (pt, ct) = (
            crate::engine::tok(&usage["prompt_tokens"]),
            crate::engine::tok(&usage["completion_tokens"]),
        );
        let resp = GatewayResponse {
            message: text,
            model: v["model"].as_str().unwrap_or(&param.model_name).to_owned(),
            finish_reason: v["choices"][0]["finish_reason"]
                .as_str()
                .unwrap_or("stop")
                .to_owned(),
            prompt_tokens: pt,
            completion_tokens: ct,
            // floor a present-but-negative upstream total too, not just the sum
            total_tokens: usage["total_tokens"]
                .as_i64()
                .unwrap_or(pt.saturating_add(ct))
                .max(0),
            raw_usage_json: if usage.is_null() {
                vec![]
            } else {
                usage.to_string().into_bytes()
            },
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

base_engine!(ResponsesEngine);

/// Extract assistant text from a Responses `output` array (message items'
/// `output_text` content), plus any function_call items.
fn responses_output(v: &Value) -> (String, Vec<Value>) {
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    if let Some(items) = v["output"].as_array() {
        for item in items {
            match item["type"].as_str() {
                Some("message") => {
                    if let Some(content) = item["content"].as_array() {
                        for c in content {
                            if c["type"] == "output_text"
                                && let Some(t) = c["text"].as_str()
                            {
                                text.push_str(t);
                            }
                        }
                    }
                }
                Some("function_call") => tool_calls.push(item.clone()),
                _ => {} // reasoning / other item types carry no assistant text
            }
        }
    }
    (text, tool_calls)
}

/// Normalize a Responses `usage` object (input_tokens/output_tokens + details) to
/// the openai usage shape so downstream billing reads it unchanged. Returns
/// (input, output, raw_usage_json). Empty json when usage is absent.
fn responses_usage(usage: &Value) -> (i64, i64, Vec<u8>) {
    if usage.is_null() {
        return (0, 0, vec![]);
    }
    // floor upstream counts so a negative can't refund quota or bill a negative
    let input = crate::engine::tok(&usage["input_tokens"]);
    let output = crate::engine::tok(&usage["output_tokens"]);
    let cached = usage["input_tokens_details"]["cached_tokens"]
        .as_i64()
        .unwrap_or(0)
        .max(0);
    let reasoning = usage["output_tokens_details"]["reasoning_tokens"]
        .as_i64()
        .unwrap_or(0)
        .max(0);
    let raw = json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input.saturating_add(output),
        "prompt_tokens_details": {"cached_tokens": cached},
        "completion_tokens_details": {"reasoning_tokens": reasoning},
    })
    .to_string()
    .into_bytes();
    (input, output, raw)
}

/// Apply one Responses SSE frame to the accumulating response; returns the
/// chunks it yields. Text rides in `response.output_text.delta`; the final
/// usage/status arrive in `response.completed`.
fn responses_apply_frame(
    v: &Value,
    status: u16,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, v) {
        return Err(err);
    }
    let mut chunks = Vec::new();
    match v["type"].as_str().unwrap_or_default() {
        "response.output_text.delta" => {
            if let Some(d) = v["delta"].as_str() {
                full.push_str(d);
                chunks.push(StreamChunk {
                    delta: d.to_owned(),
                    ..Default::default()
                });
            }
        }
        "response.completed" => {
            let r = &v["response"];
            if let Some(m) = r["model"].as_str() {
                resp.model = m.to_owned();
            }
            if let Some(st) = r["status"].as_str() {
                resp.finish_reason = st.to_owned();
            }
            let (input, output, raw) = responses_usage(&r["usage"]);
            resp.prompt_tokens = input;
            resp.completion_tokens = output;
            resp.total_tokens = input.saturating_add(output);
            resp.raw_usage_json = raw;
            chunks.push(StreamChunk {
                finish_reason: Some(resp.finish_reason.clone()),
                ..Default::default()
            });
        }
        _ => {}
    }
    Ok(chunks)
}

impl ResponsesEngine {
    fn model_name(&self) -> String {
        self.base
            .request
            .model_param_v2
            .as_ref()
            .map(|p| p.model_name.clone())
            .unwrap_or_default()
    }

    /// Native passthrough: forward the client's Responses-shaped body verbatim,
    /// ensuring `model` is present.
    fn build_body(&self) -> GResult<Value> {
        let param = self.base.param()?;
        let mut body = match &param.raw {
            Value::Object(_) => param.raw.clone(),
            _ => json!({}),
        };
        if let Some(map) = body.as_object_mut() {
            map.entry("model".to_owned())
                .or_insert_with(|| json!(param.model_name));
        }
        Ok(body)
    }

    fn url(&self) -> String {
        format!(
            "{}/v1/responses",
            self.base.base_url("mock://api.openai.com")
        )
    }

    /// Streaming Responses reply pumped live: `response.output_text.delta` frames
    /// are forwarded through `stream_tx` as they arrive (real vendors), and
    /// `response.completed` carries the final usage/status.
    async fn run_stream(&self) -> GResult<EngineOutcome> {
        let reply = self
            .base
            .send_upstream_raw(
                &self.url(),
                self.base.bearer_headers(),
                self.build_body()?,
                true,
            )
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.model_name(),
            finish_reason: "completed".to_owned(),
            ..Default::default()
        };
        if let UpstreamBody::Json(b) = &reply.body {
            // a stream request answered with JSON is an error body
            let v: Value = serde_json::from_slice(b)
                .map_err(|e| GatewayError::internal("parse responses reply").with_source(e))?;
            if let Some(err) = crate::engine::vendor_error(status, &v) {
                return Err(err);
            }
        }
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "responses",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| responses_apply_frame(v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        resp.aborted = r.aborted;
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks: r.chunks,
            streamed_live: r.streamed_live,
            ..Default::default()
        })
    }

    /// Non-streaming Responses reply: full `output` array + `usage`.
    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse responses reply").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        let (text, tool_calls) = responses_output(&v);
        let (input, output, raw_usage_json) = responses_usage(&v["usage"]);
        let resp = GatewayResponse {
            message: text,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(Value::Array(tool_calls))
            },
            model: v["model"].as_str().unwrap_or(&self.model_name()).to_owned(),
            finish_reason: v["status"].as_str().unwrap_or("completed").to_owned(),
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            raw_usage_json,
            response_v2: Some(v),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    /// Streaming Responses reply: accumulate `response.output_text.delta` frames;
    /// final usage + status arrive in the `response.completed` frame's `response`.
    fn parse_sse(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let (events, _done) = SseDecoder::decode_all(bytes);
        let mut full = String::new();
        let mut chunks = Vec::new();
        let mut model = self.model_name();
        let mut finish_reason = "completed".to_owned();
        let (mut input, mut output, mut raw_usage_json) = (0i64, 0i64, Vec::new());
        for ev in events {
            let v: Value = serde_json::from_slice(ev.as_bytes())
                .map_err(|e| GatewayError::internal("parse responses sse frame").with_source(e))?;
            if let Some(err) = crate::engine::vendor_error(status, &v) {
                return Err(err);
            }
            match v["type"].as_str().unwrap_or_default() {
                "response.output_text.delta" => {
                    if let Some(d) = v["delta"].as_str() {
                        full.push_str(d);
                        chunks.push(StreamChunk {
                            delta: d.to_owned(),
                            finish_reason: None,
                            ..Default::default()
                        });
                    }
                }
                "response.completed" => {
                    let r = &v["response"];
                    if let Some(m) = r["model"].as_str() {
                        model = m.to_owned();
                    }
                    if let Some(st) = r["status"].as_str() {
                        finish_reason = st.to_owned();
                    }
                    let (i, o, raw) = responses_usage(&r["usage"]);
                    input = i;
                    output = o;
                    raw_usage_json = raw;
                    chunks.push(StreamChunk {
                        delta: String::new(),
                        finish_reason: Some(finish_reason.clone()),
                        ..Default::default()
                    });
                }
                _ => {} // response.created / output_item.added / content_part.* etc.
            }
        }
        let resp = GatewayResponse {
            message: full,
            model,
            finish_reason,
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            raw_usage_json,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            chunks,
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl ModelEngine for ResponsesEngine {
    /// OpenAI Responses API (POST /v1/responses).
    /// Native body passthrough (param.raw holds the client's Responses-shaped request)
    /// + ensures the model field. Non-streaming parses output items + usage; streaming
    /// parses output_text.delta + response.completed. usage dialect
    /// (input_tokens/output_tokens) is normalized to the openai shape at the engine
    /// boundary.
    async fn run(&self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let reply = self
            .base
            .send_upstream(
                &self.url(),
                self.base.bearer_headers(),
                self.build_body()?,
                false,
            )
            .await?;
        match &reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, b),
            UpstreamBody::Sse(b) => self.parse_sse(reply.status, b),
            UpstreamBody::SseStream(_) => Err(GatewayError::internal(
                "unbuffered stream reached responses engine",
            )),
        }
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::MockTransport;
    use gw_consts::Protocol;
    use gw_models::{
        ChatMsg, EmbeddingParams, ImageParams, ModelParamV2, SearchParams, SttParams, TtsParams,
        VideoParams,
    };
    use std::sync::Arc;

    fn req(mt: Protocol, name: &str, typed: Option<TypedParams>) -> GatewayRequest {
        let mut p = ModelParamV2::with_name(mt, name);
        p.typed = typed;
        GatewayRequest {
            message: vec![ChatMsg::text("user", "hello families")],
            model_param_v2: Some(p),
            ..Default::default()
        }
    }

    fn t() -> SharedTransport {
        Arc::new(MockTransport)
    }

    #[tokio::test]
    async fn vertex_round_trip() {
        let e = VertexEngine::new(req(Protocol::Gemini, "gemini-pro", None), t());
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: hello families"));
        assert!(out.response.total_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }

    #[tokio::test]
    async fn vertex_stream_decodes_frames() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.stream = true;
        let e = VertexEngine::new(r, t());
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3, "chunks: {:?}", out.chunks);
        assert!(out.response.message.contains("you said: hello families"));
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
    }

    #[tokio::test]
    async fn embeddings_round_trip() {
        let e = EmbeddingsEngine::new(
            req(
                Protocol::Embeddings,
                "text-embedding-mock",
                Some(TypedParams::Embeddings(EmbeddingParams {
                    input: vec!["abc".into(), "def".into()],
                    dimensions: None,
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.embeddings.len(), 8);
        assert!(out.response.prompt_tokens > 0);
    }

    #[tokio::test]
    async fn image_round_trip() {
        let e = ImageEngine::new(
            req(
                Protocol::Image,
                "img-mock",
                Some(TypedParams::Image(ImageParams {
                    prompt: "a cat".into(),
                    n: 2,
                    size: None,
                    ..Default::default()
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "2 image(s) generated");
        assert!(out.response.response_v2.is_some());
    }

    #[tokio::test]
    async fn audio_tts_and_stt() {
        let tts = AudioEngine::new(
            req(
                Protocol::Tts,
                "tts-mock",
                Some(TypedParams::AudioTts(TtsParams {
                    input: "read this".into(),
                    voice: Some("alloy".into()),
                    response_format: None,
                })),
            ),
            t(),
            AudioKind::Tts,
        );
        assert!(
            tts.run()
                .await
                .unwrap()
                .response
                .message
                .contains("audio payload")
        );

        let stt = AudioEngine::new(
            req(
                Protocol::Stt,
                "whisper-mock",
                Some(TypedParams::AudioStt(SttParams {
                    audio_b64: "TU9DSw==".into(),
                    language: Some("en".into()),
                })),
            ),
            t(),
            AudioKind::Stt,
        );
        assert!(
            stt.run()
                .await
                .unwrap()
                .response
                .message
                .contains("transcribed")
        );
    }

    #[tokio::test]
    async fn video_and_search_and_passthrough() {
        let v = VideoEngine::new(
            req(
                Protocol::Video,
                "kling-mock",
                Some(TypedParams::Video(VideoParams {
                    prompt: "a dog surfing".into(),
                    duration_seconds: None,
                    resolution: None,
                })),
            ),
            t(),
        );
        let out = v.run().await.unwrap();
        assert_eq!(out.response.message, "mock://videos/out.mp4");
        assert_eq!(out.response.step, "succeeded");

        let s = SearchEngine::new(
            req(
                Protocol::Search,
                "brave-mock",
                Some(TypedParams::Search(SearchParams {
                    query: "rust dag".into(),
                    count: 2,
                })),
            ),
            t(),
        );
        let out = s.run().await.unwrap();
        assert!(out.response.message.contains("result 1 for rust dag"));

        let p = PassthroughEngine::new(req(Protocol::Passthrough, "e2b", None), t());
        assert_eq!(p.run().await.unwrap().response.message, "ok");
    }

    #[tokio::test]
    async fn down_account_fails_upstream() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.account = Some(gw_models::Account {
            name: "mock-vertex-down".into(),
            ..Default::default()
        });
        let e = VertexEngine::new(r, t());
        let err = e.run().await.err().unwrap();
        assert_eq!(err.http_status, 503);
    }

    #[tokio::test]
    async fn responses_api_round_trip() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({
            "input": "summarize this",
            "instructions": "be terse",
        });
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        assert!(
            out.response.message.contains("you said: summarize this"),
            "message: {}",
            out.response.message
        );
        assert_eq!(out.response.finish_reason, "completed");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert_eq!(
            out.response.total_tokens,
            out.response.prompt_tokens + out.response.completion_tokens
        );
        let u = String::from_utf8(out.response.raw_usage_json).unwrap();
        assert!(u.contains("prompt_tokens") && u.contains("completion_tokens"));
    }

    #[tokio::test]
    async fn responses_api_streaming() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.stream = true;
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({"input": "stream this"});
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        assert!(out.chunks.len() >= 2, "chunks: {:?}", out.chunks);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
        assert!(
            out.response.message.contains("you said: stream this"),
            "message: {}",
            out.response.message
        );
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }
}
