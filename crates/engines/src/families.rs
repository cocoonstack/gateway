//! The non-chat protocol engines.
//! One engine per Protocol variant (AudioEngine covers tts/stt/audio via AudioKind):
//!   Vertex generateContent / Embeddings / Image / Audio(TTS·STT·other) /
//!   Video(async task) / Search / Passthrough(register+misc).
//! Each engine only does "build request → Transport → parse response" — nothing else
//! crosses that boundary.
//! The mock protocol flags byte-level vendor differences as deferred to a later
//! fidelity pass.

use ap_models::{
    GResult, GatewayError, GatewayRequest, GatewayResponse, Recorder, SimpleRecorder, TypedParams,
};
use chrono::Utc;
use serde_json::{Value, json};

use crate::engine::{EngineOutcome, ModelEngine, StreamChunk};
use crate::sse::SseDecoder;
use crate::transport::{SharedTransport, UpstreamBody, UpstreamRequest, UpstreamResponse};

/// Shared scaffolding: request + transport + recorder + one JSON round trip.
struct Base {
    request: GatewayRequest,
    transport: SharedTransport,
    recorder: SimpleRecorder,
}

impl Base {
    fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
        Self {
            request,
            transport,
            recorder: SimpleRecorder::new(Utc::now()),
        }
    }

    fn account(&self) -> String {
        self.request
            .account
            .as_ref()
            .map(|a| a.name.clone())
            .unwrap_or_default()
    }

    /// Base URL for the upstream call: the account's configured endpoint when set
    /// (go-live), else the `mock_sentinel` (offline — MockTransport
    /// routes by the path in this sentinel); same seam as OpenAiEngine.
    fn base_url(&self, mock_sentinel: &str) -> String {
        self.request
            .account
            .as_ref()
            .map(|a| a.base_url(mock_sentinel).to_owned())
            .unwrap_or_else(|| mock_sentinel.to_owned())
    }

    /// The account's API key (read from its env var at call time when live), else
    /// the inert "mock" sentinel.
    fn api_key(&self) -> String {
        self.request
            .account
            .as_ref()
            .and_then(|a| a.api_key())
            .unwrap_or_else(|| "mock".to_owned())
    }

    fn param(&self) -> GResult<&ap_models::ModelParamV2> {
        self.request
            .model_param_v2
            .as_ref()
            .ok_or_else(|| GatewayError::bad_request("missing model param"))
    }

    /// Build and send an upstream POST, returning the raw reply (Json or Sse).
    /// Engines that stream dispatch on the body type themselves.
    async fn send_upstream(
        &self,
        url: &str,
        body: Value,
        stream: bool,
    ) -> GResult<UpstreamResponse> {
        let param = self.param()?;
        let up = UpstreamRequest {
            protocol: param.protocol,
            method: "POST".to_owned(),
            url: url.to_owned(),
            headers: vec![
                ("content-type".into(), "application/json".into()),
                // Bearer for the OpenAI-shaped families + Vertex OAuth; real key
                // when the account is live, inert "mock" otherwise.
                ("authorization".into(), format!("Bearer {}", self.api_key())),
            ],
            body: body.to_string().into_bytes(),
            stream,
            account: self.account(),
        };
        self.transport.send(up).await
    }

    /// POST body to `url`, expect JSON back (non-streaming).
    async fn round_trip(&self, url: &str, body: Value) -> GResult<(u16, Value)> {
        let reply = self.send_upstream(url, body, false).await?;
        let bytes = match &reply.body {
            UpstreamBody::Json(b) => b,
            UpstreamBody::Sse(_) => {
                return Err(GatewayError::internal(
                    "unexpected sse body for json engine",
                ));
            }
        };
        let v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse upstream response").with_source(e))?;
        // surface vendor error envelopes instead of parsing them as broken success
        if let Some(err) = crate::engine::vendor_error(reply.status, &v) {
            return Err(err);
        }
        Ok((reply.status, v))
    }
}

macro_rules! family_engine {
    ($name:ident) => {
        pub struct $name {
            base: Base,
        }

        impl $name {
            pub fn new(request: GatewayRequest, transport: SharedTransport) -> Self {
                Self {
                    base: Base::new(request, transport),
                }
            }
        }
    };
}

// ---------------------------------------------------------------- Vertex chat

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
fn gemini_parts(m: &ap_models::ChatMsg) -> Vec<Value> {
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

family_engine!(VertexEngine);

#[async_trait::async_trait]
impl ModelEngine for VertexEngine {
    /// Gemini generateContent: contents/parts request, candidates/usageMetadata response.
    async fn run(&self) -> GResult<EngineOutcome> {
        let contents: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .map(|m| {
                let role = if m.role == ap_consts::role::AI {
                    ap_consts::role::MODEL
                } else {
                    ap_consts::role::USER
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
        let url = format!(
            "{}/v1/models/{}:generateContent",
            self.base.base_url("mock://vertex.googleapis.com"),
            self.base.param()?.model_name
        );
        let (status, v) = self.base.round_trip(&url, body).await?;
        let text: String = v["candidates"][0]["content"]["parts"]
            .as_array()
            .map(|ps| ps.iter().filter_map(|p| p["text"].as_str()).collect())
            .unwrap_or_default();
        let um = &v["usageMetadata"];
        let (pt, ct) = (
            um["promptTokenCount"].as_i64().unwrap_or(0),
            um["candidatesTokenCount"].as_i64().unwrap_or(0),
        );
        // usage dialect is normalized to the openai shape at the engine boundary
        // (CommonUsage extraction follows the openai field table)
        let raw_usage =
            json!({"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct});
        let resp = GatewayResponse {
            message: text,
            model: self.base.param()?.model_name.clone(),
            finish_reason: v["candidates"][0]["finishReason"]
                .as_str()
                .unwrap_or_default()
                .to_lowercase(),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: pt + ct,
            raw_usage_json: raw_usage.to_string().into_bytes(),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Embeddings

family_engine!(EmbeddingsEngine);

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
        let pt = v["usage"]["prompt_tokens"].as_i64().unwrap_or(0);
        let resp = GatewayResponse {
            embeddings: first,
            model: param.model_name.clone(),
            prompt_tokens: pt,
            total_tokens: pt,
            raw_usage_json: v["usage"].to_string().into_bytes(),
            response_v2: Some(v),
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Image

family_engine!(ImageEngine);

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
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Audio

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
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Video

family_engine!(VideoEngine);

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
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Search

family_engine!(SearchEngine);

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
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Passthrough

family_engine!(PassthroughEngine);

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
            http_code: status as i64,
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Legacy completions

family_engine!(CompletionsEngine);

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
            usage["prompt_tokens"].as_i64().unwrap_or(0),
            usage["completion_tokens"].as_i64().unwrap_or(0),
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
            total_tokens: usage["total_tokens"].as_i64().unwrap_or(pt + ct),
            raw_usage_json: if usage.is_null() {
                vec![]
            } else {
                usage.to_string().into_bytes()
            },
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
    }

    fn recorder(&self) -> &dyn Recorder {
        &self.base.recorder
    }
}

// ---------------------------------------------------------------- Responses API

family_engine!(ResponsesEngine);

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
    let input = usage["input_tokens"].as_i64().unwrap_or(0);
    let output = usage["output_tokens"].as_i64().unwrap_or(0);
    let cached = usage["input_tokens_details"]["cached_tokens"]
        .as_i64()
        .unwrap_or(0);
    let reasoning = usage["output_tokens_details"]["reasoning_tokens"]
        .as_i64()
        .unwrap_or(0);
    let raw = json!({
        "prompt_tokens": input,
        "completion_tokens": output,
        "total_tokens": input + output,
        "prompt_tokens_details": {"cached_tokens": cached},
        "completion_tokens_details": {"reasoning_tokens": reasoning},
    })
    .to_string()
    .into_bytes();
    (input, output, raw)
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
            total_tokens: input + output,
            raw_usage_json,
            response_v2: Some(v),
            http_code: status as i64,
            ..Default::default()
        };
        Ok(EngineOutcome {
            response: resp,
            http_code: status,
            ..Default::default()
        })
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
            total_tokens: input + output,
            raw_usage_json,
            http_code: status as i64,
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
    /// OpenAI Responses API (POST /openai/responses).
    /// Native body passthrough (param.raw holds the client's Responses-shaped request)
    /// + ensures the model field. Non-streaming parses output items + usage; streaming
    /// parses output_text.delta + response.completed. usage dialect
    /// (input_tokens/output_tokens) is normalized to the openai shape at the engine
    /// boundary.
    async fn run(&self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        // native passthrough: forward the client's Responses-shaped body verbatim,
        // ensuring `model` is present (live integration swaps in the real endpoint+version).
        let mut body = match &param.raw {
            Value::Object(_) => param.raw.clone(),
            _ => json!({}),
        };
        if let Some(map) = body.as_object_mut() {
            map.entry("model".to_owned())
                .or_insert_with(|| json!(param.model_name));
        }
        let url = format!(
            "{}/openai/responses",
            self.base.base_url("mock://api.openai.com")
        );
        let reply = self
            .base
            .send_upstream(&url, body, self.base.request.stream)
            .await?;
        match &reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, b),
            UpstreamBody::Sse(b) => self.parse_sse(reply.status, b),
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
    use ap_consts::Protocol;
    use ap_models::{
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
        r.account = Some(ap_models::Account {
            name: "mock-vertex-down".into(),
            ..Default::default()
        });
        let e = VertexEngine::new(r, t());
        let err = e.run().await.err().unwrap();
        assert_eq!(err.http_status, 503);
    }

    #[tokio::test]
    async fn responses_api_round_trip() {
        // Responses request: native body lives in `raw` (the client's Responses
        // shape). Engine forwards it, parses output-item text and input/output
        // token usage.
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({
            "input": "summarize this",
            "instructions": "be terse",
        });
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        // assistant text extracted from output[].content[].output_text
        assert!(
            out.response.message.contains("you said: summarize this"),
            "message: {}",
            out.response.message
        );
        assert_eq!(out.response.finish_reason, "completed");
        // Responses input_tokens/output_tokens surfaced as prompt/completion tokens
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert_eq!(
            out.response.total_tokens,
            out.response.prompt_tokens + out.response.completion_tokens
        );
        // usage normalized to openai shape so downstream billing reads it
        let u = String::from_utf8(out.response.raw_usage_json).unwrap();
        assert!(u.contains("prompt_tokens") && u.contains("completion_tokens"));
    }

    #[tokio::test]
    async fn responses_api_streaming() {
        // stream=true → engine decodes response.output_text.delta frames into
        // chunks and reads final usage from the response.completed frame.
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.stream = true;
        r.model_param_v2.as_mut().unwrap().raw = serde_json::json!({"input": "stream this"});
        let out = ResponsesEngine::new(r, t()).run().await.unwrap();
        // at least the two text deltas + the completed marker
        assert!(out.chunks.len() >= 2, "chunks: {:?}", out.chunks);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
        // reassembled message matches what the deltas spelled out
        assert!(
            out.response.message.contains("you said: stream this"),
            "message: {}",
            out.response.message
        );
        // usage recovered from the completed frame
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
    }
}
