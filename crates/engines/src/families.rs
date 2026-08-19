//! The non-chat protocol engines, one per Protocol variant. Each engine only
//! does "build request → Transport → parse response" — nothing else crosses
//! that boundary. The mock protocol flags byte-level vendor differences as
//! deferred to a later fidelity pass.

use base64::Engine as _;
use gw_models::{GResult, GatewayError, GatewayRequest, GatewayResponse, TypedParams, VideoParams};
use gw_protocol::object;
use serde_json::{Map, Value, json};

use crate::base::{Base, VENDOR_SENTINEL, base_engine, parse_json_reply, versioned_url};
use crate::engine::{EngineOutcome, ModelEngine, StreamChunk, reject_minimax_error};
use crate::multipart::{Form, audio_kind, image_kind};
use crate::sse::SseDecoder;
use crate::transport::{Headers, SharedTransport, Transport, UpstreamBody, UpstreamRequest};

/// Gemini `parts` from a unified message: text and data-URI images (`inlineData`);
/// remote image URLs cannot be inlined without a fetch and are skipped.
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
    fn gemini_headers(&self) -> Headers {
        vec![
            ("content-type", "application/json".into()),
            ("x-goog-api-key", self.base.api_key()),
        ]
    }

    fn build_body(&self) -> Value {
        // Gemini has no system role: system turns go to systemInstruction, never contents
        let contents: Vec<Value> = self
            .base
            .request
            .message
            .iter()
            .filter(|m| m.role != gw_consts::role::SYSTEM)
            .map(|m| {
                let role = if m.role == gw_consts::role::AI {
                    gw_consts::role::MODEL
                } else {
                    gw_consts::role::USER
                };
                object([
                    ("role", role.into()),
                    ("parts", Value::Array(gemini_parts(m))),
                ])
            })
            .collect();
        let mut body = json!({});
        body["contents"] = Value::Array(contents);
        let system = self.base.system_text();
        if !system.is_empty() {
            let part = object([("text", system.into())]);
            body["systemInstruction"] = object([("parts", Value::Array(vec![part]))]);
        }
        // sampling params → generationConfig — else Gemini silently uses defaults
        if let Some(p) = self.base.chat_params() {
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
        body
    }

    /// Native Gemini streaming: `:streamGenerateContent?alt=sse` frames decoded
    /// as they arrive and forwarded through `stream_tx` (the live-pump contract).
    async fn run_stream(&self) -> GResult<EngineOutcome> {
        let body = self.build_body();
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
        crate::pump::reject_json_error("gemini", status, &reply.body)?;
        let mut full = String::new();
        let r = crate::pump::pump_sse(
            "gemini",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| vertex_apply_frame(&v, status, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = vertex_common_usage(&resp);
        Ok(EngineOutcome::from_pump(resp, status, r))
    }
}

#[async_trait::async_trait]
impl ModelEngine for VertexEngine {
    /// Gemini generateContent: contents/parts request, candidates/usageMetadata
    /// response; `:streamGenerateContent?alt=sse` when the request streams.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body();
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
            finish_reason: vertex_finish_reason(
                v["candidates"][0]["finishReason"]
                    .as_str()
                    .unwrap_or_default(),
            ),
            ..Default::default()
        };
        vertex_apply_usage(&v["usageMetadata"], &mut resp);
        crate::engine::fill_total_if_zero(&mut resp);
        resp.common_usage = vertex_common_usage(&resp);
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// Gemini finishReason in the shared vocabulary: safety-family values become
/// `content_filter`, the rest lowercase.
fn vertex_finish_reason(fr: &str) -> String {
    match fr {
        "SAFETY" | "RECITATION" | "PROHIBITED_CONTENT" | "SPII" | "BLOCKLIST" => {
            "content_filter".to_owned()
        }
        other => other.to_lowercase(),
    }
}

/// Apply one `streamGenerateContent` frame to the accumulating response;
/// returns the chunks it yields. usageMetadata is cumulative — last frame wins.
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
        resp.finish_reason = vertex_finish_reason(fr);
        chunks.push(StreamChunk {
            finish_reason: Some(resp.finish_reason.clone()),
            ..Default::default()
        });
    }
    vertex_apply_usage(&v["usageMetadata"], resp);
    Ok(chunks)
}

/// Fold a cumulative `usageMetadata` into the response (last frame wins);
/// `thoughtsTokenCount` sits outside `candidatesTokenCount`, so thoughts fold
/// into completion or billing loses them.
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

fn vertex_common_usage(resp: &GatewayResponse) -> Option<gw_models::CommonUsage> {
    Some(gw_models::CommonUsage::from_openai_parts(
        resp.prompt_tokens,
        resp.completion_tokens,
        0,
        resp.reasoning_tokens,
    ))
}

base_engine!(EmbeddingsEngine);

#[async_trait::async_trait]
impl ModelEngine for EmbeddingsEngine {
    /// Merges the openai/ali/vertex embedding engines to the openai shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // the batch moves: json! would re-copy every input string
        let (input, dimensions) = match self.base.take_typed() {
            Some(TypedParams::Embeddings(p)) => (
                Value::Array(p.input.into_iter().map(Value::String).collect()),
                p.dimensions,
            ),
            _ => (
                Value::Array(
                    std::mem::take(&mut self.base.request.message)
                        .into_iter()
                        .map(|m| Value::String(m.content))
                        .collect(),
                ),
                None,
            ),
        };
        if input.as_array().is_none_or(|a| a.is_empty()) {
            return Err(GatewayError::bad_request(
                "embeddings input must not be empty",
            ));
        }
        let mut body = json!({"model": model});
        body["input"] = input;
        if let Some(d) = dimensions {
            body["dimensions"] = json!(d);
        }
        let (status, v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "embeddings"),
                body,
            )
            .await?;
        let pt = crate::engine::tok(&v["usage"]["prompt_tokens"]);
        let resp = GatewayResponse {
            model,
            prompt_tokens: pt,
            total_tokens: pt,
            raw_usage: (!v["usage"].is_null()).then(|| v["usage"].clone()),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

/// The uniform family-engine tail: a summary message plus the native payload,
/// finished at "stop".
fn family_outcome(
    message: String,
    model: &str,
    v: serde_json::Value,
    status: u16,
) -> EngineOutcome {
    EngineOutcome::with_status(
        GatewayResponse {
            message,
            model: model.to_owned(),
            response_v2: Some(v),
            finish_reason: "stop".to_owned(),
            ..Default::default()
        },
        status,
    )
}

fn require_non_empty(v: &str, what: &str) -> GResult<()> {
    if v.is_empty() {
        return Err(GatewayError::bad_request(format!(
            "{what} must not be empty"
        )));
    }
    Ok(())
}

base_engine!(ImageEngine);

#[async_trait::async_trait]
impl ModelEngine for ImageEngine {
    /// Merges the dalle/wanx/flux/stability/... engines to the images/generations shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let (prompt, n, size, image, mask) = match self.base.take_typed() {
            Some(TypedParams::Image(p)) => (p.prompt, p.n, p.size, p.image, p.mask),
            _ => (
                self.base.last_message_text().to_owned(),
                1,
                None,
                None,
                None,
            ),
        };
        require_non_empty(&prompt, "image prompt")?;
        let (status, v, is_edit) = if let Some(image) = image {
            let image = decode_b64(&image, "image")?;
            let mut form = Form::new(&image);
            form.text("model", &model);
            form.text("prompt", &prompt);
            form.text("n", &n.to_string());
            if let Some(size) = &size {
                form.text("size", size);
            }
            let (ext, content_type) = image_kind(&image);
            form.file("image", &format!("image.{ext}"), content_type, &image);
            if let Some(mask) = mask {
                let mask = decode_b64(&mask, "mask")?;
                let (ext, content_type) = image_kind(&mask);
                form.file("mask", &format!("mask.{ext}"), content_type, &mask);
            }
            let (content_type, body) = form.finish();
            let headers = vec![
                ("content-type", content_type),
                ("authorization", format!("Bearer {}", self.base.api_key())),
            ];
            let reply = self
                .base
                .send_bytes(
                    &self
                        .base
                        .openai_url("mock://api.openai.com", "images/edits"),
                    headers,
                    body,
                    false,
                )
                .await?;
            let (status, v) = crate::base::parse_json_reply(reply)?;
            (status, v, true)
        } else {
            let mut body = json!({"model": model, "n": n});
            body["prompt"] = prompt.into();
            if let Some(s) = size {
                body["size"] = s.into();
            }
            let (status, v) = self
                .base
                .round_trip(
                    &self
                        .base
                        .openai_url("mock://api.openai.com", "images/generations"),
                    body,
                )
                .await?;
            (status, v, false)
        };
        let count = v["data"].as_array().map(|a| a.len()).unwrap_or(0);
        let verb = if is_edit { "edited" } else { "generated" };
        // gpt-image reports tokens; every model bills per image when it carries a unit price
        let (input, output) = (
            crate::engine::tok(&v["usage"]["input_tokens"]),
            crate::engine::tok(&v["usage"]["output_tokens"]),
        );
        let mut outcome = family_outcome(format!("{count} image(s) {verb}"), &model, v, status);
        outcome.response.prompt_tokens = input;
        outcome.response.completion_tokens = output;
        outcome.response.billed_units = count as i64;
        crate::engine::fill_total_if_zero(&mut outcome.response);
        Ok(outcome)
    }
}

/// Decode a client-supplied base64 payload; a bad payload is the client's 400,
/// not an upstream failure.
fn decode_b64(payload: &str, what: &str) -> GResult<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|e| GatewayError::bad_request(format!("{what} is not valid base64: {e}")))
}

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
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // speech bills per input char, transcription per second (vendor count, else play length)
        let mut units = 0;
        let mut local_seconds = None;
        let (status, v) = match self.kind {
            AudioKind::Tts => {
                let (input, voice, format) = match &self.base.param()?.typed {
                    Some(TypedParams::AudioTts(p)) => (
                        p.input.as_str(),
                        p.voice.as_deref(),
                        p.response_format.as_deref(),
                    ),
                    _ => (self.base.last_message_text(), None, None),
                };
                require_non_empty(input, "tts input")?;
                units = input.chars().count() as i64;
                let mut b = json!({"model": model, "input": input});
                if let Some(v) = voice {
                    b["voice"] = json!(v);
                }
                if let Some(f) = format {
                    b["response_format"] = json!(f);
                }
                let reply = self
                    .base
                    .send_upstream(
                        &self
                            .base
                            .openai_url("mock://api.openai.com", "audio/speech"),
                        self.base.bearer_headers(),
                        b,
                        false,
                    )
                    .await?;
                // a JSON body is an error envelope (or a compatible upstream's b64), not audio
                let status = reply.status;
                match reply.body {
                    UpstreamBody::Json(bytes) if status < 400 && !looks_like_json(&bytes) => {
                        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
                        (status, object([("audio_b64", b64.into())]))
                    }
                    body => crate::base::parse_json_reply(crate::transport::UpstreamResponse {
                        status,
                        body,
                        headers: reply.headers,
                    })?,
                }
            }
            AudioKind::Stt => {
                let (audio, language, translate) = match self.base.take_typed() {
                    Some(TypedParams::AudioStt(p)) => (p.audio_b64, p.language, p.translate),
                    _ => (String::new(), None, false),
                };
                require_non_empty(&audio, "stt audio_b64")?;
                let audio = decode_b64(&audio, "audio_b64")?;
                local_seconds = crate::multipart::audio_seconds(&audio);
                let path = if translate {
                    "audio/translations"
                } else {
                    "audio/transcriptions"
                };
                let mut form = Form::new(&audio);
                form.text("model", &model);
                if let Some(language) = &language {
                    form.text("language", language);
                }
                let (ext, content_type) = audio_kind(&audio);
                form.file("file", &format!("audio.{ext}"), content_type, &audio);
                let (content_type, body) = form.finish();
                let headers = vec![
                    ("content-type", content_type),
                    ("authorization", format!("Bearer {}", self.base.api_key())),
                ];
                let reply = self
                    .base
                    .send_bytes(
                        &self.base.openai_url("mock://api.openai.com", path),
                        headers,
                        body,
                        false,
                    )
                    .await?;
                crate::base::parse_json_reply(reply)?
            }
            AudioKind::Other => {
                let mut b = json!({"model": model});
                b["raw"] = self.base.take_raw();
                self.base
                    .round_trip(
                        &self.base.openai_url("mock://api.openai.com", "audio/other"),
                        b,
                    )
                    .await?
            }
        };
        let message = match self.kind {
            AudioKind::Stt => v["text"].as_str().unwrap_or_default().to_owned(),
            _ => format!(
                "audio payload ({} b64 bytes)",
                v["audio_b64"].as_str().map(str::len).unwrap_or(0)
            ),
        };
        // duration-priced transcription reports `usage.seconds` (or `duration`)
        let (input, output) = (
            crate::engine::tok(&v["usage"]["input_tokens"]),
            crate::engine::tok(&v["usage"]["output_tokens"]),
        );
        if self.kind == AudioKind::Stt {
            units = whole_seconds(&v["usage"]["seconds"])
                .or_else(|| whole_seconds(&v["duration"]))
                .or_else(|| local_seconds.map(|s| s.ceil() as i64))
                .unwrap_or(0);
        }
        let mut outcome = family_outcome(message, &model, v, status);
        outcome.response.prompt_tokens = input;
        outcome.response.completion_tokens = output;
        outcome.response.billed_units = units;
        crate::engine::fill_total_if_zero(&mut outcome.response);
        Ok(outcome)
    }
}

/// A vendor duration as whole billed seconds (fractions round up).
pub fn whole_seconds(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_f64().map(|f| f.ceil() as i64))
        .or_else(|| {
            v.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|f| f.ceil() as i64)
        })
        .filter(|s| *s > 0)
}

fn looks_like_json(bytes: &[u8]) -> bool {
    matches!(
        bytes.iter().find(|b| !b.is_ascii_whitespace()),
        Some(b'{' | b'[')
    )
}

/// Which async-video wire an account speaks, by its provider name (the same
/// keying the realtime bridge uses for its Gemini dialect).
#[derive(Debug, Clone, Copy, PartialEq)]
enum VideoDialect {
    /// xAI/Kling `videos/generations`: `request_id` or an inline `video_url`.
    Generations,
    /// OpenAI Sora `/v1/videos`: a video object, `seconds` as a string.
    Sora,
    /// SiliconFlow Wan `/v1/video/submit` + POST `/v1/video/status`.
    SiliconFlow,
    /// DashScope `/api/v1/services/aigc/video-generation/video-synthesis` + `/api/v1/tasks/{id}`.
    DashScope,
    /// MiniMax Hailuo `/v1/video_generation` + `/v1/query/video_generation`.
    Minimax,
}

fn video_dialect(provider: &str) -> VideoDialect {
    match provider {
        "openai" => VideoDialect::Sora,
        "siliconflow" => VideoDialect::SiliconFlow,
        "alibaba" | "dashscope" => VideoDialect::DashScope,
        "minimax" => VideoDialect::Minimax,
        _ => VideoDialect::Generations,
    }
}

/// The vendor's async handle in a submit reply, whichever dialect answered;
/// `None` for a synchronous reply that already carries the video.
pub fn video_handle(v: &Value) -> Option<&str> {
    // task ids before `request_id`: DashScope's reply carries both, and its
    // top-level request_id is the HTTP call's trace id, not the job
    v["output"]["task_id"]
        .as_str()
        .or_else(|| v["requestId"].as_str())
        .or_else(|| v["task_id"].as_str().filter(|id| !id.is_empty()))
        .or_else(|| v["request_id"].as_str())
        .or_else(|| v["id"].as_str().filter(|_| v["object"] == "video"))
}

/// One normalized poll of an async video job: the vendor body passes through,
/// `done`/`failed`/`units`/`vendor_cost` drive the gateway's one-shot settle.
pub struct VideoPoll {
    pub status: u16,
    pub body: Value,
    pub done: bool,
    /// Billable units on `done`: the clip's whole seconds when the vendor
    /// reports a duration, else the number of delivered videos.
    pub units: i64,
    pub vendor_cost: Option<i64>,
}

base_engine!(VideoEngine);

#[async_trait::async_trait]
impl ModelEngine for VideoEngine {
    /// A sync vendor answers with the video, an async one with a handle the
    /// poll surface settles later; the body follows the account's dialect.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let p = match self.base.take_typed() {
            Some(TypedParams::Video(p)) => p,
            _ => VideoParams {
                prompt: self.base.last_message_text().to_owned(),
                ..Default::default()
            },
        };
        require_non_empty(&p.prompt, "video prompt")?;
        let dialect = video_dialect(self.base.provider());
        let model = self.base.model_name()?;
        let mut body = Map::new();
        body.insert("model".into(), model.into());
        let (path, fields) = match dialect {
            VideoDialect::Sora => (
                "videos",
                vec![
                    ("seconds", p.duration_seconds.map(|d| d.to_string().into())),
                    ("size", p.resolution.map(Value::from)),
                    ("input_reference", p.image),
                ],
            ),
            VideoDialect::SiliconFlow => (
                "video/submit",
                vec![
                    ("image_size", p.resolution.map(Value::from)),
                    ("image", p.image),
                ],
            ),
            VideoDialect::DashScope => {
                let mut input = Map::with_capacity(2);
                input.insert("prompt".into(), p.prompt.into());
                if let Some(image) = p.image {
                    input.insert("img_url".into(), image);
                }
                let mut parameters = Map::with_capacity(2);
                if let Some(d) = p.duration_seconds {
                    parameters.insert("duration".into(), d.into());
                }
                if let Some(r) = p.resolution {
                    parameters.insert("size".into(), r.into());
                }
                body.insert("input".into(), Value::Object(input));
                if !parameters.is_empty() {
                    body.insert("parameters".into(), Value::Object(parameters));
                }
                let url = format!(
                    "{}/api/v1/services/aigc/video-generation/video-synthesis",
                    self.base.base_url(VENDOR_SENTINEL)
                );
                let mut headers = self.base.bearer_headers();
                headers.push(("X-DashScope-Async", "enable".into()));
                let (status, v) = self
                    .base
                    .round_trip_with(&url, headers, body.into())
                    .await?;
                return Ok(video_outcome(model, v, status));
            }
            VideoDialect::Minimax => (
                "video_generation",
                vec![
                    ("duration", p.duration_seconds.map(Value::from)),
                    ("resolution", p.resolution.map(Value::from)),
                    ("first_frame_image", p.image),
                ],
            ),
            VideoDialect::Generations => (
                "videos/generations",
                vec![
                    ("duration", p.duration_seconds.map(Value::from)),
                    ("resolution", p.resolution.map(Value::from)),
                    ("image", p.image),
                ],
            ),
        };
        body.insert("prompt".into(), p.prompt.into());
        if dialect == VideoDialect::Generations
            && let Some(ar) = p.aspect_ratio
        {
            body.insert("aspect_ratio".into(), ar.into());
        }
        for (k, v) in fields {
            if let Some(v) = v {
                body.insert(k.into(), v);
            }
        }
        let (status, v) = self
            .base
            .round_trip(&self.base.vendor_url(path), body.into())
            .await?;
        if dialect == VideoDialect::Minimax {
            reject_minimax_error(&v)?;
        }
        Ok(video_outcome(model, v, status))
    }
}

fn video_outcome(model: &str, v: Value, status: u16) -> EngineOutcome {
    let message = v["video"]["url"]
        .as_str()
        .or_else(|| v["video_url"].as_str())
        .unwrap_or_default()
        .to_owned();
    let step = v["status"]
        .as_str()
        .or_else(|| v["output"]["task_status"].as_str())
        .unwrap_or_default()
        .to_owned();
    let units = whole_seconds(&v["video"]["duration"]).unwrap_or(0);
    let mut out = family_outcome(message, model, v, status);
    out.response.step = step;
    out.response.billed_units = units;
    out
}

/// Poll an async video generation where it was submitted, in the account's dialect.
pub async fn video_poll(
    transport: &dyn Transport,
    account: &gw_models::Account,
    id: &str,
) -> GResult<VideoPoll> {
    let base = account.base_url(VENDOR_SENTINEL);
    let dialect = video_dialect(&account.provider);
    let (method, url, req_body) = match dialect {
        VideoDialect::SiliconFlow => (
            "POST",
            versioned_url(base, "video/status"),
            serde_json::to_vec(&object([("requestId", id.into())])).unwrap_or_default(),
        ),
        VideoDialect::DashScope => ("GET", format!("{base}/api/v1/tasks/{id}"), Vec::new()),
        VideoDialect::Minimax => (
            "GET",
            versioned_url(base, &format!("query/video_generation?task_id={id}")),
            Vec::new(),
        ),
        _ => (
            "GET",
            versioned_url(base, &format!("videos/{id}")),
            Vec::new(),
        ),
    };
    let (status, body) = video_send(transport, account, method, url, req_body).await?;
    if dialect == VideoDialect::Minimax {
        reject_minimax_error(&body)?;
    }
    Ok(normalize_video_poll(dialect, status, body))
}

fn normalize_video_poll(dialect: VideoDialect, status: u16, body: Value) -> VideoPoll {
    let (done, units, vendor_cost) = match dialect {
        VideoDialect::Sora => (
            body["status"] == "completed",
            whole_seconds(&body["seconds"]).unwrap_or(0),
            None,
        ),
        VideoDialect::SiliconFlow => (
            body["status"] == "Succeed",
            body["results"]["videos"].as_array().map_or(0, Vec::len) as i64,
            None,
        ),
        VideoDialect::DashScope => (
            body["output"]["task_status"] == "SUCCEEDED",
            whole_seconds(&body["usage"]["video_duration"])
                .unwrap_or_else(|| body["output"]["video_url"].is_string() as i64),
            None,
        ),
        VideoDialect::Minimax => (
            body["status"] == "Success",
            !body["file_id"].as_str().unwrap_or_default().is_empty() as i64,
            None,
        ),
        VideoDialect::Generations => (
            body["status"] == "done",
            whole_seconds(&body["video"]["duration"]).unwrap_or(0),
            // 1 tick = 1e-10 USD; micros are 1e-6
            body["usage"]["cost_in_usd_ticks"]
                .as_f64()
                .map(|t| (t / 1e4).round() as i64),
        ),
    };
    VideoPoll {
        status,
        body,
        done,
        units,
        vendor_cost,
    }
}

/// The finished clip's bytes and content type, proxied for Sora and MiniMax;
/// other dialects answer 404 because their poll carries a URL.
pub async fn video_content(
    transport: &dyn Transport,
    account: &gw_models::Account,
    id: &str,
    poll: &Value,
) -> GResult<(u16, String, bytes::Bytes)> {
    let base = account.base_url(VENDOR_SENTINEL);
    let request = match video_dialect(&account.provider) {
        VideoDialect::Sora => video_request(
            account,
            "GET",
            versioned_url(base, &format!("videos/{id}/content")),
            Vec::new(),
        ),
        VideoDialect::Minimax => {
            // the clip lives on a CDN: files/retrieve resolves the file_id to a
            // signed download_url (retrieve_content rejects video_generation files)
            let file_id = poll["file_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(|| minimax_content_gap("minimax completed without file_id"))?;
            let (_, meta) = video_send(
                transport,
                account,
                "GET",
                versioned_url(base, &format!("files/retrieve?file_id={file_id}")),
                Vec::new(),
            )
            .await?;
            reject_minimax_error(&meta)?;
            let url = meta["file"]["download_url"]
                .as_str()
                .filter(|u| !u.is_empty())
                .ok_or_else(|| minimax_content_gap("minimax file without download_url"))?;
            UpstreamRequest {
                protocol: gw_consts::Protocol::Video,
                method: "GET",
                url: url.to_owned(),
                headers: Vec::new(),
                body: Vec::new(),
                stream: false,
                account: account.name.clone(),
                replay_account: None,
            }
        }
        _ => {
            return Err(GatewayError::new(
                gw_consts::ErrCode::BUILD_REQ,
                404,
                "this vendor serves the clip by URL in the poll, not by download",
            ));
        }
    };
    let reply = transport.send(request).await?.buffered().await?;
    let content_type = reply
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("video/mp4")
        .to_owned();
    let bytes = match reply.body {
        UpstreamBody::Json(b) => b,
        UpstreamBody::Sse(b) => b.into(),
        UpstreamBody::SseStream(_) => bytes::Bytes::new(),
    };
    Ok((reply.status, content_type, bytes))
}

fn minimax_content_gap(message: &'static str) -> GatewayError {
    GatewayError::new(gw_consts::ErrCode::FED_RESP_STATUS_NOT_ZERO, 502, message)
}

fn video_request(
    account: &gw_models::Account,
    method: &'static str,
    url: String,
    body: Vec<u8>,
) -> UpstreamRequest {
    let key = account.api_key().unwrap_or_else(|| "mock".to_owned());
    let mut headers = vec![("authorization", format!("Bearer {key}"))];
    if !body.is_empty() {
        headers.push(("content-type", "application/json".into()));
    }
    UpstreamRequest {
        protocol: gw_consts::Protocol::Video,
        method,
        url,
        headers,
        body,
        stream: false,
        account: account.name.clone(),
        replay_account: None,
    }
}

async fn video_send(
    transport: &dyn Transport,
    account: &gw_models::Account,
    method: &'static str,
    url: String,
    body: Vec<u8>,
) -> GResult<(u16, Value)> {
    let reply = transport
        .send(video_request(account, method, url, body))
        .await?;
    parse_json_reply(reply)
}

base_engine!(SearchEngine);

#[async_trait::async_trait]
impl ModelEngine for SearchEngine {
    /// Merges the bingsearch/brave/serp/google_custom_search engines.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let (query, count) = match &param.typed {
            Some(TypedParams::Search(p)) => (p.query.as_str(), p.count),
            _ => (self.base.last_message_text(), 3),
        };
        require_non_empty(query, "search query")?;
        let body = json!({"query": query, "count": count});
        let (status, v) = self
            .base
            .round_trip(&self.base.vendor_url("search"), body)
            .await?;
        let titles: Vec<String> = v["results"]
            .as_array()
            .map(|rs| {
                rs.iter()
                    .filter_map(|r| r["title"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok(family_outcome(
            titles.join("; "),
            &param.model_name,
            v,
            status,
        ))
    }
}

base_engine!(ModerationsEngine);

#[async_trait::async_trait]
impl ModelEngine for ModerationsEngine {
    /// OpenAI moderations shape: `{model, input: [..]}` → per-input verdicts.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let Some(TypedParams::Moderation(p)) = self.base.take_typed() else {
            return Err(GatewayError::bad_request("moderations params are required"));
        };
        if p.input.is_empty() {
            return Err(GatewayError::bad_request(
                "moderations input must not be empty",
            ));
        }
        let model = self.base.model_name()?;
        let input = Value::Array(p.input.into_iter().map(Value::String).collect());
        let body = object([("model", model.into()), ("input", input)]);
        let (status, v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "moderations"),
                body,
            )
            .await?;
        let flagged = v["results"]
            .as_array()
            .map(|rs| {
                rs.iter()
                    .filter(|r| r["flagged"].as_bool().unwrap_or(false))
                    .count()
            })
            .unwrap_or(0);
        Ok(family_outcome(
            format!("{flagged} flagged"),
            model,
            v,
            status,
        ))
    }
}

base_engine!(RerankEngine);

#[async_trait::async_trait]
impl ModelEngine for RerankEngine {
    /// Cohere/Jina-compatible rerank: `{model, query, documents, top_n?}` →
    /// `{results: [{index, relevance_score}]}`.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        let Some(TypedParams::Rerank(p)) = self.base.take_typed() else {
            return Err(GatewayError::bad_request("rerank params are required"));
        };
        require_non_empty(&p.query, "rerank query")?;
        if p.documents.is_empty() {
            return Err(GatewayError::bad_request(
                "rerank documents must not be empty",
            ));
        }
        // the document set moves — json! would re-copy every string
        let mut body = json!({"model": model});
        body["query"] = p.query.into();
        body["documents"] = Value::Array(p.documents.into_iter().map(Value::String).collect());
        if let Some(n) = p.top_n {
            body["top_n"] = json!(n);
        }
        let (status, v) = self
            .base
            .round_trip(&self.base.vendor_url("rerank"), body)
            .await?;
        let n = v["results"].as_array().map(Vec::len).unwrap_or(0);
        let tokens = rerank_tokens(&v);
        let units = crate::engine::tok(&v["meta"]["billed_units"]["search_units"]);
        let mut out = family_outcome(format!("{n} results"), &model, v, status);
        out.response.prompt_tokens = tokens;
        out.response.total_tokens = tokens;
        out.response.billed_units = units;
        Ok(out)
    }
}

/// Rerank usage across the two wire dialects: Jina-style `usage.total_tokens`,
/// else Cohere/SiliconFlow-style `meta.tokens.{input,output}_tokens`.
fn rerank_tokens(v: &Value) -> i64 {
    let total = crate::engine::tok(&v["usage"]["total_tokens"]);
    if total > 0 {
        return total;
    }
    crate::engine::tok(&v["meta"]["tokens"]["input_tokens"])
        .saturating_add(crate::engine::tok(&v["meta"]["tokens"]["output_tokens"]))
}

base_engine!(PassthroughEngine);

#[async_trait::async_trait]
impl ModelEngine for PassthroughEngine {
    /// Dedicated integration surfaces: request body passed through as-is,
    /// placeholder protocol (byte-level alignment deferred).
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let model = self.base.model_name()?.to_owned();
        // the arbitrary vendor blob moves — json! would re-copy it whole
        let mut body = json!({"model": model});
        body["payload"] = self.base.take_raw();
        let (status, v) = self
            .base
            .round_trip(&self.base.vendor_url("passthrough"), body)
            .await?;
        let message = if v["ok"].as_bool().unwrap_or(false) {
            "ok"
        } else {
            "error"
        };
        Ok(family_outcome(message.to_owned(), &model, v, status))
    }
}

base_engine!(CompletionsEngine);

#[async_trait::async_trait]
impl ModelEngine for CompletionsEngine {
    /// The legacy openai text-completions endpoint: `{model, prompt}` request
    /// (not chat messages), `{choices:[{text}]}` response.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        let param = self.base.param()?;
        let prompt: String = self
            .base
            .request
            .message
            .iter()
            .map(|m| m.content.as_str())
            .collect();
        let mut body = json!({"model": param.model_name});
        body["prompt"] = prompt.into();
        if let Some(p) = self.base.chat_params() {
            if let Some(mt) = p.max_tokens {
                body["max_tokens"] = json!(mt);
            }
            if let Some(t) = p.temperature {
                body["temperature"] = json!(t);
            }
        }
        let (status, mut v) = self
            .base
            .round_trip(
                &self.base.openai_url("mock://api.openai.com", "completions"),
                body,
            )
            .await?;
        let usage = &v["usage"];
        let (pt, ct) = (
            crate::engine::tok(&usage["prompt_tokens"]),
            crate::engine::tok(&usage["completion_tokens"]),
        );
        // floor a present-but-negative upstream total too, not just the sum
        let total = usage["total_tokens"]
            .as_i64()
            .unwrap_or(pt.saturating_add(ct))
            .max(0);
        let raw_usage = v.get_mut("usage").map(Value::take).filter(|u| !u.is_null());
        let resp = GatewayResponse {
            message: crate::engine::take_string(&mut v, "/choices/0/text").unwrap_or_default(),
            model: crate::engine::take_string(&mut v, "/model")
                .unwrap_or_else(|| param.model_name.clone()),
            finish_reason: crate::engine::take_string(&mut v, "/choices/0/finish_reason")
                .unwrap_or_else(|| "stop".to_owned()),
            prompt_tokens: pt,
            completion_tokens: ct,
            total_tokens: total,
            raw_usage,
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }
}

base_engine!(ResponsesEngine);

impl ResponsesEngine {
    fn model_name(&self) -> String {
        self.base.model_name().unwrap_or_default().to_owned()
    }

    /// The native body as sent, or one built from the normalized turns off the native surface.
    fn build_body(&mut self) -> GResult<Value> {
        let mut body = match self.base.take_raw() {
            Value::Object(raw) => raw,
            _ => serde_json::Map::new(),
        };
        if !self.base.request.preserve_responses_wire {
            self.cross_protocol_body(&mut body);
        }
        body.insert("model".to_owned(), self.base.model_name()?.into());
        Ok(Value::Object(body))
    }

    /// A Responses body from the chat/messages turns and the typed params.
    fn cross_protocol_body(&mut self, body: &mut serde_json::Map<String, Value>) {
        let system = self.base.system_text();
        if !system.is_empty() {
            body.entry("instructions").or_insert(system.into());
        }
        let messages = std::mem::take(&mut self.base.request.message);
        let mut input = Vec::with_capacity(messages.len());
        for m in messages {
            if m.role == gw_consts::role::SYSTEM {
                continue;
            }
            if m.role == gw_consts::role::TOOL {
                input.push(function_call_output(
                    m.tool_call_id.unwrap_or_default().into(),
                    m.content,
                ));
                continue;
            }
            let role = if m.role == gw_consts::role::AI {
                "assistant"
            } else {
                "user"
            };
            let mut calls = Vec::new();
            if let Some(Value::Array(blocks)) = m.parts {
                for mut block in blocks {
                    match block["type"].as_str() {
                        Some("tool_use") => calls.push(function_call(
                            block["id"].take(),
                            block["name"].take(),
                            block["input"].take().to_string().into(),
                        )),
                        Some("tool_result") => input.push(function_call_output(
                            block["tool_use_id"].take(),
                            match block["content"].take() {
                                Value::String(s) => s,
                                Value::Array(parts) => gw_protocol::anthropic::blocks_text(&parts),
                                _ => String::new(),
                            },
                        )),
                        _ => {}
                    }
                }
            }
            if let Some(Value::Array(tool_calls)) = m.tool_calls {
                calls.extend(tool_calls.into_iter().map(|mut c| {
                    function_call(
                        c["id"].take(),
                        c["function"]["name"].take(),
                        c["function"]["arguments"].take(),
                    )
                }));
            }
            if !m.content.is_empty() {
                input.push(object([
                    ("role", role.into()),
                    ("content", m.content.into()),
                ]));
            }
            input.extend(calls);
        }
        body.insert("input".to_owned(), Value::Array(input));
        body.insert("stream".to_owned(), self.base.request.stream.into());
        if let Some(gw_models::TypedParams::Chat(p)) = self.base.take_typed() {
            if let Some(v) = p.max_tokens {
                body.insert("max_output_tokens".to_owned(), v.into());
            }
            if let Some(v) = p.temperature {
                body.insert("temperature".to_owned(), json!(v));
            }
            if let Some(v) = p.top_p {
                body.insert("top_p".to_owned(), json!(v));
            }
            if let Some(Value::Array(tools)) = p.tools {
                body.insert(
                    "tools".to_owned(),
                    Value::Array(tools.into_iter().map(responses_tool).collect()),
                );
            }
            if let Some(v) = p.tool_choice {
                body.insert("tool_choice".to_owned(), v);
            }
            if let Some(effort) = p.reasoning.and_then(|r| r.effort) {
                body.insert("reasoning".to_owned(), object([("effort", effort.into())]));
            }
        }
    }

    fn url(&self) -> String {
        self.base.openai_url("mock://api.openai.com", "responses")
    }

    /// Streaming Responses pumped live: delta frames forwarded through
    /// `stream_tx` as they arrive; `response.completed` carries final usage.
    async fn run_stream(&mut self) -> GResult<EngineOutcome> {
        let body = self.build_body()?;
        let reply = self
            .base
            .send_upstream_raw(&self.url(), self.base.bearer_headers(), body, true)
            .await?;
        let status = reply.status;
        let mut resp = GatewayResponse {
            model: self.model_name(),
            finish_reason: "completed".to_owned(),
            ..Default::default()
        };
        crate::pump::reject_json_error("responses", status, &reply.body)?;
        let mut full = String::new();
        let model_override = self.base.model_override();
        let native = self.base.request.preserve_responses_wire;
        let r = crate::pump::pump_sse(
            "responses",
            reply.body,
            self.base.request.stream_tx.clone(),
            |v| responses_apply_frame(v, status, model_override, native, &mut resp, &mut full),
        )
        .await?;
        resp.message = full;
        Ok(EngineOutcome::from_pump(resp, status, r))
    }

    /// Non-streaming Responses reply: full `output` array + `usage`.
    fn parse_json(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let mut v: Value = serde_json::from_slice(bytes)
            .map_err(|e| GatewayError::internal("parse responses reply").with_source(e))?;
        if let Some(err) = crate::engine::vendor_error(status, &v) {
            return Err(err);
        }
        // the verbatim body must not leak the served variant either
        if let Some(requested) = self.base.model_override()
            && let Some(body) = v.as_object_mut()
            && body.contains_key("model")
        {
            body.insert("model".to_owned(), requested.into());
        }
        let native = self.base.request.preserve_responses_wire;
        let (text, mut tool_calls) = responses_output(&v);
        let finish_reason = if native {
            v["status"].as_str().unwrap_or("completed").to_owned()
        } else {
            responses_finish(&v, !tool_calls.is_empty())
        };
        if !native {
            tool_calls = tool_calls
                .into_iter()
                .map(function_call_to_tool_call)
                .collect();
        }
        let (input, output, common_usage) = responses_usage(&v["usage"]);
        let resp = GatewayResponse {
            message: text,
            tool_calls: if tool_calls.is_empty() {
                None
            } else {
                Some(Value::Array(tool_calls))
            },
            model: v["model"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| self.model_name()),
            finish_reason,
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: input.saturating_add(output),
            common_usage,
            response_v2: Some(v),
            ..Default::default()
        };
        Ok(EngineOutcome::with_status(resp, status))
    }

    /// Buffered Responses SSE: the same [`responses_apply_frame`] semantics the
    /// live pump drives, over pre-decoded events.
    fn parse_sse(&self, status: u16, bytes: &[u8]) -> GResult<EngineOutcome> {
        let (events, _done) = SseDecoder::decode_all(bytes)
            .map_err(|e| GatewayError::internal(format!("decode responses sse body: {e}")))?;
        let mut resp = GatewayResponse {
            model: self.model_name(),
            finish_reason: "completed".to_owned(),
            ..Default::default()
        };
        let mut full = String::new();
        let mut chunks = Vec::new();
        let model_override = self.base.model_override();
        let native = self.base.request.preserve_responses_wire;
        for ev in events {
            let v: Value = serde_json::from_slice(ev.as_bytes())
                .map_err(|e| GatewayError::internal("parse responses sse frame").with_source(e))?;
            chunks.extend(responses_apply_frame(
                v,
                status,
                model_override,
                native,
                &mut resp,
                &mut full,
            )?);
        }
        resp.message = full;
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
    /// OpenAI Responses API (POST /v1/responses): native body passthrough with
    /// the `model` field ensured; usage normalized to the openai shape.
    async fn run(&mut self) -> GResult<EngineOutcome> {
        if self.base.request.stream {
            return self.run_stream().await;
        }
        let body = self.build_body()?;
        let reply = self
            .base
            .send_upstream(&self.url(), self.base.bearer_headers(), body, false)
            .await?;
        match &reply.body {
            UpstreamBody::Json(b) => self.parse_json(reply.status, b),
            UpstreamBody::Sse(b) => self.parse_sse(reply.status, b),
            UpstreamBody::SseStream(_) => Err(GatewayError::internal(
                "unbuffered stream reached responses engine",
            )),
        }
    }
}

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

/// A Chat- or Anthropic-shaped tool definition flattened into the Responses shape.
fn responses_tool(mut tool: Value) -> Value {
    if let Some(Value::Object(mut f)) = tool.get_mut("function").map(Value::take) {
        f.insert("type".to_owned(), "function".into());
        return Value::Object(f);
    }
    if let Value::Object(fields) = &mut tool
        && let Some(schema) = fields.remove("input_schema")
    {
        fields.insert("type".to_owned(), "function".into());
        fields.insert("parameters".to_owned(), schema);
    }
    tool
}

fn function_call(call_id: Value, name: Value, arguments: Value) -> Value {
    object([
        ("type", "function_call".into()),
        ("call_id", call_id),
        ("name", name),
        ("arguments", arguments),
    ])
}

fn function_call_output(call_id: Value, output: String) -> Value {
    object([
        ("type", "function_call_output".into()),
        ("call_id", call_id),
        ("output", output.into()),
    ])
}

/// A Responses `status`/`incomplete_details.reason` in the shared finish vocabulary.
fn responses_finish(response: &Value, tool_calls: bool) -> String {
    let status = response["status"].as_str().unwrap_or("completed");
    match (status, response["incomplete_details"]["reason"].as_str()) {
        _ if tool_calls => "tool_calls",
        ("completed", _) => "stop",
        ("incomplete", Some("content_filter")) => "content_filter",
        ("incomplete", _) => "length",
        (other, _) => other,
    }
    .to_owned()
}

fn function_call_to_tool_call(mut item: Value) -> Value {
    object([
        ("id", item["call_id"].take()),
        ("type", "function".into()),
        (
            "function",
            object([
                ("name", item["name"].take()),
                ("arguments", item["arguments"].take()),
            ]),
        ),
    ])
}

/// Normalize a Responses `usage` object; returns (input, output, common usage).
fn responses_usage(usage: &Value) -> (i64, i64, Option<gw_models::CommonUsage>) {
    if usage.is_null() {
        return (0, 0, None);
    }
    let input = crate::engine::tok(&usage["input_tokens"]);
    let output = crate::engine::tok(&usage["output_tokens"]);
    let common = gw_models::CommonUsage::from_openai_parts(
        input,
        output,
        crate::engine::tok(&usage["input_tokens_details"]["cached_tokens"]),
        crate::engine::tok(&usage["output_tokens_details"]["reasoning_tokens"]),
    );
    (input, output, Some(common))
}

/// Apply one Responses SSE frame: a native event natively, plain chunk fields elsewhere.
fn responses_apply_frame(
    mut v: Value,
    status: u16,
    model_override: Option<&str>,
    native: bool,
    resp: &mut GatewayResponse,
    full: &mut String,
) -> GResult<Vec<StreamChunk>> {
    if let Some(err) = crate::engine::vendor_error(status, &v) {
        return Err(err);
    }
    if let Some(model) = model_override
        && let Some(response) = v.get_mut("response").and_then(Value::as_object_mut)
        && response.contains_key("model")
    {
        response.insert("model".to_owned(), model.into());
    }
    let mut chunk = StreamChunk::default();
    match v["type"].as_str().unwrap_or_default() {
        "response.output_text.delta" if native => {
            full.push_str(v["delta"].as_str().unwrap_or_default());
        }
        "response.output_text.delta" => {
            if let Some(Value::String(d)) = v.get_mut("delta").map(Value::take) {
                full.push_str(&d);
                chunk.delta = d;
            }
        }
        "response.output_item.done" if !native => {
            let item = v["item"].take();
            if item["type"] == "function_call" {
                let mut call = function_call_to_tool_call(item);
                if let Value::Array(calls) = resp
                    .tool_calls
                    .get_or_insert_with(|| Value::Array(Vec::new()))
                {
                    call["index"] = calls.len().into();
                    calls.push(call.clone());
                }
                chunk.tool_calls = Some(Value::Array(vec![call]));
            }
        }
        "response.completed" | "response.incomplete" => {
            let r = &v["response"];
            if let Some(m) = r["model"].as_str() {
                resp.model = m.to_owned();
            }
            if let Some(st) = r["status"].as_str() {
                resp.finish_reason = if native {
                    st.to_owned()
                } else {
                    responses_finish(r, resp.tool_calls.is_some())
                };
            }
            let (input, output, common) = responses_usage(&r["usage"]);
            resp.prompt_tokens = input;
            resp.completion_tokens = output;
            crate::engine::fill_total_if_zero(resp);
            resp.common_usage = common;
            chunk.finish_reason = Some(resp.finish_reason.clone());
        }
        _ => {}
    }
    if native {
        chunk.native_event = Some(v);
    } else if chunk.delta.is_empty() && chunk.finish_reason.is_none() && chunk.tool_calls.is_none()
    {
        return Ok(Vec::new());
    }
    Ok(vec![chunk])
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use gw_consts::Protocol;
    use gw_models::{
        ChatMsg, EmbeddingParams, ImageParams, ModelParamV2, SearchParams, SttParams, TtsParams,
        VideoParams,
    };

    use super::*;
    use crate::transport::{MockTransport, UpstreamRequest, UpstreamResponse};

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
        let mut e = VertexEngine::new(req(Protocol::Gemini, "gemini-pro", None), t());
        let out = e.run().await.unwrap();
        assert!(out.response.message.contains("you said: hello families"));
        assert!(out.response.total_tokens > 0);
        assert_eq!(out.response.finish_reason, "stop");
    }

    #[tokio::test]
    async fn vertex_stream_decodes_frames() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.stream = true;
        let mut e = VertexEngine::new(r, t());
        let out = e.run().await.unwrap();
        assert!(out.chunks.len() >= 3, "chunks: {:?}", out.chunks);
        assert!(out.response.message.contains("you said: hello families"));
        assert_eq!(out.response.finish_reason, "stop");
        assert!(out.response.prompt_tokens > 0 && out.response.completion_tokens > 0);
        assert!(out.chunks.iter().any(|c| c.finish_reason.is_some()));
    }

    #[tokio::test]
    async fn embeddings_round_trip() {
        let mut e = EmbeddingsEngine::new(
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
        let dims = out.response.response_v2.as_ref().unwrap()["data"][0]["embedding"]
            .as_array()
            .unwrap()
            .len();
        assert_eq!(dims, 8);
        assert!(out.response.prompt_tokens > 0);
    }

    #[tokio::test]
    async fn image_round_trip() {
        let mut e = ImageEngine::new(
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
    async fn moderations_flags_and_validates() {
        let mut e = ModerationsEngine::new(
            req(
                Protocol::Moderations,
                "text-moderation",
                Some(TypedParams::Moderation(gw_models::ModerationParams {
                    input: vec!["fine".into(), "really unsafe".into()],
                })),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "1 flagged");
        assert_eq!(
            out.response.response_v2.unwrap()["results"][1]["flagged"],
            true
        );

        for typed in [
            None,
            Some(TypedParams::Moderation(gw_models::ModerationParams {
                input: vec![],
            })),
        ] {
            let mut e = ModerationsEngine::new(req(Protocol::Moderations, "m", typed), t());
            assert_eq!(e.run().await.unwrap_err().http_status, 400);
        }
    }

    #[tokio::test]
    async fn rerank_orders_and_validates() {
        let params = |query: &str, documents: Vec<&str>| {
            Some(TypedParams::Rerank(gw_models::RerankParams {
                query: query.into(),
                documents: documents.into_iter().map(str::to_owned).collect(),
                top_n: Some(1),
            }))
        };
        let mut e = RerankEngine::new(
            req(
                Protocol::Rerank,
                "rerank-mini",
                params("rust gateway", vec!["cooking", "a rust gateway"]),
            ),
            t(),
        );
        let out = e.run().await.unwrap();
        assert_eq!(out.response.message, "1 results");
        let v = out.response.response_v2.unwrap();
        assert_eq!(v["results"][0]["index"], 1, "the matching doc ranks first");
        assert!(out.response.total_tokens > 0, "usage flows from the vendor");

        for typed in [None, params("", vec!["doc"]), params("query", vec![])] {
            let mut e = RerankEngine::new(req(Protocol::Rerank, "m", typed), t());
            assert_eq!(e.run().await.unwrap_err().http_status, 400);
        }
    }

    #[tokio::test]
    async fn billed_units_come_from_search_units_seconds_and_input_characters() {
        let params = Some(TypedParams::Rerank(gw_models::RerankParams {
            query: "q".into(),
            documents: vec!["a".into(), "b".into()],
            top_n: None,
        }));
        let out = RerankEngine::new(
            req(Protocol::Rerank, "rerank-v3.5", params),
            Arc::new(BytesReply(
                br#"{"results":[{"index":0,"relevance_score":0.9}],"meta":{"billed_units":{"search_units":1}}}"#,
            )),
        )
        .run()
        .await
        .unwrap();
        assert_eq!(
            (out.response.billed_units, out.response.total_tokens),
            (1, 0)
        );

        let mut stt = AudioEngine::new(
            req(
                Protocol::Stt,
                "whisper-1",
                Some(TypedParams::AudioStt(SttParams {
                    audio_b64: "TU9DSw==".into(),
                    language: None,
                    translate: false,
                })),
            ),
            Arc::new(BytesReply(
                br#"{"text":"hi","usage":{"type":"duration","seconds":4}}"#,
            )),
            AudioKind::Stt,
        );
        assert_eq!(stt.run().await.unwrap().response.billed_units, 4);
        assert_eq!(whole_seconds(&json!(4.1)), Some(5));
        assert_eq!(whole_seconds(&json!("4")), Some(4));
        assert_eq!(whole_seconds(&json!("12.5")), Some(13));
        assert_eq!(whole_seconds(&json!("clip")), None);
        assert_eq!(whole_seconds(&json!(0)), None);

        let mut tts = AudioEngine::new(
            req(
                Protocol::Tts,
                "tts-1",
                Some(TypedParams::AudioTts(TtsParams {
                    input: "héllo wörld".into(),
                    voice: None,
                    response_format: None,
                })),
            ),
            t(),
            AudioKind::Tts,
        );
        assert_eq!(tts.run().await.unwrap().response.billed_units, 11);
    }

    #[test]
    fn rerank_tokens_reads_both_dialects() {
        assert_eq!(rerank_tokens(&json!({"usage": {"total_tokens": 7}})), 7);
        assert_eq!(
            rerank_tokens(&json!({"meta": {"tokens": {"input_tokens": 5, "output_tokens": 2}}})),
            7,
            "SiliconFlow/Cohere meta.tokens shape bills too"
        );
        assert_eq!(rerank_tokens(&json!({})), 0);
    }

    #[tokio::test]
    async fn audio_tts_and_stt() {
        let mut tts = AudioEngine::new(
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

        let mut stt = AudioEngine::new(
            req(
                Protocol::Stt,
                "whisper-mock",
                Some(TypedParams::AudioStt(SttParams {
                    audio_b64: "TU9DSw==".into(),
                    language: Some("en".into()),
                    translate: false,
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

    #[test]
    fn video_handle_prefers_the_task_id_over_a_trace_request_id() {
        for (reply, want) in [
            (
                json!({"request_id": "trace-1", "output": {"task_id": "ds-1", "task_status": "PENDING"}}),
                "ds-1",
            ),
            (json!({"request_id": "vid-xai-1"}), "vid-xai-1"),
            (json!({"requestId": "sf-1"}), "sf-1"),
            (
                json!({"task_id": "mm-1", "base_resp": {"status_code": 0}}),
                "mm-1",
            ),
            (
                json!({"id": "video_abc", "object": "video", "status": "queued"}),
                "video_abc",
            ),
        ] {
            assert_eq!(video_handle(&reply), Some(want), "{reply}");
        }
        assert_eq!(
            video_handle(&json!({"task_id": "", "video_url": "u"})),
            None
        );
    }

    #[tokio::test]
    async fn video_and_search_and_passthrough() {
        let mut v = VideoEngine::new(
            req(
                Protocol::Video,
                "kling-mock",
                Some(TypedParams::Video(VideoParams {
                    prompt: "a dog surfing".into(),
                    ..Default::default()
                })),
            ),
            t(),
        );
        let out = v.run().await.unwrap();
        assert_eq!(out.response.message, "mock://videos/out.mp4");
        assert_eq!(out.response.step, "succeeded");

        let mut s = SearchEngine::new(
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

        let mut p = PassthroughEngine::new(req(Protocol::Passthrough, "e2b", None), t());
        assert_eq!(p.run().await.unwrap().response.message, "ok");
    }

    #[tokio::test]
    async fn down_account_fails_upstream() {
        let mut r = req(Protocol::Gemini, "gemini-pro", None);
        r.account = Some(std::sync::Arc::new(gw_models::Account {
            name: "mock-vertex-down".into(),
            ..Default::default()
        }));
        let mut e = VertexEngine::new(r, t());
        let err = e.run().await.err().unwrap();
        assert_eq!(err.http_status, 503);
    }

    #[tokio::test]
    async fn responses_cross_protocol_body_comes_from_the_turns() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.stream = true;
        r.message = vec![
            ChatMsg::text("system", "be terse"),
            ChatMsg::text("user", "what time"),
            ChatMsg {
                tool_calls: Some(serde_json::json!([{"id": "call_1", "type": "function",
                    "function": {"name": "now", "arguments": "{}"}}])),
                ..ChatMsg::text("assistant", "")
            },
            ChatMsg {
                tool_call_id: Some("call_1".into()),
                ..ChatMsg::text("tool", "noon")
            },
        ];
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::Chat(gw_models::ChatParams {
            max_tokens: Some(64),
            tools: Some(serde_json::json!([
                {"type": "function", "function": {
                    "name": "now", "parameters": {"type": "object"}}},
                {"name": "later", "description": "schedule it",
                    "input_schema": {"type": "object"}},
            ])),
            reasoning: Some(Box::new(gw_models::ReasoningParam {
                effort: Some("low".into()),
                ..Default::default()
            })),
            ..Default::default()
        }));
        let body = ResponsesEngine::new(r, t()).build_body().unwrap();
        assert_eq!(body["instructions"], "be terse");
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_output_tokens"], 64);
        assert_eq!(body["reasoning"]["effort"], "low");
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["name"], "now");
        assert_eq!(
            body["tools"][1],
            serde_json::json!({"type": "function", "name": "later",
                "description": "schedule it", "parameters": {"type": "object"}})
        );
        assert_eq!(
            body["input"],
            serde_json::json!([
                {"role": "user", "content": "what time"},
                {"type": "function_call", "call_id": "call_1", "name": "now", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "noon"},
            ])
        );
        assert_eq!(
            function_call_to_tool_call(serde_json::json!({"type": "function_call",
                "call_id": "c", "name": "now", "arguments": "{}"})),
            serde_json::json!({"id": "c", "type": "function", "function": {"name": "now", "arguments": "{}"}})
        );
    }

    #[tokio::test]
    async fn responses_api_round_trip() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.preserve_responses_wire = true;
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
        let usage = out.response.common_usage.unwrap();
        assert!(usage.prompt_total() > 0 && usage.completion_total() > 0);
    }

    #[tokio::test]
    async fn responses_api_streaming() {
        let mut r = req(Protocol::Responses, "gpt-5-responses", None);
        r.stream = true;
        r.preserve_responses_wire = true;
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
        assert!(out.chunks.iter().all(|c| c.native_event.is_some()));
    }

    #[derive(Debug)]
    struct SseReply(&'static str);

    #[async_trait::async_trait]
    impl crate::transport::Transport for SseReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Sse(self.0.as_bytes().to_vec()),
                headers: Default::default(),
            })
        }
    }

    #[derive(Debug)]
    struct BytesReply(&'static [u8]);

    #[async_trait::async_trait]
    impl crate::transport::Transport for BytesReply {
        async fn send(&self, _req: UpstreamRequest) -> GResult<UpstreamResponse> {
            Ok(UpstreamResponse {
                status: 200,
                body: UpstreamBody::Json(self.0.to_vec().into()),
                headers: Default::default(),
            })
        }
    }

    #[tokio::test]
    async fn minimax_video_business_errors_surface_on_submit_and_poll() {
        let account = Arc::new(gw_models::Account {
            name: "minimax-1".into(),
            provider: "minimax".into(),
            endpoint: "https://api.minimax.io".into(),
            ..Default::default()
        });
        let transport = Arc::new(BytesReply(
            br#"{"base_resp":{"status_code":1008,"status_msg":"insufficient balance"}}"#,
        ));
        let mut request = req(
            Protocol::Video,
            "MiniMax-Hailuo-02",
            Some(TypedParams::Video(VideoParams {
                prompt: "a paper boat".into(),
                ..Default::default()
            })),
        );
        request.account = Some(Arc::clone(&account));
        let submit = VideoEngine::new(request, transport.clone())
            .run()
            .await
            .unwrap_err();
        assert_eq!(submit.http_status, 502);
        assert!(submit.message.contains("minimax base_resp 1008"));

        let poll = match video_poll(transport.as_ref(), account.as_ref(), "task-1").await {
            Ok(_) => panic!("MiniMax business error accepted as a successful poll"),
            Err(e) => e,
        };
        assert_eq!(poll.http_status, 502);
        assert!(poll.message.contains("minimax base_resp 1008"));
    }

    #[tokio::test]
    async fn tts_audio_bytes_become_the_b64_payload() {
        let mut r = req(Protocol::Tts, "gpt-4o-mini-tts", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::AudioTts(TtsParams {
            input: "hi".into(),
            voice: None,
            response_format: Some("mp3".into()),
        }));
        let out = AudioEngine::new(r, Arc::new(BytesReply(b"\xff\xfbaudio")), AudioKind::Tts)
            .run()
            .await
            .unwrap();
        assert_eq!(
            out.response.response_v2.unwrap()["audio_b64"],
            base64::engine::general_purpose::STANDARD.encode(b"\xff\xfbaudio")
        );
    }

    #[tokio::test]
    async fn image_and_transcription_usage_reach_the_response() {
        let mut r = req(Protocol::Image, "gpt-image-1", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::Image(ImageParams {
            prompt: "a circle".into(),
            n: 1,
            size: None,
            image: None,
            mask: None,
        }));
        let out = ImageEngine::new(
            r,
            Arc::new(BytesReply(
                br#"{"data":[{"b64_json":"AA=="}],"usage":{"input_tokens":15,"output_tokens":1056}}"#,
            )),
        )
        .run()
        .await
        .unwrap();
        assert_eq!(
            (
                out.response.prompt_tokens,
                out.response.completion_tokens,
                out.response.total_tokens
            ),
            (15, 1056, 1071)
        );

        let mut r = req(Protocol::Stt, "gpt-4o-mini-transcribe", None);
        r.model_param_v2.as_mut().unwrap().typed = Some(TypedParams::AudioStt(SttParams {
            audio_b64: "TU9DSw==".into(),
            language: None,
            translate: false,
        }));
        let out = AudioEngine::new(
            r,
            Arc::new(BytesReply(
                br#"{"text":"mock","usage":{"type":"tokens","input_tokens":32,"output_tokens":12}}"#,
            )),
            AudioKind::Stt,
        )
        .run()
        .await
        .unwrap();
        assert_eq!(out.response.message, "mock");
        assert_eq!(
            (out.response.prompt_tokens, out.response.completion_tokens),
            (32, 12)
        );
    }

    #[test]
    fn responses_body_uses_the_routed_model() {
        let mut request = req(Protocol::Responses, "gpt-5-served", None);
        let param = request.model_param_v2.as_mut().unwrap();
        param.raw = json!({"model": "gpt-5-public", "input": "go"});
        param.fallback_from = Some("gpt-5-public".to_owned());
        let mut engine = ResponsesEngine::new(request, Arc::new(MockTransport));
        assert_eq!(engine.build_body().unwrap()["model"], "gpt-5-served");
    }

    #[test]
    fn responses_reply_names_the_requested_model() {
        let mut request = req(Protocol::Responses, "gpt-5-served", None);
        let param = request.model_param_v2.as_mut().unwrap();
        param.raw = json!({"input": "go"});
        param.fallback_from = Some("gpt-5-public".to_owned());
        let engine = ResponsesEngine::new(request, Arc::new(MockTransport));
        let out = engine
            .parse_json(
                200,
                br#"{"id":"r","object":"response","model":"gpt-5-served-2026","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}"#,
            )
            .unwrap();
        assert_eq!(out.response.response_v2.unwrap()["model"], "gpt-5-public");
    }

    #[tokio::test]
    async fn responses_stream_forwards_reasoning_and_function_call_events_verbatim() {
        let sse = concat!(
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-served\",\"status\":\"in_progress\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[],\"encrypted_content\":\"opaque\"}}\n\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"output_index\":0,\"summary_index\":0,\"delta\":\"thinking about it\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_1\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"thinking about it\"}],\"encrypted_content\":\"opaque\"}}\n\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"now\",\"arguments\":\"\"}}\n\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":1,\"delta\":\"{}\"}\n\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"now\",\"arguments\":\"{}\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"output_index\":2,\"delta\":\"done\"}\n\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5-served\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":9,\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n",
        );
        let mut r = req(Protocol::Responses, "gpt-5-served", None);
        r.stream = true;
        r.preserve_responses_wire = true;
        let param = r.model_param_v2.as_mut().unwrap();
        param.raw = serde_json::json!({"input": "go"});
        param.fallback_from = Some("gpt-5-public".to_owned());
        let out = ResponsesEngine::new(r, Arc::new(SseReply(sse)))
            .run()
            .await
            .unwrap();
        let events: Vec<&Value> = out
            .chunks
            .iter()
            .filter_map(|c| c.native_event.as_ref())
            .collect();
        assert_eq!(events.len(), 9);
        assert_eq!(events[1]["item"]["encrypted_content"], "opaque");
        assert_eq!(events[2]["delta"], "thinking about it");
        assert_eq!(events[6]["item"]["arguments"], "{}");
        assert_eq!(events[0]["response"]["model"], "gpt-5-public");
        assert_eq!(events[8]["response"]["model"], "gpt-5-public");
        assert!(out.chunks.iter().all(|c| c.delta.is_empty()));
        assert_eq!(out.response.message, "done");
        assert_eq!(out.response.finish_reason, "completed");
        assert_eq!(out.response.common_usage.unwrap().reason, 4);
    }

    #[test]
    fn responses_cross_protocol_stream_finishes_with_indexed_tool_calls() {
        let mut resp = GatewayResponse::default();
        let mut full = String::new();
        let chunks = responses_apply_frame(
            json!({"type": "response.output_item.done", "item": {
                "type": "function_call", "call_id": "call_1", "name": "now", "arguments": "{}"}}),
            200,
            None,
            false,
            &mut resp,
            &mut full,
        )
        .unwrap();
        assert_eq!(chunks[0].tool_calls.as_ref().unwrap()[0]["index"], 0);

        let chunks = responses_apply_frame(
            json!({"type": "response.completed", "response": {
                "status": "completed", "usage": {"input_tokens": 5, "output_tokens": 2}}}),
            200,
            None,
            false,
            &mut resp,
            &mut full,
        )
        .unwrap();
        assert_eq!(chunks[0].finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(resp.finish_reason, "tool_calls");

        for (reason, want) in [
            ("max_output_tokens", "length"),
            ("content_filter", "content_filter"),
        ] {
            let mut resp = GatewayResponse::default();
            let chunks = responses_apply_frame(
                json!({"type": "response.incomplete", "response": {
                    "status": "incomplete", "incomplete_details": {"reason": reason},
                    "usage": {"input_tokens": 5, "output_tokens": 7}}}),
                200,
                None,
                false,
                &mut resp,
                &mut String::new(),
            )
            .unwrap();
            assert_eq!(chunks[0].finish_reason.as_deref(), Some(want), "{reason}");
            assert_eq!((resp.prompt_tokens, resp.completion_tokens), (5, 7));
        }
    }
}
