//! In-process benchmark against the composed router: serial latency
//! distribution + concurrent throughput. `#[ignore]`d — run with:
//!   cargo test --release -p gw-server --test bench -- --ignored --nocapture
//! Numbers reflect this implementation only (no external baseline).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

mod common;
use common::app;

fn chat_req() -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-bench")
        .body(Body::from(
            r#"{"model":"gpt-4o","messages":[{"role":"user","content":"benchmark round"}]}"#,
        ))
        .expect("request")
}

fn big_chat_req() -> Request<Body> {
    let turn = "x".repeat(2000);
    let msgs: Vec<String> = (0..24)
        .map(|i| {
            format!(
                r#"{{"role":"{}","content":"{turn}"}}"#,
                if i % 2 == 0 { "user" } else { "assistant" }
            )
        })
        .collect();
    let body = format!(r#"{{"model":"gpt-4o","messages":[{}]}}"#, msgs.join(","));
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("content-type", "application/json")
        .header("authorization", "Bearer ak-bench")
        .body(Body::from(body))
        .expect("request")
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run with --ignored --nocapture"]
async fn bench_request_clone() {
    let turn = "x".repeat(2000);
    let message: Vec<gw_models::ChatMsg> = (0..24)
        .map(|i| {
            gw_models::ChatMsg::text(if i % 2 == 0 { "user" } else { "assistant" }, turn.clone())
        })
        .collect();
    for (label, raw_kb) in [
        ("48KB msgs, no raw", 0usize),
        ("48KB msgs + 100KB raw", 100),
    ] {
        let mut param =
            gw_models::ModelParamV2::with_name(gw_consts::Protocol::OpenaiChat, "gpt-4o");
        if raw_kb > 0 {
            param.raw = serde_json::json!({"input": "y".repeat(raw_kb * 1000)});
        }
        let req = gw_models::GatewayRequest {
            is_online: true,
            message: message.clone(),
            model_param_v2: Some(param),
            ..Default::default()
        };
        const N: u32 = 10_000;
        let t = Instant::now();
        for _ in 0..N {
            std::hint::black_box(req.clone());
        }
        println!("clone [{label}]: {:?}/clone", t.elapsed() / N);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run with --ignored --nocapture"]
async fn bench_big_payload() {
    let app = app();
    for _ in 0..20 {
        app.clone().oneshot(big_chat_req()).await.unwrap();
    }
    const N: usize = 2000;
    let mut lat_us = Vec::with_capacity(N);
    for _ in 0..N {
        let t = Instant::now();
        let resp = app.clone().oneshot(big_chat_req()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        lat_us.push(t.elapsed().as_micros() as u64);
    }
    lat_us.sort_unstable();
    let pct = |p: f64| lat_us[((lat_us.len() as f64 * p) as usize).min(lat_us.len() - 1)];
    println!(
        "big-payload serial: n={N} p50={}us p95={}us p99={}us",
        pct(0.50),
        pct(0.95),
        pct(0.99),
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run with --ignored --nocapture"]
async fn bench_chat_completions() {
    let app = app();

    for _ in 0..50 {
        let resp = app.clone().oneshot(chat_req()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    const N: usize = 2000;
    let mut lat_us = Vec::with_capacity(N);
    let t0 = Instant::now();
    for _ in 0..N {
        let t = Instant::now();
        let resp = app.clone().oneshot(chat_req()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        lat_us.push(t.elapsed().as_micros() as u64);
    }
    let serial_total = t0.elapsed();
    lat_us.sort_unstable();
    let pct = |p: f64| lat_us[((lat_us.len() as f64 * p) as usize).min(lat_us.len() - 1)];
    println!(
        "serial: n={N} total={serial_total:?} rps={:.0} p50={}us p95={}us p99={}us max={}us",
        N as f64 / serial_total.as_secs_f64(),
        pct(0.50),
        pct(0.95),
        pct(0.99),
        lat_us[lat_us.len() - 1],
    );

    const WORKERS: usize = 64;
    const PER: usize = 50;
    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..PER {
                let resp = app.clone().oneshot(chat_req()).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let dur = t0.elapsed();
    let total = WORKERS * PER;
    println!(
        "concurrent: workers={WORKERS} n={total} total={dur:?} rps={:.0}",
        total as f64 / dur.as_secs_f64()
    );
}
