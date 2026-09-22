//! HTTP edge: authenticate, validate, durably enqueue, ack. No business logic here.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Router,
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use sha2::Sha256;
use sqlx::SqlitePool;
use tokio::sync::Notify;
use tracing::{error, warn};

use crate::store::{self, Enqueued, NewEvent};

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub secret: Arc<Vec<u8>>,
    pub notify: Arc<Notify>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/webhook", post(receive))
        .route("/healthz", get(|| async { "ok" }))
        // Events are ~5 KB; anything much bigger is not for us.
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

/// Only the envelope is parsed here; the full body is stored verbatim.
#[derive(Deserialize)]
struct Envelope {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    created: i64,
}

async fn receive(State(s): State<AppState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    // 1. Authenticate over the *raw* bytes (re-serialised JSON would not match).
    let signature = headers.get("x-signature").and_then(|v| v.to_str().ok());
    if !signature.is_some_and(|sig| verify_signature(&s.secret, &body, sig)) {
        warn!("rejected webhook: bad or missing signature");
        return StatusCode::UNAUTHORIZED;
    }

    // 2. Validate the envelope.
    let Ok(env) = serde_json::from_slice::<Envelope>(&body) else {
        return StatusCode::BAD_REQUEST;
    };
    let Ok(payload) = String::from_utf8(body.to_vec()) else {
        return StatusCode::BAD_REQUEST;
    };

    // 3. Persist first, then ack. If this fails we return 5xx and the provider retries.
    let ev = NewEvent { event_id: env.id, event_type: env.event_type, event_ts: env.created, payload };
    match store::enqueue(&s.pool, &ev).await {
        Ok(Enqueued::New) => {
            s.notify.notify_one(); // wake a worker immediately (keeps latency low)
            StatusCode::OK
        }
        // Already have it: still 200 so the provider stops retrying.
        Ok(Enqueued::Duplicate) => StatusCode::OK,
        Err(e) => {
            error!(error = %e, "failed to enqueue event");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

/// Expects `sha256=<hex hmac of body>`. `verify_slice` compares in constant time.
pub fn verify_signature(secret: &[u8], body: &[u8], header: &str) -> bool {
    let Some(hex_sig) = header.strip_prefix("sha256=") else { return false };
    let Ok(sig) = hex::decode(hex_sig) else { return false };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn accepts_valid_signature() {
        let sig = sign(b"secret", b"{\"a\":1}");
        assert!(verify_signature(b"secret", b"{\"a\":1}", &sig));
    }

    #[test]
    fn rejects_tampered_body_wrong_key_and_garbage() {
        let sig = sign(b"secret", b"{\"a\":1}");
        assert!(!verify_signature(b"secret", b"{\"a\":2}", &sig));
        assert!(!verify_signature(b"other", b"{\"a\":1}", &sig));
        assert!(!verify_signature(b"secret", b"{\"a\":1}", "sha256=zz"));
        assert!(!verify_signature(b"secret", b"{\"a\":1}", "nope"));
    }
}
