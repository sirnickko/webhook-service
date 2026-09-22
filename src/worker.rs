//! Queue consumers: claim -> process -> persist -> ack, with retries and backoff.

use std::{sync::Arc, time::Duration};

use sqlx::SqlitePool;
use tokio::sync::{watch, Notify};
use tracing::{error, info, warn};

use crate::{processor, store};

#[derive(Clone)]
pub struct WorkerCfg {
    pub lease: Duration,       // how long a claimed event is invisible to other workers
    pub poll: Duration,        // fallback poll when no wake-up arrives (also covers retries)
    pub max_attempts: i64,
    pub backoff_base: Duration,
    pub backoff_cap: Duration,
}

impl Default for WorkerCfg {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            poll: Duration::from_millis(500),
            max_attempts: 10,
            backoff_base: Duration::from_secs(2),
            backoff_cap: Duration::from_secs(15 * 60),
        }
    }
}

/// Exponential backoff: base * 2^(attempt-1), capped.
pub fn backoff(cfg: &WorkerCfg, attempt: i64) -> Duration {
    let exp = (attempt.max(1) - 1).min(20) as u32;
    cfg.backoff_base.saturating_mul(1u32 << exp).min(cfg.backoff_cap)
}

pub async fn run(
    id: usize,
    pool: SqlitePool,
    notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
    cfg: WorkerCfg,
) {
    info!(worker = id, "worker started");
    while !*shutdown.borrow() {
        match store::claim(&pool, cfg.lease).await {
            Ok(Some(ev)) => handle(&pool, &cfg, ev).await,
            Ok(None) => {
                tokio::select! {
                    _ = notify.notified() => {}
                    _ = tokio::time::sleep(cfg.poll) => {}
                    _ = shutdown.changed() => {}
                }
            }
            Err(e) => {
                error!(worker = id, error = %e, "claim failed");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    info!(worker = id, "worker stopped");
}

async fn handle(pool: &SqlitePool, cfg: &WorkerCfg, ev: store::Claimed) {
    let outcome: anyhow::Result<String> = async {
        let mut tx = pool.begin().await?;
        let result = processor::process(&mut tx, &ev).await?;
        store::mark_done(&mut tx, &ev.event_id, &result).await?; // ack
        tx.commit().await?;
        Ok(result)
    }
    .await;

    match outcome {
        Ok(result) => info!(event_id = %ev.event_id, attempts = ev.attempts, %result, "processed"),
        Err(e) => {
            let retry = (ev.attempts < cfg.max_attempts).then(|| backoff(cfg, ev.attempts));
            warn!(event_id = %ev.event_id, attempts = ev.attempts, error = %e, ?retry, "processing failed");
            if let Err(db_err) = store::mark_failed(pool, &ev.event_id, &e.to_string(), retry).await {
                // Lease will expire and the event will be picked up again.
                error!(event_id = %ev.event_id, error = %db_err, "could not record failure");
            }
        }
    }
}

/// 30-day retention, checked hourly.
pub async fn purge_loop(pool: SqlitePool, mut shutdown: watch::Receiver<bool>) {
    let keep = Duration::from_secs(30 * 24 * 3600);
    while !*shutdown.borrow() {
        match store::purge_older_than(&pool, keep).await {
            Ok(n) if n > 0 => info!(deleted = n, "purged old events"),
            Ok(_) => {}
            Err(e) => error!(error = %e, "purge failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(3600)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_then_caps() {
        let cfg = WorkerCfg::default();
        assert_eq!(backoff(&cfg, 1), Duration::from_secs(2));
        assert_eq!(backoff(&cfg, 2), Duration::from_secs(4));
        assert_eq!(backoff(&cfg, 3), Duration::from_secs(8));
        assert_eq!(backoff(&cfg, 30), cfg.backoff_cap);
    }
}
