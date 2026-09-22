//! Persistence + durable queue.
//!
//! The `events` table is both the audit log and the queue (transactional-outbox style):
//! a row is inserted as `pending` before we ack the provider, workers claim rows with a
//! lease, and a row only becomes `done` in the same transaction as its side effects.
//! To move to Kafka/SQS/RabbitMQ later, replace `enqueue` and `claim` and keep the rest.

use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Row, Sqlite, SqlitePool, Transaction,
};

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,               -- provider's id = idempotency key
    event_type      TEXT NOT NULL,
    event_ts        INTEGER NOT NULL,               -- provider timestamp (unix seconds)
    payload         TEXT NOT NULL,                  -- raw body exactly as received
    status          TEXT NOT NULL DEFAULT 'pending',-- pending | processing | done | dead
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,               -- unix ms
    locked_until    INTEGER NOT NULL DEFAULT 0,     -- unix ms (lease)
    result          TEXT,
    last_error      TEXT,
    received_at     INTEGER NOT NULL,
    processed_at    INTEGER
);
CREATE INDEX IF NOT EXISTS idx_events_ready ON events (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS invoices (
    invoice_id    TEXT PRIMARY KEY,
    status        TEXT NOT NULL,
    last_event_ts INTEGER NOT NULL
);
"#;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

pub async fn connect(url: &str) -> anyhow::Result<SqlitePool> {
    let opts = SqliteConnectOptions::from_str(url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        // FULL = fsync on every commit, so "we returned 200" really means "it's on disk".
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new().max_connections(8).connect_with(opts).await?;
    sqlx::raw_sql(SCHEMA).execute(&pool).await?;
    Ok(pool)
}

pub struct NewEvent {
    pub event_id: String,
    pub event_type: String,
    pub event_ts: i64,
    pub payload: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Enqueued {
    New,
    Duplicate,
}

/// Durably store the event. A repeated `event_id` is a no-op (idempotency key).
pub async fn enqueue(pool: &SqlitePool, ev: &NewEvent) -> Result<Enqueued, sqlx::Error> {
    let now = now_ms();
    let res = sqlx::query(
        "INSERT INTO events (event_id, event_type, event_ts, payload, next_attempt_at, received_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(event_id) DO NOTHING",
    )
    .bind(&ev.event_id)
    .bind(&ev.event_type)
    .bind(ev.event_ts)
    .bind(&ev.payload)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(if res.rows_affected() == 1 { Enqueued::New } else { Enqueued::Duplicate })
}

#[derive(Debug)]
pub struct Claimed {
    pub event_id: String,
    pub event_type: String,
    pub event_ts: i64,
    pub payload: String,
    pub attempts: i64,
}

/// Atomically lease the next ready event. Also picks up events whose lease expired,
/// which is how work from a crashed worker gets retried.
pub async fn claim(pool: &SqlitePool, lease: Duration) -> Result<Option<Claimed>, sqlx::Error> {
    let now = now_ms();
    let row = sqlx::query(
        "UPDATE events
            SET status = 'processing', attempts = attempts + 1, locked_until = ?1
          WHERE event_id = (
                SELECT event_id FROM events
                 WHERE (status = 'pending'    AND next_attempt_at <= ?2)
                    OR (status = 'processing' AND locked_until    <= ?2)
                 ORDER BY next_attempt_at
                 LIMIT 1)
      RETURNING event_id, event_type, event_ts, payload, attempts",
    )
    .bind(now + lease.as_millis() as i64)
    .bind(now)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| Claimed {
        event_id: r.get("event_id"),
        event_type: r.get("event_type"),
        event_ts: r.get("event_ts"),
        payload: r.get("payload"),
        attempts: r.get("attempts"),
    }))
}

/// "Ack": runs inside the same transaction as the event's side effects.
pub async fn mark_done(
    tx: &mut Transaction<'_, Sqlite>,
    event_id: &str,
    result: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE events
            SET status = 'done', result = ?2, last_error = NULL, processed_at = ?3, locked_until = 0
          WHERE event_id = ?1",
    )
    .bind(event_id)
    .bind(result)
    .bind(now_ms())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Schedule a retry, or park the event as `dead` once attempts are exhausted.
pub async fn mark_failed(
    pool: &SqlitePool,
    event_id: &str,
    error: &str,
    retry_in: Option<Duration>,
) -> Result<(), sqlx::Error> {
    let now = now_ms();
    let (status, next) = match retry_in {
        Some(d) => ("pending", now + d.as_millis() as i64),
        None => ("dead", now),
    };
    sqlx::query(
        "UPDATE events SET status = ?2, next_attempt_at = ?3, last_error = ?4, locked_until = 0
          WHERE event_id = ?1",
    )
    .bind(event_id)
    .bind(status)
    .bind(next)
    .bind(error)
    .execute(pool)
    .await?;
    Ok(())
}

/// Retention: drop finished events (and therefore their idempotency keys) after `keep`.
pub async fn purge_older_than(pool: &SqlitePool, keep: Duration) -> Result<u64, sqlx::Error> {
    let cutoff = now_ms() - keep.as_millis() as i64;
    let res = sqlx::query(
        "DELETE FROM events WHERE status IN ('done', 'dead') AND received_at < ?1",
    )
    .bind(cutoff)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}
