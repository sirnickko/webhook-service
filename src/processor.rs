//! Business logic. Must be idempotent and must not assume event order.

use anyhow::Context;
use serde::Deserialize;
use sqlx::{Sqlite, Transaction};

use crate::store::Claimed;

#[derive(Deserialize)]
struct Envelope<T> {
    data: T,
}

#[derive(Deserialize)]
struct InvoiceData {
    invoice_id: String,
}

/// Runs inside the transaction that will also mark the event done, so the side effect
/// and the ack commit (or roll back) together. Returns a short result string for the audit log.
pub async fn process(tx: &mut Transaction<'_, Sqlite>, ev: &Claimed) -> anyhow::Result<String> {
    match ev.event_type.as_str() {
        "invoice.created" | "invoice.paid" => {
            let env: Envelope<InvoiceData> =
                serde_json::from_str(&ev.payload).context("malformed invoice payload")?;
            let id = env.data.invoice_id;
            let status = if ev.event_type == "invoice.paid" { "paid" } else { "open" };

            // Out-of-order safe: only apply an event newer than what we already hold.
            // (For stricter needs, re-fetch the object from the provider's API instead.)
            let res = sqlx::query(
                "INSERT INTO invoices (invoice_id, status, last_event_ts) VALUES (?1, ?2, ?3)
                 ON CONFLICT(invoice_id) DO UPDATE
                    SET status = excluded.status, last_event_ts = excluded.last_event_ts
                  WHERE excluded.last_event_ts > invoices.last_event_ts",
            )
            .bind(&id)
            .bind(status)
            .bind(ev.event_ts)
            .execute(&mut **tx)
            .await?;

            Ok(if res.rows_affected() == 0 {
                format!("skipped stale {} for {id}", ev.event_type)
            } else {
                format!("invoice {id} -> {status}")
            })
        }
        other => Ok(format!("ignored: no handler for '{other}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{self, Enqueued, NewEvent};
    use sqlx::Row;
    use std::time::Duration;

    async fn temp_pool() -> sqlx::SqlitePool {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static N: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "wh-test-{}-{}.db",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_file(&path);
        store::connect(&format!("sqlite://{}", path.display())).await.unwrap()
    }

    fn ev(id: &str, ty: &str, ts: i64) -> NewEvent {
        NewEvent {
            event_id: id.into(),
            event_type: ty.into(),
            event_ts: ts,
            payload: format!(r#"{{"id":"{id}","type":"{ty}","created":{ts},"data":{{"invoice_id":"inv_1"}}}}"#),
        }
    }

    async fn drain(pool: &sqlx::SqlitePool) {
        while let Some(c) = store::claim(pool, Duration::from_secs(30)).await.unwrap() {
            let mut tx = pool.begin().await.unwrap();
            let r = process(&mut tx, &c).await.unwrap();
            store::mark_done(&mut tx, &c.event_id, &r).await.unwrap();
            tx.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn duplicate_event_ids_are_stored_once() {
        let pool = temp_pool().await;
        assert_eq!(store::enqueue(&pool, &ev("e1", "invoice.created", 100)).await.unwrap(), Enqueued::New);
        assert_eq!(store::enqueue(&pool, &ev("e1", "invoice.created", 100)).await.unwrap(), Enqueued::Duplicate);
    }

    #[tokio::test]
    async fn out_of_order_events_converge() {
        let pool = temp_pool().await;
        // `paid` (ts 200) arrives before `created` (ts 100).
        store::enqueue(&pool, &ev("e2", "invoice.paid", 200)).await.unwrap();
        drain(&pool).await;
        store::enqueue(&pool, &ev("e1", "invoice.created", 100)).await.unwrap();
        drain(&pool).await;

        let status: String = sqlx::query("SELECT status FROM invoices WHERE invoice_id='inv_1'")
            .fetch_one(&pool).await.unwrap().get("status");
        assert_eq!(status, "paid");
        let res: String = sqlx::query("SELECT result FROM events WHERE event_id='e1'")
            .fetch_one(&pool).await.unwrap().get("result");
        assert!(res.starts_with("skipped stale"));
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimed() {
        let pool = temp_pool().await;
        store::enqueue(&pool, &ev("e3", "invoice.created", 1)).await.unwrap();
        // A "crashed" worker: claims with a zero-length lease and never finishes.
        assert!(store::claim(&pool, Duration::ZERO).await.unwrap().is_some());
        tokio::time::sleep(Duration::from_millis(5)).await;
        let again = store::claim(&pool, Duration::from_secs(30)).await.unwrap().unwrap();
        assert_eq!(again.attempts, 2);
    }
}
