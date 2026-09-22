mod api;
mod processor;
mod store;
mod worker;

use std::{env, sync::Arc};

use anyhow::Context;
use tokio::{net::TcpListener, sync::{watch, Notify}};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let secret = env::var("WEBHOOK_SECRET").context("set WEBHOOK_SECRET (shared with the provider)")?;
    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://webhook.db".into());
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let workers: usize = env::var("WORKERS").ok().and_then(|v| v.parse().ok()).unwrap_or(4);

    let pool = store::connect(&db_url).await?;
    let notify = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut tasks = Vec::new();
    for i in 0..workers {
        tasks.push(tokio::spawn(worker::run(
            i,
            pool.clone(),
            notify.clone(),
            shutdown_rx.clone(),
            worker::WorkerCfg::default(),
        )));
    }
    tasks.push(tokio::spawn(worker::purge_loop(pool.clone(), shutdown_rx.clone())));

    let app = api::router(api::AppState {
        pool: pool.clone(),
        secret: Arc::new(secret.into_bytes()),
        notify,
    });
    let listener = TcpListener::bind(&bind).await?;
    info!(%bind, workers, "webhook service listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
            info!("shutting down");
        })
        .await?;

    shutdown_tx.send(true)?;
    for t in tasks {
        let _ = t.await;
    }
    Ok(())
}
