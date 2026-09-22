//! Traffic poller: NOT a webhook. No provider pushes traffic events, so this calls
//! TomTom's Routing API on a timer ("pull") and alerts when the live, traffic-aware
//! travel time is significantly worse than the no-traffic baseline the same response
//! gives us — no separate history to store.
//!
//! Uses TomTom instead of Google Maps: its free Developer plan (2,500 requests/day,
//! includes live traffic) needs no credit card, unlike Google's free tier which still
//! requires billing to be enabled. Get a key at https://developer.tomtom.com (free
//! signup) — no card required.
//!
//! Config (env vars):
//!   TOMTOM_API_KEY        required
//!   ORIGIN, DESTINATION   required — "lat,lon" pairs, e.g. "-1.286389,36.817223"
//!                          (TomTom's routing endpoint takes coordinates, not
//!                          addresses; look yours up once on Google Maps or
//!                          openstreetmap.org and reuse them)
//!   POLL_INTERVAL_SECS    optional, default 300 (5 min)
//!   ALERT_THRESHOLD_PCT   optional, default 20  (alert when duration is 20%+ over baseline)
//!   NTFY_TOPIC            optional — if set, also pushes to https://ntfy.sh/<topic>
//!                          (free, no account: install the ntfy app and subscribe to
//!                          the same topic name to get the alert on your phone)

use std::{env, time::Duration};

use anyhow::{bail, Context};
use serde::Deserialize;
use tracing::{info, warn};

struct Config {
    api_key: String,
    origin: String,      // "lat,lon"
    destination: String, // "lat,lon"
    poll_interval: Duration,
    threshold_pct: f64,
    ntfy_topic: Option<String>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            api_key: env::var("TOMTOM_API_KEY").context("set TOMTOM_API_KEY (free, no card, at developer.tomtom.com)")?,
            origin: env::var("ORIGIN").context("set ORIGIN as \"lat,lon\", e.g. -1.286389,36.817223")?,
            destination: env::var("DESTINATION").context("set DESTINATION as \"lat,lon\"")?,
            poll_interval: Duration::from_secs(
                env::var("POLL_INTERVAL_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(300),
            ),
            threshold_pct: env::var("ALERT_THRESHOLD_PCT").ok().and_then(|v| v.parse().ok()).unwrap_or(20.0),
            ntfy_topic: env::var("NTFY_TOPIC").ok().filter(|s| !s.is_empty()),
        })
    }
}

#[derive(Deserialize)]
struct RoutesResponse {
    #[serde(default)]
    routes: Vec<RouteInfo>,
}

#[derive(Deserialize)]
struct RouteInfo {
    summary: Summary,
}

#[derive(Deserialize)]
struct Summary {
    #[serde(rename = "travelTimeInSeconds")]
    travel_time_in_seconds: f64,
    #[serde(rename = "noTrafficTravelTimeInSeconds")]
    no_traffic_travel_time_in_seconds: f64,
    #[serde(rename = "lengthInMeters")]
    length_in_meters: i64,
}

struct Reading {
    live_secs: f64,
    baseline_secs: f64,
    distance_km: f64,
}

async fn check_traffic(client: &reqwest::Client, cfg: &Config) -> anyhow::Result<Reading> {
    // TomTom's route is a path segment in the URL: {origin}:{destination}.
    let url = format!(
        "https://api.tomtom.com/routing/1/calculateRoute/{}:{}/json",
        cfg.origin, cfg.destination
    );

    let resp = client
        .get(&url)
        .query(&[
            ("key", cfg.api_key.as_str()),
            ("traffic", "true"),
            ("computeTravelTimeFor", "all"), // asks for noTrafficTravelTimeInSeconds too
        ])
        .send()
        .await
        .context("request to TomTom Routing API failed")?;

    let status = resp.status();
    let text = resp.text().await.context("reading TomTom response body")?;
    if !status.is_success() {
        bail!("TomTom API returned {status}: {text}");
    }

    let parsed: RoutesResponse = serde_json::from_str(&text)
        .with_context(|| format!("unexpected TomTom response shape: {text}"))?;
    let Some(route) = parsed.routes.first() else {
        bail!("TomTom returned no route for '{}' -> '{}'", cfg.origin, cfg.destination);
    };

    Ok(Reading {
        live_secs: route.summary.travel_time_in_seconds,
        baseline_secs: route.summary.no_traffic_travel_time_in_seconds,
        distance_km: route.summary.length_in_meters as f64 / 1000.0,
    })
}

fn fmt_mins(secs: f64) -> String {
    format!("{:.0} min", secs / 60.0)
}

async fn send_alert(client: &reqwest::Client, cfg: &Config, msg: &str) {
    warn!("{msg}");
    let Some(topic) = &cfg.ntfy_topic else { return };
    let url = format!("https://ntfy.sh/{topic}");
    if let Err(e) = client.post(&url).body(msg.to_string()).send().await {
        warn!(error = %e, "failed to push ntfy alert");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    let client = reqwest::Client::builder().timeout(Duration::from_secs(15)).build()?;

    info!(
        origin = %cfg.origin,
        destination = %cfg.destination,
        interval_secs = cfg.poll_interval.as_secs(),
        threshold_pct = cfg.threshold_pct,
        ntfy = cfg.ntfy_topic.is_some(),
        "traffic poller started (TomTom, free tier)"
    );

    loop {
        match check_traffic(&client, &cfg).await {
            Ok(r) => {
                let over_pct = if r.baseline_secs > 0.0 {
                    (r.live_secs - r.baseline_secs) / r.baseline_secs * 100.0
                } else {
                    0.0
                };
                if over_pct >= cfg.threshold_pct {
                    let msg = format!(
                        "Traffic alert: {} -> {} is {} in traffic (normally {}), {:.0}% slower, {:.1} km",
                        cfg.origin, cfg.destination, fmt_mins(r.live_secs), fmt_mins(r.baseline_secs), over_pct, r.distance_km
                    );
                    send_alert(&client, &cfg, &msg).await;
                } else {
                    info!(
                        live = %fmt_mins(r.live_secs),
                        baseline = %fmt_mins(r.baseline_secs),
                        over_pct = format!("{over_pct:.0}%"),
                        "traffic normal"
                    );
                }
            }
            Err(e) => warn!(error = %e, "traffic check failed, will retry next interval"),
        }

        tokio::select! {
            _ = tokio::time::sleep(cfg.poll_interval) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("shutting down");
                break;
            }
        }
    }
    Ok(())
}
