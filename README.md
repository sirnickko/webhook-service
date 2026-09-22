# webhook-service

A production-style webhook receiver written in Rust, plus a companion traffic-alert
poller. Two separate programs live in this one project.

| Binary | What it does | Model |
|---|---|---|
| `webhook-service` | Receives, verifies, durably stores, and processes webhook events (Stripe/GitHub/Shopify-style) | **Push** — the provider calls you |
| `traffic-poller` | Watches a route's travel time via TomTom and alerts when traffic spikes | **Pull** — you call the provider on a timer |

---

## 1. Prerequisites

You need a working Rust toolchain and a C compiler (SQLite compiles from C source
as part of the build).

### Install Rust

Go to https://rustup.rs and run the installer for your OS. Restart your terminal
(or VS Code) afterward, then confirm it worked:

```bash
rustc --version
cargo --version
```

### Install a C compiler (Windows only)

Rust needs a linker/compiler to build SQLite. Pick **one** of these — MSVC is
usually the smoother path on Windows.

**Option A — Microsoft (MSVC), recommended:**
1. Download **Build Tools for Visual Studio**: https://visualstudio.microsoft.com/visual-cpp-build-tools/
2. In the installer, tick **Desktop development with C++**, install, and restart if prompted.
3. Switch Rust to the MSVC toolchain:
   ```powershell
   rustup toolchain install stable-x86_64-pc-windows-msvc
   rustup default stable-x86_64-pc-windows-msvc
   ```

**Option B — GNU (MinGW):**
1. Install MSYS2: https://www.msys2.org
2. Open the **MSYS2 MINGW64** terminal (not plain MSYS2) and run:
   ```bash
   pacman -S --needed mingw-w64-x86_64-toolchain
   ```
3. Add `C:\msys64\mingw64\bin` to your Windows PATH (Environment Variables → User
   variables → `Path` → New).
4. Fully close and reopen VS Code so the new PATH takes effect.

On macOS and Linux, a C compiler is normally already present (Xcode Command Line
Tools on macOS, `build-essential`/`gcc` on Linux). If `cargo build` complains about
a missing linker, install that first.

---

## 2. Get the code

```bash
git clone https://github.com/sirnickko/webhook-service.git
cd webhook-service
```

Or, if you downloaded the zip: extract it, then open the extracted `webhook-service`
folder (not a parent folder) in VS Code — `Cargo.toml` should be directly visible
in the Explorer's top level.

---

## 3. Build

```bash
cargo build
```

The first build downloads and compiles every dependency, which takes a couple of
minutes. Later builds are much faster.

Run the test suite any time to check nothing's broken:

```bash
cargo test
```

---

## 4. `webhook-service` — the webhook receiver

### What it does

1. A provider (Stripe, GitHub, your own app, etc.) sends a signed `POST` to `/webhook`.
2. The HTTP handler verifies the HMAC-SHA256 signature over the raw body, then
   **durably stores** the event (fsync'd to disk) before returning `200`.
3. Background workers claim events with a time-based lease, run the business logic,
   and only mark an event done once its side effect has committed — so a crash
   mid-processing just means another worker retries it.
4. Duplicate `event_id`s are recognized and skipped (idempotency). Failures retry
   with exponential backoff (2s → 4s → 8s → ... capped at 15 min, 10 attempts) before
   being parked as `dead`.
5. Events older than 30 days are purged automatically.

### Configuration (environment variables)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `WEBHOOK_SECRET` | yes | — | Shared HMAC secret with your webhook provider |
| `DATABASE_URL` | no | `sqlite://webhook.db` | Where events are stored |
| `BIND_ADDR` | no | `0.0.0.0:3000` | Address/port the server listens on |
| `WORKERS` | no | `4` | Number of concurrent processing workers |

### Run it

```powershell
$env:WEBHOOK_SECRET = "s3cret"
cargo run
```

You should see:
```
INFO webhook_service: webhook service listening bind=0.0.0.0:3000 workers=4
```

### Send a test event

**Bash / macOS / Linux / Git Bash:**
```bash
export WEBHOOK_SECRET=s3cret
./scripts/send.sh evt_1 invoice.paid 200 inv_42
```

**PowerShell (Windows):**
```powershell
$secret = "s3cret"
$body = '{"id":"evt_1","type":"invoice.paid","created":200,"data":{"invoice_id":"inv_42"}}'
$h = [Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
$sig = ([BitConverter]::ToString($h.ComputeHash([Text.Encoding]::UTF8.GetBytes($body))) -replace '-','').ToLower()
(Invoke-WebRequest -UseBasicParsing -Method Post -Uri http://localhost:3000/webhook `
  -Body $body -ContentType 'application/json' `
  -Headers @{'X-Signature'="sha256=$sig"}).StatusCode
```

A `200` response means it was accepted. The server terminal should log
`processed event_id=evt_1 ... result=invoice inv_42 -> paid`. Re-running the same
`id` is treated as a duplicate and skipped without reprocessing.

To inspect stored events, install the **SQLite Viewer** VS Code extension and open
`webhook.db`, or use the `sqlite3` CLI:
```bash
sqlite3 webhook.db "select event_id, status, attempts, result from events;"
```

### Wiring it to a real provider

The current code uses a made-up envelope (`id`/`type`/`created`) and a generic
`sha256=<hex>` HMAC header — this is a template, not a finished Stripe/GitHub
integration. To point it at a real provider:

1. Read that provider's webhook docs for its exact signature scheme and header
   name (e.g. Stripe: `Stripe-Signature`; GitHub: `X-Hub-Signature-256`).
2. Update the header name and `verify_signature` in `src/api.rs` to match.
3. Update the `Envelope` struct in `src/api.rs` to the provider's real field names.
4. Update the match arms in `src/processor.rs` for the event types you care about.
5. Get a public HTTPS URL. For local testing, use a tunnel (`ngrok http 3000`, or
   the provider's own CLI, e.g. `stripe listen`). For production, deploy behind a
   reverse proxy (Caddy/nginx) that terminates TLS, or use a platform that handles
   TLS for you (Fly.io, Railway).
6. Register that URL with the provider and use the signing secret **it** gives you
   as `WEBHOOK_SECRET` — not a value you make up.
7. If you outgrow a single instance, swap SQLite in `src/store.rs` for Postgres
   using `SELECT ... FOR UPDATE SKIP LOCKED` in place of the lease-based `claim()`.

---

## 5. `traffic-poller` — the traffic-alert tool

### Why it's not a webhook

No map provider (Google, Apple, TomTom) pushes traffic events to you — you have to
ask. This tool polls TomTom's Routing API on a timer instead and compares the
live, traffic-aware travel time against the no-traffic baseline from the same
response. No separate history needs to be stored.

### Get a free API key

1. Sign up at https://developer.tomtom.com — no credit card required.
2. Go to **Keys → API & SDK Keys**. A default key is created automatically and
   already covers the Routing API.
3. Click the truncated key to copy the full value.

The free **Evaluation** tier gives 2,500 requests/day across all TomTom APIs
combined — plenty for this, since a 5-minute polling interval is about 288
requests/day.

### Configuration (environment variables)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `TOMTOM_API_KEY` | yes | — | Your TomTom API key |
| `ORIGIN` | yes | — | Start point, as `"lat,lon"` |
| `DESTINATION` | yes | — | End point, as `"lat,lon"` |
| `POLL_INTERVAL_SECS` | no | `300` | Seconds between checks |
| `ALERT_THRESHOLD_PCT` | no | `20` | Alert when live time is this % over baseline |
| `NTFY_TOPIC` | no | — | If set, also pushes a phone notification via [ntfy.sh](https://ntfy.sh) |

TomTom's Routing API takes coordinates, not addresses. Look yours up once on
Google Maps or openstreetmap.org (right-click → "What's here?") and reuse them.

### Run it

```powershell
$env:TOMTOM_API_KEY = "your-key"
$env:ORIGIN = "-1.286389,36.817223"
$env:DESTINATION = "-1.319167,36.927778"
cargo run --bin traffic-poller
```

It logs `traffic poller started`, checks immediately, then every
`POLL_INTERVAL_SECS`. Normal traffic logs at `info` level; an alert logs at `warn`
level and, if `NTFY_TOPIC` is set, also pushes to your phone.

### Get phone alerts (optional, free)

1. Install the **ntfy** app (iOS/Android) or visit https://ntfy.sh in a browser.
2. Subscribe to any topic name you make up — treat it like a password, since
   anyone who knows the exact name can also subscribe (e.g. `traffic-nn-9f2k`).
3. Set that same name as `NTFY_TOPIC` before running the poller.

### Stop it

Click into its terminal and press **Ctrl+C**. It logs `shutting down` and exits
cleanly rather than being killed mid-request.

---

## 6. Project structure

```
webhook-service/
├── Cargo.toml              # dependencies for both binaries
├── src/
│   ├── main.rs              # webhook-service entry point
│   ├── api.rs                # HTTP handler: signature check, enqueue, ack
│   ├── store.rs               # SQLite-backed queue + audit log
│   ├── worker.rs               # claim/process/retry loop, retention purge
│   ├── processor.rs             # business logic per event type
│   └── bin/
│       └── traffic-poller.rs      # standalone traffic-alert poller
├── scripts/
│   └── send.sh              # bash helper to send a signed test webhook
└── README.md
```

`cargo build` compiles both binaries automatically — anything under `src/bin/` is
picked up as its own program with no extra configuration. Use `cargo run` for the
webhook server, `cargo run --bin traffic-poller` for the poller.

---

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `failed to parse manifest ... no targets specified` | `Cargo.toml` and `src/` are in different folders | Make sure both sit directly inside the same folder; move files if needed |
| `error: environment variable not found` (e.g. `WEBHOOK_SECRET`) | The `$env:` var wasn't set in *this* terminal session | Re-run the `$env:VAR = "..."` line before `cargo run` — it doesn't persist across terminals |
| `error calling dlltool 'dlltool.exe': program not found` | Using the GNU toolchain without a full MinGW-w64 install | Install `mingw-w64-x86_64-toolchain` via MSYS2 (see Prerequisites), or switch to MSVC |
| `linker link.exe not found` (MSVC) | The C++ workload didn't fully install | Re-run the Build Tools installer, choose Modify, tick "Desktop development with C++" |
| `403` from TomTom | Over the free 2,500 requests/day limit | Increase `POLL_INTERVAL_SECS`, or wait for the daily reset |
| Duplicate webhook not reprocessing | Working as intended | `event_id` is the idempotency key; re-sending the same id is a no-op by design |

---

## 8. Running both at once

They're independent programs and don't share any code path at runtime — you can
run one, the other, or both simultaneously in separate terminals:

```powershell
# Terminal 1
$env:WEBHOOK_SECRET = "s3cret"
cargo run

# Terminal 2
$env:TOMTOM_API_KEY = "your-key"
$env:ORIGIN = "-1.286389,36.817223"
$env:DESTINATION = "-1.319167,36.927778"
cargo run --bin traffic-poller
```