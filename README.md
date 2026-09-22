# webhook-service

Rust implementation of the System Design School webhook design:
`POST /webhook` -> verify HMAC -> durably enqueue -> `200` -> workers process -> persist result -> ack.

```bash
export WEBHOOK_SECRET=s3cret
cargo run                                   # listens on 0.0.0.0:3000, creates webhook.db
./scripts/send.sh evt_2 invoice.paid 200 inv_42      # signed test request
./scripts/send.sh evt_1 invoice.created 100 inv_42   # older event arriving late -> skipped
cargo test
```

Env: `WEBHOOK_SECRET` (required), `DATABASE_URL`, `BIND_ADDR`, `WORKERS`.
