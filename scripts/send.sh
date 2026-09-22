#!/usr/bin/env bash
# Usage: WEBHOOK_SECRET=s ./scripts/send.sh <event_id> <type> <created_unix_secs> <invoice_id>
set -euo pipefail
SECRET="${WEBHOOK_SECRET:?set WEBHOOK_SECRET}"
URL="${URL:-http://localhost:3000/webhook}"
ID="$1"; TYPE="$2"; TS="$3"; INV="$4"
BODY=$(printf '{"id":"%s","type":"%s","created":%s,"data":{"invoice_id":"%s"}}' "$ID" "$TYPE" "$TS" "$INV")
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -hex | sed 's/^.* //')
curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$URL" \
  -H "Content-Type: application/json" -H "X-Signature: sha256=$SIG" --data-binary "$BODY"
