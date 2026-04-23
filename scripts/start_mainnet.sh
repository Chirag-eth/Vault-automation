#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/.."

set -a
source "$ROOT/config/mainnet.env"
set +a

cd "$ROOT"
exec env PYTHONPATH=src python3 -m pred_polymarket_sync.http_api --port 8080 "$@"
