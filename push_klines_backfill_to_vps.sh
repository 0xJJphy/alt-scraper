#!/usr/bin/env bash
# push_klines_backfill_to_vps.sh — Merge the local historical 15m kline backfill
# into the VPS database. Non-destructive: only inserts candles missing on the
# VPS (ON CONFLICT DO NOTHING) — never overwrites its own live-collected data.
#
# Uso: ./push_klines_backfill_to_vps.sh [--symbol SYMBOL] [user@ip]
#   --symbol SYMBOL   Limit to one exchange-native symbol (e.g. EWTUSDT), for
#                      testing the pipeline before running the full push.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOTENV="$SCRIPT_DIR/.env"
DUMP_FILE="$SCRIPT_DIR/klines_backfill_export.bin.gz"

if [[ -f "$DOTENV" ]]; then
    set -a
    # shellcheck disable=SC1090
    source <(grep -v '^\s*#' "$DOTENV" | sed 's/\r//')
    set +a
fi

SYMBOL_FILTER=""
VPS_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --symbol)
            SYMBOL_FILTER="$2"
            shift 2
            ;;
        *)
            VPS_ARG="$1"
            shift
            ;;
    esac
done

VPS="${VPS_ARG:-${VPS_ADDRESS:-}}"
if [[ -z "$VPS" ]]; then
    read -rp "Enter VPS address (e.g. user@ip): " VPS
fi
if [[ -z "$VPS" ]]; then
    echo "ERROR: VPS address is required." >&2
    exit 1
fi

WHERE_CLAUSE="source='backfill'"
if [[ -n "$SYMBOL_FILTER" ]]; then
    WHERE_CLAUSE="source='backfill' AND symbol='$SYMBOL_FILTER'"
    echo "==> Modo prueba: solo symbol='$SYMBOL_FILTER'"
fi

CONTAINER="${DOCKER_POSTGRES_CONTAINER:-gli_postgres}"
LOCAL_DB_USER="gli_user"
LOCAL_DB_NAME="alt_scraper_dev"
REMOTE_DB_NAME="${REMOTE_DB_NAME:-alt_scraper}"

KLINE_COLS="candle_open_at, candle_close_at, symbol, exchange, base_asset, \
price_open, price_high, price_low, price_close, volume_base, volume_usd, \
buy_volume_base, sell_volume_base, volume_delta, txn_count, polled_at, \
source, ws_received_at, rest_reconciled_at, exchange_event_time"

echo "==> Verificando que el contenedor Docker local esté corriendo..."
if ! docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null | grep -q running; then
    echo "   El contenedor '$CONTAINER' no está corriendo." >&2
    exit 1
fi

echo "==> Exportando velas de backfill (local)..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d "$LOCAL_DB_NAME" \
    -c "\\copy (SELECT $KLINE_COLS FROM futures_klines_15m WHERE $WHERE_CLAUSE) TO STDOUT WITH (FORMAT binary)" \
    | gzip > "$DUMP_FILE"

ROWS=$(docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d "$LOCAL_DB_NAME" -t \
    -c "SELECT count(*) FROM futures_klines_15m WHERE $WHERE_CLAUSE;" | tr -d ' ')
SIZE=$(du -h "$DUMP_FILE" | cut -f1)
echo "   Export: $ROWS filas, $SIZE ($DUMP_FILE)"

echo "==> Transfiriendo al VPS..."
scp -C -o "ServerAliveInterval=30" -o "ServerAliveCountMax=5" \
    "$DUMP_FILE" "${VPS}:/tmp/klines_backfill_export.bin.gz"

echo "==> Generando script de merge para el VPS..."
MERGE_SQL=$(mktemp)
cat > "$MERGE_SQL" <<SQL
CREATE TEMP TABLE klines_backfill_staging (LIKE futures_klines_15m INCLUDING DEFAULTS);
\\copy klines_backfill_staging ($KLINE_COLS) FROM '/tmp/klines_backfill_export.bin' WITH (FORMAT binary)
INSERT INTO futures_klines_15m ($KLINE_COLS, created_at, updated_at)
SELECT $KLINE_COLS, NOW(), NOW() FROM klines_backfill_staging
ON CONFLICT (candle_open_at, symbol, exchange) DO NOTHING;
SELECT count(*) AS staged FROM klines_backfill_staging;
SQL
scp -q "$MERGE_SQL" "${VPS}:/tmp/klines_backfill_merge.sql"
rm -f "$MERGE_SQL"

echo "==> Cargando + fusionando en el VPS (ON CONFLICT DO NOTHING, no destructivo)..."
ssh -t "$VPS" "sudo -u postgres bash -c '
    set -e
    gunzip -c /tmp/klines_backfill_export.bin.gz > /tmp/klines_backfill_export.bin
    psql -d $REMOTE_DB_NAME -f /tmp/klines_backfill_merge.sql
    rm -f /tmp/klines_backfill_export.bin /tmp/klines_backfill_export.bin.gz /tmp/klines_backfill_merge.sql
'"

echo "==> Limpiando local..."
rm -f "$DUMP_FILE"

echo "==> Verificando en el VPS..."
if [[ -n "$SYMBOL_FILTER" ]]; then
    ssh -t "$VPS" "sudo -u postgres psql -d $REMOTE_DB_NAME -c \"SELECT exchange, count(*), min(candle_open_at)::date, max(candle_open_at)::date FROM futures_klines_15m WHERE symbol='$SYMBOL_FILTER' GROUP BY exchange;\""
else
    ssh -t "$VPS" "sudo -u postgres psql -d $REMOTE_DB_NAME -c \"SELECT exchange, count(*), min(candle_open_at)::date, max(candle_open_at)::date FROM futures_klines_15m GROUP BY exchange;\""
fi

echo "==> Volcado completo."
