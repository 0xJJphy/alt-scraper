#!/bin/bash
# vps_full_universe_rollout.sh
#
# End-to-end VPS rollout for the survivor-bias-free universe:
#   1. Apply idempotent DB migrations needed by the new universe/backfill flow.
#   2. Build/refresh market_cap_history from historical CMC snapshots.
#   3. Sync all ever-in-top assets into asset_metadata.
#   4. Backfill futures metrics for every supported exchange-native market.
#   5. Backfill spot OHLCV for every supported exchange-native market.
#   6. Optionally restart systemd daemons so they reload the new universe.
#
# Safe defaults are conservative. Override with environment variables:
#
#   BACKFILL_START=2020-01-01
#   BACKFILL_END_DAYS_AGO=1
#   BACKFILL_EXCHANGES=binance,bybit,okx
#   BACKFILL_SPOT=1
#   BACKFILL_FUTURES=1
#   BACKFILL_MCAP=1
#   APPLY_MIGRATIONS=1
#   RESTART_SERVICES=0
#   STOP_DAEMONS_DURING_BACKFILL=0
#   COINALYZE_MIN_INTERVAL=3.2
#   COINALYZE_SYMBOL_CACHE_TTL_HOURS=24
#   SPOT_SYMBOL_CACHE_TTL_HOURS=24
#   BACKFILL_SKIP_NATIVE_PATCH=1
#   MCAP_FREQUENCY=daily
#   MCAP_TOP_N=50
#   CMC_SNAPSHOT_SIZE=100
#   DRY_RUN=0
#
# Examples:
#   ./vps_full_universe_rollout.sh
#   RESTART_SERVICES=1 ./vps_full_universe_rollout.sh
#   DRY_RUN=1 ./vps_full_universe_rollout.sh

set -u

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit 1

if [ -f "$DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$DIR/.env"
    set +a
fi

if [ -x "$DIR/venv/bin/python3" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python3"
else
    PYTHON_EXEC="${PYTHON_EXEC:-/usr/bin/python3}"
fi

mkdir -p "$DIR/logs"
LOG_FILE="$DIR/logs/full_universe_rollout_$(date +%Y%m%d_%H%M%S).log"
LOCK_DIR="$DIR/.full_universe_rollout.lock"

BACKFILL_START="${BACKFILL_START:-2020-01-01}"
BACKFILL_END_DAYS_AGO="${BACKFILL_END_DAYS_AGO:-1}"
BACKFILL_EXCHANGES="${BACKFILL_EXCHANGES:-binance,bybit,okx}"
BACKFILL_SPOT="${BACKFILL_SPOT:-1}"
BACKFILL_FUTURES="${BACKFILL_FUTURES:-1}"
BACKFILL_MCAP="${BACKFILL_MCAP:-1}"
APPLY_MIGRATIONS="${APPLY_MIGRATIONS:-1}"
RESTART_SERVICES="${RESTART_SERVICES:-0}"
STOP_DAEMONS_DURING_BACKFILL="${STOP_DAEMONS_DURING_BACKFILL:-0}"
COINALYZE_MIN_INTERVAL="${COINALYZE_MIN_INTERVAL:-3.2}"
COINALYZE_SYMBOL_CACHE_TTL_HOURS="${COINALYZE_SYMBOL_CACHE_TTL_HOURS:-24}"
SPOT_SYMBOL_CACHE_TTL_HOURS="${SPOT_SYMBOL_CACHE_TTL_HOURS:-24}"
BACKFILL_SKIP_NATIVE_PATCH="${BACKFILL_SKIP_NATIVE_PATCH:-1}"
MCAP_FREQUENCY="${MCAP_FREQUENCY:-daily}"
MCAP_TOP_N="${MCAP_TOP_N:-50}"
CMC_SNAPSHOT_SIZE="${CMC_SNAPSHOT_SIZE:-100}"
DRY_RUN="${DRY_RUN:-0}"
SERVICE_NAMES="${SERVICE_NAMES:-alt-scraper-realtime.service alt-scraper-orderbook.service}"

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

log() {
    echo "[$(date)] $*" | tee -a "$LOG_FILE"
}

run_step() {
    local label="$1"
    shift
    log ""
    log "=== $label ==="
    log "Command: $*"
    if [ "$DRY_RUN" = "1" ]; then
        log "DRY_RUN=1, skipping command."
        return 0
    fi
    "$@" 2>&1 | tee -a "$LOG_FILE"
    local code=${PIPESTATUS[0]}
    log "=== $label finished with code $code ==="
    if [ "$code" -ne 0 ]; then
        log "ERROR: stopping rollout at step: $label"
        exit "$code"
    fi
}

require_file() {
    if [ ! -f "$1" ]; then
        log "ERROR: required file missing: $1"
        exit 1
    fi
}

preflight() {
    log "Full universe rollout started in $DIR as $(whoami)"
    log "Python: $PYTHON_EXEC"
    log "Start: $BACKFILL_START | end-days-ago: $BACKFILL_END_DAYS_AGO | exchanges: $BACKFILL_EXCHANGES"
    log "Mcap: $BACKFILL_MCAP | futures: $BACKFILL_FUTURES | spot: $BACKFILL_SPOT | migrations: $APPLY_MIGRATIONS"
    log "Restart services: $RESTART_SERVICES | stop daemons during backfill: $STOP_DAEMONS_DURING_BACKFILL"
    log "Coinalyze min interval: $COINALYZE_MIN_INTERVAL | futures cache ttl: $COINALYZE_SYMBOL_CACHE_TTL_HOURS | spot cache ttl: $SPOT_SYMBOL_CACHE_TTL_HOURS"

    require_file "$DIR/backfill_market_cap_history.py"
    require_file "$DIR/alt_scraper.py"
    require_file "$DIR/spot_scraper.py"

    if [ -z "${DATABASE_URL:-}" ]; then
        log "ERROR: DATABASE_URL is not set. Add it to .env on the VPS."
        exit 1
    fi
    if [ "$BACKFILL_FUTURES" = "1" ] && [ -z "${COINALYZE_API_KEY:-}" ]; then
        log "ERROR: COINALYZE_API_KEY is required for futures backfill."
        exit 1
    fi
}

acquire_lock() {
    if ! mkdir "$LOCK_DIR" 2>/dev/null; then
        log "ERROR: another rollout appears to be running: $LOCK_DIR"
        exit 1
    fi
    trap 'rm -rf "$LOCK_DIR"' EXIT
}

apply_sql_file() {
    local file="$1"
    require_file "$file"
    log ""
    log "=== Apply migration: $file ==="
    if [ "$DRY_RUN" = "1" ]; then
        log "DRY_RUN=1, skipping migration."
        return 0
    fi
    "$PYTHON_EXEC" - "$file" <<'PY' 2>&1 | tee -a "$LOG_FILE"
import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv(".env")
path = sys.argv[1]
db_url = os.environ["DATABASE_URL"]
with open(path, "r", encoding="utf-8") as f:
    sql = f.read()

conn = psycopg2.connect(db_url)
try:
    with conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    print(f"[DB] Applied migration: {path}")
finally:
    conn.close()
PY
    local code=${PIPESTATUS[0]}
    log "=== Migration finished with code $code ==="
    if [ "$code" -ne 0 ]; then
        log "ERROR: migration failed: $file"
        exit "$code"
    fi
}

stop_daemons_if_requested() {
    if [ "$STOP_DAEMONS_DURING_BACKFILL" != "1" ]; then
        return 0
    fi
    for service in $SERVICE_NAMES; do
        run_step "Stop $service" $SUDO systemctl stop "$service"
    done
}

restart_services_if_requested() {
    if [ "$RESTART_SERVICES" != "1" ]; then
        log ""
        log "RESTART_SERVICES=0, not restarting daemons automatically."
        log "Manual restart commands:"
        log "  sudo systemctl daemon-reload"
        log "  sudo systemctl restart alt-scraper-realtime.service"
        log "  sudo systemctl restart alt-scraper-orderbook.service"
        log "  sudo systemctl restart alt-scraper.timer"
        log "  sudo systemctl status alt-scraper-realtime.service --no-pager"
        log "  sudo systemctl status alt-scraper-orderbook.service --no-pager"
        return 0
    fi

    run_step "Reload systemd" $SUDO systemctl daemon-reload
    run_step "Restart daily timer" $SUDO systemctl restart alt-scraper.timer
    for service in $SERVICE_NAMES; do
        run_step "Restart $service" $SUDO systemctl restart "$service"
    done
    for service in $SERVICE_NAMES; do
        run_step "Status $service" $SUDO systemctl status "$service" --no-pager
    done
}

validate_database() {
    log ""
    log "=== Validate DB coverage ==="
    if [ "$DRY_RUN" = "1" ]; then
        log "DRY_RUN=1, skipping DB validation."
        return 0
    fi
    "$PYTHON_EXEC" - <<'PY' 2>&1 | tee -a "$LOG_FILE"
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(".env")
conn = psycopg2.connect(os.environ["DATABASE_URL"])
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(DISTINCT date) AS dates,
                MIN(date) AS min_date,
                MAX(date) AS max_date,
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) FILTER (WHERE ever_in_top_50 = true) AS ever_top_symbols
            FROM market_cap_history
        """)
        print("[DB] market_cap_history:", cur.fetchone())

        cur.execute("""
            SELECT COUNT(*) FROM asset_metadata
            WHERE (is_filtered = false OR is_filtered IS NULL)
              AND ever_in_top_50 = true
        """)
        print("[DB] asset_metadata ever-top non-filtered:", cur.fetchone()[0])

        cur.execute("""
            SELECT exchange, COUNT(*) AS rows, COUNT(DISTINCT base_asset) AS assets
            FROM futures_daily_metrics
            GROUP BY exchange
            ORDER BY exchange
        """)
        print("[DB] futures_daily_metrics:")
        for row in cur.fetchall():
            print("  ", row)

        cur.execute("""
            SELECT exchange, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols
            FROM spot_daily_ohlcv
            GROUP BY exchange
            ORDER BY exchange
        """)
        print("[DB] spot_daily_ohlcv:")
        for row in cur.fetchall():
            print("  ", row)
finally:
    conn.close()
PY
    local code=${PIPESTATUS[0]}
    if [ "$code" -ne 0 ]; then
        log "ERROR: validation failed."
        exit "$code"
    fi
}

main() {
    preflight
    acquire_lock

    if [ "$APPLY_MIGRATIONS" = "1" ]; then
        apply_sql_file "$DIR/migrations/add_market_cap_history.sql"
        apply_sql_file "$DIR/migrations/001_futures_intraday_snapshots.sql"
    fi

    stop_daemons_if_requested

    if [ "$BACKFILL_MCAP" = "1" ]; then
        run_step "Market-cap history from CMC and asset_metadata sync" \
            "$PYTHON_EXEC" -u backfill_market_cap_history.py \
                --source cmc \
                --start "$BACKFILL_START" \
                --frequency "$MCAP_FREQUENCY" \
                --top-n "$MCAP_TOP_N" \
                --snapshot-size "$CMC_SNAPSHOT_SIZE"
    fi

    IFS=',' read -ra EXCHANGES <<< "$BACKFILL_EXCHANGES"

    if [ "$BACKFILL_FUTURES" = "1" ]; then
        FUTURES_EXTRA_ARGS=""
        if [ "$BACKFILL_SKIP_NATIVE_PATCH" = "1" ]; then
            FUTURES_EXTRA_ARGS="--skip-native-patch"
        fi

        for exchange in "${EXCHANGES[@]}"; do
            exchange="$(echo "$exchange" | xargs)"
            if [ -z "$exchange" ]; then
                continue
            fi
            run_step "Futures full-universe backfill: $exchange" \
                "$PYTHON_EXEC" -u alt_scraper.py \
                    --full-universe \
                    --start "$BACKFILL_START" \
                    --end-days-ago "$BACKFILL_END_DAYS_AGO" \
                    --exchanges "$exchange" \
                    --skip-merge \
                    --coinalyze-min-interval "$COINALYZE_MIN_INTERVAL" \
                    --coinalyze-symbol-cache-ttl-hours "$COINALYZE_SYMBOL_CACHE_TTL_HOURS" \
                    $FUTURES_EXTRA_ARGS
        done
    fi

    if [ "$BACKFILL_SPOT" = "1" ]; then
        for exchange in "${EXCHANGES[@]}"; do
            exchange="$(echo "$exchange" | xargs)"
            if [ -z "$exchange" ]; then
                continue
            fi
            run_step "Spot full-universe backfill: $exchange" \
                "$PYTHON_EXEC" -u spot_scraper.py \
                    --full-universe \
                    --start "$BACKFILL_START" \
                    --exchanges "$exchange" \
                    --spot-symbol-cache-ttl-hours "$SPOT_SYMBOL_CACHE_TTL_HOURS"
        done
    fi

    validate_database
    restart_services_if_requested

    log ""
    log "Full universe rollout complete."
}

main
