#!/bin/bash
# vps_historical_backfill.sh - Long-running historical universe + metrics backfill.
#
# Run from the VPS with tmux/nohup/systemd. The steps are intentionally
# sequential and resumable:
#   1. Build point-in-time market_cap_history from CMC snapshots.
#   2. Sync asset_metadata.ever_in_top_50 from market_cap_history.
#   3. Backfill futures metrics for the full historical universe, one exchange at a time.
#   4. Backfill spot OHLCV for the same full historical universe.
#
# Configuration via environment:
#   BACKFILL_START=2020-01-01
#   BACKFILL_END_DAYS_AGO=1
#   BACKFILL_EXCHANGES=binance,bybit,okx
#   BACKFILL_SPOT=1
#   BACKFILL_SKIP_MCAP=0
#   BACKFILL_SKIP_FUTURES=0
#   BACKFILL_MCAP_ONLY=0
#   BACKFILL_FUTURES_ONLY=0  # legacy alias for BACKFILL_SKIP_MCAP=1
#   COINALYZE_MIN_INTERVAL=3.2
#   COINALYZE_SYMBOL_CACHE_TTL_HOURS=24
#   SPOT_SYMBOL_CACHE_TTL_HOURS=24
#   BACKFILL_SKIP_NATIVE_PATCH=1

set -u

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit 1

if [ -d "$DIR/venv" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python3"
else
    PYTHON_EXEC="/usr/bin/python3"
fi

mkdir -p "$DIR/logs"
LOG_FILE="$DIR/logs/historical_backfill_$(date +%Y%m%d_%H%M%S).log"

BACKFILL_START="${BACKFILL_START:-2020-01-01}"
BACKFILL_END_DAYS_AGO="${BACKFILL_END_DAYS_AGO:-1}"
BACKFILL_EXCHANGES="${BACKFILL_EXCHANGES:-binance,bybit,okx}"
BACKFILL_SPOT="${BACKFILL_SPOT:-1}"
BACKFILL_SKIP_MCAP="${BACKFILL_SKIP_MCAP:-0}"
BACKFILL_SKIP_FUTURES="${BACKFILL_SKIP_FUTURES:-0}"
BACKFILL_MCAP_ONLY="${BACKFILL_MCAP_ONLY:-0}"
BACKFILL_FUTURES_ONLY="${BACKFILL_FUTURES_ONLY:-0}"  # backward-compatible alias
COINALYZE_MIN_INTERVAL="${COINALYZE_MIN_INTERVAL:-3.2}"
COINALYZE_SYMBOL_CACHE_TTL_HOURS="${COINALYZE_SYMBOL_CACHE_TTL_HOURS:-24}"
SPOT_SYMBOL_CACHE_TTL_HOURS="${SPOT_SYMBOL_CACHE_TTL_HOURS:-24}"
BACKFILL_SKIP_NATIVE_PATCH="${BACKFILL_SKIP_NATIVE_PATCH:-1}"

run_step() {
    local label="$1"
    shift
    echo ""
    echo "[$(date)] === $label ==="
    echo "[$(date)] Command: $*"
    "$@"
    local code=$?
    echo "[$(date)] === $label finished with code $code ==="
    if [ "$code" -ne 0 ]; then
        echo "[$(date)] ERROR: stopping historical backfill at step: $label"
        exit "$code"
    fi
}

main() {
    echo "[$(date)] Historical backfill started in $DIR as $(whoami)"
    echo "[$(date)] Python: $PYTHON_EXEC"
    echo "[$(date)] Start: $BACKFILL_START | end-days-ago: $BACKFILL_END_DAYS_AGO | exchanges: $BACKFILL_EXCHANGES"
    echo "[$(date)] Coinalyze min interval: $COINALYZE_MIN_INTERVAL | futures symbol cache ttl hours: $COINALYZE_SYMBOL_CACHE_TTL_HOURS | spot symbol cache ttl hours: $SPOT_SYMBOL_CACHE_TTL_HOURS | skip native patch: $BACKFILL_SKIP_NATIVE_PATCH"

    if [ "$BACKFILL_FUTURES_ONLY" = "1" ]; then
        BACKFILL_SKIP_MCAP="1"
    fi

    if [ "$BACKFILL_SKIP_MCAP" != "1" ]; then
        run_step "Market-cap history from CMC" \
            "$PYTHON_EXEC" -u backfill_market_cap_history.py \
                --source cmc \
                --start "$BACKFILL_START" \
                --frequency daily
    fi

    if [ "$BACKFILL_MCAP_ONLY" = "1" ]; then
        echo "[$(date)] BACKFILL_MCAP_ONLY=1, stopping after market-cap history."
        return 0
    fi

    IFS=',' read -ra EXCHANGES <<< "$BACKFILL_EXCHANGES"
    if [ "$BACKFILL_SKIP_FUTURES" != "1" ]; then
        FUTURES_EXTRA_ARGS=""
        if [ "$BACKFILL_SKIP_NATIVE_PATCH" = "1" ]; then
            FUTURES_EXTRA_ARGS="--skip-native-patch"
        fi
        for exchange in "${EXCHANGES[@]}"; do
            exchange="$(echo "$exchange" | xargs)"
            if [ -z "$exchange" ]; then
                continue
            fi
            run_step "Futures metrics backfill: $exchange" \
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
            run_step "Spot OHLCV backfill: $exchange" \
                "$PYTHON_EXEC" -u spot_scraper.py \
                    --full-universe \
                    --start "$BACKFILL_START" \
                    --exchanges "$exchange" \
                    --spot-symbol-cache-ttl-hours "$SPOT_SYMBOL_CACHE_TTL_HOURS"
        done
    fi

    echo "[$(date)] Historical backfill complete."
}

main 2>&1 | tee -a "$LOG_FILE"
exit ${PIPESTATUS[0]}
