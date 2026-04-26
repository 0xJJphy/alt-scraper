#!/usr/bin/env bash
# restore_alt_scraper.sh — Transfer Supabase dump to VPS and restore into alt_scraper DB
# Usage: ./restore_alt_scraper.sh [user@vps-ip]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUMP_FILE="$SCRIPT_DIR/supabase_dump.dump"
SCHEMA_FILE="$SCRIPT_DIR/schema.sql"

# ── Load .env ────────────────────────────────────────────────────────────────
if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a; source "$SCRIPT_DIR/.env"; set +a
fi

VPS="${1:-${VPS_ADDRESS:-}}"
if [[ -z "$VPS" ]]; then
    read -rp "Enter VPS address (user@ip): " VPS
fi
[[ -z "$VPS" ]] && { echo "ERROR: VPS address required."; exit 1; }

[[ ! -f "$DUMP_FILE" ]]   && { echo "ERROR: $DUMP_FILE not found. Run pg_dump first."; exit 1; }
[[ ! -f "$SCHEMA_FILE" ]] && { echo "ERROR: $SCHEMA_FILE not found."; exit 1; }

DUMP_MB=$(du -m "$DUMP_FILE" | cut -f1)
echo "==> Dump file: $DUMP_FILE (${DUMP_MB} MB)"
echo "==> Target VPS: $VPS"
echo ""

# ── 1. Transfer files ─────────────────────────────────────────────────────────
echo "==> 1. Transfiriendo dump y schema al VPS (puede tardar según la red)..."
scp -C "$DUMP_FILE"   "$VPS:/tmp/supabase_dump.dump"
scp    "$SCHEMA_FILE" "$VPS:/tmp/schema_alt_scraper.sql"
echo "   Transferencia completada."

# ── 2. Build remote restore script ───────────────────────────────────────────
REMOTE_SCRIPT=$(cat << 'REMOTE'
#!/bin/bash
set -e

DUMP=/tmp/supabase_dump.dump
SCHEMA=/tmp/schema_alt_scraper.sql
DB=alt_scraper

echo ""
echo "==> [1/6] Verificando PostgreSQL..."
psql --version

echo ""
echo "==> [2/6] Comprobando que gli_dashboard no se toca..."
sudo -u postgres psql -d gli_dashboard -c \
    "SELECT count(*) AS tablas_gli_dashboard FROM information_schema.tables WHERE table_schema='public';"

echo ""
echo "==> [3/6] Creando base de datos alt_scraper (si no existe)..."
sudo -u postgres psql -c "SELECT 1 FROM pg_database WHERE datname='alt_scraper'" | grep -q 1 \
    && echo "   alt_scraper ya existe, continuando..." \
    || sudo -u postgres createdb -O gli_user alt_scraper

echo ""
echo "==> [4/6] Restaurando dump en alt_scraper..."
sudo -u postgres pg_restore \
    --no-owner \
    --no-acl \
    --no-privileges \
    -d "$DB" \
    "$DUMP"
echo "   pg_restore completado."

echo ""
echo "==> [5/6] Aplicando schema additions (nuevas tablas y columnas)..."
sudo -u postgres psql -d "$DB" -f "$SCHEMA"
echo "   Schema aplicado."

echo ""
echo "==> [5b] Otorgando permisos a gli_user..."
sudo -u postgres psql -d "$DB" -c "
    GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO gli_user;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO gli_user;
    GRANT EXECUTE ON ALL FUNCTIONS        IN SCHEMA public TO gli_user;
"

echo ""
echo "==> [6/6] Verificando integridad post-restore..."
sudo -u postgres psql -d "$DB" -c "
    SELECT tabla, filas FROM (
        SELECT 'asset_metadata'        AS tabla, count(*) AS filas FROM asset_metadata
        UNION ALL
        SELECT 'futures_daily_metrics',           count(*)          FROM futures_daily_metrics
        UNION ALL
        SELECT 'spot_daily_ohlcv',                count(*)          FROM spot_daily_ohlcv
        UNION ALL
        SELECT 'futures_snapshots',               count(*)          FROM futures_snapshots
        UNION ALL
        SELECT 'futures_latest',                  count(*)          FROM futures_latest
    ) t ORDER BY tabla;
"

echo ""
echo "==> Verificando que gli_dashboard sigue intacto..."
sudo -u postgres psql -d gli_dashboard -c "SELECT 'gli_dashboard OK' AS status;"

echo ""
echo "==> Limpiando temporales..."
sudo rm -f "$DUMP" "$SCHEMA" ~/restore_alt_scraper_vps.sh

echo ""
echo "========================================="
echo "  RESTAURACION COMPLETADA CON EXITO"
echo "========================================="
REMOTE
)

# ── 3. Transfer and print instructions ───────────────────────────────────────
TMPSCRIPT=$(mktemp /tmp/restore_alt_scraper_vps_XXXX.sh)
printf '%s\n' "$REMOTE_SCRIPT" > "$TMPSCRIPT"

echo ""
echo "==> 2. Transfiriendo script de restauracion..."
scp "$TMPSCRIPT" "$VPS:~/restore_alt_scraper_vps.sh"
rm -f "$TMPSCRIPT"

echo ""
echo "======================================================"
echo " TRANSFERENCIA COMPLETADA"
echo " Para restaurar en el VPS ejecuta:"
echo ""
echo "   ssh $VPS 'bash ~/restore_alt_scraper_vps.sh'"
echo ""
echo "======================================================"
