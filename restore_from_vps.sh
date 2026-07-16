#!/usr/bin/env bash
# restore_from_vps.sh — Dump de Postgres en el VPS y restaura en Docker local (OrbStack)
# Uso: ./restore_from_vps.sh [user@ip]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOTENV="$SCRIPT_DIR/.env"
DUMP_FILE="$SCRIPT_DIR/alt_scraper_vps.dump"

# Cargar .env
if [[ -f "$DOTENV" ]]; then
    set -a
    # shellcheck disable=SC1090
    source <(grep -v '^\s*#' "$DOTENV" | sed 's/\r//')
    set +a
fi

# Determinar dirección del VPS
VPS="${1:-${VPS_ADDRESS:-}}"
if [[ -z "$VPS" ]]; then
    read -rp "Enter VPS address (e.g. user@ip): " VPS
fi
if [[ -z "$VPS" ]]; then
    echo "ERROR: VPS address is required." >&2
    exit 1
fi

# Extraer parámetros de DATABASE_URL si está presente
LOCAL_DB_USER="gli_user"
LOCAL_DB_NAME="alt_scraper_dev"

if [[ -n "${DATABASE_URL:-}" ]]; then
    EXTRACTED_USER=$(python3 -c "from urllib.parse import urlparse; u = urlparse('$DATABASE_URL'); print(u.username if u.username else '')" 2>/dev/null || echo "")
    EXTRACTED_NAME=$(python3 -c "from urllib.parse import urlparse; u = urlparse('$DATABASE_URL'); print(u.path.lstrip('/') if u.path else '')" 2>/dev/null || echo "")
    if [[ -n "$EXTRACTED_USER" ]]; then LOCAL_DB_USER="$EXTRACTED_USER"; fi
    if [[ -n "$EXTRACTED_NAME" ]]; then LOCAL_DB_NAME="$EXTRACTED_NAME"; fi
fi

# Nombre del contenedor Docker y DB remota
CONTAINER="${DOCKER_POSTGRES_CONTAINER:-gli_postgres}"
REMOTE_DB_NAME="${REMOTE_DB_NAME:-alt_scraper}"

echo "==> Verificando que el contenedor Docker esté corriendo..."
if ! docker inspect "$CONTAINER" --format '{{.State.Status}}' 2>/dev/null | grep -q running; then
    echo "   El contenedor '$CONTAINER' no está corriendo. Levántalo primero."
    exit 1
fi

echo "==> Generando dump en el VPS (puede tardar varios minutos)..."
ssh -t "$VPS" "sudo -u postgres pg_dump -Fc -d $REMOTE_DB_NAME -f /tmp/alt_scraper_export.dump && sudo chmod 644 /tmp/alt_scraper_export.dump && echo DONE"

echo "==> Transfiriendo dump al local ($DUMP_FILE)..."
scp -C -o "ServerAliveInterval=30" -o "ServerAliveCountMax=5" \
    "${VPS}:/tmp/alt_scraper_export.dump" "$DUMP_FILE"

SIZE_MB=$(du -m "$DUMP_FILE" | cut -f1)
echo "   Dump recibido: ${SIZE_MB} MB"

echo "==> Eliminando el dump del VPS..."
ssh -t "$VPS" "sudo rm -f /tmp/alt_scraper_export.dump && echo OK"

echo "==> Terminando conexiones activas en Docker..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d postgres \
    -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$LOCAL_DB_NAME' AND pid <> pg_backend_pid();" \
    > /dev/null 2>&1 || true

echo "==> Recreando base de datos local..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d postgres \
    -c "DROP DATABASE IF EXISTS $LOCAL_DB_NAME;" \
    -c "CREATE DATABASE $LOCAL_DB_NAME OWNER $LOCAL_DB_USER;"

echo "==> Copiando dump al contenedor..."
docker cp "$DUMP_FILE" "${CONTAINER}:/tmp/restore.dump"

echo "==> Creando rol postgres si no existe (requerido por el dump del VPS)..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d postgres \
    -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='postgres') THEN CREATE ROLE postgres SUPERUSER LOGIN; END IF; END \$\$;" \
    > /dev/null || true

echo "==> Restaurando en Docker (pg_restore)..."
docker exec "$CONTAINER" pg_restore \
    -U "$LOCAL_DB_USER" \
    -d "$LOCAL_DB_NAME" \
    --no-owner \
    --role "$LOCAL_DB_USER" \
    /tmp/restore.dump || true   # pg_restore devuelve warnings que no son fatales

echo "==> Otorgando permisos a $LOCAL_DB_USER..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d "$LOCAL_DB_NAME" -c "
    GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA public TO $LOCAL_DB_USER;
    GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $LOCAL_DB_USER;
    GRANT EXECUTE ON ALL FUNCTIONS        IN SCHEMA public TO $LOCAL_DB_USER;
"

echo "==> Verificando tablas restauradas..."
docker exec "$CONTAINER" psql -U "$LOCAL_DB_USER" -d "$LOCAL_DB_NAME" \
    -c "SELECT relname AS tabla, n_live_tup AS filas FROM pg_stat_user_tables WHERE n_live_tup > 0 ORDER BY n_live_tup DESC LIMIT 15;"

echo "==> Limpiando temporales..."
docker exec "$CONTAINER" rm -f /tmp/restore.dump
rm -f "$DUMP_FILE"

echo "==> Restauración completa."
