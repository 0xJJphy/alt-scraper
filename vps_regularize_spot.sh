#!/bin/bash
# vps_regularize_spot.sh - Deja spot_daily_ohlcv en estado auditado y consistente.
#
# Aplica, en orden y de forma SECUENCIAL, las reparaciones de la tabla de spot. Cada paso
# COMPRUEBA ANTES SI HACE FALTA, así que el script es seguro de relanzar: en una base ya
# regularizada no toca nada y termina en el paso de auditoría.
#
# Eso no es un lujo: el paso 3 (recarga de OKX) borra y vuelve a descargar los 132 símbolos,
# y Rubik sólo sirve ~180 días de taker volume. Relanzarlo a ciegas destruiría el
# buy/sell/delta acumulado noche a noche. Por eso sólo corre si detecta velas en UTC+8.
#
# PASOS
#   0. Backup      pg_dump comprimido de la base entera, ANTES de tocar nada. Aborta si falla.
#   1. 005         volume_delta: rederiva sell/delta donde buy+sell != volume_base.
#   2. 008         anula los buy_txn_count de binance, que venían de otro par.
#   3. 006         recarga OKX con velas 1Dutc (sólo si sigue habiendo velas UTC+8).
#   4. deslistados realinea desde Coinalyze los pares de OKX ya deslistados.
#   5. coinbase    backfill incremental de spot de Coinbase.
#   6. auditoría   audit_spot_delta.py. Su salida decide el código de salida del script.
#
# CONFIGURACIÓN (variables de entorno)
#   DATABASE_URL              obligatoria (del .env)
#   COINALYZE_API_KEY_SPOT    obligatoria para los pasos 3, 4 y 5
#   REG_PYTHON                intérprete a usar             (def: $DIR/venv, si no python3)
#   REG_BACKUP_DIR            dónde dejar el dump           (def: $DIR/backups)
#   REG_BACKUP_KEEP           cuántos dumps conservar       (def: 5)
#   REG_BACKUP_FULL=1         volcar la base ENTERA en vez de sólo las tablas de spot
#   REG_PSQL / REG_PGDUMP     binarios a usar               (def: psql / pg_dump)
#                             Con Postgres en docker:
#                               REG_PSQL="docker exec -i CONTENEDOR psql -U USUARIO -d BASE"
#                               REG_PGDUMP="docker exec CONTENEDOR pg_dump -U USUARIO -d BASE"
#                             (si se fijan, DATABASE_URL ya no se les pasa)
#   REG_COINBASE_START        inicio del backfill           (def: 2017-01-01)
#   REG_SKIP_COINBASE=1       no ejecutar el paso 5
#   REG_COINBASE_CSV=0        no escribir ademas los CSV en data/spot/coinbase
#   REG_SKIP_OKX_RELOAD=1     no ejecutar los pasos 3 y 4
#   REG_FORCE_OKX_RELOAD=1    forzar el paso 3 aunque no se detecten velas UTC+8
#   REG_OKX_BPS_UMBRAL        desviación que dispara el paso 3, en bps (def: 50)
#   REG_LOCK_MINUTOS          minutos de escritura reciente que bloquean el arranque (def: 5)
#   REG_DRY_RUN=1             enseña qué haría y sale, sin escribir ni hacer backup
#   REG_NO_NOTIFY=1           no mandar los avisos de Telegram (para pruebas)
#   REG_ALLOW_ROOT=1          permitir lanzarlo como root (desaconsejado, ver abajo)
#
# SOBRE sudo
# El script lo lanzas como TU usuario, nunca con `sudo bash ...`: lo unico que hay que
# elevar es el acceso a la base, y para eso estan REG_PSQL/REG_PGDUMP. Aun asi, si los
# pones con `sudo -u postgres`, ten en cuenta que el sello de sudo caduca a los 15 min y
# durante la recarga de OKX (~30 min) no hay ninguna llamada a psql: la siguiente pediria
# contrasena y con nohup se quedaria colgada. Lo robusto es al reves — dar una vez los
# GRANT que el script te imprime y lanzarlo entero sin sudo.
#
# OJO con los avisos: notify_vps.sh manda "Alt-scraper ha empezado/terminado" sin decir
# qué servicio es, así que durante este script los Telegram parecen los del pipeline.
#
# RESTAURAR desde un backup (el dump lleva DROP/CREATE de cada tabla que incluye):
#   gunzip -c backups/alt_scraper_spot_AAAAMMDD_HHMMSS.sql.gz | psql "$DATABASE_URL"
# El DROP exige ser DUEÑO de la tabla o superusuario, y no basta con los GRANT que pide el
# script: aquí el dueño es `postgres` aunque el scraper entre como otro usuario. Si la
# restauración falla con "must be owner of table", restaura como postgres.
#
# USO
#   bash vps_regularize_spot.sh
#   REG_DRY_RUN=1 bash vps_regularize_spot.sh
#   nohup bash vps_regularize_spot.sh > /dev/null 2>&1 &     # tarda; mejor en tmux/nohup
#
# NO ejecutarlo a la vez que el pipeline nocturno: comparten la cuota de Coinalyze
# (40 req/min por key) y se pisarían las filas. El script lo detecta y aborta.

set -uo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR" || exit 1

# Como root, todo lo que escriba el script (logs, backups, los CSV de data/spot) queda con
# dueno root, y el pipeline nocturno -que corre como tu usuario- deja de poder escribirlos.
# Lo que si conviene elevar es SOLO psql/pg_dump, via REG_PSQL/REG_PGDUMP.
if [ "$(id -u)" = "0" ] && [ "${REG_ALLOW_ROOT:-0}" != "1" ]; then
    cat >&2 <<'AVISO'
[ABORTADO] No lances este script con sudo: dejaria logs, backups y CSV con dueno root.

Corre como tu usuario y eleva solo el acceso a la base:

  REG_PSQL="sudo -u postgres psql -d TU_BASE" REG_PGDUMP="sudo -u postgres pg_dump -d TU_BASE" bash vps_regularize_spot.sh

Mejor aun: da los permisos una vez (el script te dice cuales) y lanzalo sin sudo.
REG_ALLOW_ROOT=1 salta esta comprobacion.
AVISO
    exit 1
fi

if [ -f "$DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "$DIR/.env"
    set +a
fi

if [ -n "${REG_PYTHON:-}" ]; then
    PYTHON_EXEC="$REG_PYTHON"
elif [ -x "$DIR/venv/bin/python3" ]; then
    PYTHON_EXEC="$DIR/venv/bin/python3"
else
    PYTHON_EXEC="$(command -v python3 || command -v python || echo /usr/bin/python3)"
fi

BACKUP_DIR="${REG_BACKUP_DIR:-$DIR/backups}"
BACKUP_KEEP="${REG_BACKUP_KEEP:-5}"
COINBASE_START="${REG_COINBASE_START:-2017-01-01}"
OKX_BPS_UMBRAL="${REG_OKX_BPS_UMBRAL:-50}"
LOCK_MINUTOS="${REG_LOCK_MINUTOS:-5}"
DRY_RUN="${REG_DRY_RUN:-0}"

mkdir -p "$DIR/logs" "$BACKUP_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$DIR/logs/regularize_spot_$STAMP.log"
START_TIME=$(date +%s)

log()  { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }
paso() { echo | tee -a "$LOG_FILE"; log "===== $* ====="; }

# psql/pg_dump: por defecto con DATABASE_URL; si el usuario fija REG_PSQL (p.ej. un
# docker exec) se usa tal cual, sin pasarle la URL.
if [ -n "${REG_PSQL:-}" ]; then
    psql_run() { $REG_PSQL "$@"; }
else
    psql_run() { psql "$DATABASE_URL" "$@"; }
fi
if [ -n "${REG_PGDUMP:-}" ]; then
    pgdump_run() { $REG_PGDUMP "$@"; }
else
    pgdump_run() { pg_dump "$DATABASE_URL" "$@"; }
fi

# Una consulta que devuelve un solo valor, sin cabeceras ni espacios.
sql1() { psql_run -t -A -c "$1" 2>>"$LOG_FILE" | tr -d '[:space:]'; }

# notify_vps.sh reexporta el .env por su cuenta, asi que silenciarlo desde fuera con
# TELEGRAM_BOT_TOKEN="" no funciona: hace falta no llamarlo.
notificar() {
    [ "${REG_NO_NOTIFY:-0}" = "1" ] && return 0
    bash "$DIR/notify_vps.sh" "$@" >/dev/null 2>&1 || true
}

fallo() {
    log "!! ABORTADO: $*"
    notificar FAILURE "regularize-spot"
    exit 1
}

# ---------------------------------------------------------------- comprobaciones previas
paso "Comprobaciones previas"
log "Directorio: $DIR"
log "Python:     $PYTHON_EXEC"
log "Log:        $LOG_FILE"

[ -n "${DATABASE_URL:-}" ] || [ -n "${REG_PSQL:-}" ] || fallo "DATABASE_URL no está definida (ni REG_PSQL)."
# No basta con -x: en algunos sistemas hay stubs que existen, son ejecutables y fallan al
# invocarlos. Mejor descubrirlo aqui que tras seis minutos de backup.
"$PYTHON_EXEC" -c "import psycopg2, pandas" >/dev/null 2>&1 \n    || fallo "$PYTHON_EXEC no arranca o le faltan psycopg2/pandas. ¿Falta activar el venv?"

for f in audit_spot_delta.py spot_scraper.py \
         migrations/005_fix_spot_volume_delta.sql \
         migrations/008_drop_binance_foreign_txn_counts.sql; do
    [ -f "$DIR/$f" ] || fallo "Falta $f. ¿El repo está actualizado (git pull)?"
done

if [ -z "${REG_PSQL:-}" ]; then command -v psql >/dev/null || fallo "psql no está instalado."; fi
if [ -z "${REG_PGDUMP:-}" ]; then command -v pg_dump >/dev/null || fallo "pg_dump no está instalado."; fi

VIVA="$(sql1 "SELECT 1")"
[ "$VIVA" = "1" ] || fallo "No consigo conectar a la base de datos."

# Con REG_PSQL es facil apuntar a la base equivocada (`-d otra_base`): sin esto, el primer
# sintoma seria un mensaje raro de la guarda de concurrencia, no "esa tabla no esta aqui".
BASE_PSQL="$(sql1 "SELECT current_database()")"
[ "$(sql1 "SELECT to_regclass('spot_daily_ohlcv') IS NOT NULL")" = "t" ]     || fallo "En la base '$BASE_PSQL' no existe spot_daily_ohlcv. ¿Es la base correcta? Mira el DATABASE_URL del .env."
log "Base (psql): $BASE_PSQL"

# Identidad del cluster+base, para cotejarla luego con la de DATABASE_URL.
IDENT_PSQL="$(sql1 "SELECT current_database() || '@' || pg_postmaster_start_time()")"

if [ -z "${COINALYZE_API_KEY_SPOT:-}${COINALYZE_API_KEY:-}" ]; then
    log "!! Sin COINALYZE_API_KEY_SPOT: se saltan los pasos 3, 4 y 5."
    REG_SKIP_OKX_RELOAD=1; REG_SKIP_COINBASE=1
elif [ -z "${COINALYZE_API_KEY_SPOT:-}" ]; then
    log "!! Sólo hay COINALYZE_API_KEY (la de futuros): se comparte cupo de 40 req/min."
fi

# Nadie más puede estar escribiendo: el pipeline nocturno y este script se pisan.
ESCRIBIENDO="$(sql1 "SELECT count(*) FROM spot_daily_ohlcv WHERE updated_at > now() - interval '$LOCK_MINUTOS minutes'")"
if [ "${ESCRIBIENDO:-0}" != "0" ]; then
    fallo "Hay otro proceso escribiendo en spot_daily_ohlcv ($ESCRIBIENDO filas en los últimos $LOCK_MINUTOS min). Espera a que termine el pipeline."
fi

# Lo anterior no basta: una RESTAURACIÓN copia las filas con su updated_at original (de ayer),
# así que pasa esa guarda sin despeinarse y el script se pondría a migrar sobre una tabla a
# medio llenar. Aquí se mira quién está conectado y trabajando, que sí lo delata.
OTROS="$(sql1 "SELECT count(*) FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid()
                  AND backend_type = 'client backend' AND state <> 'idle'")"
if [ "${OTROS:-0}" != "0" ]; then
    log "Hay $OTROS conexión(es) trabajando ahora mismo en la base:"
    psql_run -c "SELECT pid, usename, state, now()-query_start AS lleva, left(query, 70) AS consulta
                 FROM pg_stat_activity
                 WHERE datname = current_database() AND pid <> pg_backend_pid()
                   AND backend_type = 'client backend' AND state <> 'idle';" 2>&1 | tee -a "$LOG_FILE"
    fallo "Otro proceso está usando la base (¿un restore, el pipeline?). Espera a que acabe."
fi
log "Nadie más escribe en spot_daily_ohlcv ni hay consultas activas. Vía libre."

log "Estado inicial por exchange:"
psql_run -c "SELECT exchange, count(*) AS filas, count(DISTINCT symbol) AS simbolos,
                    min(date) AS desde, max(date) AS hasta,
                    count(*) FILTER (WHERE buy_volume_base IS NOT NULL) AS con_delta
             FROM spot_daily_ohlcv GROUP BY exchange ORDER BY exchange;" 2>&1 | tee -a "$LOG_FILE"

# --------------------------------------------------------------- qué hace falta hacer
INV_ROTO="$(sql1 "SELECT count(*) FROM spot_daily_ohlcv
                  WHERE buy_volume_base IS NOT NULL AND volume_base > 0
                    AND abs(buy_volume_base + sell_volume_base - volume_base)/volume_base > 0.02")"
BIN_TXN="$(sql1 "SELECT count(*) FROM spot_daily_ohlcv WHERE exchange='binance' AND buy_txn_count IS NOT NULL")"
OKX_BPS="$(sql1 "WITH b AS (SELECT date, replace(replace(symbol,'-',''),'USDT','') AS base, price_close
                            FROM spot_daily_ohlcv WHERE exchange='binance' AND price_close > 0),
                      o AS (SELECT date, replace(replace(symbol,'-',''),'USDT','') AS base, price_close
                            FROM spot_daily_ohlcv WHERE exchange='okx' AND price_close > 0)
                 SELECT coalesce(round(avg(abs(o.price_close/b.price_close - 1))::numeric*10000, 1), 0)
                 FROM b JOIN o ON o.date = b.date AND o.base = b.base")"

log "Diagnóstico:"
log "  filas que rompen buy+sell==volume_base ... $INV_ROTO   (paso 1 si > 0)"
log "  buy_txn_count de binance ................. $BIN_TXN   (paso 2 si > 0)"
log "  desviación okx vs binance ................ ${OKX_BPS:-?} bps  (paso 3 si > $OKX_BPS_UMBRAL)"

# reload_okx_spot_1dutc.py aborta si la tabla de backup ya existe, asi que si vamos a
# recargar y ya hay una de un pase anterior, se usa un nombre con fecha. El paso 4 tiene que
# mirar en ESA misma tabla, de ahi que el nombre se calcule una vez y se pase a los dos.
OKX_BACKUP_TABLE="spot_daily_ohlcv_okx_pre1dutc"

NECESITA_OKX=0
if [ "${REG_FORCE_OKX_RELOAD:-0}" = "1" ]; then
    NECESITA_OKX=1
    log "  REG_FORCE_OKX_RELOAD=1: se fuerza la recarga de OKX."
elif awk "BEGIN{exit !(${OKX_BPS:-0} > $OKX_BPS_UMBRAL)}"; then
    NECESITA_OKX=1
fi

# ------------------------------------------------------------------------- permisos
# Cada paso escribe de una forma distinta (UPDATE en las migraciones, DELETE+INSERT en las
# recargas, CREATE TABLE para el backup de OKX) y un permiso que falte no se notaria hasta
# la mitad, con el backup ya hecho. Se comprueba antes de tocar nada. Los que solo hacen
# falta para un paso concreto se piden solo si ese paso va a correr.
USUARIO_DB="$(sql1 "SELECT current_user")"
FALTAN=""

priv() {  # priv <tabla> <privilegio> <para que>
    if [ "$(sql1 "SELECT has_table_privilege('$1','$2')")" != "t" ]; then
        FALTAN="$FALTAN
    GRANT $2 ON $1 TO $USUARIO_DB;   -- $3"
    fi
}

priv spot_daily_ohlcv SELECT "leer y volcar el backup"
priv spot_daily_ohlcv UPDATE "migraciones 005 y 008"
priv spot_daily_ohlcv INSERT "backfill y recargas"
if [ "${REG_SKIP_OKX_RELOAD:-0}" != "1" ]; then
    priv spot_daily_ohlcv DELETE "reemplazo de las velas UTC+8"
    if [ "$(sql1 "SELECT has_schema_privilege('public','CREATE')")" != "t" ]; then
        FALTAN="$FALTAN
    GRANT CREATE ON SCHEMA public TO $USUARIO_DB;   -- tabla de backup de OKX"
    fi
fi
[ "${REG_SKIP_COINBASE:-0}" = "1" ] || priv exchanges INSERT "alta de coinbase en el catalogo"

if [ -n "$FALTAN" ]; then
    log "A $USUARIO_DB (por psql, pasos 1 y 2) le faltan permisos. Como superusuario:$FALTAN"
    fallo "Permisos insuficientes (ver arriba). No se toca nada."
fi
log "Permisos de $USUARIO_DB (psql): correctos para los pasos que van a correr."

# Los pasos 3, 4 y 5 NO pasan por psql: conectan con psycopg2 usando DATABASE_URL, que
# puede ser otro usuario distinto (tipico si lanzas las migraciones con `sudo -u postgres`
# pero el scraper entra con su propia cuenta). Comprobar solo el lado psql da un OK enganoso.
if [ "${REG_SKIP_OKX_RELOAD:-0}" != "1" ] || [ "${REG_SKIP_COINBASE:-0}" != "1" ]; then
    [ -n "${DATABASE_URL:-}" ] || fallo "Los pasos 3/4/5 conectan por psycopg2 con DATABASE_URL y no esta definida. Definela en .env, o salta esos pasos con REG_SKIP_OKX_RELOAD=1 REG_SKIP_COINBASE=1."
    SALIDA_PY="$("$PYTHON_EXEC" - <<'PYEOF'
import os
import psycopg2
try:
    con = psycopg2.connect(os.environ["DATABASE_URL"])
except Exception as e:
    print("ERROR " + str(e).strip().replace(chr(10), " "))
    raise SystemExit(0)
cur = con.cursor()
cur.execute("SELECT current_user")
usuario = cur.fetchone()[0]
print("USUARIO " + usuario)
cur.execute("SELECT current_database() || '@' || pg_postmaster_start_time()")
print("IDENT " + "".join(str(cur.fetchone()[0]).split()))
for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
    cur.execute("SELECT has_table_privilege('spot_daily_ohlcv', %s)", (priv,))
    if not cur.fetchone()[0]:
        print("FALTA     GRANT " + priv + " ON spot_daily_ohlcv TO " + usuario + ";")
cur.execute("SELECT has_schema_privilege('public', 'CREATE')")
if not cur.fetchone()[0]:
    print("FALTA     GRANT CREATE ON SCHEMA public TO " + usuario + ";")
con.close()
PYEOF
)"
    case "$SALIDA_PY" in
        ERROR*) log "$SALIDA_PY"; fallo "No consigo conectar con DATABASE_URL, que es por donde van los pasos 3, 4 y 5." ;;
    esac
    USUARIO_PY="$(printf '%s' "$SALIDA_PY" | sed -n 's/^USUARIO //p')"
    IDENT_PY="$(printf '%s' "$SALIDA_PY" | sed -n 's/^IDENT //p')"
    # Si psql y DATABASE_URL apuntan a bases distintas, las migraciones irian a una y las
    # recargas a otra, cada una dejando su mitad del trabajo. Mejor no empezar.
    if [ "$(echo "$IDENT_PSQL" | tr -d '[:space:]')" != "$IDENT_PY" ]; then
        log "psql apunta a          : $IDENT_PSQL"
        log "DATABASE_URL apunta a  : $IDENT_PY"
        fallo "psql y DATABASE_URL no son la misma base. Las migraciones y las recargas acabarian en sitios distintos."
    fi
    FALTAN_PY="$(printf '%s' "$SALIDA_PY" | sed -n 's/^FALTA //p')"
    if [ -n "$FALTAN_PY" ]; then
        log "A $USUARIO_PY (DATABASE_URL, pasos 3/4/5) le faltan permisos. Como superusuario:"
        log "$FALTAN_PY"
        fallo "Permisos insuficientes para los pasos 3/4/5. No se toca nada."
    fi
    log "Permisos de $USUARIO_PY (DATABASE_URL): correctos."
fi

if [ "$DRY_RUN" = "1" ]; then
    paso "DRY RUN: nada se ha ejecutado"
    log "Se haría: backup siempre;"
    log "  paso 1 (005)      -> $([ "${INV_ROTO:-0}" != "0" ] && echo SI || echo 'no hace falta')"
    log "  paso 2 (008)      -> $([ "${BIN_TXN:-0}" != "0" ] && echo SI || echo 'no hace falta')"
    log "  paso 3 (okx 1Dutc)-> $([ "$NECESITA_OKX" = "1" ] && echo SI || echo 'no hace falta')"
    log "  paso 4 (deslistados) y paso 5 (coinbase) -> según REG_SKIP_*"
    exit 0
fi

notificar START "regularize-spot"

# --------------------------------------------------------------------- 0. BACKUP
paso "Paso 0/6 — Backup de la base de datos"
# Por defecto el dump se limita a lo que este script puede modificar: spot_daily_ohlcv, sus
# tablas de backup y el catalogo exchanges. No es tacaneria — futures_klines_15m son 14 de
# los 15 GB de la base y este script no la toca en ningun paso, asi que incluirla convierte
# un volcado de ~30 MB y segundos en uno de 2,3 GB y seis minutos, cinco veces guardado.
# Lo que hay que poder restaurar si algo sale mal cabe entero en el dump dirigido.
if [ "${REG_BACKUP_FULL:-0}" = "1" ]; then
    BACKUP_FILE="$BACKUP_DIR/alt_scraper_full_$STAMP.sql.gz"
    DUMP_ARGS=(--clean --if-exists)
    MIN_LIBRE_KB=$((6 * 1024 * 1024))
else
    BACKUP_FILE="$BACKUP_DIR/alt_scraper_spot_$STAMP.sql.gz"
    DUMP_ARGS=(--clean --if-exists -t 'spot_daily_ohlcv*' -t exchanges)
    MIN_LIBRE_KB=$((512 * 1024))
fi

# --clean --if-exists no es cosmetico: sin el, el dump no lleva DROP y restaurarlo encima de
# la tabla existente falla con "relation already exists" y deja el desaguisado a medias.

LIBRE_KB="$(df -Pk "$BACKUP_DIR" 2>/dev/null | awk 'NR==2{print $4}')"
if [ -n "${LIBRE_KB:-}" ] && [ "$LIBRE_KB" -lt "$MIN_LIBRE_KB" ]; then
    fallo "Quedan $((LIBRE_KB/1024)) MB libres en $BACKUP_DIR y hacen falta $((MIN_LIBRE_KB/1024)) MB. No se toca nada."
fi

log "Volcando a $BACKUP_FILE ..."
if ! pgdump_run ${DUMP_ARGS[@]+"${DUMP_ARGS[@]}"} | gzip > "$BACKUP_FILE"; then
    rm -f "$BACKUP_FILE"
    fallo "pg_dump falló. No se toca nada."
fi
TAM=$(wc -c < "$BACKUP_FILE" 2>/dev/null || echo 0)
# El dump dirigido ronda 15-25 MB comprimidos; el completo, 2,3 GB. Por debajo de 1 MB
# es un volcado roto (pg_dump puede salir con 0 tras escribir solo la cabecera).
if [ "$TAM" -lt 1000000 ]; then
    fallo "El backup son sólo $TAM bytes: parece un volcado incompleto. No se toca nada. ($BACKUP_FILE)"
fi
if ! gzip -t "$BACKUP_FILE" 2>/dev/null; then
    fallo "El backup no pasa la comprobación de integridad de gzip. No se toca nada."
fi
log "Backup OK: $(du -h "$BACKUP_FILE" | cut -f1)"

# Rotación: conservar sólo los N más recientes.
# Se rotan por separado: un dirigido no puede desplazar al ultimo completo, ni al reves.
PATRON="$(basename "$BACKUP_FILE" | sed 's/_[0-9]\{8\}_[0-9]\{6\}\.sql\.gz$//')"
ls -1t "$BACKUP_DIR/$PATRON"_*.sql.gz 2>/dev/null | tail -n +$((BACKUP_KEEP + 1)) | while read -r viejo; do
    log "  rotando (borro backup antiguo): $(basename "$viejo")"
    rm -f "$viejo"
done

# ------------------------------------------------------------------------ 1. 005
paso "Paso 1/6 — Migración 005 (volume_delta)"
if [ "${INV_ROTO:-0}" = "0" ]; then
    log "0 filas rompen el invariante. Nada que hacer."
else
    log "$INV_ROTO filas a reparar..."
    psql_run -v ON_ERROR_STOP=1 < "$DIR/migrations/005_fix_spot_volume_delta.sql" 2>&1 | tee -a "$LOG_FILE"
    [ "${PIPESTATUS[0]}" -eq 0 ] || fallo "La migración 005 falló. La transacción se deshizo sola; backup en $BACKUP_FILE"
fi

# ------------------------------------------------------------------------ 2. 008
paso "Paso 2/6 — Migración 008 (contadores de binance)"
if [ "${BIN_TXN:-0}" = "0" ]; then
    log "binance no tiene buy_txn_count. Nada que hacer."
else
    log "$BIN_TXN filas con buy_txn_count de otro par..."
    psql_run -v ON_ERROR_STOP=1 < "$DIR/migrations/008_drop_binance_foreign_txn_counts.sql" 2>&1 | tee -a "$LOG_FILE"
    [ "${PIPESTATUS[0]}" -eq 0 ] || fallo "La migración 008 falló. Backup en $BACKUP_FILE"
fi

# ------------------------------------------------------------- 3. recarga OKX 1Dutc
paso "Paso 3/6 — Recarga de OKX en velas 1Dutc"
if [ "${REG_SKIP_OKX_RELOAD:-0}" = "1" ]; then
    log "REG_SKIP_OKX_RELOAD=1: saltado."
elif [ "$NECESITA_OKX" != "1" ]; then
    log "OKX está a ${OKX_BPS} bps de binance (umbral $OKX_BPS_UMBRAL): ya está en UTC. Saltado."
    log "  -> importante: relanzarlo sin necesidad borraría el delta de Rubik acumulado."
elif [ ! -f "$DIR/reload_okx_spot_1dutc.py" ]; then
    log "!! Falta reload_okx_spot_1dutc.py: no se puede recargar. Sigo con el resto."
else
    if [ "$(sql1 "SELECT to_regclass('$OKX_BACKUP_TABLE') IS NOT NULL")" = "t" ]; then
        OKX_BACKUP_TABLE="spot_daily_ohlcv_okx_pre1dutc_$STAMP"
        log "Ya hay una tabla de backup de un pase anterior; esta vez se usa $OKX_BACKUP_TABLE"
    fi
    log "OKX a ${OKX_BPS} bps de binance: sigue en UTC+8. Recargando (esto tarda ~20-30 min)..."
    "$PYTHON_EXEC" -u "$DIR/reload_okx_spot_1dutc.py" --backup-table "$OKX_BACKUP_TABLE" 2>&1 | tee -a "$LOG_FILE"
    [ "${PIPESTATUS[0]}" -eq 0 ] || fallo "La recarga de OKX falló. Backup en $BACKUP_FILE"
fi

# ------------------------------------------------- 4. deslistados de OKX -> Coinalyze
paso "Paso 4/6 — Pares deslistados de OKX (realineado desde Coinalyze)"
if [ "${REG_SKIP_OKX_RELOAD:-0}" = "1" ]; then
    log "REG_SKIP_OKX_RELOAD=1: saltado."
elif [ ! -f "$DIR/reload_okx_delisted_from_coinalyze.py" ]; then
    log "!! Falta reload_okx_delisted_from_coinalyze.py. Sigo con el resto."
else
    # El script ya decide por símbolo: no toca nada si el backup está incompleto o si
    # Coinalyze acabase antes que lo guardado (perderíamos la fecha de deslistado).
    "$PYTHON_EXEC" -u "$DIR/reload_okx_delisted_from_coinalyze.py" --backup-table "$OKX_BACKUP_TABLE" 2>&1 | tee -a "$LOG_FILE"
    [ "${PIPESTATUS[0]}" -eq 0 ] || log "!! El realineado de deslistados falló. Sigo; se revisa a mano."
fi

# ------------------------------------------------------------------- 5. coinbase
paso "Paso 5/6 — Backfill de Coinbase spot"
if [ "${REG_SKIP_COINBASE:-0}" = "1" ]; then
    log "REG_SKIP_COINBASE=1: saltado."
else
    # La tabla `exchanges` es un catalogo: schema.sql siembra coinbase, pero una base que ya
    # existia nunca recibe esa fila. Sin FK no rompe el backfill, aunque conviene cuadrarla.
    psql_run -c "INSERT INTO exchanges (name, code, display_name)
                 VALUES ('coinbase', 'C', 'Coinbase') ON CONFLICT (name) DO NOTHING;"         2>&1 | tee -a "$LOG_FILE"

    # Incremental: get_incremental_start() sólo baja lo que falte desde la última fila.
    CSV_FLAG=""
    [ "${REG_COINBASE_CSV:-1}" = "1" ] && CSV_FLAG="--csv"
    log "Backfill desde $COINBASE_START (incremental; la primera vez tarda ~40 min)..."
    "$PYTHON_EXEC" -u "$DIR/spot_scraper.py" --exchanges coinbase --full-universe \
        --start "$COINBASE_START" $CSV_FLAG 2>&1 | tee -a "$LOG_FILE"
    [ "${PIPESTATUS[0]}" -eq 0 ] || fallo "El backfill de Coinbase falló. Backup en $BACKUP_FILE"
fi

# ------------------------------------------------------------------ 6. auditoría
paso "Paso 6/6 — Auditoría"
"$PYTHON_EXEC" -u "$DIR/audit_spot_delta.py" --source db 2>&1 | tee -a "$LOG_FILE"
AUDIT_RC="${PIPESTATUS[0]}"

psql_run -c "SELECT exchange, count(*) AS filas, count(DISTINCT symbol) AS simbolos,
                    min(date) AS desde, max(date) AS hasta,
                    count(*) FILTER (WHERE buy_volume_base IS NOT NULL) AS con_delta
             FROM spot_daily_ohlcv GROUP BY exchange ORDER BY exchange;" 2>&1 | tee -a "$LOG_FILE"

DUR=$(( $(date +%s) - START_TIME ))
DUR_H=$(printf '%dh %dm %ds' $((DUR/3600)) $((DUR%3600/60)) $((DUR%60)))

paso "Resumen"
log "Duración: $DUR_H"
log "Backup:   $BACKUP_FILE"
log "Log:      $LOG_FILE"

if [ "$AUDIT_RC" -eq 0 ]; then
    log "RESULTADO: base regularizada, sin violaciones del invariante."
    notificar SUCCESS "regularize-spot" "$DUR_H"
    exit 0
else
    log "RESULTADO: la auditoría sigue encontrando violaciones. Revisa el log."
    notificar FAILURE "regularize-spot"
    exit 1
fi
