# 🏗️ Estructura del Proyecto - Alts-Scraper

Este documento describe la jerarquía de archivos y la función de cada componente en el sistema de recolección de datos.

## 📂 Directorios Principales

- `docs/`: Documentación técnica del proyecto (`database.md`, `project_structure.md`, `README.md`).
- `data/`: Almacenamiento local temporal, caché de metadatos (`asset_metadata.csv`, `data/cache/market_cap_history/`) y CSVs opcionales.
- `migrations/`: Scripts SQL de migración y evolución de esquema versionados.
- `services/`: Archivos de configuración de servicios para distintas plataformas (ej: plists para macOS).
- `tests/`: Suite de pruebas unitarias automatizadas (`unittest`).
- `venv/`: Entorno virtual de Python (no incluido en el repositorio).

---

## 🐍 Scripts Core (Scrapers y Orquestación)

| Archivo | Función |
| :--- | :--- |
| `alt_scraper.py` | Scraper principal de históricos de Futuros (vía Coinalyze API y APIs nativas). |
| `spot_scraper.py` | Scraper de históricos de Spot (Binance, Bybit, OKX 1Dutc, Coinbase USD; con CVD robusto y prioridad DB sobre caché CSV). |
| `run_pipeline.py` | Orquestador diario que ejecuta los scrapers en paralelo (Spot + Futuros) y gestiona la limpieza y persistencia en PostgreSQL/Supabase. |
| `discover_universe.py` | Genera la lista de activos candidatos que han podido estar en el top 50 desde 2020 consultando CoinGecko y `asset_metadata`. |
| `backfill_market_cap_history.py` | Genera el historial diario/semanal de capitalización y pertenencia al top 50 point-in-time sin sesgo de supervivencia (CMC / CoinGecko). |
| `backfill_ls_ranges.py` | Backfill de rangos de ratios Long/Short. |
| `run_backfill.py` | Orquestador de backfill histórico masivo. |

---

## ⚡ Demonios (Servicios en Tiempo Real)

| Archivo | Función |
| :--- | :--- |
| `realtime_daemon.py` | Captura OI, Funding y L/S cada 15 min; actualiza `futures_latest` y `futures_intraday_snapshots` (48h). |
| `futures_ws_daemon.py` | Servicio WebSocket continuo que captura klines de 15m en tiempo real con reconciliación REST en `futures_klines_15m`. |
| `orderbook_daemon.py` | Servicio continuo vía WebSockets para capturar profundidad de libros de órdenes en `orderbook_latest` y `orderbook_snapshots`. Coinbase cotiza contra USD, no USDT, y su libro se guarda entero como el del resto. |

---

## 🗄️ Base de Datos (SQL y Migraciones)

| Archivo / Directorio | Función |
| :--- | :--- |
| `schema.sql` | Definición completa de tablas, índices, vistas materializadas y funciones de actualización. |
| `reset_database.sql` | Script para borrar y recrear la base de datos desde cero. |
| `check_db_size.sql` | Utilidad para monitorear el peso de las tablas e índices en disco. |
| `migrate_market_type.sql` | Script de migración para estructuración de tipos de mercado. |
| `migrations/` | Directorio de migraciones SQL aplicadas (001 a 008 y `add_market_cap_history.sql`). |

---

## 🛠️ Utilidades, Auditoría y Mantenimiento

| Archivo | Función |
| :--- | :--- |
| `audit_spot_delta.py` | Auditoría integral de delta CVD y conteo de transacciones en spot (`sell = volume - buy`, correlación retorno-delta). |
| `reload_okx_spot_1dutc.py` | Recarga histórica de velas diarias de OKX Spot alineadas a medianoche UTC (`bar=1Dutc`). |
| `reload_okx_delisted_from_coinalyze.py` | Realineación histórica UTC de tokens deslistados en OKX (`TON`, `IP`) desde Coinalyze sin sesgo de supervivencia. |
| `klines_15m_backfill.py` | Descarga de forma idempotente las klines históricas de 15m caminando hacia atrás en el tiempo. |
| `orderbook_backfill.py` | Descarga snapshots históricos de Binance/Bybit para llenar huecos de liquidez. |
| `push_klines_backfill_to_vps.sh` | Exporta localmente las klines de backfill a binario comprimido, las sube y las fusiona en el VPS. |
| `restore_from_vps.sh` | Sincroniza la base de datos de producción (VPS) al contenedor Docker de desarrollo local. |
| `notify_vps.sh` | Envío de alertas de estado del VPS vía Telegram en caso de fallos. |
| `verify_integrity.py` | Chequeo de consistencia de datos entre tablas. |

---

## 🚀 Despliegue y Configuración de Servicios

### 🐧 VPS / Systemd (Linux)
- `setup_vps.sh`: Script de instalación automatizada del entorno en un servidor Linux nuevo.
- `vps_run.sh`: Script ejecutor optimizado en VPS para la corrida diaria.
- `vps_regularize_spot.sh`: Regularización integral de spot en VPS con backup previo (migraciones 005/008, recarga OKX 1Dutc, deslistados, backfill Coinbase y auditoría).
- `vps_full_universe_rollout.sh`: Despliegue y sincronización del universo completo sin sesgo de supervivencia.
- `vps_historical_backfill.sh`: Runner para backfill histórico continuo en el VPS.
- `alt-scraper.service.template`: Servicio para la pipeline diaria.
- `alt-scraper-realtime.service.template`: Servicio para el demonio de tiempo real.
- `alt-scraper-orderbook.service.template`: Servicio para el demonio de orderbook.
- `alt-scraper-klines-ws.service.template`: Servicio para el demonio de WebSocket klines.
- `alt-scraper-historical-backfill.service.template`: Servicio para el script de backfill histórico en el VPS.
- `alt-scraper-notify@.service.template`: Servicio systemd on-failure para alertas automáticas a Telegram.
- `alt-scraper.timer`: Temporizador de systemd para ejecutar el pipeline diario (00:15 UTC).

### 🍎 macOS / Launchd
- `manage_services_mac.sh`: Script interactivo de utilidad para instalar, desinstalar, iniciar, detener y monitorizar los demonios locales en macOS.
- `services/macos/*.plist.template`: Plantillas de Launchd (realtime, klines-ws, orderbook). `manage_services_mac.sh install` las renderiza en `~/Library/LaunchAgents` sustituyendo `{{INSTALL_DIR}}` por la ruta local del repo.

---

## 🧪 Pruebas Unitarias (`tests/`)

| Archivo | Suite de Pruebas |
| :--- | :--- |
| `tests/test_futures_ws_daemon.py` | Validación de mapeos y normalizaciones de mensajes WebSocket para Binance, Bybit y OKX. |
| `tests/test_okx_utc_alignment.py` | Verificación de la alineación a medianoche UTC de las velas OKX Spot 1D (`bar=1Dutc`). |
| `tests/test_orderbook_daemon.py` | Pruebas de WebSocket de Orderbook, snapshots de profundidad, spreads e imbalances. |
| `tests/test_spot_delta.py` | Suite de invariantes de Spot Delta, reconciliación Coinalyze/Rubik y prioridad DB vs CSV. |

