# 🏗️ Estructura del Proyecto - Alts-Scraper

Este documento describe la jerarquía de archivos y la función de cada componente en el sistema de recolección de datos.

## 📂 Directorios Principales

- `docs/`: Documentación técnica del proyecto.
- `data/`: Almacenamiento local temporal y caché de metadatos (`asset_metadata.csv`).
- `services/`: Archivos de configuración de servicios para distintas plataformas (ej: plists para macOS).
- `tests/`: Suite de pruebas unitarias.
- `venv/`: Entorno virtual de Python (no incluido en el repositorio).

---

## 🐍 Scripts Core (Scrapers y Orquestación)

| Archivo | Función |
| :--- | :--- |
| `alt_scraper.py` | Scraper principal de históricos de Futuros (vía Coinalyze API). |
| `spot_scraper.py` | Scraper de históricos de Spot (vía Binance/Bybit/OKX APIs). |
| `run_pipeline.py` | Orquestador diario que ejecuta los scrapers y limpia la base de datos (retención de 48h de intradía y persistencia permanente de snapshots de futuros). |
| `discover_universe.py` | Genera la lista de activos candidatos que han podido estar en el top 50 desde 2020 consultando CoinGecko y asset_metadata. |

---

## ⚡ Demonios (Servicios en Tiempo Real)

| Archivo | Función |
| :--- | :--- |
| `realtime_daemon.py` | Servicio que captura OI, Funding y L/S cada 15 min. |
| `futures_ws_daemon.py` | Servicio WebSocket continuo que captura klines de 15m en tiempo real con reconciliación REST. |
| `orderbook_daemon.py` | Servicio continuo vía WebSockets para capturar profundidad del libro de órdenes. |

---

## 🗄️ Base de Datos (SQL)

| Archivo | Función |
| :--- | :--- |
| `schema.sql` | Definición completa de tablas, índices, vistas y funciones (incluyendo la purga optimizada). |
| `reset_database.sql` | Script para borrar y recrear la base de datos desde cero. |
| `check_db_size.sql` | Utilidad para monitorear el peso de las tablas en disco. |
| `migrate_market_type.sql` | Script de migración para cambios estructurales en el esquema. |

---

## 🛠️ Utilidades y Mantenimiento

| Archivo | Función |
| :--- | :--- |
| `klines_15m_backfill.py` | Descarga de forma idempotente las klines históricas de 15m caminando hacia atrás en el tiempo. |
| `push_klines_backfill_to_vps.sh` | Exporta localmente las klines de backfill a binario comprimido, las sube y las fusiona en la base de datos del VPS. |
| `restore_from_vps.sh` | Sincroniza la base de datos de producción (VPS) al contenedor Docker de desarrollo local. |
| `orderbook_backfill.py` | Descarga snapshots históricos de Binance/Bybit para llenar huecos. |
| `notify_vps.sh` | Envío de alertas de estado del VPS vía Telegram en caso de fallos. |
| `verify_integrity.py` | Chequeo de consistencia de datos entre tablas. |

---

## 🚀 Despliegue y Configuración de Servicios

### 🐧 VPS / Systemd (Linux)
- `alt-scraper.service.template`: Servicio para la pipeline diaria.
- `alt-scraper-realtime.service.template`: Servicio para el demonio de tiempo real.
- `alt-scraper-orderbook.service.template`: Servicio para el demonio de orderbook.
- `alt-scraper-klines-ws.service.template`: Servicio para el demonio de WebSocket klines.
- `alt-scraper-historical-backfill.service.template`: Servicio para el script de backfill histórico en el VPS.
- `alt-scraper-notify@.service.template`: Servicio systemd on-failure para alertas automáticas a Telegram.
- `alt-scraper.timer`: Temporizador de systemd para ejecutar el pipeline diario (00:15 UTC).
- `setup_vps.sh`: Script de instalación automatizada del entorno en un servidor Linux nuevo.
- `vps_run.sh`: script ejecutor optimizado en VPS.

### 🍎 macOS / Launchd
- `manage_services_mac.sh`: Script interactivo de utilidad para instalar, desinstalar, iniciar, detener y monitorizar los demonios locales en macOS.
- `services/macos/*.plist.template`: Plantillas de Launchd (realtime, klines-ws, orderbook). `manage_services_mac.sh install` las renderiza en `~/Library/LaunchAgents` sustituyendo `{{INSTALL_DIR}}` por la ruta local del repo, igual que los `*.service.template` de systemd.

---

## 🧪 Pruebas Unitarias
- `tests/test_futures_ws_daemon.py`: Suite de validación de mapeos y normalizaciones de mensajes de WebSocket para Binance, Bybit y OKX.

