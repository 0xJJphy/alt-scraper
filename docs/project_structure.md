# 🏗️ Estructura del Proyecto - Alts-Scraper

Este documento describe la jerarquía de archivos y la función de cada componente en el sistema de recolección de datos.

## 📂 Directorios Principales

- `docs/`: Documentación técnica del proyecto.
- `data/`: Almacenamiento local temporal y caché de metadatos (`asset_metadata.csv`).
- `venv/`: Entorno virtual de Python (no incluido en el repositorio).

---

## 🐍 Scripts Core (Scrapers)

| Archivo | Función |
| :--- | :--- |
| `alt_scraper.py` | Scraper principal de históricos de Futuros (vía Coinalyze API). |
| `spot_scraper.py` | Scraper de históricos de Spot (vía Binance/Bybit/OKX APIs). |
| `run_pipeline.py` | Orquestador diario que ejecuta los scrapers y limpia la base de datos. |

---

## ⚡ Demonios (Servicios en Tiempo Real)

| Archivo | Función |
| :--- | :--- |
| `realtime_daemon.py` | Servicio que captura OI, Funding y L/S cada 15 min. |
| `orderbook_daemon.py` | Servicio continuo vía WebSockets para capturar profundidad de libro. |

---

## 🗄️ Base de Datos (SQL)

| Archivo | Función |
| :--- | :--- |
| `schema.sql` | Definición completa de tablas, índices, vistas y funciones. |
| `reset_database.sql` | Script para borrar y recrear la base de datos desde cero. |
| `check_db_size.sql` | Utilidad para monitorear el peso de las tablas en disco. |
| `migrate_market_type.sql` | Script de migración para cambios estructurales en el esquema. |

---

## 🛠️ Utilidades y Mantenimiento

| Archivo | Función |
| :--- | :--- |
| `orderbook_backfill.py` | Descarga snapshots históricos de Binance/Bybit para llenar huecos. |
| `restore_from_vps.sh` | Sincroniza la base de datos de producción (VPS) a local. |
| `notify_vps.sh` | Envío de alertas de estado del VPS vía Telegram. |
| `verify_integrity.py` | Chequeo de consistencia de datos entre tablas. |

---

## 🚀 Despliegue (VPS/Systemd)

- `alt-scraper.service.template`: Plantilla para el servicio de la pipeline diaria.
- `alt-scraper-realtime.service.template`: Plantilla para el demonio de tiempo real.
- `alt-scraper-orderbook.service.template`: Plantilla para el demonio de orderbook.
- `alt-scraper.timer`: Configuración del cron de systemd (00:15 UTC).
- `setup_vps.sh`: Script de instalación automática en un servidor Linux nuevo.
- `vps_run.sh`: Script de ejecución optimizado para el entorno de producción.
