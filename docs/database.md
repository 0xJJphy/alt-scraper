# 🗄️ Documentación Completa de Base de Datos - Alts-Scraper

Esta es la referencia técnica exhaustiva de todas las tablas, campos y procesos de actualización del sistema.

---

## 🕒 Frecuencias de Actualización (Moving Forward)

| Proceso | Tabla Destino | Frecuencia |
| :--- | :--- | :--- |
| **Realtime Futures** | `futures_latest` | Cada 15 min |
| **Futures Klines WS** | `futures_klines_15m` | WebSocket; al cierre de cada vela 15m + reconciliación REST |
| **Realtime Orderbook** | `orderbook_latest` | Cada 60 seg |
| **Snapshots Futures** | `futures_snapshots` | Cada 4 horas* (Sincronizado) |
| **Snapshots Orderbook** | `orderbook_snapshots` | Cada 4 horas (00, 04, 08, 12, 16, 20 UTC) |
| **Pipeline Diaria** | `_daily_metrics` | Diario (00:15 UTC) |

---

## 📊 1. Tablas de Futuros (Derivados)

### 1.1 `futures_daily_metrics` (Histórico Diario)
*Fila única por `(date, symbol, exchange)`.*
- **Identificadores:**
    - `date` (Date): Fecha UTC del cierre.
    - `symbol` (Varchar): Símbolo completo (ej: `BTCUSDT_PERP.A`).
    - `exchange` (Varchar): Nombre del exchange (`binance`, `bybit`, `okx`).
    - `base_asset` (Varchar): Siglas del activo (`BTC`, `ETH`).
- **Open Interest (USD):**
    - `oi_usd_open`, `oi_usd_high`, `oi_usd_low`, `oi_usd_close` (Decimal 24,4).
- **Funding Rate (Tasas):**
    - `funding_open`, `funding_high`, `funding_low`, `funding_close` (Decimal 18,10).
- **Predicted Funding (Tasas Proyectadas):**
    - `pred_funding_open`, `pred_funding_high`, `pred_funding_low`, `pred_funding_close` (Decimal 18,10).
- **Ratios de Posicionamiento:** 
    - `ls_ratio`: Ratio genérico (Longs/Shorts).
    - `ls_acc_global`: Ratio de cuentas globales (Account L/S).
    - `ls_acc_top`: Ratio de cuentas de Top Traders.
    - `ls_pos_top`: Ratio de posiciones de Top Traders.
    - `longs_qty`, `shorts_qty`: Cantidades brutas de posiciones abiertas.
- **Liquidaciones (USD):** 
    - `liq_longs`, `liq_shorts`, `liq_total` (Decimal 24,4).
- **OHLCV (Precio y Volumen):** 
    - `price_open`, `price_high`, `price_low`, `price_close` (Decimal 24,8).
    - `volume_usd` (Decimal 24,4): Volumen estimado en dólares.
    - `volume_base` (Decimal 24,8): Volumen total en el activo base.
- **Microestructura (CVD y Transacciones):** 
    - `buy_volume_base`, `sell_volume_base`: Volumen segmentado por dirección.
    - `volume_delta`: CVD (Buy Volume - Sell Volume).
    - `txn_count`, `buy_txn_count`, `sell_txn_count`: Conteo de operaciones (BigInt).
- **High/Low Intradía (Calculados vía Snapshots):** 
    - `ls_acc_global_high`, `ls_acc_global_low`.
    - `ls_acc_top_high`, `ls_acc_top_low`.
    - `ls_pos_top_high`, `ls_pos_top_low`.

### 1.2 `futures_snapshots` (Capturas Intradía)
*Historial de alta frecuencia para análisis de correlación.*
- `snapshot_at` (Timestamptz): Momento exacto de la captura.
- `symbol`, `exchange`, `base_asset`.
- `oi_usd`, `funding`, `ls_acc_global`, `ls_acc_top`, `ls_pos_top`, `price`.

### 1.3 `futures_intraday_snapshots` (Paper Live / 48h)

Tabla operativa de corto plazo escrita en cada poll de `realtime_daemon.py`. Conserva el ultimo camino observado para auditar fills, entradas tardias y SL/TP del dashboard GLI Paper Live. Retencion: 48 horas. No sustituye a `futures_snapshots`, que sigue siendo la fuente 4h para agregados historicos.

### 1.4 `futures_latest` (Tiempo Real / Frontend)
*Mantiene solo la última actualización para visualización inmediata.*
- `symbol`, `exchange`, `base_asset`.
- `oi_usd`, `funding`, `pred_funding`, `ls_acc_global`, `ls_acc_top`, `ls_pos_top`, `price`.
- `liq_longs_acc`, `liq_shorts_acc`: Liquidaciones acumuladas desde las 00:00 UTC.
- `polled_at`, `updated_at`.

---

## 📚 2. Tablas de Orderbook (Liquidez)

### 2.1 `orderbook_snapshots` (Profundidad Histórica)
*Capturas cada 4 horas del estado del libro de órdenes.*
- `snapshot_at` (Timestamptz), `symbol`, `exchange`, `base_asset`, `market_type` (spot/futures).
- `mid_price`, `best_bid`, `best_ask`, `spread_bps`.
- `depth_coverage_pct`: % de profundidad real que el scraper pudo capturar (importante para validez).
- **Métricas por Banda (1%, 2.5%, 5%, 10% de distancia al Mid):**
    - `bid_qty_Xpct`, `ask_qty_Xpct`: Volumen total de órdenes en esa banda.
    - `bid_levels_Xpct`, `ask_levels_Xpct`: Cantidad de niveles de precio (densidad).
    - `imbalance_Xpct`: Sesgo del libro `(bid-ask)/(bid+ask)`. Valores > 0 indican presión compradora.

### 2.2 `orderbook_daily_metrics` (Agregación Diaria)
*Resumen de volatilidad de liquidez.*
- `spread_bps_open`, `spread_bps_high`, `spread_bps_low`, `spread_bps_close`.
- `bid_qty_Xpct_close`, `ask_qty_Xpct_close`: Profundidad al cierre del día (20:00 UTC snapshot).
- `imbalance_Xpct_high`, `imbalance_Xpct_low`: Extremos de presión compradora/vendedora del día.
- `avg_depth_coverage_pct`, `snapshot_count` (0-6).

### 2.3 `orderbook_latest` (Live Orderbook)
*Actualización cada 60 segundos vía WebSockets.*
- Estructura idéntica a `orderbook_snapshots`. Se usa para el "Live Spread" e "Imbalance" en la UI.

---

## 🏷️ 3. Tablas de Soporte y Metadata

### 3.1 `asset_metadata` (Clasificación)
- `symbol` (PK): Siglas del activo (ej: `SOL`).
- `narrative`: Categoría principal (ej: `DePIN`, `Layer 1`, `AI`).
- `is_filtered`: Boolean. `True` para stablecoins/wrapped tokens (evita ruido en alertas).
- `market_cap`, `market_cap_rank`: Datos frescos de CoinGecko.

### 3.2 `exchanges` (Configuración)
- `id`, `name`, `code` (ej: 'A' para Binance, '6' para Bybit).
- `display_name`, `is_active`.

### 3.3 `symbols` (Mapeo)
- `base_asset`, `quote_asset`, `symbol` (nombre nativo en el exchange).
- `exchange_id`, `contract_type`, `is_active`, `first_data_date`, `last_data_date`.
