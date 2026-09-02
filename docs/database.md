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

### 1.5 `futures_klines_15m` (Velas 15m Cerradas / Intradía de Alta Frecuencia)
*Velas OHLCV cerradas oficiales del exchange por `(candle_open_at, symbol, exchange)`.*
- **Identificadores y Tiempos:**
    - `candle_open_at`, `candle_close_at` (Timestamptz): Intervalo exacto de la vela.
    - `symbol`, `exchange`, `base_asset`.
- **OHLCV:** `price_open`, `price_high`, `price_low`, `price_close`, `volume_base`, `volume_usd`.
- **Microestructura (CVD):** `buy_volume_base`, `sell_volume_base`, `volume_delta`, `txn_count`.
- **Trazabilidad y Origen:**
    - `source`: Canal de entrada (`ws`, `rest_reconciled`, `backfill`).
    - `ws_received_at`, `rest_reconciled_at`, `exchange_event_time`, `polled_at`.
- **Escrita por:** `futures_ws_daemon.py` (en vivo vía WebSocket + reconciliador REST continuo) y `klines_15m_backfill.py` (descarga histórica idempotente hacia atrás).

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

### 3.4 `market_cap_history` (Historial Point-in-Time sin Sesgo de Supervivencia)
*PK compuesta `(date, symbol)`. Registra el ranking y capitalización histórica de mercado.*
- `date` (Date), `symbol` (Varchar): Activo base (ej: `BTC`, `SOL`).
- `market_cap_usd` (Decimal 24,4): Capitalización en USD a la fecha del snapshot.
- `rank_raw` (Integer): Posición absoluta en capitalización de mercado.
- `in_top_50` (Boolean): Indica si el activo pertenecía al top 50 (excluyendo stablecoins, staked y wrapped tokens).
- `ever_in_top_50` (Boolean): Flag acumulativo que indica si el activo ha estado alguna vez en el top 50 histórico.
- `source` (Varchar): Fuente del dato (`cmc`, `coingecko`).
- **Alimentado por:** `backfill_market_cap_history.py` y snapshots diarios de `alt_scraper.py`.

---

## 🪙 4. Tabla de Spot

### 4.1 `spot_daily_ohlcv` (Histórico Diario Spot)
*PK compuesta `(date, symbol, exchange)`. La escribe `spot_scraper.py`.*
- `date`, `symbol` (nativo del exchange: `BTCUSDT`, `BTC-USDT`, `BTC-USD`), `exchange` (`binance`, `bybit`, `okx`, `coinbase`).
- **OHLCV:** `price_open`, `price_high`, `price_low`, `price_close`, `volume_base`, `volume_usd`.
- **Microestructura (CVD):** `buy_volume_base`, `sell_volume_base`, `volume_delta`,
  `txn_count`, `buy_txn_count`, `sell_txn_count`.

**Invariante del delta** — se cumple por construcción y hay que mantenerlo:

```
buy_volume_base + sell_volume_base == volume_base        (tolerancia 2%)
volume_delta                       == buy - sell
0 <= buy_txn_count <= txn_count
```

`sell_volume_base` y `volume_delta` **nunca** se importan de una fuente externa: se derivan de
`volume_base - buy_volume_base`. Una fuente externa (Coinalyze, Rubik) solo aporta magnitud si su
volumen reconcilia con el `volume_base` de la fila; si no, se usa únicamente como **ratio** y se
reescala. Romper esto es lo que corrompió el delta hasta la migración `005`: el `buy` salía de un
mercado y el `sell` de otro, y el delta acababa midiendo la diferencia de escala entre los dos.

Auditar en cualquier momento con:

```bash
python audit_spot_delta.py --source db
```

Una correlación **negativa** entre `sign(volume_delta)` y el retorno diario es la firma de columnas
`buy`/`sell` invertidas en el parser de alguna fuente.

La columna `med btxn/txn` del auditor marca con `!` los exchanges cuya mediana de
`buy_txn_count / txn_count` se aleja de 0.5. Un día suelto fuera de la banda `[0.2, 0.8]` es normal
(volumen mínimo, flujo unidireccional); un sesgo sistemático significa que los dos contadores no
describen el mismo mercado.

**Coinbase** cotiza contra **USD**, no USDT (401 pares online frente a 21, y 60% del universo
trackeado frente al 11%). `volume_base` va en unidades del activo base, así que agrega igual que el
resto; el `price_close` difiere del de un par USDT lo que desvíe el peg (<0.1%). Sus velas diarias sí
cierran a **medianoche UTC**. La fuente es Coinalyze (`{BASE}USD.C`, con `bv`/`tx`/`btx` desde
2017-01-01), que reconcilia con el propio `volume_base` y por tanto entra en magnitud, sin degradar a
ratio. **Entra en el pipeline nocturno**, que pide los cuatro venues explícitamente; el default
de `--exchanges` sigue siendo los tres de USDT, así que a mano hay que nombrarlo:
`python spot_scraper.py --exchanges coinbase`.

**Alineación horaria** — todas las fuentes tienen que describir el MISMO día UTC. Las velas diarias
de OKX se piden con `bar=1Dutc`, **nunca** `1D`: en OKX la vela `1D` cierra en el corte de UTC+8, así
que cubre `[D 16:00, D+1 16:00)` UTC. Con `1D` cada fila okx describía otras 24h que la fila de
binance/bybit con la misma `date` (308 bps de desviación media en `price_close`, frente a 23 bps de
bybit) y además descorrelacionaba el delta, porque el `buy_ratio` de Rubik sí viene en días UTC.
Ver `migrations/006_reload_okx_spot_1dutc.sql`; lo fija `tests/test_okx_utc_alignment.py`.

Contraste de sanidad, comparando `price_close` por `(date, activo)` — cualquier exchange que se
salga de un dígito de mediana está en otra ventana temporal:

| | media | mediana |
|---|---|---|
| okx vs binance | 10.2 bps | 4.3 bps |
| bybit vs binance | 23.1 bps | 4.8 bps |

> `IP-USDT` y `TON-USDT` no se pudieron recargar desde OKX (`code 51001`: instrumentos
> deslistados). Se realinearon desde Coinalyze, que sí conserva su histórico y en UTC — ver 4.2.

### 4.2 Activos deslistados — no se borran nunca

La tabla conserva los pares que ya no cotizan, y su última vela marca la fecha de deslistado:

| exchange | vivos | deslistados |
|---|---|---|
| binance | 142 | 6 |
| bybit | 136 | 7 |
| okx | 130 | 2 |
| coinbase | 107 | 0 |

Quitarlos metería **sesgo de supervivencia**: un backtest sobre el universo resultante solo
vería los activos que sobrevivieron y sobreestimaría el retorno. Esto manda sobre cualquier
criterio de limpieza — si un par deslistado tiene datos defectuosos, se **corrigen**, no se
borra la fila.

Caso resuelto así: tras la recarga a `1Dutc` de la 006, `TON-USDT` e `IP-USDT` se quedaron con
velas UTC+8 (212 y 405 bps de desvío) porque OKX ya no sirve su histórico en ningún `bar`.
Coinalyze sí lo conserva (`TONUSDT.3`, `IPUSDT.3`) y en UTC, así que se realinearon con
`reload_okx_delisted_from_coinalyze.py` en vez de eliminarlas: quedaron en 6.5 y 6.8 bps
**conservando la fecha de deslistado exacta**. Se recortan 285 días del arranque de las series,
que Coinalyze no cubre; un inicio más tardío no sesga —lo que sesga es truncar la muerte.

---

## 🔄 5. Migraciones de Base de Datos

Las alteraciones estructurales y correcciones históricas se versionan en `migrations/`:

| Archivo | Propósito |
|---|---|
| `001_futures_intraday_snapshots.sql` | Crea la tabla de snapshots intradía de 48h para operativa live. |
| `002_futures_klines_15m.sql` | Crea la tabla de klines 15m con soporte OHLCV y microestructura. |
| `003_rename_matic_rndr_base_assets.sql` | Normalización de rebrandings de activos base (`MATIC` -> `POL`, `RNDR` -> `RENDER`). |
| `004_futures_klines_ws_metadata.sql` | Añade campos de metadatos y trazabilidad WS/REST a `futures_klines_15m`. |
| `005_fix_spot_volume_delta.sql` | Corrige la fórmula de CVD delta en spot (`sell = volume_base - buy`). |
| `006_reload_okx_spot_1dutc.sql` | Alinea OKX spot a velas diarias `1Dutc` (medianoche UTC en vez de UTC+8). |
| `008_drop_binance_foreign_txn_counts.sql` | Depura transacciones foráneas inconsistentes en Binance spot. |
| `add_market_cap_history.sql` | Crea `market_cap_history` para registro de top-50 point-in-time sin sesgo de supervivencia. |

