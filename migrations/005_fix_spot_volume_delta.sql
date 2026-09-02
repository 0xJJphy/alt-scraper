-- =============================================================================
-- 005 — Reparación del volume_delta corrupto en spot_daily_ohlcv
-- =============================================================================
--
-- QUÉ PASÓ
-- --------
-- patch_missing_metrics() rellenaba sell_volume_base desde una fuente distinta a la que
-- había producido buy_volume_base en esa misma fila, y luego calculaba
-- volume_delta = buy - sell mezclando las dos series. El número resultante no medía
-- presión compradora sino la diferencia de escala entre dos mercados distintos.
--
--   binance: buy venía de las klines (par USDT) y sell de Coinalyze, que por la cadena de
--            fallback podía ser el par FDUSD o USDC. El ratio sell/volume_base llegó a
--            oscilar entre 0.000 (2023-08, FDUSD recién listado) y 1.07, con delta/volume
--            entre +0.50 y -0.55. XRP acumuló ~2 años seguidos de sesgo comprador falso.
--            Mismo defecto en buy_txn_count → sell_txn_count negativo (63 filas solo en BTC).
--
--   okx:     buy/sell venían de Rubik taker-volume, que devuelve [ts, sellVol, buyVol]
--            — sell PRIMERO — y el parser los leía al revés: el signo del delta estaba
--            invertido. Además Rubik agrega TODOS los pares spot del activo, así que su
--            magnitud nunca cuadró con el volume_base del par -USDT.
--
--   bybit:   correcto. buy y sell salían los dos de Coinalyze. No se toca.
--
-- QUÉ HACE ESTA MIGRACIÓN
-- -----------------------
-- Todo es derivable de columnas que ya están bien en la propia tabla, así que la reparación
-- es determinista y no gasta cuota de API. Ojo: NO basta con re-ejecutar el scraper —
-- get_incremental_start() solo re-descarga 14 días de solape y el upsert usa
-- COALESCE(EXCLUDED.x, tabla.x), que nunca limpia un valor viejo.
--
-- El arreglo del código va en spot_scraper.py (mismo PR); esto solo sanea el histórico.
--
-- USO:
--   psql "$DATABASE_URL" -f migrations/005_fix_spot_volume_delta.sql
-- Correrlo antes en dev. Auditar con:  python audit_spot_delta.py --source db
-- =============================================================================

BEGIN;

-- Tolerancia: por debajo de esto asumimos que buy y sell describen el mismo mercado.
-- (2%, igual que RECONCILE_TOL en spot_scraper.py)

-- ---------------------------------------------------------------------------
-- Estado ANTES
-- ---------------------------------------------------------------------------
SELECT
    'ANTES' AS momento,
    exchange,
    COUNT(*) FILTER (WHERE buy_volume_base IS NOT NULL)                        AS con_delta,
    COUNT(*) FILTER (
        WHERE buy_volume_base IS NOT NULL AND volume_base > 0
          AND ABS(buy_volume_base + sell_volume_base - volume_base) / volume_base > 0.02
    )                                                                          AS invariante_roto,
    COUNT(*) FILTER (WHERE sell_volume_base < 0 OR buy_volume_base < 0)        AS negativos,
    COUNT(*) FILTER (WHERE buy_txn_count > txn_count OR sell_txn_count < 0)    AS txn_imposibles
FROM spot_daily_ohlcv
GROUP BY exchange
ORDER BY exchange;

-- ---------------------------------------------------------------------------
-- 1. BINANCE — buy_volume_base (taker buy base de las klines) es correcto.
--    sell y delta se rederivan de volume_base - buy. Repara el 100% del daño.
-- ---------------------------------------------------------------------------
UPDATE spot_daily_ohlcv
SET sell_volume_base = volume_base - buy_volume_base,
    volume_delta     = 2 * buy_volume_base - volume_base,
    updated_at       = NOW()
WHERE exchange = 'binance'
  AND volume_base > 0
  AND buy_volume_base IS NOT NULL
  AND buy_volume_base BETWEEN 0 AND volume_base
  AND (
        sell_volume_base IS NULL
     OR ABS(buy_volume_base + sell_volume_base - volume_base) / volume_base > 0.02
     OR volume_delta IS DISTINCT FROM (buy_volume_base - sell_volume_base)
  );

-- Filas donde ni el propio buy es fiable (buy fuera de [0, volume_base]): sin fuente sana,
-- preferimos un hueco a un número inventado.
UPDATE spot_daily_ohlcv
SET buy_volume_base  = NULL,
    sell_volume_base = NULL,
    volume_delta     = NULL,
    updated_at       = NOW()
WHERE exchange = 'binance'
  AND buy_volume_base IS NOT NULL
  AND (buy_volume_base < 0 OR (volume_base > 0 AND buy_volume_base > volume_base));

-- ---------------------------------------------------------------------------
-- 2. OKX — el buy guardado es en realidad el SELL real (columnas invertidas) y la
--    magnitud es un agregado de todos los pares. La PROPORCIÓN sí es válida:
--    buy_ratio_real = sell_guardado / (buy_guardado + sell_guardado)
--    y se reescala al volume_base del par -USDT.
--
--    Solo tocamos las filas que rompen el invariante: las que lo cumplen vinieron de
--    Coinalyze con la orientación correcta y ya están bien.
-- ---------------------------------------------------------------------------
UPDATE spot_daily_ohlcv AS s
SET buy_volume_base  = r.buy_real,
    sell_volume_base = s.volume_base - r.buy_real,
    volume_delta     = 2 * r.buy_real - s.volume_base,
    updated_at       = NOW()
FROM (
    SELECT date, symbol, exchange,
           volume_base * (sell_volume_base / NULLIF(buy_volume_base + sell_volume_base, 0)) AS buy_real
    FROM spot_daily_ohlcv
    WHERE exchange = 'okx'
      AND volume_base > 0
      AND buy_volume_base IS NOT NULL
      AND sell_volume_base IS NOT NULL
      AND ABS(buy_volume_base + sell_volume_base - volume_base) / volume_base > 0.02
) AS r
WHERE s.date = r.date AND s.symbol = r.symbol AND s.exchange = r.exchange
  AND r.buy_real IS NOT NULL;

-- OKX sin ratio recuperable (buy + sell = 0, o sin volume_base): a NULL.
UPDATE spot_daily_ohlcv
SET buy_volume_base  = NULL,
    sell_volume_base = NULL,
    volume_delta     = NULL,
    updated_at       = NOW()
WHERE exchange = 'okx'
  AND buy_volume_base IS NOT NULL
  AND (
        volume_base IS NULL OR volume_base <= 0
     OR COALESCE(buy_volume_base + sell_volume_base, 0) = 0
  );

-- ---------------------------------------------------------------------------
-- 3. CONTADORES DE OPERACIONES (binance) — txn_count viene de las klines y es correcto,
--    pero buy_txn_count venía de Coinalyze sobre otro par. No es reconstruible desde la
--    tabla, así que se anula donde es imposible o implausible. txn_count se conserva.
--    Banda [0.2, 0.8]: fuera de ahí, en un diario de spot, los dos contadores no pueden
--    venir del mismo mercado (la mediana observada era 0.067–0.37, y había ratios > 1).
-- ---------------------------------------------------------------------------
UPDATE spot_daily_ohlcv
SET buy_txn_count  = NULL,
    sell_txn_count = NULL,
    updated_at     = NOW()
WHERE exchange = 'binance'
  AND buy_txn_count IS NOT NULL
  AND (
        buy_txn_count < 0
     OR (txn_count IS NOT NULL AND buy_txn_count > txn_count)
     OR (txn_count > 0 AND buy_txn_count::numeric / txn_count NOT BETWEEN 0.2 AND 0.8)
  );

-- Recalcula sell_txn_count allí donde la pareja sí es coherente.
UPDATE spot_daily_ohlcv
SET sell_txn_count = txn_count - buy_txn_count,
    updated_at     = NOW()
WHERE txn_count IS NOT NULL
  AND buy_txn_count IS NOT NULL
  AND sell_txn_count IS DISTINCT FROM (txn_count - buy_txn_count);

-- ---------------------------------------------------------------------------
-- Estado DESPUÉS — invariante_roto, negativos y txn_imposibles deben quedar a 0.
-- ---------------------------------------------------------------------------
SELECT
    'DESPUES' AS momento,
    exchange,
    COUNT(*) FILTER (WHERE buy_volume_base IS NOT NULL)                        AS con_delta,
    COUNT(*) FILTER (
        WHERE buy_volume_base IS NOT NULL AND volume_base > 0
          AND ABS(buy_volume_base + sell_volume_base - volume_base) / volume_base > 0.02
    )                                                                          AS invariante_roto,
    COUNT(*) FILTER (WHERE sell_volume_base < 0 OR buy_volume_base < 0)        AS negativos,
    COUNT(*) FILTER (WHERE buy_txn_count > txn_count OR sell_txn_count < 0)    AS txn_imposibles
FROM spot_daily_ohlcv
GROUP BY exchange
ORDER BY exchange;

COMMIT;
