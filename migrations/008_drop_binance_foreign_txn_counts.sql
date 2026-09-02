-- =============================================================================
-- 008 — Los buy_txn_count de binance que sobrevivieron a la 005 siguen siendo de otro par
-- =============================================================================
--
-- QUÉ PASÓ
-- --------
-- El buy_txn_count de binance venía de Coinalyze, que para binance spot tiene una cadena de
-- fallback USDT -> FDUSD -> USDC: cuando el par USDT no estaba, se guardaba el contador de
-- OTRO mercado junto al txn_count de las klines del par USDT.
--
-- La 005 anuló los casos imposibles (buy_txn > txn, negativos) y los que caían fuera de la
-- banda [0.2, 0.8]. Pero esa banda es un filtro de plausibilidad, no de procedencia: las
-- filas contaminadas cuyo ratio cayó dentro de la banda por casualidad sobrevivieron.
--
-- LA MEDIDA QUE LO DEMUESTRA
-- --------------------------
-- Con el volume_delta ya reparado, buy_volume_base/volume_base es una referencia fiable de
-- la fracción compradora del día. Si buy_txn_count describiera el mismo mercado, el ratio de
-- operaciones tendría que parecerse al de volumen y correlacionar con él. Medido sobre la
-- tabla tras la 005:
--
--     exchange   n         med btxn/txn   med bvol/vol   |dif| medio   corr
--     bybit      176402    0.500          0.491          0.048         +0.574
--     coinbase   151517    0.520          0.493          0.062         +0.476
--     binance      4070    0.324          0.490          0.160         -0.002
--
-- bybit y coinbase (contadores y volumen de la MISMA fila de Coinalyze) correlacionan. Los
-- restos de binance dan correlación cero: no contienen señal sobre presión compradora.
-- Un backtest que construya un factor con ellos estaría ajustando a ruido.
--
-- QUÉ HACE
-- --------
-- Anula buy_txn_count y sell_txn_count SOLO en binance. `txn_count` NO se toca: viene del
-- campo `n` de las klines de binance, es del par correcto y sigue siendo válido.
--
-- El código ya no puede reintroducirlo: patch_missing_metrics() acepta el par
-- (txn_count, buy_txn_count) únicamente si el txn_count de la fuente externa reconcilia con
-- el propio dentro del 2%. Lo fija tests/test_spot_delta.py
-- (test_buy_txn_count_descartado_si_la_fuente_no_cuadra).
--
-- USO:
--   psql "$DATABASE_URL" -f migrations/008_drop_binance_foreign_txn_counts.sql
--   python audit_spot_delta.py --source db     -- la marca `!` de binance debe desaparecer
-- =============================================================================

BEGIN;

SELECT 'ANTES' AS momento,
       count(*) FILTER (WHERE buy_txn_count IS NOT NULL)  AS con_buy_txn,
       count(*) FILTER (WHERE txn_count IS NOT NULL)      AS con_txn_count
FROM spot_daily_ohlcv WHERE exchange = 'binance';

UPDATE spot_daily_ohlcv
SET buy_txn_count  = NULL,
    sell_txn_count = NULL,
    updated_at     = NOW()
WHERE exchange = 'binance'
  AND (buy_txn_count IS NOT NULL OR sell_txn_count IS NOT NULL);

-- con_buy_txn debe quedar a 0; con_txn_count debe quedar IGUAL que antes.
SELECT 'DESPUES' AS momento,
       count(*) FILTER (WHERE buy_txn_count IS NOT NULL)  AS con_buy_txn,
       count(*) FILTER (WHERE txn_count IS NOT NULL)      AS con_txn_count
FROM spot_daily_ohlcv WHERE exchange = 'binance';

COMMIT;
