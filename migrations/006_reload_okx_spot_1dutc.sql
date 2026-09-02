-- =============================================================================
-- 006 — Las velas de OKX en spot_daily_ohlcv describían otras 24h (UTC+8)
-- =============================================================================
--
-- QUÉ PASÓ
-- --------
-- spot_scraper.py pedía a OKX `bar="1D"`. Esa vela NO cierra a medianoche UTC: cierra en el
-- corte de UTC+8 (Hong Kong), es decir cubre [D 16:00, D+1 16:00) UTC. Binance y Bybit sí
-- son UTC. Resultado: cada fila okx describía una ventana distinta que la fila de
-- binance/bybit con la MISMA `date`.
--
-- Medido sobre la tabla, comparando price_close por (date, activo):
--
--     okx   vs binance   308.0 bps de media (mediana 211.7), 3249 fechas
--     bybit vs binance    23.1 bps de media (mediana   4.8)
--
-- 308 bps no es spread entre exchanges, es un desfase de día. Y explica que
-- corr(sign(volume_delta), retorno) de okx se quedara en +0.002 mientras binance daba
-- +0.160 y bybit +0.137, incluso después de arreglar la orientación buy/sell en la 005:
-- el buy_ratio de Rubik ya venía en días UTC, y lo estábamos pegando sobre una vela UTC+8.
--
-- EL ARREGLO
-- ----------
-- En el código, `bar="1Dutc"` (misma vela, alineada a UTC) en las cuatro llamadas de velas
-- diarias a OKX: spot_scraper.SpotScraper.fetch_okx, spot_scraper.OKXSpotFetcher.
-- fetch_current_day_data y los dos alt_scraper.OKXFuturesFetcher.fetch_current_day_data.
-- Lo fija tests/test_okx_utc_alignment.py.
--
-- Los endpoints de Rubik no tienen equivalente: `period="1Dutc"` devuelve code 51000. No
-- hace falta tocarlos — sus buckets diarios ya casan con el día UTC del `date` que les
-- calculamos (correlación de su volumen total con la vela 1Dutc del mismo date: 0.66-0.95;
-- con la vela 1D: 0.25-0.52).
--
-- El histórico de futures no está afectado: viene de Coinalyze, que ya es UTC (okx vs
-- binance en futures_daily_metrics: 8.1 bps, contra 6.2 de bybit). Sólo la fila del día en
-- curso salía de la API de OKX.
--
-- POR QUÉ NO HAY SQL DE REPARACIÓN AQUÍ
-- -------------------------------------
-- Esta vez el dato viejo no es recuperable desde la propia tabla: los valores describen una
-- ventana temporal que no tenemos. Hay que RE-DESCARGAR. Y un simple re-run del scraper no
-- sirve, por dos razones:
--
--   1. upsert_spot_ohlcv usa ON CONFLICT ... COALESCE(EXCLUDED.x, spot_daily_ohlcv.x), así
--      que un NULL de la recarga no limpia el valor contaminado de buy/sell/delta: quedaría
--      un precio ya en UTC junto a un delta que sigue siendo de la ventana UTC+8.
--   2. El histórico `1Dutc` de OKX no cubre las mismas fechas que `1D` (ver abajo), así que
--      quedarían fechas huérfanas con datos UTC+8 mezcladas con el resto.
--
-- Por eso la recarga borra e inserta por símbolo, en reload_okx_spot_1dutc.py:
--
--     python reload_okx_spot_1dutc.py --backup-table spot_daily_ohlcv_okx_pre1dutc
--     python audit_spot_delta.py --source db
--
-- LO QUE SE PIERDE
-- ----------------
-- OKX sirve menos historia en `1Dutc` que en `1D`, y es limitación de su API, no nuestra
-- (se reproduce llamando a /market/history-candles a pelo):
--
--     BTC-USDT    1D desde 2017-10-10   1Dutc desde 2018-01-11    (-93 días)
--     ADA-USDT    1D desde 2018-07-22   1Dutc desde 2020-01-01   (-528 días)
--     ALGO-USDT   1D desde 2019-06-16   1Dutc desde 2020-01-01   (-199 días)
--     AAVE-USDT   1D desde 2020-10-21   1Dutc desde 2020-10-23     (-2 días)
--
-- Los pares antiguos topan en 2020-01-01. Se ha elegido perder esas fechas antes que
-- dejarlas mezcladas: una fila UTC+8 junto a filas UTC es peor que no tener fila, porque
-- se agrega en silencio. El backup queda en spot_daily_ohlcv_okx_pre1dutc por si se
-- quieren recuperar para un análisis single-exchange.
--
-- VERIFICACIÓN
-- ------------
-- Tras la recarga, esta consulta debe bajar de ~308 bps a un dígito (el nivel de bybit):

WITH b AS (SELECT date, replace(replace(symbol,'-',''),'USDT','') AS base, price_close
           FROM spot_daily_ohlcv WHERE exchange='binance' AND price_close > 0),
     o AS (SELECT date, replace(replace(symbol,'-',''),'USDT','') AS base, price_close
           FROM spot_daily_ohlcv WHERE exchange='okx' AND price_close > 0),
     y AS (SELECT date, replace(replace(symbol,'-',''),'USDT','') AS base, price_close
           FROM spot_daily_ohlcv WHERE exchange='bybit' AND price_close > 0)
SELECT 'okx_vs_binance' AS pair, count(*) AS n_rows,
       round(avg(abs(o.price_close/b.price_close - 1))::numeric * 10000, 1) AS mean_bps
FROM b JOIN o ON o.date = b.date AND o.base = b.base
UNION ALL
SELECT 'bybit_vs_binance', count(*),
       round(avg(abs(y.price_close/b.price_close - 1))::numeric * 10000, 1)
FROM b JOIN y ON y.date = b.date AND y.base = b.base;

-- Limpieza del backup, cuando ya no haga falta:
--     DROP TABLE IF EXISTS spot_daily_ohlcv_okx_pre1dutc;
