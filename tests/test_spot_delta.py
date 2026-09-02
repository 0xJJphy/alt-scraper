"""Regresiones del volume_delta de spot.

Contexto: el delta estaba corrupto porque patch_missing_metrics() rellenaba
sell_volume_base desde una fuente distinta a la que había producido buy_volume_base
y luego calculaba delta = buy - sell mezclando las dos series. En Binance el sell
venía de Coinalyze (a veces del par FDUSD/USDC, otro mercado) y en OKX de Rubik con
las columnas buy/sell invertidas. Estos tests fijan las dos invariantes que lo cierran:

  * buy + sell == volume_base siempre (sell y delta se derivan, no se importan)
  * una fuente externa solo aporta magnitud si su volumen reconcilia con el propio
"""
import json
import os
import unittest
from datetime import datetime, timezone
from unittest import mock

import pandas as pd

import spot_scraper
from spot_scraper import GLOBAL_FETCHERS, patch_missing_metrics


TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _frame(**overrides):
    """Una fila de un día ya cerrado + la de hoy, para que no se dispare el fetch del
    open candle (que haría red)."""
    base = {
        "date": ["2024-05-01", TODAY],
        "volume_base": [1000.0, 1000.0],
        "price_close": [10.0, 10.0],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _cz(volume, buy, txn=None, buy_txn=None, dates=("2024-05-01", TODAY)):
    """DataFrame con la forma que devuelve CoinalyzeClient.fetch_ohlcv."""
    n = len(dates)
    data = {
        "date": list(dates),
        "volume_base": [volume] * n,
        "buy_volume_base": [buy] * n,
        "sell_volume_base": [volume - buy] * n,
        "volume_delta": [buy - (volume - buy)] * n,
    }
    if txn is not None:
        data["txn_count"] = [txn] * n
        data["buy_txn_count"] = [buy_txn] * n
    return pd.DataFrame(data)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.headers = {}

    def json(self):
        return json.loads(json.dumps(self._payload))


class SpotDeltaTest(unittest.TestCase):
    def setUp(self):
        # patch_missing_metrics decide por env si llama a Coinalyze; lo fijamos por test.
        self._env = mock.patch.dict(
            os.environ, {"COINALYZE_API_KEY_SPOT": "test-key"}, clear=False
        )
        self._env.start()
        self.addCleanup(self._env.stop)

    # ------------------------------------------------------------------ Binance

    def test_binance_sell_no_viene_de_otro_mercado(self):
        """El bug original: buy de klines (par USDT) + sell de Coinalyze (par FDUSD).

        Coinalyze devuelve aquí un mercado 100x más pequeño. Antes, sell salía de ahí
        (≈5) y delta daba ≈495 sobre un volumen de 1000: un +49% de presión compradora
        inventada. Ahora sell tiene que derivarse de volume_base - buy.
        """
        df = _frame(buy_volume_base=[500.0, 500.0])
        with mock.patch.object(
            spot_scraper.CoinalyzeClient, "fetch_ohlcv", return_value=_cz(10.0, 5.0)
        ):
            out = patch_missing_metrics(df, "BTC", "binance", "BTCUSDT")

        self.assertEqual(list(out["sell_volume_base"]), [500.0, 500.0])
        self.assertEqual(list(out["volume_delta"]), [0.0, 0.0])
        self._assert_invariante(out)

    def test_coinalyze_sin_reconciliar_se_usa_como_ratio(self):
        """Si no tenemos buy nativo y Coinalyze no cuadra en volumen, su proporción sigue
        siendo utilizable; su magnitud no."""
        df = _frame()  # sin buy_volume_base nativo
        with mock.patch.object(
            spot_scraper.CoinalyzeClient, "fetch_ohlcv", return_value=_cz(10.0, 8.0)
        ):
            out = patch_missing_metrics(df, "BTC", "binance", "BTCUSDT")

        # ratio 0.8 reescalado a volume_base=1000
        self.assertAlmostEqual(out["buy_volume_base"].iloc[0], 800.0)
        self.assertAlmostEqual(out["sell_volume_base"].iloc[0], 200.0)
        self.assertAlmostEqual(out["volume_delta"].iloc[0], 600.0)
        self._assert_invariante(out)

    def test_coinalyze_que_reconcilia_se_usa_tal_cual(self):
        """Caso Bybit: la fila entera viene de Coinalyze, los totales cuadran."""
        df = _frame()
        with mock.patch.object(
            spot_scraper.CoinalyzeClient, "fetch_ohlcv", return_value=_cz(1000.0, 600.0)
        ):
            out = patch_missing_metrics(df, "BTC", "bybit", "BTCUSDT")

        self.assertAlmostEqual(out["buy_volume_base"].iloc[0], 600.0)
        self.assertAlmostEqual(out["volume_delta"].iloc[0], 200.0)
        self._assert_invariante(out)

    # ---------------------------------------------------------------------- OKX

    def test_rubik_devuelve_sell_primero(self):
        """OKX responde [ts, sellVol, buyVol]. Leerlo al revés invertía el signo del
        delta de todo OKX."""
        payload = {
            "code": "0",
            "data": [["1714521600000", "80", "20"]],  # ts, sellVol=80, buyVol=20
        }
        with mock.patch.object(
            spot_scraper._tor.session, "get", return_value=_FakeResponse(payload)
        ):
            out = GLOBAL_FETCHERS["okx"].fetch_bulk_rubik_delta("BTC")

        self.assertEqual(list(out.columns), ["date", "buy_ratio"])
        self.assertAlmostEqual(out["buy_ratio"].iloc[0], 0.2)

    def test_okx_delta_negativo_cuando_domina_la_venta(self):
        """Con más venta que compra en Rubik, el delta resultante debe ser NEGATIVO."""
        rubik = pd.DataFrame({"date": ["2024-05-01", TODAY], "buy_ratio": [0.2, 0.2]})
        df = _frame()
        with mock.patch.dict(os.environ, {"COINALYZE_API_KEY_SPOT": "", "COINALYZE_API_KEY": ""}), \
             mock.patch.object(
                 type(GLOBAL_FETCHERS["okx"]), "fetch_bulk_rubik_delta", return_value=rubik
             ):
            out = patch_missing_metrics(df, "BTC", "okx", "BTC-USDT")

        self.assertAlmostEqual(out["buy_volume_base"].iloc[0], 200.0)
        self.assertAlmostEqual(out["volume_delta"].iloc[0], -600.0)
        self.assertLess(out["volume_delta"].iloc[0], 0)
        self._assert_invariante(out)

    def test_rubik_se_reescala_al_par_usdt(self):
        """Rubik agrega todos los pares del activo: solo su proporción es utilizable.
        buy + sell tiene que seguir cuadrando con volume_base del par -USDT."""
        rubik = pd.DataFrame({"date": ["2024-05-01", TODAY], "buy_ratio": [0.55, 0.55]})
        df = _frame()
        with mock.patch.dict(os.environ, {"COINALYZE_API_KEY_SPOT": "", "COINALYZE_API_KEY": ""}), \
             mock.patch.object(
                 type(GLOBAL_FETCHERS["okx"]), "fetch_bulk_rubik_delta", return_value=rubik
             ):
            out = patch_missing_metrics(df, "BTC", "okx", "BTC-USDT")

        self.assertAlmostEqual(out["buy_volume_base"].iloc[0], 550.0)
        self._assert_invariante(out)

    # ----------------------------------------------------------------- Coinbase

    def test_coinbase_no_confunde_ohlc(self):
        """Coinbase devuelve [time, LOW, HIGH, OPEN, close, volume] — low y high van ANTES
        que open y close. Leerlo en el orden habitual intercambia apertura con minimo."""
        payload = [[1725148800, 100.0, 130.0, 110.0, 120.0, 55.0]]
        with mock.patch.object(
            spot_scraper._tor.session, "get", return_value=_FakeResponse(payload)
        ):
            out = GLOBAL_FETCHERS["coinbase"].fetch_current_day_data("BTC")

        self.assertEqual(out["price_open"], 110.0)
        self.assertEqual(out["price_high"], 130.0)
        self.assertEqual(out["price_low"], 100.0)
        self.assertEqual(out["price_close"], 120.0)
        self.assertEqual(out["volume_base"], 55.0)
        # volume_usd no viene en la respuesta: se deriva.
        self.assertAlmostEqual(out["volume_usd"], 55.0 * 120.0)
        # El maximo tiene que ser el maximo y el minimo el minimo.
        self.assertGreaterEqual(out["price_high"], max(out["price_open"], out["price_close"]))
        self.assertLessEqual(out["price_low"], min(out["price_open"], out["price_close"]))

    def test_coinbase_usa_coinalyze_en_magnitud(self):
        """Coinalyze ES Coinbase (mismos cierres, volumen a 0.15%), asi que reconcilia y su
        buy entra en magnitud, sin degradar a ratio. Ademas el simbolo lleva quote USD."""
        captured = []

        def fake_fetch(self, cz_sym, start, end):
            captured.append(cz_sym)
            return _cz(1000.0, 620.0)

        df = _frame()
        with mock.patch.object(spot_scraper.CoinalyzeClient, "fetch_ohlcv", fake_fetch):
            out = patch_missing_metrics(df, "BTC", "coinbase", "BTC-USD")

        self.assertEqual(captured, ["BTCUSD.C"])  # no BTCUSDT.C, no fallback FDUSD/USDC
        self.assertAlmostEqual(out["buy_volume_base"].iloc[0], 620.0)
        self.assertAlmostEqual(out["sell_volume_base"].iloc[0], 380.0)
        self.assertAlmostEqual(out["volume_delta"].iloc[0], 240.0)
        self._assert_invariante(out)

    # ----------------------------------------------------------- Contadores txn

    def test_buy_txn_count_descartado_si_la_fuente_no_cuadra(self):
        """txn_count y buy_txn_count van en pareja. Mezclarlos daba buy_txn > txn y por
        tanto sell_txn negativo."""
        df = _frame(txn_count=[1000, 1000])
        with mock.patch.object(
            spot_scraper.CoinalyzeClient,
            "fetch_ohlcv",
            return_value=_cz(1000.0, 500.0, txn=100, buy_txn=90),
        ):
            out = patch_missing_metrics(df, "BTC", "binance", "BTCUSDT")

        self.assertTrue(out["buy_txn_count"].isna().all())
        self.assertTrue(out["sell_txn_count"].isna().all())
        self.assertEqual(list(out["txn_count"]), [1000, 1000])

    def test_buy_txn_count_aceptado_si_la_fuente_cuadra(self):
        df = _frame(txn_count=[1000, 1000])
        with mock.patch.object(
            spot_scraper.CoinalyzeClient,
            "fetch_ohlcv",
            return_value=_cz(1000.0, 500.0, txn=1000, buy_txn=480),
        ):
            out = patch_missing_metrics(df, "BTC", "binance", "BTCUSDT")

        self.assertEqual(list(out["buy_txn_count"]), [480, 480])
        self.assertEqual(list(out["sell_txn_count"]), [520, 520])

    # -------------------------------------------------------- Guarda de salida

    def test_guarda_anula_filas_que_violan_el_invariante(self):
        """Última red: si algo se cuela con buy > volume_base, no se escribe en la DB."""
        df = _frame(buy_volume_base=[1500.0, 500.0])
        with mock.patch.dict(os.environ, {"COINALYZE_API_KEY_SPOT": "", "COINALYZE_API_KEY": ""}):
            out = patch_missing_metrics(df, "BTC", "binance", "BTCUSDT")

        self.assertTrue(pd.isna(out["volume_delta"].iloc[0]))
        self.assertTrue(pd.isna(out["sell_volume_base"].iloc[0]))
        self.assertEqual(out["volume_delta"].iloc[1], 0.0)

    # ------------------------------------------------------------------ helpers

    def _assert_invariante(self, out):
        vol = pd.to_numeric(out["volume_base"], errors="coerce")
        buy = pd.to_numeric(out["buy_volume_base"], errors="coerce")
        sell = pd.to_numeric(out["sell_volume_base"], errors="coerce")
        delta = pd.to_numeric(out["volume_delta"], errors="coerce")
        mask = buy.notna()
        self.assertTrue(mask.any(), "el test no ha producido ninguna fila con buy")
        pd.testing.assert_series_equal(
            (buy + sell)[mask], vol[mask], check_names=False, rtol=1e-9
        )
        pd.testing.assert_series_equal(
            delta[mask], (buy - sell)[mask], check_names=False, rtol=1e-9
        )


if __name__ == "__main__":
    unittest.main()
