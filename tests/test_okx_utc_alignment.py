# -*- coding: utf-8 -*-
"""Regresiones de la alineacion horaria de las velas diarias de OKX.

Contexto: pedíamos `bar="1D"`, que en OKX cierra en el corte de UTC+8 (16:00 UTC), no a
medianoche UTC. Las filas okx de spot_daily_ohlcv describían por tanto otras 24h que las de
binance/bybit con la MISMA `date`: 308 bps de desviación media en price_close comparando por
(date, activo), frente a 23 bps entre bybit y binance. `bar="1Dutc"` es la misma vela
alineada a UTC.

Estos tests fijan el parámetro en las cuatro llamadas de velas diarias a OKX. Son de
parámetro, no de red: capturan lo que se le pide a la API sin llamarla.

Ojo: los endpoints de Rubik NO aceptan "1Dutc" (la API responde code 51000), así que ahí
`period` sigue siendo "1D". Empíricamente los buckets diarios de Rubik ya casan con el día
UTC — la correlación de su volumen total con la vela 1Dutc del mismo `date` es 0.66-0.95,
contra 0.25-0.52 con la vela 1D — así que el mapeo de fechas que hace el scraper es correcto
tal cual y no hay que desplazarlo.
"""
import json
import unittest
from unittest import mock

import alt_scraper
import spot_scraper


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.headers = {}

    def json(self):
        return json.loads(json.dumps(self._payload))


# Una única vela cerrada; suficiente para que cada fetcher termine sin pedir más páginas.
_ONE_CANDLE = {
    "code": "0",
    "data": [["1714521600000", "10", "11", "9", "10.5", "100", "1050", "1050", "1"]],
}
_EMPTY = {"code": "0", "data": []}


class OKXDailyBarTest(unittest.TestCase):
    """Toda vela diaria de OKX tiene que pedirse como 1Dutc."""

    def _captured_bars(self, calls):
        return [c.kwargs.get("params", {}).get("bar") for c in calls
                if "bar" in c.kwargs.get("params", {})]

    def test_spot_history_pide_1dutc(self):
        """SpotScraper.fetch_okx — el histórico entero de spot_daily_ohlcv."""
        with mock.patch.object(
            spot_scraper._tor.session, "get", side_effect=[_FakeResponse(_EMPTY)]
        ) as get:
            spot_scraper.SpotScraper().fetch_okx("BTC", 1714521600000, 1714608000000)

        self.assertEqual(self._captured_bars(get.call_args_list), ["1Dutc"])

    def test_spot_dia_en_curso_pide_1dutc(self):
        """OKXSpotFetcher.fetch_current_day_data — la fila de hoy.

        Con "1D", antes de las 16:00 UTC esto devolvía la vela abierta AYER a las 16:00 y
        la guardábamos como la de hoy.
        """
        with mock.patch.object(
            spot_scraper._tor.session, "get", return_value=_FakeResponse(_ONE_CANDLE)
        ) as get:
            spot_scraper.GLOBAL_FETCHERS["okx"].fetch_current_day_data("BTC")

        self.assertEqual(self._captured_bars(get.call_args_list), ["1Dutc"])

    def test_futures_dia_en_curso_pide_1dutc(self):
        """El OKXFuturesFetcher que usa GLOBAL_FETCHERS de alt_scraper.

        El histórico de futures viene de Coinalyze (ya en UTC); sólo la fila del día en
        curso salía de la API de OKX y estaba desalineada.
        """
        fetcher = alt_scraper.GLOBAL_FETCHERS["okx"]
        with mock.patch.object(fetcher, "_get", return_value=None) as get:
            fetcher.fetch_current_day_data("BTCUSDT")

        bars = [c.args[1].get("bar") for c in get.call_args_list if "bar" in c.args[1]]
        self.assertEqual(bars, ["1Dutc"])

    def test_no_queda_ningun_bar_1d_en_el_codigo(self):
        """Red de seguridad: cualquier `bar` diario nuevo que vuelva a "1D" rompe esto."""
        for path in ("spot_scraper.py", "alt_scraper.py", "realtime_daemon.py",
                     "futures_ws_daemon.py", "klines_15m_backfill.py"):
            with open(path, encoding="utf-8") as fh:
                src = fh.read()
            self.assertNotIn('"bar": "1D"', src, f'{path} vuelve a pedir bar="1D"')


if __name__ == "__main__":
    unittest.main()
