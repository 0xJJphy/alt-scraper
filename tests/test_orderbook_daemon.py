"""Regresión del deadlock de inicialización del orderbook de Binance.

Historia: los libros de binance dejaron de emitir métricas de forma progresiva
(2070 filas/mes en abril 2026 -> 38 en julio) mientras bybit y okx seguían bien
en la misma tabla. La causa no era la red:

  * _run_chunk lanzaba un _init_book por símbolo con delay=0.0, todos a la vez.
    /fapi/v1/depth pesa 20 a limit=1000 contra un presupuesto de 2400/min, así
    que ~150 símbolos costaban 3000 y garantizaban 429.
  * Un init que falla sólo loguea, dejando initialized=False.
  * El bucle WS entonces bufferiza en _pending en vez de llamar a _apply_event...
  * ...y el único reintento vivía DENTRO de _apply_event. Deadlock: el camino de
    recuperación pasaba por el código que sólo corre si ya está inicializado.

Bybit y OKX no sufrían porque reciben el snapshot por el propio WebSocket.
"""
import asyncio
import time
import unittest
from collections import deque
from unittest import mock

from orderbook_daemon import (BinanceFuturesStream, BinanceSpotStream,
                              CoinbaseSpotStream, compute_metrics)

ASSETS = [{"base_asset": "BTC", "symbol_binance": "BTCUSDT",
           "symbol_binance_spot": "BTCUSDT"}]


def _run(coro):
    """Corre una corrutina en un loop propio y lo cierra (evita ResourceWarning)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _evt(U, u, pu=None):
    e = {"s": "BTCUSDT", "U": U, "u": u, "b": [["100", "1"]], "a": [["101", "1"]]}
    if pu is not None:
        e["pu"] = pu
    return e


class InitDeadlockTest(unittest.TestCase):
    def test_failed_init_leaves_book_uninitialized(self):
        """Punto de partida del bug: un 429 deja el libro sin inicializar."""
        s = BinanceFuturesStream(ASSETS)
        with mock.patch.object(s, "_rest_snapshot", side_effect=RuntimeError("429")):
            s._init_book("BTCUSDT")
        self.assertFalse(s.books["BTCUSDT"].initialized)
        self.assertIsNone(s.books["BTCUSDT"].snapshot())
        # y el lock queda liberado, condición para que el watchdog pueda reintentar
        self.assertFalse(s._init_lock.get("BTCUSDT"))

    def test_watchdog_recovers_a_book_whose_init_failed(self):
        """El arreglo: el watchdog reintenta lo que _apply_event nunca alcanza."""
        s = BinanceFuturesStream(ASSETS)
        s.INIT_WATCHDOG_SEC = 0.01

        with mock.patch.object(s, "_rest_snapshot", side_effect=RuntimeError("429")):
            s._init_book("BTCUSDT")
        self.assertFalse(s.books["BTCUSDT"].initialized)

        async def run_one_pass():
            task = asyncio.ensure_future(s._init_watchdog())
            await asyncio.sleep(0.05)
            task.cancel()

        with mock.patch.object(s, "_rest_snapshot",
                              return_value=(10, [(100.0, 1.0)], [(101.0, 1.0)])):
            _run(run_one_pass())
            deadline = time.time() + 2
            while time.time() < deadline and not s.books["BTCUSDT"].initialized:
                time.sleep(0.01)

        self.assertTrue(s.books["BTCUSDT"].initialized,
                        "el watchdog debe recuperar un libro con init fallido")
        self.assertIsNotNone(s.books["BTCUSDT"].snapshot())

    def test_watchdog_skips_books_already_initializing(self):
        """No debe apilar inits sobre un símbolo que ya tiene uno en vuelo."""
        s = BinanceFuturesStream(ASSETS)
        s.INIT_WATCHDOG_SEC = 0.01
        s._init_lock["BTCUSDT"] = True

        calls = []

        async def run_one_pass():
            task = asyncio.ensure_future(s._init_watchdog())
            await asyncio.sleep(0.05)
            task.cancel()

        with mock.patch.object(s, "_init_book", side_effect=lambda *a, **k: calls.append(a)):
            _run(run_one_pass())
        self.assertEqual(calls, [])


class RateLimitBudgetTest(unittest.TestCase):
    # Pesos de /fapi/v1/depth por tramo de `limit` (presupuesto 2400/min por IP).
    FAPI_DEPTH_WEIGHT = {50: 2, 100: 5, 500: 10, 1000: 20}

    def test_depth_limit_and_spacing_stay_inside_binance_budget(self):
        w = self.FAPI_DEPTH_WEIGHT[BinanceFuturesStream.REST_DEPTH_LIMIT]
        per_minute = w * (60.0 / BinanceFuturesStream.INIT_SPACING_SEC)
        self.assertLessEqual(per_minute, 2400,
                             f"{per_minute}/min supera el presupuesto de Binance")

    def test_snapshot_is_not_shrunk_to_save_weight(self):
        """Medido en vivo: limit=500 recorta la cobertura de precio a la mitad
        (ATOM 99.9%->39.1%, ADA 61.9%->30.6%). En los alts el snapshot ES la
        profundidad, porque sus niveles lejanos no se refrescan por el WS, así
        que el ahorro de peso se paga en dato perdido. Se espacia, no se recorta."""
        self.assertEqual(BinanceFuturesStream.REST_DEPTH_LIMIT, 1000)
        self.assertEqual(BinanceSpotStream.REST_DEPTH_LIMIT, 1000)

    def test_inits_are_staggered_not_simultaneous(self):
        s = BinanceFuturesStream([
            {"base_asset": f"A{i}", "symbol_binance": f"A{i}USDT"} for i in range(5)
        ])
        delays = []

        def fake_thread(target=None, args=(), daemon=None):
            delays.append(args[1])
            return mock.Mock(start=lambda: None)

        with mock.patch("orderbook_daemon.threading.Thread", side_effect=fake_thread):
            async def kick():
                # sólo interesa la fase de arranque, así que se corta el bucle WS
                with mock.patch("orderbook_daemon.websockets.connect",
                                side_effect=asyncio.CancelledError):
                    try:
                        await s._run_chunk([f"A{i}USDT" for i in range(5)])
                    except asyncio.CancelledError:
                        pass
            _run(kick())

        self.assertEqual(delays, [i * s.INIT_SPACING_SEC for i in range(5)])

    def test_chunks_do_not_overlap_their_init_windows(self):
        """Los chunks comparten el presupuesto de la IP: el offset los separa."""
        s = BinanceFuturesStream(ASSETS)
        delays = []

        def fake_thread(target=None, args=(), daemon=None):
            delays.append(args[1])
            return mock.Mock(start=lambda: None)

        with mock.patch("orderbook_daemon.threading.Thread", side_effect=fake_thread):
            async def kick():
                with mock.patch("orderbook_daemon.websockets.connect",
                                side_effect=asyncio.CancelledError):
                    try:
                        await s._run_chunk(["BTCUSDT"], init_offset=200)
                    except asyncio.CancelledError:
                        pass
            _run(kick())

        self.assertEqual(delays, [200 * s.INIT_SPACING_SEC])


class LogTagTest(unittest.TestCase):
    """Futures y spot comparten tickers y la clase de spot hereda los logs de la
    de futures, así que sin el market_type dos líneas idénticas pueden venir de
    mercados distintos."""

    def test_tag_distinguishes_futures_from_spot(self):
        self.assertEqual(BinanceFuturesStream(ASSETS)._tag, "binance futures")
        self.assertEqual(BinanceSpotStream(ASSETS)._tag, "binance spot")

    def test_init_log_carries_the_market_type(self):
        for cls, expected in ((BinanceFuturesStream, "binance futures"),
                              (BinanceSpotStream, "binance spot")):
            s = cls(ASSETS)
            with mock.patch.object(s, "_rest_snapshot",
                                   return_value=(10, [(100.0, 1.0)], [(101.0, 1.0)])):
                with self.assertLogs("orderbook", level="INFO") as cm:
                    s._init_book("BTCUSDT")
            self.assertTrue(any(f"{expected} init: BTCUSDT" in m for m in cm.output),
                            f"falta el market_type en el log de {cls.__name__}: {cm.output}")


class StaggerVsBufferTest(unittest.TestCase):
    """Los dos arreglos interactúan: escalonar retrasa el init del último símbolo,
    y el buffer acotado descarta eventos viejos. El buffer tiene que cubrir la
    espera completa o se perderían deltas entre el snapshot y el primer evento
    aplicado."""

    UNIVERSE = 200          # CHUNK, el peor caso por conexión
    EVENTS_PER_SEC = 2.0    # depth@500ms

    def test_buffer_covers_the_full_stagger_window(self):
        s = BinanceFuturesStream(ASSETS)
        wait_last = (self.UNIVERSE - 1) * s.INIT_SPACING_SEC
        buffer_secs = s.PENDING_MAXLEN / self.EVENTS_PER_SEC
        self.assertGreater(
            buffer_secs, wait_last,
            f"el buffer cubre {buffer_secs:.0f}s pero el último símbolo espera "
            f"{wait_last:.0f}s: se perderían deltas")


class PendingBufferTest(unittest.TestCase):
    def test_pending_buffer_is_bounded(self):
        """Un símbolo atascado recibe ~172k eventos/día con depth@500ms."""
        s = BinanceFuturesStream(ASSETS)
        for i in range(s.PENDING_MAXLEN * 3):
            s._buffer("BTCUSDT").append(_evt(i, i))
        self.assertEqual(len(s._pending["BTCUSDT"]), s.PENDING_MAXLEN)

    def test_seq_gap_reset_keeps_buffer_bounded(self):
        """El reset por hueco de secuencia no debe reintroducir una lista infinita."""
        s = BinanceFuturesStream(ASSETS)
        s.books["BTCUSDT"].apply_snapshot([(100.0, 1.0)], [(101.0, 1.0)])
        s._last_u["BTCUSDT"] = 10
        s._init_time["BTCUSDT"] = time.time() - 10  # fuera del periodo de gracia
        with mock.patch("orderbook_daemon.threading.Thread",
                        return_value=mock.Mock(start=lambda: None)):
            s._apply_event("BTCUSDT", _evt(20, 25, pu=999))
        self.assertIsInstance(s._pending["BTCUSDT"], deque)
        self.assertEqual(s._pending["BTCUSDT"].maxlen, s.PENDING_MAXLEN)

    def test_spot_seq_gap_reset_keeps_buffer_bounded(self):
        s = BinanceSpotStream(ASSETS)
        s.books["BTCUSDT"].apply_snapshot([(100.0, 1.0)], [(101.0, 1.0)])
        s._last_u["BTCUSDT"] = 10
        s._init_time["BTCUSDT"] = time.time() - 10
        with mock.patch("orderbook_daemon.threading.Thread",
                        return_value=mock.Mock(start=lambda: None)):
            s._apply_event("BTCUSDT", _evt(20, 25))
        self.assertIsInstance(s._pending["BTCUSDT"], deque)
        self.assertEqual(s._pending["BTCUSDT"].maxlen, s.PENDING_MAXLEN)

    def test_buffered_events_are_replayed_on_init(self):
        """Lo bufferizado antes del snapshot debe aplicarse tras inicializar."""
        s = BinanceFuturesStream(ASSETS)
        s._buffer("BTCUSDT").append(_evt(8, 12))     # solapa lastUpdateId=10
        s._buffer("BTCUSDT").append(_evt(1, 5))      # anterior, se descarta
        with mock.patch.object(s, "_rest_snapshot",
                              return_value=(10, [(100.0, 1.0)], [(101.0, 1.0)])):
            s._init_book("BTCUSDT")
        self.assertTrue(s.books["BTCUSDT"].initialized)
        self.assertEqual(s._last_u["BTCUSDT"], 12)
        self.assertNotIn("BTCUSDT", s._pending)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Coinbase spot
# ---------------------------------------------------------------------------

CB_ASSETS = [{"base_asset": "BTC", "symbol_coinbase": "BTC-USD"}]


def _u(side, price, qty):
    return {"side": side, "price_level": str(price), "new_quantity": str(qty)}


def _ev(tipo, updates, prod="BTC-USD"):
    return {"type": tipo, "product_id": prod, "updates": updates}


class CoinbaseLadoTest(unittest.TestCase):
    """El lado vendedor de Coinbase se llama `offer`, no `ask`.

    Es la misma clase de trampa que invirtió el delta de OKX con Rubik: un nombre de
    campo distinto al habitual que, si se resuelve con un `else`, cambia el signo de
    todos los imbalances sin que nada falle.
    """

    def test_offer_va_al_ask_y_no_al_bid(self):
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 100, 1), _u("offer", 101, 2)]))
        libro = s.books["BTC-USD"]
        self.assertEqual(libro.bids, {100.0: 1.0})
        self.assertEqual(libro.asks, {101.0: 2.0})

    def test_un_lado_desconocido_se_ignora_en_vez_de_caer_al_ask(self):
        # Si Coinbase renombrara el campo, es preferible un libro incompleto (que deja
        # de emitir métricas) a uno invertido (que las emite mal).
        bids, asks = CoinbaseSpotStream._parse_updates([_u("ask", 101, 2)])
        self.assertEqual((bids, asks), ([], []))

    def test_cantidad_cero_borra_el_nivel(self):
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 100, 1), _u("bid", 99, 5),
                                        _u("offer", 101, 2)]))
        s._apply_event(_ev("update", [_u("bid", 99, 0)]))
        self.assertNotIn(99.0, s.books["BTC-USD"].bids)
        self.assertIn(100.0, s.books["BTC-USD"].bids)

    def test_los_deltas_previos_al_snapshot_no_se_aplican(self):
        # Sin snapshot no hay centro de recorte, y aplicarlos crearía un libro fantasma.
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("update", [_u("bid", 100, 1)]))
        self.assertFalse(s.books["BTC-USD"].initialized)


class CoinbaseLibroEnteroTest(unittest.TestCase):
    """El libro se guarda entero, sin recortar.

    Se probó recortarlo a ±25% del mid y se descartó: medido sobre los 107 libros reales
    ahorraba 17 MB de RAM y 67 ms por pasada, sin tocar base de datos ni red, y a cambio
    obligaba a re-suscribir cuando el precio derivaba, porque los niveles que caen fuera
    del recorte no vuelven por delta. Estos tests fijan que el recorte no ha vuelto.
    """

    def test_el_libro_conserva_los_niveles_lejanos(self):
        # A ±25% estos dos desaparecían y la cobertura saturaba ahí.
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 60, 1), _u("bid", 100, 1),
                                        _u("offer", 101, 1), _u("offer", 140, 1)]))
        libro = s.books["BTC-USD"]
        self.assertIn(60.0, libro.bids)
        self.assertIn(140.0, libro.asks)

    def test_la_cobertura_llega_al_100_por_cien(self):
        # La consecuencia visible: depth_coverage_pct vuelve a ser comparable con la de
        # binance en vez de saturar en ~25%, que era la pega de la versión recortada.
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 0.01, 1), _u("bid", 100, 1),
                                        _u("offer", 101, 1), _u("offer", 400, 1)]))
        m = s.books["BTC-USD"].snapshot()
        self.assertGreater(m["depth_coverage_pct"], 90.0)

    def test_las_metricas_son_las_del_libro_entero(self):
        mid = 100.0
        bids = [(mid * (1 - i / 1000.0), 1.0) for i in range(1, 401)]   # hasta -40%
        asks = [(mid * (1 + i / 1000.0), 1.0) for i in range(1, 401)]
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot",
                           [_u("bid", p, q) for p, q in bids]
                           + [_u("offer", p, q) for p, q in asks]))
        self.assertEqual(s.books["BTC-USD"].snapshot(),
                         compute_metrics(dict(bids), dict(asks)))

    def test_un_libro_disperso_deja_la_banda_ancha_nula(self):
        # Sólo hay niveles hasta el ±5%: la del 10% debe salir NULL, no inventada.
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 95, 1), _u("bid", 99, 1),
                                        _u("offer", 101, 1), _u("offer", 105, 1)]))
        m = s.books["BTC-USD"].snapshot()
        self.assertIsNone(m["imbalance_10pct"])
        self.assertIsNotNone(m["imbalance_1pct"])


class CoinbaseValidarTest(unittest.TestCase):
    """Regresión de los dos fallos silenciosos que aparecieron al probarlo en vivo.

    Los dos daban cero snapshots sin una sola línea de log: el stream se creía suscrito
    y no lo estaba, o se reconectaba en bucle creyendo que perdía mensajes.
    """

    def test_los_mensajes_de_control_cuentan_para_la_secuencia(self):
        # `subscriptions` también consume número: validar sólo los de l2_data hacía
        # saltar un hueco falso de exactamente 1 en cada suscripción.
        s = CoinbaseSpotStream(CB_ASSETS)
        esperado = s._validar({"channel": "subscriptions", "sequence_num": 0}, None)
        self.assertEqual(esperado, 1)
        esperado = s._validar({"channel": "l2_data", "sequence_num": 1}, esperado)
        self.assertEqual(esperado, 2)

    def test_un_hueco_real_rompe_la_conexion(self):
        s = CoinbaseSpotStream(CB_ASSETS)
        with self.assertRaises(ConnectionError):
            s._validar({"channel": "l2_data", "sequence_num": 5}, 3)

    def test_el_error_del_servidor_no_se_traga(self):
        # Los errores no llevan `channel`, así que el filtro por l2_data los descartaba.
        s = CoinbaseSpotStream(CB_ASSETS)
        with self.assertRaisesRegex(ConnectionError, "too many L2"):
            s._validar({"type": "error",
                        "message": "too many L2 streams requested in a single session"}, None)

    def test_el_tope_de_productos_por_conexion_se_respeta(self):
        # Medido contra el servidor: 30 van, 31 devuelve error.
        self.assertLessEqual(CoinbaseSpotStream.CHUNK, 30)


class CoinbaseResetTest(unittest.TestCase):
    def test_al_reconectar_el_libro_deja_de_emitir_metricas(self):
        # Un libro al que le han faltado deltas miente sin avisar; es preferible que no
        # emita nada hasta que llegue el snapshot nuevo, que tarda segundos.
        s = CoinbaseSpotStream(CB_ASSETS)
        s._apply_event(_ev("snapshot", [_u("bid", 100, 1), _u("offer", 101, 1)]))
        self.assertIsNotNone(s.get_metrics().get("BTC-USD"))
        s.books["BTC-USD"].reset()
        self.assertEqual(s.get_metrics(), {})
