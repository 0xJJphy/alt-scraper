import unittest
from datetime import datetime, timezone

from futures_ws_daemon import (
    KLINE_15M_SQL,
    _base_row,
    most_recent_closed_open_at,
    normalize_binance_ws,
    normalize_binance_mark_price_ws,
    normalize_bybit_ws,
    normalize_bybit_ticker_ws,
    normalize_okx_metrics_ws,
    normalize_okx_ws,
)


OPEN_MS = 1_700_000_100_000


class FuturesWsDaemonTest(unittest.TestCase):
    def test_binance_closed_kline_uses_exchange_open_time(self):
        row = normalize_binance_ws(
            {
                "data": {
                    "E": OPEN_MS + 900_000,
                    "k": {
                        "t": OPEN_MS,
                        "s": "BTCUSDT",
                        "x": True,
                        "o": "100",
                        "h": "110",
                        "l": "90",
                        "c": "105",
                        "v": "10",
                        "q": "1000",
                        "n": 12,
                        "V": "6",
                    },
                }
            },
            {"BTCUSDT": "BTC"},
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["exchange"], "binance")
        self.assertEqual(row["symbol"], "BTCUSDT")
        self.assertEqual(row["base_asset"], "BTC")
        self.assertEqual(row["source"], "ws")
        self.assertEqual(row["candle_open_at"], datetime.fromtimestamp(OPEN_MS / 1000, timezone.utc))
        self.assertEqual(row["volume_delta"], 2.0)

    def test_binance_open_kline_is_ignored(self):
        row = normalize_binance_ws(
            {"data": {"k": {"t": OPEN_MS, "s": "BTCUSDT", "x": False}}},
            {"BTCUSDT": "BTC"},
        )

        self.assertIsNone(row)

    def test_bybit_confirmed_kline_uses_start_time(self):
        row = normalize_bybit_ws(
            {
                "topic": "kline.15.ETHUSDT",
                "ts": OPEN_MS + 900_000,
                "data": [
                    {
                        "start": OPEN_MS,
                        "timestamp": OPEN_MS + 900_000,
                        "confirm": True,
                        "open": "200",
                        "high": "220",
                        "low": "190",
                        "close": "210",
                        "volume": "4",
                        "turnover": "800",
                    }
                ],
            },
            {"ETHUSDT": "ETH"},
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["exchange"], "bybit")
        self.assertEqual(row["candle_open_at"], datetime.fromtimestamp(OPEN_MS / 1000, timezone.utc))
        self.assertEqual(row["volume_usd"], 800.0)

    def test_okx_confirmed_kline_uses_array_timestamp(self):
        row = normalize_okx_ws(
            {
                "arg": {"channel": "candle15m", "instId": "SOL-USDT-SWAP"},
                "data": [[str(OPEN_MS), "10", "12", "9", "11", "5", "50", "55", "1"]],
            },
            {"SOL-USDT-SWAP": ("SOL", "SOLUSDT")},
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["exchange"], "okx")
        self.assertEqual(row["symbol"], "SOLUSDT")
        self.assertEqual(row["candle_open_at"], datetime.fromtimestamp(OPEN_MS / 1000, timezone.utc))

    def test_unaligned_candle_is_rejected(self):
        row = _base_row(
            candle_open_at=datetime.fromtimestamp((OPEN_MS + 1_000) / 1000, timezone.utc),
            symbol="BTCUSDT",
            exchange="binance",
            base_asset="BTC",
            source="ws",
        )

        self.assertIsNone(row)

    def test_upsert_conflict_key_is_candle_identity(self):
        self.assertIn("ON CONFLICT (candle_open_at, symbol, exchange)", KLINE_15M_SQL)
        self.assertNotIn("ON CONFLICT (polled_at", KLINE_15M_SQL)

    def test_gap_backstop_targets_last_fully_closed_candle(self):
        now = datetime.fromtimestamp((OPEN_MS + 900_000 + 20_000) / 1000, timezone.utc)
        self.assertEqual(
            most_recent_closed_open_at(now=now, settle_delay_s=20),
            datetime.fromtimestamp(OPEN_MS / 1000, timezone.utc),
        )

    def test_binance_mark_price_updates_latest_fields(self):
        row = normalize_binance_mark_price_ws(
            {"data": {"s": "BTCUSDT", "p": "100.5", "r": "0.0001"}},
            {"BTCUSDT": "BTC"},
        )

        self.assertEqual(row["exchange"], "binance")
        self.assertEqual(row["price"], 100.5)
        self.assertEqual(row["pred_funding"], 0.0001)
        self.assertIsNone(row["oi_usd"])

    def test_bybit_ticker_updates_oi_price_and_funding(self):
        row = normalize_bybit_ticker_ws(
            {
                "topic": "tickers.BTCUSDT",
                "data": {
                    "markPrice": "100",
                    "openInterest": "3",
                    "fundingRate": "0.0002",
                },
            },
            {"BTCUSDT": "BTC"},
        )

        self.assertEqual(row["exchange"], "bybit")
        self.assertEqual(row["price"], 100.0)
        self.assertEqual(row["oi_usd"], 300.0)
        self.assertEqual(row["pred_funding"], 0.0002)

    def test_okx_metrics_update_by_channel(self):
        inst_to_meta = {"BTC-USDT-SWAP": ("BTC", "BTCUSDT")}
        oi = normalize_okx_metrics_ws(
            {
                "arg": {"channel": "open-interest", "instId": "BTC-USDT-SWAP"},
                "data": [{"oiUsd": "12345"}],
            },
            inst_to_meta,
        )
        funding = normalize_okx_metrics_ws(
            {
                "arg": {"channel": "funding-rate", "instId": "BTC-USDT-SWAP"},
                "data": [{"fundingRate": "0.0003", "nextFundingRate": "0.0004"}],
            },
            inst_to_meta,
        )
        mark = normalize_okx_metrics_ws(
            {
                "arg": {"channel": "mark-price", "instId": "BTC-USDT-SWAP"},
                "data": [{"markPx": "101"}],
            },
            inst_to_meta,
        )

        self.assertEqual(oi["oi_usd"], 12345.0)
        self.assertEqual(funding["funding"], 0.0003)
        self.assertEqual(funding["pred_funding"], 0.0004)
        self.assertEqual(mark["price"], 101.0)


if __name__ == "__main__":
    unittest.main()
