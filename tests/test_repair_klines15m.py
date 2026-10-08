"""Reparador de huecos 15m (repair_klines15m). Sin red ni DB reales."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import repair_klines15m as rk  # noqa: E402

UTC = timezone.utc
T0 = int(datetime(2026, 5, 14, 16, 0, tzinfo=UTC).timestamp() * 1000)
S = rk.STEP_MS


def t(i: int) -> int:
    return T0 + i * S


def bar(i, price="100", vb="10", vq="1000", buy=None, trades=None):
    p = Decimal(price)
    return rk.Bar(t(i), p, p + 1, p - 1, p, Decimal(vb), Decimal(vq), None if buy is None else Decimal(buy), trades)


def db_row(i, price="100", vb="10", vq="1000", base="SOL", buy=None):
    p = Decimal(price)
    return {"candle_open_at": rk.ms_to_dt(t(i)), "base_asset": base, "price_open": p, "price_high": p + 1,
            "price_low": p - 1, "price_close": p, "volume_base": Decimal(vb), "volume_usd": Decimal(vq),
            "buy_volume_base": None if buy is None else Decimal(buy)}


# ------------------------------------------------------------------ deteccion

def test_find_gaps_interior_only():
    times = [t(0), t(1), t(4), t(5), t(6), t(8)]
    gaps = rk.find_gaps_in_series("okx", "SOLUSDT", times)
    assert [(g.prev_ms, g.next_ms, g.missing) for g in gaps] == [(t(1), t(4), 2), (t(6), t(8), 1)]
    assert gaps[0].missing_times() == [t(2), t(3)]
    assert rk.find_gaps_in_series("okx", "X", [t(0), t(1), t(2)]) == []  # sin huecos
    assert rk.find_gaps_in_series("okx", "X", [t(3)]) == []  # nada antes/despues de la unica barra


def test_summarize_by_month():
    g = [rk.Gap("binance", "A", t(0), t(3))]
    s = rk.summarize_gaps(g)
    assert s == {"gaps": 1, "missing_bars": 2, "symbols": 1, "by_month": {"2026-05": 2}}


def test_merge_windows_merges_overlapping():
    g1, g2, g3 = rk.Gap("x", "A", t(0), t(3)), rk.Gap("x", "A", t(6), t(8)), rk.Gap("x", "A", t(100), t(102))
    wins = rk.merge_windows([g3, g1, g2])
    assert len(wins) == 2 and wins[0][2] == [g1, g2] and wins[1][2] == [g3]


# ------------------------------------------------------------------ parseo de respuestas (fixtures)

def test_parse_binance():
    raw = [[T0, "100.0", "101.0", "99.0", "100.5", "10", T0 + S - 1, "1000.5", 77, "4", "400", "0"]]
    [b] = rk.parse_binance_klines(raw)
    assert (b.t, b.vb, b.vq, b.buy, b.trades) == (T0, Decimal("10"), Decimal("1000.5"), Decimal("4"), 77)
    assert rk.parse_binance_klines(raw, now_ms=T0 + 1) == []  # barra en curso


def test_parse_bybit_turnover_is_usd():
    payload = {"retCode": 0, "result": {"list": [[str(t(1)), "100", "101", "99", "100", "5", "500"],
                                                  [str(T0), "100", "101", "99", "100", "6", "600"]]}}
    bars = rk.parse_bybit_klines(payload)
    assert [(b.t, b.vb, b.vq, b.buy) for b in bars] == [(t(1), Decimal("5"), Decimal("500"), None),
                                                          (T0, Decimal("6"), Decimal("600"), None)]


def test_parse_okx_uses_volccy_not_contracts():
    payload = {"code": "0", "data": [[str(T0), "100", "101", "99", "100", "7", "70", "7000", "1"],
                                      [str(t(1)), "100", "101", "99", "100", "1", "10", "1000", "0"]]}
    bars = rk.parse_okx_candles(payload)
    assert len(bars) == 1  # la no confirmada se descarta
    assert (bars[0].vb, bars[0].vq) == (Decimal("70"), Decimal("7000"))


def test_incoherent_bars_rejected():
    ok = bar(0)
    assert rk.bar_is_coherent(ok)
    assert not rk.bar_is_coherent(rk.Bar(T0, Decimal("100"), Decimal("99"), Decimal("98"), Decimal("100"), Decimal(1), Decimal(1)))
    assert not rk.bar_is_coherent(rk.Bar(T0, Decimal("NaN"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal(1), Decimal(1)))
    assert not rk.bar_is_coherent(rk.Bar(T0, Decimal("0"), Decimal("1"), Decimal("0"), Decimal("1"), Decimal(1), Decimal(1)))
    assert not rk.bar_is_coherent(rk.Bar(T0 + 1, Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal(1), Decimal(1)))


# ------------------------------------------------------------------ validacion

def _scenario(src_vb="10", src_vq="1000"):
    gap = rk.Gap("okx", "SOLUSDT", t(8), t(10))
    dbr = {t(i): db_row(i) for i in list(range(0, 9)) + list(range(10, 19))}
    src = {t(i): bar(i, vb=src_vb, vq=src_vq) for i in range(0, 19)}
    return gap, dbr, src


def test_validation_accepts_matching_source():
    gap, dbr, src = _scenario()
    v = rk.validate_gap(gap, dbr, src)
    assert v.ok and v.compared == 16 and v.matched == 16


def test_validation_rejects_unit_mismatch():
    gap, dbr, src = _scenario(src_vb="1", src_vq="10000")  # p. ej. contratos en vez de base
    v = rk.validate_gap(gap, dbr, src)
    assert not v.ok and v.reason.startswith("source_mismatch")
    gap, dbr, src = _scenario(src_vq="100000")  # volume_usd en otra unidad
    assert not rk.validate_gap(gap, dbr, src).ok


def test_validation_needs_enough_neighbours():
    gap = rk.Gap("okx", "X", t(8), t(10))
    v = rk.validate_gap(gap, {t(8): db_row(8)}, {t(8): bar(8)})
    assert not v.ok and v.reason.startswith("insufficient")


def test_binance_buy_volume_is_validated():
    row = db_row(0, buy="4")
    assert rk.compare_bar(row, bar(0, buy="4"), "binance") == []
    assert rk.compare_bar(row, bar(0, buy="8"), "binance") == ["buy_volume_base"]
    assert rk.compare_bar(row, bar(0, buy="8"), "bybit") == []  # solo binance guarda buy


def test_compare_price_tolerance():
    row = db_row(0)
    assert rk.compare_bar(row, bar(0, price="100.00001"), "okx") == []  # 1e-7 relativo < tol 1e-6
    assert rk.compare_bar(row, bar(0, price="100.001"), "okx")  # 1e-5 > tol
    assert rk.compare_bar(row, bar(0, price="101"), "okx") == ["price_open", "price_high", "price_low", "price_close"]


class _Prov:
    def __init__(self, label, bars=None, exc=None):
        self.label, self._bars, self._exc = label, bars, exc

    def fetch(self, symbol, base, lo, hi):
        if self._exc:
            raise rk.SourceUnavailable(self._exc)
        return {k: v for k, v in self._bars.items() if lo <= k <= hi}


def test_plan_symbol_repairs_and_flags_unrecoverable():
    gap, dbr, src = _scenario()
    del src[t(9)]  # el exchange no tiene esa barra (mantenimiento)
    plan = rk.plan_symbol("okx", "SOLUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_rest", src)])
    assert plan["rows"] == [] and plan["results"][0]["unrecoverable"] == 1
    assert "bar_not_returned" in plan["results"][0]["reason"]
    src[t(9)] = bar(9)
    plan = rk.plan_symbol("okx", "SOLUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_rest", src)])
    [row] = plan["rows"]
    assert row["source"] == "gap_repair_rest" and row["base_asset"] == "SOL" and row["candle_open_at"] == rk.ms_to_dt(t(9))
    assert row["buy_volume_base"] is None and row["txn_count"] is None  # okx: NULL


def test_plan_symbol_skips_on_mismatch_and_falls_back_to_second_provider():
    gap, dbr, src = _scenario()
    bad = {k: rk.Bar(k, b.o, b.h, b.l, b.c, b.vb * 100, b.vq, None, None) for k, b in src.items()}
    plan = rk.plan_symbol("okx", "SOLUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_corpus", bad)])
    assert plan["rows"] == [] and "source_mismatch" in plan["results"][0]["reason"]
    plan = rk.plan_symbol("okx", "SOLUSDT", [gap], lambda lo, hi: dbr,
                          [_Prov("gap_repair_corpus", bad), _Prov("gap_repair_rest", src)])
    assert [r["source"] for r in plan["rows"]] == ["gap_repair_rest"]


def test_plan_symbol_source_unavailable():
    gap, dbr, _ = _scenario()
    plan = rk.plan_symbol("okx", "SOLUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_rest", exc="http_400")])
    assert plan["rows"] == [] and "source_unavailable" in plan["results"][0]["reason"]


def test_binance_row_has_delta_and_sell():
    b = rk.Bar(T0, Decimal(10), Decimal(11), Decimal(9), Decimal(10), Decimal("10"), Decimal("100"), Decimal("4"), 9)
    r = rk.build_row("binance", "SOLUSDT", "SOL", b, rk.SRC_REST)
    assert (r["buy_volume_base"], r["sell_volume_base"], r["volume_delta"], r["txn_count"]) == (4, 6, -2, 9)
    r = rk.build_row("bybit", "SOLUSDT", "SOL", b, rk.SRC_REST)
    assert (r["buy_volume_base"], r["sell_volume_base"], r["volume_delta"], r["txn_count"]) == (None,) * 4


# ------------------------------------------------------------------ escritura, manifiesto, rollback

class FakeCur:
    def __init__(self):
        self.calls = []


def test_insert_batch_returns_keys_and_uses_do_nothing():
    rows = [rk.build_row("okx", "SOLUSDT", "SOL", bar(i), rk.SRC_REST) for i in (1, 2)]
    seen = {}

    def ex(cur, sql, tuples, template):
        seen.update(sql=sql, n=len(tuples), template=template)
        return [(r[0], r[2], r[3]) for r in tuples][:1]  # el segundo "ya existia"

    keys = rk.insert_batch(FakeCur(), rows, executor=ex)
    assert keys == [(t(1), "SOLUSDT", "okx")]
    assert "ON CONFLICT (candle_open_at, symbol, exchange) DO NOTHING" in seen["sql"]
    assert "now(),now(),now()" in seen["template"]
    assert rk.insert_batch(FakeCur(), [], executor=ex) == []


def test_manifest_keys_dedup_and_rollback_guard():
    manifest = {"batches": [{"keys": [[t(1), "A", "okx"], [t(2), "A", "okx"]]}, {"keys": [[t(2), "A", "okx"]]}]}
    keys = rk.manifest_keys_for_rollback(manifest)
    assert [(rk.dt_to_ms(k[0]), k[1], k[2]) for k in keys] == [(t(1), "A", "okx"), (t(2), "A", "okx")]
    captured = {}

    def ex(cur, sql, chunk, template):
        captured.update(sql=sql, chunk=list(chunk))
        return [1] * len(chunk)

    assert rk.rollback_keys(FakeCur(), keys, executor=ex) == 2
    assert "gap_repair_" in captured["sql"] and "DELETE FROM futures_klines_15m" in captured["sql"]


class FakeConn:
    def __init__(self):
        self.commits = self.rollbacks = 0
        self.executed = []

    def cursor(self):
        conn = self

        class C:
            def execute(self, sql, *a):
                conn.executed.append(sql)

            def fetchall(self):
                return []

        return C()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _patch_planning(monkeypatch, rows):
    monkeypatch.setattr(rk, "plan_symbol", lambda *a, **k: {"rows": rows, "results": [
        {"symbol": "SOLUSDT", "prev": "", "next": "", "missing": len(rows), "repaired": len(rows), "unrecoverable": 0,
         "reason": "", "unrecoverable_times": []}]})
    monkeypatch.setattr(rk, "fetch_db_window", lambda *a, **k: {})


def test_dry_run_writes_nothing(monkeypatch, tmp_path):
    rows = [rk.build_row("okx", "SOLUSDT", "SOL", bar(1), rk.SRC_REST)]
    _patch_planning(monkeypatch, rows)
    called = []
    monkeypatch.setattr(rk, "insert_batch", lambda *a, **k: called.append(1) or [])
    db = type("DB", (), {"conn": FakeConn()})()
    gaps = {"okx": [rk.Gap("okx", "SOLUSDT", t(0), t(2))]}
    rep = rk.run_repair(db, gaps, apply=False, run_id="r1", out_dir=tmp_path)
    assert called == [] and db.conn.commits == 0 and rep["exchanges"]["okx"]["planned_rows"] == 1
    assert not list(tmp_path.glob("manifest_*.json"))


def test_apply_writes_manifest_per_batch(monkeypatch, tmp_path):
    rows = [rk.build_row("okx", "SOLUSDT", "SOL", bar(1), rk.SRC_REST)]
    _patch_planning(monkeypatch, rows)
    monkeypatch.setattr(rk, "insert_batch", lambda cur, batch, **k: [(t(1), "SOLUSDT", "okx")])
    db = type("DB", (), {"conn": FakeConn()})()
    gaps = {"okx": [rk.Gap("okx", "SOLUSDT", t(0), t(2))]}
    rep = rk.run_repair(db, gaps, apply=True, run_id="r2", out_dir=tmp_path)
    assert db.conn.commits == 1 and rep["exchanges"]["okx"]["inserted"] == 1
    m = json.loads((tmp_path / "manifest_r2.json").read_text(encoding="utf-8"))
    assert m["batches"][0]["keys"] == [[t(1), "SOLUSDT", "okx"]] and m["batches"][0]["sources"] == {"gap_repair_rest": 1}


# ------------------------------------------------------------------ REST (HTTP simulado)

class FakeHttp:
    def __init__(self, responses):
        self.responses, self.calls, self.requests = list(responses), [], 0

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


def test_rest_okx_instid_and_pagination():
    p1 = {"code": "0", "data": [[str(t(3)), "1", "2", "1", "1", "1", "10", "10", "1"],
                                [str(t(2)), "1", "2", "1", "1", "1", "10", "10", "1"]]}
    p2 = {"code": "0", "data": [[str(t(1)), "1", "2", "1", "1", "1", "10", "10", "1"]]}
    http = FakeHttp([p1, p2])
    prov = rk.RestProvider("okx", http, now_ms=lambda: t(100))
    out = prov.fetch("PEPEUSDT", "PEPE", t(1), t(3))
    assert sorted(out) == [t(1), t(2), t(3)]
    assert http.calls[0][1]["instId"] == "PEPE-USDT-SWAP" and http.calls[0][1]["after"] == t(3) + S
    with pytest.raises(rk.SourceUnavailable):
        rk.RestProvider("okx", http).fetch("X", None, t(1), t(2))


def test_rest_bybit_keeps_db_symbol_and_checks_retcode():
    ok = {"retCode": 0, "result": {"list": [[str(t(1)), "1", "2", "1", "1", "5", "50"]]}}
    http = FakeHttp([ok])
    out = rk.RestProvider("bybit", http, now_ms=lambda: t(100)).fetch("1000PEPEUSDT", "PEPE", t(1), t(1))
    assert http.calls[0][1]["symbol"] == "1000PEPEUSDT" and list(out) == [t(1)]
    with pytest.raises(rk.SourceUnavailable):
        rk.RestProvider("bybit", FakeHttp([{"retCode": 10001, "retMsg": "bad"}])).fetch("X", None, t(1), t(1))


def test_rate_limiter_spaces_requests():
    clock = {"now": 0.0}
    sleeps = []

    def sleep(d):
        sleeps.append(d)
        clock["now"] += d

    lim = rk.RateLimiter(5.0, clock=lambda: clock["now"], sleep=sleep)
    for _ in range(3):
        lim.wait()
    assert sum(sleeps) == pytest.approx(0.4)


def test_cli_defaults_to_detect():
    a = rk.build_parser().parse_args([])
    assert not a.repair and not a.apply and a.rollback is None


def test_okx_contract_units_variant_used_when_db_stores_contracts():
    """Filas OKX existentes con volume_base = `vol` (contratos, ctVal != 1): se inserta esa variante."""
    gap = rk.Gap("okx", "HBARUSDT", t(8), t(10))
    dbr = {t(i): db_row(i, vb="10", base="HBAR") for i in list(range(0, 9)) + list(range(10, 19))}
    src = {t(i): rk.Bar(t(i), Decimal(100), Decimal(101), Decimal(99), Decimal(100), Decimal("1000"), Decimal("1000"),
                        None, None, Decimal("10")) for i in range(0, 19)}  # volCcy=1000, vol=10
    plan = rk.plan_symbol("okx", "HBARUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_rest", src)])
    [row] = plan["rows"]
    assert row["volume_base"] == Decimal("10") and plan["results"][0]["units"] == ["contracts"]
    bad = {k: rk.Bar(k, b.o, b.h, b.l, b.c, b.vb, b.vq, None, None, Decimal("500")) for k, b in src.items()}
    plan = rk.plan_symbol("okx", "HBARUSDT", [gap], lambda lo, hi: dbr, [_Prov("gap_repair_rest", bad)])
    assert plan["rows"] == []  # ninguna variante reproduce las filas existentes


def test_rest_bybit_retries_transient_service_error():
    ok = {"retCode": 0, "result": {"list": [[str(t(1)), "1", "2", "1", "1", "5", "50"]]}}
    http = FakeHttp([{"retCode": 10016, "retMsg": "svc error"}, ok])
    http._sleep = lambda d: None
    out = rk.RestProvider("bybit", http, now_ms=lambda: t(100)).fetch("X", None, t(1), t(1))
    assert list(out) == [t(1)] and len(http.calls) == 2


def test_compare_tolerates_db_rounding_of_tiny_prices():
    row = {"price_open": Decimal("0.00000434"), "price_high": Decimal("0.00000434"), "price_low": Decimal("0.00000434"),
           "price_close": Decimal("0.00000434"), "volume_base": Decimal("10"), "volume_usd": Decimal("1000")}
    b = rk.Bar(T0, *(Decimal("0.000004335"),) * 4, Decimal("10"), Decimal("1000"))
    assert rk.compare_bar(row, b, "okx") == []
    assert rk.build_row("okx", "PEPEUSDT", "PEPE", b, rk.SRC_REST)["price_close"] == Decimal("0.00000434")


# ------------------------------------------------------------------ auditoria: --apply, manifiesto pending, --max-gaps

def _fake_main_env(monkeypatch, tmp_path, gaps):
    class DB:
        def __init__(self):
            self.conn = FakeConn()

        def close(self):
            pass

    monkeypatch.setattr(rk, "_DB", DB)
    monkeypatch.setattr(rk, "_load_env", lambda: None)
    monkeypatch.setattr(rk, "run_detect", lambda *a, **k: gaps)


def test_detect_apply_is_usage_error_and_never_touches_db(monkeypatch, tmp_path):
    created = []
    monkeypatch.setattr(rk, "_DB", lambda: created.append(1))
    with pytest.raises(SystemExit) as e:
        rk.main(["--detect", "--apply", "--out-dir", str(tmp_path)])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        rk.main(["--apply", "--out-dir", str(tmp_path)])
    assert e.value.code == 2
    assert created == []


def test_manifest_is_pending_on_disk_before_commit(monkeypatch, tmp_path):
    rows = [rk.build_row("okx", "SOLUSDT", "SOL", bar(1), rk.SRC_REST)]
    _patch_planning(monkeypatch, rows)
    monkeypatch.setattr(rk, "insert_batch", lambda cur, batch, **k: [(t(1), "SOLUSDT", "okx")])
    seen = {}
    conn = FakeConn()
    orig = conn.commit

    def commit():
        seen["m"] = json.loads((tmp_path / "manifest_r3.json").read_text(encoding="utf-8"))
        orig()

    conn.commit = commit
    db = type("DB", (), {"conn": conn})()
    rk.run_repair(db, {"okx": [rk.Gap("okx", "SOLUSDT", t(0), t(2))]}, apply=True, run_id="r3",
                  out_dir=tmp_path)
    assert seen["m"]["batches"][0]["status"] == "pending"
    assert seen["m"]["batches"][0]["keys"] == [[t(1), "SOLUSDT", "okx"]]
    final = json.loads((tmp_path / "manifest_r3.json").read_text(encoding="utf-8"))
    assert final["batches"][0]["status"] == "committed"


def test_rollback_includes_pending_batches():
    manifest = {"batches": [{"status": "pending", "keys": [[t(5), "A", "okx"]]},
                            {"status": "committed", "keys": [[t(6), "A", "okx"]]}]}
    keys = rk.manifest_keys_for_rollback(manifest)
    assert [rk.dt_to_ms(k[0]) for k in keys] == [t(5), t(6)]


def test_max_gaps_truncation_counts_as_omitted_exit_partial(monkeypatch, tmp_path):
    rows = [rk.build_row("okx", "SOLUSDT", "SOL", bar(1), rk.SRC_REST)]
    _patch_planning(monkeypatch, rows)
    gaps = {"okx": [rk.Gap("okx", "SOLUSDT", t(0), t(2)), rk.Gap("okx", "SOLUSDT", t(10), t(12))]}
    _fake_main_env(monkeypatch, tmp_path, gaps)
    code = rk.main(["--repair", "--max-gaps", "1", "--out-dir", str(tmp_path), "--exchange", "okx"])
    assert code == rk.EXIT_PARTIAL == 3
    rep = json.loads(next(tmp_path.glob("report_*.json")).read_text(encoding="utf-8"))
    assert rep["repair"]["exchanges"]["okx"]["omitted_gaps"] == 1
    gaps["okx"] = gaps["okx"][:1]
    code = rk.main(["--repair", "--max-gaps", "1", "--out-dir", str(tmp_path), "--exchange", "okx"])
    assert code == rk.EXIT_OK


# ------------------------------------------------------------------ modo diario --recent-days

class _DetectCur:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, None, None

    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchall(self):
        return self.rows


def test_detect_prev_margin_looks_back_but_filters_by_next_bar():
    since = rk.ms_to_dt(t(100))
    cur = _DetectCur([("SOLUSDT", rk.ms_to_dt(t(0)), rk.ms_to_dt(t(110)))])
    gaps = rk.detect_gaps(cur, "binance", since, rk.ms_to_dt(t(200)), prev_margin=timedelta(days=30))
    assert cur.params[1] == since - timedelta(days=30)  # ventana interior: barra anterior hasta 30 d antes
    assert cur.params[-1] == since and "candle_open_at >= %s" in cur.sql.split(") t")[1]  # filtro: barra siguiente
    assert gaps == [rk.Gap("binance", "SOLUSDT", t(0), t(110))] and gaps[0].missing == 109


def test_detect_without_margin_keeps_original_window():
    since = rk.ms_to_dt(t(100))
    cur = _DetectCur([])
    rk.detect_gaps(cur, "okx", since, rk.ms_to_dt(t(200)), symbols=["A"])
    assert cur.params == ["okx", since, rk.ms_to_dt(t(200)), ["A"], since]


def test_recent_days_cli(monkeypatch, tmp_path):
    seen = {}
    _fake_main_env(monkeypatch, tmp_path, {})
    monkeypatch.setattr(rk, "run_detect", lambda cur, ex, since, until, sym, margin: seen.update(
        since=since, margin=margin) or {})
    assert rk.main(["--recent-days", "3", "--out-dir", str(tmp_path)]) == rk.EXIT_OK
    age = datetime.now(UTC) - seen["since"]
    assert timedelta(days=3) <= age < timedelta(days=3, minutes=5)
    assert seen["margin"] == timedelta(days=rk.RECENT_PREV_MARGIN_DAYS)
    with pytest.raises(SystemExit) as e:
        rk.main(["--recent-days", "3", "--since", "2026-01-01", "--out-dir", str(tmp_path)])
    assert e.value.code == 2
