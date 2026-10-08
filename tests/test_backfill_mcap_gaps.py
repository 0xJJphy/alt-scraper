import math
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import backfill_mcap_gaps as bf  # noqa: E402

D0 = date(2026, 1, 1)


def _series(n, step=0.01, noise=None):
    """closes for days D0..D0+n-1 and mcaps dated T = close(T-1) * supply."""
    rets = [step * math.sin(i * 2.3999) for i in range(n)]  # irregular, deterministic daily returns
    closes = {D0 + timedelta(days=i): math.exp(sum(rets[:i + 1])) for i in range(n)}
    caps = {}
    for i in range(1, n):
        t = D0 + timedelta(days=i)
        caps[t] = closes[t - timedelta(days=1)] * 1e9 * (1 + (noise(i) if noise else 0))
    return caps, closes


def test_same_coin_passes():
    caps, closes = _series(60)
    res = bf.check(caps, closes)
    assert res["ok"] and res["pairs"] >= 50 and res["median_abs_diff"] < 1e-9


def test_supply_jumps_reported_not_rejected():
    caps, closes = _series(60)
    for i, t in enumerate(sorted(caps)):
        if i >= 20:
            caps[t] *= 1.3  # one supply step
        if i >= 40:
            caps[t] *= 1.3
    res = bf.check(caps, closes)
    assert res["ok"] and res["jump_days"] == 2


def test_other_coin_fails():
    caps, closes = _series(60)
    other = {t: v * math.exp(0.03 * ((hash(t) % 5) - 2)) for t, v in caps.items()}
    res = bf.check(other, closes)
    assert not res["ok"] and res["reason"] == "check_failed"


def test_insufficient_pairs():
    caps, closes = _series(15)
    res = bf.check(caps, closes)
    assert not res["ok"] and res["reason"] == "insufficient_pairs"


def test_mcap_row_t_is_close_of_t_minus_1():
    # If mcap were aligned to close(T) instead of close(T-1), the same-coin check must fail.
    caps, closes = _series(60, step=0.05)
    shifted = {t: closes[t] * 1e9 for t in caps if t in closes}
    assert bf.check(caps, closes)["ok"]
    assert not bf.check(shifted, closes)["ok"]


def _days(end, ks):
    return {end - timedelta(days=k) for k in ks}


def test_auto_detects_new_base_and_recent_gap_only():
    end = date(2026, 10, 8)
    start = end - timedelta(days=365)
    full = _days(end, range(1, 366))
    have = {
        "OK": full,                                  # complete
        "LATE": _days(end, range(1, 100)),           # CG history starts late: old holes only
        "RECENT": full - _days(end, [2, 3]),         # snapshot missed two recent days
        # "NEW" has no rows at all
    }
    todo = bf.select_todo(["OK", "LATE", "RECENT", "NEW"], have, start, end,
                          auto=True, recent_days=7, min_missing=1, max_bases=0)
    assert todo == ["NEW", "RECENT"]  # no rows first, then most recent days missing


def test_auto_cap_and_manual_mode():
    end = date(2026, 10, 8)
    start = end - timedelta(days=365)
    have = {"LATE": _days(end, range(1, 100))}
    assert bf.select_todo(["A", "B", "C"], {}, start, end, True, 7, 1, 2) == ["A", "B"]
    assert bf.select_todo(["LATE"], have, start, end, False, 7, 1, 0) == ["LATE"]
    assert bf.select_todo(["LATE"], have, start, end, True, 7, 1, 0) == []
