"""
test_signals.py — regression tests for the TII signal engine.

Run:  python test_signals.py

These exist so that "is the reversal logic right?" is a question you answer by
running a file, not by re-reading code. Add a case whenever you find a pattern
the scanner gets wrong.
"""
import sys
import importlib.util

import numpy as np
import pandas as pd

spec = importlib.util.spec_from_file_location("scanmod", "scan_weekly_v2_enriched.py")
s = importlib.util.module_from_spec(spec)
sys.modules["scanmod"] = s
spec.loader.exec_module(s)

FAILURES = []


def check(name, got, want):
    ok = got == want
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: got {got!r}, want {want!r}")
    if not ok:
        FAILURES.append(name)


def mk(closes, start="2020-01-06"):
    """Build a weekly OHLCV frame from a close path."""
    idx = pd.date_range(start, periods=len(closes), freq="W-MON")
    c = np.asarray(closes, dtype=float)
    return pd.DataFrame(
        {"Open": c, "High": c * 1.01, "Low": c * 0.99, "Close": c,
         "Volume": [1e6] * len(c)},
        index=idx,
    )


def pad(front_len, start_price=140.0, step=-1.0):
    """A long clean decline to sit in front of a test pattern."""
    return [start_price + step * i for i in range(front_len)]


# ---------------------------------------------------------------------------
print("\nCASE A - textbook Trend Reversal after a clean decline")
print("  (the pattern from the rules document; the old code returned "
      "'Not enough pivots' and missed it every time)")
path = pad(40) + [98, 96, 100, 106, 112, 118, 114, 108, 104, 106, 112, 118, 130]
dfA = mk(path)
sc, note, rev, piv = s.price_trend_score(dfA)
print(f"  pivots: {[(p.kind, round(p.value, 2)) for p in piv]}")
print(f"  note: {note}")
check("Case A trend_reversal detected", rev, True)
check("Case A price score", sc, 3)

# ---------------------------------------------------------------------------
print("\nCASE B - lower low, no reversal (must NOT fire)")
path = pad(40) + [98, 96, 100, 106, 112, 118, 114, 108, 94, 96, 99, 101, 103]
dfB = mk(path)
sc, note, rev, _ = s.price_trend_score(dfB)
print(f"  note: {note}")
check("Case B trend_reversal suppressed", rev, False)

# ---------------------------------------------------------------------------
print("\nCASE C - higher low but close has NOT cleared the high point")
path = pad(40) + [98, 96, 100, 106, 112, 118, 114, 108, 104, 106, 110, 114, 116]
dfC = mk(path)
sc, note, rev, _ = s.price_trend_score(dfC)
print(f"  note: {note}")
check("Case C trend_reversal suppressed", rev, False)

# ---------------------------------------------------------------------------
print("\nCASE D - New High references the prior weekly high CLOSE")
print("  (Chronicles ch.47: 'a weekly close that rises above the previous")
print("   weekly high close'. NOT the high of the bar.)")
# Prior close peak is 100. A close of 101.5 is 1.5% above it -> must fire,
# even though it is below the prior bar's HIGH of 101 * 1.01.
path = [80 + i * 0.5 for i in range(40)] + [100, 96, 98, 101.5]
nh, nh_note = s.new_high_flag(mk(path))
print(f"  note: {nh_note}")
check("Case D fires 1% above prior high close", nh, True)

# Under the 1% buffer -> must not fire.
path2 = [80 + i * 0.5 for i in range(40)] + [100, 96, 98, 100.5]
nh2, nh_note2 = s.new_high_flag(mk(path2))
print(f"  note: {nh_note2}")
check("Case D suppressed inside the 1% buffer", nh2, False)

# ---------------------------------------------------------------------------
print("\nCASE E - pivot noise resistance on a genuinely rising path")
print("  (old 3-bar fractal scored -3 on ~20% of genuinely rising paths)")
from collections import Counter
res = Counter()
for seed in range(300):
    rng = np.random.default_rng(seed)
    r = rng.normal(0.007, 0.03, 120)
    px = 100 * np.exp(np.cumsum(r))
    if px[-1] <= px[0]:
        continue
    sc, _, _, _ = s.price_trend_score(mk(list(px)))
    res[sc] += 1
tot = sum(res.values())
for k in sorted(res, reverse=True):
    print(f"    price_score {k:+d} -> {res[k]:4d} ({100 * res[k] / tot:.1f}%)")
false_down = 100 * res.get(-3, 0) / tot
print(f"  false 'Trend Down' rate on rising paths: {false_down:.1f}%")
check("Case E false-downtrend rate under 10%", false_down < 10.0, True)

# ---------------------------------------------------------------------------
print("\nCASE F - incomplete current week is dropped")
idx = pd.date_range(end=pd.Timestamp.now().normalize(), periods=5, freq="W-MON")
dfF = pd.DataFrame({"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
                    "Volume": 1.0}, index=idx)
trimmed = s.drop_incomplete_week(dfF)
check("Case F partial week removed", len(trimmed) < len(dfF), True)

# ---------------------------------------------------------------------------
print("\nCASE G - established uptrend must NOT be called a Trend Reversal")
print("  (67 of 87 reversal flags in the 2026-09-08 scan were uptrends)")
path = [100 * (1.012 ** i) + 4 * np.sin(i / 2.0) for i in range(90)]
dfG = mk(path)
sc, note, rev, _ = s.price_trend_score(dfG)
print(f"  note: {note}")
check("Case G no reversal in an established uptrend", rev, False)
check("Case G still scores the uptrend", sc, 3)

# ---------------------------------------------------------------------------
print("\nCASE H - stale breakout must NOT re-fire weeks later")
path = pad(40) + [98, 96, 100, 106, 112, 118, 114, 108, 104, 106, 112, 118, 130,
                  132, 134, 136]
dfH = mk(path)
sc, note, rev, _ = s.price_trend_score(dfH)
print(f"  note: {note}")
check("Case H stale breakout suppressed", rev, False)

# ---------------------------------------------------------------------------
print()
if FAILURES:
    print(f"{len(FAILURES)} FAILED: {', '.join(FAILURES)}")
    sys.exit(1)
print("All checks passed.")
