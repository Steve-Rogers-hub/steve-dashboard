from __future__ import annotations

import argparse
import math
import os
import shutil
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf


# ==============================
# Strategy parameters
# ==============================
TII_QUALIFY_THRESHOLD = 4

EMA_LEN = 28
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9

PHASE_LOOKBACK_WEEKS = 52
NEW_HIGH_BUFFER = 0.01

# --- Pivot / swing detection -------------------------------------------------
# PIVOT_SWING_BARS is the number of bars either side that a bar must exceed to
# count as a structural swing point. The original code used an implicit value of
# 1 (a 3-bar fractal), which flagged almost every weekly wiggle as a pivot and
# made the price component read noise rather than trend. 3 => a 7-bar swing.
PIVOT_SWING_BARS = 3

# Window (in weekly bars) searched for pivots. Widened from 30 so that a 7-bar
# swing definition still finds enough structure to work with. Tuned by sweep:
# swing=3/lookback=104 scored a genuine uptrend correctly 71.6% of the time with
# a 9.1% false 'Trend Down' rate. swing=4 cut false-downs to 7.6% but adds a week
# of lag before a pivot can confirm; swing>=5 stopped detecting the reversal
# pattern in the rules document altogether. See test_signals.py Case E.
PIVOT_LOOKBACK_BARS = 104

# Minimum weekly history required before we will score a ticker at all.
MIN_WEEKLY_BARS = 60

# Stops (permanent defaults)
DEFAULT_STOP_PCT = 0.15  # 15% fixed, per trade plan (initial stop and trailing stop from highest weekly close)

# Optional rules (keep simple for now)
BREAKEVEN_TRIGGER_PCT = 0.10  # move stop to >= entry after +10% gain

# Stall rule: if a position hasn't gained STALL_GAIN_THRESHOLD_PCT by STALL_CHECK_BARS
# weekly bars held, tighten the trailing stop to STALL_TIGHTENED_STOP_PCT below the
# highest weekly close since entry (per trade plan).
STALL_CHECK_BARS = 8
STALL_GAIN_THRESHOLD_PCT = 0.05
STALL_TIGHTENED_STOP_PCT = 0.07

# Fundamentals cache
DEFAULT_FUNDAMENTALS_CACHE = "fundamentals_cache.csv"
DEFAULT_REFRESH_DAYS = 14
DEFAULT_METADATA_PAUSE = 0.25


# ==============================
# Small utilities
# ==============================
YAHOO_TICKER_MAP = {
    "BRK.B": "BRK-B",
    "BF.B": "BF-B",
    "3M": "MMM",
}

CANONICAL_SCAN_COLUMNS = [
    "ticker",
    "company",
    "sector",
    "industry",
    "market_cap",
    "pe",
    "ps",
    "ev_ebit",
    "fcf_yield",
    "revenue_growth",
    "eps_growth",
    "fcf_growth",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roic",
    "roe",
    "debt_to_equity",
    "net_debt_ebitda",
    "current_ratio",
    "price",
    "return_1m",
    "return_3m",
    "return_6m",
    "return_12m",
    "volatility",
    "scan_date",
    "scan_tii",
    "scan_qualifies",
    "entry_signal",
    "new_high",
    "trend_reversal",
    "signal_type",
    "price_score",
    "volume_score",
    "ma_score",
    "macd_score",
    "suggested_entry_ref",
    "suggested_initial_stop",
    "scan_notes",
    "scan_error",
]

LEGACY_SCAN_COLUMNS = [
    "Ticker",
    "LastDate",
    "LastClose",
    "TII",
    "Qualifies",
    "EntrySignal",
    "NewHigh",
    "TrendReversal",
    "SignalType",
    "PriceScore",
    "VolumeScore",
    "MAScore",
    "MACDScore",
    "SuggestedEntryRef",
    "SuggestedInitialStop",
    "Notes",
    "Error",
]

CACHE_COLUMNS = [
    "ticker",
    "company",
    "sector",
    "industry",
    "market_cap",
    "pe",
    "ps",
    "ev_ebit",
    "fcf_yield",
    "revenue_growth",
    "eps_growth",
    "fcf_growth",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roic",
    "roe",
    "debt_to_equity",
    "net_debt_ebitda",
    "current_ratio",
    "fetched_at",
]


def normalize_ticker(t: str) -> str:
    t = (t or "").strip().upper()
    t = t.replace("$", "")
    if t in YAHOO_TICKER_MAP:
        return YAHOO_TICKER_MAP[t]
    t = t.replace(".", "-")
    return t


def scalar_from_series(x) -> float:
    """
    Safely extract a single float from a value that may be a scalar,
    a one-element Series (as yfinance sometimes returns), or a plain number.
    Raises RuntimeError if a Series is empty.
    """
    if isinstance(x, pd.Series):
        x = x.dropna()
        if len(x) == 0:
            raise RuntimeError("Series is empty")
        return float(x.iloc[-1])
    try:
        return float(x)
    except TypeError:
        return float(pd.Series(x).iloc[-1])


# Keep the old name as an alias so nothing else needs to change.
last_scalar = scalar_from_series


def safe_num(value) -> float:
    try:
        if value is None or value == "":
            return math.nan
        value = float(value)
        if math.isfinite(value):
            return value
    except Exception:
        pass
    return math.nan


def safe_positive(value) -> float:
    value = safe_num(value)
    return value if pd.notna(value) and value > 0 else math.nan


def pct_from_decimal(value) -> float:
    value = safe_num(value)
    if pd.isna(value):
        return math.nan
    if abs(value) <= 1.5:
        return value * 100.0
    return value


def normalize_debt_to_equity(value) -> float:
    value = safe_num(value)
    if pd.isna(value):
        return math.nan
    # Yahoo often reports debt/equity as a percentage-like number (e.g. 155.2),
    # while the dashboard expects a ratio-like scale.
    if abs(value) > 20:
        return value / 100.0
    return value


def safe_div(numerator, denominator) -> float:
    n = safe_num(numerator)
    d = safe_num(denominator)
    if pd.isna(n) or pd.isna(d) or d == 0:
        return math.nan
    return n / d


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def macd(close: pd.Series, fast=12, slow=26, signal=9):
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def compute_return_pct(series: pd.Series, lookback_bars: int) -> float:
    if series is None or len(series) <= lookback_bars:
        return math.nan
    latest = safe_num(series.iloc[-1])
    prior = safe_num(series.iloc[-lookback_bars - 1])
    if pd.isna(latest) or pd.isna(prior) or prior == 0:
        return math.nan
    return ((latest / prior) - 1.0) * 100.0


def compute_annualized_volatility_pct(series: pd.Series, lookback_bars: int = 63) -> float:
    if series is None or len(series) < 20:
        return math.nan
    returns = pd.to_numeric(series, errors="coerce").pct_change().dropna()
    if returns.empty:
        return math.nan
    window = returns.tail(lookback_bars)
    if len(window) < 10:
        return math.nan
    return float(window.std() * (252 ** 0.5) * 100.0)


def first_present(d: dict, *keys):
    for key in keys:
        if key in d and d.get(key) not in (None, ""):
            return d.get(key)
    return None


def flatten_yfinance_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance >= 0.2.x sometimes returns a MultiIndex column DataFrame when
    downloading a single ticker (columns like ('Close', 'AAPL')).
    This flattens it back to simple column names so the rest of the code
    can use df['Close'] safely regardless of yfinance version.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [col[0] for col in df.columns]
    return df


# ==============================
# Data download
# ==============================
def drop_incomplete_week(df: pd.DataFrame) -> pd.DataFrame:
    """
    Yahoo labels each weekly bar with the Monday that opens the week, and it
    returns a PARTIAL bar for the week currently in progress. dropna() does not
    remove it, so running the scan before the US Friday close scores an
    unfinished week: partial volume against a 28-period VEMA, a mid-week price
    treated as Friday's close, and New Highs that may not survive to the close.

    This matters from Melbourne, where the US Friday close lands Saturday
    morning local time. The rules document is explicit that all signals are
    triggered by Friday's weekly close, so we enforce it rather than assume it.
    """
    if df.empty:
        return df
    last_open = df.index[-1]
    if not isinstance(last_open, pd.Timestamp):
        return df
    week_end = last_open.tz_localize(None) if last_open.tzinfo else last_open
    week_end = week_end + pd.Timedelta(days=7)
    if week_end > pd.Timestamp.now():
        return df.iloc[:-1].copy()
    return df


def download_weekly(
    ticker: str,
    period: str = "max",
    enforce_week_close: bool = True,
) -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval="1wk",
        auto_adjust=True,
        progress=False,
        group_by="column",
        threads=True,
    )
    if df is not None and not df.empty:
        df = flatten_yfinance_df(df).dropna()
    # Yahoo intermittently returns a near-empty frame for period="max" on names
    # with decades of history (AVB, EA and EQR each came back with under 10 weekly
    # bars in the 2026-09-08 run). Fall back to a bounded period rather than
    # discarding the ticker.
    if period == "max" and (df is None or len(df) < MIN_WEEKLY_BARS):
        df = yf.download(
            ticker,
            period="10y",
            interval="1wk",
            auto_adjust=True,
            progress=False,
            group_by="column",
            threads=True,
        )
        if df is not None and not df.empty:
            df = flatten_yfinance_df(df).dropna()
    if df is None or df.empty:
        raise RuntimeError("No weekly data returned from Yahoo Finance")
    df = df.copy()
    if enforce_week_close:
        df = drop_incomplete_week(df)
    if len(df) < MIN_WEEKLY_BARS:
        raise RuntimeError(
            f"Only {len(df)} completed weekly bars available "
            f"(need {MIN_WEEKLY_BARS}) - insufficient history to score"
        )
    return df


def download_bars(ticker: str, period: str = "2y", interval: str = "1d") -> pd.DataFrame:
    df = yf.download(
        ticker,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        group_by="column",
        threads=True,
    )
    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker} (period={period}, interval={interval})")
    df = flatten_yfinance_df(df)
    return df.dropna().copy()


# ==============================
# Ticker loading
# ==============================
def load_tickers(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        raw = [ln.strip() for ln in f.readlines()]

    tickers: List[str] = []
    seen = set()
    for ln in raw:
        if not ln or ln.startswith("#"):
            continue
        sym = ln.split()[0]
        sym = normalize_ticker(sym)
        if sym and sym not in seen:
            tickers.append(sym)
            seen.add(sym)
    return tickers


# ==============================
# Pivot logic
# ==============================
@dataclass
class Pivot:
    kind: str
    date: pd.Timestamp
    value: float


def find_pivots(
    df: pd.DataFrame,
    lookback: int = PIVOT_LOOKBACK_BARS,
    swing: int = PIVOT_SWING_BARS,
) -> List[Pivot]:
    """
    Locate structural swing highs and lows.

    A bar is a swing high if its High is the highest of the `swing` bars
    either side of it (and likewise, inverted, for a swing low). With
    swing=1 this is the original 3-bar fractal; with swing=3 it requires
    the bar to dominate a 7-bar window, which filters out the week-to-week
    noise that previously dominated the price score.

    Ties are resolved strictly (>, <) so a flat run never produces a pivot.
    """
    if swing < 1:
        swing = 1

    w = df.iloc[-lookback:]
    n = len(w)
    pivots: List[Pivot] = []

    highs = w["High"].astype(float).to_numpy()
    lows = w["Low"].astype(float).to_numpy()

    for i in range(swing, n - swing):
        left = slice(i - swing, i)
        right = slice(i + 1, i + 1 + swing)

        if lows[i] < lows[left].min() and lows[i] < lows[right].min():
            pivots.append(Pivot("low", w.index[i], float(lows[i])))
        if highs[i] > highs[left].max() and highs[i] > highs[right].max():
            pivots.append(Pivot("high", w.index[i], float(highs[i])))

    pivots.sort(key=lambda p: p.date)
    return pivots


def detect_trend_reversal(df: pd.DataFrame, pivots: List[Pivot]):
    """
    The trade plan's Trend Reversal pattern: "Reversing a DOWNTREND to an uptrend."

    Pattern (per the rules document):
        1. price maps out a low                     -> L1
        2. the first rally creates a 'high point'   -> H1  (between L1 and L2)
        3. price pulls back to a HIGHER low         -> L2  (L2 > L1)
        4. a rally back through H1 triggers it      -> weekly close > H1

    Two extra conditions enforce what the document says in words but the raw
    pattern does not capture on its own:

      (a) DOWNTREND PRECONDITION. In any healthy uptrend the last two swing
          lows ascend, a swing high sits between them, and price is above it -
          so the bare pattern matches trivially. L1 must therefore be the
          lowest swing low in the window, and the swing high preceding L1 must
          be above H1 (a lower high into the low), confirming the structure
          being reversed was actually down.

      (b) FRESHNESS. The signal is the week the breakout happens, not every
          week thereafter. Without this the flag latches on and stays true
          until new pivots form, which is how it ended up firing on 67
          established uptrends in the 2026-09-08 scan.
    """
    lows = [p for p in pivots if p.kind == "low"]
    highs = [p for p in pivots if p.kind == "high"]

    if len(lows) < 2:
        return False, "No reversal (need two swing lows)"

    L1, L2 = lows[-2], lows[-1]
    between = [h for h in highs if L1.date < h.date < L2.date]
    if not between:
        return False, "No reversal (no rally high between the lows)"

    H1 = max(between, key=lambda h: h.value)

    if L2.value <= L1.value:
        return False, "No reversal (second low not higher)"

    # (a) the structure being reversed must have been a downtrend.
    # Part 1 defines a downtrend as lower highs AND lower lows, so test that
    # locally on the leg going into L1 rather than against the whole window.
    # An earlier version required L1 to be the lowest swing low in the entire
    # 104-week lookback; for any stock that has risen over two years the lowest
    # low sits at the start of the window, so that gate rejected everything.
    prior_lows = [p for p in lows if p.date < L1.date]
    prior_highs = [h for h in highs if h.date < L1.date]
    if prior_lows and prior_lows[-1].value <= L1.value:
        return False, "No reversal (no lower low into the base - trend was not down)"
    if prior_highs and prior_highs[-1].value <= H1.value:
        return False, "No reversal (no lower high into the base - trend was not down)"

    # (b) the breakout must be happening now, not weeks ago
    close_now = last_scalar(df["Close"].iloc[-1])
    close_prev = last_scalar(df["Close"].iloc[-2])
    if close_now <= H1.value:
        return False, "No reversal (close has not cleared the high point)"
    if close_prev > H1.value:
        return False, "No reversal (breakout already occurred in an earlier week)"

    return True, (f"Reversal: higher low {L2.value:.2f} > {L1.value:.2f}, "
                  f"close cleared {H1.value:.2f} this week")


def last_two(pivots: List[Pivot], kind: str) -> Optional[Tuple[Pivot, Pivot]]:
    items = [p for p in pivots if p.kind == kind]
    if len(items) < 2:
        return None
    return items[-2], items[-1]


# ==============================
# Scoring
# ==============================
def price_trend_score(df: pd.DataFrame):
    """
    Part 1 of the Trend Intensity Indicator.

    Order of evaluation matters and is deliberate:
      1. Established uptrend (HH & HL)            -> +3
      2. Confirmed Trend Reversal                 -> +3
      3. Established downtrend (LL & LH)          -> -3
      4. Anything else                            ->  0

    A confirmed reversal is ranked ahead of the downtrend test because the
    reversal is the fresher information: the breakout close is happening now,
    whereas the last two swing highs necessarily lag it. Previously a reversal
    scored 0, which cost it 3 points and — with weekly MACD still negative at a
    genuine turn — made a TII of 4 close to unreachable for exactly the signal
    the rules document calls "the challenge".
    """
    pivots = find_pivots(df)

    # Evaluated first, and independently: this must not sit behind an early return.
    trend_reversal, rev_note = detect_trend_reversal(df, pivots)

    highs = last_two(pivots, "high")
    lows = last_two(pivots, "low")

    is_up = is_down = False
    if highs is not None and lows is not None:
        h1, h2 = highs
        l1, l2 = lows
        is_up = (h2.value > h1.value) and (l2.value > l1.value)
        is_down = (h2.value < h1.value) and (l2.value < l1.value)

    # The reversal verdict is carried in the note on EVERY ticker, not just the
    # ones that fire. Without it there is no way to tell from the CSV which gate
    # is rejecting candidates, which is how an over-tight precondition went
    # unnoticed until the reversal count hit zero.
    if is_up:
        return +3, f"Trend Up (HH & HL) [{rev_note}]", trend_reversal, pivots
    if trend_reversal:
        return +3, f"Trend Reversal ({rev_note})", trend_reversal, pivots
    if is_down:
        return -3, f"Trend Down (LL & LH) [{rev_note}]", trend_reversal, pivots
    if highs is None or lows is None:
        return 0, f"Sideways/Mixed (insufficient swing structure) [{rev_note}]", trend_reversal, pivots
    return 0, f"Sideways/Mixed [{rev_note}]", trend_reversal, pivots


def new_high_flag(df: pd.DataFrame):
    """
    Stockradar Chronicles, Chapter 47 (New High Magic): the signal is "a weekly
    close (Friday) that rises above the previous weekly high CLOSE". The trade
    plan's shorthand - "1% or greater than a high" - means a high close, not the
    high of the bar.

    This was briefly changed to measure against prior weekly HIGHS on 8 Sep 2026,
    which cut the signal count from 16 to 6 in a 537-name scan. Reverted. Do not
    change it again without a source: Close is correct.

    "All-time" is genuinely all-time because download_weekly uses period="max".
    If that period is ever shortened, this label becomes a claim the data cannot
    support.
    """
    close = df["Close"]
    last = last_scalar(close.iloc[-1])

    prior_all_time = last_scalar(close.iloc[:-1].max())
    all_time_trigger = last >= (1 + NEW_HIGH_BUFFER) * prior_all_time

    phase_window = close.iloc[-(PHASE_LOOKBACK_WEEKS + 1):-1]
    prior_phase = last_scalar(phase_window.max()) if len(phase_window) else prior_all_time
    phase_trigger = last >= (1 + NEW_HIGH_BUFFER) * prior_phase

    if all_time_trigger:
        return True, f"All-time weekly high close (prior {prior_all_time:.2f})"
    if phase_trigger:
        return True, f"52-week weekly high close (prior {prior_phase:.2f})"
    return False, "No"


def volume_score(df: pd.DataFrame):
    vema = df["VEMA_28"]
    close = df["Close"]

    if pd.isna(vema.iloc[-1]):
        return 0, "VEMA not ready"

    v_slope = last_scalar(vema.iloc[-1]) - last_scalar(vema.iloc[-4]) if len(df) >= 5 else last_scalar(vema.iloc[-1]) - last_scalar(vema.iloc[-2])
    p_chg = last_scalar(close.iloc[-1]) - last_scalar(close.iloc[-4]) if len(df) >= 5 else last_scalar(close.iloc[-1]) - last_scalar(close.iloc[-2])

    if v_slope > 0 and p_chg > 0:
        return +2, "Volume expanding, price rising"
    if v_slope < 0 and p_chg > 0:
        return +1, "Volume contracting, price rising"
    if v_slope > 0 and p_chg < 0:
        return -1, "Volume expanding, price falling"
    if v_slope < 0 and p_chg < 0:
        return -2, "Volume contracting, price falling"
    return 0, "Neutral"


def ma_score(df: pd.DataFrame):
    e0 = df["EMA_28"].iloc[-1]
    e1 = df["EMA_28"].iloc[-2]
    if pd.isna(e0) or pd.isna(e1):
        return 0, "EMA not ready"

    c0 = last_scalar(df["Close"].iloc[-1])
    c1 = last_scalar(df["Close"].iloc[-2])
    e0 = last_scalar(e0)
    e1 = last_scalar(e1)

    if c0 > e0 and c1 <= e1:
        return +1, "Fresh cross above EMA"
    if c0 < e0 and c1 >= e1:
        return -1, "Fresh cross below EMA"
    return (+2, "Above EMA") if c0 > e0 else (-2, "Below EMA")


def macd_score(df: pd.DataFrame):
    macd_line = df["MACD"]
    sig = df["MACD_SIGNAL"]
    hist = df["MACD_HIST"]

    if pd.isna(macd_line.iloc[-1]) or pd.isna(sig.iloc[-1]) or pd.isna(hist.iloc[-1]):
        return 0, "MACD not ready"

    macd0 = last_scalar(macd_line.iloc[-1])
    sig0 = last_scalar(sig.iloc[-1])
    hist0 = last_scalar(hist.iloc[-1])
    hist1 = last_scalar(hist.iloc[-2])

    hist_rising = hist0 > hist1
    hist_pos = hist0 > 0

    lines_pos = (macd0 > 0) and (sig0 > 0)
    lines_neg = (macd0 < 0) and (sig0 < 0)

    if lines_pos:
        if hist_rising and hist_pos:
            return +3, "MACD +3"
        if hist_rising and (not hist_pos):
            return +2, "MACD +2"
        if (not hist_rising) and hist_pos:
            return +2, "MACD +2"
        return +1, "MACD +1"

    if lines_neg:
        if (not hist_rising) and (not hist_pos):
            return -3, "MACD -3"
        if (not hist_rising) and hist_pos:
            return -2, "MACD -2"
        if hist_rising and hist_pos:
            return -2, "MACD -2"
        return -1, "MACD -1"

    return (+1, "MACD mixed +1") if macd0 >= 0 else (-1, "MACD mixed -1")


# ==============================
# Main per-ticker compute
# ==============================
def compute_for_ticker(ticker: str, enforce_week_close: bool = True):
    df = download_weekly(ticker, enforce_week_close=enforce_week_close)

    df["EMA_28"] = ema(df["Close"], EMA_LEN)
    df["VEMA_28"] = ema(df["Volume"], EMA_LEN)
    m, s, h = macd(df["Close"], MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    df["MACD"], df["MACD_SIGNAL"], df["MACD_HIST"] = m, s, h

    p_sc, p_note, trend_rev, pivots = price_trend_score(df)
    nh, nh_note = new_high_flag(df)
    v_sc, v_note = volume_score(df)
    ma_sc, ma_note = ma_score(df)
    m_sc, m_note = macd_score(df)

    tii = int(p_sc + v_sc + ma_sc + m_sc)
    entry_signal = bool(nh or trend_rev)
    qualifies = bool((tii >= TII_QUALIFY_THRESHOLD) and entry_signal)

    last_close = last_scalar(df["Close"].iloc[-1])
    suggested_entry = last_close
    suggested_stop = suggested_entry * (1.0 - DEFAULT_STOP_PCT)

    signal_type = ("New High" if nh else ("Trend Reversal" if trend_rev else ""))

    meta = {
        "Ticker": ticker,
        "LastDate": str(df.index[-1].date()),
        "LastClose": last_close,
        "TII": tii,
        "Qualifies": qualifies,
        "EntrySignal": entry_signal,
        "NewHigh": bool(nh),
        "TrendReversal": bool(trend_rev),
        "SignalType": signal_type,
        "PriceScore": int(p_sc),
        "VolumeScore": int(v_sc),
        "MAScore": int(ma_sc),
        "MACDScore": int(m_sc),
        "SuggestedEntryRef": suggested_entry,
        "SuggestedInitialStop": suggested_stop,
        "Notes": f"Price:{p_note} | NewHigh:{nh_note} | Vol:{v_note} | MA:{ma_note} | MACD:{m_note}",
        "Error": "",
    }
    return df, pivots, meta


# ==============================
# Fundamentals enrichment
# ==============================
def load_fundamentals_cache(path: str) -> Dict[str, dict]:
    if not path or not os.path.exists(path):
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if df.empty or "ticker" not in df.columns:
        return {}
    cache = {}
    for _, row in df.iterrows():
        record = row.to_dict()
        ticker = normalize_ticker(str(record.get("ticker", "")))
        if ticker:
            record["ticker"] = ticker
            cache[ticker] = record
    return cache


def save_fundamentals_cache(path: str, cache: Dict[str, dict]) -> None:
    if not path:
        return
    rows = []
    for ticker, record in cache.items():
        row = {col: record.get(col, pd.NA) for col in CACHE_COLUMNS}
        row["ticker"] = ticker
        rows.append(row)
    df = pd.DataFrame(rows, columns=CACHE_COLUMNS)
    df.sort_values("ticker", inplace=True, ignore_index=True)
    df.to_csv(path, index=False)


def cache_is_fresh(record: dict, refresh_days: int) -> bool:
    fetched_at = record.get("fetched_at")
    if not fetched_at:
        return False
    ts = pd.to_datetime(fetched_at, errors="coerce", utc=True)
    if pd.isna(ts):
        return False
    age = pd.Timestamp.now(tz="UTC") - ts
    return age <= pd.Timedelta(days=refresh_days)


def statement_row_lookup(df: Optional[pd.DataFrame], candidates: List[str]) -> Optional[pd.Series]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return None
    lowered = {str(idx).strip().lower(): idx for idx in df.index}
    for candidate in candidates:
        idx = lowered.get(candidate.lower())
        if idx is not None:
            return df.loc[idx]
    return None


def compute_fcf_growth_pct_from_statement(ticker_obj) -> float:
    candidates = []
    for attr in ["cashflow", "cash_flow"]:
        try:
            stmt = getattr(ticker_obj, attr)
            if isinstance(stmt, pd.DataFrame) and not stmt.empty:
                candidates.append(stmt)
        except Exception:
            continue

    for stmt in candidates:
        row = statement_row_lookup(stmt, ["Free Cash Flow", "FreeCashFlow"])
        if row is None:
            continue
        values = pd.to_numeric(row, errors="coerce").dropna()
        if len(values) >= 2:
            latest = safe_num(values.iloc[0])
            previous = safe_num(values.iloc[1])
            if pd.notna(latest) and pd.notna(previous) and previous != 0:
                return ((latest / previous) - 1.0) * 100.0
    return math.nan


def compute_roic_pct_from_statements(ticker_obj, info: dict) -> float:
    try:
        financials = getattr(ticker_obj, "financials", None)
        balance_sheet = getattr(ticker_obj, "balance_sheet", None)
    except Exception:
        financials = None
        balance_sheet = None

    if not isinstance(financials, pd.DataFrame) or financials.empty:
        return math.nan
    if not isinstance(balance_sheet, pd.DataFrame) or balance_sheet.empty:
        return math.nan

    op_income_row = statement_row_lookup(financials, ["Operating Income", "EBIT"])
    equity_row = statement_row_lookup(balance_sheet, ["Stockholders Equity", "Total Equity Gross Minority Interest", "Common Stock Equity"])
    debt_row = statement_row_lookup(balance_sheet, ["Total Debt", "Long Term Debt And Capital Lease Obligation"])
    cash_row = statement_row_lookup(balance_sheet, ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"])

    if op_income_row is None or equity_row is None:
        return math.nan

    def _first_numeric(row):
        s = pd.to_numeric(row, errors="coerce").dropna()
        return safe_num(s.iloc[0]) if not s.empty else math.nan

    op_income = _first_numeric(op_income_row)
    equity = _first_numeric(equity_row)
    debt = _first_numeric(debt_row) if debt_row is not None else 0.0
    cash = _first_numeric(cash_row) if cash_row is not None else 0.0

    if pd.isna(debt):
        debt = 0.0
    if pd.isna(cash):
        cash = 0.0

    if pd.isna(op_income) or pd.isna(equity):
        return math.nan

    tax_rate = safe_num(first_present(info, "effectiveTaxRate"))
    if pd.isna(tax_rate):
        tax_rate = 0.21
    elif tax_rate > 1:
        tax_rate = tax_rate / 100.0

    nopat = op_income * (1.0 - tax_rate)
    invested_capital = equity + debt - cash
    if invested_capital <= 0:
        return math.nan
    return (nopat / invested_capital) * 100.0


def compute_ev_ebit_from_statements(ticker_obj, info: dict) -> float:
    """
    Prefer reading EBIT directly from the income statement rather than
    estimating it as revenue * operating_margin, which can be inaccurate
    for companies with unusual items.  Falls back to the estimate if the
    statement is unavailable.
    """
    enterprise_value = safe_positive(first_present(info, "enterpriseValue"))
    if pd.isna(enterprise_value):
        return math.nan

    # --- Attempt 1: income statement ---
    try:
        financials = getattr(ticker_obj, "financials", None)
        if isinstance(financials, pd.DataFrame) and not financials.empty:
            ebit_row = statement_row_lookup(financials, ["EBIT", "Operating Income", "OperatingIncome"])
            if ebit_row is not None:
                values = pd.to_numeric(ebit_row, errors="coerce").dropna()
                if not values.empty:
                    ebit = safe_num(values.iloc[0])
                    if pd.notna(ebit) and ebit > 0:
                        return safe_div(enterprise_value, ebit)
    except Exception:
        pass

    # --- Attempt 2: revenue * operating_margin estimate (fallback) ---
    total_revenue = safe_positive(first_present(info, "totalRevenue"))
    operating_margins_decimal = safe_num(first_present(info, "operatingMargins"))
    if pd.notna(total_revenue) and pd.notna(operating_margins_decimal) and operating_margins_decimal > 0:
        if operating_margins_decimal > 1:
            operating_margins_decimal = operating_margins_decimal / 100.0
        ebit_est = total_revenue * operating_margins_decimal
        return safe_div(enterprise_value, ebit_est)

    return math.nan


def build_fundamentals_record_from_info(ticker: str, ticker_obj, info: dict) -> dict:
    company = first_present(info, "shortName", "longName")
    sector = first_present(info, "sector", "sectorDisp")
    industry = first_present(info, "industry", "industryDisp")

    market_cap = safe_positive(first_present(info, "marketCap"))
    pe = safe_num(first_present(info, "trailingPE", "forwardPE"))
    ps = safe_num(first_present(info, "priceToSalesTrailing12Months"))
    revenue_growth = pct_from_decimal(first_present(info, "revenueGrowth"))
    eps_growth = pct_from_decimal(first_present(info, "earningsGrowth"))
    gross_margin = pct_from_decimal(first_present(info, "grossMargins"))
    operating_margin = pct_from_decimal(first_present(info, "operatingMargins"))
    net_margin = pct_from_decimal(first_present(info, "profitMargins"))
    roe = pct_from_decimal(first_present(info, "returnOnEquity"))
    current_ratio = safe_num(first_present(info, "currentRatio"))
    debt_to_equity = normalize_debt_to_equity(first_present(info, "debtToEquity"))

    ev_ebit = compute_ev_ebit_from_statements(ticker_obj, info)

    free_cash_flow = safe_num(first_present(info, "freeCashflow"))
    fcf_yield = safe_div(free_cash_flow, market_cap)
    if pd.notna(fcf_yield):
        fcf_yield *= 100.0

    ebitda = safe_positive(first_present(info, "ebitda"))
    total_debt = safe_num(first_present(info, "totalDebt"))
    total_cash = safe_num(first_present(info, "totalCash"))
    if pd.isna(total_debt):
        total_debt = 0.0
    if pd.isna(total_cash):
        total_cash = 0.0
    net_debt_ebitda = safe_div(total_debt - total_cash, ebitda)

    fcf_growth = compute_fcf_growth_pct_from_statement(ticker_obj)
    roic = compute_roic_pct_from_statements(ticker_obj, info)

    return {
        "ticker": ticker,
        "company": company,
        "sector": sector,
        "industry": industry,
        "market_cap": market_cap,
        "pe": pe,
        "ps": ps,
        "ev_ebit": ev_ebit,
        "fcf_yield": fcf_yield,
        "revenue_growth": revenue_growth,
        "eps_growth": eps_growth,
        "fcf_growth": fcf_growth,
        "gross_margin": gross_margin,
        "operating_margin": operating_margin,
        "net_margin": net_margin,
        "roic": roic,
        "roe": roe,
        "debt_to_equity": debt_to_equity,
        "net_debt_ebitda": net_debt_ebitda,
        "current_ratio": current_ratio,
        "fetched_at": pd.Timestamp.now(tz="UTC").isoformat(),
    }


def fetch_fundamentals_from_yfinance(ticker: str, pause_seconds: float = DEFAULT_METADATA_PAUSE) -> dict:
    ticker_obj = yf.Ticker(ticker)

    info = {}
    try:
        info = ticker_obj.get_info()
    except Exception:
        try:
            info = ticker_obj.info
        except Exception:
            info = {}

    if not isinstance(info, dict):
        info = {}

    record = build_fundamentals_record_from_info(ticker, ticker_obj, info)

    if pause_seconds and pause_seconds > 0:
        time.sleep(pause_seconds)

    return record


def get_fundamentals_for_ticker(
    ticker: str,
    cache: Dict[str, dict],
    refresh_days: int,
    pause_seconds: float,
) -> dict:
    ticker = normalize_ticker(ticker)
    cached = cache.get(ticker)
    if cached and cache_is_fresh(cached, refresh_days):
        return {k: cached.get(k, pd.NA) for k in CACHE_COLUMNS if k != "fetched_at"}

    try:
        record = fetch_fundamentals_from_yfinance(ticker, pause_seconds=pause_seconds)
    except Exception:
        if cached:
            return {k: cached.get(k, pd.NA) for k in CACHE_COLUMNS if k != "fetched_at"}
        record = {"ticker": ticker}
        for col in CACHE_COLUMNS:
            if col not in record:
                record[col] = pd.NA
        record["fetched_at"] = pd.Timestamp.now(tz="UTC").isoformat()

    cache[ticker] = {col: record.get(col, pd.NA) for col in CACHE_COLUMNS}
    return {k: record.get(k, pd.NA) for k in CACHE_COLUMNS if k != "fetched_at"}


# ==============================
# Positions / stops
# ==============================
POSITIONS_COLS = [
    "Ticker", "EntryDate", "EntryPrice", "StopPct",
    "HighestClose", "Breakeven", "BarsHeld"
]


def ensure_positions_file(path: str):
    if os.path.exists(path):
        return
    df = pd.DataFrame(columns=POSITIONS_COLS)
    df.to_csv(path, index=False)


def load_positions(path: str) -> pd.DataFrame:
    ensure_positions_file(path)
    df = pd.read_csv(path)

    if "WeeksHeld" in df.columns and "BarsHeld" not in df.columns:
        df = df.rename(columns={"WeeksHeld": "BarsHeld"})

    for c in POSITIONS_COLS:
        if c not in df.columns:
            df[c] = pd.NA

    df["Ticker"] = df["Ticker"].astype(str).map(normalize_ticker)
    df = df.drop_duplicates(subset=["Ticker"], keep="first").reset_index(drop=True)

    return df[POSITIONS_COLS]


def update_trailing_stops(positions: pd.DataFrame, close_map: Dict[str, float]) -> pd.DataFrame:
    if positions.empty:
        return positions.assign(CurrentClose=pd.NA, CurrentStop=pd.NA, Action=pd.NA)

    out = positions.copy()
    out["CurrentClose"] = pd.NA
    out["CurrentStop"] = pd.NA
    out["Action"] = pd.NA
    out["StallActive"] = pd.NA

    for i, r in out.iterrows():
        t = normalize_ticker(str(r["Ticker"]))

        if t not in close_map:
            out.at[i, "Action"] = "NO DATA"
            continue

        close_now = float(close_map[t])
        entry = float(r["EntryPrice"]) if pd.notna(r["EntryPrice"]) else close_now

        highest = float(r["HighestClose"]) if pd.notna(r["HighestClose"]) else entry
        highest = max(highest, close_now)

        bars = int(r["BarsHeld"]) if pd.notna(r["BarsHeld"]) else 0
        bars += 1

        gain_pct = (close_now / entry) - 1.0 if entry else 0.0
        stall_active = bool(bars >= STALL_CHECK_BARS and gain_pct < STALL_GAIN_THRESHOLD_PCT)

        stop_pct = STALL_TIGHTENED_STOP_PCT if stall_active else DEFAULT_STOP_PCT
        out.at[i, "StopPct"] = stop_pct
        out.at[i, "StallActive"] = stall_active

        breakeven_val = r["Breakeven"]
        breakeven = False
        if isinstance(breakeven_val, bool):
            breakeven = breakeven_val
        elif isinstance(breakeven_val, str):
            breakeven = breakeven_val.strip().lower() in ("true", "1", "yes", "y")
        elif pd.notna(breakeven_val):
            breakeven = bool(breakeven_val)

        if (not breakeven) and (close_now >= entry * (1.0 + BREAKEVEN_TRIGGER_PCT)):
            breakeven = True

        stop_level = highest * (1.0 - stop_pct)
        if breakeven:
            stop_level = max(stop_level, entry)

        act = "HOLD"
        if close_now <= stop_level:
            act = "STOP HIT (EXIT)"

        out.at[i, "Ticker"] = t
        out.at[i, "HighestClose"] = highest
        out.at[i, "BarsHeld"] = bars
        out.at[i, "Breakeven"] = breakeven
        out.at[i, "CurrentClose"] = close_now
        out.at[i, "CurrentStop"] = stop_level
        out.at[i, "Action"] = act

    return out


# ==============================
# Plotting
# ==============================
def plot_weekly_chart(ticker: str, df: pd.DataFrame, pivots: List[Pivot], meta: dict, outdir: str = "charts") -> str:
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, f"{ticker}_weekly.png")

    closes = df["Close"]
    ema28 = df["EMA_28"]

    last_close = last_scalar(closes.iloc[-1])
    entry_ref = float(meta.get("SuggestedEntryRef", last_close))
    stop_ref = float(meta.get("SuggestedInitialStop", last_close * (1 - DEFAULT_STOP_PCT)))

    fig = plt.figure(figsize=(12, 6))
    plt.plot(df.index, closes, label="Weekly Close")
    plt.plot(df.index, ema28, label="EMA 28")

    ph_dates = [p.date for p in pivots if p.kind == "high"]
    ph_vals = [p.value for p in pivots if p.kind == "high"]
    pl_dates = [p.date for p in pivots if p.kind == "low"]
    pl_vals = [p.value for p in pivots if p.kind == "low"]

    if ph_dates:
        plt.scatter(ph_dates, ph_vals, marker="^", label="Pivot Highs")
    if pl_dates:
        plt.scatter(pl_dates, pl_vals, marker="v", label="Pivot Lows")

    plt.axhline(entry_ref, linestyle="--", linewidth=1, label=f"Entry ref ~ {entry_ref:.2f}")
    plt.axhline(stop_ref, linestyle="--", linewidth=1, label=f"Initial stop ~ {stop_ref:.2f}")

    title = f"{ticker} Weekly | TII {meta.get('TII')} | {meta.get('SignalType','')}"
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close(fig)

    return path


# ==============================
# Row builders
# ==============================
def build_momentum_record_from_bars(bars: Optional[pd.DataFrame]) -> dict:
    if bars is None or bars.empty or "Close" not in bars.columns:
        return {
            "price": math.nan,
            "return_1m": math.nan,
            "return_3m": math.nan,
            "return_6m": math.nan,
            "return_12m": math.nan,
            "volatility": math.nan,
        }

    close = pd.to_numeric(bars["Close"], errors="coerce").dropna()
    if close.empty:
        return {
            "price": math.nan,
            "return_1m": math.nan,
            "return_3m": math.nan,
            "return_6m": math.nan,
            "return_12m": math.nan,
            "volatility": math.nan,
        }

    return {
        "price": safe_num(close.iloc[-1]),
        "return_1m": compute_return_pct(close, 21),
        "return_3m": compute_return_pct(close, 63),
        "return_6m": compute_return_pct(close, 126),
        "return_12m": compute_return_pct(close, 252),
        "volatility": compute_annualized_volatility_pct(close, 63),
    }


def build_canonical_row(ticker: str, meta: dict, fundamentals: dict, momentum: dict) -> dict:
    row = {col: pd.NA for col in CANONICAL_SCAN_COLUMNS}
    row["ticker"] = ticker

    for col in ["company", "sector", "industry", "market_cap", "pe", "ps", "ev_ebit", "fcf_yield",
                "revenue_growth", "eps_growth", "fcf_growth", "gross_margin", "operating_margin",
                "net_margin", "roic", "roe", "debt_to_equity", "net_debt_ebitda", "current_ratio"]:
        row[col] = fundamentals.get(col, pd.NA)

    for col in ["price", "return_1m", "return_3m", "return_6m", "return_12m", "volatility"]:
        row[col] = momentum.get(col, pd.NA)

    row["scan_date"] = meta.get("LastDate", pd.NA)
    row["scan_tii"] = meta.get("TII", pd.NA)
    row["scan_qualifies"] = meta.get("Qualifies", pd.NA)
    row["entry_signal"] = meta.get("EntrySignal", pd.NA)
    row["new_high"] = meta.get("NewHigh", pd.NA)
    row["trend_reversal"] = meta.get("TrendReversal", pd.NA)
    row["signal_type"] = meta.get("SignalType", pd.NA)
    row["price_score"] = meta.get("PriceScore", pd.NA)
    row["volume_score"] = meta.get("VolumeScore", pd.NA)
    row["ma_score"] = meta.get("MAScore", pd.NA)
    row["macd_score"] = meta.get("MACDScore", pd.NA)
    row["suggested_entry_ref"] = meta.get("SuggestedEntryRef", pd.NA)
    row["suggested_initial_stop"] = meta.get("SuggestedInitialStop", pd.NA)
    row["scan_notes"] = meta.get("Notes", pd.NA)
    row["scan_error"] = meta.get("Error", pd.NA)

    if pd.isna(row["price"]) and pd.notna(meta.get("LastClose")):
        row["price"] = meta.get("LastClose")

    return row


def add_legacy_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Ticker"] = out["ticker"]
    out["LastDate"] = out["scan_date"]
    out["LastClose"] = out["price"]
    out["TII"] = out["scan_tii"]
    out["Qualifies"] = out["scan_qualifies"]
    out["EntrySignal"] = out["entry_signal"]
    out["NewHigh"] = out["new_high"]
    out["TrendReversal"] = out["trend_reversal"]
    out["SignalType"] = out["signal_type"]
    out["PriceScore"] = out["price_score"]
    out["VolumeScore"] = out["volume_score"]
    out["MAScore"] = out["ma_score"]
    out["MACDScore"] = out["macd_score"]
    out["SuggestedEntryRef"] = out["suggested_entry_ref"]
    out["SuggestedInitialStop"] = out["suggested_initial_stop"]
    out["Notes"] = out["scan_notes"]
    out["Error"] = out["scan_error"]
    return out


def ordered_output_columns() -> List[str]:
    return CANONICAL_SCAN_COLUMNS + LEGACY_SCAN_COLUMNS


def maybe_copy_file(src: str, dest: str) -> None:
    if not dest:
        return
    dest_dir = os.path.dirname(os.path.abspath(dest))
    if dest_dir:
        os.makedirs(dest_dir, exist_ok=True)
    shutil.copyfile(src, dest)


# ==============================
# Main
# ==============================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default="tickers.txt", help="Path to tickers file (one per line)")
    ap.add_argument("--positions", default="positions.csv", help="Path to positions.csv")
    ap.add_argument("--plot", default="", help="Ticker to plot (e.g. TMDX)")
    ap.add_argument("--max", type=int, default=0, help="Max tickers to scan (0 = all)")

    ap.add_argument("--interval", default="1d", choices=["1d", "1wk"], help="Interval for current close / momentum bars")
    ap.add_argument("--period", default="2y", help="History period for current close / momentum bars download")

    ap.add_argument("--output", default="weekly_scan_output.csv", help="Output CSV path")
    ap.add_argument("--positions-output", default="positions_updated.csv", help="Output CSV path for updated stops")
    ap.add_argument("--dashboard-output", default="", help="Optional second copy of the scan CSV for your dashboard repo")
    ap.add_argument("--fundamentals-cache", default=DEFAULT_FUNDAMENTALS_CACHE, help="CSV cache for slow-changing fundamentals")
    ap.add_argument("--refresh-days", type=int, default=DEFAULT_REFRESH_DAYS, help="Refresh fundamentals if older than this many days")
    ap.add_argument("--metadata-pause", type=float, default=DEFAULT_METADATA_PAUSE, help="Pause between yfinance fundamentals calls")
    ap.add_argument("--skip-fundamentals", action="store_true", help="Skip fundamentals enrichment and output blanks instead")
    ap.add_argument("--allow-partial-week", action="store_true",
                    help="Score the current, unfinished weekly bar. Off by default: the trade "
                         "plan triggers on Friday's weekly close. Use ONLY to reproduce old "
                         "behaviour when A/B-diffing against a pre-fix baseline.")

    args = ap.parse_args()

    tickers = load_tickers(args.tickers)
    if args.max and args.max > 0:
        tickers = tickers[:args.max]

    print(f"Loaded {len(tickers)} tickers.")

    close_map: Dict[str, float] = {}
    bars_map: Dict[str, pd.DataFrame] = {}
    print("Downloading price bars...")
    for i, t in enumerate(tickers, 1):
        try:
            bars = download_bars(t, period=args.period, interval=args.interval)
            bars_map[t] = bars
            close_map[t] = last_scalar(bars["Close"].iloc[-1])
        except Exception:
            pass
        if i % 50 == 0:
            print(f"  bars downloaded: {i}/{len(tickers)}")

    cache = load_fundamentals_cache(args.fundamentals_cache)

    rows = []
    plot_target = normalize_ticker(args.plot) if args.plot else ""
    plot_df = None
    plot_pivots = None
    plot_meta = None

    print("Running scan + fundamentals enrichment...")
    for i, t in enumerate(tickers, 1):
        try:
            df, pivots, meta = compute_for_ticker(
                t, enforce_week_close=not args.allow_partial_week
            )
            if plot_target and t == plot_target:
                plot_df, plot_pivots, plot_meta = df, pivots, meta
        except Exception as e:
            meta = {
                "Ticker": t,
                "LastDate": pd.Timestamp.utcnow().date().isoformat(),
                "LastClose": close_map.get(t, math.nan),
                "TII": pd.NA,
                "Qualifies": False,
                "EntrySignal": False,
                "NewHigh": False,
                "TrendReversal": False,
                "SignalType": "",
                "PriceScore": pd.NA,
                "VolumeScore": pd.NA,
                "MAScore": pd.NA,
                "MACDScore": pd.NA,
                "SuggestedEntryRef": close_map.get(t, math.nan),
                "SuggestedInitialStop": math.nan,
                "Notes": "",
                "Error": str(e),
            }

        fundamentals = {}
        if args.skip_fundamentals:
            fundamentals = {col: pd.NA for col in CACHE_COLUMNS if col not in ("ticker", "fetched_at")}
            fundamentals["ticker"] = t
        else:
            fundamentals = get_fundamentals_for_ticker(
                t,
                cache=cache,
                refresh_days=args.refresh_days,
                pause_seconds=args.metadata_pause,
            )

        momentum = build_momentum_record_from_bars(bars_map.get(t))
        row = build_canonical_row(t, meta, fundamentals, momentum)
        rows.append(row)

        if i % 50 == 0:
            print(f"  scanned: {i}/{len(tickers)}")

    out = pd.DataFrame(rows, columns=CANONICAL_SCAN_COLUMNS)
    out = add_legacy_alias_columns(out)
    out = out[ordered_output_columns()].copy()

    if "scan_qualifies" in out.columns:
        out["scan_qualifies"] = out["scan_qualifies"].astype("boolean")
    for legacy_bool in ["Qualifies", "EntrySignal", "NewHigh", "TrendReversal"]:
        if legacy_bool in out.columns:
            out[legacy_bool] = out[legacy_bool].astype("boolean")

    sort_cols = [c for c in ["scan_qualifies", "scan_tii", "ticker"] if c in out.columns]
    ascending = [False, False, True][:len(sort_cols)]
    out = out.sort_values(sort_cols, ascending=ascending, na_position="last").reset_index(drop=True)

    out.to_csv(args.output, index=False)
    save_fundamentals_cache(args.fundamentals_cache, cache)

    if args.dashboard_output:
        maybe_copy_file(args.output, args.dashboard_output)

    print("\n=== WEEKLY SCAN (enriched) ===")
    preview_cols = [
        "ticker", "scan_tii", "scan_qualifies", "signal_type",
        "pe", "revenue_growth", "gross_margin", "roe", "debt_to_equity", "current_ratio", "scan_error"
    ]
    preview_cols = [c for c in preview_cols if c in out.columns]
    print(out[preview_cols].head(20).to_string(index=False))
    print(f"\nSaved: {args.output}")
    print(f"Saved cache: {args.fundamentals_cache}")
    if args.dashboard_output:
        print(f"Copied scan CSV to dashboard path: {args.dashboard_output}")

    positions = load_positions(args.positions)
    updated = update_trailing_stops(positions, close_map)
    updated.to_csv(args.positions_output, index=False)
    print(f"Saved: {args.positions_output} (updated stops + actions)")

    if plot_target:
        if plot_df is None:
            print(f"(plot) Ticker {plot_target} not found in tickers list.")
        else:
            path = plot_weekly_chart(plot_target, plot_df, plot_pivots or [], plot_meta or {})
            print(f"Saved chart: {path}")


if __name__ == "__main__":
    main()
