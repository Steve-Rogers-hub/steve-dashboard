import os
import io
import json
import math
import numpy as np
import pandas as pd
import streamlit as st
import gspread
import yfinance as yf
from datetime import datetime
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1CL5Rs7eMU4M0K5pgrKYVb7tUhJVuXn8fDIax9L0_31k"
WORKSHEET_NAME = "US Trades - USD"
HEADER_ROW_INDEX = 8
SCAN_CSV_PATH = "weekly_scan_output.csv"
TOP_SCAN_LIMIT = 100

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

HISTORY_LOOKBACK_DAYS = 550
DOWNLOAD_BATCH_SIZE = 40
QUALIFICATION_THRESHOLD = 4
DEFAULT_PORTFOLIO_SIZE = 10
DEFAULT_SECTOR_CAP = 2

# Dashboard modes
MODE_PURE_TII = "Pure TII"
MODE_ENHANCED = "Enhanced (Factor Model)"


# =========================
# GOOGLE SHEETS
# =========================
def try_get_gspread_client():
    secret_file_candidates = [
        "/etc/secrets/google-service-account.json",
        "google-service-account.json",
    ]

    creds = None
    credential_source = None

    try:
        for candidate in secret_file_candidates:
            if os.path.exists(candidate):
                creds = Credentials.from_service_account_file(candidate, scopes=SCOPES)
                credential_source = candidate
                break

        if creds is None:
            creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
            if not creds_json:
                return None, None, "Missing Google credentials."
            service_account_info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
            credential_source = "GOOGLE_SERVICE_ACCOUNT_JSON"

        return gspread.authorize(creds), credential_source, None
    except Exception as e:
        return None, None, str(e)


def get_gspread_client():
    client, credential_source, error = try_get_gspread_client()
    if client is None:
        st.error(f"Could not load Google credentials: {error}")
        st.stop()
    return client


@st.cache_data(ttl=300)
def load_sheet_data(sheet_id, worksheet_name, header_row_index):
    client = get_gspread_client()
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_values()

    if not data or len(data) <= header_row_index:
        return pd.DataFrame()

    headers = data[header_row_index]
    rows = data[header_row_index + 1:]

    clean_headers = []
    seen = {}
    for i, h in enumerate(headers):
        col = h.strip() if h else f"Column_{i + 1}"
        if col in seen:
            seen[col] += 1
            col = f"{col}_{seen[col]}"
        else:
            seen[col] = 1
        clean_headers.append(col)

    df = pd.DataFrame(rows, columns=clean_headers)
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df


def clean_portfolio_dataframe(df):
    if df.empty:
        return df

    df = df.copy()
    df = df.replace("#REF!", pd.NA)
    df = df.replace("#VALUE!", pd.NA)
    df = df.replace("None", pd.NA)
    df = df.replace("nan", pd.NA)

    numeric_cols = [
        "Quantity", "Purchase Price", "Buy Cost", "Buy Brokerage",
        "Total Purchase Costs", "Latest Market Price", "Market Value",
        "Stop/Sell Price", "Gross Profit/Loss", "Net Profit/Loss",
        "% Gain/Loss", "Previous Day Close Price", "Previous Day Market Value",
        "Highest Market Price", "Dividend Income", "Dividend Franking Credits",
        "Sold Date", "Sold Price", "Sale Price", "Sold Value", "Sold Brokerage",
        "Total Sale Proceeds", "Gross Realised Profit/Loss",
        "Net Realised Profit Loss", "Net Realised Profit/Loss",
        "Gain/Loss.2", "Gain/Loss_2",
    ]

    for col in numeric_cols:
        if col in df.columns:
            cleaned = (
                df[col].astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.replace("%", "", regex=False)
                .str.replace("(", "-", regex=False)
                .str.replace(")", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(cleaned, errors="coerce")

    for col in ["Trade Date", "Sold Date"]:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            df[col] = dt.dt.strftime("%d-%b-%Y")
            df.loc[dt.isna(), col] = ""

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def extract_sheet_ticker(value):
    if pd.isna(value):
        return ""
    s = str(value).strip().upper()
    if ":" in s:
        s = s.split(":")[-1]
    return clean_symbol(s)


@st.cache_data(ttl=300)
def load_current_positions_data(sheet_id, worksheet_name, header_row_index):
    client, credential_source, error = try_get_gspread_client()
    if client is None:
        return pd.DataFrame(), f"Google Sheets unavailable: {error}"

    try:
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
    except Exception as e:
        return pd.DataFrame(), f"Could not load worksheet '{worksheet_name}': {e}"

    if not data or len(data) <= header_row_index:
        return pd.DataFrame(), f"Worksheet '{worksheet_name}' is empty or missing the expected header row."

    headers = data[header_row_index]
    rows = data[header_row_index + 1:]

    clean_headers = []
    seen = {}
    for i, h in enumerate(headers):
        col = h.strip() if h else f"Column_{i + 1}"
        if col in seen:
            seen[col] += 1
            col = f"{col}_{seen[col]}"
        else:
            seen[col] = 1
        clean_headers.append(col)

    raw_df = pd.DataFrame(rows, columns=clean_headers)
    raw_df = raw_df.replace("", pd.NA).dropna(how="all").dropna(axis=1, how="all")
    df = clean_portfolio_dataframe(raw_df)
    if df.empty:
        return pd.DataFrame(), f"Worksheet '{worksheet_name}' contains no usable rows."

    stock_col = find_matching_column(df, ["Stock", "Ticker", "Symbol"])
    company_col = find_matching_column(df, ["Company Name", "Company", "Name"])
    status_col = find_matching_column(df, ["Position Status", "Status"])
    trade_date_col = find_matching_column(df, ["Trade Date", "Date"])
    quantity_col = find_matching_column(df, ["Quantity"])
    purchase_price_col = find_matching_column(df, ["Purchase Price", "Buy Price"])
    market_price_col = find_matching_column(df, ["Market Price", "Latest Market Price", "Current Price"])
    market_value_col = find_matching_column(df, ["Market Value", "Day Market Value", "Current Market Value"])
    net_pl_col = find_matching_column(df, ["Net Profit/Loss", "Net P/L"])
    gain_pct_col = find_matching_column(df, ["% Gain/Loss", "Gain/Loss", "Gain/Loss.2", "Gain/Loss_2"])
    sold_date_col = find_matching_column(df, ["Sold Date", "Sale Date"])

    if stock_col is None:
        return pd.DataFrame(), "The portfolio sheet does not contain a Stock/Ticker column."

    x = df.copy()
    x["ticker"] = x[stock_col].apply(extract_sheet_ticker)
    x = x[x["ticker"] != ""].copy()

    if status_col is not None:
        status_text = x[status_col].astype(str).str.strip().str.lower()
        open_mask = status_text.eq("open")
    elif sold_date_col is not None:
        open_mask = x[sold_date_col].astype(str).str.strip().eq("")
    else:
        open_mask = pd.Series(True, index=x.index)

    x = x.loc[open_mask].copy()
    if x.empty:
        return pd.DataFrame(), f"No open positions were found in '{worksheet_name}'."

    x["company"] = x[company_col].fillna(x["ticker"]) if company_col is not None else x["ticker"]
    x["position_status"] = x[status_col].fillna("Open") if status_col is not None else "Open"

    def maybe_col(col_name):
        return x[col_name] if col_name is not None and col_name in x.columns else pd.Series(index=x.index, dtype=object)

    out = pd.DataFrame({
        "Trade Date": maybe_col(trade_date_col),
        "ticker": x["ticker"],
        "company": x["company"],
        "Position Status": x["position_status"],
        "Quantity": maybe_col(quantity_col),
        "Purchase Price": maybe_col(purchase_price_col),
        "Market Price": maybe_col(market_price_col),
        "Market Value": maybe_col(market_value_col),
        "Net Profit/Loss": maybe_col(net_pl_col),
        "% Gain/Loss": maybe_col(gain_pct_col),
    })

    for c in ["Quantity", "Purchase Price", "Market Price", "Market Value", "Net Profit/Loss", "% Gain/Loss"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.sort_values(["ticker", "Trade Date"], ascending=[True, True]).reset_index(drop=True)
    out.attrs["sheet_status"] = f"Loaded live positions from Google Sheets ({worksheet_name}) via {credential_source}."
    return out, None


def enrich_positions_with_dashboard_data(positions_df, full_universe_df, top_signals_df, dashboard_mode):
    if positions_df.empty:
        return positions_df

    x = positions_df.copy()

    if dashboard_mode == MODE_PURE_TII:
        scan_cols = [c for c in [
            "ticker", "scan_tii", "signal_type", "suggested_entry_ref",
            "suggested_initial_stop", "scan_qualifies", "conviction"
        ] if c in full_universe_df.columns]
        if scan_cols:
            x = x.merge(full_universe_df[scan_cols].drop_duplicates(subset=["ticker"]), on="ticker", how="left")
    else:
        full_cols = [c for c in [
            "ticker", "tii_rank", "tii_score", "score_growth", "score_quality",
            "score_momentum", "score_balance_sheet", "score_value", "data_confidence"
        ] if c in full_universe_df.columns]
        if full_cols:
            x = x.merge(full_universe_df[full_cols].drop_duplicates(subset=["ticker"]), on="ticker", how="left")

        top_cols = [c for c in ["ticker", "conviction", "model_weight_pct", "ai_recommendation"]
                    if c in top_signals_df.columns]
        if top_cols:
            x = x.merge(top_signals_df[top_cols].drop_duplicates(subset=["ticker"]), on="ticker", how="left")

        if "conviction" not in x.columns:
            x["conviction"] = x.apply(classify_conviction, axis=1)
        else:
            x["conviction"] = x.apply(
                lambda row: row["conviction"] if pd.notna(row.get("conviction")) and str(row.get("conviction")).strip() != ""
                else classify_conviction(row),
                axis=1,
            )

    return x


# =========================
# SCAN CSV
# =========================
@st.cache_data(ttl=300)
def load_scan_data(csv_path):
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return df

    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


def find_matching_column(df, candidates):
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def detect_scan_name_column(df):
    return find_matching_column(df, ["ticker", "symbol", "stock", "name", "company", "scan_name"])


def clean_symbol(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper().replace("/", "-")


def extract_universe_symbols(df_scan):
    if df_scan.empty:
        return []
    name_col = detect_scan_name_column(df_scan)
    if not name_col:
        return []
    symbols = [clean_symbol(v) for v in df_scan[name_col].tolist()]
    symbols = [s for s in symbols if s]
    return list(dict.fromkeys(symbols))


# =========================
# MARKET DATA
# =========================
def flatten_download_frame(raw):
    if raw is None or raw.empty:
        return {}

    out = {}

    if isinstance(raw.columns, pd.MultiIndex):
        level0 = list(raw.columns.get_level_values(0))
        fields_first = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

        if set(level0).issubset(fields_first):
            tickers = list(dict.fromkeys(raw.columns.get_level_values(1)))
            for ticker in tickers:
                try:
                    frame = raw.xs(ticker, axis=1, level=1).copy()
                    frame = frame.dropna(how="all")
                    if not frame.empty:
                        out[ticker] = frame
                except Exception:
                    continue
        else:
            tickers = list(dict.fromkeys(raw.columns.get_level_values(0)))
            for ticker in tickers:
                try:
                    frame = raw[ticker].copy()
                    frame = frame.dropna(how="all")
                    if not frame.empty:
                        out[ticker] = frame
                except Exception:
                    continue
    else:
        frame = raw.copy().dropna(how="all")
        if not frame.empty:
            out["SINGLE"] = frame

    for ticker, frame in list(out.items()):
        frame = frame.reset_index()
        date_col = "Date" if "Date" in frame.columns else frame.columns[0]
        frame[date_col] = pd.to_datetime(frame[date_col], errors="coerce").dt.tz_localize(None)
        frame = frame.rename(columns={date_col: "Date"})
        keep_cols = [c for c in ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in frame.columns]
        out[ticker] = (
            frame[keep_cols]
            .dropna(subset=["Date"])
            .sort_values("Date")
            .reset_index(drop=True)
        )

    return out


@st.cache_data(ttl=60 * 60 * 12)
def download_price_history(symbols, lookback_days=HISTORY_LOOKBACK_DAYS):
    if not symbols:
        return {}

    end = pd.Timestamp.utcnow().normalize().tz_localize(None) + pd.Timedelta(days=1)
    start = end - pd.Timedelta(days=lookback_days)
    history_map = {}

    for i in range(0, len(symbols), DOWNLOAD_BATCH_SIZE):
        batch = symbols[i:i + DOWNLOAD_BATCH_SIZE]
        try:
            raw = yf.download(
                tickers=batch,
                start=start.date().isoformat(),
                end=end.date().isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
                group_by="ticker",
            )
            batch_map = flatten_download_frame(raw)
            if len(batch) == 1 and "SINGLE" in batch_map:
                history_map[batch[0]] = batch_map["SINGLE"]
            else:
                for ticker in batch:
                    if ticker in batch_map:
                        history_map[ticker] = batch_map[ticker]
        except Exception:
            continue

    return history_map


@st.cache_data(ttl=60 * 60 * 24)
def fetch_symbol_metadata(symbols):
    import time
    meta = {}
    symbols = list(symbols)[:500]
    batch_size = 20

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            tickers_obj = yf.Tickers(" ".join(batch))
            for sym in batch:
                try:
                    info = tickers_obj.tickers[sym].info or {}
                    meta[sym] = {
                        "sector": info.get("sector") or "",
                        "market_cap": info.get("marketCap"),
                        "short_name": info.get("shortName") or info.get("longName") or "",
                    }
                except Exception:
                    meta[sym] = {"sector": "", "market_cap": np.nan, "short_name": ""}
        except Exception:
            for sym in batch:
                try:
                    info = yf.Ticker(sym).info or {}
                    meta[sym] = {
                        "sector": info.get("sector") or "",
                        "market_cap": info.get("marketCap"),
                        "short_name": info.get("shortName") or info.get("longName") or "",
                    }
                except Exception:
                    meta[sym] = {"sector": "", "market_cap": np.nan, "short_name": ""}
        time.sleep(0.5)

    return meta


# =========================
# SHARED MARKET HELPERS
# =========================
def compute_macd(close_series):
    ema12 = close_series.ewm(span=12, adjust=False).mean()
    ema26 = close_series.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    hist = macd_line - signal_line
    return macd_line, signal_line, hist


def resample_to_weekly(df_daily):
    frame = df_daily.copy().set_index("Date")
    weekly = pd.DataFrame({
        "Open": frame["Open"].resample("W-FRI").first(),
        "High": frame["High"].resample("W-FRI").max(),
        "Low": frame["Low"].resample("W-FRI").min(),
        "Close": frame["Close"].resample("W-FRI").last(),
        "Volume": frame["Volume"].resample("W-FRI").sum(),
    }).dropna(subset=["Open", "High", "Low", "Close"])
    return weekly.reset_index()


# =========================
# UNIVERSE BUILD
# =========================
def compute_history_features(history_map):
    rows = []
    for symbol, hist in history_map.items():
        if hist is None or hist.empty or "Close" not in hist.columns:
            continue
        h = hist.copy().sort_values("Date")
        close = pd.to_numeric(h["Close"], errors="coerce").dropna()
        if len(close) < 30:
            continue

        latest_price = float(close.iloc[-1])

        def trailing_return(period):
            if len(close) <= period:
                return np.nan
            base = close.iloc[-period - 1]
            if pd.isna(base) or base == 0:
                return np.nan
            return (latest_price / float(base) - 1.0) * 100.0

        daily_ret = close.pct_change().dropna()
        vol = daily_ret.tail(63).std() * np.sqrt(252) * 100 if len(daily_ret) >= 20 else np.nan

        rows.append({
            "ticker": symbol,
            "price": latest_price,
            "return_1m": trailing_return(21),
            "return_3m": trailing_return(63),
            "return_6m": trailing_return(126),
            "return_12m": trailing_return(252),
            "volatility": vol,
        })

    return pd.DataFrame(rows)


def load_master_universe():
    scan_df = load_scan_data(SCAN_CSV_PATH)
    if scan_df.empty:
        return pd.DataFrame()

    universe = scan_df.copy()

    symbol_col = find_matching_column(universe, ["ticker", "symbol", "stock", "scan_name"])
    if symbol_col is None:
        raise ValueError("No ticker/symbol column found in weekly_scan_output.csv")

    universe["ticker"] = universe[symbol_col].apply(clean_symbol)
    universe = universe[universe["ticker"] != ""].copy()

    company_col = find_matching_column(universe, ["company", "name", "short_name", "long_name", "stock"])
    if company_col is not None and "company" not in universe.columns:
        universe["company"] = universe[company_col].astype(str).str.strip()
    elif "company" not in universe.columns:
        universe["company"] = universe["ticker"]

    symbols = universe["ticker"].dropna().astype(str).unique().tolist()

    history_map = download_price_history(symbols)
    history_df = compute_history_features(history_map)

    meta = fetch_symbol_metadata(symbols)
    meta_rows = []
    for s in symbols:
        m = meta.get(s, {})
        meta_rows.append({
            "ticker": s,
            "sector_meta": m.get("sector", ""),
            "market_cap_meta": m.get("market_cap", np.nan),
            "short_name_meta": m.get("short_name", ""),
        })
    meta_df = pd.DataFrame(meta_rows)

    history_cols = ["price", "return_1m", "return_3m", "return_6m", "return_12m", "volatility"]
    for col in history_cols:
        if col not in universe.columns:
            universe[col] = np.nan

    universe = universe.merge(history_df, on="ticker", how="left", suffixes=("", "_hist"))
    universe = universe.merge(meta_df, on="ticker", how="left")

    for col in history_cols:
        hist_col = f"{col}_hist"
        if hist_col in universe.columns:
            universe[col] = pd.to_numeric(universe[col], errors="coerce")
            universe[hist_col] = pd.to_numeric(universe[hist_col], errors="coerce")
            universe[col] = universe[col].fillna(universe[hist_col])

    drop_hist_cols = [f"{c}_hist" for c in history_cols if f"{c}_hist" in universe.columns]
    if drop_hist_cols:
        universe = universe.drop(columns=drop_hist_cols)

    if "sector" not in universe.columns:
        universe["sector"] = universe.get("sector_meta", "")
    else:
        universe["sector"] = universe["sector"].fillna("")
        universe.loc[universe["sector"].astype(str).str.strip() == "", "sector"] = universe["sector_meta"]

    if "market_cap" not in universe.columns:
        universe["market_cap"] = universe.get("market_cap_meta", np.nan)
    else:
        universe["market_cap"] = pd.to_numeric(universe["market_cap"], errors="coerce")
        universe["market_cap"] = universe["market_cap"].fillna(universe["market_cap_meta"])

    blank_company = universe["company"].astype(str).str.strip().isin(["", "nan", "None"])
    ticker_like_company = (
        universe["company"].astype(str).str.strip().str.upper()
        == universe["ticker"].astype(str).str.strip().str.upper()
    )
    company_needs_backfill = blank_company | ticker_like_company
    universe.loc[company_needs_backfill, "company"] = (
        universe.loc[company_needs_backfill, "short_name_meta"]
        .fillna(universe.loc[company_needs_backfill, "ticker"])
    )

    drop_cols = [c for c in ["sector_meta", "market_cap_meta", "short_name_meta"] if c in universe.columns]
    universe = universe.drop(columns=drop_cols)

    return universe


# =========================
# PURE TII ENGINE
# Mode: use scan_tii directly from the CSV. No factor model.
# Signals = qualified stocks (TII >= 4 + entry signal).
# =========================
def _parse_bool_col(series):
    """Normalise a boolean-ish column to Python bool."""
    def _parse(v):
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("true", "1", "yes")
    return series.map(_parse)


def tii_conviction_pure(row):
    """Conviction bucket for Pure TII mode."""
    qualifies = row.get("scan_qualifies", False)
    tii = row.get("scan_tii", np.nan)
    if pd.isna(tii):
        return "No Data"
    if not qualifies:
        return "Not Qualified"
    tii = int(tii)
    if tii >= 9:
        return "Strong Signal"
    if tii >= 7:
        return "Signal"
    return "Borderline"


def build_signal_summary(row):
    """One-line summary of entry/stop for Pure TII cards."""
    sig = str(row.get("signal_type", "")).strip()
    entry = row.get("suggested_entry_ref", np.nan)
    stop = row.get("suggested_initial_stop", np.nan)
    parts = []
    if sig:
        parts.append(sig)
    try:
        if pd.notna(entry):
            parts.append(f"Entry ~{float(entry):.2f}")
    except (TypeError, ValueError):
        pass
    try:
        if pd.notna(stop):
            parts.append(f"Stop ~{float(stop):.2f}")
    except (TypeError, ValueError):
        pass
    return " | ".join(parts) if parts else "—"


def build_pure_tii_view(universe_df):
    """
    Pure TII mode.
    Returns: full_universe, signals_df, portfolio_df
    - full_universe: all stocks ranked by scan_tii
    - signals_df:   qualified stocks only (scan_qualifies=True), sorted by TII
    - portfolio_df: top 10 signals with equal-weight sizing
    """
    x = universe_df.copy()

    # Numeric TII
    x["scan_tii"] = pd.to_numeric(
        x.get("scan_tii", pd.Series(dtype=float)), errors="coerce"
    )
    # Boolean qualification
    if "scan_qualifies" in x.columns:
        x["scan_qualifies"] = _parse_bool_col(x["scan_qualifies"])
    else:
        x["scan_qualifies"] = False

    # Mirror to tii_score for display compatibility with shared UI helpers
    x["tii_score"] = x["scan_tii"]

    # Conviction
    x["conviction"] = x.apply(tii_conviction_pure, axis=1)

    # Signal summary string
    x["signal_summary"] = x.apply(build_signal_summary, axis=1)

    # Rank all stocks by scan_tii
    x["tii_rank"] = (
        x["scan_tii"].rank(method="min", ascending=False).astype("Int64")
    )

    # Full universe: all stocks, sorted by TII
    full_universe = x.sort_values("scan_tii", ascending=False).reset_index(drop=True)

    # Signals: qualified stocks only
    signals = x[x["scan_qualifies"] == True].copy()  # noqa: E712

    # Sort: TII descending, then New High before Trend Reversal
    _sig_order = {"New High": 0, "Trend Reversal": 1}
    signals["_so"] = signals.get(
        "signal_type", pd.Series("", index=signals.index)
    ).map(lambda s: _sig_order.get(str(s).strip(), 2))
    signals = signals.sort_values(["scan_tii", "_so"], ascending=[False, True])
    signals = signals.drop(columns=["_so"]).reset_index(drop=True)
    signals["display_rank"] = np.arange(1, len(signals) + 1)

    # Portfolio: top 10 signals, equal weight
    portfolio_size = min(10, len(signals))
    portfolio = signals.head(portfolio_size).copy()
    if portfolio_size > 0:
        portfolio["model_weight_pct"] = round(100.0 / portfolio_size, 1)
    else:
        portfolio["model_weight_pct"] = 0.0

    return full_universe, signals, portfolio


# =========================
# ENHANCED ENGINE (Factor Model)
# =========================
def safe_numeric(series, fill_value=np.nan):
    return pd.to_numeric(series, errors="coerce").fillna(fill_value)


def clip_series(series, lower_q=0.02, upper_q=0.98):
    s = safe_numeric(series)
    if s.notna().sum() == 0:
        return s
    lower = s.quantile(lower_q)
    upper = s.quantile(upper_q)
    return s.clip(lower=lower, upper=upper)


def rank_pct(series, ascending=True):
    s = safe_numeric(series)
    valid = s.notna()
    if valid.sum() == 0:
        return pd.Series(np.nan, index=series.index)
    ranked = s[valid].rank(pct=True, ascending=ascending)
    out = pd.Series(np.nan, index=series.index, dtype=float)
    out.loc[valid] = ranked * 100
    return out


def zscore(series):
    s = safe_numeric(series)
    if s.notna().sum() < 2:
        return pd.Series(0.0, index=series.index)
    std = s.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return (s - s.mean()) / std


def fill_missing_with_median_by_group(df, value_col, group_col):
    out = df[value_col].copy()
    if group_col in df.columns:
        group_medians = df.groupby(group_col)[value_col].transform("median")
        out = out.fillna(group_medians)
    out = out.fillna(df[value_col].median())
    return out


def find_first_existing(df, candidates, default=np.nan):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series(default, index=df.index)


def standardise_factor_columns(df):
    x = df.copy()

    x["ticker"] = find_first_existing(x, ["ticker", "Ticker", "symbol", "Symbol"]).astype(str)
    x["company"] = find_first_existing(x, ["company", "Company", "name", "Name"]).astype(str)
    x["sector"] = find_first_existing(x, ["sector", "Sector", "gics_sector"]).astype(str)
    x["industry"] = find_first_existing(x, ["industry", "Industry", "gics_industry"]).astype(str)

    x["market_cap"] = safe_numeric(find_first_existing(x, ["market_cap", "Market Cap", "marketCap", "MarketCap"]))
    x["price"] = safe_numeric(find_first_existing(x, ["price", "Price", "last_price", "Last Price"]))

    x["pe"] = safe_numeric(find_first_existing(x, ["pe", "PE", "p_e", "trailing_pe", "forward_pe"]))
    x["ps"] = safe_numeric(find_first_existing(x, ["ps", "PS", "price_sales", "price_to_sales"]))
    x["ev_ebit"] = safe_numeric(find_first_existing(x, ["ev_ebit", "EV/EBIT", "ev_to_ebit"]))
    x["fcf_yield"] = safe_numeric(find_first_existing(x, ["fcf_yield", "FCF Yield", "free_cash_flow_yield"]))

    x["revenue_growth"] = safe_numeric(find_first_existing(x, ["revenue_growth", "Revenue Growth", "rev_growth", "sales_growth"]))
    x["eps_growth"] = safe_numeric(find_first_existing(x, ["eps_growth", "EPS Growth", "earnings_growth"]))
    x["fcf_growth"] = safe_numeric(find_first_existing(x, ["fcf_growth", "FCF Growth", "free_cash_flow_growth"]))

    x["gross_margin"] = safe_numeric(find_first_existing(x, ["gross_margin", "Gross Margin"]))
    x["operating_margin"] = safe_numeric(find_first_existing(x, ["operating_margin", "Operating Margin", "op_margin"]))
    x["net_margin"] = safe_numeric(find_first_existing(x, ["net_margin", "Net Margin"]))
    x["roic"] = safe_numeric(find_first_existing(x, ["roic", "ROIC"]))
    x["roe"] = safe_numeric(find_first_existing(x, ["roe", "ROE"]))

    x["debt_to_equity"] = safe_numeric(find_first_existing(x, ["debt_to_equity", "Debt/Equity", "d_e"]))
    x["net_debt_ebitda"] = safe_numeric(find_first_existing(x, ["net_debt_ebitda", "Net Debt/EBITDA"]))
    x["current_ratio"] = safe_numeric(find_first_existing(x, ["current_ratio", "Current Ratio"]))

    x["return_1m"] = safe_numeric(find_first_existing(x, ["return_1m", "1M Return", "perf_1m", "price_change_1m"]))
    x["return_3m"] = safe_numeric(find_first_existing(x, ["return_3m", "3M Return", "perf_3m", "price_change_3m"]))
    x["return_6m"] = safe_numeric(find_first_existing(x, ["return_6m", "6M Return", "perf_6m", "price_change_6m"]))
    x["return_12m"] = safe_numeric(find_first_existing(x, ["return_12m", "12M Return", "perf_12m", "price_change_12m"]))
    x["volatility"] = safe_numeric(find_first_existing(x, ["volatility", "Volatility", "vol_90d", "realized_vol"]))

    return x


def build_macro_overlay(macro_regime="Neutral"):
    regime = str(macro_regime).strip().lower()
    overlays = {
        "risk on":      {"growth": 1.15, "quality": 0.95, "value": 0.95,  "momentum": 1.10, "balance_sheet": 0.90},
        "neutral":      {"growth": 1.00, "quality": 1.00, "value": 1.00,  "momentum": 1.00, "balance_sheet": 1.00},
        "risk off":     {"growth": 0.85, "quality": 1.15, "value": 1.05,  "momentum": 0.95, "balance_sheet": 1.20},
        "inflationary": {"growth": 0.90, "quality": 1.05, "value": 1.15,  "momentum": 0.95, "balance_sheet": 1.10},
        "disinflation": {"growth": 1.10, "quality": 1.05, "value": 0.95,  "momentum": 1.05, "balance_sheet": 0.95},
    }
    return overlays.get(regime, overlays["neutral"])


def compute_tii_scores(df, macro_regime="Neutral"):
    x = standardise_factor_columns(df).copy()

    if "sector" not in x.columns:
        x["sector"] = "Unknown"
    if "industry" not in x.columns:
        x["industry"] = "Unknown"
    x["sector"] = x["sector"].replace("", np.nan).fillna("Unknown")
    x["industry"] = x["industry"].replace("", np.nan).fillna("Unknown")

    numeric_cols = [
        "market_cap", "price", "pe", "ps", "ev_ebit", "fcf_yield",
        "revenue_growth", "eps_growth", "fcf_growth",
        "gross_margin", "operating_margin", "net_margin", "roic", "roe",
        "debt_to_equity", "net_debt_ebitda", "current_ratio",
        "return_1m", "return_3m", "return_6m", "return_12m", "volatility"
    ]
    for col in numeric_cols:
        if col not in x.columns:
            x[col] = np.nan
        x[col] = pd.to_numeric(x[col], errors="coerce")
        x[col] = clip_series(x[col])

    for col in [
        "pe", "ps", "ev_ebit", "fcf_yield",
        "revenue_growth", "eps_growth", "fcf_growth",
        "gross_margin", "operating_margin", "net_margin", "roic", "roe",
        "debt_to_equity", "net_debt_ebitda", "current_ratio",
        "return_1m", "return_3m", "return_6m", "return_12m", "volatility"
    ]:
        x[col] = fill_missing_with_median_by_group(x, col, "sector")

    x["score_value"] = (
        rank_pct(x["pe"], ascending=True) * 0.25 +
        rank_pct(x["ps"], ascending=True) * 0.20 +
        rank_pct(x["ev_ebit"], ascending=True) * 0.25 +
        rank_pct(x["fcf_yield"], ascending=False) * 0.30
    )
    x["score_growth"] = (
        rank_pct(x["revenue_growth"], ascending=False) * 0.40 +
        rank_pct(x["eps_growth"], ascending=False) * 0.35 +
        rank_pct(x["fcf_growth"], ascending=False) * 0.25
    )
    x["score_quality"] = (
        rank_pct(x["gross_margin"], ascending=False) * 0.15 +
        rank_pct(x["operating_margin"], ascending=False) * 0.25 +
        rank_pct(x["net_margin"], ascending=False) * 0.15 +
        rank_pct(x["roic"], ascending=False) * 0.25 +
        rank_pct(x["roe"], ascending=False) * 0.20
    )
    x["score_balance_sheet"] = (
        rank_pct(x["debt_to_equity"], ascending=True) * 0.40 +
        rank_pct(x["net_debt_ebitda"], ascending=True) * 0.35 +
        rank_pct(x["current_ratio"], ascending=False) * 0.25
    )
    x["score_momentum_raw"] = (
        rank_pct(x["return_1m"], ascending=False) * 0.10 +
        rank_pct(x["return_3m"], ascending=False) * 0.25 +
        rank_pct(x["return_6m"], ascending=False) * 0.30 +
        rank_pct(x["return_12m"], ascending=False) * 0.35
    )
    x["score_low_vol"] = rank_pct(x["volatility"], ascending=True)
    x["score_momentum"] = x["score_momentum_raw"] * 0.85 + x["score_low_vol"] * 0.15

    overlay = build_macro_overlay(macro_regime)
    x["score_value_adj"] = x["score_value"] * overlay["value"]
    x["score_growth_adj"] = x["score_growth"] * overlay["growth"]
    x["score_quality_adj"] = x["score_quality"] * overlay["quality"]
    x["score_balance_sheet_adj"] = x["score_balance_sheet"] * overlay["balance_sheet"]
    x["score_momentum_adj"] = x["score_momentum"] * overlay["momentum"]

    x["tii_score"] = (
        x["score_quality_adj"] * 0.28 +
        x["score_growth_adj"] * 0.26 +
        x["score_momentum_adj"] * 0.22 +
        x["score_balance_sheet_adj"] * 0.14 +
        x["score_value_adj"] * 0.10
    )

    penalty = np.zeros(len(x), dtype=float)
    penalty += np.where(x["debt_to_equity"] > x["debt_to_equity"].median(), 1.5, 0)
    penalty += np.where(x["net_margin"] < x["net_margin"].median(), 1.0, 0)
    penalty += np.where(x["revenue_growth"] < x["revenue_growth"].median(), 1.0, 0)
    penalty += np.where(x["volatility"] > x["volatility"].quantile(0.90), 2.0, 0)

    x["tii_score"] = x["tii_score"] - penalty
    x["tii_score"] = pd.to_numeric(x["tii_score"], errors="coerce").round(2)

    completeness_cols = [
        "pe", "ps", "ev_ebit", "fcf_yield",
        "revenue_growth", "eps_growth", "fcf_growth",
        "gross_margin", "operating_margin", "net_margin", "roic", "roe",
        "debt_to_equity", "net_debt_ebitda", "current_ratio",
        "return_1m", "return_3m", "return_6m", "return_12m", "volatility"
    ]
    available = x[completeness_cols].notna().sum(axis=1)
    x["data_confidence"] = ((available / len(completeness_cols)) * 100).round(0)

    fallback_mask = x["data_confidence"] < 35
    fallback_score = (
        rank_pct(x["return_1m"], ascending=False) * 0.15 +
        rank_pct(x["return_3m"], ascending=False) * 0.30 +
        rank_pct(x["return_6m"], ascending=False) * 0.30 +
        rank_pct(x["return_12m"], ascending=False) * 0.25
    )
    fallback_score = fallback_score * 0.85 + rank_pct(x["volatility"], ascending=True) * 0.15

    x.loc[fallback_mask, "tii_score"] = fallback_score.loc[fallback_mask]
    x["score_source"] = np.where(fallback_mask, "Price fallback", "Full factor model")

    ranked = x["tii_score"].rank(method="min", ascending=False)
    x["tii_rank"] = ranked.astype("Int64")

    return x.sort_values("tii_score", ascending=False).reset_index(drop=True)


def split_top100_and_full_universe(scored_df):
    full_universe = scored_df.copy()
    if "market_cap" in full_universe.columns:
        eligible = full_universe.sort_values(["tii_score", "market_cap"], ascending=[False, False])
    else:
        eligible = full_universe.sort_values("tii_score", ascending=False)
    top_100 = eligible.head(100).copy().reset_index(drop=True)
    top_100["display_rank"] = np.arange(1, len(top_100) + 1)
    return top_100, full_universe


def classify_conviction(row):
    score = row.get("tii_score", np.nan)
    confidence = row.get("data_confidence", 100)
    if pd.isna(score):
        return "Unrated"
    if score >= 85:
        label = "High Conviction"
    elif score >= 75:
        label = "Conviction"
    elif score >= 65:
        label = "Watchlist+"
    elif score >= 55:
        label = "Watchlist"
    else:
        label = "Avoid"
    try:
        if float(confidence) < 35:
            label += " (Momentum)"
    except (TypeError, ValueError):
        pass
    return label


def recommended_position_size(row):
    conviction = classify_conviction(row)
    if conviction == "High Conviction":
        return 0.050
    if conviction == "Conviction":
        return 0.035
    if conviction == "Watchlist+":
        return 0.020
    if conviction == "Watchlist":
        return 0.010
    return 0.000


def build_portfolio_model_table(top_df):
    x = top_df.copy()
    x["conviction"] = x.apply(classify_conviction, axis=1)
    x["model_weight"] = x.apply(recommended_position_size, axis=1)
    x["model_weight_pct"] = (x["model_weight"] * 100).round(1)
    display_cols = [c for c in [
        "display_rank", "ticker", "company", "sector",
        "tii_score", "tii_rank", "conviction", "model_weight_pct",
        "score_growth", "score_quality", "score_momentum",
        "score_balance_sheet", "score_value", "data_confidence",
        "market_cap", "price"
    ] if c in x.columns]
    return x[display_cols].copy()


def build_ai_recommendation(row):
    growth = row.get("score_growth", np.nan)
    quality = row.get("score_quality", np.nan)
    momentum = row.get("score_momentum", np.nan)
    balance = row.get("score_balance_sheet", np.nan)
    value = row.get("score_value", np.nan)
    tii = row.get("tii_score", np.nan)

    strengths, risks = [], []
    if pd.notna(growth) and growth >= 75: strengths.append("strong growth")
    if pd.notna(quality) and quality >= 75: strengths.append("high quality")
    if pd.notna(momentum) and momentum >= 75: strengths.append("positive momentum")
    if pd.notna(balance) and balance >= 75: strengths.append("strong balance sheet")
    if pd.notna(value) and value >= 75: strengths.append("reasonable valuation")
    if pd.notna(growth) and growth < 40: risks.append("weak growth")
    if pd.notna(quality) and quality < 40: risks.append("low quality")
    if pd.notna(momentum) and momentum < 40: risks.append("poor momentum")
    if pd.notna(balance) and balance < 40: risks.append("balance-sheet risk")
    if pd.notna(value) and value < 40: risks.append("expensive valuation")

    if pd.isna(tii): stance = "Insufficient data"
    elif tii >= 85: stance = "High-conviction candidate"
    elif tii >= 75: stance = "Accumulation candidate"
    elif tii >= 65: stance = "Promising but selective"
    elif tii >= 55: stance = "Watchlist only"
    else: stance = "Avoid for now"

    strength_text = ", ".join(strengths) if strengths else "mixed factor profile"
    risk_text = ", ".join(risks) if risks else "no major factor warning"
    return f"{stance}. Strengths: {strength_text}. Risks: {risk_text}."


def add_ai_recommendations(df):
    x = df.copy()
    x["ai_recommendation"] = x.apply(build_ai_recommendation, axis=1)
    return x


def add_conviction_to_top100(top_100_df):
    x = top_100_df.copy()
    if "conviction" not in x.columns:
        x["conviction"] = x.apply(classify_conviction, axis=1)
    if "model_weight_pct" not in x.columns:
        x["model_weight_pct"] = x.apply(recommended_position_size, axis=1) * 100
        x["model_weight_pct"] = x["model_weight_pct"].round(1)
    return x


def run_enhanced_pipeline(universe_df, macro_regime="Neutral"):
    scored = compute_tii_scores(universe_df, macro_regime=macro_regime)
    top_100, full_universe = split_top100_and_full_universe(scored)
    top_100 = add_ai_recommendations(top_100)
    portfolio_model = build_portfolio_model_table(top_100)
    return full_universe, top_100, portfolio_model


# =========================
# CRASH / REGIME OVERLAY
# =========================
def normalise_percent_value(v):
    if pd.isna(v):
        return np.nan
    v = float(v)
    if abs(v) <= 1:
        return v * 100
    return v


def build_crash_signal_table(hy_oas=None, ig_oas=None, vix=None, breadth=None, nfci=None):
    hy_oas = normalise_percent_value(hy_oas)
    ig_oas = normalise_percent_value(ig_oas)
    breadth = normalise_percent_value(breadth)
    rows = [
        {"indicator": "HY OAS",  "value": hy_oas,  "threshold": "> 5.0", "breach": bool(pd.notna(hy_oas) and hy_oas > 5.0)},
        {"indicator": "IG OAS",  "value": ig_oas,  "threshold": "> 1.5", "breach": bool(pd.notna(ig_oas) and ig_oas > 1.5)},
        {"indicator": "VIX",     "value": vix,      "threshold": "> 30",  "breach": bool(pd.notna(vix) and float(vix) > 30)},
        {"indicator": "Breadth", "value": breadth,  "threshold": "< 50",  "breach": bool(pd.notna(breadth) and breadth < 50)},
        {"indicator": "NFCI",    "value": nfci,     "threshold": "> 0",   "breach": bool(pd.notna(nfci) and float(nfci) > 0)},
    ]
    out = pd.DataFrame(rows)
    out["status"] = np.where(out["breach"], "Breached", "OK")
    return out


def classify_crash_risk(signal_df):
    breaches = int(signal_df["breach"].sum())
    if breaches >= 4: return "High"
    if breaches >= 2: return "Elevated"
    if breaches == 1: return "Guarded"
    return "Low"


def crash_risk_summary_text(signal_df):
    risk = classify_crash_risk(signal_df)
    breached = signal_df.loc[signal_df["breach"], "indicator"].tolist()
    if risk == "High":
        return f"Crash/correction risk is High. Key breaches: {', '.join(breached)}." if breached else "Crash/correction risk is High."
    if risk == "Elevated":
        return f"Crash/correction risk is Elevated. Pressures showing in: {', '.join(breached)}." if breached else "Crash/correction risk is Elevated."
    if risk == "Guarded":
        return f"Crash/correction risk is Guarded. Watch: {', '.join(breached)}." if breached else "Crash/correction risk is Guarded."
    return "Crash/correction risk is Low. No major threshold breaches detected."


# =========================
# PORTFOLIO ACTION ENGINE
# =========================
UNCHANGED_AT_ANY_RISK = {"MA", "TJX", "ROBO"}
RISK_SENSITIVE_NAMES = {"NVDA", "ANET", "CRWD", "TSLA"}


def portfolio_action_for_row(row, crash_risk="Low"):
    ticker = str(row.get("ticker", "")).upper().strip()
    tii = row.get("tii_score", np.nan)
    momentum = row.get("score_momentum", np.nan)
    quality = row.get("score_quality", np.nan)
    balance = row.get("score_balance_sheet", np.nan)

    if ticker in UNCHANGED_AT_ANY_RISK:
        return "No Change"

    if pd.isna(tii):
        action = "Review"
    elif tii >= 85 and pd.notna(momentum) and momentum >= 65 and pd.notna(quality) and quality >= 70:
        action = "Add"
    elif tii >= 75 and pd.notna(quality) and quality >= 65:
        action = "Accumulate"
    elif tii >= 65:
        action = "Hold"
    elif tii >= 55:
        action = "Watch"
    else:
        action = "Avoid"

    if crash_risk == "Guarded":
        if ticker in RISK_SENSITIVE_NAMES and action in {"Add", "Accumulate"}:
            action = "Hold / Partial Add"
    elif crash_risk == "Elevated":
        if ticker in RISK_SENSITIVE_NAMES:
            if action == "Add": action = "Trim / Hold"
            elif action == "Accumulate": action = "Hold"
        elif action == "Add":
            action = "Accumulate"
    elif crash_risk == "High":
        if ticker in RISK_SENSITIVE_NAMES:
            if action in {"Add", "Accumulate"}: action = "Trim"
            elif action == "Hold": action = "Reduce"
        else:
            if action == "Add": action = "Hold"
            elif action == "Accumulate": action = "Hold / Selective"
            elif action == "Hold" and pd.notna(balance) and balance < 50: action = "Reduce"

    return action


def portfolio_action_pure_tii(row, crash_risk="Low"):
    """Action engine for Pure TII mode — based on scan_tii and signal type."""
    tii = row.get("scan_tii", np.nan)
    qualifies = row.get("scan_qualifies", False)
    sig_type = str(row.get("signal_type", "")).strip()

    if pd.isna(tii) or not qualifies:
        return "No Signal"

    tii = int(tii)

    if tii >= 8:
        action = "Strong Buy Signal"
    elif tii >= 6:
        action = "Buy Signal"
    else:
        action = "Borderline — Monitor"

    # Risk overlay
    if crash_risk in ("Elevated", "High") and sig_type == "Trend Reversal":
        action += " (Reversal — Caution)"
    elif crash_risk == "High" and action.startswith("Strong Buy"):
        action = "Buy Signal (Risk Elevated)"

    return action


def build_portfolio_actions_table(top_100_df, crash_risk="Low", dashboard_mode=MODE_ENHANCED):
    x = top_100_df.copy()

    if dashboard_mode == MODE_PURE_TII:
        x["portfolio_action"] = x.apply(
            lambda row: portfolio_action_pure_tii(row, crash_risk=crash_risk), axis=1
        )
    else:
        if "conviction" not in x.columns:
            x["conviction"] = x.apply(classify_conviction, axis=1)
        x["portfolio_action"] = x.apply(
            lambda row: portfolio_action_for_row(row, crash_risk=crash_risk), axis=1
        )

    action_order = {
        "Strong Buy Signal": 1, "Buy Signal": 2, "Borderline — Monitor": 3,
        "Buy Signal (Risk Elevated)": 4,
        "Add": 1, "Accumulate": 2, "Hold / Partial Add": 3, "Hold": 4,
        "Hold / Selective": 5, "Trim / Hold": 6, "Trim": 7, "Reduce": 8,
        "Watch": 9, "Review": 10, "Avoid": 11, "No Change": 12, "No Signal": 13,
    }
    x["action_priority"] = x["portfolio_action"].map(action_order).fillna(99)

    if dashboard_mode == MODE_PURE_TII:
        cols = [c for c in [
            "display_rank", "ticker", "company", "sector",
            "scan_tii", "signal_type", "conviction", "portfolio_action",
            "suggested_entry_ref", "suggested_initial_stop", "signal_summary"
        ] if c in x.columns]
    else:
        cols = [c for c in [
            "display_rank", "ticker", "company", "sector",
            "tii_score", "conviction", "model_weight_pct",
            "portfolio_action", "score_growth", "score_quality",
            "score_momentum", "score_balance_sheet", "score_value",
            "ai_recommendation"
        ] if c in x.columns]

    sort_col = "scan_tii" if dashboard_mode == MODE_PURE_TII else "tii_score"
    return x.sort_values(
        ["action_priority", sort_col], ascending=[True, False]
    )[cols].reset_index(drop=True)


# =========================
# AI NARRATIVE (Enhanced mode only)
# =========================
def build_market_regime_narrative(macro_regime, crash_signal_df):
    risk = classify_crash_risk(crash_signal_df)
    breaches = crash_signal_df.loc[crash_signal_df["breach"], "indicator"].tolist()
    base = f"Macro regime is {macro_regime}."
    if risk == "Low":
        return f"{base} Market stress overlay remains benign. Broad participation is acceptable, but quality should still anchor the book."
    if risk == "Guarded":
        return f"{base} Early stress signals are appearing. Keep new adds selective and favour quality, balance-sheet strength, and resilient momentum."
    if risk == "Elevated":
        breach_text = f" Pressure points: {', '.join(breaches)}." if breaches else ""
        return f"{base} Risk conditions are elevated.{breach_text} Tighten standards, reduce speculative exposure, and prefer proven compounders."
    breach_text = f" Stress is visible in {', '.join(breaches)}." if breaches else ""
    return f"{base} Defensive posture is warranted.{breach_text} Preserve capital, prioritise liquidity, and treat new risk as exceptional rather than routine."


def build_top100_summary(top_100_df, dashboard_mode=MODE_ENHANCED):
    x = top_100_df.copy()
    if dashboard_mode == MODE_PURE_TII:
        avg_tii = pd.to_numeric(x.get("scan_tii"), errors="coerce").mean()
        n_signals = int(x.get("scan_qualifies", pd.Series(False)).sum()) if "scan_qualifies" in x.columns else len(x)
        if pd.isna(avg_tii):
            return "Scan summary unavailable."
        return f"Qualified signals: {n_signals}. Average signal TII: {avg_tii:.1f}."
    else:
        avg_tii = pd.to_numeric(x.get("tii_score"), errors="coerce").mean()
        avg_growth = pd.to_numeric(x.get("score_growth"), errors="coerce").mean()
        avg_quality = pd.to_numeric(x.get("score_quality"), errors="coerce").mean()
        avg_momentum = pd.to_numeric(x.get("score_momentum"), errors="coerce").mean()
        leader_parts = []
        if pd.notna(avg_growth): leader_parts.append(f"growth {avg_growth:.1f}")
        if pd.notna(avg_quality): leader_parts.append(f"quality {avg_quality:.1f}")
        if pd.notna(avg_momentum): leader_parts.append(f"momentum {avg_momentum:.1f}")
        if pd.isna(avg_tii): return "Top 100 summary unavailable."
        return f"Top 100 average TII is {avg_tii:.1f}. Aggregate factor tone: " + ", ".join(leader_parts) + "."


def build_single_stock_ai_note(row, crash_risk="Low"):
    ticker = row.get("ticker", "")
    company = row.get("company", "")
    base = build_ai_recommendation(row)
    action = portfolio_action_for_row(row, crash_risk=crash_risk)
    return f"{ticker} ({company}): {base} Portfolio stance: {action} under current risk setting ({crash_risk})."


# =========================
# MACRO INPUT HELPERS
# =========================
def default_macro_inputs():
    return {"hy_oas": np.nan, "ig_oas": np.nan, "vix": np.nan, "breadth": np.nan, "nfci": np.nan}


def get_macro_inputs():
    if "macro_inputs" not in st.session_state:
        st.session_state["macro_inputs"] = default_macro_inputs().copy()
    return st.session_state["macro_inputs"]


# =========================
# STREAMLIT UI
# =========================
st.set_page_config(
    page_title="Steve Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)


def to_excel_bytes(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            safe_sheet = str(sheet_name)[:31]
            df.to_excel(writer, index=False, sheet_name=safe_sheet)
    output.seek(0)
    return output.getvalue()


def format_display_table(df):
    x = df.copy()
    pct_cols = [
        "model_weight_pct", "data_confidence",
        "revenue_growth", "eps_growth", "fcf_growth",
        "gross_margin", "operating_margin", "net_margin",
        "roic", "roe", "fcf_yield",
        "return_1m", "return_3m", "return_6m", "return_12m"
    ]
    money_cols = ["market_cap", "price", "suggested_entry_ref", "suggested_initial_stop"]
    score_cols = ["tii_score", "scan_tii", "score_growth", "score_quality", "score_momentum",
                  "score_balance_sheet", "score_value"]
    for col in pct_cols:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").round(1)
    for col in money_cols:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").round(2)
    for col in score_cols:
        if col in x.columns:
            x[col] = pd.to_numeric(x[col], errors="coerce").round(1)
    return x


def simple_mobile_table(df, columns):
    cols = [c for c in columns if c in df.columns]
    st.dataframe(
        format_display_table(df[cols] if cols else df),
        use_container_width=True,
        height=520,
        hide_index=True,
    )


# ── Cached data loaders ────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False, ttl=3600)
def get_universe_data():
    return load_master_universe()


@st.cache_data(show_spinner=False, ttl=3600)
def build_dashboard_data(macro_regime, dashboard_mode):
    """
    Cache key includes dashboard_mode so each mode gets its own cache entry.
    """
    universe_df = get_universe_data()

    if dashboard_mode == MODE_PURE_TII:
        full_universe_df, top_signals_df, portfolio_df = build_pure_tii_view(universe_df)
    else:
        full_universe_df, top_signals_df, portfolio_df = run_enhanced_pipeline(universe_df, macro_regime=macro_regime)
        top_signals_df = add_conviction_to_top100(top_signals_df)

    current_positions_df, sheet_error = load_current_positions_data(
        SHEET_ID, WORKSHEET_NAME, HEADER_ROW_INDEX
    )
    current_positions_df = enrich_positions_with_dashboard_data(
        current_positions_df, full_universe_df, top_signals_df, dashboard_mode
    )

    return universe_df, full_universe_df, top_signals_df, portfolio_df, current_positions_df, sheet_error


# ── Sidebar ─────────────────────────────────────────────────────────────────────

st.sidebar.title("Dashboard Controls")

dashboard_mode = st.sidebar.radio(
    "Dashboard Mode",
    options=[MODE_PURE_TII, MODE_ENHANCED],
    index=0,
    help=(
        f"**{MODE_PURE_TII}**: ranks by the TII score (-10 to +10) computed by the weekly scan. "
        f"Only qualified signals (TII ≥ 4 + entry trigger) appear in the signals list. "
        f"No fundamental overlay.\n\n"
        f"**{MODE_ENHANCED}**: adds a five-factor fundamental model (quality, growth, momentum, "
        f"balance sheet, value) on top of the scan universe."
    ),
)

macro_regime = st.sidebar.selectbox(
    "Macro Regime",
    options=["Neutral", "Risk On", "Risk Off", "Inflationary", "Disinflation"],
    index=0,
    disabled=(dashboard_mode == MODE_PURE_TII),
    help="Macro regime tilts factor weights. Disabled in Pure TII mode.",
)

mobile_mode = st.sidebar.toggle("Mobile-friendly mode", value=False)
show_full_universe = st.sidebar.toggle("Show full universe table", value=False)

st.sidebar.markdown("---")
if dashboard_mode == MODE_PURE_TII:
    st.sidebar.caption("Pure TII mode: signals are stocks qualifying from the weekly scan (TII ≥ 4 with entry signal).")
else:
    st.sidebar.caption("Enhanced mode: Top 100 drives the main dashboard. Full universe available for download and deep review.")

# ── Crash Monitor sidebar ──────────────────────────────────────────────────────

st.sidebar.markdown("---")
st.sidebar.subheader("Crash Monitor")

macro_inputs = get_macro_inputs()

with st.sidebar.expander("Manual macro inputs", expanded=False):
    hy_val     = st.number_input("HY OAS (%)", value=float(macro_inputs.get("hy_oas")) if pd.notna(macro_inputs.get("hy_oas")) else 0.0, step=0.1)
    ig_val     = st.number_input("IG OAS (%)", value=float(macro_inputs.get("ig_oas")) if pd.notna(macro_inputs.get("ig_oas")) else 0.0, step=0.1)
    vix_val    = st.number_input("VIX", value=float(macro_inputs.get("vix")) if pd.notna(macro_inputs.get("vix")) else 0.0, step=0.5)
    breadth_val= st.number_input("Breadth (%)", value=float(macro_inputs.get("breadth")) if pd.notna(macro_inputs.get("breadth")) else 0.0, step=1.0)
    nfci_val   = st.number_input("NFCI", value=float(macro_inputs.get("nfci")) if pd.notna(macro_inputs.get("nfci")) else 0.0, step=0.05)

    if st.button("Apply macro inputs", use_container_width=True):
        st.session_state["macro_inputs"] = {
            "hy_oas": hy_val, "ig_oas": ig_val, "vix": vix_val,
            "breadth": breadth_val, "nfci": nfci_val,
        }
        st.rerun()

macro_inputs = get_macro_inputs()
crash_signal_df = build_crash_signal_table(
    hy_oas=macro_inputs.get("hy_oas"),
    ig_oas=macro_inputs.get("ig_oas"),
    vix=macro_inputs.get("vix"),
    breadth=macro_inputs.get("breadth"),
    nfci=macro_inputs.get("nfci"),
)
crash_risk = classify_crash_risk(crash_signal_df)
st.sidebar.caption(crash_risk_summary_text(crash_signal_df))

# ── Build data ─────────────────────────────────────────────────────────────────

try:
    raw_universe_df, full_universe_df, top_signals_df, portfolio_df, current_positions_df, sheet_error = build_dashboard_data(
        macro_regime, dashboard_mode
    )
except Exception as e:
    st.error("Dashboard data could not be built.")
    st.exception(e)
    st.stop()

if raw_universe_df.empty or full_universe_df.empty:
    st.warning("No universe data. Check weekly_scan_output.csv.")
    st.stop()

# ── Mode banners ───────────────────────────────────────────────────────────────

if dashboard_mode == MODE_PURE_TII:
    n_signals = len(top_signals_df)
    if n_signals == 0:
        st.warning("⚠️ No qualifying signals this week (no stocks with TII ≥ 4 and an active entry trigger).")
    else:
        st.info(f"📡 **Pure TII mode** — {n_signals} qualifying signal(s) this week. Ranked by scan TII, no fundamental overlay.")
else:
    _avg_confidence = pd.to_numeric(top_signals_df.get("data_confidence"), errors="coerce").mean()
    _fallback_pct = (
        (top_signals_df.get("score_source", pd.Series()) == "Price fallback").mean() * 100
        if "score_source" in top_signals_df.columns else 100.0
    )
    if pd.isna(_avg_confidence) or _avg_confidence < 35 or _fallback_pct > 80:
        st.warning(
            "⚠️ **Momentum-only mode** — fundamental columns are missing or empty. "
            "Run scan_weekly_v2_enriched.py to populate them. See System Notes for expected schema."
        )
    elif _avg_confidence < 70:
        st.info(f"ℹ️ Average data completeness is {_avg_confidence:.0f}%. Some factor scores are estimated from sector medians.")

# ── Sector / search filters ───────────────────────────────────────────────────

selected_sector = "All"
if "sector" in top_signals_df.columns:
    sector_options = ["All"] + sorted([s for s in top_signals_df["sector"].dropna().astype(str).unique().tolist() if s and s != "Unknown"])
    selected_sector = st.sidebar.selectbox("Sector Filter", sector_options, index=0)

search_text = st.sidebar.text_input("Ticker / company search", "")

visible_df = top_signals_df.copy()
if selected_sector != "All" and "sector" in visible_df.columns:
    visible_df = visible_df[visible_df["sector"] == selected_sector].copy()
if search_text:
    q = search_text.strip().lower()
    ticker_match = visible_df["ticker"].astype(str).str.lower().str.contains(q, na=False) if "ticker" in visible_df.columns else False
    company_match = visible_df["company"].astype(str).str.lower().str.contains(q, na=False) if "company" in visible_df.columns else False
    visible_df = visible_df[ticker_match | company_match].copy()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("Steve Dashboard")
st.caption(
    f"Mode: **{dashboard_mode}** • Macro regime: {macro_regime} • "
    f"Crash risk: {crash_risk} • Updated: {datetime.now().strftime('%d %b %Y %H:%M')}"
)

# Metric cards — mode-aware
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Universe Size", f"{len(full_universe_df):,}")
with c2:
    if dashboard_mode == MODE_PURE_TII:
        st.metric("Qualifying Signals", f"{len(top_signals_df):,}")
    else:
        st.metric("Top List Size", f"{len(top_signals_df):,}")
with c3:
    if dashboard_mode == MODE_PURE_TII:
        avg_tii = pd.to_numeric(top_signals_df.get("scan_tii"), errors="coerce").mean()
        st.metric("Avg Signal TII", f"{avg_tii:.1f}" if pd.notna(avg_tii) else "N/A")
    else:
        avg_tii = pd.to_numeric(top_signals_df.get("tii_score"), errors="coerce").mean()
        st.metric("Avg Top-100 TII", f"{avg_tii:.1f}" if pd.notna(avg_tii) else "N/A")
with c4:
    if dashboard_mode == MODE_PURE_TII:
        nh_count = int((top_signals_df.get("signal_type", pd.Series()) == "New High").sum()) if "signal_type" in top_signals_df.columns else 0
        st.metric("New Highs", f"{nh_count:,}")
    else:
        hc_count = int((top_signals_df.get("conviction", pd.Series()) == "High Conviction").sum()) if "conviction" in top_signals_df.columns else 0
        st.metric("High Conviction", f"{hc_count:,}")

# ── Actions table ──────────────────────────────────────────────────────────────

top_actions_df = build_portfolio_actions_table(
    top_signals_df, crash_risk=crash_risk, dashboard_mode=dashboard_mode
)
if "portfolio_action" not in visible_df.columns and "ticker" in visible_df.columns:
    visible_df = visible_df.merge(
        top_actions_df[["ticker", "portfolio_action"]].drop_duplicates(subset=["ticker"]),
        on="ticker", how="left",
    )

# ── Downloads ──────────────────────────────────────────────────────────────────

c_dl1, c_dl2 = st.columns([1, 3])
with c_dl1:
    st.caption("Download available below after all tabs load.")
with c_dl2:
    st.caption("Workbook includes signals/top-100, portfolio model, full universe, and actions sheets.")

# ── Main tabs ──────────────────────────────────────────────────────────────────

tab_label_1 = "Signals" if dashboard_mode == MODE_PURE_TII else "Top 100"
tab_label_3 = "TII Breakdown" if dashboard_mode == MODE_PURE_TII else "Factor View"

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    tab_label_1, "Portfolio Model", tab_label_3, "Full Universe", "Actions", "Macro Monitor"
])

# ── TAB 1 ──────────────────────────────────────────────────────────────────────

with tab1:
    if dashboard_mode == MODE_PURE_TII:
        st.subheader("Qualifying Signals")
        st.caption(
            "Stocks with TII ≥ 4 and an active entry trigger (New High or Trend Reversal) from the weekly scan. "
            "Sorted by TII descending, New Highs before Trend Reversals at equal TII."
        )

        pure_cols_desktop = [
            "display_rank", "ticker", "company", "sector",
            "scan_tii", "signal_type", "conviction",
            "suggested_entry_ref", "suggested_initial_stop", "signal_summary"
        ]
        pure_cols_mobile = [
            "display_rank", "ticker", "scan_tii", "signal_type", "conviction"
        ]

        available_sort_pure = [c for c in ["scan_tii", "display_rank"] if c in visible_df.columns]
        sort_col = st.selectbox("Sort by", available_sort_pure, index=0)
        ascending = st.toggle("Ascending", value=False)
        visible_sorted = visible_df.sort_values(sort_col, ascending=ascending).copy() if sort_col else visible_df.copy()

        if mobile_mode:
            simple_mobile_table(visible_sorted, pure_cols_mobile)
        else:
            st.dataframe(
                format_display_table(visible_sorted[[c for c in pure_cols_desktop if c in visible_sorted.columns]]),
                use_container_width=True, height=620, hide_index=True,
            )

        # Pure TII cards
        st.subheader("Top 10 Signals")
        for _, row in visible_sorted.head(10).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([2.4, 1, 1])
                with c1:
                    st.markdown(f"**{row.get('ticker', '')} — {row.get('company', '')}**")
                with c2:
                    st.markdown(f"**TII:** {row.get('scan_tii', '—')}")
                with c3:
                    st.markdown(f"**{row.get('conviction', '')}**")
                summary = row.get("signal_summary", "")
                if summary:
                    st.caption(summary)

    else:
        st.subheader("Top 100 Ranked List")

        available_sort_cols = [c for c in [
            "display_rank", "tii_score", "score_growth", "score_quality",
            "score_momentum", "score_balance_sheet", "score_value",
            "market_cap", "model_weight_pct"
        ] if c in visible_df.columns]

        c1, c2 = st.columns([1, 1])
        with c1:
            sort_col = st.selectbox("Sort by", available_sort_cols, index=0 if available_sort_cols else None)
        with c2:
            ascending = st.toggle("Ascending", value=(sort_col == "display_rank"))

        visible_sorted = visible_df.sort_values(sort_col, ascending=ascending).copy() if sort_col else visible_df.copy()

        top_cols_desktop = [
            "display_rank", "ticker", "company", "sector", "tii_score",
            "conviction", "model_weight_pct", "score_growth", "score_quality",
            "score_momentum", "score_balance_sheet", "score_value",
            "data_confidence", "ai_recommendation"
        ]
        top_cols_mobile = [
            "display_rank", "ticker", "tii_score", "conviction", "model_weight_pct", "ai_recommendation"
        ]

        if mobile_mode:
            simple_mobile_table(visible_sorted, top_cols_mobile)
        else:
            st.dataframe(
                format_display_table(visible_sorted[[c for c in top_cols_desktop if c in visible_sorted.columns]]),
                use_container_width=True, height=620, hide_index=True,
            )

        # Enhanced mode cards
        st.subheader("Top 10 Snapshot")
        for _, row in visible_sorted.head(10).iterrows():
            with st.container(border=True):
                c1, c2, c3 = st.columns([2.4, 1, 1])
                with c1:
                    st.markdown(f"**{row.get('ticker', '')} — {row.get('company', '')}**")
                with c2:
                    st.markdown(f"**TII:** {row.get('tii_score', '—')}")
                with c3:
                    st.markdown(f"**{row.get('conviction', '')}**")
                ai_text = row.get("ai_recommendation", "")
                if ai_text:
                    st.caption(ai_text)

# ── TAB 2 — PORTFOLIO MODEL ────────────────────────────────────────────────────

with tab2:
    st.subheader("Portfolio Model")

    st.markdown("**Current Portfolio (live Google Sheet)**")
    if sheet_error:
        st.info(sheet_error)
    elif current_positions_df.empty:
        st.info("No open positions were returned from the Google Sheet.")
    else:
        positions_df = current_positions_df.copy()
        if selected_sector != "All" and "sector" in positions_df.columns:
            positions_df = positions_df[positions_df["sector"] == selected_sector].copy()

        h1, h2, h3, h4 = st.columns(4)
        with h1:
            st.metric("Open positions", f"{len(positions_df):,}")
        with h2:
            mv = pd.to_numeric(positions_df.get("Market Value"), errors="coerce").sum()
            st.metric("Market value", f"${mv:,.0f}" if pd.notna(mv) else "N/A")
        with h3:
            pnl = pd.to_numeric(positions_df.get("Net Profit/Loss"), errors="coerce").sum()
            st.metric("Net P/L", f"${pnl:,.0f}" if pd.notna(pnl) else "N/A")
        with h4:
            if dashboard_mode == MODE_PURE_TII:
                avg_tii = pd.to_numeric(positions_df.get("scan_tii"), errors="coerce").mean()
            else:
                avg_tii = pd.to_numeric(positions_df.get("tii_score"), errors="coerce").mean()
            st.metric("Avg holding TII", f"{avg_tii:.1f}" if pd.notna(avg_tii) else "N/A")

        if dashboard_mode == MODE_PURE_TII:
            positions_cols_desktop = [
                "Trade Date", "ticker", "company", "Position Status", "Quantity",
                "Purchase Price", "Market Price", "Market Value", "Net Profit/Loss",
                "% Gain/Loss", "scan_tii", "signal_type", "conviction",
                "suggested_entry_ref", "suggested_initial_stop"
            ]
        else:
            positions_cols_desktop = [
                "Trade Date", "ticker", "company", "Position Status", "Quantity",
                "Purchase Price", "Market Price", "Market Value", "Net Profit/Loss",
                "% Gain/Loss", "tii_rank", "tii_score", "conviction", "model_weight_pct",
                "score_growth", "score_quality", "score_momentum", "score_balance_sheet",
                "score_value", "ai_recommendation"
            ]
        positions_cols_mobile = ["ticker", "Market Value", "Net Profit/Loss", "conviction"]

        if mobile_mode:
            simple_mobile_table(positions_df, positions_cols_mobile)
        else:
            st.dataframe(
                format_display_table(positions_df[[c for c in positions_cols_desktop if c in positions_df.columns]]),
                use_container_width=True, height=320, hide_index=True,
            )

    st.markdown("---")
    st.markdown("**Model Portfolio**")

    pm_df = portfolio_df.copy()
    if selected_sector != "All" and "sector" in pm_df.columns:
        pm_df = pm_df[pm_df["sector"] == selected_sector].copy()

    if dashboard_mode == MODE_PURE_TII:
        pm_cols_desktop = [
            "display_rank", "ticker", "company", "sector",
            "scan_tii", "signal_type", "conviction", "model_weight_pct",
            "suggested_entry_ref", "suggested_initial_stop", "signal_summary"
        ]
        pm_cols_mobile = ["display_rank", "ticker", "scan_tii", "signal_type", "model_weight_pct"]
        total_w = pd.to_numeric(pm_df.get("model_weight_pct"), errors="coerce").sum()
        st.caption(f"Equal-weight model: {total_w:.0f}% across {len(pm_df)} signal(s). Max 10 positions.")
    else:
        pm_cols_desktop = [
            "display_rank", "ticker", "company", "sector",
            "tii_score", "conviction", "model_weight_pct",
            "score_growth", "score_quality", "score_momentum",
            "score_balance_sheet", "score_value", "data_confidence"
        ]
        pm_cols_mobile = ["display_rank", "ticker", "tii_score", "conviction", "model_weight_pct"]
        total_w = pd.to_numeric(pm_df.get("model_weight_pct"), errors="coerce").sum()
        st.caption(f"Model total weight shown: {total_w:.1f}%")

    if mobile_mode:
        simple_mobile_table(pm_df, pm_cols_mobile)
    else:
        st.dataframe(
            format_display_table(pm_df[[c for c in pm_cols_desktop if c in pm_df.columns]]),
            use_container_width=True, height=620, hide_index=True,
        )

# ── TAB 3 — TII BREAKDOWN / FACTOR VIEW ────────────────────────────────────────

with tab3:
    if dashboard_mode == MODE_PURE_TII:
        st.subheader("TII Component Breakdown")
        st.caption(
            "Each stock's TII (-10 to +10) is the sum of four components: "
            "Price trend (±3), Volume (±2), Moving Average (±2), MACD (±3). "
            "Qualify threshold: TII ≥ 4 with an entry trigger."
        )

        breakdown_cols = [
            "ticker", "company", "scan_tii", "signal_type",
            "price_score", "volume_score", "ma_score", "macd_score",
            "scan_qualifies"
        ]
        breakdown_cols_avail = [c for c in breakdown_cols if c in full_universe_df.columns]

        breakdown_df = full_universe_df[breakdown_cols_avail].copy()
        breakdown_df["scan_tii"] = pd.to_numeric(breakdown_df.get("scan_tii"), errors="coerce")
        breakdown_df = breakdown_df.sort_values("scan_tii", ascending=False).reset_index(drop=True)

        if not breakdown_df.empty:
            st.bar_chart(
                breakdown_df.head(30).set_index("ticker")[["scan_tii"]]
                if "scan_tii" in breakdown_df.columns else pd.DataFrame()
            )
            st.dataframe(
                format_display_table(breakdown_df.head(50)),
                use_container_width=True, height=500, hide_index=True,
            )

    else:
        st.subheader("Factor Leaders")
        c1, c2, c3 = st.columns(3)
        with c1:
            if "score_growth" in top_signals_df.columns:
                leaders = top_signals_df.nlargest(5, "score_growth")[["ticker", "company", "score_growth"]]
                st.markdown("**Growth**")
                st.dataframe(format_display_table(leaders), use_container_width=True, hide_index=True)
        with c2:
            if "score_quality" in top_signals_df.columns:
                leaders = top_signals_df.nlargest(5, "score_quality")[["ticker", "company", "score_quality"]]
                st.markdown("**Quality**")
                st.dataframe(format_display_table(leaders), use_container_width=True, hide_index=True)
        with c3:
            if "score_momentum" in top_signals_df.columns:
                leaders = top_signals_df.nlargest(5, "score_momentum")[["ticker", "company", "score_momentum"]]
                st.markdown("**Momentum**")
                st.dataframe(format_display_table(leaders), use_container_width=True, hide_index=True)

        st.subheader("Factor Distribution")
        factor_options = [c for c in [
            "tii_score", "score_growth", "score_quality",
            "score_momentum", "score_balance_sheet", "score_value"
        ] if c in top_signals_df.columns]
        selected_factor = st.selectbox("Choose factor", factor_options, index=0)
        chart_df = top_signals_df[[c for c in ["ticker", selected_factor] if c in top_signals_df.columns]].copy()
        chart_df = chart_df.dropna().sort_values(selected_factor, ascending=False).head(20)
        if not chart_df.empty:
            st.bar_chart(chart_df.set_index("ticker")[[selected_factor]])
        else:
            st.info("No factor data available for charting.")

# ── TAB 4 — FULL UNIVERSE ──────────────────────────────────────────────────────

with tab4:
    st.subheader("Full Universe")

    if not show_full_universe:
        st.info("Full universe table is hidden for performance. Enable it in the sidebar when needed.")
    else:
        fu_df = full_universe_df.copy()

        if dashboard_mode == MODE_PURE_TII:
            fu_cols_desktop = [
                "tii_rank", "ticker", "company", "sector", "scan_tii",
                "scan_qualifies", "signal_type",
                "price_score", "volume_score", "ma_score", "macd_score"
            ]
            fu_cols_mobile = ["tii_rank", "ticker", "scan_tii", "signal_type"]
        else:
            fu_cols_desktop = [
                "tii_rank", "ticker", "company", "sector", "tii_score",
                "score_growth", "score_quality", "score_momentum",
                "score_balance_sheet", "score_value", "market_cap", "data_confidence"
            ]
            fu_cols_mobile = ["tii_rank", "ticker", "tii_score", "sector"]

        if mobile_mode:
            simple_mobile_table(fu_df, fu_cols_mobile)
        else:
            st.dataframe(
                format_display_table(fu_df[[c for c in fu_cols_desktop if c in fu_df.columns]]),
                use_container_width=True, height=620, hide_index=True,
            )

# ── TAB 5 — ACTIONS ────────────────────────────────────────────────────────────

with tab5:
    st.subheader("Portfolio Actions")
    st.caption(f"Action engine uses TII strength plus crash overlay. Current crash risk: {crash_risk}.")

    c1, c2, c3 = st.columns(3)
    with c1:
        if dashboard_mode == MODE_PURE_TII:
            buy_count = int(top_actions_df["portfolio_action"].str.contains("Buy Signal", na=False).sum()) if "portfolio_action" in top_actions_df.columns else 0
            st.metric("Buy Signals", buy_count)
        else:
            add_count = int((top_actions_df["portfolio_action"] == "Add").sum()) if "portfolio_action" in top_actions_df.columns else 0
            st.metric("Add", add_count)
    with c2:
        if dashboard_mode == MODE_PURE_TII:
            monitor_count = int((top_actions_df["portfolio_action"] == "Borderline — Monitor").sum()) if "portfolio_action" in top_actions_df.columns else 0
            st.metric("Monitor", monitor_count)
        else:
            trim_count = int(top_actions_df["portfolio_action"].isin(["Trim", "Trim / Hold"]).sum()) if "portfolio_action" in top_actions_df.columns else 0
            st.metric("Trim", trim_count)
    with c3:
        no_signal_count = int((top_actions_df["portfolio_action"].isin(["No Signal", "Avoid"])).sum()) if "portfolio_action" in top_actions_df.columns else 0
        st.metric("No Signal / Avoid", no_signal_count)

    action_filter_options = ["All"] + sorted(top_actions_df["portfolio_action"].dropna().unique().tolist())
    selected_action = st.selectbox("Filter action", action_filter_options, index=0)

    display_actions_df = top_actions_df.copy()
    if selected_action != "All":
        display_actions_df = display_actions_df[display_actions_df["portfolio_action"] == selected_action].copy()

    if mobile_mode:
        action_mobile_cols = ["ticker", "scan_tii" if dashboard_mode == MODE_PURE_TII else "tii_score", "portfolio_action", "conviction"]
        simple_mobile_table(display_actions_df, action_mobile_cols)
    else:
        st.dataframe(
            format_display_table(display_actions_df),
            use_container_width=True, height=620, hide_index=True,
        )

    st.markdown("**System narrative**")
    st.write(build_top100_summary(top_signals_df, dashboard_mode=dashboard_mode))

    if dashboard_mode == MODE_ENHANCED:
        st.markdown("**Market regime note**")
        st.write(build_market_regime_narrative(macro_regime, crash_signal_df))
        st.markdown("**Sample AI notes**")
        sample_notes_df = top_actions_df.head(5).copy()
        for _, row in sample_notes_df.iterrows():
            st.caption(build_single_stock_ai_note(row, crash_risk=crash_risk))

# ── TAB 6 — MACRO MONITOR ──────────────────────────────────────────────────────

with tab6:
    st.subheader("Macro Monitor")
    st.caption("Threshold framework aligned with your crash-indicator rules.")

    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.metric("Crash Risk", crash_risk)
    with mc2:
        st.metric("Breaches", int(crash_signal_df["breach"].sum()))
    with mc3:
        st.metric("Macro Regime", macro_regime if dashboard_mode == MODE_ENHANCED else "N/A (Pure TII)")

    st.write(crash_risk_summary_text(crash_signal_df))

    st.dataframe(crash_signal_df, use_container_width=True, hide_index=True)

    breached = crash_signal_df[crash_signal_df["breach"]].copy()
    if breached.empty:
        st.success("No major crash-indicator thresholds are currently breached.")
    else:
        st.warning("One or more thresholds are breached. Review the Actions tab for tighter portfolio posture.")

    st.markdown("**Interpretation guide**")
    st.markdown(
        """
        - **Low**: broad risk posture acceptable.
        - **Guarded**: early warning signs; be more selective.
        - **Elevated**: reduce speculative exposure and tighten standards.
        - **High**: capital preservation first.
        """
    )

# ── DOWNLOAD ───────────────────────────────────────────────────────────────────

download_sheets = {
    ("Signals" if dashboard_mode == MODE_PURE_TII else "Top 100"): format_display_table(top_signals_df),
    "Portfolio Model": format_display_table(portfolio_df),
    "Actions": format_display_table(top_actions_df),
    "Macro Monitor": crash_signal_df,
    "Full Universe": format_display_table(full_universe_df),
}
if not current_positions_df.empty:
    download_sheets["Current Portfolio"] = format_display_table(current_positions_df)

download_bytes = to_excel_bytes(download_sheets)

st.download_button(
    "Download full workbook",
    data=download_bytes,
    file_name="steve_dashboard_full_output.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
    key="download_full_workbook",
)

# ── SYSTEM NOTES ───────────────────────────────────────────────────────────────

with st.expander("System Notes"):
    if dashboard_mode == MODE_PURE_TII:
        st.markdown(
            """
            **Pure TII Mode** — the original Stockradar system.

            - Signals are stocks that score **TII ≥ 4** AND have an active entry trigger (New High or Trend Reversal).
            - TII is computed by the weekly scan script from four components: Price trend (±3), Volume (±2), Moving Average (±2), MACD (±3).
            - Rankings are purely by scan TII. No fundamental overlay. No factor model.
            - Model portfolio uses **equal weighting** across up to 10 qualifying signals.
            - Stop logic: 12.5% initial stop. Breakeven trigger at +10%.

            ---
            **TII component reference**

            | Component | Max Bull | Max Bear |
            |---|---|---|
            | Price (HH+HL = uptrend) | +3 | -3 |
            | Volume (expanding + rising) | +2 | -2 |
            | Moving Average (28-EMA) | +2 | -2 |
            | MACD (lines + histogram) | +3 | -3 |
            | **Total** | **+10** | **-10** |
            """
        )
    else:
        st.markdown(
            """
            **Enhanced Mode** — Five-factor model layered over the weekly scan universe.

            - All scan stocks are ranked by a composite TII score (0–100 scale) based on quality (28%), growth (26%), momentum (22%), balance sheet (14%), and value (10%).
            - Macro regime tilts the factor weights.
            - When fundamental data is missing, scores fall back to pure momentum ranking — these are labelled **(Momentum)**.
            - Portfolio model weights are model outputs, not execution instructions.

            ---
            **Expected fundamental columns in weekly_scan_output.csv**

            | Column | Description |
            |---|---|
            | `pe` | Trailing or forward P/E |
            | `ps` | Price-to-sales |
            | `ev_ebit` | EV/EBIT |
            | `fcf_yield` | Free cash flow yield (%) |
            | `revenue_growth` | Revenue growth YoY (%) |
            | `eps_growth` | EPS growth YoY (%) |
            | `fcf_growth` | FCF growth YoY (%) |
            | `gross_margin` | Gross margin (%) |
            | `operating_margin` | Operating margin (%) |
            | `net_margin` | Net margin (%) |
            | `roic` | ROIC (%) |
            | `roe` | ROE (%) |
            | `debt_to_equity` | Debt-to-equity ratio |
            | `net_debt_ebitda` | Net debt / EBITDA |
            | `current_ratio` | Current ratio |
            """
        )
