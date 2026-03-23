import os
import json
import io
from datetime import datetime

import numpy as np
import pandas as pd
import streamlit as st
import gspread
import yfinance as yf
from google.oauth2.service_account import Credentials

# =========================
# PAGE CONFIG
# =========================
st.set_page_config(
    page_title="Steve Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

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

HISTORY_LOOKBACK_DAYS = 320
DOWNLOAD_BATCH_SIZE = 20
DEFAULT_PORTFOLIO_SIZE = 10
DEFAULT_SECTOR_CAP = 2

UNCHANGED_AT_ANY_RISK = {"MA", "TJX", "ROBO"}
RISK_SENSITIVE_NAMES = {"NVDA", "ANET", "CRWD", "TSLA"}


# =========================
# GENERAL HELPERS
# =========================
def safe_numeric(series, fill_value=np.nan):
    return pd.to_numeric(series, errors="coerce").fillna(fill_value)


def clip_series(series, lower_q=0.02, upper_q=0.98):
    s = safe_numeric(series)
    if s.notna().sum() == 0:
        return s
    return s.clip(lower=s.quantile(lower_q), upper=s.quantile(upper_q))


def rank_pct(series, ascending=True):
    s = safe_numeric(series)
    valid = s.notna()
    out = pd.Series(np.nan, index=series.index, dtype=float)
    if valid.sum() == 0:
        return out
    out.loc[valid] = s[valid].rank(pct=True, ascending=ascending) * 100
    return out


def find_first_existing(df, candidates, default=np.nan):
    for c in candidates:
        if c in df.columns:
            return df[c]
    return pd.Series(default, index=df.index)


def fill_missing_with_median_by_group(df, value_col, group_col):
    out = df[value_col].copy()
    if group_col in df.columns:
        group_medians = df.groupby(group_col)[value_col].transform("median")
        out = out.fillna(group_medians)
    return out.fillna(df[value_col].median())


def clean_symbol(value):
    if pd.isna(value):
        return ""
    return str(value).strip().upper().replace("/", "-")


def format_display_table(df):
    x = df.copy()
    pct_cols = [
        "model_weight_pct", "data_confidence", "revenue_growth", "eps_growth",
        "fcf_growth", "gross_margin", "operating_margin", "net_margin",
        "roic", "roe", "fcf_yield", "return_1m", "return_3m", "return_6m",
        "return_12m"
    ]
    money_cols = ["market_cap", "price"]
    score_cols = [
        "tii_score", "score_growth", "score_quality", "score_momentum",
        "score_balance_sheet", "score_value"
    ]
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


def to_excel_bytes(df_dict):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31])
    output.seek(0)
    return output.getvalue()


# =========================
# GOOGLE SHEETS
# =========================
def get_gspread_client():
    try:
        secret_file_candidates = [
            "/etc/secrets/google-service-account.json",
            "google-service-account.json",
        ]
        creds = None
        credential_source = None

        for candidate in secret_file_candidates:
            if os.path.exists(candidate):
                creds = Credentials.from_service_account_file(candidate, scopes=SCOPES)
                credential_source = candidate
                break

        if creds is None:
            creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
            if not creds_json:
                st.error("Missing Google credentials.")
                st.stop()
            creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
            credential_source = "GOOGLE_SERVICE_ACCOUNT_JSON"

        st.caption(f"Credential source: {credential_source}")
    except Exception as e:
        st.error(f"Could not load Google credentials: {e}")
        st.stop()

    return gspread.authorize(creds)


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
    df = df.replace("", pd.NA).dropna(how="all").dropna(axis=1, how="all")
    return df


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
                    frame = raw.xs(ticker, axis=1, level=1).copy().dropna(how="all")
                    if not frame.empty:
                        out[ticker] = frame
                except Exception:
                    pass
        else:
            tickers = list(dict.fromkeys(raw.columns.get_level_values(0)))
            for ticker in tickers:
                try:
                    frame = raw[ticker].copy().dropna(how="all")
                    if not frame.empty:
                        out[ticker] = frame
                except Exception:
                    pass
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
        out[ticker] = frame[keep_cols].dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

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
            pass

    return history_map


@st.cache_data(ttl=60 * 60 * 24)
def fetch_symbol_metadata(symbols):
    meta = {}
    for symbol in symbols:
        try:
            info = yf.Ticker(symbol).info or {}
            meta[symbol] = {
                "sector": info.get("sector") or "",
                "market_cap": info.get("marketCap"),
                "short_name": info.get("shortName") or info.get("longName") or "",
            }
        except Exception:
            meta[symbol] = {"sector": "", "market_cap": np.nan, "short_name": ""}
    return meta


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


@st.cache_data(ttl=3600)
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
    meta_df = pd.DataFrame([
        {
            "ticker": s,
            "sector_meta": meta.get(s, {}).get("sector", ""),
            "market_cap_meta": meta.get(s, {}).get("market_cap", np.nan),
            "short_name_meta": meta.get(s, {}).get("short_name", ""),
        }
        for s in symbols
    ])

    universe = universe.merge(history_df, on="ticker", how="left")
    universe = universe.merge(meta_df, on="ticker", how="left")

    if "sector" not in universe.columns:
        universe["sector"] = universe["sector_meta"]
    else:
        universe["sector"] = universe["sector"].fillna("")
        blank_sector = universe["sector"].astype(str).str.strip() == ""
        universe.loc[blank_sector, "sector"] = universe.loc[blank_sector, "sector_meta"]

    if "market_cap" not in universe.columns:
        universe["market_cap"] = universe["market_cap_meta"]
    else:
        universe["market_cap"] = pd.to_numeric(universe["market_cap"], errors="coerce")
        universe["market_cap"] = universe["market_cap"].fillna(universe["market_cap_meta"])

    blank_company = universe["company"].astype(str).str.strip().isin(["", "nan", "None"])
    universe.loc[blank_company, "company"] = universe.loc[blank_company, "short_name_meta"].fillna(universe.loc[blank_company, "ticker"])

    drop_cols = [c for c in ["sector_meta", "market_cap_meta", "short_name_meta"] if c in universe.columns]
    universe = universe.drop(columns=drop_cols)

    return universe


# =========================
# TII FACTOR ENGINE
# =========================
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
        "risk on": {"growth": 1.15, "quality": 0.95, "value": 0.95, "momentum": 1.10, "balance_sheet": 0.90},
        "neutral": {"growth": 1.00, "quality": 1.00, "value": 1.00, "momentum": 1.00, "balance_sheet": 1.00},
        "risk off": {"growth": 0.85, "quality": 1.15, "value": 1.05, "momentum": 0.95, "balance_sheet": 1.20},
        "inflationary": {"growth": 0.90, "quality": 1.05, "value": 1.15, "momentum": 0.95, "balance_sheet": 1.10},
        "disinflation": {"growth": 1.10, "quality": 1.05, "value": 0.95, "momentum": 1.05, "balance_sheet": 0.95},
    }
    return overlays.get(regime, overlays["neutral"])


def compute_tii_scores(df, macro_regime="Neutral"):
    """
    Corrected TII engine:
    - harmonises columns
    - clips outliers
    - scores each factor robustly
    - applies macro overlay
    - creates a final TII score and rank
    """
    x = standardise_factor_columns(df).copy()

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
        if col in x.columns:
            x[col] = clip_series(x[col])

    fill_cols = [
        "pe", "ps", "ev_ebit", "fcf_yield",
        "revenue_growth", "eps_growth", "fcf_growth",
        "gross_margin", "operating_margin", "net_margin", "roic", "roe",
        "debt_to_equity", "net_debt_ebitda", "current_ratio",
        "return_1m", "return_3m", "return_6m", "return_12m", "volatility"
    ]
    for col in fill_cols:
        x[col] = fill_missing_with_median_by_group(x, col, "sector")

    x["score_value"] = (
        rank_pct(x["pe"], ascending=True) * 0.25
        + rank_pct(x["ps"], ascending=True) * 0.20
        + rank_pct(x["ev_ebit"], ascending=True) * 0.25
        + rank_pct(x["fcf_yield"], ascending=False) * 0.30
    )

    x["score_growth"] = (
        rank_pct(x["revenue_growth"], ascending=False) * 0.40
        + rank_pct(x["eps_growth"], ascending=False) * 0.35
        + rank_pct(x["fcf_growth"], ascending=False) * 0.25
    )

    x["score_quality"] = (
        rank_pct(x["gross_margin"], ascending=False) * 0.15
        + rank_pct(x["operating_margin"], ascending=False) * 0.25
        + rank_pct(x["net_margin"], ascending=False) * 0.15
        + rank_pct(x["roic"], ascending=False) * 0.25
        + rank_pct(x["roe"], ascending=False) * 0.20
    )

    x["score_balance_sheet"] = (
        rank_pct(x["debt_to_equity"], ascending=True) * 0.40
        + rank_pct(x["net_debt_ebitda"], ascending=True) * 0.35
        + rank_pct(x["current_ratio"], ascending=False) * 0.25
    )

    x["score_momentum_raw"] = (
        rank_pct(x["return_1m"], ascending=False) * 0.10
        + rank_pct(x["return_3m"], ascending=False) * 0.25
        + rank_pct(x["return_6m"], ascending=False) * 0.30
        + rank_pct(x["return_12m"], ascending=False) * 0.35
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
        x["score_quality_adj"] * 0.28
        + x["score_growth_adj"] * 0.26
        + x["score_momentum_adj"] * 0.22
        + x["score_balance_sheet_adj"] * 0.14
        + x["score_value_adj"] * 0.10
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
        rank_pct(x["return_1m"], ascending=False) * 0.15
        + rank_pct(x["return_3m"], ascending=False) * 0.30
        + rank_pct(x["return_6m"], ascending=False) * 0.30
        + rank_pct(x["return_12m"], ascending=False) * 0.25
    )

    fallback_score = (
        fallback_score * 0.85
        + rank_pct(x["volatility"], ascending=True) * 0.15
    )

    x.loc[fallback_mask, "tii_score"] = fallback_score.loc[fallback_mask]
    x["score_source"] = np.where(fallback_mask, "Price fallback", "Full factor model")

    ranked = x["tii_score"].rank(method="min", ascending=False)
    x["tii_rank"] = ranked.astype("Int64")

    return x.sort_values("tii_score", ascending=False).reset_index(drop=True)

    def split_top100_and_full_universe(scored_df):
