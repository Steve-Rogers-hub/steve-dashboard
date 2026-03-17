import json

import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st

try:
    import gspread
    from google.oauth2.service_account import Credentials
except Exception:
    gspread = None
    Credentials = None


# =========================================================
# App config
# =========================================================
st.set_page_config(
    page_title="Steve's Weekly Portfolio Review",
    page_icon="📈",
    layout="wide",
)

SCAN_CSV_PATH = os.getenv("SCAN_CSV_PATH", "weekly_scan_output.csv")
DEFAULT_SHEET_NAME = os.getenv("PORTFOLIO_SHEET_NAME", "US Trades - USD")
DEFAULT_TARGET_SATELLITE_VALUE = float(os.getenv("TARGET_SATELLITE_VALUE", "40000"))
DEFAULT_NVDA_DOMINANT_MULTIPLIER = float(os.getenv("NVDA_DOMINANT_MULTIPLIER", "1.75"))


# =========================================================
# Data classes
# =========================================================
@dataclass
class Recommendation:
    action: str
    reason: str
    confidence: str
    portfolio_note: str
    opportunity_note: str
    market_regime: str
    action_bias: str
    risk_summary: str


# =========================================================
# Helpers
# =========================================================
def money(x: Optional[float]) -> str:
    if x is None or pd.isna(x):
        return "—"
    return f"${x:,.2f}"


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]
    return out


def clean_numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.replace("%", "", regex=False)
        .str.replace("(", "-", regex=False)
        .str.replace(")", "", regex=False)
        .str.strip(),
        errors="coerce",
    )


def normalize_ticker(ticker: str) -> str:
    t = str(ticker).upper().strip()
    if ":" in t:
        t = t.split(":")[-1]
    return t.strip()


def compute_market_regime(futures_tone: str, oil_status: str, vix_status: str, crash_risk: str):
    all_unknown = all(
        x == "Unknown"
        for x in [futures_tone, oil_status, vix_status, crash_risk]
    )

    if all_unknown:
        return "Unspecified", "Enter risk inputs", "Risk inputs are all unknown, so the dashboard cannot classify the market regime yet."

    score = 0

    futures_scores = {
        "Green": 1,
        "Flat": 0,
        "Red": -1,
        "Deep red": -2,
        "Unknown": 0,
    }
    oil_scores = {
        "Falling": 1,
        "Stable": 0,
        "Rising": -1,
        "Surging": -2,
        "Unknown": 0,
    }
    vix_scores = {
        "Cooling": 1,
        "Stable": 0,
        "Elevated": -1,
        "High stress": -2,
        "Unknown": 0,
    }
    crash_scores = {
        "Low": 1,
        "Moderate": 0,
        "High": -2,
        "Unknown": 0,
    }

    score += futures_scores.get(futures_tone, 0)
    score += oil_scores.get(oil_status, 0)
    score += vix_scores.get(vix_status, 0)
    score += crash_scores.get(crash_risk, 0)

    if score <= -4:
        return "Risk-off", "Patient only", "Conditions are defensive: weak futures, rising stress, or elevated crash risk."
    if score <= -1:
        return "Cautious", "Smaller and slower adds", "Conditions are mixed-to-weak. Favour patience and partial adds only."
    if score == 0:
        return "Neutral", "Selective adds only", "Conditions are balanced or mixed. Avoid forcing trades and stay selective."
    return "Supportive", "Normal growth posture", "Conditions are supportive for a normal growth stance."


# =========================================================
# Loaders
# =========================================================
@st.cache_data(ttl=300)
def load_scan_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        st.warning(f"Scan CSV not found: {path}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(path)
        return normalise_columns(df)
    except Exception as e:
        st.error(f"Could not read scan CSV: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_portfolio_csv(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return pd.DataFrame()

    try:
        df = pd.read_csv(uploaded_file)
        return normalise_columns(df)
    except Exception as e:
        st.error(f"Could not read uploaded portfolio CSV: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_google_sheet(sheet_id: str, worksheet_name: str) -> pd.DataFrame:
    try:
        if not sheet_id:
            st.error("No Google Sheet ID provided.")
            return pd.DataFrame()

        if gspread is None or Credentials is None:
            st.error("Google Sheets libraries are not available.")
            return pd.DataFrame()

       google_credentials = os.getenv("GOOGLE_CREDENTIALS")
if not google_credentials:
    st.error("GOOGLE_CREDENTIALS is not set.")
    return pd.DataFrame()

scopes = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

creds = Credentials.from_service_account_info(
    json.loads(google_credentials), scopes=scopes
)
        )
        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_id).worksheet(worksheet_name)

        rows = ws.get_all_values()
        if not rows or len(rows) < 10:
            st.error("Worksheet does not contain enough rows.")
            return pd.DataFrame()

        header_row_index = 8  # Row 9 in your sheet
        header = rows[header_row_index]
        data = rows[header_row_index + 1 :]

        seen = {}
        cleaned = []
        for i, h in enumerate(header):
            name = str(h).strip() or f"Unnamed_{i+1}"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 1
            cleaned.append(name)

        df = pd.DataFrame(data, columns=cleaned)
        st.success(f"Connected to worksheet: {worksheet_name}")
        return normalise_columns(df)

    except Exception as e:
        st.error(f"Google Sheet load error: {e}")
        return pd.DataFrame()


# =========================================================
# Data preparation
# =========================================================
def prepare_scan(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    for c in ["TII", "LastClose", "SuggestedEntryRef", "SuggestedInitialStop"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "Ticker" in out.columns:
        out["Ticker"] = out["Ticker"].astype(str).map(normalize_ticker)

    for c in ["Qualifies", "EntrySignal", "NewHigh", "TrendReversal"]:
        if c in out.columns:
            out[c] = (
                out[c]
                .astype(str)
                .str.lower()
                .map({"true": True, "false": False})
                .fillna(False)
            )

    return out


def prepare_portfolio(
    df: pd.DataFrame,
    target_satellite_value: float,
    nvda_dominant_multiplier: float,
) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    rename_map = {
        "Stock": "Ticker",
        "Latest Market Price": "LatestPrice",
        "Market Value": "MarketValue",
        "Stop/Sell Price": "StopPrice",
        "Position Status": "PositionStatus",
        "Purchase Price": "PurchasePrice",
        "Net Profit/Loss": "NetProfitLoss",
        "% Gain/Loss": "PctGainLoss",
        "Highest Price Since Entry": "HighestPriceSinceEntry",
        "Company Name": "CompanyName",
        "Trade Date": "TradeDate",
        "Quantity": "Quantity",
    }

    for old, new in rename_map.items():
        if old in out.columns:
            out = out.rename(columns={old: new})

    flexible_map = {
        "Ticker": ["Ticker", "Stock", "Code", "Symbol"],
        "LatestPrice": ["LatestPrice", "Latest Market Price", "Last Price", "Price"],
        "MarketValue": ["MarketValue", "Market Value", "Current Market Value", "Value"],
        "StopPrice": ["StopPrice", "Stop/Sell Price", "Stop Price", "Trend Reversal Sell Stop"],
        "PositionStatus": ["PositionStatus", "Position Status", "Status", "Open/Closed"],
        "PurchasePrice": ["PurchasePrice", "Purchase Price"],
        "NetProfitLoss": ["NetProfitLoss", "Net Profit/Loss"],
        "PctGainLoss": ["PctGainLoss", "% Gain/Loss"],
        "HighestPriceSinceEntry": ["HighestPriceSinceEntry", "Highest Price Since Entry"],
        "Quantity": ["Quantity"],
        "CompanyName": ["CompanyName", "Company Name"],
        "TradeDate": ["TradeDate", "Trade Date"],
    }

    def find_first(possible_names):
        for name in possible_names:
            if name in out.columns:
                return name
        return None

    for canonical_name, aliases in flexible_map.items():
        found = find_first(aliases)
        if found and found != canonical_name:
            out = out.rename(columns={found: canonical_name})

    numeric_cols = [
        "LatestPrice",
        "MarketValue",
        "StopPrice",
        "PurchasePrice",
        "NetProfitLoss",
        "PctGainLoss",
        "HighestPriceSinceEntry",
        "Quantity",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = clean_numeric_series(out[col])

    if "Ticker" in out.columns:
        out["Ticker"] = out["Ticker"].astype(str).map(normalize_ticker)

    if "PositionStatus" in out.columns:
        out["PositionStatus"] = out["PositionStatus"].astype(str).str.strip()

    if "PositionStatus" in out.columns:
        out = out[
            out["PositionStatus"]
            .astype(str)
            .str.lower()
            .str.contains("open", na=False)
        ]

    if "Ticker" in out.columns:
        out = out.dropna(subset=["Ticker"])

    if "MarketValue" in out.columns:
        out["PositionSize"] = out["MarketValue"]
        out["TargetSatellite"] = target_satellite_value
        out["DistanceFromTarget"] = out["PositionSize"] - target_satellite_value

        nvda_target = target_satellite_value * nvda_dominant_multiplier

        def band_status(row):
            ticker = row.get("Ticker", "")
            value = row.get("PositionSize")

            if pd.isna(value):
                return "Unknown"

            if ticker == "NVDA":
                if value > nvda_target * 1.25:
                    return "Overweight core"
                if value < nvda_target * 0.85:
                    return "Below core target"
                return "Dominant core"

            if value < target_satellite_value * 0.75:
                return "Underweight"
            if value > target_satellite_value * 1.25:
                return "Overweight"
            return "Balanced"

        out["BandStatus"] = out.apply(band_status, axis=1)

        def suggested_action(row):
            ticker = row.get("Ticker", "")
            status = row.get("BandStatus", "")
            value = row.get("PositionSize")

            if pd.isna(value):
                return "Review"

            if ticker == "NVDA":
                if status == "Overweight core":
                    return "Hold / trim only on upgrade opportunity"
                if status == "Below core target":
                    gap = int(max(0, nvda_target - value))
                    return f"Add approx ${gap:,} only if leadership continues"
                return "Hold dominant core"

            if status == "Underweight":
                add_amount = int(max(0, target_satellite_value - value))
                return f"Add approx ${add_amount:,} if signal remains strong"

            if status == "Overweight":
                return "Hold / trim only on upgrade opportunity"

            if status == "Balanced":
                return "Hold"

            return "Review"

        out["SuggestedAction"] = out.apply(suggested_action, axis=1)

    return out


# =========================================================
# Rotation engine
# =========================================================
def compute_rotation_candidate(
    portfolio: pd.DataFrame,
    scan: pd.DataFrame,
    market_regime: str,
):
    if portfolio.empty or scan.empty:
        return None

    if market_regime in ["Risk-off", "Cautious", "Unspecified"]:
        return None

    if "Ticker" not in portfolio.columns or "Ticker" not in scan.columns:
        return None

    current_tickers = set(portfolio["Ticker"].tolist())

    qualified = scan.copy()
    if "Qualifies" in qualified.columns:
        qualified = qualified[qualified["Qualifies"] == True]

    if qualified.empty or "TII" not in qualified.columns:
        return None

    qualified = qualified.sort_values(by="TII", ascending=False)

    new_candidates = qualified[~qualified["Ticker"].isin(current_tickers)]
    if new_candidates.empty:
        return None

    fresh = new_candidates.iloc[0]

    candidates = portfolio.copy()
    if "BandStatus" in candidates.columns:
        candidates = candidates[candidates["Ticker"] != "NVDA"]
        if not candidates.empty:
            # Prefer underweight or balanced names as rotation-out candidates
            status_rank = {
                "Underweight": 0,
                "Balanced": 1,
                "Overweight": 2,
                "Below core target": 0,
                "Dominant core": 99,
                "Overweight core": 99,
                "Unknown": 50,
            }
            candidates["StatusRank"] = candidates["BandStatus"].map(status_rank).fillna(50)
        else:
            return None

    if candidates.empty:
        return None

    if "PositionSize" not in candidates.columns:
        return None

    weakest = candidates.sort_values(by=["StatusRank", "PositionSize"], ascending=[True, True]).iloc[0]

    weakest_size = float(weakest.get("PositionSize", 0))
    fresh_tii = float(fresh.get("TII", 0))

    # Simple threshold to avoid noisy rotation ideas
    if weakest_size < 15000 and fresh_tii >= 9:
        return {
            "sell_ticker": weakest["Ticker"],
            "sell_value": weakest_size,
            "buy_ticker": fresh["Ticker"],
            "buy_tii": fresh_tii,
            "buy_signal": fresh.get("SignalType", ""),
            "suggested_rotation": min(40000, max(10000, weakest_size)),
        }

    return None


# =========================================================
# Recommendation engine
# =========================================================
def build_recommendation(
    portfolio: pd.DataFrame,
    scan: pd.DataFrame,
    target_satellite_value: float,
    nvda_dominant_multiplier: float,
    futures_tone: str,
    oil_status: str,
    vix_status: str,
    crash_risk: str,
) -> Recommendation:
    market_regime, action_bias, risk_summary = compute_market_regime(
        futures_tone, oil_status, vix_status, crash_risk
    )

    if portfolio.empty and scan.empty:
        return Recommendation(
            action="Setup required",
            reason="Load both the scan CSV and portfolio data to generate recommendations.",
            confidence="Low",
            portfolio_note="No portfolio data loaded yet.",
            opportunity_note="No scan data loaded yet.",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if scan.empty:
        return Recommendation(
            action="Hold",
            reason="No scan data is available yet.",
            confidence="Low",
            portfolio_note="Portfolio loaded, but the scan CSV is missing.",
            opportunity_note="Run your weekly Colab scan and rerun the dashboard.",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    qualified = scan.copy()
    if "Qualifies" in qualified.columns:
        qualified = qualified[qualified["Qualifies"] == True]

    if not qualified.empty and "TII" in qualified.columns:
        qualified = qualified.sort_values(by="TII", ascending=False)

    current_tickers = (
        set(portfolio["Ticker"].tolist())
        if not portfolio.empty and "Ticker" in portfolio.columns
        else set()
    )

    owned_candidates = (
        qualified[qualified["Ticker"].isin(current_tickers)]
        if not qualified.empty and "Ticker" in qualified.columns
        else pd.DataFrame()
    )
    new_candidates = (
        qualified[~qualified["Ticker"].isin(current_tickers)]
        if not qualified.empty and "Ticker" in qualified.columns
        else pd.DataFrame()
    )

    overweight = pd.DataFrame()
    underweight = pd.DataFrame()
    if not portfolio.empty and "BandStatus" in portfolio.columns:
        overweight = portfolio[portfolio["BandStatus"].isin(["Overweight", "Overweight core"])]
        underweight = portfolio[portfolio["BandStatus"].isin(["Underweight", "Below core target"])]

    if market_regime == "Unspecified":
        return Recommendation(
            action="Watch / possible add",
            reason="The portfolio and scan are loaded, but the risk panel is still unspecified.",
            confidence="Medium",
            portfolio_note="The position logic is working, but market-regime guidance is limited until futures, oil, VIX, or crash risk are entered.",
            opportunity_note="Enter risk inputs to turn the dashboard into a true market-aware decision engine.",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if market_regime == "Risk-off":
        return Recommendation(
            action="Hold / patient only",
            reason="The market regime is risk-off. Preserve flexibility and avoid forcing adds into weak conditions.",
            confidence="High",
            portfolio_note="Use the current portfolio as the base. Let stops and strong existing trends do the work.",
            opportunity_note="Fresh scan names can stay on watch, but adds should be smaller and slower until conditions stabilise.",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if market_regime == "Cautious" and not underweight.empty:
        smallest = underweight.sort_values(by="PositionSize", ascending=True).iloc[0]
        return Recommendation(
            action="Selective add only",
            reason=f"The market regime is cautious. {smallest['Ticker']} is underweight, but adds should be partial rather than full.",
            confidence="Medium",
            portfolio_note="Focus on existing leaders and underweight satellites only if they remain technically strong.",
            opportunity_note=f"Most underweight holding: {smallest['Ticker']} | Current value {money(smallest.get('PositionSize'))}",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    rotation = compute_rotation_candidate(portfolio, scan, market_regime)
    if rotation is not None:
        return Recommendation(
            action="Rotation candidate detected",
            reason=f"{rotation['buy_ticker']} is a fresh high-ranking leader and {rotation['sell_ticker']} is one of the weakest current non-core positions.",
            confidence="Medium",
            portfolio_note="This is the first step toward a true capital-rotation engine: weakest holding versus strongest fresh leader.",
            opportunity_note=(
                f"Consider rotating about {money(rotation['suggested_rotation'])} "
                f"from {rotation['sell_ticker']} into {rotation['buy_ticker']} "
                f"({rotation['buy_signal']}, TII {int(rotation['buy_tii'])})."
            ),
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if not portfolio.empty and "Ticker" in portfolio.columns and "PositionSize" in portfolio.columns:
        nvda_rows = portfolio[portfolio["Ticker"] == "NVDA"]
        if not nvda_rows.empty:
            nvda_value = float(nvda_rows["PositionSize"].iloc[0])
            nvda_target = target_satellite_value * nvda_dominant_multiplier

            if nvda_value > nvda_target * 1.25 and not new_candidates.empty:
                top = new_candidates.iloc[0]
                return Recommendation(
                    action="Consider trim + rotate",
                    reason=f"NVDA is materially above its intended dominant-core band and {top['Ticker']} is a fresh qualified leader.",
                    confidence="Medium",
                    portfolio_note="This supports gradual diversification while keeping NVDA as the dominant core holding.",
                    opportunity_note=f"Top fresh candidate: {top['Ticker']} | TII {int(top['TII']) if not pd.isna(top.get('TII')) else '—'} | {top.get('SignalType', '')}",
                    market_regime=market_regime,
                    action_bias=action_bias,
                    risk_summary=risk_summary,
                )

    if not underweight.empty and not owned_candidates.empty:
        strongest_owned = owned_candidates.iloc[0]
        weakest_underweight = underweight.sort_values(by="PositionSize", ascending=True).iloc[0]
        return Recommendation(
            action="Watch / selective add",
            reason=f"{weakest_underweight['Ticker']} is underweight and {strongest_owned['Ticker']} remains one of your owned scan-qualified names.",
            confidence="Medium",
            portfolio_note="This favours adding only to underweight holdings that still rank well in the scan, rather than forcing brand new positions.",
            opportunity_note=f"Most underweight holding: {weakest_underweight['Ticker']} | Strongest owned leader: {strongest_owned['Ticker']}",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if not underweight.empty:
        weakest_underweight = underweight.sort_values(by="PositionSize", ascending=True).iloc[0]
        return Recommendation(
            action="Watch / possible add",
            reason=f"{weakest_underweight['Ticker']} is materially below your $40k satellite target.",
            confidence="Medium",
            portfolio_note="Use scan strength to decide whether to add to underweight satellites or rotate into a stronger fresh name.",
            opportunity_note=(
                f"Most underweight holding: {weakest_underweight['Ticker']} | Current value {money(weakest_underweight.get('PositionSize'))} | "
                f"{weakest_underweight.get('SuggestedAction', '')}"
            ),
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    if not new_candidates.empty:
        top = new_candidates.iloc[0]
        return Recommendation(
            action="Watch / possible add",
            reason=f"{top['Ticker']} is a fresh qualified scan leader, but no forced rotation is required yet.",
            confidence="Medium",
            portfolio_note="Current holdings remain broadly aligned with the growth framework.",
            opportunity_note=f"Top fresh candidate: {top['Ticker']} | TII {int(top['TII']) if not pd.isna(top.get('TII')) else '—'} | {top.get('SignalType', '')}",
            market_regime=market_regime,
            action_bias=action_bias,
            risk_summary=risk_summary,
        )

    return Recommendation(
        action="Hold",
        reason="Current holdings remain stronger than fresh alternatives from the weekly scan.",
        confidence="High",
        portfolio_note="No immediate rebalance is required under the current $40k satellite framework.",
        opportunity_note="Let winners run and reassess after the next scan.",
        market_regime=market_regime,
        action_bias=action_bias,
        risk_summary=risk_summary,
    )


# =========================================================
# UI
# =========================================================
st.title("Steve's Weekly Portfolio Review")
st.caption("Private dashboard for weekly scan review, portfolio geometry, and rule-based action suggestions.")

with st.sidebar:
    st.header("Data sources")
    scan_path = st.text_input("Weekly scan CSV path", value=SCAN_CSV_PATH)
    portfolio_source = st.radio("Portfolio source", ["Upload CSV", "Google Sheet"], index=1)

    uploaded_portfolio = None
    sheet_id = ""
    worksheet_name = DEFAULT_SHEET_NAME

    if portfolio_source == "Upload CSV":
        uploaded_portfolio = st.file_uploader("Upload portfolio CSV", type=["csv"])
    else:
        sheet_id = st.text_input("Google Sheet ID")
        worksheet_name = st.text_input("Worksheet name", value=DEFAULT_SHEET_NAME)

    st.header("Portfolio rules")
    target_satellite_value = st.number_input(
        "Satellite target ($)",
        min_value=1000,
        value=int(DEFAULT_TARGET_SATELLITE_VALUE),
        step=1000,
    )
    nvda_dominant_multiplier = st.number_input(
        "NVDA dominance multiplier",
        min_value=1.0,
        value=float(DEFAULT_NVDA_DOMINANT_MULTIPLIER),
        step=0.05,
    )

    st.header("Risk panel")
    futures_tone = st.selectbox(
        "Futures tone",
        ["Unknown", "Green", "Flat", "Red", "Deep red"],
        index=0,
    )
    oil_status = st.selectbox(
        "Oil",
        ["Unknown", "Falling", "Stable", "Rising", "Surging"],
        index=0,
    )
    vix_status = st.selectbox(
        "VIX",
        ["Unknown", "Cooling", "Stable", "Elevated", "High stress"],
        index=0,
    )
    crash_risk = st.selectbox(
        "Crash risk",
        ["Unknown", "Low", "Moderate", "High"],
        index=0,
    )

    if st.button("Refresh dashboard data"):
        st.cache_data.clear()
        st.rerun()


# Load data
scan_df = prepare_scan(load_scan_csv(scan_path))

if portfolio_source == "Upload CSV":
    raw_portfolio_df = load_portfolio_csv(uploaded_portfolio)
else:
    raw_portfolio_df = load_google_sheet(sheet_id, worksheet_name)

portfolio_df = prepare_portfolio(
    raw_portfolio_df,
    target_satellite_value=float(target_satellite_value),
    nvda_dominant_multiplier=float(nvda_dominant_multiplier),
)

total_value = 0.0
open_holdings = 0
if not portfolio_df.empty and "PositionSize" in portfolio_df.columns:
    total_value = float(portfolio_df["PositionSize"].sum())
    open_holdings = len(portfolio_df)

rec = build_recommendation(
    portfolio_df,
    scan_df,
    target_satellite_value=float(target_satellite_value),
    nvda_dominant_multiplier=float(nvda_dominant_multiplier),
    futures_tone=futures_tone,
    oil_status=oil_status,
    vix_status=vix_status,
    crash_risk=crash_risk,
)

rotation = compute_rotation_candidate(portfolio_df, scan_df, rec.market_regime)

# Summary cards
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.metric("Portfolio value", money(total_value))
    st.caption(f"Open holdings: {open_holdings}")

with c2:
    qualified_count = 0
    if not scan_df.empty and "Qualifies" in scan_df.columns:
        qualified_count = int(scan_df[scan_df["Qualifies"] == True].shape[0])
    st.metric("Qualified scan names", qualified_count)

    if not scan_df.empty and "Ticker" in scan_df.columns and "TII" in scan_df.columns:
        top3 = ", ".join(scan_df.sort_values(by="TII", ascending=False)["Ticker"].head(3).tolist())
        st.caption(f"Top names: {top3}")

with c3:
    st.metric("This week's action", rec.action)
    st.caption(f"Confidence: {rec.confidence}")

with c4:
    st.metric("Market regime", rec.market_regime)
    st.caption(f"Action bias: {rec.action_bias}")

st.divider()

st.subheader("AI Recommendation")
st.info(
    f"**Primary call:** {rec.action}\n\n"
    f"**Market regime:** {rec.market_regime}\n\n"
    f"**Action bias:** {rec.action_bias}\n\n"
    f"**Risk summary:** {rec.risk_summary}\n\n"
    f"**Why:** {rec.reason}\n\n"
    f"**Portfolio note:** {rec.portfolio_note}\n\n"
    f"**Opportunity note:** {rec.opportunity_note}"
)

left, right = st.columns([1.5, 1])

with left:
    st.subheader("Portfolio geometry")

    if portfolio_df.empty:
        st.warning("Load a portfolio CSV or Google Sheet to see portfolio geometry.")
    else:
        display_cols = [
            c for c in [
                "Ticker",
                "CompanyName",
                "PositionSize",
                "LatestPrice",
                "StopPrice",
                "BandStatus",
                "SuggestedAction",
            ]
            if c in portfolio_df.columns
        ]
        show_df = portfolio_df[display_cols].copy()

        if "PositionSize" in show_df.columns:
            show_df["PositionSize"] = show_df["PositionSize"].map(money)
        if "LatestPrice" in show_df.columns:
            show_df["LatestPrice"] = show_df["LatestPrice"].map(money)
        if "StopPrice" in show_df.columns:
            show_df["StopPrice"] = show_df["StopPrice"].map(money)

        st.dataframe(show_df, width="stretch", hide_index=True)

with right:
    st.subheader("Scan leaders")

    if scan_df.empty:
        st.warning("No weekly scan CSV found yet.")
    else:
        leaders = scan_df.copy()
        if "TII" in leaders.columns:
            leaders = leaders.sort_values(by="TII", ascending=False)

        leader_cols = [
            c for c in [
                "Ticker",
                "TII",
                "SignalType",
                "LastClose",
                "SuggestedEntryRef",
                "SuggestedInitialStop",
            ]
            if c in leaders.columns
        ]

        leaders = leaders[leader_cols].head(12)

        for c in ["LastClose", "SuggestedEntryRef", "SuggestedInitialStop"]:
            if c in leaders.columns:
                leaders[c] = leaders[c].map(money)

        st.dataframe(leaders, width="stretch", hide_index=True)

st.divider()

st.subheader("Position management")

pm1, pm2 = st.columns(2)

with pm1:
    st.markdown("**Underweight satellites**")
    if not portfolio_df.empty and "BandStatus" in portfolio_df.columns:
        under = portfolio_df[portfolio_df["BandStatus"].isin(["Underweight", "Below core target"])].copy()
        if not under.empty:
            under_display = under[[c for c in ["Ticker", "PositionSize", "SuggestedAction"] if c in under.columns]].copy()
            if "PositionSize" in under_display.columns:
                under_display["PositionSize"] = under_display["PositionSize"].map(money)
            st.dataframe(under_display, width="stretch", hide_index=True)
        else:
            st.write("None")

with pm2:
    st.markdown("**Overweight positions**")
    if not portfolio_df.empty and "BandStatus" in portfolio_df.columns:
        over = portfolio_df[portfolio_df["BandStatus"].isin(["Overweight", "Overweight core"])].copy()
        if not over.empty:
            over_display = over[[c for c in ["Ticker", "PositionSize", "SuggestedAction"] if c in over.columns]].copy()
            if "PositionSize" in over_display.columns:
                over_display["PositionSize"] = over_display["PositionSize"].map(money)
            st.dataframe(over_display, width="stretch", hide_index=True)
        else:
            st.write("None")

st.divider()

st.subheader("Capital rotation engine")

if rotation is None:
    st.write("No rotation candidate detected under current market conditions.")
else:
    st.warning(
        f"Potential rotation: consider rotating about {money(rotation['suggested_rotation'])} "
        f"from {rotation['sell_ticker']} into {rotation['buy_ticker']}."
    )
    rot_df = pd.DataFrame(
        [
            {
                "Sell candidate": rotation["sell_ticker"],
                "Approx value to rotate": money(rotation["suggested_rotation"]),
                "Buy candidate": rotation["buy_ticker"],
                "Buy signal": rotation["buy_signal"],
                "Buy TII": int(rotation["buy_tii"]),
            }
        ]
    )
    st.dataframe(rot_df, width="stretch", hide_index=True)

st.divider()

st.subheader("Risk summary")

rs1, rs2, rs3, rs4 = st.columns(4)
with rs1:
    st.metric("Futures", futures_tone)
with rs2:
    st.metric("Oil", oil_status)
with rs3:
    st.metric("VIX", vix_status)
with rs4:
    st.metric("Crash risk", crash_risk)

st.caption(
    "This version adds automatic capital rotation detection: weakest current non-core holding versus strongest fresh scan leader, filtered by market regime."
)