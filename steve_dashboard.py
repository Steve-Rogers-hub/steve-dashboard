import os
import json
import streamlit as st
import pandas as pd
import gspread
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


# =========================
# GOOGLE SHEETS
# =========================

def get_gspread_client():
    try:
        secret_file_path = "/etc/secrets/google-service-account.json"

        if os.path.exists(secret_file_path):
            creds = Credentials.from_service_account_file(
                secret_file_path,
                scopes=SCOPES,
            )
        else:
            creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

            if not creds_json:
                st.error("Missing Google credentials.")
                st.stop()

            service_account_info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(
                service_account_info,
                scopes=SCOPES,
            )

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

    if not data:
        return pd.DataFrame()

    if len(data) <= header_row_index:
        return pd.DataFrame()

    headers = data[header_row_index]
    rows = data[header_row_index + 1:]

    clean_headers = []
    seen = {}

    for i, h in enumerate(headers):
        col = h.strip() if h else f"Column_{i+1}"
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
        "Quantity",
        "Purchase Price",
        "Buy Cost",
        "Buy Brokerage",
        "Total Purchase Costs",
        "Latest Market Price",
        "Market Value",
        "Stop/Sell Price",
        "Gross Profit/Loss",
        "Net Profit/Loss",
        "% Gain/Loss",
        "Previous Day Close Price",
        "Previous Day Market Value",
        "Highest Market Price",
        "Dividend Income",
        "Dividend Franking Credits",
        "Sold Date",
        "Sold Price",
        "Sale Price",
        "Sold Value",
        "Sold Brokerage",
        "Total Sale Proceeds",
        "Gross Realised Profit/Loss",
        "Net Realised Profit Loss",
        "Net Realised Profit/Loss",
        "Gain/Loss.2",
        "Gain/Loss_2",
    ]

    for col in numeric_cols:
        if col in df.columns:
            cleaned = (
                df[col]
                .astype(str)
                .str.replace("$", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.replace("%", "", regex=False)
                .str.replace("(", "-", regex=False)
                .str.replace(")", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(cleaned, errors="coerce")

    date_cols = ["Trade Date", "Sold Date"]
    for col in date_cols:
        if col in df.columns:
            dt = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
            df[col] = dt.dt.strftime("%d-%b-%Y")
            df.loc[dt.isna(), col] = ""

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str).str.strip()

    return df


# =========================
# WALL STREET SCAN CSV
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


def detect_scan_name_column(df):
    candidates = ["ticker", "symbol", "stock", "name", "company", "scan_name"]
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return df.columns[0] if len(df.columns) > 0 else None


def detect_scan_score_column(df):
    candidates = [
        "composite_score",
        "score",
        "rank",
        "rating",
        "quality_score",
        "pricescore",
    ]
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return None


def prepare_top_scan_table(df, top_n=100):
    if df.empty:
        return df

    df = df.copy()
    name_col = detect_scan_name_column(df)
    score_col = detect_scan_score_column(df)

    if score_col and score_col in df.columns:
        df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
        df = df.sort_values(by=score_col, ascending=False, na_position="last")

    return df.head(top_n)


# =========================
# DECISION ENGINE
# =========================

def classify_market_regime(vix, oil, us10y, breadth, hy_oas):
    risk_score = 0

    if vix >= 30:
        risk_score += 2
    elif vix >= 22:
        risk_score += 1

    if oil >= 95:
        risk_score += 1

    if us10y >= 4.75:
        risk_score += 1

    if breadth <= 50:
        risk_score += 2
    elif breadth <= 60:
        risk_score += 1

    if hy_oas >= 5.0:
        risk_score += 2
    elif hy_oas >= 4.0:
        risk_score += 1

    if risk_score >= 6:
        return "Risk-Off", risk_score
    if risk_score >= 3:
        return "Cautious", risk_score
    return "Constructive", risk_score


def build_ai_recommendation(
    regime,
    risk_score,
    portfolio_value,
    open_positions,
    net_profit,
    qualified_scan_names,
    top_names,
    satellite_target,
    nvda_multiplier,
):
    confidence = "Medium"
    call = "Watch / possible add"
    action_bias = "Selective adds acceptable."
    portfolio_posture = "Balanced"
    scan_reading = "Normal opportunity set."
    risk_note = "No major stress signal."
    sizing_note = f"Use gradual adds and keep NVDA dominance near {nvda_multiplier:.2f}x."
    next_step = "Review top-ranked scans before acting."

    if regime == "Risk-Off":
        confidence = "High"
        call = "Defend / reduce risk"
        action_bias = "Reduce weaker exposure, avoid broad new buying, preserve flexibility."
        portfolio_posture = "Defensive"
        scan_reading = "Scan opportunities are less important than protecting capital."
        risk_note = "Multiple macro stress indicators are elevated."
        sizing_note = "Only consider exceptional setups; default to smaller or no adds."
        next_step = "Review weakest open holdings and tighten risk controls."

    elif regime == "Cautious":
        confidence = "Medium"
        call = "Hold / selective add"
        action_bias = "Favor only strongest setups and keep adds measured."
        portfolio_posture = "Guarded"
        scan_reading = "There are opportunities, but quality matters more than quantity."
        risk_note = "Market conditions are mixed and deserve caution."
        sizing_note = "Smaller position sizes are appropriate."
        next_step = "Focus on top-ranked names and avoid lower-conviction entries."

    elif regime == "Constructive":
        confidence = "Medium"
        portfolio_posture = "Constructive"
        scan_reading = "Backdrop supports selective offense."
        risk_note = "Macro stress appears contained."

        if portfolio_value < satellite_target:
            call = "Build selectively"
            action_bias = "Add gradually toward target size while keeping quality high."
            next_step = "Use the scan list to identify the highest-quality incremental adds."
        elif open_positions >= 12:
            call = "Hold / optimize"
            action_bias = "Improve quality rather than expanding the portfolio too broadly."
            next_step = "Compare top scan names against weaker current holdings."
        else:
            call = "Watch / possible add"
            action_bias = "Selective new adds are acceptable."
            next_step = "Review the strongest few scan candidates for timing."

    profit_state = "Profitable" if net_profit >= 0 else "Underwater"
    top_names_text = ", ".join(top_names) if top_names else "N/A"

    return {
        "primary_call": call,
        "confidence": confidence,
        "market_regime": regime,
        "risk_score": risk_score,
        "action_bias": action_bias,
        "portfolio_posture": portfolio_posture,
        "scan_reading": scan_reading,
        "risk_note": risk_note,
        "sizing_note": sizing_note,
        "next_step": next_step,
        "profit_state": profit_state,
        "top_names_text": top_names_text,
        "qualified_scan_names": qualified_scan_names,
    }


# =========================
# APP
# =========================

st.set_page_config(page_title="Steve Dashboard", layout="wide")

st.title("Steve Dashboard")
st.caption("Live portfolio data from Google Sheets plus Wall Street scan output")

# =========================
# SIDEBAR
# =========================

st.sidebar.header("Market Sentiment Inputs")

vix = st.sidebar.number_input("VIX", min_value=0.0, value=22.0, step=0.1)
oil = st.sidebar.number_input("Oil (WTI)", min_value=0.0, value=72.0, step=0.1)
us10y = st.sidebar.number_input("US 10Y Yield (%)", min_value=0.0, value=4.30, step=0.01)
breadth = st.sidebar.number_input("Breadth (% above key trend)", min_value=0.0, max_value=100.0, value=58.0, step=1.0)
hy_oas = st.sidebar.number_input("HY OAS (%)", min_value=0.0, value=3.8, step=0.1)

st.sidebar.header("Portfolio Rules")

satellite_target = st.sidebar.number_input("Satellite target ($)", min_value=0.0, value=40000.0, step=1000.0)
nvda_multiplier = st.sidebar.number_input("NVDA dominance multiplier", min_value=0.1, value=1.75, step=0.05)

# =========================
# LOAD DATA
# =========================

try:
    df_portfolio = load_sheet_data(SHEET_ID, WORKSHEET_NAME, HEADER_ROW_INDEX)
    df_portfolio = clean_portfolio_dataframe(df_portfolio)
except Exception as e:
    st.error(f"Could not load worksheet '{WORKSHEET_NAME}': {e}")
    df_portfolio = pd.DataFrame()

df_scan = load_scan_data(SCAN_CSV_PATH)
df_scan_top = prepare_top_scan_table(df_scan, TOP_SCAN_LIMIT)

# =========================
# PORTFOLIO METRICS
# =========================

portfolio_value = 0.0
open_positions = 0
net_profit = 0.0

if not df_portfolio.empty:
    if "Market Value" in df_portfolio.columns:
        portfolio_value = df_portfolio["Market Value"].fillna(0).sum()

    if "Position Status" in df_portfolio.columns:
        open_positions = (
            df_portfolio["Position Status"]
            .astype(str)
            .str.lower()
            .eq("open")
            .sum()
        )

    if "Net Profit/Loss" in df_portfolio.columns:
        net_profit = df_portfolio["Net Profit/Loss"].fillna(0).sum()

qualified_scan_names = len(df_scan_top) if not df_scan_top.empty else 0
scan_name_col = detect_scan_name_column(df_scan_top)
top_names = []
if scan_name_col and not df_scan_top.empty:
    top_names = df_scan_top[scan_name_col].astype(str).head(3).tolist()

market_regime, risk_score = classify_market_regime(vix, oil, us10y, breadth, hy_oas)

decision = build_ai_recommendation(
    regime=market_regime,
    risk_score=risk_score,
    portfolio_value=portfolio_value,
    open_positions=open_positions,
    net_profit=net_profit,
    qualified_scan_names=qualified_scan_names,
    top_names=top_names,
    satellite_target=satellite_target,
    nvda_multiplier=nvda_multiplier,
)

# =========================
# TOP AI RECOMMENDATION BLOCK
# =========================

st.header("AI Recommendation")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Portfolio value", f"${portfolio_value:,.2f}")
m2.metric("Qualified scan names", f"{qualified_scan_names:,}")
m3.metric("This week's action", decision["primary_call"])
m4.metric("Market regime", decision["market_regime"])

st.info(
    f"**Primary call:** {decision['primary_call']}\n\n"
    f"**Confidence:** {decision['confidence']}\n\n"
    f"**Portfolio posture:** {decision['portfolio_posture']}\n\n"
    f"**Action bias:** {decision['action_bias']}\n\n"
    f"**Risk note:** {decision['risk_note']}\n\n"
    f"**Scan reading:** {decision['scan_reading']}\n\n"
    f"**Sizing note:** {decision['sizing_note']}\n\n"
    f"**Current profit state:** {decision['profit_state']}\n\n"
    f"**Top scan names:** {decision['top_names_text']}\n\n"
    f"**Next step:** {decision['next_step']}"
)

# =========================
# PORTFOLIO SECTION
# =========================

st.header("US Trades - USD")

if df_portfolio.empty:
    st.warning("Portfolio sheet loaded no usable rows.")
else:
    st.write(f"Rows loaded: {len(df_portfolio)}")

    c1, c2, c3 = st.columns(3)
    c1.metric("Portfolio Value", f"${portfolio_value:,.2f}")
    c2.metric("Open Positions", f"{open_positions:,}")
    c3.metric("Net Profit/Loss", f"${net_profit:,.2f}")

    preferred_cols = [
        "Trade Date",
        "Stock",
        "Company Name",
        "Position Status",
        "Quantity",
        "Purchase Price",
        "Latest Market Price",
        "Market Value",
        "Net Profit/Loss",
        "% Gain/Loss",
    ]
    available_cols = [c for c in preferred_cols if c in df_portfolio.columns]

    st.subheader("Portfolio Table")
    if available_cols:
        st.dataframe(df_portfolio[available_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(df_portfolio, use_container_width=True, hide_index=True)

# =========================
# WALL STREET SCAN SECTION
# =========================

st.header("Wall Street Scan")

if df_scan_top.empty:
    st.warning("weekly_scan_output.csv not found or empty.")
else:
    st.write(f"Showing top {len(df_scan_top)} scan rows")

    score_col = detect_scan_score_column(df_scan_top)

    col1, col2 = st.columns(2)

    with col1:
        st.metric("Qualified Scan Names", f"{len(df_scan_top):,}")

    with col2:
        st.metric("Top Names", decision["top_names_text"])

    if score_col and scan_name_col and score_col in df_scan_top.columns:
        try:
            scan_chart = df_scan_top[[scan_name_col, score_col]].copy()
            scan_chart[score_col] = pd.to_numeric(scan_chart[score_col], errors="coerce")
            scan_chart = scan_chart.dropna(subset=[score_col]).head(15)

            if not scan_chart.empty:
                st.subheader("Top Scan Scores")
                st.bar_chart(scan_chart.set_index(scan_name_col)[score_col])
        except Exception:
            pass

    st.subheader("Wall Street Scan Table")
    st.dataframe(df_scan_top, use_container_width=True, hide_index=True)

# =========================
# DEBUG
# =========================

with st.expander("Detected Portfolio Columns"):
    if df_portfolio.empty:
        st.write("No portfolio columns detected.")
    else:
        st.write(df_portfolio.columns.tolist())

with st.expander("Detected Scan Columns"):
    if df_scan.empty:
        st.write("No scan columns detected.")
    else:
        st.write(df_scan.columns.tolist())