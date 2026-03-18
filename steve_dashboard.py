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
        raise ValueError(f"Not enough rows for header_row_index={header_row_index}")

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
        else:
            df[col] = df[col]

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
    candidates = [
        "ticker",
        "symbol",
        "stock",
        "name",
        "company",
        "scan_name",
    ]
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return df.columns[0] if len(df.columns) > 0 else None


def detect_scan_score_column(df):
    candidates = [
        "score",
        "composite_score",
        "rank",
        "rating",
        "quality_score",
    ]
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c in lower_map:
            return lower_map[c]
    return None


# =========================
# APP
# =========================

st.set_page_config(page_title="Steve Dashboard", layout="wide")

st.title("Steve Dashboard")
st.caption("Live portfolio data from Google Sheets plus Wall Street scan output")

# Load portfolio
try:
    df_portfolio = load_sheet_data(SHEET_ID, WORKSHEET_NAME, HEADER_ROW_INDEX)
    df_portfolio = clean_portfolio_dataframe(df_portfolio)
except Exception as e:
    st.error(f"Could not load worksheet '{WORKSHEET_NAME}': {e}")
    df_portfolio = pd.DataFrame()

# Load scan
df_scan = load_scan_data(SCAN_CSV_PATH)

# =========================
# PORTFOLIO SECTION
# =========================

st.header("US Trades - USD")

if df_portfolio.empty:
    st.warning("Portfolio sheet loaded no usable rows.")
else:
    st.write(f"Rows loaded: {len(df_portfolio)}")

    portfolio_value = 0.0
    if "Market Value" in df_portfolio.columns:
        portfolio_value = df_portfolio["Market Value"].fillna(0).sum()

    open_positions = 0
    if "Position Status" in df_portfolio.columns:
        open_positions = (
            df_portfolio["Position Status"]
            .astype(str)
            .str.lower()
            .eq("open")
            .sum()
        )

    net_profit = 0.0
    if "Net Profit/Loss" in df_portfolio.columns:
        net_profit = df_portfolio["Net Profit/Loss"].fillna(0).sum()

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

if df_scan.empty:
    st.warning("weekly_scan_output.csv not found or empty.")
else:
    st.write(f"Scan rows loaded: {len(df_scan)}")

    name_col = detect_scan_name_column(df_scan)
    score_col = detect_scan_score_column(df_scan)

    col1, col2 = st.columns(2)

    with col1:
        st.metric("Qualified Scan Names", f"{len(df_scan):,}")

    with col2:
        if name_col and len(df_scan) > 0:
            top_names = ", ".join(df_scan[name_col].astype(str).head(3).tolist())
            st.metric("Top Names", top_names if top_names else "N/A")
        else:
            st.metric("Top Names", "N/A")

    if score_col and score_col in df_scan.columns:
        try:
            scan_chart = df_scan[[name_col, score_col]].copy()
            scan_chart[score_col] = pd.to_numeric(scan_chart[score_col], errors="coerce")
            scan_chart = scan_chart.dropna(subset=[score_col]).head(15)

            if not scan_chart.empty and name_col:
                st.subheader("Top Scan Scores")
                st.bar_chart(scan_chart.set_index(name_col)[score_col])
        except Exception:
            pass

    st.subheader("Wall Street Scan Table")
    st.dataframe(df_scan, use_container_width=True, hide_index=True)

# =========================
# DEBUG SECTION
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