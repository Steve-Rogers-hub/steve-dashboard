import os
import json
import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SHEET_ID = "1CL5Rs7eMU4M0K5pgrKYVb7tUhJVuXn8fDIax9L0_31k"
WORKSHEET_NAME = "US Trades - USD"
HEADER_ROW_INDEX = 8

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


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
def load_sheet_data(sheet_id: str, worksheet_name: str, header_row_index: int) -> pd.DataFrame:
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
        col_name = h.strip() if h else f"Column_{i+1}"
        if col_name in seen:
            seen[col_name] += 1
            col_name = f"{col_name}_{seen[col_name]}"
        else:
            seen[col_name] = 1
        clean_headers.append(col_name)

    df = pd.DataFrame(rows, columns=clean_headers)

    # Remove fully empty rows and columns
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df = df.fillna("")

    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

    return df


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

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
        "Total Return",
        "Trade Brokerage",
        "Total Sale Proceeds",
        "Gross Realised Profit/Loss",
        "Net Realised Profit/Loss",
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

    return df


def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    date_cols = [
        "Trade Date",
        "Sold Date",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    return df


st.set_page_config(page_title="Steve Dashboard", layout="wide")

st.title("Steve Dashboard")
st.caption("Live data from Google Sheets")

try:
    df_us = load_sheet_data(
        sheet_id=SHEET_ID,
        worksheet_name=WORKSHEET_NAME,
        header_row_index=HEADER_ROW_INDEX,
    )
    df_us = clean_dataframe(df_us)
    df_us = convert_numeric_columns(df_us)
    df_us = convert_date_columns(df_us)
except Exception as e:
    st.error(f"Could not load worksheet '{WORKSHEET_NAME}': {e}")
    st.stop()

if df_us.empty:
    st.warning("The worksheet loaded successfully, but no data rows were found.")
    st.stop()

st.subheader("US Trades - USD")
st.write(f"Rows loaded: {len(df_us)}")
st.dataframe(df_us, use_container_width=True, hide_index=True)

st.subheader("Quick Summary")

total_rows = len(df_us)
total_columns = len(df_us.columns)

open_holdings = 0
if "Position Status" in df_us.columns:
    open_holdings = (df_us["Position Status"].astype(str).str.lower() == "open").sum()

portfolio_value = 0.0
if "Market Value" in df_us.columns:
    portfolio_value = df_us["Market Value"].fillna(0).sum()

net_profit_loss = 0.0
if "Net Profit/Loss" in df_us.columns:
    net_profit_loss = df_us["Net Profit/Loss"].fillna(0).sum()

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Rows", f"{total_rows:,}")

with col2:
    st.metric("Total Columns", f"{total_columns:,}")

with col3:
    st.metric("Open Holdings", f"{open_holdings:,}")

with col4:
    st.metric("Portfolio Value", f"${portfolio_value:,.2f}")

st.subheader("Profit Summary")
st.metric("Net Profit/Loss", f"${net_profit_loss:,.2f}")

st.subheader("Selected Columns")

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

available_cols = [col for col in preferred_cols if col in df_us.columns]

if available_cols:
    st.dataframe(df_us[available_cols], use_container_width=True, hide_index=True)
else:
    st.info("None of the preferred columns were found exactly as named in the worksheet.")

st.subheader("Detected Columns")
st.write(df_us.columns.tolist())