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
def load_sheet_data(sheet_id, worksheet_name, header_row_index):
    client = get_gspread_client()
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_values()

    headers = data[header_row_index]
    rows = data[header_row_index + 1:]

    # Clean headers
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

    # Clean data
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")

    return df


def clean_and_format(df):
    df = df.copy()

    # Remove spreadsheet errors
    df = df.replace("#REF!", pd.NA)
    df = df.replace("#VALUE!", pd.NA)

    # Numeric cleanup
    numeric_cols = [
        "Quantity",
        "Purchase Price",
        "Buy Cost",
        "Buy Brokerage",
        "Total Purchase Costs",
        "Latest Market Price",
        "Market Value",
        "Net Profit/Loss",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace("$", "")
                .str.replace(",", "")
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date formatting
    if "Trade Date" in df.columns:
        df["Trade Date"] = pd.to_datetime(df["Trade Date"], errors="coerce")
        df["Trade Date"] = df["Trade Date"].dt.strftime("%d-%b-%Y")

    return df


# ---------------- UI ----------------

st.set_page_config(page_title="Steve Dashboard", layout="wide")

st.title("Steve Dashboard")
st.caption("Live data from Google Sheets")

df = load_sheet_data(SHEET_ID, WORKSHEET_NAME, HEADER_ROW_INDEX)
df = clean_and_format(df)

st.subheader("US Trades - USD")
st.write(f"Rows loaded: {len(df)}")

st.dataframe(df, use_container_width=True)

# -------- Summary --------

st.subheader("Quick Summary")

portfolio_value = df["Market Value"].sum() if "Market Value" in df.columns else 0
open_positions = (
    (df["Position Status"].str.lower() == "open").sum()
    if "Position Status" in df.columns
    else 0
)

col1, col2 = st.columns(2)

col1.metric("Portfolio Value", f"${portfolio_value:,.2f}")
col2.metric("Open Positions", open_positions)

# -------- Debug --------

st.subheader("Detected Columns")
st.write(df.columns.tolist())