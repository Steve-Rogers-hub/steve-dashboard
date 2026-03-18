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
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if not creds_json:
        st.error("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable.")
        st.stop()

    try:
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
    df = df.replace("", pd.NA).dropna(how="all").fillna("")

    return df

def clean_us_trades_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    df.columns = [str(col).strip() for col in df.columns]

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

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
    df_us = clean_us_trades_dataframe(df_us)
except Exception as e:
    st.error(f"Could not load worksheet '{WORKSHEET_NAME}': {e}")
    st.stop()

if df_us.empty:
    st.warning("The worksheet loaded successfully, but no data rows were found.")
    st.stop()

st.subheader("US Trades - USD")
st.write(f"Rows loaded: {len(df_us)}")
st.dataframe(df_us, use_container_width=True, hide_index=True)