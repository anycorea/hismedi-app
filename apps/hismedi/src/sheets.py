import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

SCOPE = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def _client():
    info = st.secrets["gcp_service_account"]
    creds = Credentials.from_service_account_info(info, scopes=SCOPE)
    return gspread.authorize(creds)

def _load_worksheet_df(sheet_name: str) -> pd.DataFrame:
    sh = _client().open_by_key(st.secrets["sheet_id"])
    ws = sh.worksheet(sheet_name)
    values = ws.get_all_values()
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header)
    return df

@st.cache_data(ttl=3600)
def load_departments():
    df = _load_worksheet_df("departments")
    df["dept_id"] = pd.to_numeric(df["dept_id"], errors="coerce")
    df = df[df["is_active"].str.upper() == "TRUE"]
    return df

@st.cache_data(ttl=3600)
def load_doctors():
    df = _load_worksheet_df("doctors")
    df["doctor_id"] = pd.to_numeric(df["doctor_id"], errors="coerce")
    df["dept_id"] = pd.to_numeric(df["dept_id"], errors="coerce")
    df = df[df["is_active"].str.upper() == "TRUE"]

    dep = load_departments()[["dept_id", "dept_name", "dept_reservation_url"]]
    df = df.merge(dep, on="dept_id", how="left")
    return df
