import streamlit as st
import pandas as pd
from supabase import create_client, Client

st.set_page_config(page_title="처방 조회 시스템", layout="wide")

# Supabase 연결
url = st.secrets["SUPABASE_URL"]
key = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(url, key)

st.title("💊 처방 데이터 조회")

# 데이터 로드
@st.cache_data(ttl=600)
def load_data():
    data = supabase.table("prescriptions").select("*").execute()
    return pd.DataFrame(data.data)

df = load_data()

# 필터 UI
col1, col2, col3, col4 = st.columns(4)
with col1:
    sel_diagnosis = st.selectbox("진단", ["전체"] + sorted(df["진단"].dropna().unique().tolist()))
with col2:
    sel_date = st.date_input("진료일", value=None)
with col3:
    sel_patient = st.text_input("환자번호 (일부 검색 가능)")
with col4:
    sel_category = st.selectbox("처방구분", ["전체"] + sorted(df["처방구분"].dropna().unique().tolist()))

# 필터 적용
filtered = df.copy()
if sel_diagnosis != "전체":
    filtered = filtered[filtered["진단"] == sel_diagnosis]
if sel_category != "전체":
    filtered = filtered[filtered["처방구분"] == sel_category]
if sel_date:
    filtered = filtered[filtered["진료일"] == pd.to_datetime(sel_date)]
if sel_patient:
    filtered = filtered[filtered["환자번호"].str.contains(sel_patient, case=False)]

st.markdown(f"🔍 검색 결과: {len(filtered)}건")
st.dataframe(filtered, use_container_width=True, height=600)

# 다운로드
st.download_button(
    "⬇️ 엑셀 다운로드",
    data=filtered.to_csv(index=False).encode("utf-8-sig"),
    file_name="prescriptions_filtered.csv",
    mime="text/csv"
)
