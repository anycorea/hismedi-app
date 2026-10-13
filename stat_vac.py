import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, date

# -----------------------------------------------------------------------------
# 1. Page Configuration & Layout
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="예방접종 현황 조회 및 통계",
    page_icon="💉",
    layout="wide"
)

# -----------------------------------------------------------------------------
# 2. Google Sheets Authentication & Data Load
# -----------------------------------------------------------------------------
SPREADSHEET_ID = "1a4TtjVYZDKOGetmfawS7VtTmMfAhBKRyXMY-SX4tsRU"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

@st.cache_resource(ttl=3600)
def init_gspread():
    """st.secrets["gspread_auth"] 기반 서비스 계정 인증"""
    if "gspread_auth" not in st.secrets:
        st.error("`st.secrets['gspread_auth']` 설정이 존재하지 않습니다. Streamlit Secrets를 확인해주세요.")
        st.stop()
        
    creds_dict = dict(st.secrets["gspread_auth"])
    credentials = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(credentials)
    return client

@st.cache_data(ttl=300)
def load_sheets_data():
    """vac, std 시트 데이터 로드 및 병합"""
    try:
        client = init_gspread()
        doc = client.open_by_key(SPREADSHEET_ID)

        # vac 시트 및 std 시트 가져오기
        vac_sheet = doc.worksheet("vac")
        std_sheet = doc.worksheet("std")

        df_vac = pd.DataFrame(vac_sheet.get_all_records())
        df_std = pd.DataFrame(std_sheet.get_all_records())

        # 열 이름 공백 제거
        df_vac.columns = df_vac.columns.str.strip()
        df_std.columns = df_std.columns.str.strip()

        # 데이터 병합 (처방코드 기준)
        if "처방코드" in df_vac.columns and "처방코드" in df_std.columns:
            # std 시트에서 처방코드 및 예방접종명(표준명) 선택
            target_std_col = "예방접종명(표준명)" if "예방접종명(표준명)" in df_std.columns else df_std.columns[-1]
            df_std_sub = df_std[["처방코드", target_std_col]].drop_duplicates(subset=["처방코드"])

            df_merged = pd.merge(df_vac, df_std_sub, on="처방코드", how="left")
            
            # 매핑되지 않은 경우 기존 처방명으로 대체
            df_merged[target_std_col] = df_merged[target_std_col].fillna(df_merged["처방명"])
            df_merged.rename(columns={target_std_col: "예방접종명(표준명)"}, inplace=True)
        else:
            df_merged = df_vac.copy()
            if "예방접종명(표준명)" not in df_merged.columns:
                df_merged["예방접종명(표준명)"] = df_merged.get("처방명", "미분류")

        # 접수일자 datetime 포맷 변환
        if "접수일자" in df_merged.columns:
            df_merged["접수일자_dt"] = pd.to_datetime(df_merged["접수일자"].astype(str), errors="coerce")
            df_merged["접수일자_str"] = df_merged["접수일자_dt"].dt.strftime("%Y-%m-%d")

        return df_merged

    except Exception as e:
        st.error(f"구글 시트 데이터를 불러오는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 3. Main Application Flow
# -----------------------------------------------------------------------------
st.title("💉 예방접종 접종 내역 및 통계 조회")
st.caption("구글 시트('vac', 'std') 데이터 연동 시스템")

with st.spinner("구글 시트 데이터를 동기화 중입니다..."):
    df_raw = load_sheets_data()

if df_raw.empty:
    st.warning("불러올 수 있는 데이터가 없거나 시트 구조를 확인해야 합니다.")
    st.stop()

# -----------------------------------------------------------------------------
# 4. Sidebar Filters
# -----------------------------------------------------------------------------
st.sidebar.header("🔍 조회 조건 필터")

# 4-1. 날짜 범위 필터
valid_dates = df_raw["접수일자_dt"].dropna()
if not valid_dates.empty:
    min_date = valid_dates.min().date()
    max_date = valid_dates.max().date()
else:
    min_date = date.today()
    max_date = date.today()

date_range = st.sidebar.date_input(
    "접수일자 선택",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# 4-2. 예방접종명(표준명) Multi-Select
std_list = sorted(list(df_raw["예방접종명(표준명)"].dropna().unique()))
selected_std = st.sidebar.multiselect(
    "예방접종명(표준명)",
    options=std_list,
    default=std_list
)

# 4-3. 과목명 필터 (존재할 경우)
if "과목명" in df_raw.columns:
    dept_list = sorted(list(df_raw["과목명"].dropna().unique()))
    selected_dept = st.sidebar.multiselect(
        "과목명",
        options=dept_list,
        default=dept_list
    )
else:
    selected_dept = None

# -----------------------------------------------------------------------------
# 5. Data Filtering
# -----------------------------------------------------------------------------
filtered_df = df_raw.copy()

# 날짜 필터링
if "접수일자_dt" in filtered_df.columns:
    filtered_df = filtered_df[
        (filtered_df["접수일자_dt"].dt.date >= start_date) &
        (filtered_df["접수일자_dt"].dt.date <= end_date)
    ]

# 표준명 필터링
if selected_std:
    filtered_df = filtered_df[filtered_df["예방접종명(표준명)"].isin(selected_std)]

# 과목명 필터링
if selected_dept and "과목명" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["과목명"].isin(selected_dept)]

# -----------------------------------------------------------------------------
# 6. Dashboards & Metrics
# -----------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("총 접종 건수", f"{len(filtered_df):,} 건")

with col2:
    patient_cnt = filtered_df["차트번호"].nunique() if "차트번호" in filtered_df.columns else 0
    st.metric("실 환자 수", f"{patient_cnt:,} 명")

with col3:
    std_cnt = filtered_df["예방접종명(표준명)"].nunique()
    st.metric("선택 백신 종류", f"{std_cnt:,} 개")

with col4:
    doc_cnt = filtered_df["주치의"].nunique() if "주치의" in filtered_df.columns else 0
    st.metric("참여 주치의 수", f"{doc_cnt:,} 명")

st.divider()

# -----------------------------------------------------------------------------
# 7. Main Data Table & CSV Download
# -----------------------------------------------------------------------------
st.subheader("📋 접종 내역 상세 조회")

# 표시용 컬럼 정렬
display_cols = [col for col in [
    "접수일자_str", "과목명", "주치의", "차트번호", "환자명", 
    "성별", "나이", "처방코드", "처방명", "예방접종명(표준명)"
] if col in filtered_df.columns]

output_df = filtered_df[display_cols].rename(columns={"접수일자_str": "접수일자"})

st.dataframe(output_df, use_container_width=True, hide_index=True)

# CSV 다운로드 버튼
csv_data = output_df.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    label="📥 조회 결과 CSV 다운로드",
    data=csv_data,
    file_name=f"stat_vac_{start_date}_{end_date}.csv",
    mime="text/csv"
)

# -----------------------------------------------------------------------------
# 8. Charts & Aggregation Analysis
# -----------------------------------------------------------------------------
st.divider()
st.subheader("📊 예방접종(표준명)별 집계 통계")

if not filtered_df.empty:
    chart_col1, chart_col2 = st.columns([1, 1])

    with chart_col1:
        summary_df = (
            filtered_df.groupby("예방접종명(표준명)")
            .size()
            .reset_index(name="접종 건수")
            .sort_values(by="접종 건수", ascending=False)
        )
        st.write("**백신 종류별 접종 건수**")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

    with chart_col2:
        st.write("**백신 종류별 비율 차트**")
        st.bar_chart(summary_df.set_index("예방접종명(표준명)"))
else:
    st.info("선택한 조건에 일치하는 데이터가 없습니다.")
