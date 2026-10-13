from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st

# 페이지 설정
st.set_page_config(
    page_title="예방접종 현황 조회 앱", page_icon="💉", layout="wide"
)

# 구글 시트 ID 설정 (시크릿 또는 직접 지정)
SHEET_ID = st.secrets.get("gsheet", {}).get(
    "spreadsheet_id", "1a4TtjVYZDKOGetmfawS7VtTmMfAhBKRyXMY-SX4tsRU"
)


@st.cache_data(ttl=600)
def load_data_from_gsheet():
  """gspread 및 st.secrets의 [gcp_service_account]를 사용하여 vac 및 std 시트 데이터를 불러옵니다."""
  scope = [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/drive",
  ]

  # secrets.toml의 [gcp_service_account] 섹션을 딕셔너리로 가져옴
  creds_dict = dict(st.secrets["gcp_service_account"])
  credentials = Credentials.from_service_account_info(
      creds_dict, scopes=scope
  )
  gc = gspread.authorize(credentials)

  # 스프레드시트 열기
  spreadsheet = gc.open_by_key(SHEET_ID)

  # 1. vac 시트 데이터 로드
  ws_vac = spreadsheet.worksheet("vac")
  df_vac = pd.DataFrame(ws_vac.get_all_records())

  # 2. std 시트 데이터 로드
  ws_std = spreadsheet.worksheet("std")
  df_std = pd.DataFrame(ws_std.get_all_records())

  return df_vac, df_std


# 제목
st.title("💉 예방접종 접종 내역 조회 및 관리 앱")
st.markdown(
    "구글 시트(`vac`, `std`) 데이터를 기반으로 기간별·예방접종 표준명별 접종"
    " 내역을 조회합니다."
)

# 데이터 로드
with st.spinner("구글 시트에서 데이터를 불러오는 중입니다..."):
  try:
    df_vac, df_std = load_data_from_gsheet()
  except Exception as e:
    st.error(
        f"구글 시트 연동 중 오류가 발생했습니다. secrets 설정을 확인해주세요."
        f" 오류: {e}"
    )
    st.stop()

if df_vac is not None and df_std is not None:
  # 데이터 병합 (처방코드 기준 표준명 매핑)
  if "처방코드" in df_vac.columns and "처방코드" in df_std.columns:
    # 표준명 컬럼 찾기 (예방접종명(표준명) 또는 유사 컬럼)
    std_col_candidates = [
        col for col in df_std.columns if "표준명" in col or "예방접종명" in col
    ]
    std_col_name = std_col_candidates[0] if std_col_candidates else "예방접종명(표준명)"

    df_merged = pd.merge(
        df_vac,
        df_std[["처방코드", std_col_name]],
        on="처방코드",
        how="left",
    )
    # 표준명 컬럼명 통일
    if std_col_name != "예방접종명(표준명)":
      df_merged.rename(
          columns={std_col_name: "예방접종명(표준명)"}, inplace=True
      )
  else:
    st.error("시트 내에 '처방코드' 컬럼이 존재하지 않습니다.")
    st.stop()

  # 날짜 형식 변환
  if "접수일자" in df_merged.columns:
    df_merged["접수일자"] = pd.to_datetime(
        df_merged["접수일자"], errors="coerce"
    ).dt.date

  # --- 사이드바 필터 설정 ---
  st.sidebar.header("🔍 조회 필터")

  # 1. 기간 선택 필터
  if "접수일자" in df_merged.columns and not df_merged["접수일자"].isna().all():
    min_date = df_merged["접수일자"].min()
    max_date = df_merged["접수일자"].max()

    start_date, end_date = st.sidebar.date_input(
        "조회 기간 선택",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
  else:
    start_date, end_date = None, None

  # 2. 예방접종명(표준명) 멀티 선택 필터
  target_col = (
      "예방접종명(표준명)"
      if "예방접종명(표준명)" in df_merged.columns
      else df_merged.columns[-1]
  )
  unique_std_names = sorted(df_merged[target_col].dropna().unique().tolist())
  selected_std_names = st.sidebar.multiselect(
      "예방접종명(표준명) 선택",
      options=unique_std_names,
      default=unique_std_names,
  )

  # 3. 과목명 필터
  if "과목명" in df_merged.columns:
    unique_departments = sorted(df_merged["과목명"].dropna().unique().tolist())
    selected_departments = st.sidebar.multiselect(
        "과목명 선택", options=unique_departments, default=unique_departments
    )
  else:
    selected_departments = []

  # --- 필터 적용 ---
  filtered_df = df_merged.copy()

  if start_date and end_date:
    filtered_df = filtered_df[
        (filtered_df["접수일자"] >= start_date)
        & (filtered_df["접수일자"] <= end_date)
    ]

  if selected_std_names:
    filtered_df = filtered_df[filtered_df[target_col].isin(selected_std_names)]

  if selected_departments and "과목명" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["과목명"].isin(selected_departments)]

  # --- 메인 화면 출력 ---
  col1, col2, col3 = st.columns(3)
  with col1:
    st.metric(label="총 조회 건수", value=f"{len(filtered_df):,} 건")
  with col2:
    st.metric(
        label="접종 환자 수 (중복제거)",
        value=(
            f"{filtered_df['차트번호'].nunique():,}"
            if "차트번호" in filtered_df.columns
            else "-"
        ),
    )
  with col3:
    st.metric(
        label="선택된 표준 백신 수",
        value=f"{filtered_df[target_col].nunique():,} 개",
    )

  st.divider()

  # 데이터 프레임 출력
  st.subheader("📋 예방접종 접종 목록")
  st.dataframe(filtered_df, use_container_width=True)

  # 데이터 다운로드 버튼
  csv_data = filtered_df.to_csv(index=False).encode("utf-8-sig")
  st.download_button(
      label="📥 조회 결과 CSV 다운로드",
      data=csv_data,
      file_name=f"vaccine_report_{datetime.now().strftime('%Y%m%d')}.csv",
      mime="text/csv",
  )

  # --- 통계 차트 ---
  st.divider()
  st.subheader("📊 표준 예방접종별 접종 현황 통계")
  if not filtered_df.empty:
    chart_data = (
        filtered_df[target_col].value_counts().reset_index(name="접종 건수")
    )
    chart_data.columns = ["예방접종명(표준명)", "접종 건수"]
    st.bar_chart(chart_data.set_index("예방접종명(표준명)"))
  else:
    st.info("조건에 맞는 데이터가 없습니다.")
