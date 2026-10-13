from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st

# 페이지 설정 (와이드 레이아웃 적용)
st.set_page_config(
    page_title="히즈메디병원 예방접종 통계", page_icon="💉", layout="wide"
)

# 커스텀 CSS로 UI 세련미 강화
st.markdown(
    """
    <style>
        .main-title { font-size: 26px; font-weight: bold; color: #1f77b4; margin-bottom: 0px; }
        .sub-title { font-size: 14px; color: #666; margin-bottom: 20px; }
        .metric-card { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #1f77b4; }
    </style>
""",
    unsafe_allow_html=True,
)

# 구글 시트 ID 설정
SHEET_ID = st.secrets.get("gsheet", {}).get(
    "spreadsheet_id", "1a4TtjVYZDKOGetmfawS7VtTmMfAhBKRyXMY-SX4tsRU"
)


@st.cache_data(ttl=600)
def load_data_from_gsheet():
  """구글 시트에서 vac 및 std 데이터 로드"""
  scope = [
      "https://www.googleapis.com/auth/spreadsheets",
      "https://www.googleapis.com/auth/drive",
  ]
  creds_dict = dict(st.secrets["gcp_service_account"])
  credentials = Credentials.from_service_account_info(
      creds_dict, scopes=scope
  )
  gc = gspread.authorize(credentials)
  spreadsheet = gc.open_by_key(SHEET_ID)

  ws_vac = spreadsheet.worksheet("vac")
  df_vac = pd.DataFrame(ws_vac.get_all_records())

  ws_std = spreadsheet.worksheet("std")
  df_std = pd.DataFrame(ws_std.get_all_records())

  return df_vac, df_std


# 데이터 로드
with st.spinner("데이터를 안전하게 불러오는 중입니다..."):
  try:
    df_vac, df_std = load_data_from_gsheet()
  except Exception as e:
    st.error(f"구글 시트 연동 오류: {e}")
    st.stop()

# 데이터 전처리 및 병합
if "처방코드" in df_vac.columns and "처방코드" in df_std.columns:
  std_col_candidates = [
      col for col in df_std.columns if "표준명" in col or "예방접종명" in col
  ]
  std_col_name = std_col_candidates[0] if std_col_candidates else "예방접종명(표준명)"

  df_merged = pd.merge(
      df_vac, df_std[["처방코드", std_col_name]], on="처방코드", how="left"
  )
  if std_col_name != "예방접종명(표준명)":
    df_merged.rename(
        columns={std_col_name: "예방접종명(표준명)"}, inplace=True
    )
else:
  st.error("시트 내에 '처방코드' 컬럼이 존재하지 않습니다.")
  st.stop()

# 날짜 형식 변환 및 연/월 파생 변수 생성
if "접수일자" in df_merged.columns:
  df_merged["접수일자"] = pd.to_datetime(
      df_merged["접수일자"], errors="coerce"
  ).dt.date
  df_merged["연도"] = df_merged["접수일자"].apply(
      lambda x: x.year if pd.notnull(x) else None
  )
  df_merged["월"] = df_merged["접수일자"].apply(
      lambda x: x.strftime("%Y-%m") if pd.notnull(x) else None
  )

target_col = "예방접종명(표준명)"
if target_col not in df_merged.columns:
  df_merged[target_col] = df_merged["처방명"]

# ==========================================
# 🛠️ [왼쪽 메뉴] 필터 및 설정 영역
# ==========================================
st.sidebar.markdown(
    '<p class="main-title">💉 히즈메디병원</p>', unsafe_allow_html=True
)
st.sidebar.markdown(
    '<p class="sub-title">예방접종 통계 및 분석 시스템</p>', unsafe_allow_html=True
)
st.sidebar.divider()

st.sidebar.header("📅 기간 설정")
period_mode = st.sidebar.radio(
    "기간 단위 선택", ["선택 기간", "일별", "월별", "연도별"]
)

min_date = (
    df_merged["접수일자"].min()
    if not df_merged["접수일자"].isna().all()
    else datetime.today().date()
)
max_date = (
    df_merged["접수일자"].max()
    if not df_merged["접수일자"].isna().all()
    else datetime.today().date()
)

if period_mode == "선택 기간":
  start_date, end_date = st.sidebar.date_input(
      "조회 기간", value=(min_date, max_date), min_value=min_date, max_value=max_date
  )
elif period_mode == "월별":
  all_months = sorted(df_merged["월"].dropna().unique().tolist())
  selected_month = st.sidebar.selectbox(
      "조회 월 선택", all_months, index=len(all_months) - 1 if all_months else 0
  )
  # 해당 월의 시작일/마지막일 계산
  start_date = datetime.strptime(selected_month + "-01", "%Y-%m-%d").date()
  if start_date.month == 12:
    end_date = datetime(start_date.year + 1, 1, 1).date() - pd.Timedelta(days=1)
  else:
    end_date = (
        datetime(start_date.year, start_date.month + 1, 1).date()
        - pd.Timedelta(days=1)
    )
elif period_mode == "연도별":
  all_years = sorted(df_merged["연도"].dropna().unique().tolist())
  selected_year = st.sidebar.selectbox(
      "조회 연도 선택", all_years, index=len(all_years) - 1 if all_years else 0
  )
  start_date = datetime(selected_year, 1, 1).date()
  end_date = datetime(selected_year, 12, 31).date()
else:  # 일별
  selected_day = st.sidebar.date_input("조회 일자 선택", value=max_date)
  start_date, end_date = selected_day, selected_day

st.sidebar.divider()

# --- 진료과 선택 편의 기능 (전체선택/해제 지원) ---
st.sidebar.header("🏥 진료과 선택")
unique_depts = sorted(df_merged["과목명"].dropna().unique().tolist())
dept_select_all = st.sidebar.checkbox("진료과 전체 선택", value=True)

if dept_select_all:
  selected_depts = st.sidebar.multiselect(
      "진료과", options=unique_depts, default=unique_depts
  )
else:
  selected_depts = st.sidebar.multiselect("진료과", options=unique_depts, default=[])

st.sidebar.divider()

# --- 표준 예방접종명 선택 편의 기능 (전체선택/해제 지원) ---
st.sidebar.header("💉 예방접종 표준명 선택")
unique_vax = sorted(df_merged[target_col].dropna().unique().tolist())
vax_select_all = st.sidebar.checkbox("예방접종 표준명 전체 선택", value=True)

if vax_select_all:
  selected_vax = st.sidebar.multiselect(
      "예방접종 표준명", options=unique_vax, default=unique_vax
  )
else:
  selected_vax = st.sidebar.multiselect(
      "예방접종 표준명", options=unique_vax, default=[]
  )

# ==========================================
# 📊 [메인 화면] 데이터 필터링 및 통계 시각화
# ==========================================
filtered_df = df_merged.copy()

# 날짜 필터 적용
if period_mode == "일별":
  filtered_df = filtered_df[filtered_df["접수일자"] == start_date]
else:
  filtered_df = filtered_df[
      (filtered_df["접수일자"] >= start_date)
      & (filtered_df["접수일자"] <= end_date)
  ]

# 진료과 및 표준명 필터 적용
if selected_depts:
  filtered_df = filtered_df[filtered_df["과목명"].isin(selected_depts)]
if selected_vax:
  filtered_df = filtered_df[filtered_df[target_col].isin(selected_vax)]

# 타이틀 및 개요
st.title("📊 히즈메디병원 예방접종 통계 대시보드")
st.markdown(
    f"**조회 기간:** `{start_date}` ~ `{end_date}` | **선택된 진료과:**"
    f" `{len(selected_depts)}개` | **선택된 백신 표준명:**"
    f" `{len(selected_vax)}개`"
)

# 주요 지표 카드
m1, m2, m3, m4 = st.columns(4)
with m1:
  st.metric(label="총 접종 건수", value=f"{len(filtered_df):,} 건")
with m2:
  unique_patients = (
      filtered_df["차트번호"].nunique() if "차트번호" in filtered_df.columns else 0
  )
  st.metric(label="접종 환자 수 (중복제거)", value=f"{unique_patients:,} 명")
with m3:
  active_vax_count = filtered_df[target_col].nunique()
  st.metric(label="시행된 백신 종류", value=f"{active_vax_count:,} 종")
with m4:
  active_dept_count = (
      filtered_df["과목명"].nunique() if "과목명" in filtered_df.columns else 0
  )
  st.metric(label="참여 진료과 수", value=f"{active_dept_count:,} 개")

st.divider()

# 탭 구성 (1. 통계 및 보고서, 2. 상세 접종 내역)
tab1, tab2 = st.tabs(
    ["📈 예방접종 통계 및 보고서 미리보기", "📋 개인별 상세 접종 내역 조회"]
)

with tab1:
  st.subheader("🏆 표준 예방접종별 접종 순위 통계 (많은 순)")

  if not filtered_df.empty:
    # 많은 순으로 정렬된 통계 테이블 생성
    stat_table = (
        filtered_df[target_col]
        .value_counts()
        .reset_index(name="접종 건수")
    )
    stat_table.columns = ["예방접종명 (표준명)", "접종 건수"]
    stat_table["점유율(%)"] = (
        stat_table["접종 건수"] / stat_table["접종 건수"].sum() * 100
    ).round(2)

    c_left, c_right = st.columns([1.2, 1])

    with c_left:
      st.dataframe(
          stat_table, use_container_width=True, hide_index=True, height=400
      )

    with c_right:
      # 바 차트 시각화 (많은 순 정렬)
      st.bar_chart(
          stat_table.set_index("예방접종명 (표준명)")["접종 건수"], height=400
      )

    st.divider()

    # --- 보고서 출력(미리보기) 영역 ---
    st.subheader("📑 병원 제출용 보고서 미리보기")
    st.markdown(
        "아래 요약된 통계표는 공식 보고서 형태로 확인 및 인쇄하실 수"
        " 있습니다."
    )

    report_container = st.container()
    with report_container:
      st.markdown(
          f"<h3 style='text-align: center;'>[ 히즈메디병원 예방접종 현황 통계 보고서"
          f" ]</h3>",
          unsafe_allow_html=True,
      )
      st.markdown(
          f"**- 보고 기간:** {start_date} ~ {end_date} &nbsp;&nbsp;|&nbsp;&nbsp;"
          f" **- 총 접종 건수:** {len(filtered_df):,}건 &nbsp;&nbsp;|&nbsp;&nbsp;"
          f" **- 발행일시:** {datetime.now().strftime('%Y-%m-%d %H:%M')}"
      )
      st.dataframe(stat_table, use_container_width=True, hide_index=True)

    # 인쇄 안내 및 버튼
    col_p1, col_p2 = st.columns([1, 4])
    with col_p1:
      if st.button("🖨️ 보고서 인쇄 / PDF 저장"):
        st.markdown(
            "<script>window.print();</script>", unsafe_allow_html=True
        )
        st.info(
            "브라우저 인쇄 창이 호출되었습니다. 대상 프린터 또는 'PDF로"
            " 저장'을 선택하세요."
        )

  else:
    st.info("선택한 조건에 해당하는 데이터가 존재하지 않습니다.")

with tab2:
  st.subheader("🔎 개인별 자세한 접종 내역 조회 및 다운로드")
  st.markdown(
      "조건에 일치하는 환자별 상세 접종 기록입니다. 필요시 CSV 파일로"
      " 다운로드할 수 있습니다."
  )

  # 검색어 필터 (환자명 또는 차트번호 검색)
  search_query = st.text_input(
      "환자명 또는 차트번호 검색",
      placeholder="이름 또는 차트번호를 입력하세요...",
  )

  display_df = filtered_df.copy()
  if search_query:
    display_df = display_df[
        display_df["환자명"].astype(str).str.contains(search_query, na=False)
        | display_df["차트번호"]
        .astype(str)
        .str.contains(search_query, na=False)
    ]

  st.dataframe(display_df, use_container_width=True, height=450)

  # CSV 다운로드 버튼
  csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
  st.download_button(
      label="📥 상세 조회 결과 CSV 다운로드",
      data=csv_data,
      file_name=f"hismedi_vaccine_detail_{datetime.now().strftime('%Y%m%d')}.csv",
      mime="text/csv",
  )
