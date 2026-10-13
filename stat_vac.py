from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# 페이지 기본 설정
st.set_page_config(
    page_title="예방접종 통계 및 분석", page_icon="💉", layout="wide"
)

# 커스텀 CSS 및 A4 세로 인쇄 최적화 스타일
st.markdown(
    """
    <style>
        /* 사이드바 여백 최적화 */
        [data-testid="stSidebar"] > div:first-child {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }
        
        /* 사이드바 제목 스타일 */
        .sidebar-title {
            font-size: 22px !important;
            font-weight: 800 !important;
            color: #1E3A8A !important;
            margin-bottom: 2px !important;
            line-height: 1.2;
        }
        .sidebar-sub {
            font-size: 12px !important;
            color: #6B7280 !important;
            margin-bottom: 12px !important;
        }

        /* 구분선 간격 줄이기 */
        hr { margin: 8px 0 !important; }

        /* 보고서 상단 타이틀 스타일 */
        .report-header {
            background-color: #F8FAFC;
            border: 1px solid #E2E8F0;
            border-radius: 8px;
            padding: 16px;
            text-align: center;
            margin-bottom: 15px;
        }
        .report-header h2 {
            font-size: 22px !important;
            font-weight: 700;
            color: #0F172A;
            margin-bottom: 8px !important;
        }
        .report-header .info-line {
            font-size: 13px;
            color: #475569;
            font-weight: 500;
        }

        /* ==========================================
           🖨️ A4 세로 용지 인쇄/PDF 최적화 스타일
           ================================---------- */
        @media print {
            /* 브라우저 인쇄 시 사이드바, 버튼, 네비게이션 숨김 */
            [data-testid="stSidebar"], .no-print, button, header, footer, iframe {
                display: none !important;
            }
            
            /* 본문 영역 마진 및 패딩 제거하여 전체 활용 */
            .main .block-container {
                padding: 0 !important;
                margin: 0 !important;
                max-width: 100% !important;
            }

            /* A4 세로 용지 크기 및 여백 지정 */
            @page {
                size: A4 portrait;
                margin: 15mm;
            }

            body {
                font-size: 12pt;
                color: #000000;
                background: #ffffff !important;
            }

            /* 보고서 헤더 테두리 깔끔하게 인쇄 */
            .report-header {
                background-color: #ffffff !important;
                border: 1px solid #000000 !important;
                padding: 10px !important;
                margin-bottom: 10px !important;
            }

            /* 데이터 테이블 및 차트가 인쇄 시 잘리지 않도록 설정 */
            table, tr, td, th {
                page-break-inside: avoid;
            }
            
            /* 스트림릿 데이터프레임 스크롤 해제 및 전체 표시 */
            [data-testid="stDataFrame"] div {
                max-height: none !important;
                overflow: visible !important;
            }
        }
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
  """gspread 및 st.secrets의 [gcp_service_account]를 사용하여 vac, std 시트 로드"""
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


# 데이터 로드 및 예외 처리
try:
  df_vac, df_std = load_data_from_gsheet()
except Exception as e:
  st.error(f"데이터 로드 오류: {e}")
  st.stop()

# 데이터 전처리 및 표준명 매핑
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
  st.error("시트에 '처방코드' 컬럼이 없습니다.")
  st.stop()

# 날짜 형식 변환 및 연/월 추출
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
# 🛠️ [왼쪽 메뉴] 필터 설정
# ==========================================
st.sidebar.markdown(
    '<div class="sidebar-title">💉 예방접종 통계 및 분석</div>',
    unsafe_allow_html=True,
)
st.sidebar.markdown(
    '<div class="sidebar-sub">히즈메디병원 접종 현황 관리</div>',
    unsafe_allow_html=True,
)
st.sidebar.divider()

# 1. 기간 선택
st.sidebar.subheader("📅 기간 선택")
period_mode = st.sidebar.radio(
    "기간 단위",
    ["선택기간", "일별", "월별", "연도별"],
    label_visibility="collapsed",
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

start_date, end_date = min_date, max_date

if period_mode == "선택기간":
  date_range = st.sidebar.date_input(
      "조회 기간",
      value=(min_date, max_date),
      min_value=min_date,
      max_value=max_date,
  )
  if isinstance(date_range, (list, tuple)):
    if len(date_range) == 2:
      start_date, end_date = date_range
    elif len(date_range) == 1:
      start_date = end_date = date_range[0]
elif period_mode == "월별":
  all_months = sorted(df_merged["월"].dropna().unique().tolist())
  if all_months:
    selected_month = st.sidebar.selectbox("월 선택", all_months, index=len(all_months) - 1)
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
  if all_years:
    selected_year = st.sidebar.selectbox("연도 선택", all_years, index=len(all_years) - 1)
    start_date = datetime(selected_year, 1, 1).date()
    end_date = datetime(selected_year, 12, 31).date()
else:  # 일별
  selected_day = st.sidebar.date_input("일자 선택", value=max_date)
  start_date = end_date = selected_day

st.sidebar.divider()

# 2. 진료과 선택
st.sidebar.subheader("🏥 진료과 선택")
unique_depts = sorted(df_merged["과목명"].dropna().unique().tolist())
dept_all = st.sidebar.checkbox("진료과 전체 선택", value=True)

if dept_all:
  selected_depts = st.sidebar.multiselect(
      "진료과", options=unique_depts, default=unique_depts, label_visibility="collapsed"
  )
else:
  selected_depts = st.sidebar.multiselect(
      "진료과", options=unique_depts, default=[], label_visibility="collapsed"
  )

st.sidebar.divider()

# 3. 예방접종 종류 선택
st.sidebar.subheader("💉 예방접종 종류")
unique_vax = sorted(df_merged[target_col].dropna().unique().tolist())
vax_all = st.sidebar.checkbox("예방접종 전체 선택", value=True)

if vax_all:
  selected_vax = st.sidebar.multiselect(
      "예방접종 종류", options=unique_vax, default=unique_vax, label_visibility="collapsed"
  )
else:
  selected_vax = st.sidebar.multiselect(
      "예방접종 종류", options=unique_vax, default=[], label_visibility="collapsed"
  )

# ==========================================
# 📊 [메인 화면] 통계 및 데이터 출력
# ==========================================
filtered_df = df_merged.copy()

# 필터링 적용
filtered_df = filtered_df[
    (filtered_df["접수일자"] >= start_date) & (filtered_df["접수일자"] <= end_date)
]
if selected_depts:
  filtered_df = filtered_df[filtered_df["과목명"].isin(selected_depts)]
if selected_vax:
  filtered_df = filtered_df[filtered_df[target_col].isin(selected_vax)]

# 주요 집계 수치 계산
total_cases = len(filtered_df)
unique_patients = (
    filtered_df["차트번호"].nunique() if "차트번호" in filtered_df.columns else 0
)
current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M")

# 탭 구성
tab1, tab2 = st.tabs(["📊 예방접종 통계 및 보고서", "📋 개인별 상세 접종 내역"])

with tab1:
  # 상단 통합 보고서 타이틀 헤더
  st.markdown(
      f"""
      <div class="report-header">
          <h2>[ 히즈메디병원 예방접종 현황 통계 보고서 ]</h2>
          <div class="info-line">
              <b>조회 기간:</b> {start_date} ~ {end_date} &nbsp;|&nbsp;
              <b>총 접종 건수:</b> {total_cases:,}건 &nbsp;|&nbsp;
              <b>접종 환자 수(중복제거):</b> {unique_patients:,}명 &nbsp;|&nbsp;
              <b>발행일시:</b> {current_time_str}
          </div>
      </div>
      """,
      unsafe_allow_html=True,
  )

  if not filtered_df.empty:
    # 🏆 내림차순(많은 순) 정렬 통계 생성 (건수 기준 내림차순 정렬 보장)
    stat_table = (
        filtered_df[target_col]
        .value_counts()
        .reset_index(name="접종 건수")
    )
    stat_table.columns = ["예방접종명 (표준명)", "접종 건수"]
    stat_table["점유율(%)"] = (
        stat_table["접종 건수"] / stat_table["접종 건수"].sum() * 100
    ).round(2)

    # 인쇄 버튼
    print_button_html = """
        <div class="no-print" style="margin-bottom: 15px;">
            <button onclick="window.parent.print();" style="
                background-color: #2563EB;
                color: #ffffff;
                border: none;
                padding: 8px 18px;
                border-radius: 6px;
                font-size: 14px;
                font-weight: 600;
                cursor: pointer;
                box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            ">
                🖨️ 보고서 인쇄 / PDF 저장
            </button>
        </div>
    """
    components.html(print_button_html, height=45)

    # 1. 통계 데이터 전체 테이블 표시 (인쇄 시 잘리지 않고 전체 행 표시)
    st.markdown("### 📋 표준 예방접종별 접종 순위표")
    st.dataframe(
        stat_table,
        use_container_width=True,
        hide_index=True,
        height=min(350, len(stat_table) * 35 + 40),
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # 2. 차트 표시 (많은 건수 순으로 정렬된 데이터 기준 바 차트)
    st.markdown("### 📊 예방접종 건수 시각화 차트")
    chart_data = stat_table.set_index("예방접종명 (표준명)")["접종 건수"]
    st.bar_chart(chart_data, height=400)

  else:
    st.info("선택한 조건에 해당하는 데이터가 존재하지 않습니다.")

with tab2:
  # 환자명/차트번호 검색
  search_query = st.text_input(
      "검색",
      placeholder="환자명 또는 차트번호 입력...",
      label_visibility="collapsed",
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

  # CSV 다운로드
  csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
  st.download_button(
      label="📥 상세 내역 CSV 다운로드",
      data=csv_data,
      file_name=f"vaccine_detail_{datetime.now().strftime('%Y%m%d')}.csv",
      mime="text/csv",
  )
