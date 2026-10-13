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
            margin-bottom: 12px;
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

        /* 진료과별 요약 박스 스타일 */
        .dept-summary-box {
            background-color: #F1F5F9;
            border-left: 4px solid #3B82F6;
            padding: 10px 14px;
            border-radius: 6px;
            margin-bottom: 15px;
            font-size: 13px;
            color: #1E293B;
        }

        /* ==========================================
           🖨️ A4 세로 용지 인쇄/PDF 최적화 스타일
           ================================---------- */
        @media print {
            [data-testid="stSidebar"], .no-print, button, header, footer, iframe {
                display: none !important;
            }
            
            .main .block-container {
                padding: 0 !important;
                margin: 0 !important;
                max-width: 100% !important;
            }

            @page {
                size: A4 portrait;
                margin: 12mm;
            }

            body {
                font-size: 10pt;
                color: #000000;
                background: #ffffff !important;
            }

            .report-header {
                background-color: #ffffff !important;
                border: 1px solid #000000 !important;
                padding: 8px !important;
                margin-bottom: 8px !important;
            }

            .dept-summary-box {
                background-color: #ffffff !important;
                border: 1px solid #94A3B8 !important;
                border-left: 4px solid #000000 !important;
            }

            table, tr, td, th {
                page-break-inside: avoid;
            }
            
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
  # 1. 상단 통합 보고서 타이틀 헤더
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

  # 2. 진료과별 요약 표시
  if not filtered_df.empty and "과목명" in filtered_df.columns:
    dept_summary_parts = []
    for dept in sorted(filtered_df["과목명"].dropna().unique()):
      dept_sub_df = filtered_df[filtered_df["과목명"] == dept]
      d_cases = len(dept_sub_df)
      d_patients = (
          dept_sub_df["차트번호"].nunique()
          if "차트번호" in dept_sub_df.columns
          else 0
      )
      dept_summary_parts.append(
          f"<b>{dept}</b> : 접종 건수 {d_cases:,}건 &nbsp;|&nbsp; 접종 환자수"
          f" {d_patients:,}명"
      )

    dept_summary_html = " &nbsp;&nbsp;|&nbsp;&nbsp; ".join(dept_summary_parts)
    st.markdown(
        f"""
        <div class="dept-summary-box">
            <b>🏥 진료과별 현황:</b> {dept_summary_html}
        </div>
        """,
        unsafe_allow_html=True,
    )

  if not filtered_df.empty:
    # 🏆 통계 집계 생성 (예방접종명, 처방코드, 처방명, 접종건수, 점유율)
    agg_df = (
        filtered_df.groupby([target_col, "처방코드", "처방명"])
        .size()
        .reset_index(name="접종건수")
    )
    # 접종건수 기준 내림차순 정렬
    stat_table = agg_df.sort_values(by="접종건수", ascending=False).reset_index(
        drop=True
    )
    stat_table.columns = ["예방접종명", "처방코드", "처방명", "접종건수"]

    total_sum = stat_table["접종건수"].sum()
    stat_table["점유율(%)"] = (
        stat_table["접종건수"] / total_sum * 100
    ).map("{:.2f}%".format)

    # 컬럼 순서 재배치 (예방접종명, 처방코드, 처방명, 접종건수, 점유율(%))
    stat_table = stat_table[
        ["예방접종명", "처방코드", "처방명", "접종건수", "점유율(%)"]
    ]

    # 💡 데이터를 2개의 열(Column)로 나누어 배치
    mid_idx = (len(stat_table) + 1) // 2
    df_left = stat_table.iloc[:mid_idx].reset_index(drop=True)
    df_right = stat_table.iloc[mid_idx:].reset_index(drop=True)

    col1, col2 = st.columns(2)
    with col1:
      st.dataframe(
          df_left,
          use_container_width=True,
          hide_index=True,
          height=min(420, len(df_left) * 35 + 40),
      )
    with col2:
      st.dataframe(
          df_right,
          use_container_width=True,
          hide_index=True,
          height=min(420, len(df_right) * 35 + 40),
      )

    st.markdown("<br>", unsafe_allow_html=True)

    # 3. 인쇄 버튼을 맨 아래로 배치
    print_button_html = """
        <div class="no-print" style="text-align: center; margin-top: 10px; margin-bottom: 20px;">
            <button onclick="window.parent.print();" style="
                background-color: #2563EB;
                color: #ffffff;
                border: none;
                padding: 10px 24px;
                border-radius: 6px;
                font-size: 15px;
                font-weight: 600;
                cursor: pointer;
                box-shadow: 0 1px 3px rgba(0,0,0,0.15);
            ">
                🖨️ 보고서 인쇄 / PDF 저장
            </button>
        </div>
    """
    components.html(print_button_html, height=60)

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

  # 차트번호 9자리 텍스트형 포맷팅 (앞자리 0 채우기, 쉼표 제거)
  if "차트번호" in display_df.columns:
    display_df["차트번호"] = display_df["차트번호"].apply(
        lambda x: (
            str(int(x)).zfill(9)
            if pd.notnull(x) and str(x).isdigit()
            else str(x).zfill(9)
        )
    )

  # 불필요한 파생 컬럼(연도, 월) 및 기본 인덱스 숨기기 정돈
  cols_to_drop = [c for c in ["연도", "월"] if c in display_df.columns]
  if cols_to_drop:
    display_df = display_df.drop(columns=cols_to_drop)

  st.dataframe(display_df, use_container_width=True, hide_index=True, height=450)

  # CSV 다운로드
  csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
  st.download_button(
      label="📥 상세 내역 CSV 다운로드",
      data=csv_data,
      file_name=f"vaccine_detail_{datetime.now().strftime('%Y%m%d')}.csv",
      mime="text/csv",
  )
