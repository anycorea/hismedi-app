from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import streamlit as st

# 페이지 기본 설정
st.set_page_config(
    page_title="예방접종 통계 및 분석", page_icon="💉", layout="wide"
)

# 커스텀 CSS 및 박스 제거 레이아웃 스타일
st.markdown(
    """
    <style>
        /* 사이드바 여백 및 가독성 최적화 */
        [data-testid="stSidebar"] > div:first-child {
            padding-top: 1rem !important;
            padding-bottom: 1rem !important;
        }
        
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

        hr { margin: 8px 0 !important; }

        /* 세련된 박스형 탭 메뉴 스타일링 */
        [data-testid="stTabs"] button[data-baseweb="tab"] {
            font-size: 16px !important;
            font-weight: 700 !important;
            padding: 12px 24px !important;
            border-radius: 8px 8px 0 0 !important;
            background-color: #F1F5F9 !important;
            color: #475569 !important;
            border: 1px solid #CBD5E1 !important;
            margin-right: 6px !important;
        }
        [data-testid="stTabs"] button[data-baseweb="tab"][aria-selected="true"] {
            background-color: #2563EB !important;
            color: #ffffff !important;
            border-color: #2563EB !important;
            box-shadow: 0 4px 6px -1px rgba(37, 99, 235, 0.2);
        }

        /* 깔끔한 인라인 요약 영역 스타일 */
        .header-container {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            border-bottom: 2px solid #E2E8F0;
            padding-bottom: 8px;
            margin-bottom: 10px;
        }
        .main-stats-text {
            font-size: 18px;
            font-weight: 800;
            color: #0F172A;
            letter-spacing: -0.3px;
        }
        .main-stats-text span {
            color: #2563EB;
        }
        .sub-info-text {
            font-size: 12.5px;
            color: #64748B;
            font-weight: 500;
        }
        .dept-stats-text {
            font-size: 13.5px;
            font-weight: 600;
            color: #334155;
            margin-bottom: 15px;
            padding: 4px 0;
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


# ==========================================
# 🛠️ [왼쪽 메뉴] 필터 설정 및 데이터 새로고침
# ==========================================
st.sidebar.markdown(
    '<div class="sidebar-title">💉 예방접종 통계 및 분석</div>',
    unsafe_allow_html=True,
)
st.sidebar.markdown(
    '<div class="sidebar-sub">히즈메디병원 접종 현황 관리</div>',
    unsafe_allow_html=True,
)

if st.sidebar.button("🔄 데이터 새로고침", use_container_width=True):
  st.cache_data.clear()
  st.sidebar.success("최신 데이터를 불러왔습니다!")
  st.rerun()

st.sidebar.divider()

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
dept_mode = st.sidebar.radio(
    "진료과 모드",
    ["전체 진료과 보기 (기본)", "특정 진료과만 선택"],
    label_visibility="collapsed",
    key="dept_mode_radio",
)

unique_depts = sorted(df_merged["과목명"].dropna().unique().tolist())
if dept_mode == "전체 진료과 보기 (기본)":
  selected_depts = unique_depts
else:
  selected_depts = st.sidebar.multiselect(
      "조회할 진료과 선택",
      options=unique_depts,
      default=[],
      placeholder="진료과를 선택하세요...",
      key="dept_multiselect",
  )
  if not selected_depts:
    selected_depts = unique_depts

st.sidebar.divider()

# 3. 예방접종 종류 선택
st.sidebar.subheader("💉 예방접종 종류 선택")
vax_mode = st.sidebar.radio(
    "접종 종류 모드",
    ["전체 예방접종 보기 (기본)", "특정 접종만 선택"],
    label_visibility="collapsed",
)

unique_vax = sorted(df_merged[target_col].dropna().unique().tolist())
if vax_mode == "전체 예방접종 보기 (기본)":
  selected_vax = unique_vax
else:
  selected_vax = st.sidebar.multiselect(
      "조회할 예방접종 선택",
      options=unique_vax,
      default=[],
      placeholder="백신을 선택하세요...",
  )
  if not selected_vax:
    selected_vax = unique_vax

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

# 통계 테이블 데이터 미리 준비
if not filtered_df.empty:
  agg_df = (
      filtered_df.groupby([target_col, "처방코드", "처방명"])
      .size()
      .reset_index(name="접종건수")
  )
  stat_table = agg_df.sort_values(by="접종건수", ascending=False).reset_index(
      drop=True
  )
  stat_table.columns = ["예방접종명", "처방코드", "처방명", "접종건수"]

  total_sum = stat_table["접종건수"].sum()
  stat_table["점유율(%)"] = (
      stat_table["접종건수"] / total_sum * 100
  ).map("{:.2f}%".format)

  stat_table = stat_table[
      ["예방접종명", "처방코드", "처방명", "접종건수", "점유율(%)"]
  ]
else:
  stat_table = pd.DataFrame()

# 진료과별 요약 문자열 준비 (소아청소년과 우선 정렬)
dept_summary_parts = []
if not filtered_df.empty and "과목명" in filtered_df.columns:
  depts = sorted(filtered_df["과목명"].dropna().unique().tolist())
  # '소아청소년과'가 있으면 맨 앞으로 배치
  if "소아청소년과" in depts:
    depts.remove("소아청소년과")
    depts = ["소아청소년과"] + depts

  for dept in depts:
    dept_sub_df = filtered_df[filtered_df["과목명"] == dept]
    d_cases = len(dept_sub_df)
    d_patients = (
        dept_sub_df["차트번호"].nunique()
        if "차트번호" in dept_sub_df.columns
        else 0
    )
    dept_summary_parts.append(
        f"<b>{dept}</b> : 건수 {d_cases:,}건 (환자수 {d_patients:,}명)"
    )

dept_summary_html = (
    " &nbsp;&nbsp;|&nbsp;&nbsp; ".join(dept_summary_parts)
    if dept_summary_parts
    else "데이터 없음"
)

# ==========================================
# 📑 [최상단 요약 정보 및 진료과 통계 배치] (박스 제거, 슬림화)
# ==========================================
st.markdown(
    f"""
    <div class="header-container">
        <div class="main-stats-text">
            총 접종 건수: <span>{total_cases:,}건</span> &nbsp;&nbsp;|&nbsp;&nbsp; 접종 환자 수(중복제거): <span>{unique_patients:,}명</span>
        </div>
        <div class="sub-info-text">
            <b>조회 기간:</b> {start_date} ~ {end_date} &nbsp;&nbsp;|&nbsp;&nbsp; <b>발행일시:</b> {current_time_str}
        </div>
    </div>
    <div class="dept-stats-text">
        [{dept_summary_html}]
    </div>
    """,
    unsafe_allow_html=True,
)

# ==========================================
# 📑 [탭 구성] 세련된 박스형 탭 메뉴 (2개 탭)
# ==========================================
tab1, tab2 = st.tabs(["📊 예방접종 통계 및 분석", "📋 개인별 상세 접종 내역"])

with tab1:
  if not stat_table.empty:
    st.dataframe(
        stat_table,
        use_container_width=True,
        hide_index=True,
        height=min(600, len(stat_table) * 33 + 40),
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # 📥 통계 요약표 CSV 다운로드 버튼 추가
    stat_csv_data = stat_table.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="📥 통계 요약표 CSV 다운로드",
        data=stat_csv_data,
        file_name=f"vaccine_stat_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
  else:
    st.info("선택한 조건에 해당하는 데이터가 존재하지 않습니다.")

with tab2:
  # 🔒 개인정보 보호 비밀번호 인증 세션 초기화
  if "auth_passed" not in st.session_state:
    st.session_state.auth_passed = False

  if not st.session_state.auth_passed:
    st.warning(
        "🔒 개인별 상세 접종 내역에는 민감한 환자 개인정보가 포함되어 있습니다."
    )
    entered_pw = st.text_input(
        "비밀번호를 입력하세요", type="password", placeholder="비밀번호 입력..."
    )
    if st.button("확인"):
      if entered_pw == "15880223":
        st.session_state.auth_passed = True
        st.rerun()
      else:
        st.error("비밀번호가 틀렸습니다.")
  else:
    # 인증 완료 후 화면
    if st.button("🔓 잠금 (로그아웃)", key="logout_btn"):
      st.session_state.auth_passed = False
      st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # 전체 항목 통합 검색 박스
    search_query = st.text_input(
        "통합 검색",
        placeholder=(
            "모든 항목 검색 (환자명, 차트번호, 백신명, 처방코드, 과목명 등 입력)..."
        ),
    )

    display_df = filtered_df.copy()

    # 차트번호 9자리 텍스트형 포맷팅 (앞자리 0 채우기, 쉼표 제거)
    if "차트번호" in display_df.columns:
      display_df["차트번호"] = display_df["차트번호"].apply(
          lambda x: (
              str(int(x)).zfill(9)
              if pd.notnull(x) and str(x).isdigit()
              else str(x).zfill(9)
          )
      )

    # 불필요한 파생 컬럼(연도, 월) 숨기기 정돈
    cols_to_drop = [c for c in ["연도", "월"] if c in display_df.columns]
    if cols_to_drop:
      display_df = display_df.drop(columns=cols_to_drop)

    # 전체 항목(모든 컬럼) 대상 통합 검색 필터링 적용
    if search_query:
      mask = display_df.astype(str).apply(
          lambda row: row.str.contains(search_query, case=False, na=False).any(),
          axis=1,
      )
      display_df = display_df[mask]

    st.dataframe(
        display_df, use_container_width=True, hide_index=True, height=450
    )

    csv_data = display_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        label="📥 상세 내역 CSV 다운로드",
        data=csv_data,
        file_name=f"vaccine_detail_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
