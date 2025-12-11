import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
from typing import Any, Optional
import html
import calendar
import re
import streamlit.components.v1 as components

# ------------------------------------------------------
# App / Secrets
# ------------------------------------------------------

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")  # 현재는 미사용이지만 향후 대비해서 유지

# Daily 보고용 시트
SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

# Weekly(주간업무) 시트
# secrets.toml 예시:
# [weekly_board]
# spreadsheet_id = "..."
# worksheet_name = "주간업무"
WEEKLY_SPREADSHEET_ID = st.secrets["weekly_board"]["spreadsheet_id"]
WEEKLY_WORKSHEET_NAME = st.secrets["weekly_board"]["worksheet_name"]

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

WEEK_COL = "WEEK"

# 시트 구조 관련 상수
HEADER_ROW = 1
DATA_START_ROW = HEADER_ROW + 1

# 요일 한글 표기
WEEKDAY_MAP = ["월", "화", "수", "목", "금", "토", "일"]

# ------------------------------------------------------
# Layout (상단 여백 줄이기)
# ------------------------------------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 2.3rem;
            padding-bottom: 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------------------------------------------------
# Google Sheets Connection (Daily)
# ------------------------------------------------------


@st.cache_resource
def get_gspread_client() -> gspread.Client:
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPE,
    )
    return gspread.authorize(credentials)


def get_worksheet() -> gspread.Worksheet:
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)


# ------------------------------------------------------
# Google Sheets Connection (Weekly)
# ------------------------------------------------------


@st.cache_resource
def get_weekly_worksheet() -> gspread.Worksheet:
    client = get_gspread_client()
    sh = client.open_by_key(WEEKLY_SPREADSHEET_ID)
    return sh.worksheet(WEEKLY_WORKSHEET_NAME)


# ------------------------------------------------------
# 날짜 유틸 함수
# ------------------------------------------------------


def parse_date_cell(v: Any) -> Optional[date]:
    """Daily 시트의 DATE 셀을 date 객체로 변환."""
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        # 1) ISO 형식 시도: 2025-11-24
        try:
            return date.fromisoformat(s)
        except Exception:
            pass

        # 2) 한글 형식: 2025년 11월 24일 (월) / 2025년11월24일 등
        m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return date(y, mth, d)
            except Exception:
                return None

    return None


def format_date_for_display(d: Any) -> str:
    """화면 상단 제목용: YYYY-MM-DD(요일)"""
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return str(d)
    w = WEEKDAY_MAP[d.weekday()]
    return d.strftime("%Y-%m-%d") + f"({w})"


def format_date_simple(d: Any) -> str:
    """YYYY-MM-DD 문자열 (테이블 내부용)"""
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return str(d)


def format_date_with_weekday(d: Any) -> str:
    """테이블용 DATE 컬럼: YYYY-MM-DD (요일)"""
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return str(d)
    w = WEEKDAY_MAP[d.weekday()]
    return d.strftime("%Y-%m-%d") + f" ({w})"


def escape_html(text: Any) -> str:
    if text is None:
        return ""
    s = html.escape(str(text))
    return s.replace("\n", "<br>")


# ------------------------------------------------------
# Load Daily Report DF
# ------------------------------------------------------


@st.cache_data(ttl=60)
def load_daily_df() -> pd.DataFrame:
    ws = get_worksheet()
    records = ws.get_all_records()

    # 데이터가 아예 없으면 빈 DF 반환
    if not records:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df = pd.DataFrame(records)

    # DATE 컬럼 필수
    if "DATE" not in df.columns:
        st.error("Daily 시트의 헤더에 'DATE' 열이 필요합니다.")
        st.stop()

    # 시트 상 실제 행 번호 (헤더=1행 → 데이터는 DATA_START_ROW부터)
    df["__row"] = df.index + DATA_START_ROW

    # DATE 파싱 (모두 date 객체로 통일)
    parsed = df["DATE"].apply(parse_date_cell)

    # 파싱 실패값 경고
    invalid_mask = parsed.isna()
    if invalid_mask.any():
        invalid_values = df.loc[invalid_mask, "DATE"].astype(str).unique()
        st.warning(
            "파싱할 수 없는 DATE 값이 있어 제외되었습니다: "
            + ", ".join(invalid_values),
        )

    # 유효한 행만 사용
    valid_mask = ~invalid_mask
    df = df[valid_mask].copy()
    if df.empty:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df["DATE"] = parsed[valid_mask].values

    # 내용/비고 정리
    for col in ["내용", "비고"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    return df


# ------------------------------------------------------
# Save / Update Entry
# ------------------------------------------------------


def save_daily_entry(
    selected_date: date,
    content: str,
    note: str,
    df: pd.DataFrame,
) -> None:
    ws = get_worksheet()

    if not df.empty:
        mask = df["DATE"] == selected_date
    else:
        mask = pd.Series([], dtype=bool)

    if not df.empty and mask.any():
        # 기존 행 업데이트 (B, C 두 셀을 한 번에)
        row_number = int(df.loc[mask, "__row"].iloc[0])
        ws.update(
            f"B{row_number}:C{row_number}",
            [[content, note]],
        )
    else:
        # 새 행 추가
        ws.append_row(
            [selected_date.isoformat(), content, note],
            value_input_option="USER_ENTERED",
        )

    # 캐시 무효화
    load_daily_df.clear()


# ------------------------------------------------------
# Weekly (주간업무) DF 로드 & 카드 렌더링
# ------------------------------------------------------


@st.cache_data(ttl=300)
def load_weekly_df() -> pd.DataFrame:
    """
    '주간업무' 시트를 읽어서 WEEK 기준으로 최신 순 정렬된 DF 반환.
    WEEK 형식 예: 2025.12.08~2025.12.21
    """
    ws = get_weekly_worksheet()
    values = ws.get_all_values()

    if not values or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    df = pd.DataFrame(rows, columns=header)

    # 완전히 빈 행 제거
    df = df.replace("", pd.NA)
    df = df.dropna(how="all")
    df = df.fillna("")

    if WEEK_COL not in df.columns:
        st.warning("주간업무 시트에 'WEEK' 열이 없습니다.")
        return pd.DataFrame()

    def parse_start(week_str: str) -> Optional[date]:
        try:
            s = str(week_str).split("~")[0].strip()
            return datetime.strptime(s, "%Y.%m.%d").date()
        except Exception:
            return None

    df["_start"] = df[WEEK_COL].astype(str).apply(parse_start)
    df = df.dropna(subset=["_start"])
    df = df.sort_values("_start", ascending=False).reset_index(drop=True)

    return df


def render_weekly_cards(df_weekly: pd.DataFrame, week_str: str) -> None:
    """
    선택한 WEEK 한 줄을 기존 주간업무 앱과 비슷한 느낌으로 렌더링.
    - 상단에 기간 제목
    - 부서별로 테두리 있는 컨테이너 + 내부 연한 회색 박스
    """
    row_df = df_weekly[df_weekly[WEEK_COL] == week_str]
    if row_df.empty:
        st.info("선택한 기간의 주간업무 데이터가 없습니다.")
        return

    row = row_df.iloc[0]
    dept_cols = [
        c for c in df_weekly.columns
        if c not in [WEEK_COL, "_start"] and not c.startswith("Unnamed")
    ]

    col_a, col_b = st.columns(2)

    card_idx = 0
    for dept in dept_cols:
        raw_text = row.get(dept, "")
        if raw_text is None:
            continue
        text = str(raw_text).strip()
        if not text:
            continue

        target_col = col_a if card_idx % 2 == 0 else col_b
        card_idx += 1

        with target_col:
            with st.container(border=True):

                # 부서명: 카드 테두리와 거의 붙게, 아래 여백은 살짝만
                st.markdown(
                    f"<div style='font-size:0.82rem; font-weight:700; margin:-0.18rem 0 0.04rem 0;'>{dept}</div>",
                    unsafe_allow_html=True,
                )

                # 회색 박스: 왼쪽 정렬 + 폰트 조금 키우고, 위/아래 여백 정리
                st.markdown(
                    f"""<div style="
                        background:#f3f4f6;
                        border-radius:0.5rem;
                        padding:0.16rem 0.60rem;
                        margin-bottom:0.18rem;
                        font-size:0.80rem;
                        line-height:1.30;
                        color:#111827;
                        white-space:pre-wrap;
                        text-align:left;
                    ">{escape_html(text)}</div>""",
                    unsafe_allow_html=True,
                )

    if card_idx == 0:
        st.info("선택한 기간에 작성된 부서별 업무 내용이 없습니다.")


# ------------------------------------------------------
# 외부 진료시간표 시트 미리보기
# ------------------------------------------------------

def render_sheet_preview() -> None:
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")

    src_view = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/htmlview"
        f"?gid={gid}&rm=minimal"
    )

    st.components.v1.html(
        f"""
        <iframe
            src="{src_view}"
            style="
              width: 100%;
              height: 700px;
              border: 1px solid #ddd;
              border-radius: 0.5rem;
              background: white;
            "
        ></iframe>
        """,
        height=720,
        scrolling=True,
    )


# ------------------------------------------------------
# UI 기본 환경
# ------------------------------------------------------

df_daily = load_daily_df()

# 플래시 메시지 상태
if "flash" not in st.session_state:
    st.session_state["flash"] = None

# 진료시간표 모달 상태
if "timetable_open" not in st.session_state:
    st.session_state["timetable_open"] = False

today = date.today()

# 1일 보고 기본 날짜
default_single = today

# ------------------------------------------------------
# 사이드바
# ------------------------------------------------------

with st.sidebar:
    st.markdown(
        f"<h2 style='font-size:1.6rem; font-weight:700;'>{APP_TITLE}</h2>",
        unsafe_allow_html=True,
    )

    # 1) 업무현황 (월)
    st.markdown("### 업무현황 (월)")
    if df_daily.empty:
        st.caption("아직 작성된 보고가 없어 월 선택 옵션이 없습니다.")
        selected_ym = None
    else:
        ym_set = {(d.year, d.month) for d in df_daily["DATE"]}
        ym_options = sorted(ym_set, reverse=True)  # 최근 연/월이 위로

        default_ym = (today.year, today.month)
        if default_ym in ym_options:
            default_index = ym_options.index(default_ym)
        else:
            default_index = 0

        selected_ym = st.selectbox(
            "월 선택",
            ym_options,
            index=default_index,
            format_func=lambda ym: f"{ym[0]}년 {ym[1]:02d}월",
        )

    # 2) 1일 업무 메모 (사이드바로 이동)
    st.markdown("### 1일 업무 메모")
    selected_date = st.date_input(
        "날짜",
        value=default_single,
    )
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]

    # 선택된 날짜의 기존 내용 불러오기
    if not df_daily.empty and (df_daily["DATE"] == selected_date).any():
        row = df_daily[df_daily["DATE"] == selected_date].iloc[0]
        default_content = row["내용"]
        has_existing = True
    else:
        default_content = ""
        has_existing = False

    content = st.text_area(
        "내용",
        height=140,
        value=default_content,
        placeholder="짤막하게 메모를 남겨두세요.\n(예: 정대표 미팅 - 14시)",
        key="sidebar_daily_memo",
    )

    col_s_save, col_s_clear = st.columns(2)
    with col_s_save:
        if st.button("저장", use_container_width=True):
            save_daily_entry(selected_date, content, "", df_daily)
            st.session_state["flash"] = ("success", "저장되었습니다.")
            st.rerun()

    with col_s_clear:
        if has_existing and st.button("비우기", use_container_width=True):
            save_daily_entry(selected_date, "", "", df_daily)
            st.session_state["flash"] = (
                "info",
                "이 날짜의 메모를 모두 비웠습니다.",
            )
            st.rerun()

    # 3) 진료시간표
    st.markdown("### 진료시간표")

    is_open = st.session_state.get("timetable_open", False)

    # 열기 / 닫기 버튼
    if is_open:
        if st.button("진료시간표 닫기", use_container_width=True):
            st.session_state["timetable_open"] = False
            st.rerun()
    else:
        if st.button("진료시간표 열기", use_container_width=True):
            st.session_state["timetable_open"] = True
            st.rerun()

    # '새 창에서 열기' 링크 (항상 표시)
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_open = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"
    )

    st.markdown(
        f'<a href="{src_open}" target="_blank" '
        f'style="font-size:0.8rem; color:#4f46e5; text-decoration:none;">'
        f'새 창에서 열기 ↗</a>',
        unsafe_allow_html=True,
    )

# --------------------------- 진료시간표 ---------------------------

if st.session_state.get("timetable_open", False):
    with st.spinner("진료시간표를 불러오는 중..."):
        render_sheet_preview()

# --------------------------- 월별 보기 ---------------------------

if df_daily.empty or selected_ym is None:
    st.info("아직 작성된 보고가 없습니다.")
else:
    year, month = selected_ym

    # 선택한 월의 시작/끝 날짜 계산
    start_date = date(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = date(year, month, last_day)

    # 상단 제목
    st.markdown(f"## {year}년 {month:02d}월")

    # 월별 보기 레이아웃: 왼쪽 Daily 표(1/3), 오른쪽 Weekly 카드(2/3)
    col_left, col_right = st.columns([1, 2])

    # ---------------- 왼쪽: Daily 월별 표 ----------------
    with col_left:
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = (
            df_daily.loc[mask, ["DATE", "내용"]]
            .copy()
            .sort_values("DATE")
            .reset_index(drop=True)
        )

        if period_df.empty:
            st.info("해당 월에 작성된 보고가 없습니다.")
        else:
            # 날짜 문자열 변환
            period_df["DATE_STR"] = period_df["DATE"].apply(
                format_date_with_weekday
            )
            period_df["CONTENT_STR"] = period_df["내용"].astype(str)

            # HTML 테이블 직접 렌더링 (index 완전히 제거)
            rows_html = ""
            for _, r in period_df.iterrows():
                d_str = escape_html(r["DATE_STR"])
                c_str = escape_html(r["CONTENT_STR"])
                rows_html += f"""
<tr>
  <td class="m-date">{d_str}</td>
  <td class="m-content">{c_str}</td>
</tr>
"""

            table_html = f"""
<style>
.m-table {{
  border-collapse: collapse;
  width: 100%;
  font-size: 0.80rem;
}}
.m-table thead th {{
  text-align: center;
  padding: 4px 6px;
  border-bottom: 1px solid #e5e7eb;
  color: #4b5563;
}}
.m-table tbody td {{
  vertical-align: top;
  padding: 3px 6px;
  border-bottom: 1px solid #f3f4f6;
}}
.m-date {{
  white-space: nowrap;
  width: 8.5rem;
  color: #111827;
  font-weight: 600;
}}
.m-content {{
  white-space: pre-wrap;
  color: #111827;
}}
</style>

<table class="m-table">
  <thead>
    <tr>
      <th>DATE</th>
      <th>내용</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
</table>
"""

            st.markdown(table_html, unsafe_allow_html=True)

    # ---------------- 오른쪽: Weekly 주간업무 카드 ----------------
    with col_right:
        try:
            weekly_df = load_weekly_df()
        except Exception:
            st.info("주간업무 시트 연결 설정이 아직 완료되지 않았습니다.")
            weekly_df = pd.DataFrame()

        if not weekly_df.empty:
            week_options = weekly_df[WEEK_COL].astype(str).tolist()

            # 기본 선택: 가장 최근 주간 (0번째)
            default_week_idx = 0
            # 세션에 이전 선택이 있으면 유지
            prev_week = st.session_state.get("weekly_week_select")
            if prev_week in week_options:
                default_week_idx = week_options.index(prev_week)

            selected_week = st.selectbox(
                "기간선택 (주간업무)",
                options=week_options,
                index=default_week_idx,
                key="weekly_week_select",
            )

            render_weekly_cards(weekly_df, selected_week)
        else:
            st.info("주간업무 데이터가 없습니다.")


# ------------------------------------------------------
# 플래시 메시지 출력
# ------------------------------------------------------

flash = st.session_state.get("flash")
if flash:
    level, msg = flash
    if level == "success":
        st.success(msg)
    elif level == "info":
        st.info(msg)
    elif level == "warning":
        st.warning(msg)
    st.session_state["flash"] = None
