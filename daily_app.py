import streamlit as st
import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
import textwrap

# ------------------------------------------------------
# App / Secrets
# ------------------------------------------------------

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")

SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ------------------------------------------------------
# Google Sheets Connection
# ------------------------------------------------------

@st.cache_resource
def get_gspread_client():
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPE,
    )
    return gspread.authorize(credentials)


def get_worksheet():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)


# ------------------------------------------------------
# 날짜 유틸 함수
# ------------------------------------------------------

def parse_date_cell(v):
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


def format_date_for_display(d):
    """화면 상단 제목용: YYYY-MM-DD(요일)"""
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return str(d)
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    w = weekday_map[d.weekday()]
    return d.strftime("%Y-%m-%d") + f"({w})"


def format_date_simple(d):
    """YYYY-MM-DD 문자열 (테이블 내부용)"""
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%Y-%m-%d")
    return str(d)


def format_date_with_weekday(d):
    """테이블용 DATE 컬럼: YYYY-MM-DD (요일)"""
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return str(d)
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    w = weekday_map[d.weekday()]
    return d.strftime("%Y-%m-%d") + f" ({w})"


# ------------------------------------------------------
# Load Daily Report DF
# ------------------------------------------------------

@st.cache_data(ttl=60)
def load_daily_df():
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

    # 시트 상 실제 행 번호 (헤더=1행 → 데이터는 2행부터)
    df["__row"] = df.index + 2

    # DATE 파싱 (모두 date 객체로 통일)
    parsed = df["DATE"].apply(parse_date_cell)
    df = df[parsed.notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df["DATE"] = parsed

    # 내용/비고 정리
    for col in ["내용", "비고"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    return df


# ------------------------------------------------------
# Save / Update Entry
# ------------------------------------------------------

def save_daily_entry(selected_date: date, content: str, note: str, df: pd.DataFrame):
    ws = get_worksheet()

    if not df.empty:
        mask = df["DATE"] == selected_date
    else:
        mask = pd.Series([], dtype=bool)

    if not df.empty and mask.any():
        # 기존 행 업데이트
        row_number = int(df.loc[mask, "__row"].iloc[0])
        ws.update_cell(row_number, 2, content)
        ws.update_cell(row_number, 3, note)
    else:
        # 새 행 추가
        ws.append_row(
            [selected_date.isoformat(), content, note],
            value_input_option="USER_ENTERED",
        )

    # 캐시 무효화
    load_daily_df.clear()


# ------------------------------------------------------
# UI 기본 환경
# ------------------------------------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

df_daily = load_daily_df()

st.sidebar.title("Daily Report")
mode = st.sidebar.radio("보기 모드", ("1일 보고", "기간 요약"))

today = date.today()

# 1일 보고 기본 날짜: 오늘
default_single = today

# 기간 기본값: 이번 주 월요일 ~ 일요일
weekday_idx = today.weekday()  # 월=0
week_start = today - timedelta(days=weekday_idx)
week_end = week_start + timedelta(days=6)
default_range = (week_start, week_end)


# --------------------------- 1일 보고 모드 ---------------------------

if mode == "1일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD",
    )
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]

    # 헤더: YYYY-MM-DD(요일)
    st.subheader(format_date_for_display(selected_date))

    # 현재 날짜 데이터 로딩
    if not df_daily.empty and (df_daily["DATE"] == selected_date).any():
        row = df_daily[df_daily["DATE"] == selected_date].iloc[0]
        default_content = row["내용"]
        default_note = row["비고"]
        has_existing = True
    else:
        default_content = ""
        default_note = ""
        has_existing = False

    col_left, col_right = st.columns([3, 1])

    with col_left:
        content = st.text_area(
            "내용",
            height=350,
            value=default_content,
            placeholder="이 날의 업무를 자유롭게 작성하세요.\n(엔터로 줄바꿈)",
        )

    with col_right:
        note = st.text_area(
            "비고 (선택)",
            height=350,
            value=default_note,
            placeholder="특이사항이 있을 때만 작성하세요.",
        )

    # 버튼 영역
    save_col, clear_col, _ = st.columns([1, 1, 5])

    with save_col:
        if st.button("저장", type="primary"):
            save_daily_entry(selected_date, content, note, df_daily)
            st.success("저장되었습니다.")
            st.rerun()

    with clear_col:
        if has_existing and st.button("내용 비우기"):
            save_daily_entry(selected_date, "", "", df_daily)
            st.info("이 날짜의 내용/비고를 모두 비웠습니다.")
            st.rerun()

    st.caption("※ 줄바꿈은 Google Sheet 셀 안에 그대로 저장됩니다.")


# --------------------------- 기간 요약 모드 ---------------------------
else:
    # 기간 날짜 선택 UI
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD",
    )

    if isinstance(selected_range, (list, tuple)):
        if len(selected_range) == 2:
            start_date, end_date = selected_range
        elif len(selected_range) == 1:
            start_date = end_date = selected_range[0]
        else:
            start_date = end_date = today
    else:
        start_date = end_date = selected_range
    # 요일 표시 준비
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    start_w = weekday_map[start_date.weekday()]
    end_w = weekday_map[end_date.weekday()]

    # 상단 제목 : 2025-11-03(월) ~ 2025-11-30(일)
    st.subheader(
        f"{start_date.strftime('%Y-%m-%d')}({start_w}) ~ "
        f"{end_date.strftime('%Y-%m-%d')}({end_w})"
    )

    if df_daily.empty:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        # 선택한 기간 필터링
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask, ["DATE", "내용", "비고"]].copy().sort_values("DATE")

        if period_df.empty:
            st.info("해당 기간의 보고가 없습니다.")
        else:
            # 날짜를 'YYYY-MM-DD (요일)' 형식으로 표시
            def format_date_with_weekday(d: date) -> str:
                w = weekday_map[d.weekday()]
                return f"{d.strftime('%Y-%m-%d')} ({w})"

            period_df["DATE"] = period_df["DATE"].apply(format_date_with_weekday)

            # 표 스타일 : 헤더 가운데 정렬, 셀은 줄바꿈 그대로 보이도록
            styled = (
                period_df.style
                .set_properties(
                    subset=["내용", "비고"],
                    **{"white-space": "pre-wrap"}  # \n 을 줄바꿈으로 표시
                )
                .set_table_styles(
                    [
                        {
                            "selector": "th",
                            "props": [("text-align", "center")],
                        },
                        {
                            "selector": "th.col_heading",
                            "props": [("text-align", "center")],
                        },
                        {
                            "selector": "table",
                            "props": [
                                ("width", "100%"),           # 가로 전체 사용
                                ("border-collapse", "collapse"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("vertical-align", "top"),   # 여러 줄일 때 위쪽 정렬
                                ("padding", "4px 8px"),
                                ("border", "1px solid #eee"),
                            ],
                        },
                    ]
                )
            )

            # 표 출력 (줄바꿈 포함, 화면 폭에 맞게)
            st.table(styled)

    st.markdown("---")
    st.caption("브라우저 인쇄(Ctrl+P)를 사용해 이 화면을 바로 출력할 수 있습니다.")
