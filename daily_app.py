import streamlit as st
import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta

# ------------------------------------------------------
# App / Secrets
# ------------------------------------------------------

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")

SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]


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
# Utilities
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

        # 2) 한글 형식: 2025년 11월 24일 (월)
        m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return date(y, mth, d)
            except Exception:
                return None

    return None


def format_date_for_display(d: date) -> str:
    """YYYY-MM-DD(요일) 형태로 표시."""
    if not isinstance(d, (date, datetime)):
        return str(d)
    if isinstance(d, datetime):
        d = d.date()
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    w = weekday_map[d.weekday()]
    return d.strftime("%Y-%m-%d") + f"({w})"


def format_date_simple(d) -> str:
    """YYYY-MM-DD 문자열로 표시 (테이블용)."""
    if isinstance(d, (date, datetime)):
        return d.strftime("%Y-%m-%d")
    return str(d)


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

    # 실제 시트 행 번호 (헤더=1행 → 데이터는 2행부터)
    df["__row"] = df.index + 2

    # DATE 파싱
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
# UI Layout
# ------------------------------------------------------

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)

df_daily = load_daily_df()

st.sidebar.title("Daily Report")
mode = st.sidebar.radio("보기 모드", ("1일 보고", "기간 요약"))

today = date.today()

# 1일 보고 기본 날짜: 항상 오늘
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
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD",
    )

    # date_input 반환값 정규화
    if isinstance(selected_range, (list, tuple)):
        if len(selected_range) == 2:
            start_date, end_date = selected_range
        elif len(selected_range) == 1:
            start_date = end_date = selected_range[0]
        else:
            start_date = end_date = today
    else:
        start_date = end_date = selected_range

    # 헤더: YYYY-MM-DD(요일) ~ YYYY-MM-DD(요일)
    start_label = format_date_for_display(start_date)
    end_label = format_date_for_display(end_date)
    st.subheader(f"{start_label} ~ {end_label}")

    if df_daily.empty:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        # 기간 필터링
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask].copy().sort_values("DATE")

        if period_df.empty:
            st.info("해당 기간의 보고가 없습니다.")
        else:
            # 1) DATE는 문자열로
            period_df["DATE"] = period_df["DATE"].apply(format_date_simple)

            # 2) 내용/비고의 줄바꿈을 <br>로 치환 (HTML에서 줄바꿈 유지)
            period_df["내용"] = period_df["내용"].astype(str).str.replace("\n", "<br>")
            period_df["비고"] = period_df["비고"].astype(str).str.replace("\n", "<br>")

            # 3) 우리가 보여줄 컬럼만 선택
            show_df = period_df[["DATE", "내용", "비고"]]

            # 4) pandas HTML 테이블 생성해서 그대로 렌더링
            html_table = show_df.to_html(
                index=False,      # 왼쪽 인덱스 번호 숨김
                escape=False,     # <br> 등을 이스케이프하지 않음
                border=0
            )

            # 5) Streamlit에서 HTML 허용하고 출력
            st.markdown(html_table, unsafe_allow_html=True)

    st.caption("※ 인쇄는 브라우저의 Ctrl+P 기능을 사용하세요.")
