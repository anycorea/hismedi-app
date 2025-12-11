import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
from typing import Any, Optional
import calendar
import re
import time
import streamlit.components.v1 as components

# ------------------------------------------------------
# App / Secrets
# ------------------------------------------------------

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")  # 현재는 미사용이지만 향후 대비해서 유지

SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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
# Google Sheets Connection
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
# 외부 진료시간표 시트 로딩 / 미리보기
# ------------------------------------------------------

@st.cache_data(ttl=300)
def load_timetable_df() -> pd.DataFrame:
    """
    진료시간표 시트를 DataFrame으로 불러온 뒤,
    1행, 2행은 숨기고(제거) 나머지만 반환.
    인덱스/열 이름 없이 '인쇄 미리보기'처럼 쓸 예정.
    """
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid_raw = st.secrets["gsheet_preview"].get("gid", "0")

    try:
        gid = int(gid_raw)
    except ValueError:
        gid = 0

    client = get_gspread_client()
    sh = client.open_by_key(sheet_id)
    ws = sh.get_worksheet_by_id(gid)

    values = ws.get_all_values()  # 2차원 리스트

    # 1, 2행 숨기기 → 0,1 인덱스 제거
    if len(values) <= 2:
        return pd.DataFrame()

    data_rows = values[2:]  # 3행부터 끝까지

    # DataFrame으로 변환
    df = pd.DataFrame(data_rows)

    # 완전히 빈 열은 제거 (전부 "" 인 경우)
    df = df.replace("", pd.NA)
    df = df.dropna(axis=1, how="all")
    df = df.fillna("")

    return df


def render_sheet_preview() -> None:
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")

    # 구글 시트 미니멀 뷰 (셀합치기 포함, UI 최소화)
    src_view = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/htmlview"
        f"?gid={gid}&rm=minimal"
    )

    # 새 창에서 열기용 편집 URL
    src_open = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"
    )

    st.markdown(
        f"""
        <div style="
            margin-top: 1.2rem;
            margin-bottom: 0.4rem;
            padding: 0.8rem 1.0rem;
            border-radius: 0.75rem;
            border: 1px solid #d4d4ff;
            background: linear-gradient(135deg, #f4f5ff, #ffffff);
            display: flex;
            justify-content: space-between;
            align-items: center;
        ">
            <div>
                <div style="font-size: 1.05rem; font-weight: 700; color: #1f2933;">
                    🗓 진료시간표
                </div>
                <div style="font-size: 0.85rem; color: #6b7280; margin-top: 2px;">
                    외래 진료 스케줄 확인용 안내표입니다.
                </div>
            </div>
            <a href="{src_open}" target="_blank" style="
                font-size: 0.82rem;
                text-decoration: none;
                padding: 0.35rem 0.9rem;
                border-radius: 999px;
                border: 1px solid #4f46e5;
                color: #4f46e5;
                background: #eef2ff;
                font-weight: 500;
            ">
                새 창에서 열기 ↗
            </a>
        </div>
        """,
        unsafe_allow_html=True,
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

today = date.today()

# 1일 보고 기본 날짜
default_single = today

# ------------------------------------------------------
# 사이드바
# ------------------------------------------------------

with st.sidebar:
    st.markdown(f"<h2 style='font-size:1.6rem; font-weight:700;'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    mode = st.radio("", ("1일 보고", "월별 보기"))
    show_timetable = st.checkbox("진료시간표 보기", value=True)

# --------------------------- 1일 보고 모드 ---------------------------

if mode == "1일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD",
    )
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]

    # 상단 제목
    st.markdown(f"## {selected_date.year}년 {selected_date.month}월 {selected_date.day}일")

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

    # 보고작성 영역 (조금 낮은 높이)
    col_left, col_right = st.columns([3, 1])

    with col_left:
        content = st.text_area(
            "내용",
            height=260,
            value=default_content,
            placeholder="이 날의 업무를 자유롭게 작성하세요.\n(엔터로 줄바꿈)",
        )

    with col_right:
        note = st.text_area(
            "비고 (선택)",
            height=260,
            value=default_note,
            placeholder="특이사항이 있을 때만 작성하세요.",
        )

    # 버튼을 오른쪽 아래에 모아서 배치
    btn_spacer, btn_save, btn_clear = st.columns([6, 1, 1])

    with btn_save:
        if st.button("저장", type="primary", use_container_width=True):
            save_daily_entry(selected_date, content, note, df_daily)
            st.session_state["flash"] = ("success", "저장되었습니다.")
            st.rerun()

    with btn_clear:
        if has_existing and st.button("내용 비우기", use_container_width=True):
            save_daily_entry(selected_date, "", "", df_daily)
            st.session_state["flash"] = (
                "info",
                "이 날짜의 내용/비고를 모두 비웠습니다.",
            )
            st.rerun()

    # ---------------- 진료시간표 (보고작성 아래쪽) ----------------
    if show_timetable:

        # 항상 표시되는 안내 문구
        st.caption("※ 진료시간표는 구글시트에서 불러오는 중일 수 있습니다.")

        # 스피너는 최소 0.5초 동안 보이도록 강제
        with st.spinner("진료시간표를 불러오는 중..."):
            time.sleep(0.5)
            render_sheet_preview()

# --------------------------- 월별 보기 모드 ---------------------------
else:
    if df_daily.empty:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        # 실제 데이터가 있는 (연, 월)만 모아서 한 박스에서 선택
        ym_set = {(d.year, d.month) for d in df_daily["DATE"]}
        ym_options = sorted(ym_set, reverse=True)  # 최근 연/월이 위로 오도록

        default_ym = (today.year, today.month)
        if default_ym in ym_options:
            default_index = ym_options.index(default_ym)
        else:
            default_index = 0

        selected_ym = st.sidebar.selectbox(
            "월 선택",
            ym_options,
            index=default_index,
            format_func=lambda ym: f"{ym[0]}년 {ym[1]:02d}월",
        )
        year, month = selected_ym

        # 선택한 월의 시작/끝 날짜 계산
        start_date = date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        end_date = date(year, month, last_day)

        start_w = WEEKDAY_MAP[start_date.weekday()]
        end_w = WEEKDAY_MAP[end_date.weekday()]

        # 상단 제목
        st.markdown(f"## {year}년 {month:02d}월")

        # 해당 월 데이터 필터링
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = (
            df_daily.loc[mask, ["DATE", "내용", "비고"]]
            .copy()
            .sort_values("DATE")
        )

        if period_df.empty:
            st.info("해당 월에 작성된 보고가 없습니다.")
        else:
            # 날짜 표시: YYYY-MM-DD (요일)
            period_df["DATE"] = period_df["DATE"].apply(format_date_with_weekday)

            styled = (
                period_df.style.set_properties(
                    subset=["내용", "비고"],
                    **{"white-space": "pre-wrap"},
                ).set_table_styles(
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
                                ("width", "100%"),
                                ("border-collapse", "collapse"),
                            ],
                        },
                        {
                            "selector": "td",
                            "props": [
                                ("vertical-align", "top"),
                                ("padding", "4px 8px"),
                                ("border", "1px solid #eee"),
                            ],
                        },
                    ]
                )
            )

            st.table(styled)


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
