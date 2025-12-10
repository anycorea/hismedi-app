import streamlit as st
import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime

# -------------------------------
# 기본 설정
# -------------------------------

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")

st.set_page_config(
    page_title=APP_TITLE,
    layout="wide",
)

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = st.secrets["gsheet"].get("worksheet_name", "Daily")
SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]


# -------------------------------
# 구글시트 연결 함수
# -------------------------------

@st.cache_resource
def get_gspread_client():
    """서비스 계정으로 gspread 클라이언트 생성"""
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPE,
    )
    client = gspread.authorize(credentials)
    return client


def get_worksheet():
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet(SHEET_NAME)
    return ws


@st.cache_data(ttl=60)
def load_daily_df():
    ws = get_worksheet()
    records = ws.get_all_records()

    # 데이터가 아예 없으면 빈 DF 반환
    if not records:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df = pd.DataFrame(records)

    # DATE 컬럼 필수 체크
    if "DATE" not in df.columns:
        st.error("Daily 시트의 헤더에 'DATE' 열이 필요합니다.")
        st.stop()

    # 시트 상의 실제 행 번호 (헤더가 1행이므로 데이터는 2행부터)
    df["__row"] = df.index + 2

    # ---- 여기서 직접 파싱 ----
    def parse_date_cell(v):
        # 이미 date/datetime 이면 그대로(또는 date로) 사용
        if isinstance(v, date):
            return v
        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            s = v.strip()

            # 1) ISO 형식 시도: 2025-11-24
            try:
                return date.fromisoformat(s)
            except Exception:
                pass

            # 2) 한글 형식: 2025년 11월 24일 (월)  /  2025년11월24일 등
            m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
            if m:
                y, mth, d = map(int, m.groups())
                try:
                    return date(y, mth, d)
                except Exception:
                    return None

        # 그 외(빈칸, 이상값)는 사용 안 함
        return None

    parsed = df["DATE"].apply(parse_date_cell)

    # 파싱 성공한 행만 남기기
    df = df[parsed.notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df["DATE"] = parsed

    # 내용/비고 컬럼 정리
    for col in ["내용", "비고"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    return df


def save_daily_entry(selected_date: date, content: str, note: str, df: pd.DataFrame):
    """
    지정한 날짜의 내용을 Daily 시트에 저장.
    - 있으면 B, C열 업데이트
    - 없으면 새 행 append
    """
    ws = get_worksheet()

    if not df.empty:
        mask = df["DATE"] == selected_date
    else:
        mask = pd.Series(dtype=bool)

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


# -------------------------------
# UI: 사이드바 (모드 + 날짜/기간 선택)
# -------------------------------

st.sidebar.title("Daily Report")

df_daily = load_daily_df()

# 보기 모드 선택
mode = st.sidebar.radio("보기 모드", ("1일 보고", "기간 요약"))

today = date.today()

# --------------------------------------
# 날짜 기본값 (항상 오늘)
# --------------------------------------
default_single = today

# --------------------------------------
# 기간 기본값: "이번 주 월요일 ~ 일요일" 자동 설정
# --------------------------------------
weekday_idx = today.weekday()  # 월=0
week_start = today - timedelta(days=weekday_idx)
week_end = week_start + timedelta(days=6)
default_range = (week_start, week_end)

# --------------------------------------
# 1일 보고 모드
# --------------------------------------
if mode == "1일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD",
    )

    # Streamlit은 list로 반환하는 경우가 있으므로 보호
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]

# --------------------------------------
# 기간 요약 모드
# --------------------------------------
else:
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD",
    )

    # (start, end) 형태로 정규화
    if isinstance(selected_range, (list, tuple)):
        if len(selected_range) == 2:
            start_date, end_date = selected_range
        elif len(selected_range) == 1:
            start_date = end_date = selected_range[0]
        else:
            start_date = end_date = today
    else:
        start_date = end_date = selected_range


# -------------------------------
# 메인 영역
# -------------------------------

st.title(APP_TITLE)

if mode == "1일 보고":
    # 헤더
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    weekday = weekday_map[selected_date.weekday()]
    st.subheader(f"{selected_date.strftime('%Y-%m-%d')}({weekday})")

    # 현재 선택 날짜 데이터
    if not df_daily.empty:
        mask = df_daily["DATE"] == selected_date
        if mask.any():
            row = df_daily.loc[mask].iloc[0]
            default_content = row.get("내용", "") or ""
            default_note = row.get("비고", "") or ""
            has_existing = True
        else:
            default_content = ""
            default_note = ""
            has_existing = False
    else:
        default_content = ""
        default_note = ""
        has_existing = False

    col_left, col_right = st.columns([3, 1])

    with col_left:
        content = st.text_area(
            "내용",
            value=default_content,
            height=350,
            placeholder="이 날의 전체 업무를 한 번에 요약해서 적어 주세요.\n(엔터로 줄바꿈)",
        )

    with col_right:
        note = st.text_area(
            "비고 (선택)",
            value=default_note,
            height=350,
            placeholder="추가 메모, 특이사항 등이 있을 때만 적어도 충분합니다.",
        )

    btn_cols = st.columns([1, 1, 6])
    with btn_cols[0]:
        if st.button("저장", type="primary"):
            save_daily_entry(selected_date, content, note, df_daily)
            st.success("저장되었습니다.")
            st.rerun()

    with btn_cols[1]:
        if has_existing and st.button("내용 비우기"):
            save_daily_entry(selected_date, "", "", df_daily)
            st.info("이 날짜의 내용/비고를 비웠습니다.")
            st.rerun()

    st.markdown("---")
    st.caption("※ 엔터로 줄바꿈한 내용은 그대로 구글시트 셀 안에 저장됩니다.")

else:
    # 기간 요약 모드
    weekday_map = ["월", "화", "수", "목", "금", "토", "일"]
    start_w = weekday_map[start_date.weekday()]
    end_w = weekday_map[end_date.weekday()]

    st.subheader(
        f"{start_date.strftime('%Y-%m-%d')}({start_w}) ~ "
        f"{end_date.strftime('%Y-%m-%d')}({end_w})"
    )

    if df_daily.empty:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask].copy().sort_values("DATE")

        if period_df.empty:
            st.info("해당 기간의 보고가 없습니다.")
        else:
            def first_line(text: str) -> str:
                if not isinstance(text, str):
                    return ""
                lines = text.splitlines()
                return lines[0] if lines else ""

            period_df["DATE"] = period_df["DATE"].apply(
                lambda d: d.strftime("%Y-%m-%d")
            )

            # 날짜(문자열) 정리
            period_df["DATE"] = period_df["DATE"].apply(lambda d: d.strftime("%Y-%m-%d"))

            # 전체 내용 그대로 보여주기
            show_df = period_df[["DATE", "내용", "비고"]]
            st.dataframe(show_df, use_container_width=True)
            st.caption(
                "※ 각 날짜의 전체 내용은 '1일 보고' 모드에서 해당 날짜를 선택하여 확인하세요."
            )

    st.markdown("---")
    st.caption("브라우저 인쇄(Ctrl+P)를 사용해 이 화면을 바로 출력할 수 있습니다.")
