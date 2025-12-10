import streamlit as st
import pandas as pd
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

    # 1) DATE를 안전하게 변환 (이상한 값은 NaT로 처리)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date

    # 2) DATE가 없는 행(잘못된 값, 공백 등)은 버린다
    df = df[~df["DATE"].isna()].copy()

    # 3) 다 버리고 나면 비어 있을 수도 있음 → 그때도 안전하게 빈 DF 반환
    if df.empty:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    # 4) 내용/비고 컬럼 정리
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

mode = st.sidebar.radio("보기 모드", ("1일 보고", "기간 요약"))

today = date.today()

if df_daily.empty:
    default_single = today
    default_range = (today, today)   # 기간도 오늘~오늘
else:
    existing_dates = df_daily["DATE"]
    default_single = today
    default_range = (today, today)   # 항상 처음은 오늘 기준으로

if mode == "1일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD",
    )
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]
else:
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD",
    )

    # date_input 반환값을 항상 (start_date, end_date)로 정규화
    if isinstance(selected_range, (list, tuple)):
        if len(selected_range) == 2:
            start_date, end_date = selected_range
        elif len(selected_range) == 1:
            start_date = end_date = selected_range[0]
        else:
            start_date = end_date = today
    else:
        # 단일 날짜만 선택된 경우
        start_date = end_date = selected_range

st.sidebar.markdown("---")
if st.sidebar.button("오늘로 이동"):
    st.experimental_rerun()

# -------------------------------
# 메인 영역
# -------------------------------

st.title(APP_TITLE)

if mode == "1일 보고":
    # 헤더
    st.subheader(f"{selected_date.strftime('%Y-%m-%d')} 1일 보고")

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
            st.experimental_rerun()

    with btn_cols[1]:
        if has_existing and st.button("내용 비우기"):
            save_daily_entry(selected_date, "", "", df_daily)
            st.info("이 날짜의 내용/비고를 비웠습니다.")
            st.experimental_rerun()

    st.markdown("---")
    st.caption("※ 엔터로 줄바꿈한 내용은 그대로 구글시트 셀 안에 저장됩니다.")

else:
    # 기간 요약 모드
    st.subheader(
        f"{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 기간 요약"
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
            period_df["요약"] = period_df["내용"].apply(first_line)
            period_df["비고여부"] = period_df["비고"].apply(
                lambda x: "O" if isinstance(x, str) and x.strip() else ""
            )

            show_df = period_df[["DATE", "요약", "비고여부"]]

            st.dataframe(show_df, use_container_width=True)
            st.caption(
                "※ 각 날짜의 전체 내용은 '1일 보고' 모드에서 해당 날짜를 선택하여 확인하세요."
            )

    st.markdown("---")
    st.caption("브라우저 인쇄(Ctrl+P)를 사용해 이 화면을 바로 출력할 수 있습니다.")
