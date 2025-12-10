import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime

# -------------------------------
# 기본 설정
# -------------------------------
st.set_page_config(
    page_title="Daily Report",
    layout="wide",
)

SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Daily"  # 시트 탭 이름 (이미 만드신 이름과 동일하게)

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
    sh = client.open_by_key(st.secrets["SPREADSHEET_ID"])
    ws = sh.worksheet(SHEET_NAME)
    return ws


@st.cache_data(ttl=60)
def load_daily_df():
    """
    Daily 시트 전체를 DataFrame으로 로드.
    DATE 컬럼은 date 객체,
    __row 컬럼은 실제 시트의 행 번호.
    """
    ws = get_worksheet()
    records = ws.get_all_records(numericise_dates=True)  # 날짜를 datetime으로 가져옴

    if not records:
        # 데이터가 한 줄도 없을 때 (헤더만 있는 상태)
        # 빈 데이터프레임이지만, 사용 시에는 이 케이스를 따로 처리
        df = pd.DataFrame(columns=["DATE", "내용", "비고"])
        return df

    df = pd.DataFrame(records)

    if "DATE" not in df.columns:
        st.error("Daily 시트의 헤더에 'DATE' 컬럼이 필요합니다.")
        st.stop()

    # DATE가 datetime으로 들어왔다면 date로 바꿔주기
    if isinstance(df.loc[0, "DATE"], datetime):
        df["DATE"] = df["DATE"].dt.date
    else:
        # 혹시 문자열이면 파싱 시도 (가능하면 ISO yyyy-mm-dd 형식 사용 권장)
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date

    # 실제 시트의 행 번호 (헤더가 1행이므로 2행부터 데이터)
    df["__row"] = df.index + 2

    # 내용/비고가 없을 때 NaN 방지
    if "내용" not in df.columns:
        df["내용"] = ""
    if "비고" not in df.columns:
        df["비고"] = ""

    return df


def save_daily_entry(selected_date: date, content: str, note: str, df: pd.DataFrame):
    """
    지정한 날짜의 내용을 Daily 시트에 저장.
    - 있으면 B, C열 업데이트
    - 없으면 새 행 append
    """
    ws = get_worksheet()

    # 이미 데이터가 있을 경우
    if not df.empty:
        mask = df["DATE"] == selected_date
    else:
        mask = pd.Series(dtype=bool)

    if not df.empty and mask.any():
        # 기존 행 업데이트
        row_number = int(df.loc[mask, "__row"].iloc[0])
        # 2열(B) = 내용, 3열(C) = 비고
        ws.update_cell(row_number, 2, content)
        ws.update_cell(row_number, 3, note)
    else:
        # 새 행 추가
        ws.append_row(
            [selected_date.isoformat(), content, note],
            value_input_option="USER_ENTERED",
        )

    # 캐시된 데이터프레임 초기화
    load_daily_df.clear()


# -------------------------------
# UI: 사이드바 (모드 + 날짜/기간 선택)
# -------------------------------
df_daily = load_daily_df()

st.sidebar.title("Daily Report")

mode = st.sidebar.radio("보기 모드", ("일일 보고", "기간 요약"))

today = date.today()

if df_daily.empty:
    default_single = today
    default_range = (today, today)
else:
    existing_dates = df_daily["DATE"]
    default_single = today if today in set(existing_dates) else existing_dates.max()
    default_range = (existing_dates.min(), existing_dates.max())

if mode == "일일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD",
    )
    # date_input 결과가 list/tuple이 될 수 있으니 단일 날짜만 보장
    if isinstance(selected_date, (list, tuple)):
        selected_date = selected_date[0]
else:
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD",
    )
    # (start, end) 형태
    if isinstance(selected_range, (list, tuple)):
        start_date, end_date = selected_range
    else:
        # 사용자가 단일 날짜만 선택한 경우
        start_date = end_date = selected_range

st.sidebar.markdown("---")
if st.sidebar.button("오늘로 이동"):
    # 버튼 누르면 오늘 날짜 기준으로 다시 로드
    # (실제 값은 위에서 default를 today로 잡게 되어 있고,
    #  Streamlit에서는 위젯 상태를 유지하므로, 이 버튼은 안내용에 가깝습니다.)
    st.experimental_rerun()

# -------------------------------
# 메인 영역
# -------------------------------
st.title("1일 업무보고")

if mode == "일일 보고":
    # 헤더
    st.subheader(f"{selected_date.strftime('%Y-%m-%d')} 일일 보고")

    # 현재 선택 날짜에 대한 기존 데이터 찾기
    if not df_daily.empty:
        mask = df_daily["DATE"] == selected_date
        if mask.any():
            row = df_daily.loc[mask].iloc[0]
            default_content = str(row.get("내용", "") or "")
            default_note = str(row.get("비고", "") or "")
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
            # 이 날짜의 내용을 공란으로 업데이트 (행은 유지)
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
            # 요약용 컬럼 만들기 (내용의 첫 줄만)
            def first_line(text):
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
                "※ 각 날짜의 전체 내용은 '일일 보고' 모드에서 해당 날짜를 선택하여 확인하세요."
            )

    st.markdown("---")
    st.caption("브라우저 인쇄(Ctrl+P)를 사용해 이 화면을 바로 출력할 수 있습니다.")
