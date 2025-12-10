import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from google.oauth2.service_account import Credentials
import gspread

# ---------------------------
# 구글시트 인증
# ---------------------------
def get_worksheet():
    # 구글 시트 접근 범위
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Streamlit secrets 에 저장된 서비스 계정 JSON 로드
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scope
    )

    client = gspread.authorize(creds)

    spreadsheet_id = st.secrets["gsheet"]["spreadsheet_id"]
    worksheet_name = st.secrets["gsheet"]["worksheet_name"]

    sh = client.open_by_key(spreadsheet_id)
    return sh.worksheet(worksheet_name)

@st.cache_data
def load_daily_df():
    ws = get_worksheet()
    records = ws.get_all_records()
    df = pd.DataFrame(records)

    if df.empty:
        return df

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["내용"] = df["내용"].fillna("")
    df["비고"] = df["비고"].fillna("")

    return df

# ---------------------------
# UI 기본 환경
# ---------------------------
st.set_page_config(layout="wide")

st.title("HISMEDI † Daily report")

df_daily = load_daily_df()
today = date.today()

# ---------------------------
# 사이드바
# ---------------------------
st.sidebar.title("Daily Report")
mode = st.sidebar.radio("보기 모드", ("1일 보고", "기간 요약"))

default_single = today
weekday_idx = today.weekday()
week_start = today - timedelta(days=weekday_idx)
week_end = week_start + timedelta(days=6)
default_range = (week_start, week_end)

# ---------------------------
# 날짜 포맷 함수
# ---------------------------
def format_date_for_display(d):
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    if isinstance(d, datetime):
        d = d.date()
    return f"{d.strftime('%Y-%m-%d')}({weekdays[d.weekday()]})"

def format_date_simple(d):
    if isinstance(d, datetime):
        d = d.date()
    return d.strftime("%Y-%m-%d")

def format_date_with_weekday(d):
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    if isinstance(d, datetime):
        d = d.date()
    return f"{d.strftime('%Y-%m-%d')} ({weekdays[d.weekday()]})"

# ---------------------------
# 1일 보고
# ---------------------------
if mode == "1일 보고":
    selected_date = st.sidebar.date_input(
        "날짜 선택",
        value=default_single,
        format="YYYY-MM-DD"
    )

    st.subheader(f"{format_date_for_display(selected_date)} 1일 보고")

    existing_row = None
    if not df_daily.empty:
        match = df_daily[df_daily["DATE"] == selected_date]
        if not match.empty:
            existing_row = match.iloc[0]

    content_input = st.text_area(
        "내용",
        value=existing_row["내용"] if existing_row is not None else "",
        height=300,
        placeholder="이 날의 전체 업무를 입력하세요. (줄바꿈 가능)"
    )
    note_input = st.text_area(
        "비고 (선택)",
        value=existing_row["비고"] if existing_row is not None else "",
        height=150,
        placeholder="추가 메모 등이 있을 때만 입력하세요."
    )

    if st.button("저장"):
        ws = get_worksheet()
        records = ws.get_all_records()
        df = pd.DataFrame(records)

        if df.empty:
            new_row = {
                "DATE": selected_date.strftime("%Y-%m-%d"),
                "내용": content_input,
                "비고": note_input,
            }
            ws.append_row(list(new_row.values()))
        else:
            df_dates = pd.to_datetime(df["DATE"], errors="coerce").dt.date
            if selected_date in set(df_dates):
                row_idx = df_dates[df_dates == selected_date].index[0] + 2
                ws.update_cell(row_idx, 2, content_input)
                ws.update_cell(row_idx, 3, note_input)
            else:
                ws.append_row([
                    selected_date.strftime("%Y-%m-%d"),
                    content_input,
                    note_input
                ])

        st.success("저장되었습니다!")
        st.rerun()

    st.caption("※ 엔터로 줄바꿈한 내용은 그대로 구글시트에 저장됩니다.")

# ---------------------------
# 기간 요약 (표 방식)
# ---------------------------
else:
    selected_range = st.sidebar.date_input(
        "기간 선택",
        value=default_range,
        format="YYYY-MM-DD"
    )

    if isinstance(selected_range, (list, tuple)):
        if len(selected_range) == 2:
            start_date, end_date = selected_range
        else:
            start_date = end_date = selected_range[0]
    else:
        start_date = end_date = selected_range

    st.subheader(f"{format_date_for_display(start_date)} ~ {format_date_for_display(end_date)}")

    if df_daily.empty:
        st.info("아직 데이터가 없습니다.")
    else:
        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask].copy().sort_values("DATE")

        if period_df.empty:
            st.info("해당 기간에 보고가 없습니다.")
        else:
            period_df["DATE"] = period_df["DATE"].apply(format_date_with_weekday)
            period_df["내용"] = period_df["내용"].str.replace("\n", "<br>")
            period_df["비고"] = period_df["비고"].str.replace("\n", "<br>")

            show_df = period_df[["DATE", "내용", "비고"]]

            html_table = show_df.to_html(
                index=False,
                escape=False,
                border=0
            )

            styled_html = f"""
            <style>
            table {{
                width: 100%;
                border-collapse: collapse;
                table-layout: fixed;
                font-size: 0.95rem;
            }}
            th {{
                text-align: center !important;
                padding: 8px;
                border-bottom: 2px solid #ccc;
                background-color: #fafafa;
            }}
            td {{
                vertical-align: top;
                padding: 6px 8px;
                border: 1px solid #eee;
                word-wrap: break-word;
            }}
            </style>
            {html_table}
            """

            st.markdown(styled_html, unsafe_allow_html=True)

    st.caption("※ 인쇄는 Ctrl+P 를 사용하세요.")
