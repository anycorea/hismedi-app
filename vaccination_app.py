import streamlit as st
import gspread
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials

# ----------------------------------------------------
# 1. 기본 설정
# ----------------------------------------------------
st.set_page_config(
    page_title="예방접종 예진표",
    page_icon="💉",
    layout="centered"
)

SHEET_HEADERS = [
    "ID", "DATE", "성명", "주민번호", "성별", "생년월일", "외국인번호",
    "집전화", "휴대전화", "체중",
    "접종동의", "알림동의", "이상동의",
    "1", "1상세", "2", "2상세", "3", "3상세",
    "4", "5", "6", "6상세", "7", "8", "9", "9상세",
    "10", "11", "11상세", "작성자", "관계"
]

# ----------------------------------------------------
# 2. Google Sheets 연결
# ----------------------------------------------------
@st.cache_resource
def get_worksheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=scopes
    )

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(
        st.secrets["gsheet"]["spreadsheet_id"]
    )

    return spreadsheet.worksheet(
        st.secrets["gsheet"]["worksheet_name"]
    )


# ----------------------------------------------------
# 3. 테스트 화면
# ----------------------------------------------------
st.title("💉 예방접종 예진표")
st.caption("Google Sheets 연결 테스트")

st.info(
    "현재는 실제 예진표 작성 화면이 아닙니다.\n\n"
    "Google Sheets 저장 기능을 확인하기 위한 테스트 화면입니다."
)

test_name = st.text_input(
    "테스트 성명",
    placeholder="예: 홍길동"
)

if st.button("Google Sheet 저장 테스트", use_container_width=True):

    if not test_name.strip():
        st.warning("테스트 성명을 입력해주세요.")

    else:
        try:
            worksheet = get_worksheet()

            now = datetime.now(ZoneInfo("Asia/Seoul"))

            row = [""] * len(SHEET_HEADERS)

            row[SHEET_HEADERS.index("ID")] = uuid.uuid4().hex
            row[SHEET_HEADERS.index("DATE")] = now.strftime("%Y-%m-%d %H:%M:%S")
            row[SHEET_HEADERS.index("성명")] = test_name.strip()

            worksheet.append_row(
                row,
                value_input_option="USER_ENTERED"
            )

            st.success(
                f"✅ [{test_name.strip()}] 테스트 데이터가 저장되었습니다."
            )

        except Exception as e:
            st.error("❌ Google Sheets 저장에 실패했습니다.")
            st.exception(e)
