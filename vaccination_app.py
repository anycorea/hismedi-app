import streamlit as st
import gspread
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials


# ============================================================
# 기본 설정
# ============================================================
st.set_page_config(page_title="예방접종 예진표", page_icon="💉", layout="centered")

TZ = ZoneInfo("Asia/Seoul")

SHEET_HEADERS = [
    "ID", "DATE", "성명", "주민번호", "성별", "생년월일", "외국인번호", "집전화", "휴대전화", "체중",
    "접종동의", "알림동의", "이상동의", "1", "1상세", "2", "2상세", "3", "3상세", "4", "5",
    "6", "6상세", "7", "8", "9", "9상세", "10", "11", "11상세", "작성자", "관계"
]


# ============================================================
# 화면 스타일
# ============================================================
st.markdown("""
<style>
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"] { display: none !important; }
    .block-container { max-width: 760px; padding-top: 1.2rem; padding-bottom: 4rem; }
    h1 { text-align: center; font-size: 2rem !important; margin-bottom: 0.3rem !important; }
    .form-description { text-align: center; color: #666; margin-bottom: 1.5rem; line-height: 1.6; }
    .section-title { font-size: 1.25rem; font-weight: 700; margin-top: 2rem; margin-bottom: 0.8rem; padding-bottom: 0.45rem; border-bottom: 2px solid #333; }
    .question-text { font-weight: 600; line-height: 1.55; margin-bottom: 0.2rem; }
    .required { color: #d32f2f; font-weight: 700; }
    .notice-box { padding: 1rem; border: 1px solid #ddd; border-radius: 8px; background: #fafafa; font-size: 0.92rem; line-height: 1.6; margin-bottom: 1rem; }
    div[data-testid="stRadio"] { margin-bottom: 0.6rem; }
    div[data-testid="stTextInput"] { margin-bottom: 0.3rem; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Google Sheets
# ============================================================
@st.cache_resource
def get_worksheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    return spreadsheet.worksheet(st.secrets["gsheet"]["worksheet_name"])


# ============================================================
# 공통 함수
# ============================================================
def clean(value):
    return "" if value is None else str(value).strip()


def digits_only(value):
    return "".join(ch for ch in str(value) if ch.isdigit())


def question(number, text, detail_text=None):
    st.markdown(f'<div class="question-text">{number}. {text} <span class="required">*</span></div>', unsafe_allow_html=True)
    answer = st.radio(f"{number}번 답변", ["예", "아니오"], index=None, horizontal=True, key=f"q{number}", label_visibility="collapsed")
    detail = st.text_input(detail_text, key=f"q{number}_detail") if detail_text and answer == "예" else ""
    st.divider()
    return answer, detail


# ============================================================
# 제출 완료 화면
# ============================================================
if st.session_state.get("submitted", False):
    st.title("💉 예방접종 예진표")
    st.success("예진표가 정상적으로 제출되었습니다.")
    st.write("작성하신 내용이 접수되었습니다. 접종 전 의료진의 안내에 따라 진료 및 예진을 받아주세요.")

    if st.button("새 예진표 작성", use_container_width=True):
        st.session_state.clear()
        st.rerun()

    st.stop()


# ============================================================
# 제목 / 개인정보 안내
# ============================================================
st.title("💉 예방접종 예진표")
st.markdown('<div class="form-description">안전한 예방접종을 위하여 아래 질문사항을 잘 읽어보시고<br>정확하게 작성하여 주시기 바랍니다.</div>', unsafe_allow_html=True)

st.markdown('<div class="section-title">개인정보 처리 안내</div>', unsafe_allow_html=True)
st.markdown("""
<div class="notice-box">
예방접종 업무를 위하여 주민등록번호 등 개인정보 및 민감정보가 수집될 수 있습니다.<br><br>
입력하신 정보는 예방접종 예진 및 관련 업무를 위하여 사용됩니다.<br>
아래 내용을 확인한 후 예진표를 작성해 주세요.
</div>
""", unsafe_allow_html=True)

privacy_confirm = st.checkbox("위 개인정보 처리 안내를 확인하였습니다.")

if not privacy_confirm:
    st.info("개인정보 처리 안내 확인 후 예진표를 작성할 수 있습니다.")
    st.stop()


# ============================================================
# 예진표 입력
# ============================================================
with st.form("vaccination_form", clear_on_submit=False):

    # 인적사항
    st.markdown('<div class="section-title">접종 대상자 인적 사항</div>', unsafe_allow_html=True)

    name = st.text_input("성명 *", placeholder="접종 대상자의 성명을 입력해주세요.")
    rrn = st.text_input("주민등록번호", placeholder="예: 900101-1234567", max_chars=14)
    gender = st.radio("성별 *", ["남", "여"], index=None, horizontal=True)
    birth_date = st.date_input("실제 생년월일 *", value=None, min_value=date(1900, 1, 1), max_value=date.today(), format="YYYY-MM-DD")
    foreigner_no = st.text_input("외국인 등록번호", placeholder="외국인인 경우 입력해주세요.")

    col1, col2 = st.columns(2)
    with col1: home_phone = st.text_input("전화번호 (집)", placeholder="선택 입력")
    with col2: mobile_phone = st.text_input("휴대전화 *", placeholder="010-0000-0000")

    weight = st.number_input("체중 (kg)", min_value=0.0, max_value=300.0, value=None, step=0.1, placeholder="체중을 입력해주세요.")


    # 동의사항
    st.markdown('<div class="section-title">예방접종 업무를 위한 동의 사항</div>', unsafe_allow_html=True)

    st.markdown("**1. 예방접종 내역 사전 확인**  \n예방접종을 하기 전에 접종 대상자의 예방접종 내역을 예방접종통합관리시스템으로 사전 확인하는 것에 동의합니다.")
    vaccination_consent = st.radio("접종동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")
    st.divider()

    st.markdown("**2. 다음 접종 및 완료 여부 알림**  \n예방접종의 다음 접종 및 완료 여부에 관한 정보를 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
    notification_consent = st.radio("알림동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")
    st.divider()

    st.markdown("**3. 예방접종 후 이상반응 알림**  \n예방접종 후 이상반응 발생 여부와 관련된 알림을 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
    adverse_consent = st.radio("이상동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")


    # 확인사항
    st.markdown('<div class="section-title">접종 대상자에 대한 확인 사항</div>', unsafe_allow_html=True)
    st.caption("각 질문에 반드시 '예' 또는 '아니오'를 선택해주세요.")

    q1, q1_detail = question(1, "최근 1개월 이내에 받은 예방접종이 있습니까?", "그렇다면 예방접종명을 적어주세요.")
    q2, q2_detail = question(2, "과거에 예방접종 후 이상반응이 나타나서 치료를 받은 적이 있습니까?", "그렇다면 이상반응과 해당 예방접종명을 적어주세요.")
    q3, q3_detail = question(3, "오늘 아픈 곳이 있습니까?", "그렇다면 아픈 증상을 적어주세요.")
    q4, _ = question(4, "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?")
    q5, _ = question(5, "약이나 음식물(예: 계란) 혹은 백신 접종으로 두드러기, 알레르기 증상(예: 발진, 아나필락시스: 쇼크, 호흡곤란, 의식소실, 입술/입안의 부종 등)을 보인 적이 있습니까?")
    q6, q6_detail = question(6, "암, 백혈병 혹은 면역계 질환이 있습니까?", "그렇다면 병명을 적어주세요.")
    q7, _ = question(7, "최근 3개월 이내에 스테로이드제, 항암제, 방사선 치료를 받은 적이 있습니까?")
    q8, _ = question(8, "최근 1년 동안 수혈을 받았거나 면역글로불린을 투여받은 적이 있습니까?")
    q9, q9_detail = question(9, "(코로나19) 혈액응고장애를 앓고 있거나, 항응고제를 복용 중이십니까?", "그렇다면 질환명 또는 약 종류를 적어주세요.")
    q10, _ = question(10, "경련을 한 적이 있거나 기타 뇌신경계 질환(예: 길랭-바레 증후군 포함)이 있습니까?")
    q11, q11_detail = question(11, "그 외 선천성 기형, 천식 및 폐질환, 심장질환, 신장질환, 간질환, 당뇨 및 내분비 질환, 혈액 질환(혈액응고장애 외)으로 진찰 받거나 치료 받은 일이 있습니까?", "그렇다면 병명을 적어주세요.")


    # 작성자
    st.markdown('<div class="section-title">작성자 확인</div>', unsafe_allow_html=True)
    st.write("의사의 진찰결과와 이상반응에 대한 설명을 듣고 예방접종을 하겠습니다.")

    writer = st.text_input("본인(법정대리인, 보호자) 성명 *", placeholder="작성자의 성명을 입력해주세요.")
    relationship = st.text_input("접종 대상자와의 관계 *", placeholder="예: 본인, 부, 모, 배우자")
    final_confirm = st.checkbox("위 내용을 확인하였으며 작성한 내용이 사실과 다름없음을 확인합니다.")

    submitted = st.form_submit_button("예진표 제출", use_container_width=True, type="primary")


# ============================================================
# 제출 검사 / 저장
# ============================================================
if submitted:
    errors = []

    if not clean(name): errors.append("성명을 입력해주세요.")
    if gender is None: errors.append("성별을 선택해주세요.")
    if birth_date is None: errors.append("실제 생년월일을 입력해주세요.")
    if not clean(mobile_phone): errors.append("휴대전화를 입력해주세요.")
    if clean(rrn) and len(digits_only(rrn)) != 13: errors.append("주민등록번호를 정확히 입력해주세요.")

    if vaccination_consent is None: errors.append("예방접종 내역 사전 확인 동의 여부를 선택해주세요.")
    if notification_consent is None: errors.append("다음 접종 및 완료 여부 알림 동의 여부를 선택해주세요.")
    if adverse_consent is None: errors.append("예방접종 후 이상반응 알림 동의 여부를 선택해주세요.")

    answers = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11]
    for number, answer in enumerate(answers, 1):
        if answer is None: errors.append(f"{number}번 확인사항에 답변해주세요.")

    details = [(1, q1, q1_detail), (2, q2, q2_detail), (3, q3, q3_detail), (6, q6, q6_detail), (9, q9, q9_detail), (11, q11, q11_detail)]
    for number, answer, detail in details:
        if answer == "예" and not clean(detail): errors.append(f"{number}번 질문의 상세 내용을 입력해주세요.")

    if not clean(writer): errors.append("작성자 성명을 입력해주세요.")
    if not clean(relationship): errors.append("접종 대상자와의 관계를 입력해주세요.")
    if not final_confirm: errors.append("최종 확인 항목에 체크해주세요.")

    if errors:
        st.error("입력하지 않은 항목이 있습니다.\n\n" + "\n\n".join(f"• {error}" for error in errors))

    else:
        try:
            worksheet = get_worksheet()
            now = datetime.now(TZ)

            record = {
                "ID": uuid.uuid4().hex, "DATE": now.strftime("%Y-%m-%d %H:%M:%S"),
                "성명": clean(name), "주민번호": clean(rrn), "성별": clean(gender),
                "생년월일": birth_date.strftime("%Y-%m-%d"), "외국인번호": clean(foreigner_no),
                "집전화": clean(home_phone), "휴대전화": clean(mobile_phone),
                "체중": "" if weight is None else str(weight),

                "접종동의": clean(vaccination_consent), "알림동의": clean(notification_consent), "이상동의": clean(adverse_consent),

                "1": clean(q1), "1상세": clean(q1_detail),
                "2": clean(q2), "2상세": clean(q2_detail),
                "3": clean(q3), "3상세": clean(q3_detail),
                "4": clean(q4), "5": clean(q5),
                "6": clean(q6), "6상세": clean(q6_detail),
                "7": clean(q7), "8": clean(q8),
                "9": clean(q9), "9상세": clean(q9_detail),
                "10": clean(q10),
                "11": clean(q11), "11상세": clean(q11_detail),
                "작성자": clean(writer), "관계": clean(relationship)
            }

            row = [record.get(header, "") for header in SHEET_HEADERS]
            worksheet.append_row(row, value_input_option="USER_ENTERED")

            st.session_state.submitted = True
            st.rerun()

        except Exception as e:
            st.error("예진표 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
            st.exception(e)
