import streamlit as st
import streamlit.components.v1 as components
import gspread
import uuid
import base64
from datetime import datetime, date
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from vaccination_pdf import create_print_pdf


# ============================================================
# 기본 설정
# ============================================================
st.set_page_config(page_title="예방접종 예진표", page_icon="💉", layout="wide")

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
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"],
    [data-testid="stToolbar"], [data-testid="stDecoration"],
    [data-testid="stStatusWidget"] { display: none !important; }

    .block-container { padding-top: 1.2rem !important; padding-bottom: 2rem !important; }
    h1 { text-align: center; font-size: 2rem !important; margin-bottom: 0.3rem !important; }

    .patient-page { max-width: 760px; margin: auto; }
    .form-description { text-align: center; color: #666; margin-bottom: 1.5rem; line-height: 1.6; }

    .section-title {
        font-size: 1.25rem; font-weight: 700; margin-top: 2rem; margin-bottom: 0.8rem;
        padding-bottom: 0.45rem; border-bottom: 2px solid #333;
    }

    .question-text { font-weight: 600; line-height: 1.55; margin-bottom: 0.2rem; }
    .required { color: #d32f2f; font-weight: 700; }

    .notice-box {
        padding: 1rem; border: 1px solid #ddd; border-radius: 8px;
        background: #fafafa; font-size: 0.92rem; line-height: 1.6; margin-bottom: 1rem;
    }

    div[data-testid="stRadio"] { margin-bottom: 0.6rem; }
    div[data-testid="stTextInput"] { margin-bottom: 0.3rem; }

    .admin-title { font-size: 1.65rem; font-weight: 800; margin-bottom: 0.2rem; }
    .admin-sub { color: #666; margin-bottom: 1rem; }
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


def format_registration_number(value):
    digits = digits_only(value)
    if len(digits) == 13: return f"{digits[:6]}-{digits[6:]}"
    return digits


def format_phone(value):
    digits = digits_only(value)

    if not digits: return ""
    if len(digits) == 11: return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if digits.startswith("02") and len(digits) == 10: return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if digits.startswith("02") and len(digits) == 9: return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
    if len(digits) == 10: return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return digits


def question(number, text, detail_text=None, forced_answer=None):
    st.markdown(f'<div class="question-text">{number}. {text} <span class="required">*</span></div>', unsafe_allow_html=True)

    if forced_answer is not None:
        st.radio(
            f"{number}번 답변", ["예", "아니오"],
            index=0 if forced_answer == "예" else 1,
            horizontal=True, key=f"q{number}_forced",
            disabled=True, label_visibility="collapsed"
        )

        st.divider()
        return forced_answer, ""

    answer = st.radio(
        f"{number}번 답변", ["예", "아니오"], index=None,
        horizontal=True, key=f"q{number}",
        label_visibility="collapsed"
    )

    detail = ""

    if detail_text and answer == "예":
        detail = st.text_input(detail_text, key=f"q{number}_detail", placeholder="상세 내용을 입력해주세요.")

    st.divider()
    return answer, detail


def get_records():
    values = get_worksheet().get_all_values()

    if len(values) <= 1:
        return []

    headers = values[0]
    records = []

    for sheet_row, row in enumerate(values[1:], start=2):
        padded = row + [""] * max(0, len(headers) - len(row))
        record = dict(zip(headers, padded))
        record["_sheet_row"] = sheet_row
        records.append(record)

    return records


def rrn_front(value):
    digits = digits_only(value)
    return digits[:6] if len(digits) >= 6 else digits


def record_time(record):
    value = clean(record.get("DATE"))

    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
    except Exception:
        return value


def record_date(record):
    value = clean(record.get("DATE"))
    return value[:10]


# ============================================================
# PDF 미리보기
# ============================================================
def show_pdf_preview(pdf_data):
    encoded = base64.b64encode(pdf_data).decode()

    html = f"""
    <iframe
        src="data:application/pdf;base64,{encoded}#toolbar=0&navpanes=0&scrollbar=0"
        width="100%"
        height="780"
        style="border:1px solid #ccc; border-radius:6px;">
    </iframe>
    """

    components.html(html, height=800, scrolling=False)


# ============================================================
# 관리자 화면
# ============================================================
def admin_page():
    st.markdown('<div class="admin-title">💉 예방접종 관리자</div>', unsafe_allow_html=True)
    st.markdown('<div class="admin-sub">접종 대상자 선택 및 예진표 인쇄</div>', unsafe_allow_html=True)

    # --------------------------------------------------------
    # 로그인
    # --------------------------------------------------------
    if not st.session_state.get("admin_authenticated", False):
        login_col, empty_col = st.columns([1, 2])

        with login_col:
            password = st.text_input("관리자 비밀번호", type="password")

            if st.button("로그인", type="primary", use_container_width=True):
                if password == st.secrets["admin"]["password"]:
                    st.session_state.admin_authenticated = True
                    st.rerun()
                else:
                    st.error("관리자 비밀번호가 올바르지 않습니다.")

        st.stop()

    # --------------------------------------------------------
    # 상단
    # --------------------------------------------------------
    top1, top2, top3 = st.columns([2, 1, 1])

    with top1:
        selected_date = st.date_input("접수일", value=date.today(), format="YYYY-MM-DD")

    with top2:
        if st.button("오늘", use_container_width=True):
            st.session_state.admin_date = date.today()
            st.rerun()

    with top3:
        if st.button("로그아웃", use_container_width=True):
            st.session_state.admin_authenticated = False
            st.rerun()

    # --------------------------------------------------------
    # 데이터
    # --------------------------------------------------------
    try:
        records = get_records()
    except Exception as e:
        st.error("예진표 데이터를 불러오는 중 오류가 발생했습니다.")
        st.exception(e)
        st.stop()

    date_text = selected_date.strftime("%Y-%m-%d")
    records = [r for r in records if record_date(r) == date_text]
    records.reverse()

    left, right = st.columns([0.85, 1.55], gap="large")

    # ========================================================
    # 왼쪽 - 접종자 리스트
    # ========================================================
    with left:
        st.markdown("### 접종자")

        if not records:
            st.info("선택한 날짜에 접수된 예진표가 없습니다.")
            st.stop()

        st.caption(f"{date_text} · {len(records)}명")

        labels = []

        for record in records:
            label = (
                f"{record_time(record)}　"
                f"{clean(record.get('성명'))}　"
                f"{rrn_front(record.get('주민번호'))}　"
                f"{clean(record.get('관계'))}"
            )

            labels.append(label)

        selected_label = st.radio(
            "접종자 선택",
            labels,
            index=0,
            label_visibility="collapsed"
        )

        selected_index = labels.index(selected_label)
        selected = records[selected_index]

        st.divider()

        st.caption(
            f"선택: {clean(selected.get('성명'))} / "
            f"{rrn_front(selected.get('주민번호'))} / "
            f"{clean(selected.get('관계'))}"
        )

    # ========================================================
    # 오른쪽 - 미리보기 / 출력
    # ========================================================
    with right:
        st.markdown("### 예진표 미리보기 / 인쇄")

        mode = st.radio(
            "인쇄 용지",
            ["빈 용지", "미리 출력된 양식 용지"],
            horizontal=True
        )

        is_preprinted = mode == "미리 출력된 양식 용지"

        # ----------------------------------------------------
        # 양식용지 좌표 보정
        # ----------------------------------------------------
        if is_preprinted:
            with st.expander("⚙️ 양식용지 위치 보정"):
                c1, c2 = st.columns(2)

                with c1:
                    offset_x = st.number_input(
                        "좌우 이동 (mm)",
                        min_value=-20.0, max_value=20.0,
                        value=0.0, step=0.5
                    )

                    scale_x_percent = st.number_input(
                        "가로 배율 (%)",
                        min_value=95.0, max_value=105.0,
                        value=100.0, step=0.1
                    )

                with c2:
                    offset_y = st.number_input(
                        "상하 이동 (mm)",
                        min_value=-20.0, max_value=20.0,
                        value=0.0, step=0.5
                    )

                    scale_y_percent = st.number_input(
                        "세로 배율 (%)",
                        min_value=95.0, max_value=105.0,
                        value=100.0, step=0.1
                    )

        else:
            offset_x = 0.0
            offset_y = 0.0
            scale_x_percent = 100.0
            scale_y_percent = 100.0

        scale_x = scale_x_percent / 100.0
        scale_y = scale_y_percent / 100.0

        # ----------------------------------------------------
        # 선택 즉시 PDF 생성
        # ----------------------------------------------------
        try:
            pdf_data = create_print_pdf(
                selected,
                mode="preprinted" if is_preprinted else "blank",
                offset_x_mm=offset_x,
                offset_y_mm=offset_y,
                scale_x=scale_x,
                scale_y=scale_y
            )

        except Exception as e:
            st.error("예진표 미리보기를 생성하지 못했습니다.")
            st.exception(e)
            st.stop()

        # ----------------------------------------------------
        # 미리보기
        # ----------------------------------------------------
        show_pdf_preview(pdf_data)

        # ----------------------------------------------------
        # 인쇄
        # ----------------------------------------------------
        filename = (
            f"{clean(selected.get('성명'))}_"
            f"{rrn_front(selected.get('주민번호'))}_"
            f"예방접종예진표.pdf"
        )

        st.download_button(
            "🖨 인쇄용 PDF 열기",
            data=pdf_data,
            file_name=filename,
            mime="application/pdf",
            use_container_width=True,
            type="primary"
        )

        if is_preprinted:
            st.caption("미리 출력된 양식 용지: 입력값과 체크 표시만 인쇄됩니다.")
        else:
            st.caption("빈 용지: 예진표 양식과 입력값이 함께 인쇄됩니다.")

    st.stop()


# ============================================================
# 관리자 모드
# ============================================================
admin_mode = st.query_params.get("admin") == "1"

if admin_mode:
    admin_page()


# ============================================================
# 환자 화면 폭 제한
# ============================================================
st.markdown('<div class="patient-page">', unsafe_allow_html=True)


# ============================================================
# 제출 완료
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

st.markdown(
    '<div class="form-description">안전한 예방접종을 위하여 아래 질문사항을 잘 읽어보시고<br>정확하게 작성하여 주시기 바랍니다.</div>',
    unsafe_allow_html=True
)

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
# 인적사항
# ============================================================
st.markdown('<div class="section-title">접종 대상자 인적 사항</div>', unsafe_allow_html=True)

name = st.text_input("성명 *", placeholder="접종 대상자의 성명을 입력해주세요.")
rrn = st.text_input("주민등록번호", placeholder="숫자 13자리 입력", max_chars=14)
gender = st.radio("성별 *", ["남", "여"], index=None, horizontal=True)

birth_date = st.date_input(
    "실제 생년월일 *",
    value=None,
    min_value=date(1900, 1, 1),
    max_value=date.today(),
    format="YYYY-MM-DD"
)

foreigner_no = st.text_input("외국인 등록번호", placeholder="외국인인 경우 숫자 13자리 입력", max_chars=14)

col1, col2 = st.columns(2)

with col1:
    home_phone = st.text_input("전화번호 (집)", placeholder="숫자만 입력")

with col2:
    mobile_phone = st.text_input("휴대전화 *", placeholder="예: 01012345678")

weight = st.number_input(
    "체중 (kg)",
    min_value=0.0, max_value=300.0,
    value=None, step=0.1,
    placeholder="체중을 입력해주세요."
)


# ============================================================
# 동의 사항
# ============================================================
st.markdown('<div class="section-title">예방접종 업무를 위한 동의 사항</div>', unsafe_allow_html=True)

st.markdown("**1. 예방접종 내역 사전 확인**  \n예방접종을 하기 전에 접종 대상자의 예방접종 내역을 예방접종통합관리시스템으로 사전 확인하는 것에 동의합니다.")
vaccination_consent = st.radio("접종동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown("**2. 다음 접종 및 완료 여부 알림**  \n예방접종의 다음 접종 및 완료 여부에 관한 정보를 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
notification_consent = st.radio("알림동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown("**3. 예방접종 후 이상반응 알림**  \n예방접종 후 이상반응 발생 여부와 관련된 알림을 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
adverse_consent = st.radio("이상동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")


# ============================================================
# 문진
# ============================================================
st.markdown('<div class="section-title">접종 대상자에 대한 확인 사항</div>', unsafe_allow_html=True)
st.caption("각 질문에 반드시 '예' 또는 '아니오'를 선택해주세요.")

q1, q1_detail = question(1, "최근 1개월 이내에 받은 예방접종이 있습니까?", "그렇다면 예방접종명을 적어주세요.")

q2, q2_detail = question(
    2, "과거에 예방접종 후 이상반응이 나타나서 치료를 받은 적이 있습니까?",
    "그렇다면 이상반응과 해당 예방접종명을 적어주세요."
)

q3, q3_detail = question(3, "오늘 아픈 곳이 있습니까?", "그렇다면 아픈 증상을 적어주세요.")

# 남성일 경우 자동 아니오
if gender == "남":
    q4, _ = question(
        4,
        "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?",
        forced_answer="아니오"
    )
else:
    q4, _ = question(4, "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?")

q5, _ = question(
    5,
    "약이나 음식물(예: 계란) 혹은 백신 접종으로 두드러기, 알레르기 증상"
    "(예: 발진, 아나필락시스: 쇼크, 호흡곤란, 의식소실, 입술/입안의 부종 등)을 보인 적이 있습니까?"
)

q6, q6_detail = question(6, "암, 백혈병 혹은 면역계 질환이 있습니까?", "그렇다면 병명을 적어주세요.")
q7, _ = question(7, "최근 3개월 이내에 스테로이드제, 항암제, 방사선 치료를 받은 적이 있습니까?")
q8, _ = question(8, "최근 1년 동안 수혈을 받았거나 면역글로불린을 투여받은 적이 있습니까?")

q9, q9_detail = question(
    9,
    "(코로나19) 혈액응고장애를 앓고 있거나, 항응고제를 복용 중이십니까?",
    "그렇다면 질환명 또는 약 종류를 적어주세요."
)

q10, _ = question(10, "경련을 한 적이 있거나 기타 뇌신경계 질환(예: 길랭-바레 증후군 포함)이 있습니까?")

q11, q11_detail = question(
    11,
    "그 외 선천성 기형, 천식 및 폐질환, 심장질환, 신장질환, 간질환, 당뇨 및 내분비 질환, "
    "혈액 질환(혈액응고장애 외)으로 진찰 받거나 치료 받은 일이 있습니까?",
    "그렇다면 병명을 적어주세요."
)


# ============================================================
# 작성자
# ============================================================
st.markdown('<div class="section-title">작성자 확인</div>', unsafe_allow_html=True)

st.write("의사의 진찰결과와 이상반응에 대한 설명을 듣고 예방접종을 하겠습니다.")

writer = st.text_input("본인(법정대리인, 보호자) 성명 *", placeholder="작성자의 성명을 입력해주세요.")
relationship = st.text_input("접종 대상자와의 관계 *", placeholder="예: 본인, 부, 모, 배우자")

final_confirm = st.checkbox("위 내용을 확인하였으며 작성한 내용이 사실과 다름없음을 확인합니다.")


# ============================================================
# 제출
# ============================================================
submitted = st.button("예진표 제출", use_container_width=True, type="primary")

if submitted:
    errors = []

    formatted_rrn = format_registration_number(rrn)
    formatted_foreigner_no = format_registration_number(foreigner_no)
    formatted_home_phone = format_phone(home_phone)
    formatted_mobile_phone = format_phone(mobile_phone)

    if not clean(name): errors.append("성명을 입력해주세요.")
    if gender is None: errors.append("성별을 선택해주세요.")
    if birth_date is None: errors.append("실제 생년월일을 입력해주세요.")
    if not clean(mobile_phone): errors.append("휴대전화를 입력해주세요.")

    if clean(rrn) and len(digits_only(rrn)) != 13: errors.append("주민등록번호 숫자 13자리를 정확히 입력해주세요.")
    if clean(foreigner_no) and len(digits_only(foreigner_no)) != 13: errors.append("외국인 등록번호 숫자 13자리를 정확히 입력해주세요.")

    mobile_digits = digits_only(mobile_phone)
    if mobile_digits and len(mobile_digits) not in (10, 11): errors.append("휴대전화 번호를 정확히 입력해주세요.")

    if vaccination_consent is None: errors.append("예방접종 내역 사전 확인 동의 여부를 선택해주세요.")
    if notification_consent is None: errors.append("다음 접종 및 완료 여부 알림 동의 여부를 선택해주세요.")
    if adverse_consent is None: errors.append("예방접종 후 이상반응 알림 동의 여부를 선택해주세요.")

    answers = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11]

    for number, answer in enumerate(answers, 1):
        if answer is None: errors.append(f"{number}번 확인사항에 답변해주세요.")

    details = [
        (1, q1, q1_detail), (2, q2, q2_detail), (3, q3, q3_detail),
        (6, q6, q6_detail), (9, q9, q9_detail), (11, q11, q11_detail)
    ]

    for number, answer, detail in details:
        if answer == "예" and not clean(detail): errors.append(f"{number}번 질문의 상세 내용을 입력해주세요.")

    if not clean(writer): errors.append("작성자 성명을 입력해주세요.")
    if not clean(relationship): errors.append("접종 대상자와의 관계를 입력해주세요.")
    if not final_confirm: errors.append("최종 확인 항목에 체크해주세요.")

    if errors:
        st.error("입력하지 않았거나 확인이 필요한 항목이 있습니다.\n\n" + "\n\n".join(f"• {error}" for error in errors))

    else:
        try:
            now = datetime.now(TZ)

            record = {
                "ID": uuid.uuid4().hex, "DATE": now.strftime("%Y-%m-%d %H:%M:%S"),
                "성명": clean(name), "주민번호": formatted_rrn, "성별": clean(gender),
                "생년월일": birth_date.strftime("%Y-%m-%d"), "외국인번호": formatted_foreigner_no,
                "집전화": formatted_home_phone, "휴대전화": formatted_mobile_phone,
                "체중": "" if weight is None else str(weight),

                "접종동의": clean(vaccination_consent), "알림동의": clean(notification_consent),
                "이상동의": clean(adverse_consent),

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
            get_worksheet().append_row(row, value_input_option="RAW")

            st.session_state.submitted = True
            st.rerun()

        except Exception as e:
            st.error("예진표 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
            st.exception(e)

st.markdown("</div>", unsafe_allow_html=True)
