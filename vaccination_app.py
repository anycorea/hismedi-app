import streamlit as st
import gspread
import uuid
import io
from datetime import datetime, date
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from pypdf import PdfReader, PdfWriter


# ============================================================
# 기본 설정
# ============================================================
st.set_page_config(page_title="예방접종 예진표", page_icon="💉", layout="centered")

TZ = ZoneInfo("Asia/Seoul")
PDF_TEMPLATE = "vaccination_form.pdf"

SHEET_HEADERS = [
    "ID", "DATE", "성명", "주민번호", "성별", "생년월일", "외국인번호", "집전화", "휴대전화", "체중",
    "접종동의", "알림동의", "이상동의", "1", "1상세", "2", "2상세", "3", "3상세", "4", "5",
    "6", "6상세", "7", "8", "9", "9상세", "10", "11", "11상세", "작성자", "관계"
]

# ============================================================
# PDF 위치 미세조정
# ============================================================
# 1pt ≒ 0.353mm
# 전체 글씨가 오른쪽으로 가야 하면 X를 +, 왼쪽이면 -
# 전체 글씨가 위로 가야 하면 Y를 +, 아래면 -
PDF_OFFSET_X = 0
PDF_OFFSET_Y = 0

# 원본 PDF 기준 좌표
PDF_COORDS = {
    "성명": (108, 729),
    "주민번호": (282, 729),
    "성별_남": (468, 729),
    "성별_여": (494, 729),

    "생년월일": (108, 716),
    "외국인번호": (282, 716),

    "집전화": (122, 702),
    "휴대전화": (297, 702),
    "체중": (489, 702),

    "접종동의_예": (462, 610),
    "접종동의_아니오": (496, 610),

    "알림동의_예": (462, 576),
    "알림동의_아니오": (496, 576),

    "이상동의_예": (462, 553),
    "이상동의_아니오": (496, 553),

    "1_예": (432, 504),
    "1_아니오": (486, 504),
    "1상세": (306, 495),

    "2_예": (432, 477),
    "2_아니오": (486, 477),
    "2상세": (306, 468),

    "3_예": (432, 451),
    "3_아니오": (486, 451),
    "3상세": (306, 442),

    "4_예": (432, 424),
    "4_아니오": (486, 424),

    "5_예": (432, 392),
    "5_아니오": (486, 392),

    "6_예": (432, 367),
    "6_아니오": (486, 367),
    "6상세": (306, 357),

    "7_예": (432, 342),
    "7_아니오": (486, 342),

    "8_예": (432, 322),
    "8_아니오": (486, 322),

    "9_예": (432, 299),
    "9_아니오": (486, 299),
    "9상세": (306, 289),

    "10_예": (432, 273),
    "10_아니오": (486, 273),

    "11_예": (432, 250),
    "11_아니오": (486, 250),
    "11상세": (306, 234),

    "작성자": (140, 194),
    "관계": (428, 194),
    "작성일": (432, 167)
}


# ============================================================
# 화면 스타일
# ============================================================
st.markdown("""
<style>
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"],
    [data-testid="stToolbar"], [data-testid="stDecoration"],
    [data-testid="stStatusWidget"] { display: none !important; }

    .block-container { max-width: 760px; padding-top: 1.2rem !important; padding-bottom: 2rem !important; }
    h1 { text-align: center; font-size: 2rem !important; margin-bottom: 0.3rem !important; }

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

    .admin-card {
        padding: 1rem; border: 1px solid #ddd; border-radius: 8px;
        background: #fafafa; margin-bottom: 1rem; line-height: 1.7;
    }

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


def question(number, text, detail_text=None):
    st.markdown(f'<div class="question-text">{number}. {text} <span class="required">*</span></div>', unsafe_allow_html=True)

    answer = st.radio(
        f"{number}번 답변", ["예", "아니오"], index=None, horizontal=True,
        key=f"q{number}", label_visibility="collapsed"
    )

    detail = ""
    if detail_text and answer == "예":
        detail = st.text_input(detail_text, key=f"q{number}_detail", placeholder="상세 내용을 입력해주세요.")

    st.divider()
    return answer, detail


def get_records():
    worksheet = get_worksheet()
    values = worksheet.get_all_values()

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


def mask_rrn(value):
    digits = digits_only(value)
    if len(digits) == 13: return f"{digits[:6]}-{digits[6]}******"
    return clean(value)


def answer_text(record, number):
    answer = clean(record.get(str(number), ""))
    detail = clean(record.get(f"{number}상세", ""))
    if detail: return f"{answer} / {detail}"
    return answer


# ============================================================
# PDF 함수
# ============================================================
@st.cache_resource
def register_pdf_fonts():
    pdfmetrics.registerFont(UnicodeCIDFont("HYSMyeongJo-Medium"))


def pdf_xy(key):
    x, y = PDF_COORDS[key]
    return x + PDF_OFFSET_X, y + PDF_OFFSET_Y


def draw_pdf_text(c, key, value, size=8):
    value = clean(value)

    if not value:
        return

    x, y = pdf_xy(key)
    c.setFont("HYSMyeongJo-Medium", size)
    c.drawString(x, y, value)


def draw_pdf_check(c, key):
    x, y = pdf_xy(key)

    # 원본 체크박스 안에 V 표시
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x, y, "V")


def draw_answer_check(c, prefix, answer):
    answer = clean(answer)

    if answer == "예":
        draw_pdf_check(c, f"{prefix}_예")

    elif answer == "아니오":
        draw_pdf_check(c, f"{prefix}_아니오")


def create_vaccination_pdf(record):
    register_pdf_fonts()

    template = PdfReader(PDF_TEMPLATE)
    original_page = template.pages[0]

    width = float(original_page.mediabox.width)
    height = float(original_page.mediabox.height)

    overlay_buffer = io.BytesIO()
    c = canvas.Canvas(overlay_buffer, pagesize=(width, height))

    # --------------------------------------------------------
    # 인적사항
    # --------------------------------------------------------
    draw_pdf_text(c, "성명", record.get("성명"), 8)
    draw_pdf_text(c, "주민번호", record.get("주민번호"), 8)

    if clean(record.get("성별")) == "남":
        draw_pdf_check(c, "성별_남")
    elif clean(record.get("성별")) == "여":
        draw_pdf_check(c, "성별_여")

    draw_pdf_text(c, "생년월일", record.get("생년월일"), 8)
    draw_pdf_text(c, "외국인번호", record.get("외국인번호"), 8)
    draw_pdf_text(c, "집전화", record.get("집전화"), 8)
    draw_pdf_text(c, "휴대전화", record.get("휴대전화"), 8)

    weight = clean(record.get("체중"))
    if weight:
        draw_pdf_text(c, "체중", weight, 8)

    # --------------------------------------------------------
    # 개인정보 동의
    # --------------------------------------------------------
    draw_answer_check(c, "접종동의", record.get("접종동의"))
    draw_answer_check(c, "알림동의", record.get("알림동의"))
    draw_answer_check(c, "이상동의", record.get("이상동의"))

    # --------------------------------------------------------
    # 문진 1~11
    # --------------------------------------------------------
    for number in range(1, 12):
        draw_answer_check(c, str(number), record.get(str(number)))

    # 상세내용
    for number in [1, 2, 3, 6, 9, 11]:
        draw_pdf_text(c, f"{number}상세", record.get(f"{number}상세"), 7)

    # --------------------------------------------------------
    # 작성자
    # --------------------------------------------------------
    draw_pdf_text(c, "작성자", record.get("작성자"), 8)
    draw_pdf_text(c, "관계", record.get("관계"), 8)

    # 환자가 제출한 날짜
    submitted_date = clean(record.get("DATE"))

    try:
        dt = datetime.strptime(submitted_date, "%Y-%m-%d %H:%M:%S")
        date_text = f"{dt.year}      {dt.month}      {dt.day}"
    except Exception:
        date_text = datetime.now(TZ).strftime("%Y      %m      %d")

    draw_pdf_text(c, "작성일", date_text, 8)

    c.save()
    overlay_buffer.seek(0)

    # --------------------------------------------------------
    # 원본 + 오버레이 병합
    # --------------------------------------------------------
    overlay_pdf = PdfReader(overlay_buffer)
    original_page.merge_page(overlay_pdf.pages[0])

    writer = PdfWriter()
    writer.add_page(original_page)

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)

    return output.getvalue()


# ============================================================
# 관리자 화면
# ============================================================
def admin_page():
    st.title("💉 예방접종 관리자")
    st.caption("예방접종 예진표 접수 내역 조회 및 출력")

    # --------------------------------------------------------
    # 관리자 로그인
    # --------------------------------------------------------
    if not st.session_state.get("admin_authenticated", False):
        st.markdown('<div class="section-title">관리자 로그인</div>', unsafe_allow_html=True)

        password = st.text_input("관리자 비밀번호", type="password", placeholder="비밀번호를 입력해주세요.")

        if st.button("로그인", use_container_width=True, type="primary"):
            if password == st.secrets["admin"]["password"]:
                st.session_state.admin_authenticated = True
                st.rerun()
            else:
                st.error("관리자 비밀번호가 올바르지 않습니다.")

        st.stop()

    # --------------------------------------------------------
    # 로그아웃
    # --------------------------------------------------------
    col1, col2 = st.columns([3, 1])

    with col1:
        st.success("관리자 로그인 상태입니다.")

    with col2:
        if st.button("로그아웃", use_container_width=True):
            st.session_state.admin_authenticated = False
            st.session_state.pop("pdf_data", None)
            st.session_state.pop("pdf_record_id", None)
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

    st.markdown('<div class="section-title">접수 내역</div>', unsafe_allow_html=True)

    if not records:
        st.info("현재 접수된 예진표가 없습니다.")
        st.stop()

    records = list(reversed(records))

    # --------------------------------------------------------
    # 검색
    # --------------------------------------------------------
    filter_col1, filter_col2 = st.columns([1, 2])

    with filter_col1:
        period = st.selectbox("조회 범위", ["오늘", "전체"])

    with filter_col2:
        search = st.text_input("검색", placeholder="성명 또는 휴대전화")

    today_text = datetime.now(TZ).strftime("%Y-%m-%d")
    filtered = []

    for record in records:
        if period == "오늘" and not clean(record.get("DATE")).startswith(today_text):
            continue

        keyword = clean(search).lower()

        if keyword:
            name_value = clean(record.get("성명")).lower()
            phone_value = clean(record.get("휴대전화")).lower()
            phone_digits = digits_only(record.get("휴대전화"))
            keyword_digits = digits_only(keyword)

            name_match = keyword in name_value
            phone_match = keyword in phone_value
            digit_match = bool(keyword_digits) and keyword_digits in phone_digits

            if not (name_match or phone_match or digit_match):
                continue

        filtered.append(record)

    st.caption(f"조회 결과: {len(filtered)}건")

    if not filtered:
        st.info("조건에 해당하는 예진표가 없습니다.")
        st.stop()

    # --------------------------------------------------------
    # 환자 선택
    # --------------------------------------------------------
    options = {}

    for record in filtered:
        record_id = clean(record.get("ID"))
        label = f"{clean(record.get('DATE'))} | {clean(record.get('성명'))} | {clean(record.get('휴대전화'))}"
        options[label] = record_id

    selected_label = st.selectbox(
        "예진표 선택",
        list(options.keys()),
        index=None,
        placeholder="확인할 예진표를 선택해주세요."
    )

    if selected_label is None:
        st.info("위 목록에서 확인할 예진표를 선택해주세요.")
        st.stop()

    selected_id = options[selected_label]
    selected = next((record for record in filtered if clean(record.get("ID")) == selected_id), None)

    if selected is None:
        st.error("선택한 예진표를 찾을 수 없습니다.")
        st.stop()

    # 다른 환자를 선택하면 기존 PDF 제거
    if st.session_state.get("pdf_record_id") != selected_id:
        st.session_state.pop("pdf_data", None)
        st.session_state.pop("pdf_record_id", None)

    # --------------------------------------------------------
    # 환자 정보
    # --------------------------------------------------------
    st.markdown('<div class="section-title">접종 대상자 정보</div>', unsafe_allow_html=True)

    weight_text = clean(selected.get("체중"))
    if weight_text:
        weight_text += " kg"

    st.markdown(
        f"""
        <div class="admin-card">
        <b>접수일시</b>　{clean(selected.get("DATE"))}<br>
        <b>성명</b>　{clean(selected.get("성명"))}<br>
        <b>주민등록번호</b>　{mask_rrn(selected.get("주민번호"))}<br>
        <b>성별</b>　{clean(selected.get("성별"))}<br>
        <b>생년월일</b>　{clean(selected.get("생년월일"))}<br>
        <b>외국인 등록번호</b>　{mask_rrn(selected.get("외국인번호"))}<br>
        <b>전화번호(집)</b>　{clean(selected.get("집전화"))}<br>
        <b>휴대전화</b>　{clean(selected.get("휴대전화"))}<br>
        <b>체중</b>　{weight_text}
        </div>
        """,
        unsafe_allow_html=True
    )

    # --------------------------------------------------------
    # 동의
    # --------------------------------------------------------
    st.markdown('<div class="section-title">동의 사항</div>', unsafe_allow_html=True)

    st.write(f"예방접종 내역 사전 확인: **{clean(selected.get('접종동의'))}**")
    st.write(f"다음 접종 및 완료 여부 알림: **{clean(selected.get('알림동의'))}**")
    st.write(f"예방접종 후 이상반응 알림: **{clean(selected.get('이상동의'))}**")

    # --------------------------------------------------------
    # 문진
    # --------------------------------------------------------
    st.markdown('<div class="section-title">문진 내용</div>', unsafe_allow_html=True)

    questions = [
        (1, "최근 1개월 이내에 받은 예방접종"),
        (2, "과거 예방접종 후 이상반응"),
        (3, "오늘 아픈 곳"),
        (4, "현재 임신 중이거나 한 달 내 임신 가능성"),
        (5, "약·음식물·백신 관련 알레르기"),
        (6, "암·백혈병·면역계 질환"),
        (7, "최근 3개월 이내 스테로이드·항암제·방사선 치료"),
        (8, "최근 1년 이내 수혈·면역글로불린 투여"),
        (9, "혈액응고장애 또는 항응고제 복용"),
        (10, "경련 또는 기타 뇌신경계 질환"),
        (11, "기타 질환 진찰 또는 치료")
    ]

    for number, text in questions:
        value = answer_text(selected, number)

        if clean(selected.get(str(number))) == "예":
            st.warning(f"{number}. {text} → {value}")
        else:
            st.write(f"**{number}. {text}** → {value}")

    # --------------------------------------------------------
    # 작성자
    # --------------------------------------------------------
    st.markdown('<div class="section-title">작성자</div>', unsafe_allow_html=True)

    st.markdown(
        f"""
        <div class="admin-card">
        <b>작성자</b>　{clean(selected.get("작성자"))}<br>
        <b>접종 대상자와의 관계</b>　{clean(selected.get("관계"))}
        </div>
        """,
        unsafe_allow_html=True
    )

    # --------------------------------------------------------
    # PDF 생성
    # --------------------------------------------------------
    st.markdown('<div class="section-title">예진표 출력</div>', unsafe_allow_html=True)

    st.caption("선택한 환자의 정보를 원본 예방접종 예진표에 입력하여 PDF를 생성합니다.")

    if st.button("📄 선택한 예진표 PDF 생성", use_container_width=True, type="primary"):
        try:
            with st.spinner("예진표 PDF를 생성하고 있습니다..."):
                st.session_state.pdf_data = create_vaccination_pdf(selected)
                st.session_state.pdf_record_id = selected_id

            st.success("PDF가 생성되었습니다.")

        except FileNotFoundError:
            st.error("vaccination_form.pdf 파일을 찾을 수 없습니다.")

        except Exception as e:
            st.error("PDF 생성 중 오류가 발생했습니다.")
            st.exception(e)

    # --------------------------------------------------------
    # PDF 다운로드
    # --------------------------------------------------------
    if st.session_state.get("pdf_data") and st.session_state.get("pdf_record_id") == selected_id:
        patient_name = clean(selected.get("성명")) or "환자"
        date_string = datetime.now(TZ).strftime("%Y%m%d")
        filename = f"{patient_name}_예방접종예진표_{date_string}.pdf"

        st.download_button(
            "⬇️ PDF 다운로드 / 인쇄",
            data=st.session_state.pdf_data,
            file_name=filename,
            mime="application/pdf",
            use_container_width=True
        )

        st.caption("다운로드한 PDF를 열어 인쇄할 때 용지 크기는 A4, 배율은 100% 또는 실제 크기로 설정해주세요.")

    st.stop()


# ============================================================
# 관리자 모드 진입
# ============================================================
admin_mode = st.query_params.get("admin") == "1"

if admin_mode:
    admin_page()


# ============================================================
# 환자 제출 완료 화면
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
# 환자 화면 - 제목 / 개인정보 안내
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
# 환자 화면 - 접종 대상자 인적 사항
# ============================================================
st.markdown('<div class="section-title">접종 대상자 인적 사항</div>', unsafe_allow_html=True)

name = st.text_input("성명 *", placeholder="접종 대상자의 성명을 입력해주세요.")
rrn = st.text_input("주민등록번호", placeholder="숫자 13자리 입력 (예: 9001010123456)", max_chars=14)
gender = st.radio("성별 *", ["남", "여"], index=None, horizontal=True)

birth_date = st.date_input(
    "실제 생년월일 *", value=None,
    min_value=date(1900, 1, 1), max_value=date.today(),
    format="YYYY-MM-DD"
)

foreigner_no = st.text_input("외국인 등록번호", placeholder="외국인인 경우 숫자 13자리 입력", max_chars=14)

col1, col2 = st.columns(2)

with col1:
    home_phone = st.text_input("전화번호 (집)", placeholder="숫자만 입력")

with col2:
    mobile_phone = st.text_input("휴대전화 *", placeholder="예: 01012345678")

weight = st.number_input(
    "체중 (kg)", min_value=0.0, max_value=300.0,
    value=None, step=0.1, placeholder="체중을 입력해주세요."
)


# ============================================================
# 환자 화면 - 동의 사항
# ============================================================
st.markdown('<div class="section-title">예방접종 업무를 위한 동의 사항</div>', unsafe_allow_html=True)

st.markdown(
    "**1. 예방접종 내역 사전 확인**  \n"
    "예방접종을 하기 전에 접종 대상자의 예방접종 내역을 예방접종통합관리시스템으로 사전 확인하는 것에 동의합니다."
)
vaccination_consent = st.radio("접종동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown(
    "**2. 다음 접종 및 완료 여부 알림**  \n"
    "예방접종의 다음 접종 및 완료 여부에 관한 정보를 문자 및 모바일앱으로 수신하는 것에 동의합니다."
)
notification_consent = st.radio("알림동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown(
    "**3. 예방접종 후 이상반응 알림**  \n"
    "예방접종 후 이상반응 발생 여부와 관련된 알림을 문자 및 모바일앱으로 수신하는 것에 동의합니다."
)
adverse_consent = st.radio("이상동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")


# ============================================================
# 환자 화면 - 문진
# ============================================================
st.markdown('<div class="section-title">접종 대상자에 대한 확인 사항</div>', unsafe_allow_html=True)
st.caption("각 질문에 반드시 '예' 또는 '아니오'를 선택해주세요.")

q1, q1_detail = question(1, "최근 1개월 이내에 받은 예방접종이 있습니까?", "그렇다면 예방접종명을 적어주세요.")

q2, q2_detail = question(
    2, "과거에 예방접종 후 이상반응이 나타나서 치료를 받은 적이 있습니까?",
    "그렇다면 이상반응과 해당 예방접종명을 적어주세요."
)

q3, q3_detail = question(3, "오늘 아픈 곳이 있습니까?", "그렇다면 아픈 증상을 적어주세요.")
q4, _ = question(4, "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?")

q5, _ = question(
    5, "약이나 음식물(예: 계란) 혹은 백신 접종으로 두드러기, 알레르기 증상"
    "(예: 발진, 아나필락시스: 쇼크, 호흡곤란, 의식소실, 입술/입안의 부종 등)을 보인 적이 있습니까?"
)

q6, q6_detail = question(6, "암, 백혈병 혹은 면역계 질환이 있습니까?", "그렇다면 병명을 적어주세요.")
q7, _ = question(7, "최근 3개월 이내에 스테로이드제, 항암제, 방사선 치료를 받은 적이 있습니까?")
q8, _ = question(8, "최근 1년 동안 수혈을 받았거나 면역글로불린을 투여받은 적이 있습니까?")

q9, q9_detail = question(
    9, "(코로나19) 혈액응고장애를 앓고 있거나, 항응고제를 복용 중이십니까?",
    "그렇다면 질환명 또는 약 종류를 적어주세요."
)

q10, _ = question(10, "경련을 한 적이 있거나 기타 뇌신경계 질환(예: 길랭-바레 증후군 포함)이 있습니까?")

q11, q11_detail = question(
    11, "그 외 선천성 기형, 천식 및 폐질환, 심장질환, 신장질환, 간질환, 당뇨 및 내분비 질환, "
    "혈액 질환(혈액응고장애 외)으로 진찰 받거나 치료 받은 일이 있습니까?",
    "그렇다면 병명을 적어주세요."
)


# ============================================================
# 환자 화면 - 작성자
# ============================================================
st.markdown('<div class="section-title">작성자 확인</div>', unsafe_allow_html=True)

st.write("의사의 진찰결과와 이상반응에 대한 설명을 듣고 예방접종을 하겠습니다.")

writer = st.text_input("본인(법정대리인, 보호자) 성명 *", placeholder="작성자의 성명을 입력해주세요.")
relationship = st.text_input("접종 대상자와의 관계 *", placeholder="예: 본인, 부, 모, 배우자")
final_confirm = st.checkbox("위 내용을 확인하였으며 작성한 내용이 사실과 다름없음을 확인합니다.")


# ============================================================
# 환자 화면 - 제출
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
            worksheet = get_worksheet()
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
            worksheet.append_row(row, value_input_option="RAW")

            st.session_state.submitted = True
            st.rerun()

        except Exception as e:
            st.error("예진표 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
            st.exception(e)
