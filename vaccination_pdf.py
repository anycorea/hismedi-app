import io
from datetime import datetime

import fitz
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from pypdf import PdfReader, PdfWriter


# ============================================================
# 기본 설정
# ============================================================
PDF_TEMPLATE = "vaccination_form.pdf"
A4_WIDTH = 595.276
A4_HEIGHT = 841.890


# ============================================================
# 원본 예방접종 예진표 실측 기준 좌표
#
# 중요:
# - 아래 값이 "빈 용지"의 마스터 좌표
# - 양식 용지도 동일 좌표 사용
# - 양식용지는 관리자 보정값만 추가 적용
# ============================================================
PDF_COORDS = {
    # --------------------------------------------------------
    # 인적사항
    # --------------------------------------------------------
    "성명": (130.0, 722.5),
    "주민번호": (329.0, 722.5),
    "성별_남": (459.0, 722.5),
    "성별_여": (484.0, 722.5),

    "생년월일": (130.0, 708.5),
    "외국인번호": (329.0, 708.5),

    "집전화": (130.0, 694.5),
    "휴대전화": (329.0, 694.5),
    "체중": (465.0, 694.5),

    # --------------------------------------------------------
    # 개인정보 동의
    # 원본 체크박스 실측
    # --------------------------------------------------------
    "접종동의_예": (460.5, 600.0),
    "접종동의_아니오": (488.5, 600.0),

    "알림동의_예": (460.5, 563.6),
    "알림동의_아니오": (488.5, 563.6),

    "이상동의_예": (460.5, 540.0),
    "이상동의_아니오": (488.5, 540.0),

    # --------------------------------------------------------
    # 문진 1 ~ 3
    # 원본 체크박스 위치 직접 기준
    # --------------------------------------------------------
    "1_예": (429.5, 492.5),
    "1_아니오": (482.0, 492.5),
    "1상세": (321.0, 480.5),

    "2_예": (429.5, 463.3),
    "2_아니오": (482.0, 463.3),
    "2상세": (321.0, 451.5),

    "3_예": (429.5, 434.2),
    "3_아니오": (482.0, 434.2),
    "3상세": (321.0, 422.5),

    # --------------------------------------------------------
    # 문진 4 ~ 8
    # --------------------------------------------------------
    "4_예": (429.5, 410.8),
    "4_아니오": (482.0, 410.8),

    "5_예": (429.5, 385.9),
    "5_아니오": (482.0, 385.9),

    "6_예": (429.5, 355.6),
    "6_아니오": (482.0, 355.6),
    "6상세": (321.0, 343.5),

    "7_예": (429.5, 331.4),
    "7_아니오": (482.0, 331.4),

    "8_예": (429.5, 312.4),
    "8_아니오": (482.0, 312.4),

    # --------------------------------------------------------
    # 문진 9 ~ 11
    # --------------------------------------------------------
    "9_예": (429.5, 288.2),
    "9_아니오": (482.0, 288.2),
    "9상세": (321.0, 276.0),

    "10_예": (429.5, 264.7),
    "10_아니오": (482.0, 264.7),

    "11_예": (429.5, 232.7),
    "11_아니오": (482.0, 232.7),
    "11상세": (321.0, 220.5),

    # --------------------------------------------------------
    # 작성자 / 관계 / 작성일
    # --------------------------------------------------------
    "작성자": (195.0, 176.0),
    "관계": (445.0, 176.0),
    "작성일": (410.0, 149.0)
}


# ============================================================
# 각 좌표가 어느 관리자 조정 영역에 속하는지
# ============================================================
KEY_SECTION = {
    "성명": "personal",
    "주민번호": "personal",
    "성별_남": "personal",
    "성별_여": "personal",
    "생년월일": "personal",
    "외국인번호": "personal",
    "집전화": "personal",
    "휴대전화": "personal",
    "체중": "personal",

    "접종동의_예": "consent",
    "접종동의_아니오": "consent",
    "알림동의_예": "consent",
    "알림동의_아니오": "consent",
    "이상동의_예": "consent",
    "이상동의_아니오": "consent",

    "1_예": "q1_3",
    "1_아니오": "q1_3",
    "1상세": "q1_3",
    "2_예": "q1_3",
    "2_아니오": "q1_3",
    "2상세": "q1_3",
    "3_예": "q1_3",
    "3_아니오": "q1_3",
    "3상세": "q1_3",

    "4_예": "q4_8",
    "4_아니오": "q4_8",
    "5_예": "q4_8",
    "5_아니오": "q4_8",
    "6_예": "q4_8",
    "6_아니오": "q4_8",
    "6상세": "q4_8",
    "7_예": "q4_8",
    "7_아니오": "q4_8",
    "8_예": "q4_8",
    "8_아니오": "q4_8",

    "9_예": "q9_11",
    "9_아니오": "q9_11",
    "9상세": "q9_11",
    "10_예": "q9_11",
    "10_아니오": "q9_11",
    "11_예": "q9_11",
    "11_아니오": "q9_11",
    "11상세": "q9_11",

    "작성자": "writer",
    "관계": "writer",
    "작성일": "writer"
}


# ============================================================
# 기본 보정값
#
# blank / preprinted 모두 이 값에서 시작
# ============================================================
def default_settings():
    return {
        "global": {"x": 0.0, "y": 0.0, "scale_x": 1.0, "scale_y": 1.0},
        "personal": {"x": 0.0, "y": 0.0},
        "consent": {"x": 0.0, "y": 0.0},
        "q1_3": {"x": 0.0, "y": 0.0},
        "q4_8": {"x": 0.0, "y": 0.0},
        "q9_11": {"x": 0.0, "y": 0.0},
        "writer": {"x": 0.0, "y": 0.0}
    }


# ============================================================
# 공통 함수
# ============================================================
def clean(value):
    return "" if value is None else str(value).strip()


def mm_to_pt(mm):
    return float(mm) * 2.834645669


def register_fonts():
    try:
        pdfmetrics.getFont("HYSMyeongJo-Medium")
    except KeyError:
        pdfmetrics.registerFont(UnicodeCIDFont("HYSMyeongJo-Medium"))


# ============================================================
# 관리자 보정 적용
# ============================================================
def transformed_xy(key, settings):
    x, y = PDF_COORDS[key]

    global_setting = settings.get("global", {})
    section_name = KEY_SECTION.get(key)
    section_setting = settings.get(section_name, {}) if section_name else {}

    scale_x = float(global_setting.get("scale_x", 1.0))
    scale_y = float(global_setting.get("scale_y", 1.0))

    # A4 좌상단 기준이 아니라 PDF 전체 기준으로 간격 보정
    x = x * scale_x
    y = A4_HEIGHT - ((A4_HEIGHT - y) * scale_y)

    global_x = float(global_setting.get("x", 0.0))
    global_y = float(global_setting.get("y", 0.0))

    section_x = float(section_setting.get("x", 0.0))
    section_y = float(section_setting.get("y", 0.0))

    x += mm_to_pt(global_x + section_x)
    y += mm_to_pt(global_y + section_y)

    return x, y


# ============================================================
# 일반 텍스트
# ============================================================
def draw_text(c, key, value, settings, size=8):
    value = clean(value)

    if not value:
        return

    x, y = transformed_xy(key, settings)

    c.setFont("HYSMyeongJo-Medium", size)
    c.drawString(x, y, value)


# ============================================================
# 체크 표시
#
# 기존보다 작게 조정하여 실제 □ 안에 들어가도록 함
# ============================================================
def draw_check(c, key, settings):
    x, y = transformed_xy(key, settings)

    c.setFont("Helvetica-Bold", 7.2)
    c.drawString(x, y, "V")


def draw_answer(c, prefix, answer, settings):
    answer = clean(answer)

    if answer == "예":
        draw_check(c, f"{prefix}_예", settings)

    elif answer == "아니오":
        draw_check(c, f"{prefix}_아니오", settings)


# ============================================================
# 긴 상세내용
#
# 입력칸을 넘어가지 않도록 글자수에 따라 글씨 축소
# ============================================================
def draw_detail(c, key, value, settings):
    value = clean(value)

    if not value:
        return

    if len(value) <= 12:
        size = 7.0
    elif len(value) <= 18:
        size = 6.2
    else:
        size = 5.5

    draw_text(c, key, value, settings, size)


# ============================================================
# 날짜 표시
# ============================================================
def get_written_date(record):
    try:
        dt = datetime.strptime(clean(record.get("DATE")), "%Y-%m-%d %H:%M:%S")
        return f"{dt.year}     {dt.month}     {dt.day}"
    except Exception:
        return ""


# ============================================================
# 데이터만 들어있는 투명 PDF 생성
# ============================================================
def create_overlay(record, settings=None):
    register_fonts()

    if settings is None:
        settings = default_settings()

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(A4_WIDTH, A4_HEIGHT))

    # --------------------------------------------------------
    # 인적사항
    # --------------------------------------------------------
    draw_text(c, "성명", record.get("성명"), settings, 8)
    draw_text(c, "주민번호", record.get("주민번호"), settings, 8)

    gender = clean(record.get("성별"))

    if gender == "남":
        draw_check(c, "성별_남", settings)

    elif gender == "여":
        draw_check(c, "성별_여", settings)

    draw_text(c, "생년월일", record.get("생년월일"), settings, 8)
    draw_text(c, "외국인번호", record.get("외국인번호"), settings, 8)
    draw_text(c, "집전화", record.get("집전화"), settings, 8)
    draw_text(c, "휴대전화", record.get("휴대전화"), settings, 8)
    draw_text(c, "체중", record.get("체중"), settings, 8)

    # --------------------------------------------------------
    # 개인정보 동의
    # --------------------------------------------------------
    draw_answer(c, "접종동의", record.get("접종동의"), settings)
    draw_answer(c, "알림동의", record.get("알림동의"), settings)
    draw_answer(c, "이상동의", record.get("이상동의"), settings)

    # --------------------------------------------------------
    # 문진 1 ~ 11
    # --------------------------------------------------------
    for number in range(1, 12):
        draw_answer(c, str(number), record.get(str(number)), settings)

    # --------------------------------------------------------
    # 상세내용
    # --------------------------------------------------------
    for number in [1, 2, 3, 6, 9, 11]:
        draw_detail(c, f"{number}상세", record.get(f"{number}상세"), settings)

    # --------------------------------------------------------
    # 작성자 / 관계 / 작성일
    # --------------------------------------------------------
    draw_text(c, "작성자", record.get("작성자"), settings, 8)
    draw_text(c, "관계", record.get("관계"), settings, 8)
    draw_text(c, "작성일", get_written_date(record), settings, 8)

    c.save()
    buffer.seek(0)

    return buffer


# ============================================================
# 최종 인쇄 PDF
#
# blank:
#   원본 양식 + 입력 데이터
#
# preprinted:
#   입력 데이터만
#
# 좌표 기준은 둘 다 완전히 동일함
# ============================================================
def create_print_pdf(record, mode="blank", settings=None):
    if settings is None:
        settings = default_settings()

    overlay = create_overlay(record, settings)
    overlay_reader = PdfReader(overlay)
    overlay_page = overlay_reader.pages[0]

    writer = PdfWriter()

    # --------------------------------------------------------
    # 빈 A4
    # --------------------------------------------------------
    if mode == "blank":
        template = PdfReader(PDF_TEMPLATE)

        page = template.pages[0]
        page.merge_page(overlay_page)

        writer.add_page(page)

    # --------------------------------------------------------
    # 이미 양식이 인쇄된 A4
    # 데이터만 출력
    # --------------------------------------------------------
    else:
        page = writer.add_blank_page(width=A4_WIDTH, height=A4_HEIGHT)
        page.merge_page(overlay_page)

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)

    return output.getvalue()


# ============================================================
# 관리자 화면 미리보기
#
# 양식용지를 선택해도 화면에서는
# "양식 + 데이터" 완성 형태를 보여줌
# ============================================================
def create_preview_pdf(record, settings=None):
    return create_print_pdf(record, mode="blank", settings=settings)


# ============================================================
# PDF → PNG
# ============================================================
def pdf_to_png(pdf_data, zoom=1.55):
    document = fitz.open(stream=pdf_data, filetype="pdf")

    page = document.load_page(0)
    matrix = fitz.Matrix(zoom, zoom)

    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = pixmap.tobytes("png")

    document.close()

    return image
