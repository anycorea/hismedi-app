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
# PDF 기본 좌표
# ============================================================
PDF_COORDS = {
    "성명": (108, 729), "주민번호": (282, 729), "성별_남": (468, 729), "성별_여": (494, 729),
    "생년월일": (108, 716), "외국인번호": (282, 716),
    "집전화": (122, 702), "휴대전화": (297, 702), "체중": (489, 702),

    "접종동의_예": (462, 610), "접종동의_아니오": (496, 610),
    "알림동의_예": (462, 576), "알림동의_아니오": (496, 576),
    "이상동의_예": (462, 553), "이상동의_아니오": (496, 553),

    "1_예": (432, 504), "1_아니오": (486, 504), "1상세": (306, 495),
    "2_예": (432, 477), "2_아니오": (486, 477), "2상세": (306, 468),
    "3_예": (432, 451), "3_아니오": (486, 451), "3상세": (306, 442),

    "4_예": (432, 424), "4_아니오": (486, 424),
    "5_예": (432, 392), "5_아니오": (486, 392),
    "6_예": (432, 367), "6_아니오": (486, 367), "6상세": (306, 357),
    "7_예": (432, 342), "7_아니오": (486, 342),
    "8_예": (432, 322), "8_아니오": (486, 322),

    "9_예": (432, 299), "9_아니오": (486, 299), "9상세": (306, 289),
    "10_예": (432, 273), "10_아니오": (486, 273),
    "11_예": (432, 250), "11_아니오": (486, 250), "11상세": (306, 234),

    "작성자": (140, 194), "관계": (428, 194), "작성일": (432, 167)
}


# ============================================================
# 좌표 영역
# ============================================================
KEY_SECTION = {
    "성명": "personal", "주민번호": "personal", "성별_남": "personal", "성별_여": "personal",
    "생년월일": "personal", "외국인번호": "personal", "집전화": "personal", "휴대전화": "personal", "체중": "personal",

    "접종동의_예": "consent", "접종동의_아니오": "consent",
    "알림동의_예": "consent", "알림동의_아니오": "consent",
    "이상동의_예": "consent", "이상동의_아니오": "consent",

    "1_예": "q1_3", "1_아니오": "q1_3", "1상세": "q1_3",
    "2_예": "q1_3", "2_아니오": "q1_3", "2상세": "q1_3",
    "3_예": "q1_3", "3_아니오": "q1_3", "3상세": "q1_3",

    "4_예": "q4_8", "4_아니오": "q4_8",
    "5_예": "q4_8", "5_아니오": "q4_8",
    "6_예": "q4_8", "6_아니오": "q4_8", "6상세": "q4_8",
    "7_예": "q4_8", "7_아니오": "q4_8",
    "8_예": "q4_8", "8_아니오": "q4_8",

    "9_예": "q9_11", "9_아니오": "q9_11", "9상세": "q9_11",
    "10_예": "q9_11", "10_아니오": "q9_11",
    "11_예": "q9_11", "11_아니오": "q9_11", "11상세": "q9_11",

    "작성자": "writer", "관계": "writer", "작성일": "writer"
}


# ============================================================
# 기본 보정값
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
# 공통
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


def transformed_xy(key, settings):
    x, y = PDF_COORDS[key]

    global_setting = settings.get("global", {})
    section_name = KEY_SECTION.get(key)
    section_setting = settings.get(section_name, {}) if section_name else {}

    scale_x = float(global_setting.get("scale_x", 1.0))
    scale_y = float(global_setting.get("scale_y", 1.0))

    x = x * scale_x
    y = A4_HEIGHT - ((A4_HEIGHT - y) * scale_y)

    total_x_mm = float(global_setting.get("x", 0.0)) + float(section_setting.get("x", 0.0))
    total_y_mm = float(global_setting.get("y", 0.0)) + float(section_setting.get("y", 0.0))

    x += mm_to_pt(total_x_mm)
    y += mm_to_pt(total_y_mm)

    return x, y


def draw_text(c, key, value, settings, size=8):
    value = clean(value)
    if not value: return

    x, y = transformed_xy(key, settings)
    c.setFont("HYSMyeongJo-Medium", size)
    c.drawString(x, y, value)


def draw_check(c, key, settings):
    x, y = transformed_xy(key, settings)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x, y, "V")


def draw_answer(c, prefix, answer, settings):
    answer = clean(answer)

    if answer == "예":
        draw_check(c, f"{prefix}_예", settings)
    elif answer == "아니오":
        draw_check(c, f"{prefix}_아니오", settings)


# ============================================================
# 데이터 오버레이
# ============================================================
def create_overlay(record, settings=None):
    register_fonts()

    if settings is None:
        settings = default_settings()

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(A4_WIDTH, A4_HEIGHT))

    # 인적사항
    draw_text(c, "성명", record.get("성명"), settings)
    draw_text(c, "주민번호", record.get("주민번호"), settings)

    if clean(record.get("성별")) == "남":
        draw_check(c, "성별_남", settings)
    elif clean(record.get("성별")) == "여":
        draw_check(c, "성별_여", settings)

    draw_text(c, "생년월일", record.get("생년월일"), settings)
    draw_text(c, "외국인번호", record.get("외국인번호"), settings)
    draw_text(c, "집전화", record.get("집전화"), settings)
    draw_text(c, "휴대전화", record.get("휴대전화"), settings)
    draw_text(c, "체중", record.get("체중"), settings)

    # 개인정보 동의
    draw_answer(c, "접종동의", record.get("접종동의"), settings)
    draw_answer(c, "알림동의", record.get("알림동의"), settings)
    draw_answer(c, "이상동의", record.get("이상동의"), settings)

    # 문진
    for number in range(1, 12):
        draw_answer(c, str(number), record.get(str(number)), settings)

    for number in [1, 2, 3, 6, 9, 11]:
        draw_text(c, f"{number}상세", record.get(f"{number}상세"), settings, 7)

    # 작성자
    draw_text(c, "작성자", record.get("작성자"), settings)
    draw_text(c, "관계", record.get("관계"), settings)

    try:
        dt = datetime.strptime(clean(record.get("DATE")), "%Y-%m-%d %H:%M:%S")
        date_text = f"{dt.year}      {dt.month}      {dt.day}"
    except Exception:
        date_text = ""

    draw_text(c, "작성일", date_text, settings)

    c.save()
    buffer.seek(0)

    return buffer


# ============================================================
# 최종 인쇄 PDF
# ============================================================
def create_print_pdf(record, mode="blank", settings=None):
    if settings is None:
        settings = default_settings()

    overlay = create_overlay(record, settings)
    overlay_page = PdfReader(overlay).pages[0]

    writer = PdfWriter()

    if mode == "blank":
        template = PdfReader(PDF_TEMPLATE)
        page = template.pages[0]
        page.merge_page(overlay_page)
        writer.add_page(page)

    else:
        writer.add_blank_page(width=A4_WIDTH, height=A4_HEIGHT)
        page = writer.pages[0]
        page.merge_page(overlay_page)

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)

    return output.getvalue()


# ============================================================
# 화면 미리보기
# ============================================================
def create_preview_pdf(record, settings=None):
    # 실제 인쇄가 양식용지여도 화면에는 양식+데이터를 보여줌
    return create_print_pdf(record, mode="blank", settings=settings)


def pdf_to_png(pdf_data, zoom=1.55):
    document = fitz.open(stream=pdf_data, filetype="pdf")
    page = document.load_page(0)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
    image = pixmap.tobytes("png")
    document.close()
    return image
