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
# PDF 좌표
# ============================================================
# 단위: PDF point
# 1mm ≒ 2.83465pt
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


def transformed_xy(key, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    x, y = PDF_COORDS[key]
    x = x * float(scale_x)
    y = A4_HEIGHT - ((A4_HEIGHT - y) * float(scale_y))
    x += mm_to_pt(offset_x_mm)
    y += mm_to_pt(offset_y_mm)
    return x, y


def draw_text(c, key, value, size=8, **kwargs):
    value = clean(value)
    if not value: return
    x, y = transformed_xy(key, **kwargs)
    c.setFont("HYSMyeongJo-Medium", size)
    c.drawString(x, y, value)


def draw_check(c, key, **kwargs):
    x, y = transformed_xy(key, **kwargs)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x, y, "V")


def draw_answer(c, prefix, answer, **kwargs):
    answer = clean(answer)
    if answer == "예": draw_check(c, f"{prefix}_예", **kwargs)
    elif answer == "아니오": draw_check(c, f"{prefix}_아니오", **kwargs)


# ============================================================
# 입력 데이터 오버레이
# ============================================================
def create_overlay(record, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    register_fonts()

    kwargs = {
        "offset_x_mm": offset_x_mm, "offset_y_mm": offset_y_mm,
        "scale_x": scale_x, "scale_y": scale_y
    }

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(A4_WIDTH, A4_HEIGHT))

    draw_text(c, "성명", record.get("성명"), 8, **kwargs)
    draw_text(c, "주민번호", record.get("주민번호"), 8, **kwargs)

    if clean(record.get("성별")) == "남": draw_check(c, "성별_남", **kwargs)
    elif clean(record.get("성별")) == "여": draw_check(c, "성별_여", **kwargs)

    draw_text(c, "생년월일", record.get("생년월일"), 8, **kwargs)
    draw_text(c, "외국인번호", record.get("외국인번호"), 8, **kwargs)
    draw_text(c, "집전화", record.get("집전화"), 8, **kwargs)
    draw_text(c, "휴대전화", record.get("휴대전화"), 8, **kwargs)
    draw_text(c, "체중", record.get("체중"), 8, **kwargs)

    draw_answer(c, "접종동의", record.get("접종동의"), **kwargs)
    draw_answer(c, "알림동의", record.get("알림동의"), **kwargs)
    draw_answer(c, "이상동의", record.get("이상동의"), **kwargs)

    for number in range(1, 12):
        draw_answer(c, str(number), record.get(str(number)), **kwargs)

    for number in [1, 2, 3, 6, 9, 11]:
        draw_text(c, f"{number}상세", record.get(f"{number}상세"), 7, **kwargs)

    draw_text(c, "작성자", record.get("작성자"), 8, **kwargs)
    draw_text(c, "관계", record.get("관계"), 8, **kwargs)

    try:
        dt = datetime.strptime(clean(record.get("DATE")), "%Y-%m-%d %H:%M:%S")
        date_text = f"{dt.year}      {dt.month}      {dt.day}"
    except Exception:
        date_text = ""

    draw_text(c, "작성일", date_text, 8, **kwargs)

    c.save()
    buffer.seek(0)
    return buffer


# ============================================================
# 인쇄용 PDF 생성
# ============================================================
def create_print_pdf(record, mode="blank", offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    overlay = create_overlay(record, offset_x_mm, offset_y_mm, scale_x, scale_y)
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
# 화면 미리보기용 PDF
# ============================================================
def create_preview_pdf(record, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    # 양식용지 인쇄를 선택해도 화면에서는 최종 완성 모습을 보여줌
    return create_print_pdf(record, "blank", offset_x_mm, offset_y_mm, scale_x, scale_y)


# ============================================================
# PDF → PNG 미리보기
# ============================================================
def pdf_to_png(pdf_data, zoom=1.55):
    document = fitz.open(stream=pdf_data, filetype="pdf")
    page = document.load_page(0)
    matrix = fitz.Matrix(zoom, zoom)
    pixmap = page.get_pixmap(matrix=matrix, alpha=False)
    image = pixmap.tobytes("png")
    document.close()
    return image
