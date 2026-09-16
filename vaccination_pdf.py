import io
from datetime import datetime
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
# PDF 원본 기준 좌표
# ============================================================
# 단위: PDF point
# 1 mm ≒ 2.83465 pt
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


def transformed_xy(key, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    x, y = PDF_COORDS[key]

    # A4 좌측 하단을 기준으로 전체 좌표 배율 조정
    x = x * float(scale_x)
    y = A4_HEIGHT - ((A4_HEIGHT - y) * float(scale_y))

    x += mm_to_pt(offset_x_mm)
    y += mm_to_pt(offset_y_mm)

    return x, y


def draw_text(c, key, value, size=8, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    value = clean(value)
    if not value: return

    x, y = transformed_xy(key, offset_x_mm, offset_y_mm, scale_x, scale_y)
    c.setFont("HYSMyeongJo-Medium", size)
    c.drawString(x, y, value)


def draw_check(c, key, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    x, y = transformed_xy(key, offset_x_mm, offset_y_mm, scale_x, scale_y)
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x, y, "V")


def draw_answer(c, prefix, answer, **kwargs):
    answer = clean(answer)
    if answer == "예": draw_check(c, f"{prefix}_예", **kwargs)
    elif answer == "아니오": draw_check(c, f"{prefix}_아니오", **kwargs)


# ============================================================
# 환자 데이터 오버레이
# ============================================================
def create_overlay(record, offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    register_fonts()

    kwargs = {
        "offset_x_mm": offset_x_mm, "offset_y_mm": offset_y_mm,
        "scale_x": scale_x, "scale_y": scale_y
    }

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=(A4_WIDTH, A4_HEIGHT))

    # 인적사항
    draw_text(c, "성명", record.get("성명"), 8, **kwargs)
    draw_text(c, "주민번호", record.get("주민번호"), 8, **kwargs)

    if clean(record.get("성별")) == "남": draw_check(c, "성별_남", **kwargs)
    elif clean(record.get("성별")) == "여": draw_check(c, "성별_여", **kwargs)

    draw_text(c, "생년월일", record.get("생년월일"), 8, **kwargs)
    draw_text(c, "외국인번호", record.get("외국인번호"), 8, **kwargs)
    draw_text(c, "집전화", record.get("집전화"), 8, **kwargs)
    draw_text(c, "휴대전화", record.get("휴대전화"), 8, **kwargs)
    draw_text(c, "체중", record.get("체중"), 8, **kwargs)

    # 개인정보 동의
    draw_answer(c, "접종동의", record.get("접종동의"), **kwargs)
    draw_answer(c, "알림동의", record.get("알림동의"), **kwargs)
    draw_answer(c, "이상동의", record.get("이상동의"), **kwargs)

    # 문진
    for number in range(1, 12):
        draw_answer(c, str(number), record.get(str(number)), **kwargs)

    for number in [1, 2, 3, 6, 9, 11]:
        draw_text(c, f"{number}상세", record.get(f"{number}상세"), 7, **kwargs)

    # 작성자
    draw_text(c, "작성자", record.get("작성자"), 8, **kwargs)
    draw_text(c, "관계", record.get("관계"), 8, **kwargs)

    submitted_date = clean(record.get("DATE"))

    try:
        dt = datetime.strptime(submitted_date, "%Y-%m-%d %H:%M:%S")
        date_text = f"{dt.year}      {dt.month}      {dt.day}"
    except Exception:
        date_text = ""

    draw_text(c, "작성일", date_text, 8, **kwargs)

    c.save()
    buffer.seek(0)

    return buffer


# ============================================================
# 최종 인쇄 PDF
# ============================================================
def create_print_pdf(record, mode="blank", offset_x_mm=0.0, offset_y_mm=0.0, scale_x=1.0, scale_y=1.0):
    """
    mode="blank"
        빈 A4 용지에 인쇄.
        공식 양식 + 환자 입력값 모두 출력.

    mode="preprinted"
        이미 양식이 인쇄된 종이에 출력.
        환자 입력값만 출력.
    """

    overlay_buffer = create_overlay(record, offset_x_mm, offset_y_mm, scale_x, scale_y)
    overlay_reader = PdfReader(overlay_buffer)
    overlay_page = overlay_reader.pages[0]

    writer = PdfWriter()

    if mode == "blank":
        template_reader = PdfReader(PDF_TEMPLATE)
        page = template_reader.pages[0]
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
