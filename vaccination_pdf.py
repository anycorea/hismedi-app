import io
import threading
from pathlib import Path
from functools import lru_cache
from datetime import datetime

import fitz
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from pypdf import PdfReader, PdfWriter


# ============================================================
# 기본 설정
# ============================================================
PDF_TEMPLATE = Path(__file__).resolve().with_name("vaccination_form.pdf")
_FONT_LOCK = threading.Lock()
A4_WIDTH = 595.276
A4_HEIGHT = 841.890


# ============================================================
# 예방접종 예진표 기준 좌표
#
# 빈 용지 = 마스터 좌표
# 양식 용지 = 동일 좌표 + 관리자 보정값
# ============================================================
PDF_COORDS = {
    # --------------------------------------------------------
    # 인적사항
    # --------------------------------------------------------
    "성명": (130.0, 722.4173),
    "주민번호": (329.0, 722.4173),
    "성별_남": (463.0, 722.4173),
    "성별_여": (484.4567, 722.4173),

    "생년월일": (130.0, 708.4173),
    "외국인번호": (329.0, 708.4173),

    "집전화": (144.1732, 694.4173),
    "휴대전화": (303.4882, 694.4173),
    "체중": (465.0, 694.4173),

    # --------------------------------------------------------
    # 개인정보 동의
    # --------------------------------------------------------
    "접종동의_예": (461.5, 599.9173),
    "접종동의_아니오": (489.5, 599.9173),

    "알림동의_예": (461.5, 563.5173),
    "알림동의_아니오": (489.5, 563.5173),

    "이상동의_예": (461.5, 539.9173),
    "이상동의_아니오": (489.5, 539.9173),

    # --------------------------------------------------------
    # 문진 1 ~ 3
    # --------------------------------------------------------
    "1_예": (430.5, 491.0),
    "1_아니오": (483.0, 491.0),
    "1상세": (321.0, 485.8346),

    "2_예": (430.5, 461.8),
    "2_아니오": (483.0, 461.8),
    "2상세": (321.0, 456.8346),

    "3_예": (430.5, 432.7),
    "3_아니오": (483.0, 432.7),
    "3상세": (321.0, 427.8346),

    # --------------------------------------------------------
    # 문진 4 ~ 8
    # --------------------------------------------------------
    "4_예": (430.5, 409.3),
    "4_아니오": (483.0, 409.3),

    "5_예": (430.5, 384.4),
    "5_아니오": (483.0, 384.4),

    "6_예": (430.5, 354.1),
    "6_아니오": (483.0, 354.1),
    "6상세": (321.0, 348.8346),

    "7_예": (430.5, 329.9),
    "7_아니오": (483.0, 329.9),

    "8_예": (430.5, 310.9),
    "8_아니오": (483.0, 310.9),

    # --------------------------------------------------------
    # 문진 9 ~ 11
    # --------------------------------------------------------
    "9_예": (430.5, 286.7),
    "9_아니오": (483.0, 286.7),
    "9상세": (321.0, 281.3346),

    "10_예": (430.5, 263.2),
    "10_아니오": (483.0, 263.2),

    "11_예": (430.5, 231.2),
    "11_아니오": (483.0, 231.2),
    "11상세": (321.0, 218.7480),

    # --------------------------------------------------------
    # 작성자 / 관계
    # --------------------------------------------------------
    "작성자": (196.3386, 175.9173),
    "관계": (449.6693, 175.9173),

    # --------------------------------------------------------
    # 작성일
    #
    # 날짜를 한 문자열로 출력하지 않고
    # 년 / 월 / 일을 각각 독립적으로 출력
    # --------------------------------------------------------
    # 원본 양식의 '년 / 월 / 일' 글자 앞 빈칸에 값이 들어가도록 기준 좌표 고정
    "작성년": (370.0, 149.5827),
    "작성월": (423.0, 149.5827),
    "작성일": (451.0, 149.5827)
}


# ============================================================
# 관리자 위치 조정 영역
# ============================================================
KEY_SECTION = {
    "성명": "personal", "주민번호": "personal",
    "성별_남": "personal", "성별_여": "personal",
    "생년월일": "personal", "외국인번호": "personal",
    "집전화": "personal", "휴대전화": "personal", "체중": "personal",

    "접종동의_예": "consent", "접종동의_아니오": "consent",
    "알림동의_예": "consent", "알림동의_아니오": "consent",
    "이상동의_예": "consent", "이상동의_아니오": "consent",

    "1_예": "questions", "1_아니오": "questions", "1상세": "details",
    "2_예": "questions", "2_아니오": "questions", "2상세": "details",
    "3_예": "questions", "3_아니오": "questions", "3상세": "details",

    "4_예": "questions", "4_아니오": "questions",
    "5_예": "questions", "5_아니오": "questions",
    "6_예": "questions", "6_아니오": "questions", "6상세": "details",
    "7_예": "questions", "7_아니오": "questions",
    "8_예": "questions", "8_아니오": "questions",

    "9_예": "questions", "9_아니오": "questions", "9상세": "details",
    "10_예": "questions", "10_아니오": "questions",
    "11_예": "questions", "11_아니오": "questions", "11상세": "details",

    "작성자": "writer", "관계": "writer",
    "작성년": "date", "작성월": "date", "작성일": "date"
}


# ============================================================
# 기본 보정값
#
# blank / preprinted 모두 같은 기준
# ============================================================
def default_settings():
    return {
        "global": {"x": 0.0, "y": 0.0, "scale_x": 1.0, "scale_y": 1.0},
        "personal": {"x": 0.0, "y": 0.0},
        "consent": {"x": 0.0, "y": 0.0},
        "questions": {"x": 0.0, "y": 0.0},
        "details": {"x": 0.0, "y": 0.0},
        "writer": {"x": 0.0, "y": 0.0},
        "date": {"x": 0.0, "y": 0.0}
    }


# ============================================================
# 공통 함수
# ============================================================
def clean(value):
    return "" if value is None else str(value).strip()


def mm_to_pt(mm):
    return float(mm) * 2.834645669


def register_fonts():
    with _FONT_LOCK:
        try:
            pdfmetrics.getFont("HYSMyeongJo-Medium")
        except KeyError:
            pdfmetrics.registerFont(UnicodeCIDFont("HYSMyeongJo-Medium"))


@lru_cache(maxsize=2)
def _template_bytes(path, mtime_ns, size):
    return Path(path).read_bytes()


# ============================================================
# 관리자 보정 적용
#
# 관리자 화면의 X/Y 값은 mm 단위
# ============================================================
def transformed_xy(key, settings):
    x, y = PDF_COORDS[key]

    global_setting = settings.get("global", {})
    section_name = KEY_SECTION.get(key)
    section_setting = settings.get(section_name, {}) if section_name else {}

    scale_x = float(global_setting.get("scale_x", 1.0))
    scale_y = float(global_setting.get("scale_y", 1.0))

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
# 텍스트 출력
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
# V를 작게 출력하여 원본 □ 안쪽에 위치
# ============================================================
def draw_check(c, key, settings):
    x, y = transformed_xy(key, settings)
    c.setFont("Helvetica-Bold", 7.0)
    c.drawString(x, y, "V")


def draw_answer(c, prefix, answer, settings):
    answer = clean(answer)

    if answer == "예":
        draw_check(c, f"{prefix}_예", settings)
    elif answer == "아니오":
        draw_check(c, f"{prefix}_아니오", settings)


# ============================================================
# 상세내용
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
# 작성일 추출
# ============================================================
def get_written_date(record):
    raw = clean(record.get("DATE"))

    try:
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return dt.year, dt.month, dt.day
    except Exception:
        pass

    try:
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return dt.year, dt.month, dt.day
    except Exception:
        return "", "", ""


# ============================================================
# 입력 데이터만 들어있는 투명 PDF
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
    # 작성자 / 관계
    # --------------------------------------------------------
    draw_text(c, "작성자", record.get("작성자"), settings, 8)
    draw_text(c, "관계", record.get("관계"), settings, 8)

    # --------------------------------------------------------
    # 작성일
    #
    # 년 / 월 / 일을 각각 별도 좌표에 출력
    # --------------------------------------------------------
    year, month, day = get_written_date(record)
    draw_text(c, "작성년", year, settings, 7.5)
    draw_text(c, "작성월", month, settings, 7.5)
    draw_text(c, "작성일", day, settings, 7.5)

    c.save()
    buffer.seek(0)

    return buffer


# ============================================================
# 최종 인쇄 PDF
#
# blank:
#   원본 PDF 양식 + 입력 데이터
#
# preprinted:
#   입력 데이터만
#
# 두 모드 모두 입력 데이터 좌표는 동일
# ============================================================
def create_print_pdf(record, mode="blank", settings=None):
    if mode not in ("blank", "preprinted"):
        raise ValueError("Unknown print mode")
    if settings is None:
        settings = default_settings()

    overlay = create_overlay(record, settings)
    overlay_reader = PdfReader(overlay)
    overlay_page = overlay_reader.pages[0]

    writer = PdfWriter()

    if mode == "blank":
        path = Path(PDF_TEMPLATE)
        stat = path.stat()
        template = PdfReader(io.BytesIO(_template_bytes(str(path), stat.st_mtime_ns, stat.st_size)))
        page = writer.add_page(template.pages[0])
        page.merge_page(overlay_page)

    else:
        page = writer.add_blank_page(width=A4_WIDTH, height=A4_HEIGHT)
        page.merge_page(overlay_page)

    output = io.BytesIO()
    writer.write(output)
    output.seek(0)

    return output.getvalue()


# ============================================================
# 관리자 미리보기
#
# 양식용지를 선택하더라도 화면에서는
# 원본 양식 위에 입력값을 합쳐서 보여줌
# ============================================================
def create_preview_pdf(record, settings=None):
    return create_print_pdf(record, mode="blank", settings=settings)


# ============================================================
# PDF → PNG 미리보기
# ============================================================
def pdf_to_png(pdf_data, zoom=1.55):
    with fitz.open(stream=pdf_data, filetype="pdf") as document:
        page = document.load_page(0)
        pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), alpha=False)
        return pixmap.tobytes("png")
