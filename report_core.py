"""병원 실적 집계. 원본을 수정하지 않으며 S시트에 의존하지 않습니다."""
from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ALL = "전체(합계)"
SHEETS = ("Na", "Da", "Ca", "Ea", "Za", "H")
SEOUL = ZoneInfo("Asia/Seoul")


def text(value):
    return " ".join(str(value or "").split())


def number(value):
    if value is None or value == "":
        return 0.0
    if isinstance(value, (float, int)):
        return float(value)
    s = str(value).strip().replace(",", "")
    if s in ("", "-", "—"):
        return 0.0
    try:
        return float(s)
    except ValueError as exc:
        raise ValueError(f"숫자 열에 계산할 수 없는 값이 있습니다: {value!r}") from exc


@dataclass(frozen=True)
class Row:
    year: int
    month: int
    cells: tuple

    def val(self, col):
        return self.cells[col - 1] if col <= len(self.cells) else None

    def txt(self, col):
        return text(self.val(col))

    def num(self, col):
        return number(self.val(col))


def parse_source(raw):
    result = {}
    for name in SHEETS:
        if name not in raw:
            raise ValueError(f"필수 시트가 없습니다: {name}")
        output = []
        for i, cells in enumerate(raw[name], 1):
            if len(cells) < 2:
                continue
            ys = re.fullmatch(r"(20\d{2})(?:년)?(?:\.0)?", text(cells[0]))
            ms = re.fullmatch(r"(\d{1,2})(?:월)?(?:\.0)?", text(cells[1]))
            if not ys or not ms:
                continue
            y, m = int(ys[1]), int(ms[1])
            if not 1 <= m <= 12:
                raise ValueError(f"{name}!B{i}: 월이 1~12 범위를 벗어났습니다.")
            # H의 연월만 미리 채운 빈 행은 실제 자료로 보지 않습니다. 숫자 0은 유효합니다.
            cols = (7, 8) if name == "H" else ((9, 13, 14, 16, 17) if name == "Na" else ((8, 9, 10, 11, 12, 13, 14, 15, 16, 17) if name == "Ca" else (9,)))
            if not any(c <= len(cells) and cells[c-1] not in (None, "") for c in cols):
                continue
            row = Row(y, m, tuple(cells))
            for col in cols:
                try:
                    row.num(col)
                except ValueError as exc:
                    raise ValueError(f"{name} {i}행: {exc}") from exc
            output.append(row)
        result[name] = output
    return result


def available_years(data):
    return sorted({r.year for rows in data.values() for r in rows}, reverse=True)


def available_months(data, year):
    return sorted({r.month for rows in data.values() for r in rows if r.year == year})


def doctors(data, year):
    return sorted({r.txt(7) for n, rows in data.items() if n != "H" for r in rows
                   if r.year == year and not is_total(n, r) and r.txt(7) not in ("", "-", "합계")})


def is_total(sheet, row):
    return row.txt(8 if sheet == "Na" else 6) == "합계"


@dataclass(frozen=True)
class Metric:
    key: str
    section: str
    label: str
    unit: str
    source: str
    col: int = 0
    kind: str = "total"
    category: str = ""
    emphasis: bool = False


METRICS = [
    Metric("revenue_all", "진료수입", "전체 · 검진 포함", "원", "Na", kind="combined", emphasis=True),
    Metric("revenue", "진료수입", "전체 · 검진 제외", "원", "Na", 17, emphasis=True),
    Metric("revenue_child", "진료수입", "소아", "원", "Na", 17, "child"),
    Metric("revenue_adult", "진료수입", "성인", "원", "Na", 17, "adult"),
    Metric("revenue_out", "진료수입", "외래", "원", "Na", 14),
    Metric("revenue_in", "진료수입", "입원", "원", "Na", 16),
    Metric("revenue_check", "진료수입", "건강검진 · 별도", "원", "H", 8),
    Metric("patients_out", "환자수", "외래", "명", "Na", 9, emphasis=True),
    Metric("patients_out_child", "환자수", "외래 · 소아", "명", "Na", 9, "child"),
    Metric("patients_out_adult", "환자수", "외래 · 성인", "명", "Na", 9, "adult"),
    Metric("patients_in", "환자수", "입원 · 재원", "명", "Na", 13, emphasis=True),
    Metric("patients_in_child", "환자수", "입원 · 소아", "명", "Na", 13, "child"),
    Metric("patients_in_adult", "환자수", "입원 · 성인", "명", "Na", 13, "adult"),
    Metric("patients_check", "환자수", "건강검진 · 별도", "명", "H", 7),
    Metric("daily_out", "일당진료비", "외래", "원", "Na", kind="ratio"),
    Metric("daily_in", "일당진료비", "입원", "원", "Na", kind="ratio"),
    Metric("image_total", "영상검사", "합계", "건", "Da", 9, emphasis=True),
]
for code in ("DR", "MR", "CT", "BM", "MG", "CD", "RF"):
    METRICS.append(Metric("image_" + code, "영상검사", code, "건", "Da", 9, "category", code))
METRICS.append(Metric("image_other", "영상검사", "기타", "건", "Da", 9, "other"))
METRICS.append(Metric("lab_total", "진단검사", "합계", "건", "Ca", 17, emphasis=True))
for col, label in enumerate(("진단혈액", "일반화학", "면역화학", "일반소변", "혈액은행", "분자진단", "수기면역", "외주검사", "임의검사"), 8):
    METRICS.append(Metric("lab_" + str(col), "진단검사", label, "건", "Ca", col))
METRICS.append(Metric("endo_total", "내시경", "합계", "건", "Ea", 9, emphasis=True))
for code in ("CLO", "colon", "gastro", "CPP", "EMR", "PP", "결장 CPP", "결장 EMR", "SIG 출혈", "Sigmoidoscopy", "결장 출혈지혈법", "결장 이물제거", "상부 EMR", "상부 출혈지혈법"):
    METRICS.append(Metric("endo_" + code, "내시경", code, "건", "Ea", 9, "category", code))
METRICS += [Metric("pt_total", "물리치료", "합계", "건", "Za", 9, emphasis=True),
            Metric("pt_PT", "물리치료", "PT · PT2 포함", "건", "Za", 9, "pt"),
            Metric("pt_MT", "물리치료", "MT", "건", "Za", 9, "category", "MT")]


def month_values(data, year, month, doctor):
    pools = {n: [r for r in data[n] if (r.year, r.month) == (year, month)] for n in SHEETS}
    selected, detail = {}, {}
    for n in SHEETS:
        if n == "H":
            selected[n] = pools[n] if doctor == ALL else []
            detail[n] = selected[n]
            continue
        detail[n] = [r for r in pools[n] if not is_total(n, r) and (doctor == ALL or r.txt(7) == doctor)]
        if doctor == ALL and pools[n]:
            selected[n] = [r for r in pools[n] if is_total(n, r)]
            if len(selected[n]) != 1:
                raise ValueError(f"{year}년 {month}월 {n}: 합계 행이 {len(selected[n])}개입니다. 원본을 확인해주세요.")
        else:
            selected[n] = detail[n]
    values = {}
    for metric in METRICS:
        n = metric.source
        if metric.kind in ("combined", "ratio", "other"):
            continue
        if not pools[n] or (n == "H" and doctor != ALL):
            values[metric.key] = None
            continue
        rows = selected[n]
        if metric.kind == "child":
            rows = [r for r in detail[n] if "소아청소년과" in r.txt(6)]
        elif metric.kind == "adult":
            rows = [r for r in detail[n] if "소아청소년과" not in r.txt(6)]
        elif metric.kind == "category":
            rows = [r for r in detail[n] if r.txt(8) == metric.category]
        elif metric.kind == "pt":
            rows = [r for r in detail[n] if r.txt(8) in ("PT", "PT2")]
        values[metric.key] = sum(r.num(metric.col) for r in rows)
    if doctor == ALL:
        a, b = values["revenue"], values["revenue_check"]
        values["revenue_all"] = a + b if a is not None and b is not None else None
    else:
        values["revenue_all"] = values["revenue"]
    for suffix in ("out", "in"):
        a, b = values["revenue_" + suffix], values["patients_" + suffix]
        values["daily_" + suffix] = a / b if a is not None and b else None
    total = values["image_total"]
    values["image_other"] = None if total is None else total - sum(values["image_" + c] or 0 for c in ("DR", "MR", "CT", "BM", "MG", "CD", "RF"))
    return values


def cumulative(values, months):
    out = {}
    for metric in METRICS:
        entries = [values[m][metric.key] for m in months if values[m][metric.key] is not None]
        out[metric.key] = sum(entries) if entries else None
    for suffix in ("out", "in"):
        rk, pk = "revenue_" + suffix, "patients_" + suffix
        paired = [m for m in months if values[m][rk] is not None and values[m][pk] is not None]
        denominator = sum(values[m][pk] for m in paired)
        out["daily_" + suffix] = sum(values[m][rk] for m in paired) / denominator if denominator else None
    return out


def format_value(value):
    return "—" if value is None else f"{value:,.0f}"


def coverage(data, year, months):
    return {n: [m for m in months if any((r.year, r.month) == (year, m) for r in data[n])] for n in SHEETS}


GROUPS = [("경영 실적", ("진료수입", "환자수", "일당진료비")),
          ("검사 실적", ("영상검사", "진단검사")),
          ("내시경 · 물리치료", ("내시경", "물리치료"))]


def make_pdf(data, year, doctor, months, end_month, mode="summary", hospital="히즈메디병원"):
    """summary: selected month+YTD portrait, trend: chosen months landscape, <=6 per page."""
    from reportlab.pdfgen.canvas import Canvas
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.colors import HexColor
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    fontdir = Path(__file__).parent / "fonts"
    for name, filename in (("ReportKR", "NanumGothic-Regular.ttf"), ("ReportKR-Bold", "NanumGothic-Bold.ttf")):
        if name not in pdfmetrics.getRegisteredFontNames():
            pdfmetrics.registerFont(TTFont(name, str(fontdir / filename)))
    month_list = sorted(set(months))
    annual_months = [m for m in available_months(data, year) if m <= end_month]
    wanted = annual_months if mode == "summary" else month_list
    values = {m: month_values(data, year, m, doctor) for m in wanted}
    totals = cumulative(values, wanted)
    chunks = [month_list[i:i+6] for i in range(0, len(month_list), 6)] if mode == "trend" else [[end_month]]
    page_size = landscape(A4) if mode == "trend" else A4
    w, h = page_size
    stream = io.BytesIO()
    canvas = Canvas(stream, pagesize=page_size)
    canvas.setTitle(f"{hospital} {year}년 실적 보고서 - {doctor}")
    canvas.setAuthor(hospital)
    ink, muted, accent, pale, rule = map(HexColor, ("#172D42", "#586778", "#165C71", "#EDF4F6", "#D9E1E6"))
    margin = 34 if mode == "trend" else 38
    right = w - margin
    page_no = 0
    page_count = len(chunks) * len(GROUPS)
    stamp = datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M")

    def draw(txt, x, y, size=11, bold=False, color=ink, align="left"):
        canvas.setFont("ReportKR-Bold" if bold else "ReportKR", size)
        canvas.setFillColor(color)
        getattr(canvas, {"left": "drawString", "right": "drawRightString", "center": "drawCentredString"}[align])(x, y, str(txt))

    for chunk in chunks:
        for title, sections in GROUPS:
            page_no += 1
            canvas.setFillColor(accent)
            canvas.rect(margin, h-43, 26, 4, fill=1, stroke=0)
            draw(hospital, margin+36, h-44, 11, True, accent)
            draw(title + " 보고서", margin, h-77, 23, True)
            if mode == "summary":
                subtitle = f"{year}년 {end_month}월  |  {doctor}  |  누계: 1~{end_month}월 중 자료가 있는 월"
            else:
                subtitle = f"{year}년  |  {doctor}  |  월별 비교: " + ", ".join(f"{m}월" for m in chunk)
            draw(subtitle, margin, h-99, 10.5, color=muted)
            if mode == "trend":
                draw("누계 범위: " + ", ".join(f"{m}월" for m in month_list), margin, h-115, 9, color=muted)
            top = h-137 if mode == "trend" else h-126
            label_w = 151 if mode == "trend" else 236
            headers = ([f"{m}월" for m in chunk] if mode == "trend" else [f"{end_month}월", "누계"]) + (["선택월 누계"] if mode == "trend" else [])
            col_w = (right-margin-label_w) / len(headers)
            canvas.setFillColor(ink)
            canvas.rect(margin, top-25, right-margin, 25, fill=1, stroke=0)
            draw("항목 / 단위", margin+9, top-17, 10.5, True, HexColor("#FFFFFF"))
            for j, label in enumerate(headers):
                draw(label, margin+label_w+col_w*(j+1)-8, top-17, 10, True, HexColor("#FFFFFF"), "right")
            y = top-25
            row_h = 14.7 if mode == "trend" else 26
            band_h = 18 if mode == "trend" else 25
            for section in sections:
                y -= band_h
                canvas.setFillColor(pale)
                canvas.rect(margin, y, right-margin, band_h, fill=1, stroke=0)
                draw(section, margin+9, y+(5 if mode == "trend" else 8), 10.5 if mode == "trend" else 12, True, accent)
                for metric in [t for t in METRICS if t.section == section]:
                    y -= row_h
                    if metric.emphasis:
                        canvas.setFillColor(HexColor("#F7F9FB"))
                        canvas.rect(margin, y, right-margin, row_h, fill=1, stroke=0)
                    baseline = y+(4.1 if mode == "trend" else 7)
                    font_size = 10 if mode == "trend" else 12.5
                    draw(f"{metric.label} ({metric.unit})", margin+9, baseline, font_size, metric.emphasis)
                    cells = ([values[m][metric.key] for m in chunk] if mode == "trend" else [values[end_month][metric.key]]) + [totals[metric.key]]
                    for j, value in enumerate(cells):
                        label = format_value(value)
                        # Values are never truncated. Six-month panels reserve >=89pt per column.
                        draw(label, margin+label_w+col_w*(j+1)-8, baseline, font_size, metric.emphasis, align="right")
                    canvas.setStrokeColor(rule)
                    canvas.setLineWidth(.25)
                    canvas.line(margin, y, right, y)
            footer_y = 52
            if y < footer_y + 45:
                raise ValueError("PDF 표가 페이지 높이를 초과했습니다.")
            draw("— : 자료 없음·해당 없음·분모 0   /   0 : 자료가 있는 월의 실적 0", margin, footer_y+16, 8.5, color=muted)
            draw("일당진료비 누계 = 동일 기간 진료수입 합계 ÷ 환자수 합계", margin, footer_y+3, 8.5, color=muted)
            cov = coverage(data, year, wanted)
            missing = [f"{n}({','.join(str(m) for m in wanted if m not in ms)}월)" for n, ms in cov.items() if len(ms) < len(wanted) and (n != "H" or doctor == ALL)]
            if missing:
                msg = "자료 없는 월: " + " / ".join(missing)
                # Wrap the coverage note to avoid long historical selections spilling over.
                lines = []
                for word in msg.split(" / "):
                    if lines and pdfmetrics.stringWidth(lines[-1] + " / " + word, "ReportKR", 8) < right-margin:
                        lines[-1] += " / " + word
                    else:
                        lines.append(word)
                for k, line in enumerate(lines):
                    draw(line, margin, y-16-k*11, 8, color=muted)
            canvas.setStrokeColor(rule)
            canvas.line(margin, 35, right, 35)
            draw(f"조회본 생성 {stamp}", margin, 21, 8, color=muted)
            draw(f"{page_no} / {page_count}", right, 21, 8, color=muted, align="right")
            canvas.showPage()
    canvas.save()
    return stream.getvalue()
