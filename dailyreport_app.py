"""Hismedi 진료현황 (Streamlit / Google Sheets).

Secrets: [gcp_service_account], [gsheet] spreadsheet_id.
데이터: 20YY (연도별 시트; 기존 20YY_Q1~Q4도 전환기 지원), RAW_검진, PIN_관리.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from PIL import Image
from pathlib import Path
from zoneinfo import ZoneInfo
import html
import base64
import re

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

st.set_page_config(page_title="Hismedi 진료현황", page_icon="🏥", layout="wide", initial_sidebar_state="expanded")

SHEET_PIN = "PIN_관리"
FIELDS = ["일자", "진료과", "의사명", "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래수입", "입원수입", "검진제외", "검진포함"]
NUMERIC = FIELDS[3:]
CLINICAL_FIELDS = ["외래", "입원", "퇴원", "재원", "외래수입", "입원수입", "검진제외"]
QUARTER_RE = re.compile(r"^(20\d{2})_Q[1-4]$")
YEAR_RE = re.compile(r"^20\d{2}$")
TZ = ZoneInfo("Asia/Seoul")

st.markdown("""<style>
.block-container {padding-top:.85rem; padding-bottom:2rem; max-width:100%}
[data-testid="stSidebar"] {background:#f7f9fc}
[data-testid="stSidebar"] {min-width:290px;max-width:320px}
[data-testid="stSidebar"] [data-testid="stImage"] img {object-fit:contain!important;object-position:left center!important;max-width:100%;height:auto!important}
/* Readable sidebar; the native tabs are replaced by a radio menu. */
[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {padding-top:0px!important}
[data-testid="stSidebar"] label, [data-testid="stSidebar"] p {font-size:15px!important}
[data-testid="stSidebar"] .stMarkdown h3 {font-size:19px!important;font-weight:750!important}
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {font-weight:700!important;font-size:15px!important}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {font-size:13px!important}
/* Main navigation is an ordinary horizontal radio, with visible text even if BaseWeb tabs are clipped. */
[data-testid="stMain"] [role="radiogroup"] {gap:12px!important;flex-wrap:wrap!important}
[data-testid="stMain"] [role="radiogroup"] label {border:1px solid #d4dfec!important;border-radius:8px!important;padding:10px 17px!important;background:#f5f8fc!important}
[data-testid="stMain"] [role="radiogroup"] label p {font-size:16px!important;font-weight:750!important;color:#162f50!important}
[data-testid="stSidebar"] [data-baseweb="popover"] [role="listbox"],
[data-baseweb="popover"] [role="listbox"] {max-height:78vh!important;}
[data-testid="stSidebar"] .stButton button {width:100%}
.report-scroll {overflow-x:auto;max-width:100%;border:1px solid #d5dde7;border-radius:8px;margin-top:.15rem}
.report-table {border-collapse:collapse;width:100%;min-width:1180px;font-size:14px;text-align:center;line-height:1.42}
.report-table th {background:#e2e9f2;color:#1e334e;border:1px solid #cbd5e1;padding:10px 8px;white-space:nowrap;font-size:14px}
.report-table td {border:1px solid #e4e8ef;padding:9px 9px;white-space:nowrap;color:#1b293b}
.report-table td:nth-child(2),.report-table td:nth-child(3){text-align:left}
.report-table tbody tr:nth-child(even):not(.total) td{background:#f8fafc}
.report-table tbody tr.total td{background:#dfeaf9!important;font-weight:750;border-top:2px solid #769dce}
.report-table tbody tr.subtotal td{background:#eef4fb!important;font-weight:700;border-top:1px solid #a9c2df}
.report-table tbody tr.exam td{background:#f2f7f4}
.report-table .num{text-align:right;font-variant-numeric:tabular-nums}
.month-table {min-width:1300px}
.month-table th,.month-table td {padding:10px 8px;font-size:14px}
.month-table th:last-child,.month-table td:last-child {background:#e9f1fc;font-weight:750}
/* Center the branded sidebar section, emphasize the date values without altering other widgets. */
[data-testid="stSidebar"] [data-testid="stDateInput"] input {font-size:16px!important;font-weight:750!important;color:#163b67!important;background:#fff!important}
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {font-size:16px!important;font-weight:750!important}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] {font-size:15px!important}
.report-note{font-size:13px;color:#64748b;margin-top:.5rem}
/* Footer credit is independent of Streamlit Community Cloud branding. */
.anycorea-credit {position:fixed;bottom:12px;right:16px;z-index:5;
 background:rgba(255,255,255,.94);color:#617084;font-size:12px;
 padding:5px 10px;border:1px solid #dce3ec;border-radius:7px;
 box-shadow:0 1px 4px rgba(0,0,0,.05);pointer-events:none}
</style>""", unsafe_allow_html=True)


def number(x):
    try:
        value = str(x).strip().replace(",", "").replace("₩", "")
        return int(round(float(value))) if value else 0
    except (TypeError, ValueError, OverflowError):
        return 0


def parse_day(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if not s or s.startswith("#"):
        return None
    # 구글시트·엑셀에서 날짜가 일련번호(예 46334)로 반환되는 경우
    if re.fullmatch(r"\d{5}(?:\.0+)?", s):
        serial = int(float(s))
        if 30000 <= serial <= 80000:
            return date(1899, 12, 30) + timedelta(days=serial)
    s = re.sub(r"\.0+$", "", s)
    compact = re.sub(r"\s+", "", s)
    compact = compact.rstrip(".")
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d%H:%M:%S"):
        try:
            return datetime.strptime(compact, fmt).date()
        except ValueError:
            pass
    return None


# 이름이 달라도 같은 진료과로 묶고, 의사 이름 없는 자료는 공통으로 관리합니다.
EXCLUDED_HOSPITAL_DEPTS = {"건강검진"}  # 과별 조회에는 표시하지만 병원 총합에서는 제외
DEPT_ALIASES = {"일반외과": "외과"}
NO_PROVIDER = "공통"
DEPT_DISPLAY_ORDER = ["소아청소년과", "내과", "외과", "정형외과", "건강검진", "마취통증의학과", "진단검사의학과"]


def order_departments(values):
    available = {str(v) for v in values if str(v).strip() and str(v) != "전체"}
    return [d for d in DEPT_DISPLAY_ORDER if d in available] + sorted(available - set(DEPT_DISPLAY_ORDER))


@st.cache_data(show_spinner=False)
def padded_logo(path: str):
    """Add a white safety margin so logo never touches Streamlit image edges."""
    with Image.open(path) as img:
        img = img.convert("RGBA")
        # Avoid visually clipping original image even when the source has no margins.
        margin = round(max(img.size) * .16)
        canvas = Image.new("RGBA", (img.width + margin * 2, img.height + margin * 2), (255, 255, 255, 255))
        canvas.alpha_composite(img, (margin, margin))
        output = BytesIO()
        canvas.convert("RGB").save(output, format="PNG")
        return output.getvalue()


def show_logo(path: str, width: int, *, centered: bool = False):
    """HTML image with its own explicit canvas. Avoid Streamlit image clipping/cropping."""
    if not Path(path).is_file():
        st.markdown("**HISMEDI HOSPITAL**")
        return
    binary = padded_logo(path)
    data = base64.b64encode(binary).decode("ascii")
    align = "center" if centered else "left"
    st.markdown(
        f'<div style="display:flex;justify-content:{align};align-items:center;'
        f'width:100%;min-height:{int(width*1.04)}px;margin:0 0 4px 0;overflow:visible">'
        f'<img src="data:image/png;base64,{data}" alt="Hismedi Hospital" '
        f'style="width:{width}px;height:auto;max-width:100%;display:block;object-fit:contain;'
        f'overflow:visible;clip-path:none"/></div>',
        unsafe_allow_html=True,
    )


def dept_normalize(raw):
    dept = re.sub(r"\s+", " ", str(raw or "").strip())
    dept = re.sub(r"\s*(치료|진료)$", "", dept).strip()
    return DEPT_ALIASES.get(dept, dept)


def provider_normalize(raw):
    name = re.sub(r"\s+", "", str(raw or "").strip())
    if not name or name in {"공통", "미지정", "-"}:
        return NO_PROVIDER
    # 접미어가 붙은 동일 의사명: 유현수PD -> 유현수
    name = re.sub(r"(?i)(?:PD)$", "", name).strip()
    return name or NO_PROVIDER


def parse_provider(text):
    source = str(text or "").strip()
    if not source or source in {"합계", "총계", "소계", "전체", "진료과"}:
        return None
    matched = re.fullmatch(r"(.*?)\s*\(([^()]*)\)", source)
    if matched:
        dept, provider = matched.groups()
    else:
        # '소아청소년과', '외과 치료'처럼 괄호 없는 행은 과 공통실적으로 분류
        dept, provider = source, NO_PROVIDER
    dept = dept_normalize(dept)
    if not dept or dept in {"합계", "총계", "소계", "전체"}:
        return None
    return dept, provider_normalize(provider)


@st.cache_resource
def gclient():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def spreadsheet():
    return gclient().open_by_key(st.secrets["gsheet"]["spreadsheet_id"])


@st.cache_data(ttl=180, show_spinner=False)
def load_source():
    book = spreadsheet()
    clinical_rows = []
    source_titles = []
    worksheets = book.worksheets()
    year_names = {ws.title for ws in worksheets if YEAR_RE.fullmatch(ws.title)}
    # 같은 연도 시트와 기존 분기 시트가 동시에 있으면 연도 시트만 사용 (중복 방지).
    sources = [ws for ws in worksheets if YEAR_RE.fullmatch(ws.title) or
               (QUARTER_RE.fullmatch(ws.title) and ws.title[:4] not in year_names)]
    sources.sort(key=lambda ws: ws.title)
    for ws in sources:
        source_titles.append(ws.title)
        rows = ws.get_all_values()
        # 원본 첫 2행은 병합 헤더. 3행부터 자료.
        for r in rows[2:]:
            if len(r) < 11:
                continue
            when = parse_day(r[0])
            parsed = parse_provider(r[1])
            if when is None or parsed is None:
                continue  # 합계/소계 등 원본 집계행 제외 (중복 방지)
            dept, doctor = parsed
            out_revenue = number(r[7])
            in_revenue = number(r[9])
            clinical_rows.append({
                "일자": when, "진료과": dept, "의사명": doctor,
                "외래": number(r[2]), "입원": number(r[4]), "퇴원": number(r[5]), "재원": number(r[6]),
                "외래수입": out_revenue, "입원수입": in_revenue,
                "검진제외": out_revenue + in_revenue,
            })  # 낮병동: 환자수 D, 수입 I 둘 다 제외

    exam_rows = []
    try:
        for r in book.worksheet("RAW_검진").get_all_values()[1:]:
            if len(r) < 6:
                continue
            when = parse_day(r[1]) or parse_day(r[0])  # Date(B열) 우선
            if when is not None:
                exam_rows.append({"일자": when, "남": number(r[2]), "여": number(r[3]),
                                  "계": number(r[4]), "검진수입": number(r[5])})
    except WorksheetNotFound:
        pass
    return (pd.DataFrame(clinical_rows, columns=["일자", "진료과", "의사명"] + CLINICAL_FIELDS),
            pd.DataFrame(exam_rows, columns=["일자", "남", "여", "계", "검진수입"]),
            tuple(source_titles))


@st.cache_data(ttl=20, show_spinner=False)
def load_accounts():
    rows = spreadsheet().worksheet(SHEET_PIN).get_all_values()
    if not rows:
        return pd.DataFrame(columns=["진료과", "의사", "PIN", "권한", "_row"])
    headers = [x.strip() for x in rows[0]]
    required = {"진료과", "의사", "PIN", "권한"}
    if not required.issubset(headers):
        raise ValueError("PIN_관리의 1행은 진료과, 의사, PIN, 권한이어야 합니다.")
    accounts = []
    for row_index, row in enumerate(rows[1:], 2):
        obj = dict(zip(headers, row))
        if not str(obj.get("의사", "")).strip():
            continue
        accounts.append({"진료과": dept_normalize(obj.get("진료과", "")),
                         "의사": provider_normalize(obj.get("의사", "")),
                         "PIN": str(obj.get("PIN", "")).strip(),
                         "권한": str(obj.get("권한", "user")).strip().lower(), "_row": row_index})
    return pd.DataFrame(accounts, columns=["진료과", "의사", "PIN", "권한", "_row"])


def pin_valid(pin):
    return bool(re.fullmatch(r"\d{4}", str(pin)))


def clear_session():
    # Date inputs have explicit keys. Remove them on logout/login so the new
    # session starts on yesterday, and report/filter never retain old ranges.
    for key in ("account_row", "login_pin", "login_name", "report_start_day", "report_end_day", "main_nav"):
        st.session_state.pop(key, None)


def reset_report_dates():
    yesterday = datetime.now(TZ).date() - timedelta(days=1)
    st.session_state["report_start_day"] = yesterday
    st.session_state["report_end_day"] = yesterday



def logged_in_account(accounts):
    rid = st.session_state.get("account_row")
    if rid is None:
        return None
    matched = accounts[accounts["_row"] == rid]
    if len(matched) != 1:
        clear_session()
        return None
    row = matched.iloc[0].to_dict()
    if row["PIN"] != st.session_state.get("login_pin") or row["의사"] != st.session_state.get("login_name"):
        clear_session()
        return None
    return row


def update_pin_account(row_index, dept, name, pin, role):
    spreadsheet().worksheet(SHEET_PIN).update(
        range_name=f"A{row_index}:D{row_index}",
        values=[[dept, name, pin, role]], value_input_option="RAW")
    load_accounts.clear()


def summarize_rows(df, day_label, group_cols):
    result = []
    if df.empty:
        return result
    grouped = df.groupby(group_cols, sort=True, dropna=False)[CLINICAL_FIELDS].sum()
    for index, values in grouped.iterrows():
        keys = (index,) if len(group_cols) == 1 else index
        names = dict(zip(group_cols, keys))
        row = {"일자": day_label, "진료과": names.get("진료과", "전체"),
               "의사명": names.get("의사명", "합계")}
        row.update({k: 0 for k in NUMERIC})
        row.update({k: int(values[k]) for k in CLINICAL_FIELDS})
        row["검진포함"] = row["검진제외"]
        result.append(row)
    return result


def total_row(rows, date_label, label, category="합계"):
    return {"일자": date_label, "진료과": label, "의사명": category,
            **{k: sum(int(r.get(k, 0)) for r in rows) for k in NUMERIC}}


def has_any_activity(row):
    """Hide zero-only individual/provider rows, but keep totals calculable."""
    return any(number(row.get(field, 0)) != 0 for field in NUMERIC)


def report_data(clinical, exams, start, end, dept, doctor, master, user):
    frame = clinical[(clinical["일자"] >= start) & (clinical["일자"] <= end)].copy()
    exam = exams[(exams["일자"] >= start) & (exams["일자"] <= end)].copy()
    label = start.isoformat() if start == end else f"{start:%Y-%m-%d} ~ {end:%Y-%m-%d}"
    all_clinical = summarize_rows(frame, label, ["진료과", "의사명"])
    exam_record = None
    if not exam.empty:
        sums = exam[["남", "여", "계", "검진수입"]].sum()
        exam_record = {"일자": label, "진료과": "건강검진", "의사명": "검진실적",
                       **{c: 0 for c in NUMERIC}}
        exam_record.update({"남": int(sums["남"]), "여": int(sums["여"]), "계": int(sums["계"]),
                            "검진포함": int(sums["검진수입"])})

    # 병원 총합: 임상원본 건강검진은 제외. RAW_검진은 독립 검진실적으로 반영.
    hospital_sources = [r for r in all_clinical if r["진료과"] not in EXCLUDED_HOSPITAL_DEPTS]
    if exam_record is not None:
        hospital_sources.append(exam_record)
    hospital_total = total_row(hospital_sources, label, "병원 전체", "합계")
    hospital_total["_class"] = "total"

    if master:
        filtered = [r.copy() for r in all_clinical
                    if (dept == "전체" or r["진료과"] == dept)
                    and (doctor == "전체" or r["의사명"] == doctor)]
        # 건강검진 최홍우의 외래 환자수는 건강검진 별도 집계와 혼동하지 않도록 숨깁니다.
        for r in filtered:
            if r["진료과"] == "건강검진" and r["의사명"] == "최홍우":
                r["외래"] = 0
        shown = []
        # 개인 실적이 모두 0이면 행을 숨깁니다. 합계 계산은 원본 자료를 유지합니다.
        filtered = [r for r in filtered if has_any_activity(r)]
        for dept_name in order_departments({r["진료과"] for r in filtered}):
            group = sorted((r for r in filtered if r["진료과"] == dept_name),
                           key=lambda r: (r["의사명"] == NO_PROVIDER, r["의사명"]))
            shown.extend(group)
            if doctor == "전체" and group:
                subtotal = total_row(group, label, dept_name, "진료과 합계")
                subtotal["_class"] = "subtotal"
                shown.append(subtotal)
        # 검진은 건강검진 과로 표시합니다. 다른 진료과를 선택한 경우에는 보이지 않습니다.
        if (dept in ("전체", "건강검진") and doctor == "전체" and exam_record is not None):
            shown.append(exam_record)
            # 건강검진 조회라면 RAW 검진 수치도 선택 합계에 포함합니다.
        # 화면에 표시된 소계는 다시 더하지 않으며, 선택 조건에 따른 합계만 산출.
        if dept == "전체" and doctor == "전체":
            aggregate = hospital_total
        else:
            aggregate_sources = filtered + ([exam_record] if dept == "건강검진" and doctor == "전체" and exam_record is not None else [])
            aggregate = total_row(aggregate_sources, label, dept if dept != "전체" else "전체", "합계")
        return pd.DataFrame(shown, columns=FIELDS + ["_class"]), aggregate, []

    # 일반 계정: 본인/소속과/병원 전체 합계. 타 의사 개인 행은 절대 노출하지 않음.
    own_dept, own_name = dept_normalize(user["진료과"]), provider_normalize(user["의사"])
    own_rows = [r for r in all_clinical if r["진료과"] == own_dept and r["의사명"] == own_name]
    dept_rows = [r for r in all_clinical if r["진료과"] == own_dept]
    own = total_row(own_rows, label, own_dept, own_name)
    dept_total = total_row(dept_rows, label, own_dept, "진료과 합계")
    dept_total["_class"] = "subtotal"
    # 로그인한 의사에게도 병원 전체 건강검진 원본의 집계치를 독립행으로 제공합니다.
    # 건강검진 과 소속 의사는 최홍우의 임상 외래 환자수를 노출하지 않습니다.
    if own_dept == "건강검진" and own_name == "최홍우":
        own["외래"] = 0
    visible = ([own] if has_any_activity(own) else [])
    if has_any_activity(dept_total):
        visible.append(dept_total)
    if exam_record is not None:
        exam_record["_class"] = "exam"
        visible.append(exam_record)
    visible.append(hospital_total)
    return pd.DataFrame(visible, columns=FIELDS + ["_class"]), None, []


# 기간별과 무관한 자동 주간/월간 집계
MONTH_METRICS = {
    "외래환자수": "외래", "재원환자수": "재원", "검진환자수": "계",
    "외래수입": "외래수입", "입원수입": "입원수입",
    "전체수입(검진제외)": "검진제외", "전체수입(검진포함)": "검진포함",
}


def period_total(clinical, exams, start, end, dept="전체", doctor="전체"):
    """집계 전용: 원본 건강검진은 병원 총합 제외, RAW_검진만 검진 실적에 포함."""
    sub = clinical[(clinical["일자"] >= start) & (clinical["일자"] <= end)]
    if dept != "전체":
        sub = sub[sub["진료과"] == dept]
    if doctor != "전체":
        sub = sub[sub["의사명"] == doctor]
    if dept == "전체" and doctor == "전체":
        sub = sub[~sub["진료과"].isin(EXCLUDED_HOSPITAL_DEPTS)]
    # 건강검진 임상 원본의 외래 환자수는 별도 RAW 검진 환자수와 혼동하지 않도록 표시 안 함
    totals = {col: int(sub[col].sum()) if col in sub else 0 for col in CLINICAL_FIELDS}
    if dept == "건강검진":
        totals["외래"] = 0
    extras = {"남": 0, "여": 0, "계": 0, "검진포함": totals["검진제외"]}
    # 검진 환자수/검진수입은 병원 전체/건강검진 조회에만 포함
    if (dept in ("전체", "건강검진")) and doctor == "전체":
        e = exams[(exams["일자"] >= start) & (exams["일자"] <= end)]
        for k in ("남", "여", "계"):
            extras[k] = int(e[k].sum()) if k in e else 0
        extras["검진포함"] += int(e["검진수입"].sum()) if "검진수입" in e else 0
    return {**totals, **extras}


def weekly_table(clinical, exams, dept, doctor, master, user, today):
    """데이터가 존재하는 주만 최근 주부터: master는 조건별 집계 1행, 일반은 본인/과/병원."""
    dates = list(clinical["일자"]) + list(exams["일자"])
    if not dates:
        return pd.DataFrame(columns=FIELDS + ["_class"])
    first = min(dates)
    latest = max(max(dates), today)
    first_monday = first - timedelta(days=first.weekday())
    last_monday = latest - timedelta(days=latest.weekday())
    rows = []
    current = last_monday
    while current >= first_monday:
        week_end = current + timedelta(days=6)
        label = f"{current:%Y-%m-%d} ~ {week_end:%Y-%m-%d}"
        if master:
            values = period_total(clinical, exams, current, week_end, dept, doctor)
            if has_any_activity(values):
                rows.append({"일자": label, "진료과": dept if dept != "전체" else "병원 전체",
                             "의사명": doctor if doctor != "전체" else "합계", **values, "_class": "total"})
        else:
            own_dept = dept_normalize(user["진료과"])
            own_name = provider_normalize(user["의사"])
            candidates = [
                (own_dept, own_name, own_dept, own_name, ""),
                (own_dept, "진료과 합계", own_dept, "전체", "subtotal"),
                ("건강검진", "검진실적", "건강검진", "전체", "exam"),
                ("병원 전체", "합계", "전체", "전체", "total"),
            ]
            for dd, nn, fdept, fdoctor, kind in candidates:
                if dd == "건강검진" and own_dept == "건강검진":
                    # 중복 건강검진 표시는 방지
                    continue
                values = period_total(clinical, exams, current, week_end, fdept, fdoctor)
                if has_any_activity(values):
                    rows.append({"일자": label, "진료과": dd, "의사명": nn, **values, "_class": kind})
        current -= timedelta(days=7)
    return pd.DataFrame(rows, columns=FIELDS + ["_class"])


def monthly_table(clinical, exams, year, metric, dept, doctor, master, user):
    """12개월 고정 컬럼, 실적별 행 및 연간 합계; 과거 데이터도 연도 시트에서 읽음."""
    field = MONTH_METRICS[metric]
    year_data = clinical[clinical["일자"].map(lambda d: d.year == year)]
    years_exam = exams[exams["일자"].map(lambda d: d.year == year)]
    entries = []
    if master:
        clinical_keys = year_data[["진료과", "의사명"]].drop_duplicates().itertuples(index=False, name=None)
        for dep, name in clinical_keys:
            if dept != "전체" and dep != dept:
                continue
            if doctor != "전체" and name != doctor:
                continue
            # 연도별 개인행: 일부 임상 건강검진의 외래는 제외
            entries.append((dep, name, dep, name, ""))
        if dept in ("전체", "건강검진") and doctor == "전체":
            entries.append(("건강검진", "검진실적", "건강검진_RAW", "전체", "exam"))
    else:
        own_dept = dept_normalize(user["진료과"])
        own_name = provider_normalize(user["의사"])
        entries = [
            (own_dept, own_name, own_dept, own_name, ""),
            (own_dept, "진료과 합계", own_dept, "전체", "subtotal"),
            ("건강검진", "검진실적", "건강검진_RAW", "전체", "exam"),
            ("병원 전체", "합계", "전체", "전체", "total"),
        ]
    out = []
    for dep, name, fdept, fdoctor, kind in entries:
        row = {"진료과": dep, "의사명": name}
        for month in range(1, 13):
            first = date(year, month, 1)
            last = date(year + 1, 1, 1) - timedelta(days=1) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
            if fdept == "건강검진_RAW":
                em = years_exam[(years_exam["일자"] >= first) & (years_exam["일자"] <= last)]
                value = int(em["계"].sum()) if field == "계" else int(em["검진수입"].sum()) if field == "검진포함" else 0
            else:
                value = period_total(clinical, exams, first, last, fdept, fdoctor).get(field, 0)
                if dep == "건강검진" and name != "검진실적" and field == "외래":
                    value = 0
            row[f"{month}월"] = value
        row["누계"] = sum(row[f"{m}월"] for m in range(1, 13))
        row["_class"] = kind
        if row["누계"] != 0:
            out.append(row)
    # 과/의사별 행이 여러 개인 경우 진료과 합계 추가 (마스터만)
    if master and doctor == "전체":
        detailed = [r for r in out if r["_class"] == ""]
        depts = order_departments({r["진료과"] for r in detailed})
        for d in depts:
            group = [r for r in detailed if r["진료과"] == d]
            if group:
                subtotal = {"진료과": d, "의사명": "진료과 합계", "_class": "subtotal"}
                for col in [f"{m}월" for m in range(1, 13)] + ["누계"]:
                    subtotal[col] = sum(int(r[col]) for r in group)
                out.append(subtotal)
    ordered = order_departments({r["진료과"] for r in out})
    out.sort(key=lambda r: (ordered.index(r["진료과"]) if r["진료과"] in ordered else 999,
                            r["_class"] == "subtotal", r["_class"] == "total", r["의사명"]))
    # 선택 범위에 따른 단일 최종합계 (중복 소계 제외)
    if master:
        total = {"진료과": dept if dept != "전체" else "병원 전체",
                 "의사명": "합계", "_class": "total"}
        for month in range(1, 13):
            first = date(year, month, 1)
            last = date(year + 1, 1, 1) - timedelta(days=1) if month == 12 else date(year, month + 1, 1) - timedelta(days=1)
            total[f"{month}월"] = period_total(clinical, exams, first, last, dept, doctor).get(field, 0)
        total["누계"] = sum(total[f"{m}월"] for m in range(1, 13))
        out.append(total)
    return pd.DataFrame(out, columns=["진료과", "의사명"] + [f"{m}월" for m in range(1, 13)] + ["누계", "_class"])


def format_monthly(df):
    cols = ["진료과", "의사명"] + [f"{m}월" for m in range(1, 13)] + ["누계"]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    rows = []
    for _, row in df.iterrows():
        cls = str(row.get("_class", ""))
        cells = "".join(f"<td>{html.escape(str(row[c]))}</td>" if c in ("진료과", "의사명")
                        else f'<td class="num">{int(row[c]):,}</td>' for c in cols)
        rows.append(f'<tr class="{cls}">{cells}</tr>')
    return f'<div class="report-scroll"><table class="report-table month-table"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


def monthly_excel(df, metric, year):
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter
    wb = Workbook()
    ws = wb.active
    ws.title = f"{year}_{metric}"[:31]
    cols = ["진료과", "의사명"] + [f"{m}월" for m in range(1, 13)] + ["누계"]
    ws.append(cols)
    for _, row in df.iterrows():
        ws.append([row[c] for c in cols])
    for cells in ws:
        for cell in cells:
            cell.alignment = Alignment(horizontal="right" if cell.column > 2 else "left")
            if cell.row == 1:
                cell.fill = PatternFill("solid", fgColor="DFE9F5")
                cell.font = Font(bold=True)
            elif cell.column > 2:
                cell.number_format = "#,##0"
        ws.row_dimensions[cells[0].row].height = 22
    for i in range(1, len(cols)+1):
        ws.column_dimensions[get_column_letter(i)].width = 21 if i <= 2 else 15
    ws.freeze_panes = "C2"
    data = BytesIO()
    wb.save(data)
    return data.getvalue()


def format_report(df, totals):
    head = '''<thead><tr><th rowspan="2">일자</th><th rowspan="2">진료과</th><th rowspan="2">의사명</th>
    <th colspan="4">환자수</th><th colspan="3">환자수(검진)</th><th colspan="4">진료수입</th></tr>
    <tr><th>외래</th><th>입원</th><th>퇴원</th><th>재원</th><th>남</th><th>여</th><th>계</th>
    <th>외래</th><th>입원</th><th>검진제외</th><th>검진포함</th></tr></thead>'''
    body = []
    for _, row in df.iterrows():
        kind = row.get("_class", "")
        if not kind and row["의사명"] == "검진실적":
            kind = "exam"
        cells = "".join(f"<td>{html.escape(str(row[c]))}</td>" for c in FIELDS[:3])
        cells += "".join(f'<td class="num">{int(row[c]):,}</td>' for c in NUMERIC)
        body.append(f'<tr class="{kind}">{cells}</tr>')
    if totals is not None:
        cells = "".join(f'<td class="num">{int(totals[c]):,}</td>' for c in NUMERIC)
        body.append(f'<tr class="total"><td>합계</td><td>전체</td><td>합계</td>{cells}</tr>')
    return f'<div class="report-scroll"><table class="report-table">{head}<tbody>{"".join(body)}</tbody></table></div>'


def to_excel(df, totals):
    # 출력 파일은 선택된 화면 내용과 동일; 단순 데이터 저장이 아닌 2단 헤더 양식
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    book = Workbook()
    ws = book.active
    ws.title = "진료현황"
    ws.append(["일자", "진료과", "의사명", "환자수", None, None, None, "환자수(검진)", None, None, "진료수입", None, None, None])
    ws.append([None, None, None, "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래", "입원", "검진제외", "검진포함"])
    for address in ("A1:A2", "B1:B2", "C1:C2", "D1:G1", "H1:J1", "K1:N1"):
        ws.merge_cells(address)
    for _, row in df.iterrows():
        ws.append([row[c] for c in FIELDS])
    if totals is not None:
        ws.append(["합계", "전체", "합계"] + [totals[c] for c in NUMERIC])
    for rr in ws:
        for cell in rr:
            cell.alignment = Alignment(horizontal="left" if cell.column in (2, 3) and cell.row > 2 else
                                       ("right" if cell.column > 3 and cell.row > 2 else "center"), vertical="center")
            cell.border = Border(bottom=Side(style="hair", color="DBE3EF"))
            if cell.row <= 2:
                cell.fill = PatternFill("solid", fgColor="DFE9F5")
                cell.font = Font(bold=True, color="1E334E")
            elif cell.row == ws.max_row and totals is not None or (cell.row > 2 and str(ws.cell(cell.row, 3).value).endswith("합계")):
                cell.fill = PatternFill("solid", fgColor="E7F0FD")
                cell.font = Font(bold=True)
            if cell.column >= 4 and cell.row > 2:
                cell.number_format = '#,##0'
        ws.row_dimensions[rr[0].row].height = 22
    for col in range(1, 15):
        ws.column_dimensions[get_column_letter(col)].width = 21 if col == 1 else 18 if col in (2, 3) else 15
    ws.freeze_panes = "D3"
    out = BytesIO()
    book.save(out)
    return out.getvalue()


try:
    accounts = load_accounts()
except Exception as ex:
    st.error(f"PIN_관리 시트를 불러오지 못했습니다. 시트명과 공유권한을 확인하세요. ({ex})")
    st.stop()

user = logged_in_account(accounts)
if user is None:
    # Login: hospital mark and title aligned horizontally, with form BELOW them.
    st.markdown('<div style="height:32px"></div>', unsafe_allow_html=True)
    login_col, _ = st.columns([1.45, 1.25], gap="large")
    with login_col:
        logo_col, heading_col = st.columns([1, 4.2], gap="small", vertical_alignment="center")
        with logo_col:
            show_logo("Hismedi_logo.png", 148)
        with heading_col:
            st.markdown('<div style="font-size:clamp(25px,2.5vw,43px);font-weight:800;'
                        'color:#242b35;line-height:1.3;white-space:nowrap">Hismedi 진료현황</div>',
                        unsafe_allow_html=True)
            st.caption("4자리 PIN을 입력한 뒤 Enter를 누르세요.")
        st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
        with st.form("login_form"):
            pin = st.text_input("PIN 코드", type="password", max_chars=4, autocomplete="off")
            login = st.form_submit_button("로그인", use_container_width=True)
        if login:
            matched = accounts[accounts["PIN"] == pin.strip()]
            if not pin_valid(pin) or len(matched) != 1:
                st.error("PIN이 올바르지 않거나 중복되어 있습니다.")
            else:
                account = matched.iloc[0]
                # Clear all prior account/date/menu state, then initialize for this login.
                clear_session()
                reset_report_dates()
                st.session_state["account_row"] = int(account["_row"])
                st.session_state["login_pin"] = str(account["PIN"])
                st.session_state["login_name"] = str(account["의사"])
                st.rerun()
    st.markdown('<div class="anycorea-credit">Created by anycorea</div>', unsafe_allow_html=True)
    st.stop()

is_master = user["권한"] == "master"
try:
    clinical, exams, sheets = load_source()
except Exception as ex:
    st.error(f"실적 원본을 읽지 못했습니다. ({ex})")
    st.stop()

all_days = ([] if clinical.empty else clinical["일자"].tolist()) + ([] if exams.empty else exams["일자"].tolist())
if not all_days:
    st.warning("원본 실적이 없습니다. 2026(연도별) 및 RAW_검진 시트를 확인하세요.")
    st.stop()

min_day, max_day = min(all_days), max(all_days)
yesterday = datetime.now(TZ).date() - timedelta(days=1)
initial_day = yesterday  # 기본값은 전일, 데이터가 없어도 임의로 다른 날짜로 변경하지 않음

with st.sidebar:
    st.markdown('<div style="height:0px"></div>', unsafe_allow_html=True)
    show_logo("Hismedi_logo.png", 98, centered=True)
    st.markdown('<div style="text-align:center;font-size:19px;font-weight:800;'
                'margin-top:-8px">Hismedi 진료현황</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="text-align:center;font-size:14px;color:#4c607b;'
                f'margin-top:5px;font-weight:650">접속: {html.escape(str(user["의사"]))} · '
                f'{html.escape("마스터" if is_master else str(user["진료과"]))}</div>', unsafe_allow_html=True)
    st.divider()
    st.markdown("**조회 기간**")
    if "report_start_day" not in st.session_state:
        st.session_state["report_start_day"] = initial_day
    if "report_end_day" not in st.session_state:
        st.session_state["report_end_day"] = initial_day
    start = st.date_input("시작일", format="YYYY-MM-DD", key="report_start_day")
    end = st.date_input("종료일", format="YYYY-MM-DD", key="report_end_day")
    if start > end:
        st.error("시작일이 종료일보다 늦습니다.")
        st.stop()
    depts = order_departments(set(clinical["진료과"].dropna().tolist()) | {
        x for x in accounts["진료과"].tolist() if x and x != "전체"} | {"건강검진"})
    if is_master:
        dept_choice = st.selectbox("진료과", ["전체"] + depts)
        matching_doctors = clinical if dept_choice == "전체" else clinical[clinical["진료과"] == dept_choice]
        names = sorted(set(matching_doctors["의사명"].tolist()) | {
            str(a["의사"]) for _, a in accounts.iterrows() if a["권한"] != "master" and
            (dept_choice == "전체" or a["진료과"] == dept_choice)})
        if len(names) == 0:
            doctor_choice = "전체"
            st.caption("의사: 등록된 의사 없음")
        elif len(names) == 1:
            doctor_choice = "전체"
            st.caption(f"의사: {names[0]} (1명 · 자동 표시)")
        else:
            doctor_choice = st.selectbox("의사", ["전체"] + names)
    else:
        dept_choice, doctor_choice = user["진료과"], user["의사"]
        st.caption(f"진료과: {dept_choice}")
        st.caption("조회: 본인 실적 / 소속 진료과 합계 / 병원 전체 합계")
    if st.button("🔄 데이터 동기화", use_container_width=True):
        load_source.clear()
        load_accounts.clear()
        st.rerun()
    st.divider()
    if st.button("로그아웃", use_container_width=True):
        clear_session()
        st.rerun()

report, totals, _ = report_data(clinical, exams, start, end, dept_choice, doctor_choice, is_master, user)
st.markdown('<div style="height:48px"></div>', unsafe_allow_html=True)
menu_items = ["📊 진료실적(기간별)", "📅 진료실적(주간)", "📆 진료실적(월간/연간)",
              "🔒 내 PIN 번호 변경", "⬇️ 엑셀다운로드"]
if is_master:
    menu_items.append("⚙️ 권한관리")
active_menu = st.radio("메뉴", menu_items, horizontal=True, label_visibility="collapsed", key="main_nav")
st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)

# 전역 기간 입력과 독립적인 주간·월간 자료
if active_menu == "📊 진료실적(기간별)":
    st.markdown(format_report(report, totals), unsafe_allow_html=True)
    if report.empty:
        st.info("해당 기간에 조회되는 실적이 없습니다.")
    st.caption(f"조회: {start:%Y-%m-%d} ~ {end:%Y-%m-%d} · 낮병동 제외 · 임상원본 건강검진 병원 합계 제외 · RAW_검진 통합")
    if not is_master:
        st.caption("일반 계정에는 타 의사의 개별 실적을 표시하지 않습니다.")

if active_menu == "📅 진료실적(주간)":
    st.caption("월요일~일요일 · 최신 주간부터 표시 · 왼쪽 조회 기간과 무관 · 진료과/의사 필터 적용")
    weekly = weekly_table(clinical, exams, dept_choice, doctor_choice, is_master, user, datetime.now(TZ).date())
    st.markdown(format_report(weekly, None), unsafe_allow_html=True)
    if weekly.empty:
        st.info("해당 조건에 집계된 주간 실적이 없습니다.")

if active_menu == "📆 진료실적(월간/연간)":
    available_years = sorted(set([d.year for d in all_days] + [datetime.now(TZ).year]), reverse=True)
    year_col, metric_col = st.columns([1, 2.5])
    with year_col:
        selected_year = st.selectbox("조회 연도", available_years, index=available_years.index(datetime.now(TZ).year))
    with metric_col:
        selected_metric = st.selectbox("실적 지표", list(MONTH_METRICS.keys()))
    monthly = monthly_table(clinical, exams, selected_year, selected_metric, dept_choice, doctor_choice, is_master, user)
    st.markdown(format_monthly(monthly), unsafe_allow_html=True)
    if monthly.empty:
        st.info("해당 연도의 실적이 없습니다.")
    if selected_metric == "재원환자수":
        st.caption("재원환자수는 원본에 기록된 일자별 재원 수를 합산한 값입니다(월말 재원 또는 고유 환자 수가 아닙니다).")
    st.caption("1월~12월 고정 · 누계 자동 합산 · 왼쪽 조회 기간과 무관 · 진료과/의사 필터 적용")

if active_menu == "🔒 내 PIN 번호 변경":
    st.subheader("내 PIN 번호 변경")
    with st.form("change_own_pin"):
        current = st.text_input("현재 PIN", type="password", max_chars=4)
        new = st.text_input("새 PIN (4자리)", type="password", max_chars=4)
        verify = st.text_input("새 PIN 확인", type="password", max_chars=4)
        submit = st.form_submit_button("PIN 변경")
    if submit:
        if current != user["PIN"]:
            st.error("현재 PIN이 일치하지 않습니다.")
        elif not pin_valid(new) or new != verify:
            st.error("새 PIN은 4자리 숫자이며 확인 값과 일치해야 합니다.")
        elif new in accounts["PIN"].tolist():
            st.error("이미 등록된 PIN입니다.")
        else:
            try:
                update_pin_account(int(user["_row"]), user["진료과"], user["의사"], new, user["권한"])
                clear_session()
                st.success("PIN이 변경되었습니다. 새 PIN으로 다시 로그인하세요.")
                st.rerun()
            except Exception as ex:
                st.error(f"PIN 변경 실패: {ex}")

if active_menu == "⬇️ 엑셀다운로드":
    st.subheader("엑셀다운로드")
    st.caption("기간별/주간 실적은 기존 2단 헤더, 월간/연간 실적은 1~12월/누계 형태로 저장합니다.")
    kind = st.radio("다운로드 종류", ["기간별", "주간", "월간/연간"], horizontal=True)
    if kind == "기간별":
        payload = to_excel(report, totals)
        filename = f"Hismedi_기간별_{start:%Y%m%d}_{end:%Y%m%d}.xlsx"
    elif kind == "주간":
        weekly = weekly_table(clinical, exams, dept_choice, doctor_choice, is_master, user, datetime.now(TZ).date())
        payload = to_excel(weekly, None)
        filename = "Hismedi_주간실적.xlsx"
    else:
        ycol, mcol = st.columns([1, 2])
        years = sorted(set([d.year for d in all_days] + [datetime.now(TZ).year]), reverse=True)
        with ycol:
            export_year = st.selectbox("연도", years)
        with mcol:
            export_metric = st.selectbox("지표", list(MONTH_METRICS))
        monthly = monthly_table(clinical, exams, export_year, export_metric, dept_choice, doctor_choice, is_master, user)
        payload = monthly_excel(monthly, export_metric, export_year)
        filename = f"Hismedi_월간연간_{export_year}_{export_metric}.xlsx"
    st.download_button("⬇️ 조회 결과 엑셀 다운로드", data=payload,
                       file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if is_master and active_menu == "⚙️ 권한관리":
        st.subheader("권한관리")
        st.caption("PIN_관리 시트의 진료과 / 의사 / PIN / 권한 4개 열을 사용합니다. 계정은 추가·삭제할 수 있고, 원본에 새 진료과·의사가 나타나면 조회 목록에 자동 반영됩니다.")
        st.dataframe(accounts[["진료과", "의사", "권한"]], use_container_width=True, hide_index=True)
        action = st.radio("관리 작업", ["계정 추가", "계정 수정", "계정 삭제"], horizontal=True)
        if action == "계정 추가":
            with st.form("account_create"):
                dept = st.selectbox("진료과", ["전체"] + depts + ["➕ 새 진료과 추가"])
                new_dept = st.text_input("새 진료과 이름 (새 진료과 추가 선택 시에만 입력)")
                name = st.text_input("이름")
                pin = st.text_input("PIN (4자리)", type="password", max_chars=4)
                role = st.selectbox("권한", ["user", "master"])
                submitted = st.form_submit_button("계정 추가")
            if submitted:
                name = name.strip()
                if dept == "➕ 새 진료과 추가":
                    dept = dept_normalize(new_dept)
                duplicate = accounts[(accounts["진료과"] == dept) & (accounts["의사"] == name)]
                if not dept or not name or not pin_valid(pin) or (role == "user" and dept == "전체"):
                    st.error("진료과와 이름, 4자리 PIN을 확인하세요. 일반 사용자는 소속 진료과를 지정해야 합니다.")
                elif pin in accounts["PIN"].tolist() or not duplicate.empty:
                    st.error("중복된 PIN 또는 계정입니다.")
                else:
                    try:
                        spreadsheet().worksheet(SHEET_PIN).append_row([dept, name, pin, role], value_input_option="RAW")
                        load_accounts.clear()
                        st.rerun()
                    except Exception as ex:
                        st.error(f"추가 실패: {ex}")
        elif not accounts.empty:
            options = {int(row["_row"]): f"{row['진료과']} · {row['의사']} ({row['권한']})"
                       for _, row in accounts.iterrows()}
            selected = st.selectbox("계정 선택", list(options), format_func=lambda x: options[x])
            item = accounts[accounts["_row"] == selected].iloc[0]
            if action == "계정 수정":
                with st.form("account_edit"):
                    candidates = sorted(set(depts) | {item["진료과"]})
                    departments = ["전체"] + [x for x in candidates if x != "전체"]
                    dept = st.selectbox("진료과", departments, index=departments.index(item["진료과"]))
                    name = st.text_input("이름", value=item["의사"])
                    pin = st.text_input("새 PIN (변경하지 않으려면 빈칸)", type="password", max_chars=4)
                    role = st.selectbox("권한", ["user", "master"], index=0 if item["권한"] != "master" else 1)
                    submitted = st.form_submit_button("수정 저장")
                if submitted:
                    new_pin = pin or item["PIN"]
                    others = accounts[accounts["_row"] != selected]
                    masters = accounts[accounts["권한"] == "master"]
                    if not name.strip() or not pin_valid(new_pin) or (role == "user" and dept == "전체"):
                        st.error("진료과, 이름, PIN을 확인하세요.")
                    elif new_pin in others["PIN"].tolist() or not others[(others["진료과"] == dept) & (others["의사"] == name.strip())].empty:
                        st.error("중복 PIN 또는 중복 계정입니다.")
                    elif item["권한"] == "master" and role != "master" and len(masters) <= 1:
                        st.error("마지막 마스터 권한은 해제할 수 없습니다.")
                    else:
                        try:
                            update_pin_account(selected, dept, name.strip(), new_pin, role)
                            if selected == user["_row"]:
                                clear_session()
                            st.rerun()
                        except Exception as ex:
                            st.error(f"수정 실패: {ex}")
            else:
                with st.form("account_delete"):
                    confirmed = st.checkbox("선택 계정을 삭제합니다.")
                    submitted = st.form_submit_button("계정 삭제")
                if submitted:
                    if not confirmed:
                        st.warning("삭제 확인을 선택하세요.")
                    elif selected == user["_row"]:
                        st.error("로그인한 본인의 계정은 삭제할 수 없습니다.")
                    elif item["권한"] == "master" and len(accounts[accounts["권한"] == "master"]) <= 1:
                        st.error("마지막 마스터 계정은 삭제할 수 없습니다.")
                    else:
                        try:
                            spreadsheet().worksheet(SHEET_PIN).delete_rows(selected)
                            load_accounts.clear()
                            st.rerun()
                        except Exception as ex:
                            st.error(f"삭제 실패: {ex}")

# 제작자 표시는 Streamlit 자체 도구/배지와 별개로 제공합니다.
st.markdown(
    '<div class="anycorea-credit" aria-label="Developer credit">Created by anycorea</div>',
    unsafe_allow_html=True,
)
