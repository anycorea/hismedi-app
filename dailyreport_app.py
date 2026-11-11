"""Hismedi 진료현황 (Streamlit / Google Sheets).

Secrets: [gcp_service_account], [gsheet] spreadsheet_id.
데이터: 20YY (연도별 시트만), RAW_검진, PIN_관리.
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
import hashlib
import hmac
import secrets
import tempfile
from streamlit.components.v1 import declare_component

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

# 브라우저/앱 창 제목 옆 파비콘에도 병원 로고를 적용합니다.
ICON_FILE = Path("Hismedi_logo.png")
try:
    PAGE_ICON = Image.open(ICON_FILE).copy() if ICON_FILE.exists() else "🏥"
except Exception:
    PAGE_ICON = "🏥"
st.set_page_config(page_title="Hismedi 진료현황", page_icon=PAGE_ICON, layout="wide", initial_sidebar_state="expanded")

SHEET_PIN = "PIN_관리"
FIELDS = ["일자", "진료과", "의사명", "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래수입", "입원수입", "검진제외", "검진포함"]
NUMERIC = FIELDS[3:]
CLINICAL_FIELDS = ["외래", "입원", "퇴원", "재원", "외래수입", "입원수입", "검진제외"]
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
/* Weekly tables follow the same neutral zebra striping as the month table.
   Keep a subtle highlight for summary rows without painting each week solid blue. */
.week-table tbody tr.total td {background:#eef4fb!important;border-top:1px solid #c9d7e8}
.week-table tbody tr:nth-child(even).total td {background:#f8fafc!important}
.week-table tbody tr.subtotal td {background:#eef4fb!important}
.month-table {min-width:1300px}
.month-table th,.month-table td {padding:10px 8px;font-size:14px}
.month-table th:last-child,.month-table td:last-child {background:#e9f1fc;font-weight:750}
/* Move the complete sidebar brand group upward without shifting date input widgets. */
.st-key-sidebar_brand {margin-top:-15px!important;margin-bottom:-10px!important}
/* Center the branded sidebar section, emphasize the date values without altering other widgets. */
[data-testid="stSidebar"] [data-testid="stDateInput"] input {font-size:16px!important;font-weight:750!important;color:#163b67!important;background:#fff!important}
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {font-size:16px!important;font-weight:750!important}
[data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] {font-size:15px!important}
.report-note{font-size:13px;color:#64748b;margin-top:.5rem}
/* Keep the source-data cutoff highly visible on every report tab. */
.data-cutoff-banner{display:flex;flex-wrap:wrap;align-items:center;gap:8px;
  margin:3px 0 15px 0;padding:11px 16px;border:1px solid #b9d5ec;
  border-left:5px solid #2776b6;border-radius:8px;background:#f0f7ff;color:#173c65;
  font-size:17px;font-weight:800;line-height:1.6}
.data-cutoff-detail{font-size:13px;font-weight:600;color:#52718c}
/* Strong, clearly separated monthly/yearly report controls. */
.st-key-monthly_report_filters {
  background:#edf4fc;
  border:1px solid #b9cfe8;
  border-left:5px solid #2668ad;
  border-radius:11px;
  padding:16px 20px 17px;
  margin:10px 0 18px;
}
.st-key-monthly_report_filters [data-testid="stWidgetLabel"] p {
  font-size:17px!important;
  font-weight:800!important;
  color:#173a60!important;
}
.st-key-monthly_report_filters [data-baseweb="select"] > div {
  background:#fff!important;
  border:2px solid #89afd8!important;
  border-radius:8px!important;
  min-height:49px!important;
}
.st-key-monthly_report_filters [data-baseweb="select"] span,
.st-key-monthly_report_filters [data-baseweb="select"] input {
  font-size:17px!important;
  font-weight:700!important;
  color:#173a60!important;
}
/* Footer credit is independent of Streamlit Community Cloud branding. */
.anycorea-credit {position:fixed;bottom:12px;right:16px;z-index:5;
 background:rgba(255,255,255,.94);color:#617084;font-size:12px;
 padding:5px 10px;border:1px solid #dce3ec;border-radius:7px;
 box-shadow:0 1px 4px rgba(0,0,0,.05);pointer-events:none}
/* 모든 계정의 실적표: 흰 바탕, 동일한 1px 선, 기본 글꼴과 굵기. */
.report-table.personal-view tbody tr td,
.report-table.personal-view tbody tr:nth-child(even) td,
.report-table.personal-view tbody tr.total td,
.report-table.personal-view tbody tr.subtotal td,
.report-table.personal-view tbody tr.exam td,
.report-table.personal-view.month-table tbody tr td:last-child {
    background:#fff!important;
    border:1px solid #d8dde5!important;
    font-family:inherit!important;
    font-size:14px!important;
    font-weight:400!important;
}
.report-table.personal-view tbody tr.personal td {color:#111111!important}
.report-table.personal-view tbody tr.subtotal td {color:#1357b5!important}
.report-table.personal-view tbody tr.total td {color:#c62828!important}
.report-table.personal-view tbody tr.exam td {color:#111111!important}
/* 주간 2차 메뉴: 일반 사용자 개인/진료과/병원전체, 마스터 전체/진료과 */
.st-key-weekly_scope_menu [role="radiogroup"] label {
    background:#fff!important;border:1px solid #b8cce3!important;
    padding:9px 20px!important;border-radius:8px!important;
}
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


def show_logo(path: str, width: int, *, centered: bool = False, top_offset: int = 0):
    """HTML image with its own explicit canvas. Avoid Streamlit image clipping/cropping."""
    if not Path(path).is_file():
        st.markdown("**HISMEDI HOSPITAL**")
        return
    binary = padded_logo(path)
    data = base64.b64encode(binary).decode("ascii")
    align = "center" if centered else "left"
    st.markdown(
        f'<div style="display:flex;justify-content:{align};align-items:center;'
        f'width:100%;min-height:{int(width*1.04)}px;margin:{int(top_offset)}px 0 4px 0;overflow:visible">'
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
    # 임상 원본은 연도별 시트(2026, 2027, ...)만 사용합니다.
    # 분기(Q) 시트는 절대 집계하지 않습니다.
    sources = [ws for ws in worksheets if YEAR_RE.fullmatch(ws.title)]
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


# Browser-local employee number storage. Only the 6-digit ID is persisted;
# passwords and authenticated sessions are never persisted in localStorage.
@st.cache_resource(show_spinner=False)
def get_employee_remember_component():
    directory = Path(tempfile.gettempdir()) / "hismedi_employee_remember_v1"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "index.html").write_text(r"""<!DOCTYPE html><html><head><meta charset="utf-8"></head>
<body style="margin:0"><script>
const KEY='hismedi.dailyreport.employee_id.v1';
let lastValue=undefined;
function respond(v){
  if(v===lastValue)return;
  lastValue=v;
  window.parent.postMessage({isStreamlitMessage:true,type:'streamlit:setComponentValue',value:v},'*');
}
window.addEventListener('message', (event)=>{
  if(!event.data || event.data.type!=='streamlit:render')return;
  const args=event.data.args || {};
  try {
    if(args.saveId && /^\d{6}$/.test(args.saveId))localStorage.setItem(KEY,args.saveId);
    if(args.clearId)localStorage.removeItem(KEY);
    respond(localStorage.getItem(KEY)||'');
  } catch(e) {respond('');}
  window.parent.postMessage({isStreamlitMessage:true,type:'streamlit:setFrameHeight',height:0},'*');
});
window.parent.postMessage({isStreamlitMessage:true,type:'streamlit:componentReady',apiVersion:1},'*');
</script></body></html>""", encoding="utf-8")
    return declare_component("hismedi_employee_remember_v1",path=str(directory))


def employee_password_valid(password):
    return (8 <= len(password) <= 32 and not password.isspace()
            and any(ch.isalpha() for ch in password)
            and any(ch.isdigit() for ch in password))


def password_hash(password):
    salt=secrets.token_bytes(16)
    rounds=360000
    digest=hashlib.pbkdf2_hmac('sha256',password.encode('utf-8'),salt,rounds)
    return f"pbkdf2_sha256${rounds}${salt.hex()}${digest.hex()}"


def verify_password(password,stored):
    stored=str(stored or '0000')
    if stored.startswith('pbkdf2_sha256$'):
        try:
            _,r,s,d=stored.split('$')
            digest=hashlib.pbkdf2_hmac('sha256',password.encode('utf-8'),bytes.fromhex(s),int(r))
            return hmac.compare_digest(digest,bytes.fromhex(d))
        except (ValueError,OverflowError):
            return False
    return hmac.compare_digest(password,stored)


def password_is_initial(stored):
    return str(stored or '0000') == '0000'


def update_account_fields(row_index, changes):
    ws=spreadsheet().worksheet(SHEET_PIN)
    headers=[str(x).strip() for x in ws.row_values(1)]
    missing=set(changes)-set(headers)
    if missing:
        raise ValueError('PIN_관리 시트에 필요한 열이 없습니다: '+', '.join(sorted(missing)))
    updates=[]
    for name,value in changes.items():
        col=headers.index(name)+1
        updates.append({'range':gspread.utils.rowcol_to_a1(row_index,col), 'values':[[str(value)]]})
    ws.batch_update(updates,value_input_option='RAW')
    load_accounts.clear()


@st.cache_data(ttl=20, show_spinner=False)
def load_accounts():
    rows = spreadsheet().worksheet(SHEET_PIN).get_all_values()
    if not rows:
        return pd.DataFrame(columns=["진료과", "의사", "PIN", "권한", "PW", "_row"])
    headers = [x.strip() for x in rows[0]]
    required = {"진료과", "의사", "PIN", "권한", "PW"}
    if not required.issubset(headers):
        raise ValueError("PIN_관리의 헤더에는 진료과, 의사, PIN, 권한, PW가 필요합니다.")
    accounts = []
    for row_index, row in enumerate(rows[1:], 2):
        obj = dict(zip(headers, row))
        if not str(obj.get("의사", "")).strip():
            continue
        accounts.append({"진료과": dept_normalize(obj.get("진료과", "")),
                         "의사": provider_normalize(obj.get("의사", "")),
                         "PIN": str(obj.get("PIN", "")).strip(),
                         "권한": str(obj.get("권한", "user")).strip().lower(),
                         "PW": str(obj.get("PW", "")).strip() or "0000", "_row": row_index})
    return pd.DataFrame(accounts, columns=["진료과", "의사", "PIN", "권한", "PW", "_row"])


def registered_roster(accounts):
    """조회 선택기와 개인행 표시용 명부. 실적 원본의 집계에는 적용하지 않음."""
    if accounts.empty:
        return set()
    return {(dept_normalize(row["진료과"]), provider_normalize(row["의사"]))
            for _, row in accounts.iterrows()
            if row["권한"] != "master" and row["진료과"] not in ("", "전체")
            and provider_normalize(row["의사"]) != NO_PROVIDER}


def pin_valid(pin):
    return bool(re.fullmatch(r"\d{6}", str(pin)))


def clear_session():
    # Date inputs have explicit keys. Remove them on logout/login so the new
    # session starts on yesterday, and report/filter never retain old ranges.
    for key in ("account_row", "login_pin", "login_name", "login_pw_snapshot", "report_start_day", "report_end_day", "main_nav", "test_account_row", "weekly_scope_choice", "monthly_metric_selection"):
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
    if (row["PIN"] != st.session_state.get("login_pin") or row["의사"] != st.session_state.get("login_name")
            or row["PW"] != st.session_state.get("login_pw_snapshot")):
        clear_session()
        return None
    return row


def update_pin_account(row_index, dept, name, pin, role):
    update_account_fields(row_index, {"진료과":dept,"의사":name,"PIN":pin,"권한":role})


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


def report_data(clinical, exams, start, end, dept, doctor, master, user, accounts):
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
        # 개인 행: PIN_관리에서 등록한 의사만 표시합니다.
        # 과별/병원 합계: 등록 여부에 관계없이 임상 원본 전체에서 집계합니다.
        roster = registered_roster(accounts)
        scope = [r.copy() for r in all_clinical if (dept == "전체" or r["진료과"] == dept)]
        displayed = [r.copy() for r in scope if (r["진료과"], r["의사명"]) in roster
                     and (doctor == "전체" or r["의사명"] == doctor)]
        for r in displayed:
            if r["진료과"] == "건강검진" and r["의사명"] == "최홍우":
                r["외래"] = 0
        displayed = [r for r in displayed if has_any_activity(r)]
        shown = []
        # 진료과별 소계는 표시 여부와 무관하게 원본의 모든 의사·공통실적을 포함합니다.
        group_depts = order_departments({r["진료과"] for r in scope} | {r["진료과"] for r in displayed})
        for dept_name in group_depts:
            group = sorted((r for r in displayed if r["진료과"] == dept_name), key=lambda r: r["의사명"])
            shown.extend(group)
            if doctor == "전체":
                raw_group = [r for r in scope if r["진료과"] == dept_name]
                # 건강검진 임상환자수(외래)는 별도 검진자료로 관리합니다.
                if dept_name == "건강검진":
                    raw_group = [{**r, "외래": 0} for r in raw_group]
                subtotal = total_row(raw_group, label, dept_name, "진료과 합계")
                if has_any_activity(subtotal):
                    subtotal["_class"] = "subtotal"
                    shown.append(subtotal)
        if dept in ("전체", "건강검진") and doctor == "전체" and exam_record is not None:
            exam_record["_class"] = "exam"
            shown.append(exam_record)
        if dept == "전체" and doctor == "전체":
            aggregate = hospital_total
        else:
            raw_scope = [r for r in scope if doctor == "전체" or r["의사명"] == doctor]
            if dept == "건강검진":
                raw_scope = [{**r, "외래": 0} for r in raw_scope]
            extras = ([exam_record] if dept == "건강검진" and doctor == "전체" and exam_record is not None else [])
            aggregate = total_row(raw_scope + extras, label, dept if dept != "전체" else "전체", "합계")
        return pd.DataFrame(shown, columns=FIELDS + ["_class"]), aggregate, []

    # 일반 계정: 본인/소속과/병원 전체 합계. 타 의사 개인 행은 절대 노출하지 않음.
    own_dept, own_name = dept_normalize(user["진료과"]), provider_normalize(user["의사"])
    own_rows = [r for r in all_clinical if r["진료과"] == own_dept and r["의사명"] == own_name]
    dept_rows = [r for r in all_clinical if r["진료과"] == own_dept]
    own = total_row(own_rows, label, own_dept, own_name)
    own["_class"] = "personal"
    dept_total = total_row(dept_rows, label, own_dept, "진료과 합계")
    dept_total["_class"] = "subtotal"
    # 로그인한 의사에게도 병원 전체 건강검진 원본의 집계치를 독립행으로 제공합니다.
    # 건강검진 과 소속 의사는 최홍우의 임상 외래 환자수를 노출하지 않습니다.
    if own_dept == "건강검진" and own_name == "최홍우":
        own["외래"] = 0
    visible = ([own] if has_any_activity(own) else [])
    if has_any_activity(dept_total):
        visible.append(dept_total)
    # 개인 계정은 진료과 선택이 없습니다. 내과 소속 계정에만
    # RAW_검진의 별도 건강검진 행을 표시합니다.
    if own_dept == "내과" and exam_record is not None:
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

# 월간/연간에서는 마스터와 개인 계정 모두 7개 지표를 조회할 수 있습니다.
# 개인별 행에는 본인 실적만, 건강검진과 병원 전체는 별도 행으로 집계됩니다.
DOCTOR_MONTH_METRICS = list(MONTH_METRICS)


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
    """데이터가 존재하는 주만 최근 주부터: master는 조건별 합계, 일반 의사는 본인 실적만."""
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
            # 일반 의사 계정의 주간 탭은 본인 실적만 표시합니다.
            # 진료과·병원 전체 합계 및 RAW_검진 별도 행은 주간 탭에서 제외합니다.
            values = period_total(clinical, exams, current, week_end, own_dept, own_name)
            if has_any_activity(values):
                rows.append({"일자": label, "진료과": own_dept, "의사명": own_name,
                             **values, "_class": ""})
        current -= timedelta(days=7)
    return pd.DataFrame(rows, columns=FIELDS + ["_class"])


def weekly_personal_scope(clinical, exams, user, today, scope):
    """일반 사용자 주간 3단계 조회: 개인 / 진료과 합계 / 병원 전체 합계."""
    if scope not in {"개인", "진료과", "병원전체"}:
        raise ValueError("허용되지 않은 주간 조회 범위")
    dates = list(clinical["일자"]) + list(exams["일자"])
    if not dates:
        return pd.DataFrame(columns=FIELDS + ["_class"])
    own_dept = dept_normalize(user["진료과"])
    own_name = provider_normalize(user["의사"])
    first = min(dates)
    cursor = max(max(dates), today)
    current = cursor - timedelta(days=cursor.weekday())
    first_monday = first - timedelta(days=first.weekday())
    results = []
    while current >= first_monday:
        end_week = current + timedelta(days=6)
        if scope == "개인":
            dept, doctor, row_dept, row_name, kind = own_dept, own_name, own_dept, own_name, "personal"
        elif scope == "진료과":
            dept, doctor, row_dept, row_name, kind = own_dept, "전체", own_dept, "진료과 합계", "subtotal"
        else:
            dept, doctor, row_dept, row_name, kind = "전체", "전체", "병원 전체", "합계", "total"
        values = period_total(clinical, exams, current, end_week, dept, doctor)
        if has_any_activity(values):
            results.append({"일자": f"{current:%Y-%m-%d} ~ {end_week:%Y-%m-%d}",
                            "진료과": row_dept, "의사명": row_name,
                            **values, "_class": kind})
        current -= timedelta(days=7)
    return pd.DataFrame(results, columns=FIELDS + ["_class"])


def monthly_table(clinical, exams, year, metric, dept, doctor, master, user, accounts):
    """12개월 고정 컬럼, 실적별 행 및 연간 합계; 과거 데이터도 연도 시트에서 읽음."""
    if metric not in MONTH_METRICS:
        raise ValueError("지원하지 않는 월간/연간 실적 지표입니다.")
    field = MONTH_METRICS[metric]
    year_mask = pd.Series([d.year == year for d in clinical["일자"]], index=clinical.index, dtype=bool)
    exam_mask = pd.Series([d.year == year for d in exams["일자"]], index=exams.index, dtype=bool)
    year_data = clinical.loc[year_mask]
    years_exam = exams.loc[exam_mask]
    entries = []
    if master:
        roster = registered_roster(accounts)
        clinical_keys = year_data[["진료과", "의사명"]].drop_duplicates().itertuples(index=False, name=None)
        for dep, name in clinical_keys:
            if (dep, name) not in roster:
                continue
            if dept != "전체" and dep != dept:
                continue
            if doctor != "전체" and name != doctor:
                continue
            entries.append((dep, name, dep, name, ""))
        if dept in ("전체", "건강검진") and doctor == "전체":
            entries.append(("건강검진", "검진실적", "건강검진_RAW", "전체", "exam"))
    else:
        own_dept = dept_normalize(user["진료과"])
        own_name = provider_normalize(user["의사"])
        entries = [
            (own_dept, own_name, own_dept, own_name, "personal"),
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
    # 마스터의 진료과 소계는 PIN 등록 여부와 관계없이 원본 전체에서 집계합니다.
    if master and doctor == "전체":
        source_depts = order_departments(set(year_data["진료과"].dropna().tolist()))
        if dept != "전체":
            source_depts = [d for d in source_depts if d == dept]
        for d in source_depts:
            subtotal = {"진료과": d, "의사명": "진료과 합계", "_class": "subtotal"}
            for month in range(1, 13):
                first = date(year, month, 1)
                last = (date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)) - timedelta(days=1)
                subtotal[f"{month}월"] = period_total(clinical, exams, first, last, d, "전체").get(field, 0)
            subtotal["누계"] = sum(subtotal[f"{m}월"] for m in range(1, 13))
            if subtotal["누계"]:
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


def format_monthly(df, table_class=""):
    cols = ["진료과", "의사명"] + [f"{m}월" for m in range(1, 13)] + ["누계"]
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    rows = []
    for _, row in df.iterrows():
        cls = str(row.get("_class", ""))
        cells = "".join(f"<td>{html.escape(str(row[c]))}</td>" if c in ("진료과", "의사명")
                        else f'<td class="num">{int(row[c]):,}</td>' for c in cols)
        rows.append(f'<tr class="{cls}">{cells}</tr>')
    return f'<div class="report-scroll"><table class="report-table month-table {html.escape(table_class)}"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>'


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


def format_report(df, totals, table_class="", hide_exam_inclusive=False):
    """기본 실적표 HTML. 주간 표는 월간 표와 동일한 중립 배경 적용."""
    head = '''<thead><tr><th rowspan="2">일자</th><th rowspan="2">진료과</th><th rowspan="2">의사명</th>
    <th colspan="4">환자수</th><th colspan="3">환자수(검진)</th><th colspan="4">진료수입</th></tr>
    <tr><th>외래</th><th>입원</th><th>퇴원</th><th>재원</th><th>남</th><th>여</th><th>계</th>
    <th>외래</th><th>입원</th><th>검진제외</th><th>검진포함</th></tr></thead>'''
    # 일반 의사 주간 화면에서 검진포함 수입 열 자체를 표시하지 않습니다.
    visible_numeric = [col for col in NUMERIC if not (hide_exam_inclusive and col == "검진포함")]
    if hide_exam_inclusive:
        head = head.replace('<th colspan="4">진료수입</th>', '<th colspan="3">진료수입</th>')
        head = head.replace('<th>검진제외</th><th>검진포함</th>', '<th>검진제외</th>')
    body = []
    for _, row in df.iterrows():
        kind = row.get("_class", "")
        if not kind and row["의사명"] == "검진실적":
            kind = "exam"
        cells = "".join(f"<td>{html.escape(str(row[c]))}</td>" for c in FIELDS[:3])
        cells += "".join(f'<td class="num">{int(row[c]):,}</td>' for c in visible_numeric)
        body.append(f'<tr class="{kind}">{cells}</tr>')
    if totals is not None:
        cells = "".join(f'<td class="num">{int(totals[c]):,}</td>' for c in visible_numeric)
        body.append(f'<tr class="total"><td>합계</td><td>전체</td><td>합계</td>{cells}</tr>')
    return f'<div class="report-scroll"><table class="report-table {html.escape(table_class)}">{head}<tbody>{"".join(body)}</tbody></table></div>'


def to_excel(df, totals, hide_exam_inclusive=False):
    # 출력 파일은 선택된 화면 내용과 동일; 단순 데이터 저장이 아닌 2단 헤더 양식
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    book = Workbook()
    ws = book.active
    ws.title = "진료현황"
    visible_fields = [c for c in FIELDS if not (hide_exam_inclusive and c == "검진포함")]
    ws.append(["일자", "진료과", "의사명", "환자수", None, None, None, "환자수(검진)", None, None,
               "진료수입", None, None] + ([] if hide_exam_inclusive else [None]))
    ws.append([None, None, None, "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래", "입원", "검진제외"]
              + ([] if hide_exam_inclusive else ["검진포함"]))
    merges = ["A1:A2", "B1:B2", "C1:C2", "D1:G1", "H1:J1", "K1:M1" if hide_exam_inclusive else "K1:N1"]
    for address in merges:
        ws.merge_cells(address)
    for _, row in df.iterrows():
        ws.append([row[c] for c in visible_fields])
    if totals is not None:
        ws.append(["합계", "전체", "합계"] + [totals[c] for c in visible_fields[3:]])
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
    for col in range(1, len(visible_fields) + 1):
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

# Persist only employee ID in this device's browser profile. The browser
# component supplies this asynchronously and never receives the password.
remember_component=get_employee_remember_component()
remembered_id=remember_component(
    saveId=st.session_state.get('save_employee_id',''),
    clearId=st.session_state.pop('clear_employee_id',False),
    key='hismedi_employee_id_store',default='')
if remembered_id and not st.session_state.get('employee_login_id'):
    st.session_state['employee_login_id']=remembered_id

user = logged_in_account(accounts)
if user is None:
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
            st.caption("6자리 사번과 비밀번호를 입력한 뒤 Enter를 누르세요.")
        st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
        with st.form("login_form"):
            pin=st.text_input("사번 (6자리)",max_chars=6,key='employee_login_id',autocomplete='username')
            password=st.text_input("비밀번호",type="password",autocomplete='off')
            login=st.form_submit_button("로그인",use_container_width=True)
        if login:
            matched=accounts[accounts['PIN']==pin.strip()]
            locked_until=st.session_state.get('locked_until')
            if locked_until and datetime.now(TZ)<locked_until:
                st.error('로그인 시도 횟수가 초과됐습니다. 잠시 뒤 다시 시도하세요.')
            elif (not pin_valid(pin) or len(matched)!=1 or
                  not verify_password(password, matched.iloc[0]['PW'] if len(matched)==1 else '')):
                tries=st.session_state.get('login_failures',0)+1
                st.session_state['login_failures']=tries
                if tries>=5:
                    st.session_state['locked_until']=datetime.now(TZ)+timedelta(minutes=5)
                    st.session_state['login_failures']=0
                st.error('사번 또는 비밀번호가 올바르지 않습니다.')
            else:
                account=matched.iloc[0]
                clear_session()
                reset_report_dates()
                st.session_state['account_row']=int(account['_row'])
                st.session_state['login_pin']=str(account['PIN'])
                st.session_state['login_name']=str(account['의사'])
                st.session_state['login_pw_snapshot']=str(account['PW'])
                st.session_state['save_employee_id']=str(account['PIN'])
                st.session_state['login_failures']=0
                st.session_state.pop('locked_until',None)
                st.rerun()
        if st.button('이 PC에 기억된 사번 지우기',key='forget_employee_id'):
            st.session_state.pop('save_employee_id',None)
            st.session_state.pop('employee_login_id',None)
            st.session_state['clear_employee_id']=True
            st.rerun()
    st.markdown('<div class="anycorea-credit">Created by anycorea</div>',unsafe_allow_html=True)
    st.stop()

if password_is_initial(user['PW']):
    st.warning('초기 비밀번호(0000)입니다. 실적을 조회하기 전에 개인 비밀번호를 설정해 주세요.')
    with st.form('first_password_change'):
        new_pw=st.text_input('새 비밀번호 (8~32자, 영문·숫자 포함)',type='password')
        confirm_pw=st.text_input('새 비밀번호 확인',type='password')
        submit=st.form_submit_button('비밀번호 설정')
    if submit:
        if not employee_password_valid(new_pw) or new_pw != confirm_pw:
            st.error('비밀번호는 영문과 숫자를 포함한 8~32자로 설정하고, 확인 값과 일치해야 합니다.')
        else:
            try:
                update_account_fields(int(user['_row']),{'PW':password_hash(new_pw)})
                clear_session()
                st.success('비밀번호를 설정했습니다. 새 비밀번호로 다시 로그인하세요.')
                st.rerun()
            except Exception as ex:
                st.error(f'변경 실패: {ex}')
    st.stop()

# The authenticated principal is always the real account. A master can preview
# another user's read-only report, without acquiring their credentials.
real_user = user
real_is_master = real_user["권한"] == "master"
preview_row = st.session_state.get("test_account_row") if real_is_master else None
preview_target = None
if preview_row is not None:
    candidates = accounts[(accounts["_row"] == preview_row) & (accounts["권한"] == "user")]
    if len(candidates) == 1:
        preview_target = candidates.iloc[0].to_dict()
    else:
        # An account may have been deleted or had its role changed in Sheets.
        st.session_state.pop("test_account_row", None)
preview_active = preview_target is not None
if preview_active:
    user = preview_target
is_master = real_is_master and not preview_active
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
    if preview_active:
        st.warning(f"🔎 관리자 테스트 모드 · {user['진료과']} / {user['의사']}")
        if st.button("↩️ 테스트 종료 · 마스터로 돌아가기", use_container_width=True, key="preview_exit"):
            st.session_state.pop("test_account_row", None)
            st.session_state.pop("main_nav", None)
            reset_report_dates()
            st.rerun()
    with st.container(key="sidebar_brand"):
        show_logo("Hismedi_logo.png", 98, centered=True, top_offset=-24)
        st.markdown('<div style="text-align:center;font-size:19px;font-weight:800;'
                    'margin-top:2px">Hismedi 진료현황</div>', unsafe_allow_html=True)
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
    roster = registered_roster(accounts)
    depts = order_departments({d for d, _ in roster})
    if is_master:
        dept_choice = st.selectbox("진료과", ["전체"] + depts)
        names = sorted({n for d, n in roster if dept_choice == "전체" or d == dept_choice})
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
    if not preview_active and st.button("🔄 데이터 동기화", use_container_width=True):
        load_source.clear()
        load_accounts.clear()
        st.rerun()
    st.divider()
    if st.button("로그아웃", use_container_width=True):
        clear_session()
        st.rerun()

report, totals, _ = report_data(clinical, exams, start, end, dept_choice, doctor_choice, is_master, user, accounts)
st.markdown('<div style="height:48px"></div>', unsafe_allow_html=True)
menu_items = ["📊 진료실적(기간별)", "📅 진료실적(주간)", "📆 진료실적(월간/연간)"]
if not preview_active:
    menu_items += ["🔒 내 비밀번호 변경", "⬇️ 엑셀다운로드"]
if is_master:
    menu_items.append("⚙️ 권한관리")
# A previous tab selection (e.g. account admin) must never remain active
# after switching into the restricted preview.
if st.session_state.get("main_nav") not in menu_items:
    st.session_state["main_nav"] = menu_items[0]
active_menu = st.radio("메뉴", menu_items, horizontal=True, label_visibility="collapsed", key="main_nav")
st.markdown('<div style="height:12px"></div>', unsafe_allow_html=True)
if preview_active:
    st.warning(f"🔎 **관리자 테스트 모드 — {user['진료과']} / {user['의사']}** · 실제 로그인은 마스터 계정입니다. \n"
               "이 화면은 읽기 전용이며 비밀번호 변경·계정 수정·엑셀 다운로드가 제한됩니다.")

# 마지막 반영일은 왼쪽 조회 종료일이 아니라, 구글시트 원본에 존재하는
# 가장 최근 실적 일자로 계산합니다. 임상/검진은 갱신일이 다를 수 있습니다.
clinical_cutoff = max(clinical["일자"]) if not clinical.empty else None
exam_cutoff = max(exams["일자"]) if not exams.empty else None
primary_cutoff = clinical_cutoff or exam_cutoff
if primary_cutoff is not None:
    # 연도별 데이터는 해당 첫 연도의 1월 1일부터 최신 원본 일자까지 안내합니다.
    # 실제 조회 일자·현재 날짜가 아닌 원본 기록만을 기준으로 계산합니다.
    data_start_year = min(all_days).year
    coverage_start = date(data_start_year, 1, 1)
    cutoff_text = (f"📢 {coverage_start:%Y년 %m월 %d일} ~ "
                   f"{primary_cutoff:%Y년 %m월 %d일}까지 반영되었습니다.")
    details = []
    if clinical_cutoff is not None and exam_cutoff is not None and clinical_cutoff != exam_cutoff:
        details.append(f"건강검진 자료: {exam_cutoff:%Y년 %m월 %d일}까지")
    elif clinical_cutoff is None and exam_cutoff is not None:
        details.append("임상실적 원본 없음 · 건강검진 자료 기준")
    detail_html = (f'<span class="data-cutoff-detail">({html.escape(" · ".join(details))})</span>'
                   if details else "")
    st.markdown(f'<div class="data-cutoff-banner">{html.escape(cutoff_text)}{detail_html}</div>',
                unsafe_allow_html=True)

# 전역 기간 입력과 독립적인 주간·월간 자료
if active_menu == "📊 진료실적(기간별)":
    st.markdown(format_report(report, totals, "personal-view"), unsafe_allow_html=True)
    if report.empty:
        st.info("해당 기간에 조회되는 실적이 없습니다.")
    st.caption(f"조회: {start:%Y-%m-%d} ~ {end:%Y-%m-%d} · 낮병동 제외 · 임상원본 건강검진 병원 합계 제외 · RAW_검진 통합")
    if not is_master:
        st.caption("일반 계정에는 타 의사의 개별 실적을 표시하지 않습니다.")

if active_menu == "📅 진료실적(주간)":
    st.caption("월요일~일요일 · 최신 주간부터 표시 · 왼쪽 조회 기간과 무관 · 아래 주간 조회 범위 선택 적용")
    if is_master:
        with st.container(key="weekly_scope_menu"):
            master_weekly_scope = st.radio(
                "주간 조회 범위", ["전체", "진료과"], horizontal=True,
                key="master_weekly_scope_choice",
            )
            if master_weekly_scope == "진료과":
                # 계정관리 목록을 기본으로 하되 건강검진(RAW_검진)은 항상 선택 가능.
                weekly_depts = order_departments({d for d, _ in registered_roster(accounts)} | {"건강검진"})
                weekly_dept = st.selectbox("진료과 선택", weekly_depts, key="master_weekly_dept_choice")
            else:
                weekly_dept = "전체"
        weekly = weekly_table(clinical, exams, weekly_dept, "전체", True, user, datetime.now(TZ).date())
        if not weekly.empty and weekly_dept != "전체":
            weekly = weekly.copy()
            weekly["의사명"] = "진료과 합계"
            weekly["_class"] = "subtotal"
        st.markdown(format_report(weekly, None, "week-table personal-view"), unsafe_allow_html=True)
    else:
        with st.container(key="weekly_scope_menu"):
            weekly_scope = st.radio("주간 조회 범위", ["개인", "진료과", "병원전체"],
                                    horizontal=True, key="weekly_scope_choice")
        weekly = weekly_personal_scope(clinical, exams, user, datetime.now(TZ).date(), weekly_scope)
        st.markdown(format_report(weekly, None, "week-table personal-view", hide_exam_inclusive=True), unsafe_allow_html=True)
    if weekly.empty:
        st.info("해당 조건에 집계된 주간 실적이 없습니다.")

if active_menu == "📆 진료실적(월간/연간)":
    available_years = sorted(set([d.year for d in all_days] + [datetime.now(TZ).year]), reverse=True)
    with st.container(key="monthly_report_filters"):
        st.markdown("**📅 월간·연간 실적 조회 조건**")
        year_col, metric_col = st.columns([1, 2.5], gap="medium")
        with year_col:
            selected_year = st.selectbox("📆 조회 연도", available_years, index=available_years.index(datetime.now(TZ).year))
        with metric_col:
            selected_metric = st.selectbox(
                "📊 실적 지표 선택",
                list(MONTH_METRICS) if is_master else DOCTOR_MONTH_METRICS,
                key="monthly_metric_selection",
            )
    monthly = monthly_table(clinical, exams, selected_year, selected_metric, dept_choice, doctor_choice, is_master, user, accounts)
    st.markdown(format_monthly(monthly, "personal-view"), unsafe_allow_html=True)
    if monthly.empty:
        st.info("해당 연도의 실적이 없습니다.")
    if selected_metric == "재원환자수":
        st.caption("재원환자수는 원본에 기록된 일자별 재원 수를 합산한 값입니다(월말 재원 또는 고유 환자 수가 아닙니다).")
    st.caption("1월~12월 고정 · 누계 자동 합산 · 왼쪽 조회 기간과 무관 · 진료과/의사 필터 적용")

if not preview_active and active_menu == "🔒 내 비밀번호 변경":
    st.subheader("내 비밀번호 변경")
    with st.form("change_own_password"):
        current=st.text_input("현재 비밀번호",type="password")
        new=st.text_input("새 비밀번호 (8~32자, 영문·숫자 포함)",type="password")
        verify=st.text_input("새 비밀번호 확인",type="password")
        submit=st.form_submit_button("비밀번호 변경")
    if submit:
        if not verify_password(current,user['PW']):
            st.error("현재 비밀번호가 일치하지 않습니다.")
        elif not employee_password_valid(new) or new != verify:
            st.error("새 비밀번호는 영문과 숫자를 포함한 8~32자이며 확인 값과 일치해야 합니다.")
        else:
            try:
                update_account_fields(int(user['_row']),{'PW':password_hash(new)})
                clear_session()
                st.success("비밀번호가 변경되었습니다. 새 비밀번호로 다시 로그인하세요.")
                st.rerun()
            except Exception as ex:
                st.error(f"비밀번호 변경 실패: {ex}")

if not preview_active and active_menu == "⬇️ 엑셀다운로드":
    st.subheader("엑셀다운로드")
    st.caption("기간별/주간 실적은 기존 2단 헤더, 월간/연간 실적은 1~12월/누계 형태로 저장합니다.")
    kind = st.radio("다운로드 종류", ["기간별", "주간", "월간/연간"], horizontal=True)
    if kind == "기간별":
        payload = to_excel(report, totals)
        filename = f"Hismedi_기간별_{start:%Y%m%d}_{end:%Y%m%d}.xlsx"
    elif kind == "주간":
        if is_master:
            export_scope = st.radio("주간 조회 범위", ["전체", "진료과"], horizontal=True, key="master_weekly_export_scope")
            if export_scope == "진료과":
                export_depts = order_departments({d for d, _ in registered_roster(accounts)} | {"건강검진"})
                export_dept = st.selectbox("진료과 선택", export_depts, key="master_weekly_export_dept")
            else:
                export_dept = "전체"
            weekly = weekly_table(clinical, exams, export_dept, "전체", True, user, datetime.now(TZ).date())
            if not weekly.empty and export_dept != "전체":
                weekly = weekly.copy()
                weekly["의사명"] = "진료과 합계"
                weekly["_class"] = "subtotal"
        else:
            export_scope = st.selectbox("주간 조회 범위", ["개인", "진료과", "병원전체"], key="weekly_export_scope")
            weekly = weekly_personal_scope(clinical, exams, user, datetime.now(TZ).date(), export_scope)
        payload = to_excel(weekly, None, hide_exam_inclusive=not is_master)
        filename = "Hismedi_주간실적.xlsx"
    else:
        ycol, mcol = st.columns([1, 2])
        years = sorted(set([d.year for d in all_days] + [datetime.now(TZ).year]), reverse=True)
        with ycol:
            export_year = st.selectbox("연도", years)
        with mcol:
            export_metric = st.selectbox(
                "지표", list(MONTH_METRICS) if is_master else DOCTOR_MONTH_METRICS,
                key="monthly_export_metric_selection",
            )
        monthly = monthly_table(clinical, exams, export_year, export_metric, dept_choice, doctor_choice, is_master, user, accounts)
        payload = monthly_excel(monthly, export_metric, export_year)
        filename = f"Hismedi_월간연간_{export_year}_{export_metric}.xlsx"
    st.download_button("⬇️ 조회 결과 엑셀 다운로드", data=payload,
                       file_name=filename, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if real_is_master and not preview_active and active_menu == "⚙️ 권한관리":
        st.subheader("권한관리")
        st.caption("PIN_관리의 진료과 / 의사 / PIN(사번) / 권한 / PW 열을 사용합니다. PW는 변경 후 암호화 검증값으로 보관합니다. 미등록 의사의 실적도 과별/병원 합계에는 포함됩니다.")
        st.dataframe(accounts[["진료과", "의사", "권한"]], use_container_width=True, hide_index=True)
        st.divider()
        st.markdown("#### 🔎 사용자 화면 테스트 (마스터 전용)")
        st.caption("직원 비밀번호를 입력하거나 변경하지 않고, 해당 직원의 실적 화면과 조회 권한을 확인합니다. 테스트 중에는 모든 변경·다운로드 기능이 비활성화됩니다.")
        test_accounts = accounts[accounts["권한"] == "user"].copy()
        if test_accounts.empty:
            st.info("테스트할 일반 사용자 계정이 없습니다.")
        else:
            target_options = {
                int(row["_row"]): f"{row['진료과']} · {row['의사']} (사번 {row['PIN']})"
                for _, row in test_accounts.iterrows()
            }
            test_col, start_col = st.columns([3, 1], vertical_alignment="bottom")
            with test_col:
                selected_test_row = st.selectbox(
                    "테스트할 직원", list(target_options),
                    format_func=lambda key: target_options[key], key="preview_target_selection")
            with start_col:
                if st.button("사용자 화면 테스트", type="primary", use_container_width=True):
                    st.session_state["test_account_row"] = int(selected_test_row)
                    st.session_state.pop("main_nav", None)
                    reset_report_dates()
                    st.rerun()
        st.divider()
        action = st.radio("관리 작업", ["계정 추가", "계정 수정", "계정 삭제"], horizontal=True)
        if action == "계정 추가":
            with st.form("account_create"):
                dept = st.selectbox("진료과", ["전체"] + depts + ["➕ 새 진료과 추가"])
                new_dept = st.text_input("새 진료과 이름 (새 진료과 추가 선택 시에만 입력)")
                name = st.text_input("이름")
                pin = st.text_input("사번 (6자리)", max_chars=6)
                role = st.selectbox("권한", ["user", "master"])
                submitted = st.form_submit_button("계정 추가")
            if submitted:
                name = name.strip()
                if dept == "➕ 새 진료과 추가":
                    dept = dept_normalize(new_dept)
                duplicate = accounts[(accounts["진료과"] == dept) & (accounts["의사"] == name)]
                if not dept or not name or not pin_valid(pin) or (role == "user" and dept == "전체"):
                    st.error("진료과와 이름, 6자리 PIN을 확인하세요. 일반 사용자는 소속 진료과를 지정해야 합니다.")
                elif pin in accounts["PIN"].tolist() or not duplicate.empty:
                    st.error("중복된 PIN 또는 계정입니다.")
                else:
                    try:
                        ws=spreadsheet().worksheet(SHEET_PIN)
                        headers=[str(x).strip() for x in ws.row_values(1)]
                        row=['']*len(headers)
                        for key,val in {'진료과':dept,'의사':name,'PIN':pin,'권한':role,'PW':'0000'}.items():
                            row[headers.index(key)]=val
                        ws.append_row(row,value_input_option="RAW")
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
                    pin = st.text_input("새 사번 (변경하지 않으려면 빈칸)", max_chars=6)
                    reset_pw=st.checkbox("비밀번호를 초기값 0000으로 재설정 (다음 로그인 시 변경 필수)")
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
                            if reset_pw:
                                update_account_fields(selected,{'PW':'0000'})
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
