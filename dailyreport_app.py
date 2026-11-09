"""Hismedi 진료현황 (Streamlit / Google Sheets).

Secrets: [gcp_service_account], [gsheet] spreadsheet_id.
데이터: 20YY_Q1~Q4, RAW_검진, PIN_관리(진료과/의사/PIN/권한).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo
import html
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
QUARTER_RE = re.compile(r"^20\d{2}_Q[1-4]$")
TZ = ZoneInfo("Asia/Seoul")

st.markdown("""<style>
.block-container {padding-top:.85rem; padding-bottom:2rem; max-width:100%}
[data-testid="stSidebar"] {background:#f7f9fc}
[data-testid="stSidebar"] [data-testid="stImage"] img {object-fit:contain!important;object-position:left center;max-width:100%;height:auto}
[data-testid="stSidebar"] .stButton button {width:100%}
.report-scroll {overflow-x:auto;max-width:100%;border:1px solid #d5dde7;border-radius:8px;margin-top:.15rem}
.report-table {border-collapse:collapse;width:100%;min-width:1110px;font-size:13px;text-align:center;line-height:1.4}
.report-table th {background:#e2e9f2;color:#1e334e;border:1px solid #cbd5e1;padding:9px 7px;white-space:nowrap}
.report-table td {border:1px solid #e4e8ef;padding:8px 8px;white-space:nowrap;color:#1b293b}
.report-table td:nth-child(2),.report-table td:nth-child(3){text-align:left}
.report-table tbody tr:nth-child(even):not(.total) td{background:#f8fafc}
.report-table tbody tr.total td{background:#dfeaf9!important;font-weight:750;border-top:2px solid #769dce}
.report-table tbody tr.subtotal td{background:#eef4fb!important;font-weight:700;border-top:1px solid #a9c2df}
.report-table tbody tr.exam td{background:#f2f7f4}
.report-table .num{text-align:right;font-variant-numeric:tabular-nums}
.report-note{font-size:12px;color:#64748b;margin-top:.5rem}
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
    quarter_titles = []
    for ws in book.worksheets():
        if not QUARTER_RE.fullmatch(ws.title):
            continue
        quarter_titles.append(ws.title)
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
            tuple(quarter_titles))


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
    for key in ("account_row", "login_pin", "login_name"):
        st.session_state.pop(key, None)


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


def report_data(clinical, exams, start, end, dept, doctor, master, user):
    frame = clinical[(clinical["일자"] >= start) & (clinical["일자"] <= end)].copy()
    exam = exams[(exams["일자"] >= start) & (exams["일자"] <= end)].copy()
    label = start.isoformat() if start == end else f"{start:%Y-%m-%d} ~ {end:%Y-%m-%d}"
    all_clinical = summarize_rows(frame, label, ["진료과", "의사명"])
    exam_record = None
    if not exam.empty:
        sums = exam[["남", "여", "계", "검진수입"]].sum()
        exam_record = {"일자": label, "진료과": "검진", "의사명": "검진실적",
                       **{c: 0 for c in NUMERIC}}
        exam_record.update({"남": int(sums["남"]), "여": int(sums["여"]), "계": int(sums["계"]),
                            "검진포함": int(sums["검진수입"])})

    # 병원 총합: 건강검진 원본은 제외. RAW_검진은 독립 검진실적으로 반영.
    hospital_sources = [r for r in all_clinical if r["진료과"] not in EXCLUDED_HOSPITAL_DEPTS]
    if exam_record is not None:
        hospital_sources.append(exam_record)
    hospital_total = total_row(hospital_sources, label, "병원 전체", "합계")
    hospital_total["_class"] = "total"

    if master:
        filtered = [r.copy() for r in all_clinical
                    if (dept == "전체" or r["진료과"] == dept)
                    and (doctor == "전체" or r["의사명"] == doctor)]
        shown = []
        for dept_name in sorted({r["진료과"] for r in filtered}):
            group = sorted((r for r in filtered if r["진료과"] == dept_name),
                           key=lambda r: (r["의사명"] == NO_PROVIDER, r["의사명"]))
            shown.extend(group)
            if doctor == "전체" and group:
                subtotal = total_row(group, label, dept_name, "진료과 합계")
                subtotal["_class"] = "subtotal"
                shown.append(subtotal)
        # 검진 독립행은 전체 조회 시에만 보여줍니다.
        if dept == "전체" and doctor == "전체" and exam_record is not None:
            shown.append(exam_record)
        # 화면에 표시된 소계는 다시 더하지 않으며, 선택 조건에 따른 합계만 산출.
        if dept == "전체" and doctor == "전체":
            aggregate = hospital_total
        else:
            aggregate = total_row(filtered, label, dept if dept != "전체" else "전체", "합계")
        return pd.DataFrame(shown, columns=FIELDS + ["_class"]), aggregate, []

    # 일반 계정: 본인/소속과/병원 전체 합계. 타 의사 개인 행은 절대 노출하지 않음.
    own_dept, own_name = dept_normalize(user["진료과"]), provider_normalize(user["의사"])
    own_rows = [r for r in all_clinical if r["진료과"] == own_dept and r["의사명"] == own_name]
    dept_rows = [r for r in all_clinical if r["진료과"] == own_dept]
    own = total_row(own_rows, label, own_dept, own_name)
    dept_total = total_row(dept_rows, label, own_dept, "진료과 합계")
    dept_total["_class"] = "subtotal"
    return pd.DataFrame([own, dept_total, hospital_total], columns=FIELDS + ["_class"]), None, []


def format_report(df, totals):
    head = '''<thead><tr><th rowspan="2">일자</th><th rowspan="2">진료과</th><th rowspan="2">의사명</th>
    <th colspan="4">환자수</th><th colspan="3">환자수(검진)</th><th colspan="4">진료수입</th></tr>
    <tr><th>외래</th><th>입원</th><th>퇴원</th><th>재원</th><th>남</th><th>여</th><th>계</th>
    <th>외래</th><th>입원</th><th>검진제외</th><th>검진포함</th></tr></thead>'''
    body = []
    for _, row in df.iterrows():
        kind = row.get("_class", "")
        if not kind and row["진료과"] == "검진":
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
    left, _ = st.columns([1, 2])
    with left:
        if Path("Hismedi_logo.png").is_file():
            st.image("Hismedi_logo.png", width=112)
        st.title("Hismedi 진료현황")
        st.caption("4자리 PIN을 입력한 뒤 Enter를 누르세요.")
        with st.form("login_form"):
            pin = st.text_input("PIN 코드", type="password", max_chars=4, autocomplete="off")
            login = st.form_submit_button("로그인", use_container_width=True)
        if login:
            matched = accounts[accounts["PIN"] == pin.strip()]
            if not pin_valid(pin) or len(matched) != 1:
                st.error("PIN이 올바르지 않거나 중복되어 있습니다.")
            else:
                account = matched.iloc[0]
                st.session_state["account_row"] = int(account["_row"])
                st.session_state["login_pin"] = str(account["PIN"])
                st.session_state["login_name"] = str(account["의사"])
                st.rerun()
    st.stop()

is_master = user["권한"] == "master"
try:
    clinical, exams, sheets = load_source()
except Exception as ex:
    st.error(f"실적 원본을 읽지 못했습니다. ({ex})")
    st.stop()

all_days = ([] if clinical.empty else clinical["일자"].tolist()) + ([] if exams.empty else exams["일자"].tolist())
if not all_days:
    st.warning("원본 실적이 없습니다. 2026_Q4 및 RAW_검진 시트를 확인하세요.")
    st.stop()

min_day, max_day = min(all_days), max(all_days)
yesterday = datetime.now(TZ).date() - timedelta(days=1)
initial_day = yesterday  # 기본값은 전일, 데이터가 없어도 임의로 다른 날짜로 변경하지 않음

with st.sidebar:
    if Path("Hismedi_logo.png").is_file():
        st.image("Hismedi_logo.png", width=95)
    st.markdown("### Hismedi 진료현황")
    st.caption(f"접속: {user['의사']} · {'마스터' if is_master else user['진료과']}")
    st.divider()
    st.markdown("**조회 기간**")
    start = st.date_input("시작일", value=initial_day, format="YYYY-MM-DD")
    end = st.date_input("종료일", value=initial_day, format="YYYY-MM-DD")
    if start > end:
        st.error("시작일이 종료일보다 늦습니다.")
        st.stop()
    depts = sorted(set(clinical["진료과"].dropna().tolist()) | {
        x for x in accounts["진료과"].tolist() if x and x != "전체"})
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
    if st.button("🔄 구글시트 데이터 동기화", use_container_width=True):
        load_source.clear()
        load_accounts.clear()
        st.rerun()
    st.divider()
    if st.button("로그아웃", use_container_width=True):
        clear_session()
        st.rerun()

report, totals, _ = report_data(clinical, exams, start, end, dept_choice, doctor_choice, is_master, user)
tabs = st.tabs(["📊 진료실적", "🔒 내 PIN 번호 변경", "⬇️ 엑셀다운로드"] + (["⚙️ 권한관리"] if is_master else []))

with tabs[0]:
    st.markdown(format_report(report, totals), unsafe_allow_html=True)
    if report.empty:
        st.info("해당 기간에 조회되는 실적이 없습니다.")
    st.caption(f"조회: {start:%Y-%m-%d} ~ {end:%Y-%m-%d} · 낮병동 제외 · 건강검진은 과별 표시, 병원 합계 제외 · RAW_검진 통합")
    if not is_master:
        st.caption("일반 계정에는 타 의사의 개별 실적을 표시하지 않습니다.")

with tabs[1]:
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

with tabs[2]:
    st.subheader("엑셀다운로드")
    st.caption("현재 화면에 표시된 행과 합계가 같은 2단 헤더 엑셀 파일로 저장됩니다.")
    st.download_button("⬇️ 조회 결과 엑셀 다운로드", data=to_excel(report, totals),
                       file_name=f"Hismedi_진료현황_{start:%Y%m%d}_{end:%Y%m%d}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if is_master:
    with tabs[3]:
        st.subheader("권한관리")
        st.caption("PIN_관리 시트의 진료과 / 의사 / PIN / 권한 4개 열을 사용합니다. 계정은 추가·삭제할 수 있고, 원본에 새 진료과·의사가 나타나면 조회 목록에 자동 반영됩니다.")
        st.dataframe(accounts[["진료과", "의사", "권한"]], use_container_width=True, hide_index=True)
        action = st.radio("관리 작업", ["계정 추가", "계정 수정", "계정 삭제"], horizontal=True)
        if action == "계정 추가":
            with st.form("account_create"):
                dept = st.selectbox("진료과", ["전체"] + depts)
                name = st.text_input("이름")
                pin = st.text_input("PIN (4자리)", type="password", max_chars=4)
                role = st.selectbox("권한", ["user", "master"])
                submitted = st.form_submit_button("계정 추가")
            if submitted:
                name = name.strip()
                duplicate = accounts[(accounts["진료과"] == dept) & (accounts["의사"] == name)]
                if not name or not pin_valid(pin) or (role == "user" and dept == "전체"):
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
