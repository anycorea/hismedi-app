"""HISMEDI 진료현황 — Google Sheets / Streamlit.

필수 Streamlit secrets:
[gcp_service_account] : Google service-account JSON fields
[gsheet]
spreadsheet_id = "..."
[app]
TITLE = "Hismedi 진료현황"
TZ = "Asia/Seoul"

서비스 계정에 원본 Google Sheet 편집자 권한을 부여해야 PIN 변경/관리 가능.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path
import html
import re

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

st.set_page_config(page_title="Hismedi 진료현황", page_icon="🏥", layout="wide", initial_sidebar_state="expanded")

SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
SHEET_PIN = "PIN_관리"
COLUMNS = ["일자", "진료과", "의사명", "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래수입", "입원수입", "검진제외", "검진포함"]
NUMERIC = COLUMNS[3:]

st.markdown("""<style>
.block-container {padding-top:1.0rem;max-width:100%}
[data-testid="stSidebar"] {background:#f7f9fc}
[data-testid="stSidebar"] .stButton button {width:100%}
.report-wrap {overflow-x:auto;border:1px solid #ccd4dd;border-radius:5px;margin-top:12px}
.report-table {border-collapse:collapse;width:100%;min-width:1120px;font-size:13px;text-align:center}
.report-table th {background:#e0e6ee;border:1px solid #c7cdd4;padding:9px 6px;white-space:nowrap;color:#152238}
.report-table td {border:1px solid #e0e4e8;padding:7px 8px;white-space:nowrap;color:#18212e}
.report-table td:nth-child(2), .report-table td:nth-child(3) {text-align:left}
.report-table tr:nth-child(even) td {background:#f8fafc}
.report-table tr.total td {background:#e7eef9;font-weight:750;border-top:2px solid #8aa6cf}
.report-table tr.exam td {background:#f1f7f3}
.report-table .num {text-align:right;font-variant-numeric:tabular-nums}
.small-help {font-size:12px;color:#667085}
</style>""", unsafe_allow_html=True)


@st.cache_resource
def client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
    return gspread.authorize(credentials)


def doc():
    return client().open_by_key(SPREADSHEET_ID)


@st.cache_data(ttl=120, show_spinner=False)
def fetch_data():
    workbook = doc()
    clinical = []
    for ws in workbook.worksheets():
        if re.fullmatch(r"20\d{2}_Q[1-4]", ws.title):
            rows = ws.get_all_values()
            for r in rows[2:]:
                if len(r) < 11:
                    continue
                day = parse_day(r[0])
                deptdoctor = str(r[1]).strip()
                if not day or not deptdoctor or any(w in deptdoctor for w in ("합계", "총계", "소계")):
                    continue
                matched = re.match(r"^\s*(.*?)\s*\(([^()]+)\)\s*$", deptdoctor)
                if not matched:
                    continue
                department, doctor = (x.strip() for x in matched.groups())
                # 예: 내과 치료(윤한결) -> 내과; 세부 원본 문자열은 표시용으로만 사용
                department = re.sub(r"\s+(치료|진료)$", "", department).strip()
                clinical.append({"일자": day, "진료과": department, "의사명": doctor,
                                 "외래": number(r[2]), "입원": number(r[4]), "퇴원": number(r[5]), "재원": number(r[6]),
                                 "외래수입": number(r[7]), "입원수입": number(r[9]),
                                 "검진제외": number(r[7]) + number(r[9])})
    clinical_df = pd.DataFrame(clinical)
    exam_rows = []
    try:
        for r in workbook.worksheet("RAW_검진").get_all_values()[1:]:
            if len(r) < 6:
                continue
            day = parse_day(r[1]) or parse_day(r[0])
            if day:
                exam_rows.append({"일자": day, "남": number(r[2]), "여": number(r[3]), "계": number(r[4]), "검진수입": number(r[5])})
    except WorksheetNotFound:
        pass
    exam_df = pd.DataFrame(exam_rows)
    return clinical_df, exam_df


@st.cache_data(ttl=20, show_spinner=False)
def fetch_accounts():
    rows = doc().worksheet(SHEET_PIN).get_all_values()
    if not rows:
        return pd.DataFrame(columns=["진료과", "의사", "PIN", "권한", "_row"])
    headings = [h.strip() for h in rows[0]]
    accounts = []
    for i, row in enumerate(rows[1:], start=2):
        vals = dict(zip(headings, row))
        if not str(vals.get("의사", "")).strip():
            continue
        accounts.append({"진료과": str(vals.get("진료과", "")).strip(),
                         "의사": str(vals.get("의사", "")).strip(),
                         "PIN": str(vals.get("PIN", "")).strip(),
                         "권한": str(vals.get("권한", "user")).strip().lower(), "_row": i})
    return pd.DataFrame(accounts)


def number(value):
    try:
        return int(round(float(str(value).replace(",", "").replace("₩", "").strip())))
    except (TypeError, ValueError):
        return 0


def parse_day(value):
    if isinstance(value, (date, datetime)):
        return value.date() if isinstance(value, datetime) else value
    s = str(value).strip()
    if not s or s.startswith("#"):
        return None
    s = re.sub(r"\.0$", "", s)
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def pin_valid(pin):
    return bool(re.fullmatch(r"\d{4}", str(pin)))


def refresh():
    fetch_data.clear()
    fetch_accounts.clear()


def authenticated_user(accounts):
    identity = st.session_state.get("account_row")
    if identity is None:
        return None
    match = accounts[accounts["_row"] == identity]
    if match.empty:
        st.session_state.pop("account_row", None)
        return None
    row = match.iloc[0].to_dict()
    if row["PIN"] != st.session_state.get("login_pin") or row["의사"] != st.session_state.get("login_name"):
        st.session_state.pop("account_row", None)
        return None
    return row


def add_account(dept, name, pin, permission):
    ws = doc().worksheet(SHEET_PIN)
    ws.append_row([dept, name, pin, permission], value_input_option="RAW")
    fetch_accounts.clear()


def change_account(row_number, dept, name, pin, permission):
    doc().worksheet(SHEET_PIN).update(range_name=f"A{row_number}:D{row_number}", values=[[dept, name, pin, permission]], value_input_option="RAW")
    fetch_accounts.clear()


def remove_account(row_number):
    doc().worksheet(SHEET_PIN).delete_rows(int(row_number))
    fetch_accounts.clear()


def row_record(day, dept, doctor, values):
    return {"일자": day, "진료과": dept, "의사명": doctor, **{c: int(values.get(c, 0)) for c in NUMERIC}}


def summary(clinical, exam, mode, start, end, dept_choice, doctor_choice):
    if not clinical.empty:
        clinical = clinical[(clinical["일자"] >= start) & (clinical["일자"] <= end)].copy()
    if not exam.empty:
        exam = exam[(exam["일자"] >= start) & (exam["일자"] <= end)].copy()

    if mode == "개인":
        clinical = clinical[(clinical["진료과"] == dept_choice) & (clinical["의사명"] == doctor_choice)]
        exam = exam.iloc[0:0]
    elif mode == "진료과":
        clinical = clinical[clinical["진료과"] == dept_choice]
        exam = exam.iloc[0:0]
    elif mode == "의사":
        clinical = clinical[clinical["의사명"] == doctor_choice]
        exam = exam.iloc[0:0]
    elif mode == "전체" and dept_choice != "전체":
        clinical = clinical[clinical["진료과"] == dept_choice]
        exam = exam.iloc[0:0]

    out = []
    if not clinical.empty:
        grouped = clinical.groupby(["진료과", "의사명"], dropna=False, sort=True)[["외래", "입원", "퇴원", "재원", "외래수입", "입원수입", "검진제외"]].sum()
        for (dept, doctor), vals in grouped.iterrows():
            rec = {"남": 0, "여": 0, "계": 0, "검진포함": vals["검진제외"], **vals.to_dict()}
            out.append(row_record("기간합계" if start != end else start.isoformat(), dept, doctor, rec))

    # 검진은 의사/진료과 소속 정보가 없어 병원 전체일 때 별도 행으로 표시.
    if mode == "전체" and dept_choice == "전체" and not exam.empty:
        sums = exam[["남", "여", "계", "검진수입"]].sum().to_dict()
        out.append(row_record("기간합계" if start != end else start.isoformat(), "검진", "공통", {
            "남": sums["남"], "여": sums["여"], "계": sums["계"],
            "검진포함": sums["검진수입"]}))
    result = pd.DataFrame(out, columns=COLUMNS)
    totals = {c: int(result[c].sum()) if not result.empty else 0 for c in NUMERIC}
    return result, totals


def html_report(frame, totals):
    headings = """<thead><tr><th rowspan="2">일자</th><th rowspan="2">진료과</th><th rowspan="2">의사명</th>
<th colspan="4">환자수</th><th colspan="3">환자수(검진)</th><th colspan="4">진료수입</th></tr>
<tr><th>외래</th><th>입원</th><th>퇴원</th><th>재원</th><th>남</th><th>여</th><th>계</th>
<th>외래</th><th>입원</th><th>검진제외</th><th>검진포함</th></tr></thead>"""
    body = []
    for _, item in frame.iterrows():
        is_exam = item["진료과"] == "검진"
        cells = "".join(f'<td>{html.escape(str(item[c]))}</td>' for c in COLUMNS[:3])
        cells += "".join(f'<td class="num">{int(item[c]):,}</td>' for c in NUMERIC)
        body.append(f'<tr class="{"exam" if is_exam else ""}">{cells}</tr>')
    values = "".join(f'<td class="num">{totals[c]:,}</td>' for c in NUMERIC)
    body.append(f'<tr class="total"><td>합계</td><td>전체</td><td>전체</td>{values}</tr>')
    return f'<div class="report-wrap"><table class="report-table">{headings}<tbody>{"".join(body)}</tbody></table></div>'


def excel_report(frame, totals):
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sheet = writer.book.create_sheet("진료현황")
        writer.book.remove(writer.book["Sheet"]) if "Sheet" in writer.book else None
        sheet.append(["일자", "진료과", "의사명", "환자수", None, None, None, "환자수(검진)", None, None, "진료수입", None, None, None])
        sheet.append([None, None, None, "외래", "입원", "퇴원", "재원", "남", "여", "계", "외래", "입원", "검진제외", "검진포함"])
        for region in ("A1:A2", "B1:B2", "C1:C2", "D1:G1", "H1:J1", "K1:N1"):
            sheet.merge_cells(region)
        for _, r in frame.iterrows():
            sheet.append([r[c] for c in COLUMNS])
        sheet.append(["합계", "전체", "전체"] + [totals[c] for c in NUMERIC])
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        for row in sheet.iter_rows():
            for cell in row:
                cell.alignment = Alignment(horizontal="center" if cell.column <= 3 or cell.row <= 2 else "right", vertical="center")
                if cell.row <= 2:
                    cell.fill = PatternFill("solid", fgColor="DEE5ED")
                    cell.font = Font(bold=True)
                if cell.row == sheet.max_row:
                    cell.fill = PatternFill("solid", fgColor="E8F0FC")
                    cell.font = Font(bold=True)
                if cell.row >= 3 and cell.column >= 4:
                    cell.number_format = '#,##0'
        for col in "ABCDEFGHIJKLMN":
            sheet.column_dimensions[col].width = 15 if col not in "BC" else 18
        sheet.freeze_panes = "D3"
    return buffer.getvalue()


try:
    accounts = fetch_accounts()
except Exception as e:
    st.error(f"PIN_관리 시트를 읽을 수 없습니다. 시트 이름·서비스 계정 권한을 확인하세요. ({e})")
    st.stop()

user = authenticated_user(accounts)
if user is None:
    left, right = st.columns([1, 2])
    with left:
        if Path("Hismedi_logo.png").exists():
            st.image("Hismedi_logo.png", width=180)
        st.title("Hismedi 진료현황")
        st.caption("4자리 PIN 코드를 입력하고 Enter를 누르세요.")
        with st.form("pin_login"):
            pin = st.text_input("PIN 코드", max_chars=4, type="password", autocomplete="off")
            submitted = st.form_submit_button("로그인", use_container_width=True)
        if submitted:
            matches = accounts[accounts["PIN"] == pin.strip()]
            if not pin_valid(pin):
                st.error("4자리 숫자를 입력해 주세요.")
            elif len(matches) != 1:
                st.error("PIN 코드가 올바르지 않거나 중복 등록되어 있습니다. 관리자에게 문의해 주세요.")
            else:
                account = matches.iloc[0]
                st.session_state["account_row"] = int(account["_row"])
                st.session_state["login_pin"] = str(account["PIN"])
                st.session_state["login_name"] = str(account["의사"])
                st.rerun()
    st.stop()

is_master = user["권한"] == "master"
try:
    clinical, exams = fetch_data()
except Exception as e:
    st.error(f"원본 데이터 연결 오류: {e}")
    st.stop()

all_dates = []
if not clinical.empty:
    all_dates.extend(clinical["일자"].tolist())
if not exams.empty:
    all_dates.extend(exams["일자"].tolist())
if not all_dates:
    st.warning("원본 데이터가 없습니다. 20XX_Q1~Q4, RAW_검진 시트를 확인해 주세요.")
    st.stop()
latest, earliest = max(all_dates), min(all_dates)

with st.sidebar:
    if Path("Hismedi_logo.png").exists():
        st.image("Hismedi_logo.png", width=145)
    st.markdown("### Hismedi 진료현황")
    st.caption(f"접속: {user['의사']} · {user['진료과']} ({'마스터' if is_master else '일반'})")
    st.divider()
    period_type = st.radio("날짜", ["일자별", "기간별"], horizontal=True)
    if period_type == "일자별":
        selected = st.date_input("조회일", value=latest, min_value=earliest, max_value=max(latest, date.today()))
        start = end = selected
    else:
        period = st.date_input("조회기간", value=(max(earliest, latest - timedelta(days=30)), latest),
                               min_value=earliest, max_value=max(latest, date.today()))
        if not isinstance(period, (tuple, list)) or len(period) != 2:
            st.info("시작일과 종료일을 모두 선택하세요.")
            st.stop()
        start, end = period
        if start > end:
            st.error("시작일은 종료일보다 늦을 수 없습니다.")
            st.stop()

    depts = sorted(clinical["진료과"].unique().tolist()) if not clinical.empty else []
    doctors = sorted(clinical["의사명"].unique().tolist()) if not clinical.empty else []
    if is_master:
        mode = st.selectbox("조회 범위", ["전체", "진료과", "의사"])
        if mode == "진료과":
            dept_choice = st.selectbox("진료과", depts)
            doctor_choice = "전체"
        elif mode == "의사":
            doctor_choice = st.selectbox("의사", doctors)
            dept_choice = "전체"
        else:
            dept_choice = "전체"
            doctor_choice = "전체"
    else:
        mode = st.selectbox("조회 범위", ["개인", "진료과", "전체"],
                            format_func=lambda x: {"개인": "내 실적", "진료과": "소속 진료과 실적", "전체": "병원 전체 실적"}[x])
        dept_choice, doctor_choice = user["진료과"], user["의사"]
        st.caption(f"소속: {dept_choice} / {doctor_choice}")

    if st.button("🔄 구글시트 데이터 동기화", use_container_width=True):
        refresh()
        st.rerun()
    st.divider()
    st.markdown("**계정**")
    if st.button("로그아웃", use_container_width=True):
        for k in ("account_row", "login_pin", "login_name"):
            st.session_state.pop(k, None)
        st.rerun()

# 메인 화면은 표를 최상단에 배치하고, 계정 메뉴는 접을 수 있는 영역으로 별도 제공
report, totals = summary(clinical, exams, mode, start, end, dept_choice, doctor_choice)
st.markdown(html_report(report, totals), unsafe_allow_html=True)
st.caption(f"조회 기간: {start:%Y-%m-%d} ~ {end:%Y-%m-%d}  |  검진 실적은 병원 전체 조회에서만 합산합니다. 낮병동 제외.")
st.download_button("⬇️ 조회 결과 엑셀 다운로드", data=excel_report(report, totals),
                   file_name=f"Hismedi_진료현황_{start:%Y%m%d}_{end:%Y%m%d}.xlsx",
                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

with st.expander("🔒 내 PIN 번호 변경"):
    with st.form("my_pin"):
        old_pin = st.text_input("현재 PIN", max_chars=4, type="password")
        new_pin = st.text_input("새 PIN (4자리)", max_chars=4, type="password")
        confirm_pin = st.text_input("새 PIN 확인", max_chars=4, type="password")
        change = st.form_submit_button("PIN 변경")
    if change:
        if old_pin != user["PIN"]:
            st.error("현재 PIN이 일치하지 않습니다.")
        elif not pin_valid(new_pin) or new_pin != confirm_pin:
            st.error("새 PIN은 4자리 숫자이며 확인값과 일치해야 합니다.")
        elif new_pin in accounts["PIN"].tolist():
            st.error("이미 사용 중인 PIN입니다. 다른 PIN을 선택해 주세요.")
        else:
            try:
                change_account(user["_row"], user["진료과"], user["의사"], new_pin, user["권한"])
                st.success("PIN이 변경되었습니다. 새 PIN으로 다시 로그인하세요.")
                for k in ("account_row", "login_pin", "login_name"):
                    st.session_state.pop(k, None)
            except Exception as e:
                st.error(f"변경 실패: {e}")

if is_master:
    with st.expander("⚙️ 마스터 권한관리 · 계정 추가/수정/삭제"):
        st.caption("계정은 PIN_관리 시트와 동기화됩니다. 계정 삭제 시 해당 사용자는 더 이상 로그인할 수 없습니다.")
        public = accounts[["진료과", "의사", "권한"]].copy()
        st.dataframe(public, use_container_width=True, hide_index=True)
        action = st.radio("작업 선택", ["계정 추가", "계정 수정", "계정 삭제"], horizontal=True)
        if action == "계정 추가":
            with st.form("add_account"):
                dept = st.text_input("진료과")
                name = st.text_input("의사/사용자 이름")
                newp = st.text_input("4자리 PIN", type="password", max_chars=4)
                role = st.selectbox("권한", ["user", "master"])
                submitted = st.form_submit_button("계정 추가")
            if submitted:
                if not dept.strip() or not name.strip() or not pin_valid(newp):
                    st.error("진료과·이름과 4자리 숫자 PIN을 입력하세요.")
                elif newp in accounts["PIN"].tolist():
                    st.error("중복 PIN은 등록할 수 없습니다.")
                elif not accounts[(accounts["진료과"] == dept.strip()) & (accounts["의사"] == name.strip())].empty:
                    st.error("동일한 진료과/이름의 계정이 이미 있습니다.")
                else:
                    try:
                        add_account(dept.strip(), name.strip(), newp, role)
                        st.success("추가되었습니다.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"추가 실패: {e}")
        else:
            labels = {int(r["_row"]): f"{r['진료과']} · {r['의사']} ({r['권한']})" for _, r in accounts.iterrows()}
            target = st.selectbox("대상 계정", options=list(labels), format_func=lambda x: labels[x])
            chosen = accounts[accounts["_row"] == target].iloc[0]
            if action == "계정 수정":
                with st.form("edit_account"):
                    dept = st.text_input("진료과", value=chosen["진료과"])
                    name = st.text_input("이름", value=chosen["의사"])
                    newp = st.text_input("PIN (변경 시 새 PIN)", value=chosen["PIN"], max_chars=4, type="password")
                    role = st.selectbox("권한", ["user", "master"], index=0 if chosen["권한"] != "master" else 1)
                    submitted = st.form_submit_button("수정 저장")
                if submitted:
                    other = accounts[accounts["_row"] != target]
                    if not dept.strip() or not name.strip() or not pin_valid(newp):
                        st.error("입력 내용을 확인하세요.")
                    elif newp in other["PIN"].tolist():
                        st.error("중복 PIN입니다.")
                    elif chosen["권한"] == "master" and role != "master" and len(accounts[accounts["권한"] == "master"]) <= 1:
                        st.error("마지막 마스터 계정의 권한은 해제할 수 없습니다.")
                    else:
                        try:
                            change_account(target, dept.strip(), name.strip(), newp, role)
                            st.success("수정되었습니다.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"수정 실패: {e}")
            else:
                with st.form("delete_account"):
                    confirm = st.checkbox("선택한 계정을 삭제하겠습니다.")
                    submitted = st.form_submit_button("계정 삭제")
                if submitted:
                    if not confirm:
                        st.warning("삭제 확인을 선택하세요.")
                    elif target == user["_row"]:
                        st.error("로그인 중인 본인 계정은 삭제할 수 없습니다.")
                    elif chosen["권한"] == "master" and len(accounts[accounts["권한"] == "master"]) <= 1:
                        st.error("마지막 마스터 계정은 삭제할 수 없습니다.")
                    else:
                        try:
                            remove_account(target)
                            st.success("삭제되었습니다.")
                            st.rerun()
                        except Exception as e:
                            st.error(f"삭제 실패: {e}")
