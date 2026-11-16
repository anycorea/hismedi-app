"""HISMEDI 치료재료 관리 - 1차: 직원 인증/권한/관리부서 관리.

Streamlit Secrets:
[gcp_service_account] # existing complete Google service account
...
[treatments]
spreadsheet_id = "..."
emp_worksheet = "Emp"
admin_setup_key = "a-long-unique-random-secret"  # only for initializing first MASTER
"""
from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound

st.set_page_config(page_title="HISMEDI | 치료재료 관리", page_icon="🏥", layout="wide")
KST = timezone(timedelta(hours=9))
EMP_HEADERS = ["사번", "소속부서", "이름", "관리부서", "PIN", "권한", "사용여부"]
AUTH_HEADERS = ["사번", "PIN해시", "솔트", "반복횟수", "변경필요", "실패횟수", "잠금종료", "수정일시"]
LOG_HEADERS = ["일시", "작업자사번", "작업", "대상사번", "상세"]
ITERATIONS = 260_000
LOCK_MINUTES = 15
MAX_FAILURES = 5
DEFAULT_DEPARTMENTS = [
    "소아청소년과", "내과", "외과", "정형외과", "마취통증의학과", "진단검사의학과", "영상의학과",
    "진단검사의학팀", "영상의학팀", "물리치료팀", "외래1팀", "외래2팀", "6병동", "7병동",
    "8병동", "수술실", "내시경실", "건강검진팀", "주사실(성인)", "주사실(소아)", "약제팀",
]
DEFAULT_DEPARTMENTS = list(dict.fromkeys(DEFAULT_DEPARTMENTS))


def now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def normalize_employee_id(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def safe_spreadsheet_error(exc: Exception) -> str:
    return f"Google Sheets 연결 또는 처리 오류: {type(exc).__name__}. 서비스 계정 공유 권한, Secrets, 시트 구조를 확인해 주세요."


@st.cache_resource(show_spinner=False)
def get_book():
    credentials = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(credentials)
    return client.open_by_key(st.secrets["treatments"]["spreadsheet_id"])


def get_ws(name: str, headers: list[str] | None = None):
    book = get_book()
    try:
        sheet = book.worksheet(name)
    except WorksheetNotFound:
        if headers is None:
            raise
        sheet = book.add_worksheet(title=name, rows=500, cols=max(len(headers), 8))
        sheet.update(range_name="A1", values=[headers])
    if headers:
        actual = sheet.row_values(1)
        if actual[:len(headers)] != headers:
            raise ValueError(f"'{name}' 시트의 제목행이 예상 구조와 다릅니다: {actual}")
    return sheet


def rows_as_dicts(ws, columns: list[str]) -> list[dict]:
    values = ws.get_all_values()
    output = []
    for index, cells in enumerate(values[1:], start=2):
        if not any(str(v).strip() for v in cells):
            continue
        expanded = (cells + [""] * len(columns))[: len(columns)]
        entry = dict(zip(columns, [str(v).strip() for v in expanded]))
        entry["_row"] = index
        output.append(entry)
    return output


def employees() -> list[dict]:
    return rows_as_dicts(get_ws(st.secrets["treatments"].get("emp_worksheet", "Emp"), EMP_HEADERS), EMP_HEADERS)


def auth_rows() -> list[dict]:
    return rows_as_dicts(get_ws("Auth", AUTH_HEADERS), AUTH_HEADERS)


def write_auth(employee_id: str, password: str, must_change: bool):
    if not (password.isdigit() and 6 <= len(password) <= 12):
        raise ValueError("PIN은 6~12자리 숫자여야 합니다.")
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), ITERATIONS).hex()
    record = [employee_id, hashed, salt, str(ITERATIONS), "Y" if must_change else "N", "0", "", now()]
    ws = get_ws("Auth", AUTH_HEADERS)
    match = next((r for r in auth_rows() if r["사번"] == employee_id), None)
    if match:
        ws.update(range_name=f"A{match['_row']}:H{match['_row']}", values=[record])
    else:
        ws.append_row(record, value_input_option="RAW")


def update_auth_attempt(record: dict, count: int, locked_until: str = ""):
    get_ws("Auth", AUTH_HEADERS).update(
        range_name=f"F{record['_row']}:G{record['_row']}",
        values=[[str(count), locked_until]],
    )


def verify_auth(employee_id: str, pin: str) -> tuple[bool, str]:
    auth = next((r for r in auth_rows() if r["사번"] == employee_id), None)
    if not auth:
        return False, "등록된 PIN이 없습니다. 관리자에게 초기 PIN 발급을 요청하세요."
    if auth["잠금종료"]:
        try:
            if datetime.fromisoformat(auth["잠금종료"]) > datetime.now(KST):
                return False, "로그인 시도 횟수 초과로 계정이 잠겼습니다. 잠시 후 다시 시도하세요."
        except ValueError:
            pass
    try:
        actual = hashlib.pbkdf2_hmac(
            "sha256", pin.encode("utf-8"), bytes.fromhex(auth["솔트"]), int(auth["반복횟수"])
        )
        expected = bytes.fromhex(auth["PIN해시"])
        valid = hmac.compare_digest(actual, expected)
    except (ValueError, TypeError):
        valid = False
    if valid:
        update_auth_attempt(auth, 0)
        return True, auth["변경필요"]
    failure_count = int(auth["실패횟수"] or 0) + 1
    if failure_count >= MAX_FAILURES:
        until = (datetime.now(KST) + timedelta(minutes=LOCK_MINUTES)).isoformat(timespec="seconds")
        update_auth_attempt(auth, failure_count, until)
        return False, "로그인 시도 횟수 초과로 15분 동안 잠깁니다."
    update_auth_attempt(auth, failure_count)
    return False, "사번 또는 PIN을 확인해 주세요."


def audit(action: str, target: str = "", detail: str = ""):
    actor = st.session_state.get("employee_id", "SYSTEM")
    get_ws("Audit_Log", LOG_HEADERS).append_row([now(), actor, action, target, detail], value_input_option="RAW")


def sign_out():
    for key in ["employee_id", "forced_change", "issued_pin"]:
        st.session_state.pop(key, None)
    st.rerun()


def department_list(data: list[dict]) -> list[str]:
    result = list(DEFAULT_DEPARTMENTS)
    for record in data:
        for dept in [record["소속부서"], *record["관리부서"].split(",")]:
            dept = dept.strip()
            if dept and dept not in result and dept.upper() != "전체":
                result.append(dept)
    return result


def allowed_departments(emp: dict, data: list[dict]) -> list[str]:
    if emp["권한"].upper() == "MASTER":
        return department_list(data)
    configured = [d.strip() for d in emp["관리부서"].split(",") if d.strip()]
    return department_list(data) if "전체" in configured else configured


def save_emp(fields: list[str], existing_row: int | None = None):
    ws = get_ws(st.secrets["treatments"].get("emp_worksheet", "Emp"), EMP_HEADERS)
    if existing_row:
        # PIN column E is intentionally not written during employee edits.
        ws.update(range_name=f"A{existing_row}:D{existing_row}", values=[fields[:4]])
        ws.update(range_name=f"F{existing_row}:G{existing_row}", values=[fields[5:7]])
    else:
        ws.append_row(fields, value_input_option="RAW")


def is_active(emp: dict) -> bool:
    return emp["사용여부"].upper() == "Y"


def valid_id(identifier: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", identifier))



def render_header():
    """Compact hospital-branded header; reuse repository logo."""
    st.markdown("""
    <style>
    .block-container {padding-top: 1.25rem; padding-bottom: 2rem;}
    div[data-testid="stAlert"] {font-size: .93rem;}
    </style>
    """, unsafe_allow_html=True)
    logo_paths = [
        Path(__file__).resolve().parent / "Hismedi_logo.png",
        Path(__file__).resolve().parent / "hismedi_logo.png",
    ]
    logo = next((path for path in logo_paths if path.is_file()), None)
    left, right = st.columns([1, 11], gap="medium", vertical_alignment="center")
    with left:
        if logo:
            st.image(str(logo), width=72)
        else:
            st.markdown("🏥")
    with right:
        st.markdown("""
        <div style="font-size:1.55rem; font-weight:750; line-height:1.25; color:var(--text-color);">
        HISMEDI 치료재료 관리
        </div>
        <div style="font-size:.84rem; color:gray; margin-top:.24rem;">
        직원 인증 · 관리부서 설정
        </div>
        """, unsafe_allow_html=True)
    st.divider()

def main():
    render_header()
    try:
        data = employees()
        get_ws("Auth", AUTH_HEADERS)
        get_ws("Audit_Log", LOG_HEADERS)
    except Exception as exc:
        st.error(safe_spreadsheet_error(exc))
        st.caption(str(exc) if isinstance(exc, ValueError) else "Google Sheets 제목행과 공유 설정을 확인하세요.")
        st.stop()

    # A MASTER cannot self-register solely using their employee ID.
    masters = [r for r in data if r["권한"].upper() == "MASTER" and is_active(r)]
    initialized_master = any(r["사번"] in {a["사번"] for a in auth_rows()} for r in masters)
    if not initialized_master:
        st.info("최초 사용: MASTER 계정의 PIN을 설정해 주세요.")
        if not st.secrets["treatments"].get("admin_setup_key", ""):
            st.warning("현재 실행 중인 앱에서 admin_setup_key를 찾지 못했습니다. 치료재료 앱의 Streamlit Cloud → Settings → Secrets에서 [treatments] 아래에 admin_setup_key = \"숫자8자리\"가 있는지 확인하고 저장해 주세요.")
            st.stop()
        if not masters:
            st.error("Emp 시트에 사용여부 Y인 Master 계정이 없습니다.")
            st.stop()
        with st.form("initialize_master"):
            identifier = st.text_input("MASTER 사번", max_chars=6)
            setup_key = st.text_input("최초 설정키", type="password")
            first_pin = st.text_input("새 PIN (숫자 6~12자리)", type="password")
            pin_confirm = st.text_input("PIN 확인", type="password")
            pressed = st.form_submit_button("최초 관리자 PIN 설정", type="primary")
        if pressed:
            master_ids = {r["사번"] for r in masters}
            if identifier not in master_ids or not hmac.compare_digest(
                setup_key, str(st.secrets["treatments"]["admin_setup_key"])
            ):
                st.error("인증정보가 일치하지 않습니다.")
            elif first_pin != pin_confirm:
                st.error("PIN이 일치하지 않습니다.")
            else:
                try:
                    write_auth(identifier, first_pin, False)
                    audit("FIRST_MASTER_SETUP", identifier)
                    st.success("첫 MASTER PIN 등록 완료. 이제 로그인해 주세요.")
                    st.rerun()
                except ValueError as exc:
                    st.error(str(exc))
        st.stop()

    if "employee_id" not in st.session_state:
        with st.form("login"):
            identifier = st.text_input("사번 (6자리)", max_chars=6)
            pin = st.text_input("PIN", type="password")
            submitted = st.form_submit_button("로그인", type="primary", use_container_width=True)
        if submitted:
            emp = next((r for r in data if r["사번"] == normalize_employee_id(identifier)), None)
            if emp is None or not is_active(emp):
                st.error("사번 또는 PIN을 확인해 주세요.")
            elif not valid_id(emp["사번"]):
                st.error("Emp의 사번은 숫자 6자리여야 합니다.")
            else:
                try:
                    valid, status = verify_auth(emp["사번"], pin)
                    if valid:
                        st.session_state.employee_id = emp["사번"]
                        st.session_state.forced_change = status == "Y"
                        audit("LOGIN", emp["사번"])
                        st.rerun()
                    st.error(status)
                except Exception as exc:
                    st.error(safe_spreadsheet_error(exc))
        st.caption("PIN을 발급받지 않은 직원은 MASTER에게 초기 PIN 발급을 요청하세요.")
        st.stop()

    emp = next((r for r in data if r["사번"] == st.session_state.employee_id), None)
    if not emp or not is_active(emp):
        sign_out()
    role = emp["권한"].upper()
    if role not in ("MASTER", "USER"):
        st.error("Emp의 권한은 Master 또는 User여야 합니다.")
        sign_out()

    with st.sidebar:
        st.subheader(f"{emp['이름']} 님")
        st.write(f"소속: {emp['소속부서']}")
        st.write(f"권한: **{role}**")
        if st.button("로그아웃", use_container_width=True):
            sign_out()

    if st.session_state.get("forced_change"):
        st.warning("임시 PIN으로 로그인했습니다. 새 PIN으로 변경해야 이용할 수 있습니다.")
        tabs = ["PIN 변경"]
    else:
        tabs = ["내 정보", "PIN 변경"] + (["직원 관리"] if role == "MASTER" else [])

    tab_objects = st.tabs(tabs)
    for name, tab in zip(tabs, tab_objects):
        with tab:
            if name == "내 정보":
                st.subheader("나의 관리부서")
                st.write(" · ".join(allowed_departments(emp, data)) or "설정된 관리부서가 없습니다.")
                st.info("현재는 로그인·직원관리 1차 버전입니다. 품목/입출고/재고 기능은 다음 단계에서 추가됩니다.")
            elif name == "PIN 변경":
                with st.form("change_pin"):
                    old_pin = st.text_input("현재 PIN", type="password")
                    new_pin = st.text_input("새 PIN (숫자 6~12자리)", type="password")
                    new_pin_check = st.text_input("새 PIN 확인", type="password")
                    pressed = st.form_submit_button("PIN 변경", type="primary")
                if pressed:
                    if new_pin != new_pin_check:
                        st.error("새 PIN이 일치하지 않습니다.")
                    elif new_pin == old_pin:
                        st.error("기존 PIN과 다른 번호를 사용하세요.")
                    else:
                        try:
                            good, _ = verify_auth(emp["사번"], old_pin)
                            if not good:
                                st.error("현재 PIN이 일치하지 않습니다.")
                            else:
                                write_auth(emp["사번"], new_pin, False)
                                audit("CHANGE_PIN", emp["사번"])
                                st.session_state.forced_change = False
                                st.success("PIN이 변경되었습니다.")
                                st.rerun()
                        except ValueError as exc:
                            st.error(str(exc))
                        except Exception as exc:
                            st.error(safe_spreadsheet_error(exc))
            elif name == "직원 관리":
                st.subheader("직원 조회")
                view_rows = [{
                    "사번": x["사번"], "소속부서": x["소속부서"], "이름": x["이름"],
                    "관리부서": x["관리부서"], "권한": x["권한"], "사용여부": x["사용여부"]
                } for x in data]
                st.dataframe(pd.DataFrame(view_rows), hide_index=True, use_container_width=True)
                st.divider()
                left, right = st.columns(2)
                with left:
                    st.markdown("#### 직원 추가 / 수정")
                    choices = ["[신규 직원]"] + [f"{x['사번']} | {x['이름']}" for x in data]
                    selected = st.selectbox("대상 직원", choices)
                    target = None if selected == "[신규 직원]" else next(
                        x for x in data if x["사번"] == selected.split(" | ")[0]
                    )
                    departments = department_list(data)
                    with st.form("edit_emp"):
                        eid = st.text_input("사번 (6자리)", value=target["사번"] if target else "", disabled=target is not None)
                        person_name = st.text_input("이름", value=target["이름"] if target else "")
                        owned = st.text_input("소속부서", value=target["소속부서"] if target else "")
                        current = [i.strip() for i in target["관리부서"].split(",") if i.strip()] if target else []
                        managed = st.multiselect("관리부서 (복수 선택 가능)", options=departments, default=[x for x in current if x in departments])
                        permission = st.selectbox("권한", ["User", "Master"], index=0 if not target or target["권한"].upper() == "USER" else 1)
                        enabled = st.selectbox("사용여부", ["Y", "N"], index=0 if not target or is_active(target) else 1)
                        save = st.form_submit_button("직원정보 저장", type="primary")
                    if save:
                        if not valid_id(eid):
                            st.error("사번은 숫자 6자리여야 합니다.")
                        elif not person_name.strip() or not owned.strip():
                            st.error("이름과 소속부서를 입력해 주세요.")
                        elif target is None and any(x["사번"] == eid for x in data):
                            st.error("이미 존재하는 사번입니다.")
                        elif target and target["사번"] == emp["사번"] and (permission != "Master" or enabled == "N"):
                            st.error("로그인 중인 MASTER 자신의 권한을 해제하거나 사용중지할 수 없습니다.")
                        else:
                            row = [eid, owned.strip(), person_name.strip(), ", ".join(managed), "", permission, enabled]
                            try:
                                save_emp(row, target["_row"] if target else None)
                                audit("UPDATE_EMP" if target else "ADD_EMP", eid, "권한:" + permission)
                                st.success("저장했습니다.")
                                st.rerun()
                            except Exception as exc:
                                st.error(safe_spreadsheet_error(exc))
                with right:
                    st.markdown("#### 초기 PIN 발급 / 재발급")
                    candidate_list = [x for x in data if x["사용여부"].upper() == "Y"]
                    if candidate_list:
                        target_option = st.selectbox("대상 직원", [f"{x['사번']} | {x['이름']}" for x in candidate_list])
                        target_id = target_option.split(" | ")[0]
                        if st.button("임시 PIN 발급", type="secondary"):
                            try:
                                temp_pin = f"{secrets.randbelow(90000000) + 10000000:08d}"
                                write_auth(target_id, temp_pin, True)
                                audit("ISSUE_TEMP_PIN", target_id)
                                st.session_state.issued_pin = (target_id, temp_pin)
                            except Exception as exc:
                                st.error(safe_spreadsheet_error(exc))
                        if "issued_pin" in st.session_state:
                            issued_id, issued_pin = st.session_state.issued_pin
                            st.warning("임시 PIN은 아래에서 한 번 확인 후 대상 직원에게 직접 전달하세요. 직원은 로그인 후 PIN을 변경해야 합니다.")
                            st.code(f"사번: {issued_id}  /  임시 PIN: {issued_pin}")
                            if st.button("임시 PIN 표시 지우기"):
                                st.session_state.pop("issued_pin", None)
                                st.rerun()
                    else:
                        st.info("사용 중인 직원이 없습니다.")
    st.caption("HISMEDI · 치료재료 관리 / 로그인 및 직원관리 1차")


if __name__ == "__main__":
    main()
