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
import uuid
import base64
from io import BytesIO
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import requests
from google.auth.transport.requests import AuthorizedSession
from PIL import Image
import hmac
import re
import secrets
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

BASE_PATH = Path(__file__).resolve().parent
ICON_PATH = BASE_PATH / "Hismedi_logo.png"
try:
    page_icon = Image.open(ICON_PATH).copy() if ICON_PATH.is_file() else "🏥"
except Exception:
    page_icon = "🏥"
st.set_page_config(page_title="HISMEDI | 치료재료 관리", page_icon=page_icon, layout="wide")
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
LOGGER = logging.getLogger("hismedi.material")


def api_action(fn, *args, **kwargs):
    """Retry transient Google API read failures and quota issues, with longer 429 cooldown.

    CAUTION: do not use automatic retries for non-idempotent appends unless a
    request identifier and reconciliation have been implemented.
    """
    for attempt in range(4):
        try:
            return fn(*args, **kwargs)
        except APIError as exc:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None)
            detail = str(exc).lower()
            retryable = status in (429, 500, 502, 503, 504) or (
                status == 403 and ("ratelimitexceeded" in detail or "quota" in detail)
            )
            if not retryable or attempt == 3:
                LOGGER.exception("Google Sheets API request failed: HTTP %s", status)
                raise
            # A 429 after batch uploads usually needs a quota-window reset,
            # not the 1-2 second wait used for normal transient failures.
            if status == 429 or (status == 403 and "quota" in detail):
                delay = (15, 25, 35)[attempt]
            else:
                delay = (2, 4, 8)[attempt]
            LOGGER.warning("Google API %s; retry in %s seconds", status, delay)
            time.sleep(delay)




def now() -> str:
    return datetime.now(KST).isoformat(timespec="seconds")


def normalize_employee_id(value: Any) -> str:
    s = str(value).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def safe_spreadsheet_error(exc: Exception) -> str:
    if isinstance(exc, APIError):
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 429:
            return "Google Sheets API 요청 한도를 초과했습니다(429). 잠시 후 다시 시도해 주세요."
        if status == 403:
            return "Google Sheets API 접근이 거부되었습니다(403). API 사용 설정·할당량·서비스 계정 권한을 확인하세요."
        if status == 404:
            return "Google Sheets 문서를 찾지 못했습니다(404). 스프레드시트 ID와 공유 권한을 확인하세요."
        return f"Google Sheets API 오류(HTTP {status or '알 수 없음'}). Streamlit 로그에서 상세 원인을 확인하세요."
    return f"Google Sheets 연결 또는 처리 오류: {type(exc).__name__}. 서비스 계정 공유 권한, Secrets, 시트 구조를 확인해 주세요."


@st.cache_resource(show_spinner=False)
def get_book():
    credentials = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    client = gspread.authorize(credentials)
    return client.open_by_key(st.secrets["treatments"]["spreadsheet_id"])


@st.cache_resource(show_spinner=False)
def existing_ws(name: str):
    """Reuse worksheet metadata across Streamlit reruns and open browser sessions."""
    return api_action(get_book().worksheet, name)


def get_ws(name: str, headers: list[str] | None = None):
    try:
        return existing_ws(name)
    except WorksheetNotFound:
        if headers is None:
            raise
        book = get_book()
        # Only create a new worksheet when it is genuinely absent.
        sheet = api_action(book.add_worksheet, title=name, rows=500, cols=max(len(headers), 8))
        api_action(sheet.update, range_name="A1", values=[headers])
        existing_ws.clear()
        return sheet


def rows_as_dicts(ws, columns: list[str]) -> list[dict]:
    values = api_action(ws.get_all_values)
    if not values or values[0][:len(columns)] != columns:
        raise ValueError(f"'{ws.title}' 시트의 제목행이 예상 구조와 다릅니다: {values[0] if values else []}")
    output = []
    for index, cells in enumerate(values[1:], start=2):
        if not any(str(v).strip() for v in cells):
            continue
        expanded = (cells + [""] * len(columns))[:len(columns)]
        entry = dict(zip(columns, [str(v).strip() for v in expanded]))
        entry["_row"] = index
        output.append(entry)
    return output


def employees_fresh() -> list[dict]:
    return rows_as_dicts(get_ws(st.secrets["treatments"].get("emp_worksheet", "Emp"), EMP_HEADERS), EMP_HEADERS)


@st.cache_data(ttl=20, show_spinner=False)
def employees() -> list[dict]:
    return employees_fresh()


def auth_rows_fresh() -> list[dict]:
    return rows_as_dicts(get_ws("Auth", AUTH_HEADERS), AUTH_HEADERS)


@st.cache_data(ttl=20, show_spinner=False)
def auth_rows() -> list[dict]:
    return auth_rows_fresh()


def write_auth(employee_id: str, password: str, must_change: bool):
    if not (password.isdigit() and 6 <= len(password) <= 12):
        raise ValueError("PIN은 6~12자리 숫자여야 합니다.")
    salt = secrets.token_hex(16)
    hashed = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), bytes.fromhex(salt), ITERATIONS).hex()
    record = [employee_id, hashed, salt, str(ITERATIONS), "Y" if must_change else "N", "0", "", now()]
    ws = get_ws("Auth", AUTH_HEADERS)
    match = next((r for r in auth_rows_fresh() if r["사번"] == employee_id), None)
    if match:
        ws.update(range_name=f"A{match['_row']}:H{match['_row']}", values=[record])
    else:
        ws.append_row(record, value_input_option="RAW")
    auth_rows.clear()


def update_auth_attempt(record: dict, count: int, locked_until: str = ""):
    get_ws("Auth", AUTH_HEADERS).update(
        range_name=f"F{record['_row']}:G{record['_row']}",
        values=[[str(count), locked_until]],
    )
    auth_rows.clear()


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


def test_context():
    """Returns actual MASTER and selected USER only in live test mode."""
    master_id = st.session_state.get("employee_id", "")
    user_id = st.session_state.get("material_preview_user", "")
    return (master_id, user_id) if master_id and user_id else None


def test_mark(memo: str) -> str:
    context = test_context()
    label = f"[TEST MASTER:{context[0]} USER:{context[1]}] " if context else ""
    return (label + str(memo or ""))[:300]


def test_audit(action: str, detail: str = ""):
    context = test_context()
    if not context:
        return
    try:
        audit("TEST_" + action, context[1], f"실제 작업자 MASTER:{context[0]} / USER 화면:{context[1]} / {detail}"[:300])
    except Exception:
        LOGGER.exception("테스트 작업이력 기록 실패: %s", action)


def sign_out():
    for key in ["employee_id", "forced_change", "issued_pin", "material_preview_user", "material_cart"]:
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
    employees.clear()


def is_active(emp: dict) -> bool:
    return emp["사용여부"].upper() == "Y"


def valid_id(identifier: str) -> bool:
    return bool(re.fullmatch(r"\d{6}", identifier))



BASE_DIR = Path(__file__).resolve().parent
LOGO_FILE = BASE_DIR / "Hismedi_logo.png"
BUILDING_FILE = BASE_DIR / "Hismedi_building.jpg"


def render_style():
    st.markdown("""
    <style>
    .block-container {max-width: 1760px; padding-top: 2.1rem; padding-bottom: 1.8rem;}
    h1 {font-size: 1.45rem !important; margin-bottom: .35rem !important;}
    h2 {font-size: 1.18rem !important;}
    h3 {font-size: 1.03rem !important;}
    div[data-testid="stAlert"] {font-size: .94rem;}
    div[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] {gap: .27rem;}
    [data-testid="stSidebar"] [data-testid="stImage"] img {object-fit:contain!important;object-position:center center!important;max-width:100%!important;height:auto!important;}
    [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {padding-top:0!important;}
    [data-testid="stSidebar"] [data-testid="stDivider"] {margin: .35rem 0!important;}
    [data-testid="stMain"] [data-testid="stVerticalBlock"] {scroll-margin-top:1.5rem;}
    [data-testid="stMain"] [data-baseweb="tab-list"] {padding-top:5px!important; overflow:visible!important;}
    /* Desktop left menu: full-width real buttons, same dimensions as logout. */
    [data-testid="stSidebar"] [class*="st-key-hm_nav_normal_"] button,
    [data-testid="stSidebar"] [class*="st-key-hm_nav_selected_"] button {
        width:100%!important;min-height:40px!important;
        justify-content:flex-start!important;text-align:left!important;
        padding:8px 14px!important;border-radius:8px!important;
        border:1px solid #d8e1eb!important;background:#ffffff!important;
        color:#203850!important;font-size:14px!important;font-weight:600!important;
        box-shadow:none!important;transition:all .12s ease!important;
    }
    [data-testid="stSidebar"] [class*="st-key-hm_nav_normal_"] button:hover {
        background:#f3f7fc!important;border-color:#9fbcd9!important;
    }
    [data-testid="stSidebar"] [class*="st-key-hm_nav_selected_"] button {
        background:#eaf3ff!important;border:1px solid #367cba!important;
        color:#17466f!important;font-weight:750!important;
        box-shadow:inset 3px 0 0 #367cba!important;
    }
    [data-testid="stSidebar"] [class*="st-key-hm_nav_normal_"] button p,
    [data-testid="stSidebar"] [class*="st-key-hm_nav_selected_"] button p {
        text-align:left!important;font-size:14px!important;
    }
    [data-testid="stSidebar"] {min-width:250px!important;max-width:290px!important;}
    /* Constrain login photo independently of the visitor's display resolution. */
    .hm-login-photo {width:100%;display:flex;justify-content:center;align-items:center;min-height:200px;}
    .hm-login-photo img {display:block;max-width:100%;width:auto;height:auto;max-height:min(58vh,520px);object-fit:contain;border-radius:8px;}
    .hm-login-heading {font-size:clamp(20px,1.6vw,26px);font-weight:750;margin:0 0 10px;}
    .hm-login-description {display:flex;flex-wrap:wrap;align-items:baseline;gap:7px 18px;margin:0 0 11px;line-height:1.5;}
    .hm-login-label {font-weight:750;font-size:15px;white-space:nowrap;}
    .hm-login-help {font-size:12px;color:#718096;}
    @media (max-width:1000px) {
        .block-container {padding-left:1.1rem;padding-right:1.1rem;padding-top:1.65rem;}
        .hm-login-photo img {max-height:42vh;}
    }
    .hm-head-title {font-size:1.45rem;font-weight:730;line-height:1.3;letter-spacing:-.035em;}
    .hm-head-note {font-size:.87rem;color:gray;margin-top:.25rem;}
    .hm-caption {font-size:.85rem;color:gray;}
    </style>
    """, unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def padded_logo(path: str) -> bytes:
    """진료현황 앱과 동일한 로고 보호 여백. 원본 픽셀은 자르지 않습니다."""
    with Image.open(path) as image:
        image = image.convert("RGBA")
        margin = round(max(image.size) * .16)
        canvas = Image.new("RGBA", (image.width + 2 * margin, image.height + 2 * margin), (255, 255, 255, 255))
        canvas.alpha_composite(image, (margin, margin))
        buffer = BytesIO()
        canvas.convert("RGB").save(buffer, format="PNG")
        return buffer.getvalue()


def show_logo(path: Path, width: int, *, centered: bool = False):
    """고정 캔버스 안에 contain 비율로 표시해 상하단 잘림 방지."""
    if not path.is_file():
        st.markdown("**HISMEDI HOSPITAL**")
        return
    encoded = base64.b64encode(padded_logo(str(path))).decode("ascii")
    align = "center" if centered else "left"
    st.markdown(
        f'<div style="display:flex;justify-content:{align};align-items:center;'
        f'width:100%;min-height:{int(width*1.05)}px;overflow:visible;margin:0 0 4px 0">'
        f'<img alt="HISMEDI HOSPITAL" src="data:image/png;base64,{encoded}" '
        f'style="width:{int(width)}px;max-width:100%;height:auto;display:block;'
        f'object-fit:contain;overflow:visible;clip-path:none"></div>',
        unsafe_allow_html=True,
    )


def render_header(subtitle: str = "병원 치료재료 · 부서별 물품관리"):
    left, right = st.columns([.8, 11.2], gap="medium", vertical_alignment="center")
    with left:
        if LOGO_FILE.is_file():
            show_logo(LOGO_FILE, 88)
        else:
            st.markdown("🏥")
    with right:
        st.markdown('<div class="hm-head-title">HISMEDI 치료재료 관리</div>'
                    f'<div class="hm-head-note">{subtitle}</div>', unsafe_allow_html=True)
    st.divider()


@st.cache_data(show_spinner=False)
def encoded_building(path: str) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("ascii")


def render_login_visual():
    """Responsive desktop login: the photo stays bounded on smaller monitors."""
    if BUILDING_FILE.is_file():
        image_col, form_col = st.columns([1, 1.05], gap="large", vertical_alignment="center")
        with image_col:
            st.markdown(
                '<div class="hm-login-photo"><img alt="히즈메디병원 외관" '
                'src="data:image/jpeg;base64,' + encoded_building(str(BUILDING_FILE)) + '"></div>',
                unsafe_allow_html=True,
            )
        return form_col
    return st.container()


def show_pending_area(title: str, features: list[str]):
    st.subheader(title)
    st.info("이 메뉴는 다음 개발 단계에서 연결됩니다. 아직 데이터를 등록하거나 재고를 변경하지 않습니다.")
    st.caption("예정 기능: " + " · ".join(features))


def render_personal_info(emp: dict, data: list[dict]):
    st.subheader("내 정보")
    a, b, c = st.columns([1, 1, 2])
    a.metric("이름", emp["이름"])
    b.metric("권한", emp["권한"].upper())
    c.metric("소속부서", emp["소속부서"])
    st.markdown("**관리 가능한 부서**")
    departments = allowed_departments(emp, data)
    st.write(" · ".join(departments) if departments else "관리부서가 지정되지 않았습니다.")


def render_pin_change(emp: dict):
    st.subheader("내 PIN 변경")
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


def render_employee_admin(emp: dict, data: list[dict]):
    st.subheader("직원 관리")
    lookup_tab, edit_tab, issue_tab = st.tabs(["직원 조회", "직원 추가·수정", "PIN 발급·초기화"])
    with lookup_tab:
        view_rows = [{
            "사번": x["사번"], "소속부서": x["소속부서"], "이름": x["이름"],
            "관리부서": x["관리부서"], "권한": x["권한"], "사용여부": x["사용여부"]
        } for x in data]
        st.dataframe(pd.DataFrame(view_rows), hide_index=True, use_container_width=True, height=580)
    with edit_tab:
        choices = ["[신규 직원]"] + [f"{x['사번']} | {x['이름']}" for x in data]
        selected = st.selectbox("대상 직원", choices)
        target = None if selected == "[신규 직원]" else next(
            x for x in data if x["사번"] == selected.split(" | ")[0]
        )
        departments = department_list(data)
        with st.form("edit_emp"):
            left, right = st.columns(2)
            with left:
                eid = st.text_input("사번 (6자리)", value=target["사번"] if target else "", disabled=target is not None)
                person_name = st.text_input("이름", value=target["이름"] if target else "")
                owned = st.text_input("소속부서", value=target["소속부서"] if target else "")
            with right:
                current = [i.strip() for i in target["관리부서"].split(",") if i.strip()] if target else []
                managed = st.multiselect("관리부서 (복수 선택)", options=departments,
                                         default=[x for x in current if x in departments])
                permission = st.selectbox("권한", ["User", "Master"],
                                          index=0 if not target or target["권한"].upper() == "USER" else 1)
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
    with issue_tab:
        candidate_list = [x for x in data if is_active(x)]
        if candidate_list:
            target_option = st.selectbox("대상 직원", [f"{x['사번']} | {x['이름']}" for x in candidate_list])
            target_id = target_option.split(" | ")[0]
            if st.button("임시 PIN 발급", type="primary"):
                try:
                    temp_pin = f"{secrets.randbelow(90000000) + 10000000:08d}"
                    write_auth(target_id, temp_pin, True)
                    audit("ISSUE_TEMP_PIN", target_id)
                    st.session_state.issued_pin = (target_id, temp_pin)
                except Exception as exc:
                    st.error(safe_spreadsheet_error(exc))
            if "issued_pin" in st.session_state:
                issued_id, issued_pin = st.session_state.issued_pin
                st.warning("임시 PIN은 대상 직원에게 직접 전달하세요. 로그인 후 반드시 변경해야 합니다.")
                st.code(f"사번: {issued_id}  /  임시 PIN: {issued_pin}")
                if st.button("임시 PIN 표시 지우기"):
                    st.session_state.pop("issued_pin", None)
                    st.rerun()
        else:
            st.info("사용 중인 직원이 없습니다.")


# ─────────────────────────────────────────────────────────────────────────────
# HIRA master catalog import: Drive XLSX -> validated staging -> Google Sheets.
# This code intentionally does not alter Emp/Auth or departmental stock tables.
# ─────────────────────────────────────────────────────────────────────────────
HIRA_HEADERS = [
    "코드", "급여구분", "현재상태", "중분류", "중분류코드", "품명", "규격", "단위",
    "상한금액", "제조회사", "제조수입업소", "최초등재일", "적용일", "종료일",
    "고시번호", "분류파일탭", "기준일", "비고",
]
HIRA_LOG_HEADERS = ["반영일시", "담당사번", "기준일", "등록건수", "신규", "변경", "제외", "출처파일"]
HIRA_TYPES = ("급여", "비급여", "정액수가", "분류")
HIRA_DATE = "2026-11-01"  # 최초 2026-11-01 파일 기준. 다음 달은 파일명에서 추출.


def hira_drive_session():
    # 기존 Google 서비스 계정의 새 자격 증명으로 Drive 읽기 전용 스코프만 사용.
    credentials = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return AuthorizedSession(credentials)


def drive_hira_files(folder_id: str):
    session = hira_drive_session()
    items, page_token = [], None
    while True:
        params = {
            "q": "'" + folder_id + "' in parents and trashed = false",
            "fields": "nextPageToken,files(id,name,mimeType,size,modifiedTime)",
            "page_size": 1000,
            "supportsAllDrives": "true", "includeItemsFromAllDrives": "true",
        }
        if page_token:
            params["page_token"] = page_token
        response = session.get("https://www.googleapis.com/drive/v3/files", params=params, timeout=45)
        response.raise_for_status()
        payload = response.json()
        items.extend(payload.get("files", []))
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
    return items


def detect_hira_files(files):
    matched = defaultdict(list)
    for f in files:
        name = f["name"]
        if not name.lower().endswith(".xlsx"):
            continue
        if "MEDICAL_DEVICE_PRICE_LIST" in name.upper():
            kind = "분류"
        elif "비급여" in name:
            kind = "비급여"
        elif "정액수가" in name:
            kind = "정액수가"
        elif "급여" in name:
            kind = "급여"
        else:
            continue
        matched[kind].append(f)
    wrong = [k for k in HIRA_TYPES if len(matched[k]) != 1]
    if wrong:
        raise ValueError("원본 4개가 종류별로 정확히 1개씩 필요합니다. 확인할 파일: " + ", ".join(
            f"{k}({len(matched[k])}개)" for k in wrong))
    return {k: matched[k][0] for k in HIRA_TYPES}


def download_hira_bytes(file_id: str):
    session = hira_drive_session()
    response = session.get(
        f"https://www.googleapis.com/drive/v3/files/{file_id}",
        params={"alt": "media", "supportsAllDrives": "true"}, timeout=180,
    )
    response.raise_for_status()
    if len(response.content) > 50 * 1024 * 1024:
        raise ValueError("원본 파일이 50MB를 초과합니다. 관리자에게 문의해 주세요.")
    return response.content


def tidy_cell(value):
    if pd.isna(value):
        return ""
    return str(value).strip().replace("\x00", "")


def normalize_date(value):
    if pd.isna(value) or str(value).strip() in ("", "-", "9999-12-31"):
        return "" if str(value).strip() != "9999-12-31" else "9999-12-31"
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    # Excel may mix serial dates, ISO strings and strings including 00:00:00.
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}(?: 00:00:00)?", text):
        return text[:10]
    if re.fullmatch(r"\d{5}(?:\.0+)?", text):
        try:
            return (datetime(1899, 12, 30) + timedelta(days=int(float(text)))).strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            pass
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return text


def hira_extract_date(files):
    # 기준일이 발견되지 않으면 사용자에게 보여줄 기본값을 사용한다.
    candidates = []
    for item in files.values():
        m = re.search(r"(20\d{2})[._-](\d{1,2})[._-](\d{1,2})", item["name"])
        if m:
            try:
                candidates.append(datetime(int(m[1]), int(m[2]), int(m[3])).strftime("%Y-%m-%d"))
            except ValueError:
                pass
    if candidates and len(set(candidates)) != 1:
        raise ValueError("원본 파일들의 기준일이 서로 다릅니다. 같은 월 자료 4개를 올려주세요.")
    return candidates[0] if candidates else HIRA_DATE


def state_of_sheet(title):
    if "급여중지" in title:
        return "급여중지/예정"
    if "삭제" in title:
        return "삭제/예정"
    return "등재"


def row_from_record(raw, category, state, snapshot):
    code = tidy_cell(raw.get("코드", "")).upper()
    if not code or not re.fullmatch(r"[A-Z0-9][A-Z0-9_-]{4,19}", code):
        return None
    note = tidy_cell(raw.get("비고", ""))
    data = {
        "코드": code, "급여구분": category, "현재상태": state,
        "중분류": tidy_cell(raw.get("중분류", "")),
        "중분류코드": tidy_cell(raw.get("중분류코드", "")),
        "품명": tidy_cell(raw.get("품명", "")), "규격": tidy_cell(raw.get("규격", "")),
        "단위": tidy_cell(raw.get("단위", "")), "상한금액": tidy_cell(raw.get("상한금액", "")),
        "제조회사": tidy_cell(raw.get("제조회사", "")),
        "제조수입업소": tidy_cell(raw.get("제조·수입업소", raw.get("수입(판매)업소", ""))),
        "최초등재일": normalize_date(raw.get("최초등재일", "")),
        "적용일": normalize_date(raw.get("적용일자", "")),
        "종료일": normalize_date(raw.get("종료일자", "")),
        "고시번호": tidy_cell(raw.get("고시번호", "")),
        "분류파일탭": "", "기준일": snapshot, "비고": note,
    }
    return data


def parse_hira_documents(file_bytes, filenames, snapshot):
    """Three authoritative datasets + group classification from price-list.

    Source status records are kept, with explicitly documented priority. Repeated
    code across sources is never silently counted as separate unique items.
    """
    records, duplicates, source_counts = {}, [], Counter()
    conflicts = []
    # For the current catalog only: prefer a listed item to a historical deletion.
    # A shared code can refer to different product descriptions. Keep BOTH source
    # records in the conflict review, never silently discard that history.
    priority = {"등재": 3, "급여중지/예정": 2, "삭제/예정": 1}
    for category in HIRA_TYPES[:3]:
        with pd.ExcelFile(BytesIO(file_bytes[category]), engine="openpyxl") as xls:
            for sheet_name in xls.sheet_names:
                frame = pd.read_excel(xls, sheet_name=sheet_name, dtype=str, keep_default_na=False)
                frame.columns = [str(x).strip() for x in frame.columns]
                if "코드" not in frame.columns:
                    continue
                state = state_of_sheet(sheet_name)
                for raw in frame.to_dict(orient="records"):
                    record = row_from_record(raw, category, state, snapshot)
                    if not record:
                        continue
                    code = record["코드"]
                    source_counts[category] += 1
                    if code in records:
                        old = records[code]
                        duplicates.append((code, old["급여구분"], category, old["현재상태"], state))
                        conflicts.append({
                            "코드": code,
                            "첫품명": old["품명"], "첫상태": old["현재상태"],
                            "첫적용일": old["적용일"], "첫종료일": old["종료일"],
                            "다음품명": record["품명"], "다음상태": state,
                            "다음적용일": record["적용일"], "다음종료일": record["종료일"],
                            "내용상이": "Y" if old["품명"] != record["품명"] else "N",
                        })
                        if priority[state] > priority[old["현재상태"]]:
                            records[code] = record
                    else:
                        records[code] = record
    # The classification workbook contains duplicates of item lists and group
    # headings. We enrich by exact code; never append it as a second item feed.
    classified = {}
    with pd.ExcelFile(BytesIO(file_bytes["분류"]), engine="openpyxl") as xls:
        for sheet_name in xls.sheet_names:
            if sheet_name == "별도산정불가":
                continue   # no stable reimbursement code in this worksheet
            frame = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str, keep_default_na=False)
            # A column generally contains codes; non-code heading rows ignored.
            for col_value in frame.iloc[:, 0].tolist():
                code = str(col_value).strip().upper()
                if code in records and code not in classified:
                    classified[code] = sheet_name
    for code, group in classified.items():
        records[code]["분류파일탭"] = group
    data = [records[key] for key in sorted(records)]
    if not data:
        raise ValueError("품목코드가 추출되지 않았습니다. 원본 엑셀의 헤더를 확인하세요.")
    overview = {
        "원본품목행": sum(source_counts.values()), "고유코드": len(data),
        "중복코드발견": len(duplicates), "분류연결": len(classified),
        "구분별": dict(Counter(v["급여구분"] for v in data)),
        "상태별": dict(Counter(v["현재상태"] for v in data)),
        "중복예시": duplicates[:25], "충돌상세": conflicts,
        "파일명": filenames, "기준일": snapshot,
    }
    return data, overview


def load_existing_hira():
    try:
        ws = get_ws("HIRA_Items")
    except WorksheetNotFound:
        return {}
    rows = api_action(ws.get_all_values)
    if not rows:
        return {}
    if rows[0][:len(HIRA_HEADERS)] != HIRA_HEADERS:
        raise ValueError("기존 HIRA_Items의 열 구조가 다릅니다. 자동 덮어쓰기를 중단합니다.")
    return {r[0]: tuple((r + [""]*len(HIRA_HEADERS))[:len(HIRA_HEADERS)]) for r in rows[1:] if r and r[0]}


def diff_hira(rows, existing):
    # Compare excluding snapshot date / status-derived import metadata to avoid
    # treating every unchanged item as modified each month.
    column_idxs = [i for i, h in enumerate(HIRA_HEADERS) if h != "기준일"]
    current = {r["코드"]: tuple(str(r[h]) for h in HIRA_HEADERS) for r in rows}
    added = sorted(set(current) - set(existing))
    removed = sorted(set(existing) - set(current))
    changed = sorted(k for k in set(existing) & set(current)
                     if tuple(current[k][i] for i in column_idxs) != tuple(existing[k][i] for i in column_idxs))
    return {"신규": added, "변경": changed, "기준목록제외": removed}


def hira_import_api(fn, *args, **kwargs):
    """Exponential cooldown for Sheets API read/write quota (safe for idempotent calls only)."""
    delays = (0, 20, 40, 65, 90)
    for attempt, delay in enumerate(delays):
        if delay:
            time.sleep(delay)
        try:
            return fn(*args, **kwargs)
        except APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            detail = str(exc).lower()
            retry = status in (429, 500, 502, 503, 504) or (status == 403 and ("quota" in detail or "ratelimit" in detail))
            LOGGER.warning("HIRA staged import request failed: HTTP %s, retry=%s, attempt=%s", status, retry, attempt+1)
            if not retry or attempt == len(delays)-1:
                raise


def hira_expected_row(row):
    return [str(row.get(col, "")) for col in HIRA_HEADERS]


def hira_normalize_cells(cells):
    return (list(map(str, cells)) + [""] * len(HIRA_HEADERS))[:len(HIRA_HEADERS)]


def hira_resume_stage(book, rows):
    """Reuse a prior incomplete stage only when its full code column and sampled data match."""
    try:
        all_sheets = hira_import_api(book.worksheets)
    except Exception:
        LOGGER.exception("Cannot enumerate HIRA stage sheets")
        raise
    candidates = sorted((ws for ws in all_sheets if ws.title.startswith("HIRA_Stage_")),
                        key=lambda ws: ws.title, reverse=True)
    expected_codes = [str(r["코드"]) for r in rows]
    for ws in candidates[:8]:
        try:
            # A single column read locates the precise safe resume position.
            code_cells = hira_import_api(ws.get, f"A1:A{len(rows)+1}")
            codes = [str(row[0]) if row else "" for row in code_cells]
            if not codes or codes[0] != HIRA_HEADERS[0]:
                continue
            data_codes = codes[1:]
            if len(data_codes) > len(expected_codes):
                continue
            first_mismatch = next((i for i, (a,b) in enumerate(zip(data_codes, expected_codes)) if a != b), None)
            if first_mismatch is not None:
                # Existing stage is from a different snapshot or damaged; never overwrite it.
                continue
            # Verify the already-written rows against current source content at spaced checkpoints.
            count = len(data_codes)
            if count:
                sample = sorted({0, count//4, count//2, (3*count)//4, count-1})
                if any(hira_normalize_cells((hira_import_api(ws.get, f"A{idx+2}:R{idx+2}") or [[]])[0]) != hira_expected_row(rows[idx])
                       for idx in sample):
                    continue
            return ws, count
        except APIError:
            raise
        except Exception:
            LOGGER.exception("Invalid reusable HIRA stage: %s", ws.title)
    return None, 0


def publish_hira(rows, overview, changes):
    """Resume incomplete import, rate limit uploads, then publish atomically by sheet rename."""
    book = get_book()
    if len(rows) > 120000:
        raise ValueError("파일이 예상 크기를 벗어나 자동 반영을 중단했습니다.")
    if len({str(r["코드"]) for r in rows}) != len(rows):
        raise ValueError("통합 목록 내 중복 품목코드가 존재해 반영을 중단했습니다.")
    # Google Sheets rejects cells containing over 50,000 characters.
    for idx, row in enumerate(rows):
        if any(len(str(row.get(h, ""))) > 49000 for h in HIRA_HEADERS):
            raise ValueError(f"{idx+1}번째 품목의 텍스트가 Google Sheets 셀 한도를 초과했습니다.")
    stage, resume_count = hira_resume_stage(book, rows)
    if stage is None:
        stage_name = "HIRA_Stage_" + datetime.now(KST).strftime("%m%d_%H%M%S")
        stage = hira_import_api(book.add_worksheet, title=stage_name, rows=len(rows)+1, cols=len(HIRA_HEADERS))
        hira_import_api(stage.update, range_name="A1:R1", values=[HIRA_HEADERS], value_input_option="RAW")
    else:
        st.info(f"이전 저장 임시시트에서 {resume_count:,}건을 확인했습니다. 나머지만 이어서 저장합니다.")
    bar = st.progress(resume_count / max(len(rows), 1), text=f"저장 중: {resume_count:,} / {len(rows):,}건")
    # 750 rows per call: ~67 write calls for 50k rows. Slow enough to stay below
    # the Sheets 60-writes-per-minute service-account quota including other app traffic.
    batch_size = 750
    current = resume_count
    while current < len(rows):
        block = [hira_expected_row(row) for row in rows[current:current+batch_size]]
        start = current + 2
        end = current + len(block) + 1
        try:
            hira_import_api(stage.update, range_name=f"A{start}:R{end}", values=block, value_input_option="RAW")
        except APIError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            raise RuntimeError(f"임시시트 {stage.title}에서 {current:,}건까지 저장됨; Google Sheets HTTP {status}. 다음 실행 시 이어서 저장할 수 있습니다.") from exc
        current += len(block)
        bar.progress(current/len(rows), text=f"저장 중: {current:,} / {len(rows):,}건")
        if current < len(rows):
            time.sleep(1.6)
    # Verify actual populated codes rather than preallocated row_count.
    code_cells = hira_import_api(stage.get, f"A1:A{len(rows)+1}")
    actual_codes = [str(r[0]) if r else "" for r in code_cells]
    expected_codes = ["코드"] + [str(r["코드"]) for r in rows]
    if actual_codes != expected_codes:
        raise ValueError("임시시트 저장 후 코드 검증이 실패했습니다. 기존 운영 DB는 유지됩니다.")
    # Compare selected complete rows to catch accidental truncation or column shifting.
    for idx in sorted({0, len(rows)//4, len(rows)//2, 3*len(rows)//4, len(rows)-1}):
        actual = hira_normalize_cells((hira_import_api(stage.get, f"A{idx+2}:R{idx+2}") or [[]])[0])
        if actual != hira_expected_row(rows[idx]):
            raise ValueError(f"{idx+1}번째 행 데이터 검증에 실패했습니다. 기존 운영 DB는 유지됩니다.")
    old = None
    try:
        old = hira_import_api(book.worksheet, "HIRA_Items")
    except WorksheetNotFound:
        pass
    backup = None
    if old:
        backup = "HIRA_Backup_" + datetime.now(KST).strftime("%m%d_%H%M%S")
        hira_import_api(old.update_title, backup)
    try:
        hira_import_api(stage.update_title, "HIRA_Items")
        # Worksheet ID changes when the stage sheet replaces the active catalog.
        existing_ws.clear()
    except Exception:
        if old and backup:
            try:
                hira_import_api(old.update_title, "HIRA_Items")
            except Exception:
                LOGGER.exception("Failed to restore original HIRA_Items")
        raise
    try:
        conflict_rows = overview.get("충돌상세", [])
        if conflict_rows:
            headers = ["기준일"] + list(conflict_rows[0].keys())
            ws = get_ws("HIRA_Conflicts", headers)
            old_rows = hira_import_api(ws.get_all_values)
            existing = {(r[0], r[1]) for r in old_rows[1:] if len(r)>=2}
            for row in conflict_rows:
                if (overview["기준일"], row["코드"]) not in existing:
                    ws.append_row([overview["기준일"]] + [str(row[k]) for k in conflict_rows[0]], value_input_option="RAW")
    except Exception:
        LOGGER.exception("HIRA published but collision archive failed")
    try:
        log_ws = get_ws("HIRA_Imports", HIRA_LOG_HEADERS)
        log_ws.append_row([now(), st.session_state.get("employee_id", ""),
                 overview["기준일"], str(len(rows)), str(len(changes["신규"])),
                 str(len(changes["변경"])), str(len(changes["기준목록제외"])),
                 " | ".join(overview["파일명"].values())], value_input_option="RAW")
    except Exception:
        LOGGER.exception("HIRA published but history append failed")
    bar.empty()
    st.cache_data.clear()


def render_hira_import():
    st.subheader("심평원 DB 업데이트")
    st.caption("Google Drive 원본파일(급여·비급여·정액수가·분류) 확인 → 비교 → 관리자 승인")
    st.warning("최초 대량 등록은 Google Sheets 호출량이 많습니다. 진행 중에는 버튼을 다시 누르지 마세요.")
    folder_id = st.secrets["treatments"].get("drive_folder_id", "").strip()
    if not folder_id:
        st.error("Streamlit Secrets의 [treatments]에 drive_folder_id 설정이 없습니다.")
        return
    if st.button("① Google Drive 파일 확인", use_container_width=False):
        try:
            files = detect_hira_files(drive_hira_files(folder_id))
            st.session_state["hira_files"] = files
            st.session_state.pop("hira_import_preview", None)
        except Exception as exc:
            LOGGER.exception("Drive list failed")
            st.error(f"Drive 원본 조회 실패: {type(exc).__name__}. 403이면 Drive API 활성화/공유권한을 확인하세요.")
    files = st.session_state.get("hira_files")
    if not files:
        return
    st.dataframe(pd.DataFrame([{"유형":k,"파일명":v["name"],"크기(KB)":round(int(v.get("size") or 0)/1024)}
                               for k,v in files.items()]), hide_index=True, use_container_width=True)
    if st.button("② 원본 분석 및 변경사항 비교", type="primary"):
        try:
            with st.spinner("4개 엑셀을 다운로드·통합·비교하는 중입니다..."):
                by_kind = {k: download_hira_bytes(v["id"]) for k,v in files.items()}
                date = hira_extract_date(files)
                rows, overview = parse_hira_documents(by_kind, {k:v["name"] for k,v in files.items()}, date)
                existing = load_existing_hira()
                changes = diff_hira(rows, existing)
            st.session_state["hira_import_preview"] = (rows, overview, changes)
        except Exception as exc:
            LOGGER.exception("HIRA parse/compare failed")
            st.error(f"분석 실패: {type(exc).__name__}: {str(exc)[:250] if isinstance(exc, ValueError) else 'Streamlit 로그를 확인하세요.'}")
    preview = st.session_state.get("hira_import_preview")
    if not preview:
        return
    rows, overview, changes = preview
    a,b,c,d=st.columns(4)
    a.metric("통합 고유 품목", f"{len(rows):,}")
    b.metric("신규", f"{len(changes['신규']):,}")
    c.metric("변경", f"{len(changes['변경']):,}")
    d.metric("기준목록 제외", f"{len(changes['기준목록제외']):,}")
    st.caption(f"기준일 {overview['기준일']} · 원본 품목행 {overview['원본품목행']:,} · 중복코드 {overview['중복코드발견']:,} · 분류연결 {overview['분류연결']:,}")
    if overview["중복코드발견"]:
        st.warning("동일 코드가 다른 품명이나 상태로 여러 시트에 존재합니다. 현재 기준목록에는 등재를 우선한 1개 행만 저장하지만, 삭제·과거이력의 품명은 다를 수 있으므로 아래 내용을 반드시 검토하세요.")
        conflict_rows = overview.get("충돌상세", [])
        if conflict_rows:
            st.dataframe(pd.DataFrame(conflict_rows), hide_index=True, use_container_width=True)
            st.download_button(
                "중복코드 원본 비교 CSV 다운로드",
                data=pd.DataFrame(conflict_rows).to_csv(index=False).encode("utf-8-sig"),
                file_name="HIRA_중복코드_검토.csv", mime="text/csv",
            )
    st.dataframe(pd.DataFrame(rows[:30], columns=HIRA_HEADERS), hide_index=True, use_container_width=True)
    st.caption("'기준목록 제외'는 삭제 확정이 아니라 이번 원본에서 미검출된 코드입니다. 병원 사용품목과 재고기록은 수정하지 않습니다.")
    confirmed = st.checkbox("변경사항을 확인했고, 심평원 기준품목 DB만 교체하는 데 동의합니다.")
    reviewed = True
    if overview["중복코드발견"]:
        reviewed = st.checkbox("동일 코드의 서로 다른 품명·상태를 확인했으며, 등재 품목을 현재목록으로 사용하고 중복 이력은 별도로 보관하는 데 동의합니다.")
    if st.button("③ HIRA_Items에 최종 반영", type="primary", disabled=not (confirmed and reviewed)):
        try:
            publish_hira(rows, overview, changes)
            st.session_state.pop("hira_import_preview", None)
            st.success("심평원 품목 DB 반영을 완료했습니다. 기존 Emp·Auth·재고정보는 변경하지 않았습니다.")
        except Exception as exc:
            LOGGER.exception("HIRA publish failed")
            st.error(f"반영 실패: {str(exc)[:350] if isinstance(exc, (ValueError, RuntimeError)) else type(exc).__name__}. 기존 품목 DB는 그대로 유지됩니다. 임시 시트를 삭제하지 마세요.")


@st.cache_data(ttl=240, show_spinner=False)
def hira_search_cached(sheet_id: str):
    try:
        raw = api_action(get_ws("HIRA_Items").get_all_values)
    except WorksheetNotFound:
        return pd.DataFrame(columns=HIRA_HEADERS)
    if not raw or raw[0][:len(HIRA_HEADERS)] != HIRA_HEADERS:
        return pd.DataFrame(columns=HIRA_HEADERS)
    data = [(r + [""]*len(HIRA_HEADERS))[:len(HIRA_HEADERS)] for r in raw[1:] if r and r[0]]
    return pd.DataFrame(data, columns=HIRA_HEADERS)


def render_hira_search():
    st.subheader("심평원 치료재료 품목검색")
    st.caption("심평원 기준정보 검색 · 물품 신청은 왼쪽 ‘물품 신청’ 메뉴에서 진행합니다.")
    try:
        frame = combined_catalog()
    except Exception as exc:
        st.error(safe_spreadsheet_error(exc))
        return
    if frame.empty:
        st.info("아직 품목 DB가 없습니다. MASTER가 '심평원 DB 업데이트' 메뉴에서 먼저 등록해 주세요.")
        return
    a,b,c=st.columns([3,1,1])
    with a:q=st.text_input("품목 검색", placeholder="코드 · 품명 · 중분류 · 제조사")
    with b:kind=st.selectbox("급여구분", ["전체"]+sorted(frame["급여구분"].unique().tolist()))
    with c:state=st.selectbox("품목상태", ["전체"]+sorted(frame["현재상태"].unique().tolist()))
    result=frame
    if kind!="전체":result=result[result["급여구분"]==kind]
    if state!="전체":result=result[result["현재상태"]==state]
    if q.strip():
        pattern=re.escape(q.strip())
        mask = pd.Series(False, index=result.index)
        for col in ["코드","품명","중분류","제조회사","제조수입업소"]:
            mask = mask | result[col].str.contains(pattern, case=False, na=False, regex=True)
        result=result[mask]
    st.caption(f"검색 결과: {len(result):,}건 / 전체 {len(frame):,}건")
    display = result.head(2000).copy()
    if '단위' in display:
        display['단위'] = display['단위'].map(display_unit)
    order = [c for c in DETAIL_FIELDS if c in display.columns]
    display = display[order + [c for c in display.columns if c not in order]]
    st.dataframe(display, hide_index=True, use_container_width=True, height=620)
    if len(result)>2000:
        st.caption("화면에는 처음 2,000건만 표시합니다. 검색 조건을 좁혀 주세요.")




# Hospital-only items: never overwrite the HIRA master catalog.
HOSPITAL_HEADERS = ["등록요청ID", "품목코드", "품명", "규격", "중분류", "기준단위", "신청단위", "단위환산수량", "제조수입업소", "상한금액", "비고", "상태", "요청사번", "요청일시", "승인사번", "승인일시"]


@st.cache_data(ttl=20, show_spinner=False)
def hospital_item_rows(_sheet_id):
    try:
        return rows_as_dicts(get_ws('Hospital_Items', HOSPITAL_HEADERS), HOSPITAL_HEADERS)
    except WorksheetNotFound:
        return []


def hospital_items_fresh():
    return rows_as_dicts(get_ws('Hospital_Items', HOSPITAL_HEADERS), HOSPITAL_HEADERS)


def hospital_catalog():
    approved = [r for r in hospital_item_rows(str(st.secrets['treatments']['spreadsheet_id'])) if r['상태'] == '승인' and r['품목코드']]
    records = []
    for r in approved:
        records.append({
            '코드': r['품목코드'], '급여구분': '병원자체', '현재상태': '등재',
            '중분류': r['중분류'] or '병원 자체품목', '중분류코드': '',
            '품명': r['품명'], '규격': r['규격'], '단위': r['기준단위'],
            '상한금액': r['상한금액'], '제조회사': r['제조수입업소'],
            '제조수입업소': r['제조수입업소'], '_병원신청단위': r['신청단위'],
            '_병원환산수량': r['단위환산수량'],
        })
    return pd.DataFrame(records)


def combined_catalog():
    hra = hira_search_cached(str(st.secrets['treatments']['spreadsheet_id']))
    hosp = hospital_catalog()
    if hosp.empty:
        return hra
    return pd.concat([hra, hosp], ignore_index=True).fillna('')


def hospital_duplicate_warning(name, spec, hira, own):
    def norm(v): return re.sub(r'[^a-z0-9가-힣]', '', str(v).casefold())
    query = norm(name)
    if not query:
        return []
    matches = []
    for df in (hira, own):
        if df.empty: continue
        for _, row in df.iterrows():
            title = norm(row.get('품명',''))
            if title and (title == query or (len(query) >= 5 and (query in title or title in query))):
                matches.append((row.get('코드',''), row.get('품명',''), row.get('규격','')))
                if len(matches) >= 12: return matches
    return matches


def render_hospital_register(emp, data):
    st.subheader('병원 자체품목 등록')
    st.caption('산정불가 물품, 장갑, 소모품 등 심평원 코드가 없는 품목을 관리합니다. 먼저 심평원 및 병원 품목을 검색해야 등록 요청할 수 있습니다.')
    catalog = combined_catalog()
    query = st.text_input('① 필수 사전검색 — 품명·코드·규격', key='hosp_search', placeholder='예: Latex glove / 라텍스 장갑')
    if not query.strip():
        st.info('품목명 또는 규격을 검색한 후 등록 요청을 진행해 주세요.')
        return
    tokens = [x.casefold() for x in query.split()]
    if not catalog.empty:
        cols = [c for c in DETAIL_FIELDS if c in catalog.columns]
        masked = catalog[cols].fillna('').astype(str).agg(' '.join,axis=1).str.casefold()
        found = catalog.loc[masked.map(lambda x: all(t in x for t in tokens))].head(70)
        if not found.empty:
            st.warning(f'기존 품목이 {len(found)}건 검색되었습니다. 중복 등록을 피하고 기존 품목을 먼저 확인하세요.')
            st.dataframe(found[cols], use_container_width=True, hide_index=True, height=240)
        else:
            st.success('입력한 검색어와 일치하는 품목을 찾지 못했습니다. 유사한 명칭도 검색해 보세요.')
    is_master = emp['권한'].upper() == 'MASTER' and not test_context()
    with st.form('hospital_register_form'):
        st.markdown('**② 신규 품목 등록 요청**' + (' (MASTER 직접 승인 가능)' if is_master else ''))
        a,b = st.columns(2)
        name = a.text_input('품명 *', value=query[:120], max_chars=150)
        spec = b.text_input('규격 *', max_chars=150)
        a,b,c=st.columns(3)
        category=a.text_input('중분류',value='병원 일반소모품',max_chars=100)
        base=b.selectbox('재고 기준단위 *',['EA','BOX','PACK','SET','KIT','UNIT','PAIR','ROLL','BOTTLE','기타'])
        real_base=c.text_input('기타 기준단위',max_chars=24) if base=='기타' else ''
        a,b,c=st.columns(3)
        req_unit=a.selectbox('기본 신청단위',['EA','BOX','PACK','SET','KIT','UNIT','PAIR','ROLL','BOTTLE','기타'])
        req_custom=b.text_input('기타 신청단위',max_chars=24) if req_unit=='기타' else ''
        factor=c.number_input('신청단위 1개당 기준수량',min_value=1,max_value=1000000,value=1,step=1)
        manufacturer=st.text_input('제조·수입업소 (선택)',max_chars=150)
        memo=st.text_input('비고·등록사유',max_chars=300)
        checked=st.checkbox('기존 품목을 검색했고 같은 제품이 등록되지 않았음을 확인했습니다.')
        submitted=st.form_submit_button('병원 자체품목 등록' if is_master else 'MASTER에게 신규등록 요청', type='primary')
    if submitted:
        try:
            name=name.strip();spec=spec.strip()
            base = (real_base if base=='기타' else base).strip().upper()
            req_unit = (req_custom if req_unit=='기타' else req_unit).strip().upper()
            validate_request_unit(req_unit, factor, base)
            if not checked or len(name)<2 or not spec or not base:
                raise ValueError('품명·규격·기준단위·기존품목 검색확인을 모두 입력해 주세요.')
            fresh_data=employees_fresh()
            fresh_emp=next((x for x in fresh_data if x['사번']==emp['사번'] and is_active(x)),None)
            if fresh_emp is None:
                raise PermissionError('사용자 정보가 변경되었습니다. 다시 로그인해 주세요.')
            # Recheck latest registry plus catalog immediately before registration.
            existing=hospital_items_fresh()
            norm=lambda t:re.sub(r'[^a-z0-9가-힣]','',str(t).casefold())
            if any(norm(r['품명'])==norm(name) and norm(r['규격'])==norm(spec) and r['상태']!='반려' for r in existing):
                raise ValueError('같은 품명·규격의 자체품목이 이미 등록 또는 승인대기 중입니다.')
            hra=get_hira_items()
            exact=hospital_duplicate_warning(name,spec,hra,pd.DataFrame())
            if any(norm(t)==norm(name) and norm(sp)==norm(spec) for _,t,sp in exact):
                raise ValueError('심평원에 동일한 품명·규격이 있습니다. 기존 품목을 신청해 주세요.')
            code=''; status='요청';approver=''; approved_at=''
            if is_master:
                used={r['품목코드'] for r in existing if r['품목코드']}
                used.update(set(hra['코드'].astype(str)))
                number=max([int(c[1:]) for c in used if re.fullmatch(r'H\d{6}',c)] or [0])+1
                code=f'H{number:06d}'
                status='승인';approver=emp['사번'];approved_at=now()
            record=[uuid.uuid4().hex,code,name,spec,category,base,req_unit,str(int(factor)),manufacturer,'',test_mark(memo),status,emp['사번'],now(),approver,approved_at]
            # Never retry append writes automatically; an unknown server outcome may create duplicates.
            get_ws('Hospital_Items',HOSPITAL_HEADERS).append_row(record,value_input_option='RAW')
            hospital_item_rows.clear()
            if is_master: st.success(f'품목 등록 완료: {code} · 물품신청에서 검색할 수 있습니다.')
            else: st.success('신규등록 요청을 저장했습니다. MASTER 승인 후 물품 신청이 가능합니다.')
            test_audit('HOSPITAL_ITEM',f'{name}/{spec} 상태:{status} 코드:{code}')
        except Exception as exc:
            st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))
    if is_master:
        st.divider();st.markdown('#### 승인 대기 / 등록된 병원 품목')
        pending=[r for r in hospital_item_rows(str(st.secrets['treatments']['spreadsheet_id'])) if r['상태']=='요청']
        if pending:
            pending_df=pd.DataFrame(pending)
            st.dataframe(pending_df[['요청일시','품명','규격','기준단위','신청단위','단위환산수량','요청사번']],hide_index=True,use_container_width=True)
            opts={r['등록요청ID']:f"{r['품명']} / {r['규격']} ({r['요청사번']})" for r in pending}
            request_id=st.selectbox('처리할 등록요청',list(opts),format_func=lambda x:opts[x])
            approve, reject=st.columns(2)
            def change_status(new_status):
                ws=get_ws('Hospital_Items',HOSPITAL_HEADERS)
                fresh=hospital_items_fresh()
                target=next((r for r in fresh if r['등록요청ID']==request_id),None)
                if not target or target['상태']!='요청':
                    raise ValueError('이미 처리되었거나 삭제된 요청입니다. 동기화해 주세요.')
                valid=next((x for x in employees_fresh() if x['사번']==emp['사번'] and x['권한'].upper()=='MASTER' and is_active(x)),None)
                if valid is None: raise PermissionError('MASTER 권한이 없습니다.')
                code=''
                if new_status=='승인':
                    hira=get_hira_items()
                    conflicts=hospital_duplicate_warning(target['품명'],target['규격'],hira,pd.DataFrame())
                    norm=lambda t:re.sub(r'[^a-z0-9가-힣]','',str(t).casefold())
                    if any(norm(t)==norm(target['품명']) and norm(sp)==norm(target['규격']) for _,t,sp in conflicts):
                        raise ValueError('심평원 동일 품목이 확인됩니다. 승인 전에 검토하세요.')
                    used={r['품목코드'] for r in fresh if r['품목코드']}
                    used.update(set(hira['코드'].astype(str)))
                    number=max([int(c[1:]) for c in used if re.fullmatch(r'H\d{6}',c)] or [0])+1
                    code=f'H{number:06d}'
                # Apply code and approval status in a single Sheets batch update.
                ops=[{'range':f'L{target["_row"]}:P{target["_row"]}',
                      'values':[[new_status,target['요청사번'],target['요청일시'],emp['사번'],now()]]}]
                if code:
                    ops.insert(0,{'range':f'B{target["_row"]}', 'values':[[code]]})
                ws.batch_update(ops, value_input_option='RAW')
                hospital_item_rows.clear()
                test_audit('HOSPITAL_ITEM_APPROVAL',f"{request_id}: {new_status}, {code}")
            with approve:
                if st.button('선택 품목 승인',type='primary',use_container_width=True):
                    try: change_status('승인');st.success('승인되었습니다.');st.rerun()
                    except Exception as exc: st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))
            with reject:
                if st.button('선택 품목 반려',use_container_width=True):
                    try: change_status('반려');st.success('반려 처리했습니다.');st.rerun()
                    except Exception as exc: st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))
        else:
            st.info('신규등록 승인 대기 품목이 없습니다.')
        approved=[r for r in hospital_item_rows(str(st.secrets['treatments']['spreadsheet_id'])) if r['상태']=='승인']
        if approved:
            st.dataframe(pd.DataFrame(approved)[['품목코드','품명','규격','중분류','기준단위','신청단위','단위환산수량']],hide_index=True,use_container_width=True,height=220)


# ================================================================
# 치료재료 신청 -> 출고 -> 수령 및 부서별 수동 재고
# 초기사용 시 필요한 시트만 만들며 기존 Emp/Auth/HIRA_Items는 보존합니다.
# ================================================================
REQ_HEADERS_BASE = ["요청ID", "신청일시", "신청사번", "신청자", "수령부서", "품목코드", "품명", "규격", "신청수량", "상태", "출고수량", "출고일시", "출고담당", "수령일시", "수령담당", "비고"]
# 기존 A:P 칼럼의 순서는 절대 변경하지 않습니다. 신규 정보만 Q:W에 추가합니다.
REQ_HEADERS_EXTRA = ["중분류", "심평원단위", "상한금액", "제조수입업소", "신청단위", "단위환산수량", "재고기준단위"]
REQ_HEADERS = REQ_HEADERS_BASE + REQ_HEADERS_EXTRA
REQUEST_UNIT_CHOICES = ["EA", "BOX", "PACK", "SET", "KIT", "UNIT", "ROLL", "BAG", "BOTTLE", "PAIR", "기타(직접입력)"]


def validate_request_unit(unit, factor, official_unit):
    unit = str(unit or "").strip().upper()
    official_unit = str(official_unit or "").strip().upper()
    if not unit or len(unit) > 24 or not re.fullmatch(r"[A-Z0-9가-힣_./() -]+", unit):
        raise ValueError("신청단위를 24자 이내로 정확하게 입력해 주세요.")
    factor = material_qty(factor)
    if not official_unit and factor != 1:
        raise ValueError("심평원 기준단위가 비어 있습니다. 관리자에게 기준단위 확인을 요청해 주세요.")
    return unit, factor


def request_conversion(row):
    """Legacy requests have no saved factor: they use their historic 1:1 behavior."""
    factor = str(row.get("단위환산수량", "")).strip()
    if not factor:
        return 1
    return material_qty(factor)


def request_unit_label(row):
    return str(row.get("신청단위", "")).strip() or str(row.get("심평원단위", "")).strip() or "개"


@st.cache_resource(show_spinner=False)
def ensure_request_schema():
    """One-time backwards-compatible migration of the existing request tab."""
    ws = get_ws("Material_Requests", REQ_HEADERS)
    headers = api_action(ws.row_values, 1)
    if headers[:len(REQ_HEADERS_BASE)] != REQ_HEADERS_BASE:
        raise ValueError("Material_Requests의 기존 A:P 제목행이 예상 구조와 다릅니다. 자동 변경하지 않았습니다.")
    if headers[len(REQ_HEADERS_BASE):len(REQ_HEADERS)] == REQ_HEADERS_EXTRA:
        return True
    if any(str(v).strip() for v in headers[len(REQ_HEADERS_BASE):]):
        raise ValueError("Material_Requests의 Q열 이후에 다른 항목이 있습니다. 덮어쓰지 않고 중단했습니다.")
    if ws.col_count < len(REQ_HEADERS):
        api_action(ws.resize, cols=len(REQ_HEADERS))
    api_action(ws.update, range_name="Q1:W1", values=[REQ_HEADERS_EXTRA], value_input_option="RAW")
    return True

STOCK_HEADERS_BASE = ["거래ID", "일시", "부서", "품목코드", "품명", "유형", "증감수량", "작업사번", "메모", "관련요청ID"]
STOCK_HEADERS_EXTRA = ["입력BOX수량", "입력낱개수량", "BOX당기준수량", "입력방식"]
STOCK_HEADERS = STOCK_HEADERS_BASE + STOCK_HEADERS_EXTRA


@st.cache_resource(show_spinner=False)
def ensure_stock_schema():
    """Append-only upgrade for earlier stock transaction records (A:J preserved)."""
    ws = get_ws('Material_StockEvents', STOCK_HEADERS)
    header = api_action(ws.row_values, 1)
    if header[:len(STOCK_HEADERS_BASE)] != STOCK_HEADERS_BASE:
        raise ValueError('Material_StockEvents의 기존 제목행이 예상 구조와 다릅니다. 수정을 중단했습니다.')
    if header[len(STOCK_HEADERS_BASE):len(STOCK_HEADERS)] == STOCK_HEADERS_EXTRA:
        return True
    if any(str(x).strip() for x in header[len(STOCK_HEADERS_BASE):]):
        raise ValueError('Material_StockEvents K열 이후 다른 항목이 있어 자동 변경하지 않았습니다.')
    if ws.col_count < len(STOCK_HEADERS):
        api_action(ws.resize, cols=len(STOCK_HEADERS))
    api_action(ws.update, range_name='K1:N1', values=[STOCK_HEADERS_EXTRA], value_input_option='RAW')
    return True


def box_conversion_for(code: str, stock_rows: list[dict], request_rows: list[dict]) -> int | None:
    """Show BOX breakdown only for a confirmed, unambiguous recorded factor."""
    saved = [r for r in stock_rows if r['품목코드'] == code and str(r.get('BOX당기준수량', '')).isdigit()]
    if saved:
        return int(sorted(saved, key=lambda r: (r['일시'], r['_row']))[-1]['BOX당기준수량'])
    factors = {int(r['단위환산수량']) for r in request_rows
               if r['품목코드'] == code and str(r.get('신청단위', '')).upper() == 'BOX'
               and str(r.get('단위환산수량', '')).isdigit()}
    return next(iter(factors)) if len(factors) == 1 else None


def format_box_remainder(total: int, factor: int, unit: str) -> str:
    if factor < 1:
        raise ValueError('BOX당 환산수량은 1 이상이어야 합니다.')
    sign = '-' if total < 0 else ''
    boxes, each = divmod(abs(int(total)), factor)
    return f'{sign}{boxes:,} BOX + {each:,} {unit}'



@st.cache_resource(show_spinner=False)
def ensure_material_sheets():
    # One-time initialization. Worksheet handles are also cached.
    ensure_request_schema()
    ensure_stock_schema()
    return True


@st.cache_data(ttl=12, show_spinner=False)
def load_requests_cached(_sheet_id: str):
    ensure_request_schema()
    return rows_as_dicts(get_ws("Material_Requests", REQ_HEADERS), REQ_HEADERS)


@st.cache_data(ttl=12, show_spinner=False)
def load_stock_events_cached(_sheet_id: str):
    ensure_stock_schema()
    return rows_as_dicts(get_ws("Material_StockEvents", STOCK_HEADERS), STOCK_HEADERS)


def material_refresh():
    load_requests_cached.clear()
    load_stock_events_cached.clear()
    hira_search_cached.clear()
    hospital_item_rows.clear()


def all_requests():
    return load_requests_cached(str(st.secrets['treatments']['spreadsheet_id']))


def all_stock_events():
    return load_stock_events_cached(str(st.secrets['treatments']['spreadsheet_id']))


def material_depts(emp, employee_data):
    depts = allowed_departments(emp, employee_data)
    if not depts:
        st.warning("관리 가능한 부서가 지정되지 않았습니다. MASTER에게 관리부서 등록을 요청하세요.")
    return depts


def get_hira_items():
    return combined_catalog()


def material_catalog_lookup(code):
    frame = get_hira_items()
    match = frame[frame['코드'] == str(code).strip().upper()]
    if len(match) != 1:
        raise ValueError('선택한 코드가 심평원 기준 DB에 없거나 중복되어 있습니다. 최신 품목목록을 다시 확인하세요.')
    return match.iloc[0].to_dict()


def material_qty(value):
    try:
        number = int(str(value).replace(',', '').strip())
    except ValueError:
        raise ValueError('수량은 정수로 입력해 주세요.')
    if not 1 <= number <= 1000000:
        raise ValueError('수량은 1~1,000,000 사이여야 합니다.')
    return number


def stock_by_department(events=None, requests=None):
    # 부서별 현재고 = 기초/사용/조정 이벤트 + 수령 확정 수량
    events = all_stock_events() if events is None else events
    requests = all_requests() if requests is None else requests
    balances = defaultdict(int)
    names = {}
    for row in events:
        key = (row['부서'], row['품목코드'])
        try:
            balances[key] += int(row['증감수량'])
        except ValueError:
            raise ValueError('재고 거래기록의 증감수량에 숫자가 아닌 값이 있습니다.')
        names[key] = row['품명']
    for row in requests:
        if row['상태'] != '수령완료':
            continue
        key = (row['수령부서'], row['품목코드'])
        try:
            balances[key] += int(row['출고수량']) * request_conversion(row)
        except ValueError:
            raise ValueError('수령완료 자료의 출고수량을 확인하세요.')
        names[key] = row['품명']
    return balances, names


def material_save_requests(emp, department, cart, memo):
    if not cart:
        raise ValueError('신청할 품목을 추가해 주세요.')
    # Re-check role + allowed departments on the server before saving.
    fresh_emp = next((r for r in employees_fresh() if r['사번'] == emp['사번'] and is_active(r)), None)
    if fresh_emp is None or department not in allowed_departments(fresh_emp, employees_fresh()):
        raise PermissionError('신청할 부서에 대한 관리 권한이 없습니다.')
    created = now()
    group_id = ("TEST-" if test_context() else "") + uuid.uuid4().hex[:12].upper()
    rows = []
    for item in cart:
        source = material_catalog_lookup(item['코드'])
        unit, factor = validate_request_unit(item.get('신청단위'), item.get('단위환산수량'), source['단위'])
        rows.append([group_id + '-' + str(len(rows) + 1).zfill(3), created, emp['사번'], emp['이름'], department,
                     source['코드'], source['품명'], source['규격'], str(material_qty(item['수량'])),
                     '신청', '', '', '', '', '', test_mark(memo),
                     source['중분류'], source['단위'], source['상한금액'], source['제조수입업소'],
                     unit, str(factor), source['단위']])
    ensure_request_schema()
    api_action(get_ws('Material_Requests', REQ_HEADERS).append_rows, rows, value_input_option='RAW')
    material_refresh()
    test_audit("REQUEST", f"신청번호:{group_id}, 부서:{department}, 품목수:{len(cart)}")
    return group_id


def material_set_request_status(request, new_status, actor, delivered_qty=None):
    if actor['권한'].upper() != 'MASTER' and new_status in ('출고', '반려'):
        raise PermissionError('출고 승인 및 반려는 MASTER만 가능합니다.')
    if new_status == '수령완료' and test_context() and not request['요청ID'].startswith('TEST-'):
        raise PermissionError('USER 테스트 중에는 테스트 신청건만 수령할 수 있습니다.')
    if new_status == '수령완료' and actor['권한'].upper() != 'MASTER':
        fresh = next((x for x in employees_fresh() if x['사번'] == actor['사번'] and is_active(x)), None)
        if fresh is None or request['수령부서'] not in allowed_departments(fresh, employees_fresh()):
            raise PermissionError('수령 부서에 대한 권한이 없습니다.')
    expected = {'출고':'신청', '반려':'신청', '수령완료':'출고'}[new_status]
    ws = get_ws('Material_Requests', REQ_HEADERS)
    # Re-fetch live row and compare request ID, state and quantity before any update.
    raw = api_action(ws.row_values, request['_row'])
    current = (raw + ['']*len(REQ_HEADERS))[:len(REQ_HEADERS)]
    if current[0] != request['요청ID'] or current[9] != expected:
        raise ValueError('신청 상태가 다른 사용자에 의해 변경되었습니다. 데이터 동기화 후 다시 확인하세요.')
    if new_status == '출고':
        qty = material_qty(delivered_qty)
        if qty > int(current[8]):
            raise ValueError('출고수량은 신청수량을 초과할 수 없습니다.')
        # J: M: 상태 / 출고수량 / 일시 / 담당
        api_action(ws.update, range_name=f'J{request["_row"]}:M{request["_row"]}',
                   values=[['출고', str(qty), now(), actor['사번']]], value_input_option='RAW')
    elif new_status == '반려':
        api_action(ws.update, range_name=f'J{request["_row"]}', values=[['반려']], value_input_option='RAW')
    else:
        if int(current[10]) < 1:
            raise ValueError('출고수량이 확인되지 않습니다.')
        # One row status update = stock increases only once after receipt.
        api_action(ws.update, range_name=f'J{request["_row"]}', values=[['수령완료']], value_input_option='RAW')
        api_action(ws.update, range_name=f'N{request["_row"]}:O{request["_row"]}',
                   values=[[now(), actor['사번']]], value_input_option='RAW')
    material_refresh()
    test_audit("REQUEST_STATUS", f"요청ID:{request['요청ID']}, 상태:{new_status}")


def save_stock_event(emp, department, code, action, quantity, memo, *, target_total=None,
                     box_qty=None, each_qty=None, box_factor=None, input_mode='기준단위'):
    fresh_data = employees_fresh()
    fresh_emp = next((r for r in fresh_data if r['사번'] == emp['사번'] and is_active(r)), None)
    if fresh_emp is None or department not in allowed_departments(fresh_emp, fresh_data):
        raise PermissionError('해당 부서의 재고를 관리할 권한이 없습니다.')
    item = material_catalog_lookup(code)
    before = stock_by_department()[0].get((department, code), 0)
    if action == '초기재고':
        if target_total is None or target_total < 0:
            raise ValueError('초기재고는 0 이상이어야 합니다.')
        # Initial stock registration allowed only once per dept/item to avoid accidental overwrite.
        if any(r['부서'] == department and r['품목코드'] == code and r['유형'] == '초기재고' for r in all_stock_events()):
            raise ValueError('초기재고가 이미 등록되었습니다. 실사·조정 기능을 이용하세요.')
        delta = target_total - before
    elif action == '사용':
        delta = -material_qty(quantity)
    elif action == '실사조정':
        if target_total is None or target_total < 0:
            raise ValueError('실사 수량은 0 이상이어야 합니다.')
        delta = target_total - before
        if not memo.strip():
            raise ValueError('실사조정 사유를 입력해 주세요.')
    else:
        raise ValueError('허용되지 않는 재고 처리 방식입니다.')
    if before + delta < 0:
        raise ValueError(f'현재 재고 {before:,}개를 초과하여 차감할 수 없습니다.')
    if delta == 0:
        raise ValueError('현재고와 입력 수량이 같아 변경할 내용이 없습니다.')
    # Store the human-entered BOX + remainder values separately from canonical delta.
    # Historic inventory rows remain valid and retain their original 10 columns.
    extra = ['', '', '', '기준단위']
    if input_mode == 'BOX+낱개':
        if any(v is None for v in (box_qty, each_qty, box_factor)):
            raise ValueError('BOX 및 낱개 수량을 확인해 주세요.')
        boxes, each, factor = int(box_qty), int(each_qty), int(box_factor)
        if boxes < 0 or each < 0 or factor < 1 or each >= factor:
            raise ValueError('낱개는 BOX당 수량보다 작아야 합니다. 수량과 환산값을 확인하세요.')
        if boxes * factor + each != int(quantity):
            raise ValueError('BOX+낱개 수량과 저장 수량이 일치하지 않습니다.')
        extra = [str(boxes), str(each), str(factor), 'BOX+낱개']
    record = [uuid.uuid4().hex, now(), department, code, item['품명'], action, str(delta),
              emp['사번'], test_mark(memo), ''] + extra
    ensure_stock_schema()
    api_action(get_ws('Material_StockEvents', STOCK_HEADERS).append_row, record, value_input_option='RAW')
    material_refresh()
    test_audit("STOCK", f"부서:{department}, 코드:{code}, 유형:{action}, 증감:{delta}")
    return before + delta


DETAIL_FIELDS = ['중분류', '코드', '품명', '규격', '단위', '상한금액', '제조수입업소', '급여구분', '현재상태']


def display_unit(value):
    """Display a unit as EA rather than 1EA; do not modify source or quantity."""
    raw = str(value or '').strip()
    match = re.fullmatch(r'1\s*(EA|BOX|PACK|SET|KIT|UNIT|PAIR|ROLL|BOTTLE)', raw, flags=re.I)
    return match.group(1).upper() if match else raw


def pick_detail_row(frame, key, *, height=260, message='표에서 품목 한 행을 클릭하여 선택하세요.'):
    """One consistent detailed, single-row selector for all material workflows."""
    if frame.empty:
        st.info('검색 결과가 없습니다.')
        return None
    columns = [col for col in DETAIL_FIELDS if col in frame.columns]
    display = frame[columns].copy().fillna('')
    if '단위' in display:
        display['단위'] = display['단위'].map(display_unit)
    display = display.rename(columns={'단위': '기준단위'})
    # Original index is reset only on the view; selection maps to the same position in frame.
    event = st.dataframe(
        display.reset_index(drop=True), on_select='rerun', selection_mode='single-row',
        key=key, hide_index=True, use_container_width=True, height=height,
        column_config={
            '중분류': st.column_config.TextColumn('중분류', width='medium'),
            '코드': st.column_config.TextColumn('품목코드', width='small'),
            '품명': st.column_config.TextColumn('품명', width='large'),
            '규격': st.column_config.TextColumn('규격', width='medium'),
            '기준단위': st.column_config.TextColumn('단위', width='small'),
            '상한금액': st.column_config.TextColumn('상한금액', width='small'),
            '제조수입업소': st.column_config.TextColumn('제조수입업소', width='medium'),
        },
    )
    positions = event.selection.rows
    if not positions:
        st.caption(message)
        return None
    position = positions[0]
    if position < 0 or position >= len(frame):
        return None
    return frame.iloc[position].to_dict()


def material_selector(key_prefix, *, label='품목 검색'):
    catalog = get_hira_items()
    if catalog.empty:
        st.warning('심평원 품목 DB가 아직 없습니다. MASTER가 먼저 심평원 DB를 등록해 주세요.')
        return None
    keyword = st.text_input(label, key=key_prefix+'_term', placeholder='품목코드 · 품명 · 규격 · 중분류 · 제조수입업소')
    if not keyword.strip():
        st.caption('검색어를 입력한 후 아래 상세 표에서 해당 품목의 행을 클릭하세요.')
        return None
    terms = [x.casefold() for x in keyword.split() if x]
    fields = [col for col in ['코드','품명','규격','중분류','제조수입업소'] if col in catalog]
    matrix = catalog[fields].fillna('').astype(str).agg(' '.join, axis=1).str.casefold()
    found = catalog.loc[matrix.map(lambda value: all(term in value for term in terms))].head(150)
    if found.empty:
        st.info('검색된 품목이 없습니다.')
        return None
    st.caption(f'검색 결과 최대 150건 표시 · 실제 검색 {len(found):,}건')
    search_key = hashlib.sha256(keyword.strip().casefold().encode()).hexdigest()[:12]
    record = pick_detail_row(found, f'{key_prefix}_details_{search_key}', height=300)
    if record is None:
        return None
    st.success(f"선택: {record['코드']} · {record['품명']} · {record['규격']}")
    if record['현재상태'] != '등재':
        st.warning('이 품목은 현재 등재 상태가 아닙니다. 실제 구매·청구 가능 여부를 확인하세요.')
    return record


def request_detail_selector(items, key, caption):
    """Show complete item information for issue/receive decisions, not code/name only."""
    frame = pd.DataFrame(items)
    if frame.empty:
        return None
    show = pd.DataFrame([{
        '부서': r.get('수령부서',''), '중분류': r.get('중분류',''),
        '코드': r.get('품목코드',''), '품명': r.get('품명',''),
        '규격': r.get('규격',''), '단위': r.get('심평원단위',''),
        '상한금액': r.get('상한금액',''), '제조수입업소': r.get('제조수입업소',''),
        '신청단위': r.get('신청단위',''), '신청수량': r.get('신청수량',''),
        '출고수량': r.get('출고수량',''), '요청ID': r.get('요청ID',''),
    } for r in items])
    show['단위'] = show['단위'].map(display_unit)
    event = st.dataframe(show, hide_index=True, use_container_width=True, height=285,
                         on_select='rerun', selection_mode='single-row', key=key)
    selected = event.selection.rows
    if not selected:
        st.caption(caption)
        return None
    return items[selected[0]] if selected[0] < len(items) else None


def render_request_entry(emp, data):
    st.markdown('#### 물품 신청')
    depts = material_depts(emp, data)
    if not depts:
        return
    department = st.selectbox('신청·수령 부서', depts, key='req_department')
    item = material_selector('req_item', label='① 기존 품목 검색 (필수)')
    if item is not None:
        detail = {k: item.get(k, '') for k in ['중분류','코드','품명','규격','단위','상한금액','제조수입업소']}
        detail['단위'] = display_unit(detail['단위'])
        item_table, qty_box = st.columns([6.2, 1.1], gap='small', vertical_alignment='bottom')
        with item_table:
            st.dataframe(pd.DataFrame([detail]), hide_index=True, use_container_width=True, height=88)
        with qty_box:
            qty = st.number_input('신청수량', min_value=1, max_value=1000000,
                                  value=1, step=1, key='req_qty')
        official_unit = str(item.get('단위', '')).strip().upper()
        st.caption(f'심평원 등록단위: {display_unit(official_unit) or "미등록"} · 신청단위는 별도로 지정할 수 있습니다.')
        col2,col3 = st.columns([1.35,2.5])
        with col2:
            preferred = str(item.get('_병원신청단위','')).strip().upper()
            choices = list(dict.fromkeys(([preferred] if preferred else []) + ([official_unit] if official_unit else []) + REQUEST_UNIT_CHOICES))
            selected_unit = st.selectbox('신청단위', choices, key='req_selected_unit')
            unit = st.text_input('직접 입력할 단위', max_chars=24, key='req_other_unit') if selected_unit == '기타(직접입력)' else selected_unit
        with col3:
            factor = st.number_input(f'신청단위 1개당 기준단위({official_unit or "미등록"}) 수량',
                                      min_value=1, max_value=1000000, value=1, step=1, key='req_factor',
                                      help='예: 1 BOX=100 EA라면 100. 단위가 같으면 1.')
        if unit and official_unit and unit.upper() != official_unit:
            st.info(f'신청: {qty:,} {unit} / 기준재고 반영 예상: {qty * factor:,} {official_unit} (환산은 실제 포장규격 확인 후 입력)')
        else:
            st.caption(f'기준재고 반영 예상: {qty * factor:,} {display_unit(official_unit or unit)}')
        if st.button('선택 품목 신청목록에 추가', use_container_width=False):
            try:
                unit, factor = validate_request_unit(unit, factor, official_unit)
                if unit != official_unit and factor == 1:
                    st.warning('단위가 다르지만 환산수량이 1입니다. 실제 포장수량을 확인해 주세요.')
                cart = st.session_state.setdefault('material_cart', [])
                found = next((r for r in cart if r['코드'] == item['코드'] and r['신청단위'] == unit and int(r['단위환산수량']) == factor), None)
                if found:
                    found['수량'] += int(qty)
                else:
                    cart.append({'중분류':item['중분류'], '코드':item['코드'], '품명':item['품명'],
                        '규격':item['규격'], '단위':official_unit, '상한금액':item['상한금액'],
                        '제조수입업소':item['제조수입업소'], '신청단위':unit,
                        '단위환산수량':factor, '수량':int(qty)})
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))
    cart = st.session_state.get('material_cart', [])
    if cart:
        st.markdown('**② 신청목록 — 신청단위와 환산수량을 확인한 뒤 저장**')
        display = pd.DataFrame(cart)
        if '단위' in display:
            display['단위'] = display['단위'].map(display_unit)
        # Keep editable quantity immediately after middle-category, not far right.
        ordered = ['중분류', '수량', '코드', '품명', '규격', '단위', '상한금액',
                   '제조수입업소', '신청단위', '단위환산수량']
        display = display[[c for c in ordered if c in display.columns] +
                          [c for c in display.columns if c not in ordered]]
        edited = st.data_editor(display, hide_index=True, use_container_width=True, num_rows='fixed',
            disabled=['중분류','코드','품명','규격','단위','상한금액','제조수입업소'],
            column_config={
                '수량': st.column_config.NumberColumn('신청 수량',min_value=1,max_value=1000000,step=1,required=True),
                '신청단위': st.column_config.TextColumn('신청단위',required=True,help='EA / BOX / PACK / SET / KIT 등'),
                '단위환산수량': st.column_config.NumberColumn('1 신청단위당 기준단위 수량',min_value=1,max_value=1000000,step=1,required=True)
            }, key='material_cart_editor')
        st.session_state.material_cart = edited.to_dict('records')
        cart = st.session_state.material_cart
        st.caption('예: 심평원 단위 EA / 신청단위 BOX / 환산수량 100 → 신청수량 5 = 수령 시 재고 500 EA')
        choices = {str(i): f"{r['코드']} · {r['품명'][:35]} · {r['신청단위']}" for i,r in enumerate(cart)}
        rem = st.selectbox('목록에서 제거할 품목', ['선택 안 함']+list(choices), format_func=lambda x: choices.get(x,x))
        if st.button('선택 품목 제거') and rem!='선택 안 함':
            st.session_state.material_cart.pop(int(rem))
            st.rerun()
        memo = st.text_input('신청 메모', max_chars=300)
        confirmed = st.checkbox('신청단위와 포장당 환산수량을 확인했습니다.', key='request_unit_confirm')
        if st.button(f'물품 {len(cart)}종 신청 확정', type='primary', disabled=not confirmed):
            try:
                for r in cart:
                    source = material_catalog_lookup(r['코드'])
                    validate_request_unit(r['신청단위'], r['단위환산수량'], source['단위'])
                    material_qty(r['수량'])
                group = material_save_requests(emp, department, cart, memo)
                st.session_state.material_cart = []
                st.success(f'신청 완료 · 신청번호 {group}')
                st.rerun()
            except Exception as exc:
                st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))


def visible_requests(emp, data, statuses=None):
    requests = all_requests()
    allowed = set(material_depts(emp,data))
    visible = [r for r in requests if emp['권한'].upper()=='MASTER' or r['수령부서'] in allowed]
    if statuses: visible = [r for r in visible if r['상태'] in statuses]
    return sorted(visible, key=lambda r: r['신청일시'], reverse=True)


# MASTER의 거래내역 즉시 수정 기능: Sheets 직접 편집 대신 화면에서 편집/저장.
# 수령완료 후 재고 수량은 수령 기록을 바꾸지 않고 재고실사/조정으로 수정합니다.
MASTER_EDIT_FIELDS = {
    '신청': ('수령부서', '신청수량', '신청단위', '단위환산수량', '비고'),
    '출고': ('출고수량', '비고'),
    '수령완료': ('비고',),
    '반려': ('비고',),
}


def save_master_request_edits(before, after, edit_status, actor):
    """Validate current request rows before batching edits. Keep IDs/status/official HIRA fields immutable."""
    if actor['권한'].upper() != 'MASTER' or test_context():
        raise PermissionError('실제 MASTER 계정에서만 신청내역을 수정할 수 있습니다.')
    if edit_status not in MASTER_EDIT_FIELDS:
        raise ValueError('잘못된 편집 상태입니다.')
    allowed = MASTER_EDIT_FIELDS[edit_status]
    originals = {str(r['요청ID']): r for r in before}
    edits = []
    for obj in after:
        rid = str(obj['요청ID'])
        old = originals.get(rid)
        if not old or old['상태'] != edit_status:
            raise ValueError('편집할 신청번호 또는 상태가 일치하지 않습니다.')
        changed = {}
        for field in allowed:
            raw = obj.get(field, '')
            value = str(raw).strip()
            if value != str(old.get(field, '')).strip():
                changed[field] = value
        if not changed:
            continue
        final = {**old, **changed}
        if edit_status == '신청':
            if not final['수령부서'] or final['수령부서'] not in department_list(employees_fresh()):
                raise ValueError(f'{rid}: 등록된 부서를 선택해 주세요.')
            material_qty(final['신청수량'])
            if '신청단위' in changed or '단위환산수량' in changed:
                validate_request_unit(final['신청단위'], final['단위환산수량'], final.get('심평원단위', ''))
        if edit_status == '출고':
            qty = material_qty(final['출고수량'])
            if qty > int(final['신청수량']):
                raise ValueError(f'{rid}: 출고수량은 신청수량을 초과할 수 없습니다.')
        if '비고' in changed:
            if len(final['비고']) > 300:
                raise ValueError(f'{rid}: 비고는 300자 이내여야 합니다.')
            if old.get('요청ID', '').startswith('TEST-') and '[TEST MASTER:' in str(old.get('비고','')) and '[TEST MASTER:' not in final['비고']:
                raise ValueError(f'{rid}: 테스트 기록의 TEST 표시를 제거할 수 없습니다.')
        edits.append((old, changed))
    if not edits:
        return 0
    if len(edits) > 20:
        raise ValueError('한 번에 최대 20건까지 수정할 수 있습니다.')
    # A single live read lowers API usage and checks changes made by another browser.
    ws = get_ws('Material_Requests', REQ_HEADERS)
    live_rows = api_action(ws.get_all_values)
    if not live_rows or live_rows[0][:len(REQ_HEADERS)] != REQ_HEADERS:
        raise ValueError('신청내역 시트 제목행이 변경되었습니다. 수정하지 않았습니다.')
    changeset = []
    audit_details = []
    for old, changes in edits:
        ix = int(old['_row']) - 1
        if ix < 1 or ix >= len(live_rows):
            raise ValueError('신청 행이 이동되었습니다. 데이터 동기화 후 다시 시도하세요.')
        current = (live_rows[ix] + [''] * len(REQ_HEADERS))[:len(REQ_HEADERS)]
        previous = [str(old.get(col, '')) for col in REQ_HEADERS]
        if current != previous:
            raise ValueError(f"{old['요청ID']}: 다른 사용자에 의해 정보가 변경되었습니다. 동기화 후 재시도하세요.")
        for field, value in changes.items():
            col = REQ_HEADERS.index(field) + 1
            pos = gspread.utils.rowcol_to_a1(int(old['_row']), col)
            changeset.append({'range': pos, 'values': [[value]]})
            audit_details.append((old['요청ID'], field, old.get(field, ''), value))
    # Batch API update writes only modified cells; no re-writing the whole dataset.
    api_action(ws.batch_update, changeset, value_input_option='RAW')
    material_refresh()
    for rid, field, original, changed in audit_details:
        try:
            audit('MASTER_REQUEST_EDIT', rid, f'{field}: {original} -> {changed}'[:300])
        except Exception:
            LOGGER.exception('MASTER 수정 이력 기록 실패: %s %s', rid, field)
    return len(edits)


def render_master_inline_editor(emp, visible):
    """Inline editable grid for MASTER; official HIRA data remains read-only."""
    if emp['권한'].upper() != 'MASTER' or test_context():
        return
    with st.expander('✏️ MASTER 신청내역 바로 수정', expanded=False):
        st.caption('상태별로 선택하여 표에서 직접 수정하고 저장하세요. 코드·품명·규격·심평원 단위·상한금액은 원본정보이므로 변경할 수 없습니다.')
        state = st.selectbox('수정할 신청 상태', list(MASTER_EDIT_FIELDS), key='master_edit_state')
        records = [r for r in visible if r['상태'] == state][:100]
        if not records:
            st.info('선택한 상태의 신청내역이 없습니다.')
            return
        columns = ['요청ID', '수령부서','품목코드','품명','규격','신청수량','신청단위','단위환산수량','출고수량','상태','비고']
        view = pd.DataFrame([{c: r.get(c,'') for c in columns} for r in records])
        permitted = set(MASTER_EDIT_FIELDS[state])
        edited = st.data_editor(view, hide_index=True, use_container_width=True, num_rows='fixed',
            disabled=[c for c in columns if c not in permitted],
            column_config={
                '신청일시': st.column_config.TextColumn('신청일', width='small'),
                '수령부서': st.column_config.TextColumn('부서', width='small'),
                '품목코드': st.column_config.TextColumn('코드', width='small'),
                '심평원단위': st.column_config.TextColumn('기준단위', width='small'),
                '수령부서': st.column_config.TextColumn('수령부서'),
                '신청수량': st.column_config.TextColumn('신청수량 (정수)'),
                '신청단위': st.column_config.TextColumn('신청단위', width='small'),
                '단위환산수량': st.column_config.TextColumn('1단위당 환산수량'),
                '출고수량': st.column_config.TextColumn('출고수량 (정수)'),
                '비고': st.column_config.TextColumn('비고', width='large'),
            }, key='master_request_editor_' + state)
        if state == '수령완료':
            st.info('수령완료 건의 수량을 고치면 재고가 함께 변경되므로 여기서는 비고만 수정합니다. 수량 정정은 재고관리 → 실사조정을 사용하세요.')
        if st.button('변경사항 저장', type='primary', key='master_request_save_' + state):
            try:
                result = save_master_request_edits(records, edited.to_dict('records'), state, emp)
                if result:
                    st.success(f'{result}건을 저장했습니다. 수정 이력은 Audit_Log에 기록됩니다.')
                    st.rerun()
                else:
                    st.info('변경된 내용이 없습니다.')
            except Exception as exc:
                st.error(str(exc) if isinstance(exc, (ValueError, PermissionError)) else safe_spreadsheet_error(exc))


def delete_master_request_row(request, actor):
    """Delete exactly one not-yet-received request after a live, full-row comparison.

    Fulfilled/issued stock movements are never hard-deleted; those require
    reversal/stock adjustment to maintain an auditable inventory ledger.
    """
    if actor['권한'].upper() != 'MASTER' or test_context():
        raise PermissionError('실제 MASTER 계정에서만 요청행을 삭제할 수 있습니다.')
    if request['상태'] not in ('신청', '반려'):
        raise ValueError('출고 또는 수령된 품목은 삭제할 수 없습니다. 상태 정정 및 재고조정을 사용하세요.')
    ws = get_ws('Material_Requests', REQ_HEADERS)
    live = api_action(ws.get_all_values)
    row_number = int(request['_row'])
    if row_number <= 1 or row_number > len(live):
        raise ValueError('신청행이 이동되었으므로 삭제를 중단했습니다. 동기화 후 다시 시도하세요.')
    current = (live[row_number - 1] + [''] * len(REQ_HEADERS))[:len(REQ_HEADERS)]
    expected = [str(request.get(col, '')) for col in REQ_HEADERS]
    if current != expected:
        raise ValueError('다른 직원이 해당 행을 변경했습니다. 동기화 후 다시 시도하세요.')
    if current[9] not in ('신청', '반려') or current[10].strip() or current[13].strip():
        raise ValueError('이미 출고 또는 수령된 기록은 삭제할 수 없습니다.')
    # A deliberate single delete, not an automatically retried request.
    ws.delete_rows(row_number)
    material_refresh()
    try:
        audit('MASTER_REQUEST_DELETE', request['요청ID'],
              f"부서:{request['수령부서']} 품목:{request['품목코드']} 수량:{request['신청수량']} 단위:{request.get('신청단위','')} 내용:{str(dict(zip(REQ_HEADERS, expected)))[:450]}")
    except Exception:
        LOGGER.exception('신청행 삭제 후 감사기록 저장에 실패했습니다: %s', request['요청ID'])
    return request['요청ID']


def render_request_history(emp,data):
    st.markdown('#### 신청·출고·수령 현황')
    items = visible_requests(emp, data)
    if not items:
        st.info('현재 표시할 신청내역이 없습니다.')
        return
    frame = pd.DataFrame(items)
    master_edit = emp['권한'].upper() == 'MASTER' and not test_context()
    dept_choice = st.selectbox('부서 필터', ['전체'] + sorted(set(frame['수령부서'])), key='history_dept')
    status_options = ['신청','출고','수령완료','반려','전체'] if master_edit else ['전체','신청','출고','수령완료','반려']
    status_choice = st.selectbox('상태 필터', status_options, key='history_status')
    if dept_choice != '전체':
        frame = frame[frame['수령부서'] == dept_choice]
    if status_choice != '전체':
        frame = frame[frame['상태'] == status_choice]
    rows = frame.head(1000).to_dict('records')
    if not rows:
        st.info('선택한 조건에 해당하는 신청내역이 없습니다.')
        return
    # Compact everyday columns: detailed reference fields remain available in the expander.
    columns = ['신청일시','수령부서','중분류','품목코드','품명','규격',
               '심평원단위','신청단위','신청수량','출고수량','상태','비고']
    all_columns = ['신청일시','요청ID','수령부서','중분류','품목코드','품명','규격',
                   '심평원단위','상한금액','제조수입업소','신청단위','단위환산수량',
                   '신청수량','출고수량','상태','신청자','출고담당','수령담당','비고']
    original_catalog=get_hira_items()
    code_info=original_catalog.drop_duplicates('코드').set_index('코드').to_dict('index') if not original_catalog.empty else {}
    def show_value(r, col):
        if col=='신청일시': return str(r.get(col,''))[:10]
        value=str(r.get(col,''))
        if not value and col in ('중분류','심평원단위','상한금액','제조수입업소'):
            source=code_info.get(r.get('품목코드',''),{})
            return str(source.get('단위' if col=='심평원단위' else col,''))
        return display_unit(value) if col == '심평원단위' else value
    st.caption('기존 신청건의 빈 품목정보는 현재 품목DB를 참고해 표시합니다. 과거 저장값은 바꾸지 않습니다.')
    if master_edit and status_choice != '전체':
        # The main management grid itself is editable. No separate expander.
        editable = set(MASTER_EDIT_FIELDS[status_choice])
        can_delete = status_choice in ('신청', '반려')
        visible_columns = (['_삭제'] if can_delete else []) + columns
        internal_columns = visible_columns + ['요청ID']
        display_rows = []
        for r in rows:
            obj = {col: show_value(r,col) for col in columns}
            obj['요청ID'] = r['요청ID']
            if can_delete:
                obj['_삭제'] = False
            display_rows.append(obj)
        table = pd.DataFrame(display_rows, columns=internal_columns)
        st.caption('✏️ 수정 가능한 칸을 표에서 직접 바꾸고 저장하세요. 수령완료된 수량은 재고 조정으로 정정합니다.'
                   + (' 삭제할 신청행은 왼쪽 체크박스를 선택하세요.' if can_delete else ''))
        edited = st.data_editor(
            table, use_container_width=True, height=500, hide_index=True, num_rows='fixed',
            column_order=visible_columns,
            disabled=[c for c in internal_columns if c not in editable and c != '_삭제'],
            column_config={
                '_삭제': st.column_config.CheckboxColumn('삭제', help='신청 또는 반려 상태의 한 행만 삭제 가능'),
                '신청일시': st.column_config.TextColumn('신청일', width='small'),
                '수령부서': st.column_config.TextColumn('부서', width='small'),
                '심평원단위': st.column_config.TextColumn('기준단위', width='small'),
                '품명': st.column_config.TextColumn('품명', width='medium'),
                '중분류': st.column_config.TextColumn('중분류', width='small'),
                '신청수량': st.column_config.TextColumn('신청수량', width='small'),
                '출고수량': st.column_config.TextColumn('출고수량', width='small'),
                '신청단위': st.column_config.TextColumn('신청단위'),
                '단위환산수량': st.column_config.TextColumn('환산수량'),
                '비고': st.column_config.TextColumn('비고', width='large'),
            }, key='request_history_inline_' + status_choice + '_' + dept_choice
        )
        col_save, col_delete = st.columns([1, 1])
        with col_save:
            if st.button('변경사항 저장', type='primary', use_container_width=True,
                         key='request_history_save_' + status_choice):
                try:
                    count = save_master_request_edits(rows, edited.to_dict('records'), status_choice, emp)
                    if count:
                        st.success(f'{count}건의 변경사항을 저장했습니다.')
                        st.rerun()
                    else:
                        st.info('수정된 항목이 없습니다.')
                except Exception as exc:
                    st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))
        with col_delete:
            if can_delete:
                marked = edited[edited['_삭제'] == True]
                label = '선택한 1행 삭제' if len(marked) == 1 else '삭제할 1행 선택'
                if st.button(label, use_container_width=True, disabled=len(marked) != 1,
                             key='request_history_delete_' + status_choice):
                    st.session_state['pending_request_delete'] = str(marked.iloc[0]['요청ID'])
            else:
                st.caption('출고·수령완료 행은 삭제할 수 없습니다.')
        pending = st.session_state.get('pending_request_delete')
        if pending:
            target = next((r for r in rows if r['요청ID'] == pending), None)
            if target is None or target['상태'] not in ('신청', '반려'):
                st.session_state.pop('pending_request_delete', None)
            else:
                st.warning(f"요청ID {pending} · {target['품명']} · {target['신청수량']} {request_unit_label(target)} 삭제 확인")
                yes, no = st.columns(2)
                with yes:
                    if st.button('이 1행 삭제 확정', type='primary', use_container_width=True):
                        try:
                            delete_master_request_row(target, emp)
                            st.session_state.pop('pending_request_delete', None)
                            st.success('선택한 신청행을 삭제했습니다.')
                            st.rerun()
                        except Exception as exc:
                            st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))
                with no:
                    if st.button('삭제 취소', use_container_width=True):
                        st.session_state.pop('pending_request_delete', None)
                        st.rerun()
    else:
        if master_edit and status_choice == '전체':
            st.caption('전체 이력은 조회용입니다. 위 상태 필터를 선택하면 같은 표에서 수량을 바로 수정할 수 있습니다.')
        compact=pd.DataFrame([{c:show_value(r,c) for c in columns} for r in rows])
        compact=compact.rename(columns={'신청일시':'신청일','심평원단위':'기준단위','수령부서':'부서'})
        st.dataframe(compact, hide_index=True, use_container_width=True, height=410)
    with st.expander('품목 상세·상한금액·제조수입업소·신청 이력 전체보기'):
        details=pd.DataFrame([{c:show_value(r,c) for c in all_columns} for r in rows]).rename(columns={'신청일시':'신청일'})
        st.dataframe(details,hide_index=True,use_container_width=True,height=340)
    st.caption('신청 → MASTER 출고 → 부서 수령확인 후 해당 부서 재고에 반영됩니다.')

def render_master_issue(emp,data):
    st.markdown('#### 신청 승인·출고 (MASTER)')
    items=visible_requests(emp,data, ['신청'])
    if not items:
        st.info('출고 대기 신청이 없습니다.'); return
    row = request_detail_selector(items, 'issue_detail_selection', '출고할 신청 행을 클릭하세요.')
    if row is None:
        return
    qty=st.number_input(f'실제 출고수량 ({request_unit_label(row)})',min_value=1,max_value=max(1,int(row['신청수량'])),value=int(row['신청수량']),key='issue_qty')
    a,b=st.columns(2)
    with a:
        if st.button('출고 확정',type='primary',use_container_width=True):
            try:
                material_set_request_status(row,'출고',emp,int(qty))
                st.success('출고 처리했습니다. 해당 부서의 수령 확인을 기다립니다.'); st.rerun()
            except Exception as exc: st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))
    with b:
        if st.button('신청 반려',use_container_width=True):
            try:
                material_set_request_status(row,'반려',emp)
                st.success('신청을 반려했습니다.');st.rerun()
            except Exception as exc: st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))


def render_receipt(emp,data):
    st.markdown('#### 부서 물품 수령확인')
    items=visible_requests(emp,data,['출고'])
    if test_context():
        items = [r for r in items if r['요청ID'].startswith('TEST-')]
    if not items:
        st.info('수령 확인 대기 물품이 없습니다.');return
    row = request_detail_selector(items, 'receipt_detail_selection', '수령할 물품 행을 클릭하세요.')
    if row is None:
        return
    st.info(f"실제 출고 {row['출고수량']} {request_unit_label(row)} · 수령 시 기준재고 {int(row['출고수량']) * request_conversion(row):,} {row.get('재고기준단위') or row.get('심평원단위') or '개'} 증가 · 포장규격을 확인하세요.")
    if st.button('수령 완료 · 재고 반영',type='primary',use_container_width=True):
        try:
            material_set_request_status(row,'수령완료',emp)
            st.success('수령완료 · 부서 재고에 반영되었습니다.'); st.rerun()
        except Exception as exc:st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


def render_stock_overview(emp,data):
    depts=material_depts(emp,data)
    if not depts:return
    balances,names=stock_by_department()
    catalog = get_hira_items()
    unit_map = dict(zip(catalog['코드'], catalog['단위']))
    stock_events = all_stock_events()
    requests = all_requests()
    factor_cache = {}
    show=[]
    for (dept,code),qty in balances.items():
        if dept not in depts:
            continue
        unit = str(unit_map.get(code, '') or 'EA').upper()
        if code not in factor_cache:
            factor_cache[code] = box_conversion_for(code, stock_events, requests)
        factor = factor_cache[code]
        show.append({'부서':dept,'품목코드':code,'품명':names.get((dept,code),''),
                     '현재고':qty,'재고기준단위':unit,
                     'BOX당 수량':factor if factor else '',
                     'BOX+낱개 표시':format_box_remainder(qty,factor,unit) if factor else '환산 미설정'})
    if not show:
        st.info('등록된 재고가 없습니다. 재고관리에서 최초 재고를 입력하거나 물품을 수령하세요.');return
    frame=pd.DataFrame(show)
    dept=st.selectbox('재고 조회 부서',['전체']+depts,key='stock_filter_dept')
    if dept!='전체': frame=frame[frame['부서']==dept]
    q=st.text_input('재고 품명·코드 검색',key='stock_filter_term')
    if q:
        frame=frame[frame[['품목코드','품명']].astype(str).agg(' '.join,axis=1).str.contains(re.escape(q),case=False,regex=True)]
    a,b=st.columns(2)
    a.metric('조회 품목 수',f'{len(frame):,}')
    b.metric('조회 부서 수',f'{frame["부서"].nunique():,}')
    st.caption('서로 다른 재고단위(EA, SET, KIT 등)는 합산하지 않습니다. 현재고는 품목별 기준단위로 표시합니다.')
    st.dataframe(frame.sort_values(['부서','품목코드']),hide_index=True,use_container_width=True,height=490)
    st.download_button('재고현황 CSV 다운로드',frame.to_csv(index=False).encode('utf-8-sig'),'HISMEDI_재고현황.csv','text/csv')


def render_stock_entry(emp,data):
    st.markdown('#### 초기재고 · 사용등록 · 실사조정')
    depts=material_depts(emp,data)
    if not depts:return
    dept=st.selectbox('관리 부서',depts,key='stock_entry_dept')
    item=material_selector('stock_item')
    if item is None:return
    code=item['코드']
    balances,_=stock_by_department()
    before=balances.get((dept,code),0)
    base_unit=str(item.get('단위') or 'EA').strip().upper()
    existing_factor=box_conversion_for(code,all_stock_events(),all_requests())
    st.metric('현재 등록 재고',f'{before:,} {base_unit}')
    if existing_factor:
        st.caption('포장환산 표시: '+format_box_remainder(before,existing_factor,base_unit)+
                   f' (1 BOX = {existing_factor:,} {base_unit})')
    action=st.radio('작업 선택',['초기재고','사용','실사조정'],horizontal=True,key='stock_action')
    mode=st.radio('수량 입력 방식',['기준단위 직접입력','BOX + 낱개'],horizontal=True,
                  key='stock_quantity_mode',help='BOX와 낱개 모두 같은 기준단위 수량으로 환산해 저장합니다.')
    box_qty=each_qty=box_factor=None
    if mode=='BOX + 낱개':
        c1,c2,c3=st.columns(3)
        with c1:
            box_qty=st.number_input('BOX 수량',min_value=0,max_value=1000000,value=0,step=1,key='stock_boxes')
        with c2:
            each_qty=st.number_input(f'낱개 수량 ({display_unit(base_unit)})',min_value=0,max_value=1000000,
                                     value=0,step=1,key='stock_eaches')
        with c3:
            box_factor=st.number_input(f'1 BOX당 {base_unit} 수량',min_value=1,max_value=1000000,
                                       value=existing_factor or 100,step=1,key='stock_box_factor')
        qty=int(box_qty)*int(box_factor)+int(each_qty)
        if each_qty >= box_factor:
            st.error('낱개 수량은 BOX당 수량보다 작게 입력하세요. (예: 1 BOX=100 EA → 낱개 0~99 EA)')
        st.info(f'입력: {box_qty:,} BOX + {each_qty:,} {base_unit} = 합계 {qty:,} {base_unit}')
    else:
        if action=='초기재고':
            label='초기 보유 수량'
            initial=0
            floor=0
        elif action=='사용':
            label='실제 사용 수량'
            initial=1
            floor=1
        else:
            label='실사 후 실제 보유 수량'
            initial=max(0,before)
            floor=0
        qty=st.number_input(f'{label} ({base_unit})',min_value=floor,max_value=1000000,
                            value=initial,step=1,key='stock_direct_qty')
    if action in ('초기재고','실사조정'):
        st.caption('입력값은 변경 후의 실제 보유 총량입니다. 기존 재고에 무조건 더하지 않습니다.')
    elif qty>before:
        st.warning('사용량이 현재 재고보다 많습니다.')
    memo=st.text_input('사유 / 메모 (실사조정 시 필수)',max_chars=300,key='stock_note')
    invalid = mode=='BOX + 낱개' and each_qty >= box_factor
    invalid = invalid or (action=='사용' and qty<1)
    invalid = invalid or qty>1000000
    if st.button('재고 변경 저장',type='primary',key='stock_write',disabled=invalid):
        try:
            new=save_stock_event(emp,dept,code,action,qty,memo,
                                 target_total=int(qty) if action!='사용' else None,
                                 box_qty=box_qty,each_qty=each_qty,box_factor=box_factor,
                                 input_mode='BOX+낱개' if mode=='BOX + 낱개' else '기준단위')
            st.success(f'저장 완료 · 변경 후 재고 {new:,} {base_unit}')
            st.rerun()
        except Exception as exc:
            st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


def render_initial_stock_bulk(emp, data):
    st.markdown('#### 초기재고 일괄 등록')
    st.caption('구글시트에서 품목코드·수량 두 열을 작성해 엑셀(xlsx) 또는 CSV로 업로드할 수 있습니다. 기존 거래이력은 삭제하지 않습니다.')
    departments = material_depts(emp, data)
    if not departments: return
    dept = st.selectbox('초기재고 등록 부서', departments, key='bulk_stock_dept')
    template = pd.DataFrame(columns=['품목코드','수량'])
    st.download_button('빈 양식 CSV 다운로드', template.to_csv(index=False).encode('utf-8-sig'),
                       'HISMEDI_초기재고양식.csv', 'text/csv', key='bulk_template')
    file = st.file_uploader('초기재고 파일', type=['csv','xlsx'], key='bulk_stock_upload')
    if file is None: return
    try:
        if file.name.lower().endswith('.xlsx'):
            incoming = pd.read_excel(file, dtype={'품목코드':str})
        else:
            try: incoming = pd.read_csv(file, encoding='utf-8-sig', dtype={'품목코드':str})
            except UnicodeDecodeError:
                file.seek(0)
                incoming = pd.read_csv(file, encoding='cp949', dtype={'품목코드':str})
        if not {'품목코드','수량'}.issubset(incoming.columns):
            raise ValueError('품목코드, 수량 열이 필요합니다.')
        if len(incoming)>1000 or not len(incoming):
            raise ValueError('한 번에 1~1,000개 품목을 입력하세요.')
        incoming = incoming[['품목코드','수량']].copy()
        incoming['품목코드'] = incoming['품목코드'].astype(str).str.strip().str.upper()
        if incoming['품목코드'].duplicated().any():
            raise ValueError('파일에 중복 품목코드가 있습니다.')
        incoming['수량'] = pd.to_numeric(incoming['수량'], errors='coerce')
        if incoming['수량'].isna().any() or ((incoming['수량']%1)!=0).any() or (incoming['수량']<0).any():
            raise ValueError('수량은 0 이상의 정수만 입력하세요.')
        catalog = get_hira_items()
        if catalog.empty: raise ValueError('심평원 기준 품목 DB가 없습니다.')
        index = catalog.drop_duplicates('코드').set_index('코드')
        missing = sorted(set(incoming['품목코드'])-set(index.index))
        if missing: raise ValueError('심평원 DB에 없는 품목코드: '+', '.join(missing[:15]))
        existing_initial = {(r['부서'],r['품목코드']) for r in all_stock_events() if r['유형']=='초기재고'}
        duplicates=[c for c in incoming['품목코드'] if (dept,c) in existing_initial]
        if duplicates: raise ValueError('이미 초기재고 등록됨: '+', '.join(duplicates[:15]))
        balances,_=stock_by_department()
        incoming['품명'] = incoming['품목코드'].map(index['품명'])
        incoming['기존재고'] = incoming['품목코드'].map(lambda c: balances.get((dept,c),0))
        incoming['증감'] = incoming['수량']-incoming['기존재고']
        st.dataframe(incoming[['품목코드','품명','기존재고','수량','증감']],hide_index=True,use_container_width=True,height=280)
        st.caption('입력 수량을 현재 실물 총수량으로 취급합니다. 예: 기존 수령재고 10개, 실제 15개 → +5개만 조정')
        confirmed=st.checkbox('해당 부서의 실제 보유량을 확인했습니다.',key='bulk_confirm')
        if st.button('초기재고 일괄 저장',type='primary',disabled=not confirmed):
            fresh=employees_fresh()
            fresh_emp=next((x for x in fresh if x['사번']==emp['사번'] and is_active(x)),None)
            if fresh_emp is None or dept not in allowed_departments(fresh_emp,fresh):
                raise PermissionError('부서 관리 권한이 변경되었습니다.')
            latest_events = all_stock_events_cached_fresh()
            latest_req = all_requests_cached_fresh()
            fresh_initial={(r['부서'],r['품목코드']) for r in latest_events if r['유형']=='초기재고'}
            if any((dept,c) in fresh_initial for c in incoming['품목코드']):
                raise ValueError('초기재고가 다른 사용자에 의해 등록되었습니다. 다시 확인하세요.')
            fresh_balance,_=stock_by_department(latest_events,latest_req)
            records=[]
            for r in incoming.itertuples(index=False):
                before=fresh_balance.get((dept,r.품목코드),0)
                delta=int(r.수량)-before
                if delta:
                    records.append([uuid.uuid4().hex,now(),dept,r.품목코드,r.품명,'초기재고',str(delta),emp['사번'],test_mark('초기재고 일괄등록'),''])
            if records:
                api_action(get_ws('Material_StockEvents',STOCK_HEADERS).append_rows,records,value_input_option='RAW')
            material_refresh()
            test_audit('BULK_STOCK', f'부서:{dept}, 품목수:{len(records)}')
            st.success(f'초기재고 입력 완료: {len(incoming):,}개 품목 (실제 수량 변경 {len(records):,}건)')
    except Exception as exc:
        st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


def all_stock_events_cached_fresh():
    ensure_stock_schema()
    return rows_as_dicts(get_ws('Material_StockEvents',STOCK_HEADERS),STOCK_HEADERS)


def all_requests_cached_fresh():
    ensure_request_schema()
    return rows_as_dicts(get_ws('Material_Requests',REQ_HEADERS),REQ_HEADERS)


def render_stock_history(emp,data):
    depts=set(material_depts(emp,data))
    frame=pd.DataFrame([e for e in all_stock_events() if e['부서'] in depts])
    if frame.empty:
        st.info('등록된 재고 변경이력이 없습니다.');return
    st.dataframe(frame[['일시','부서','품목코드','품명','유형','증감수량','작업사번','메모']].iloc[::-1].head(1000),hide_index=True,use_container_width=True)


def render_work_summary(emp,data):
    st.subheader('물품관리 현황')
    depts=set(material_depts(emp,data))
    requests=[r for r in all_requests() if emp['권한'].upper()=='MASTER' or r['수령부서'] in depts]
    balances,_=stock_by_department()
    a,b,c,d=st.columns(4)
    a.metric('신청 대기',sum(r['상태']=='신청' for r in requests))
    b.metric('수령 대기',sum(r['상태']=='출고' for r in requests))
    c.metric('수령 완료',sum(r['상태']=='수령완료' for r in requests))
    d.metric('부서별 재고 품목',sum(dept in depts and amount!=0 for (dept,_),amount in balances.items()))
    st.caption('실제 재고는 부서별 최초 수량·사용·실사조정 및 수령완료 자료로 계산됩니다. 중앙창고 보유량을 가정하지 않습니다.')
    render_request_history(emp,data)


def render_material_menu(choice, emp, data):
    ensure_material_sheets()
    if choice=='물품관리 현황':render_work_summary(emp,data)
    elif choice=='물품 신청':
        tabs=st.tabs(['신규 신청','신청·처리 현황'])
        with tabs[0]:render_request_entry(emp,data)
        with tabs[1]:render_request_history(emp,data)
    elif choice=='출고관리':
        if emp['권한'].upper()!='MASTER':
            st.error('출고 승인은 MASTER만 가능합니다.');return
        tabs=st.tabs(['출고 대기','전체 신청 이력'])
        with tabs[0]:render_master_issue(emp,data)
        with tabs[1]:render_request_history(emp,data)
    elif choice=='물품 수령':render_receipt(emp,data)
    elif choice=='재고관리':
        tabs=st.tabs(['부서별 재고현황','초기재고·사용·실사조정','초기재고 일괄등록','재고 변경이력'])
        with tabs[0]:render_stock_overview(emp,data)
        with tabs[1]:render_stock_entry(emp,data)
        with tabs[2]:render_initial_stock_bulk(emp,data)
        with tabs[3]:render_stock_history(emp,data)
    elif choice=='통계 및 보고서':
        tabs=st.tabs(['신청·출고 현황','재고 현황'])
        with tabs[0]:render_request_history(emp,data)
        with tabs[1]:render_stock_overview(emp,data)

def render_user_preview(choice, emp, data):
    """Safe read-only UI of an actual USER's authorized departments.

    The real session remains authenticated as MASTER; no workflow write control
    is ever rendered in this mode.
    """
    st.info(f"🔎 USER 화면 테스트 — {emp['이름']} / {emp['소속부서']} · 읽기 전용 (실제 MASTER 로그인 유지)")
    st.caption("신청·출고·수령·재고변경·PIN 변경은 테스트 중 저장되지 않습니다.")
    if choice == '내 정보':
        st.write({'사번': emp['사번'], '이름': emp['이름'], '소속부서': emp['소속부서'],
                  '관리부서': ', '.join(material_depts(emp, data)), '권한': 'USER'})
    elif choice == '품목정보':
        render_hira_search()
    elif choice == '물품관리 현황':
        render_work_summary(emp, data)
    elif choice in ('물품 신청', '물품 수령'):
        if choice == '물품 수령':
            st.markdown('#### 부서 물품 수령 대기 (조회 전용)')
            items = visible_requests(emp, data, ['출고'])
        else:
            st.markdown('#### 물품 신청 현황 (조회 전용)')
            items = visible_requests(emp, data)
        if items:
            columns = ['신청일시', '요청ID', '수령부서', '품목코드', '품명', '신청수량', '출고수량', '상태']
            st.dataframe(pd.DataFrame(items)[columns].head(1000), hide_index=True, use_container_width=True)
        else:
            st.info('조회할 신청 내역이 없습니다.')
        st.caption('실제 신청·수령 동작 테스트는 USER 본인 계정에서 진행합니다.')
    elif choice == '재고관리':
        st.markdown('#### 관리부서 재고현황 (조회 전용)')
        render_stock_overview(emp, data)
    elif choice == '통계 및 보고서':
        render_work_summary(emp, data)
    else:
        st.info('해당 작업은 읽기 전용 테스트에서 수행하지 않습니다.')


def main():
    render_style()
    try:
        data = employees()
    except Exception as exc:
        st.error(safe_spreadsheet_error(exc))
        if isinstance(exc, ValueError):
            st.caption(str(exc))
        elif isinstance(exc, APIError) and getattr(getattr(exc, "response", None), "status_code", None) == 429:
            st.caption("Google Sheets 요청 한도가 회복될 때까지 자동 재시도를 진행했지만 실패했습니다. 대량 DB 업로드와 동시 사용 시 발생할 수 있습니다.")
        else:
            st.caption("Google Sheets 연결 상태와 Streamlit 로그의 실제 오류번호를 확인하세요.")
        st.stop()

    # Bootstrap check is only necessary before login, not on every widget rerun.
    masters = [r for r in data if r["권한"].upper() == "MASTER" and is_active(r)]
    initialized_master = True
    if "employee_id" not in st.session_state:
        try:
            get_ws("Auth", AUTH_HEADERS)
            get_ws("Audit_Log", LOG_HEADERS)
            configured_ids = {row["사번"] for row in auth_rows()}
        except Exception as exc:
            st.error(safe_spreadsheet_error(exc))
            st.stop()
        initialized_master = any(r["사번"] in configured_ids for r in masters)
    if not initialized_master:
        render_header("최초 관리자 설정")
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
        # 로그인 화면에서는 상단 로고/제목 반복 없이 간결하게 표시합니다.
        st.markdown('<div style="height:clamp(8px,2vh,24px)"></div>', unsafe_allow_html=True)
        with render_login_visual():
            st.markdown('<div class="hm-login-heading">HISMEDI 치료재료 관리</div>', unsafe_allow_html=True)
            st.markdown(
                '<div class="hm-login-description">'
                '<span class="hm-login-label">직원 로그인</span>'
                '<span class="hm-login-help">PIN을 발급받지 않은 직원은 MASTER에게 초기 PIN 발급을 요청하세요.</span>'
                '</div>', unsafe_allow_html=True,
            )
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
        st.stop()

    emp = next((r for r in data if r["사번"] == st.session_state.employee_id), None)
    if not emp or not is_active(emp):
        sign_out()
    role = emp["권한"].upper()
    if role not in ("MASTER", "USER"):
        st.error("Emp의 권한은 Master 또는 User여야 합니다.")
        sign_out()

    if st.session_state.get("forced_change"):
        render_header("임시 PIN 변경")
        st.warning("임시 PIN으로 로그인했습니다. 새로운 PIN으로 변경한 후 이용할 수 있습니다.")
        render_pin_change(emp)
        st.stop()

    # Never modify the authenticated principal when previewing another account.
    principal = emp
    is_master = role == 'MASTER'
    preview_id = st.session_state.get('material_preview_user') if is_master else None
    targets = [r for r in data if is_active(r) and r['권한'].upper() == 'USER']
    target = next((r for r in targets if r['사번'] == preview_id), None)
    if preview_id and target is None:
        st.session_state.pop('material_preview_user', None)
    preview_active = target is not None
    shown_emp = target if preview_active else principal
    shown_role = 'USER' if preview_active else role

    with st.sidebar:
        if BUILDING_FILE.is_file():
            # The photo is compact so the entire menu fits typical laptop displays.
            photo_left, photo, photo_right = st.columns([.7, 1, .7])
            with photo:
                st.image(str(BUILDING_FILE), use_container_width=True)
        st.caption(f"{principal['이름']} · {principal['소속부서']} · {role}")
        if preview_active:
            st.caption(f"USER 테스트: {shown_emp['이름']} · {shown_emp['소속부서']}")
        menu = ['물품관리 현황', '품목정보', '병원 자체품목 등록', '물품 신청', '물품 수령', '재고관리',
                '통계 및 보고서', '내 정보']
        if not preview_active:
            menu += ['PIN 변경']
        if is_master and not preview_active:
            menu.insert(4, '출고관리')
            menu += ['직원 관리', '심평원 DB 업데이트']
        if st.session_state.get('material_nav') not in menu:
            st.session_state['material_nav'] = menu[0]
        choice = st.session_state['material_nav']
        for i, item in enumerate(menu):
            style_key = ('hm_nav_selected_' if choice == item else 'hm_nav_normal_') + str(i)
            with st.container(key=style_key):
                if st.button(item, key=f'mat_menu_{i}', use_container_width=True):
                    st.session_state['material_nav'] = item
                    st.rerun()

        st.divider()
        if st.button('🔄 데이터 동기화', use_container_width=True,
                     help='Google Sheets의 최신 직원·품목정보를 다시 읽습니다.'):
            material_refresh()
            employees.clear()
            auth_rows.clear()
            st.session_state.pop('hira_files', None)
            st.session_state.pop('hira_import_preview', None)
            hospital_item_rows.clear()
            st.rerun()
        if st.button('로그아웃', use_container_width=True):
            st.session_state.pop('material_preview_user', None)
            sign_out()
        if is_master:
            st.divider()
            st.markdown('**USER 화면 테스트 (실제 저장)**')
            st.caption('MASTER 로그인 유지 · 선택한 USER 권한으로 저장 · TEST 표시 기록')
            if targets:
                choices = {r['사번']: f"{r['이름']} · {r['소속부서']} ({r['사번']})" for r in targets}
                options = list(choices)
                selected_user = st.selectbox('테스트 직원', options,
                                             index=options.index(preview_id) if preview_id in options else 0,
                                             format_func=lambda key: choices[key], key='material_preview_pick')
                if st.button('USER 화면으로 보기', use_container_width=True,
                             disabled=preview_active and selected_user == preview_id):
                    st.session_state.pop('material_cart', None)
                    st.session_state['material_preview_user'] = selected_user
                    st.session_state['material_nav'] = '물품관리 현황'
                    st.rerun()
                if preview_active and st.button('↩ MASTER 화면으로 복귀', use_container_width=True,
                                                type='primary'):
                    st.session_state.pop('material_preview_user', None)
                    st.session_state.pop('material_cart', None)
                    st.session_state['material_nav'] = '물품관리 현황'
                    st.rerun()
            else:
                st.caption('테스트할 USER가 없습니다.')

    # MASTER can execute actual USER workflow without logging out. Every
    # record created by this mode carries a TEST marker and an audit entry.
    if preview_active:
        st.warning(
            f"USER 실저장 테스트: {shown_emp['이름']} · {shown_emp['소속부서']} "
            "— 실제 저장됩니다. 신청·수령·재고에 TEST 표시를 남깁니다. "
            "테스트 데이터는 운영 시작 전에 별도로 정리해야 합니다."
        )
    if choice in ('물품관리 현황', '물품 신청', '물품 수령', '재고관리',
                  '출고관리', '통계 및 보고서'):
        try:
            render_material_menu(choice, shown_emp if preview_active else principal, data)
        except Exception as exc:
            LOGGER.exception('Material workflow failed')
            st.error(str(exc) if isinstance(exc, (ValueError, PermissionError)) else safe_spreadsheet_error(exc))
    elif choice == '품목정보':
        render_hira_search()
    elif choice == '병원 자체품목 등록':
        render_hospital_register(shown_emp if preview_active else principal, data)
    elif choice == '내 정보':
        render_personal_info(shown_emp if preview_active else principal, data)
    elif choice == 'PIN 변경':
        render_pin_change(principal)
    elif choice == '직원 관리' and is_master:
        render_employee_admin(principal, data)
    elif choice == '심평원 DB 업데이트' and is_master:
        render_hira_import()

    st.caption("HISMEDI · 치료재료 관리 | PC용 · Google Sheets 기록 기반")


if __name__ == "__main__":
    main()
