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
    """Retry transient Google Sheets quota/server failures, not permission failures."""
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
            time.sleep(min(2 ** attempt, 6))




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


def get_ws(name: str, headers: list[str] | None = None):
    book = get_book()
    try:
        sheet = api_action(book.worksheet, name)
    except WorksheetNotFound:
        if headers is None:
            raise
        sheet = api_action(book.add_worksheet, title=name, rows=500, cols=max(len(headers), 8))
        api_action(sheet.update, range_name="A1", values=[headers])
    # Header check is performed using the same get_all_values request below,
    # not an extra row_values API call on every Streamlit rerun.
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



BASE_DIR = Path(__file__).resolve().parent
LOGO_FILE = BASE_DIR / "Hismedi_logo.png"
BUILDING_FILE = BASE_DIR / "Hismedi_building.jpg"


def render_style():
    st.markdown("""
    <style>
    .block-container {max-width: 1760px; padding-top: .8rem; padding-bottom: 1.8rem;}
    h1 {font-size: 1.45rem !important; margin-bottom: .35rem !important;}
    h2 {font-size: 1.18rem !important;}
    h3 {font-size: 1.03rem !important;}
    div[data-testid="stAlert"] {font-size: .94rem;}
    div[data-testid="stSidebar"] div[data-testid="stVerticalBlock"] {gap: .55rem;}
    [data-testid="stSidebar"] [data-testid="stImage"] img {object-fit:contain!important;object-position:center center!important;max-width:100%!important;height:auto!important;}
    [data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {padding-top:.4rem!important;}
    /* Desktop navigation: entire row is a clearly clickable rectangular button. */
    [data-testid="stSidebar"] [role="radiogroup"] {display:flex!important;flex-direction:column!important;gap:6px!important;width:100%!important;}
    [data-testid="stSidebar"] [role="radiogroup"] label {
        display:flex!important;align-items:center!important;width:100%!important;
        min-height:39px!important;padding:8px 12px!important;
        border:1px solid #d5deea!important;border-radius:7px!important;
        background:#fff!important;cursor:pointer!important;box-sizing:border-box!important;
        transition:background-color .12s,border-color .12s!important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:has(input:checked) {
        border-color:#2876b8!important;background:#eaf3fc!important;
        box-shadow:inset 3px 0 0 #2876b8!important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label:hover {background:#f1f6fc!important;}
    [data-testid="stSidebar"] [role="radiogroup"] label p {
        font-size:14px!important;font-weight:650!important;color:#183753!important;
        white-space:normal!important;line-height:1.35!important;
    }
    [data-testid="stSidebar"] [role="radiogroup"] label [data-testid="stMarkdownContainer"] {width:100%!important;}
    [data-testid="stSidebar"] [role="radiogroup"] label [data-baseweb="radio"] {display:none!important;}
    [data-testid="stSidebar"] {min-width:250px!important;max-width:290px!important;}
    /* Constrain login photo independently of the visitor's display resolution. */
    .hm-login-photo {width:100%;display:flex;justify-content:center;align-items:center;min-height:200px;}
    .hm-login-photo img {display:block;max-width:100%;width:auto;height:auto;max-height:min(58vh,520px);object-fit:contain;border-radius:8px;}
    .hm-login-heading {font-size:clamp(20px,1.6vw,26px);font-weight:750;margin:0 0 10px;}
    .hm-login-description {display:flex;flex-wrap:wrap;align-items:baseline;gap:7px 18px;margin:0 0 11px;line-height:1.5;}
    .hm-login-label {font-weight:750;font-size:15px;white-space:nowrap;}
    .hm-login-help {font-size:12px;color:#718096;}
    @media (max-width:1000px) {
        .block-container {padding-left:1.1rem;padding-right:1.1rem;}
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


def publish_hira(rows, overview, changes):
    """Two-phase staged import; never erase current HIRA_Items before validation."""
    book = get_book()
    stage_name = "HIRA_Stage_" + datetime.now(KST).strftime("%m%d_%H%M%S")
    if len(rows) > 120000:
        raise ValueError("파일이 예상 크기를 벗어나 자동 반영을 중단했습니다.")
    stage = api_action(book.add_worksheet, title=stage_name, rows=len(rows) + 1, cols=len(HIRA_HEADERS))
    batch_size = 750
    bar = st.progress(0, text="Google Sheets 임시 시트에 분할 저장 중입니다.")
    api_action(stage.update, range_name="A1:R1", values=[HIRA_HEADERS])
    for start in range(0, len(rows), batch_size):
        block = [[str(r[h]) for h in HIRA_HEADERS] for r in rows[start:start+batch_size]]
        endrow = start + len(block) + 1
        api_action(stage.update, range_name=f"A{start+2}:R{endrow}", values=block, value_input_option="RAW")
        bar.progress(min(1., (start+len(block))/len(rows)), text=f"저장 중: {start+len(block):,} / {len(rows):,}건")
    # Confirm count in the server's used-range metadata without downloading 50k rows.
    actual = api_action(stage.get, f"A1:A{len(rows)+1}")
    if len(actual) != len(rows) + 1:
        raise ValueError(f"저장 검증 실패: 예상 {len(rows):,}, 실제 {len(actual)-1:,}건. 현재 자료는 유지됩니다.")
    old = None
    try:
        old = api_action(book.worksheet, "HIRA_Items")
    except WorksheetNotFound:
        pass
    backup = None
    if old:
        backup = "HIRA_Backup_" + datetime.now(KST).strftime("%m%d_%H%M%S")
        api_action(old.update_title, backup)
    try:
        api_action(stage.update_title, "HIRA_Items")
    except Exception:
        if old and backup:
            try:
                api_action(old.update_title, "HIRA_Items")
            except Exception:
                LOGGER.exception("Failed to restore original HIRA_Items title")
        raise
    # Store collision detail for audit. This is independent of the current item
    # catalog and keeps the historical deleted description discoverable.
    try:
        conflict_rows = overview.get("충돌상세", [])
        if conflict_rows:
            headers = ["기준일"] + list(conflict_rows[0].keys())
            ws = get_ws("HIRA_Conflicts", headers)
            old_rows = api_action(ws.get_all_values)
            existing_keys = {(r[0], r[1]) for r in old_rows[1:] if len(r) >= 2}
            for row in conflict_rows:
                if (overview["기준일"], row["코드"]) not in existing_keys:
                    api_action(ws.append_row, [overview["기준일"]] + [str(row[k]) for k in conflict_rows[0]], value_input_option="RAW")
    except Exception:
        LOGGER.exception("HIRA publish completed, but collision archive write failed")
    # Import log is best-effort. Never report failure after successful publication.
    try:
        log_ws = get_ws("HIRA_Imports", HIRA_LOG_HEADERS)
        api_action(log_ws.append_row, [now(), st.session_state.get("employee_id", ""),
                 overview["기준일"], str(len(rows)), str(len(changes["신규"])),
                 str(len(changes["변경"])), str(len(changes["기준목록제외"])),
                 " | ".join(overview["파일명"].values())], value_input_option="RAW")
    except Exception:
        LOGGER.exception("HIRA import published but history append failed")
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
            st.error(f"반영 실패: {type(exc).__name__}. 기존 품목 DB가 유지되는지 확인해 주세요. Streamlit 로그에 상세 오류가 기록됩니다.")


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
        frame = hira_search_cached(str(st.secrets["treatments"]["spreadsheet_id"]))
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
    st.dataframe(result.head(2000), hide_index=True, use_container_width=True, height=620)
    if len(result)>2000:
        st.caption("화면에는 처음 2,000건만 표시합니다. 검색 조건을 좁혀 주세요.")



# ================================================================
# 치료재료 신청 -> 출고 -> 수령 및 부서별 수동 재고
# 초기사용 시 필요한 시트만 만들며 기존 Emp/Auth/HIRA_Items는 보존합니다.
# ================================================================
REQ_HEADERS = ["요청ID", "신청일시", "신청사번", "신청자", "수령부서", "품목코드", "품명", "규격", "신청수량", "상태", "출고수량", "출고일시", "출고담당", "수령일시", "수령담당", "비고"]
STOCK_HEADERS = ["거래ID", "일시", "부서", "품목코드", "품명", "유형", "증감수량", "작업사번", "메모", "관련요청ID"]


def ensure_material_sheets():
    # Google Sheets already has employee/auth sheets. Do not modify them.
    get_ws("Material_Requests", REQ_HEADERS)
    get_ws("Material_StockEvents", STOCK_HEADERS)


@st.cache_data(ttl=12, show_spinner=False)
def load_requests_cached(_sheet_id: str):
    return rows_as_dicts(get_ws("Material_Requests", REQ_HEADERS), REQ_HEADERS)


@st.cache_data(ttl=12, show_spinner=False)
def load_stock_events_cached(_sheet_id: str):
    return rows_as_dicts(get_ws("Material_StockEvents", STOCK_HEADERS), STOCK_HEADERS)


def material_refresh():
    load_requests_cached.clear()
    load_stock_events_cached.clear()
    hira_search_cached.clear()


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
    return hira_search_cached(str(st.secrets['treatments']['spreadsheet_id']))


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
            balances[key] += int(row['출고수량'])
        except ValueError:
            raise ValueError('수령완료 자료의 출고수량을 확인하세요.')
        names[key] = row['품명']
    return balances, names


def material_save_requests(emp, department, cart, memo):
    if not cart:
        raise ValueError('신청할 품목을 추가해 주세요.')
    # Re-check role + allowed departments on the server before saving.
    fresh_emp = next((r for r in employees() if r['사번'] == emp['사번'] and is_active(r)), None)
    if fresh_emp is None or department not in allowed_departments(fresh_emp, employees()):
        raise PermissionError('신청할 부서에 대한 관리 권한이 없습니다.')
    created = now()
    group_id = uuid.uuid4().hex[:12].upper()
    rows = []
    for item in cart:
        source = material_catalog_lookup(item['코드'])
        rows.append([group_id + '-' + str(len(rows) + 1).zfill(3), created, emp['사번'], emp['이름'], department,
                     source['코드'], source['품명'], source['규격'], str(material_qty(item['수량'])),
                     '신청', '', '', '', '', '', str(memo)[:300]])
    api_action(get_ws('Material_Requests', REQ_HEADERS).append_rows, rows, value_input_option='RAW')
    material_refresh()
    return group_id


def material_set_request_status(request, new_status, actor, delivered_qty=None):
    if actor['권한'].upper() != 'MASTER' and new_status in ('출고', '반려'):
        raise PermissionError('출고 승인 및 반려는 MASTER만 가능합니다.')
    if new_status == '수령완료' and actor['권한'].upper() != 'MASTER':
        fresh = next((x for x in employees() if x['사번'] == actor['사번'] and is_active(x)), None)
        if fresh is None or request['수령부서'] not in allowed_departments(fresh, employees()):
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


def save_stock_event(emp, department, code, action, quantity, memo, *, target_total=None):
    fresh_data = employees()
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
    record = [uuid.uuid4().hex, now(), department, code, item['품명'], action, str(delta),
              emp['사번'], str(memo)[:300], '']
    api_action(get_ws('Material_StockEvents', STOCK_HEADERS).append_row, record, value_input_option='RAW')
    material_refresh()
    return before + delta


def material_selector(key_prefix, *, label='품목 검색'):
    catalog = get_hira_items()
    if catalog.empty:
        st.warning('심평원 품목 DB가 아직 없습니다. MASTER가 먼저 심평원 DB를 등록해 주세요.')
        return None
    keyword = st.text_input(label, key=key_prefix+'_term', placeholder='품목코드 · 품명 · 규격 · 분류 (일부 입력)')
    if not keyword.strip():
        st.caption('품목코드나 품명의 일부를 입력하면 목록에서 선택할 수 있습니다.')
        return None
    terms = [x.casefold() for x in keyword.split() if x]
    fields = ['코드','품명','규격','중분류']
    matrix = catalog[fields].fillna('').astype(str).agg(' '.join, axis=1).str.casefold()
    found = catalog.loc[matrix.map(lambda text: all(t in text for t in terms))].head(150)
    if found.empty:
        st.info('검색된 품목이 없습니다.')
        return None
    ids = found['코드'].tolist()
    selected_code = st.selectbox('검색 결과 선택 (최대 150건)', ids,
        format_func=lambda x: f"{x} | {str(found.loc[found['코드']==x, '품명'].iloc[0])[:65]}",
        key=key_prefix+'_selected')
    record = found.loc[found['코드'] == selected_code].iloc[0].to_dict()
    st.caption(f"{record['급여구분']} · {record['현재상태']} · {record['규격']}")
    if record['현재상태'] != '등재':
        st.warning('이 품목은 현재 등재 상태가 아닙니다. 실제 구매·청구 가능 여부를 확인하세요.')
    return record


def render_request_entry(emp, data):
    st.markdown('#### 물품 신청')
    depts = material_depts(emp, data)
    if not depts: return
    department = st.selectbox('신청·수령 부서', depts, key='req_department')
    item = material_selector('req_item')
    if item is not None:
        a,b = st.columns([1,2])
        with a: qty = st.number_input('신청 수량', min_value=1, max_value=1000000, value=1, step=1, key='req_qty')
        with b: st.write(''); st.write('')
        if st.button('선택 품목 신청목록에 추가', use_container_width=False):
            cart = st.session_state.setdefault('material_cart', [])
            found = next((r for r in cart if r['코드'] == item['코드']), None)
            if found: found['수량'] += int(qty)
            else: cart.append({'코드':item['코드'], '품명':item['품명'], '규격':item['규격'], '수량':int(qty)})
            st.rerun()
    cart = st.session_state.get('material_cart', [])
    if cart:
        st.markdown('**신청목록**')
        display = pd.DataFrame(cart)
        edited = st.data_editor(display, hide_index=True, use_container_width=True, num_rows='fixed',
            disabled=['코드','품명','규격'], column_config={'수량': st.column_config.NumberColumn('신청 수량',min_value=1, max_value=1000000,step=1)},key='material_cart_editor')
        st.session_state.material_cart = edited.to_dict('records')
        rem = st.selectbox('목록에서 제거할 품목', ['선택 안 함']+[r['코드'] for r in cart])
        if st.button('선택 품목 제거') and rem!='선택 안 함':
            st.session_state.material_cart = [r for r in cart if r['코드'] != rem]
            st.rerun()
        memo = st.text_input('신청 메모', max_chars=300)
        if st.button(f'물품 {len(cart)}종 신청 확정', type='primary'):
            try:
                group = material_save_requests(emp, department, st.session_state.material_cart, memo)
                st.session_state.material_cart = []
                st.success(f'신청 완료 · 신청번호 {group}')
                st.rerun()
            except Exception as exc: st.error(str(exc) if isinstance(exc,(ValueError, PermissionError)) else safe_spreadsheet_error(exc))


def visible_requests(emp, data, statuses=None):
    requests = all_requests()
    allowed = set(material_depts(emp,data))
    visible = [r for r in requests if emp['권한'].upper()=='MASTER' or r['수령부서'] in allowed]
    if statuses: visible = [r for r in visible if r['상태'] in statuses]
    return sorted(visible, key=lambda r: r['신청일시'], reverse=True)


def render_request_history(emp,data):
    st.markdown('#### 신청·출고·수령 현황')
    items=visible_requests(emp,data)
    if not items:
        st.info('현재 표시할 신청내역이 없습니다.'); return
    frame=pd.DataFrame(items)
    columns=['신청일시','요청ID','수령부서','품목코드','품명','신청수량','출고수량','상태','신청자','출고담당','수령담당']
    dept_choice=st.selectbox('부서 필터', ['전체']+sorted(set(frame['수령부서'])),key='history_dept')
    status_choice=st.selectbox('상태 필터', ['전체','신청','출고','수령완료','반려'],key='history_status')
    if dept_choice!='전체':frame=frame[frame['수령부서']==dept_choice]
    if status_choice!='전체':frame=frame[frame['상태']==status_choice]
    st.dataframe(frame[columns].head(1000),hide_index=True,use_container_width=True,height=460)
    st.caption('신청 → MASTER 출고 → 부서 수령확인 후 해당 부서 재고에 반영됩니다.')


def render_master_issue(emp,data):
    st.markdown('#### 신청 승인·출고 (MASTER)')
    items=visible_requests(emp,data, ['신청'])
    if not items:
        st.info('출고 대기 신청이 없습니다.'); return
    labels={r['요청ID']:f"{r['요청ID']} | {r['수령부서']} | {r['품명'][:45]} | {r['신청수량']}개" for r in items}
    rid=st.selectbox('출고 처리할 신청',list(labels),format_func=lambda x:labels[x],key='issue_id')
    row=next(r for r in items if r['요청ID']==rid)
    qty=st.number_input('실제 출고수량',min_value=1,max_value=max(1,int(row['신청수량'])),value=int(row['신청수량']),key='issue_qty')
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
    if not items:
        st.info('수령 확인 대기 물품이 없습니다.');return
    labels={r['요청ID']:f"{r['수령부서']} | {r['품명'][:50]} | 출고 {r['출고수량']}개 | {r['요청ID']}" for r in items}
    rid=st.selectbox('수령할 물품',list(labels),format_func=lambda x:labels[x],key='receive_id')
    row=next(r for r in items if r['요청ID']==rid)
    st.info('실제 수량을 확인하고 수령 완료를 누르면 해당 부서 재고가 증가합니다.')
    if st.button('수령 완료 · 재고 반영',type='primary',use_container_width=True):
        try:
            material_set_request_status(row,'수령완료',emp)
            st.success('수령완료 · 부서 재고에 반영되었습니다.'); st.rerun()
        except Exception as exc:st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


def render_stock_overview(emp,data):
    depts=material_depts(emp,data)
    if not depts:return
    balances,names=stock_by_department()
    show=[{'부서':dept,'품목코드':code,'품명':names.get((dept,code),''),'현재고':qty}
          for (dept,code),qty in balances.items() if dept in depts]
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
    b.metric('현재고 합계',f'{int(frame["현재고"].sum()):,}')
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
    st.metric('현재 등록 재고',f'{before:,}개')
    action=st.radio('작업 선택',['초기재고','사용','실사조정'],horizontal=True,key='stock_action')
    if action=='초기재고':
        st.caption('처음 시스템을 도입할 때 각 부서에서 실제 보유한 수량을 한 번 등록합니다.')
        qty=st.number_input('초기 보유 수량',min_value=0,max_value=1000000,value=0,step=1,key='initial_qty')
    elif action=='사용':
        qty=st.number_input('실제 사용 수량',min_value=1,max_value=1000000,value=1,step=1,key='used_qty')
    else:
        qty=st.number_input('실사 후 실제 보유 수량',min_value=0,max_value=1000000,value=max(0,before),step=1,key='count_qty')
    memo=st.text_input('사유 / 메모 (실사조정 시 필수)',max_chars=300,key='stock_note')
    if st.button('재고 변경 저장',type='primary',key='stock_write'):
        try:
            new=save_stock_event(emp,dept,code,action,qty,memo,target_total=int(qty) if action!='사용' else None)
            st.success(f'저장 완료 · 변경 후 재고 {new:,}개');st.rerun()
        except Exception as exc:st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


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
            fresh=employees()
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
                    records.append([uuid.uuid4().hex,now(),dept,r.품목코드,r.품명,'초기재고',str(delta),emp['사번'],'초기재고 일괄등록',''])
            if records:
                api_action(get_ws('Material_StockEvents',STOCK_HEADERS).append_rows,records,value_input_option='RAW')
            material_refresh()
            st.success(f'초기재고 입력 완료: {len(incoming):,}개 품목 (실제 수량 변경 {len(records):,}건)')
    except Exception as exc:
        st.error(str(exc) if isinstance(exc,(ValueError,PermissionError)) else safe_spreadsheet_error(exc))


def all_stock_events_cached_fresh():
    return rows_as_dicts(get_ws('Material_StockEvents',STOCK_HEADERS),STOCK_HEADERS)


def all_requests_cached_fresh():
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

def main():
    render_style()
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
    try:
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

    with st.sidebar:
        if BUILDING_FILE.is_file():
            # 병원 외관 사진은 작은 크기로만 표시하며 업무 메뉴 공간을 확보합니다.
            photo_left, photo, photo_right = st.columns([0.28, 1, 0.28])
            with photo:
                st.image(str(BUILDING_FILE), use_container_width=True)
        st.caption(f"{emp['이름']} · {emp['소속부서']} · {role}")
        st.divider()
        menu = ["물품관리 현황", "품목정보", "물품 신청", "물품 수령", "재고관리", "통계 및 보고서", "내 정보", "PIN 변경"]
        if role == "MASTER":
            menu.insert(3, "출고관리")
        if role == "MASTER":
            menu += ["직원 관리", "심평원 DB 업데이트"]
        choice = st.radio("업무 메뉴", menu, label_visibility="collapsed", key="material_nav")
        st.divider()
        if st.button("🔄 데이터 동기화", use_container_width=True, help="Google Sheets의 최신 직원·품목자료를 다시 읽습니다."):
            # Refresh of cache only; never pushes Google Drive source files automatically.
            material_refresh()
            st.session_state.pop("hira_files", None)
            st.session_state.pop("hira_import_preview", None)
            st.toast("동기화를 요청했습니다. 최신 Google Sheets 자료를 다시 읽습니다.")
            st.rerun()
        if st.button("로그아웃", use_container_width=True):
            sign_out()

    # 로그인 후 오른쪽 작업영역에는 로고·제목 헤더를 넣지 않습니다.
    if choice in ("물품관리 현황", "물품 신청", "물품 수령", "재고관리", "출고관리", "통계 및 보고서"):
        try:
            render_material_menu(choice, emp, data)
        except Exception as exc:
            LOGGER.exception('Material workflow failed')
            st.error(str(exc) if isinstance(exc, (ValueError, PermissionError)) else safe_spreadsheet_error(exc))
    elif choice == "품목정보":
        render_hira_search()
    elif choice == "내 정보":
        render_personal_info(emp, data)
    elif choice == "PIN 변경":
        render_pin_change(emp)
    elif choice == "직원 관리" and role == "MASTER":
        render_employee_admin(emp, data)
    elif choice == "심평원 DB 업데이트" and role == "MASTER":
        render_hira_import()

    st.caption("HISMEDI · 치료재료 관리 | PC용 · Google Sheets 기록 기반")


if __name__ == "__main__":
    main()
