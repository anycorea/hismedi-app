import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import gspread
import uuid
import base64
import copy
import hashlib
import json
import math
import threading
import hmac
import tempfile
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from vaccination_pdf import create_print_pdf, create_preview_pdf, pdf_to_png, default_settings, clean


# ============================================================
# 기본 설정
# ============================================================
st.set_page_config(page_title="예방접종 예진표", page_icon="💉", layout="wide")

TZ = ZoneInfo("Asia/Seoul")

SHEET_HEADERS = [
    "ID", "DATE", "성명", "주민번호", "성별", "생년월일", "외국인번호", "집전화", "휴대전화", "체중",
    "접종동의", "알림동의", "이상동의", "1", "1상세", "2", "2상세", "3", "3상세", "4", "5",
    "6", "6상세", "7", "8", "9", "9상세", "10", "11", "11상세", "작성자", "관계"
]

SETTING_HEADERS = ["MODE", "SECTION", "OFFSET_X_MM", "OFFSET_Y_MM", "SCALE_X", "SCALE_Y", "UPDATED_AT"]

SECTION_LABELS = {
    "global": "전체",
    "personal": "인적사항",
    "consent": "개인정보 동의",
    "questions": "문진 체크",
    "details": "상세입력",
    "writer": "작성자 · 관계",
    "date": "날짜"
}

SECTION_KEYS = list(SECTION_LABELS.keys())


# ============================================================
# 디자인
# ============================================================
st.markdown("""
<style>
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"],
    [data-testid="stToolbar"], [data-testid="stDecoration"],
    [data-testid="stStatusWidget"] { display: none !important; }

    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1.5rem !important;
        max-width: 1750px !important;
    }

    h1 { text-align: center; font-size: 2rem !important; margin-bottom: 0.3rem !important; }

    .patient-wrap { max-width: 760px; margin: 0 auto; }

    .form-description {
        text-align: center; color: #6b7280;
        margin-bottom: 1.5rem; line-height: 1.6;
    }

    .section-title {
        font-size: 1.25rem; font-weight: 700;
        margin-top: 2rem; margin-bottom: 0.8rem;
        padding-bottom: 0.45rem; border-bottom: 2px solid #333;
    }

    .question-text { font-weight: 600; line-height: 1.55; margin-bottom: 0.2rem; }
    .required { color: #d32f2f; font-weight: 700; }

    .notice-box {
        padding: 1rem; border: 1px solid #ddd;
        border-radius: 10px; background: #fafafa;
        color: #222222 !important;
        font-size: 0.92rem; line-height: 1.6;
        margin-bottom: 1rem;
    }

    .admin-header {
        padding: 0.2rem 0 0.8rem 0;
        border-bottom: 1px solid #e5e7eb;
        margin-bottom: 1rem;
    }

    .admin-title {
        font-size: 1.75rem; font-weight: 800;
        letter-spacing: -0.04em;
    }

    .admin-sub {
        color: #6b7280; margin-top: 0.15rem;
        font-size: 0.92rem;
    }

    .panel-title {
        font-size: 1.12rem; font-weight: 750;
        margin: 0.35rem 0 0.6rem 0;
    }

    .patient-count {
        display: inline-block;
        padding: 0.15rem 0.5rem;
        background: #f1f5f9;
        border-radius: 999px;
        font-size: 0.78rem;
        color: #475569;
        margin-left: 0.3rem;
    }

    .print-note {
        padding: 0.75rem 0.9rem;
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        color: #64748b;
        font-size: 0.84rem;
        margin-top: 0.6rem;
    }

    .adjust-help {
        padding: 0.7rem 0.85rem;
        background: #f8fafc;
        border-left: 4px solid #94a3b8;
        border-radius: 5px;
        color: #475569;
        font-size: 0.85rem;
        line-height: 1.55;
        margin-bottom: 0.8rem;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #e2e8f0;
        border-radius: 9px;
        overflow: hidden;
    }

    div[data-testid="stImage"] img {
        border: 1px solid #d8dee7;
        box-shadow: 0 4px 18px rgba(0,0,0,0.08);
        background: white;
    }

    div[data-testid="stRadio"] { margin-bottom: 0.25rem; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# Google Sheets
# ============================================================
def get_spreadsheet():
    # A connection belongs to one Streamlit session. Different patients can
    # submit concurrently without sharing an HTTP session or a global write lock.
    if "_sheet_connection" not in st.session_state:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
        client = gspread.authorize(credentials)
        client.set_timeout((5, 25))
        st.session_state["_sheet_connection"] = client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    return st.session_state["_sheet_connection"]


@st.cache_resource
def sheet_lock():
    # Only administrative settings need read/compare/write serialization.
    # This lock is process-local; it is not a distributed database transaction.
    return threading.RLock()


def get_worksheet():
    if "_patient_worksheet" not in st.session_state:
        st.session_state["_patient_worksheet"] = get_spreadsheet().worksheet(st.secrets["gsheet"]["worksheet_name"])
    return st.session_state["_patient_worksheet"]


def get_settings_worksheet():
    if "_settings_worksheet" not in st.session_state:
        st.session_state["_settings_worksheet"] = get_spreadsheet().worksheet("print_settings")
    return st.session_state["_settings_worksheet"]


def digits_only(value):
    return "".join(ch for ch in str(value) if ch in "0123456789")


def safe_float(value, default):
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def format_registration_number(value):
    digits = digits_only(value)
    if len(digits) == 13: return f"{digits[:6]}-{digits[6:]}"
    return digits


def format_phone(value):
    digits = digits_only(value)

    if not digits: return ""
    if len(digits) == 11: return f"{digits[:3]}-{digits[3:7]}-{digits[7:]}"
    if digits.startswith("02") and len(digits) == 10: return f"{digits[:2]}-{digits[2:6]}-{digits[6:]}"
    if digits.startswith("02") and len(digits) == 9: return f"{digits[:2]}-{digits[2:5]}-{digits[5:]}"
    if len(digits) == 10: return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"

    return digits


def install_input_masks_and_numeric_months():
    """브라우저에서 입력 중 하이픈 표시 + 달력의 영문 월을 숫자 월로 표시."""
    components.html(
        r"""
        <script>
        (function () {
            const doc = window.parent.document;
            if (window.parent.__vaccinationObserver) {
                window.parent.__vaccinationObserver.disconnect();
            }
            if (window.parent.__vaccinationTimer) {
                window.parent.clearTimeout(window.parent.__vaccinationTimer);
            }

            function digits(value) {
                return (value || "").replace(/\D/g, "");
            }

            function formatRrn(value) {
                const d = digits(value).slice(0, 13);
                if (d.length <= 6) return d;
                return d.slice(0, 6) + "-" + d.slice(6);
            }

            function formatPhone(value) {
                const d = digits(value).slice(0, 11);
                if (!d) return "";

                if (d.startsWith("02")) {
                    if (d.length <= 2) return d;
                    if (d.length <= 5) return d.slice(0, 2) + "-" + d.slice(2);
                    if (d.length <= 9) return d.slice(0, 2) + "-" + d.slice(2, 5) + "-" + d.slice(5);
                    return d.slice(0, 2) + "-" + d.slice(2, 6) + "-" + d.slice(6, 10);
                }

                if (d.length <= 3) return d;
                if (d.length <= 6) return d.slice(0, 3) + "-" + d.slice(3);
                if (d.length <= 10) return d.slice(0, 3) + "-" + d.slice(3, 6) + "-" + d.slice(6);
                return d.slice(0, 3) + "-" + d.slice(3, 7) + "-" + d.slice(7, 11);
            }

            const masks = [
                ["숫자 13자리 입력", formatRrn],
                ["외국인인 경우 숫자 13자리 입력", formatRrn],
                ["숫자만 입력", formatPhone],
                ["예: 010-1234-5678", formatPhone]
            ];

            function bindMasks() {
                for (const [placeholder, formatter] of masks) {
                    const input = doc.querySelector('input[placeholder="' + placeholder + '"]');
                    if (!input || input.dataset.vaccinationMask === "1") continue;
                    input.dataset.vaccinationMask = "1";
                    // Streamlit/React가 실제 변경값을 상태로 인식하도록
                    // native value setter + input 이벤트를 다시 전달한다.
                    input.addEventListener("input", function () {
                        if (input.dataset.vaccinationFormatting === "1") return;

                        const formatted = formatter(input.value);
                        if (input.value === formatted) return;

                        input.dataset.vaccinationFormatting = "1";

                        const setter = Object.getOwnPropertyDescriptor(
                            window.parent.HTMLInputElement.prototype, "value"
                        ).set;
                        setter.call(input, formatted);

                        input.dispatchEvent(new window.parent.InputEvent("input", {
                            bubbles: true,
                            inputType: "insertText",
                            data: null
                        }));

                        input.dataset.vaccinationFormatting = "0";
                    }, true);
                }
            }

            const monthMap = {
                January:"1월", February:"2월", March:"3월", April:"4월",
                May:"5월", June:"6월", July:"7월", August:"8월",
                September:"9월", October:"10월", November:"11월", December:"12월"
            };

            function numericMonths() {
                const walker = doc.createTreeWalker(doc.body, NodeFilter.SHOW_TEXT);
                let node;
                while ((node = walker.nextNode())) {
                    const text = (node.nodeValue || "").trim();
                    if (monthMap[text]) node.nodeValue = node.nodeValue.replace(text, monthMap[text]);
                    else {
                        for (const [eng, kor] of Object.entries(monthMap)) {
                            if (text.startsWith(eng + " ")) {
                                node.nodeValue = node.nodeValue.replace(eng, kor);
                                break;
                            }
                        }
                    }
                }
            }

            function applyAll() {
                bindMasks();
                numericMonths();
                // Scope the blue treatment to this one native Streamlit button.
                for (const button of doc.querySelectorAll('button')) {
                    if (button.textContent.trim() !== '🔄 새로고침') continue;
                    button.style.setProperty('background-color', '#1976d2', 'important');
                    button.style.setProperty('border-color', '#1565c0', 'important');
                    button.style.setProperty('color', '#ffffff', 'important');
                    for (const text of button.querySelectorAll('p, span')) {
                        text.style.setProperty('color', '#ffffff', 'important');
                    }
                }
            }

            applyAll();
            const observer = new MutationObserver(function () {
                window.parent.clearTimeout(window.parent.__vaccinationTimer);
                window.parent.__vaccinationTimer = window.parent.setTimeout(applyAll, 80);
            });
            window.parent.__vaccinationObserver = observer;
            observer.observe(doc.body, {childList:true, subtree:true});
        })();
        </script>
        """,
        height=0,
        width=0,
    )


def record_date(record):
    return clean(record.get("DATE"))[:10]


def record_time(record):
    try:
        return datetime.strptime(clean(record.get("DATE")), "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
    except Exception:
        return clean(record.get("DATE"))


def parse_sheet(values, required_headers):
    if not values:
        raise ValueError("시트의 첫 행에 열 제목이 필요합니다.")
    headers = [clean(v) for v in values[0]]
    nonempty = [h for h in headers if h]
    if len(nonempty) != len(set(nonempty)) or not set(required_headers).issubset(headers):
        raise ValueError("시트 열 제목이 누락되었거나 중복되었습니다.")
    return [dict(zip(headers, row + [""] * max(0, len(headers) - len(row))))
            for row in values[1:] if any(clean(v) for v in row)]


def get_records():
    # Patient records stay in this authenticated browser session, not a global cache.
    if "admin_records" not in st.session_state:
        values = get_worksheet().get_all_values()
        st.session_state.admin_records = parse_sheet(values, SHEET_HEADERS)
    return st.session_state.admin_records


def refresh_admin():
    st.session_state.pop("admin_printed_ids", None)
    for key in list(st.session_state):
        if key in ("admin_records", "pdf_bundle") or key.startswith(("print_settings_", "settings_base_", "adjust_")):
            del st.session_state[key]
    load_settings_rows.clear()
    st.session_state["table_generation"] = st.session_state.get("table_generation", 0) + 1


@st.cache_data(ttl=30, show_spinner=False)
def load_settings_rows():
    return parse_sheet(get_settings_worksheet().get_all_values(), SETTING_HEADERS)


def settings_from_rows(rows, mode):
    settings = default_settings()
    seen = set()
    for row in rows:
        if clean(row.get("MODE")) != mode:
            continue
        section = clean(row.get("SECTION"))
        if section not in settings:
            continue
        if section in seen:
            raise ValueError("출력 설정에 같은 영역이 중복되어 있습니다.")
        seen.add(section)
        for source, target, low, high in (
            ("OFFSET_X_MM", "x", -30.0, 30.0),
            ("OFFSET_Y_MM", "y", -30.0, 30.0),
            ("SCALE_X", "scale_x", 0.95, 1.05),
            ("SCALE_Y", "scale_y", 0.95, 1.05),
        ):
            if target.startswith("scale") and section != "global":
                continue
            value = safe_float(row.get(source), float("nan"))
            if not math.isfinite(value) or not low <= value <= high:
                raise ValueError("출력 설정 숫자 또는 허용 범위를 확인해주세요.")
            settings[section][target] = value
    return settings


def load_print_settings(mode):
    return settings_from_rows(load_settings_rows(), mode)


def save_print_settings(mode, settings):
    # Never clear the sheet. Change only this mode's rows in one batch.
    with sheet_lock():
        worksheet = get_settings_worksheet()
        values = worksheet.get_all_values()
        existing = parse_sheet(values, SETTING_HEADERS)
        current = settings_from_rows(existing, mode)
        baseline = st.session_state.get(f"settings_base_{mode}")
        if baseline is not None and current != baseline:
            raise ValueError("다른 관리자가 설정을 변경했습니다. 새로고침 후 다시 조정해주세요.")
        headers = [clean(v) for v in values[0]]
        now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        updates = []
        next_row = len(values) + 1
        for section in SECTION_KEYS:
            matching = [i for i, row in enumerate(values[1:], 2)
                        if clean(dict(zip(headers, row)).get("MODE")) == mode
                        and clean(dict(zip(headers, row)).get("SECTION")) == section]
            row_num = matching[0] if matching else next_row
            if not matching:
                next_row += 1
            item = settings[section]
            data = dict(zip(SETTING_HEADERS, [mode, section, item["x"], item["y"],
                item.get("scale_x", 1.0), item.get("scale_y", 1.0), now]))
            # Single-cell ranges preserve any additional site-specific columns.
            for col, header in enumerate(headers, 1):
                if header in data:
                    updates.append({"range": gspread.utils.rowcol_to_a1(row_num, col),
                                    "values": [[data[header]]]})
        if next_row - 1 > worksheet.row_count:
            worksheet.add_rows(next_row - 1 - worksheet.row_count)
        worksheet.batch_update(updates, value_input_option="RAW")
        load_settings_rows.clear()
        st.session_state[f"settings_base_{mode}"] = copy.deepcopy(settings)


def append_submission(record):
    worksheet = get_worksheet()
    headers = [clean(v) for v in worksheet.row_values(1)]
    parse_sheet([headers], SHEET_HEADERS)
    # Header order can differ; values must follow the actual sheet columns.
    row = [record.get(header, "") for header in headers]
    st.session_state["pending_submission"] = copy.deepcopy(record)
    try:
        worksheet.append_row(row, value_input_option="RAW", insert_data_option="INSERT_ROWS",
                             table_range="A1:" + gspread.utils.rowcol_to_a1(1, len(headers)))
    except gspread.exceptions.APIError as exc:
        # Definitive rejection: safe to let the patient try again after correction.
        # Timeouts and server errors remain uncertain and must be reconciled.
        if exc.response.status_code in (400, 401, 403, 404, 429):
            st.session_state.pop("pending_submission", None)
        raise
    st.session_state.pop("pending_submission", None)
    st.session_state.submitted = True


def reconcile_submission(record):
    # A timed-out append may already have succeeded. Never blindly append it again.
    worksheet = get_worksheet()
    headers = [clean(v) for v in worksheet.row_values(1)]
    parse_sheet([headers], SHEET_HEADERS)
    ids = worksheet.col_values(headers.index("ID") + 1)
    return record["ID"] in ids[1:]


def get_pdf_bundle(record, mode, settings):
    payload = json.dumps([record, mode, settings], sort_keys=True, ensure_ascii=False)
    key = hashlib.sha256(payload.encode()).hexdigest()
    cached = st.session_state.get("pdf_bundle")
    if cached is None or cached[0] != key:
        preview = create_preview_pdf(record, settings)
        printable = preview if mode == "blank" else create_print_pdf(record, mode, settings)
        cached = (key, pdf_to_png(preview), printable)
        st.session_state["pdf_bundle"] = cached
    return cached[1], cached[2]


def combine_print_settings(base, delta):
    combined = default_settings()

    for section in combined:
        combined[section]["x"] = float(base.get(section, {}).get("x", 0.0)) + float(delta.get(section, {}).get("x", 0.0))
        combined[section]["y"] = float(base.get(section, {}).get("y", 0.0)) + float(delta.get(section, {}).get("y", 0.0))

    combined["global"]["scale_x"] = float(base.get("global", {}).get("scale_x", 1.0)) * float(delta.get("global", {}).get("scale_x", 1.0))
    combined["global"]["scale_y"] = float(base.get("global", {}).get("scale_y", 1.0)) * float(delta.get("global", {}).get("scale_y", 1.0))
    return combined


def effective_print_settings(mode, current_settings):
    if mode == "blank": return current_settings
    return combine_print_settings(load_print_settings("blank"), current_settings)


def settings_state_key(mode):
    return f"print_settings_{mode}"


def ensure_settings_loaded(mode):
    key = settings_state_key(mode)

    if key not in st.session_state:
        try:
            st.session_state[key] = load_print_settings(mode)
            st.session_state[f"settings_base_{mode}"] = copy.deepcopy(st.session_state[key])
        except Exception:
            st.error("출력 설정을 읽지 못했습니다. print_settings 시트와 연결 상태를 확인한 뒤 새로고침해주세요.")
            st.stop()

    return st.session_state[key]


# ============================================================
# 문진
# ============================================================
def question(number, text, detail_text=None, forced_answer=None):
    st.markdown(f'<div class="question-text">{number}. {text} <span class="required">*</span></div>', unsafe_allow_html=True)

    if forced_answer is not None:
        st.radio(
            f"{number}번 답변", ["예", "아니오"],
            index=0 if forced_answer == "예" else 1,
            horizontal=True, key=f"q{number}_forced",
            disabled=True, label_visibility="collapsed"
        )

        st.divider()
        return forced_answer, ""

    answer = st.radio(
        f"{number}번 답변", ["예", "아니오"],
        index=None, horizontal=True,
        key=f"q{number}", label_visibility="collapsed"
    )

    detail = ""

    if detail_text and answer == "예":
        detail = st.text_input(detail_text, key=f"q{number}_detail", placeholder="상세 내용을 입력해주세요.")

    st.divider()

    return answer, detail


# ============================================================
# 1클릭 인쇄
# ============================================================
# Only IDs and click timestamps are stored here; patient data stays in its original sheet.
PRINT_HISTORY_HEADERS = ["RECORD_ID", "PRINT_REQUESTED_AT", "EVENT_ID"]


def get_print_history_worksheet():
    if "_print_history_worksheet" not in st.session_state:
        with sheet_lock():
            spreadsheet = get_spreadsheet()
            try:
                worksheet = spreadsheet.worksheet("print_history")
            except gspread.WorksheetNotFound:
                try:
                    worksheet = spreadsheet.add_worksheet(title="print_history", rows=1000, cols=3)
                except gspread.exceptions.APIError:
                    # Another worker may have created the sheet concurrently.
                    worksheet = spreadsheet.worksheet("print_history")
            headers = worksheet.row_values(1)
            if not headers:
                worksheet.update(range_name="A1:C1", values=[PRINT_HISTORY_HEADERS],
                                 value_input_option="RAW")
            elif headers != PRINT_HISTORY_HEADERS:
                raise ValueError("print_history 시트의 A1:C1 열 제목을 확인해주세요.")
            st.session_state["_print_history_worksheet"] = worksheet
    return st.session_state["_print_history_worksheet"]


def load_printed_ids():
    if "admin_printed_ids" not in st.session_state:
        ids = get_print_history_worksheet().col_values(1)
        st.session_state["admin_printed_ids"] = {clean(v) for v in ids[1:] if clean(v)}
    return st.session_state["admin_printed_ids"]


def save_print_event(event):
    # Append avoids read/modify/write races between administrators.
    # A duplicate after an uncertain timeout is harmless for the printed-ID set.
    get_print_history_worksheet().append_row(
        [event["record_id"], event["requested_at"], event["token"]],
        value_input_option="RAW", insert_data_option="INSERT_ROWS", table_range="A1:C1")


def accept_print_event(event, record_id):
    if not isinstance(event, dict) or event.get("record_id") != record_id:
        return False
    token = event.get("token")
    if not isinstance(token, str) or not 1 <= len(token) <= 100:
        return False
    seen = st.session_state.setdefault("print_seen_events", set())
    if token in seen:
        return False
    seen.add(token)
    item = {"record_id": record_id, "token": token,
            "requested_at": datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")}
    st.session_state.setdefault("print_local_ids", set()).add(record_id)
    pending = st.session_state.setdefault("print_pending_events", {})
    pending[token] = item
    try:
        save_print_event(item)
    except Exception:
        # The print operation can continue; show an explicit persistent save warning.
        pass
    else:
        pending.pop(token, None)
        if "admin_printed_ids" in st.session_state:
            st.session_state["admin_printed_ids"].add(record_id)
    return True


def retry_print_history():
    pending = st.session_state.setdefault("print_pending_events", {})
    for token, item in list(pending.items()):
        try:
            save_print_event(item)
        except Exception:
            break
        else:
            pending.pop(token, None)
            if "admin_printed_ids" in st.session_state:
                st.session_state["admin_printed_ids"].add(item["record_id"])


def admin_instructions():
    st.markdown("""**[설명]**

- 진행순서 : 날짜 확인(필요시 선택) → 새로고침 → 접종자 선택 → 용지 선택(확인) → 바로 인쇄
- 새로고침 버튼은 수시로 클릭해도 무방합니다.
- 바로 인쇄 : 클릭 이후 접종자 리스트에서 배경색이 회색처리되나 이후에도 선택 출력이 가능합니다.
""")
    st.caption("회색은 ‘바로 인쇄’ 클릭 기록입니다. 인쇄창에서 취소해도 유지됩니다. 다른 관리자의 인쇄 기록은 새로고침하면 반영됩니다.")


# A small, bidirectional component: no extra package or separately uploaded HTML needed.
# textContent is used for all patient fields, preventing HTML injection.
ADMIN_COMPONENT_HTML = r"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8"><style>
*{box-sizing:border-box}body{margin:0;font-family:Arial,'Malgun Gothic',sans-serif;color:#313744}
button{font:inherit;cursor:pointer}button:focus-visible,tr:focus-visible{outline:3px solid #1976d2;outline-offset:-3px}
.scroll{height:500px;overflow:auto;border:1px solid #e2e8f0;border-radius:9px;background:#fff}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:14px;table-layout:fixed}
th{position:sticky;top:0;background:#f7f8fa;color:#667085;text-align:left;font-weight:400;z-index:1}
th,td{padding:10px 7px;border-bottom:1px solid #e5e7eb;border-right:1px solid #e5e7eb;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;height:39px}
th:last-child,td:last-child{border-right:0}th button{background:none;border:0;padding:0;color:inherit;font:inherit;width:100%;text-align:left}
th:first-child{width:32px}th:nth-child(2){width:64px}th:nth-child(4){width:104px}th:nth-child(5){width:62px}
tr.patient{cursor:pointer}tr.patient td{background:#fff}tr.patient:hover td{background:#eff6ff}
tr.patient.selected td{background:#fff0f0}tr.patient.printed td,tr.patient.printed:hover td,tr.patient.printed.selected td{background:#dedede;color:#30343b}
tr.patient.selected td:first-child{box-shadow:inset 3px 0 #ff4b4b}input[type=checkbox]{accent-color:#ff4b4b;pointer-events:none;margin:0;width:17px;height:17px}
#print{width:100%;height:48px;border:0;border-radius:8px;background:#ff4b4b;color:white;font-size:16px;font-weight:700}
#print:hover{background:#e94040}#print:disabled{opacity:.55;cursor:wait}#message{font-size:12px;color:#b91c1c}
</style></head><body><div id="root"></div><script>
let args={}, kind='', sortField='', sortDirection=1, selected='', busy=false;
const root=document.getElementById('root');
const send=(type,data={})=>window.parent.postMessage({isStreamlitMessage:true,type,...data},'*');
const value=v=>send('streamlit:setComponentValue',{value:v,dataType:'json'});
const height=h=>send('streamlit:setFrameHeight',{height:h});
function paintSelection(){
 for(const row of document.querySelectorAll('tr.patient')){
  const active=row.dataset.id===selected;
  row.classList.toggle('selected',active);row.setAttribute('aria-selected',String(active));
  row.querySelector('input').checked=active;
 }
}
function drawTable(){
 const old=document.querySelector('.scroll'); const scroll=old?old.scrollTop:0;
 root.replaceChildren();
 const box=document.createElement('div');box.className='scroll';
 const table=document.createElement('table');table.setAttribute('aria-label','접종자 목록');
 const head=document.createElement('thead'),hr=document.createElement('tr');
 const columns=[['',''],['time','시간'],['name','성명'],['birth','생년월일'],['relation','관계']];
 for(const [field,label] of columns){
  const th=document.createElement('th');
  if(field){const b=document.createElement('button');b.textContent=label+(sortField===field?(sortDirection===1?' ↑':' ↓'):'');
   b.onclick=()=>{sortDirection=sortField===field?-sortDirection:1;sortField=field;drawTable()};th.appendChild(b);
  }else{th.setAttribute('aria-label','선택')}
  hr.appendChild(th);
 }
 head.appendChild(hr);table.appendChild(head);
 const body=document.createElement('tbody'),rows=[...(args.rows||[])];
 if(sortField)rows.sort((a,b)=>String(a[sortField]).localeCompare(String(b[sortField]),'ko')*sortDirection);
 for(const data of rows){
  const tr=document.createElement('tr');tr.className='patient'+(data.printed?' printed':'');tr.dataset.id=data.id;tr.tabIndex=0;
  if(data.printed)tr.title='바로 인쇄 클릭 기록 있음 · 다시 인쇄할 수 있습니다';
  const td=document.createElement('td'),check=document.createElement('input');check.type='checkbox';check.tabIndex=-1;
  check.setAttribute('aria-label',data.name+' 선택');td.appendChild(check);tr.appendChild(td);
  for(const [field] of columns.slice(1)){const cell=document.createElement('td');cell.textContent=data[field];cell.title=data[field];tr.appendChild(cell)}
  const choose=()=>{selected=data.id;paintSelection();value({record_id:data.id})};
  tr.onclick=choose;tr.onkeydown=e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();choose()}};
  body.appendChild(tr);
 }
 table.appendChild(body);box.appendChild(table);root.appendChild(box);box.scrollTop=scroll;paintSelection();height(502);
}
function drawPrint(){
 if(document.getElementById('print'))return; // Preserve the PDF iframe during Streamlit reruns.
 const button=document.createElement('button');button.id='print';button.textContent='🖨 바로 인쇄';
 const message=document.createElement('div');message.id='message';root.append(button,message);height(58);
 button.onclick=()=>{
  if(busy||!args.pdf)return;
  const snapshot={...args};busy=true;button.disabled=true;message.textContent='';
  try{
   const binary=atob(snapshot.pdf),bytes=Uint8Array.from(binary,c=>c.charCodeAt(0));
   const url=URL.createObjectURL(new Blob([bytes],{type:'application/pdf'}));
   const frame=document.createElement('iframe');
   Object.assign(frame.style,{position:'fixed',right:'0',bottom:'0',width:'1px',height:'1px',border:'0'});
   let disposed=false;
   const cleanup=()=>{if(disposed)return;disposed=true;frame.remove();URL.revokeObjectURL(url)};
   const unlock=()=>{busy=false;button.disabled=false};
   // Keep the original PDF-in-iframe printing method. The component's stable key
   // keeps this frame alive when the server updates the grey row.
   frame.onload=()=>setTimeout(()=>{
    try{frame.contentWindow.addEventListener('afterprint',()=>{unlock();setTimeout(cleanup,1000)},{once:true});
        frame.contentWindow.focus();frame.contentWindow.print();}
    catch(error){window.open(url,'_blank');message.textContent='인쇄창이 열리지 않으면 팝업 차단을 확인해주세요.';height(85)}
    finally{unlock()}
   },700);
   frame.src=url;document.body.appendChild(frame);
   const token=Date.now().toString(36)+'-'+Math.random().toString(36).slice(2);
   value({record_id:snapshot.record_id,token});
   // Do not destroy a long-lived print dialog on an arbitrary timeout.
   window.addEventListener('pagehide',cleanup,{once:true});
   setTimeout(unlock,15000);
  }catch(error){busy=false;button.disabled=false;message.textContent='인쇄 준비에 실패했습니다. 다시 시도해주세요.';height(85)}
 };
}
window.addEventListener('message',event=>{
 if(event.source!==window.parent||!event.data||event.data.type!=='streamlit:render')return;
 args=event.data.args||{};
 if(kind!==args.kind){root.replaceChildren();kind=args.kind}
 if(kind==='table'){
  selected=args.selected_id||'';drawTable();
 }else{drawPrint()}
});
send('streamlit:componentReady',{apiVersion:1});
</script></body></html>"""


@st.cache_resource(show_spinner=False)
def admin_component_directory():
    directory = tempfile.TemporaryDirectory(prefix="vaccination_admin_")
    Path(directory.name, "index.html").write_text(ADMIN_COMPONENT_HTML, encoding="utf-8")
    return directory  # Retain the object so the directory stays alive for the server lifetime.


def admin_component(**kwargs):
    component = components.declare_component("vaccination_admin_controls", path=admin_component_directory().name)
    return component(**kwargs)


def print_button(pdf_data, record_id, mode):
    return admin_component(kind="print", pdf=base64.b64encode(pdf_data).decode(),
                           record_id=record_id, mode=mode, default=None, key="admin_print_control")


# ============================================================
# 관리자 출력 보정 UI
# ============================================================
def print_adjustment_ui(mode):
    settings = ensure_settings_loaded(mode)

    st.markdown(
        """
        <div class="adjust-help">
        <b>출력 위치 조정</b><br>
        빈 용지는 기준값을 조정합니다. 양식 용지는 <b>저장된 빈 용지 설정을 기본값으로 상속</b>하고 여기 값만 추가 보정합니다.<br>
        +좌우 = 오른쪽 / -좌우 = 왼쪽 · +상하 = 위 / -상하 = 아래
        </div>
        """,
        unsafe_allow_html=True
    )

    section_label = st.selectbox(
        "조정할 영역",
        list(SECTION_LABELS.values()),
        key=f"adjust_section_{mode}"
    )

    section = next(key for key, value in SECTION_LABELS.items() if value == section_label)
    current = settings[section]

    c1, c2 = st.columns(2)

    with c1:
        new_x = st.number_input(
            "좌우 이동 (mm)",
            min_value=-30.0, max_value=30.0,
            value=float(current.get("x", 0.0)),
            step=0.5,
            key=f"adjust_x_{mode}_{section}"
        )

    with c2:
        new_y = st.number_input(
            "상하 이동 (mm)",
            min_value=-30.0, max_value=30.0,
            value=float(current.get("y", 0.0)),
            step=0.5,
            key=f"adjust_y_{mode}_{section}"
        )

    settings[section]["x"] = new_x
    settings[section]["y"] = new_y

    if section == "global":
        c3, c4 = st.columns(2)

        with c3:
            scale_x_percent = st.number_input(
                "가로 간격 (%)",
                min_value=95.0, max_value=105.0,
                value=float(current.get("scale_x", 1.0)) * 100.0,
                step=0.1,
                key=f"adjust_scale_x_{mode}"
            )

        with c4:
            scale_y_percent = st.number_input(
                "세로 간격 (%)",
                min_value=95.0, max_value=105.0,
                value=float(current.get("scale_y", 1.0)) * 100.0,
                step=0.1,
                key=f"adjust_scale_y_{mode}"
            )

        settings["global"]["scale_x"] = scale_x_percent / 100.0
        settings["global"]["scale_y"] = scale_y_percent / 100.0

    b1, b2 = st.columns(2)

    with b1:
        if st.button("💾 현재값 저장", use_container_width=True, type="primary", key=f"save_settings_{mode}"):
            try:
                save_print_settings(mode, settings)
                st.success("출력 위치 설정을 저장했습니다.")
            except Exception as e:
                st.error("출력 위치 설정 저장에 실패했습니다.")
                st.exception(e)

    with b2:
        if st.button("↺ 전체 초기화", use_container_width=True, key=f"reset_settings_{mode}"):
            reset_values = default_settings()
            try:
                save_print_settings(mode, reset_values)
            except Exception as e:
                st.error(f"설정 초기화에 실패했습니다: {type(e).__name__}")
                st.stop()
            st.session_state[settings_state_key(mode)] = reset_values

            for key in list(st.session_state.keys()):
                if key.startswith("adjust_") and mode in key:
                    del st.session_state[key]

            st.rerun()

    return settings


# ============================================================
# 관리자 화면
# ============================================================
def admin_page():
    st.markdown("""
    <div class="admin-header">
        <div class="admin-title">💉 예방접종 관리자</div>
        <div class="admin-sub">접종 대상자 확인 · 예진표 미리보기 · 인쇄</div>
    </div>
    """, unsafe_allow_html=True)

    # --------------------------------------------------------
    # 로그인 - Enter 가능
    # --------------------------------------------------------
    if not st.session_state.get("admin_authenticated", False):
        c1, c2, c3 = st.columns([1, 1.2, 1])

        with c2:
            st.markdown("### 관리자 로그인")

            with st.form("admin_login_form"):
                password = st.text_input("비밀번호", type="password", placeholder="관리자 비밀번호")
                login = st.form_submit_button("로그인", type="primary", use_container_width=True)

            if login:
                if hmac.compare_digest(password.encode(), str(st.secrets["admin"]["password"]).encode()):
                    st.session_state.admin_authenticated = True
                    st.rerun()
                else:
                    st.error("비밀번호가 올바르지 않습니다.")

        st.stop()

    if "admin_date" not in st.session_state:
        st.session_state.admin_date = datetime.now(TZ).date()

    # 관리자 달력도 영문 월 대신 숫자 월로 표시합니다.
    install_input_masks_and_numeric_months()

    # --------------------------------------------------------
    # PC 2단 구성
    # --------------------------------------------------------
    left, right = st.columns([0.78, 2.22], gap="large")

    # ========================================================
    # 왼쪽
    # ========================================================
    with left:
        st.markdown('<div class="panel-title">접수 관리</div>', unsafe_allow_html=True)

        selected_date = st.date_input("접수일자", key="admin_date", format="YYYY-MM-DD")

        b1, b2 = st.columns(2)

        with b1:
            if st.button("🔄 새로고침", use_container_width=True, on_click=refresh_admin):
                # on_click already invalidated the session snapshot before this run.
                pass

        with b2:
            if st.button("로그아웃", use_container_width=True):
                st.session_state.clear()
                st.rerun()

        try:
            all_records = get_records()
        except Exception as e:
            st.error("접수 데이터를 불러오지 못했습니다.")
            st.exception(e)
            st.stop()

        date_text = selected_date.strftime("%Y-%m-%d")
        records = [r for r in all_records if record_date(r) == date_text]
        records.reverse()

        st.markdown(
            f'<div class="panel-title">접종자 <span class="patient-count">{len(records)}명</span></div>',
            unsafe_allow_html=True
        )

        if not records:
            st.info("선택한 날짜에 접수된 예진표가 없습니다.")
            admin_instructions()
            st.stop()

        try:
            printed_ids = set(load_printed_ids())
        except Exception:
            printed_ids = set()
            st.warning("인쇄 기록을 불러오지 못했습니다. 회색 표시가 정확하지 않을 수 있습니다. 새로고침해주세요.")
        printed_ids.update(st.session_state.get("print_local_ids", set()))

        if st.session_state.get("print_pending_events"):
            st.warning("인쇄 클릭 기록을 구글시트에 저장하지 못했습니다. 현재 화면에는 회색으로 표시되지만 다른 관리자에게는 아직 반영되지 않습니다. 로그아웃 전에 저장을 재시도해주세요.")
            if st.button("인쇄 기록 저장 재시도", key="retry_print_history"):
                retry_print_history()
                st.rerun()

        by_id = {clean(record.get("ID")): record for record in records}
        if "" in by_id or len(by_id) != len(records):
            st.error("접수 ID가 비어 있거나 중복되어 있습니다. 구글시트의 ID를 확인해주세요.")
            admin_instructions()
            st.stop()

        table_key = f"patients_{date_text}_{st.session_state.get('table_generation', 0)}"
        previous_event = st.session_state.get(table_key)
        selected_id = previous_event.get("record_id", "") if isinstance(previous_event, dict) else ""
        table_rows = [{
            "id": clean(record.get("ID")), "time": record_time(record),
            "name": clean(record.get("성명"))[:8], "birth": clean(record.get("생년월일")),
            "relation": clean(record.get("관계"))[:6],
            "printed": clean(record.get("ID")) in printed_ids
        } for record in records]
        event = admin_component(kind="table", rows=table_rows,
                                selected_id=selected_id, default=None, key=table_key)
        admin_instructions()
        selected_id = event.get("record_id", "") if isinstance(event, dict) else ""
        if selected_id not in by_id:
            st.caption("↑ 접종자를 선택해주세요.")
            st.stop()
        selected = by_id[selected_id]

        st.success(
            f"선택 · {clean(selected.get('성명'))} / "
            f"{clean(selected.get('생년월일'))} / "
            f"{clean(selected.get('관계'))}"
        )

    # ========================================================
    # 오른쪽
    # ========================================================
    with right:
        head1, head2 = st.columns([1.5, 1])

        with head1:
            st.markdown('<div class="panel-title">예진표 미리보기</div>', unsafe_allow_html=True)

        with head2:
            mode_label = st.radio(
                "인쇄 용지",
                ["빈 용지", "양식 용지"],
                horizontal=True,
                label_visibility="collapsed"
            )

        mode = "blank" if mode_label == "빈 용지" else "preprinted"

        # ----------------------------------------------------
        # 출력 위치 조정
        # ----------------------------------------------------
        with st.expander("⚙️ 출력 위치 조정", expanded=False):
            settings = print_adjustment_ui(mode)

        # ----------------------------------------------------
        # PDF / 미리보기 생성
        # ----------------------------------------------------
        try:
            effective_settings = effective_print_settings(mode, settings)
            preview_png, print_pdf = get_pdf_bundle(selected, mode, effective_settings)

        except Exception as e:
            st.error("예진표 생성 중 오류가 발생했습니다.")
            st.exception(e)
            st.stop()

        preview_col, print_col = st.columns([1.55, 0.65], gap="large")

        # ----------------------------------------------------
        # 미리보기
        # ----------------------------------------------------
        with preview_col:
            st.image(preview_png, use_column_width=True)

        # ----------------------------------------------------
        # 인쇄
        # ----------------------------------------------------
        with print_col:
            st.markdown("### 인쇄")

            st.write(f"**접종자**  \n{clean(selected.get('성명'))}")
            st.write(f"**접수시간**  \n{record_time(selected)}")
            st.write(f"**용지**  \n{'빈 A4 용지' if mode == 'blank' else '미리 출력된 양식 용지'}")

            st.divider()

            print_event = print_button(print_pdf, clean(selected.get("ID")), mode)
            if accept_print_event(print_event, clean(selected.get("ID"))):
                st.rerun()

            if mode == "blank":
                st.markdown(
                    '<div class="print-note">빈 A4에 <b>원본 양식 + 입력 데이터</b>가 함께 인쇄됩니다.</div>',
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    '<div class="print-note">화면은 완성된 예진표를 보여주지만 실제 종이에는 <b>입력 데이터만</b> 인쇄됩니다.</div>',
                    unsafe_allow_html=True
                )

            st.caption("프린터 설정은 A4 / 실제 크기 100%를 권장합니다.")

    st.stop()


# ============================================================
# 관리자 진입
# ============================================================
if st.query_params.get("admin") == "1":
    admin_page()


# ============================================================
# 환자 화면
# ============================================================
st.markdown('<div class="patient-wrap">', unsafe_allow_html=True)

if st.session_state.get("submitted", False):
    st.title("💉 예방접종 예진표")
    st.success("예진표가 정상적으로 제출되었습니다.")
    st.write("작성하신 내용이 접수되었습니다. 접종 전 의료진의 안내에 따라 진료 및 예진을 받아주세요.")

    if st.button("새 예진표 작성", use_container_width=True):
        st.session_state.clear()
        st.rerun()

    st.stop()


if st.session_state.get("pending_submission"):
    pending = st.session_state["pending_submission"]
    st.warning("제출 결과를 확인하지 못했습니다. 중복 접수를 막기 위해 추가 제출을 멈췄습니다.")
    st.caption(f"접수 확인번호: {pending['ID']}")
    st.write("아래 버튼으로 저장 여부를 확인해주세요. 계속 확인되지 않으면 이 화면을 직원에게 보여주세요. 새로 작성하기 전에 기존 접수 여부를 확인해야 합니다.")
    if st.button("저장 여부 다시 확인", type="primary"):
        try:
            found = reconcile_submission(pending)
        except Exception:
            st.error("연결되지 않습니다. 잠시 후 확인하거나 직원에게 문의해주세요.")
        else:
            if found:
                st.session_state.pop("pending_submission", None)
                st.session_state.submitted = True
                st.rerun()
            else:
                st.info("아직 접수가 확인되지 않습니다. 직원에게 접수 확인번호를 알려주세요.")
    st.stop()


# ============================================================
# 제목 / 개인정보
# ============================================================
st.title("💉 예방접종 예진표")

st.markdown(
    '<div class="form-description">안전한 예방접종을 위하여 아래 질문사항을 잘 읽어보시고<br>정확하게 작성하여 주시기 바랍니다.</div>',
    unsafe_allow_html=True
)

st.markdown('<div class="section-title">개인정보 처리 안내</div>', unsafe_allow_html=True)

st.markdown("""
<div class="notice-box">
예방접종 업무를 위하여 주민등록번호 등 개인정보 및 민감정보가 수집될 수 있습니다.<br><br>
입력하신 정보는 예방접종 예진 및 관련 업무를 위하여 사용됩니다.<br>
아래 내용을 확인한 후 예진표를 작성해 주세요.
</div>
""", unsafe_allow_html=True)

privacy_confirm = st.checkbox("위 개인정보 처리 안내를 확인하였습니다.")

if not privacy_confirm:
    st.info("개인정보 처리 안내 확인 후 예진표를 작성할 수 있습니다.")
    st.stop()


# 입력 마스크와 달력 월 숫자 표시는 환자 화면에서만 적용합니다.
install_input_masks_and_numeric_months()


# ============================================================
# 인적사항
# ============================================================
st.markdown('<div class="section-title">접종 대상자 인적 사항</div>', unsafe_allow_html=True)

name = st.text_input("성명 *", placeholder="접종 대상자의 성명을 입력해주세요.")
rrn = st.text_input("주민등록번호", placeholder="숫자 13자리 입력", max_chars=14)
gender = st.radio("성별 *", ["남", "여"], index=None, horizontal=True)

birth_date = st.date_input(
    "실제 생년월일 *",
    value=None,
    min_value=date(1900, 1, 1),
    max_value=datetime.now(TZ).date(),
    format="YYYY-MM-DD"
)

foreigner_no = st.text_input("외국인 등록번호", placeholder="외국인인 경우 숫자 13자리 입력", max_chars=14)

col1, col2 = st.columns(2)

with col1:
    home_phone = st.text_input("전화번호 (집)", placeholder="숫자만 입력", max_chars=13)

with col2:
    mobile_phone = st.text_input("휴대전화 *", placeholder="예: 010-1234-5678", max_chars=13)

weight = st.number_input(
    "체중 (kg)",
    min_value=0.0,
    max_value=300.0,
    value=None,
    step=0.1,
    placeholder="체중을 입력해주세요."
)


# ============================================================
# 동의
# ============================================================
st.markdown('<div class="section-title">예방접종 업무를 위한 동의 사항</div>', unsafe_allow_html=True)

st.markdown("**1. 예방접종 내역 사전 확인**  \n예방접종을 하기 전에 접종 대상자의 예방접종 내역을 예방접종통합관리시스템으로 사전 확인하는 것에 동의합니다.")
vaccination_consent = st.radio("접종동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown("**2. 다음 접종 및 완료 여부 알림**  \n예방접종의 다음 접종 및 완료 여부에 관한 정보를 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
notification_consent = st.radio("알림동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")

st.divider()

st.markdown("**3. 예방접종 후 이상반응 알림**  \n예방접종 후 이상반응 발생 여부와 관련된 알림을 문자 및 모바일앱으로 수신하는 것에 동의합니다.")
adverse_consent = st.radio("이상동의", ["예", "아니오"], index=None, horizontal=True, label_visibility="collapsed")


# ============================================================
# 문진
# ============================================================
st.markdown('<div class="section-title">접종 대상자에 대한 확인 사항</div>', unsafe_allow_html=True)
st.caption("각 질문에 반드시 '예' 또는 '아니오'를 선택해주세요.")

q1, q1_detail = question(1, "최근 1개월 이내에 받은 예방접종이 있습니까?", "그렇다면 예방접종명을 적어주세요.")

q2, q2_detail = question(
    2,
    "과거에 예방접종 후 이상반응이 나타나서 치료를 받은 적이 있습니까?",
    "그렇다면 이상반응과 해당 예방접종명을 적어주세요."
)

q3, q3_detail = question(3, "오늘 아픈 곳이 있습니까?", "그렇다면 아픈 증상을 적어주세요.")

if gender == "남":
    q4, _ = question(
        4,
        "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?",
        forced_answer="아니오"
    )
else:
    q4, _ = question(4, "(여성) 현재 임신 중이거나 다음 한 달 동안 임신할 가능성이 있습니까?")

q5, _ = question(
    5,
    "약이나 음식물(예: 계란) 혹은 백신 접종으로 두드러기, 알레르기 증상"
    "(예: 발진, 아나필락시스: 쇼크, 호흡곤란, 의식소실, 입술/입안의 부종 등)을 보인 적이 있습니까?"
)

q6, q6_detail = question(6, "암, 백혈병 혹은 면역계 질환이 있습니까?", "그렇다면 병명을 적어주세요.")
q7, _ = question(7, "최근 3개월 이내에 스테로이드제, 항암제, 방사선 치료를 받은 적이 있습니까?")
q8, _ = question(8, "최근 1년 동안 수혈을 받았거나 면역글로불린을 투여받은 적이 있습니까?")

q9, q9_detail = question(
    9,
    "(코로나19) 혈액응고장애를 앓고 있거나, 항응고제를 복용 중이십니까?",
    "그렇다면 질환명 또는 약 종류를 적어주세요."
)

q10, _ = question(10, "경련을 한 적이 있거나 기타 뇌신경계 질환(예: 길랭-바레 증후군 포함)이 있습니까?")

q11, q11_detail = question(
    11,
    "그 외 선천성 기형, 천식 및 폐질환, 심장질환, 신장질환, 간질환, 당뇨 및 내분비 질환, "
    "혈액 질환(혈액응고장애 외)으로 진찰 받거나 치료 받은 일이 있습니까?",
    "그렇다면 병명을 적어주세요."
)


# ============================================================
# 작성자
# ============================================================
st.markdown('<div class="section-title">작성자 확인</div>', unsafe_allow_html=True)

st.write("의사의 진찰결과와 이상반응에 대한 설명을 듣고 예방접종을 하겠습니다.")

writer_default_label = clean(name) if clean(name) else "접종 대상자 본인"
writer_choice = st.selectbox(
    "본인(법정대리인, 보호자) 성명 *",
    [writer_default_label, "직접입력"],
    index=0,
    key="writer_choice"
)

if writer_choice == "직접입력":
    writer = st.text_input(
        "작성자 성명 직접입력 *",
        placeholder="법정대리인 또는 보호자 성명을 입력해주세요.",
        key="writer_manual"
    )
else:
    writer = clean(name)

relationship_choice = st.selectbox(
    "접종 대상자와의 관계 *",
    ["본인", "부", "모", "배우자", "직접입력"],
    index=0,
    key="relationship_choice"
)

if relationship_choice == "직접입력":
    relationship = st.text_input(
        "관계 직접입력 *",
        placeholder="접종 대상자와의 관계를 입력해주세요.",
        key="relationship_manual"
    )
else:
    relationship = relationship_choice

final_confirm = st.checkbox("위 내용을 확인하였으며 작성한 내용이 사실과 다름없음을 확인합니다.")


# ============================================================
# 제출
# ============================================================
submitted = st.button("예진표 제출", use_container_width=True, type="primary")

if submitted:
    errors = []

    formatted_rrn = format_registration_number(rrn)
    formatted_foreigner_no = format_registration_number(foreigner_no)
    formatted_home_phone = format_phone(home_phone)
    formatted_mobile_phone = format_phone(mobile_phone)

    if not clean(name): errors.append("성명을 입력해주세요.")
    if gender is None: errors.append("성별을 선택해주세요.")
    if birth_date is None: errors.append("실제 생년월일을 입력해주세요.")
    if not clean(mobile_phone): errors.append("휴대전화를 입력해주세요.")

    if clean(rrn) and len(digits_only(rrn)) != 13:
        errors.append("주민등록번호 숫자 13자리를 정확히 입력해주세요.")

    if clean(foreigner_no) and len(digits_only(foreigner_no)) != 13:
        errors.append("외국인 등록번호 숫자 13자리를 정확히 입력해주세요.")

    mobile_digits = digits_only(mobile_phone)

    if clean(mobile_phone) and len(mobile_digits) not in (10, 11):
        errors.append("휴대전화 번호를 정확히 입력해주세요.")

    if vaccination_consent is None:
        errors.append("예방접종 내역 사전 확인 동의 여부를 선택해주세요.")

    if notification_consent is None:
        errors.append("다음 접종 및 완료 여부 알림 동의 여부를 선택해주세요.")

    if adverse_consent is None:
        errors.append("예방접종 후 이상반응 알림 동의 여부를 선택해주세요.")

    answers = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11]

    for number, answer in enumerate(answers, 1):
        if answer is None:
            errors.append(f"{number}번 확인사항에 답변해주세요.")

    details = [
        (1, q1, q1_detail),
        (2, q2, q2_detail),
        (3, q3, q3_detail),
        (6, q6, q6_detail),
        (9, q9, q9_detail),
        (11, q11, q11_detail)
    ]

    for number, answer, detail in details:
        if answer == "예" and not clean(detail):
            errors.append(f"{number}번 질문의 상세 내용을 입력해주세요.")

    if not clean(writer):
        errors.append("작성자 성명을 입력해주세요.")

    if not clean(relationship):
        errors.append("접종 대상자와의 관계를 입력해주세요.")

    if not final_confirm:
        errors.append("최종 확인 항목에 체크해주세요.")

    if errors:
        st.error(
            "입력하지 않았거나 확인이 필요한 항목이 있습니다.\n\n"
            + "\n\n".join(f"• {error}" for error in errors)
        )

    else:
        try:
            now = datetime.now(TZ)

            record = {
                "ID": uuid.uuid4().hex,
                "DATE": now.strftime("%Y-%m-%d %H:%M:%S"),

                "성명": clean(name),
                "주민번호": formatted_rrn,
                "성별": clean(gender),
                "생년월일": birth_date.strftime("%Y-%m-%d"),
                "외국인번호": formatted_foreigner_no,
                "집전화": formatted_home_phone,
                "휴대전화": formatted_mobile_phone,
                "체중": "" if weight is None else str(weight),

                "접종동의": clean(vaccination_consent),
                "알림동의": clean(notification_consent),
                "이상동의": clean(adverse_consent),

                **{str(number): clean(answer) for number, answer in enumerate(answers, 1)},
                **{f"{number}상세": clean(detail) for number, answer, detail in details},

                "작성자": clean(writer),
                "관계": clean(relationship)
            }

            append_submission(record)

        except Exception:
            if st.session_state.get("pending_submission"):
                st.rerun()
            st.error("예진표를 저장하지 못했습니다. 잠시 후 다시 제출해주세요. 반복되면 연결 상태·접근 권한·시트 열 제목을 직원에게 확인해주세요.")
        else:
            st.rerun()

st.markdown("</div>", unsafe_allow_html=True)
