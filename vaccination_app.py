import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import gspread
import uuid
import base64
from datetime import datetime, date
from zoneinfo import ZoneInfo
from google.oauth2.service_account import Credentials
from vaccination_pdf import create_print_pdf, create_preview_pdf, pdf_to_png, default_settings


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
@st.cache_resource
def get_spreadsheet():
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(dict(st.secrets["gcp_service_account"]), scopes=scopes)
    client = gspread.authorize(credentials)
    return client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])


@st.cache_resource
def get_worksheet():
    return get_spreadsheet().worksheet(st.secrets["gsheet"]["worksheet_name"])


@st.cache_resource
def get_settings_worksheet():
    return get_spreadsheet().worksheet("print_settings")


# ============================================================
# 공통 함수
# ============================================================
def clean(value):
    return "" if value is None else str(value).strip()


def digits_only(value):
    return "".join(ch for ch in str(value) if ch.isdigit())


def safe_float(value, default):
    try:
        return float(value)
    except Exception:
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
                    input.addEventListener("input", function () {
                        const formatted = formatter(input.value);
                        if (input.value !== formatted) input.value = formatted;
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
            }

            applyAll();
            const observer = new MutationObserver(applyAll);
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


def get_records():
    values = get_worksheet().get_all_values()

    if len(values) <= 1:
        return []

    headers = values[0]
    records = []

    for sheet_row, row in enumerate(values[1:], start=2):
        padded = row + [""] * max(0, len(headers) - len(row))
        record = dict(zip(headers, padded))
        record["_sheet_row"] = sheet_row
        records.append(record)

    return records


# ============================================================
# 출력 설정 읽기 / 저장
# ============================================================
def load_print_settings(mode):
    settings = default_settings()

    try:
        rows = get_settings_worksheet().get_all_records()
    except Exception:
        return settings

    for row in rows:
        if clean(row.get("MODE")) != mode:
            continue

        section = clean(row.get("SECTION"))

        if section not in settings:
            continue

        settings[section]["x"] = safe_float(row.get("OFFSET_X_MM"), 0.0)
        settings[section]["y"] = safe_float(row.get("OFFSET_Y_MM"), 0.0)

        if section == "global":
            settings[section]["scale_x"] = safe_float(row.get("SCALE_X"), 1.0)
            settings[section]["scale_y"] = safe_float(row.get("SCALE_Y"), 1.0)

    return settings


def save_print_settings(mode, settings):
    worksheet = get_settings_worksheet()

    try:
        existing = worksheet.get_all_records()
    except Exception:
        existing = []

    preserved = []

    for row in existing:
        if clean(row.get("MODE")) != mode:
            preserved.append([
                clean(row.get("MODE")),
                clean(row.get("SECTION")),
                clean(row.get("OFFSET_X_MM")),
                clean(row.get("OFFSET_Y_MM")),
                clean(row.get("SCALE_X")),
                clean(row.get("SCALE_Y")),
                clean(row.get("UPDATED_AT"))
            ])

    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    new_rows = []

    for section in SECTION_KEYS:
        setting = settings[section]

        new_rows.append([
            mode,
            section,
            setting.get("x", 0.0),
            setting.get("y", 0.0),
            setting.get("scale_x", 1.0) if section == "global" else 1.0,
            setting.get("scale_y", 1.0) if section == "global" else 1.0,
            now
        ])

    worksheet.clear()
    worksheet.update(range_name="A1", values=[SETTING_HEADERS] + preserved + new_rows)


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
        st.session_state[key] = load_print_settings(mode)

    return st.session_state[key]


def reset_print_settings(mode):
    st.session_state[settings_state_key(mode)] = default_settings()


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
def print_button(pdf_data, key_name):
    encoded = base64.b64encode(pdf_data).decode()

    html = f"""
    <style>
        body {{ margin:0; font-family:Arial,sans-serif; }}
        button {{
            width:100%; height:48px; border:0; border-radius:8px;
            background:#ff4b4b; color:white; font-size:16px;
            font-weight:700; cursor:pointer;
        }}
        button:hover {{ opacity:0.92; }}
    </style>

    <button id="print_{key_name}">🖨 바로 인쇄</button>

    <script>
        document.getElementById("print_{key_name}").onclick = function() {{
            const binary = atob("{encoded}");
            const bytes = new Uint8Array(binary.length);

            for (let i = 0; i < binary.length; i++) {{
                bytes[i] = binary.charCodeAt(i);
            }}

            const blob = new Blob([bytes], {{type:"application/pdf"}});
            const url = URL.createObjectURL(blob);
            const frame = document.createElement("iframe");

            frame.style.position = "fixed";
            frame.style.right = "0";
            frame.style.bottom = "0";
            frame.style.width = "1px";
            frame.style.height = "1px";
            frame.style.border = "0";
            frame.src = url;

            document.body.appendChild(frame);

            frame.onload = function() {{
                setTimeout(function() {{
                    try {{
                        frame.contentWindow.focus();
                        frame.contentWindow.print();
                    }} catch (e) {{
                        window.open(url, "_blank");
                    }}
                }}, 700);
            }};
        }};
    </script>
    """

    components.html(html, height=54)


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
            st.session_state[settings_state_key(mode)] = reset_values
            save_print_settings(mode, reset_values)

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
                if password == st.secrets["admin"]["password"]:
                    st.session_state.admin_authenticated = True
                    st.rerun()
                else:
                    st.error("비밀번호가 올바르지 않습니다.")

        st.stop()

    if "admin_date" not in st.session_state:
        st.session_state.admin_date = date.today()

    # 관리자 달력도 영문 월 대신 숫자 월로 표시합니다.
    install_input_masks_and_numeric_months()

    try:
        all_records = get_records()
    except Exception as e:
        st.error("접수 데이터를 불러오지 못했습니다.")
        st.exception(e)
        st.stop()

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
            if st.button("🔄 새로고침", use_container_width=True):
                # 자동 갱신은 하지 않습니다. 관리자가 원할 때만 최신 Sheet 데이터를 다시 읽습니다.
                st.rerun()

        with b2:
            if st.button("로그아웃", use_container_width=True):
                st.session_state.clear()
                st.rerun()

        date_text = selected_date.strftime("%Y-%m-%d")
        records = [r for r in all_records if record_date(r) == date_text]
        records.reverse()

        st.markdown(
            f'<div class="panel-title">접종자 <span class="patient-count">{len(records)}명</span></div>',
            unsafe_allow_html=True
        )

        if not records:
            st.info("선택한 날짜에 접수된 예진표가 없습니다.")
            st.stop()

        table_rows = []

        for record in records:
            table_rows.append({
                "시간": record_time(record),
                "성명": clean(record.get("성명"))[:8],
                "생년월일": clean(record.get("생년월일")),
                "관계": clean(record.get("관계"))[:6]
            })

        patient_df = pd.DataFrame(table_rows)

        event = st.dataframe(
            patient_df,
            use_container_width=True,
            hide_index=True,
            height=500,
            on_select="rerun",
            selection_mode="single-row",
            column_config={
                "시간": st.column_config.TextColumn("시간", width=55),
                "성명": st.column_config.TextColumn("성명", width=75),
                "생년월일": st.column_config.TextColumn("생년월일", width=95),
                "관계": st.column_config.TextColumn("관계", width=55)
            }
        )

        selected_rows = event.selection.rows

        if not selected_rows:
            st.caption("↑ 접종자를 선택해주세요.")
            st.stop()

        selected = records[selected_rows[0]]

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
            preview_pdf = create_preview_pdf(selected, effective_settings)
            preview_png = pdf_to_png(preview_pdf)

            print_pdf = create_print_pdf(
                selected,
                mode=mode,
                settings=effective_settings
            )

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

            print_button(print_pdf, f"{clean(selected.get('ID'))[:10]}_{mode}")

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
    max_value=date.today(),
    format="YYYY-MM-DD"
)

foreigner_no = st.text_input("외국인 등록번호", placeholder="외국인인 경우 숫자 13자리 입력", max_chars=14)

col1, col2 = st.columns(2)

with col1:
    home_phone = st.text_input("전화번호 (집)", placeholder="숫자만 입력")

with col2:
    mobile_phone = st.text_input("휴대전화 *", placeholder="예: 010-1234-5678")

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

writer = st.text_input("본인(법정대리인, 보호자) 성명 *", placeholder="작성자의 성명을 입력해주세요.")
relationship = st.text_input("접종 대상자와의 관계 *", placeholder="예: 본인, 부, 모, 배우자")

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

    if mobile_digits and len(mobile_digits) not in (10, 11):
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

                "1": clean(q1), "1상세": clean(q1_detail),
                "2": clean(q2), "2상세": clean(q2_detail),
                "3": clean(q3), "3상세": clean(q3_detail),
                "4": clean(q4),
                "5": clean(q5),
                "6": clean(q6), "6상세": clean(q6_detail),
                "7": clean(q7),
                "8": clean(q8),
                "9": clean(q9), "9상세": clean(q9_detail),
                "10": clean(q10),
                "11": clean(q11), "11상세": clean(q11_detail),

                "작성자": clean(writer),
                "관계": clean(relationship)
            }

            row = [record.get(header, "") for header in SHEET_HEADERS]
            get_worksheet().append_row(row, value_input_option="RAW")

            st.session_state.submitted = True
            st.rerun()

        except Exception as e:
            st.error("예진표 저장 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
            st.exception(e)

st.markdown("</div>", unsafe_allow_html=True)
