# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Google Sheets 연동)
- Streamlit + gspread + Google Service Account
- secrets.toml 에서 자격/스프레드시트 ID를 읽어옵니다.
- private_key 가 삼중따옴표(실제 줄바꿈) 또는 한 줄 문자열(\n 포함) 양식을 모두 지원합니다.
"""

import time
import hashlib
import secrets
from datetime import datetime, timedelta
from gspread.exceptions import APIError

import pandas as pd
import streamlit as st

# ─────────────────────────────
# KST 타임존
# ─────────────────────────────
try:
    from zoneinfo import ZoneInfo
    def tz_kst():
        return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst():
        return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# ─────────────────────────────
# Google / gspread
# ─────────────────────────────
import gspread
from google.oauth2.service_account import Credentials


# =============================================================================
# Streamlit 기본 설정
# =============================================================================
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.1rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# =============================================================================
# 공통 유틸
# =============================================================================
def kst_now_str():
    now = datetime.now(tz=tz_kst())
    return now.strftime("%Y-%m-%d %H:%M:%S (%Z)")

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(str(s).encode()).hexdigest()

def _to_bool(x) -> bool:
    s = str(x).strip().lower()
    return s in ("true", "1", "y", "yes", "t")


# =============================================================================
# Google 인증/연결
# =============================================================================
def _normalize_private_key(raw: str) -> str:
    """
    secrets.toml의 private_key 가
    - 한 줄 문자열에 \n 이 들어있으면 실제 줄바꿈으로 교체
    - 이미 삼중따옴표로 줄바꿈 되어있으면 그대로 사용
    """
    if not raw:
        return raw
    if "\\n" in raw and "BEGIN PRIVATE KEY" in raw:
        return raw.replace("\\n", "\n")
    return raw

@st.cache_resource(show_spinner=False)
def get_gspread_client():
    svc = dict(st.secrets["gcp_service_account"])
    svc["private_key"] = _normalize_private_key(svc.get("private_key", ""))

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_workbook():
    gc = get_gspread_client()
    book_id = st.secrets["sheets"]["HR_SHEET_ID"]
    return gc.open_by_key(book_id)

# =============================================================================
# 데이터 로딩
# =============================================================================
@st.cache_data(ttl=60, show_spinner=True)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    """
    지정 워크시트(sheet_name)의 모든 레코드를 DataFrame으로 반환
    """
    wb = get_workbook()
    ws = wb.worksheet(sheet_name)
    rows = ws.get_all_records(numericise_ignore=["all"])
    df = pd.DataFrame(rows)

    # 보정
    if "관리자여부" in df.columns:
        df["관리자여부"] = df["관리자여부"].map(_to_bool)
    if "재직여부" in df.columns:
        df["재직여부"] = df["재직여부"].map(_to_bool)

    for c in ["입사일", "퇴사일"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: str(x).strip() if str(x).strip() else "")

    # 필수 컬럼 최소 보장
    for c in ["사번", "이름", "PIN_hash"]:
        if c not in df.columns:
            df[c] = ""
    return df

# gspread 원본 시트 핸들/헤더 맵
def _get_ws_and_headers(sheet_name: str = "사원"):
    wb = get_workbook()
    ws = wb.worksheet(sheet_name)
    header = ws.row_values(1)  # 1행 헤더
    if not header:
        raise RuntimeError(f"'{sheet_name}' 시트의 헤더(1행)를 찾을 수 없습니다.")
    hmap = {name: idx + 1 for idx, name in enumerate(header)}  # 이름->1기반컬럼
    return ws, header, hmap

def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    """
    '사번' 컬럼에서 sabun 과 정확히 일치하는 행의 gspread row index(1기반)를 반환.
    헤더가 1행이므로, 실제 데이터는 2행부터.
    없으면 0 반환.
    """
    col_idx = hmap.get("사번")
    if not col_idx:
        return 0
    col_vals = ws.col_values(col_idx)  # 헤더 포함
    sabun_s = str(sabun).strip()
    for i, v in enumerate(col_vals[1:], start=2):  # 2행부터
        if str(v).strip() == sabun_s:
            return i
    return 0

def _update_cell(ws, row: int, col: int, value):
    ws.update_cell(row, col, value)

# =============================================================================
# 로그인 / 세션 관리
# =============================================================================
SESSION_TTL_MIN = 30  # 로그인 유지 시간(분)

def _session_valid() -> bool:
    exp = st.session_state.get("auth_expires_at")
    authed = st.session_state.get("authed", False)
    if not authed or exp is None:
        return False
    return time.time() < exp

def _start_session(user_info: dict):
    st.session_state["authed"] = True
    st.session_state["user"] = user_info
    st.session_state["auth_expires_at"] = time.time() + SESSION_TTL_MIN * 60

def logout():
    for k in ("authed", "user", "auth_expires_at"):
        st.session_state.pop(k, None)
    st.cache_data.clear()
    st.rerun()

def show_login_form(emp_df: pd.DataFrame):
    st.header("로그인")
    sabun = st.text_input("사번", placeholder="예) 123456")
    pin = st.text_input("PIN (숫자)", type="password")
    btn = st.button("로그인", use_container_width=True, type="primary")

    if not btn:
        st.stop()

    if not sabun or not pin:
        st.error("사번과 PIN을 입력하세요.")
        st.stop()

    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if row.empty:
        st.error("사번을 찾을 수 없습니다.")
        st.stop()

    r = row.iloc[0]
    if not _to_bool(r.get("재직여부", False)):
        st.error("재직 상태가 아닙니다.")
        st.stop()

    pin_hash = str(r.get("PIN_hash", "")).strip().lower()
    if pin_hash != _sha256_hex(pin):
        st.error("PIN이 올바르지 않습니다.")
        st.stop()

    user_info = {
        "사번": str(r.get("사번", "")),
        "이름": str(r.get("이름", "")),
        "관리자여부": _to_bool(r.get("관리자여부", False)),
    }
    _start_session(user_info)
    st.success(f"{user_info['이름']}님 환영합니다!")
    st.rerun()

def require_login(emp_df: pd.DataFrame):
    if not _session_valid():
        for k in ("authed", "user", "auth_expires_at"):
            st.session_state.pop(k, None)
        show_login_form(emp_df)
        st.stop()

# =============================================================================
# UI: 상단 상태표시
# =============================================================================
def render_status_line():
    try:
        wb = get_workbook()
        title = wb.title
        st.success(
            f"시트 연결 OK | 파일: **{title}** | time={kst_now_str()}",
            icon="✅",
        )
    except Exception as e:
        st.error(f"시트 연결 실패: {e}", icon="🛑")

# =============================================================================
# 탭: 사원
# =============================================================================
def tab_employees(emp_df: pd.DataFrame):
    st.subheader("사원")
    st.caption("사원 기본정보(조회/필터). 편집은 추후 입력폼/승인 절차와 함께 추가 예정입니다.")

    df = emp_df.copy()

    cols_top = st.columns([1, 1, 1, 1, 1, 1, 2])
    with cols_top[0]:
        dept1 = st.selectbox("부서1", ["(전체)"] + sorted([x for x in df.get("부서1", []).dropna().unique() if x]), index=0)
    with cols_top[1]:
        dept2 = st.selectbox("부서2", ["(전체)"] + sorted([x for x in df.get("부서2", []).dropna().unique() if x]), index=0)
    with cols_top[2]:
        grade = st.selectbox("직급", ["(전체)"] + sorted([x for x in df.get("직급", []).dropna().unique() if x]), index=0)
    with cols_top[3]:
        duty = st.selectbox("직무", ["(전체)"] + sorted([x for x in df.get("직무", []).dropna().unique() if x]), index=0)
    with cols_top[4]:
        group = st.selectbox("직군", ["(전체)"] + sorted([x for x in df.get("직군", []).dropna().unique() if x]), index=0)
    with cols_top[5]:
        active = st.selectbox("재직여부", ["(전체)", "재직", "퇴직"], index=0)
    with cols_top[6]:
        q = st.text_input("검색(사번/이름/이메일)", "")

    view = df.copy()
    if dept1 != "(전체)" and "부서1" in view.columns:
        view = view[view["부서1"] == dept1]
    if dept2 != "(전체)" and "부서2" in view.columns:
        view = view[view["부서2"] == dept2]
    if grade != "(전체)" and "직급" in view.columns:
        view = view[view["직급"] == grade]
    if duty != "(전체)" and "직무" in view.columns:
        view = view[view["직무"] == duty]
    if group != "(전체)" and "직군" in view.columns:
        view = view[view["직군"] == group]
    if active != "(전체)" and "재직여부" in view.columns:
        view = view[view["재직여부"] == (active == "재직")]

    if q.strip():
        key = q.strip().lower()
        def _match(row):
            buf = []
            for c in ("사번", "이메일", "이름"):
                if c in row:
                    buf.append(str(row[c]).lower())
            return any(key in s for s in buf)
        view = view[view.apply(_match, axis=1)]

    st.write(f"결과: **{len(view):,}명**")
    st.dataframe(view, use_container_width=True, height=640)

    sheet_id = st.secrets["sheets"]["HR_SHEET_ID"]
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    st.caption(f"📄 원본 스프레드시트 열기: [{url}]({url})")

# =============================================================================
# 탭: 관리자 (PIN 등록/변경 + 일괄 발급)
# =============================================================================
def _random_pin(length=6) -> str:
    digits = "0123456789"
    return "".join(secrets.choice(digits) for _ in range(length))

def tab_admin_pin(emp_df: pd.DataFrame):
    st.subheader("관리자 - PIN 등록/변경")
    st.caption("사번을 선택하고 새 PIN을 입력해 저장합니다. PIN은 숫자만 사용하세요(예: 4~8자리).")

    # ── 단일 변경 ─────────────────────────────────────────────────────────────
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"])

    choices = ["(선택)"] + df["표시"].tolist()
    sel = st.selectbox(
         "직원 선택(사번 - 이름)",
         choices,
         index=0,
         key="pin_emp_select"     # ← 고유 키
)

    target = None
    if sel != "(선택)":
        sabun = sel.split(" - ", 1)[0]
        target = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]

        col1, col2, col3, col4 = st.columns([1,1,1,2])
        with col1:
            st.metric("사번", str(target.get("사번","")))
        with col2:
            st.metric("이름", str(target.get("이름","")))
        with col3:
            st.metric("재직", "재직" if _to_bool(target.get("재직여부", False)) else "퇴직")
        with col4:
            st.metric("PIN 상태", "설정됨" if str(target.get("PIN_hash","")).strip() else "미설정")

        st.divider()

        # PIN 입력
        pin1 = st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
        pin2 = st.text_input("새 PIN 확인", type="password", key="adm_pin2")

        cols_btn = st.columns([1,1,4])
        with cols_btn[0]:
            do_save = st.button("PIN 저장/변경", type="primary", use_container_width=True)
        with cols_btn[1]:
            do_clear = st.button("PIN 비우기", use_container_width=True)

        if do_save:
            # 검증
            if not pin1 or not pin2:
                st.error("PIN을 두 번 모두 입력하세요.")
                st.stop()
            if pin1 != pin2:
                st.error("PIN 확인이 일치하지 않습니다.")
                st.stop()
            if not pin1.isdigit():
                st.error("PIN은 숫자만 입력하세요.")
                st.stop()
            if not _to_bool(target.get("재직여부", False)):
                st.error("퇴직자는 변경할 수 없습니다.")
                st.stop()

            # gspread 셀 업데이트
            try:
                ws, header, hmap = _get_ws_and_headers("사원")
                if "PIN_hash" not in hmap:
                    st.error("'사원' 시트에 PIN_hash 컬럼이 없습니다. (헤더 행에 'PIN_hash' 추가)")
                    st.stop()

                row_idx = _find_row_by_sabun(ws, hmap, sabun)
                if row_idx == 0:
                    st.error("시트에서 해당 사번을 찾지 못했습니다.")
                    st.stop()

                new_hash = _sha256_hex(pin1)
                _update_cell(ws, row_idx, hmap["PIN_hash"], new_hash)

                st.cache_data.clear()  # 캐시 무효화
                st.success(f"[{sabun}] PIN 저장 완료")
                st.toast("PIN 변경 반영됨", icon="✅")
            except Exception as e:
                st.exception(e)

        if do_clear:
            try:
                ws, header, hmap = _get_ws_and_headers("사원")
                if "PIN_hash" not in hmap:
                    st.error("'사원' 시트에 PIN_hash 컬럼이 없습니다. (헤더 행에 'PIN_hash' 추가)")
                    st.stop()
                row_idx = _find_row_by_sabun(ws, hmap, sabun)
                if row_idx == 0:
                    st.error("시트에서 해당 사번을 찾지 못했습니다.")
                    st.stop()
                _update_cell(ws, row_idx, hmap["PIN_hash"], "")
                st.cache_data.clear()
                st.success(f"[{sabun}] PIN 초기화(빈 값) 완료")
                st.toast("PIN 초기화 반영됨", icon="✅")
            except Exception as e:
                st.exception(e)
    else:
        st.info("사번을 선택하면 상세/변경 UI가 표시됩니다.")

    st.divider()

    # ── 일괄 PIN 발급 ────────────────────────────────────────────────────────
    st.markdown("### 🔐 전 직원 일괄 PIN 발급")
    st.caption("- 시트에는 **해시(PIN_hash)** 만 저장됩니다. 새 PIN 목록은 **관리용 CSV**로만 내려받으세요.\n- 매우 민감한 데이터이므로 CSV 파일은 안전한 장소에 보관하세요.")

    col_opt = st.columns([1,1,1,1,2])
    with col_opt[0]:
        only_active = st.checkbox("재직자만", True)
    with col_opt[1]:
        only_empty = st.checkbox("PIN 미설정자만", True)
    with col_opt[2]:
        overwrite_all = st.checkbox("기존 PIN 덮어쓰기", False, disabled=only_empty)
    with col_opt[3]:
        pin_len = st.number_input("PIN 자릿수", min_value=4, max_value=8, value=6, step=1)
    with col_opt[4]:
        uniq = st.checkbox("서로 다른 PIN 보장", True)

    # 대상 만들기
    candidates = emp_df.copy()
    if only_active and "재직여부" in candidates.columns:
        candidates = candidates[candidates["재직여부"] == True]

    if only_empty:
        candidates = candidates[(candidates["PIN_hash"].astype(str).str.strip() == "")]
    elif not overwrite_all:
        #⛏️ 여기서 이모지에 반드시 따옴표!
        st.warning("현재 설정에서는 'PIN 미설정자만' 또는 '기존 PIN 덮어쓰기' 중 하나를 선택해야 합니다.", icon="⚠️")

    candidates = candidates.copy()
    candidates["사번"] = candidates["사번"].astype(str)

    st.write(f"대상자 수: **{len(candidates):,}명**")

    col_btns = st.columns([1,1,2,2])
    with col_btns[0]:
        do_preview = st.button("미리보기 생성", use_container_width=True)
    with col_btns[1]:
        do_issue = st.button("발급 실행(시트 업데이트)", type="primary", use_container_width=True)

    if do_preview or do_issue:
        if len(candidates) == 0:
            st.warning("대상자가 없습니다.", icon="⚠️")
        else:
            # PIN 생성
            used = set()
            new_pins = []
            for _ in range(len(candidates)):
                while True:
                    p = _random_pin(pin_len)
                    if not uniq or p not in used:
                        used.add(p)
                        new_pins.append(p)
                        break
            preview = candidates[["사번", "이름"]].copy()
            preview["새_PIN"] = new_pins

            # 미리보기 표시 + CSV 다운로드
            st.dataframe(preview, use_container_width=True, height=360)
            csv = preview.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "CSV 다운로드 (사번,이름,새_PIN)",
                data=csv,
                file_name=f"PIN_bulk_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

            if do_issue:
                try:
                    ws, header, hmap = _get_ws_and_headers("사원")
                    if "PIN_hash" not in hmap or "사번" not in hmap:
                        st.error("'사원' 시트에 '사번' 또는 'PIN_hash' 헤더가 없습니다.")
                        st.stop()

                    # 행별 업데이트 (규모가 250명 내외라 단건 업데이트로도 충분)
                    pin_col = hmap["PIN_hash"]
                    pbar = st.progress(0.0, text="시트 업데이트 중...")
                    for i, (_, row) in enumerate(preview.iterrows(), start=1):
                        sabun = str(row["사번"])
                        r_idx = _find_row_by_sabun(ws, hmap, sabun)
                        if r_idx > 0:
                            _update_cell(ws, r_idx, pin_col, _sha256_hex(row["새_PIN"]))
                        # 가벼운 속도 조절(과도한 API 호출 방지)
                        time.sleep(0.02)
                        pbar.progress(i / len(preview))

                    st.cache_data.clear()
                    st.success(f"일괄 발급 완료: {len(preview):,}명 반영", icon="✅")
                    st.toast("PIN 일괄 발급 반영됨", icon="✅")
                except Exception as e:
                    st.exception(e)

# =============================================================================
# 부서이력 유틸 (시트명: '부서이력')
# =============================================================================
HIST_SHEET = "부서이력"

def _ensure_emp_extra_cols(ws_emp, header_emp, need_cols=None):
    """사원 시트에 보조 컬럼이 없으면 헤더(1행) 끝에 추가.
    보호범위로 인해 자동 추가가 불가하면 경고만 띄우고 계속 진행(나중에 수동 추가 가능).
    """
    if need_cols is None:
        need_cols = ["이전부서1", "이전부서2", "현재부서시작일"]

    missing = [c for c in need_cols if c not in header_emp]
    if not missing:
        return {name: idx + 1 for idx, name in enumerate(header_emp)}

    # 보호 범위일 수 있으니 시도-실패 처리
    try:
        new_header = header_emp + missing
        ws_emp.update("1:1", [new_header])  # 1행 전체 갱신
        header_emp = new_header
        st.toast("사원 시트에 보조 컬럼 추가: " + ", ".join(missing), icon="✅")
    except APIError:
        st.warning(
            "사원 시트 1행이 보호되어 있어 보조 컬럼을 자동으로 추가할 수 없습니다.\n"
            "아래 헤더를 1행 맨 오른쪽에 직접 추가한 뒤 다시 실행해 주세요:\n"
            f"- {', '.join(missing)}"
        )
        # 헤더를 다시 읽어 최신 상태로 맵 구성(수동 추가 후 재실행 시 반영)
        header_emp = ws_emp.row_values(1)

    return {name: idx + 1 for idx, name in enumerate(header_emp)}

def ensure_dept_history_sheet():
    """'부서이력' 시트가 없으면 생성하고 헤더를 세팅."""
    wb = get_workbook()
    try:
        wb.worksheet(HIST_SHEET)
        return
    except Exception:
        pass
    ws = wb.add_worksheet(title=HIST_SHEET, rows=200, cols=10)
    headers = ["사번","이름","부서1","부서2","시작일","종료일","변경사유","승인자","메모","등록시각"]
    ws.update("A1", [headers])

@st.cache_data(ttl=60, show_spinner=False)
def read_dept_history_df() -> pd.DataFrame:
    """부서이력 전체를 DF로 읽기."""
    ensure_dept_history_sheet()
    wb = get_workbook()
    ws = wb.worksheet(HIST_SHEET)
    rows = ws.get_all_records(numericise_ignore=["all"])
    df = pd.DataFrame(rows)
    # 날짜 문자열 정리
    for c in ["시작일","종료일","등록시각"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)
    return df

def _hist_append_row(rec: dict):
    """부서이력 1행 append (dict -> 헤더 순)."""
    wb = get_workbook()
    ws = wb.worksheet(HIST_SHEET)
    header = ws.row_values(1)
    row = [rec.get(h, "") for h in header]
    ws.append_row(row, value_input_option="USER_ENTERED")

def _hist_close_active_range(ws_hist, sabun: str, end_date: str):
    """해당 사번의 '종료일이 빈' **마지막(최신)** 행만 종료일로 닫기."""
    header = ws_hist.row_values(1)
    hmap = {name: idx+1 for idx, name in enumerate(header)}  # 1-based
    sabun_col = hmap.get("사번"); end_col = hmap.get("종료일")
    if not (sabun_col and end_col):
        return

    values = ws_hist.get_all_values()  # 헤더 포함
    last_open_idx = 0
    for i in range(2, len(values)+1):         # 2행부터 데이터
        row = values[i-1]
        if row[sabun_col-1].strip() == str(sabun).strip() and row[end_col-1].strip() == "":
            last_open_idx = i                 # 가장 아래쪽(최신) 오픈행 갱신

    if last_open_idx:
        ws_hist.update_cell(last_open_idx, end_col, end_date)

def apply_department_change(emp_df: pd.DataFrame, sabun: str, new_dept1: str, new_dept2: str,
                            start_date: datetime.date, reason: str = "", approver: str = "") -> dict:
    """
    부서 이동을 기록하고(부서이력), 필요 시 '사원' 시트의 현재부서를 업데이트.
    규칙:
      - 기존 '종료일 빈' 최신 구간을 (start_date - 1)로 닫음
      - 새 구간: 시작일 = start_date, 종료일 = ""
      - start_date <= 오늘이면 '사원'의 현재부서 갱신 + 이전부서/시작일 보존
    """
    ensure_dept_history_sheet()
    wb = get_workbook()
    ws_hist = wb.worksheet(HIST_SHEET)

    today = datetime.now(tz=tz_kst()).date()
    start_str = start_date.strftime("%Y-%m-%d")
    prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # 사원(현재) 정보
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if row.empty:
        raise RuntimeError("사번을 찾지 못했습니다.")
    r = row.iloc[0]
    name = str(r.get("이름", ""))
    prev_d1 = str(r.get("부서1", ""))
    prev_d2 = str(r.get("부서2", ""))

    # 1) 기존 오픈 구간 닫기(최신 1건만)
    _hist_close_active_range(ws_hist, sabun=str(sabun), end_date=prev_end)

    # 2) 새 구간 append (부서이력에 신규 레코드)
    _hist_append_row({
        "사번": str(sabun),
        "이름": name,
        "부서1": new_dept1,
        "부서2": new_dept2,
        "시작일": start_str,
        "종료일": "",
        "변경사유": reason,
        "승인자": approver,
        "메모": "",
        "등록시각": kst_now_str(),
    })

    # 3) 오늘 적용 대상이면 '사원' 현재부서 갱신 + 이전부서/시작일 기록
    applied = False
    if start_date <= today:
        ws_emp, header_emp, hmap_emp = _get_ws_and_headers("사원")
        # 보조 컬럼 보장
        hmap_emp = _ensure_emp_extra_cols(ws_emp, header_emp)

        row_idx = _find_row_by_sabun(ws_emp, hmap_emp, str(sabun))
        if row_idx > 0:
            # 이전부서 보존
            if "이전부서1" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["이전부서1"], prev_d1)
            if "이전부서2" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["이전부서2"], prev_d2)
            # 현재부서 갱신
            if "부서1" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서1"], new_dept1)
            if "부서2" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서2"], new_dept2)
            # 현재부서 시작일 기록
            if "현재부서시작일" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["현재부서시작일"], start_str)

            applied = True

    st.cache_data.clear()
    return {
        "applied_now": applied,
        "start_date": start_str,
        "new_dept1": new_dept1,
        "new_dept2": new_dept2,
        "prev_dept1": prev_d1,
        "prev_dept2": prev_d2,
    }

def sync_current_department_from_history(as_of_date: datetime.date = None) -> int:
    """
    '부서이력'을 기준으로 '사원' 시트의 현재 부서(부서1/부서2)를 동기화.
    규칙: as_of_date(기본=오늘) 기준으로 시작일 <= D 이고 (종료일이 비었거나 종료일 >= D) 인 최신 구간을 현재값으로 반영.
    반환: 업데이트된 사원 수
    """
    ensure_dept_history_sheet()
    hist = read_dept_history_df()
    emp = read_sheet_df("사원")

    if as_of_date is None:
        as_of_date = datetime.now(tz=tz_kst()).date()
    D = as_of_date.strftime("%Y-%m-%d")

    # 사번별 최신 구간 선택
    updates = {}  # sabun -> (dept1, dept2)
    for sabun, grp in hist.groupby("사번"):
        def ok(row):
            s = row.get("시작일","")
            e = row.get("종료일","")
            return (s and s <= D) and ((not e) or e >= D)
        cand = grp[grp.apply(ok, axis=1)]
        if cand.empty:
            continue
        cand = cand.sort_values("시작일").iloc[-1]
        updates[str(sabun)] = (str(cand.get("부서1","")), str(cand.get("부서2","")))

    if not updates:
        return 0

    wb = get_workbook()
    ws_emp, header_emp, hmap_emp = _get_ws_and_headers("사원")
    changed = 0
    for _, r in emp.iterrows():
        sabun = str(r.get("사번",""))
        if sabun in updates:
            d1, d2 = updates[sabun]
            row_idx = _find_row_by_sabun(ws_emp, hmap_emp, sabun)
            if row_idx > 0:
                if "부서1" in hmap_emp:
                    _update_cell(ws_emp, row_idx, hmap_emp["부서1"], d1)
                if "부서2" in hmap_emp:
                    _update_cell(ws_emp, row_idx, hmap_emp["부서2"], d2)
                changed += 1

    st.cache_data.clear()
    return changed

# =============================================================================
# 메인
# =============================================================================
def main():
    st.title(APP_TITLE)
    render_status_line()

    # 1) 데이터 읽기
    try:
        emp_df = read_sheet_df("사원")
    except Exception as e:
        st.error(f"'사원' 시트 로딩 실패: {e}")
        return

    # 2) 로그인 요구
    require_login(emp_df)

    # 3) 사이드바 사용자/로그아웃
    u = st.session_state["user"]
    with st.sidebar:
        st.write(f"👤 **{u['이름']}** ({u['사번']})")
        if st.button("로그아웃", use_container_width=True):
            logout()

    # 4) 탭 구성 (관리자여부에 따라 '관리자' 탭 보이기)
    tabs_names = ["사원", "도움말"]
    if u.get("관리자여부", False):
        tabs_names.append("관리자")  # PIN 등록/변경 + 일괄 발급

    tabs = st.tabs(tabs_names)

    # 사원
    with tabs[0]:
        tab_employees(emp_df)

    # 도움말
    with tabs[1]:
        st.markdown(
            """
            ### 사용 안내
            - Google Sheets의 **사원** 시트와 연동해 조회합니다.  
            - `secrets.toml` 의 서비스 계정(편집자 권한)이 시트에 공유되어 있어야 합니다.  
            - `private_key` 는  
              - **삼중따옴표 + 실제 줄바꿈** 또는  
              - **한 줄 문자열 + `\\n` 이스케이프** 모두 지원합니다.  
            - 관리자 탭에서 개별 PIN 변경과 **일괄 PIN 발급**이 가능합니다. (시트에는 해시만 저장)
            """
        )

    # 관리자 (PIN + 부서이동)
    if u.get("관리자여부", False):
        with tabs[2]:
            tab_admin_pin(emp_df)
            st.divider()
            tab_admin_transfer(emp_df)

def tab_admin_transfer(emp_df: pd.DataFrame):
    st.subheader("관리자 - 부서(근무지) 이동")
    st.caption("이동 이력 기록 + (필요 시) 사원 시트 현재부서 반영. 예정일 이동은 이력만 넣고, 나중에 '동기화'로 반영하세요.")

    ensure_dept_history_sheet()

# 헤더 점검/보정 유틸 UI
with st.expander("헤더 상태 점검/보정", expanded=False):
    if st.button("사원 헤더 자동 보정", use_container_width=True, key="btn_fix_headers"):
        try:
            ws_emp, header_emp, _ = _get_ws_and_headers("사원")
            # 필요 헤더 보장: 이전부서1/이전부서2/현재부서시작일
            _ensure_emp_extra_cols(ws_emp, header_emp)
            st.cache_data.clear()
            st.success("헤더 점검/보정 완료", icon="✅")
        except Exception as e:
            st.exception(e)

    # 사번 선택
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"])
    sel = st.selectbox(
        "직원 선택(사번 - 이름)",
        ["(선택)"] + df["표시"].tolist(),
        index=0,
        key="transfer_emp_select"   # ← 고유 키
    )

    if sel == "(선택)":
        st.info("사번을 선택하면 이동 입력 폼이 표시됩니다.")
        return

    sabun = sel.split(" - ", 1)[0]
    target = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]

    # 현재 정보
    c1, c2, c3, c4 = st.columns([1,1,1,2])
    with c1: st.metric("사번", str(target.get("사번","")))
    with c2: st.metric("이름", str(target.get("이름","")))
    with c3: st.metric("현재 부서1", str(target.get("부서1","")))
    with c4: st.metric("현재 부서2", str(target.get("부서2","")))

    st.divider()

    # 옵션 목록(기존 값 기반)
    opt_d1 = sorted([x for x in emp_df.get("부서1", []).dropna().unique() if x])
    opt_d2 = sorted([x for x in emp_df.get("부서2", []).dropna().unique() if x])

    colA, colB, colC = st.columns([1,1,1])
    with colA:
        start_date = st.date_input("시작일(발령일)", datetime.now(tz=tz_kst()).date(), key="transfer_start_date")
    with colB:
        new_d1_pick = st.selectbox("새 부서1(선택 또는 직접입력)", ["(직접입력)"] + opt_d1, index=0, key="transfer_new_dept1_pick")
    with colC:
        new_d2_pick = st.selectbox("새 부서2(선택 또는 직접입력)", ["(직접입력)"] + opt_d2, index=0, key="transfer_new_dept2_pick")

    nd1 = st.text_input("부서1 직접입력", value="" if new_d1_pick != "(직접입력)" else "", key="transfer_nd1")
    nd2 = st.text_input("부서2 직접입력", value="" if new_d2_pick != "(직접입력)" else "", key="transfer_nd2")

    new_dept1 = new_d1_pick if new_d1_pick != "(직접입력)" else nd1
    new_dept2 = new_d2_pick if new_d2_pick != "(직접입력)" else nd2

    colR = st.columns([2,3])
    with colR[0]:
        reason = st.text_input("변경사유", "", key="transfer_reason")
    with colR[1]:
        approver = st.text_input("승인자", "", key="transfer_approver")

    ok = st.button("이동 기록 + 현재 반영", type="primary", use_container_width=True, key="transfer_apply_btn")

    if ok:
        if not (new_dept1.strip() or new_dept2.strip()):
            st.error("새 부서1/부서2 중 최소 하나는 입력/선택되어야 합니다.")
            return
        try:
            rep = apply_department_change(
                emp_df=emp_df,
                sabun=str(sabun),
                new_dept1=new_dept1.strip(),
                new_dept2=new_dept2.strip(),
                start_date=start_date,
                reason=reason.strip(),
                approver=approver.strip(),
            )
            if rep["applied_now"]:
                st.success(f"이동 기록 완료 및 현재부서 반영: {rep['new_dept1']} / {rep['new_dept2']} (시작일 {rep['start_date']})", icon="✅")
            else:
                st.info(f"이동 이력만 기록됨(시작일 {rep['start_date']}). 해당 날짜 이후 '동기화'에서 일괄 반영하세요.", icon="ℹ️")
            st.toast("부서 이동 처리됨", icon="✅")
        except Exception as e:
            st.exception(e)

    # 개인 이력 미리보기
    try:
        hist = read_dept_history_df()
        my = hist[hist["사번"] == str(sabun)].copy()
        if not my.empty:
            my = my.sort_values(["시작일","등록시각"], ascending=[False, False])
            st.markdown("#### 개인 부서이력")
            st.dataframe(my[["시작일","종료일","부서1","부서2","변경사유","승인자"]], use_container_width=True, height=260)
    except Exception:
        pass

    st.divider()
    colSync = st.columns([1,2])
    with colSync[0]:
        if st.button("오늘 기준 전체 동기화", use_container_width=True):
            try:
                cnt = sync_current_department_from_history()
                st.success(f"사원 시트 현재부서 동기화 완료: {cnt}명 반영", icon="✅")
                st.toast("동기화 완료", icon="✅")
            except Exception as e:
                st.exception(e)

# =============================================================================
if __name__ == "__main__":
    main()
