# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Google Sheets 연동)
- Streamlit + gspread + Google Service Account
- secrets.toml에서 자격/스프레드시트 ID를 읽어옵니다.
"""

import time
import hashlib
import secrets as pysecrets
from datetime import datetime, timedelta

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
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError:
    # 클라우드 환경에서 gspread가 없을 때 자동 설치
    import subprocess, sys

    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "gspread==6.1.2", "google-auth==2.31.0"]
    )
    import gspread
    from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# =============================================================================
# Streamlit 기본 설정
# =============================================================================
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown(
    """
    <style>
      .block-container {padding-top: 1.1rem;}
      /* 탭 클릭 영역/가독성 확대 */
      .stTabs [role="tab"] { padding: 12px 20px !important; font-size: 1.05rem !important; }
      /* 평가 입력 레이아웃 가독성 */
      .eval-desc p { margin: 0; }
      /* 점수 입력 테이블 라인 정렬 */
      .score-row {padding: 10px 6px; border-bottom: 1px solid rgba(49,51,63,.10);}
      .score-name {font-weight: 700;}
      .score-desc {color: #4b5563;}
      .score-badge {min-width: 36px; text-align: center; font-weight: 700;
                    padding: 6px 8px; border-radius: 10px; background: rgba(49,51,63,.06);}
      .score-center {display:flex; align-items:center; justify-content:center; height:100%;}
      .score-buttons .stButton>button {padding: 4px 10px; margin: 0 2px;}
      .score-buttons {display:flex; align-items:center; justify-content:center; gap:4px;}
      .score-head {font-size: .9rem; color:#6b7280; margin-bottom: .4rem;}
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

# ── 시트명 상수(환경설정 가능) ─────────────────────────────────────────────
EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")


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
def _get_ws_and_headers(sheet_name: str | None = None):
    wb = get_workbook()
    sheet = sheet_name or EMP_SHEET
    ws = wb.worksheet(sheet)
    header = ws.row_values(1)  # 1행 헤더
    if not header:
        raise RuntimeError(f"'{sheet}' 시트의 헤더(1행)를 찾을 수 없습니다.")
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
# 탭: 직원
# =============================================================================
def tab_staff(emp_df: pd.DataFrame):
    st.subheader("직원")
    st.caption("직원 기본정보(조회/필터). 편집은 추후 입력폼/승인 절차와 함께 추가 예정입니다.")

    df = emp_df.copy()

    cols_top = st.columns([1, 1, 1, 1, 1, 1, 2])
    with cols_top[0]:
        dept1 = st.selectbox(
            "부서1", ["(전체)"] + sorted([x for x in df.get("부서1", []).dropna().unique() if x]), index=0
        )
    with cols_top[1]:
        dept2 = st.selectbox(
            "부서2", ["(전체)"] + sorted([x for x in df.get("부서2", []).dropna().unique() if x]), index=0
        )
    with cols_top[2]:
        grade = st.selectbox(
            "직급", ["(전체)"] + sorted([x for x in df.get("직급", []).dropna().unique() if x]), index=0
        )
    with cols_top[3]:
        duty = st.selectbox(
            "직무", ["(전체)"] + sorted([x for x in df.get("직무", []).dropna().unique() if x]), index=0
        )
    with cols_top[4]:
        group = st.selectbox(
            "직군", ["(전체)"] + sorted([x for x in df.get("직군", []).dropna().unique() if x]), index=0
        )
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

# 예전 이름과의 호환
tab_employees = tab_staff


# =============================================================================
# 탭: 관리자 (PIN 등록/변경 + 일괄 발급)
# =============================================================================
def _random_pin(length=6) -> str:
    digits = "0123456789"
    return "".join(pysecrets.choice(digits) for _ in range(length))


def tab_admin_pin(emp_df: pd.DataFrame):
    st.subheader("관리자 - PIN 등록/변경")
    st.caption("사번을 선택하고 새 PIN을 입력해 저장합니다. PIN은 숫자만 사용하세요(예: 4~8자리).")

    # ── 단일 변경 ─────────────────────────────────────────────────────────────
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"])

    choices = ["(선택)"] + df["표시"].tolist()
    sel = st.selectbox("직원 선택(사번 - 이름)", choices, index=0, key="pin_emp_select")

    target = None
    if sel != "(선택)":
        sabun = sel.split(" - ", 1)[0]
        target = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]

        col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
        with col1:
            st.metric("사번", str(target.get("사번", "")))
        with col2:
            st.metric("이름", str(target.get("이름", "")))
        with col3:
            st.metric("재직", "재직" if _to_bool(target.get("재직여부", False)) else "퇴직")
        with col4:
            st.metric("PIN 상태", "설정됨" if str(target.get("PIN_hash", "")).strip() else "미설정")

        st.divider()

        # PIN 입력
        pin1 = st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
        pin2 = st.text_input("새 PIN 확인", type="password", key="adm_pin2")

        cols_btn = st.columns([1, 1, 4])
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
                ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
                if "PIN_hash" not in hmap:
                    st.error(f"'{EMP_SHEET}' 시트에 PIN_hash 컬럼이 없습니다. (헤더 행에 'PIN_hash' 추가)")
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
                ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
                if "PIN_hash" not in hmap:
                    st.error(f"'{EMP_SHEET}' 시트에 PIN_hash 컬럼이 없습니다. (헤더 행에 'PIN_hash' 추가)")
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
    st.caption(
        "- 시트에는 **해시(PIN_hash)** 만 저장됩니다. 새 PIN 목록은 **관리용 CSV**로만 내려받으세요.\n"
        "- 매우 민감한 데이터이므로 CSV 파일은 안전한 장소에 보관하세요."
    )

    col_opt = st.columns([1, 1, 1, 1, 2])
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
        st.warning("현재 설정에서는 'PIN 미설정자만' 또는 '기존 PIN 덮어쓰기' 중 하나를 선택해야 합니다.", icon="⚠️")

    candidates = candidates.copy()
    candidates["사번"] = candidates["사번"].astype(str)

    st.write(f"대상자 수: **{len(candidates):,}명**")

    col_btns = st.columns([1, 1, 2, 2])
    with col_btns[0]:
        do_preview = st.button("미리보기 생성", use_container_width=True)
    with col_btns[1]:
        do_issue = st.button("발급 실행(시트 업데이트)", type="primary", use_container_width=True)

    preview = None
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

            # ── 미리보기(대상자) 표시 ─────────────────────────────────────────
            st.dataframe(preview, use_container_width=True, height=360)

            # ── CSV: 직원 전체(사번,이름,새_PIN)로 내려받기 ─────────────────────
            #   - 새 PIN이 생성된 직원만 '새_PIN' 값이 채워지고, 나머지는 공백("")
            full = emp_df[["사번", "이름"]].copy()
            full["사번"] = full["사번"].astype(str)
            join_src = preview[["사번", "새_PIN"]].copy()
            join_src["사번"] = join_src["사번"].astype(str)
            csv_df = full.merge(join_src, on="사번", how="left")
            csv_df["새_PIN"] = csv_df["새_PIN"].fillna("")
            csv_df = csv_df.sort_values("사번")
            csv_all = csv_df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "CSV 전체 다운로드 (사번,이름,새_PIN)",
                data=csv_all,
                file_name=f"PIN_ALL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
            # 대상자만 CSV (선택)
            csv_targets = preview.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "CSV 대상자만 다운로드 (사번,이름,새_PIN)",
                data=csv_targets,
                file_name=f"PIN_TARGETS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    if do_issue and preview is not None:
        try:
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap or "사번" not in hmap:
                st.error(f"'{EMP_SHEET}' 시트에 '사번' 또는 'PIN_hash' 헤더가 없습니다.")
                st.stop()

            # 한 번만 읽어서 사번 -> 행번호 매핑
            sabun_col = hmap["사번"]
            pin_col = hmap["PIN_hash"]
            sabun_values = ws.col_values(sabun_col)[1:]  # 헤더 제외
            pos = {str(v).strip(): i for i, v in enumerate(sabun_values, start=2)}

            # 단일 셀 범위를 여러 개 묶어 한 번에 업데이트
            updates = []
            for _, row in preview.iterrows():
                sabun = str(row["사번"]).strip()
                r_idx = pos.get(sabun, 0)
                if r_idx:
                    a1 = gspread.utils.rowcol_to_a1(r_idx, pin_col)
                    updates.append({"range": a1, "values": [[_sha256_hex(row["새_PIN"])]]})

            if not updates:
                st.warning("업데이트할 대상이 없습니다.", icon="⚠️")
                st.stop()

            # 큰 페이로드를 안전하게 나눠서 전송(쿼터 회피)
            CHUNK = 100
            pbar = st.progress(0.0, text="시트 업데이트(배치) 중...")
            for i in range(0, len(updates), CHUNK):
                ws.batch_update(updates[i:i + CHUNK])
                pbar.progress(min(1.0, (i + CHUNK) / len(updates)))
                time.sleep(0.2)  # 약간 페이싱

            st.cache_data.clear()
            st.success(f"일괄 발급 완료: {len(updates):,}명 반영", icon="✅")
            st.toast("PIN 일괄 발급 반영됨", icon="✅")
        except Exception as e:
            st.exception(e)


# =============================================================================
# 평가 항목 유틸 (시트: '평가_항목')
# =============================================================================
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고"]


def ensure_eval_items_sheet():
    """'평가_항목' 시트가 없으면 생성하고, 헤더를 보장."""
    wb = get_workbook()
    try:
        ws = wb.worksheet(EVAL_ITEMS_SHEET)
    except Exception:
        ws = wb.add_worksheet(title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        ws.update("A1", [EVAL_ITEM_HEADERS])
        return
    # 헤더 보강(누락 컬럼 추가)
    header = ws.row_values(1)
    if not header:
        ws.update("A1", [EVAL_ITEM_HEADERS])
        return
    need = [h for h in EVAL_ITEM_HEADERS if h not in header]
    if need:
        new_header = header + need
        ws.update("1:1", [new_header])


@st.cache_data(ttl=60, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    """평가_항목 → DataFrame 반환."""
    ensure_eval_items_sheet()
    wb = get_workbook()
    ws = wb.worksheet(EVAL_ITEMS_SHEET)
    rows = ws.get_all_records(numericise_ignore=["all"])
    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=EVAL_ITEM_HEADERS)
    # 타입 보정
    if "순서" in df.columns:
        def _to_int(x):
            s = str(x).strip()
            try:
                return int(float(s))
            except Exception:
                return 0
        df["순서"] = df["순서"].apply(_to_int)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    # 정렬 및 필터
    df = df.sort_values(["순서", "항목"]).reset_index(drop=True)
    if only_active and "활성" in df.columns:
        df = df[df["활성"] == True]
    return df


def _new_eval_item_id(ws) -> str:
    """ITM0001 형태의 신규 항목ID 생성."""
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    col = hmap.get("항목ID")
    if not col:
        return "ITM0001"
    vals = ws.col_values(col)[1:]  # 헤더 제외
    nums = []
    for v in vals:
        s = str(v).strip()
        if s.startswith("ITM"):
            try:
                nums.append(int(s[3:]))
            except Exception:
                pass
    nxt = (max(nums) + 1) if nums else 1
    return f"ITM{nxt:04d}"


def upsert_eval_item(item_id: str | None, name: str, desc: str, order: int, active: bool, memo: str = ""):
    """항목ID가 있으면 업데이트, 없으면 신규 추가."""
    ensure_eval_items_sheet()
    wb = get_workbook()
    ws = wb.worksheet(EVAL_ITEMS_SHEET)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    # 신규면 ID 발급
    if not item_id:
        item_id = _new_eval_item_id(ws)
        row = [""] * len(header)
        row[hmap["항목ID"] - 1] = item_id
        row[hmap["항목"] - 1] = name
        row[hmap["내용"] - 1] = desc
        row[hmap["순서"] - 1] = int(order)
        row[hmap["활성"] - 1] = bool(active)
        if "비고" in hmap:
            row[hmap["비고"] - 1] = memo
        ws.append_row(row, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return item_id

    # 업데이트: 항목ID 행 찾기
    col_id = hmap.get("항목ID")
    vals = ws.col_values(col_id)  # 헤더 포함
    target_row = 0
    for i, v in enumerate(vals[1:], start=2):
        if str(v).strip() == str(item_id).strip():
            target_row = i
            break
    if target_row == 0:
        # 못 찾으면 신규로
        return upsert_eval_item(None, name, desc, order, active, memo)

    ws.update_cell(target_row, hmap["항목"], name)
    ws.update_cell(target_row, hmap["내용"], desc)
    ws.update_cell(target_row, hmap["순서"], int(order))
    ws.update_cell(target_row, hmap["활성"], bool(active))
    if "비고" in hmap:
        ws.update_cell(target_row, hmap["비고"], memo)
    st.cache_data.clear()
    return item_id


def deactivate_eval_item(item_id: str):
    """활성=False (소프트 삭제)."""
    ensure_eval_items_sheet()
    wb = get_workbook()
    ws = wb.worksheet(EVAL_ITEMS_SHEET)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    col_id = hmap.get("항목ID")
    col_active = hmap.get("활성")
    if not (col_id and col_active):
        return
    vals = ws.col_values(col_id)
    for i, v in enumerate(vals[1:], start=2):
        if str(v).strip() == str(item_id).strip():
            ws.update_cell(i, col_active, False)
            break
    st.cache_data.clear()


def delete_eval_item_row(item_id: str):
    """행 자체 삭제(완전 삭제)."""
    ensure_eval_items_sheet()
    wb = get_workbook()
    ws = wb.worksheet(EVAL_ITEMS_SHEET)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    col_id = hmap.get("항목ID")
    vals = ws.col_values(col_id)
    for i, v in enumerate(vals[1:], start=2):
        if str(v).strip() == str(item_id).strip():
            ws.delete_rows(i)
            break
    st.cache_data.clear()


def update_eval_items_order(df_order: pd.DataFrame):
    """순서 값 일괄 반영(df: cols=['항목ID','순서'])."""
    ensure_eval_items_sheet()
    wb = get_workbook()
    ws = wb.worksheet(EVAL_ITEMS_SHEET)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    col_id = hmap.get("항목ID")
    col_ord = hmap.get("순서")
    vals = ws.col_values(col_id)
    pos = {str(v).strip(): i for i, v in enumerate(vals[1:], start=2)}
    for _, r in df_order.iterrows():
        iid = str(r["항목ID"]).strip()
        if iid in pos:
            ws.update_cell(pos[iid], col_ord, int(r["순서"]))
    st.cache_data.clear()


# =============================================================================
# 평가 응답 유틸 (연 1회 / 시트명: '평가_응답_YYYY')
# =============================================================================
EVAL_RESP_SHEET_PREFIX = "평가_응답_"
EVAL_BASE_HEADERS = [
    "연도","평가유형","평가대상사번","평가대상이름",
    "평가자사번","평가자이름","총점","상태","제출시각",
    "서명_대상","서명시각_대상","서명_평가자","서명시각_평가자","잠금"
]
EVAL_TYPES = ["자기","1차","2차"]

def _eval_sheet_name(year: int | str) -> str:
    return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def _emp_name_by_sabun(emp_df: pd.DataFrame, sabun: str) -> str:
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    return "" if row.empty else str(row.iloc[0].get("이름",""))

def _ensure_eval_response_sheet(year: int, item_ids: list[str]) -> gspread.Worksheet:
    """연도별 응답 시트를 보장하고, 활성 항목ID에 대한 점수 컬럼과 서명/잠금 컬럼을 보강."""
    wb = get_workbook()
    sname = _eval_sheet_name(year)
    try:
        ws = wb.worksheet(sname)
    except Exception:
        ws = wb.add_worksheet(title=sname, rows=800, cols=100)
        ws.update("A1", [EVAL_BASE_HEADERS + [f"점수_{iid}" for iid in item_ids]])
        return ws

    header = ws.row_values(1) or []
    needed = list(EVAL_BASE_HEADERS) + [f"점수_{iid}" for iid in item_ids]
    add_cols = [h for h in needed if h not in header]
    if add_cols:
        new_header = header + add_cols
        ws.update("1:1", [new_header])
    return ws

def _eval_find_row(ws: gspread.Worksheet, hmap: dict, year: int, eval_type: str,
                   target_sabun: str, evaluator_sabun: str) -> int:
    """복합키(연도, 평가유형, 평가대상사번, 평가자사번)로 기존 행 검색. 없으면 0."""
    cY = hmap.get("연도"); cT = hmap.get("평가유형")
    cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    if not all([cY, cT, cTS, cES]):
        return 0
    values = ws.get_all_values()
    for i in range(2, len(values)+1):
        row = values[i-1]
        try:
            if (str(row[cY-1]).strip() == str(year).strip() and
                str(row[cT-1]).strip() == str(eval_type).strip() and
                str(row[cTS-1]).strip() == str(target_sabun).strip() and
                str(row[cES-1]).strip() == str(evaluator_sabun).strip()):
                return i
        except Exception:
            pass
    return 0

def upsert_eval_response(emp_df: pd.DataFrame, year: int, eval_type: str,
                         target_sabun: str, evaluator_sabun: str,
                         scores: dict[str, int], status: str = "제출") -> dict:
    """
    평가 응답 업서트.
      - 점수: {항목ID: 1~5}
      - 총점: 항목수×5를 100으로 정규화(반드시 100점 만점 스케일)
    """
    items = read_eval_items_df(only_active=True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)

    header = ws.row_values(1)
    hmap = {n: i+1 for i, n in enumerate(header)}

    # 총점(100점 만점) 계산
    scores_list = [int(scores.get(iid, 0)) for iid in item_ids]
    scores_list = [min(5, max(0, s)) for s in scores_list]
    raw = sum(scores_list)
    denom = max(1, len(item_ids) * 5)
    total_100 = round(raw * (100.0 / denom), 1)

    # 기본 필드 채우기
    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)

    # 기존행 찾기
    row_idx = _eval_find_row(ws, hmap, year, eval_type, target_sabun, evaluator_sabun)
    now = kst_now_str()

    # 신규 → append
    if row_idx == 0:
        rowbuf = [""] * len(header)
        def put(col_name, val):
            c = hmap.get(col_name)
            if c: rowbuf[c-1] = val

        put("연도", int(year))
        put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("총점", total_100); put("상태", status); put("제출시각", now)

        for iid in item_ids:
            cname = f"점수_{iid}"
            c = hmap.get(cname)
            if c:
                rowbuf[c-1] = min(5, max(1, int(scores.get(iid, 3))))  # 최소 1점 보장

        ws.append_row(rowbuf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "row": None, "total": total_100}

    # 기존행 → 업데이트
    ws.update_cell(row_idx, hmap["총점"], total_100)
    ws.update_cell(row_idx, hmap["상태"], status)
    ws.update_cell(row_idx, hmap["제출시각"], now)
    ws.update_cell(row_idx, hmap["평가대상이름"], t_name)
    ws.update_cell(row_idx, hmap["평가자이름"], e_name)
    for iid in item_ids:
        cname = f"점수_{iid}"
        c = hmap.get(cname)
        if c: ws.update_cell(row_idx, c, min(5, max(1, int(scores.get(iid, 3)))))

    st.cache_data.clear()
    return {"action": "update", "row": row_idx, "total": total_100}

def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    """내가 '평가자'로 제출한 해당 연도 응답."""
    sname = _eval_sheet_name(year)
    wb = get_workbook()
    try:
        ws = wb.worksheet(sname)
    except Exception:
        return pd.DataFrame(columns=EVAL_BASE_HEADERS)
    rows = ws.get_all_records(numericise_ignore=["all"])
    df = pd.DataFrame(rows)
    if df.empty: return df
    df = df[df["평가자사번"].astype(str) == str(sabun)]
    df = df.sort_values(["평가유형","평가대상사번","제출시각"], ascending=[True, True, False])
    return df

def sign_eval_response(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str,
                       who: str, signature_text: str) -> int:
    """
    who: '대상' 또는 '평가자'
    signature_text: 서명란에 저장할 텍스트(이름 등). 필요 시 이미지/BASE64로 확장 가능.
    반환: 업데이트된 행 번호(없으면 0)
    """
    # 활성 항목ID 확보(헤더 보강 위해)
    items = read_eval_items_df(only_active=True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)

    header = ws.row_values(1); hmap = {n:i+1 for i,n in enumerate(header)}
    idx = _eval_find_row(ws, hmap, year, eval_type, target_sabun, evaluator_sabun)
    if idx == 0:
        return 0

    if who == "대상":
        c_sig, c_at = hmap.get("서명_대상"), hmap.get("서명시각_대상")
    else:
        c_sig, c_at = hmap.get("서명_평가자"), hmap.get("서명시각_평가자")

    now = kst_now_str()
    if c_sig: ws.update_cell(idx, c_sig, signature_text)
    if c_at:  ws.update_cell(idx, c_at, now)

    st.cache_data.clear()
    return idx

def set_eval_lock(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str, locked: bool) -> int:
    """응답 행의 '잠금' 값을 True/False 로 설정. 반환: 행 번호(없으면 0)"""
    items = read_eval_items_df(only_active=True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)

    header = ws.row_values(1); hmap = {n:i+1 for i,n in enumerate(header)}
    idx = _eval_find_row(ws, hmap, year, eval_type, target_sabun, evaluator_sabun)
    if idx == 0:
        return 0
    c_lock = hmap.get("잠금")
    if c_lock:
        ws.update_cell(idx, c_lock, bool(locked))
        st.cache_data.clear()
    return idx


# =============================================================================
# 부서이력 유틸 (시트명: '부서이력')
# =============================================================================
HIST_SHEET = "부서이력"


def ensure_dept_history_sheet():
    """'부서이력' 시트를 보장. 없으면 생성 + 헤더 세팅."""
    wb = get_workbook()
    try:
        ws = wb.worksheet(HIST_SHEET)
        return ws
    except WorksheetNotFound:
        pass  # 정말 없을 때만 생성 시도

    # 새 시트 생성
    try:
        ws = wb.add_worksheet(title=HIST_SHEET, rows=200, cols=10)
        headers = ["사번", "이름", "부서1", "부서2", "시작일", "종료일", "변경사유", "승인자", "메모", "등록시각"]
        ws.update("A1", [headers])
        return ws
    except APIError:
        st.error("부서이력 시트를 만들 수 없습니다. (권한/시트수/보호 영역/쿼터 확인)")
        raise


@st.cache_data(ttl=60, show_spinner=False)
def read_dept_history_df() -> pd.DataFrame:
    """부서이력 전체를 DF로 읽기."""
    ensure_dept_history_sheet()
    wb = get_workbook()
    ws = wb.worksheet(HIST_SHEET)
    rows = ws.get_all_records(numericise_ignore=["all"])
    df = pd.DataFrame(rows)
    # 날짜 문자열 정리
    for c in ["시작일", "종료일", "등록시각"]:
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
    """해당 사번의 '종료일이 빈' 최신행을 종료일로 닫기."""
    header = ws_hist.row_values(1)
    hmap = {name: idx + 1 for idx, name in enumerate(header)}  # 1-based
    sabun_col = hmap.get("사번")
    end_col = hmap.get("종료일")
    if not (sabun_col and end_col):
        return
    values = ws_hist.get_all_values()
    for i in range(2, len(values) + 1):
        if values[i - 1][sabun_col - 1].strip() == str(sabun).strip():
            if values[i - 1][end_col - 1].strip() == "":
                ws_hist.update_cell(i, end_col, end_date)


def apply_department_change(
    emp_df: pd.DataFrame,
    sabun: str,
    new_dept1: str,
    new_dept2: str,
    start_date: datetime.date,
    reason: str = "",
    approver: str = "",
) -> dict:
    """
    부서 이동을 기록하고(부서이력), 필요 시 '직원' 시트의 현재 부서를 업데이트.
    """
    ensure_dept_history_sheet()
    wb = get_workbook()
    ws_hist = wb.worksheet(HIST_SHEET)

    today = datetime.now(tz=tz_kst()).date()
    start_str = start_date.strftime("%Y-%m-%d")
    prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # 직원 정보
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if row.empty:
        raise RuntimeError("사번을 찾지 못했습니다.")
    r = row.iloc[0]
    name = str(r.get("이름", ""))

    # 1) 기존 구간 닫기
    _hist_close_active_range(ws_hist, sabun=str(sabun), end_date=prev_end)

    # 2) 새 구간 append
    _hist_append_row(
        {
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
        }
    )

    # 3) 오늘 적용 대상이면 '직원' 현재부서도 갱신
    applied = False
    if start_date <= today:
        ws_emp, header_emp, hmap_emp = _get_ws_and_headers(EMP_SHEET)
        row_idx = _find_row_by_sabun(ws_emp, hmap_emp, str(sabun))
        if row_idx > 0:
            if "부서1" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서1"], new_dept1)
            if "부서2" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서2"], new_dept2)
            applied = True

    st.cache_data.clear()
    return {"applied_now": applied, "start_date": start_str, "new_dept1": new_dept1, "new_dept2": new_dept2}


def sync_current_department_from_history(as_of_date: datetime.date = None) -> int:
    """
    '부서이력'을 기준으로 '직원' 시트의 현재 부서(부서1/부서2)를 동기화.
    """
    ensure_dept_history_sheet()
    hist = read_dept_history_df()
    emp = read_sheet_df(EMP_SHEET)

    if as_of_date is None:
        as_of_date = datetime.now(tz=tz_kst()).date()
    D = as_of_date.strftime("%Y-%m-%d")

    updates = {}  # sabun -> (dept1, dept2)
    for sabun, grp in hist.groupby("사번"):
        def ok(row):
            s = row.get("시작일", "")
            e = row.get("종료일", "")
            return (s and s <= D) and ((not e) or e >= D)

        cand = grp[grp.apply(ok, axis=1)]
        if cand.empty:
            continue
        cand = cand.sort_values("시작일").iloc[-1]
        updates[str(sabun)] = (str(cand.get("부서1", "")), str(cand.get("부서2", "")))

    if not updates:
        return 0

    ws_emp, header_emp, hmap_emp = _get_ws_and_headers(EMP_SHEET)
    changed = 0
    for _, r in emp.iterrows():
        sabun = str(r.get("사번", ""))
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
# 관리자: 부서 이동 UI
# =============================================================================
def tab_admin_transfer(emp_df: pd.DataFrame):
    st.subheader("관리자 - 부서(근무지) 이동")
    st.caption("이동 이력 기록 + (필요 시) 직원 시트 현재부서 반영. 예정일 이동은 이력만 넣고, 나중에 '동기화'로 반영하세요.")

    ensure_dept_history_sheet()

    # 사번 선택
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"])
    sel = st.selectbox(
        "직원 선택(사번 - 이름)",
        ["(선택)"] + df["표시"].tolist(),
        index=0,
        key="transfer_emp_select",
    )

    if sel == "(선택)":
        st.info("사번을 선택하면 이동 입력 폼이 표시됩니다.")
        return

    sabun = sel.split(" - ", 1)[0]
    target = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]

    # 현재 정보
    c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
    with c1:
        st.metric("사번", str(target.get("사번", "")))
    with c2:
        st.metric("이름", str(target.get("이름", "")))
    with c3:
        st.metric("현재 부서1", str(target.get("부서1", "")))
    with c4:
        st.metric("현재 부서2", str(target.get("부서2", "")))

    st.divider()

    # 옵션 목록(기존 값 기반)
    opt_d1 = sorted([x for x in emp_df.get("부서1", []).dropna().unique() if x])
    opt_d2 = sorted([x for x in emp_df.get("부서2", []).dropna().unique() if x])

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        start_date = st.date_input(
            "시작일(발령일)", datetime.now(tz=tz_kst()).date(), key="transfer_start_date"
        )
    with colB:
        new_d1_pick = st.selectbox(
            "새 부서1(선택 또는 직접입력)", ["(직접입력)"] + opt_d1, index=0, key="transfer_new_dept1_pick"
        )
    with colC:
        new_d2_pick = st.selectbox(
            "새 부서2(선택 또는 직접입력)", ["(직접입력)"] + opt_d2, index=0, key="transfer_new_dept2_pick"
        )

    nd1 = st.text_input(
        "부서1 직접입력", value="" if new_d1_pick != "(직접입력)" else "", key="transfer_nd1"
    )
    nd2 = st.text_input(
        "부서2 직접입력", value="" if new_d2_pick != "(직접입력)" else "", key="transfer_nd2"
    )

    new_dept1 = new_d1_pick if new_d1_pick != "(직접입력)" else nd1
    new_dept2 = new_d2_pick if new_d2_pick != "(직접입력)" else nd2

    colR = st.columns([2, 3])
    with colR[0]:
        reason = st.text_input("변경사유", "", key="transfer_reason")
    with colR[1]:
        approver = st.text_input("승인자", "", key="transfer_approver")

    ok = st.button(
        "이동 기록 + 현재 반영", type="primary", use_container_width=True, key="transfer_apply_btn"
    )

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
                st.success(
                    f"이동 기록 완료 및 현재부서 반영: {rep['new_dept1']} / {rep['new_dept2']} (시작일 {rep['start_date']})",
                    icon="✅",
                )
            else:
                st.info(
                    f"이동 이력만 기록됨(시작일 {rep['start_date']}). 해당 날짜 이후 '동기화'에서 일괄 반영하세요.",
                    icon="ℹ️",
                )
            st.toast("부서 이동 처리됨", icon="✅")
        except Exception as e:
            st.exception(e)

    # 개인 이력 미리보기
    try:
        hist = read_dept_history_df()
        my = hist[hist["사번"] == str(sabun)].copy()
        if not my.empty:
            my = my.sort_values(["시작일", "등록시각"], ascending=[False, False])
            st.markdown("#### 개인 부서이력")
            st.dataframe(
                my[["시작일", "종료일", "부서1", "부서2", "변경사유", "승인자"]],
                use_container_width=True,
                height=260,
            )
    except Exception:
        pass

    st.divider()
    colSync = st.columns([1, 2])
    with colSync[0]:
        if st.button("오늘 기준 전체 동기화", use_container_width=True):
            try:
                cnt = sync_current_department_from_history()
                st.success(f"직원 시트 현재부서 동기화 완료: {cnt}명 반영", icon="✅")
                st.toast("동기화 완료", icon="✅")
            except Exception as e:
                st.exception(e)


# =============================================================================
# 관리자: 평가 항목 관리 UI
# =============================================================================
def tab_admin_eval_items():
    st.subheader("관리자 - 평가 항목 관리 (1~5점 척도, 총 20개)")
    st.caption("가중치 없이 20개 항목을 1~5점으로 평가하면 합계가 100점 만점이 됩니다. (영역 없음)")

    df = read_eval_items_df(only_active=False)
    st.write(f"현재 등록 항목: **{len(df)}개** (활성: {df[df['활성']==True].shape[0]}개)")

    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        edit_df = df[["항목ID", "항목", "순서", "활성"]].copy().reset_index(drop=True)
        edited = st.data_editor(
            edit_df,
            use_container_width=True,
            height=380,
            column_config={
                "항목ID": st.column_config.TextColumn(disabled=True),
                "항목": st.column_config.TextColumn(disabled=True),
                "활성": st.column_config.CheckboxColumn(disabled=True),
                "순서": st.column_config.NumberColumn(step=1, min_value=0),
            },
            num_rows="fixed",
        )
        if st.button("순서 일괄 저장", use_container_width=True):
            try:
                update_eval_items_order(edited[["항목ID", "순서"]])
                st.success("순서가 반영되었습니다.")
                st.rerun()
            except Exception as e:
                st.exception(e)

    st.divider()
    st.markdown("### 신규 등록 / 수정")

    choices = ["(신규)"] + [f"{r['항목ID']} - {r['항목']}" for _, r in df.iterrows()]
    sel = st.selectbox("대상 선택", choices, index=0, key="eval_item_pick")

    # 기본값
    item_id = None
    name = ""
    desc = ""
    order = (df["순서"].max() + 1 if not df.empty else 1)
    active = True
    memo = ""

    if sel != "(신규)":
        iid = sel.split(" - ", 1)[0]
        row = df.loc[df["항목ID"] == iid].iloc[0]
        item_id = row["항목ID"]
        name = str(row.get("항목", ""))
        desc = str(row.get("내용", ""))
        order = int(row.get("순서", 0))
        active = bool(row.get("활성", True))
        memo = str(row.get("비고", ""))

    c1, c2 = st.columns([3, 1])
    with c1:
        name = st.text_input("항목명", value=name, placeholder="예: 책임감", key="eval_item_name")
        desc = st.text_area("설명(문항 내용)", value=desc, height=100, key="eval_item_desc")
        memo = st.text_input("비고(선택)", value=memo, key="eval_item_memo")
    with c2:
        order = st.number_input("순서", min_value=0, step=1, value=int(order), key="eval_item_order")
        active = st.checkbox("활성", value=active, key="eval_item_active")

        if st.button("저장(신규/수정)", type="primary", use_container_width=True):
            if not name.strip():
                st.error("항목명을 입력하세요.")
            else:
                try:
                    new_id = upsert_eval_item(
                        item_id=item_id,
                        name=name.strip(),
                        desc=desc.strip(),
                        order=int(order),
                        active=bool(active),
                        memo=memo.strip(),
                    )
                    st.success(f"저장 완료 (항목ID: {new_id})")
                    st.rerun()
                except Exception as e:
                    st.exception(e)

        st.write("")
        if item_id:
            if st.button("비활성화(소프트 삭제)", use_container_width=True):
                try:
                    deactivate_eval_item(item_id)
                    st.success("비활성화 완료")
                    st.rerun()
                except Exception as e:
                    st.exception(e)
            if st.button("행 삭제(완전 삭제)", use_container_width=True):
                try:
                    delete_eval_item_row(item_id)
                    st.success("삭제 완료")
                    st.rerun()
                except Exception as e:
                    st.exception(e)


# =============================================================================
# 탭: 평가 입력 (자기/1차/2차 공용) — 버튼형(1~5)만, 0점 없음, 라인 정렬
# =============================================================================
def tab_eval_input(emp_df: pd.DataFrame):
    st.subheader("평가 입력 (자기 / 1차 / 2차)")
    this_year = datetime.now(tz=tz_kst()).year
    colY = st.columns([1, 3])
    with colY[0]:
        year = st.number_input("평가 연도", min_value=2000, max_value=2100, value=int(this_year), step=1)

    items = read_eval_items_df(only_active=True)
    if items.empty:
        st.warning("활성화된 평가 항목이 없습니다. 관리자에게 문의하세요.", icon="⚠️")
        return

    u = st.session_state["user"]
    me_sabun = str(u["사번"])
    me_name  = str(u["이름"])
    is_admin = bool(u.get("관리자여부", False))

    st.markdown("#### 대상/유형 선택")
    if is_admin:
        df = emp_df.copy()
        if "재직여부" in df.columns:
            df = df[df["재직여부"] == True]
        df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df = df.sort_values(["사번"])
        sel = st.selectbox("평가 **대상자** (사번 - 이름)", ["(선택)"] + df["표시"].tolist(), index=0)
        if sel == "(선택)":
            st.info("평가 대상자를 선택하세요.")
            return
        target_sabun = sel.split(" - ", 1)[0]
        target_name = _emp_name_by_sabun(emp_df, target_sabun)
        eval_type = st.radio("평가유형", EVAL_TYPES, horizontal=True)
        evaluator_sabun = me_sabun
        evaluator_name  = me_name
        st.caption(f"평가자: {evaluator_name} ({evaluator_sabun})")
    else:
        target_sabun = me_sabun
        target_name  = me_name
        eval_type = "자기"
        evaluator_sabun = me_sabun
        evaluator_name  = me_name
        st.info(f"대상자: {target_name} ({target_sabun}) · 평가유형: 자기", icon="👤")

    # ─────────────────────────────────────────────────────────────
    # 점수 입력 UI — 버튼(1~5)만, 한 줄 정렬, 0점 없음, 기본값 3
    # ─────────────────────────────────────────────────────────────
    st.markdown("#### 점수 입력 (각 1~5)")
    st.caption("모든 항목은 1~5 중 하나를 반드시 선택합니다. (기본 3)")

    # 줄맞춤을 위한 Grid 스타일 (행 전체에 하나의 밑줄만)
    st.markdown(
        """
        <style>
          .score-grid { 
            display: grid; 
            grid-template-columns: 2fr 7fr 3fr; 
            align-items: center;
            gap: 0.5rem;
            padding: 10px 6px;
            border-bottom: 1px solid rgba(49,51,63,.10);
          }
          .score-grid .name { font-weight: 700; }
          .score-grid .desc { color: #4b5563; }
          .score-grid .input { display:flex; align-items:center; justify-content:center; }
          .score-grid .input div[role="radiogroup"] { 
            display:flex; gap: 10px; align-items:center; justify-content:center; 
          }
          .score-head {font-size: .9rem; color:#6b7280; margin-bottom: .4rem;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="score-head">항목 / 내용 / 점수</div>', unsafe_allow_html=True)

    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    scores = {}

    for r in items_sorted.itertuples(index=False):
        iid  = getattr(r, "항목ID")
        name = getattr(r, "항목") or ""
        desc = getattr(r, "내용") or ""

        # 저장된 값 없으면 3, 1~5 보장
        cur_val = int(st.session_state.get(f"score_{iid}", 3))
        if cur_val < 1 or cur_val > 5:
            cur_val = 3

        # 행 그리드 시작
        st.markdown('<div class="score-grid">', unsafe_allow_html=True)
        st.markdown(f'<div class="name">{name}</div>', unsafe_allow_html=True)  # 좌: 항목명
        st.markdown(f'<div class="desc">{desc.replace(chr(10), "<br/>")}</div>', unsafe_allow_html=True)  # 중: 설명
        st.markdown('<div class="input">', unsafe_allow_html=True)  # 우: 점수

        if getattr(st, "segmented_control", None):
            new_val = st.segmented_control(
                " ",
                options=[1, 2, 3, 4, 5],
                format_func=lambda x: str(x),
                default_value=cur_val,
                key=f"seg_{iid}",
            )
        else:
            new_val = int(
                st.radio(
                    " ",
                    ["1", "2", "3", "4", "5"],
                    index=(cur_val - 1),
                    horizontal=True,
                    key=f"seg_{iid}",
                    label_visibility="collapsed",
                )
            )

        st.markdown('</div>', unsafe_allow_html=True)   # .input 닫기
        st.markdown('</div>', unsafe_allow_html=True)   # .score-grid 닫기

        # 값 보관 (1~5 보장)
        new_val = min(5, max(1, int(new_val)))
        scores[str(iid)] = new_val
        st.session_state[f"score_{iid}"] = new_val

    # 합계(100점 만점) 계산 및 표시
    raw = int(sum(scores.values()))
    denom = max(1, len(items_sorted) * 5)  # 항목수 × 5
    total_100 = round(raw * (100.0 / denom), 1)

    st.markdown("---")
    cM1, cM2 = st.columns([1, 3])
    with cM1:
        st.metric("합계(100점 만점)", total_100)
    with cM2:
        st.progress(min(1.0, total_100 / 100.0), text=f"총점 {total_100}점")

    # 제출/저장 & 리셋
    col_submit = st.columns([1, 1, 4])
    with col_submit[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True)
    with col_submit[1]:
        do_reset = st.button("모든 점수 3점으로", use_container_width=True)

    if do_reset:
        for r in items_sorted.itertuples(index=False):
            st.session_state[f"score_{getattr(r, '항목ID')}"] = 3
        st.rerun()

    if do_save:
        try:
            rep = upsert_eval_response(
                emp_df=emp_df,
                year=int(year),
                eval_type=eval_type,
                target_sabun=str(target_sabun),
                evaluator_sabun=str(evaluator_sabun),
                scores=scores,
                status="제출",
            )
            if rep["action"] == "insert":
                st.success(f"제출 완료 (총점 {rep['total']}점)", icon="✅")
            else:
                st.success(f"업데이트 완료 (총점 {rep['total']}점)", icon="✅")
            st.toast("평가 저장됨", icon="✅")
        except Exception as e:
            st.exception(e)

    st.markdown("#### 내 제출 현황")
    try:
        my = read_my_eval_rows(int(year), evaluator_sabun)
        if my.empty:
            st.caption("제출된 평가가 없습니다.")
        else:
            st.dataframe(
                my[["평가유형", "평가대상사번", "평가대상이름", "총점", "상태", "제출시각"]],
                use_container_width=True,
                height=260,
            )
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")


# =============================================================================
# 메인
# =============================================================================
def main():
    st.title(APP_TITLE)
    render_status_line()

    # 1) 데이터 읽기
    try:
        emp_df = read_sheet_df(EMP_SHEET)
    except Exception as e:
        st.error(f"'{EMP_SHEET}' 시트 로딩 실패: {e}")
        return

    # 2) 로그인 요구
    require_login(emp_df)

    # 3) 사이드바 사용자/로그아웃
    u = st.session_state["user"]
    with st.sidebar:
        st.write(f"👤 **{u['이름']}** ({u['사번']})")
        if st.button("로그아웃", use_container_width=True):
            logout()

    # 4) 탭 구성 (도움말은 항상 맨 오른쪽)
    if u.get("관리자여부", False):
        tabs = st.tabs(["직원", "평가", "관리자", "도움말"])
    else:
        tabs = st.tabs(["직원", "평가", "도움말"])

    # 직원
    with tabs[0]:
        tab_staff(emp_df)

    # 평가
    with tabs[1]:
        tab_eval_input(emp_df)

    # 관리자
    if u.get("관리자여부", False):
        with tabs[2]:
            st.subheader("관리자 메뉴")
            admin_page = st.radio(
                "기능 선택",
                ["PIN 관리", "부서(근무지) 이동", "평가 항목 관리"],
                horizontal=True,
                key="admin_page_selector",
            )
            st.divider()
            if admin_page == "PIN 관리":
                tab_admin_pin(emp_df)
            elif admin_page == "부서(근무지) 이동":
                tab_admin_transfer(emp_df)
            else:
                tab_admin_eval_items()

    # 도움말(맨 오른쪽)
    with tabs[-1]:
        st.markdown(
            """
            ### 사용 안내
            - Google Sheets 연동 조회/관리
            - 직원 시트명은 기본 **직원**이며, `secrets.toml`의 `[sheets].EMP_SHEET`로 변경 가능
            - 관리자: **개별 PIN/일괄 PIN/부서이동/평가항목 관리**
            - 평가: **자기/1차/2차 입력(1~5점)** — ITM 코드는 숨김, 항목/내용/점수 3열 정렬
            """
        )


# =============================================================================
if __name__ == "__main__":
    main()

