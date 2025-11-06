# -*- coding: utf-8 -*-

import re
import streamlit as st
from typing import Any, Tuple
import re, time, random, hashlib, secrets as pysecrets
from typing import Any, Optional
from html import escape as _html_escape
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError
from gspread.utils import rowcol_to_a1
import gspread
import hmac
import streamlit.components.v1 as components
from gspread.utils import rowcol_to_a1 as _rowcol_to_a1
from zoneinfo import ZoneInfo
import html
from typing import Tuple, Dict
from gspread.exceptions import APIError as _APIError
import pytz
import re as _re
import re as _re_local
import pandas as pd
from datetime import datetime

# ────────────────────────────────────────────────────────────────
# Page config -> 반드시 가장 먼저 호출 (이 두 줄은 그대로!)
# ────────────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI † HR · JD")
st.set_page_config(page_title=APP_TITLE, layout="wide")

# ▼ 바로 아래에 둡니다 (레이아웃 폭은 건드리지 않음)
st.markdown("""
<style>
  /* 상단 여백만 살짝 줄임 */
  :where([data-testid="stAppViewContainer"]) .block-container { padding-top: 0.1rem !important; }

  /* 제목: 통일/굵게/약간 크게 */
  .app-title-hero{
    font-weight: 800; font-size: 1.6rem; line-height: 1.15; margin: .1rem 0 .2rem !important;
  }
  @media (min-width:1400px){ .app-title-hero{ font-size:1.75rem; } }

  /* 캡션(“DB연결 …”) 위/아래 간격도 축소 */
  :where([data-testid="stCaptionContainer"]){
    margin: .05rem 0 .15rem !important; line-height: 1.25;
  }

  /* 탭: 볼드 + 간격 확장 (신/구 DOM 동시 대응) */
  .stTabs [role='tab']{ font-weight:700 !important; }
  .stTabs [role='tablist']{ gap: 18px !important; }
  .stTabs button[role='tab']{ font-weight:700 !important; margin-right:18px !important; }
  div[data-baseweb="tab-list"] button{ font-weight:700 !important; margin-right:18px !important; }
</style>
""", unsafe_allow_html=True)

# 제목은 한 번만 여기서 출력 (로그인 전/후 공통, 최상단 고정)
st.markdown(f"<div class='app-title-hero'>{APP_TITLE}</div>", unsafe_allow_html=True)

# ────────────────────────────────────────────────────────────────
# 공용 유틸
# ────────────────────────────────────────────────────────────────
def _ensure_capacity(ws, min_row: int, min_col: int):
    """워크시트 최소 (min_row x min_col) 크기 보장. 필요한 경우에만 행/열 확장."""
    try:
        r_needed = int(min_row) if min_row is not None else 0
        c_needed = int(min_col) if min_col is not None else 0
        if hasattr(ws, "row_count") and ws.row_count < r_needed:
            ws.add_rows(r_needed - int(ws.row_count))
        if hasattr(ws, "col_count") and ws.col_count < c_needed:
            ws.add_cols(c_needed - int(ws.col_count))
    except Exception:
        # 시트 객체가 예상과 다르거나 권한 문제 등으로 실패해도 앱이 죽지 않도록 방어
        pass

# HISMEDI † HR · JD  app
# Tabs: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말

# ═════════════════════════════════════════════════════════════════════════════
# Imports
# ═════════════════════════════════════════════════════════════════════════════

# ==============================================================================
# Helper Utilities (pure functions) — hardened
# ==============================================================================

def _to_bool(x: Any, *, default: bool = False) -> bool:
    """
    문자열/숫자/불리언을 안전하게 bool로 변환한다.
    - True로 인식: "true","1","y","yes","t","on","enabled","enable","ok","예","응","그래"
    - False로 인식: "false","0","n","no","f","off","disabled","disable","아니오","아냐","아니","ㄴ"
    - 불리언이면 그대로 반환.
    - int/float는 0이면 False, 그 외는 True.
    - None 또는 공백/미매핑 문자열은 default 반환(기본 False).
    변환은 대소문자/공백/양끝 따옴표를 무시하고 처리한다.
    """
    if isinstance(x, bool):
        return x
    if x is None:
        return default

    s = str(x).strip().strip('\'"').lower()

    truthy = {
        "true","1","y","yes","t","on","enabled","enable","ok",
        "예","응","그래"
    }
    falsy = {
        "false","0","n","no","f","off","disabled","disable",
        "아니오","아냐","아니","ㄴ"
    }

    if s in truthy:
        return True
    if s in falsy:
        return False

    # 숫자 문자열일 수 있음
    try:
        num = float(s)
        return num != 0.0
    except Exception:
        pass

    return default

def _normalize_private_key(raw: Optional[str]) -> Optional[str]:
    """
    Secrets/환경변수에 PEM/OPENSSH 개인키가 '이스케이프된 문자열'로 저장된 경우
    (예: '\\n', '\\r\\n', '\\t'가 리터럴 문자로 들어간 상태) 실제 제어문자로 복원한다.
    또한 줄바꿈/경계선/말미 개행 등 포맷을 최대한 표준에 가깝게 정돈한다.

    처리 내용:
    1) None/빈값은 그대로 반환.
    2) 리터럴 "\\r\\n" → "\n", "\\n" → "\n", "\\t" → "\t"
    3) 실제로 섞여 들어간 "\r" 제거(모두 LF로 변환).
    4) 다양한 프롤로그에 대응: "BEGIN PRIVATE KEY", "BEGIN RSA PRIVATE KEY",
       "BEGIN OPENSSH PRIVATE KEY" 모두 지원.
    5) BEGIN/END 라인이 양끝 공백 없이 '단독 라인'이 되도록 트리밍.
    6) 파일 끝에 개행 1개를 보장(일부 라이브러리에서 필요).
    7) 이미 정상 포맷이면 idempotent하게 동일 문자열을 돌려줌.
    """
    if not raw:
        return raw

    s = str(raw)

    # 1) 우선 흔한 리터럴 이스케이프를 실제 제어문자로 복원
    #    (환경에 따라 이미 실제 제어문자일 수 있으므로 두 경우 모두 안전)
    s = s.replace("\\r\\n", "\n")
    s = s.replace("\\n", "\n")
    s = s.replace("\\t", "\t")

    # 2) CRLF → LF 통일
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # 3) BEGIN/END 가 포함된 PEM/OPENSSH 키인지 식별
    markers = (
        "BEGIN PRIVATE KEY",
        "BEGIN RSA PRIVATE KEY",
        "BEGIN OPENSSH PRIVATE KEY",
    )
    if not any(m in s for m in markers):
        # 키 포맷 문구가 없다면, 단순히 줄바꿈/탭만 정리한 결과를 반환
        # (불필요한 과도한 가공을 피함)
        return s

    # 4) 라인 단위로 정리: 트레일링 스페이스 제거, 빈줄 정리
    lines = s.split("\n")

    def _trim_boundary(line: str) -> str:
        # '-----BEGIN ...-----' 또는 '-----END ...-----' 라인은 앞뒤 공백 제거
        t = line.strip()
        if t.startswith("-----BEGIN ") or t.startswith("-----END "):
            return t
        return line.rstrip()  # 중간 라인은 오른쪽 공백만 제거

    lines = [_trim_boundary(ln) for ln in lines]

    # 5) BEGIN/END 경계 보정: 경계가 중간에 끼어 있거나 공백 라인에 둘러싸여도 정돈
    #    불필요한 앞뒤 빈 라인 제거
    while lines and not lines[0].startswith("-----BEGIN "):
        # BEGIN 전의 잡다한 라인은 제거(환경변수 주석 등)
        if lines[0].strip() == "":
            lines.pop(0)
        else:
            break
    while lines and lines[-1].strip() == "":
        lines.pop()

    # 6) 마지막에 정확히 하나의 개행 보장
    normalized = "\n".join(lines)
    if not normalized.endswith("\n"):
        normalized += "\n"

    return normalized

# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

def _is_quota_429(err) -> bool:
    """gspread APIError의 429(쿼터 초과) 감지."""
    try:
        if isinstance(err, _APIError):
            resp = getattr(err, "response", None)
            code = getattr(resp, "status_code", None)
            return code == 429
    except Exception:
        pass
    return False

AUTO_FIX_HEADERS = False

# ===== Cached summary helpers (performance) =====
@st.cache_data(ttl=300, show_spinner=False)
def get_eval_summary_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {(사번, 평가유형)->(총점, 제출시각)} for the year (헤더 변형에 강건).
    의존: read_eval_items_df, _ensure_eval_resp_sheet, _retry, _ws_values, _extract_year
    """
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()] if not items.empty else []
    try:
        ws = _ensure_eval_resp_sheet(int(_year), item_ids)
        header = _retry(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _ws_values(ws)
    except Exception:
        return {}

    cY   = hmap.get('연도') or hmap.get('년도')
    cType= hmap.get('평가유형')
    cTS  = hmap.get('평가대상사번') or hmap.get('사번')
    cTot = hmap.get('총점')
    cSub = hmap.get('제출시각') or hmap.get('제출일시') or hmap.get('제출시간')

    # 필수 키가 하나도 없으면 빈 결과
    if not cTS:
        return {}

    out: dict[tuple[str, str], tuple[str, str]] = {}
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            ry = (str(r[cY - 1]).strip() if cY else _extract_year(r[cSub - 1] if cSub else ''))
            if str(ry) != str(_year):
                continue
            k0 = str(r[cTS - 1]).strip() if cTS else ''
            k1 = str(r[cType - 1]).strip() if cType else ''
            if not k0:
                continue
            tot = str(r[cTot - 1]) if cTot else ''
            sub = str(r[cSub - 1]) if cSub else ''
            k = (k0, k1)
            if k not in out or str(out[k][1]) < sub:
                out[k] = (tot, sub)
        except Exception:
            pass
    return out

@st.cache_data(ttl=300, show_spinner=False)
def get_comp_summary_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {사번->(주업무, 기타업무, 자격유지, 제출시각)} for the year (헤더 변형에 강건).
    의존: _ensure_comp_simple_sheet, _retry, _ws_values, _extract_year
    """
    try:
        ws = _ensure_comp_simple_sheet(int(_year))
        header = _retry(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _ws_values(ws)
    except Exception:
        return {}

    cY    = hmap.get('연도') or hmap.get('년도')
    cTS   = hmap.get('평가대상사번') or hmap.get('사번')
    cMain = hmap.get('주업무평가') or hmap.get('주업무')
    cExtra= hmap.get('기타업무평가') or hmap.get('기타업무')
    cQual = hmap.get('자격유지') or hmap.get('자격')
    cSub  = hmap.get('제출시각') or hmap.get('제출일시') or hmap.get('제출시간')

    if not cTS:
        return {}

    out: dict[str, tuple[str, str, str, str]] = {}
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            ry = (str(r[cY - 1]).strip() if cY else _extract_year(r[cSub - 1] if cSub else ''))
            if str(ry) != str(_year):
                continue
            sab  = str(r[cTS - 1]).strip()
            if not sab:
                continue
            main = str(r[cMain - 1]) if cMain else ''
            extra= str(r[cExtra - 1]) if cExtra else ''
            qual = str(r[cQual - 1]) if cQual else ''
            sub  = str(r[cSub - 1]) if cSub else ''
            if sab not in out or str(out[sab][3]) < sub:
                out[sab] = (main, extra, qual, sub)
        except Exception:
            pass
    return out

@st.cache_data(ttl=120, show_spinner=False)
def get_jd_approval_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {(사번, 최신버전)->(상태, 승인시각)} for the year from '직무기술서_승인'.
    의존: _ws, _ws_get_all_records, pd
    """
    try:
        ws = _ws("직무기술서_승인")
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        df = pd.DataFrame(columns=["연도","사번","버전","상태","승인시각"])

    # 타입 정리
    for c in ["연도", "버전"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in ["사번", "상태", "승인시각"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # 안전한 연도 필터
    yr = pd.to_numeric(df.get("연도", pd.Series([None]*len(df))), errors="coerce").fillna(-1).astype(int)
    df = df[yr == int(_year)]

    out = {}
    if not df.empty:
        df = df.sort_values(["사번", "버전", "승인시각"], ascending=[True, True, True]).reset_index(drop=True)
        for _, rr in df.iterrows():
            k = (str(rr.get("사번","")), int(rr.get("버전",0)))
            out[k] = (str(rr.get("상태","")), str(rr.get("승인시각","")))
    return out

# Optional zoneinfo (KST)
try:
    def tz_kst():
        return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    def tz_kst():
        return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# gspread (배포 최적화: 자동 pip 설치 제거, 의존성 사전 설치 전제)

# --- Safe shim for batch helpers: defined early to avoid NameError ---
try:
    _ = rowcol_to_a1  # ensure imported

    if 'gs_enqueue_range' not in globals():
        def gs_enqueue_range(ws, a1, values, value_input_option="USER_ENTERED"):
            # ✅ gspread 시그니처: update(range_name, values, ...)
            ws.update(a1, values, value_input_option=value_input_option)

    if 'gs_enqueue_cell' not in globals():
        def gs_enqueue_cell(ws, row, col, value, value_input_option="USER_ENTERED"):
            ws.update(rowcol_to_a1(int(row), int(col)), [[value]], value_input_option=value_input_option)

    if 'gs_flush' not in globals():
        def gs_flush():
            return  # no-op (배치 큐가 아니라 즉시 업데이트 방식)
except Exception:
    pass
# --- end shim ---

# ═════════════════════════════════════════════════════════════════════════════
# Sync Utility (Force refresh Google Sheets caches)
# ═════════════════════════════════════════════════════════════════════════════
def force_sync(min_interval: int = 25):
    """Safely refresh data caches. Throttle & lock; keep session/auth stable.
    - Throttle: ignore if called within min_interval seconds of last run
    - Lock: prevent re-entrancy while running
    - Clear only data cache (keep resource cache warm)
    - Preserve key session values
    """
    now = time.time()

    # Re-entrancy guard
    if st.session_state.get("_sync_lock", False):
        return

    # Throttle
    last_ts = float(st.session_state.get("_last_sync_ts", 0.0) or 0.0)
    if now - last_ts < float(min_interval):
        return

    st.session_state["_sync_lock"] = True
    try:
        # Clear only data cache (avoid cold starts on resources/auth)
        try:
            # streamlit >=1.18
            st.cache_data.clear()
        except Exception:
            pass

        # Clear module-level lightweight caches
        for _c in ('_WS_CACHE', '_HDR_CACHE', '_VAL_CACHE'):
            d = globals().get(_c, None)
            if isinstance(d, dict):
                try:
                    d.clear()
                except Exception:
                    pass

        # Session pruning (keep user/auth & selections)
        SAFE_KEEP = {
            "user","authed","auth_expires_at","_state_owner_sabun",
            "glob_target_sabun","glob_target_name",
            "left_pick","pick_q",
            "_last_sync_ts","_sync_lock"
        }
        PREFIXES = ("eval", "jd", "cmpS", "cmpD")
        ACL_KEYS  = {"acl_df", "acl_header", "acl_editor", "auth_editor"}

        try:
            to_del = []
            for k in list(st.session_state.keys()):
                if k in SAFE_KEEP:
                    continue
                if k in ACL_KEYS or any(k.startswith(p) for p in PREFIXES):
                    to_del.append(k)
            for k in to_del:
                del st.session_state[k]
        except Exception:
            pass

        # Mark refreshed BEFORE rerun
        st.session_state["_last_sync_ts"] = now

    finally:
        # ✅ unlock BEFORE rerun to avoid rare lock persistence
        st.session_state["_sync_lock"] = False

    # Trigger rerun last
    st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# Utils
# ═════════════════════════════════════════════════════════════════════════════

def current_year() -> int:
    """Return KST-based current year if tz_kst() is available, otherwise system year."""
    try:
        return datetime.now(tz=tz_kst()).year  # tz_kst() must return a tzinfo
    except Exception:
        return datetime.now().year

def kst_now_str():
    try:
        return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _jd_plain_html(text: str) -> str:
    if text is None:
        text = ""
    s = str(text).replace("\r\n", "\n").replace("\r", "\n")
    return '<div class="jd-tight">' + html.escape(s).replace("\n", "<br>") + "</div>"

# --- Year normalization helper ----------------------------------------------
def _extract_year(val):
    try:
        s = str(val).strip()
        if not s:
            return ""
        m = _re.search(r'(19|20)\d{2}', s)
        return m.group(0) if m else ""
    except Exception:
        return ""

# --- Dashboard year-scoped helpers (robust) -----------------------------------
def _dash_eval_scores_for_year(_year: int) -> dict:
    """Return {(sabun, eval_type)->(total, submitted_at)} for the given year.
    - Accepts header variants and parses year from 제출시각 if needed."""
    try:
        ws = _ws(EVAL_RESP_SHEET_NAME)  # "인사평가"
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return {}
    if df.empty:
        return {}

    col = lambda *names: next((n for n in names if n in df.columns), None)
    cY    = col("연도","년도")
    cTS   = col("평가대상사번","사번")
    cType = col("평가유형","유형")
    cTot  = col("총점","합계","점수")
    cSub  = col("제출시각","제출일시","제출시간")

    # Year filter
    if cY:
        df = df[df[cY].astype(str) == str(_year)]
    elif cSub:
        df = df[(df[cSub].astype(str).map(_extract_year).astype(str) == str(_year))]
    else:
        return {}

    # Normalize 평가유형 -> {'자기','1차','2차'}
    def norm_type(x: str) -> str:
        s = str(x or "").strip().lower()
        if s in ("자기","self","self-eval","자기평가","본인","본인평가"): return "자기"
        if s in ("1차","1차평가","manager","mgr","상급자","부서장","부서장평가"): return "1차"
        if s in ("2차","2차평가","admin","hr","최종","인사","경영진"): return "2차"
        return str(x).strip() or ""

    df["_T"]   = df[cType].map(norm_type) if cType else ""
    df["_SUB"] = df[cSub].astype(str) if cSub else ""

    out: dict[tuple[str,str], tuple[str,str]] = {}
    for _, r in df.iterrows():
        sab = str(r.get(cTS, "")).strip()
        tp  = str(r.get("_T","")).strip()
        tot = str(r.get(cTot, "")) if cTot else ""
        sub = str(r.get("_SUB",""))
        if not sab or not tp:
            continue
        k = (sab, tp)
        if k not in out or str(out[k][1]) < str(sub):
            out[k] = (tot, sub)
    return out

def _dash_comp_status_for_year(_year: int) -> dict:
    """Return {sabun->(main, extra, qual, submitted_at)} for the given year.
    - Accepts header variants and parses year from 제출시각 if needed."""
    try:
        ws = _ws(COMP_SIMPLE_NAME)  # "직무능력평가"
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return {}
    if df.empty:
        return {}

    col = lambda *names: next((n for n in names if n in df.columns), None)
    cY    = col("연도","년도")
    cTS   = col("평가대상사번","사번")
    cMain = col("주업무평가","주업무")
    cExtra= col("기타업무평가","기타업무")
    cQual = col("자격유지","자격")
    cSub  = col("제출시각","제출일시","제출시간")

    # Year filter
    if cY:
        df = df[df[cY].astype(str) == str(_year)]
    elif cSub:
        df = df[(df[cSub].astype(str).map(_extract_year).astype(str) == str(_year))]
    else:
        return {}

    df["_SUB"] = df[cSub].astype(str) if cSub else ""
    out: dict[str, tuple[str,str,str,str]] = {}
    for _, r in df.iterrows():
        sab  = str(r.get(cTS, "")).strip()
        main = str(r.get(cMain, "")) if cMain else ""
        extra= str(r.get(cExtra, "")) if cExtra else ""
        qual = str(r.get(cQual, "")) if cQual else ""
        sub  = str(r.get("_SUB",""))
        if not sab:
            continue
        if sab not in out or str(out[sab][3]) < str(sub):
            out[sab] = (main, extra, qual, sub)
    return out

def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()

def _pin_hash(pin: str, sabun: str) -> str:
    p = "" if pin is None else str(pin).strip()
    s = "" if sabun is None else str(sabun).strip()
    return hashlib.sha256(f"{s}:{p}".encode()).hexdigest()

def show_submit_banner(text: str):
    try:
        st.markdown(
            "<div style=\"background:#FEF3C7;border:1px solid #FDE68A;padding:.55rem .8rem;border-radius:.5rem;font-weight:600;line-height:1.35;margin:0 0 12px 0;\">"
            f"{text}</div>",
            unsafe_allow_html=True
        )
    except Exception:
        st.info(text)

def _eq(a: str, b: str) -> bool:
    # 안전한 상수시간 비교 (hex 문자열 케이스)
    try:
        return hmac.compare_digest(str(a), str(b))
    except Exception:
        return str(a) == str(b)

def _norm_str(v) -> str:
    return "" if v is None else str(v).strip()

def verify_pin(user_sabun: str, pin: str) -> bool:
    """
    제출 직전 PIN 재인증.
    허용 저장 형태: SHA256(pin) 또는 SHA256(sabun:pin)
    우선순위:
      1) st.session_state["pin_map"]       → 평문 비교(개발/임시 용도)
      2) st.session_state["pin_hash_map"]  → 해시 비교
      3) st.session_state["user"]          → pin / pin_hash 필드 비교
      4) st.session_state["emp_df"]        → 직원시트 기반 보조 검증
    """
    sabun = _norm_str(user_sabun)
    # 숫자로 들어와도 선행 0 보존: 무조건 문자열 취급
    val = _norm_str(pin)
    if not (sabun and val):
        return False

    # 미리 두 형태의 후보 해시 계산
    h1 = _sha256_hex(val)            # SHA256(pin)
    h2 = _pin_hash(val, sabun)       # SHA256(sabun:pin)

    # 1) 평문 맵
    pin_map = st.session_state.get("pin_map", {}) or {}
    if sabun in pin_map:
        if _eq(_norm_str(pin_map.get(sabun, "")), val):
            return True

    # 2) 해시 맵
    pin_hash_map = st.session_state.get("pin_hash_map", {}) or {}
    if sabun in pin_hash_map:
        stored_hash = _norm_str(pin_hash_map.get(sabun, "")).lower()
        if _eq(stored_hash, h1) or _eq(stored_hash, h2):
            return True

    # 3) 세션 사용자 객체
    u = st.session_state.get("user", {}) or {}
    if _norm_str(u.get("사번", "")) == sabun:
        if "pin" in u:
            if _eq(_norm_str(u.get("pin", "")), val):
                return True
        if "pin_hash" in u:
            stored_hash = _norm_str(u.get("pin_hash", "")).lower()
            if _eq(stored_hash, h1) or _eq(stored_hash, h2):
                return True

    # 4) 직원 DF 기반 (시트)
    try:
        emp_df = st.session_state.get("emp_df")
        if emp_df is not None and "사번" in emp_df.columns and "PIN_hash" in emp_df.columns:
            row = emp_df.loc[emp_df["사번"].astype(str).str.strip() == sabun]
            if not row.empty:
                stored_hash = _norm_str(row.iloc[0].get("PIN_hash", "")).lower()
                if _eq(stored_hash, h1) or _eq(stored_hash, h2):
                    return True
    except Exception:
        pass

    return False

# ═════════════════════════════════════════════════════════════════════════════
# Google Auth / Sheets
# ═════════════════════════════════════════════════════════════════════════════
API_BACKOFF_SEC = [0.0, 0.8, 1.6, 3.2, 6.4, 9.6]  # base + jitter

def _retry(fn, *args, **kwargs):
    """Retry helper: handle 429/503 and 403(rate/quota) with jittered backoff."""
    last = None
    for b in API_BACKOFF_SEC:
        try:
            # gspread Worksheet.update positional-order fix
            try:
                fn_name = getattr(fn, '__name__', '')
                fn_qual = getattr(fn, '__qualname__', '')
                if 'update' == fn_name or fn_qual.endswith('.update'):
                    if len(args) >= 2 and 'range_name' not in kwargs and 'values' not in kwargs:
                        a1, a2 = args[0], args[1]
                        if isinstance(a1, str) and (':' in a1 or '!' in a1 or re.match(r'^[A-Za-z]+\d+(?::[A-Za-z]+\d+)?$', a1)):
                            args = (a2, a1) + tuple(args[2:])
            except Exception:
                pass
            return fn(*args, **kwargs)
        except APIError as e:
            status = None
            retry_after = None
            msg = ""
            try:
                status = getattr(e, "response", None).status_code
                headers = getattr(e, "response", None).headers or {}
                retry_after = headers.get("Retry-After")
            except Exception:
                pass
            try:
                msg = str(e).lower()
            except Exception:
                msg = ""

            retryable = (status in (429, 503)) or (status == 403 and ("rate" in msg or "quota" in msg or "too many" in msg))
            if not retryable and status in (400, 401, 404):
                # Non-retryable client errors → 즉시 재발생
                raise

            wait = float(retry_after) if retry_after else (b + random.uniform(0, 0.6))
            time.sleep(max(0.25, wait))
            last = e
        except Exception as e:
            # 알 수 없는 예외: 같은 backoff 규칙으로 재시도 후 마지막에 재발생
            last = e
            time.sleep(b + random.uniform(0, 0.6))
    if last:
        raise last
    return fn(*args, **kwargs)

@st.cache_resource(show_spinner=False)
def get_client():
    """gspread Client (service account). 캐시되어 재사용됨."""
    try:
        svc = dict(st.secrets["gcp_service_account"])
    except Exception:
        raise RuntimeError("gcp_service_account secrets가 설정되어 있지 않습니다.")
    svc["private_key"] = _normalize_private_key(svc.get("private_key", ""))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_book():
    """HR 구글시트 Spreadsheet 객체 (캐시)."""
    try:
        key = st.secrets["sheets"]["HR_SHEET_ID"]
    except Exception:
        raise RuntimeError("sheets.HR_SHEET_ID가 secrets에 없습니다.")
    return get_client().open_by_key(key)

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

_WS_CACHE: dict[str, Tuple[float, Any]] = {}
_HDR_CACHE: dict[str, Tuple[float, list[str], dict]] = {}
_WS_TTL, _HDR_TTL = 120, 120

_VAL_CACHE: dict[str, Tuple[float, list]] = {}
_VAL_TTL = 90

def _ws_values(ws, key: str | None = None):
    """워크시트 전체 값(2차원 리스트)을 TTL 캐시로 가져오기."""
    key = key or getattr(ws, "title", "") or "ws_values"
    now = time.time()
    hit = _VAL_CACHE.get(key)
    if hit and (now - hit[0] < _VAL_TTL):
        return hit[1]
    vals = _retry(ws.get_all_values)
    _VAL_CACHE[key] = (now, vals)
    return vals

def _ws(title: str):
    """제목으로 워크시트 가져오기 (TTL 캐시)."""
    now = time.time()
    hit = _WS_CACHE.get(title)
    if hit and (now - hit[0] < _WS_TTL):
        return hit[1]
    ws = _retry(get_book().worksheet, title)
    _WS_CACHE[title] = (now, ws)
    return ws

def _hdr(ws, key: str) -> Tuple[list[str], dict]:
    """헤더 1행과 헤더→index 매핑 (TTL 캐시)."""
    now = time.time()
    hit = _HDR_CACHE.get(key)
    if hit and (now - hit[0] < _HDR_TTL):
        return hit[1], hit[2]
    header = _retry(ws.row_values, 1) or []
    hmap = {n: i + 1 for i, n in enumerate(header)}
    _HDR_CACHE[key] = (now, header, hmap)
    return header, hmap

def _ws_get_all_records(ws):
    """get_all_values 기반의 안정적 레코드 변환 (헤더 기준 dict list)."""
    try:
        title = getattr(ws, "title", None) or ""
        vals = _ws_values(ws, title)
        if not vals:
            return []
        header = [str(x).strip() for x in (vals[0] if vals else [])]
        if not header:
            return []
        out = []
        for i in range(1, len(vals)):
            row = vals[i] if i < len(vals) else []
            rec = {}
            for j, h in enumerate(header):
                rec[h] = row[j] if j < len(row) else ""
            out.append(rec)
        return out
    except Exception:
        # gspread 버전별 파라미터 차이 대응
        try:
            return _retry(ws.get_all_records, numericise_ignore=["all"])
        except TypeError:
            return _retry(ws.get_all_records)

# ═════════════════════════════════════════════════════════════════════════════
# Sheet Readers (TTL↑)
# ═════════════════════════════════════════════════════════════════════════════
LAST_GOOD: dict[str, pd.DataFrame] = {}

@st.cache_data(ttl=600, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    """구글시트 → DataFrame (빈칸 재직여부=True로 해석, 호환 유지)"""
    try:
        ws = _ws(sheet_name)
        df = pd.DataFrame(_ws_get_all_records(ws))
        if df.empty:
            return df

        # 사번은 문자열 키로 고정
        if "사번" in df.columns:
            df["사번"] = df["사번"].astype(str)

        # 재직여부: 빈칸은 True, 나머지는 _to_bool 규칙 적용
        if "재직여부" in df.columns:
            df["재직여부"] = df["재직여부"].map(
                lambda v: True if str(v).strip() == "" else _to_bool(v)
            )

        LAST_GOOD[sheet_name] = df.copy()
        return df

    except APIError as e:
        if _is_quota_429(e):
            try:
                st.warning("구글시트 읽기 할당량(1분) 초과. 잠시 후 좌측 '동기화'를 눌러 다시 시도해 주세요.", icon="⏳")
            except Exception:
                pass
            return pd.DataFrame()
        if sheet_name in LAST_GOOD:
            st.info(f"네트워크 혼잡으로 캐시 데이터를 표시합니다: {sheet_name}")
            return LAST_GOOD[sheet_name]
        raise

@st.cache_data(ttl=600, show_spinner=False)
def read_emp_df() -> pd.DataFrame:
    """직원 시트 표준화: 필수 컬럼 보강 및 dtype 정리"""
    df = read_sheet_df(EMP_SHEET)

    # 최소 컬럼 보강
    for c in ["사번", "이름", "PIN_hash", "재직여부", "적용여부"]:
        if c not in df.columns:
            df[c] = "" if c != "재직여부" else True

    # dtype 정리
    df["사번"] = df["사번"].astype(str)
    # 재직여부/적용여부를 확실히 bool로
    for _col in ["재직여부", "적용여부"]:
        if _col in df.columns:
            df[_col] = df[_col].map(
                lambda v: True if str(v).strip() == "" else _to_bool(v)
            ).astype(bool)

    return df

@st.cache_data(ttl=600, show_spinner=False)
def read_acl_df(only_enabled: bool = True) -> pd.DataFrame:
    """권한(acl): **Google Sheets 전용** 로더.
    - 시트: '권한'
    - 컬럼 보강: 사번/역할 기본 보장
    - only_enabled=True이면 '활성' 컬럼이 truthy인 행만 필터
    """
    try:
        ws = _ws("권한")
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        st.warning(f"권한 시트 로드 실패: {e}")
        return pd.DataFrame(columns=["사번", "역할", "활성"])

    if df.empty:
        return pd.DataFrame(columns=["사번", "역할", "활성"])

    # 문자열화/트림
    for c in ["사번", "역할"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
        else:
            df[c] = ""  # 최소 보장

    # 활성 컬럼 정리 (_to_bool 사용, 빈칸은 False로)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(lambda v: _to_bool(v, default=False)).astype(bool)
    else:
        df["활성"] = False

    # 선택적 필터
    if only_enabled:
        df = df[df["활성"] == True]  # noqa: E712

    # 키 최소 보장
    df = df[(df["사번"] != "") & (df["역할"] != "")]
    return df.reset_index(drop=True)

# ═════════════════════════════════════════════════════════════════════════════
# Login + Session
# ═════════════════════════════════════════════════════════════════════════════
SESSION_TTL_MIN=30

def _session_valid()->bool:
    exp=st.session_state.get("auth_expires_at")
    ok=st.session_state.get("authed", False)
    return bool(ok and exp and time.time()<exp)

def _start_session(user: dict):
    st.session_state["authed"]=True
    st.session_state["user"]=user
    st.session_state["auth_expires_at"]=time.time()+SESSION_TTL_MIN*60
    st.session_state["_state_owner_sabun"]=str(user.get("사번",""))

def _ensure_state_owner():
    try:
        cur=str(st.session_state.get("user",{}).get("사번","") or "")
        owner=str(st.session_state.get("_state_owner_sabun","") or "")
        if owner and (owner!=cur):
            for k in list(st.session_state.keys()):
                if k not in ("authed","user","auth_expires_at","_state_owner_sabun"):
                    st.session_state.pop(k, None)
            st.session_state["_state_owner_sabun"]=cur
    except Exception: pass

def logout():
    for k in list(st.session_state.keys()):
        try: del st.session_state[k]
        except Exception: pass
    try: st.cache_data.clear()
    except Exception: pass
    st.rerun()

# --- Enter Key Binder (사번→PIN, PIN→로그인) -------------------------------

def _inject_login_keybinder():
    components.html(
        """
        <script>
        (function(){
          function byLabelStartsWith(txt){
            const doc = window.parent.document;
            const labels = Array.from(doc.querySelectorAll('label'));
            const lab = labels.find(l => (l.innerText||"").trim().startsWith(txt));
            if(!lab) return null;
            const root = lab.closest('div[data-testid="stTextInput"]') || lab.parentElement;
            return root ? root.querySelector('input') : null;
          }
          function findLoginBtn(){
            const doc = window.parent.document;
            const btns = Array.from(doc.querySelectorAll('button'));
            return btns.find(b => (b.textContent||"").trim() === '로그인');
          }
          function commit(el){
            if(!el) return;
            el.dispatchEvent(new Event('input',{bubbles:true}));
            el.dispatchEvent(new Event('change',{bubbles:true}));
            el.blur();
          }
          function bind(){
            const sab = byLabelStartsWith('사번');
            const pin = byLabelStartsWith('PIN');
            if(!sab || !pin) return false;
            if(!sab._bound){
              sab._bound = true;
              sab.addEventListener('keydown', function(e){
                if(e.key==='Enter'){ e.preventDefault(); commit(sab); setTimeout(()=>{ try{ pin.focus(); pin.select(); }catch(_){}} ,0); }
              });
            }
            if(!pin._bound){
              pin._bound = true;
              pin.addEventListener('keydown', function(e){
                if(e.key==='Enter'){ e.preventDefault(); commit(pin); setTimeout(()=>{ try{ (findLoginBtn()||{}).click(); }catch(_){}} ,60); }
              });
            }
            return true;
          }
          bind();
          const mo = new MutationObserver(() => { bind(); });
          mo.observe(window.parent.document.body, { childList:true, subtree:true });
          setTimeout(()=>{ try{ mo.disconnect(); }catch(e){} }, 8000);
        })();
        </script>
        """,
        height=0, width=0
    )

def show_login(emp_df: pd.DataFrame):
    st.markdown("### 로그인")
    sabun = st.text_input("사번", key="login_sabun", placeholder="사번 ******")
    pin   = st.text_input("PIN (숫자)", type="password", key="login_pin", placeholder="비밀번호 ****")
    _inject_login_keybinder()
    if st.button("로그인", type="primary"):
        if not sabun or not pin:
            st.error("사번과 PIN을 입력하세요."); st.stop()
        row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
        if row.empty: st.error("사번을 찾을 수 없습니다."); st.stop()
        r=row.iloc[0]
        if not _to_bool(r.get("재직여부", True)):
            st.error("재직 상태가 아닙니다."); st.stop()
        stored=str(r.get("PIN_hash","")).strip().lower()
        entered_plain=_sha256_hex(pin.strip())
        entered_salted=_pin_hash(pin.strip(), str(r.get("사번","")))
        if stored not in (entered_plain, entered_salted):
            st.error("PIN이 올바르지 않습니다."); st.stop()
        _start_session({"사번":str(r.get("사번","")), "이름":str(r.get("이름",""))})
        st.success("환영합니다!"); st.rerun()

def require_login(emp_df: pd.DataFrame):
    if not _session_valid():
        for k in ("authed","user","auth_expires_at","_state_owner_sabun"): st.session_state.pop(k, None)
        show_login(emp_df); st.stop()
    else:
        _ensure_state_owner()

# ═════════════════════════════════════════════════════════════════════════════
# ACL (권한) + Staff Filters (TTL↑)
# ═════════════════════════════════════════════════════════════════════════════
AUTH_SHEET = "권한"
AUTH_HEADERS = ["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]

EVAL_ITEMS_SHEET = st.secrets.get("sheets", {}).get("EVAL_ITEMS_SHEET", "평가_항목")
EVAL_ITEM_HEADERS = ["항목ID","항목","내용","순서","활성","비고","설명","유형","구분"]
EVAL_RESP_SHEET_NAME = "인사평가"
EVAL_BASE_HEADERS = ["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각","잠금"]

@st.cache_data(ttl=300, show_spinner=False)
def read_auth_df(only_enabled: bool = True) -> pd.DataFrame:
    """권한 시트 로딩 → 필수 컬럼 보강, dtype/불리언 정리."""
    try:
        ws = get_book().worksheet(AUTH_SHEET)
        raw = _ws_get_all_records(ws)
        df = pd.DataFrame(raw)
    except Exception:
        df = pd.DataFrame(columns=AUTH_HEADERS)

    # 필수 컬럼 보강
    for c in AUTH_HEADERS:
        if c not in df.columns:
            df[c] = ""

    # 문자열 정리
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        df[c] = df[c].astype(str).str.strip()

    # 활성 → bool (_to_bool 사용, 기본 False)
    df["활성"] = df["활성"].map(lambda v: _to_bool(v, default=False)).astype(bool)

    if only_enabled:
        df = df[df["활성"] == True]  # noqa: E712

    return df.reset_index(drop=True)

def is_admin(sabun: str) -> bool:
    try:
        df = read_auth_df()
        if df.empty:
            return False
        q = df[
            (df["사번"].astype(str) == str(sabun)) &
            (df["역할"].str.lower() == "admin") &
            (df["활성"] == True)  # noqa: E712
        ]
        return not q.empty
    except Exception:
        return False

def get_allowed_sabuns(emp_df: pd.DataFrame, sabun: str, include_self: bool = True) -> set[str]:
    """
    내 권한 규칙에 따라 접근 가능한 사번 집합을 계산.
    - 역할 'master'는 전체 허용
    - 범위유형: (공란)=전체 / '부서' / '개별'
    """
    sabun = str(sabun).strip()
    allowed = {sabun} if include_self else set()

    df = read_auth_df(only_enabled=True)
    if df.empty:
        return allowed

    mine = df[(df["사번"].astype(str) == sabun) & (df["활성"] == True)]  # noqa: E712
    if mine.empty:
        return allowed

    # (1) master 전체 허용
    if any(r.strip().lower() == "master" for r in mine["역할"].astype(str)):
        try:
            return set(emp_df["사번"].astype(str)) if not emp_df.empty else allowed
        except Exception:
            return allowed

    # (2) 범위유형별
    # emp_df 컬럼 가드
    has_dept1 = "부서1" in emp_df.columns
    has_dept2 = "부서2" in emp_df.columns
    has_active = "재직여부" in emp_df.columns

    for _, r in mine.iterrows():
        t = str(r.get("범위유형", "")).strip()
        if t == "":
            try:
                return set(emp_df["사번"].astype(str)) if not emp_df.empty else allowed
            except Exception:
                return allowed
        elif t == "부서":
            tgt = emp_df.copy()
            tgt["사번"] = tgt["사번"].astype(str)
            if has_active:
                tgt = tgt[tgt["재직여부"] == True]  # noqa: E712
            d1 = str(r.get("부서1", "")).strip()
            d2 = str(r.get("부서2", "")).strip()
            if d1 and has_dept1:
                tgt = tgt[tgt["부서1"].astype(str) == d1]
            if d2 and has_dept2:
                tgt = tgt[tgt["부서2"].astype(str) == d2]
            allowed.update(tgt["사번"].astype(str).tolist())
        elif t == "개별":
            for p in re.split(r"[,\s]+", str(r.get("대상사번", "")).strip()):
                if p:
                    allowed.add(p)

    return allowed

# ─ Debounce (no-UI) ───────────────────────────────────────────────────────────
def _debounce_passed(name: str, wait: float, clicked: bool) -> bool:
    """Allow action once per 'wait' seconds per name; uses session_state only."""
    if not clicked:
        return False
    now = time.time()
    key = f"_debounce_{name}"
    last = float(st.session_state.get(key, 0.0) or 0.0)
    if now - last < float(wait):
        return False
    st.session_state[key] = now
    return True

# ═════════════════════════════════════════════════════════════════════════════
# Global Target Sync
# ═════════════════════════════════════════════════════════════════════════════
def set_global_target(sabun:str, name:str=""):
    st.session_state["glob_target_sabun"]=str(sabun).strip()
    st.session_state["glob_target_name"]=str(name).strip()

def get_global_target()->Tuple[str,str]:
    return (str(st.session_state.get("glob_target_sabun","") or ""),
            str(st.session_state.get("glob_target_name","") or ""))

# ═════════════════════════════════════════════════════════════════════════════
# Left: 직원선택 (Enter 동기화)
# ═════════════════════════════════════════════════════════════════════════════
def render_staff_picker_left(emp_df: pd.DataFrame):
    # ▼ 필터 초기화 플래그 처리(위젯 생성 전에 초기화해야 오류 없음)
    if st.session_state.get("_left_reset", False):
        u0 = st.session_state.get("user", {})
        me0 = str(u0.get("사번", ""))
        nm0 = str(u0.get("이름", ""))

        # 검색/대상선택 UI 리셋
        st.session_state["pick_q"] = ""
        st.session_state["left_pick"] = "(선택)"
        st.session_state["left_preselect_sabun"] = ""

        # 탭별 대상자도 "미선택"으로 초기화
        try:
            set_global_target("", "")
        except Exception:
            pass
        st.session_state["eval2_target_sabun"] = ""
        st.session_state["eval2_target_name"]  = ""
        st.session_state["jd2_target_sabun"]   = ""
        st.session_state["jd2_target_name"]    = ""
        st.session_state["cmpS_target_sabun"]  = ""
        st.session_state["cmpS_target_name"]   = ""

        st.session_state["_left_reset"] = False

    u = st.session_state.get("user", {})
    me = str(u.get("사번", ""))
    df = emp_df.copy()
    # 적용여부가 체크된 직원만 좌측 메뉴에 노출
    if "적용여부" in df.columns:
        df = df[df["적용여부"]==True].copy()

    # ✅ 관리자라도 범위유형이 '부서/개별'이면 해당 범위만 보이도록 통일
    allowed = get_allowed_sabuns(emp_df, me, include_self=True)
    df = df[df["사번"].astype(str).isin(allowed)].copy()

    with st.form("left_search_form", clear_on_submit=False):
        q = st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
        submitted = st.form_submit_button("검색 적용(Enter)")

    view = df.copy()
    if q.strip():
        k = q.strip().lower()
        view = view[view.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번", "이름"] if c in r), axis=1)]

    view = view.sort_values("사번") if "사번" in view.columns else view
    sabuns = view["사번"].astype(str).tolist()
    names = view.get("이름", pd.Series([""] * len(view))).astype(str).tolist()
    opts = [f"{s} - {n}" for s, n in zip(sabuns, names)]

    pre_sel_sab = st.session_state.get("left_preselect_sabun", "")
    if submitted:
        exact_idx = -1
        if q.strip():
            for i, (s, n) in enumerate(zip(sabuns, names)):
                if q.strip() == s or q.strip() == n:
                    exact_idx = i
                    break
        target_idx = exact_idx if exact_idx >= 0 else (0 if sabuns else -1)
        if target_idx >= 0:
            pre_sel_sab = sabuns[target_idx]
            st.session_state["left_preselect_sabun"] = pre_sel_sab

    idx0 = 0
    if pre_sel_sab:
        try:
            idx0 = 1 + sabuns.index(pre_sel_sab)
        except ValueError:
            idx0 = 0

    picked = st.selectbox("**대상 선택**", ["(선택)"] + opts, index=idx0, key="left_pick")

    # ▼ 필터 초기화: 플래그만 세우고 즉시 rerun (다음 런 시작 시 초기화됨)
    clicked_reset = st.button("필터 초기화", use_container_width=True)
    if _debounce_passed("__left_reset", 1.0, clicked_reset):
        st.session_state["_left_reset"] = True
        st.rerun()

    if picked and picked != "(선택)":
        sab = picked.split(" - ", 1)[0].strip()
        name = picked.split(" - ", 1)[1].strip() if " - " in picked else ""
        set_global_target(sab, name)
        st.session_state["eval2_target_sabun"] = sab
        st.session_state["eval2_target_name"] = name
        st.session_state["jd2_target_sabun"] = sab
        st.session_state["jd2_target_name"] = name
        st.session_state["cmpS_target_sabun"] = sab
        st.session_state["cmpS_target_name"] = name

        # ▼ 표도 '대상선택'에 맞춰 1명만 필터
        if "사번" in view.columns:
            view = view[view["사번"].astype(str) == sab]

    cols = [c for c in ["사번", "이름", "부서1", "부서2", "직급"] if c in view.columns]
    st.caption(f"총 {len(view)}명")

# ── 관리자/부서장: 대시보드 왼쪽 표에 합쳐서 표시 ───────────────────────────

    # 빠른 화면을 원하면 '대시보드 보기'를 끄세요.
    show_dashboard_cols = st.checkbox("대시보드 보기(요약 컬럼 표시)", value=False, help="끄면 기본 직원표만 빠르게 표시됩니다.")
    try:

        am_admin_or_mgr = (is_admin(me) or len(get_allowed_sabuns(emp_df, me, include_self=False)) > 0)
    except Exception:
        am_admin_or_mgr = False

    if am_admin_or_mgr and not view.empty and show_dashboard_cols:
        # 연도 선택 (기본=올해)
        this_year = current_year()
        dash_year = st.number_input("연도(현황판)", min_value=2000, max_value=2100, value=int(this_year), step=1, key="left_dash_year")

        eval_map = _dash_eval_scores_for_year(int(dash_year))
        comp_map = _dash_comp_status_for_year(int(dash_year))
        appr_map = get_jd_approval_map_cached(int(dash_year), st.session_state.get("appr_rev", 0))

        # view에 컬럼 합치기
        ext_rows = []
        for _, r in view.iterrows():
            sab = str(r.get("사번","")).strip()

            # 인사평가 점수
            s_self = eval_map.get((sab, "자기"), ("", ""))[0]
            s_mgr  = eval_map.get((sab, "1차"), ("", ""))[0]
            s_adm  = eval_map.get((sab, "2차"), ("", ""))[0]

            # JD 작성/승인
            latest = _jd_latest_for(sab, int(dash_year))
            jd_write = "완료" if latest else ""
            jd_appr  = ""
            if latest:
                try:
                    ver = int(str(latest.get("버전", 0)).strip() or "0")
                except Exception:
                    ver = 0
                st_ap = appr_map.get((sab, ver), ("",""))[0] if ver else ""
                jd_appr = (st_ap if st_ap else "")

            # 직무능력평가 항목명
            main, extra, qual = "", "", ""
            if sab in comp_map:
                main, extra, qual, _ = comp_map[sab]

            ext_rows.append({
                "사번": sab,
                "인사평가(자기)": s_self, "인사평가(1차)": s_mgr, "인사평가(2차)": s_adm,
                "직무기술서(작성)": jd_write, "직무기술서(승인)": jd_appr,
                "직무능력평가(주업무)": main, "직무능력평가(기타업무)": extra, "직무능력평가(자격유지)": qual
            })

        add_df = pd.DataFrame(ext_rows)
        add_df["사번"] = add_df["사번"].astype(str)
        view2 = view.copy()
        view2["사번"] = view2["사번"].astype(str)
        view2 = view2.merge(add_df, on="사번", how="left")

        ext_cols = cols + [
            "인사평가(자기)","인사평가(1차)","인사평가(2차)",
            "직무기술서(작성)","직무기술서(승인)",
            "직무능력평가(주업무)","직무능력평가(기타업무)","직무능력평가(자격유지)"
        ]

        st.dataframe(
            view2[ext_cols],
            use_container_width=True,
            height=420,
            hide_index=True,
            column_config={
                "인사평가(자기)": st.column_config.TextColumn("자기"),
                "인사평가(1차)": st.column_config.TextColumn("1차"),
                "인사평가(2차)": st.column_config.TextColumn("2차"),
                "직무기술서(작성)": st.column_config.TextColumn("JD작성"),
                "직무기술서(승인)": st.column_config.TextColumn("JD승인"),
                "직무능력평가(주업무)": st.column_config.TextColumn("주업무"),
                "직무능력평가(기타업무)": st.column_config.TextColumn("기타업무"),
                "직무능력평가(자격유지)": st.column_config.TextColumn("자격유지"),
            },
        )

    else:
        st.dataframe(view[cols], use_container_width=True, height=(360 if not show_dashboard_cols else 420), hide_index=True)

def _eval_sheet_name(year: int | str) -> str: return EVAL_RESP_SHEET_NAME

def ensure_eval_items_sheet():
    wb=get_book()
    try:
        ws=wb.worksheet(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        _retry(ws.update, "A1", [EVAL_ITEM_HEADERS]); return
    try:
        header=_retry(ws.row_values, 1) or []
    except Exception as e:
        if _is_quota_429(e):
            try: st.warning('구글시트 읽기 할당량(1분) 초과. 잠시 후 좌측 "동기화"를 눌러 다시 시도해 주세요.', icon='⏳')
            except Exception: pass
            return
        raise
    need=[h for h in EVAL_ITEM_HEADERS if h not in header]
    if need:
        try:
            _retry(ws.update, "1:1", [header+need])
        except Exception as e:
            if _is_quota_429(e):
                try: st.warning('구글시트 쓰기 할당량(1분) 초과. 잠시 후 좌측 "동기화" 후 다시 시도해 주세요.', icon='⏳')
                except Exception: pass
                return
            raise

@st.cache_data(ttl=300, show_spinner=False)
def read_eval_items_df(only_active: bool = False) -> pd.DataFrame:
    """
    평가_항목: Google Sheets 전용 로더
    - 시트: EVAL_ITEMS_SHEET (기본 '평가_항목')
    - 컬럼 보강: EVAL_ITEM_HEADERS
    - only_active=True면 '활성' truthy만 필터
    - 정렬: '순서' 오름차순 → '항목' 알파벳/가나다 순
    """
    try:
        # 시트가 없으면 생성 및 헤더 보장
        ensure_eval_items_sheet()
        ws = _ws(EVAL_ITEMS_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        df = pd.DataFrame(columns=EVAL_ITEM_HEADERS)

    # 필수 컬럼 보강
    for c in EVAL_ITEM_HEADERS:
        if c not in df.columns:
            df[c] = ""

    # 타입/트림
    for c in ["항목ID","항목","내용","비고","설명","유형","구분"]:
        df[c] = df[c].astype(str).str.strip()
    # 순서 숫자화
    try:
        df["순서"] = pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)
    except Exception:
        df["순서"] = 0

    # 활성 → bool
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool).astype(bool)
    else:
        df["활성"] = True

    if only_active:
        df = df[df["활성"] == True]  # noqa: E712

    # 정렬
    df = df.sort_values(["순서", "항목"], na_position="last").reset_index(drop=True)
    return df

def _ensure_eval_resp_sheet(year:int, item_ids:list[str]):
    name=_eval_sheet_name(year)
    wb=get_book()
    try:
        ws=_ws(name)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=name, rows=5000, cols=max(50, len(item_ids)+16))
        _WS_CACHE[name]=(time.time(), ws)
    need=list(EVAL_BASE_HEADERS)+[f"점수_{iid}" for iid in item_ids]
    header,_=_hdr(ws, name)
    if not header:
        _retry(ws.update, "1:1", [need]); _HDR_CACHE[name]=(time.time(), need, {n:i+1 for i,n in enumerate(need)})
    else:
        miss=[h for h in need if h not in header]
        if miss:
            new=header+miss; _retry(ws.update, "1:1", [new])
            _HDR_CACHE[name]=(time.time(), new, {n:i+1 for i,n in enumerate(new)})
    return ws

def _emp_name_by_sabun(emp_df: pd.DataFrame, sabun: str) -> str:
    row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    return "" if row.empty else str(row.iloc[0].get("이름",""))

def upsert_eval_response(emp_df: pd.DataFrame, year: int, eval_type: str,
                         target_sabun: str, evaluator_sabun: str,
                         scores: dict[str,int], status="제출")->dict:
    items=read_eval_items_df(True); item_ids=[str(x) for x in items["항목ID"].tolist()]
    ws=_ensure_eval_resp_sheet(year, item_ids)
    header=_retry(ws.row_values, 1); hmap={n:i+1 for i,n in enumerate(header)}
    def c5(v):
        try: v=int(v)
        except: v=3
        return min(5,max(1,v))
    scores_list=[c5(scores.get(i,3)) for i in item_ids]
    total=round(sum(scores_list)*(100.0/max(1,len(item_ids)*5)),1)
    tname=_emp_name_by_sabun(emp_df, target_sabun); ename=_emp_name_by_sabun(emp_df, evaluator_sabun)
    now=kst_now_str()
    values = _ws_values(ws); cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
    row_idx=0
    for i in range(2, len(values)+1):
        r=values[i-1]
        try:
            if (str(r[cY-1]).strip()==str(year) and str(r[cT-1]).strip()==eval_type and
                str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun)):
                row_idx=i; break
        except: pass
    if row_idx==0:
        buf=[""]*len(header)
        def put(k,v): c=hmap.get(k); buf[c-1]=v if c else ""
        put("연도", int(year)); put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", tname)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", ename)
        put("총점", total); put("상태", status); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c=hmap.get(f"점수_{iid}")
            if c: buf[c-1]=sc
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action":"insert","total":total}
    else:
        payload={"총점": total, "상태": status, "제출시각": now, "평가대상이름": tname, "평가자이름": ename}
        for iid, sc in zip(item_ids, scores_list): payload[f"점수_{iid}"]=sc
        def _batch_row(ws, idx, hmap, kv):
            upd=[]
            for k,v in kv.items():
                c=hmap.get(k)
                if c:
                    a1=gspread.utils.rowcol_to_a1(idx, c)
                    upd.append({"range": a1, "values": [[v]]})
            if upd: _retry(ws.batch_update, upd)
        _batch_row(ws, row_idx, hmap, payload)
        st.cache_data.clear()
        return {"action":"update","total":total}

@st.cache_data(ttl=300, show_spinner=False)
def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    name=_eval_sheet_name(year)
    try:
        ws=_ws(name); df=pd.DataFrame(_ws_get_all_records(ws))
    except Exception: return pd.DataFrame(columns=EVAL_BASE_HEADERS)
    if df.empty: return df
    if "평가자사번" in df.columns: df=df[df["평가자사번"].astype(str)==str(sabun)]
    sort_cols=[c for c in ["평가유형","평가대상사번","제출시각"] if c in df.columns]
    if sort_cols: df=df.sort_values(sort_cols, ascending=[True,True,False]).reset_index(drop=True)
    return df

def tab_eval(emp_df: pd.DataFrame):
    """인사평가 탭 (심플·자동 라우팅)
    - 역할: employee / manager / admin
    - 유형 자동결정:
        employee: 본인=자기
        manager : 본인=자기, 부서원=1차(부서원의 자기 '제출' 후 입력 가능)
        admin   : 대상이 manager면 1차(그 manager의 자기 '제출' 후), 그 외(직원)는 2차(1차 '제출' 후)
    - 직원 자기평가는 제출 후 수정 불가(자동 잠금)
    """

# --- 기본값/데이터 로드 -------------------------------
    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="eval2_year")

    u = st.session_state["user"]; me_sabun = str(u["사번"]); me_name = str(u["이름"])

    items = read_eval_items_df(True)
    if items.empty:
        st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️")
        return
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    item_ids = [str(x) for x in items_sorted["항목ID"].tolist()]

# --- 역할 판정 -------------------------------
    def is_manager_role(_sabun: str) -> bool:
        # 본인 제외 부하가 1명이라도 있으면 manager (admin 제외)
        return (not is_admin(_sabun)) and len(get_allowed_sabuns(emp_df, _sabun, include_self=False)) > 0

    def role_of(_sabun: str) -> str:
        if is_admin(_sabun): return "admin"
        if is_manager_role(_sabun): return "manager"
        return "employee"

    my_role = role_of(me_sabun)

# --- 대상 후보 목록 -------------------------------
    def list_targets_for(me_role: str) -> pd.DataFrame:
        base = emp_df.copy(); base["사번"] = base["사번"].astype(str)
        if "재직여부" in base.columns:
            base = base[base["재직여부"] == True]
        if me_role == "employee":
            return base[base["사번"] == me_sabun]
        elif me_role == "manager":
            allowed = set(str(x) for x in get_allowed_sabuns(emp_df, me_sabun, include_self=True))
            return base[base["사번"].isin(allowed)]

        else:  # admin
            # ✅ 관리자라도 범위 규칙을 따르되, 자기 자신은 제외(자기평가 없음)
            allowed = set(str(x) for x in get_allowed_sabuns(emp_df, me_sabun, include_self=True))
            return base[base["사번"].isin(allowed - {me_sabun})]

    view = list_targets_for(my_role)[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)

# --- 제출 여부 / 저장값 조회 -------------------------------
    def has_submitted(_year: int, _type: str, _target_sabun: str) -> bool:
        """해당 연도+유형+대상자의 '상태'가 제출/완료인지 검사(평가자 무관)."""
        try:
            ws = _ensure_eval_resp_sheet(int(_year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i+1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cS=hmap.get("상태")
            if not all([cY, cT, cTS, cS]): return False
            for r in values[1:]:
                try:
                    if (str(r[cY-1]).strip()==str(_year)
                        and str(r[cT-1]).strip()==_type
                        and str(r[cTS-1]).strip()==str(_target_sabun)):
                        if str(r[cS-1]).strip() in {"제출","완료"}: return True
                except: pass
        except: pass
        return False

    def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> Tuple[dict, dict]:
        """현 평가자 기준 저장된 점수/메타 로드"""
        try:
            ws = _ensure_eval_resp_sheet(int(year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i+1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
            row_idx = 0
            for i in range(2, len(values)+1):
                r = values[i-1]
                try:
                    if (str(r[cY-1]).strip()==str(year) and str(r[cT-1]).strip()==str(eval_type)
                        and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun)):
                        row_idx = i; break
                except: pass
            if row_idx == 0: return {}, {}
            row = values[row_idx-1]
            scores = {}
            for iid in item_ids:
                col = hmap.get(f"점수_{iid}")
                if col:
                    try: v = int(str(row[col-1]).strip() or "0")
                    except: v = 0
                    if v: scores[iid] = v
            meta = {}
            for k in ["상태","잠금","제출시각","총점"]:
                c = hmap.get(k)
                if c and c-1 < len(row): meta[k] = row[c-1]
            return scores, meta
        except Exception:
            return {}, {}

# --- 대상 선택 + 유형 자동결정 -------------------------------
    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("eval2_target_sabun", (glob_sab if my_role!="employee" else me_sabun))
    st.session_state.setdefault("eval2_target_name",  (glob_name if my_role!="employee" else me_name))
    st.session_state.setdefault("eval2_edit_mode",    False)

    if my_role == "employee":
        target_sabun, target_name = me_sabun, me_name
    else:
        _sabuns = view["사번"].astype(str).tolist()
        _names  = view["이름"].astype(str).tolist()
        _d2     = view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""] * len(_sabuns)
        _opts   = [f"{s} - {n} - {d2}" for s, n, d2 in zip(_sabuns, _names, _d2)]
        _target = st.session_state.get("eval2_target_sabun", (_sabuns[_sabuns.index(me_sabun)] if (my_role=="manager" and me_sabun in _sabuns) else (_sabuns[0] if _sabuns else "")))
        _idx    = _sabuns.index(_target) if _target in _sabuns else 0
        _idx2 = (1 + _sabuns.index(_target)) if (_target in _sabuns) else 0
        _sel = st.selectbox("대상자 선택", ["(선택)"] + _opts, index=_idx2, key="eval2_pick_editor_select")
        if _sel == "(선택)":
            st.session_state["eval2_target_sabun"] = ""
            st.session_state["eval2_target_name"]  = ""
            st.info("대상자를 선택하세요.", icon="👈")
            return
        _sel_sab = _sel.split(" - ",1)[0] if isinstance(_sel,str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["eval2_target_sabun"] = str(_sel_sab)
        try:
            st.session_state["eval2_target_name"] = str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["eval2_target_name"] = ""
        target_sabun = st.session_state["eval2_target_sabun"]
        target_name  = st.session_state["eval2_target_name"]

    st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    # === 제출시각 배너(인사평가) ===
    try:
        _emap = get_eval_summary_map_cached(int(year), st.session_state.get('eval_rev', 0))
        def _b(stage:str) -> str:
            try:
                return (str(_emap.get((str(target_sabun), stage), ("",""))[1]).strip() or "미제출")
            except Exception:
                return "미제출"
        _banner = f"🕒 제출시각  |  [자기] {_b('자기')}  |  [1차] {_b('1차')}  |  [2차] {_b('2차')}"
        show_submit_banner(_banner)
    except Exception:
        pass

    target_role = role_of(target_sabun)
    if my_role == "employee":
        eval_type = "자기"
    elif my_role == "manager":
        eval_type = "자기" if target_sabun == me_sabun else "1차"
    else:  # admin
        eval_type = "1차" if target_role == "manager" else "2차"

    st.info(f"평가유형: **{eval_type}** (자동 결정)", icon="ℹ️")

# --- 선행조건 / 잠금 -------------------------------
    prereq_ok, prereq_msg = True, ""
    if eval_type == "1차":
        if not has_submitted(year, "자기", target_sabun):
            prereq_ok = False; prereq_msg = "대상자의 '자기평가'가 제출되어야 1차평가를 입력할 수 있습니다."
    elif eval_type == "2차":
        if not has_submitted(year, "1차", target_sabun):
            prereq_ok = False; prereq_msg = "대상자의 '1차평가'가 제출되어야 2차평가를 입력할 수 있습니다."

    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, target_sabun, me_sabun)
    is_locked = (str(saved_meta.get("잠금","")).upper()=="Y") or (str(saved_meta.get("상태","")).strip() in {"제출","완료"})
    # 직원 자기평가: 제출되어 있으면 항상 잠금
    if my_role=="employee" and eval_type=="자기" and has_submitted(year,"자기",me_sabun):
        is_locked = True

    if is_locked:
        st.info("이 응답은 잠겨 있습니다.", icon="🔒")
    if not prereq_ok:
        st.warning(prereq_msg, icon="🧩")

# --- 보기/수정 모드 -------------------------------
    if st.button(("수정모드로 전환" if not st.session_state["eval2_edit_mode"] else "보기모드로 전환"),
                 use_container_width=True, key="eval2_toggle"):
        st.session_state["eval2_edit_mode"] = not st.session_state["eval2_edit_mode"]
        st.rerun()
    # '실제' 편집 가능 여부는 선행조건/잠금도 반영
    requested_edit = bool(st.session_state["eval2_edit_mode"])
    edit_mode = requested_edit and prereq_ok and (not is_locked)
    st.caption(f"현재: **{'수정모드' if edit_mode else '보기모드'}**")

# --- 점수 입력 UI: 표만 -------------------------------
    st.markdown("#### 점수 입력 (자기/1차/2차) — 표에서 직접 수정하세요.")

    # ◇◇ Helper: 특정 평가유형(자기/1차/2차)의 '대상자 기준' 최신 점수(평가자 무관) 로드
    def _stage_scores_any_evaluator(_year: int, _etype: str, _target_sabun: str) -> dict[str, int]:
        try:
            ws = _ensure_eval_resp_sheet(int(_year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i+1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cDT=hmap.get("제출시각")
            # 최신 제출시각 우선
            picked = None; picked_dt = ""
            for r in values[1:]:
                try:
                    if (str(r[cY-1]).strip()==str(_year)
                        and str(r[cT-1]).strip()==str(_etype)
                        and str(r[cTS-1]).strip()==str(_target_sabun)):
                        ts = str(r[cDT-1]) if (cDT and cDT-1 < len(r)) else ""
                        if ts >= (picked_dt or ""):
                            picked = r; picked_dt = ts or ""
                except Exception:
                    pass
            if not picked: return {}
            out: dict[str,int] = {}
            for iid in item_ids:
                col = hmap.get(f"점수_{iid}")
                if col and col-1 < len(picked):
                    try:
                        v = int(str(picked[col-1]).strip() or "0")
                        if v: out[iid] = v
                    except Exception:
                        pass
            return out
        except Exception:
            return {}

    # ◇◇ 일괄 적용(현재 사용자의 '편집 대상' 컬럼에만 적용)
    _year_safe = int(st.session_state.get("eval2_year", datetime.now(tz=tz_kst()).year))
    _eval_type_safe = str(st.session_state.get("eval_type") or st.session_state.get("eval2_type") or ("자기"))
    kbase = f"E2_{_year_safe}_{_eval_type_safe}_{me_sabun}_{target_sabun}"
    slider_key = f"{kbase}_slider_multi"
    if slider_key not in st.session_state:
        if saved_scores:
            avg = round(sum(saved_scores.values()) / max(1, len(saved_scores)))
            st.session_state[slider_key] = int(min(5, max(1, avg)))
        else:
            st.session_state[slider_key] = 3
    bulk_score = st.slider("일괄 점수(현재 편집 컬럼)", 1, 5, step=1, key=slider_key, disabled=not edit_mode)
    if st.button("일괄 적용", use_container_width=True, disabled=not edit_mode, key=f"bulk_multi_{kbase}"):
        for _iid in item_ids:
            st.session_state[f"eval2_seg_{_iid}_{kbase}"] = str(int(bulk_score))
        st.toast(f"모든 항목에 {bulk_score}점 적용", icon="✅")

    # ◇◇ 현재 편집 대상 컬럼/표시 컬럼 결정
    editable_col_name = {"자기":"자기평가","1차":"1차평가","2차":"2차평가"}.get(str(eval_type), "자기평가")
    if my_role == "employee":
        visible_cols = ["자기평가"]
    elif eval_type == "1차":
        visible_cols = ["자기평가","1차평가"]
    else:  # eval_type == "2차": 자기평가도 함께 보여줌
        visible_cols = ["자기평가","1차평가","2차평가"]

    # ◇◇ 시드 데이터 구성
    # - 편집 컬럼: 세션상태 or 현재 저장된 점수(saved_scores)
    # - 참조 컬럼: 가장 최근 제출된 이전 단계 점수
    stage_self = _stage_scores_any_evaluator(int(year), "자기", str(target_sabun)) if "자기평가" in visible_cols else {}
    stage_1st  = _stage_scores_any_evaluator(int(year), "1차", str(target_sabun))  if "1차평가" in visible_cols else {}

    def _seed_for_editable(iid: str):
        # 기본값 공란(None)
        rkey = f"eval2_seg_{iid}_{kbase}"
        if rkey in st.session_state:
            try:
                v = st.session_state[rkey]
                return int(v) if (v is not None and str(v).strip()!="") else None
            except Exception:
                return None
        if iid in saved_scores:
            try:
                return int(saved_scores[iid])
            except Exception:
                return None
        return None

    rows = []
    for r in items_sorted.itertuples(index=False):
        iid = str(getattr(r, "항목ID"))
        row = {
            "항목": getattr(r, "항목") or "",
            "내용": getattr(r, "내용") or "",
            "자기평가": None,
            "1차평가": None,
            "2차평가": None
        }
        # 참조 점수(읽기 컬럼)
        if "자기평가" in visible_cols:
            if editable_col_name=="자기평가":
                row["자기평가"] = _seed_for_editable(iid)
            else:
                v = stage_self.get(iid, None)
                row["자기평가"] = int(v) if v is not None else None
        if "1차평가" in visible_cols:
            if editable_col_name=="1차평가":
                row["1차평가"] = _seed_for_editable(iid)
            else:
                v = stage_1st.get(iid, None)
                row["1차평가"] = int(v) if v is not None else None
        if "2차평가" in visible_cols and editable_col_name=="2차평가":
            row["2차평가"] = _seed_for_editable(iid)

        rows.append(row)

    df_tbl = pd.DataFrame(rows, index=item_ids)

    # ◇◇ 합계 행(표 안에 표시) — 각 컬럼별 합계(빈칸은 0으로 간주)
    def _col_sum(col: str) -> int:
        if col not in df_tbl.columns: return 0
        s = (pd.to_numeric(df_tbl[col], errors="coerce")).fillna(0).astype(int).sum()
        return int(s)

    sum_row = {"항목": "합계", "내용": ""}
    for c in ["자기평가","1차평가","2차평가"]:
        if c in visible_cols:
            sum_row[c] = _col_sum(c)
    df_tbl_with_sum = pd.concat([df_tbl, pd.DataFrame([sum_row], columns=["항목","내용"]+visible_cols)], ignore_index=True)

    # ◇◇ 데이터 에디터 렌더링
    col_cfg = {
        "항목": st.column_config.TextColumn("항목", disabled=True),
        "내용": st.column_config.TextColumn("내용", disabled=True),
    }
    if "자기평가" in visible_cols:
        col_cfg["자기평가"] = st.column_config.NumberColumn("자기평가", min_value=1, max_value=5, step=1, help="자기평가 1~5점", disabled=(editable_col_name!="자기평가" or not edit_mode))
    if "1차평가" in visible_cols:
        col_cfg["1차평가"] = st.column_config.NumberColumn("1차평가", min_value=1, max_value=5, step=1, help="1차평가 1~5점", disabled=(editable_col_name!="1차평가" or not edit_mode))
    if "2차평가" in visible_cols:
        col_cfg["2차평가"] = st.column_config.NumberColumn("2차평가", min_value=1, max_value=5, step=1, help="2차평가 1~5점", disabled=(editable_col_name!="2차평가" or not edit_mode))

    edited = st.data_editor(
        df_tbl_with_sum[["항목","내용"] + visible_cols],
        hide_index=True,
        use_container_width=True,
        disabled=False,  # 일부 컬럼만 disabled
        num_rows="fixed",
        column_config=col_cfg,
        height=min(560, 64 + 36 * len(df_tbl_with_sum))
    )

    # ◇◇ 점수 dict 구성(합계 행 제외, 편집 컬럼만 저장) — 공란은 저장하지 않음
    scores = {}
    if editable_col_name in edited.columns:
        values = list(edited[editable_col_name].tolist())[:-1]  # 마지막 행은 합계
        for iid, v in zip(item_ids, values):
            if v is None or str(v).strip()=="":
                continue
            try:
                val = int(v)
            except Exception:
                continue
            st.session_state[f"eval2_seg_{iid}_{kbase}"] = str(val)
            scores[iid] = val
        st.markdown("#### 제출 확인")
    cb1, cb2 = st.columns([2, 1])
    with cb1:
        attest_ok = st.checkbox(
            "본인은 입력한 내용이 사실이며, 회사의 인사평가 정책에 따라 제출함을 확인합니다.",
            key=f"eval_attest_ok_{kbase}",
            disabled=not edit_mode
        )
    with cb2:
        pin_input = st.text_input(
            "PIN 재입력",
            value="",
            type="password",
            key=f"eval_attest_pin_{kbase}",
            disabled=not edit_mode
        )

    # 🔐 PIN 검증 대상:
    # - 자기평가 : 대상자 사번
    # - 1차/2차  : 평가자(본인) 사번
    sabun_for_pin = str(target_sabun) if str(eval_type) == "자기" else str(me_sabun)

    cbtn = st.columns([1, 1, 3])
    with cbtn[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True,
                            key=f"eval_save_{kbase}", disabled=not edit_mode)
    with cbtn[1]:
        do_reset = st.button("초기화", use_container_width=True,
                             key=f"eval_reset_{kbase}", disabled=not edit_mode)

    if do_reset:
        for _iid in item_ids:
            _k = f"eval2_seg_{_iid}_{kbase}"
            if _k in st.session_state: del st.session_state[_k]
        st.rerun()

    if do_save:
        if not attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        elif not verify_pin(sabun_for_pin, pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            try:
                rep = upsert_eval_response(
                    emp_df, int(year), eval_type, str(target_sabun), str(me_sabun), scores, "제출"
                )
                st.success(
                    ("제출 완료" if rep.get("action") == "insert" else "업데이트 완료")
                    + f" (총점 {rep.get('total','?')}점)",
                    icon="✅",
                )
                st.session_state["eval2_edit_mode"] = False
                st.session_state['eval_rev'] = st.session_state.get('eval_rev', 0) + 1
                st.rerun()
            except Exception as e:
                st.exception(e)

# ═════════════════════════════════════════════════════════════════════════════
# 직무기술서
# ═════════════════════════════════════════════════════════════════════════════
JOBDESC_SHEET = "직무기술서"
JOBDESC_HEADERS = [
    "사번","이름","연도","버전","부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","제출시각"
]

def ensure_jobdesc_sheet():
    wb = get_book()
    try:
        ws = wb.worksheet(JOBDESC_SHEET)
        header = _retry(ws.row_values, 1) or []
        need = [h for h in JOBDESC_HEADERS if h not in header]
        if need:
            if AUTO_FIX_HEADERS:
                _retry(ws.update, "1:1", [header + need])
            else:
                try:
                    st.warning(
                        "시트 헤더에 다음 컬럼이 없습니다: "
                        + ", ".join(need)
                        + "\n→ 시트를 직접 수정한 뒤 좌측 🔄 동기화 버튼을 눌러주세요.",
                        icon="⚠️",
                    )
                except Exception:
                    pass
        return ws
    except Exception as e:
        # WorksheetNotFound 등
        ws = _retry(wb.add_worksheet, title=JOBDESC_SHEET, rows=2000, cols=80)
        _retry(ws.update, "A1", [JOBDESC_HEADERS])
        return ws

@st.cache_data(ttl=600, show_spinner=False)
def read_jobdesc_df(_rev: int = 0) -> pd.DataFrame:
    ensure_jobdesc_sheet()
    ws = _ws(JOBDESC_SHEET)
    df = pd.DataFrame(_ws_get_all_records(ws))
    if df.empty:
        return pd.DataFrame(columns=JOBDESC_HEADERS)
    # 타입 정리
    for c in JOBDESC_HEADERS:
        if c in df.columns:
            df[c] = df[c].astype(str)
    for c in ["연도","버전"]:
        if c in df.columns:
            def _i(x):
                try:
                    return int(float(str(x).strip()))
                except:
                    return 0
            df[c] = df[c].apply(_i)
    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)
    return df

def _jd_latest_for(sabun: str, year: int) -> dict | None:
    df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
    if df.empty:
        return None
    sub = df[(df["사번"].astype(str) == str(sabun)) & (df["연도"].astype(int) == int(year))].copy()
    if sub.empty:
        return None
    try:
        sub["버전"] = sub["버전"].astype(int)
    except Exception:
        pass
    sub = sub.sort_values(["버전"], ascending=[False]).reset_index(drop=True)
    row = sub.iloc[0].to_dict()
    for k, v in row.items():
        row[k] = ("" if v is None else str(v))
    return row

def _jobdesc_next_version(sabun: str, year: int) -> int:
    df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
    if df.empty:
        return 1
    # 안전: 타입 강제
    df["사번"] = df.get("사번", "").astype(str)
    df["연도"] = pd.to_numeric(df.get("연도", 0), errors="coerce").fillna(0).astype(int)
    df["버전"] = pd.to_numeric(df.get("버전", 0), errors="coerce").fillna(0).astype(int)

    sub = df[(df["사번"] == str(sabun)) & (df["연도"] == int(year))]
    if sub.empty:
        return 1
    return int(sub["버전"].max()) + 1

def _ws_batch_row(ws, idx: int, hmap: dict, kv: dict):
    """단일 행(idx)에 대해 key→value 매핑을 A1 좌표로 batch_update."""
    upd = []
    for k, v in kv.items():
        c = hmap.get(k)
        if not c:
            continue
        try:
            a1 = gspread.utils.rowcol_to_a1(int(idx), int(c))
            upd.append({"range": a1, "values": [[v]]})
        except Exception:
            pass
    if upd:
        _retry(ws.batch_update, upd)

def upsert_jobdesc(rec: dict, as_new_version: bool = False) -> dict:
    ensure_jobdesc_sheet()
    ws = _ws(JOBDESC_SHEET)
    header = _retry(ws.row_values, 1) or []   # ← 가드 추가
    hmap = {n: i + 1 for i, n in enumerate(header)}
    sabun = str(rec.get("사번", "")).strip()
    year = int(rec.get("연도", 0))

    # 이름 자동 채움
    rec["이름"] = _emp_name_by_sabun(read_emp_df(), sabun)

    # 버전 결정
    if as_new_version:
        ver = _jobdesc_next_version(sabun, year)
    else:
        try_ver = int(str(rec.get("버전", 0) or 0))
        if try_ver <= 0:
            ver = _jobdesc_next_version(sabun, year)
        else:
            df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
            exist = not df[(df["사번"] == sabun) & (df["연도"].astype(int) == year) & (df["버전"].astype(int) == try_ver)].empty
            ver = try_ver if exist else 1
    rec["버전"] = int(ver)
    rec["제출시각"] = kst_now_str()
    rec["이름"] = _emp_name_by_sabun(read_emp_df(), sabun)

    values = _ws_values(ws)
    row_idx = 0
    cS, cY, cV = hmap.get("사번"), hmap.get("연도"), hmap.get("버전")
    for i in range(2, len(values) + 1):
        row = values[i - 1]
        if str(row[cS - 1]).strip() == sabun and str(row[cY - 1]).strip() == str(year) and str(row[cV - 1]).strip() == str(ver):
            row_idx = i
            break

    def build_row():
        buf = [""] * len(header)
        for k, v in rec.items():
            c = hmap.get(k)
            if c:
                buf[c - 1] = v
        return buf

    if row_idx == 0:
        _retry(ws.append_row, build_row(), value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "version": ver}
    else:
        _ws_batch_row(ws, row_idx, hmap, rec)
        st.cache_data.clear()
        return {"action": "update", "version": ver}

# ────────────────────────────────────────────────────────────────────────────
# 인쇄용 HTML (심플 · 모든 섹션 포함 · 첫 페이지부터 연속 인쇄)
# - 미리보기 내부에 인쇄 버튼 노출
# - 한글 폰트 스택 강화, 줄바꿈 품질 향상
# ────────────────────────────────────────────────────────────────────────────
def _jd_print_html(jd: dict, meta: dict) -> str:
    def g(k): return (str(jd.get(k, "")) or "—").strip()
    def m(k): return (str(meta.get(k, "")) or "—").strip()

    # Combine departments
    dept = m('부서1')
    if m('부서2') != '—' and m('부서2'):
        dept = f"{dept} / {m('부서2')}" if dept and dept != '—' else m('부서2')

# --- Meta rows -------------------------------
    row1 = [("사번", m('사번')), ("이름", m('이름')), ("부서", dept or "—")]
    row2 = [("직종", m('직종')), ("직군", m('직군')), ("직무명", m('직무명'))]
    row3 = [("연도", m('연도')), ("버전", m('버전')), ("제정일", m('제정일')), ("개정일", m('개정일')), ("검토주기", m('검토주기'))]

    # Helpers
    def trow_3cols_kvk(title_pairs, wide_last=True):
        cells = []
        for i, (k, v) in enumerate(title_pairs):
            wide_cls = " wide" if (wide_last and i == 2) else ""
            cells.append(f"<td class='k'>{k}</td><td class='v{wide_cls}'>{v}</td>")
        return "<tr>" + "".join(cells) + "</tr>"

    def trow_5cols_kvk(title_pairs):
        cells = []
        for (k, v) in title_pairs:
            cells.append(f"<td class='k'>{k}</td><td class='v'>{v}</td>")
        return "<tr>" + "".join(cells) + "</tr>"

    def block(title, body):
        body_val = (body or "").strip() or "—"
        return f"""
        <section class="blk">
          <div class="cap">{title}</div>
          <div class="body">{body_val}</div>
        </section>
        """

    body_html = (
        block("직무개요", g("직무개요")) +
        block("주요업무", g("주업무")) +
        block("기타업무", g("기타업무")) +
        block("자격교육요건", f"""
            <div class="grid edu">
              <div class="cell"><b>필요학력</b><div>{g("필요학력")}</div></div>
              <div class="cell"><b>전공계열</b><div>{g("전공계열")}</div></div>
              <div class="cell"><b>면허</b><div>{g("면허")}</div></div>
              <div class="cell"><b>경력(자격요건)</b><div>{g("경력(자격요건)")}</div></div>

              <div class="cell span2"><b>직원공통필수교육</b><div>{g("직원공통필수교육")}</div></div>
              <div class="cell span2"><b>특성화교육</b><div>{g("특성화교육")}</div></div>
              <div class="cell span2"><b>보수교육</b><div>{g("보수교육")}</div></div>
              <div class="cell span2"><b>기타교육</b><div>{g("기타교육")}</div></div>
            </div>
        """)
    )

    html = f"""
    <html>
    <head>
      <meta charset="utf-8" />
      <title>직무기술서 출력</title>
      <style>
        :root {{ --fg:#111; --muted:#666; --line:#e5e7eb; }}
        html, body {{
          color: var(--fg);
          font-family: 'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic','Segoe UI',Roboto,system-ui,sans-serif;
          -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
          orphans: 2; widows: 2; word-break: keep-all; overflow-wrap: anywhere;
        }}
        .print-wrap {{ max-width: 900px; margin: 0 auto; padding: 28px 24px; background:#fff; }}
        .actionbar {{ display:flex; justify-content:flex-end; margin-bottom:12px; }}
        .actionbar button {{ padding:6px 12px; border:1px solid var(--line); background:#fff; border-radius:6px; cursor:pointer; }}
        header {{ border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:18px; }}
        header h1 {{ margin:0; font-size: 22px; }}

        /* === Meta tables (3 rows) === */
        table.meta6 {{ width:100%; border-collapse:collapse; margin-top:4px; font-size:13px; color:var(--muted); table-layout:fixed; }}
        table.meta6 td {{ padding:4px 6px; vertical-align:top; border-bottom:1px dashed var(--line); }}
        table.meta6 td.k {{ width:10%; color:#111; font-weight:700; white-space:nowrap; }}
        table.meta6 td.v {{ width:20%; color:#333; overflow:hidden; text-overflow:ellipsis; }}
        table.meta6 td.v.wide {{ width:30%; }} /* 부서/직무명 등 넓은 칸 */

        table.meta10 {{ width:100%; border-collapse:collapse; margin-top:4px; font-size:13px; color:var(--muted); table-layout:fixed; }}
        table.meta10 td {{ padding:4px 6px; vertical-align:top; border-bottom:1px dashed var(--line); }}
        table.meta10 td.k {{ width:10%; color:#111; font-weight:700; white-space:nowrap; }}
        table.meta10 td.v {{ width:10%; color:#333; overflow:hidden; text-overflow:ellipsis; }}

        /* === Blocks with SMALL captions and 11px body === */
        .blk {{ break-inside: auto; page-break-inside: auto; margin: 12px 0 16px; }}
        .blk .cap {{ font-size:13px; color:#111; font-weight:700; margin: 2px 0 6px; }}
        .blk .body {{ white-space: pre-wrap; font-size:11px; line-height: 1.55; border:1px solid var(--line); padding:10px; border-radius:8px; min-height:60px; }}

        /* Education grid */
        .grid.edu {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px; }}
        .grid.edu .cell {{ border:1px solid var(--line); border-radius:8px; padding:8px; }}
        /* INNER field labels smaller than section caption (12px < 13px) */
        .grid.edu .cell > b {{ font-size:12px; color:#111; }}
        .grid.edu .cell > div {{ font-size:11px; line-height:1.55; color:#333; }}
        .grid.edu .cell.span2 {{ grid-column: 1 / -1; }} /* full-width lines for long fields */

        /* Signature area */
        .sign {{ margin-top:20px; display:flex; gap:16px; }}
        .sign > div {{ flex:1; border:1px dashed var(--line); border-radius:8px; padding:10px; min-height:70px; }}
        .sign .cap {{ font-size:13px; color:#111; font-weight:700; margin-bottom:6px; }}
        .sign .body {{ font-size:11px; line-height:1.55; color:#333; }}

        @media print {{
          @page {{ size: A4; margin: 18mm 14mm; }}
          body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
          .actionbar {{ display:none !important; }}
        }}
      </style>
    </head>
    <body>
      <div class="print-wrap">
        <div class="actionbar"><button onclick="window.print()">인쇄</button></div>
        <header>
          <h1>직무기술서 (Job Description)</h1>
          <!-- Row 1 -->
          <table class="meta6">
            {trow_3cols_kvk(row1, wide_last=True)}
          </table>
          <!-- Row 2 -->
          <table class="meta6">
            {trow_3cols_kvk(row2, wide_last=True)}
          </table>
          <!-- Row 3 -->
          <table class="meta10">
            {trow_5cols_kvk(row3)}
          </table>
        </header>
        {body_html}
        <div class="sign">
          <div>
            <div class="cap">직원 확인 서명</div>
            <div class="body"></div>
          </div>
          <div>
            <div class="cap">부서장 확인 서명</div>
            <div class="body"></div>
          </div>
        </div>
      </div>
    </body>
    </html>
    """
    return html

# ===== JD Approval (within JD tab) =====
JD_APPROVAL_SHEET = "직무기술서_승인"
JD_APPROVAL_HEADERS = ["연도","사번","이름","버전","승인자사번","승인자이름","상태","승인시각","비고"]

def ensure_jd_approval_sheet():
    wb = get_book()
    try:
        _ = wb.worksheet(JD_APPROVAL_SHEET)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=JD_APPROVAL_SHEET, rows=3000, cols=20)
        _retry(ws.update, "1:1", [JD_APPROVAL_HEADERS])

@st.cache_data(ttl=300, show_spinner=False)
def read_jd_approval_df(_rev: int = 0) -> pd.DataFrame:
    ensure_jd_approval_sheet()
    try:
        ws = _ws(JD_APPROVAL_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        df = pd.DataFrame(columns=JD_APPROVAL_HEADERS)
    for c in JD_APPROVAL_HEADERS:
        if c not in df.columns:
            df[c] = ""
    if "연도" in df.columns:
        df["연도"] = pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int)
    if "버전" in df.columns:
        df["버전"] = pd.to_numeric(df["버전"], errors="coerce").fillna(0).astype(int)
    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)
    return df

def _ws_batch_row(ws, idx, hmap, kv: dict):
    updates = []
    max_c = 0
    for k, v in kv.items():
        c = hmap.get(k)
        if not c:
            continue
        try:
            cc = int(c)
        except Exception:
            continue
        if cc > max_c:
            max_c = cc
        a1 = gspread.utils.rowcol_to_a1(int(idx), cc)
        updates.append({"range": f"'{ws.title}'!{a1}", "values": [[v]]})
    if updates:
        _ensure_capacity(ws, int(idx), int(max_c) if max_c else None)
        body = {"valueInputOption": "USER_ENTERED", "data": updates}
        _retry(ws.spreadsheet.values_batch_update, body)

def _jd_latest_version_for(sabun: str, year: int) -> int:
    row = _jd_latest_for(sabun, int(year)) or {}
    try:
        return int(row.get("버전", 0) or 0)
    except Exception:
        return 0

def set_jd_approval(year: int, sabun: str, name: str, version: int,
                    approver_sabun: str, approver_name: str, status: str, remark: str = "") -> dict:
    """
    (연도, 사번, 버전) 기준 upsert. status: '승인' | '반려'
    """
    ensure_jd_approval_sheet()
    ws = _ws(JD_APPROVAL_SHEET)
    header = _retry(ws.row_values, 1) or JD_APPROVAL_HEADERS
    hmap = {n: i+1 for i, n in enumerate(header)}
    values = _ws_values(ws)
    cY = hmap.get("연도"); cS = hmap.get("사번"); cV = hmap.get("버전")
    target_row = 0
    for i in range(2, len(values)+1):
        r = values[i-1] if i-1 < len(values) else []
        try:
            if (str(r[cY-1]).strip()==str(year) and str(r[cS-1]).strip()==str(sabun) and str(r[cV-1]).strip()==str(version)):
                target_row = i
                break
        except Exception:
            pass
    now = kst_now_str() if "kst_now_str" in globals() else str(pd.Timestamp.now()).split(".")[0]
    payload = {
        "연도": int(year),
        "사번": str(sabun),
        "이름": str(name),
        "버전": int(version),
        "승인자사번": str(approver_sabun),
        "승인자이름": str(approver_name),
        "상태": str(status),
        "승인시각": now,
        "비고": str(remark or ""),
    }
    if target_row > 0:
        _ws_batch_row(ws, target_row, hmap, payload)
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "update", "row": target_row}
    else:
        rowbuf = [""] * len(header)
        for k, v in payload.items():
            c = hmap.get(k)
            if c:
                rowbuf[c-1] = v
        _retry(ws.append_row, rowbuf, value_input_option="USER_ENTERED")
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "insert", "row": len(values) + 1}

def tab_job_desc(emp_df: pd.DataFrame):
    """JD editor with 2-row header and 4-row education layout + print button order handled by _jd_print_html()."""
    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="jd2_year")

    u = st.session_state["user"]
    me_sabun = str(u["사번"]); me_name = str(u["이름"])

    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False)) > 0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("jd2_target_sabun", glob_sab or "")
    st.session_state.setdefault("jd2_target_name",  glob_name or me_name)
    st.session_state.setdefault("jd2_edit_mode",    False)

    # 대상자 선택
    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base = emp_df.copy()
        base["사번"] = base["사번"].astype(str)
        base = base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns:
            base = base[base["재직여부"] == True]
        cols = ["사번", "이름", "부서1", "부서2"]
        extra = ["직급"] if "직급" in base.columns else []
        view = base[cols + extra].copy().sort_values(["사번"]).reset_index(drop=True)
        _sabuns = view["사번"].astype(str).tolist(); _names = view["이름"].astype(str).tolist()
        _d2 = view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""] * len(_sabuns)
        _opts = [f"{s} - {n} - {d2}" for s, n, d2 in zip(_sabuns, _names, _d2)]
        _target = st.session_state.get("jd2_target_sabun", glob_sab or "")
        _idx = _sabuns.index(_target) if _target in _sabuns else 0
        _idx2 = (1 + _sabuns.index(_target)) if (_target in _sabuns) else 0
        _sel = st.selectbox("대상자 선택", ["(선택)"] + _opts, index=_idx2, key="jd2_pick_editor_select")
        if _sel == "(선택)":
            st.session_state["jd2_target_sabun"] = ""
            st.session_state["jd2_target_name"]  = ""
            st.info("대상자를 선택하세요.", icon="👈")
            return
        _sel_sab = _sel.split(" - ", 1)[0] if isinstance(_sel, str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["jd2_target_sabun"] = str(_sel_sab)
        try:
            st.session_state["jd2_target_name"] = str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["jd2_target_name"] = ""
        target_sabun = st.session_state["jd2_target_sabun"]; target_name = st.session_state["jd2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")
    try:
        _jd = _jd_latest_for(str(target_sabun), int(year)) or {}
        _sub_ts = (str(_jd.get('제출시각','')).strip() or "미제출")
        latest_ver = _jd_latest_version_for(str(target_sabun), int(year))

        appr_df = read_jd_approval_df(st.session_state.get('appr_rev', 0))
        _appr_status = "미제출"
        _appr_time = ""
        if latest_ver > 0 and not appr_df.empty:
            # 최신 승인/반려 레코드 한 건 선택 (승인시각 기준 내림차순)
            sub = appr_df[(appr_df['연도'] == int(year)) &
                          (appr_df['사번'].astype(str) == str(target_sabun)) &
                          (appr_df['버전'] == int(latest_ver))].copy()
            if not sub.empty:
                if '승인시각' in sub.columns:
                    sub = sub.sort_values(['승인시각'], ascending=[False]).reset_index(drop=True)
                srow = sub.iloc[0].to_dict()
                _appr_status = str(srow.get('상태','')).strip() or "미제출"     # 승인 / 반려 / (없음)
                _appr_time   = str(srow.get('승인시각','')).strip()

        # 표기: 제출시각(직원 제출) | [부서장 승인여부] 승인/반려 (승인시각)
        _appr_right = _appr_status if _appr_status else "미제출"
        if _appr_time:
            _appr_right += f" {_appr_time}"
        show_submit_banner(f"🕒 제출시각  |  {_sub_ts if _sub_ts else '미제출'}  |  [부서장 승인여부] {_appr_right}")
    except Exception:
        pass

    # 모드 토글 (인사평가와 동일 레이아웃)
    if st.button(("수정모드로 전환" if not st.session_state["jd2_edit_mode"] else "보기모드로 전환"),
                 use_container_width=True, key="jd2_toggle"):
        st.session_state["jd2_edit_mode"] = not st.session_state["jd2_edit_mode"]
        st.rerun()
    st.caption(f"현재: **{'수정모드' if st.session_state['jd2_edit_mode'] else '보기모드'}**")
    edit_mode = bool(st.session_state["jd2_edit_mode"])

    # 현재/초기 레코드
    jd_saved = _jd_latest_for(target_sabun, int(year))

    def _safe_get(col, default=""):
        try:
            return emp_df.loc[emp_df["사번"].astype(str) == str(target_sabun)].get(col, default).values[0] if col in emp_df.columns else default
        except Exception:
            return default

    jd_current = jd_saved if jd_saved else {
        "사번": str(target_sabun), "연도": int(year), "버전": 0,
        "부서1": _safe_get("부서1",""), "부서2": _safe_get("부서2",""),
        "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
        "직군": _safe_get("직군",""), "직종": _safe_get("직무",""), "직무명": "", "제정일": "", "개정일": "", "검토주기": "1년",
        "직무개요": "", "주업무": "", "기타업무": "",
        "필요학력": "", "전공계열": "",
        "직원공통필수교육": "", "보수교육": "", "기타교육": "", "특성화교육": "",
        "면허": "", "경력(자격요건)": "", "비고": ""
    }

    with st.expander("현재 저장된 직무기술서 요약", expanded=False):
        st.write(f"**직무명:** {(jd_saved or {}).get('직무명', '')}")
        cc = st.columns(2)
        with cc[0]:
            st.markdown("**직무개요**")
            st.markdown(_jd_plain_html((jd_saved or {}).get("직무개요","") or "—"), unsafe_allow_html=True)
            st.markdown("**주업무**")
            st.markdown(_jd_plain_html((jd_saved or {}).get("주업무","") or "—"), unsafe_allow_html=True)
        with cc[1]:
            st.markdown("**기타업무**")
            st.markdown(_jd_plain_html((jd_saved or {}).get("기타업무","") or "—"), unsafe_allow_html=True)

    # =================== Header Row 1 (가로) ===================
    r1 = st.columns([1, 1, 1, 1, 1.6])
    with r1[0]:
        version = st.number_input("버전(없으면 자동)", min_value=0, max_value=999,
                                  value=int(str(jd_current.get("버전", 0)) or 0),
                                  step=1, key="jd2_ver", disabled=not edit_mode)
    with r1[1]:
        d_create = st.text_input("제정일", value=jd_current.get("제정일",""), key="jd2_d_create", disabled=not edit_mode)
    with r1[2]:
        d_update = st.text_input("개정일", value=jd_current.get("개정일",""), key="jd2_d_update", disabled=not edit_mode)
    with r1[3]:
        review = st.text_input("검토주기", value=jd_current.get("검토주기",""), key="jd2_review", disabled=not edit_mode)
    with r1[4]:
        memo = st.text_input("비고", value=jd_current.get("비고",""), key="jd2_memo", disabled=not edit_mode)

    # =================== Header Row 2 (가로) ===================
    r2 = st.columns([1, 1, 1, 1, 1.6])
    with r2[0]:
        dept1  = st.text_input("부서1", value=jd_current.get("부서1",""), key="jd2_dept1", disabled=not edit_mode)
    with r2[1]:
        dept2  = st.text_input("부서2", value=jd_current.get("부서2",""), key="jd2_dept2", disabled=not edit_mode)
    with r2[2]:
        group  = st.text_input("직군", value=jd_current.get("직군",""), key="jd2_group", disabled=not edit_mode)
    with r2[3]:
        series = st.text_input("직종", value=jd_current.get("직종",""), key="jd2_series", disabled=not edit_mode)
    with r2[4]:
        jobname= st.text_input("직무명", value=jd_current.get("직무명",""), key="jd2_jobname", disabled=not edit_mode)

    # 본문
    job_summary = st.text_area("직무개요", value=jd_current.get("직무개요",""), height=80,  key="jd2_summary", disabled=not edit_mode)
    job_main    = st.text_area("주업무",   value=jd_current.get("주업무",""),   height=120, key="jd2_main",    disabled=not edit_mode)
    job_other   = st.text_area("기타업무", value=jd_current.get("기타업무",""), height=80,  key="jd2_other",   disabled=not edit_mode)

    # =================== Education/Qualification (4 rows) ===================
    # R1: 필요학력 | 전공계열 | 면허 | 경력(자격요건)
    e1 = st.columns([1,1,1,1])
    with e1[0]: edu_req    = st.text_input("필요학력",        value=jd_current.get("필요학력",""),        key="jd2_edu",        disabled=not edit_mode)
    with e1[1]: major_req  = st.text_input("전공계열",        value=jd_current.get("전공계열",""),        key="jd2_major",      disabled=not edit_mode)
    with e1[2]: license_   = st.text_input("면허",            value=jd_current.get("면허",""),            key="jd2_license",    disabled=not edit_mode)
    with e1[3]: career     = st.text_input("경력(자격요건)", value=jd_current.get("경력(자격요건)",""), key="jd2_career",     disabled=not edit_mode)

    # R2: 직원공통필수교육 (full width)
    edu_common = st.text_input("직원공통필수교육", value=jd_current.get("직원공통필수교육",""), key="jd2_edu_common", disabled=not edit_mode)

    # R3: 특성화교육 (full width)
    edu_spec   = st.text_input("특성화교육",       value=jd_current.get("특성화교육",""),       key="jd2_edu_spec",   disabled=not edit_mode)

    # R4: 보수교육 | 기타교육
    e4 = st.columns([1,1])
    with e4[0]: edu_cont   = st.text_input("보수교육",        value=jd_current.get("보수교육",""),        key="jd2_edu_cont",   disabled=not edit_mode)
    with e4[1]: edu_etc    = st.text_input("기타교육",        value=jd_current.get("기타교육",""),        key="jd2_edu_etc",    disabled=not edit_mode)

    # 제출 확인
    ca1, ca2 = st.columns([2, 1])
    with ca1:
        jd_attest_ok = st.checkbox(
            "본인은 입력한 직무기술서 내용이 사실이며, 회사 정책에 따라 제출함을 확인합니다.",
            key=f"jd_attest_ok_{year}_{target_sabun}_{me_sabun}",
        )
    with ca2:
        jd_pin_input = st.text_input(
            "PIN 재입력",
            value="",
            type="password",
            key=f"jd_attest_pin_{year}_{target_sabun}_{me_sabun}",
        )

    # 버튼
    cbtn = st.columns([1, 1])
    with cbtn[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True, key="jd2_save", disabled=not edit_mode)
    with cbtn[1]:
        do_print = st.button("인쇄", type="secondary", use_container_width=True, key="jd2_print", disabled=False)

    if do_save:
        if not jd_attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        elif not verify_pin(me_sabun, jd_pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            rec = {
                "사번": str(target_sabun), "연도": int(year), "버전": int(version or 0),
                "부서1": dept1, "부서2": dept2, "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
                "직군": group, "직종": series, "직무명": jobname,
                "제정일": d_create, "개정일": d_update, "검토주기": review,
                "직무개요": job_summary, "주업무": job_main, "기타업무": job_other,
                "필요학력": edu_req, "전공계열": major_req,
                "직원공통필수교육": edu_common, "보수교육": edu_cont, "기타교육": edu_etc, "특성화교육": edu_spec,
                "면허": license_, "경력(자격요건)": career, "비고": memo
            }
            try:
                rep = upsert_jobdesc(rec, as_new_version=(version == 0))
                st.success(f"저장 완료 (버전 {rep['version']})", icon="✅")
                st.session_state['jobdesc_rev'] = st.session_state.get('jobdesc_rev', 0) + 1
                st.rerun()
            except Exception as e:
                st.exception(e)

    # 인쇄
    if do_print:
        meta = {
            "사번": str(target_sabun), "이름": str(target_name),
            "부서1": str(dept1), "부서2": str(dept2),
            "연도": int(year), "버전": int(version or (jd_current.get("버전") or 1)),
            "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
            "제정일": str(d_create), "개정일": str(d_update),
            "검토주기": str(review),
            "직종": str(series), "직군": str(group), "직무명": str(jobname),
        }
        html = _jd_print_html(jd_current, meta)

        components.html(html, height=1000, scrolling=True)

    # ===== (관리자/부서장) 승인 처리 =====
    if am_admin_or_mgr:
        st.markdown("### 부서장 승인")
        appr_df = read_jd_approval_df(st.session_state.get("appr_rev", 0))
        latest_ver = _jd_latest_version_for(target_sabun, int(year))
        _approved = False
        if latest_ver > 0 and not appr_df.empty:
            _ok = appr_df[(appr_df['연도'] == int(year)) & (appr_df['사번'].astype(str) == str(target_sabun)) & (appr_df['버전'] == int(latest_ver)) & (appr_df['상태'].astype(str) == '승인')]
            _approved = not _ok.empty
        cur_status = ""
        cur_when = ""
        cur_who = ""
        if latest_ver > 0 and not appr_df.empty:
            sub = appr_df[(appr_df["연도"]==int(year)) & (appr_df["사번"].astype(str)==str(target_sabun)) & (appr_df["버전"]==int(latest_ver))]
            if not sub.empty:
                srow = sub.sort_values(["승인시각"], ascending=[False]).iloc[0].to_dict()
                cur_status = str(srow.get("상태",""))
                cur_when = str(srow.get("승인시각",""))
                cur_who = str(srow.get("승인자이름",""))
        # 의견/핀 입력 (의견을 좌측에 크게)
        c_remark, c_pin = st.columns([4,1])
        with c_remark:
            if _approved:
                st.markdown("<div class='approval-dim'>부서장 승인이 완료된 대상자입니다. (수정/변경 불가)</div>", unsafe_allow_html=True)
            else:
                appr_remark = st.text_input("부서장 의견", key=f"jd_appr_remark_{year}_{target_sabun}")
        with c_pin:
            appr_pin = st.text_input("부서장 PIN 재입력", type="password", key=f"jd_appr_pin_{year}_{target_sabun}")

        if not _approved:
            # 승인/반려 버튼
            b1, b2 = st.columns([1,1])
            with b1:
                do_ok = st.button("승인", type="primary", use_container_width=True, disabled=not (latest_ver>0))
            with b2:
                do_rej = st.button("반려", use_container_width=True, disabled=not (latest_ver>0))

            if do_ok or do_rej:
                if not verify_pin(me_sabun, appr_pin):
                    st.error("부서장 PIN 이 올바르지 않습니다.", icon="🚫")
                else:
                    status = "승인" if do_ok else "반려"
                    with st.spinner("처리 중..."):
                        res = set_jd_approval(
                            year=int(year),
                            sabun=str(target_sabun),
                            name=str(target_name),
                            version=int(latest_ver),
                            approver_sabun=str(me_sabun),
                            approver_name=str(me_name),
                            status=status,
                            remark=appr_remark
                        )
                        st.session_state["appr_rev"] = st.session_state.get("appr_rev", 0) + 1
                    st.success(f"{status} 처리되었습니다. ({res.get('action')})", icon="✅")
                    appr_df = read_jd_approval_df(st.session_state.get("appr_rev", 0))
                base["사번"] = base["사번"].astype(str)
            base = base[base["사번"].isin({str(s) for s in allowed})]
            if "재직여부" in base.columns:
                base = base[base["재직여부"] == True]
            base = base.sort_values(["사번"]).reset_index(drop=True)

# ═════════════════════════════════════════════════════════════════════════════
# 직무능력평가 + JD 요약 스크롤
# ═════════════════════════════════════════════════════════════════════════════
COMP_SIMPLE_NAME = "직무능력평가"
COMP_SIMPLE_HEADERS = [
    "연도","평가대상사번","평가대상이름","평가자사번","평가자이름",
    "주업무평가","기타업무평가","교육이수","자격유지","종합의견",
    "상태","제출시각","잠금"
]
def _simp_sheet_name(year:int|str)->str: return COMP_SIMPLE_NAME

def _ensure_comp_simple_sheet(year:int):
    wb = get_book(); name = _simp_sheet_name(year)
    try:
        ws = wb.worksheet(name)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=name, rows=2000, cols=50)
    # 항상 표준 헤더로 정렬/덮어쓰기
    _retry(ws.update, "1:1", [COMP_SIMPLE_HEADERS])
    return ws

def _jd_latest_for_comp(sabun:str, year:int)->dict:
    try:
        df=read_jobdesc_df()
        if df is None or len(df)==0: return {}
        q=df[(df["사번"].astype(str)==str(sabun))&(df["연도"].astype(int)==int(year))]
        if q.empty: return {}
        if "버전" in q.columns:
            try:
                q = q.copy()
                q["버전"] = pd.to_numeric(q["버전"], errors="coerce").fillna(0)
            except Exception: pass
            q=q.sort_values("버전").iloc[-1]
        else:
            q=q.iloc[-1]
        return {c:q.get(c,"") for c in q.index}
    except Exception: return {}

def _edu_completion_from_jd(jd_row:dict)->str:
    val=str(jd_row.get("직원공통필수교육","")).strip()
    return "완료" if val else "미완료"

def upsert_comp_simple_response(emp_df: pd.DataFrame, year:int, target_sabun:str,
                                evaluator_sabun:str, main_grade:str, extra_grade:str,
                                qual_status:str, opinion:str, eval_date:str)->dict:
    ws = _ensure_comp_simple_sheet(year)
    header = _retry(ws.row_values,1) or COMP_SIMPLE_HEADERS
    hmap = {n:i+1 for i,n in enumerate(header)}

    jd = _jd_latest_for_comp(target_sabun, int(year))
    edu_status = _edu_completion_from_jd(jd)

    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = _kst_now_str_safe() if "kst_now_str" not in globals() else kst_now_str()

    values = _ws_values(ws)
    cY=hmap.get("연도"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values)+1):
        r = values[i-1]
        try:
            if (str(r[cY-1]).strip()==str(year) and
                str(r[cTS-1]).strip()==str(target_sabun) and
                str(r[cES-1]).strip()==str(evaluator_sabun)):
                row_idx = i; break
        except Exception:
            pass

    if row_idx == 0:
        buf = [""]*len(header)
        def put(k,v): 
            c=hmap.get(k)
            if c: buf[c-1]=v
        put("연도", int(year)); put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("주업무평가", main_grade); put("기타업무평가", extra_grade)
        put("교육이수", edu_status); put("자격유지", qual_status); put("종합의견", opinion)
        put("상태","제출"); put("제출시각", now); put("잠금","")
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action":"insert"}
    else:
        _ws_batch_row_v2(ws, row_idx, hmap, {
            "주업무평가": main_grade,
            "기타업무평가": extra_grade,
            "교육이수": edu_status,
            "자격유지": qual_status,
            "종합의견": opinion,
            "상태": "제출",
            "제출시각": now,
        })
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action":"update"}

@st.cache_data(ttl=300, show_spinner=False)
def read_my_comp_simple_rows(year:int, sabun:str)->pd.DataFrame:
    try:
        ws = get_book().worksheet(_simp_sheet_name(year))
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=COMP_SIMPLE_HEADERS)
    if df.empty:
        return df

    # 필터
    if "평가자사번" in df.columns:
        df = df[df["평가자사번"].astype(str)==str(sabun)]
    if "연도" in df.columns:
        df = df[df["연도"].astype(str)==str(year)]

    # 제출시각 파싱(있으면)
    if "제출시각" in df.columns:
        df["_제출_dt"] = pd.to_datetime(df["제출시각"], errors="coerce")

    # 동적 정렬: 대상사번 오름차순, 제출시각 내림차순
    sort_cols = []
    ascending = []
    if "평가대상사번" in df.columns:
        sort_cols.append("평가대상사번"); ascending.append(True)
    if "_제출_dt" in df.columns:
        sort_cols.append("_제출_dt"); ascending.append(False)

    if sort_cols:
        df = df.sort_values(sort_cols, ascending=ascending)

    return df.reset_index(drop=True)

def tab_competency(emp_df: pd.DataFrame):
    # 권한 게이트: 관리자/평가권한자만 접근 가능 (일반 직원 접근 불가)
    u_check = st.session_state.get('user', {})
    me_check = str(u_check.get('사번',''))
    am_admin_or_mgr = (is_admin(me_check) or len(get_allowed_sabuns(emp_df, me_check, include_self=False))>0)
    if not am_admin_or_mgr:
        st.warning('권한이 없습니다. 관리자/평가 권한자만 접근할 수 있습니다.', icon='🔒')
        return

    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmpS_year")

    u=st.session_state.get("user",{}); me_sabun=str(u.get("사번","")); me_name=str(u.get("이름",""))
    allowed=set(map(str, get_allowed_sabuns(emp_df, me_sabun, include_self=True)))
    df=emp_df.copy()
    if "사번" not in df.columns:
        st.info("직원 데이터에 '사번' 컬럼이 없습니다.", icon="ℹ️"); return
    df["사번"]=df["사번"].astype(str); df=df[df["사번"].isin(allowed)].copy()
    if "재직여부" in df.columns: df=df[df["재직여부"]==True]
    for c in ["이름","부서1","부서2","직급"]:
        if c not in df.columns: df[c]=""

    try: df["사번_sort"]=df["사번"].astype(int)
    except Exception: df["사번_sort"]=df["사번"].astype(str)
    df=df.sort_values(["사번_sort","이름"]).reset_index(drop=True)

    glob_sab, _ = get_global_target()
    default = glob_sab if glob_sab in set(df["사번"].astype(str)) else (str(me_sabun) if str(me_sabun) in set(df["사번"]) else df["사번"].astype(str).tolist()[0])
    sabuns=df["사번"].astype(str).tolist(); names=df["이름"].astype(str).tolist()
    d2s=df["부서2"].astype(str).tolist() if "부서2" in df.columns else [""]*len(sabuns)
    opts=[f"{s} - {n} - {d2}" for s,n,d2 in zip(sabuns,names,d2s)]
    sel_idx=sabuns.index(default) if default in sabuns else 0
    sel_label = st.selectbox("대상자 선택", ["(선택)"] + opts, index=0 if not st.session_state.get("cmpS_target_sabun") else (1 + sabuns.index(st.session_state.get("cmpS_target_sabun"))) if st.session_state.get("cmpS_target_sabun") in sabuns else 0, key="cmpS_pick_select")
    if sel_label == "(선택)":
        st.session_state["cmpS_target_sabun"] = ""
        st.session_state["cmpS_target_name"] = ""
        st.info("대상자를 선택하세요.", icon="👈")
        return
    sel_sab=sel_label.split(" - ",1)[0] if isinstance(sel_label,str) else sabuns[sel_idx]
    st.session_state["cmpS_target_sabun"]=str(sel_sab)
    st.session_state["cmpS_target_name"]=_emp_name_by_sabun(emp_df, str(sel_sab))

    st.success(f"대상자: {_emp_name_by_sabun(emp_df, sel_sab)} ({sel_sab})", icon="✅")

    # === 제출시각 배너(직무능력평가) ===
    comp_locked = False
    try:
        _cmap = get_comp_summary_map_cached(int(year), st.session_state.get('comp_rev', 0))
        _cts = (str(_cmap.get(str(sel_sab), ("","","",""))[3]).strip())
        show_submit_banner(f"🕒 제출시각  |  {_cts if _cts else '미제출'}")
        comp_locked = bool(_cts)
    except Exception:
        pass

    with st.expander("직무기술서 요약", expanded=False):
        jd=_jd_latest_for_comp(sel_sab, int(year))
        if jd:
            def V(key): return (_html_escape((jd.get(key,"") or "").strip()) or "—")
            html = f"""
            <div class="scrollbox">
              <div class="kv"><div class="k">직무명</div><div class="v">{V('직무명')}</div></div>
              <div class="kv"><div class="k">직무개요</div><div class="v">{_jd_plain_html(jd.get('직무개요', '') or '—')}</div></div>
              <div class="kv"><div class="k">주요 업무</div><div class="v">{_jd_plain_html(jd.get('주업무', '') or '—')}</div></div>
              <div class="kv"><div class="k">기타업무</div><div class="v">{_jd_plain_html(jd.get('기타업무', '') or '—')}</div></div>
              <div class="kv"><div class="k">필요학력 / 전공</div><div class="v">{V('필요학력')} / {V('전공계열')}</div></div>
              <div class="kv"><div class="k">면허 / 경력(자격요건)</div><div class="v">{V('면허')} / {V('경력(자격요건)')}</div></div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.caption("직무기술서가 없습니다. JD 없이도 평가를 진행할 수 있습니다.")

    st.markdown("### 평가 입력")
    grade_options=["우수","양호","보통","미흡"]
    colG=st.columns(4)
    with colG[0]: g_main = st.radio("주업무 평가", grade_options, index=2, key="cmpS_main", horizontal=False, disabled=comp_locked)
    with colG[1]: g_extra= st.radio("기타업무 평가", grade_options, index=2, key="cmpS_extra", horizontal=False, disabled=comp_locked)
    with colG[2]: qual   = st.radio("직무 자격 유지 여부", ["직무 유지","직무 변경","직무비부여"], index=0, key="cmpS_qual", disabled=comp_locked)
    with colG[3]:
        eval_date = ""  # 입력란 제거: 제출시각으로 대체 기록

    try: edu_status=_edu_completion_from_jd(_jd_latest_for_comp(sel_sab, int(year)))
    except Exception: edu_status="미완료"
    st.metric("교육이수 (자동)", edu_status)
    opinion=st.text_area("종합평가 의견", value="", height=150, key="cmpS_opinion", disabled=comp_locked)

    # ===== 제출 확인(PIN 재확인 + 동의 체크) =====
    cb1, cb2 = st.columns([2, 1])
    with cb1:
        comp_attest_ok = st.checkbox(
            "본인은 입력한 직무능력평가 내용이 사실이며, 회사 정책에 따라 제출함을 확인합니다.",
            key=f"comp_attest_ok_{year}_{sel_sab}_{me_sabun}",
        )
    with cb2:
        comp_pin_input = st.text_input(
            "PIN 재입력",
            value="",
            type="password",
            key=f"comp_attest_pin_{year}_{sel_sab}_{me_sabun}",
        )

    cbtn = st.columns([1, 1, 3])
    with cbtn[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True, key="cmpS_save", disabled=comp_locked)
    with cbtn[1]:
        do_reset = st.button("초기화", use_container_width=True, key="cmpS_reset")

    if do_reset:
        for k in ["cmpS_main","cmpS_extra","cmpS_qual","cmpS_opinion"]:
            if k in st.session_state: del st.session_state[k]
        st.rerun()

    if do_save:
        # 1) 동의 체크
        if not comp_attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        # 2) PIN 검증
        elif not verify_pin(me_sabun, comp_pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            rep = upsert_comp_simple_response(
                emp_df, int(year), str(sel_sab), str(me_sabun), g_main, g_extra, qual, opinion, eval_date
            )
            st.success(("제출 완료" if rep.get("action")=="insert" else "업데이트 완료"), icon="✅")
        st.session_state['comp_rev'] = st.session_state.get('comp_rev', 0) + 1

# ═════════════════════════════════════════════════════════════════════════════
# 관리자: 직원/ PIN 관리 / 인사평가 항목 관리 / 권한 관리
# ═════════════════════════════════════════════════════════════════════════════
REQ_EMP_COLS = [
"사번","이름","부서1","부서2","직급","직무","직군","입사일","퇴사일","기타1","기타2","재직여부","적용여부",
    "PIN_hash","PIN_No"
]

def _get_ws_and_headers(sheet_name: str):
    ws=_ws(sheet_name)
    header,_h=_hdr(ws, sheet_name)
    if not header: raise RuntimeError(f"'{sheet_name}' 헤더(1행) 없음")
    return ws, header, _h

def ensure_emp_sheet_columns():
    ws, header, hmap = _get_ws_and_headers(EMP_SHEET)

    # 필요한(필수) 컬럼 중 현재 헤더에 없는 것들
    need = [c for c in REQ_EMP_COLS if c not in header]
    if need:
        if AUTO_FIX_HEADERS:
            # 기존 헤더 순서를 유지하고, 누락분만 뒤에 추가
            _retry(ws.update, "1:1", [header + need])
            # 갱신된 헤더/맵 재로딩
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
        else:
            try:
                st.warning(
                    f"직원 시트 헤더에 다음 컬럼이 없습니다: {', '.join(need)}\n"
                    "→ 시트를 직접 수정한 뒤 좌측 🔄 동기화 버튼을 눌러주세요.",
                    icon="⚠️",
                )
            except Exception:
                pass

    return ws, header, hmap

def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    c=hmap.get("사번");
    if not c: return 0
    vals=_retry(ws.col_values, c)[1:]
    for i,v in enumerate(vals, start=2):
        if str(v).strip()==str(sabun).strip(): return i
    return 0

def tab_staff_admin(emp_df: pd.DataFrame):
    """직원 시트 편집: 부서 드롭다운 + 체크박스 저장(부분 갱신)."""
    # 1) 시트/헤더 확보
    ws, header, hmap = ensure_emp_sheet_columns()
    view = emp_df.copy()

    # 2) 민감 컬럼 숨기기
    for c in ["PIN_hash", "PIN_No"]:
        view = view.drop(columns=[c], errors="ignore")

    st.write(f"결과: **{len(view):,}명**")

    # 3) 드롭다운 옵션(직원 시트 유니크)
    try:
        dept1_options = [""] + sorted({str(x).strip() for x in emp_df.get("부서1", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()})
    except Exception:
        dept1_options = [""]
    try:
        dept2_options = [""] + sorted({str(x).strip() for x in emp_df.get("부서2", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()})
    except Exception:
        dept2_options = [""]

    # 4) 에디터
    colcfg = {
        "사번": st.column_config.TextColumn("사번", disabled=True),
        "이름": st.column_config.TextColumn("이름"),
        "부서1": st.column_config.SelectboxColumn("부서1", options=dept1_options),
        "부서2": st.column_config.SelectboxColumn("부서2", options=dept2_options),
        "직급": st.column_config.TextColumn("직급"),
        "직무": st.column_config.TextColumn("직무"),
        "직군": st.column_config.TextColumn("직군"),
        "입사일": st.column_config.TextColumn("입사일"),
        "퇴사일": st.column_config.TextColumn("퇴사일"),
        "기타1": st.column_config.TextColumn("기타1"),
        "기타2": st.column_config.TextColumn("기타2"),
        "재직여부": st.column_config.CheckboxColumn("재직여부"),
        "적용여부": st.column_config.CheckboxColumn("적용여부"),
    }

    edited = st.data_editor(
        view,
        use_container_width=True,
        height=560,
        hide_index=True,
        num_rows="fixed",
        column_config=colcfg,
    )

    # 5) 저장(변경된 칼럼만 부분 갱신)
    if st.button("변경사항 저장", type="primary", use_container_width=True):
        try:
            before = view.set_index("사번")
            after  = edited.set_index("사번")

            # 안전장치: 빈 키 제거
            before = before[before.index.astype(str) != ""]
            after  = after[after.index.astype(str) != ""]

            change_cnt = 0
            for sabun in after.index:
                if sabun not in before.index:
                    continue  # num_rows="fixed" 환경에서는 거의 없음

                payload = {}
                for c in after.columns:
                    if c not in before.columns:
                        continue
                    v0 = before.loc[sabun, c]
                    v1 = after.loc[sabun, c]
                    if str(v0) != str(v1):
                        if c in ("재직여부", "적용여부"):
                            payload[c] = bool(v1)
                        else:
                            payload[c] = v1

                if not payload:
                    continue

                row_idx = _find_row_by_sabun(ws, hmap, str(sabun))
                if row_idx > 0:
                    _ws_batch_row(ws, row_idx, hmap, payload)
                    change_cnt += 1

            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.success(f"저장 완료: {change_cnt}명 반영", icon="✅")
        except Exception as e:
            st.exception(e)

def reissue_pin_inline(sabun: str, length: int = 4):
    ws, header, hmap = ensure_emp_sheet_columns()
    if "PIN_hash" not in hmap or "PIN_No" not in hmap: raise RuntimeError("PIN_hash/PIN_No 필요")
    row_idx=_find_row_by_sabun(ws, hmap, str(sabun))
    if row_idx==0: raise RuntimeError("사번을 찾지 못했습니다.")
    pin = "".join(pysecrets.choice("0123456789") for _ in range(length))
    ph  = _pin_hash(pin, str(sabun))
    _retry(ws.update_cell, row_idx, hmap["PIN_hash"], ph)
    _retry(ws.update_cell, row_idx, hmap["PIN_No"], pin)
    st.cache_data.clear()
    return {"PIN_No": pin, "PIN_hash": ph}

def tab_admin_pin(emp_df):
    ws, header, hmap = ensure_emp_sheet_columns()
    df = emp_df.copy()
    # 적용여부가 체크된 직원만 선택 대상으로 노출
    if "적용여부" in df.columns:
        df = df[df["적용여부"]==True].copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"]) if "사번" in df.columns else df
    sel = st.selectbox("직원 선택(사번 - 이름)", ["(선택)"] + df.get("표시", pd.Series(dtype=str)).tolist(), index=0, key="adm_pin_pick")
    if sel != "(선택)":
        sabun = sel.split(" - ", 1)[0]
        row   = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]
        st.write(f"사번: **{sabun}** / 이름: **{row.get('이름','')}**")
        pin1 = st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
        pin2 = st.text_input("새 PIN 확인", type="password", key="adm_pin2")
        col = st.columns([1, 1, 2])
        with col[0]: do_save = st.button("PIN 저장/변경", type="primary", use_container_width=True, key="adm_pin_save")
        with col[1]: do_clear = st.button("PIN 비우기", use_container_width=True, key="adm_pin_clear")
        if do_save:
            if not pin1 or not pin2: st.error("PIN을 두 번 모두 입력하세요."); return
            if pin1 != pin2: st.error("PIN 확인이 일치하지 않습니다."); return
            if not pin1.isdigit(): st.error("PIN은 숫자만 입력하세요."); return
            if not _to_bool(row.get("재직여부", False)): st.error("퇴직자는 변경할 수 없습니다."); return
            if "PIN_hash" not in hmap or "PIN_No" not in hmap: st.error(f"'{EMP_SHEET}' 시트에 PIN_hash/PIN_No가 없습니다."); return
            r = _find_row_by_sabun(ws, hmap, sabun)
            if r == 0: st.error("시트에서 사번을 찾지 못했습니다."); return
            hashed = _pin_hash(pin1.strip(), str(sabun))
            _retry(ws.update_cell, r, hmap["PIN_hash"], hashed)
            _retry(ws.update_cell, r, hmap["PIN_No"], pin1.strip())
            st.cache_data.clear(); st.success("PIN 저장 완료", icon="✅")
        if do_clear:
            if "PIN_hash" not in hmap or "PIN_No" not in hmap: st.error(f"'{EMP_SHEET}' 시트에 PIN_hash/PIN_No가 없습니다."); return
            r = _find_row_by_sabun(ws, hmap, sabun)
            if r == 0: st.error("시트에서 사번을 찾지 못했습니다."); return
            _retry(ws.update_cell, r, hmap["PIN_hash"], "")
            _retry(ws.update_cell, r, hmap["PIN_No"], "")
            st.cache_data.clear(); st.success("PIN 초기화 완료", icon="✅")

def tab_admin_eval_items():
    df = read_eval_items_df(only_active=False).copy()
    for c in ["항목ID", "항목", "내용", "비고"]:
        if c in df.columns: df[c]=df[c].astype(str)
    if "순서" in df.columns: df["순서"]=pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)
    if "활성" in df.columns: df["활성"]=df["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))
    st.write(f"현재 등록: **{len(df)}개** (활성 {df[df.get('활성', False)==True].shape[0]}개)")
    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        edit_df=df[["항목ID","항목","순서","활성"]].copy().reset_index(drop=True)
        edited=st.data_editor(
            edit_df,use_container_width=True,height=420,hide_index=True,
            column_order=["항목ID","항목","순서","활성"],
            column_config={
                "항목ID": st.column_config.TextColumn(disabled=True),
                "항목": st.column_config.TextColumn(disabled=True),
                "활성": st.column_config.CheckboxColumn(),
                "순서": st.column_config.NumberColumn(step=1, min_value=0),
            },
        )

        if st.button("순서 일괄 저장", type="primary", use_container_width=True):
            try:
                ws = get_book().worksheet(EVAL_ITEMS_SHEET)
                header = _retry(ws.row_values, 1) or []
                hmap = {n: i+1 for i, n in enumerate(header)}

                # 기본 컬럼 확인
                col_id = hmap.get("항목ID")
                col_ord = hmap.get("순서")
                col_act = hmap.get("활성")
                if not (col_id and col_ord):
                    st.error("'항목ID' 또는 '순서' 헤더가 없습니다."); st.stop()

                # 현재 시트의 항목ID 순서 읽기 (2행부터)
                id_vals = _retry(ws.col_values, col_id)[1:]
                n = len(id_vals)

                # 편집 결과를 dict로 준비 (iid -> 값)
                def _to_bool_local(x):
                    if isinstance(x, bool): return x
                    if x is None: return False
                    s = str(x).strip().lower()
                    return s in ("1","y","yes","true","t","on","checked")

                edited_map_order = { str(r["항목ID"]).strip(): int(r["순서"]) for _, r in edited.iterrows() }
                edited_map_active = { str(r["항목ID"]).strip(): _to_bool_local(r["활성"]) for _, r in edited.iterrows() } if "활성" in edited.columns else {}

                # 범위 문자열 생성 도우미 (주어진 컬럼 인덱스 -> 'A2:A{n+1}' 형태)

                def _col_range(col_idx: int, start_row: int, end_row: int) -> str:
                    letters = _re_local.match(r"([A-Z]+)", gspread.utils.rowcol_to_a1(1, col_idx)).group(1)
                    return f"{letters}{start_row}:{letters}{end_row}"

                # D열(순서), E열(활성) 일괄 덮어쓰기 — 체크박스는 bool로
                if n > 0:
                    order_values = [[ int(edited_map_order.get(iid, 0)) ] for iid in id_vals ]
                    _retry(ws.update, _col_range(col_ord, 2, n+1), order_values, value_input_option="USER_ENTERED")

                    if col_act:
                        active_values = [[ bool(edited_map_active.get(iid, False)) ] for iid in id_vals ]
                        _retry(ws.update, _col_range(col_act, 2, n+1), active_values, value_input_option="USER_ENTERED")

                st.success("업데이트 완료", icon="✅")

            except Exception as e:
                st.exception(e)

    st.divider()
    st.markdown("### 신규 등록 / 수정")
    choices=["(신규)"] + ([f"{r['항목ID']} - {r['항목']}" for _,r in df.iterrows()] if not df.empty else [])
    sel=st.selectbox("대상 선택", choices, index=0, key="adm_eval_pick")

    item_id=None; name=""; desc=""; order=int(df["순서"].max()+1) if ("순서" in df.columns and not df.empty) else 1
    active=True; memo=""
    if sel!="(신규)" and not df.empty:
        iid=sel.split(" - ",1)[0]; row=df.loc[df["항목ID"]==iid]
        if not row.empty:
            row=row.iloc[0]
            item_id=str(row.get("항목ID","")); name=str(row.get("항목","")); desc=str(row.get("내용","")); memo=str(row.get("비고",""))
            try: order=int(row.get("순서",0) or 0)
            except Exception: order=0
            active=(str(row.get("활성","")).strip().lower() in ("true","1","y","yes","t"))

    c1, c2 = st.columns([3,1])
    with c1:
        name = st.text_input("항목명", value=name, key="adm_eval_name")
        desc = st.text_area("설명(문항 내용)", value=desc, height=100, key="adm_eval_desc")
        memo = st.text_input("비고(선택)", value=memo, key="adm_eval_memo")
    with c2:
        order = st.number_input("순서", min_value=0, step=1, value=int(order), key="adm_eval_order")
        active = st.checkbox("활성", value=bool(active), key="adm_eval_active")
        if st.button("저장(신규/수정)", type="primary", use_container_width=True, key="adm_eval_save_v3"):
            if not name.strip():
                st.error("항목명을 입력하세요.")
            else:
                try:
                    ensure_eval_items_sheet()
                    ws = get_book().worksheet(EVAL_ITEMS_SHEET)
                    header = _retry(ws.row_values, 1) or EVAL_ITEM_HEADERS
                    hmap   = {n: i + 1 for i, n in enumerate(header)}
                    if not item_id:
                        col_id = hmap.get("항목ID"); nums=[]
                        if col_id:
                            vals=_retry(ws.col_values, col_id)[1:]
                            for v in vals:
                                s=str(v).strip()
                                if s.startswith("ITM"):
                                    try: nums.append(int(s[3:]))
                                    except Exception: pass
                        new_id=f"ITM{((max(nums)+1) if nums else 1):04d}"
                        rowbuf=[""]*len(header)
                        def put(k,v): c=hmap.get(k); rowbuf[c-1]=v if c else ""
                        put("항목ID",new_id); put("항목",name.strip()); put("내용",desc.strip())
                        put("순서",int(order)); put("활성",bool(active));
                        if "비고" in hmap: put("비고", memo.strip())
                        _retry(ws.append_row, rowbuf, value_input_option="USER_ENTERED")
                        st.cache_data.clear(); st.success(f"저장 완료 (항목ID: {new_id})"); st.rerun()
                    else:
                        col_id=hmap.get("항목ID"); idx=0
                        if col_id:
                            vals=_retry(ws.col_values, col_id)
                            for i,v in enumerate(vals[1:], start=2):
                                if str(v).strip()==str(item_id).strip(): idx=i; break
                        if idx==0: st.error("대상 항목을 찾을 수 없습니다.")
                        else:
                            ws.update_cell(idx, hmap["항목"], name.strip())
                            ws.update_cell(idx, hmap["내용"], desc.strip())
                            ws.update_cell(idx, hmap["순서"], int(order))
                            ws.update_cell(idx, hmap["활성"], bool(active))
                            if "비고" in hmap:
                                gs_enqueue_cell(ws, idx, hmap["비고"], memo.strip(), "USER_ENTERED")
                            gs_flush()
                            st.success("업데이트 완료", icon="✅")

                except Exception as e:
                    st.exception(e)

# ===== [BEGIN ACL_EDIT_RIGHTS] ===============================================
def tab_admin_acl(emp_df: pd.DataFrame):
    """권한 관리: master/admin 편집 허용, 저장은 시트 전체 덮어쓰기."""
    me = st.session_state.get("user", {})

    # 내 역할 목록 로드 (활성만)
    try:
        auth_df = read_auth_df(only_enabled=True)
        rows = auth_df.loc[
            auth_df["사번"].astype(str) == str(st.session_state.get("user", {}).get("사번", "")),
            "역할"
        ].astype(str)
        my_roles = [r.strip().lower() for r in rows if r.strip()]
    except Exception:
        my_roles = []

    # master 또는 admin 이면 편집 가능
    can_edit = any(r in {"master", "admin"} for r in my_roles)

    st.caption(f"내 역할: {', '.join(my_roles) or '없음'} / 편집권한: {'O' if can_edit else 'X'}")

    if not can_edit:
        st.error("admin 이상만 저장할 수 있습니다. (표/저장 비활성화)", icon="🛡️")
# ===== [END ACL_EDIT_RIGHTS] =================================================

    # 직원 룩업
    base = emp_df[["사번","이름","부서1","부서2"]].copy() if not emp_df.empty else pd.DataFrame(columns=["사번","이름","부서1","부서2"])
    base["사번"] = base["사번"].astype(str).str.strip()
    emp_lookup = {str(r["사번"]).strip(): str(r.get("이름","")).strip() for _, r in base.iterrows()}

    # 현재 권한 불러오기
    df = read_auth_df(only_enabled=False).copy()
    for c in AUTH_HEADERS:
        if c not in df.columns:
            df[c] = ""

    # 편집용 보기: 사번 라벨을 "사번 - 이름"으로
    label_by_sabun = {s: (f"{s} - {n}" if n else s) for s, n in emp_lookup.items()}
    df["사번"] = df["사번"].map(lambda s: label_by_sabun.get(str(s).strip(), str(s).strip()))

    # 에디터
    role_options = ["", "master", "admin", "manager", "employee"]
    scope_options = ["", "부서", "개별"]

    colcfg = {
        "사번":     st.column_config.SelectboxColumn("사번", options=list(label_by_sabun.values())),
        "이름":     st.column_config.TextColumn("이름", disabled=True),
        "역할":     st.column_config.SelectboxColumn("역할", options=role_options),
        "범위유형": st.column_config.SelectboxColumn("범위유형", options=scope_options),
        "부서1":    st.column_config.TextColumn("부서1"),
        "부서2":    st.column_config.TextColumn("부서2"),
        "대상사번": st.column_config.TextColumn("대상사번", help="쉼표/공백 구분"),
        "활성":     st.column_config.CheckboxColumn("활성"),
        "비고":     st.column_config.TextColumn("비고"),
    }

    st.info("사번은 '사번 - 이름' 라벨로 선택하세요. 저장 시 실제 사번으로 변환됩니다.", icon="ℹ️")
    edited = st.data_editor(
        df[AUTH_HEADERS],
        use_container_width=True,
        height=520,
        hide_index=True,
        column_config=colcfg,
        disabled=not can_edit,
        num_rows="dynamic",
    )

    # 저장: 전체 덮어쓰기
    if st.button("권한 전체 반영 (시트 저장)", type="primary", use_container_width=True, disabled=not can_edit):
        try:
            # 사번 라벨 → 실제 사번
            inv_label = {v: k for k, v in label_by_sabun.items()}
            save_df = edited.copy()
            save_df["사번"] = save_df["사번"].map(lambda v: inv_label.get(str(v).strip(), str(v).split(" - ",1)[0].strip()))
            # 이름 파생
            save_df["이름"] = save_df["사번"].map(lambda s: emp_lookup.get(str(s).strip(), "")).fillna("").astype(str)

            # 헤더/값 준비
            ws = get_book().worksheet(AUTH_SHEET)
            _retry(ws.update, "1:1", [AUTH_HEADERS], value_input_option="USER_ENTERED")

            values = []
            for _, r in save_df[AUTH_HEADERS].iterrows():
                values.append([r.get(c, "") for c in AUTH_HEADERS])

            if values:
                _retry(ws.update, f"2:{len(values)+1}", values, value_input_option="USER_ENTERED")
            # 값이 0개면 헤더 아래를 정리
            else:
                try:
                    ws.resize(rows=2)
                except Exception:
                    pass

            try:
                st.cache_data.clear()
            except Exception:
                pass
            st.success("권한 시트 저장 완료", icon="✅")
        except Exception as e:
            st.exception(e)

def tab_help():
    st.markdown("""
        - 좌측에서 `검색(사번/이름)` 후 **Enter** → 첫 번째 결과가 자동으로 선택됩니다.
    - 대상선택(드롭다운박스)로 직원을 선택해도 됩니다.
    - 선택된 직원은 우측 모든 탭과 동기화됩니다.
    - 권한(ACL)에 따라 보이는 직원 범위가 달라집니다. 관리자는 전 직원이 보입니다.
    - 로그인: `사번` 입력 후 **Enter** → `PIN` 포커스 / `PIN` 입력 후 **Enter** → 로그인.
    - 인사평가: 평가 항목은 관리자 메뉴의 **평가 항목 관리**에서 활성/순서를 조정합니다.
    - 직무기술서/직무능력평가: 동기화된 대상자를 기준으로 편집·제출합니다.
    - PIN/평가항목/권한관리: 관리자 탭에서 처리합니다.
    - 구글시트 구조
        - 직원: `직원` 시트
        - 권한: `권한` 시트 (역할=admin/manager, 범위유형: 공란=전체 · 부서 · 개별)
        - 평가 항목: `평가_항목` 시트
        - 인사평가: `인사평가` 시트
        - 직무기술서: `직무기술서` 시트
        - 직무기술서(부서장 승인): `직무기술서_승인` 시트
        - 직무능력평가: `직무능력평가` 시트
    """)

# ═════════════════════════════════════════════════════════════════════════════
# Main App
# ═════════════════════════════════════════════════════════════════════════════
def main():
    emp_df = read_emp_df()
    st.session_state["emp_df"] = emp_df.copy()

    if not _session_valid():
        st.markdown(f"<div class='app-title-hero'>{APP_TITLE}</div>", unsafe_allow_html=True)
        show_login(emp_df)
        return

    require_login(emp_df)

    left, right = st.columns([1.35, 3.65], gap="large")

    with left:
        st.markdown(f"<div class='app-title-hero'>{APP_TITLE}</div>", unsafe_allow_html=True)
        u = st.session_state.get("user", {})
        st.caption(f"DB연결 {kst_now_str()}")
        st.markdown(f"- 사용자: **{u.get('이름','')} ({u.get('사번','')})**")

        # 상단 컨트롤: [로그아웃] | [동기화]
        c1, c2 = st.columns([1, 1], gap="small")
        with c1:
            if st.button("로그아웃", key="btn_logout", use_container_width=True):
                logout()
        with c2:
            clicked_sync = st.button("🔄 동기화", key="sync_left", use_container_width=True, help="캐시를 비우고 구글시트에서 다시 불러옵니다.")
            if _debounce_passed("__sync_left", 1.0, clicked_sync):
                force_sync(min_interval=25)

        # 좌측 메뉴
        render_staff_picker_left(emp_df)

    with right:
        tabs = st.tabs(["인사평가","직무기술서","직무능력평가","관리자","도움말"])

        with tabs[0]:
            tab_eval(emp_df)

        with tabs[1]:
            tab_job_desc(emp_df)

        with tabs[2]:
            tab_competency(emp_df)

        with tabs[3]:
            me = str(st.session_state.get("user", {}).get("사번", ""))
            if not is_admin(me):
                st.warning("관리자 전용 메뉴입니다.", icon="🔒")
            else:
                # 관리자 기능(시트 기반)만 유지
                a1, a2, a3, a4 = st.tabs(["직원","PIN 관리","평가 항목 관리","권한 관리"])
                with a1:
                    tab_staff_admin(emp_df)
                with a2:
                    tab_admin_pin(emp_df)
                with a3:
                    tab_admin_eval_items()
                with a4:
                    tab_admin_acl(emp_df)

        with tabs[4]:
            tab_help()

if __name__ == "__main__":
    main()

@st.cache_data(ttl=120, show_spinner=False)
def get_jd_approval_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    Returns mapping {(사번, 버전) -> (상태, 승인시각)} for the given year.
    - 빈 시트/헤더 누락/타입 캐스팅 실패에도 안전
    - 승인시각은 가급적 datetime으로 파싱하여 최신 정렬
    - 가능한 경우 read_jd_approval_df(_rev) 재사용(캐시 일관)
    """
    sheet_name = globals().get("JD_APPROVAL_SHEET", "직무기술서_승인")
    headers_default = ["연도","사번","이름","버전","승인자사번","승인자이름","상태","승인시각","비고"]
    headers = list(globals().get("JD_APPROVAL_HEADERS", headers_default))

    # 1) 시트 보장
    try:
        ensure_fn = globals().get("ensure_jd_approval_sheet")
        if callable(ensure_fn):
            ensure_fn()
    except Exception:
        pass

    # 2) 데이터 로드: read_jd_approval_df 우선
    df = None
    try:
        _read = globals().get("read_jd_approval_df")
        if callable(_read):
            df = _read(_rev)
    except Exception:
        df = None

    # 2-보: 직접 로드 (ws → records) 폴백
    if df is None:
        try:
            _ws_func = globals().get("_ws")
            _get_records = globals().get("_ws_get_all_records")
            if callable(_ws_func) and callable(_get_records):
                ws = _ws_func(sheet_name)
                raw = _get_records(ws)
                df = pd.DataFrame(raw)
        except Exception:
            df = None

    # 2-폴백-2: 또 다른 헬퍼가 있으면 사용
    if df is None:
        try:
            get_df = globals().get("get_sheet_as_df")
            if callable(get_df):
                df = get_df(sheet_name)
        except Exception:
            df = None

    if df is None or df.empty:
        df = pd.DataFrame(columns=headers)

    # 3) 헤더 보강
    for c in headers:
        if c not in df.columns:
            df[c] = ""

    # 4) 타입/정규화
    df["연도"] = pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int)
    if "버전" in df.columns:
        df["버전"] = pd.to_numeric(df["버전"], errors="coerce").fillna(0).astype(int)
    else:
        df["버전"] = 0

    for c in ["사번","상태","승인시각"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
        else:
            df[c] = ""

    # 승인시각 파싱 컬럼(정렬/최신 선택용)
    try:
        df["_승인_dt"] = pd.to_datetime(df["승인시각"], errors="coerce")
    except Exception:
        df["_승인_dt"] = pd.NaT

    # 5) 연도 필터
    try:
        df = df[df["연도"] == int(_year)]
    except Exception:
        df = df.iloc[0:0]

    # 6) 결과 구성: (사번,버전)별 최신 승인시각 1건만 남김
    out = {}
    if not df.empty:
        # 사번/버전 그룹으로 최신 승인시각 선택
        # _승인_dt가 NaT인 경우를 대비해 보조키로 원본 문자열도 고려
        df = df.copy()
        df["_승인_key"] = df["_승인_dt"].fillna(pd.Timestamp.min)

        # 최신 승인시각으로 정렬 후 drop_duplicates(keep='last')도 가능하지만,
        # 그룹집계가 좀 더 명시적
        idx = (
            df.sort_values(["사번","버전","_승인_key"], ascending=[True, True, True])
              .groupby(["사번","버전"], as_index=False).tail(1).index
        )
        sub = df.loc[idx]

        # 사번,버전 오름차순으로 정렬(보기 일관성)
        sub = sub.sort_values(["사번","버전"], kind="stable").reset_index(drop=True)

        for _, rr in sub.iterrows():
            k = (str(rr.get("사번","")), int(rr.get("버전",0)))
            out[k] = (str(rr.get("상태","")), str(rr.get("승인시각","")))

    return out

# ===== Batch write helpers (robust) =====

# Google Sheets API practical limits
_GS_MAX_ITEMS_PER_REQUEST = 500     # 안전권장: 한 번에 500개 range 이하
_GS_MAX_TOTAL_CELLS_HINT   = 50000  # 너무 큰 페이로드인 경우 방어용 힌트

def _gs_queue_init():
    if "gs_queue" not in st.session_state:
        st.session_state.gs_queue = []

def gs_enqueue_range(ws, range_a1, values_2d, value_input_option="USER_ENTERED"):
    """
    하나의 A1 범위와 2차원 values를 큐에 적재.
    - ws: gspread Worksheet
    - range_a1: "A1:B9" 혹은 "Sheet1!A1:B9" (시트명 없으면 자동 부착)
    - values_2d: [[...], [...], ...]
    """
    if ws is None or range_a1 is None:
        return
    _gs_queue_init()
    title = getattr(ws, "title", None) or ""
    rng = range_a1 if "!" in str(range_a1) else f"{title}!{range_a1}"
    # None → "" (Sheets values API는 None을 허용하지 않음)
    safe_vals = [[("" if v is None else v) for v in (row or [])] for row in (values_2d or [[]])]
    st.session_state.gs_queue.append({
        "range": rng,
        "values": safe_vals,
        "value_input_option": value_input_option or "USER_ENTERED",
    })

def gs_enqueue_cell(ws, row, col, value, value_input_option="USER_ENTERED"):
    """
    단일 셀 쓰기를 큐에 적재.
    """
    if ws is None:
        return
    _gs_queue_init()
    a1 = _rowcol_to_a1(int(row), int(col))
    title = getattr(ws, "title", None) or ""
    rng = f"{title}!{a1}"
    st.session_state.gs_queue.append({
        "range": rng,
        "values": [[("" if value is None else value)]],
        "value_input_option": value_input_option or "USER_ENTERED",
    })

def _chunked(iterable, n):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def gs_flush():
    """
    큐에 쌓인 업데이트를 valueInputOption 별로 그룹핑하여
    values_batch_update → (실패 시) batch_update 순으로 시도.
    - 큰 페이로드는 500개 단위로 청크 분할
    - 성공/실패와 무관하게 마지막엔 큐를 비움(중복 전송 방지)
    """
    data = st.session_state.get("gs_queue") or []
    if not data:
        return

    # 그룹핑: value_input_option 별
    grouped = {}
    total_cells = 0
    for item in data:
        mode = item.get("value_input_option", "USER_ENTERED")
        rng  = item.get("range")
        vals = item.get("values") or [[]]
        grouped.setdefault(mode, []).append({"range": rng, "values": vals})
        # 셀 개수 대략 추산(방어용)
        try:
            total_cells += sum(len(r) for r in vals)
        except Exception:
            pass

    # 너무 큰 페이로드 경고(실행은 계속)
    if total_cells > _GS_MAX_TOTAL_CELLS_HINT:
        try:
            st.warning(f"대량 업데이트 감지: 총 {total_cells:,} 셀. 일부 요청을 분할 전송합니다.", icon="⚠️")
        except Exception:
            pass

    sh = get_book()  # gspread Spreadsheet
    # 모드별로 전송
    try:
        for mode, payload in grouped.items():
            # 500개 단위로 분할 전송
            for chunk in _chunked(payload, _GS_MAX_ITEMS_PER_REQUEST):
                body = {"valueInputOption": mode, "data": chunk}
                try:
                    # 우선 values_batch_update 사용(더 가벼움)
                    sh.values_batch_update(body)
                except Exception:
                    # 폴백: batch_update
                    try:
                        sh.batch_update(body)
                    except Exception as e:
                        # 일부 실패 시 즉시 알리고 예외 전파
                        try:
                            st.warning("일부 값 저장에 실패했습니다. 동기화 후 다시 시도해 주세요.", icon="⚠️")
                        except Exception:
                            pass
                        raise e
    finally:
        # 성공/실패와 상관없이 큐 비움(중복전송 방지)
        st.session_state.gs_queue = []
# ===== End helpers =====

# --- Compatibility shim ----------------------------------------------
def _kst_now_str_safe():
    """Backwards-compat helper; delegates to kst_now_str()."""
    return kst_now_str()

# --- Compatibility shim ----------------------------------------------
def _ws_batch_row_v2(ws, idx: int, hmap: dict, kv: dict):
    """Alias to _ws_batch_row for backward compatibility."""
    return _ws_batch_row(ws, idx, hmap, kv)
