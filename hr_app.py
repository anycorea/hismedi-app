# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Google Sheets 연동)
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import re, hashlib, random, time, secrets as pysecrets
from datetime import datetime, timedelta
from typing import Any
import pandas as pd, streamlit as st

# KST
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# gspread
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread==6.1.2", "google-auth==2.31.0"])
    import gspread
    from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# ── App Config ────────────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

# ▼ 도움말 패널(st.help) 전역 비활성화 — 상단 ‘No docs available’ 예방
if not getattr(st, "_help_disabled", False):
    def _noop_help(*args, **kwargs): return None
    st.help = _noop_help
    st._help_disabled = True

# ▼ 전역 스타일
st.markdown(
    """
    <style>
      .block-container { padding-top: 1.35rem !important; }
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .grid-head{ font-size:.9rem; color:#6b7280; margin:.2rem 0 .5rem; }
      .app-title{ font-weight:800; font-size:1.28rem; margin: .2rem 0 .6rem; }
      @media (min-width:1280px){ .app-title{ font-size: 1.34rem; } }
      section[data-testid="stHelp"], div[data-testid="stHelp"]{ display:none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Utils ─────────────────────────────────────────────────────────────────────
def kst_now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t")
def _normalize_private_key(raw: str) -> str:
    if not raw: return raw
    return raw.replace("\\n","\n") if "\\n" in raw and "BEGIN PRIVATE KEY" in raw else raw
def _pin_hash(pin: str, sabun: str) -> str:
    plain = f"{str(sabun).strip()}:{str(pin).strip()}"
    return hashlib.sha256(plain.encode()).hexdigest()

# ── Google API Retry Helper ───────────────────────────────────────────────────
API_BACKOFF_SEC = [0.0, 0.8, 1.6, 3.2, 6.4, 9.6]  # 더 길고 완만한 백오프 (429 대비)
def _retry_call(fn, *args, **kwargs):
    last = None
    for backoff in API_BACKOFF_SEC:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            last = e
            time.sleep(backoff + random.uniform(0, 0.25))
    if last: raise last
    return fn(*args, **kwargs)

# ── Google Auth / Sheets ──────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_gspread_client():
    svc = dict(st.secrets["gcp_service_account"])
    svc["private_key"] = _normalize_private_key(svc.get("private_key",""))
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_workbook():
    return get_gspread_client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

# ── gspread read-throttle helpers (cache) ─────────────────────────────────────
_WS_CACHE: dict[str, tuple[float, Any]] = {}
_HDR_CACHE: dict[str, tuple[float, list[str], dict[str, int]]] = {}
_WS_TTL  = 120
_HDR_TTL = 120

def _ws_cached(title: str):
    now = time.time()
    hit = _WS_CACHE.get(title)
    if hit and (now - hit[0] < _WS_TTL): return hit[1]
    ws = _retry_call(get_workbook().worksheet, title)
    _WS_CACHE[title] = (now, ws)
    return ws

def _sheet_header_cached(ws, cache_key: str) -> tuple[list[str], dict[str, int]]:
    now = time.time()
    hit = _HDR_CACHE.get(cache_key)
    if hit and (now - hit[0] < _HDR_TTL): return hit[1], hit[2]
    header = _retry_call(ws.row_values, 1) or []
    hmap = {n: i + 1 for i, n in enumerate(header)} if header else {}
    _HDR_CACHE[cache_key] = (now, header, hmap)
    return header, hmap

def _ws_get_all_records(ws):
    try: return _retry_call(ws.get_all_records, numericise_ignore=["all"])
    except TypeError: return _retry_call(ws.get_all_records)

def _batch_update_row(ws, row_idx: int, hmap: dict, kv: dict):
    upd = []
    for k, v in kv.items():
        c = hmap.get(k)
        if c:
            a1 = gspread.utils.rowcol_to_a1(row_idx, c)
            upd.append({"range": a1, "values": [[v]]})
    if upd: _retry_call(ws.batch_update, upd)

# ── Non-critical error silencer ───────────────────────────────────────────────
SILENT_NONCRITICAL_ERRORS = True
def _silent_df_exception(e: Exception, where: str, empty_columns: list[str] | None = None) -> pd.DataFrame:
    if not SILENT_NONCRITICAL_ERRORS: st.error(f"{where}: {e}")
    return pd.DataFrame(columns=empty_columns or [])

# ── Sheet Helpers ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=90, show_spinner=False)
def read_sheet_df(sheet_name: str, *, silent: bool = False) -> pd.DataFrame:
    try:
        ws = _ws_cached(sheet_name)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        if sheet_name == EMP_SHEET and "emp_df_cache" in st.session_state:
            if not silent: st.caption("※ 직원 시트 실시간 로딩 실패 → 캐시 사용")
            df = st.session_state["emp_df_cache"].copy()
        else:
            raise

    if "관리자여부" in df.columns: df["관리자여부"] = df["관리자여부"].map(_to_bool)
    if "재직여부" in df.columns: df["재직여부"] = df["재직여부"].map(_to_bool)
    for c in ["입사일", "퇴사일"]:
        if c in df.columns: df[c] = df[c].astype(str)
    for c in ["사번", "이름", "PIN_hash"]:
        if c not in df.columns: df[c] = ""
    if "사번" in df.columns: df["사번"] = df["사번"].astype(str)
    return df

def _get_ws_and_headers(sheet_name: str):
    ws = _ws_cached(sheet_name)
    header, hmap = _sheet_header_cached(ws, sheet_name)
    if not header: raise RuntimeError(f"'{sheet_name}' 헤더(1행) 없음")
    return ws, header, hmap

def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    c = hmap.get("사번")
    if not c: return 0
    for i, v in enumerate(_retry_call(ws.col_values, c)[1:], start=2):
        if str(v).strip() == str(sabun).strip(): return i
    return 0

def _update_cell(ws, row, col, value): _retry_call(ws.update_cell, row, col, value)

def _hide_doctors(df: pd.DataFrame) -> pd.DataFrame:
    """
    (의료진 포함 버전)
    이전엔 '직무'에 '의사'가 포함된 행을 숨겼는데,
    지금은 아무도 숨기지 않고 원본을 그대로 반환합니다.
    """
    return df

def _build_name_map(df: pd.DataFrame) -> dict:
    if df.empty: return {}
    return {str(r["사번"]): str(r.get("이름", "")) for _, r in df.iterrows()}

# === Login Enter Key Binder (사번 Enter→PIN, PIN Enter→로그인) ==============
import streamlit.components.v1 as components

def _inject_login_keybinder():
    """사번 Enter→PIN 포커스, PIN Enter→'로그인' 버튼 클릭
    (입력값 커밋 후 클릭하도록 보강: input/change/blur + 지연 클릭)
    """
    import streamlit.components.v1 as components
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
            try{
              el.dispatchEvent(new Event('input',  {bubbles:true}));
              el.dispatchEvent(new Event('change', {bubbles:true}));
              el.blur();
            }catch(e){}
          }
          function bind(){
            const sab = byLabelStartsWith('사번');
            const pin = byLabelStartsWith('PIN');
            const btn = findLoginBtn();
            if(!sab || !pin) return false;

            if(!sab._bound){
              sab._bound = true;
              sab.addEventListener('keydown', function(e){
                if(e.key === 'Enter'){
                  e.preventDefault();
                  commit(sab);
                  // 커밋 후 다음 필드로 포커스
                  setTimeout(function(){
                    try{ pin.focus(); pin.select(); }catch(_){}
                  }, 0);
                }
              });
            }

            if(!pin._bound){
              pin._bound = true;
              pin.addEventListener('keydown', function(e){
                if(e.key === 'Enter'){
                  e.preventDefault();
                  // 두 필드 모두 커밋 후 약간 지연하여 버튼 클릭
                  commit(pin);
                  commit(sab);
                  const b = findLoginBtn();
                  setTimeout(function(){
                    try{ if(b){ b.click(); } }catch(_){}
                  }, 60); // 동기화 여유
                }
              });
            }
            return true;
          }

          // 초기 바인딩 + 재렌더 대비(짧은 기간 관찰)
          bind();
          const mo = new MutationObserver(() => { bind(); });
          mo.observe(window.parent.document.body, { childList:true, subtree:true });
          setTimeout(() => { try{ mo.disconnect(); }catch(e){} }, 8000);
        })();
        </script>
        """,
        height=0, width=0
    )
# ============================================================================

# ── Session/Auth ──────────────────────────────────────────────────────────────
SESSION_TTL_MIN = 30
def _session_valid() -> bool:
    exp = st.session_state.get("auth_expires_at"); authed = st.session_state.get("authed", False)
    return bool(authed and exp and time.time() < exp)

def _start_session(user_info: dict):
    st.session_state["authed"]=True; st.session_state["user"]=user_info
    st.session_state["auth_expires_at"]=time.time()+SESSION_TTL_MIN*60

def logout():
    for k in ("authed","user","auth_expires_at"): st.session_state.pop(k, None)
    st.cache_data.clear(); st.rerun()

def show_login_form(emp_df: pd.DataFrame):
    st.header("로그인")

    sabun = st.text_input("사번", placeholder="예) 123456", key="login_sabun")
    pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")

    col = st.columns([1, 3])
    with col[0]:
        do_login = st.button("로그인", use_container_width=True, type="primary", key="login_btn")

    # ⬇️ 엔터키 동작(사번→PIN, PIN→로그인) 주입
    _inject_login_keybinder()

    # ── 서버 검증/세션 시작 ───────────────────────────────────────────
    if not do_login:
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

    stored = str(r.get("PIN_hash","")).strip().lower()
    entered_plain  = _sha256_hex(pin.strip())
    entered_salted = _pin_hash(pin.strip(), str(r.get("사번","")))
    if stored not in (entered_plain, entered_salted):
        st.error("PIN이 올바르지 않습니다.")
        st.stop()

    _start_session({
        "사번": str(r.get("사번","")),
        "이름": str(r.get("이름","")),
        "관리자여부": False,
    })
    st.success(f"{str(r.get('이름',''))}님 환영합니다!")
    st.rerun()

def require_login(emp_df: pd.DataFrame):
    if not _session_valid():
        for k in ("authed","user","auth_expires_at"): st.session_state.pop(k, None)
        show_login_form(emp_df); st.stop()

# ── ACL(권한) ─────────────────────────────────────────────────────────────────
AUTH_SHEET="권한"
AUTH_HEADERS=["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]
SEED_ADMINS=[
    {"사번":"113001","이름":"병원장","역할":"admin","범위유형":"","부서1":"","부서2":"","대상사번":"","활성":True,"비고":"seed"},
    {"사번":"524007","이름":"행정원장","역할":"admin","범위유형":"","부서1":"","부서2":"","대상사번":"","활성":True,"비고":"seed"},
    {"사번":"524003","이름":"이영하","역할":"admin","범위유형":"","부서1":"행정부","부서2":"총무팀","대상사번":"","활성":True,"비고":"seed"},
]

@st.cache_data(ttl=3600, show_spinner=False)
def _auth_seed_once_cached() -> bool:
    """
    SEED_ADMINS 주입은 무겁기 때문에 1시간에 한 번만 시도합니다.
    헤더는 ensure_auth_sheet()에서 즉시 보장합니다.
    """
    wb = get_workbook()
    try:
        ws = wb.worksheet(AUTH_SHEET)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=AUTH_SHEET, rows=1000, cols=20)
        _retry_call(ws.update, "1:1", [AUTH_HEADERS])
    header = _retry_call(ws.row_values, 1) or []
    need = [h for h in AUTH_HEADERS if h not in header]
    if need:
        _retry_call(ws.update, "1:1", [header + need])
        header = _retry_call(ws.row_values, 1) or []

    # ↓ 이 부분이 상대적으로 무겁다: 전체 레코드 스캔
    vals = _ws_get_all_records(ws)
    cur_admins = {str(r.get("사번","")).strip() for r in vals if str(r.get("역할","")).strip()=="admin"}
    add = [r for r in SEED_ADMINS if r["사번"] not in cur_admins]
    if add:
        rows = [[r.get(h, "") for h in header] for r in add]
        _retry_call(ws.append_rows, rows, value_input_option="USER_ENTERED")
    return True

def ensure_auth_sheet():
    """
    권한시트의 '존재'와 '헤더'를 가볍게 보장합니다.
    무거운 시드 주입은 _auth_seed_once_cached()에서 캐시로 제어합니다.
    """
    wb = get_workbook()
    try:
        ws = wb.worksheet(AUTH_SHEET)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=AUTH_SHEET, rows=1000, cols=20)
        _retry_call(ws.update, "1:1", [AUTH_HEADERS])
        # 최초 생성 시에만 시드도 함께
        _auth_seed_once_cached()
        return ws

    header = _retry_call(ws.row_values, 1) or []
    need = [h for h in AUTH_HEADERS if h not in header]
    if need:
        _retry_call(ws.update, "1:1", [header + need])
    # 시드는 캐시에 의해 1시간에 1회만
    _auth_seed_once_cached()
    return ws

@st.cache_data(ttl=300, show_spinner=False)
def ensure_auth_sheet_once() -> bool:
    """
    5분 캐시로 ensure_auth_sheet 호출 빈도를 낮춥니다.
    실패 시 False 반환(예외 전파 없음) → 호출측에서 메시지 관리.
    """
    try:
        ensure_auth_sheet()
        return True
    except Exception:
        return False

@st.cache_data(ttl=60, show_spinner=False)
def read_auth_df() -> pd.DataFrame:
    try:
        # 과거: ensure_auth_sheet() 직접 호출 → 빈번한 429
        # 변경: 캐시된 보장 호출
        _ = ensure_auth_sheet_once()
        ws = _ws_cached(AUTH_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        return _silent_df_exception(e, "권한 시트 읽기", AUTH_HEADERS)

    if df.empty:
        return pd.DataFrame(columns=AUTH_HEADERS)

    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]:
        if c not in df.columns:
            df[c] = ""

    # 타입/정리
    df["사번"] = df["사번"].astype(str)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    return df

# === 평가자(evaluator) 유틸 (단건 upsert/remove) =============================
def _auth__get_ws_hmap():
    ws = ensure_auth_sheet()
    header = _retry_call(ws.row_values, 1) or AUTH_HEADERS
    hmap = {n: i + 1 for i, n in enumerate(header)}
    return ws, header, hmap

def _auth__find_rows(ws, hmap, **filters) -> list[int]:
    values = _retry_call(ws.get_all_values)
    rows = []
    for i in range(2, len(values) + 1):
        row = values[i - 1]
        ok = True
        for k, v in filters.items():
            c = hmap.get(k)
            if not c: ok = False; break
            if str(row[c - 1]).strip() != str(v).strip(): ok = False; break
        if ok: rows.append(i)
    return rows

def _auth_upsert_admin(sabun: str, name: str, active: bool = True, memo: str = "grid"):
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "admin"})
    if rows:
        c = hmap.get("활성")
        for r in rows:
            if c: _retry_call(ws.update_cell, r, c, bool(active))
        return
    buf = [""] * len(header)
    def put(k, v): c = hmap.get(k); buf[c - 1] = v if c else ""
    put("사번", sabun); put("이름", name); put("역할", "admin")
    put("범위유형",""); put("부서1",""); put("부서2",""); put("대상사번","")
    put("활성", bool(active)); put("비고", memo)
    _retry_call(ws.append_row, buf, value_input_option="USER_ENTERED")

def _auth_remove_admin(sabun: str):
    if sabun in {a["사번"] for a in SEED_ADMINS}: return
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "admin"})
    for r in sorted(rows, reverse=True): _retry_call(ws.delete_rows, r)

def _auth_upsert_dept(sabun: str, name: str, dept1: str, dept2: str = "", active: bool = True, memo: str = "grid"):
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "manager", "범위유형": "부서", "부서1": dept1, "부서2": (dept2 or "")})
    if rows:
        c = hmap.get("활성")
        for r in rows:
            if c: _retry_call(ws.update_cell, r, c, bool(active))
        return
    buf = [""] * len(header)
    def put(k, v): c = hmap.get(k); buf[c - 1] = v if c else ""
    put("사번", sabun); put("이름", name); put("역할", "manager")
    put("범위유형", "부서"); put("부서1", dept1); put("부서2", (dept2 or ""))
    put("대상사번",""); put("활성", bool(active)); put("비고", memo)
    _retry_call(ws.append_row, buf, value_input_option="USER_ENTERED")

def _auth_remove_dept(sabun: str, dept1: str, dept2: str = ""):
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "manager", "범위유형": "부서", "부서1": dept1, "부서2": (dept2 or "")})
    for r in sorted(rows, reverse=True): _retry_call(ws.delete_rows, r)

def _auth_upsert_eval(sabun: str, name: str, dept1: str, dept2: str = "", active: bool = True, memo: str = "grid"):
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "evaluator", "범위유형": "부서", "부서1": dept1, "부서2": (dept2 or "")})
    if rows:
        c = hmap.get("활성")
        for r in rows:
            if c: _retry_call(ws.update_cell, r, c, bool(active))
        return
    buf = [""] * len(header)
    def put(k, v): c = hmap.get(k); buf[c - 1] = v if c else ""
    put("사번", sabun); put("이름", name); put("역할", "evaluator")
    put("범위유형", "부서"); put("부서1", dept1); put("부서2", (dept2 or ""))
    put("대상사번",""); put("활성", bool(active)); put("비고", memo)
    _retry_call(ws.append_row, buf, value_input_option="USER_ENTERED")

def _auth_remove_eval(sabun: str, dept1: str, dept2: str = ""):
    ws, header, hmap = _auth__get_ws_hmap()
    rows = _auth__find_rows(ws, hmap, **{"사번": sabun, "역할": "evaluator", "범위유형": "부서", "부서1": dept1, "부서2": (dept2 or "")})
    for r in sorted(rows, reverse=True): _retry_call(ws.delete_rows, r)

def is_admin(sabun: str) -> bool:
    s = str(sabun).strip()
    if s in {a["사번"] for a in SEED_ADMINS}: return True
    try: df = read_auth_df()
    except Exception: return False
    if df.empty: return False
    q = df[(df["사번"].astype(str) == s) & (df["역할"].str.lower() == "admin") & (df["활성"] == True)]
    return not q.empty

def _infer_implied_scopes(emp_df:pd.DataFrame,sabun:str)->list[dict]:
    out=[]; me=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    if me.empty: return out
    r=me.iloc[0]; grade=str(r.get("직급","")); d1=str(r.get("부서1","")); d2=str(r.get("부서2","")); name=str(r.get("이름",""))
    if "부장" in grade: out.append({"사번":sabun,"이름":name,"역할":"manager","범위유형":"부서","부서1":d1,"부서2":"","대상사번":"","활성":True,"비고":"implied:부장"})
    if "팀장" in grade: out.append({"사번":sabun,"이름":name,"역할":"manager","범위유형":"부서","부서1":d1,"부서2":d2,"대상사번":"","활성":True,"비고":"implied:팀장"})
    return out

def get_allowed_sabuns(emp_df:pd.DataFrame,sabun:str,include_self:bool=True)->set[str]:
    sabun=str(sabun)
    if is_admin(sabun): return set(emp_df["사번"].astype(str).tolist())
    allowed=set([sabun]) if include_self else set()
    df=read_auth_df()
    if not df.empty:
        mine=df[(df["사번"].astype(str)==sabun)&(df["활성"]==True)]
        for _,r in mine.iterrows():
            t=str(r.get("범위유형","")).strip()
            if t=="부서":
                d1=str(r.get("부서1","")).strip(); d2=str(r.get("부서2","")).strip()
                tgt=emp_df.copy()
                if d1: tgt=tgt[tgt["부서1"].astype(str)==d1]
                if d2: tgt=tgt[tgt["부서2"].astype(str)==d2]
                allowed.update(tgt["사번"].astype(str).tolist())
            elif t=="개별":
                parts=[p for p in re.split(r"[,\s]+", str(r.get("대상사번","")).strip()) if p]
                allowed.update(parts)
    for r in _infer_implied_scopes(emp_df, sabun):
        if r["범위유형"]=="부서":
            d1=r["부서1"]; d2=r["부서2"]
            tgt=emp_df.copy()
            if d1: tgt=tgt[tgt["부서1"].astype(str)==d1]
            if d2: tgt=tgt[tgt["부서2"].astype(str)==d2]
            allowed.update(tgt["사번"].astype(str).tolist())
    return allowed

def is_manager(emp_df:pd.DataFrame,sabun:str)->bool:
    return len(get_allowed_sabuns(emp_df,sabun,include_self=False))>0

def get_evaluable_targets(emp_df: pd.DataFrame, evaluator_sabun: str) -> set[str]:
    df = read_auth_df()
    if df.empty: return set()
    mine = df[(df["사번"].astype(str) == str(evaluator_sabun)) & (df["역할"].str.lower() == "evaluator") & (df["범위유형"] == "부서") & (df["활성"] == True)]
    my_scopes_team = {(str(r["부서1"]).strip(), str(r["부서2"]).strip()) for _, r in mine.iterrows()}
    my_scopes_dept = {d1 for (d1, d2) in my_scopes_team if d2 == ""}
    all_team_evals = df[(df["역할"].str.lower() == "evaluator") & (df["범위유형"] == "부서") & (df["활성"] == True)]
    team_has_evaluator = {(str(r["부서1"]).strip(), str(r["부서2"]).strip()) for _, r in all_team_evals.iterrows() if str(r["부서2"]).strip()}
    allowed = set()
    for _, row in emp_df.iterrows():
        sab = str(row.get("사번", "")).strip()
        d1  = str(row.get("부서1", "")).strip()
        d2  = str(row.get("부서2", "")).strip()
        if (d1, d2) in team_has_evaluator:
            if (d1, d2) in my_scopes_team: allowed.add(sab)
            continue
        if d1 in my_scopes_dept: allowed.add(sab)
    return allowed

# ── Settings (직무기술서 기본값) ─────────────────────────────────────────────
SETTINGS_SHEET = "설정"
SETTINGS_HEADERS = ["키", "값", "메모", "수정시각", "수정자사번", "수정자이름", "활성"]

def ensure_settings_sheet():
    wb = get_workbook()
    try:
        ws = wb.worksheet(SETTINGS_SHEET)
        header = _retry_call(ws.row_values, 1) or []
        need = [h for h in SETTINGS_HEADERS if h not in header]
        if need:
            _retry_call(ws.update, "1:1", [header + need])
        return ws
    except WorksheetNotFound:
        ws = _retry_call(wb.add_worksheet, title=SETTINGS_SHEET, rows=200, cols=10)
        _retry_call(ws.update, "A1", [SETTINGS_HEADERS])
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_settings_df() -> pd.DataFrame:
    try:
        ensure_settings_sheet()
        ws = _ws_cached(SETTINGS_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        return _silent_df_exception(e, "설정 시트 읽기", SETTINGS_HEADERS)
    if df.empty: return pd.DataFrame(columns=SETTINGS_HEADERS)
    if "활성" in df.columns: df["활성"] = df["활성"].map(_to_bool)
    for c in ["키", "값", "메모", "수정자사번", "수정자이름"]:
        if c in df.columns: df[c] = df[c].astype(str)
    return df

def get_setting(key: str, default: str = "") -> str:
    try: df = read_settings_df()
    except Exception: return default
    if df.empty or "키" not in df.columns: return default
    q = df[df["키"].astype(str) == str(key)]
    if "활성" in df.columns: q = q[q["활성"] == True]
    if q.empty: return default
    return str(q.iloc[-1].get("값", default))

def set_setting(key: str, value: str, memo: str, editor_sabun: str, editor_name: str):
    try:
        ws = ensure_settings_sheet()
        header = _retry_call(ws.row_values, 1) or SETTINGS_HEADERS
        hmap = {n: i + 1 for i, n in enumerate(header)}
        col_key = hmap.get("키"); row_idx = 0
        if col_key:
            vals = _retry_call(ws.col_values, col_key)
            for i, v in enumerate(vals[1:], start=2):
                if str(v).strip() == str(key).strip(): row_idx = i; break
        now = kst_now_str()
        if row_idx == 0:
            row = [""] * len(header)
            def put(k, v): c = hmap.get(k); row[c - 1] = v if c else ""
            put("키", key); put("값", value); put("메모", memo); put("수정시각", now)
            put("수정자사번", editor_sabun); put("수정자이름", editor_name); put("활성", True)
            _retry_call(ws.append_row, row, value_input_option="USER_ENTERED")
        else:
            updates = []
            for k, v in [("값", value), ("메모", memo), ("수정시각", now), ("수정자사번", editor_sabun), ("수정자이름", editor_name), ("활성", True)]:
                c = hmap.get(k)
                if c:
                    a1 = gspread.utils.rowcol_to_a1(row_idx, c)
                    updates.append({"range": a1, "values": [[v]]})
            if updates: _retry_call(ws.batch_update, updates)
        st.cache_data.clear()
    except Exception:
        pass

# ── Status Line ───────────────────────────────────────────────────────────────
def render_status_line():
    try:
        _ = get_workbook()
        st.caption(f"DB연결 {kst_now_str()}")
    except Exception as e:
        st.error(f"DB 연결 실패: {e}", icon="🛑")

직원(Employee)


# ======================================================================
# 📌 권한관리(Admin / ACL & Admin Tools)
# ======================================================================
# ── 부서이력/이동(필수 최소) ──────────────────────────────────────────────────

HIST_SHEET = "부서이력"

def ensure_dept_history_sheet():
    """
    부서(근무지) 이동 이력 시트 보장 + 헤더 정렬.
    gspread 호출은 캐시/재시도를 사용해 429를 완화합니다.
    """
    try:
        ws = _ws_cached(HIST_SHEET)
    except WorksheetNotFound:
        wb = get_workbook()
        ws = _retry_call(wb.add_worksheet, title=HIST_SHEET, rows=5000, cols=30)
        _WS_CACHE[HIST_SHEET] = (time.time(), ws)

    default_headers = [
        "사번", "이름",
        "부서1", "부서2",
        "시작일", "종료일",
        "변경사유", "승인자", "메모",
        "등록시각",
    ]
    header, hmap = _sheet_header_cached(ws, HIST_SHEET)
    if not header:
        _retry_call(ws.update, "1:1", [default_headers])
        header = default_headers
        hmap = {n: i + 1 for i, n in enumerate(header)}
        _HDR_CACHE[HIST_SHEET] = (time.time(), header, hmap)
    else:
        need = [h for h in default_headers if h not in header]
        if need:
            new_header = header + need
            _retry_call(ws.update, "1:1", [new_header])
            header = new_header
            hmap = {n: i + 1 for i, n in enumerate(header)}
            _HDR_CACHE[HIST_SHEET] = (time.time(), header, hmap)

    return ws


@st.cache_data(ttl=60, show_spinner=False)
def read_dept_history_df():
    ensure_dept_history_sheet()
    ws = get_workbook().worksheet(HIST_SHEET)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty:
        return df
    for c in ["시작일", "종료일", "등록시각"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)
    return df


def apply_department_change(
    emp_df,
    sabun,
    new_dept1,
    new_dept2,
    start_date,
    reason="",
    approver="",
):
    ws_hist = ensure_dept_history_sheet()

    start_str = start_date.strftime("%Y-%m-%d")
    prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if row.empty:
        raise RuntimeError("사번을 찾지 못했습니다.")
    name = str(row.iloc[0].get("이름", ""))

    header, hmap = _sheet_header_cached(ws_hist, HIST_SHEET)
    values = _retry_call(ws_hist.get_all_values)

    cS = hmap.get("사번")
    cE = hmap.get("종료일")
    if cS and cE:
        for i in range(2, len(values) + 1):
            row_i = values[i - 1]
            try:
                if str(row_i[cS - 1]).strip() == str(sabun).strip() and str(row_i[cE - 1]).strip() == "":
                    _retry_call(ws_hist.update_cell, i, cE, prev_end)
            except IndexError:
                continue

    rec = {
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
    rowbuf = [rec.get(h, "") for h in header]
    _retry_call(ws_hist.append_row, rowbuf, value_input_option="USER_ENTERED")

    applied = False
    if start_date <= datetime.now(tz=tz_kst()).date():
        ws_emp = _ws_cached(EMP_SHEET)
        header_emp, hmap_emp = _sheet_header_cached(ws_emp, EMP_SHEET)

        row_idx = _find_row_by_sabun(ws_emp, hmap_emp, str(sabun))
        if row_idx > 0:
            if "부서1" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서1"], new_dept1)
            if "부서2" in hmap_emp:
                _update_cell(ws_emp, row_idx, hmap_emp["부서2"], new_dept2)
            applied = True

    st.cache_data.clear()
    return {
        "applied_now": applied,
        "start_date": start_str,
        "new_dept1": new_dept1,
        "new_dept2": new_dept2,
    }


def sync_current_department_from_history(as_of_date=None):
    ensure_dept_history_sheet()
    hist = read_dept_history_df()
    emp = read_sheet_df(EMP_SHEET)
    if as_of_date is None:
        as_of_date = datetime.now(tz=tz_kst()).date()
    D = as_of_date.strftime("%Y-%m-%d")
    updates = {}
    for sabun, grp in hist.groupby("사번"):
        def ok(row):
            s = row.get("시작일", ""); e = row.get("종료일", "")
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
                if "부서1" in hmap_emp: _update_cell(ws_emp, row_idx, hmap_emp["부서1"], d1)
                if "부서2" in hmap_emp: _update_cell(ws_emp, row_idx, hmap_emp["부서2"], d2)
                changed += 1
    st.cache_data.clear()
    return changed


# ── 관리자: PIN / 부서이동 / 평가항목 / 권한 ─────────────────────────────────

def _random_pin(length=6):
    return "".join(pysecrets.choice("0123456789") for _ in range(length))


def tab_admin_pin(emp_df):
    st.markdown("### PIN 관리")
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"]) if "사번" in df.columns else df
    sel = st.selectbox("직원 선택(사번 - 이름)", ["(선택)"] + df.get("표시", pd.Series(dtype=str)).tolist(), index=0, key="adm_pin_pick")
    if sel != "(선택)":
        sabun = sel.split(" - ", 1)[0]
        row = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]
        st.write(f"사번: **{sabun}** / 이름: **{row.get('이름','')}**")
        pin1 = st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
        pin2 = st.text_input("새 PIN 확인", type="password", key="adm_pin2")
        col = st.columns([1, 1, 2])
        with col[0]: do_save = st.button("PIN 저장/변경", type="primary", use_container_width=True, key="adm_pin_save")
        with col[1]: do_clear = st.button("PIN 비우기", use_container_width=True, key="adm_pin_clear")
        if do_save:
            if not pin1 or not pin2:
                st.error("PIN을 두 번 모두 입력하세요."); return
            if pin1 != pin2:
                st.error("PIN 확인이 일치하지 않습니다."); return
            if not pin1.isdigit():
                st.error("PIN은 숫자만 입력하세요."); return
            if not _to_bool(row.get("재직여부", False)):
                st.error("퇴직자는 변경할 수 없습니다."); return
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap:
                st.error(f"'{EMP_SHEET}' 시트에 PIN_hash가 없습니다."); return
            r = _find_row_by_sabun(ws, hmap, sabun)
            if r == 0:
                st.error("시트에서 사번을 찾지 못했습니다."); return
            _update_cell(ws, r, hmap["PIN_hash"], _pin_hash(pin1.strip(), str(sabun)))
            st.cache_data.clear()
            st.success("PIN 저장 완료", icon="✅")
        if do_clear:
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap:
                st.error(f"'{EMP_SHEET}' 시트에 PIN_hash가 없습니다."); return
            r = _find_row_by_sabun(ws, hmap, sabun)
            if r == 0:
                st.error("시트에서 사번을 찾지 못했습니다."); return
            _update_cell(ws, r, hmap["PIN_hash"], "")
            st.cache_data.clear()
            st.success("PIN 초기화 완료", icon="✅")

    st.divider()
    st.markdown("#### 전 직원 일괄 PIN 발급")
    col = st.columns([1, 1, 1, 1, 2])
    with col[0]: only_active = st.checkbox("재직자만", True, key="adm_pin_only_active")
    with col[1]: only_empty = st.checkbox("PIN 미설정자만", True, key="adm_pin_only_empty")
    with col[2]: overwrite_all = st.checkbox("기존 PIN 덮어쓰기", False, disabled=only_empty, key="adm_pin_overwrite")
    with col[3]: pin_len = st.number_input("자릿수", min_value=4, max_value=8, value=6, step=1, key="adm_pin_len")
    with col[4]: uniq = st.checkbox("서로 다른 PIN 보장", True, key="adm_pin_uniq")
    candidates = emp_df.copy()
    if only_active and "재직여부" in candidates.columns: candidates = candidates[candidates["재직여부"] == True]
    if only_empty: candidates = candidates[(candidates["PIN_hash"].astype(str).str.strip() == "")]
    elif not overwrite_all: st.warning("'PIN 미설정자만' 또는 '덮어쓰기' 중 하나 선택 필요", icon="⚠️")
    candidates = candidates.copy()
    if "사번" in candidates.columns: candidates["사번"] = candidates["사번"].astype(str)
    st.write(f"대상자 수: **{len(candidates):,}명**")
    col2 = st.columns([1, 1, 2, 2])
    with col2[0]: do_preview = st.button("미리보기 생성", use_container_width=True, key="adm_pin_prev")
    with col2[1]: do_issue = st.button("발급 실행(시트 업데이트)", type="primary", use_container_width=True, key="adm_pin_issue")
    preview = None
    if do_preview or do_issue:
        if len(candidates) == 0:
            st.warning("대상자가 없습니다.", icon="⚠️")
        else:
            used = set(); new_pins = []
            for _ in range(len(candidates)):
                while True:
                    p = _random_pin(pin_len)
                    if not uniq or p not in used:
                        used.add(p); new_pins.append(p); break
            preview = candidates[["사번", "이름"]].copy(); preview["새_PIN"] = new_pins
            st.dataframe(preview, use_container_width=True, height=360)
            full = emp_df[["사번", "이름"]].copy(); full["사번"] = full["사번"].astype(str)
            join_src = preview[["사번", "새_PIN"]].copy(); join_src["사번"] = join_src["사번"].astype(str)
            csv_df = full.merge(join_src, on="사번", how="left"); csv_df["새_PIN"] = csv_df["새_PIN"].fillna("")
            csv_df = csv_df.sort_values("사번")
            st.download_button("CSV 전체 다운로드 (사번,이름,새_PIN)", data=csv_df.to_csv(index=False, encoding="utf-8-sig"), file_name=f"PIN_ALL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)
            st.download_button("CSV 대상자만 다운로드 (사번,이름,새_PIN)", data=preview.to_csv(index=False, encoding="utf-8-sig"), file_name=f"PIN_TARGETS_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)
    if do_issue and preview is not None:
        try:
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap or "사번" not in hmap:
                st.error(f"'{EMP_SHEET}' 시트에 '사번' 또는 'PIN_hash' 헤더가 없습니다.")
                return

            sabun_col = hmap["사번"]
            pin_col   = hmap["PIN_hash"]

            sabun_values = _retry_call(ws.col_values, sabun_col)[1:]
            pos = {str(v).strip(): i for i, v in enumerate(sabun_values, start=2)}

            updates = []
            for _, row in preview.iterrows():
                sabun = str(row["사번"]).strip()
                r_idx = pos.get(sabun, 0)
                if r_idx:
                    a1 = gspread.utils.rowcol_to_a1(r_idx, pin_col)
                    hashed = _pin_hash(str(row["새_PIN"]), sabun)
                    updates.append({"range": a1, "values": [[hashed]]})

            if not updates:
                st.warning("업데이트할 대상이 없습니다.", icon="⚠️")
                return

            CHUNK = 100
            total = len(updates)
            pbar = st.progress(0.0, text="시트 업데이트(배치) 중...")
            for i in range(0, total, CHUNK):
                _retry_call(ws.batch_update, updates[i:i+CHUNK])
                done = min(i + CHUNK, total)
                pbar.progress(done / total, text=f"{done}/{total} 반영 중…")
                time.sleep(0.2)

            st.cache_data.clear()
            st.success(f"일괄 발급 완료: {total:,}명 반영", icon="✅")
            st.toast("PIN 일괄 발급 반영됨", icon="✅")

        except Exception as e:
            st.exception(e)


def tab_admin_transfer(emp_df):
    st.markdown("### 부서(근무지) 이동")
    df = emp_df.copy()
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df = df.sort_values(["사번"]) if "사번" in df.columns else df
    sel = st.selectbox("직원 선택(사번 - 이름)", ["(선택)"] + df.get("표시", pd.Series(dtype=str)).tolist(), index=0, key="adm_tr_pick")
    if sel == "(선택)":
        st.info("사번을 선택하면 이동 입력 폼이 표시됩니다.")
        return
    sabun = sel.split(" - ", 1)[0]
    target = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]
    c = st.columns([1, 1, 1, 1])
    with c[0]: st.metric("사번", str(target.get("사번", "")))
    with c[1]: st.metric("이름", str(target.get("이름", "")))
    with c[2]: st.metric("현재 부서1", str(target.get("부서1", "")))
    with c[3]: st.metric("현재 부서2", str(target.get("부서2", "")))
    st.divider()
    opt_d1 = sorted([x for x in emp_df.get("부서1", pd.Series(dtype=str)).dropna().unique() if x]) if "부서1" in emp_df.columns else []
    opt_d2 = sorted([x for x in emp_df.get("부서2", pd.Series(dtype=str)).dropna().unique() if x]) if "부서2" in emp_df.columns else []
    col = st.columns([1, 1, 1])
    with col[0]: start_date = st.date_input("시작일(발령일)", datetime.now(tz=tz_kst()).date(), key="adm_tr_start")
    with col[1]: new_d1 = st.selectbox("새 부서1(선택 또는 직접입력)", ["(직접입력)"] + opt_d1, index=0, key="adm_tr_d1_pick")
    with col[2]: new_d2 = st.selectbox("새 부서2(선택 또는 직접입력)", ["(직접입력)"] + opt_d2, index=0, key="adm_tr_d2_pick")
    nd1 = st.text_input("부서1 직접입력", value="" if new_d1 != "(직접입력)" else "", key="adm_tr_nd1")
    nd2 = st.text_input("부서2 직접입력", value="" if new_d2 != "(직접입력)" else "", key="adm_tr_nd2")
    new_dept1 = new_d1 if new_d1 != "(직접입력)" else nd1
    new_dept2 = new_d2 if new_d2 != "(직접입력)" else nd2
    col2 = st.columns([2, 3])
    with col2[0]: reason = st.text_input("변경사유", "", key="adm_tr_reason")
    with col2[1]: approver = st.text_input("승인자", "", key="adm_tr_approver")
    if st.button("이동 기록 + 현재 반영", type="primary", use_container_width=True, key="adm_tr_apply"):
        if not (str(new_dept1).strip() or str(new_dept2).strip()):
            st.error("새 부서1/부서2 중 최소 하나는 입력/선택"); return
        try:
            rep = apply_department_change(emp_df, str(sabun), str(new_dept1).strip(), str(new_dept2).strip(), start_date, str(reason).strip(), str(approver).strip())
            if rep["applied_now"]:
                st.success(f"이동 기록 + 현재부서 반영: {rep['new_dept1']} / {rep['new_dept2']} (시작일 {rep['start_date']})", icon="✅")
            else:
                st.info(f"이동 이력만 기록됨(시작일 {rep['start_date']}). 이후 '동기화'에서 반영.", icon="ℹ️")
            st.toast("부서 이동 처리됨", icon="✅")
        except Exception as e:
            st.exception(e)
    st.divider()
    if st.button("오늘 기준 전체 동기화", use_container_width=True, key="adm_tr_sync"):
        try:
            cnt = sync_current_department_from_history()
            st.success(f"직원 시트 현재부서 동기화 완료: {cnt}명 반영", icon="✅")
        except Exception as e:
            st.exception(e)


def tab_admin_eval_items():
    st.markdown("### 평가 항목 관리")

    df = read_eval_items_df(only_active=False).copy()

    for c in ["항목ID", "항목", "내용", "비고"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "순서" in df.columns:
        df["순서"] = pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(lambda x: str(x).strip().lower() in ("true", "1", "y", "yes", "t"))

    st.write(f"현재 등록: **{len(df)}개** (활성 {df[df.get('활성', False)==True].shape[0]}개)")

    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        edit_df = df[["항목ID", "항목", "순서", "활성"]].copy().reset_index(drop=True)

        edit_df["항목ID"] = edit_df["항목ID"].astype(str)
        edit_df["항목"] = edit_df["항목"].astype(str)

        def _toi(x):
            try: return int(float(str(x).strip()))
            except Exception: return 0

        def _tob(x):
            return str(x).strip().lower() in ("true", "1", "y", "yes", "t")

        edit_df["순서"] = edit_df["순서"].apply(_toi)
        edit_df["활성"] = edit_df["활성"].apply(_tob)

        st.caption("표에서 **순서**만 변경 가능합니다. (다른 열은 읽기 전용)")

        edited = st.data_editor(
            edit_df,
            use_container_width=True,
            height=420,
            hide_index=True,
            column_order=["항목ID", "항목", "순서", "활성"],
            column_config={
                "항목ID": st.column_config.TextColumn(disabled=True),
                "항목": st.column_config.TextColumn(disabled=True),
                "활성": st.column_config.CheckboxColumn(disabled=True),
                "순서": st.column_config.NumberColumn(step=1, min_value=0),
            },
        )

        if st.button("순서 일괄 저장", type="primary", use_container_width=True):
            try:
                ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
                header = ws.row_values(1) or []
                hmap = {n: i + 1 for i, n in enumerate(header)}

                col_id = hmap.get("항목ID")
                col_ord = hmap.get("순서")
                if not col_id or not col_ord:
                    st.error("'항목ID' 또는 '순서' 헤더가 없습니다.")
                    st.stop()

                id_vals = _retry_call(ws.col_values, col_id)[1:]
                pos = {str(v).strip(): i for i, v in enumerate(id_vals, start=2)}

                changed = 0
                for _, r in edited.iterrows():
                    iid = str(r["항목ID"]).strip()
                    new = int(_toi(r["순서"]))
                    if iid in pos:
                        a1 = gspread.utils.rowcol_to_a1(pos[iid], col_ord)
                        _retry_call(ws.update, a1, [[new]])
                        changed += 1

                st.cache_data.clear()
                st.success(f"순서 저장 완료: {changed}건 반영", icon="✅")
                st.rerun()
            except Exception as e:
                st.exception(e)

    st.divider()
    st.markdown("### 신규 등록 / 수정")

    choices = ["(신규)"] + [f"{r['항목ID']} - {r['항목']}" for _, r in df.iterrows()] if not df.empty else ["(신규)"]
    sel = st.selectbox("대상 선택", choices, index=0, key="adm_eval_pick")

    item_id = None
    name = ""
    desc = ""
    order = int(df["순서"].max() + 1) if ("순서" in df.columns and not df.empty) else 1
    active = True
    memo = ""

    if sel != "(신규)" and not df.empty:
        iid = sel.split(" - ", 1)[0]
        row = df.loc[df["항목ID"] == iid]
        if not row.empty:
            row   = row.iloc[0]
            item_id = str(row.get("항목ID",""))
            name    = str(row.get("항목",""))
            desc    = str(row.get("내용",""))
            memo    = str(row.get("비고",""))
            try: order = int(row.get("순서", 0) or 0)
            except Exception: order = 0
            active = (str(row.get("활성","")).strip().lower() in ("true","1","y","yes","t"))

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
                    ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
                    header = ws.row_values(1) or EVAL_ITEM_HEADERS
                    hmap   = {n: i + 1 for i, n in enumerate(header)}

                    if not item_id:
                        col_id = hmap.get("항목ID")
                        nums = []
                        if col_id:
                            vals = _retry_call(ws.col_values, col_id)[1:]
                            for v in vals:
                                s = str(v).strip()
                                if s.startswith("ITM"):
                                    try: nums.append(int(s[3:]))
                                    except Exception: pass
                        new_id = f"ITM{((max(nums)+1) if nums else 1):04d}"

                        rowbuf = [""] * len(header)
                        def put(k, v):
                            c = hmap.get(k)
                            if c: rowbuf[c - 1] = v
                        put("항목ID", new_id)
                        put("항목", name.strip())
                        put("내용", desc.strip())
                        put("순서", int(order))
                        put("활성", bool(active))
                        if "비고" in hmap: put("비고", memo.strip())

                        _retry_call(ws.append_row, rowbuf, value_input_option="USER_ENTERED")
                        st.cache_data.clear()
                        st.success(f"저장 완료 (항목ID: {new_id})")
                        st.rerun()

                    else:
                        col_id = hmap.get("항목ID")
                        idx = 0
                        if col_id:
                            vals = _retry_call(ws.col_values, col_id)
                            for i, v in enumerate(vals[1:], start=2):
                                if str(v).strip() == str(item_id).strip():
                                    idx = i; break
                        if idx == 0:
                            st.error("대상 항목을 찾을 수 없습니다.")
                        else:
                            ws.update_cell(idx, hmap["항목"], name.strip())
                            ws.update_cell(idx, hmap["내용"], desc.strip())
                            ws.update_cell(idx, hmap["순서"], int(order))
                            ws.update_cell(idx, hmap["활성"], bool(active))
                            if "비고" in hmap: ws.update_cell(idx, hmap["비고"], memo.strip())
                            st.cache_data.clear()
                            st.success("업데이트 완료")
                            st.rerun()
                except Exception as e:
                    st.exception(e)

        if item_id:
            if st.button("비활성화(소프트 삭제)", use_container_width=True, key="adm_eval_disable_v3"):
                try:
                    ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
                    header = ws.row_values(1); hmap = {n: i + 1 for i, n in enumerate(header)}
                    col_id = hmap.get("항목ID"); col_active = hmap.get("활성")
                    if not (col_id and col_active):
                        st.error("'항목ID' 또는 '활성' 컬럼이 없습니다.")
                    else:
                        vals = _retry_call(ws.col_values, col_id)
                        for i, v in enumerate(vals[1:], start=2):
                            if str(v).strip() == str(item_id).strip():
                                ws.update_cell(i, col_active, False); break
                        st.cache_data.clear()
                        st.success("비활성화 완료"); st.rerun()
                except Exception as e:
                    st.exception(e)

            if st.button("행 삭제(완전 삭제)", use_container_width=True, key="adm_eval_delete_v3"):
                try:
                    ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
                    header = ws.row_values(1); hmap = {n: i + 1 for i, n in enumerate(header)}
                    col_id = hmap.get("항목ID")
                    if not col_id:
                        st.error("'항목ID' 컬럼이 없습니다.")
                    else:
                        vals = _retry_call(ws.col_values, col_id)
                        for i, v in enumerate(vals[1:], start=2):
                            if str(v).strip() == str(item_id).strip():
                                ws.delete_rows(i); break
                        st.cache_data.clear()
                        st.success("삭제 완료"); st.rerun()
                except Exception as e:
                    st.exception(e)


def tab_admin_jobdesc_defaults():
    st.markdown("### 직무기술서 기본값")
    cur_create = get_setting("JD.제정일", "")
    cur_update = get_setting("JD.개정일", "")
    cur_review = get_setting("JD.검토주기", "")

    c = st.columns([1, 1, 1])
    with c[0]:
        v_create = st.text_input("제정일 기본값", value=cur_create, key="adm_jd_create")
    with c[1]:
        v_update = st.text_input("개정일 기본값", value=cur_update, key="adm_jd_update")
    with c[2]:
        v_review = st.text_input("검토주기 기본값", value=cur_review, key="adm_jd_review")

    memo = st.text_input("비고(선택)", value="", key="adm_jd_memo")

    if st.button("저장", type="primary", use_container_width=True, key="adm_jd_save"):
        u = st.session_state.get("user", {"사번": "", "이름": ""})
        try:
            set_setting("JD.제정일", v_create, memo, str(u.get("사번", "")), str(u.get("이름", "")))
            set_setting("JD.개정일", v_update, memo, str(u.get("사번", "")), str(u.get("이름", "")))
            set_setting("JD.검토주기", v_review, memo, str(u.get("사번", "")), str(u.get("이름", "")))
            st.success("저장되었습니다.", icon="✅")
        except Exception as e:
            st.exception(e)

    st.divider()
    df = read_settings_df()
    if df.empty:
        st.caption("설정 데이터가 없습니다.")
    else:
        st.dataframe(df.sort_values("키"), use_container_width=True, height=240)


# ── 권한관리(ACL): 심플 단일 표 편집 + 전체 반영 ─────────────────────────────

def tab_admin_acl(emp_df):
    """
    권한관리 — 단일 표에서 추가/수정/삭제 + 전체 반영 저장
    - 사번 컬럼은 '사번 - 이름' 레이블로 선택
    - 선택 시 자동으로 이름 컬럼 동기화
    - 검색/필터 등 부가 UI 제거 (심플)
    - 저장 시 전체 덮어쓰기
    """
    st.markdown("### 권한 관리")

    me = st.session_state.get("user", {})
    try:
        am_admin = is_admin(str(me.get("사번", "")))
    except Exception:
        am_admin = False
    if not am_admin:
        st.error("Master만 저장할 수 있습니다. (표/저장 모두 비활성화)", icon="🛡️")

    try:
        base = emp_df[["사번", "이름", "부서1", "부서2"]].copy()
    except Exception:
        base = pd.DataFrame(columns=["사번","이름","부서1","부서2"])
    if "사번" in base.columns:
        base["사번"] = base["사번"].astype(str).str.strip()
    emp_lookup = {}
    for _, r in base.iterrows():
        s = str(r.get("사번", "")).strip()
        emp_lookup[s] = {
            "이름":  str(r.get("이름", "")).strip(),
            "부서1": str(r.get("부서1", "")).strip(),
            "부서2": str(r.get("부서2", "")).strip(),
        }
    sabuns = sorted([s for s in emp_lookup.keys() if s])

    labels = []
    label_by_sabun = {}
    sabun_by_label = {}
    for s in sabuns:
        nm = emp_lookup[s]["이름"]
        label = f"{s} - {nm}" if nm else s
        labels.append(label)
        label_by_sabun[s] = label
        sabun_by_label[label] = s

    df_auth = read_auth_df()
    if df_auth.empty:
        df_auth = pd.DataFrame(columns=AUTH_HEADERS)

    def _tostr(x): return "" if x is None else str(x)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df_auth.columns:
            df_auth[c] = df_auth[c].map(_tostr)
    if "활성" in df_auth.columns:
        df_auth["활성"] = df_auth["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

    df_disp = df_auth.copy()
    if "사번" in df_disp.columns:
        df_disp["사번"] = df_disp["사번"].map(lambda v: label_by_sabun.get(str(v).strip(), str(v).strip()))

    role_options = ["admin","manager","evaluator"]
    scope_options = ["","부서","개별"]

    if "삭제" not in df_disp.columns:
        df_disp.insert(len(df_disp.columns), "삭제", False)

    colcfg = {
        "사번": st.column_config.SelectboxColumn(
            label="사번 - 이름",
            options=labels,
            help="사번을 선택하면 이름이 자동으로 입력됩니다."
        ),
        "이름": st.column_config.TextColumn(
            label="이름",
            help="사번 선택 시 자동 보정됩니다."
        ),
        "역할": st.column_config.SelectboxColumn(
            label="역할",
            options=role_options,
            help="권한 역할 (admin/manager/evaluator)"
        ),
        "범위유형": st.column_config.SelectboxColumn(
            label="범위유형",
            options=scope_options,
            help="빈값=전체 / 부서 / 개별"
        ),
        "부서1": st.column_config.TextColumn(label="부서1"),
        "부서2": st.column_config.TextColumn(label="부서2"),
        "대상사번": st.column_config.TextColumn(
            label="대상사번",
            help="범위유형이 '개별'일 때 대상 사번(쉼표/공백 구분)"
        ),
        "활성": st.column_config.CheckboxColumn(label="활성"),
        "비고": st.column_config.TextColumn(label="비고"),
        "삭제": st.column_config.CheckboxColumn(label="삭제", help="저장 시 체크된 행은 삭제됩니다."),
    }

    edited = st.data_editor(
        df_disp[[c for c in AUTH_HEADERS if c in df_disp.columns] + ["삭제"]],
        key="acl_editor_simple",
        use_container_width=True,
        height=520,
        hide_index=True,
        num_rows="dynamic",
        disabled=not am_admin,
        column_config=colcfg,
    )

    def _editor_to_canonical(df):
        df = df.copy()
        if "사번" in df.columns:
            for i, val in df["사번"].items():
                v = str(val).strip()
                if not v:
                    continue
                sab = sabun_by_label.get(v)
                if sab is None:
                    if " - " in v:
                        sab = v.split(" - ", 1)[0].strip()
                    else:
                        sab = v
                df.at[i, "사번"] = sab
                nm = emp_lookup.get(sab, {}).get("이름", "")
                if nm:
                    df.at[i, "이름"] = nm
        return df

    edited_canon = _editor_to_canonical(edited.drop(columns=["삭제"], errors="ignore"))

    def _validate_and_fix(df):
        df = df.copy().fillna("")
        errs = []

        df = df[df.astype(str).apply(lambda r: "".join(r.values).strip() != "", axis=1)]

        if "사번" in df.columns:
            for i, row in df.iterrows():
                sab = str(row.get("사번","")).strip()
                if not sab:
                    errs.append(f"{i+1}행: 사번이 비어 있습니다."); continue
                if sab not in emp_lookup:
                    errs.append(f"{i+1}행: 사번 '{sab}' 은(는) 직원 목록에 없습니다."); continue
                nm = emp_lookup[sab]["이름"]
                if str(row.get("이름","")).strip() != nm:
                    df.at[i, "이름"] = nm
                if not str(row.get("부서1","")).strip():
                    df.at[i, "부서1"] = emp_lookup[sab]["부서1"]
                if not str(row.get("부서2","")).strip():
                    df.at[i, "부서2"] = emp_lookup[sab]["부서2"]

        if "역할" in df.columns:
            bad = df[~df["역할"].isin(role_options) & (df["역할"].astype(str).str.strip()!="")]
            for i in bad.index.tolist():
                errs.append(f"{i+1}행: 역할 값이 잘못되었습니다. ({df.loc[i,'역할']})")
        if "범위유형" in df.columns:
            bad = df[~df["범위유형"].isin(scope_options) & (df["범위유형"].astype(str).str.strip()!="")]
            for i in bad.index.tolist():
                errs.append(f"{i+1}행: 범위유형 값이 잘못되었습니다. ({df.loc[i,'범위유형']})")

        keycols = [c for c in ["사번","역할","범위유형","부서1","부서2","대상사번"] if c in df.columns]
        if keycols:
            dup = df.assign(_key=df[keycols].astype(str).agg("|".join, axis=1)).duplicated("_key", keep=False)
            if dup.any():
                dup_idx = (dup[dup]).index.tolist()
                errs.append("중복 규칙 발견: " + ", ".join(str(i+1) for i in dup_idx) + " 행")

        if "활성" in df.columns:
            df["활성"] = df["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

        for c in AUTH_HEADERS:
            if c not in df.columns: df[c] = ""
        df = df[AUTH_HEADERS].copy()

        return df, errs

    fixed_df, errs = _validate_and_fix(edited_canon)

    if errs:
        msg = "저장 전 확인이 필요합니다:\n- " + "\n- ".join(errs)
        st.warning(msg)

    colb = st.columns([1,2,4])
    with colb[0]:
        do_save = st.button("🗂️ 권한 전체 반영", type="primary", use_container_width=True, disabled=(not am_admin))
    with colb[1]:
        st.caption("※ 표에서 추가·수정·삭제 후 꼭 저장을 눌러 반영하세요.")
    with colb[2]:
        st.caption("※ 저장 시 전체 덮어쓰기.")

    if do_save:
        if errs:
            st.error("유효성 오류가 있어 저장하지 않았습니다. 위 경고를 확인해주세요.", icon="⚠️")
            return
        try:
            ws = get_workbook().worksheet(AUTH_SHEET)
            header = ws.row_values(1) or AUTH_HEADERS

            _retry_call(ws.clear)
            _retry_call(ws.update, "A1", [header])

            out = fixed_df.copy()
            rows = out.apply(lambda r: [str(r.get(h, "")) for h in header], axis=1).tolist()

            if rows:
                CHUNK = 500
                for i in range(0, len(rows), CHUNK):
                    _retry_call(ws.append_rows, rows[i:i+CHUNK], value_input_option="USER_ENTERED")

            st.cache_data.clear()
            st.success("권한이 전체 반영되었습니다.", icon="✅")
            st.rerun()

        except Exception as e:
            st.exception(e)


# ======================================================================
# 📌 Startup Checks
# ======================================================================
# ── Startup Sanity Checks & Safe Runner (BEGIN) ──────────────────────────────
def startup_sanity_checks():
    problems = []
    try:
        emp = read_sheet_df(EMP_SHEET, silent=True)
        needed = ["사번", "이름"]
        miss = [c for c in needed if c not in emp.columns]
        if miss:
            problems.append(f"[직원시트] 필수 컬럼 누락: {', '.join(miss)}")
        if "사번" in emp.columns and emp["사번"].dtype != object:
            try:
                emp["사번"] = emp["사번"].astype(str)
            except Exception:
                problems.append("[직원시트] 사번 문자열 변환 실패")
    except Exception as e:
        problems.append(f"[직원시트] 로딩 실패: {e}")

    # 권한시트 보장은 5분 캐시 사용(429 완화)
    try:
        ok = ensure_auth_sheet_once()
        if not ok:
            problems.append("[권한시트] 보장 실패(캐시 호출 실패)")
    except Exception as e:
        problems.append(f"[권한시트] 보장 실패: {e}")

    try:
        _ = read_settings_df()
    except Exception as e:
        problems.append(f"[설정시트] 로딩 실패: {e}")
    try:
        _ = read_jobdesc_df()
    except Exception as e:
        problems.append(f"[직무기술서] 로딩 실패: {e}")
    try:
        _ = read_eval_items_df(only_active=False)
    except Exception as e:
        problems.append(f"[평가항목] 로딩 실패: {e}")

    return problems

def safe_run(render_fn, *args, title: str = "", **kwargs):
    """탭/섹션 하나를 안전하게 감싸서, 예외가 나도 전체 앱이 멈추지 않도록."""
    try:
        return render_fn(*args, **kwargs)
    except Exception as e:
        msg = f"[{title}] 렌더 실패: {e}" if title else f"렌더 실패: {e}"
        st.error(msg, icon="🛑")
        return None
# ── Startup Sanity Checks & Safe Runner (END) ────────────────────────────────


# ======================================================================
# 📌 Startup & Main
# ======================================================================
# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    st.markdown(f"## {APP_TITLE}")
    render_status_line()

    # 1) 직원 시트 로딩 + 세션 캐시/네임맵 구성
    try:
        emp_df_all = read_sheet_df(EMP_SHEET, silent=True)
    except Exception as e:
        st.error(f"'{EMP_SHEET}' 시트 로딩 실패: {e}")
        return

    # ▶ 스타트업 헬스체크: 경고만 출력(앱은 계속 실행)
    for warn in startup_sanity_checks():
        st.warning(warn, icon="⚠️")

    st.session_state["emp_df_cache"] = emp_df_all.copy()
    st.session_state["name_by_sabun"] = _build_name_map(emp_df_all)

    # 2) 로그인 요구
    require_login(emp_df_all)

    # 3) 로그인 직후: 관리자 플래그 최신화
    try:
        st.session_state["user"]["관리자여부"] = is_admin(st.session_state["user"]["사번"])
    except Exception:
        st.session_state["user"]["관리자여부"] = (
            st.session_state["user"]["사번"] in {a["사번"] for a in SEED_ADMINS}
        )
        st.warning("권한 시트 조회 오류로 관리자 여부를 시드 기준으로 판정했습니다.", icon="⚠️")

    # 4) 데이터 뷰 분기 (의료진 포함, 필터 없음)
    emp_df_for_staff = emp_df_all
    emp_df_for_rest  = emp_df_all

    # 5) 사이드바 사용자/로그아웃
    u = st.session_state["user"]
    with st.sidebar:
        st.write(f"👤 **{u['이름']}** ({u['사번']})")
        role_badge = "관리자" if u.get("관리자여부", False) else (
            "매니저" if is_manager(emp_df_all, u["사번"]) else "직원"
        )
        st.caption(f"권한: {role_badge}")
        if st.button("로그아웃", use_container_width=True):
            logout()

    # 6) 탭 구성
    if u.get("관리자여부", False):
        tabs = st.tabs(["직원", "인사평가", "직무기술서", "직무능력평가", "관리자", "도움말"])
    else:
        tabs = st.tabs(["직원", "인사평가", "직무기술서", "직무능력평가", "도움말"])

    with tabs[0]:
        safe_run(tab_staff, emp_df_for_staff, title="직원")

    with tabs[1]:
        safe_run(tab_eval_input, emp_df_for_rest, title="평가")

    with tabs[2]:
        safe_run(tab_job_desc, emp_df_for_rest, title="직무기술서")

    with tabs[3]:
        safe_run(tab_competency, emp_df_for_rest, title="직무능력평가")

    if u.get("관리자여부", False):
        with tabs[4]:
            st.subheader("관리자 메뉴")
            admin_page = st.radio(
                "기능 선택",
                ["PIN 관리", "부서(근무지) 이동", "평가 항목 관리", "권한 관리"],
                horizontal=True,
                key="admin_page_selector",
            )
            st.divider()
            if admin_page == "PIN 관리":
                safe_run(tab_admin_pin,       emp_df_for_rest, title="관리자·PIN")
            elif admin_page == "부서(근무지) 이동":
                safe_run(tab_admin_transfer,  emp_df_for_rest, title="관리자·부서이동")
            elif admin_page == "평가 항목 관리":
                safe_run(tab_admin_eval_items,                  title="관리자·평가항목")
            else:
                safe_run(tab_admin_acl,       emp_df_for_rest, title="관리자·권한")

    def _render_help():
        st.markdown(
            """
            ### 사용 안내
            - 직원 탭: 전체 데이터(의사 포함), 권한에 따라 행 제한
            - 평가/직무기술서/직무능력평가/관리자: 동일 데이터 기반, 권한에 따라 접근
            - 상태표시: 상단에 'DB연결 … (KST)'
            
            ### 권한(Role) 설명
            - **admin**: 시스템 최상위 관리자, 모든 메뉴 접근 가능
            - **manager**: 지정된 부서 소속 직원 관리 가능 (부장/팀장은 자동 권한 부여)
            - **evaluator**: 평가 권한 보유, 지정된 부서 직원 평가 가능
            - **seed**: 초기 시스템에서 강제로 삽입된 보장 관리자 계정 (삭제 불가)
            """
        )

            # 관리자 전용: DB열기
        me = st.session_state.get("user", {})
        my_empno = str(me.get("사번", ""))
        if my_empno and is_admin(my_empno):
            sheet_id = st.secrets.get("sheets", {}).get("HR_SHEET_ID")
            if sheet_id:
                url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
                st.caption(f"📄 DB열기: [{url}]({url})")

    with tabs[-1]:
        safe_run(_render_help, title="도움말")

# ── 엔트리포인트 ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
