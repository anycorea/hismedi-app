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
API_BACKOFF_SEC = [0.0, 0.6, 1.2, 2.4]
def _retry_call(fn, *args, **kwargs):
    last = None
    for backoff in API_BACKOFF_SEC:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            last = e
            time.sleep(backoff + random.uniform(0, 0.15))
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
def ensure_auth_sheet():
    wb = get_workbook()
    try:
        ws = wb.worksheet(AUTH_SHEET)
        header = _retry_call(ws.row_values, 1) or []
        need = [h for h in AUTH_HEADERS if h not in header]
        if need:
            _retry_call(ws.update, "1:1", [header + need])
            header = _retry_call(ws.row_values, 1) or []
        # 시드 주입
        vals = _retry_call(ws.get_all_records, numericise_ignore=["all"])
        cur_admins = {str(r.get("사번", "")).strip() for r in vals if str(r.get("역할", "")).strip() == "admin"}
        add = [r for r in SEED_ADMINS if r["사번"] not in cur_admins]
        if add:
            rows = [[r.get(h, "") for h in header] for r in add]
            _retry_call(ws.append_rows, rows, value_input_option="USER_ENTERED")
        return ws
    except WorksheetNotFound:
        ws = _retry_call(wb.add_worksheet, title=AUTH_SHEET, rows=1000, cols=20)
        _retry_call(ws.update, "A1", [AUTH_HEADERS])
        _retry_call(ws.append_rows, [[r.get(h, "") for h in AUTH_HEADERS] for r in SEED_ADMINS], value_input_option="USER_ENTERED")
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_auth_df() -> pd.DataFrame:
    try:
        ensure_auth_sheet()
        ws = _ws_cached(AUTH_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        return _silent_df_exception(e, "권한 시트 읽기", AUTH_HEADERS)

    if df.empty: return pd.DataFrame(columns=AUTH_HEADERS)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df.columns: df[c] = df[c].astype(str)
    if "활성" in df.columns: df["활성"] = df["활성"].map(_to_bool)
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

# ======================================================================
# 📌 직원(Employee)
# ======================================================================
# ── 직원 탭 ───────────────────────────────────────────────────────────────────
def tab_staff(emp_df: pd.DataFrame):
    u=st.session_state["user"]; me=str(u["사번"])
    if not is_admin(me):
        allowed=get_allowed_sabuns(emp_df,me,include_self=True)
        emp_df=emp_df[emp_df["사번"].astype(str).isin(allowed)].copy()

    st.subheader("직원")
    df=emp_df.copy()
    c=st.columns([1,1,1,1,1,1,2])
    with c[0]: dept1=st.selectbox("부서1",["(전체)"]+sorted([x for x in df.get("부서1",[]).dropna().unique() if x]),index=0,key="staff_dept1")
    with c[1]: dept2=st.selectbox("부서2",["(전체)"]+sorted([x for x in df.get("부서2",[]).dropna().unique() if x]),index=0,key="staff_dept2")
    with c[2]: grade=st.selectbox("직급",["(전체)"]+sorted([x for x in df.get("직급",[]).dropna().unique() if x]),index=0,key="staff_grade")
    with c[3]: duty =st.selectbox("직무",["(전체)"]+sorted([x for x in df.get("직무",[]).dropna().unique() if x]),index=0,key="staff_duty")
    with c[4]: group=st.selectbox("직군",["(전체)"]+sorted([x for x in df.get("직군",[]).dropna().unique() if x]),index=0,key="staff_group")
    with c[5]: active=st.selectbox("재직여부",["(전체)","재직","퇴직"],index=0,key="staff_active")
    with c[6]: q=st.text_input("검색(사번/이름/이메일)","",key="staff_q")

    view=df.copy()
    if dept1!="(전체)" and "부서1" in view: view=view[view["부서1"]==dept1]
    if dept2!="(전체)" and "부서2" in view: view=view[view["부서2"]==dept2]
    if grade!="(전체)" and "직급" in view: view=view[view["직급"]==grade]
    if duty !="(전체)" and "직무" in view: view=view[view["직무"]==duty]
    if group!="(전체)" and "직군" in view: view=view[view["직군"]==group]
    if active!="(전체)" and "재직여부" in view: view=view[view["재직여부"]==(active=="재직")]
    if q.strip():
        k=q.strip().lower()
        view=view[view.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이메일","이름"] if c in r), axis=1)]
    st.write(f"결과: **{len(view):,}명**")
    st.dataframe(view, use_container_width=True, height=560)
    sheet_id = st.secrets["sheets"]["HR_SHEET_ID"]
    st.caption(f"📄 원본: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")

# ======================================================================
# 📌 인사평가(Evaluation)
# ======================================================================
# ── 평가(1~5, 100점 환산) ─────────────────────────────────────────────────────
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고"]
EVAL_RESP_SHEET_PREFIX = "평가_응답_"
EVAL_BASE_HEADERS = ["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각","서명_대상","서명시각_대상","서명_평가자","서명시각_평가자","잠금"]
EVAL_TYPES = ["자기", "1차", "2차"]

def ensure_eval_items_sheet():
    wb = get_workbook()
    try:
        ws = wb.worksheet(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws = _retry_call(wb.add_worksheet, title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        _retry_call(ws.update, "A1", [EVAL_ITEM_HEADERS]); return
    header = _retry_call(ws.row_values, 1) or []
    need = [h for h in EVAL_ITEM_HEADERS if h not in header]
    if need: _retry_call(ws.update, "1:1", [header + need])

@st.cache_data(ttl=60, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    try:
        ensure_eval_items_sheet()
        ws = _ws_cached(EVAL_ITEMS_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=EVAL_ITEM_HEADERS)
    if df.empty: return pd.DataFrame(columns=EVAL_ITEM_HEADERS)
    if "순서" in df.columns:
        def _i(x):
            try: return int(float(str(x).strip()))
            except: return 0
        df["순서"] = df["순서"].apply(_i)
    if "활성" in df.columns: df["활성"] = df["활성"].map(_to_bool)
    cols = [c for c in ["순서", "항목"] if c in df.columns]
    if cols: df = df.sort_values(cols).reset_index(drop=True)
    if only_active and "활성" in df.columns: df = df[df["활성"] == True]
    return df

def _eval_sheet_name(year: int | str) -> str: return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def _ensure_eval_response_sheet(year: int, item_ids: list[str]):
    title = _eval_sheet_name(year)
    wb = get_workbook()
    try:
        ws = _ws_cached(title)
    except WorksheetNotFound:
        ws = _retry_call(wb.add_worksheet, title=title, rows=5000, cols=max(50, len(item_ids) + 16))
        _WS_CACHE[title] = (time.time(), ws)
    required = list(EVAL_BASE_HEADERS) + [f"점수_{iid}" for iid in item_ids]
    header, _ = _sheet_header_cached(ws, title)
    if not header:
        _retry_call(ws.update, "1:1", [required])
        _HDR_CACHE[title] = (time.time(), required, {n: i + 1 for i, n in enumerate(required)})
    else:
        need = [h for h in required if h not in header]
        if need:
            new_header = header + need
            _retry_call(ws.update, "1:1", [new_header])
            _HDR_CACHE[title] = (time.time(), new_header, {n: i + 1 for i, n in enumerate(new_header)})
    return ws

def _emp_name_by_sabun(emp_df: pd.DataFrame, sabun: str) -> str:
    s = str(sabun)
    try:
        m = st.session_state.get("name_by_sabun")
        if isinstance(m, dict) and s in m: return m[s]
    except Exception: pass
    row = emp_df.loc[emp_df["사번"].astype(str) == s]
    if not row.empty: return str(row.iloc[0].get("이름", ""))
    if "emp_df_cache" in st.session_state:
        row2 = st.session_state["emp_df_cache"].loc[st.session_state["emp_df_cache"]["사번"].astype(str) == s]
        if not row2.empty: return str(row2.iloc[0].get("이름", ""))
    return ""

def upsert_eval_response(emp_df: pd.DataFrame, year: int, eval_type: str, target_sabun: str, evaluator_sabun: str, scores: dict[str, int], status: str = "제출") -> dict:
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)
    header = _retry_call(ws.row_values, 1)
    hmap = {n: i + 1 for i, n in enumerate(header)}
    def clamp5(v):
        try: v = int(v)
        except: v = 3
        return min(5, max(1, v))
    scores_list = [clamp5(scores.get(iid, 3)) for iid in item_ids]
    total_100 = round(sum(scores_list) * (100.0 / max(1, len(item_ids) * 5)), 1)
    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = kst_now_str()
    values = _retry_call(ws.get_all_values)
    cY = hmap.get("연도"); cT = hmap.get("평가유형"); cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            if (str(r[cY - 1]).strip() == str(year) and str(r[cT - 1]).strip() == str(eval_type)
                and str(r[cTS - 1]).strip() == str(target_sabun) and str(r[cES - 1]).strip() == str(evaluator_sabun)):
                row_idx = i; break
        except: pass
    if row_idx == 0:
        buf = [""] * len(header)
        def put(k, v): c = hmap.get(k); buf[c - 1] = v if c else ""
        put("연도", int(year)); put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("총점", total_100); put("상태", status); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c = hmap.get(f"점수_{iid}")
            if c: buf[c - 1] = sc
        _retry_call(ws.append_row, buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "row": None, "total": total_100}
    payload = {"총점": total_100, "상태": status, "제출시각": now, "평가대상이름": t_name, "평가자이름": e_name}
    for iid, sc in zip(item_ids, scores_list): payload[f"점수_{iid}"] = sc
    _batch_update_row(ws, row_idx, hmap, payload)
    st.cache_data.clear()
    return {"action": "update", "row": row_idx, "total": total_100}

@st.cache_data(ttl=60, show_spinner=False)
def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    name = _eval_sheet_name(year)
    try:
        ws = _ws_cached(name)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=EVAL_BASE_HEADERS)
    if df.empty: return df
    if "평가자사번" in df.columns: df = df[df["평가자사번"].astype(str) == str(sabun)]
    sort_cols = [c for c in ["평가유형", "평가대상사번", "제출시각"] if c in df.columns]
    if sort_cols: df = df.sort_values(sort_cols, ascending=[True, True, False]).reset_index(drop=True)
    return df

def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> tuple[dict, dict]:
    try:
        items = read_eval_items_df(True)
        item_ids = [str(x) for x in items["항목ID"].tolist()]
        ws = _ensure_eval_response_sheet(year, item_ids)
        header = _retry_call(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _retry_call(ws.get_all_values)
        cY = hmap.get("연도"); cT = hmap.get("평가유형"); cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
        row_idx = 0
        for i in range(2, len(values) + 1):
            r = values[i - 1]
            try:
                if (str(r[cY - 1]).strip() == str(year) and str(r[cT - 1]).strip() == str(eval_type)
                    and str(r[cTS - 1]).strip() == str(target_sabun) and str(r[cES - 1]).strip() == str(evaluator_sabun)):
                    row_idx = i; break
            except: pass
        if row_idx == 0: return {}, {}
        row = values[row_idx - 1]
        scores = {}
        for iid in item_ids:
            col = hmap.get(f"점수_{iid}")
            if col:
                try: v = int(str(row[col - 1]).strip() or "0")
                except: v = 0
                if v: scores[iid] = v
        meta = {}
        for k in ["상태", "잠금", "제출시각", "총점"]:
            c = hmap.get(k)
            if c: meta[k] = row[c - 1]
        return scores, meta
    except Exception:
        return {}, {}

def tab_eval_input(emp_df: pd.DataFrame):
    st.subheader("평가")
    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("평가 연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="eval2_year")
    u = st.session_state["user"]; me_sabun = str(u["사번"]); me_name = str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or is_manager(emp_df, me_sabun))
    allowed_sabuns = get_allowed_sabuns(emp_df, me_sabun, include_self=True)
    items = read_eval_items_df(only_active=True)
    if items.empty: st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️"); return
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    item_ids = [str(x) for x in items_sorted["항목ID"].tolist()]
    st.session_state.setdefault("eval2_target_sabun", me_sabun)
    st.session_state.setdefault("eval2_target_name",  me_name)
    st.session_state.setdefault("eval2_edit_mode",    False)

    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name  = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
        eval_type = "자기"; st.caption("평가유형: **자기**")
    else:
        base = emp_df.copy(); base["사번"] = base["사번"].astype(str)
        base = base[base["사번"].isin({str(s) for s in allowed_sabuns})]
        if "재직여부" in base.columns: base = base[base["재직여부"] == True]
        cflt = st.columns([1,1,1,2,1])
        with cflt[0]:
            opt_d1 = ["(전체)"] + sorted([x for x in base.get("부서1", []).dropna().unique() if x])
            f_d1 = st.selectbox("부서1", opt_d1, index=0, key="eval2_f_d1")
        with cflt[1]:
            sub = base if f_d1 == "(전체)" else base[base["부서1"].astype(str) == f_d1]
            opt_d2 = ["(전체)"] + sorted([x for x in sub.get("부서2", []).dropna().unique() if x])
            f_d2 = st.selectbox("부서2", opt_d2, index=0, key="eval2_f_d2")
        with cflt[2]:
            opt_g = ["(전체)"] + sorted([x for x in base.get("직급", []).dropna().unique() if x])
            f_g = st.selectbox("직급", opt_g, index=0, key="eval2_f_grade")
        with cflt[3]: f_q = st.text_input("검색(사번/이름)", "", key="eval2_f_q")
        with cflt[4]: only_active = st.checkbox("재직만", True, key="eval2_f_active")

        view = base[["사번","이름","부서1","부서2","직급","재직여부"]].copy()
        if only_active and "재직여부" in view.columns: view = view[view["재직여부"] == True]
        if f_d1 != "(전체)": view = view[view["부서1"].astype(str) == f_d1]
        if f_d2 != "(전체)": view = view[view["부서2"].astype(str) == f_d2]
        if f_g  != "(전체)": view = view[view["직급"].astype(str) == f_g]
        if f_q and f_q.strip():
            k = f_q.strip().lower()
            view = view[view.apply(lambda r: k in str(r["사번"]).lower() or k in str(r["이름"]).lower(), axis=1)]
        view = view.sort_values(["부서1","부서2","사번"]).reset_index(drop=True)
        view["선택"] = (view["사번"] == st.session_state["eval2_target_sabun"])
        edited_pick = st.data_editor(
            view[["선택","사번","이름","부서1","부서2","직급"]],
            use_container_width=True, height=360, key="eval2_pick_editor",
            column_config={"선택": st.column_config.CheckboxColumn()}, num_rows="fixed",
        )
        picked = edited_pick.loc[edited_pick["선택"] == True]
        if not picked.empty:
            r = picked.iloc[-1]
            st.session_state["eval2_target_sabun"] = str(r["사번"])
            st.session_state["eval2_target_name"]  = str(r["이름"])
        target_sabun = st.session_state["eval2_target_sabun"]
        target_name  = st.session_state["eval2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")
        eval_type_key = f"eval2_type_{year}_{me_sabun}_{target_sabun}"
        if eval_type_key not in st.session_state: st.session_state[eval_type_key] = "1차"
        eval_type = st.radio("평가유형", ["자기","1차","2차"], horizontal=True, key=eval_type_key)

    col_mode = st.columns([1,3])
    with col_mode[0]:
        if st.button(("수정모드로 전환" if not st.session_state["eval2_edit_mode"] else "보기모드로 전환"),
                     use_container_width=True, key="eval2_toggle"):
            st.session_state["eval2_edit_mode"] = not st.session_state["eval2_edit_mode"]; st.rerun()
    with col_mode[1]: st.caption(f"현재: **{'수정모드' if st.session_state['eval2_edit_mode'] else '보기모드'}**")
    edit_mode = bool(st.session_state["eval2_edit_mode"])

    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, target_sabun, me_sabun)
    kbase = f"E2_{year}_{eval_type}_{me_sabun}_{target_sabun}"
    if (not am_admin_or_mgr) and (not saved_scores) and (not edit_mode):
        st.session_state["eval2_edit_mode"] = True; edit_mode = True

    st.markdown("#### 점수 입력 (각 1~5)")
    c_head, c_slider, c_btn = st.columns([5,2,1])
    with c_head: st.caption("라디오로 개별 점수를 고르거나, 슬라이더 ‘일괄 적용’을 사용하세요.")
    slider_key = f"{kbase}_slider"
    if slider_key not in st.session_state:
        if saved_scores:
            avg = round(sum(saved_scores.values()) / max(1, len(saved_scores)))
            st.session_state[slider_key] = int(min(5, max(1, avg)))
        else:
            st.session_state[slider_key] = 3
    with c_slider:
        bulk_score = st.slider("일괄 점수", min_value=1, max_value=5, step=1, key=slider_key, disabled=not edit_mode)
    with c_btn:
        if st.button("일괄 적용", use_container_width=True, key=f"{kbase}_apply", disabled=not edit_mode):
            st.session_state[f"__apply_bulk_{kbase}"] = int(bulk_score)
            st.toast(f"모든 항목에 {bulk_score}점 적용", icon="✅")
    if st.session_state.get(f"__apply_bulk_{kbase}") is not None:
        _v = int(st.session_state[f"__apply_bulk_{kbase}"])
        for _iid in item_ids: st.session_state[f"eval2_seg_{_iid}_{kbase}"] = str(_v)
        del st.session_state[f"__apply_bulk_{kbase}"]

    scores = {}
    for r in items_sorted.itertuples(index=False):
        iid = str(getattr(r, "항목ID")); name = getattr(r, "항목") or ""; desc = getattr(r, "내용") or ""
        rkey = f"eval2_seg_{iid}_{kbase}"
        if rkey not in st.session_state:
            st.session_state[rkey] = str(int(saved_scores[iid])) if iid in saved_scores else "3"
        st.markdown('<div class="eval-row">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([2, 6, 3])
        with c1: st.markdown(f'**{name}**')
        with c2:
            if desc.strip(): st.caption(desc)
        with c3:
            st.radio(" ", ["1","2","3","4","5"], horizontal=True, key=rkey,
                     label_visibility="collapsed", disabled=not edit_mode)
        st.markdown('</div>', unsafe_allow_html=True)
        scores[iid] = int(st.session_state[rkey])

    total_100 = round(sum(scores.values()) * (100.0 / max(1, len(items_sorted) * 5)), 1)
    st.markdown("---")
    cM1, cM2 = st.columns([1, 3])
    with cM1: st.metric("합계(100점 만점)", total_100)
    with cM2: st.progress(min(1.0, total_100 / 100.0), text=f"총점 {total_100}점")
    col_submit = st.columns([1, 4])
    with col_submit[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True,
                            key=f"eval2_save_{kbase}", disabled=not edit_mode)
    if do_save:
        try:
            rep = upsert_eval_response(emp_df, int(year), eval_type, str(target_sabun), str(me_sabun), scores, "제출")
            st.success(("제출 완료" if rep["action"] == "insert" else "업데이트 완료") + f" (총점 {rep['total']}점)", icon="✅")
            st.toast("평가 저장됨", icon="✅")
            st.session_state["eval2_edit_mode"] = False; st.rerun()
        except Exception as e:
            st.exception(e)

    st.markdown("#### 내 제출 현황")
    try:
        my = read_my_eval_rows(int(year), me_sabun)
        if my.empty: st.caption("제출된 평가가 없습니다.")
        else:
            st.dataframe(my[["평가유형", "평가대상사번", "평가대상이름", "총점", "상태", "제출시각"]], use_container_width=True, height=260)
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

# ======================================================================
# 📌 직무기술서(Job Description)
# ======================================================================
# ── 직무기술서 ────────────────────────────────────────────────────────────────
JOBDESC_SHEET="직무기술서"
JOBDESC_HEADERS = [
    "사번","연도","버전","부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","서명방식","서명데이터","제출시각"
]

def ensure_jobdesc_sheet():
    wb=get_workbook()
    try:
        ws=wb.worksheet(JOBDESC_SHEET)
        header=_retry_call(ws.row_values,1) or []
        need=[h for h in JOBDESC_HEADERS if h not in header]
        if need: _retry_call(ws.update,"1:1",[header+need])
        return ws
    except WorksheetNotFound:
        ws=_retry_call(wb.add_worksheet,title=JOBDESC_SHEET, rows=1200, cols=60)
        _retry_call(ws.update,"A1",[JOBDESC_HEADERS]); return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_jobdesc_df()->pd.DataFrame:
    ensure_jobdesc_sheet()
    ws=_ws_cached(JOBDESC_SHEET)
    df=pd.DataFrame(_ws_get_all_records(ws))
    if df.empty: return pd.DataFrame(columns=JOBDESC_HEADERS)
    for c in JOBDESC_HEADERS:
        if c in df.columns: df[c]=df[c].astype(str)
    for c in ["연도","버전"]:
        if c in df.columns:
            def _i(x):
                try: return int(float(str(x).strip()))
                except: return 0
            df[c]=df[c].apply(_i)
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    return df

def _jobdesc_next_version(sabun:str, year:int)->int:
    df=read_jobdesc_df()
    if df.empty: return 1
    sub=df[(df["사번"]==str(sabun))&(df["연도"].astype(int)==int(year))]
    return 1 if sub.empty else int(sub["버전"].astype(int).max())+1

def upsert_jobdesc(rec:dict, as_new_version:bool=False)->dict:
    ensure_jobdesc_sheet()
    ws=_ws_cached(JOBDESC_SHEET)
    header=_retry_call(ws.row_values,1); hmap={n:i+1 for i,n in enumerate(header)}
    sabun=str(rec.get("사번","")).strip(); year=int(rec.get("연도",0))
    if as_new_version:
        ver=_jobdesc_next_version(sabun,year)
    else:
        try_ver=int(str(rec.get("버전",0) or 0))
        if try_ver<=0: ver=_jobdesc_next_version(sabun,year)
        else:
            df=read_jobdesc_df()
            exist=not df[(df["사번"]==sabun)&(df["연도"].astype(int)==year)&(df["버전"].astype(int)==try_ver)].empty
            ver=try_ver if exist else 1
    rec["버전"]=int(ver); rec["제출시각"]=kst_now_str()

    values=_retry_call(ws.get_all_values); row_idx=0
    cS,cY,cV=hmap.get("사번"),hmap.get("연도"),hmap.get("버전")
    for i in range(2,len(values)+1):
        row=values[i-1]
        if str(row[cS-1]).strip()==sabun and str(row[cY-1]).strip()==str(year) and str(row[cV-1]).strip()==str(ver):
            row_idx=i; break

    def build_row():
        buf=[""]*len(header)
        for k,v in rec.items():
            c=hmap.get(k)
            if c: buf[c-1]=v
        return buf

    if row_idx==0:
        _retry_call(ws.append_row, build_row(), value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action":"insert","version":ver}
    else:
        for k,v in rec.items():
            c=hmap.get(k)
            if c: _retry_call(ws.update_cell, row_idx, c, v)
        st.cache_data.clear()
        return {"action":"update","version":ver}

def tab_job_desc(emp_df: pd.DataFrame):
    st.subheader("직무기술서")
    def _jd_latest_for(sabun: str, year: int) -> dict | None:
        try: df = read_jobdesc_df()
        except Exception: return None
        if df.empty: return None
        sub = df[(df["사번"].astype(str) == str(sabun)) & (df["연도"].astype(int) == int(year))].copy()
        if sub.empty: return None
        try: sub["버전"] = sub["버전"].astype(int)
        except Exception: pass
        sub = sub.sort_values(["버전"], ascending=[False]).reset_index(drop=True)
        row = sub.iloc[0].to_dict()
        for k, v in row.items(): row[k] = "" if v is None else str(v)
        return row
    def _jd_blank_from_emp(emp_df: pd.DataFrame, sabun: str, year: int) -> dict:
        me = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
        r = me.iloc[0] if not me.empty else {}
        today = datetime.now(tz=tz_kst()).date().strftime("%Y-%m-%d")
        def _g(key, default=""):
            try: return str(r.get(key, default))
            except Exception: return str(default)
        return {
            "사번": str(sabun), "연도": int(year), "버전": 0,
            "부서1": _g("부서1"), "부서2": _g("부서2"),
            "작성자사번": str(sabun), "작성자이름": _emp_name_by_sabun(emp_df, sabun),
            "직군": _g("직군"), "직종": _g("직종"), "직무명": _g("직무"),
            "제정일": get_setting("JD.제정일", today), "개정일": get_setting("JD.개정일", today),
            "검토주기": get_setting("JD.검토주기", "1년"),
            "직무개요": "", "주업무": "", "기타업무": "",
            "필요학력": "", "전공계열": "", "직원공통필수교육": "", "보수교육": "", "기타교육": "", "특성화교육": "",
            "면허": "", "경력(자격요건)": "", "비고": "", "서명방식": "", "서명데이터": "",
        }

    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="jd2_year")
    u = st.session_state["user"]; me_sabun = str(u["사번"]); me_name = str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or is_manager(emp_df, me_sabun))
    allowed_sabuns = get_allowed_sabuns(emp_df, me_sabun, include_self=True)
    st.session_state.setdefault("jd2_target_sabun", me_sabun)
    st.session_state.setdefault("jd2_target_name",  me_name)
    st.session_state.setdefault("jd2_edit_mode",    False)

    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name  = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base = emp_df.copy(); base["사번"] = base["사번"].astype(str)
        base = base[base["사번"].isin({str(s) for s in allowed_sabuns})]
        if "재직여부" in base.columns: base = base[base["재직여부"] == True]
        cflt = st.columns([1,1,1,2])
        with cflt[0]:
            opt_d1 = ["(전체)"] + sorted([x for x in base.get("부서1", []).dropna().unique() if x])
            f_d1 = st.selectbox("부서1", opt_d1, index=0, key="jd2_f_d1")
        with cflt[1]:
            sub = base if f_d1 == "(전체)" else base[base["부서1"].astype(str) == f_d1]
            opt_d2 = ["(전체)"] + sorted([x for x in sub.get("부서2", []).dropna().unique() if x])
            f_d2 = st.selectbox("부서2", opt_d2, index=0, key="jd2_f_d2")
        with cflt[2]:
            opt_g = ["(전체)"] + sorted([x for x in base.get("직급", []).dropna().unique() if x])
            f_g = st.selectbox("직급", opt_g, index=0, key="jd2_f_grade")
        with cflt[3]: f_q = st.text_input("검색(사번/이름)", key="jd2_f_q")
        view = base[["사번","이름","부서1","부서2","직급"]].copy()
        if f_d1 != "(전체)": view = view[view["부서1"].astype(str) == f_d1]
        if f_d2 != "(전체)": view = view[view["부서2"].astype(str) == f_d2]
        if f_g  != "(전체)": view = view[view["직급"].astype(str) == f_g]
        if f_q and f_q.strip():
            k = f_q.strip().lower()
            view = view[view.apply(lambda r: k in str(r["사번"]).lower() or k in str(r["이름"]).lower(), axis=1)]
        view = view.sort_values(["부서1","부서2","사번"]).reset_index(drop=True)
        view["선택"] = (view["사번"] == st.session_state["jd2_target_sabun"])
        edited = st.data_editor(
            view[["선택","사번","이름","부서1","부서2","직급"]],
            use_container_width=True, height=360, key="jd2_pick_editor",
            column_config={"선택": st.column_config.CheckboxColumn()}, num_rows="fixed",
        )
        picked = edited.loc[edited["선택"] == True]
        if not picked.empty:
            r = picked.iloc[-1]
            st.session_state["jd2_target_sabun"] = str(r["사번"])
            st.session_state["jd2_target_name"]  = str(r["이름"])
        target_sabun = st.session_state["jd2_target_sabun"]
        target_name  = st.session_state["jd2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    col_mode = st.columns([1,3])
    with col_mode[0]:
        if st.button(("수정모드로 전환" if not st.session_state["jd2_edit_mode"] else "보기모드로 전환"),
                     use_container_width=True, key="jd2_toggle"):
            st.session_state["jd2_edit_mode"] = not st.session_state["jd2_edit_mode"]; st.rerun()
    with col_mode[1]: st.caption(f"현재: **{'수정모드' if st.session_state['jd2_edit_mode'] else '보기모드'}**")
    edit_mode = bool(st.session_state["jd2_edit_mode"])

    jd_saved   = _jd_latest_for(target_sabun, int(year))
    jd_current = jd_saved if jd_saved else _jd_blank_from_emp(emp_df, target_sabun, int(year))
    if (not am_admin_or_mgr) and (jd_saved is None) and (not st.session_state["jd2_edit_mode"]):
        st.session_state["jd2_edit_mode"] = True; edit_mode = True

    with st.expander("현재 저장된 직무기술서 요약", expanded=False):
        st.write(f"**직무명:** {(jd_saved or {}).get('직무명','')}")
        cc = st.columns(2)
        with cc[0]: st.markdown("**주업무**");  st.write((jd_saved or {}).get("주업무","") or "—")
        with cc[1]: st.markdown("**기타업무**"); st.write((jd_saved or {}).get("기타업무","") or "—")

    col = st.columns([1,1,2,2])
    with col[0]:
        version = st.number_input("버전(없으면 자동)", min_value=0, max_value=999,
                                  value=int(str(jd_current.get("버전", 0)) or 0),
                                  step=1, key="jd2_ver", disabled=not edit_mode)
    with col[1]:
        jobname = st.text_input("직무명", value=jd_current.get("직무명",""),
                                key="jd2_jobname", disabled=not edit_mode)
    with col[2]:
        memo = st.text_input("비고", value=jd_current.get("비고",""),
                             key="jd2_memo", disabled=not edit_mode)
    with col[3]: pass

    c2 = st.columns([1,1,1,1])
    with c2[0]: dept1 = st.text_input("부서1", value=jd_current.get("부서1",""), key="jd2_dept1", disabled=not edit_mode)
    with c2[1]: dept2 = st.text_input("부서2", value=jd_current.get("부서2",""), key="jd2_dept2", disabled=not edit_mode)
    with c2[2]: group = st.text_input("직군",  value=jd_current.get("직군",""),  key="jd2_group",  disabled=not edit_mode)
    with c2[3]: series= st.text_input("직종",  value=jd_current.get("직종",""),  key="jd2_series", disabled=not edit_mode)

    c3 = st.columns([1,1,1])
    with c3[0]: d_create = st.text_input("제정일",   value=jd_current.get("제정일",""),   key="jd2_d_create", disabled=not edit_mode)
    with c3[1]: d_update = st.text_input("개정일",   value=jd_current.get("개정일",""),   key="jd2_d_update", disabled=not edit_mode)
    with c3[2]: review   = st.text_input("검토주기", value=jd_current.get("검토주기",""), key="jd2_review",   disabled=not edit_mode)

    job_summary = st.text_area("직무개요", value=jd_current.get("직무개요",""), height=80,  key="jd2_summary", disabled=not edit_mode)
    job_main    = st.text_area("주업무",   value=jd_current.get("주업무",""),   height=120, key="jd2_main",    disabled=not edit_mode)
    job_other   = st.text_area("기타업무", value=jd_current.get("기타업무",""), height=80,  key="jd2_other",   disabled=not edit_mode)

    c4 = st.columns([1,1,1,1,1,1])
    with c4[0]: edu_req    = st.text_input("필요학력",        value=jd_current.get("필요학력",""),        key="jd2_edu",        disabled=not edit_mode)
    with c4[1]: major_req  = st.text_input("전공계열",        value=jd_current.get("전공계열",""),        key="jd2_major",      disabled=not edit_mode)
    with c4[2]: edu_common = st.text_input("직원공통필수교육", value=jd_current.get("직원공통필수교육",""), key="jd2_edu_common", disabled=not edit_mode)
    with c4[3]: edu_cont   = st.text_input("보수교육",        value=jd_current.get("보수교육",""),        key="jd2_edu_cont",   disabled=not edit_mode)
    with c4[4]: edu_etc    = st.text_input("기타교육",        value=jd_current.get("기타교육",""),        key="jd2_edu_etc",    disabled=not edit_mode)
    with c4[5]: edu_spec   = st.text_input("특성화교육",      value=jd_current.get("특성화교육",""),      key="jd2_edu_spec",   disabled=not edit_mode)

    c5 = st.columns([1,1,2])
    with c5[0]: license_ = st.text_input("면허", value=jd_current.get("면허",""), key="jd2_license", disabled=not edit_mode)
    with c5[1]: career   = st.text_input("경력(자격요건)", value=jd_current.get("경력(자격요건)",""), key="jd2_career", disabled=not edit_mode)
    with c5[2]: pass

    c6 = st.columns([1,2,1])
    with c6[0]:
        _opt = ["", "text", "image"]
        _sv  = jd_current.get("서명방식","")
        _idx = _opt.index(_sv) if _sv in _opt else 0
        sign_type = st.selectbox("서명방식", _opt, index=_idx, key="jd2_sign_type", disabled=not edit_mode)
    with c6[1]:
        sign_data = st.text_input("서명데이터", value=jd_current.get("서명데이터",""), key="jd2_sign_data", disabled=not edit_mode)
    with c6[2]:
        do_save = st.button("저장/업서트", type="primary", use_container_width=True, key="jd2_save", disabled=not edit_mode)

    if do_save:
        rec = {
            "사번": str(target_sabun), "연도": int(year), "버전": int(version or 0),
            "부서1": dept1, "부서2": dept2, "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
            "직군": group, "직종": series, "직무명": jobname,
            "제정일": d_create, "개정일": d_update, "검토주기": review,
            "직무개요": job_summary, "주업무": job_main, "기타업무": job_other,
            "필요학력": edu_req, "전공계열": major_req,
            "직원공통필수교육": edu_common, "보수교육": edu_cont, "기타교육": edu_etc, "특성화교육": edu_spec,
            "면허": license_, "경력(자격요건)": career, "비고": memo, "서명방식": sign_type, "서명데이터": sign_data,
        }
        try:
            rep = upsert_jobdesc(rec, as_new_version=(version == 0))
            st.success(f"저장 완료 (버전 {rep['version']})", icon="✅"); st.rerun()
        except Exception as e:
            st.exception(e)

# ======================================================================
# 📌 직무능력평가(Competency)
# ======================================================================
# ── 직무능력평가(간편·상세) ───────────────────────────────────────────────────
# 간편형은 JD(직무기술서)와 연동, 상세형은 세부 항목 1~5점 평가 (UI에서 '가중치' 미노출)

COMP_SIMPLE_PREFIX = "직무능력_간편_응답_"
COMP_SIMPLE_HEADERS = [
    "연도","평가대상사번","평가대상이름","평가자사번","평가자이름",
    "평가일자","주업무평가","기타업무평가","교육이수","자격유지","종합의견",
    "상태","제출시각","잠금"
]

def _simp_sheet_name(year:int|str)->str:
    return f"{COMP_SIMPLE_PREFIX}{int(year)}"

def _ensure_comp_simple_sheet(year:int):
    wb = get_workbook()
    name = _simp_sheet_name(year)
    try:
        ws = wb.worksheet(name)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=name, rows=1000, cols=50)
        ws.update("1:1", [COMP_SIMPLE_HEADERS])
        return ws
    header = ws.row_values(1) or []
    need = [h for h in COMP_SIMPLE_HEADERS if h not in header]
    if need:
        ws.update("1:1", [header + need])
    return ws

def _jd_latest_for(sabun:str, year:int) -> dict:
    """대상자의 직무기술서(해당 연도, 최대 버전) 1건 반환 또는 {}."""
    try:
        df = read_jobdesc_df()
        if df.empty: return {}
        q = df[(df["사번"].astype(str)==str(sabun)) & (df["연도"].astype(int)==int(year))]
        if q.empty: return {}
        q = q.sort_values("버전").iloc[-1]
        return {c: q.get(c, "") for c in q.index}
    except Exception:
        return {}

def _edu_completion_from_jd(jd_row:dict) -> str:
    """직원공통필수교육이 비어있지 않으면 '완료', 아니면 '미완료'."""
    val = str(jd_row.get("직원공통필수교육","")).strip()
    return "완료" if val else "미완료"

def upsert_comp_simple_response(
    emp_df: pd.DataFrame,
    year: int,
    target_sabun: str,
    evaluator_sabun: str,
    main_grade: str,   # A/B/C/D/E
    extra_grade: str,  # '해당없음' 또는 A/B/C/D/E
    qual_status: str,  # 직무 유지 / 직무 변경 / 직무비부여
    opinion: str,
    eval_date: str     # YYYY-MM-DD
) -> dict:
    ws = _ensure_comp_simple_sheet(year)
    header = ws.row_values(1) or COMP_SIMPLE_HEADERS
    hmap = {n:i+1 for i,n in enumerate(header)}

    # 교육이수: JD 기준 자동
    jd = _jd_latest_for(target_sabun, int(year))
    edu_status = _edu_completion_from_jd(jd)

    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = kst_now_str()

    # upsert 키: 연도 + 평가대상사번 + 평가자사번
    values = ws.get_all_values()
    cY   = hmap.get("연도")
    cTS  = hmap.get("평가대상사번")
    cES  = hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values)+1):
        r = values[i-1]
        try:
            if (str(r[cY-1]).strip()==str(year)
                and str(r[cTS-1]).strip()==str(target_sabun)
                and str(r[cES-1]).strip()==str(evaluator_sabun)):
                row_idx = i
                break
        except:
            pass

    if row_idx == 0:
        buf = [""]*len(header)
        def put(k,v):
            c=hmap.get(k)
            if c: buf[c-1]=v
        put("연도", int(year))
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("평가일자", eval_date)
        put("주업무평가", main_grade)
        put("기타업무평가", extra_grade)
        put("교육이수", edu_status)
        put("자격유지", qual_status)
        put("종합의견", opinion)
        put("상태", "제출")
        put("제출시각", now)
        put("잠금", "")
        ws.append_row(buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action":"insert"}
    else:
        def upd(k,v):
            c=hmap.get(k)
            if c: ws.update_cell(row_idx, c, v)
        upd("평가일자", eval_date)
        upd("주업무평가", main_grade)
        upd("기타업무평가", extra_grade)
        upd("교육이수", edu_status)
        upd("자격유지", qual_status)
        upd("종합의견", opinion)
        upd("상태", "제출")
        upd("제출시각", now)
        st.cache_data.clear()
        return {"action":"update"}

@st.cache_data(ttl=60, show_spinner=False)
def read_my_comp_simple_rows(year:int, sabun:str)->pd.DataFrame:
    try:
        ws = get_workbook().worksheet(_simp_sheet_name(year))
        df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    except Exception:
        return pd.DataFrame(columns=COMP_SIMPLE_HEADERS)
    if df.empty: return df
    df = df[df["평가자사번"].astype(str)==str(sabun)]
    sort_cols = [c for c in ["평가대상사번","평가일자","제출시각"] if c in df.columns]
    if sort_cols: df = df.sort_values(sort_cols, ascending=[True, False, False])
    return df


# ── 상세(선택) 평가 — 시트에 '가중치' 컬럼이 없어도 동작(있으면 내부 계산에만 사용) ──
COMP_ITEM_SHEET = "직무능력_항목"
# 최소 헤더만 보장 (가중치 비강제)
COMP_ITEM_HEADERS_MIN = ["항목ID","영역","항목","내용","순서","활성","비고"]

COMP_RESP_PREFIX   = "직무능력_응답_"
COMP_BASE_HEADERS  = ["연도","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각"]

def ensure_comp_items_sheet():
    """항목 시트가 없으면 생성(최소 헤더만). 기존 시트는 존중, '가중치'는 강제 추가하지 않음."""
    wb = get_workbook()
    try:
        ws = wb.worksheet(COMP_ITEM_SHEET)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=COMP_ITEM_SHEET, rows=200, cols=12)
        ws.update("1:1", [COMP_ITEM_HEADERS_MIN])
        return ws
    header = ws.row_values(1) or []
    need = [h for h in COMP_ITEM_HEADERS_MIN if h not in header]
    if need:
        ws.update("1:1", [header + need])
    return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_comp_items_df(only_active=True)->pd.DataFrame:
    ensure_comp_items_sheet()
    ws = get_workbook().worksheet(COMP_ITEM_SHEET)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty:
        return pd.DataFrame(columns=list(COMP_ITEM_HEADERS_MIN))
    # 숫자화
    if "순서" in df.columns:
        def _n(x):
            try: return int(float(str(x).strip()))
            except: return 0
        df["순서"] = df["순서"].apply(_n)
    # 활성 bool
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    # 정렬/필터
    sort_cols = [c for c in ["영역","순서","항목"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    if only_active and "활성" in df.columns:
        df = df[df["활성"]==True]
    return df

def _comp_sheet_name(year:int|str)->str:
    return f"{COMP_RESP_PREFIX}{int(year)}"

def _ensure_comp_resp_sheet(year:int, item_ids:list[str])->gspread.Worksheet:
    wb = get_workbook(); name = _comp_sheet_name(year)
    try:
        ws = wb.worksheet(name)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=name, rows=2000, cols=100)
        ws.update("1:1", [COMP_BASE_HEADERS + [f"점수_{iid}" for iid in item_ids]])
        return ws
    header = ws.row_values(1) or []
    need = list(COMP_BASE_HEADERS) + [f"점수_{iid}" for iid in item_ids]
    add  = [h for h in need if h not in header]
    if add:
        ws.update("1:1", [header + add])
    return ws

def upsert_comp_response(
    emp_df:pd.DataFrame, year:int, target_sabun:str, evaluator_sabun:str,
    scores:dict[str,int], status:str="제출"
)->dict:
    items = read_comp_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_comp_resp_sheet(year, item_ids)
    header = ws.row_values(1); hmap = {n:i+1 for i,n in enumerate(header)}

    # 내부 계산용 weight (시트에 '가중치'가 없으면 모두 동일 가중)
    if "가중치" in items.columns:
        def _w(x):
            try: return float(str(x).strip())
            except: return 0.0
        weights = [max(0.0, _w(items.loc[items["항목ID"]==iid, "가중치"].iloc[0])) if not items[items["항목ID"]==iid].empty else 0.0
                   for iid in item_ids]
    else:
        weights = [1.0 for _ in item_ids]

    wsum = sum(weights) if sum(weights) > 0 else float(len(item_ids))
    total_norm = 0.0
    for iid, w in zip(item_ids, weights):
        s = scores.get(iid, 0)
        try: s = int(s)
        except: s = 0
        s = min(5, max(1, s)) if s else 0
        total_norm += (s/5.0) * (w if wsum>0 else 1.0)

    total_100 = round((total_norm / (wsum if wsum>0 else max(1,len(item_ids)))) * 100.0, 1)

    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now    = kst_now_str()

    # upsert 키: 연도 + 평가대상사번 + 평가자사번
    values = ws.get_all_values()
    cY  = hmap.get("연도"); cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values)+1):
        r = values[i-1]
        try:
            if str(r[cY-1]).strip()==str(year) and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun):
                row_idx = i; break
        except:
            pass

    if row_idx == 0:
        buf = [""]*len(header)
        def put(k,v):
            c = hmap.get(k)
            if c: buf[c-1]=v
        put("연도", int(year)); put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("총점", total_100); put("상태", status); put("제출시각", now)
        for iid in item_ids:
            c = hmap.get(f"점수_{iid}")
            if c: buf[c-1] = int(scores.get(iid,0) or 0)
        ws.append_row(buf, value_input_option="USER_ENTERED"); st.cache_data.clear()
        return {"action":"insert","total":total_100}
    else:
        payload = {
            "총점": total_100, "상태": status, "제출시각": now,
            "평가대상이름": t_name, "평가자이름": e_name,
        }
        for iid in item_ids:
            payload[f"점수_{iid}"] = int(scores.get(iid, 0) or 0)
        _batch_update_row(ws, row_idx, hmap, payload)
        st.cache_data.clear()
        return {"action":"update","total":total_100}


def tab_competency(emp_df: pd.DataFrame):
    st.subheader("직무능력평가")

    tabs = st.tabs(["간편(직무기술서 연동)", "상세"])

    # ──────────────────────────────────────────────────────────────────────────
    # 1) 간편(직무기술서 연동)
    # ──────────────────────────────────────────────────────────────────────────
    with tabs[0]:
        this_year = datetime.now(tz=tz_kst()).year
        colY = st.columns([1,3])
        with colY[0]:
            year = st.number_input("평가 연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmpS_year")

        # 대상은 내 'evaluator 권한'으로 필터 (관리자는 전체 가능)
        u = st.session_state["user"]
        me_sabun = str(u["사번"]); me_name = str(u["이름"])

        st.markdown("#### 평가 대상 선택")
        evaluable = get_evaluable_targets(emp_df, me_sabun)
        df = emp_df.copy()
        df = df[df["사번"].astype(str).isin(evaluable)]
        if "재직여부" in df.columns:
            df = df[df["재직여부"] == True]

        if df.empty:
            st.warning("현재 맡은 팀(또는 부서)에 대한 평가 권한이 없습니다. (관리자는 전체 가능 / evaluator 권한 필요)", icon="⚠️")
            return

        df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df = df.sort_values(["사번"])
        sel = st.selectbox("평가 대상자 (사번 - 이름)", ["(선택)"] + df["표시"].tolist(), index=0, key="cmpS_target")
        if sel == "(선택)":
            st.info("평가 대상자를 선택하세요.")
            return
        target_sabun = sel.split(" - ", 1)[0]
        target_name = _emp_name_by_sabun(emp_df, target_sabun)

        # JD 요약
        jd = _jd_latest_for(target_sabun, int(year))
        with st.expander("직무기술서 요약", expanded=True):
            st.write(f"**직무명:** {jd.get('직무명','') if jd else ''}")
            c = st.columns(2)
            with c[0]:
                st.markdown("**주업무**")
                st.write((jd.get("주업무","") or "").strip() or "—")
            with c[1]:
                st.markdown("**기타업무**")
                st.write((jd.get("기타업무","") or "").strip() or "—")
            st.caption("※ 해당 연도의 직무기술서 최신 버전을 표시합니다. (없으면 빈 값)")

        # 입력 UI
        st.markdown("#### 평가 입력")
        grade_options = [("A","탁월(A)"), ("B","우수(B)"), ("C","보통(C)"), ("D","부족(D)"), ("E","저조(E)")]
        grade_labels  = [lbl for _, lbl in grade_options]
        label2code    = {lbl:code for code,lbl in grade_options}

        colG = st.columns([1,1,1,2])
        with colG[0]:
            g_main_lbl = st.radio("주업무", grade_labels, index=2, horizontal=False, key="cmpS_main")
            g_main = label2code[g_main_lbl]
        with colG[1]:
            extra_labels = ["해당없음"] + grade_labels
            g_extra_lbl = st.radio("기타업무", extra_labels, index=0, horizontal=False, key="cmpS_extra")
            g_extra = "해당없음" if g_extra_lbl=="해당없음" else label2code[g_extra_lbl]
        with colG[2]:
            qual = st.radio("직무 자격 유지 여부", ["직무 유지","직무 변경","직무비부여"], index=0, key="cmpS_qual")
        with colG[3]:
            eval_date = st.date_input("평가일자", datetime.now(tz=tz_kst()).date(), key="cmpS_date").strftime("%Y-%m-%d")

        edu_status = _edu_completion_from_jd(jd)
        st.metric("교육이수 (자동)", edu_status)

        opinion = st.text_area("종합평가 의견", value="", height=150, key="cmpS_opinion")

        cbtn = st.columns([1,1,3])
        with cbtn[0]:
            do_save = st.button("제출/저장", type="primary", use_container_width=True, key="cmpS_save")
        with cbtn[1]:
            do_reset = st.button("초기화", use_container_width=True, key="cmpS_reset")

        if do_reset:
            for k in ["cmpS_main","cmpS_extra","cmpS_qual","cmpS_opinion"]:
                if k in st.session_state: del st.session_state[k]
            st.rerun()

        if do_save:
            try:
                rep = upsert_comp_simple_response(
                    emp_df, int(year), str(target_sabun), str(me_sabun),
                    g_main, g_extra, qual, opinion, eval_date
                )
                st.success(("제출 완료" if rep["action"]=="insert" else "업데이트 완료"), icon="✅")
                st.toast("직무능력평가 저장됨", icon="✅")
            except Exception as e:
                st.exception(e)

        st.markdown("#### 내 제출 현황")
        try:
            my = read_my_comp_simple_rows(int(year), me_sabun)
            if my.empty:
                st.caption("제출된 평가가 없습니다.")
            else:
                st.dataframe(
                    my[["평가대상사번","평가대상이름","평가일자","주업무평가","기타업무평가","교육이수","자격유지","상태","제출시각"]],
                    use_container_width=True, height=260
                )
        except Exception:
            st.caption("제출 현황을 불러오지 못했습니다.")

    # ──────────────────────────────────────────────────────────────────────────
    # 2) 상세(선택) — UI에 '가중치' 용어 미노출 (있어도 내부 계산만)
    # ──────────────────────────────────────────────────────────────────────────
    with tabs[1]:
        this_year = datetime.now(tz=tz_kst()).year
        colY = st.columns([1,3])
        with colY[0]:
            year = st.number_input("평가 연도(상세)", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmpW_year")

        items = read_comp_items_df(only_active=True)
        if items.empty:
            st.warning("활성화된 상세 평가 항목이 없습니다.", icon="⚠️")
            return

        u = st.session_state["user"]
        me_sabun = str(u["사번"]); me_name = str(u["이름"])

        st.markdown("#### 대상 선택(상세)")
        evaluable = get_evaluable_targets(emp_df, me_sabun)
        df = emp_df.copy()
        df = df[df["사번"].astype(str).isin(evaluable)]
        if "재직여부" in df.columns:
            df = df[df["재직여부"] == True]

        if df.empty:
            st.warning("현재 맡은 팀(또는 부서)에 대한 평가 권한이 없습니다. (관리자는 전체 가능 / evaluator 권한 필요)", icon="⚠️")
            return

        df["표시"]=df.apply(lambda r:f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df=df.sort_values(["사번"])
        sel=st.selectbox("평가 대상자(상세) (사번 - 이름)", ["(선택)"]+df["표시"].tolist(), index=0, key="cmpW_target")
        if sel=="(선택)":
            st.info("평가 대상자를 선택하세요.")
            return

        target_sabun=sel.split(" - ",1)[0]
        target_name=_emp_name_by_sabun(emp_df, target_sabun)
        evaluator_sabun=me_sabun
        evaluator_name=me_name

        st.markdown("#### 점수 입력(상세)")
        st.caption("각 항목 1~5점입니다. 내부 반영 규칙이 있더라도 화면에는 표시하지 않습니다.")
        st.markdown(
            """
            <style>
              .cmp-grid{display:grid;grid-template-columns:2fr 7fr 3fr;gap:.5rem;
                        align-items:center;padding:10px 6px;border-bottom:1px solid rgba(49,51,63,.10)}
              .cmp-grid .name{font-weight:700}
              .cmp-grid .desc{color:#4b5563}
              .cmp-grid .input{display:flex;align-items:center;justify-content:center}
              .cmp-grid .input div[role="radiogroup"]{display:flex;gap:10px;align-items:center;justify-content:center}
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown('<div class="grid-head">영역/항목 / 내용 / 점수</div>', unsafe_allow_html=True)

        items_sorted=items.sort_values(["영역","순서","항목"]).reset_index(drop=True)
        scores={}
        for r in items_sorted.itertuples(index=False):
            iid=getattr(r,"항목ID"); area=getattr(r,"영역") if "영역" in items.columns else ""
            name=getattr(r,"항목") if "항목" in items.columns else str(iid)
            desc=getattr(r,"내용") if "내용" in items.columns else ""

            cur=int(st.session_state.get(f"cmp_{iid}",3))
            if cur<1 or cur>5: cur=3

            st.markdown('<div class="cmp-grid">', unsafe_allow_html=True)
            st.markdown(f'<div class="name">{"["+str(area)+"] " if str(area) else ""}{name}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="desc">{str(desc).replace(chr(10), "<br/>")}</div>', unsafe_allow_html=True)
            st.markdown('<div class="input">', unsafe_allow_html=True)
            new_val = int(st.radio(" ", ["1","2","3","4","5"], index=(cur-1), horizontal=True, key=f"cmp_seg_{iid}", label_visibility="collapsed"))
            st.markdown('</div></div>', unsafe_allow_html=True)

            v=min(5,max(1,int(new_val))); scores[str(iid)]=v; st.session_state[f"cmp_{iid}"]=v

        # 총점 계산 (내부적으로 '가중치'가 있으면 자동 반영하되 UI 미노출)
        if len(items_sorted)>0:
            rep = upsert_comp_response(emp_df, int(year), str(target_sabun), str(evaluator_sabun), scores, "제출") if st.button(
                "제출/저장(상세)", type="primary", use_container_width=True, key="cmpW_save"
            ) else None
            # 미리보기 총점
            try:
                # 임시 계산 재사용을 위해 한 번 더 계산
                tmp = upsert_comp_response(emp_df, int(year), str(target_sabun), str(evaluator_sabun), scores, "미리보기")
                total_100 = tmp["total"]
            except Exception:
                total_100 = round(sum(scores.values()) * (100.0 / max(1, len(items_sorted) * 5)), 1)

            st.markdown("---")
            cM1,cM2=st.columns([1,3])
            with cM1: st.metric("합계(100점 만점)", total_100)
            with cM2: st.progress(min(1.0,total_100/100.0), text=f"총점 {total_100}점")

            if rep:
                st.success(("제출 완료" if rep["action"]=="insert" else "업데이트 완료")+f" (총점 {rep['total']}점)", icon="✅")
                st.toast("직무능력평가 저장됨(상세)", icon="✅")
        else:
            st.info("평가 항목이 없습니다.", icon="ℹ️")

        st.markdown("#### 내 제출 현황(상세)")
        try:
            my=read_my_comp_rows(int(year), evaluator_sabun)
            if my.empty: st.caption("제출된 평가가 없습니다.")
            else: st.dataframe(my[["평가대상사번","평가대상이름","총점","상태","제출시각"]], use_container_width=True, height=260)
        except Exception:
            st.caption("제출 현황을 불러오지 못했습니다.")


@st.cache_data(ttl=60, show_spinner=False)
def read_my_comp_rows(year:int, sabun:str)->pd.DataFrame:
    name=_comp_sheet_name(year)
    try:
        ws=get_workbook().worksheet(name)
    except Exception:
        return pd.DataFrame(columns=COMP_BASE_HEADERS)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: return df
    df=df[df["평가자사번"].astype(str)==str(sabun)]
    return df.sort_values(["평가대상사번","제출시각"], ascending=[True,False])


# ======================================================================
# 📌 권한관리(Admin / ACL & Admin Tools)
# ======================================================================
# ── 부서이력/이동(필수 최소) ──────────────────────────────────────────────────
HIST_SHEET="부서이력"
def ensure_dept_history_sheet():
    """
    부서(근무지) 이동 이력 시트 보장 + 헤더 정렬.
    gspread 호출은 캐시/재시도를 사용해 429를 완화합니다.
    """
    # 프로젝트에 이미 선언돼 있던 상수를 그대로 사용하세요.
    # HIST_SHEET = "부서이동이력"

    # 1) 시트 객체(캐시)
    try:
        ws = _ws_cached(HIST_SHEET)
    except WorksheetNotFound:
        wb = get_workbook()
        ws = _retry_call(wb.add_worksheet, title=HIST_SHEET, rows=5000, cols=30)
        _WS_CACHE[HIST_SHEET] = (time.time(), ws)

    # 2) 헤더 보정
    default_headers = [
        "사번", "이름",
        "부서1", "부서2",
        "시작일", "종료일"
        "변경사유", "승인자", "메모"
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
def read_dept_history_df()->pd.DataFrame:
    ensure_dept_history_sheet(); ws=get_workbook().worksheet(HIST_SHEET)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: return df
    for c in ["시작일","종료일","등록시각"]:
        if c in df.columns: df[c]=df[c].astype(str)
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    return df

def apply_department_change(
    emp_df: pd.DataFrame,
    sabun: str,
    new_dept1: str,
    new_dept2: str,
    start_date: datetime.date,
    reason: str = "",
    approver: str = "",
) -> dict:
    # 1) 이력 시트 핸들(캐시 사용)
    ws_hist = ensure_dept_history_sheet()  # ← 여기서 ws 반환받음(재조회 금지)

    # 2) 날짜 계산
    start_str = start_date.strftime("%Y-%m-%d")
    prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

    # 3) 대상자 기본 정보
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if row.empty:
        raise RuntimeError("사번을 찾지 못했습니다.")
    name = str(row.iloc[0].get("이름", ""))

    # 4) 헤더/맵(캐시) + 기존 미종료 레코드 종료일 업데이트
    header, hmap = _sheet_header_cached(ws_hist, HIST_SHEET)
    values = _retry_call(ws_hist.get_all_values)  # 전체 1회 읽기

    cS = hmap.get("사번")
    cE = hmap.get("종료일")  # ※ 이 컬럼명이 이력 시트에 실제로 존재해야 합니다.
    if cS and cE:
        for i in range(2, len(values) + 1):
            row_i = values[i - 1]
            try:
                if str(row_i[cS - 1]).strip() == str(sabun).strip() and str(row_i[cE - 1]).strip() == "":
                    _retry_call(ws_hist.update_cell, i, cE, prev_end)
            except IndexError:
                # 행 길이가 짧은 경우(빈 셀 꼬임) 무시
                continue

    # 5) 신규 이력 추가
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

    # 6) 효력 즉시 반영(오늘 이후면 미적용)
    applied = False
    if start_date <= datetime.now(tz=tz_kst()).date():
        # EMP 시트도 캐시 헬퍼 사용
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

def sync_current_department_from_history(as_of_date:datetime.date=None)->int:
    ensure_dept_history_sheet()
    hist=read_dept_history_df(); emp=read_sheet_df(EMP_SHEET)
    if as_of_date is None: as_of_date=datetime.now(tz=tz_kst()).date()
    D=as_of_date.strftime("%Y-%m-%d")
    updates={}
    for sabun, grp in hist.groupby("사번"):
        def ok(row):
            s=row.get("시작일",""); e=row.get("종료일","")
            return (s and s<=D) and ((not e) or e>=D)
        cand=grp[grp.apply(ok, axis=1)]
        if cand.empty: continue
        cand=cand.sort_values("시작일").iloc[-1]
        updates[str(sabun)]=(str(cand.get("부서1","")), str(cand.get("부서2","")))
    if not updates: return 0
    ws_emp, header_emp, hmap_emp = _get_ws_and_headers(EMP_SHEET)
    changed=0
    for _, r in emp.iterrows():
        sabun=str(r.get("사번",""))
        if sabun in updates:
            d1,d2=updates[sabun]; row_idx=_find_row_by_sabun(ws_emp,hmap_emp,sabun)
            if row_idx>0:
                if "부서1" in hmap_emp: _update_cell(ws_emp,row_idx,hmap_emp["부서1"],d1)
                if "부서2" in hmap_emp: _update_cell(ws_emp,row_idx,hmap_emp["부서2"],d2)
                changed+=1
    st.cache_data.clear(); return changed

# ── 관리자: PIN / 부서이동 / 평가항목 / 권한 ─────────────────────────────────
def _random_pin(length=6)->str:
    return "".join(pysecrets.choice("0123456789") for _ in range(length))

def tab_admin_pin(emp_df: pd.DataFrame):
    st.markdown("### PIN 관리")
    df=emp_df.copy(); df["표시"]=df.apply(lambda r:f"{str(r.get('사번',''))} - {str(r.get('이름',''))}",axis=1)
    df=df.sort_values(["사번"])
    sel=st.selectbox("직원 선택(사번 - 이름)", ["(선택)"]+df["표시"].tolist(), index=0, key="adm_pin_pick")
    if sel!="(선택)":
        sabun=sel.split(" - ",1)[0]; row=df.loc[df["사번"].astype(str)==str(sabun)].iloc[0]
        st.write(f"사번: **{sabun}** / 이름: **{row.get('이름','')}**")
        pin1=st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
        pin2=st.text_input("새 PIN 확인", type="password", key="adm_pin2")
        col=st.columns([1,1,2])
        with col[0]: do_save=st.button("PIN 저장/변경", type="primary", use_container_width=True, key="adm_pin_save")
        with col[1]: do_clear=st.button("PIN 비우기", use_container_width=True, key="adm_pin_clear")
        if do_save:
            if not pin1 or not pin2: st.error("PIN을 두 번 모두 입력하세요."); return
            if pin1!=pin2: st.error("PIN 확인이 일치하지 않습니다."); return
            if not pin1.isdigit(): st.error("PIN은 숫자만 입력하세요."); return
            if not _to_bool(row.get("재직여부",False)): st.error("퇴직자는 변경할 수 없습니다."); return
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap: st.error(f"'{EMP_SHEET}' 시트에 PIN_hash가 없습니다."); return
            r=_find_row_by_sabun(ws,hmap,sabun)
            if r==0: st.error("시트에서 사번을 찾지 못했습니다."); return
            _update_cell(ws, r, hmap["PIN_hash"], _pin_hash(pin1.strip(), str(sabun)))
            st.cache_data.clear()
            st.success("PIN 저장 완료", icon="✅")
        if do_clear:
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
            if "PIN_hash" not in hmap: st.error(f"'{EMP_SHEET}' 시트에 PIN_hash가 없습니다."); return
            r=_find_row_by_sabun(ws,hmap,sabun)
            if r==0: st.error("시트에서 사번을 찾지 못했습니다."); return
            _update_cell(ws, r, hmap["PIN_hash"], ""); st.cache_data.clear()
            st.success("PIN 초기화 완료", icon="✅")

    st.divider()
    st.markdown("#### 전 직원 일괄 PIN 발급")
    col=st.columns([1,1,1,1,2])
    with col[0]: only_active=st.checkbox("재직자만", True, key="adm_pin_only_active")
    with col[1]: only_empty=st.checkbox("PIN 미설정자만", True, key="adm_pin_only_empty")
    with col[2]: overwrite_all=st.checkbox("기존 PIN 덮어쓰기", False, disabled=only_empty, key="adm_pin_overwrite")
    with col[3]: pin_len=st.number_input("자릿수", min_value=4, max_value=8, value=6, step=1, key="adm_pin_len")
    with col[4]: uniq=st.checkbox("서로 다른 PIN 보장", True, key="adm_pin_uniq")
    candidates=emp_df.copy()
    if only_active and "재직여부" in candidates.columns: candidates=candidates[candidates["재직여부"]==True]
    if only_empty: candidates=candidates[(candidates["PIN_hash"].astype(str).str.strip()=="")]
    elif not overwrite_all: st.warning("'PIN 미설정자만' 또는 '덮어쓰기' 중 하나 선택 필요", icon="⚠️")
    candidates=candidates.copy(); candidates["사번"]=candidates["사번"].astype(str)
    st.write(f"대상자 수: **{len(candidates):,}명**")
    col2=st.columns([1,1,2,2])
    with col2[0]: do_preview=st.button("미리보기 생성", use_container_width=True, key="adm_pin_prev")
    with col2[1]: do_issue=st.button("발급 실행(시트 업데이트)", type="primary", use_container_width=True, key="adm_pin_issue")
    preview=None
    if do_preview or do_issue:
        if len(candidates)==0: st.warning("대상자가 없습니다.", icon="⚠️")
        else:
            used=set(); new_pins=[]
            for _ in range(len(candidates)):
                while True:
                    p=_random_pin(pin_len)
                    if not uniq or p not in used:
                        used.add(p); new_pins.append(p); break
            preview=candidates[["사번","이름"]].copy(); preview["새_PIN"]=new_pins
            st.dataframe(preview, use_container_width=True, height=360)
            full=emp_df[["사번","이름"]].copy(); full["사번"]=full["사번"].astype(str)
            join_src=preview[["사번","새_PIN"]].copy(); join_src["사번"]=join_src["사번"].astype(str)
            csv_df=full.merge(join_src, on="사번", how="left"); csv_df["새_PIN"]=csv_df["새_PIN"].fillna("")
            csv_df=csv_df.sort_values("사번")
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

            # 시트 내 사번 → 행번호 맵 구성
            sabun_values = _retry_call(ws.col_values, sabun_col)[1:]
            pos = {str(v).strip(): i for i, v in enumerate(sabun_values, start=2)}

            # 업데이트 payload 구성(솔트 적용)
            updates = []
            for _, row in preview.iterrows():
                sabun = str(row["사번"]).strip()
                r_idx = pos.get(sabun, 0)
                if r_idx:
                    a1 = gspread.utils.rowcol_to_a1(r_idx, pin_col)
                    hashed = _pin_hash(str(row["새_PIN"]), sabun)  # ← 솔트(사번) 적용
                    updates.append({"range": a1, "values": [[hashed]]})

            if not updates:
                st.warning("업데이트할 대상이 없습니다.", icon="⚠️")
                return

            # 배치 반영 + 진행률(정확도 개선)
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

def tab_admin_transfer(emp_df: pd.DataFrame):
    st.markdown("### 부서(근무지) 이동")
    df=emp_df.copy(); df["표시"]=df.apply(lambda r:f"{str(r.get('사번',''))} - {str(r.get('이름',''))}",axis=1); df=df.sort_values(["사번"])
    sel=st.selectbox("직원 선택(사번 - 이름)", ["(선택)"]+df["표시"].tolist(), index=0, key="adm_tr_pick")
    if sel=="(선택)": st.info("사번을 선택하면 이동 입력 폼이 표시됩니다."); return
    sabun=sel.split(" - ",1)[0]; target=df.loc[df["사번"].astype(str)==str(sabun)].iloc[0]
    c=st.columns([1,1,1,1])
    with c[0]: st.metric("사번", str(target.get("사번","")))
    with c[1]: st.metric("이름", str(target.get("이름","")))
    with c[2]: st.metric("현재 부서1", str(target.get("부서1","")))
    with c[3]: st.metric("현재 부서2", str(target.get("부서2","")))
    st.divider()
    opt_d1=sorted([x for x in emp_df.get("부서1",[]).dropna().unique() if x])
    opt_d2=sorted([x for x in emp_df.get("부서2",[]).dropna().unique() if x])
    col=st.columns([1,1,1])
    with col[0]: start_date=st.date_input("시작일(발령일)", datetime.now(tz=tz_kst()).date(), key="adm_tr_start")
    with col[1]: new_d1=st.selectbox("새 부서1(선택 또는 직접입력)", ["(직접입력)"]+opt_d1, index=0, key="adm_tr_d1_pick")
    with col[2]: new_d2=st.selectbox("새 부서2(선택 또는 직접입력)", ["(직접입력)"]+opt_d2, index=0, key="adm_tr_d2_pick")
    nd1 = st.text_input("부서1 직접입력", value="" if new_d1!="(직접입력)" else "", key="adm_tr_nd1")
    nd2 = st.text_input("부서2 직접입력", value="" if new_d2!="(직접입력)" else "", key="adm_tr_nd2")
    new_dept1 = new_d1 if new_d1!="(직접입력)" else nd1
    new_dept2 = new_d2 if new_d2!="(직접입력)" else nd2
    col2=st.columns([2,3])
    with col2[0]: reason=st.text_input("변경사유", "", key="adm_tr_reason")
    with col2[1]: approver=st.text_input("승인자", "", key="adm_tr_approver")
    if st.button("이동 기록 + 현재 반영", type="primary", use_container_width=True, key="adm_tr_apply"):
        if not (new_dept1.strip() or new_dept2.strip()): st.error("새 부서1/부서2 중 최소 하나는 입력/선택"); return
        try:
            rep=apply_department_change(emp_df, str(sabun), new_dept1.strip(), new_dept2.strip(), start_date, reason.strip(), approver.strip())
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
            cnt=sync_current_department_from_history()
            st.success(f"직원 시트 현재부서 동기화 완료: {cnt}명 반영", icon="✅")
        except Exception as e:
            st.exception(e)

def tab_admin_eval_items():
    import pandas as pd
    import streamlit as st

    st.markdown("### 평가 항목 관리")

    df = read_eval_items_df(only_active=False).copy()

    # ▶ 안전 캐스팅
    for c in ["항목ID", "항목", "내용", "비고"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "순서" in df.columns:
        df["순서"] = pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(lambda x: str(x).strip().lower() in ("true", "1", "y", "yes", "t"))

    st.write(f"현재 등록: **{len(df)}개** (활성 {df[df.get('활성', False)==True].shape[0]}개)")

    # ── 목록/순서 편집
    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        # 원본 df
        edit_df = df[["항목ID", "항목", "순서", "활성"]].copy().reset_index(drop=True)

        # 타입 정규화(React 185 방지: 순수 파이썬 타입)
        edit_df["항목ID"] = edit_df["항목ID"].astype(str)
        edit_df["항목"] = edit_df["항목"].astype(str)

        def _toi(x):
            try:
                return int(float(str(x).strip()))
            except:
                return 0

        def _tob(x):
            return str(x).strip().lower() in ("true", "1", "y", "yes", "t")

        edit_df["순서"] = edit_df["순서"].apply(_toi)
        edit_df["활성"] = edit_df["활성"].apply(_tob)

        st.caption("표에서 **순서**만 변경 가능합니다. (다른 열은 읽기 전용)")

        # ⚠️ key / num_rows 제거 → React #185 회피
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

                # 시트에서 항목ID → 행번호 맵
                id_vals = _retry_call(ws.col_values, col_id)[1:]
                pos = {str(v).strip(): i for i, v in enumerate(id_vals, start=2)}

                # 변경분만 업데이트(간단히 모두 반영해도 호출 수는 작음)
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
            except: order = 0
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
                                    except: pass
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

def tab_admin_acl(emp_df: pd.DataFrame):
    import pandas as pd
    import streamlit as st

    st.markdown("### 권한 관리")

    # 로그인 사용자 / 권한
    me = st.session_state.get("user", {})
    try:
        am_admin = is_admin(str(me.get("사번", "")))
    except Exception:
        am_admin = False

    # 데이터 로드
    df_auth = read_auth_df()
    if df_auth.empty:
        df_auth = pd.DataFrame(columns=AUTH_HEADERS)

    # 타입 정규화
    def _tostr(x):
        return "" if x is None else str(x)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df_auth.columns:
            df_auth[c] = df_auth[c].map(_tostr)
    if "활성" in df_auth.columns:
        df_auth["활성"] = df_auth["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

    # 보기용/검색
    with st.expander("검색/필터", expanded=False):
        colf = st.columns([1,1,1,1,2])
        with colf[0]: f_role = st.selectbox("역할", ["(전체)","admin","manager","evaluator"], index=0, key="acl_f_role")
        with colf[1]: f_scope = st.selectbox("범위유형", ["(전체)","부서","개별",""], index=0, key="acl_f_scope")
        with colf[2]: f_active = st.selectbox("활성", ["(전체)","True","False"], index=0, key="acl_f_active")
        with colf[3]: pass
        with colf[4]: f_q = st.text_input("검색(사번/이름/부서/대상사번/비고)", "", key="acl_f_q")

    view = df_auth.copy()
    if f_role != "(전체)" and "역할" in view.columns:
        view = view[view["역할"] == f_role]
    if f_scope != "(전체)" and "범위유형" in view.columns:
        view = view[view["범위유형"] == f_scope]
    if f_active != "(전체)" and "활성" in view.columns:
        view = view[view["활성"] == (f_active == "True")]
    if f_q.strip():
        k = f_q.strip().lower()
        def _match_row(r):
            cols = [c for c in ["사번","이름","부서1","부서2","대상사번","비고"] if c in r.index]
            return any(k in str(r[c]).lower() for c in cols)
        view = view[view.apply(_match_row, axis=1)]

    st.caption(f"규칙 수: **{len(view):,}건** / (저장은 관리자만 가능)")

    # 편집 표 — 단일 표에서 추가/수정/삭제
    edit_cols = [c for c in AUTH_HEADERS if c in view.columns]  # 순서 보장
    if "삭제" not in view.columns:
        view.insert(len(view.columns), "삭제", False)

    edited = st.data_editor(
        view[edit_cols + ["삭제"]],
        use_container_width=True,
        height=520,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "사번": st.column_config.TextColumn(help="사번(필수)"),
            "이름": st.column_config.TextColumn(help="표시용 이름"),
            "역할": st.column_config.SelectboxColumn(options=["admin","manager","evaluator"], help="권한 역할"),
            "범위유형": st.column_config.SelectboxColumn(options=["","부서","개별"], help="부서/개별 권한"),
            "부서1": st.column_config.TextColumn(help="부서 권한일 때 1차 부서명"),
            "부서2": st.column_config.TextColumn(help="부서 권한일 때 2차 부서명(선택)"),
            "대상사번": st.column_config.TextColumn(help="개별 권한일 때 대상 사번(쉼표/공백 구분)"),
            "활성": st.column_config.CheckboxColumn(help="권한 활성 여부"),
            "비고": st.column_config.TextColumn(),
            "삭제": st.column_config.CheckboxColumn(help="저장 시 체크된 행은 삭제됩니다."),
        },
    )

    # 저장 버튼(관리자만)
    cbtn = st.columns([1,2,2])
    with cbtn[0]:
        do_save = st.button("AUTH 저장(표 내용 전체 반영)", type="primary", use_container_width=True, disabled=(not am_admin))
    with cbtn[1]:
        st.caption("※ 관리자만 저장할 수 있습니다. (전체 덮어쓰기)")
    with cbtn[2]:
        st.caption("※ '삭제' 체크된 행은 반영 시 제거됩니다.")

    if do_save:
        try:
            # 1) 삭제 표시된 행 제외
            out = edited.copy()
            if "삭제" in out.columns:
                out = out[out["삭제"] != True]
                out = out.drop(columns=["삭제"], errors="ignore")

            # 2) 필드/타입 정리
            for c in AUTH_HEADERS:
                if c not in out.columns:
                    out[c] = ""
            out = out[AUTH_HEADERS].copy()

            def _boolize(x):
                s = str(x).strip().lower()
                return s in ("true","1","y","yes","t")
            if "활성" in out.columns:
                out["활성"] = out["활성"].map(_boolize)

            for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
                if c in out.columns:
                    out[c] = out[c].fillna("").map(lambda x: "" if x is None else str(x))

            # 3) 시트 전체 덮어쓰기
            ws = get_workbook().worksheet(AUTH_SHEET)
            header = ws.row_values(1) or AUTH_HEADERS

            _retry_call(ws.clear)
            _retry_call(ws.update, "A1", [header])

            if not out.empty:
                rows = out.apply(lambda r: [r.get(h, "") for h in header], axis=1).tolist()
                CHUNK = 500
                for i in range(0, len(rows), CHUNK):
                    _retry_call(ws.append_rows, rows[i:i+CHUNK], value_input_option="USER_ENTERED")

            st.cache_data.clear()
            st.success("권한 규칙이 저장되었습니다. (전체 덮어쓰기)", icon="✅")
            st.rerun()

        except Exception as e:
            st.exception(e)

    st.divider()

    # ── 권한 규칙 추가 (부서만; evaluator 허용)
    st.markdown("### 권한 규칙 추가 (부서만)")
    df_pick = emp_df.copy()
    df_pick["표시"] = df_pick.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    df_pick = df_pick.sort_values(["사번"])

    c1, c2 = st.columns([2,2])
    with c1: giver = st.selectbox("권한 주체(사번 - 이름)", ["(선택)"]+df_pick["표시"].tolist(), index=0, key="acl_giver")
    with c2: role  = st.selectbox("역할", ["manager","evaluator","admin"], index=0, key="acl_role")

    st.caption("범위유형: **부서** (개별 권한 UI는 사용 안 함)")
    cA, cB, cC = st.columns([1,1,1])
    with cA:
        dept1 = st.selectbox("부서1", [""]+sorted([x for x in emp_df.get("부서1",[]).dropna().unique() if x]),
                             index=0, key="acl_dept1")
    with cB:
        sub = emp_df.copy()
        if dept1: sub = sub[sub["부서1"].astype(str)==dept1]
        opt_d2 = [""]+sorted([x for x in sub.get("부서2",[]).dropna().unique() if x])
        dept2 = st.selectbox("부서2(선택)", opt_d2, index=0, key="acl_dept2")
    with cC: active = st.checkbox("활성", True, key="acl_active_dep")

    memo = st.text_input("비고(선택)", "", key="acl_memo")

    add_rows=[]
    if st.button("➕ 부서 권한 추가", type="primary", use_container_width=True, key="acl_add_dep"):
        if giver=="(선택)":
            st.warning("권한 주체를 선택하세요.", icon="⚠️")
        else:
            sab = giver.split(" - ",1)[0]
            name = _emp_name_by_sabun(emp_df, sab)
            if role == "admin":
                add_rows.append({"사번":sab,"이름":name,"역할":"admin","범위유형":"","부서1":"","부서2":"",
                                 "대상사번":"","활성":bool(active),"비고":memo.strip()})
            elif role == "manager":
                add_rows.append({"사번":sab,"이름":name,"역할":"manager","범위유형":"부서","부서1":dept1,"부서2":dept2,
                                 "대상사번":"","활성":bool(active),"비고":memo.strip()})
            else:  # evaluator
                add_rows.append({"사번":sab,"이름":name,"역할":"evaluator","범위유형":"부서","부서1":dept1,"부서2":dept2,
                                 "대상사번":"","활성":bool(active),"비고":memo.strip()})
    if add_rows:
        try:
            ws = get_workbook().worksheet(AUTH_SHEET)
            header = ws.row_values(1)
            rows = [[r.get(h,"") for h in header] for r in add_rows]
            _retry_call(ws.append_rows, rows, value_input_option="USER_ENTERED")
            st.cache_data.clear()
            st.success(f"규칙 {len(rows)}건 추가 완료", icon="✅")
            st.rerun()
        except Exception as e:
            st.exception(e)

    st.divider()
    st.markdown("#### 권한 규칙 목록")
    df_auth_all = read_auth_df()
    if df_auth_all.empty:
        st.caption("권한 규칙이 없습니다.")
    else:
        view = df_auth_all.sort_values(["역할","사번","범위유형","부서1","부서2","대상사번"])
        st.dataframe(view, use_container_width=True, height=380)

    st.divider()
    st.markdown("#### 규칙 삭제 (행 번호)")
    del_row = st.number_input("삭제할 시트 행 번호 (헤더=1)", min_value=2, step=1, value=2, key="acl_del_row")
    if st.button("🗑️ 해당 행 삭제", use_container_width=True, key="acl_del_btn"):
        try:
            ws = get_workbook().worksheet(AUTH_SHEET)
            _retry_call(ws.delete_rows, int(del_row))
            st.cache_data.clear()
            st.success(f"{del_row}행 삭제 완료", icon="✅")
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

    try:
        ensure_auth_sheet()
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
            """
        )
        sheet_id = st.secrets.get("sheets", {}).get("HR_SHEET_ID")
        if sheet_id:
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
            st.caption(f"📄 원본 스프레드시트: [{url}]({url})")

    with tabs[-1]:
        safe_run(_render_help, title="도움말")

# ── 엔트리포인트 ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
