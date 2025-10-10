# HISMEDI HR App
# Tabs: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말

# ══════════════════════════════════════════════════════════════════════════════
# Imports
# ══════════════════════════════════════════════════════════════════════════════
import re, time, random, hashlib, secrets as pysecrets
from datetime import datetime, timedelta
from typing import Any, Tuple
import pandas as pd
import streamlit as st
from html import escape as _html_escape
import unicodedata as _ud  # ← for display width padding

# Optional zoneinfo (KST)
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# gspread (배포 최적화: 자동 pip 설치 제거, 의존성 사전 설치 전제)
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# ══════════════════════════════════════════════════════════════════════════════
# Sync Utility (Force refresh Google Sheets caches)
# ══════════════════════════════════════════════════════════════════════════════
def force_sync():
    """모든 캐시를 무효화하고 즉시 리런하여 구글시트 최신 데이터로 갱신합니다."""
    # 1) Streamlit 캐시 비우기
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.cache_resource.clear()
    except Exception:
        pass

    # 2) 세션 상태 중 로컬 캐시성 키 초기화(프로젝트 규칙에 맞게 prefix 추가/수정)
    try:
        for k in list(st.session_state.keys()):
            if k.startswith(("__cache_", "_df_", "_cache_", "gs_")):
                del st.session_state[k]
    except Exception:
        pass

    # 3) 즉시 리런
    st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# App Config / Style
# ══════════════════════════════════════════════════════════════════════════════
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

# Disable st.help "No docs available"
if not getattr(st, "_help_disabled", False):
    def _noop_help(*args, **kwargs): return None
    st.help = _noop_help
    st._help_disabled = True

st.markdown(
    """
    <style>
      .block-container{ padding-top: 2.5rem !important; } 
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .badge{display:inline-block;padding:.25rem .5rem;border-radius:.5rem;border:1px solid #9ae6b4;background:#e6ffed;color:#0f5132;font-weight:600;}
      section[data-testid="stHelp"], div[data-testid="stHelp"]{ display:none !important; }
      .muted{color:#6b7280;}
      .app-title-hero{ font-weight:800; font-size:1.6rem; line-height:1.15; margin:.2rem 0 .6rem; }
      @media (min-width:1400px){ .app-title-hero{ font-size:1.8rem; } }
      div[data-testid="stFormSubmitButton"] button[kind="secondary"]{ padding: 0.35rem 0.5rem; font-size: .82rem; }

      /* JD Summary scroll box (Competency tab) */
      .scrollbox{ max-height: 280px; overflow-y: auto; padding: .6rem .75rem; background: #fafafa;
                  border: 1px solid #e5e7eb; border-radius: .5rem; }
      .scrollbox .kv{ margin-bottom: .6rem; }
      .scrollbox .k{ font-weight: 700; margin-bottom: .2rem; }
      .scrollbox .v{ white-space: pre-wrap; word-break: break-word; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════════
# Utils
# ══════════════════════════════════════════════════════════════════════════════
def kst_now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t")
def _normalize_private_key(raw: str) -> str:
    if not raw: return raw
    return raw.replace("\\n","\\n") if "\\n" in raw and "BEGIN PRIVATE KEY" in raw else raw
def _pin_hash(pin: str, sabun: str) -> str:
    return hashlib.sha256(f"{str(sabun).strip()}:{str(pin).strip()}".encode()).hexdigest()

# Display width helpers for aligned radio labels
def _disp_len(s: str) -> int:
    n = 0
    for ch in str(s):
        n += 2 if _ud.east_asian_width(ch) in ("F", "W") else 1
    return n

def _pad_disp(s: str, width: int) -> str:
    s = str(s)
    pad = max(0, width - _disp_len(s))
    return s + (" " * pad)

# ─────────────────────────────────────────────────────────────────────────────
# Attestation / PIN Utilities
# ─────────────────────────────────────────────────────────────────────────────
import json as _json  # (추가) 제출 데이터 해시 직렬화용

def _attest_hash(payload: dict) -> str:
    s = _json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _client_meta() -> str:
    ua = st.session_state.get("_ua", "")
    if not ua:
        try:
            ctx = st.runtime.scriptrunner.script_run_context.get_script_run_ctx()
            if ctx and getattr(ctx, "request", None):
                ua = ctx.request.headers.get("User-Agent", "")
        except Exception:
            ua = ""
        st.session_state["_ua"] = ua
    return (ua or "")[:180]

def verify_pin(user_sabun: str, pin: str) -> bool:
    sabun = str(user_sabun).strip()
    val = str(pin).strip()
    pin_map = st.session_state.get("pin_map", {})
    if sabun in pin_map:
        return str(pin_map.get(sabun, "")) == val
    pin_hash_map = st.session_state.get("pin_hash_map", {})
    if sabun in pin_hash_map:
        try:
            return str(pin_hash_map.get(sabun, "")) == _pin_hash(val, sabun)
        except Exception:
            pass
    u = st.session_state.get("user", {}) or {}
    if str(u.get("사번", "")).strip() == sabun:
        if "pin" in u:
            return str(u.get("pin", "")) == val
        if "pin_hash" in u:
            try:
                return str(u.get("pin_hash", "")) == _pin_hash(val, sabun)
            except Exception:
                pass
    return False

# ══════════════════════════════════════════════════════════════════════════════
# Google Auth / Sheets
# ══════════════════════════════════════════════════════════════════════════════
API_BACKOFF_SEC = [0.0, 0.8, 1.6, 3.2, 6.4, 9.6]
def _retry(fn, *args, **kwargs):
    last=None
    for b in API_BACKOFF_SEC:
        try: return fn(*args, **kwargs)
        except APIError as e:
            last=e; time.sleep(b+random.uniform(0,0.25))
    if last: raise last
    return fn(*args, **kwargs)

@st.cache_resource(show_spinner=False)
def get_client():
    svc = dict(st.secrets["gcp_service_account"])
    svc["private_key"] = _normalize_private_key(svc.get("private_key",""))
    scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds=Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_book():
    return get_client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

_WS_CACHE = {}
_HDR_CACHE = {}
_WS_TTL, _HDR_TTL = 120, 120

def _ws(title: str):
    now=time.time(); hit=_WS_CACHE.get(title)
    if hit and (now-hit[0]<_WS_TTL): return hit[1]
    ws=_retry(get_book().worksheet, title); _WS_CACHE[title]=(now,ws); return ws

def _hdr(ws, key: str):
    now=time.time(); hit=_HDR_CACHE.get(key)
    if hit and (now-hit[0]<_HDR_TTL): return hit[1], hit[2]
    header=_retry(ws.row_values, 1) or []; hmap={n:i+1 for i,n in enumerate(header)}
    _HDR_CACHE[key]=(now, header, hmap); return header, hmap

def _ws_get_all_records(ws):
    try: return _retry(ws.get_all_records, numericise_ignore=["all"])
    except TypeError: return _retry(ws.get_all_records)

# ══════════════════════════════════════════════════════════════════════════════
# Sheet Readers (TTL↑)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=600, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    ws=_ws(sheet_name)
    df=pd.DataFrame(_ws_get_all_records(ws))
    if df.empty: return df
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    if "재직여부" in df.columns: df["재직여부"]=df["재직여부"].map(_to_bool)
    return df

@st.cache_data(ttl=600, show_spinner=False)
def read_emp_df() -> pd.DataFrame:
    df = read_sheet_df(EMP_SHEET)
    for c in ["사번","이름","PIN_hash"]:
        if c not in df.columns: df[c]=""
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    return df

# ══════════════════════════════════════════════════════════════════════════════
# Login + Session
# ══════════════════════════════════════════════════════════════════════════════
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
import streamlit.components.v1 as components
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
    sabun = st.text_input("사번", key="login_sabun")
    pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")
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

# ══════════════════════════════════════════════════════════════════════════════
# ACL (권한) + Staff Filters (TTL↑)
# ══════════════════════════════════════════════════════════════════════════════
AUTH_SHEET="권한"
AUTH_HEADERS=["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]

@st.cache_data(ttl=300, show_spinner=False)
def read_auth_df()->pd.DataFrame:
    try:
        ws=_ws(AUTH_SHEET); df=pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=AUTH_HEADERS)
    if df.empty: return pd.DataFrame(columns=AUTH_HEADERS)
    for c in AUTH_HEADERS:
        if c not in df.columns: df[c]=""
    df["사번"]=df["사번"].astype(str)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    return df

def is_admin(sabun:str)->bool:
    try:
        df=read_auth_df()
        if df.empty: return False
        q=df[(df["사번"].astype(str)==str(sabun)) & (df["역할"].str.lower()=="admin") & (df["활성"]==True)]
        return not q.empty
    except Exception: return False

def get_allowed_sabuns(emp_df:pd.DataFrame, sabun:str, include_self:bool=True)->set[str]:
    sabun=str(sabun); allowed=set([sabun]) if include_self else set()
    if is_admin(sabun): return set(emp_df["사번"].astype(str).tolist())
    df=read_auth_df()
    if not df.empty:
        mine=df[(df["사번"].astype(str)==sabun) & (df["활성"]==True)]
        for _,r in mine.iterrows():
            t=str(r.get("범위유형","")).strip()
            if t=="부서":
                d1=str(r.get("부서1","")).strip(); d2=str(r.get("부서2","")).strip()
                tgt=emp_df.copy()
                if d1: tgt=tgt[tgt["부서1"].astype(str)==d1]
                if d2: tgt=tgt[tgt["부서2"].astype(str)==d2]
                allowed.update(tgt["사번"].astype(str).tolist())
            elif t=="개별":
                for p in re.split(r"[,\s]+", str(r.get("대상사번","")).strip()): 
                    if p: allowed.add(p)
    return allowed

# ══════════════════════════════════════════════════════════════════════════════
# Global Target Sync
# ══════════════════════════════════════════════════════════════════════════════
def set_global_target(sabun:str, name:str=""):
    st.session_state["glob_target_sabun"]=str(sabun).strip()
    st.session_state["glob_target_name"]=str(name).strip()

def get_global_target()->Tuple[str,str]:
    return (str(st.session_state.get("glob_target_sabun","") or ""),
            str(st.session_state.get("glob_target_name","") or ""))

# ══════════════════════════════════════════════════════════════════════════════
# Left: 직원선택 (라디오 + 모노스페이스 정렬 / 행 클릭)
# ══════════════════════════════════════════════════════════════════════════════
def render_staff_picker_left(emp_df: pd.DataFrame):
    """표처럼 보이는 라디오 목록. 행 전체 클릭으로 선택 및 전역 동기화."""
    # 권한 필터
    u  = st.session_state.get("user", {})
    me = str(u.get("사번", ""))
    df = emp_df.copy()
    if not is_admin(me):
        allowed = get_allowed_sabuns(emp_df, me, include_self=True)
        df = df[df["사번"].astype(str).isin(allowed)].copy()

    # 검색
    with st.form("left_search_form", clear_on_submit=False):
        q = st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
        submitted = st.form_submit_button("검색 적용(Enter)")

    view = df.copy()
    if q.strip():
        k = q.strip().lower()
        view = view[view.apply(
            lambda r: any(k in str(r[c]).lower() for c in ["사번", "이름"] if c in r),
            axis=1
        )]

    # 정렬
    if "사번" in view.columns:
        try:
            view["__sab_int__"] = pd.to_numeric(view["사번"], errors="coerce")
        except Exception:
            view["__sab_int__"] = None
        view = view.sort_values(["__sab_int__", "사번"]).drop(columns=["__sab_int__"])

    # Enter 시 첫 행 자동 선택
    if submitted and not view.empty:
        first = str(view.iloc[0]["사번"])
        name = _emp_name_by_sabun(emp_df, first)
        set_global_target(first, name)
        st.session_state["eval2_target_sabun"] = first
        st.session_state["eval2_target_name"]  = name
        st.session_state["jd2_target_sabun"]   = first
        st.session_state["jd2_target_name"]    = name
        st.session_state["cmpS_target_sabun"]  = first
        st.session_state["cmpS_target_name"]   = name
        st.session_state["left_selected_sabun"]= first

    # 현재 선택값
    g_sab, g_name = get_global_target()
    cur = (st.session_state.get("left_selected_sabun") or g_sab or "").strip()

    # 표시 컬럼
    cols = [c for c in ["사번", "이름", "부서1", "부서2", "직급"] if c in view.columns]
    v = view[cols].copy().astype(str)

    # 폭 계산(가시폭 기준) 및 제한
    maxw = {c: max([_disp_len(c)] + [_disp_len(x) for x in v[c].tolist()]) for c in cols}
    for c in maxw: maxw[c] = min(maxw[c], 16)

    header_label = "  " + "  ".join(_pad_disp(c, maxw[c]) for c in cols)

    options = []
    display_map = {}
    for _, r in v.iterrows():
        sab = str(r["사번"])
        row = "  " + "  ".join(_pad_disp(str(r[c]), maxw[c]) for c in cols)
        options.append(sab); display_map[sab]=row

    cur_idx = options.index(cur) if (cur and cur in options) else 0 if options else 0

    # 스타일
    st.markdown("""
    <style>
      div[data-testid="stRadio"] input[type="radio"] { display:none !important; }
      div[data-testid="stRadio"] label p {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace !important;
        white-space: pre !important;
        font-size: 13px !important;
        line-height: 1.25 !important;
        margin: 0 !important;
      }
      div[data-testid="stRadio"] > div { gap: 6px !important; }
      div[data-testid="stRadio"] label[data-selected="true"] { background: #e6ffed !important; border-radius: 6px; }
    </style>
    """, unsafe_allow_html=True)

    # 헤더 표시
    st.code(header_label, language=None)

    # 라디오
    selected = st.radio(
        label="직원 선택",
        options=options,
        index=cur_idx if options else 0,
        format_func=lambda sab: display_map.get(sab, sab),
        label_visibility="collapsed",
        key="left_radio_picker",
    )

    if selected and selected != cur:
        name = _emp_name_by_sabun(emp_df, selected)
        set_global_target(selected, name)
        st.session_state["eval2_target_sabun"] = selected
        st.session_state["eval2_target_name"]  = name
        st.session_state["jd2_target_sabun"]   = selected
        st.session_state["jd2_target_name"]    = name
        st.session_state["cmpS_target_sabun"]  = selected
        st.session_state["cmpS_target_name"]   = name
        st.session_state["left_selected_sabun"]= selected
        cur = selected

    if cur:
        sel_name = _emp_name_by_sabun(emp_df, cur)
        st.success(f"대상자: {sel_name} ({cur})", icon="✅")
    else:
        st.info("좌측 목록에서 행을 클릭해 대상자를 선택하세요.", icon="👤")

# (The rest of the app remains identical to the user's provided file.)
