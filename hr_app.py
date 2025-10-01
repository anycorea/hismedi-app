# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (서명 이미지 내장/카드형 표시/출력 포함)
- 메인 탭: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
- 로그인: Enter(사번→PIN, PIN→로그인)
- 좌측 검색 Enter → 대상 선택 자동 동기화
- 서명관리: URL 대신 Base64 내장 이미지(B64) 우선 사용
- PDF 출력: 브라우저 인쇄 + (가능 시) ReportLab PDF 다운로드
"""

# ══════════════════════════════════════════════════════════════════════════════
# Imports
# ══════════════════════════════════════════════════════════════════════════════
import io, re, time, random, hashlib
from datetime import datetime
from typing import Any, Tuple
import pandas as pd
import streamlit as st
from html import escape as _html_escape

# Optional zoneinfo (KST)
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# gspread (사전 설치 전제)
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

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
      .block-container{ padding-top: 2.0rem !important; } 
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

      /* Signature card */
      .sigcard{border:1px solid #e5e7eb;border-radius:.75rem;padding:12px;background:#fff;}
      .sigcard h4{margin:.2rem 0 .4rem;}
      .sigmeta{font-size:.9rem;color:#374151;margin:.2rem 0;}
      .print-hint{font-size:.9rem;color:#6b7280;}
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
def _pin_hash(pin: str, sabun: str) -> str:
    return hashlib.sha256(f"{str(sabun).strip()}:{str(pin).strip()}".encode()).hexdigest()

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
    # private_key \n 정규화
    pk = svc.get("private_key","")
    if "\\n" in pk and "BEGIN PRIVATE KEY" in pk:
        svc["private_key"] = pk.replace("\\n","\n")
    scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds=Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_book():
    return get_client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

_WS_CACHE: dict[str, Tuple[float, Any]] = {}
_HDR_CACHE: dict[str, Tuple[float, list[str], dict]] = {}
_WS_TTL, _HDR_TTL = 120, 120

def _ws(title: str):
    now=time.time(); hit=_WS_CACHE.get(title)
    if hit and (now-hit[0]<_WS_TTL): return hit[1]
    ws=_retry(get_book().worksheet, title); _WS_CACHE[title]=(now,ws); return ws

def _hdr(ws, key: str) -> Tuple[list[str], dict]:
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

# --- Enter Key Binder (사번→PIN, PIN→로그인) --------------------------------
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
# ACL (권한) + Staff Filters
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
# Signature utilities (Base64-first; URL fallback)
# ══════════════════════════════════════════════════════════════════════════════
def drive_direct(url: str) -> str:
    """Convert Google Drive share URL to direct view URL; otherwise return as-is."""
    if not url: return ""
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", url) or re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    return f"https://drive.google.com/uc?export=view&id={m.group(1)}" if m else url

def _to_data_uri_from_b64(b64_str: str) -> str:
    """Return data:image/png;base64,... from raw base64 (no header)."""
    s = (b64_str or "").strip()
    if not s:
        return ""
    if s.startswith("data:image"):
        return s
    s = re.sub(r"\\s+", "", s)
    return f"data:image/png;base64,{s}"

@st.cache_data(ttl=300, show_spinner=False)
def read_sign_df() -> pd.DataFrame:
    """Read '서명관리' → (사번, 서명B64/서명URL, 활성, 비고) and produce sign_render."""
    try:
        df = read_sheet_df("서명관리")
    except Exception:
        df = pd.DataFrame(columns=["사번","서명B64","서명URL","활성","비고"])

    if df is None or df.empty:
        return pd.DataFrame(columns=["사번","서명B64","서명URL","활성","비고","sign_render"])

    if "사번" not in df.columns: df["사번"] = ""
    if "서명B64" not in df.columns: df["서명B64"] = ""
    if "서명URL" not in df.columns:
        for alt in ["서명", "서명링크", "SignURL", "sign_url"]:
            if alt in df.columns:
                df["서명URL"] = df[alt]; break
        else:
            df["서명URL"] = ""
    df["사번"] = df["사번"].astype(str)

    # 활성 기본 True
    if "활성" in df.columns:
        df["활성"] = df["활성"].astype(str).str.lower().isin(["true","1","y","yes","t"])
    else:
        df["활성"] = True

    df["sign_data_uri"] = df["서명B64"].astype(str).fillna("").map(_to_data_uri_from_b64)
    df["sign_url_norm"] = df["서명URL"].astype(str).fillna("").map(drive_direct)
    df["sign_render"] = df.apply(lambda r: r["sign_data_uri"] if r["sign_data_uri"] else r["sign_url_norm"], axis=1)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def build_sign_map(df: pd.DataFrame) -> dict:
    """Return {사번: sign_render} where 활성=True and sign exists."""
    if df is None or df.empty: return {}
    d = {}
    for _, r in df.iterrows():
        sab = str(r.get("사번",""))
        v = str(r.get("sign_render",""))
        act = bool(r.get("활성", True))
        if sab and v and act:
            d[sab] = v
    return d

# ══════════════════════════════════════════════════════════════════════════════
# Left: 직원선택 (Enter 동기화)
# ══════════════════════════════════════════════════════════════════════════════
def render_staff_picker_left(emp_df: pd.DataFrame):
    u=st.session_state.get("user",{}); me=str(u.get("사번",""))
    df=emp_df.copy()
    if not is_admin(me):
        allowed=get_allowed_sabuns(emp_df, me, include_self=True)
        df=df[df["사번"].astype(str).isin(allowed)].copy()

    with st.form("left_search_form", clear_on_submit=False):
        q = st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
        submitted = st.form_submit_button("검색 적용(Enter)")
    view=df.copy()
    if q.strip():
        k=q.strip().lower()
        view=view[view.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이름"] if c in r), axis=1)]

    view=view.sort_values("사번") if "사번" in view.columns else view
    sabuns = view["사번"].astype(str).tolist()
    names  = view.get("이름", pd.Series(['']*len(view))).astype(str).tolist()
    opts   = [f"{s} - {n}" for s,n in zip(sabuns, names)]

    pre_sel_sab = st.session_state.get("left_preselect_sabun", "")
    if submitted:
        exact_idx = -1
        if q.strip():
            for i,(s,n) in enumerate(zip(sabuns,names)):
                if q.strip()==s or q.strip()==n:
                    exact_idx = i; break
        target_idx = exact_idx if exact_idx >= 0 else (0 if sabuns else -1)
        if target_idx >= 0:
            pre_sel_sab = sabuns[target_idx]
            st.session_state["left_preselect_sabun"] = pre_sel_sab

    idx0 = 0
    if pre_sel_sab:
        try: idx0 = 1 + sabuns.index(pre_sel_sab)
        except ValueError: idx0 = 0

    picked=st.selectbox("대상 선택", ["(선택)"]+opts, index=idx0, key="left_pick")
    if picked and picked!="(선택)":
        sab=picked.split(" - ",1)[0].strip()
        name=picked.split(" - ",1)[1].strip() if " - " in picked else ""
        set_global_target(sab, name)
        st.session_state["eval_target_sabun"]=sab
        st.session_state["eval_target_name"]=name
        st.session_state["jd_target_sabun"]=sab
        st.session_state["jd_target_name"]=name
        st.session_state["cmp_target_sabun"]=sab
        st.session_state["cmp_target_name"]=name

    cols=[c for c in ["사번","이름","부서1","부서2","직급"] if c in view.columns]
    st.dataframe(view[cols], use_container_width=True, height=260, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# 인사평가 (간략 구현 + 서명 카드 + PDF)
# ══════════════════════════════════════════════════════════════════════════════
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고"]
EVAL_RESP_SHEET_PREFIX = "인사평가_"
EVAL_BASE_HEADERS = ["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각"]

def _eval_sheet_name(year: int | str) -> str: return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def ensure_eval_items_sheet():
    wb=get_book()
    try:
        ws=wb.worksheet(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        _retry(ws.update, "A1", [EVAL_ITEM_HEADERS]); return
    header=_retry(ws.row_values, 1) or []
    need=[h for h in EVAL_ITEM_HEADERS if h not in header]
    if need: _retry(ws.update, "1:1", [header+need])

@st.cache_data(ttl=300, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    ensure_eval_items_sheet()
    ws=_ws(EVAL_ITEMS_SHEET)
    df=pd.DataFrame(_ws_get_all_records(ws))
    if df.empty: return pd.DataFrame(columns=EVAL_ITEM_HEADERS)
    if "순서" in df.columns:
        def _i(x):
            try: return int(float(str(x).strip()))
            except: return 0
        df["순서"]=df["순서"].apply(_i)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    cols=[c for c in ["순서","항목"] if c in df.columns]
    if cols: df=df.sort_values(cols).reset_index(drop=True)
    if only_active and "활성" in df.columns: df=df[df["활성"]==True]
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

def render_signature_card(title: str, name: str, sabun: str, sign_map: dict):
    st.markdown(f"<div class='sigcard'><h4>{_html_escape(title)}</h4>", unsafe_allow_html=True)
    col1, col2 = st.columns([1,2])
    with col1:
        img = sign_map.get(str(sabun), "")
        if img:
            st.image(img, caption="서명", use_column_width=True)
        else:
            st.info("서명 없음", icon="🖊️")
    with col2:
        st.markdown(f"<div class='sigmeta'><b>이름</b>: {_html_escape(name or '—')}<br><b>사번</b>: {_html_escape(sabun or '—')}</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

def tab_eval(emp_df: pd.DataFrame):
    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="eval_year")

    u = st.session_state["user"]; me_sabun = str(u["사번"]); me_name = str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False))>0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)
    items = read_eval_items_df(True)
    if items.empty: st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️"); return
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    item_ids = [str(x) for x in items_sorted["항목ID"].tolist()]

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("eval_target_sabun", glob_sab or me_sabun)
    st.session_state.setdefault("eval_target_name",  glob_name or me_name)

    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base=emp_df.copy(); base["사번"]=base["사번"].astype(str)
        base=base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns: base=base[base["재직여부"]==True]
        view=base[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)
        _sabuns=view["사번"].astype(str).tolist(); _names=view["이름"].astype(str).tolist()
        _d2=view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""]*len(_sabuns)
        _opts=[f"{s} - {n} - {d2}" for s,n,d2 in zip(_sabuns,_names,_d2)]
        _target = st.session_state.get("eval_target_sabun", glob_sab or "")
        _idx = _sabuns.index(_target) if _target in _sabuns else 0
        _sel = st.selectbox("대상자 선택", _opts, index=_idx, key="eval_pick_editor_select")
        _sel_sab = _sel.split(" - ",1)[0] if isinstance(_sel,str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["eval_target_sabun"]=str(_sel_sab)
        try:
            st.session_state["eval_target_name"]=str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["eval_target_name"]=""
        target_sabun=st.session_state["eval_target_sabun"]
        target_name =st.session_state["eval_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    # 제출 현황 표 (간단)
    st.markdown("#### 내 제출 현황")
    try:
        my=read_my_eval_rows(int(year), me_sabun)
        cols=[c for c in ["평가유형","평가대상사번","평가대상이름","총점","상태","제출시각"] if c in my.columns]
        st.dataframe(my[cols] if cols else my, use_container_width=True, height=220)
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

    # 1차/2차 평가자 서명 카드
    try:
        _sign_map = build_sign_map(read_sign_df())
        if _sign_map:
            st.markdown("#### 서명(평가자)")
            ws = _ensure_eval_resp_sheet(int(year), item_ids)
            header = _retry(ws.row_values, 1) or []
            idx = {n:i for i,n in enumerate(header)}
            values = _retry(ws.get_all_values)
            first, second = None, None
            for r in values[1:]:
                try:
                    if (str(r[idx["연도"]]).strip() == str(int(year))
                        and str(r[idx["평가대상사번"]]).strip() == str(target_sabun).strip()):
                        et = str(r[idx["평가유형"]]).strip()
                        if et == "1차" and first is None:
                            first = r
                        elif et == "2차" and second is None:
                            second = r
                except Exception:
                    pass
            cc = st.columns(2)
            if first:
                with cc[0]:
                    render_signature_card("1차 평가자",
                                          name=first[idx.get("평가자이름","")] if "평가자이름" in idx else "",
                                          sabun=first[idx.get("평가자사번","")] if "평가자사번" in idx else "",
                                          sign_map=_sign_map)
            if second:
                with cc[1]:
                    render_signature_card("2차 평가자",
                                          name=second[idx.get("평가자이름","")] if "평가자이름" in idx else "",
                                          sabun=second[idx.get("평가자사번","")] if "평가자사번" in idx else "",
                                          sign_map=_sign_map)
    except Exception:
        st.caption("서명 카드 렌더 중 오류가 발생했습니다.")

    # 출력 / PDF (브라우저 인쇄 + ReportLab 보조)
    def _eval_print_html():
        return f"""
        <h3>인사평가 - 서명 요약</h3>
        <p>대상: {target_name} ({target_sabun}) / 연도: {year}</p>
        <p class='print-hint'>※ 브라우저 인쇄(Ctrl/⌘+P) → PDF 저장을 권장합니다.</p>
        """
    render_pdf_controls("인사평가_서명", _eval_print_html, images_to_embed=None)

# ══════════════════════════════════════════════════════════════════════════════
# 직무기술서 (승인자 + 서명 카드 + PDF)
# ══════════════════════════════════════════════════════════════════════════════
JOBDESC_SHEET="직무기술서"
JOBDESC_HEADERS = [
    "사번","연도","버전","부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","서명방식","서명데이터","제출시각",
    "승인자사번","승인자이름"
]

def ensure_jobdesc_sheet():
    wb=get_book()
    try:
        ws=wb.worksheet(JOBDESC_SHEET)
        header=_retry(ws.row_values,1) or []
        need=[h for h in JOBDESC_HEADERS if h not in header]
        if need: _retry(ws.update,"1:1",[header+need])
        return ws
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet,title=JOBDESC_SHEET, rows=1200, cols=60)
        _retry(ws.update,"A1",[JOBDESC_HEADERS]); return ws

@st.cache_data(ttl=600, show_spinner=False)
def read_jobdesc_df()->pd.DataFrame:
    ensure_jobdesc_sheet()
    ws=_ws(JOBDESC_SHEET)
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

def _jd_latest_for(sabun:str, year:int)->dict|None:
    df=read_jobdesc_df()
    if df.empty: return None
    sub=df[(df["사번"].astype(str)==str(sabun))&(df["연도"].astype(int)==int(year))].copy()
    if sub.empty: return None
    try: sub["버전"]=sub["버전"].astype(int)
    except Exception: pass
    sub=sub.sort_values(["버전"], ascending=[False]).reset_index(drop=True)
    row=sub.iloc[0].to_dict()
    for k,v in row.items(): row[k]=("" if v is None else str(v))
    return row

def _jobdesc_next_version(sabun:str, year:int)->int:
    df=read_jobdesc_df()
    if df.empty: return 1
    sub=df[(df["사번"]==str(sabun))&(df["연도"].astype(int)==int(year))]
    return 1 if sub.empty else int(sub["버전"].astype(int).max())+1

def upsert_jobdesc(rec:dict, as_new_version:bool=False)->dict:
    ensure_jobdesc_sheet()
    ws=_ws(JOBDESC_SHEET)
    header=_retry(ws.row_values,1); hmap={n:i+1 for i,n in enumerate(header)}
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

    values=_retry(ws.get_all_values); row_idx=0
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
        _retry(ws.append_row, build_row(), value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action":"insert","version":ver}
    else:
        for k,v in rec.items():
            c=hmap.get(k)
            if c: _retry(ws.update_cell, row_idx, c, v)
        st.cache_data.clear()
        return {"action":"update","version":ver}

def tab_job_desc(emp_df: pd.DataFrame):
    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="jd_year")
    u=st.session_state["user"]; me_sabun=str(u["사번"]); me_name=str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False))>0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("jd_target_sabun", glob_sab or me_sabun)
    st.session_state.setdefault("jd_target_name",  glob_name or me_name)
    st.session_state.setdefault("jd_edit_mode",    False)

    if not am_admin_or_mgr:
        target_sabun=me_sabun; target_name=me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base=emp_df.copy(); base["사번"]=base["사번"].astype(str)
        base=base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns: base=base[base["재직여부"]==True]
        view=base[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)
        _sabuns=view["사번"].astype(str).tolist(); _names=view["이름"].astype(str).tolist()
        _d2=view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""]*len(_sabuns)
        _opts=[f"{s} - {n} - {d2}" for s,n,d2 in zip(_sabuns,_names,_d2)]
        _target=st.session_state.get("jd_target_sabun", glob_sab or "")
        _idx=_sabuns.index(_target) if _target in _sabuns else 0
        _sel=st.selectbox("대상자 선택", _opts, index=_idx, key="jd_pick_editor_select")
        _sel_sab=_sel.split(" - ",1)[0] if isinstance(_sel,str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["jd_target_sabun"]=str(_sel_sab)
        try:
            st.session_state["jd_target_name"]=str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["jd_target_name"]=""
        target_sabun=st.session_state["jd_target_sabun"]; target_name=st.session_state["jd_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    col_mode=st.columns([1,3])
    with col_mode[0]:
        if st.button(("수정모드로 전환" if not st.session_state["jd_edit_mode"] else "보기모드로 전환"),
                     use_container_width=True, key="jd_toggle"):
            st.session_state["jd_edit_mode"]=not st.session_state["jd_edit_mode"]; st.rerun()
    with col_mode[1]: st.caption(f"현재: **{'수정모드' if st.session_state['jd_edit_mode'] else '보기모드'}**")
    edit_mode=bool(st.session_state["jd_edit_mode"])

    jd_saved=_jd_latest_for(target_sabun, int(year))
    jd_current=jd_saved if jd_saved else {
        "사번":str(target_sabun),"연도":int(year),"버전":0,
        "부서1":emp_df.loc[emp_df["사번"].astype(str)==str(target_sabun)].get("부서1","").values[0] if "부서1" in emp_df.columns else "",
        "부서2":emp_df.loc[emp_df["사번"].astype(str)==str(target_sabun)].get("부서2","").values[0] if "부서2" in emp_df.columns else "",
        "작성자사번":me_sabun,"작성자이름":_emp_name_by_sabun(emp_df, me_sabun),
        "직군":"","직종":"","직무명":"","제정일":"","개정일":"","검토주기":"1년",
        "직무개요":"","주업무":"","기타업무":"","필요학력":"","전공계열":"",
        "직원공통필수교육":"","보수교육":"","기타교육":"","특성화교육":"",
        "면허":"","경력(자격요건)":"","비고":"","서명방식":"","서명데이터":"",
        "승인자사번":"","승인자이름":""
    }

    with st.expander("현재 저장된 직무기술서 요약", expanded=False):
        st.write(f"**직무명:** {(jd_saved or {}).get('직무명','')}")
        cc=st.columns(2)
        with cc[0]: st.markdown("**주업무**");  st.write((jd_saved or {}).get("주업무","") or "—")
        with cc[1]: st.markdown("**기타업무**"); st.write((jd_saved or {}).get("기타업무","") or "—")

    col = st.columns([1,1,2,2])
    with col[0]:
        version = st.number_input("버전(없으면 자동)", min_value=0, max_value=999,
                                  value=int(str(jd_current.get("버전", 0)) or 0),
                                  step=1, key="jd_ver", disabled=not edit_mode)
    with col[1]:
        jobname = st.text_input("직무명", value=jd_current.get("직무명",""),
                                key="jd_jobname", disabled=not edit_mode)
    with col[2]:
        memo = st.text_input("비고", value=jd_current.get("비고",""),
                             key="jd_memo", disabled=not edit_mode)
    with col[3]: pass

    c2 = st.columns([1,1,1,1])
    with c2[0]: dept1 = st.text_input("부서1", value=jd_current.get("부서1",""), key="jd_dept1", disabled=not edit_mode)
    with c2[1]: dept2 = st.text_input("부서2", value=jd_current.get("부서2",""), key="jd_dept2", disabled=not edit_mode)
    with c2[2]: group = st.text_input("직군",  value=jd_current.get("직군",""),  key="jd_group",  disabled=not edit_mode)
    with c2[3]: series= st.text_input("직종",  value=jd_current.get("직종",""), key="jd_series", disabled=not edit_mode)

    c3 = st.columns([1,1,1])
    with c3[0]: d_create = st.text_input("제정일",   value=jd_current.get("제정일",""),   key="jd_d_create", disabled=not edit_mode)
    with c3[1]: d_update = st.text_input("개정일",   value=jd_current.get("개정일",""),   key="jd_d_update", disabled=not edit_mode)
    with c3[2]: review   = st.text_input("검토주기", value=jd_current.get("검토주기",""), key="jd_review",   disabled=not edit_mode)

    job_summary = st.text_area("직무개요", value=jd_current.get("직무개요",""), height=80,  key="jd_summary", disabled=not edit_mode)
    job_main    = st.text_area("주업무",   value=jd_current.get("주업무",""),   height=120, key="jd_main",    disabled=not edit_mode)
    job_other   = st.text_area("기타업무", value=jd_current.get("기타업무",""), height=80,  key="jd_other",   disabled=not edit_mode)

    c4 = st.columns([1,1,1,1,1,1])
    with c4[0]: edu_req    = st.text_input("필요학력",        value=jd_current.get("필요학력",""),        key="jd_edu",        disabled=not edit_mode)
    with c4[1]: major_req  = st.text_input("전공계열",        value=jd_current.get("전공계열",""),        key="jd_major",      disabled=not edit_mode)
    with c4[2]: edu_common = st.text_input("직원공통필수교육", value=jd_current.get("직원공통필수교육",""), key="jd_edu_common", disabled=not edit_mode)
    with c4[3]: edu_cont   = st.text_input("보수교육",        value=jd_current.get("보수교육",""),        key="jd_edu_cont",   disabled=not edit_mode)
    with c4[4]: edu_etc    = st.text_input("기타교육",        value=jd_current.get("기타교육",""),        key="jd_edu_etc",    disabled=not edit_mode)
    with c4[5]: edu_spec   = st.text_input("특성화교육",      value=jd_current.get("특성화교육",""),      key="jd_edu_spec",   disabled=not edit_mode)

    c5 = st.columns([1,1,2])
    with c5[0]: license_ = st.text_input("면허", value=jd_current.get("면허",""), key="jd_license", disabled=not edit_mode)
    with c5[1]: career   = st.text_input("경력(자격요건)", value=jd_current.get("경력(자격요건)",""), key="jd_career", disabled=not edit_mode)

    c6 = st.columns([1,2,1])
    with c6[0]:
        _opt = ["", "text", "image"]
        _sv  = jd_current.get("서명방식","")
        _idx = _opt.index(_sv) if _sv in _opt else 0
        sign_type = st.selectbox("서명방식", _opt, index=_idx, key="jd_sign_type", disabled=not edit_mode)
    with c6[1]:
        sign_data = st.text_input("서명데이터", value=jd_current.get("서명데이터",""), key="jd_sign_data", disabled=not edit_mode)

    # 승인자 입력
    ap_col = st.columns([1,1])
    with ap_col[0]:
        approver_sabun = st.text_input("승인자 사번", value=(jd_current.get("승인자사번","") if jd_current else ""), key="jd_approver_sabun", disabled=not edit_mode)
    with ap_col[1]:
        approver_name  = st.text_input("승인자 이름", value=(jd_current.get("승인자이름","") if jd_current else ""), key="jd_approver_name", disabled=not edit_mode)

    save_btn = st.button("저장/업서트", type="primary", use_container_width=True, key="jd_save", disabled=not edit_mode)
    if save_btn:
        rec = {
            "사번": str(target_sabun), "연도": int(year), "버전": int(version or 0),
            "부서1": dept1, "부서2": dept2, "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
            "직군": group, "직종": series, "직무명": jobname,
            "제정일": d_create, "개정일": d_update, "검토주기": review,
            "직무개요": job_summary, "주업무": job_main, "기타업무": job_other,
            "필요학력": edu_req, "전공계열": major_req,
            "직원공통필수교육": edu_common, "보수교육": edu_cont, "기타교육": edu_etc, "특성화교육": edu_spec,
            "면허": license_, "경력(자격요건)": career, "비고": memo, "서명방식": sign_type, "서명데이터": sign_data,
            "승인자사번": approver_sabun, "승인자이름": approver_name,
        }
        try:
            rep = upsert_jobdesc(rec, as_new_version=(version == 0))
            st.success(f"저장 완료 (버전 {rep['version']})", icon="✅"); st.rerun()
        except Exception as e:
            st.exception(e)

    # 승인자 서명 카드
    try:
        _sign_map = build_sign_map(read_sign_df())
        if _sign_map:
            st.markdown("#### 승인자 서명")
            ap_sab = (jd_saved or {}).get("승인자사번","") if jd_saved else approver_sabun
            ap_name = (jd_saved or {}).get("승인자이름","") if jd_saved else approver_name
            if ap_sab:
                render_signature_card("승인자", ap_name, ap_sab, _sign_map)
            else:
                st.info("승인자 정보가 없습니다.", icon="ℹ️")
    except Exception:
        st.caption("승인자 서명 카드 렌더 중 오류가 발생했습니다.")

    # 출력 / PDF
    def _jd_print_html():
        return f"""
        <h3>직무기술서 - 승인 서명</h3>
        <p>대상: {target_name} ({target_sabun}) / 연도: {year} / 직무명: {_html_escape(jobname or (jd_saved or {}).get('직무명',''))}</p>
        <p class='print-hint'>※ 브라우저 인쇄(Ctrl/⌘+P) → PDF 저장을 권장합니다.</p>
        """
    render_pdf_controls("직무기술서_승인서명", _jd_print_html, images_to_embed=None)

# ══════════════════════════════════════════════════════════════════════════════
# 직무능력평가 (간편형 + 서명 카드 + PDF)
# ══════════════════════════════════════════════════════════════════════════════
COMP_SIMPLE_PREFIX = "직무능력평가_"
COMP_SIMPLE_HEADERS = [
    "연도","평가대상사번","평가대상이름","평가자사번","평가자이름",
    "평가일자","주업무평가","기타업무평가","교육이수","자격유지","종합의견",
    "상태","제출시각","잠금"
]
def _simp_sheet_name(year:int|str)->str: return f"{COMP_SIMPLE_PREFIX}{int(year)}"

def _ensure_comp_simple_sheet(year:int):
    wb=get_book(); name=_simp_sheet_name(year)
    try:
        ws=wb.worksheet(name)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=name, rows=1000, cols=50)
        _retry(ws.update, "1:1", [COMP_SIMPLE_HEADERS]); return ws
    header=_retry(ws.row_values,1) or []
    need=[h for h in COMP_SIMPLE_HEADERS if h not in header]
    if need: _retry(ws.update, "1:1", [header+need])
    return ws

def _jd_latest_for_comp(sabun:str, year:int)->dict:
    try:
        df=read_jobdesc_df()
        if df is None or len(df)==0: return {}
        q=df[(df["사번"].astype(str)==str(sabun))&(df["연도"].astype(int)==int(year))]
        if q.empty: return {}
        if "버전" in q.columns:
            try: q["버전"]=pd.to_numeric(q["버전"], errors="coerce").fillna(0)
            except Exception: pass
            q=q.sort_values("버전").iloc[-1]
        else:
            q=q.iloc[-1]
        return {c:q.get(c,"") for c in q.index}
    except Exception: return {}

def _edu_completion_from_jd(jd_row:dict)->str:
    val=str(jd_row.get("직원공통필수교육","")).strip()
    return "완료" if val else "미완료"

@st.cache_data(ttl=300, show_spinner=False)
def read_my_comp_simple_rows(year:int, sabun:str)->pd.DataFrame:
    try:
        ws=get_book().worksheet(_simp_sheet_name(year))
        df=pd.DataFrame(_ws_get_all_records(ws))
    except Exception: return pd.DataFrame(columns=COMP_SIMPLE_HEADERS)
    if df.empty: return df
    df=df[df["평가자사번"].astype(str)==str(sabun)]
    sort_cols=[c for c in ["평가대상사번","평가일자","제출시각"] if c in df.columns]
    if sort_cols: df=df.sort_values(sort_cols, ascending=[True,False,False])
    return df.reset_index(drop_by=True) if hasattr(df, "reset_index") else df.reset_index(drop=True)

def upsert_comp_simple_response(emp_df: pd.DataFrame, year:int, target_sabun:str,
                                evaluator_sabun:str, main_grade:str, extra_grade:str,
                                qual_status:str, opinion:str, eval_date:str)->dict:
    ws=_ensure_comp_simple_sheet(year)
    header=_retry(ws.row_values,1) or COMP_SIMPLE_HEADERS; hmap={n:i+1 for i,n in enumerate(header)}
    jd=_jd_latest_for_comp(target_sabun, int(year)); edu_status=_edu_completion_from_jd(jd)
    t_name=_emp_name_by_sabun(emp_df, target_sabun); e_name=_emp_name_by_sabun(emp_df, evaluator_sabun)
    now=kst_now_str()
    values=_retry(ws.get_all_values); cY=hmap.get("연도"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
    row_idx=0
    for i in range(2, len(values)+1):
        r=values[i-1]
        try:
            if (str(r[cY-1]).strip()==str(year) and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun)):
                row_idx=i; break
        except: pass
    if row_idx==0:
        buf=[""]*len(header)
        def put(k,v): c=hmap.get(k); buf[c-1]=v if c else ""
        put("연도",int(year)); put("평가대상사번",str(target_sabun)); put("평가대상이름",t_name)
        put("평가자사번",str(evaluator_sabun)); put("평가자이름",e_name)
        put("평가일자",eval_date); put("주업무평가",main_grade); put("기타업무평가",extra_grade)
        put("교육이수",edu_status); put("자격유지",qual_status); put("종합의견",opinion)
        put("상태","제출"); put("제출시각",now); put("잠금","")
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action":"insert"}
    else:
        def upd(k,v):
            c=hmap.get(k)
            if c: _retry(ws.update_cell, row_idx, c, v)
        upd("평가일자",eval_date); upd("주업무평가",main_grade); upd("기타업무평가",extra_grade)
        upd("교육이수",edu_status); upd("자격유지",qual_status); upd("종합의견",opinion)
        upd("상태","제출"); upd("제출시각",now)
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action":"update"}

def tab_competency(emp_df: pd.DataFrame):
    # 권한: 관리자/평가권한자만
    u_check = st.session_state.get('user', {})
    me_check = str(u_check.get('사번',''))
    am_admin_or_mgr = (is_admin(me_check) or len(get_allowed_sabuns(emp_df, me_check, include_self=False))>0)
    if not am_admin_or_mgr:
        st.warning('권한이 없습니다. 관리자/평가 권한자만 접근할 수 있습니다.', icon='🔒')
        return

    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmp_year")

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
    sel_label=st.selectbox("대상자 선택", opts, index=sel_idx, key="cmp_pick_select")
    sel_sab=sel_label.split(" - ",1)[0] if isinstance(sel_label,str) else sabuns[sel_idx]
    st.session_state["cmp_target_sabun"]=str(sel_sab)
    st.session_state["cmp_target_name"]=_emp_name_by_sabun(emp_df, str(sel_sab))

    st.success(f"대상자: {_emp_name_by_sabun(emp_df, sel_sab)} ({sel_sab})", icon="✅")

    with st.expander("직무기술서 요약", expanded=True):
        jd=_jd_latest_for(sel_sab, int(year))
        if jd:
            def V(key): return (_html_escape((jd.get(key,"") or "").strip()) or "—")
            html = f"""
            <div class="scrollbox">
              <div class="kv"><div class="k">직무명</div><div class="v">{V('직무명')}</div></div>
              <div class="kv"><div class="k">직무개요</div><div class="v">{V('직무개요')}</div></div>
              <div class="kv"><div class="k">주요 업무</div><div class="v">{V('주업무')}</div></div>
              <div class="kv"><div class="k">기타업무</div><div class="v">{V('기타업무')}</div></div>
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
    with colG[0]: g_main = st.radio("주업무 평가", grade_options, index=2, key="cmp_main", horizontal=False)
    with colG[1]: g_extra= st.radio("기타업무 평가", grade_options, index=2, key="cmp_extra", horizontal=False)
    with colG[2]: qual   = st.radio("직무 자격 유지 여부", ["직무 유지","직무 변경","직무비부여"], index=0, key="cmp_qual")
    with colG[3]:
        try: eval_date=st.date_input("평가일자", datetime.now(tz=tz_kst()).date(), key="cmp_date").strftime("%Y-%m-%d")
        except Exception: eval_date=st.date_input("평가일자", datetime.now().date(), key="cmp_date").strftime("%Y-%m-%d")

    try: edu_status=_edu_completion_from_jd(_jd_latest_for_comp(sel_sab, int(year)))
    except Exception: edu_status="미완료"
    st.metric("교육이수 (자동)", edu_status)
    opinion=st.text_area("종합평가 의견", value="", height=140, key="cmp_opinion")

    cbtn=st.columns([1,1,3])
    with cbtn[0]: do_save=st.button("제출/저장", type="primary", use_container_width=True, key="cmp_save")
    with cbtn[1]: do_reset=st.button("초기화", use_container_width=True, key="cmp_reset")
    if do_reset:
        for k in ["cmp_main","cmp_extra","cmp_qual","cmp_opinion"]:
            if k in st.session_state: del st.session_state[k]
        st.rerun()
    if do_save:
        rep=upsert_comp_simple_response(emp_df, int(year), str(sel_sab), str(me_sabun), g_main, g_extra, qual, opinion, eval_date)
        st.success(("제출 완료" if rep.get("action")=="insert" else "업데이트 완료"), icon="✅")

    st.markdown("### 내 제출 현황")
    try:
        my=read_my_comp_simple_rows(int(year), me_sabun)
        cols=[c for c in ["평가대상사번","평가대상이름","평가일자","주업무평가","기타업무평가","교육이수","자격유지","상태","제출시각"] if c in my.columns]
        st.dataframe(my[cols] if cols else my, use_container_width=True, height=220)
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

    # 평가자 서명 카드
    try:
        _sign_map = build_sign_map(read_sign_df())
        if _sign_map:
            st.markdown("#### 평가자 서명")
            render_signature_card("평가자", me_name, me_sabun, _sign_map)
    except Exception:
        st.caption("평가자 서명 카드 렌더 중 오류가 발생했습니다.")

    # 출력 / PDF
    def _cmp_print_html():
        return f"""
        <h3>직무능력평가 - 평가자 서명</h3>
        <p>대상: {st.session_state.get('cmp_target_name','')} ({st.session_state.get('cmp_target_sabun','')}) / 연도: {year}</p>
        <p class='print-hint'>※ 브라우저 인쇄(Ctrl/⌘+P) → PDF 저장을 권장합니다.</p>
        """
    render_pdf_controls("직무능력평가_서명", _cmp_print_html, images_to_embed=None)

# ══════════════════════════════════════════════════════════════════════════════

# PDF Controls (브라우저 인쇄 + ReportLab 보조)
def _slug_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "sec"

# ══════════════════════════════════════════════════════════════════════════════
def render_pdf_controls(section_title: str, html_content_getter, images_to_embed: dict|None):
    slug = _slug_key(section_title)
    st.markdown("#### 출력 / PDF")
    col = st.columns([1,1,3])
    with col[0]:
        if st.button("브라우저로 인쇄하기", key=f"print_{slug}", use_container_width=True):
            st.info("브라우저 메뉴에서 인쇄(Ctrl/⌘+P) → PDF로 저장을 선택하세요.", icon="🖨️")

    with col[1]:
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            from reportlab.lib.utils import ImageReader
            import base64
            if st.button("PDF 다운로드", key=f"pdfdl_{slug}", use_container_width=True):
                buf = io.BytesIO()
                c = canvas.Canvas(buf, pagesize=A4)
                width, height = A4
                y = height - 50
                c.setFont("Helvetica-Bold", 14)
                c.drawString(40, y, f"{section_title}")
                y -= 20
                c.setFont("Helvetica", 10)
                text = re.sub(r"<[^>]+>", " ", (html_content_getter() or ""))
                for line in re.findall(r".{1,80}", text):
                    y -= 12; c.drawString(40, y, line.strip())
                    if y < 100: c.showPage(); y = height - 50

                # (선택) 이미지를 PDF에 추가 — data:image/png;base64,... 형식만 처리
                if images_to_embed:
                    for title, data_uri in images_to_embed.items():
                        if not data_uri: continue
                        try:
                            if data_uri.startswith("data:image"):
                                b64 = data_uri.split("base64,",1)[-1]
                                raw = base64.b64decode(b64)
                                img = ImageReader(io.BytesIO(raw))
                                if y < 220:
                                    c.showPage(); y = height - 50
                                y -= 160
                                c.drawString(40, y+145, str(title))
                                c.drawImage(img, 40, y-10, width=220, preserveAspectRatio=True, mask='auto')
                                y -= 20
                        except Exception:
                            pass

                c.showPage(); c.save()
                pdf_bytes = buf.getvalue()
                st.download_button("PDF 저장", key=f"pdfsave_{slug}", data=pdf_bytes,
                                   file_name=f"{section_title}.pdf",
                                   mime="application/pdf",
                                   use_container_width=True)
        except Exception:
            st.caption("PDF 라이브러리가 없어 **브라우저 인쇄** 방식을 사용합니다.")

# ══════════════════════════════════════════════════════════════════════════════
# 관리자 탭 (서명 업로드/관리)
# ══════════════════════════════════════════════════════════════════════════════
def admin_sign_uploader():
    u = st.session_state.get("user", {})
    me = str(u.get("사번",""))
    if not is_admin(me):
        st.info("관리자 전용 메뉴입니다.", icon="🔒")
        return
    st.markdown("### 서명 등록(이미지 업로드) - 관리자용")
    sabun_for_upload = st.text_input("사번", value="", key="sign_upload_sabun")
    file = st.file_uploader("서명 이미지 (PNG/JPG 권장)", type=["png","jpg","jpeg"], key="sign_upload_file")
    if st.button("업로드/저장", key="admin_sign_upload", type="primary", disabled=not sabun_for_upload or not file):
        try:
            import base64
            b64 = base64.b64encode(file.read()).decode("utf-8")
            ws = _ws("서명관리")
            header = _retry(ws.row_values, 1) or []
            hmap = {n:i+1 for i,n in enumerate(header)}
            if "서명B64" not in hmap:
                new_header = header + ["서명B64"]
                _retry(ws.update, "1:1", [new_header])
                hmap = {n:i+1 for i,n in enumerate(new_header)}
            values = _retry(ws.get_all_values)
            row_idx = 0
            cS = hmap.get("사번")
            for i in range(2, len(values)+1):
                row = values[i-1]
                if cS and str(row[cS-1]).strip() == str(sabun_for_upload).strip():
                    row_idx = i; break
            if row_idx == 0:
                buf = [""] * len(hmap)
                buf[hmap["사번"]-1] = str(sabun_for_upload).strip()
                buf[hmap["서명B64"]-1] = b64
                if "활성" in hmap: buf[hmap["활성"]-1] = "TRUE"
                _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
            else:
                _retry(ws.update_cell, row_idx, hmap["서명B64"], b64)
                if "활성" in hmap:
                    _retry(ws.update_cell, row_idx, hmap["활성"], "TRUE")
            st.cache_data.clear()
            st.success("서명이 저장되었습니다.", icon="✅")
        except Exception as e:
            st.exception(e)

    st.markdown("---")
    st.markdown("#### 현재 등록된 서명 미리보기")
    try:
        sdf = read_sign_df()
        if not sdf.empty:
            preview = sdf[["사번","sign_render"]].copy()
            preview.columns=["사번","서명"]
            st.data_editor(preview, use_container_width=True, height=260,
                           column_config={"서명": st.column_config.ImageColumn("서명")})
        else:
            st.caption("등록된 서명이 없습니다.")
    except Exception:
        st.caption("서명 미리보기를 불러올 수 없습니다.")

# ══════════════════════════════════════════════════════════════════════════════
# 도움말
# ══════════════════════════════════════════════════════════════════════════════
def tab_help():
    st.markdown("### 도움말")
    st.markdown(
        """
        - **서명 방식**: `서명관리` 시트에 `사번`과 `서명B64`(Base64)를 입력하면, 앱이 자동으로 카드형 서명 이미지를 표시합니다.
        - **표시 위치**:
          - 인사평가: 1차/2차 평가자 **이름+서명** 카드
          - 직무기술서: 승인자 **이름+서명** 카드
          - 직무능력평가: 평가자 **이름+서명** 카드
        - **PDF 출력**: 각 탭 하단 **브라우저 인쇄** 버튼으로 PDF 저장을 권장합니다. (ReportLab이 설치되어 있으면 간단 PDF도 다운로드 가능)
        """
    )

# ══════════════════════════════════════════════════════════════════════════════
# App Main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.markdown(f"<div class='app-title-hero'>{_html_escape(APP_TITLE)}</div>", unsafe_allow_html=True)
    emp_df = read_emp_df()
    require_login(emp_df)

    # 좌측 영역
    with st.sidebar:
        st.markdown("### 직원 선택")
        render_staff_picker_left(emp_df)
        st.markdown("---")
        u=st.session_state.get("user",{})
        st.caption(f"로그인: {u.get('이름','')} ({u.get('사번','')})")
        if st.button("로그아웃", use_container_width=True):
            logout()

    tabs = st.tabs(["인사평가", "직무기술서", "직무능력평가", "관리자", "도움말"])
    with tabs[0]: tab_eval(emp_df)
    with tabs[1]: tab_job_desc(emp_df)
    with tabs[2]: tab_competency(emp_df)
    with tabs[3]: admin_sign_uploader()
    with tabs[4]: tab_help()

if __name__ == "__main__":
    main()
