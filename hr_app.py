# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (2025-09-21 FULL)
- 메인 탭: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
- 로그인: Enter(사번→PIN, PIN→로그인) 단축키
- 좌측 직원 선택: Enter 검색 → 대상 자동 동기화
- 권한관리: num_rows="dynamic", 옵션 고정, 유효성 검증 + 전체 저장
- 인사평가: 평가 항목 시트 기반 응답 저장(연도·유형·대상·평가자 키로 upsert)
- 직무기술서: 버전 관리 업서트(버전 0 입력시 자동 증가), 조회/수정/저장
- 직무능력평가: 간편형 — JD요약 참조, 간단 평가 저장
- 관리자: 직원/관리, PIN 관리, 평가 항목, 권한 관리
"""

# ──────────────────────────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────────────────────────
import re, time, random, hashlib
from datetime import datetime
from typing import Any, Tuple, Set
import pandas as pd
import streamlit as st

# Optional zoneinfo (KST)
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# gspread
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# ──────────────────────────────────────────────────────────────────────────────
# App Config / Style
# ──────────────────────────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
      .block-container{ padding-top: 2.2rem !important; } 
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .muted{color:#6b7280;}
      .scrollbox{ max-height: 280px; overflow-y: auto; padding: .6rem .75rem; background: #fafafa;
                  border: 1px solid #e5e7eb; border-radius: .5rem; }
      .scrollbox .kv{ margin-bottom: .6rem; }
      .scrollbox .k{ font-weight: 700; margin-bottom: .2rem; }
      .scrollbox .v{ white-space: pre-wrap; word-break: break-word; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Utils
# ──────────────────────────────────────────────────────────────────────────────
def kst_now_str() -> str:
    return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(str(s).encode()).hexdigest()

def _to_bool(x) -> bool:
    return str(x).strip().lower() in ("true","1","y","yes","t")

# ──────────────────────────────────────────────────────────────────────────────
# Google Auth / Sheets
# ──────────────────────────────────────────────────────────────────────────────
API_BACKOFF_SEC = [0.0, 0.6, 1.2, 2.4, 4.8, 7.2]
def _retry(fn, *args, **kwargs):
    last=None
    for b in API_BACKOFF_SEC:
        try: return fn(*args, **kwargs)
        except APIError as e:
            last=e; time.sleep(b+random.uniform(0,0.2))
    if last: raise last
    return fn(*args, **kwargs)

@st.cache_resource(show_spinner=False)
def get_client():
    svc = dict(st.secrets["gcp_service_account"])
    # normalize private_key \n
    pk = svc.get("private_key","")
    if "\\n" in pk and "BEGIN PRIVATE KEY" in pk: svc["private_key"] = pk.replace("\\n","\n")
    scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds=Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_book():
    return get_client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET   = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")
AUTH_SHEET  = st.secrets.get("sheets", {}).get("AUTH_SHEET", "권한")

# lightweight caches for worksheet/header
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

# ──────────────────────────────────────────────────────────────────────────────
# Sheet Readers
# ──────────────────────────────────────────────────────────────────────────────
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
    try:
        df = read_sheet_df(EMP_SHEET)
    except Exception:
        df = pd.DataFrame(columns=["사번","이름","부서1","부서2","직급","재직여부","PIN_hash"])
    for c in ["사번","이름","PIN_hash","부서1","부서2","직급","재직여부"]:
        if c not in df.columns: df[c]=""
    df["사번"]=df["사번"].astype(str)
    if "재직여부" in df.columns: df["재직여부"]=df["재직여부"].map(_to_bool)
    return df

# ──────────────────────────────────────────────────────────────────────────────
# Login + Session
# ──────────────────────────────────────────────────────────────────────────────
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
        entered_salted=hashlib.sha256(f"{str(r.get('사번','')).strip()}:{pin.strip()}".encode()).hexdigest()
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

# ──────────────────────────────────────────────────────────────────────────────
# ACL (권한)
# ──────────────────────────────────────────────────────────────────────────────
AUTH_HEADERS=["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]

@st.cache_data(ttl=300, show_spinner=False)
def read_auth_df()->pd.DataFrame:
    try:
        df=read_sheet_df(AUTH_SHEET)
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

def get_allowed_sabuns(emp_df:pd.DataFrame, sabun:str, include_self:bool=True)->Set[str]:
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

# ──────────────────────────────────────────────────────────────────────────────
# Global Target Sync + Left Picker
# ──────────────────────────────────────────────────────────────────────────────
def set_global_target(sabun:str, name:str=""):
    st.session_state["glob_target_sabun"]=str(sabun).strip()
    st.session_state["glob_target_name"]=str(name).strip()

def get_global_target()->Tuple[str,str]:
    return (str(st.session_state.get("glob_target_sabun","") or ""),
            str(st.session_state.get("glob_target_name","") or ""))

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

    cols=[c for c in ["사번","이름","부서1","부서2","직급"] if c in view.columns]
    st.dataframe(view[cols], use_container_width=True, height=300, hide_index=True)

# ──────────────────────────────────────────────────────────────────────────────
# 인사평가 (항목 + 응답)
# ──────────────────────────────────────────────────────────────────────────────
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고"]
EVAL_RESP_SHEET_PREFIX = "평가_응답_"
EVAL_BASE_HEADERS = ["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각","서명_대상","서명시각_대상","서명_평가자","서명시각_평가자","잠금"]

def _eval_sheet_name(year: int | str) -> str:
    return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

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
    values=_retry(ws.get_all_values); cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
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
    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="eval2_year")

    u = st.session_state["user"]; me_sabun = str(u["사번"]); me_name = str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False))>0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)
    items = read_eval_items_df(True)
    if items.empty: st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️"); return
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    item_ids = [str(x) for x in items_sorted["항목ID"].tolist()]

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("eval2_target_sabun", glob_sab or me_sabun)
    st.session_state.setdefault("eval2_target_name",  glob_name or me_name)
    st.session_state.setdefault("eval2_edit_mode",    False)

    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
        eval_type = "자기"; st.caption("평가유형: **자기**")
    else:
        base=emp_df.copy(); base["사번"]=base["사번"].astype(str)
        base=base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns: base=base[base["재직여부"]==True]
        view=base[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)
        _sabuns=view["사번"].astype(str).tolist(); _names=view["이름"].astype(str).tolist()
        _d2=view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""]*len(_sabuns)
        _opts=[f"{s} - {n} - {d2}" for s,n,d2 in zip(_sabuns,_names,_d2)]
        _target = st.session_state.get("eval2_target_sabun", glob_sab or "")
        _idx = _sabuns.index(_target) if _target in _sabuns else 0
        _sel = st.selectbox("대상자 선택", _opts, index=_idx, key="eval2_pick_editor_select")
        _sel_sab = _sel.split(" - ",1)[0] if isinstance(_sel,str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["eval2_target_sabun"]=str(_sel_sab)
        try:
            st.session_state["eval2_target_name"]=str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["eval2_target_name"]=""
        target_sabun=st.session_state["eval2_target_sabun"]
        target_name =st.session_state["eval2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")
        eval_type = st.radio("평가유형", ["자기","1차","2차"], horizontal=True, key=f"eval2_type_{year}_{me_sabun}_{target_sabun}")

    col_mode = st.columns([1,3])
    with col_mode[0]:
        if st.button(("수정모드로 전환" if not st.session_state["eval2_edit_mode"] else "보기모드로 전환"),
                     use_container_width=True, key="eval2_toggle"):
            st.session_state["eval2_edit_mode"] = not st.session_state["eval2_edit_mode"]; st.rerun()
    with col_mode[1]: st.caption(f"현재: **{'수정모드' if st.session_state['eval2_edit_mode'] else '보기모드'}**")
    edit_mode = bool(st.session_state["eval2_edit_mode"])

    def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> Tuple[dict, dict]:
        try:
            ws=_ensure_eval_resp_sheet(int(year), item_ids)
            header=_retry(ws.row_values,1) or []; hmap={n:i+1 for i,n in enumerate(header)}
            values=_retry(ws.get_all_values); cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
            row_idx=0
            for i in range(2, len(values)+1):
                r=values[i-1]
                try:
                    if (str(r[cY-1]).strip()==str(year) and str(r[cT-1]).strip()==str(eval_type)
                        and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun)):
                        row_idx=i; break
                except: pass
            if row_idx==0: return {}, {}
            row=values[row_idx-1]; scores={}
            for iid in item_ids:
                col=hmap.get(f"점수_{iid}")
                if col:
                    try: v=int(str(row[col-1]).strip() or "0")
                    except: v=0
                    if v: scores[iid]=v
            meta={}
            for k in ["상태","잠금","제출시각","총점"]:
                c=hmap.get(k)
                if c: meta[k]=row[c-1]
            return scores, meta
        except Exception:
            return {}, {}

    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, target_sabun, me_sabun)

    kbase=f"E2_{year}_{eval_type}_{me_sabun}_{target_sabun}"
    if (not am_admin_or_mgr) and (not saved_scores) and (not edit_mode):
        st.session_state["eval2_edit_mode"]=True; edit_mode=True

    st.markdown("#### 점수 입력 (각 1~5)")
    c_head, c_slider, c_btn = st.columns([5,2,1])
    with c_head: st.caption("라디오로 개별 점수를 고르거나, 슬라이더 ‘일괄 적용’을 사용하세요.")
    slider_key=f"{kbase}_slider"
    if slider_key not in st.session_state:
        if saved_scores:
            avg=round(sum(saved_scores.values())/max(1,len(saved_scores)))
            st.session_state[slider_key]=int(min(5,max(1,avg)))
        else:
            st.session_state[slider_key]=3
    with c_slider:
        bulk_score=st.slider("일괄 점수",1,5,st.session_state[slider_key],1, key=slider_key, disabled=not edit_mode)
    with c_btn:
        if st.button("일괄 적용", use_container_width=True, key=f"{kbase}_apply", disabled=not edit_mode):
            st.session_state[f"__apply_bulk_{kbase}"]=int(bulk_score); st.toast(f"모든 항목에 {bulk_score}점 적용", icon="✅")
    if st.session_state.get(f"__apply_bulk_{kbase}") is not None:
        _v=int(st.session_state[f"__apply_bulk_{kbase}"]); 
        for _iid in item_ids: st.session_state[f"eval2_seg_{_iid}_{kbase}"]=str(_v)
        del st.session_state[f"__apply_bulk_{kbase}"]

    scores={}
    for r in items_sorted.itertuples(index=False):
        iid=str(getattr(r, "항목ID")); name=getattr(r, "항목") or ""; desc=getattr(r, "내용") or ""
        rkey=f"eval2_seg_{iid}_{kbase}"
        if rkey not in st.session_state:
            st.session_state[rkey]=str(int(saved_scores[iid])) if iid in saved_scores else "3"
        col = st.columns([2,6,3])
        with col[0]: st.markdown(f'**{name}**')
        with col[1]:
            if str(desc).strip(): st.caption(str(desc))
        with col[2]:
            st.radio(" ", ["1","2","3","4","5"], horizontal=True, key=rkey, label_visibility="collapsed", disabled=not edit_mode)
        scores[iid]=int(st.session_state[rkey])

    total_100 = round(sum(scores.values()) * (100.0 / max(1, len(items_sorted) * 5)), 1)
    st.markdown("---")
    cM1, cM2 = st.columns([1, 3])
    with cM1: st.metric("합계(100점 만점)", total_100)
    with cM2: st.progress(min(1.0, total_100/100.0), text=f"총점 {total_100}점")

    if st.button("제출/저장", type="primary", key=f"eval2_save_{kbase}", disabled=not edit_mode):
        try:
            rep=upsert_eval_response(emp_df, int(year), eval_type, str(target_sabun), str(me_sabun), scores, "제출")
            st.success(("제출 완료" if rep["action"]=="insert" else "업데이트 완료")+f" (총점 {rep['total']}점)", icon="✅")
            st.session_state["eval2_edit_mode"]=False; st.rerun()
        except Exception as e:
            st.exception(e)

    st.markdown("#### 내 제출 현황")
    try:
        my=read_my_eval_rows(int(year), me_sabun)
        cols=[c for c in ["평가유형","평가대상사번","평가대상이름","총점","상태","제출시각"] if c in my.columns]
        st.dataframe(my[cols] if cols else my, use_container_width=True, height=260)
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

# ──────────────────────────────────────────────────────────────────────────────
# 직무기술서 (업서트 + 버전)
# ──────────────────────────────────────────────────────────────────────────────
JOBDESC_SHEET="직무기술서"
JOBDESC_HEADERS = [
    "사번","연도","버전","부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","서명방식","서명데이터","제출시각"
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
        ws=_retry(wb.add_worksheet,title=JOBDESC_SHEET, rows=2000, cols=60)
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
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="jd2_year")
    u=st.session_state["user"]; me_sabun=str(u["사번"]); me_name=str(u["이름"])
    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False))>0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("jd2_target_sabun", glob_sab or me_sabun)
    st.session_state.setdefault("jd2_target_name",  glob_name or me_name)
    st.session_state.setdefault("jd2_edit_mode",    False)

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
        _target=st.session_state.get("jd2_target_sabun", glob_sab or "")
        _idx=_sabuns.index(_target) if _target in _sabuns else 0
        _sel=st.selectbox("대상자 선택", _opts, index=_idx, key="jd2_pick_editor_select")
        _sel_sab=_sel.split(" - ",1)[0] if isinstance(_sel,str) and " - " in _sel else (_sabuns[_idx] if _sabuns else "")
        st.session_state["jd2_target_sabun"]=str(_sel_sab)
        try:
            st.session_state["jd2_target_name"]=str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["jd2_target_name"]=""
        target_sabun=st.session_state["jd2_target_sabun"]; target_name=st.session_state["jd2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    col_mode=st.columns([1,3])
    with col_mode[0]:
        if st.button(("수정모드로 전환" if not st.session_state["jd2_edit_mode"] else "보기모드로 전환"),
                     use_container_width=True, key="jd2_toggle"):
            st.session_state["jd2_edit_mode"]=not st.session_state["jd2_edit_mode"]; st.rerun()
    with col_mode[1]: st.caption(f"현재: **{'수정모드' if st.session_state['jd2_edit_mode'] else '보기모드'}**")
    edit_mode=bool(st.session_state["jd2_edit_mode"])

    jd_saved=_jd_latest_for(target_sabun, int(year))
    jd_current=jd_saved if jd_saved else {
        "사번":str(target_sabun),"연도":int(year),"버전":0,
        "부서1":emp_df.loc[emp_df["사번"].astype(str)==str(target_sabun)].get("부서1","").values[0] if "부서1" in emp_df.columns else "",
        "부서2":emp_df.loc[emp_df["사번"].astype(str)==str(target_sabun)].get("부서2","").values[0] if "부서2" in emp_df.columns else "",
        "작성자사번":me_sabun,"작성자이름":_emp_name_by_sabun(emp_df, me_sabun),
        "직군":"","직종":"","직무명":"","제정일":"","개정일":"","검토주기":"1년",
        "직무개요":"","주업무":"","기타업무":"","필요학력":"","전공계열":"",
        "직원공통필수교육":"","보수교육":"","기타교육":"","특성화교육":"",
        "면허":"","경력(자격요건)":"","비고":"","서명방식":"","서명데이터":""
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
    with c2[3]: series= st.text_input("직종",  value=jd_current.get("직종",""), key="jd2_series", disabled=not edit_mode)

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
        sign_data = st.text_input("서명데이터(텍스트/이미지-Base64 등)", value=jd_current.get("서명데이터",""),
                                  key="jd2_sign_data", disabled=not edit_mode)
    with c6[2]:
        pass

    if edit_mode and st.button("저장(업서트)", type="primary", use_container_width=True, key="jd2_save"):
        payload = {
            "사번": target_sabun, "연도": int(year), "버전": int(version),
            "부서1": dept1, "부서2": dept2, "작성자사번": me_sabun, "작성자이름": me_name,
            "직군": group, "직종": series, "직무명": jobname, "제정일": d_create, "개정일": d_update, "검토주기": review,
            "직무개요": job_summary, "주업무": job_main, "기타업무": job_other,
            "필요학력": edu_req, "전공계열": major_req, "직원공통필수교육": edu_common,
            "보수교육": edu_cont, "기타교육": edu_etc, "특성화교육": edu_spec,
            "면허": license_, "경력(자격요건)": career, "비고": memo,
            "서명방식": sign_type, "서명데이터": sign_data
        }
        try:
            rep=upsert_jobdesc(payload, as_new_version=(int(version)==0))
            st.success(f"저장 완료 (버전 {rep['version']}, {rep['action']})", icon="✅")
            st.rerun()
        except Exception as e:
            st.exception(e)

# ──────────────────────────────────────────────────────────────────────────────
# 직무능력평가 (간편형)
# ──────────────────────────────────────────────────────────────────────────────
def tab_competency(emp_df: pd.DataFrame):
    u = st.session_state.get('user', {})
    me = str(u.get('사번','')); me_name=str(u.get("이름",""))
    am_admin_or_mgr = (is_admin(me) or len(get_allowed_sabuns(emp_df, me, include_self=False))>0)
    allowed = get_allowed_sabuns(emp_df, me, include_self=True)

    this_year = datetime.now(tz=tz_kst()).year
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="comp_year")

    if not am_admin_or_mgr:
        target_sabun=me; target_name=me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base=emp_df.copy(); base["사번"]=base["사번"].astype(str)
        base=base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns: base=base[base["재직여부"]==True]
        view=base[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)
        _sabuns=view["사번"].astype(str).tolist(); _names=view["이름"].astype(str).tolist()
        _opts=[f"{s} - {n}" for s,n in zip(_sabuns,_names)]
        _idx=0
        _sel=st.selectbox("대상자 선택", _opts, index=_idx if _opts else 0, key="comp_pick")
        target_sabun=_sel.split(" - ",1)[0] if _opts else me
        target_name =_sel.split(" - ",1)[1] if _opts and " - " in _sel else me_name

    jd=_jd_latest_for(target_sabun, int(year))
    with st.expander("직무기술서 요약", expanded=False):
        st.write(f"**직무명:** {(jd or {}).get('직무명','')}")
        cc=st.columns(2)
        with cc[0]: st.markdown("**주업무**");  st.write((jd or {}).get("주업무","") or "—")
        with cc[1]: st.markdown("**기타업무**"); st.write((jd or {}).get("기타업무","") or "—")

    # 간편 역량 항목(고정형) — 필요 시 평가_항목 공유 가능하지만, 여기선 간단히 5개 고정
    comp_items=[
        ("C1","업무이해/적응"),
        ("C2","문제해결/개선"),
        ("C3","협업/의사소통"),
        ("C4","책임감/신뢰"),
        ("C5","전문성/학습")
    ]
    st.markdown("#### 역량 점수 (각 1~5)")
    edit=True
    scores={}
    base_key=f"COMP_{year}_{me}_{target_sabun}"
    for cid, cname in comp_items:
        k=f"{base_key}_{cid}"
        st.session_state.setdefault(k, "3")
        st.radio(cname, ["1","2","3","4","5"], horizontal=True, key=k)
        scores[cid]=int(st.session_state[k])

    total_100 = round(sum(scores.values()) * (100.0 / (len(comp_items) * 5)), 1)
    st.markdown("---")
    cM1, cM2 = st.columns([1, 3])
    with cM1: st.metric("합계(100점 만점)", total_100)
    with cM2: st.progress(min(1.0, total_100/100.0), text=f"총점 {total_100}점")

    # 평가_응답_연도 시트에 "평가유형=역량" 으로 저장
    if st.button("제출/저장", type="primary", key=f"comp_save_{base_key}"):
        try:
            rep=upsert_eval_response(emp_df, int(year), "역량", str(target_sabun), str(me), scores, "제출")
            st.success(("제출 완료" if rep["action"]=="insert" else "업데이트 완료")+f" (총점 {rep['total']}점)", icon="✅")
        except Exception as e:
            st.exception(e)

# ──────────────────────────────────────────────────────────────────────────────
# 관리자 서브탭들
# ──────────────────────────────────────────────────────────────────────────────
def tab_staff_admin(emp_df: pd.DataFrame):
    st.caption("직원 시트는 외부 소스(인사 DB) 연동 권장. 이 화면은 조회/내보내기 중심입니다.")
    cols=[c for c in ["사번","이름","부서1","부서2","직급","재직여부"] if c in emp_df.columns]
    st.dataframe(emp_df[cols] if cols else emp_df, use_container_width=True, height=460)
    st.download_button("CSV 내보내기", emp_df.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"직원_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                       mime="text/csv", use_container_width=True)

def tab_admin_pin(emp_df: pd.DataFrame):
    me=st.session_state["user"]["사번"]
    if not is_admin(me):
        st.error("관리자만 접근할 수 있습니다.", icon="🛡️"); return
    sabuns=emp_df["사번"].astype(str).tolist(); names=emp_df.get("이름", pd.Series([""]*len(emp_df))).astype(str).tolist()
    opts=[f"{s} - {n}" for s,n in zip(sabuns,names)]
    idx=0
    sel=st.selectbox("직원 선택", opts, index=idx if opts else 0, key="pin_pick")
    tgt_sab=sel.split(" - ",1)[0] if opts else ""

    new_pin = st.text_input("새 PIN (숫자)", type="password", key="pin_new")
    if st.button("PIN 재설정", type="primary"):
        if not tgt_sab or not new_pin.strip():
            st.error("대상과 새 PIN을 입력하세요."); return
        try:
            wb=get_book(); ws=_ws(EMP_SHEET)
            header=_retry(ws.row_values,1) or []; hmap={n:i+1 for i,n in enumerate(header)}
            values=_retry(ws.get_all_values)
            row_idx=0; cS=hmap.get("사번"); cP=hmap.get("PIN_hash")
            for i in range(2,len(values)+1):
                r=values[i-1]
                if str(r[cS-1]).strip()==str(tgt_sab): row_idx=i; break
            if row_idx==0: st.error("직원 시트에서 대상을 찾지 못했습니다."); return
            salted=hashlib.sha256(f"{tgt_sab}:{new_pin.strip()}".encode()).hexdigest()
            _retry(ws.update_cell, row_idx, cP, salted)
            st.cache_data.clear()
            st.success("PIN이 재설정되었습니다.", icon="✅")
        except Exception as e:
            st.exception(e)

def tab_admin_eval_items(emp_df: pd.DataFrame):
    me=st.session_state["user"]["사번"]
    if not is_admin(me):
        st.error("관리자만 접근할 수 있습니다.", icon="🛡️"); return
    df=read_eval_items_df(only_active=False).copy()
    if df.empty:
        df=pd.DataFrame({h:[] for h in EVAL_ITEM_HEADERS})
    # 안전한 기본값
    for h in EVAL_ITEM_HEADERS:
        if h not in df.columns: df[h]=""
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)

    column_config={
        "항목ID": st.column_config.TextColumn("항목ID", help="고유 ID"),
        "항목": st.column_config.TextColumn("항목"),
        "내용": st.column_config.TextColumn("내용"),
        "순서": st.column_config.NumberColumn("순서", min_value=0, step=1),
        "활성": st.column_config.CheckboxColumn("활성"),
        "비고": st.column_config.TextColumn("비고")
    }
    edited = st.data_editor(
        df[EVAL_ITEM_HEADERS],
        key="eval_items_editor",
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        height=520,
        column_config=column_config,
    )

    if st.button("항목 저장(전체 반영)", type="primary"):
        try:
            wb=get_book()
            try:
                ws=wb.worksheet(EVAL_ITEMS_SHEET)
            except WorksheetNotFound:
                ws=wb.add_worksheet(title=EVAL_ITEMS_SHEET, rows=200, cols=10)
                ws.update("A1",[EVAL_ITEM_HEADERS])
            header=_retry(ws.row_values,1) or EVAL_ITEM_HEADERS
            ws.clear(); ws.update("A1",[header])
            out=edited.copy().fillna("")
            # 순서 정수 보정
            if "순서" in out.columns:
                def _i(x):
                    try: return int(float(str(x).strip()))
                    except: return 0
                out["순서"]=out["순서"].apply(_i)
            rows = out.apply(lambda r: [str(r.get(h, "")) for h in header], axis=1).tolist()
            if rows:
                CHUNK=500
                for i in range(0, len(rows), CHUNK):
                    ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")
            st.cache_data.clear()
            st.success("평가 항목이 반영되었습니다.", icon="✅")
        except Exception as e:
            st.exception(e)

def tab_admin_acl(emp_df: pd.DataFrame):
    me = st.session_state.get("user", {})
    am_admin = is_admin(str(me.get("사번","")))
    if not am_admin:
        st.error("Master만 저장할 수 있습니다. (표/저장 모두 비활성화)", icon="🛡️")

    # 직원 레이블/룩업
    base = emp_df[["사번","이름","부서1","부서2"]].copy() if not emp_df.empty else pd.DataFrame(columns=["사번","이름","부서1","부서2"])
    base["사번"]=base["사번"].astype(str).str.strip()
    emp_lookup = {str(r["사번"]).strip(): {"이름": str(r.get("이름","")).strip(),
                                           "부서1": str(r.get("부서1","")).strip(),
                                           "부서2": str(r.get("부서2","")).strip()} for _,r in base.iterrows()}
    sabuns = sorted(emp_lookup.keys())
    labels, label_by_sabun, sabun_by_label = [], {}, {}
    for s in sabuns:
        nm=emp_lookup[s]["이름"]; lab=f"{s} - {nm}" if nm else s
        labels.append(lab); label_by_sabun[s]=lab; sabun_by_label[lab]=s

    df_auth = read_auth_df().copy()
    if df_auth.empty: df_auth = pd.DataFrame(columns=AUTH_HEADERS)
    def _tostr(x): return "" if x is None else str(x)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df_auth.columns: df_auth[c]=df_auth[c].map(_tostr)
    if "활성" in df_auth.columns:
        df_auth["활성"]=df_auth["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

    df_disp=df_auth.copy()
    if "사번" in df_disp.columns:
        df_disp["사번"]=df_disp["사번"].map(lambda v: label_by_sabun.get(str(v).strip(), str(v).strip()))

    role_options  = ["admin","manager","evaluator"]
    scope_options = ["","부서","개별"]

    if "삭제" not in df_disp.columns:
        df_disp.insert(len(df_disp.columns), "삭제", False)

    column_config = {
        "사번": st.column_config.SelectboxColumn("사번 - 이름", options=labels, help="사번을 선택하면 이름은 자동 동기화됩니다."),
        "이름": st.column_config.TextColumn("이름", help="사번 선택 시 자동 보정됩니다."),
        "역할": st.column_config.SelectboxColumn("역할", options=role_options),
        "범위유형": st.column_config.SelectboxColumn("범위유형", options=scope_options, help="빈값=전체 / 부서 / 개별"),
        "부서1": st.column_config.TextColumn("부서1"),
        "부서2": st.column_config.TextColumn("부서2"),
        "대상사번": st.column_config.TextColumn("대상사번", help="범위유형이 '개별'일 때 대상 사번(쉼표/공백 구분)"),
        "활성": st.column_config.CheckboxColumn("활성"),
        "비고": st.column_config.TextColumn("비고"),
        "삭제": st.column_config.CheckboxColumn("삭제", help="저장 시 체크된 행은 삭제됩니다."),
    }

    edited = st.data_editor(
        df_disp[[c for c in AUTH_HEADERS if c in df_disp.columns] + ["삭제"]],
        key="auth_editor",
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        height=520,
        disabled=not am_admin,
        column_config=column_config,
    )

    def _editor_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
        df=df.copy()
        if "사번" in df.columns:
            for i, val in df["사번"].items():
                v=str(val).strip()
                if not v: continue
                sab = sabun_by_label.get(v) or (v.split(" - ",1)[0].strip() if " - " in v else v)
                df.at[i,"사번"]=sab
                nm = emp_lookup.get(sab,{}).get("이름","")
                if nm: df.at[i,"이름"]=nm
        return df

    edited_canon = _editor_to_canonical(edited.drop(columns=["삭제"], errors="ignore"))

    def _validate_and_fix(df: pd.DataFrame):
        df=df.copy().fillna("")
        errs=[]

        # 빈행 제거
        df = df[df.astype(str).apply(lambda r: "".join(r.values).strip() != "", axis=1)]

        # 기본 필드 보정
        if "사번" in df.columns:
            for i,row in df.iterrows():
                sab=str(row.get("사번","")).strip()
                if not sab:
                    errs.append(f"{i+1}행: 사번이 비어 있습니다."); continue
                if sab not in emp_lookup:
                    errs.append(f"{i+1}행: 사번 '{sab}' 은(는) 직원 목록에 없습니다."); continue
                nm=emp_lookup[sab]["이름"]
                if str(row.get("이름","")).strip()!=nm: df.at[i,"이름"]=nm
                if not str(row.get("부서1","")).strip(): df.at[i,"부서1"]=emp_lookup[sab]["부서1"]
                if not str(row.get("부서2","")).strip(): df.at[i,"부서2"]=emp_lookup[sab]["부서2"]

        if "역할" in df.columns:
            bad=df[~df["역할"].isin(role_options) & (df["역할"].astype(str).str.strip()!="")]
            for i in bad.index.tolist():
                errs.append(f"{i+1}행: 역할 값이 잘못되었습니다. ({df.loc[i,'역할']})")
        if "범위유형" in df.columns:
            bad=df[~df["범위유형"].isin(scope_options) & (df["범위유형"].astype(str).str.strip()!="")]
            for i in bad.index.tolist():
                errs.append(f"{i+1}행: 범위유형 값이 잘못되었습니다. ({df.loc[i,'범위유형']})")

        # 중복 규칙 탐지
        keycols=[c for c in ["사번","역할","범위유형","부서1","부서2","대상사번"] if c in df.columns]
        if keycols:
            dup=df.assign(_key=df[keycols].astype(str).agg("|".join, axis=1)).duplicated("_key", keep=False)
            if dup.any():
                dup_idx=(dup[dup]).index.tolist()
                errs.append("중복 규칙 발견: " + ", ".join(str(i+1) for i in dup_idx) + " 행")

        if "활성" in df.columns:
            df["활성"]=df["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

        for c in AUTH_HEADERS:
            if c not in df.columns: df[c]=""
        df=df[AUTH_HEADERS].copy()
        return df, errs

    fixed_df, errs = _validate_and_fix(edited_canon)

    if errs:
        st.warning("저장 전 확인이 필요합니다:\n- " + "\n- ".join(errs))

    c1, c2 = st.columns([1,4])
    with c1:
        do_save = st.button("🗂️ 권한 전체 반영", type="primary", use_container_width=True, disabled=(not am_admin))
    with c2:
        st.caption("※ 표에서 추가·수정·삭제 후 **저장**을 눌러 반영합니다. (전체 덮어쓰기)")

    if do_save:
        if errs:
            st.error("유효성 오류가 있어 저장하지 않았습니다. 위 경고를 확인해주세요.", icon="⚠️")
            return
        try:
            wb=get_book()
            try:
                ws=wb.worksheet(AUTH_SHEET)
            except WorksheetNotFound:
                ws=wb.add_worksheet(title=AUTH_SHEET, rows=500, cols=12)
                ws.update("A1", [AUTH_HEADERS])
            header = ws.row_values(1) or AUTH_HEADERS

            # 전체 초기화 후 헤더 재기입
            ws.clear()
            ws.update("A1", [header])

            out=fixed_df.copy()
            rows = out.apply(lambda r: [str(r.get(h, "")) for h in header], axis=1).tolist()
            if rows:
                CHUNK=500
                for i in range(0, len(rows), CHUNK):
                    ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")

            st.cache_data.clear()
            st.success("권한이 전체 반영되었습니다.", icon="✅")
            st.rerun()
        except Exception as e:
            st.exception(e)

# ──────────────────────────────────────────────────────────────────────────────
# 도움말
# ──────────────────────────────────────────────────────────────────────────────
def tab_help():
    st.markdown("### 도움말")
    st.write("- 좌측 상단에서 대상자를 검색/선택하면, 각 탭에서 동일 대상이 유지됩니다.")
    st.write("- 관리자 메뉴의 ‘권한 관리’에서 관리자/매니저/평가자 권한을 부여할 수 있습니다.")
    st.write("- 권한 규칙 저장 시 전체 덮어쓰기를 수행합니다. 저장 전에 경고를 꼭 확인하세요.")
    st.write("- 인사평가/직무기술서/직무능력평가 데이터는 구글시트에 실시간 반영됩니다.")

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    st.title(APP_TITLE)
    emp_df = read_emp_df()
    require_login(emp_df)

    # 좌측 사이드바 상단 정보 영역
    with st.sidebar:
        u = st.session_state.get("user", {})
        uname = str(u.get("이름", "") or "")
        usabun = str(u.get("사번", "") or "")
        st.markdown(
            f"""
- DB연결 {kst_now_str()}
- 사용자: {uname} ({usabun})
            """.strip()
        )
        st.divider()

        # 기존 좌측 직원 검색/선택 + 로그아웃
        st.markdown("#### 직원 검색/선택")
        render_staff_picker_left(emp_df)
        if st.button("로그아웃", use_container_width=True):
            logout()

    tabs = st.tabs(["인사평가", "직무기술서", "직무능력평가", "관리자", "도움말"])

    with tabs[0]:
        tab_eval(emp_df)

    with tabs[1]:
        tab_job_desc(emp_df)

    with tabs[2]:
        tab_competency(emp_df)

    with tabs[3]:
        st.subheader("관리자 메뉴")
        a1, a2, a3, a4 = st.tabs(["직원/관리", "PIN 관리", "평가 항목", "권한 관리"])
        with a1: tab_staff_admin(emp_df)
        with a2: tab_admin_pin(emp_df)
        with a3: tab_admin_eval_items(emp_df)
        with a4: tab_admin_acl(emp_df)  # 권한 관리 — 단일 호출 (중복 제거)

    with tabs[4]:
        tab_help()

if __name__ == "__main__":
    main()
