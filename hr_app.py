# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (5-tabs unified)
- 메인 탭: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
- 로그인: Enter 키 이동/제출
- 좌측 '직원 선택': 권한 기반(관리자=전 직원, 일반=권한 범위)
- 직원 페이지: 관리자 탭 안에 포함(관리자만)
- 인사평가: 항목 로딩/저장/일괄적용 유지
"""
# ── Imports ──────────────────────────────────────────────────────────────────
import re, hashlib, random, time, secrets as pysecrets
from datetime import datetime
from typing import Any, Tuple
import pandas as pd, streamlit as st

# ── Timezone ─────────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# ── Google Sheets/gspread ────────────────────────────────────────────────────
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread==6.1.2", "google-auth==2.31.0"])
    import gspread
    from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

# ── App Config & CSS ─────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown("""
<style>
  .block-container { padding-top: .6rem !important; }
  .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
  .stTabs { margin-top: .4rem !important; }
  .badge-green{background:#E6FFED;border:1px solid #8BEA9B;color:#0F5132;
    display:inline-block;padding:.25rem .5rem;border-radius:.5rem;font-weight:600;}
  .badge-amber{background:#FFF4E5;border:1px solid #F7C774;color:#8A6D3B;
    display:inline-block;padding:.25rem .5rem;border-radius:.5rem;}
</style>
""", unsafe_allow_html=True)

# ── Utils ────────────────────────────────────────────────────────────────────
def now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t")
def _normalize_private_key(raw: str) -> str:
    if not raw: return raw
    return raw.replace("\\n", "\\n").replace("\\\\n", "\\n") if "BEGIN PRIVATE KEY" in raw else raw
def _pin_hash(pin: str, sabun: str) -> str:
    return hashlib.sha256(f"{str(sabun).strip()}:{str(pin).strip()}".encode()).hexdigest()

# ── gspread helpers ──────────────────────────────────────────────────────────
API_BACKOFF_SEC = [0.0, 0.8, 1.6, 3.2, 6.4, 9.6]
def _retry(fn, *args, **kwargs):
    last=None
    for b in API_BACKOFF_SEC:
        try: return fn(*args, **kwargs)
        except APIError as e:
            last=e; time.sleep(b + random.uniform(0,0.25))
    if last: raise last
    return fn(*args, **kwargs)

@st.cache_resource(show_spinner=False)
def _client():
    svc = dict(st.secrets["gcp_service_account"])
    svc["private_key"] = _normalize_private_key(svc.get("private_key",""))
    scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    return gspread.authorize(Credentials.from_service_account_info(svc, scopes=scopes))

@st.cache_resource(show_spinner=False)
def _book():
    return _client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

_WS_CACHE: dict[str, Tuple[float, Any]] = {}
_HDR_CACHE: dict[str, Tuple[float, list[str], dict]] = {}
_WS_TTL, _HDR_TTL = 120, 120

def _ws(title: str):
    now=time.time(); hit=_WS_CACHE.get(title)
    if hit and (now-hit[0]<_WS_TTL): return hit[1]
    ws=_retry(_book().worksheet, title); _WS_CACHE[title]=(now,ws); return ws

def _hdr(ws, key: str) -> Tuple[list[str], dict]:
    now=time.time(); hit=_HDR_CACHE.get(key)
    if hit and (now-hit[0]<_HDR_TTL): return hit[1], hit[2]
    header=_retry(ws.row_values, 1) or []; hmap={n:i+1 for i,n in enumerate(header)}
    _HDR_CACHE[key]=(now, header, hmap); return header, hmap

def _get_df(sheet: str) -> pd.DataFrame:
    ws=_ws(sheet)
    try: vals=_retry(ws.get_all_records, numericise_ignore=["all"])
    except TypeError: vals=_retry(ws.get_all_records)
    df=pd.DataFrame(vals)
    if df.empty: return pd.DataFrame()
    for c in ["사번","이름","PIN_hash"]:
        if c not in df.columns: df[c]=""
    df["사번"]=df["사번"].astype(str)
    if "재직여부" in df.columns: df["재직여부"]=df["재직여부"].map(_to_bool)
    return df

# ── Login (Enter key binding) ────────────────────────────────────────────────
import streamlit.components.v1 as components
def _inject_login_keybinder():
    components.html("""
    <script>
    (function(){
      function byLabel(txt){
        const doc=window.parent.document;
        const labels=[...doc.querySelectorAll('label')];
        const lab=labels.find(l=>(l.innerText||'').trim().startsWith(txt));
        if(!lab) return null;
        const root=lab.closest('div[data-testid="stTextInput"]')||lab.parentElement;
        return root? root.querySelector('input'): null;
      }
      function loginBtn(){
        const doc=window.parent.document;
        return [...doc.querySelectorAll('button')].find(b=>(b.textContent||'').trim()==='로그인');
      }
      function commit(el){
        if(!el) return;
        try{ el.dispatchEvent(new Event('input',{bubbles:true}));
             el.dispatchEvent(new Event('change',{bubbles:true}));
             el.blur(); }catch(e){}
      }
      function bind(){
        const sab=byLabel('사번'); const pin=byLabel('PIN'); const btn=loginBtn();
        if(!sab||!pin) return false;
        if(!sab._bound){
          sab._bound=true;
          sab.addEventListener('keydown',e=>{
            if(e.key==='Enter'){ e.preventDefault(); commit(sab); setTimeout(()=>{try{pin.focus();pin.select();}catch(_){}}); }
          });
        }
        if(!pin._bound){
          pin._bound=true;
          pin.addEventListener('keydown',e=>{
            if(e.key==='Enter'){ e.preventDefault(); commit(pin); commit(sab); const b=loginBtn(); setTimeout(()=>{try{b&&b.click();}catch(_){}} ,60); }
          });
        }
        return true;
      }
      bind();
      const mo=new MutationObserver(()=>{bind();});
      mo.observe(window.parent.document.body,{childList:true,subtree:true});
      setTimeout(()=>{try{mo.disconnect();}catch(e){}},8000);
    })();
    </script>
    """, height=0, width=0)

SESSION_TTL_MIN=30
def _session_valid()->bool:
    exp=st.session_state.get("auth_expires_at"); ok=st.session_state.get("authed", False)
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
    for k in list(st.session_state.keys()): st.session_state.pop(k, None)
    try: st.cache_data.clear()
    except Exception: pass
    st.rerun()

def show_login(emp_df: pd.DataFrame):
    st.header("로그인")
    sabun = st.text_input("사번", key="login_sabun")
    pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")
    if st.button("로그인", type="primary"):
        if not sabun or not pin: st.error("사번과 PIN을 입력하세요."); st.stop()
        row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
        if row.empty: st.error("사번을 찾을 수 없습니다."); st.stop()
        r=row.iloc[0]
        if not _to_bool(r.get("재직여부", True)): st.error("재직 상태가 아닙니다."); st.stop()
        stored=str(r.get("PIN_hash","")).strip().lower()
        entered_plain=_sha256_hex(pin.strip())
        entered_salted=_pin_hash(pin.strip(), str(r.get("사번","")))
        if stored not in (entered_plain, entered_salted):
            st.error("PIN이 올바르지 않습니다."); st.stop()
        _start_session({"사번":str(r.get("사번","")), "이름":str(r.get("이름",""))})
        st.success("환영합니다!"); st.rerun()
    _inject_login_keybinder()

def require_login(emp_df: pd.DataFrame):
    if not _session_valid():
        for k in ("authed","user","auth_expires_at","_state_owner_sabun"): st.session_state.pop(k, None)
        show_login(emp_df); st.stop()
    else:
        _ensure_state_owner()

# ── ACL ──────────────────────────────────────────────────────────────────────
AUTH_SHEET="권한"
AUTH_HEADERS=["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]
SEED_ADMINS=[
    {"사번":"113001","이름":"병원장","역할":"admin","범위유형":"","부서1":"","부서2":"","대상사번":"","활성":True,"비고":"seed"},
    {"사번":"524007","이름":"행정원장","역할":"admin","범위유형":"","부서1":"","부서2":"","대상사번":"","활성":True,"비고":"seed"},
]

@st.cache_data(ttl=60, show_spinner=False)
def read_auth_df()->pd.DataFrame:
    try:
        try: ws=_ws(AUTH_SHEET)
        except WorksheetNotFound:
            wb=_book(); ws=_retry(wb.add_worksheet, title=AUTH_SHEET, rows=1000, cols=20); _retry(ws.update, "1:1", [AUTH_HEADERS])
        df=pd.DataFrame(_retry(ws.get_all_records))
    except Exception:
        return pd.DataFrame(columns=AUTH_HEADERS)
    if df.empty: return pd.DataFrame(columns=AUTH_HEADERS)
    for c in AUTH_HEADERS:
        if c not in df.columns: df[c]=""
    df["사번"]=df["사번"].astype(str)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    return df

def is_admin(sabun:str)->bool:
    if str(sabun) in {a["사번"] for a in SEED_ADMINS}: return True
    df=read_auth_df()
    if df.empty: return False
    q=df[(df["사번"].astype(str)==str(sabun)) & (df["역할"].str.lower()=="admin") & (df["활성"]==True)]
    return not q.empty

def _infer_implied_scopes(emp_df:pd.DataFrame,sabun:str)->list[dict]:
    out=[]; me=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    if me.empty: return out
    r=me.iloc[0]; grade=str(r.get("직급","")); d1=str(r.get("부서1","")); d2=str(r.get("부서2","")); name=str(r.get("이름",""))
    if "부장" in grade: out.append({"사번":sabun,"이름":name,"역할":"manager","범위유형":"부서","부서1":d1,"부서2":"","대상사번":"","활성":True,"비고":"implied"})
    if "팀장" in grade: out.append({"사번":sabun,"이름":name,"역할":"manager","범위유형":"부서","부서1":d1,"부서2":d2,"대상사번":"","활성":True,"비고":"implied"})
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
                for p in re.split(r"[,\s]+", str(r.get("대상사번","")).strip()): 
                    if p: allowed.add(p)
    for r in _infer_implied_scopes(emp_df, sabun):
        if r["범위유형"]=="부서":
            d1=r["부서1"]; d2=r["부서2"]; tgt=emp_df.copy()
            if d1: tgt=tgt[tgt["부서1"].astype(str)==d1]
            if d2: tgt=tgt[tgt["부서2"].astype(str)==d2]
            allowed.update(tgt["사번"].astype(str).tolist())
    return allowed

# ── Global Target (selected staff) ───────────────────────────────────────────
def set_target(sabun:str, name:str=""):
    st.session_state["target_sabun"]=str(sabun).strip()
    st.session_state["target_name"]=str(name).strip()

def get_target(emp_df:pd.DataFrame)->Tuple[str,str]:
    sab=str(st.session_state.get("target_sabun","") or "").strip()
    nam=str(st.session_state.get("target_name","") or "").strip()
    if sab and not nam:
        row=emp_df.loc[emp_df["사번"].astype(str)==sab]
        if not row.empty: nam=str(row.iloc[0].get("이름",""))
    return sab, nam

def badge(emp_df:pd.DataFrame):
    sab, nam = get_target(emp_df)
    if sab: st.markdown(f"<span class='badge-green'>대상: {nam} ({sab})</span>", unsafe_allow_html=True)
    else:   st.markdown("<span class='badge-amber'>대상 미선택</span>", unsafe_allow_html=True)

# ── Left Staff Picker (권한 기반) ─────────────────────────────────────────────
def render_staff_picker_left(emp_df: pd.DataFrame):
    st.markdown("### 직원 선택")
    u=st.session_state.get("user",{}); me=str(u.get("사번",""))
    df=emp_df.copy()
    if not is_admin(me):
        allowed=get_allowed_sabuns(emp_df, me, include_self=True)
        df=df[df["사번"].astype(str).isin({str(s) for s in allowed})].copy()
    q=st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
    if q.strip():
        k=q.strip().lower()
        df=df[df.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이름"] if c in r), axis=1)]
    show=[c for c in (["사번","이름","부서1","부서2","직급"] if not is_admin(me) else df.columns) if c in df.columns]
    df=df.sort_values("사번") if "사번" in df.columns else df
    options=[f"{str(r['사번'])} - {str(r.get('이름',''))}" for _,r in df.iterrows()]
    picked=st.radio("대상 직원", options, index=0 if options else None, label_visibility="collapsed")
    if picked:
        sab=picked.split(" - ",1)[0].strip()
        name=picked.split(" - ",1)[1].strip() if " - " in picked else ""
        set_target(sab, name)
    st.dataframe(df[show], use_container_width=True, height=260, hide_index=True)

# ── 직원 탭(관리자 전용) ──────────────────────────────────────────────────────
def tab_staff_admin(emp_df: pd.DataFrame):
    me=str(st.session_state.get("user",{}).get("사번",""))
    if not is_admin(me):
        st.warning("관리자 전용 메뉴입니다.", icon="🔒"); return
    st.subheader("직원 (관리자 전용)")
    df=emp_df.copy()
    c=st.columns([1,1,1,1,1,1,2])
    with c[0]: dept1 = st.selectbox("부서1", ["(전체)"] + sorted([x for x in df.get("부서1", pd.Series(dtype=str)).dropna().unique() if x]), index=0)
    with c[1]: dept2 = st.selectbox("부서2", ["(전체)"] + sorted([x for x in df.get("부서2", pd.Series(dtype=str)).dropna().unique() if x]), index=0)
    with c[2]: grade = st.selectbox("직급",  ["(전체)"] + sorted([x for x in df.get("직급",  pd.Series(dtype=str)).dropna().unique() if x]), index=0)
    with c[3]: duty  = st.selectbox("직무",  ["(전체)"] + sorted([x for x in df.get("직무",  pd.Series(dtype=str)).dropna().unique() if x]), index=0)
    with c[4]: group = st.selectbox("직군",  ["(전체)"] + sorted([x for x in df.get("직군",  pd.Series(dtype=str)).dropna().unique() if x]), index=0)
    with c[5]: active= st.selectbox("재직여부", ["(전체)","재직","퇴직"], index=0)
    with c[6]: q     = st.text_input("검색(사번/이름)", "")
    view=df.copy()
    if dept1 != "(전체)" and "부서1" in view: view = view[view["부서1"] == dept1]
    if dept2 != "(전체)" and "부서2" in view: view = view[view["부서2"] == dept2]
    if grade != "(전체)" and "직급"  in view: view = view[view["직급"]  == grade]
    if duty  != "(전체)" and "직무"  in view: view = view[view["직무"]  == duty]
    if group != "(전체)" and "직군"  in view: view = view[view["직군"]  == group]
    if active!= "(전체)" and "재직여부" in view: view = view[view["재직여부"] == (active == "재직")]
    if q.strip():
        k=q.strip().lower()
        view=view[view.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이름"] if c in r), axis=1)]
    st.write(f"결과: **{len(view):,}명**")
    st.dataframe(view.drop(columns=["PIN_hash"], errors="ignore"), use_container_width=True, height=560, hide_index=True)

# ── 인사평가(항목 로딩/일괄적용/저장) ─────────────────────────────────────────
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID","항목","내용","순서","활성","비고"]
EVAL_RESP_SHEET_PREFIX = "평가_응답_"
EVAL_BASE_HEADERS = ["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각"]

def ensure_eval_items_sheet():
    wb=_book()
    try: ws=_ws(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=EVAL_ITEMS_SHEET, rows=200, cols=10); _retry(ws.update, "A1", [EVAL_ITEM_HEADERS]); return
    header=_retry(ws.row_values,1) or []
    need=[h for h in EVAL_ITEM_HEADERS if h not in header]
    if need: _retry(ws.update,"1:1",[header+need])

@st.cache_data(ttl=60, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    try:
        ensure_eval_items_sheet()
        ws=_ws(EVAL_ITEMS_SHEET)
        df=pd.DataFrame(_retry(ws.get_all_records))
    except Exception:
        return pd.DataFrame(columns=EVAL_ITEM_HEADERS)
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

def _eval_sheet_name(year:int|str)->str: return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def _ensure_eval_response_sheet(year:int, item_ids:list[str]):
    title=_eval_sheet_name(year); wb=_book()
    try: ws=_ws(title)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=title, rows=5000, cols=max(50, len(item_ids)+16)); _WS_CACHE[title]=(time.time(), ws)
    required=list(EVAL_BASE_HEADERS)+[f"점수_{iid}" for iid in item_ids]
    header,_=_hdr(ws, title)
    if not header:
        _retry(ws.update,"1:1",[required]); _HDR_CACHE[title]=(time.time(), required, {n:i+1 for i,n in enumerate(required)})
    else:
        need=[h for h in required if h not in header]
        if need:
            new_header=header+need; _retry(ws.update,"1:1",[new_header])
            _HDR_CACHE[title]=(time.time(), new_header, {n:i+1 for i,n in enumerate(new_header)})
    return ws

def _emp_name(emp_df:pd.DataFrame, sabun:str)->str:
    row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    return "" if row.empty else str(row.iloc[0].get("이름",""))

def upsert_eval_response(emp_df: pd.DataFrame, year: int, eval_type: str, target_sabun: str, evaluator_sabun: str, scores: dict[str, int], status: str = "제출") -> dict:
    items=read_eval_items_df(True); item_ids=[str(x) for x in items["항목ID"].tolist()]
    ws=_ensure_eval_response_sheet(year, item_ids)
    header=_retry(ws.row_values,1); hmap={n:i+1 for i,n in enumerate(header)}
    def c5(v): 
        try: v=int(v)
        except: v=3
        return min(5,max(1,v))
    scores_list=[c5(scores.get(i,3)) for i in item_ids]
    total=round(sum(scores_list)*(100.0/max(1,len(item_ids)*5)), 1)
    tname=_emp_name(emp_df, target_sabun); ename=_emp_name(emp_df, evaluator_sabun); now=now_str()
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
            c=hmap.get(f"점수_{iid}"); 
            if c: buf[c-1]=sc
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action":"insert","row":None,"total":total}
    payload={"총점": total, "상태": status, "제출시각": now, "평가대상이름": tname, "평가자이름": ename}
    for iid, sc in zip(item_ids, scores_list): payload[f"점수_{iid}"]=sc
    upd=[]
    for k,v in payload.items():
        c=hmap.get(k)
        if c:
            a1=gspread.utils.rowcol_to_a1(row_idx, c)
            upd.append({"range": a1, "values": [[v]]})
    if upd: _retry(ws.batch_update, upd)
    st.cache_data.clear()
    return {"action":"update","row":row_idx,"total":total}

def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> tuple[dict, dict]:
    try:
        items = read_eval_items_df(True)
        item_ids = [str(x) for x in items["항목ID"].tolist()]
        ws = _ensure_eval_response_sheet(year, item_ids)
        header = _retry(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _retry(ws.get_all_values)
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
        for k in ["상태", "제출시각", "총점"]:
            c = hmap.get(k)
            if c: meta[k] = row[c - 1]
        return scores, meta
    except Exception:
        return {}, {}

def tab_eval(emp_df: pd.DataFrame):
    st.subheader("인사평가")
    sab, nam = get_target(emp_df)
    badge(emp_df)
    if not sab:
        st.info("좌측에서 직원 한 명을 먼저 선택하세요.", icon="🧭"); return
    year = datetime.now(tz=tz_kst()).year
    items=read_eval_items_df(True)
    if items.empty: st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️"); return
    items=items.sort_values([c for c in ["순서","항목"] if c in items.columns]).reset_index(drop=True)

    me=str(st.session_state.get("user",{}).get("사번",""))
    eval_type = "자기" if sab==me else "1차"

    # 저장된 점수 로드
    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, sab, me)

    # 일괄적용 컨트롤
    col = st.columns([1,1,2,2])
    with col[0]: bulk_base = st.number_input("기본 점수", min_value=1, max_value=5, value=3, step=1, key="bulk_base")
    with col[1]: do_apply = st.button("일괄적용", key="bulk_apply")
    if do_apply:
        for iid in items["항목ID"].astype(str).tolist():
            st.session_state[f"eval_{iid}"] = int(bulk_base)

    # 항목 슬라이더 (저장된 값 우선)
    scores={}
    for _,r in items.iterrows():
        iid=str(r.get("항목ID","")); label=str(r.get("항목","(항목)"))
        default = int(saved_scores.get(iid, st.session_state.get(f"eval_{iid}", 3) or 3))
        scores[iid]=st.slider(label, 1, 5, default, 1, key=f"eval_{iid}")

    # 진행률 표시
    filled = sum(1 for v in scores.values() if v)
    st.caption(f"입력 진행: {filled}/{len(scores)}")

    if st.button("제출/저장", type="primary"):
        rep=upsert_eval_response(emp_df, int(year), eval_type, sab, me, scores, "제출")
        st.success(f"제출 완료 (총점 {rep.get('total')})", icon="✅")

# ── 직무기술서 / 직무능력평가 (타겟 동기화된 단순 폼, 저장 훅만 둠) ────────────────
def tab_jobdesc(emp_df: pd.DataFrame):
    st.subheader("직무기술서")
    sab, nam = get_target(emp_df)
    badge(emp_df)
    if not sab:
        st.info("좌측에서 직원 한 명을 먼저 선택하세요.", icon="🧭"); return
    st.text_area("주요 업무/책임", key="jd_main")
    st.text_area("필수 자격/역량", key="jd_req")
    st.text_area("우대사항", key="jd_pref")
    if st.button("저장", type="primary"): st.success("임시 저장 완료", icon="✅")

def tab_competency(emp_df: pd.DataFrame):
    st.subheader("직무능력평가")
    sab, nam = get_target(emp_df)
    badge(emp_df)
    if not sab:
        st.info("좌측에서 직원 한 명을 먼저 선택하세요.", icon="🧭"); return
    st.slider("직무지식", 1, 5, 3, 1, key="cm_knowledge")
    st.slider("문제해결", 1, 5, 3, 1, key="cm_problem")
    st.slider("협업/커뮤니케이션", 1, 5, 3, 1, key="cm_comm")
    if st.button("저장", type="primary"): st.success("임시 저장 완료", icon="✅")

# ── Admin/Help ───────────────────────────────────────────────────────────────
def admin_pin(emp_df): st.info("PIN 관리(기존 화면 연결 자리).", icon="🛠️")
def admin_transfer(emp_df): st.info("부서 이동(기존 화면 연결 자리).", icon="🛠️")

def tab_admin(emp_df: pd.DataFrame):
    st.subheader("관리자")
    tabs = st.tabs(["직원","PIN 관리","부서 이동","(권한/항목 관리)"])
    with tabs[0]: tab_staff_admin(emp_df)
    with tabs[1]: admin_pin(emp_df)
    with tabs[2]: admin_transfer(emp_df)
    with tabs[3]: st.info("기존 '권한/항목 관리' 화면 연결", icon="🛠️")

def tab_help():
    st.subheader("도움말")
    st.caption("기존 도움말 내용을 유지합니다.")

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    emp_df=_get_df(EMP_SHEET)
    st.session_state["emp_df"]=emp_df.copy()
    if not _session_valid():
        show_login(emp_df); return
    require_login(emp_df)

    left, right = st.columns([1, 4], gap="large")
    with left:
        u=st.session_state.get("user",{})
        st.markdown(f"**{APP_TITLE}**")
        st.caption(f"DB연결 {now_str()}")
        st.markdown(f"- 사용자: **{u.get('이름','')} ({u.get('사번','')})**")
        st.button("로그아웃", on_click=logout)
        st.divider()
        render_staff_picker_left(emp_df)

    with right:
        tabs = st.tabs(["인사평가","직무기술서","직무능력평가","관리자","도움말"])
        with tabs[0]: tab_eval(emp_df)
        with tabs[1]: tab_jobdesc(emp_df)
        with tabs[2]: tab_competency(emp_df)
        with tabs[3]: tab_admin(emp_df)
        with tabs[4]: tab_help()

if __name__ == "__main__":
    main()
