# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Streamlined)
- One-time staff selection → global sync across tabs
- Minimal staff table (search-only), admin vs. non-admin columns
- Keep Login/관리자/도움말 behaviors; plug your existing handlers where noted
"""
# ══════════════════════════════════════════════════════════════════════════════
# Imports
# ══════════════════════════════════════════════════════════════════════════════
import time, hashlib, random, re
from datetime import datetime
from typing import Any, Tuple
import pandas as pd
import streamlit as st

# Optional: zoneinfo
try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))

# Optional: gspread (lazy install if missing)
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ModuleNotFoundError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread==6.1.2", "google-auth==2.31.0"])
    import gspread
    from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError, WorksheetNotFound

# ══════════════════════════════════════════════════════════════════════════════
# App Config
# ══════════════════════════════════════════════════════════════════════════════
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
      .block-container { padding-top: 1.2rem !important; }
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .badge-green{background:#E6FFED;border:1px solid #8BEA9B;color:#0F5132;
        display:inline-block;padding:.25rem .5rem;border-radius:.5rem;font-weight:600;}
      .badge-amber{background:#FFF4E5;border:1px solid #F7C774;color:#8A6D3B;
        display:inline-block;padding:.25rem .5rem;border-radius:.5rem;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════════
# Utils
# ══════════════════════════════════════════════════════════════════════════════
def now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t")
def _normalize_private_key(raw: str) -> str:
    if not raw: return raw
    return raw.replace("\n", "\n").replace("\\n", "\n") if "BEGIN PRIVATE KEY" in raw else raw
def _pin_hash(pin: str, sabun: str) -> str:
    return hashlib.sha256(f"{str(sabun).strip()}:{str(pin).strip()}".encode()).hexdigest()

# ══════════════════════════════════════════════════════════════════════════════
# Google Sheets
# ══════════════════════════════════════════════════════════════════════════════
API_BACKOFF_SEC = [0.0, 0.8, 1.6, 3.2, 6.4, 9.6]
def _retry(fn, *args, **kwargs):
    last=None
    for b in API_BACKOFF_SEC:
        try: return fn(*args, **kwargs)
        except APIError as e:
            last=e; time.sleep(b + random.uniform(0,0.2))
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

# ══════════════════════════════════════════════════════════════════════════════
# Session/Auth
# ══════════════════════════════════════════════════════════════════════════════
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

def require_login(emp_df: pd.DataFrame):
    if not _session_valid():
        for k in ("authed","user","auth_expires_at","_state_owner_sabun"): st.session_state.pop(k, None)
        show_login(emp_df); st.stop()
    else:
        _ensure_state_owner()

# ══════════════════════════════════════════════════════════════════════════════
# ACL (Admin)
# ══════════════════════════════════════════════════════════════════════════════
AUTH_SHEET="권한"
AUTH_HEADERS=["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]

@st.cache_data(ttl=60, show_spinner=False)
def read_auth_df()->pd.DataFrame:
    try:
        ws=_ws(AUTH_SHEET); df=pd.DataFrame(_retry(ws.get_all_records))
    except Exception: return pd.DataFrame(columns=AUTH_HEADERS)
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
# Global Target (selected staff)
# ══════════════════════════════════════════════════════════════════════════════
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

def badge():
    sab, nam = get_target(st.session_state["emp_df"])
    if sab: st.markdown(f"<span class='badge-green'>대상: {nam} ({sab})</span>", unsafe_allow_html=True)
    else:   st.markdown("<span class='badge-amber'>대상 미선택</span>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Staff Picker (left column) + Staff Tab (minimal)
# ══════════════════════════════════════════════════════════════════════════════
def render_staff_picker_left(emp_df: pd.DataFrame, *, is_admin_view: bool):
    st.markdown("### 직원 선택")
    q=st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
    df=emp_df.copy()
    if q.strip():
        k=q.strip().lower()
        df=df[df.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이름"] if c in r), axis=1)]
    if is_admin_view:
        show=[c for c in df.columns if c!="PIN_hash"]
    else:
        show=[c for c in ["사번","이름","부서1","부서2","직급"] if c in df.columns]
    df=df.sort_values("사번") if "사번" in df.columns else df
    options=[f"{str(r['사번'])} - {str(r.get('이름',''))}" for _,r in df.iterrows()]
    picked=st.radio("대상 직원", options, index=0 if options else None, label_visibility="collapsed")
    if picked:
        sab=picked.split(" - ",1)[0].strip()
        name=picked.split(" - ",1)[1].strip() if " - " in picked else ""
        set_target(sab, name)
    st.dataframe(df[show], use_container_width=True, height=260, hide_index=True)

def tab_staff_minimal(emp_df: pd.DataFrame):
    st.subheader("직원")
    me=str(st.session_state.get("user",{}).get("사번",""))
    if not is_admin(me):
        allowed=get_allowed_sabuns(emp_df, me, include_self=True)
        emp_df=emp_df[emp_df["사번"].astype(str).isin(allowed)].copy()
    q=st.text_input("검색(사번/이름)", key="staff_q_simple")
    view=emp_df.copy()
    if q.strip():
        k=q.strip().lower()
        view=view[view.apply(lambda r: any(k in str(r[c]).lower() for c in ["사번","이름"] if c in r), axis=1)]
    show = [c for c in (["사번","이름","부서1","부서2","직급"] if not is_admin(me) else view.columns) if c in view.columns]
    view=view.sort_values("사번") if "사번" in view.columns else view
    st.write(f"결과: **{len(view):,}명**")
    pick=st.selectbox("대상 선택", ["(선택)"]+[f"{s} - {n}" for s,n in zip(view["사번"].astype(str), view.get("이름", pd.Series(['']*len(view))))], index=0)
    if pick!="(선택)":
        sab=pick.split(" - ",1)[0].strip()
        name=pick.split(" - ",1)[1].strip() if " - " in pick else ""
        set_target(sab, name)
    st.dataframe(view[show], use_container_width=True, height=560, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# Evaluation Tab (synced to global target)
# ══════════════════════════════════════════════════════════════════════════════
# NOTE: Plug your existing evaluation item loaders & upsert here as needed.
def _read_eval_items()->pd.DataFrame:
    try:
        ws=_ws("평가_항목"); df=pd.DataFrame(_retry(ws.get_all_records))
    except Exception: return pd.DataFrame(columns=["항목ID","항목","순서","활성"])
    if df.empty: return df
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    if "순서" in df.columns:
        def _i(x):
            try: return int(float(str(x).strip()))
            except: return 0
        df["순서"]=df["순서"].apply(_i)
    if "활성" in df.columns: df=df[df["활성"]==True]
    return df.sort_values([c for c in ["순서","항목"] if c in df.columns]).reset_index(drop=True)

def _ensure_eval_resp_sheet(year:int, item_ids:list[str]):
    title=f"평가_응답_{int(year)}"
    wb=_book()
    try:
        ws=_ws(title)
    except WorksheetNotFound:
        ws=_retry(wb.add_worksheet, title=title, rows=5000, cols=max(50, len(item_ids)+16))
        _WS_CACHE[title]=(time.time(), ws)
    base=["연도","평가유형","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각"]
    need=base+[f"점수_{i}" for i in item_ids]
    header,_=_hdr(ws, title)
    if not header:
        _retry(ws.update, "1:1", [need]); _HDR_CACHE[title]=(time.time(), need, {n:i+1 for i,n in enumerate(need)})
    else:
        miss=[h for h in need if h not in header]
        if miss:
            new=header+miss; _retry(ws.update, "1:1", [new])
            _HDR_CACHE[title]=(time.time(), new, {n:i+1 for i,n in enumerate(new)})
    return ws

def _emp_name(emp_df:pd.DataFrame, sabun:str)->str:
    row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    return "" if row.empty else str(row.iloc[0].get("이름",""))

def _upsert_eval(emp_df:pd.DataFrame, year:int, eval_type:str, target_sabun:str, evaluator_sabun:str, scores:dict)->float:
    items=_read_eval_items(); item_ids=[str(x) for x in items.get("항목ID", pd.Series(dtype=str)).tolist()]
    ws=_ensure_eval_resp_sheet(year, item_ids)
    header=_retry(ws.row_values, 1); hmap={n:i+1 for i,n in enumerate(header)}
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
        put("총점", total); put("상태", "제출"); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c=hmap.get(f"점수_{iid}"); 
            if c: buf[c-1]=sc
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
    else:
        payload={"총점": total, "상태": "제출", "제출시각": now, "평가대상이름": tname, "평가자이름": ename}
        for iid, sc in zip(item_ids, scores_list): payload[f"점수_{iid}"]=sc
        # batch update
        upd=[]
        for k,v in payload.items():
            c=hmap.get(k)
            if c:
                a1=gspread.utils.rowcol_to_a1(row_idx, c)
                upd.append({"range": a1, "values": [[v]]})
        if upd: _retry(ws.batch_update, upd)
    st.cache_data.clear()
    return total

def tab_eval(emp_df: pd.DataFrame):
    st.subheader("인사평가")
    sab, nam = get_target(emp_df)
    badge()
    if not sab:
        st.info("좌측에서 직원 한 명을 먼저 선택하세요.", icon="🧭"); return
    # year: auto / override via 설정 시트 가능
    year = datetime.now(tz=tz_kst()).year
    items=_read_eval_items()
    if items.empty: st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️"); return
    items=items.sort_values([c for c in ["순서","항목"] if c in items.columns]).reset_index(drop=True)
    scores={}
    for _,r in items.iterrows():
        iid=str(r.get("항목ID","")); label=str(r.get("항목","(항목)"))
        scores[iid]=st.slider(label, 1, 5, 3, 1, key=f"eval_{iid}")
    me=str(st.session_state.get("user",{}).get("사번",""))
    eval_type = "자기" if sab==me else "1차"
    if st.button("제출/저장", type="primary"):
        total=_upsert_eval(emp_df, int(year), eval_type, sab, me, scores)
        st.success(f"제출 완료 (총점 {total})", icon="✅")

# ══════════════════════════════════════════════════════════════════════════════
# Admin / Help tabs (placeholders to keep behavior; wire your existing pages)
# ══════════════════════════════════════════════════════════════════════════════
def tab_admin(emp_df: pd.DataFrame):
    st.subheader("관리자")
    st.caption("여기에 기존 PIN 관리 / 부서 이동 / 평가 항목 관리 / 권한 관리 화면을 그대로 연결하세요.")
    st.info("관리자용 페이지는 기존 함수 호출로 유지됩니다.", icon="🛠️")

def tab_help():
    st.subheader("도움말")
    st.caption("기존 도움말 내용을 유지합니다.")

# ══════════════════════════════════════════════════════════════════════════════
# Main App
# ══════════════════════════════════════════════════════════════════════════════
def main():
    # 1) Load employees
    emp_df=_get_df(EMP_SHEET)
    st.session_state["emp_df"]=emp_df.copy()
    # 2) Auth
    if not _session_valid():
        show_login(emp_df); return
    require_login(emp_df)

    # 3) Layout: left (login info + picker) / right (tabs)
    left, right = st.columns([1, 4], gap="large")
    with left:
        u=st.session_state.get("user",{})
        st.markdown(f"**{APP_TITLE}**")
        st.caption(f"DB연결 {now_str()}")
        st.markdown(f"- 사용자: **{u.get('이름','')} ({u.get('사번','')})**")
        st.button("로그아웃", on_click=logout)
        st.divider()
        render_staff_picker_left(emp_df, is_admin_view=is_admin(str(u.get("사번",""))))

    with right:
        tabs = st.tabs(["직원","인사평가","관리자","도움말"])
        with tabs[0]: tab_staff_minimal(emp_df)
        with tabs[1]: tab_eval(emp_df)
        with tabs[2]: tab_admin(emp_df)
        with tabs[3]: tab_help()

if __name__ == "__main__":
    main()
