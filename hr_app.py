# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (2025-09-21, 권한관리/탭 안정화 버전)
- 메인 탭: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
- 관리자 → 서브탭: 직원/관리, PIN 관리, 평가 항목, 권한 관리 (중복 라벨 제거)
- 권한관리: st.data_editor 안정화(고유 key, num_rows="dynamic", 옵션 고정, 삭제 가상컬럼), 저장 전 유효성 검증
- 로그인: Enter(사번→PIN, PIN→로그인) 단축키
- 좌측 검색 Enter → 대상 선택 자동 동기화
- 캐시 TTL 최적화, 구글시트 전제(서비스 계정/Sheet ID는 secrets에서 읽음)
"""

# ──────────────────────────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────────────────────────
import re, time, random, hashlib
from datetime import datetime, timedelta
from typing import Any, Tuple
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
      .block-container{ padding-top: 2.0rem !important; } 
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .muted{color:#6b7280;}
      .scrollbox{ max-height: 280px; overflow-y: auto; padding: .6rem .75rem; background: #fafafa;
                  border: 1px solid #e5e7eb; border-radius: .5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Utils
# ──────────────────────────────────────────────────────────────────────────────
def kst_now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t")

# ──────────────────────────────────────────────────────────────────────────────
# Google Auth / Sheets
# ──────────────────────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────────────────────
# Sheet Readers
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    ws=get_book().worksheet(sheet_name)
    try:
        rows=ws.get_all_records(numericise_ignore=["all"])
    except TypeError:
        rows=ws.get_all_records()
    df=pd.DataFrame(rows)
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

def tab_admin_acl(emp_df: pd.DataFrame):
    st.markdown("### 권한 관리")
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
# Global Target Sync + Left Picker (간단형)
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
    st.dataframe(view[cols], use_container_width=True, height=260, hide_index=True)

# ──────────────────────────────────────────────────────────────────────────────
# 탭: 인사평가(요약), 직무기술서(요약), 직무능력평가(접근권한 게이트)
# ──────────────────────────────────────────────────────────────────────────────
def tab_eval(emp_df: pd.DataFrame):
    st.info("인사평가 탭은 기존 로직을 유지합니다. (이 빌드에서는 요약 UI만 배치)", icon="ℹ️")
    st.caption("필요 시 기존 평가 로직을 이 파일로 통합해 드릴 수 있습니다.")

def tab_job_desc(emp_df: pd.DataFrame):
    st.info("직무기술서 탭은 기존 로직을 유지합니다. (이 빌드에서는 요약 UI만 배치)", icon="ℹ️")
    st.caption("필요 시 기존 직무기술서 저장/버전 로직을 이 파일로 통합해 드릴 수 있습니다.")

def tab_competency(emp_df: pd.DataFrame):
    u = st.session_state.get('user', {})
    me = str(u.get('사번',''))
    am_admin_or_mgr = (is_admin(me) or len(get_allowed_sabuns(emp_df, me, include_self=False))>0)
    if not am_admin_or_mgr:
        st.warning('권한이 없습니다. 관리자/평가 권한자만 접근할 수 있습니다.', icon='🔒')
        return
    st.success("접근 허용됨: 관리자/평가권한자", icon="✅")
    st.caption("이 영역에 간편 직무능력평가 UI를 배치하세요.")

# ──────────────────────────────────────────────────────────────────────────────
# 관리자 서브탭 (스텁 + 권한관리)
# ──────────────────────────────────────────────────────────────────────────────
def tab_staff_admin(emp_df: pd.DataFrame):
    st.caption("직원/관리 탭 (스텁) — 기존 기능을 여기에 이식 가능.")

def tab_admin_pin(emp_df: pd.DataFrame):
    st.caption("PIN 관리 탭 (스텁) — PIN 재설정, 해싱 등 기존 기능을 이식 가능.")

def tab_admin_eval_items(emp_df: pd.DataFrame):
    st.caption("평가 항목 탭 (스텁) — 평가 항목 CRUD/UI 이식 가능.")

def tab_help():
    st.markdown("### 도움말")
    st.write("- 좌측 상단에서 대상자를 검색/선택하면, 각 탭에서 동일 대상이 유지됩니다.")
    st.write("- 관리자 메뉴의 ‘권한 관리’에서 관리자/매니저/평가자 권한을 부여할 수 있습니다.")
    st.write("- 권한 규칙 저장 시 전체 덮어쓰기를 수행합니다. 저장 전에 경고를 꼭 확인하세요.")

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main():
    st.title(APP_TITLE)
    emp_df = read_emp_df()
    require_login(emp_df)

    # 좌측 선택
    with st.sidebar:
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
