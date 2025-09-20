# -*- coding: utf-8 -*-
# HISMEDI HR App (full single-file) — includes working "권한 관리" admin sub-tab.
# Tabs: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
# Notes:
# - Requires st.secrets with gcp_service_account and sheets IDs.
# - Admin sub-tab "권한 관리" reads/writes Google Sheet "권한".
# - First line has no leading blank line.

import time, random, re, hashlib
from datetime import datetime, timedelta
from typing import Any, Tuple

import pandas as pd
import streamlit as st
from html import escape as _html_escape

# ===== Config =====
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

# ===== Style =====
st.markdown(
    """
    <style>
      .block-container{ padding-top: 2.0rem !important; }
      .stTabs [role='tab']{ padding:10px 14px !important; font-size:1.02rem !important; }
      .scrollbox{ max-height: 280px; overflow-y: auto; padding: .6rem .75rem;
                  background: #fafafa; border: 1px solid #e5e7eb; border-radius: .5rem; }
      .scrollbox .k{ font-weight: 700; margin-bottom: .2rem; }
      .scrollbox .v{ white-space: pre-wrap; word-break: break-word; }
    </style>
    """, unsafe_allow_html=True
)

# ===== Utils =====
def _to_bool(x) -> bool: return str(x).strip().lower() in ("true","1","y","yes","t","on")
def _sha256_hex(s: str) -> str: return hashlib.sha256(str(s).encode()).hexdigest()

try:
    from zoneinfo import ZoneInfo
    def tz_kst(): return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst(): return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
def kst_now_str(): return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")

# ===== Google Sheets =====
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError

def _normalize_private_key(raw: str) -> str:
    if not raw: return raw
    return raw.replace("\n","
") if "\n" in raw and "BEGIN PRIVATE KEY" in raw else raw

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

_WS_CACHE = {}
_HDR_CACHE = {}
_WS_TTL, _HDR_TTL = 90, 90

def _retry(fn, *args, **kwargs):
    last=None
    for b in [0.0,0.6,1.2,2.4,4.8,7.0]:
        try: return fn(*args, **kwargs)
        except APIError as e:
            last=e; time.sleep(b+random.uniform(0,0.2))
    if last: raise last
    return fn(*args, **kwargs)

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

# ===== Sheets/Models =====
EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")
AUTH_SHEET = "권한"
AUTH_HEADERS = ["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]

@st.cache_data(ttl=300, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    ws=_ws(sheet_name); df=pd.DataFrame(_ws_get_all_records(ws))
    if df.empty: return df
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def read_emp_df() -> pd.DataFrame:
    df=read_sheet_df(EMP_SHEET).copy()
    for c in ["사번","이름","PIN_hash","재직여부"]:
        if c not in df.columns: df[c]=""
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    if "재직여부" in df.columns: df["재직여부"]=df["재직여부"].map(_to_bool)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def read_auth_df() -> pd.DataFrame:
    try:
        ws=_ws(AUTH_SHEET); df=pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=AUTH_HEADERS)
    if df.empty: return pd.DataFrame(columns=AUTH_HEADERS)
    for c in AUTH_HEADERS:
        if c not in df.columns: df[c]=""
    df["사번"]=df["사번"].astype(str)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    return df[AUTH_HEADERS].copy()

def _ensure_auth_headers(ws):
    header, hmap = _hdr(ws, AUTH_SHEET)
    need=[h for h in AUTH_HEADERS if h not in header]
    if need:
        _retry(ws.update, "1:1", [header+need])
        header, hmap = _hdr(ws, AUTH_SHEET)
    return header, hmap

def save_auth_df(df: pd.DataFrame):
    ws=_ws(AUTH_SHEET)
    _ensure_auth_headers(ws)
    df = df.copy()
    for c in AUTH_HEADERS:
        if c not in df.columns:
            df[c] = "" if c!="활성" else False
    df=df[AUTH_HEADERS]
    df["사번"]=df["사번"].astype(str)
    if "활성" in df.columns:
        df["활성"]=df["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t","on"))
    payload=[AUTH_HEADERS]+df.fillna("").values.tolist()
    _retry(ws.clear)
    _retry(ws.update, "A1", payload, value_input_option="RAW")
    try: read_auth_df.clear()
    except Exception: pass

def is_admin(sabun: str) -> bool:
    df=read_auth_df()
    if df.empty: return False
    q=df[(df["사번"].astype(str)==str(sabun))&(df["역할"].astype(str).str.lower()=="admin")&(df["활성"]==True)]
    return not q.empty

def get_allowed_sabuns(emp_df: pd.DataFrame, sabun: str, include_self=True)->set[str]:
    sabun=str(sabun); allowed=set([sabun]) if include_self else set()
    if is_admin(sabun): return set(emp_df["사번"].astype(str).tolist())
    df=read_auth_df()
    if not df.empty:
        mine=df[(df["사번"].astype(str)==sabun)&(df["활성"]==True)]
        for _,r in mine.iterrows():
            t=str(r.get("범위유형","")).strip()
            if t=="부서":
                d1=str(r.get("부서1","")).strip(); d2=str(r.get("부서2","")).strip()
                tgt=emp_df.copy()
                if d1 and "부서1" in tgt.columns: tgt=tgt[tgt["부서1"].astype(str)==d1]
                if d2 and "부서2" in tgt.columns: tgt=tgt[tgt["부서2"].astype(str)==d2]
                allowed.update(tgt["사번"].astype(str).tolist())
            elif t=="개별":
                for p in re.split(r"[,\s]+", str(r.get("대상사번","")).strip()):
                    if p: allowed.add(p)
    return allowed

# ===== Login & Session =====
SESSION_TTL_MIN=30
def _session_valid()->bool:
    exp=st.session_state.get("auth_expires_at")
    ok=st.session_state.get("authed", False)
    return bool(ok and exp and time.time()<exp)

def _start_session(user: dict):
    st.session_state["authed"]=True
    st.session_state["user"]=user
    st.session_state["auth_expires_at"]=time.time()+SESSION_TTL_MIN*60

def show_login(emp_df: pd.DataFrame):
    st.markdown("### 로그인")
    sabun = st.text_input("사번", key="login_sabun")
    pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")
    if st.button("로그인", type="primary"):
        if not sabun or not pin: st.error("사번과 PIN을 입력하세요."); st.stop()
        row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
        if row.empty: st.error("사번을 찾을 수 없습니다."); st.stop()
        r=row.iloc[0]
        if not _to_bool(r.get("재직여부", True)): st.error("재직 상태가 아닙니다."); st.stop()
        stored=str(r.get("PIN_hash","")).strip().lower()
        entered=_sha256_hex(pin.strip())
        if stored not in (entered, _sha256_hex(f"{sabun}:{pin}")):
            st.error("PIN이 올바르지 않습니다."); st.stop()
        _start_session({"사번":str(r.get("사번","")), "이름":str(r.get("이름",""))})
        st.success("환영합니다!"); st.rerun()

def require_login(emp_df: pd.DataFrame):
    if not _session_valid(): show_login(emp_df); st.stop()

# ===== Tabs: 인사평가 / 직무기술서 / 직무능력평가 (placeholders kept minimal) =====
def tab_eval(emp_df: pd.DataFrame):
    st.markdown("### 인사평가")
    st.info("여기에 인사평가 폼/리스트 구현을 연결하세요.")

def tab_job_desc(emp_df: pd.DataFrame):
    st.markdown("### 직무기술서")
    st.info("여기에 직무기술서 보기/편집 UI를 연결하세요.")

def tab_competency(emp_df: pd.DataFrame):
    st.markdown("### 직무능력평가")
    st.info("여기에 직무능력평가 간편형 UI를 연결하세요.")

# ===== Admin sub tabs =====
def tab_staff_admin(emp_df: pd.DataFrame):
    st.markdown("### 직원(관리자 전용)")
    st.dataframe(emp_df, use_container_width=True, height=420)

def tab_admin_pin(emp_df: pd.DataFrame):
    st.markdown("### PIN 관리")
    st.caption("샘플 화면입니다.")

def tab_admin_transfer(emp_df: pd.DataFrame):
    st.markdown("### 부서 이동")
    st.caption("샘플 화면입니다.")

def tab_admin_eval_items():
    st.markdown("### 평가 항목 관리")
    st.caption("샘플 화면입니다.")

def tab_admin_acl():
    st.markdown("### 권한 관리")
    st.caption('데이터 소스: 구글시트 **"권한"** 시트')

    df = read_auth_df().copy()
    if df.empty:
        df = pd.DataFrame(columns=AUTH_HEADERS)

    colcfg = {
        "사번":      st.column_config.TextColumn(width="small"),
        "이름":      st.column_config.TextColumn(width="small"),
        "역할":      st.column_config.TextColumn(width="small", help="예: admin / (빈칸)"),
        "범위유형":  st.column_config.SelectboxColumn(options=["","부서","개별"], help="권한 범위"),
        "부서1":     st.column_config.TextColumn(width="small"),
        "부서2":     st.column_config.TextColumn(width="small"),
        "대상사번":  st.column_config.TextColumn(help="개별 선택 시 쉼표/공백 구분"),
        "활성":      st.column_config.CheckboxColumn(),
        "비고":      st.column_config.TextColumn(width="medium"),
    }

    st.write(f"현재 등록: **{len(df):,}건**")
    edited = st.data_editor(
        df, key="acl_editor",
        num_rows="dynamic", use_container_width=True, hide_index=True,
        column_config=colcfg,
    )

    c = st.columns([1,1,1,2])
    with c[0]:
        if st.button("변경 저장", type="primary", use_container_width=True, key="acl_save"):
            try:
                save_auth_df(edited)
                st.success("저장 완료 · 권한 시트에 반영되었습니다.", icon="✅")
                st.rerun()
            except Exception as e:
                st.exception(e)
    with c[1]:
        if st.button("새로고침", use_container_width=True, key="acl_refresh"):
            try: read_auth_df.clear()
            except Exception: pass
            st.rerun()
    with c[2]:
        csv_bytes = edited.reindex(columns=AUTH_HEADERS).to_csv(index=False).encode("utf-8-sig")
        st.download_button("CSV로 내보내기", data=csv_bytes, file_name="권한_backup.csv",
                           mime="text/csv", use_container_width=True)
    with c[3]:
        up = st.file_uploader("CSV 업로드(헤더 포함)", type=["csv"], accept_multiple_files=False, key="acl_upload")
        if up is not None:
            try:
                df_up = pd.read_csv(up)
                for col in AUTH_HEADERS:
                    if col not in df_up.columns:
                        df_up[col] = "" if col != "활성" else False
                df_up = df_up.reindex(columns=AUTH_HEADERS)
                df_up["사번"] = df_up["사번"].astype(str)
                if "활성" in df_up.columns:
                    df_up["활성"] = df_up["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t","on"))
                st.dataframe(df_up, use_container_width=True, hide_index=True)
                if st.button("업로드 내용을 저장", type="primary", key="acl_upload_commit"):
                    save_auth_df(df_up)
                    st.success("업로드 내용을 저장했습니다.", icon="✅")
                    st.rerun()
            except Exception as e:
                st.error(f"업로드 실패: {e}")

# ===== Help =====
def tab_help():
    st.markdown("### 도움말")
    st.write("""
- 좌측 상단의 직원 검색/선택과 각 탭의 대상 선택은 연동됩니다.
- 관리자 권한은 구글시트 "권한" 시트에서 `역할=admin`, `활성=True` 로 부여합니다.
- 문제 발생 시 캐시를 초기화하거나, 시트 헤더(1행)를 점검해 주세요.
""")

# ===== Main =====
def main():
    emp_df = read_emp_df()
    require_login(emp_df)

    u = st.session_state.get("user", {})
    me = str(u.get("사번",""))

    tabs = st.tabs(["인사평가", "직무기술서", "직무능력평가", "관리자", "도움말"])
    with tabs[0]: tab_eval(emp_df)
    with tabs[1]: tab_job_desc(emp_df)
    with tabs[2]: tab_competency(emp_df)
    with tabs[3]:
        if not is_admin(me):
            st.warning("관리자 전용 메뉴입니다.", icon="🔒")
        else:
            a1, a2, a3, a4, a5 = st.tabs(["직원", "PIN 관리", "부서 이동", "평가 항목 관리", "권한 관리"])
            with a1: tab_staff_admin(emp_df)
            with a2: tab_admin_pin(emp_df)
            with a3: tab_admin_transfer(emp_df)
            with a4: tab_admin_eval_items()
            with a5: tab_admin_acl()
    with tabs[4]: tab_help()

if __name__ == "__main__":
    main()
