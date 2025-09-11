# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Google Sheets 연동)
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import time, re, hashlib, random, secrets as pysecrets
from datetime import datetime, timedelta
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

# ── Page Config (FIRST Streamlit command) ────────────────────────────────────
st.set_page_config(page_title="HISMEDI - HR App", layout="wide")

# ── Guard Bootstrap (place ABOVE any @guard_page usage) ──────────────────────
try:
    guard_page  # already defined?
except NameError:
    import streamlit as st
    import traceback, time

    def show_recovery_card(error):
        with st.container(border=True):
            st.error("앱 실행 중 오류가 발생했어요.")
            st.caption(type(error).__name__ if isinstance(error, Exception) else "Error")
            with st.expander("자세한 오류 로그"):
                st.code(traceback.format_exc() if isinstance(error, Exception) else str(error))
            st.button("🔄 다시 시도", on_click=st.rerun, use_container_width=True)

    def guard_page(fn):
        def _inner(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                show_recovery_card(e)
        return _inner

# ── Recovery / Retry Utils (ADD) ──────────────────────────────────────────────
import traceback

# 로그인/인증 상태를 보존할 세션 키 (현재 파일 구조 기준)
AUTH_KEYS = {"authed", "user", "auth_expires_at"}

def soft_reset():
    """인증키는 보존하고 나머지 상태만 초기화 후 재실행"""
    for k in list(st.session_state.keys()):
        if k not in AUTH_KEYS:
            del st.session_state[k]
    st.rerun()

def hard_reload():
    """쿼리스트링에 타임스탬프를 붙여 강제 리로드 느낌 + rerun"""
    try:
        st.experimental_set_query_params(_ts=str(int(time.time())))
    except Exception:
        pass
    st.rerun()

def show_recovery_card(error):
    """에러 발생 시 복구 UI 카드 표시"""
    with st.container(border=True):
        st.error("앱 실행 중 오류가 발생했어요.")
        st.caption(type(error).__name__ if isinstance(error, Exception) else "Error")
        with st.expander("자세한 오류 로그"):
            st.code(traceback.format_exc() if isinstance(error, Exception) else str(error))
        c1, c2, c3 = st.columns(3)
        c1.button("🔄 다시 시도", on_click=st.rerun, use_container_width=True)
        c2.button("🧹 상태 초기화 후 재시작", on_click=soft_reset, use_container_width=True)
        c3.button("♻️ 강제 리로드(캐시 무시)", on_click=hard_reload, use_container_width=True)

def render_global_actions():
    """사이드बार에 항상 보이는 복구 버튼 3종"""
    with st.sidebar:
        st.markdown("### ⚙️ 빠른 복구")
        st.button("🔄 다시 시도", on_click=st.rerun, use_container_width=True)
        st.button("🧹 상태 초기화", on_click=soft_reset, use_container_width=True)
        st.button("♻️ 강제 리로드", on_click=hard_reload, use_container_width=True)

# ── App Config ────────────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown(
    """
    <style>
      .block-container { padding-top: 1.35rem !important; }
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .grid-head{ font-size:.9rem; color:#6b7280; margin:.2rem 0 .5rem; }
      .app-title{
        font-size: 1.28rem; line-height: 1.45rem; margin: .2rem 0 .6rem; font-weight: 800;
      }
      @media (min-width:1280px){
        .app-title{ font-size: 1.34rem; line-height: 1.5rem; }
      }
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

def _gs_retry(callable_fn, tries: int = 5, base: float = 0.6, factor: float = 2.0):
    """gspread API 호출을 지수 백오프로 재시도."""
    for i in range(tries):
        try:
            return callable_fn()
        except APIError:
            time.sleep(base * (factor ** i) + random.uniform(0, 0.2))
    return callable_fn()

# ── Non-critical error silencer ───────────────────────────────────────────────
SILENT_NONCRITICAL_ERRORS = True  # 읽기/표시 오류는 숨김, 저장 오류만 노출

def _silent_df_exception(e: Exception, where: str, empty_columns: list[str] | None = None) -> pd.DataFrame:
    if not SILENT_NONCRITICAL_ERRORS:
        st.error(f"{where}: {e}")
    return pd.DataFrame(columns=empty_columns or [])

# ── Google API Retry Helper ───────────────────────────────────────────────────
API_MAX_RETRY = 4
API_BACKOFF_SEC = [0.0, 0.6, 1.2, 2.4]

def _retry_call(fn, *args, **kwargs):
    err = None
    for i in range(API_MAX_RETRY):
        try:
            return fn(*args, **kwargs)
        except (APIError, Exception) as e:
            err = e
            time.sleep(API_BACKOFF_SEC[min(i, len(API_BACKOFF_SEC) - 1)])
    raise err

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

# ── Sheet Helpers ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=90, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    try:
        ws = _retry_call(get_workbook().worksheet, sheet_name)
        records = _retry_call(ws.get_all_records, numericise_ignore=["all"])
        df = pd.DataFrame(records)
    except Exception:
        if sheet_name == EMP_SHEET and "emp_df_cache" in st.session_state:
            st.caption("※ 직원 시트 실시간 로딩 실패 → 캐시 사용")
            df = st.session_state["emp_df_cache"].copy()
        else:
            raise

    if "관리자여부" in df.columns:
        df["관리자여부"] = df["관리자여부"].map(_to_bool)
    if "재직여부" in df.columns:
        df["재직여부"] = df["재직여부"].map(_to_bool)

    for c in ["입사일", "퇴사일"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    for c in ["사번", "이름", "PIN_hash"]:
        if c not in df.columns:
            df[c] = ""

    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)

    return df

def _get_ws_and_headers(sheet_name: str):
    ws = get_workbook().worksheet(sheet_name)
    header = ws.row_values(1) or []
    if not header:
        raise RuntimeError(f"'{sheet_name}' 헤더(1행) 없음")
    return ws, header, {n:i+1 for i,n in enumerate(header)}

def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    c = hmap.get("사번")
    if not c: return 0
    for i, v in enumerate(ws.col_values(c)[1:], start=2):
        if str(v).strip() == str(sabun).strip():
            return i
    return 0

def _update_cell(ws, row, col, value): ws.update_cell(row, col, value)

def _hide_doctors(df: pd.DataFrame) -> pd.DataFrame:
    if "직무" not in df.columns:
        return df
    col = df["직무"].astype(str).str.strip().str.lower()
    return df[~col.eq("의사")]

@st.cache_data(ttl=120, show_spinner=False)
def _build_name_map(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    return {str(r["사번"]): str(r.get("이름", "")) for _, r in df.iterrows()}

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
    import streamlit.components.v1 as components

    st.header("로그인")

    with st.form("login_form", clear_on_submit=False):
        sabun = st.text_input("사번", placeholder="예) 123456", key="login_sabun")
        pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")
        submitted = st.form_submit_button("로그인", use_container_width=True, type="primary")

    components.html("""
    <script>
    (function(){
      function qdoc(){ try{ return window.frameElement?.ownerDocument || window.parent.document; }catch(e){ return document; } }
      function labelInput(labelText){
        const doc = qdoc();
        const label = [...doc.querySelectorAll('label')].find(l => l.textContent.trim() === labelText);
        if (!label) return null;
        return label.parentElement.querySelector('input');
      }
      function setup(){
        const doc = qdoc();
        const sabun = labelInput('사번');
        const pin   = labelInput('PIN (숫자)');
        const loginBtn = [...doc.querySelectorAll('button')].find(b => b.innerText.trim() === '로그인');
        if (sabun && !sabun.value) sabun.focus();
        doc.addEventListener('keydown', function(e){
          const active = doc.activeElement;
          if (e.key === 'Enter'){
            if (active === sabun && pin){ e.preventDefault(); pin.focus(); }
            else if (active === pin && loginBtn){ e.preventDefault(); loginBtn.click(); }
          }
        }, true);
      }
      setTimeout(setup, 120);
    })();
    </script>
    """, height=0)

    if not submitted:
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

    if str(r.get("PIN_hash","")).strip().lower() != _sha256_hex(pin.strip()):
        st.error("PIN이 올바르지 않습니다."); st.stop()

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
    wb=get_workbook()
    try:
        ws=wb.worksheet(AUTH_SHEET)
        header=ws.row_values(1) or []
        need=[h for h in AUTH_HEADERS if h not in header]
        if need: ws.update("1:1",[header+need]); header=ws.row_values(1)
        vals=ws.get_all_records(numericise_ignore=["all"])
        cur_admins={str(r.get("사번","")).strip() for r in vals if str(r.get("역할","")).strip()=="admin"}
        add=[r for r in SEED_ADMINS if r["사번"] not in cur_admins]
        if add:
            rows=[[r.get(h,"") for h in header] for r in add]
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        return ws
    except WorksheetNotFound:
        ws=wb.add_worksheet(title=AUTH_SHEET, rows=1000, cols=20)
        ws.update("A1",[AUTH_HEADERS])
        ws.append_rows([[r.get(h,"") for h in AUTH_HEADERS] for r in SEED_ADMINS], value_input_option="USER_ENTERED")
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_auth_df() -> pd.DataFrame:
    try:
        ensure_auth_sheet()
        ws = get_workbook().worksheet(AUTH_SHEET)
        df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    except Exception as e:
        return _silent_df_exception(e, "권한 시트 읽기", AUTH_HEADERS)

    if df.empty:
        return pd.DataFrame(columns=AUTH_HEADERS)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    return df

def is_admin(sabun: str) -> bool:
    s = str(sabun).strip()
    if s in {a["사번"] for a in SEED_ADMINS}:
        return True
    try:
        df = read_auth_df()
    except Exception:
        return False
    if df.empty:
        return False
    q = df[
        (df["사번"].astype(str) == s)
        & (df["역할"].str.lower() == "admin")
        & (df["활성"] == True)
    ]
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

# ── Settings: 직무기술서 기본값 ────────────────────────────────────────────────
SETTINGS_SHEET = "설정"
SETTINGS_HEADERS = ["키", "값", "메모", "수정시각", "수정자사번", "수정자이름", "활성"]

def ensure_settings_sheet():
    wb = get_workbook()
    try:
        ws = wb.worksheet(SETTINGS_SHEET)
        header = ws.row_values(1) or []
        need = [h for h in SETTINGS_HEADERS if h not in header]
        if need:
            ws.update("1:1", [header + need])
        return ws
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=SETTINGS_SHEET, rows=200, cols=10)
        ws.update("A1", [SETTINGS_HEADERS])
        return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_settings_df() -> pd.DataFrame:
    try:
        ensure_settings_sheet()
        ws = get_workbook().worksheet(SETTINGS_SHEET)
        df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    except Exception as e:
        return _silent_df_exception(e, "설정 시트 읽기", SETTINGS_HEADERS)

    if df.empty:
        return pd.DataFrame(columns=SETTINGS_HEADERS)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    for c in ["키", "값", "메모", "수정자사번", "수정자이름"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def get_setting(key: str, default: str = "") -> str:
    try:
        df = read_settings_df()
    except Exception:
        return default

    if df.empty or "키" not in df.columns:
        return default
    q = df[df["키"].astype(str) == str(key)]
    if "활성" in df.columns:
        q = q[q["활성"] == True]
    if q.empty:
        return default
    return str(q.iloc[-1].get("값", default))

def set_setting(key: str, value: str, memo: str, editor_sabun: str, editor_name: str):
    try:
        ws = ensure_settings_sheet()
        header = ws.row_values(1) or SETTINGS_HEADERS
        hmap = {n: i + 1 for i, n in enumerate(header)}

        col_key = hmap.get("키")
        row_idx = 0
        if col_key:
            vals = _gs_retry(lambda: ws.col_values(col_key))
            for i, v in enumerate(vals[1:], start=2):
                if str(v).strip() == str(key).strip():
                    row_idx = i
                    break

        now = kst_now_str()
        if row_idx == 0:
            row = [""] * len(header)
            def put(k, v):
                c = hmap.get(k)
                if c:
                    row[c - 1] = v
            put("키", key); put("값", value); put("메모", memo); put("수정시각", now)
            put("수정자사번", editor_sabun); put("수정자이름", editor_name); put("활성", True)
            _gs_retry(lambda: ws.append_row(row, value_input_option="USER_ENTERED"))
        else:
            updates = []
            for k, v in [
                ("값", value), ("메모", memo), ("수정시각", now),
                ("수정자사번", editor_sabun), ("수정자이름", editor_name), ("활성", True),
            ]:
                c = hmap.get(k)
                if c:
                    a1 = gspread.utils.rowcol_to_a1(row_idx, c)
                    updates.append({"range": a1, "values": [[v]]})
            if updates:
                _gs_retry(lambda: ws.batch_update(updates))
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
    st.dataframe(view, use_container_width=True, height=640)
    sheet_id = st.secrets["sheets"]["HR_SHEET_ID"]
    st.caption(f"📄 원본: https://docs.google.com/spreadsheets/d/{sheet_id}/edit")

# ── 평가(1~5, 100점 환산) ─────────────────────────────────────────────────────
EVAL_ITEMS_SHEET = "평가_항목"
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고"]

EVAL_RESP_SHEET_PREFIX = "평가_응답_"
EVAL_BASE_HEADERS = [
    "연도", "평가유형",
    "평가대상사번", "평가대상이름",
    "평가자사번", "평가자이름",
    "총점", "상태", "제출시각",
    "서명_대상", "서명시각_대상",
    "서명_평가자", "서명시각_평가자",
    "잠금"
]
EVAL_TYPES = ["자기", "1차", "2차"]

def ensure_eval_items_sheet():
    wb = get_workbook()
    try:
        ws = wb.worksheet(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        ws.update("A1", [EVAL_ITEM_HEADERS])
        return
    header = ws.row_values(1) or []
    need = [h for h in EVAL_ITEM_HEADERS if h not in header]
    if need:
        ws.update("1:1", [header + need])

@st.cache_data(ttl=60, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    try:
        ensure_eval_items_sheet()
        ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
        df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    except Exception:
        return pd.DataFrame(columns=EVAL_ITEM_HEADERS)

    if df.empty:
        return pd.DataFrame(columns=EVAL_ITEM_HEADERS)

    if "순서" in df.columns:
        def _i(x):
            try:
                return int(float(str(x).strip()))
            except:
                return 0
        df["순서"] = df["순서"].apply(_i)

    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)

    cols = [c for c in ["순서", "항목"] if c in df.columns]
    if cols:
        df = df.sort_values(cols).reset_index(drop=True)
    if only_active and "활성" in df.columns:
        df = df[df["활성"] == True]
    return df

def _eval_sheet_name(year: int | str) -> str:
    return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def _ensure_eval_response_sheet(year: int, item_ids: list[str]):
    wb = get_workbook()
    s = _eval_sheet_name(year)
    try:
        ws = wb.worksheet(s)
    except WorksheetNotFound:
        ws = wb.add_worksheet(title=s, rows=800, cols=100)
        ws.update("A1", [EVAL_BASE_HEADERS + [f"점수_{i}" for i in item_ids]])
        return ws
    header = ws.row_values(1) or []
    need = list(EVAL_BASE_HEADERS) + [f"점수_{i}" for i in item_ids]
    add = [h for h in need if h not in header]
    if add:
        ws.update("1:1", [header + add])
    return ws

def _emp_name_by_sabun(emp_df: pd.DataFrame, sabun: str) -> str:
    s = str(sabun)
    try:
        m = st.session_state.get("name_by_sabun")
        if isinstance(m, dict) and s in m:
            return m[s]
    except Exception:
        pass
    row = emp_df.loc[emp_df["사번"].astype(str) == s]
    if not row.empty:
        return str(row.iloc[0].get("이름", ""))
    if "emp_df_cache" in st.session_state:
        row2 = st.session_state["emp_df_cache"].loc[st.session_state["emp_df_cache"]["사번"].astype(str) == s]
        if not row2.empty:
            return str(row2.iloc[0].get("이름", ""))
    return ""

def upsert_eval_response(
    emp_df: pd.DataFrame,
    year: int,
    eval_type: str,
    target_sabun: str,
    evaluator_sabun: str,
    scores: dict[str, int],
    status: str = "제출"
) -> dict:
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}

    def clamp5(v):
        try:
            v = int(v)
        except:
            v = 3
        return min(5, max(1, v))

    scores_list = [clamp5(scores.get(iid, 3)) for iid in item_ids]
    total_100 = round(sum(scores_list) * (100.0 / max(1, len(item_ids) * 5)), 1)

    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = kst_now_str()

    values = ws.get_all_values()
    cY = hmap.get("연도"); cT = hmap.get("평가유형")
    cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            if (
                str(r[cY - 1]).strip() == str(year)
                and str(r[cT - 1]).strip() == str(eval_type)
                and str(r[cTS - 1]).strip() == str(target_sabun)
                and str(r[cES - 1]).strip() == str(evaluator_sabun)
            ):
                row_idx = i
                break
        except:
            pass

    if row_idx == 0:
        buf = [""] * len(header)
        def put(k, v):
            c = hmap.get(k)
            if c:
                buf[c - 1] = v
        put("연도", int(year)); put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("총점", total_100); put("상태", status); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c = hmap.get(f"점수_{iid}")
            if c:
                buf[c - 1] = sc
        ws.append_row(buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "row": None, "total": total_100}

    ws.update_cell(row_idx, hmap["총점"], total_100)
    ws.update_cell(row_idx, hmap["상태"], status)
    ws.update_cell(row_idx, hmap["제출시각"], now)
    ws.update_cell(row_idx, hmap["평가대상이름"], t_name)
    ws.update_cell(row_idx, hmap["평가자이름"], e_name)
    for iid, sc in zip(item_ids, scores_list):
        c = hmap.get(f"점수_{iid}")
        if c:
            ws.update_cell(row_idx, c, sc)
    st.cache_data.clear()
    return {"action": "update", "row": row_idx, "total": total_100}

@st.cache_data(ttl=60, show_spinner=False)
def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    name = _eval_sheet_name(year)
    try:
        ws = get_workbook().worksheet(name)
        df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    except Exception:
        return pd.DataFrame(columns=EVAL_BASE_HEADERS)

    if df.empty:
        return df

    if "평가자사번" in df.columns:
        df = df[df["평가자사번"].astype(str) == str(sabun)]

    sort_cols = [c for c in ["평가유형", "평가대상사번", "제출시각"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True, True, False]).reset_index(drop=True)
    return df

def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> tuple[dict, dict]:
    try:
        items = read_eval_items_df(True)
        item_ids = [str(x) for x in items["항목ID"].tolist()]
        ws = _ensure_eval_response_sheet(year, item_ids)
        header = ws.row_values(1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = ws.get_all_values()

        cY = hmap.get("연도"); cT = hmap.get("평가유형")
        cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
        row_idx = 0
        for i in range(2, len(values) + 1):
            r = values[i - 1]
            try:
                if (
                    str(r[cY - 1]).strip() == str(year)
                    and str(r[cT - 1]).strip() == str(eval_type)
                    and str(r[cTS - 1]).strip() == str(target_sabun)
                    and str(r[cES - 1]).strip() == str(evaluator_sabun)
                ):
                    row_idx = i
                    break
            except:
                pass
        if row_idx == 0:
            return {}, {}

        row = values[row_idx - 1]
        scores = {}
        for iid in item_ids:
            col = hmap.get(f"점수_{iid}")
            if col:
                try:
                    v = int(str(row[col - 1]).strip() or "0")
                except:
                    v = 0
                if v:
                    scores[iid] = v

        meta = {}
        for k in ["상태", "잠금", "제출시각", "총점"]:
            c = hmap.get(k)
            if c:
                meta[k] = row[c - 1]
        return scores, meta
    except Exception:
        return {}, {}

def tab_eval_input(emp_df: pd.DataFrame):
    st.subheader("평가")

    # ── 스타일(세로 간격 최소화)
    st.markdown(
        """
        <style>
          .eval-row{padding:1px 0 !important;border-bottom:1px solid rgba(49,51,63,.06);}
          .eval-row .name{margin:0 !important;line-height:1.2 !important;}
          .eval-row .desc{margin:.05rem 0 .2rem !important;line-height:1.2 !important;color:#4b5563;}
          .eval-row .stRadio{margin:0 !important;}
          .eval-row [role="radiogroup"]{margin:0 !important;align-items:center;}
          .eval-row [role="radiogroup"] label{margin:0 !important;}
          .bulk-row{margin:.15rem 0 !important;}
          .stSlider{margin-top:.1rem !important;margin-bottom:.1rem !important;}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── 연도
    this_year = datetime.now(tz=tz_kst()).year
    colY = st.columns([1, 3])
    with colY[0]:
        year = st.number_input(
            "평가 연도", min_value=2000, max_value=2100,
            value=int(this_year), step=1, key="eval_year"
        )

    # ── 항목 로드
    items = read_eval_items_df(only_active=True)
    if items.empty:
        st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️")
        return

    # ── 권한/대상 선택
    u = st.session_state["user"]
    me_sabun = str(u["사번"]); me_name = str(u["이름"])
    am_admin = is_admin(me_sabun)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    c_tgt, c_type, _ = st.columns([2, 1.6, 6.4])

    if am_admin or is_manager(emp_df, me_sabun):
        df = emp_df.copy()
        df = df[df["사번"].astype(str).isin(allowed)]
        if "재직여부" in df.columns:
            df = df[df["재직여부"] == True]
        df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df = df.sort_values(["사번"])
        with c_tgt:
            sel = st.selectbox(
                "평가 대상자", ["(선택)"] + df["표시"].tolist(),
                index=0, key="eval_target_select"
            )
        if sel == "(선택)":
            st.info("평가 대상자를 선택하세요.")
            return
        target_sabun = sel.split(" - ", 1)[0]
        target_name = _emp_name_by_sabun(emp_df, target_sabun)
        with c_type:
            type_key = f"eval_type_{year}_{me_sabun}_{target_sabun}"
            if type_key not in st.session_state:
                st.session_state[type_key] = "1차"
            eval_type = st.radio("평가유형", EVAL_TYPES, horizontal=True, key=type_key)
    else:
        target_sabun = me_sabun
        target_name = me_name
        with c_tgt:
            st.text_input("평가 대상자", f"{target_name} ({target_sabun})", disabled=True, key="eval_target_me")
        with c_type:
            eval_type = "자기"
            st.text_input("평가유형", "자기", disabled=True, key="eval_type_me")

    evaluator_sabun = me_sabun
    evaluator_name = me_name

    # ── 저장된 점수/잠금 확인
    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, target_sabun, evaluator_sabun)
    is_self_case = (eval_type == "자기" and target_sabun == evaluator_sabun)
    already_submitted = bool(saved_meta) and str(saved_meta.get("상태", "")).strip() in ("제출", "완료")
    locked_flag = str(saved_meta.get("잠금", "")).strip().lower() in ("true", "1", "y", "yes")

    # 유니크 키 베이스
    kbase = f"evalbulk_{year}_{eval_type}_{evaluator_sabun}_{target_sabun}"
    edit_flag_key = f"__edit_on_{kbase}"
    apply_saved_once_key = f"__apply_saved_once_{kbase}"

    # 자기평가 잠금 상태면: 제출 현황만 노출 (수정 모드로 전환 버튼 제공)
    if is_self_case and (already_submitted or locked_flag) and not st.session_state.get(edit_flag_key, False):
        st.info("이미 제출된 자기평가입니다. 아래 ‘수정 모드로 전환’ 버튼을 눌러야 편집할 수 있습니다.", icon="ℹ️")
        if st.button("✏️ 수정 모드로 전환", key=f"{kbase}_edit_on", use_container_width=True):
            st.session_state[edit_flag_key] = True      # rerun 없이 그대로 진행
            st.session_state[apply_saved_once_key] = False  # 저장값 강제 반영 플래그 초기화
        st.markdown("#### 내 제출 현황")
        try:
            my = read_my_eval_rows(int(year), evaluator_sabun)
            if my.empty:
                st.caption("제출된 평가가 없습니다.")
            else:
                st.dataframe(
                    my[["평가유형", "평가대상사번", "평가대상이름", "총점", "상태", "제출시각"]],
                    use_container_width=True, height=260
                )
        except Exception:
            st.caption("제출 현황을 불러오지 못했습니다.")
        return

    # ── 제목 + (일괄 슬라이더 + 적용 버튼) : rerun 없이 세션키로 주입
    c_head, c_slider, c_btn = st.columns([5, 2, 1])
    with c_head:
        st.markdown("#### 점수 입력 (각 1~5)")

    slider_key = f"{kbase}_slider"
    if slider_key not in st.session_state:
        if saved_scores:
            avg = round(sum(saved_scores.values()) / max(1, len(saved_scores)))
            st.session_state[slider_key] = int(min(5, max(1, avg)))
        else:
            st.session_state[slider_key] = 3

    with c_slider:
        bulk_score = st.slider("일괄 점수", min_value=1, max_value=5, step=1, key=slider_key)
    with c_btn:
        if st.button("일괄 적용", use_container_width=True, key=f"{kbase}_apply"):
            st.session_state[f"__apply_bulk_{kbase}"] = int(bulk_score)
            st.toast(f"모든 항목에 {bulk_score}점 적용", icon="✅")

    # ── 일괄 적용 플래그 처리(라디오 생성 전에 값 세팅)
    apply_key = f"__apply_bulk_{kbase}"
    if st.session_state.get(apply_key) is not None:
        _v = int(st.session_state[apply_key])
        for _iid in items["항목ID"].astype(str):
            st.session_state[f"eval_seg_{_iid}_{kbase}"] = str(_v)
        del st.session_state[apply_key]

    # ── 항목 렌더링 (이름 | 설명 | 점수)
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    scores = {}

    # 수정 모드로 막 전환했거나(locked case) / 처음 로드 시 저장값을 강제 1회 주입
    if st.session_state.get(edit_flag_key, False) and not st.session_state.get(apply_saved_once_key, False):
        for _iid, _v in saved_scores.items():
            st.session_state[f"eval_seg_{_iid}_{kbase}"] = str(int(_v))
        st.session_state[apply_saved_once_key] = True

    for r in items_sorted.itertuples(index=False):
        iid = str(getattr(r, "항목ID"))
        name = getattr(r, "항목") or ""
        desc = getattr(r, "내용") or ""

        rkey = f"eval_seg_{iid}_{kbase}"
        if rkey not in st.session_state:
            if iid in saved_scores:
                st.session_state[rkey] = str(int(saved_scores[iid]))
            else:
                st.session_state[rkey] = "3"

        st.markdown('<div class="eval-row">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([2, 6, 3])
        with c1:
            st.markdown(f'<div class="name">{name}</div>', unsafe_allow_html=True)
        with c2:
            if desc.strip():
                st.markdown(f'<div class="desc">{desc.replace(chr(10), "<br/>")}</div>', unsafe_allow_html=True)
        with c3:
            st.radio(" ", ["1", "2", "3", "4", "5"], horizontal=True, key=rkey, label_visibility="collapsed")
        st.markdown('</div>', unsafe_allow_html=True)

        scores[iid] = int(st.session_state[rkey])

    # ── 합계/저장
    total_100 = round(sum(scores.values()) * (100.0 / max(1, len(items_sorted) * 5)), 1)
    st.markdown("---")
    cM1, cM2 = st.columns([1, 3])
    with cM1:
        st.metric("합계(100점 만점)", total_100)
    with cM2:
        st.progress(min(1.0, total_100 / 100.0), text=f"총점 {total_100}점")

    col_submit = st.columns([1, 4])
    with col_submit[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True, key=f"eval_save_{kbase}")

    if do_save:
        try:
            rep = upsert_eval_response(
                emp_df, int(year), eval_type,
                str(target_sabun), str(evaluator_sabun),
                scores, "제출"
            )
            st.success(("제출 완료" if rep["action"] == "insert" else "업데이트 완료") + f" (총점 {rep['total']}점)", icon="✅")
            st.toast("평가 저장됨", icon="✅")
        except Exception:
            st.error("저장 중 문제가 발생했습니다. 네트워크/권한을 확인하세요.", icon="🛑")

    st.markdown("#### 내 제출 현황")
    try:
        my = read_my_eval_rows(int(year), evaluator_sabun)
        if my.empty:
            st.caption("제출된 평가가 없습니다.")
        else:
            st.dataframe(
                my[["평가유형", "평가대상사번", "평가대상이름", "총점", "상태", "제출시각"]],
                use_container_width=True, height=260
            )
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

# ── 직무기술서 ────────────────────────────────────────────────────────────────
JOBDESC_SHEET="직무기술서"
JOBDESC_HEADERS = [
    "사번","연도","버전",
    "부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","서명방식","서명데이터","제출시각"
]

def ensure_jobdesc_sheet():
    wb=get_workbook()
    try:
        ws=wb.worksheet(JOBDESC_SHEET)
        header=ws.row_values(1) or []
        need=[h for h in JOBDESC_HEADERS if h not in header]
        if need: ws.update("1:1",[header+need])
        return ws
    except WorksheetNotFound:
        ws=wb.add_worksheet(title=JOBDESC_SHEET, rows=1200, cols=60)
        ws.update("A1",[JOBDESC_HEADERS]); return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_jobdesc_df()->pd.DataFrame:
    ensure_jobdesc_sheet()
    ws=get_workbook().worksheet(JOBDESC_SHEET)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
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
    ws=get_workbook().worksheet(JOBDESC_SHEET)
    header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
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

    values=ws.get_all_values(); row_idx=0
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
        ws.append_row(build_row(), value_input_option="USER_ENTERED"); st.cache_data.clear()
        return {"action":"insert","version":ver}
    else:
        for k,v in rec.items():
            c=hmap.get(k)
            if c: ws.update_cell(row_idx, c, v)
        st.cache_data.clear()
        return {"action":"update","version":ver}

def tab_job_desc(emp_df: pd.DataFrame):
    st.subheader("직무기술서")

    u = st.session_state["user"]
    me_sabun = str(u["사번"])
    me_name  = str(u["이름"])
    allowed  = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    st.markdown("#### 대상/연도 선택")
    if is_admin(me_sabun) or is_manager(emp_df, me_sabun):
        df = emp_df.copy()
        df = df[df["사번"].astype(str).isin(allowed)]
        if "재직여부" in df.columns:
            df = df[df["재직여부"] == True]
        df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df = df.sort_values(["사번"])
        sel = st.selectbox("대상자 (사번 - 이름)", ["(선택)"] + df["표시"].tolist(), index=0, key="job_target")
        if sel == "(선택)":
            st.info("대상자를 선택하세요.")
            return
        target_sabun = sel.split(" - ", 1)[0]
        target_name  = _emp_name_by_sabun(emp_df, target_sabun)
    else:
        target_sabun = me_sabun
        target_name  = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")

    today         = datetime.now(tz=tz_kst()).strftime("%Y-%m-%d")
    defval_create = get_setting("JD.제정일",   today)
    defval_update = get_setting("JD.개정일",   today)
    defval_review = get_setting("JD.검토주기", "1년")

    row_emp = emp_df.loc[emp_df["사번"].astype(str) == str(target_sabun)]
    pref_dept1  = str(row_emp.iloc[0].get("부서1", "")) if not row_emp.empty else ""
    pref_dept2  = str(row_emp.iloc[0].get("부서2", "")) if not row_emp.empty else ""
    pref_group  = str(row_emp.iloc[0].get("직군",  "")) if (not row_emp.empty and "직군" in row_emp.columns)  else ""
    pref_series = str(row_emp.iloc[0].get("직종",  "")) if (not row_emp.empty and "직종" in row_emp.columns)  else ""
    pref_job    = str(row_emp.iloc[0].get("직무",  "")) if (not row_emp.empty and "직무" in row_emp.columns)  else ""

    jobname_default = pref_job or ""

    col = st.columns([1, 1, 2, 2])
    with col[0]:
        year = st.number_input("연도", min_value=2000, max_value=2100, value=int(datetime.now(tz=tz_kst()).year), step=1, key="job_year")
    with col[1]:
        version = st.number_input("버전(없으면 자동)", min_value=0, max_value=999, value=0, step=1, key="job_ver")
    with col[2]:
        jobname = st.text_input("직무명", value=jobname_default, key="job_jobname")
    with col[3]:
        memo = st.text_input("비고", value="", key="job_memo")

    c2 = st.columns([1, 1, 1, 1])
    with c2[0]:
        dept1 = st.text_input("부서1", value=pref_dept1, key="job_dept1")
    with c2[1]:
        dept2 = st.text_input("부서2", value=pref_dept2, key="job_dept2")
    with c2[2]:
        group = st.text_input("직군",  value=pref_group,  key="job_group")
    with c2[3]:
        series = st.text_input("직종",  value=pref_series, key="job_series")

    c3 = st.columns([1, 1, 1])
    with c3[0]:
        d_create = st.text_input("제정일", value=defval_create, key="job_d_create")
    with c3[1]:
        d_update = st.text_input("개정일", value=defval_update, key="job_d_update")
    with c3[2]:
        review   = st.text_input("검토주기", value=defval_review, key="job_review")

    job_summary = st.text_area("직무개요", "", height=80,  key="job_summary")
    job_main    = st.text_area("주업무",   "", height=120, key="job_main")
    job_other   = st.text_area("기타업무", "", height=80,  key="job_other")

    c4 = st.columns([1, 1, 1, 1, 1, 1])
    with c4[0]:
        edu_req    = st.text_input("필요학력", "", key="job_edu")
    with c4[1]:
        major_req  = st.text_input("전공계열", "", key="job_major")
    with c4[2]:
        edu_common = st.text_input("직원공통필수교육", "", key="job_edu_common")
    with c4[3]:
        edu_cont   = st.text_input("보수교육", "", key="job_edu_cont")
    with c4[4]:
        edu_etc    = st.text_input("기타교육", "", key="job_edu_etc")
    with c4[5]:
        edu_spec   = st.text_input("특성화교육", "", key="job_edu_spec")

    c5 = st.columns([1, 1, 2])
    with c5[0]:
        license_ = st.text_input("면허", "", key="job_license")
    with c5[1]:
        career   = st.text_input("경력(자격요건)", "", key="job_career")
    with c5[2]:
        pass

    c6 = st.columns([1, 2, 1])
    with c6[0]:
        sign_type = st.selectbox("서명방식", ["", "text", "image"], index=0, key="job_sign_type")
    with c6[1]:
        sign_data = st.text_input("서명데이터", "", key="job_sign_data")
    with c6[2]:
        do_save   = st.button("저장/업서트", type="primary", use_container_width=True, key="job_save_btn")

    if do_save:
        rec = {
            "사번": str(target_sabun),
            "연도": int(year),
            "버전": int(version or 0),
            "부서1": dept1,
            "부서2": dept2,
            "작성자사번": me_sabun,
            "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
            "직군": group,
            "직종": series,
            "직무명": jobname,
            "제정일": d_create,
            "개정일": d_update,
            "검토주기": review,
            "직무개요": job_summary,
            "주업무": job_main,
            "기타업무": job_other,
            "필요학력": edu_req,
            "전공계열": major_req,
            "직원공통필수교육": edu_common,
            "보수교육": edu_cont,
            "기타교육": edu_etc,
            "특성화교육": edu_spec,
            "면허": license_,
            "경력(자격요건)": career,
            "비고": memo,
            "서명방식": sign_type,
            "서명데이터": sign_data,
        }
        try:
            rep = upsert_jobdesc(rec, as_new_version=(version == 0))
            st.success(f"저장 완료 (버전 {rep['version']})", icon="✅")
        except Exception as e:
            st.exception(e)

# ── 직무능력평가(가중치) ─────────────────────────────────────────────────────
COMP_ITEM_SHEET="직무능력_항목"
COMP_ITEM_HEADERS=["항목ID","영역","항목","내용","가중치","순서","활성","비고"]
COMP_RESP_PREFIX="직무능력_응답_"
COMP_BASE_HEADERS=["연도","평가대상사번","평가대상이름","평가자사번","평가자이름","총점","상태","제출시각"]

def ensure_comp_items_sheet():
    wb=get_workbook()
    try:
        ws=wb.worksheet(COMP_ITEM_SHEET)
    except WorksheetNotFound:
        ws=wb.add_worksheet(title=COMP_ITEM_SHEET, rows=200, cols=12); ws.update("A1",[COMP_ITEM_HEADERS]); return ws
    header=ws.row_values(1) or []; need=[h for h in COMP_ITEM_HEADERS if h not in header]
    if need: ws.update("1:1",[header+need]); return ws
    return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_comp_items_df(only_active=True)->pd.DataFrame:
    ensure_comp_items_sheet(); ws=get_workbook().worksheet(COMP_ITEM_SHEET)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: return pd.DataFrame(columns=COMP_ITEM_HEADERS)
    for c in ["가중치","순서"]:
        if c in df.columns:
            def _n(x):
                try: return float(str(x).strip())
                except: return 0.0
            df[c]=df[c].apply(_n)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    df=df.sort_values(["영역","순서","항목"]).reset_index(drop=True)
    if only_active and "활성" in df.columns: df=df[df["활성"]==True]
    return df

def _comp_sheet_name(year:int|str)->str: return f"{COMP_RESP_PREFIX}{int(year)}"

def _ensure_comp_resp_sheet(year:int, item_ids:list[str])->gspread.Worksheet:
    wb=get_workbook(); name=_comp_sheet_name(year)
    try: ws=wb.worksheet(name)
    except WorksheetNotFound:
        ws=wb.add_worksheet(title=name, rows=1000, cols=100); ws.update("A1",[COMP_BASE_HEADERS+[f"점수_{iid}" for iid in item_ids]]); return ws
    header=ws.row_values(1) or []; need=list(COMP_BASE_HEADERS)+[f"점수_{iid}" for iid in item_ids]; add=[h for h in need if h not in header]
    if add: ws.update("1:1",[header+add])
    return ws

def upsert_comp_response(emp_df:pd.DataFrame, year:int, target_sabun:str, evaluator_sabun:str, scores:dict[str,int], status:str="제출")->dict:
    items=read_comp_items_df(True); item_ids=[str(x) for x in items["항목ID"].tolist()]
    ws=_ensure_comp_resp_sheet(year,item_ids); header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
    weights=[max(0.0,float(items[items["항목ID"]==iid].iloc[0]["가중치"])) if not items[items["항목ID"]==iid].empty else 0.0 for iid in item_ids]
    wsum=sum(weights) if sum(weights)>0 else len(item_ids)
    total=0.0
    for iid, w in zip(item_ids, weights):
        s=int(scores.get(iid,0))
        s=min(5,max(1,s)) if s else 0
        total+=(s/5.0)*(w if wsum>0 else 1.0)
    total_100 = round((total/wsum)*100.0, 1) if wsum>0 else (round((total/max(1,len(item_ids)))*100.0,1))
    t_name=_emp_name_by_sabun(emp_df, target_sabun); e_name=_emp_name_by_sabun(emp_df, evaluator_sabun); now=kst_now_str()

    values=ws.get_all_values(); cY=hmap.get("연도"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
    row_idx=0
    for i in range(2,len(values)+1):
        r=values[i-1]
        try:
            if str(r[cY-1]).strip()==str(year) and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun):
                row_idx=i; break
        except: pass

    if row_idx==0:
        buf=[""]*len(header)
        def put(k,v):
            c=hmap.get(k)
            if c: buf[c-1]=v
        put("연도",int(year)); put("평가대상사번",str(target_sabun)); put("평가대상이름",t_name)
        put("평가자사번",str(evaluator_sabun)); put("평가자이름",e_name)
        put("총점",total_100); put("상태",status); put("제출시각",now)
        for iid in item_ids:
            c=hmap.get(f"점수_{iid}")
            if c: buf[c-1]=int(scores.get(iid,0) or 0)
        ws.append_row(buf, value_input_option="USER_ENTERED"); st.cache_data.clear()
        return {"action":"insert","total":total_100}
    else:
        ws.update_cell(row_idx, hmap["총점"], total_100)
        ws.update_cell(row_idx, hmap["상태"], status)
        ws.update_cell(row_idx, hmap["제출시각"], now)
        ws.update_cell(row_idx, hmap["평가대상이름"], t_name)
        ws.update_cell(row_idx, hmap["평가자이름"], e_name)
        for iid in item_ids:
            c=hmap.get(f"점수_{iid}")
            if c: ws.update_cell(row_idx, c, int(scores.get(iid,0) or 0))
        st.cache_data.clear()
        return {"action":"update","total":total_100}

@st.cache_data(ttl=60, show_spinner=False)
def read_my_comp_rows(year:int, sabun:str)->pd.DataFrame:
    name=_comp_sheet_name(year)
    try: ws=get_workbook().worksheet(name)
    except Exception: return pd.DataFrame(columns=COMP_BASE_HEADERS)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: return df
    df=df[df["평가자사번"].astype(str)==str(sabun)]
    return df.sort_values(["평가대상사번","제출시각"], ascending=[True,False])

def tab_competency(emp_df: pd.DataFrame):
    st.subheader("직무능력평가")
    this_year = datetime.now(tz=tz_kst()).year
    colY = st.columns([1,3])
    with colY[0]:
        year = st.number_input("평가 연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmp_year")

    items = read_comp_items_df(only_active=True)
    if items.empty:
        st.warning("활성화된 직무능력 항목이 없습니다.", icon="⚠️"); return

    u = st.session_state["user"]
    me_sabun = str(u["사번"]); me_name = str(u["이름"])
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    st.markdown("#### 대상 선택")
    if is_admin(me_sabun) or is_manager(emp_df, me_sabun):
        df = emp_df.copy(); df=df[df["사번"].astype(str).isin(allowed)]
        if "재직여부" in df.columns: df=df[df["재직여부"]==True]
        df["표시"]=df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
        df=df.sort_values(["사번"])
        sel=st.selectbox("평가 **대상자** (사번 - 이름)", ["(선택)"]+df["표시"].tolist(), index=0, key="cmp_target")
        if sel=="(선택)": st.info("평가 대상자를 선택하세요."); return
        target_sabun=sel.split(" - ",1)[0]; target_name=_emp_name_by_sabun(emp_df, target_sabun)
        evaluator_sabun=me_sabun; evaluator_name=me_name
    else:
        target_sabun=me_sabun; target_name=me_name
        evaluator_sabun=me_sabun; evaluator_name=me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")

    st.markdown("#### 점수 입력")
    st.caption("각 항목 1~5점, 가중치 자동 정규화.")
    st.markdown(
        """
        <style>
          .cmp-grid{display:grid;grid-template-columns:2fr 6fr 2fr 2fr;gap:.5rem;
                    align-items:center;padding:10px 6px;border-bottom:1px solid rgba(49,51,63,.10)}
          .cmp-grid .name{font-weight:700}
          .cmp-grid .desc{color:#4b5563}
          .cmp-grid .input{display:flex;align-items:center;justify-content:center}
          .cmp-grid .input div[role="radiogroup"]{display:flex;gap:10px;align-items:center;justify-content:center}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="grid-head">영역/항목 / 내용 / 가중치 / 점수</div>', unsafe_allow_html=True)

    items_sorted=items.sort_values(["영역","순서","항목"]).reset_index(drop=True)
    scores={}; weight_sum=0.0
    for r in items_sorted.itertuples(index=False):
        iid=getattr(r,"항목ID"); area=getattr(r,"영역") or ""; name=getattr(r,"항목") or ""
        desc=getattr(r,"내용") or ""; w=float(getattr(r,"가중치") or 0.0)
        label=f"[{area}] {name}" if area else name
        cur=int(st.session_state.get(f"cmp_{iid}",3))
        if cur<1 or cur>5: cur=3

        st.markdown('<div class="cmp-grid">', unsafe_allow_html=True)
        st.markdown(f'<div class="name">{label}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="desc">{desc.replace(chr(10), "<br/>")}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="desc" style="text-align:center">{w:g}</div>', unsafe_allow_html=True)
        st.markdown('<div class="input">', unsafe_allow_html=True)
        if getattr(st, "segmented_control", None):
            new_val = st.segmented_control(" ", options=[1,2,3,4,5], default_value=cur, key=f"cmp_seg_{iid}")
        else:
            new_val = int(st.radio(" ", ["1","2","3","4","5"], index=(cur-1), horizontal=True, key=f"cmp_seg_{iid}", label_visibility="collapsed"))
        st.markdown('</div></div>', unsafe_allow_html=True)

        v=min(5,max(1,int(new_val))); scores[str(iid)]=v; st.session_state[f"cmp_{iid}"]=v; weight_sum+=max(0.0,w)

    total=0.0
    if len(items_sorted)>0:
        for r in items_sorted.itertuples(index=False):
            iid=getattr(r,"항목ID"); w=float(getattr(r,"가중치") or 0.0); s=scores.get(str(iid),0)
            total+=(s/5.0)*(w if weight_sum>0 else 1.0)
        total_100=round((total/(weight_sum if weight_sum>0 else len(items_sorted)))*100.0, 1)
    else:
        total_100=0.0

    st.markdown("---")
    cM1,cM2=st.columns([1,3])
    with cM1: st.metric("합계(100점 만점)", total_100)
    with cM2: st.progress(min(1.0,total_100/100.0), text=f"총점 {total_100}점")

    cbtn=st.columns([1,1,3])
    with cbtn[0]: do_save=st.button("제출/저장", type="primary", use_container_width=True, key="cmp_save")
    with cbtn[1]: do_reset=st.button("모든 점수 3점으로", use_container_width=True, key="cmp_reset")

    if do_reset:
        for r in items_sorted.itertuples(index=False): st.session_state[f"cmp_{getattr(r,'항목ID')}"]=3
        st.rerun()

    if do_save:
        try:
            rep=upsert_comp_response(emp_df,int(year),str(target_sabun),str(evaluator_sabun),scores,"제출")
            st.success(("제출 완료" if rep["action"]=="insert" else "업데이트 완료")+f" (총점 {rep['total']}점)", icon="✅")
            st.toast("직무능력평가 저장됨", icon="✅")
        except Exception as e:
            st.exception(e)

    st.markdown("#### 내 제출 현황")
    try:
        my=read_my_comp_rows(int(year), evaluator_sabun)
        if my.empty: st.caption("제출된 평가가 없습니다.")
        else: st.dataframe(my[["평가대상사번","평가대상이름","총점","상태","제출시각"]], use_container_width=True, height=260)
    except Exception:
        st.caption("제출 현황을 불러오지 못했습니다.")

# ── 부서이력/이동(필수 최소) ───────────────────────────────────────────────
@guard_page
def section_dept_history_min():
    st.header("🏷️ 부서이력/이동 (필수 최소)")
    st.button("🔄 다시 불러오기", on_click=st.rerun)

    # gspread 클라이언트 준비 (세션 캐시)
    try:
        if "gc" not in st.session_state:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.readonly",
            ]
            sa = st.secrets.get("gcp_service_account", {})
            creds = Credentials.from_service_account_info(sa, scopes=scopes)
            st.session_state.gc = gspread.authorize(creds)
        gc = st.session_state.gc

        # 시트 키/워크시트명은 secrets 또는 텍스트 입력으로
        colk, colw = st.columns(2)
        sheet_key = colk.text_input("스프레드시트 KEY", value=st.secrets.get("gspread", {}).get("SHEET_KEY", ""), type="default")
        ws_name   = colw.text_input("워크시트명", value=st.secrets.get("gspread", {}).get("WS_DEPT_HISTORY", "부서이동"))

        if not sheet_key or not ws_name:
            st.info("스프레드시트 KEY와 워크시트명을 입력/설정하세요.")
            return

        # 데이터 로드
        def _fetch_rows():
            ws = gc.open_by_key(sheet_key).worksheet(ws_name)
            return ws.get_all_records()

        rows = call_api_with_refresh(_fetch_rows)
        df = pd.DataFrame(rows)
        if df.empty:
            st.warning("데이터가 없습니다.")
        else:
            st.dataframe(df, use_container_width=True)

    except (WorksheetNotFound, APIError) as e:
        show_recovery_card(e)
        return
    except Exception as e:
        show_recovery_card(e)
        return

    # (선택) 간단 등록 폼
    with st.expander("➕ 부서 이동 기록 추가"):
        with st.form("dept_move_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns([1,1,1,1])
            emp_id = c1.text_input("사번/ID")
            emp_nm = c2.text_input("성명")
            from_d = c3.text_input("이전 부서")
            to_d   = c4.text_input("이동 부서")
            moved_at = st.date_input("이동일", value=datetime.now(tz_kst()).date())
            submitted = st.form_submit_button("저장")

        if submitted:
            try:
                def _append():
                    ws = gc.open_by_key(sheet_key).worksheet(ws_name)
                    ws.append_row([
                        emp_id, emp_nm, from_d, to_d,
                        datetime.combine(moved_at, datetime.min.time()).strftime("%Y-%m-%d"),
                        datetime.now(tz_kst()).strftime("%Y-%m-%d %H:%M:%S"),
                    ])
                    return True
                call_api_with_refresh(_append)
                st.success("저장되었습니다.")
                st.rerun()
            except Exception as e:
                show_recovery_card(e)

# ── 관리자: PIN / 부서이동 / 평가항목 / 권한 ───────────────────────────────
@guard_page
def section_admin():
    st.header("🛠️ 관리자")
    st.button("🔄 다시 불러오기", on_click=st.rerun)

    tabs = st.tabs(["PIN", "부서이동 설정", "평가항목", "권한"])

    # 공통: gspread 준비
    try:
        if "gc" not in st.session_state:
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
            sa = st.secrets.get("gcp_service_account", {})
            creds = Credentials.from_service_account_info(sa, scopes=scopes)
            st.session_state.gc = gspread.authorize(creds)
        gc = st.session_state.gc
        sheet_key = st.secrets.get("gspread", {}).get("SHEET_KEY", "")
        if not sheet_key:
            st.info("secrets.gspread.SHEET_KEY가 필요합니다.")
            return
    except Exception as e:
        show_recovery_card(e)
        return

    # ── PIN
    with tabs[0]:
        st.subheader("관리자 PIN")
        ws_name = st.secrets.get("gspread", {}).get("WS_ADMIN_PIN", "ADMIN_PIN")
        try:
            def _read():
                ws = gc.open_by_key(sheet_key).worksheet(ws_name)
                return ws.get_all_records()
            rows = call_api_with_refresh(_read)
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except (WorksheetNotFound, APIError) as e:
            st.warning(f"워크시트 '{ws_name}' 없음. 생성 후 사용하세요.")
        except Exception as e:
            show_recovery_card(e)

        with st.form("pin_update", clear_on_submit=True):
            admin_id = st.text_input("관리자 ID")
            new_pin  = st.text_input("새 PIN (숫자 4~6)", type="password")
            submitted = st.form_submit_button("PIN 저장")
        if submitted:
            if not (admin_id and re.fullmatch(r"\d{4,6}", new_pin or "")):
                st.error("ID와 4~6자리 PIN을 입력하세요.")
            else:
                try:
                    def _append():
                        ws = gc.open_by_key(sheet_key).worksheet(ws_name)
                        ws.append_row([admin_id, hashlib.sha256(new_pin.encode()).hexdigest(), datetime.now(tz_kst()).strftime("%Y-%m-%d %H:%M:%S")])
                        return True
                    call_api_with_refresh(_append)
                    st.success("저장되었습니다.")
                    st.rerun()
                except Exception as e:
                    show_recovery_card(e)

    # ── 부서이동 설정
    with tabs[1]:
        st.subheader("부서 마스터/이동 규칙")
        ws_dept = st.secrets.get("gspread", {}).get("WS_DEPT_MASTER", "DEPT_MASTER")
        try:
            def _read():
                ws = gc.open_by_key(sheet_key).worksheet(ws_dept)
                return ws.get_all_records()
            rows = call_api_with_refresh(_read)
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except (WorksheetNotFound, APIError):
            st.warning(f"워크시트 '{ws_dept}' 없음. 생성 후 사용하세요.")
        except Exception as e:
            show_recovery_card(e)

        with st.form("dept_master_add", clear_on_submit=True):
            c1, c2 = st.columns(2)
            dept_code = c1.text_input("부서코드")
            dept_name = c2.text_input("부서명")
            s = st.form_submit_button("부서 추가")
        if s:
            try:
                def _append():
                    ws = gc.open_by_key(sheet_key).worksheet(ws_dept)
                    ws.append_row([dept_code, dept_name, datetime.now(tz_kst()).strftime("%Y-%m-%d %H:%M:%S")])
                    return True
                call_api_with_refresh(_append)
                st.success("추가되었습니다.")
                st.rerun()
            except Exception as e:
                show_recovery_card(e)

    # ── 평가항목
    with tabs[2]:
        st.subheader("평가 항목 관리")
        ws_eval = st.secrets.get("gspread", {}).get("WS_EVAL_ITEMS", "EVAL_ITEMS")
        try:
            def _read():
                ws = gc.open_by_key(sheet_key).worksheet(ws_eval)
                return ws.get_all_records()
            rows = call_api_with_refresh(_read)
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except (WorksheetNotFound, APIError):
            st.warning(f"워크시트 '{ws_eval}' 없음. 생성 후 사용하세요.")
        except Exception as e:
            show_recovery_card(e)

        with st.form("eval_add", clear_on_submit=True):
            c1, c2 = st.columns([2,1])
            item = c1.text_input("평가항목")
            weight = c2.number_input("가중치", min_value=0.0, max_value=100.0, value=10.0, step=1.0)
            s = st.form_submit_button("항목 추가")
        if s:
            try:
                def _append():
                    ws = gc.open_by_key(sheet_key).worksheet(ws_eval)
                    ws.append_row([item, weight, datetime.now(tz_kst()).strftime("%Y-%m-%d %H:%M:%S")])
                    return True
                call_api_with_refresh(_append)
                st.success("추가되었습니다.")
                st.rerun()
            except Exception as e:
                show_recovery_card(e)

    # ── 권한
    with tabs[3]:
        st.subheader("권한 관리 (역할별)")
        ws_role = st.secrets.get("gspread", {}).get("WS_ROLES", "ROLES")
        try:
            def _read():
                ws = gc.open_by_key(sheet_key).worksheet(ws_role)
                return ws.get_all_records()
            rows = call_api_with_refresh(_read)
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except (WorksheetNotFound, APIError):
            st.warning(f"워크시트 '{ws_role}' 없음. 생성 후 사용하세요.")
        except Exception as e:
            show_recovery_card(e)

        with st.form("role_add", clear_on_submit=True):
            c1, c2 = st.columns([1,3])
            role = c1.text_input("역할 코드")
            perms = c2.text_input("권한(콤마구분, 예: read,write,admin)")
            s = st.form_submit_button("역할 추가")
        if s:
            try:
                def _append():
                    ws = gc.open_by_key(sheet_key).worksheet(ws_role)
                    ws.append_row([role, perms, datetime.now(tz_kst()).strftime("%Y-%m-%d %H:%M:%S")])
                    return True
                call_api_with_refresh(_append)
                st.success("추가되었습니다.")
                st.rerun()
            except Exception as e:
                show_recovery_card(e)

# ── 메인 ────────────────────────────────────────────────────────────────────
def section_main():
    st.header("👤 메인")
    st.button("🔄 다시 불러오기", on_click=st.rerun)

    # 예시: 간단 라우팅
    page = st.sidebar.selectbox("페이지", ["메인", "부서이력/이동", "관리자"])
    if page == "부서이력/이동":
        section_dept_history_min()
        return
    if page == "관리자":
        section_admin()
        return

    # 메인 카드/요약 영역
    try:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("오늘 날짜", datetime.now(tz_kst()).strftime("%Y-%m-%d"))
        with c2:
            st.metric("랜덤 토큰", pysecrets.token_hex(4))
        with c3:
            st.metric("앱 상태", "Ready" if st.session_state.get("app_ready") else "Init")

        st.write("필요한 위젯/요약을 여기에 구성하세요.")
    except Exception as e:
        show_recovery_card(e)

# ── 엔트리포인트 ─────────────────────────────────────────────────────────────
def main():
    init_state()
    render_global_actions()
    section_main()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        show_recovery_card(e)
