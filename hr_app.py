# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (Google Sheets 연동)
"""

# ── Imports ───────────────────────────────────────────────────────────────────
import time, re, hashlib, secrets as pysecrets
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

# ── App Config ────────────────────────────────────────────────────────────────
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")
st.markdown(
    """
    <style>
      /* 상단 여백을 조금 늘려 잘림 방지 */
      .block-container { padding-top: 1.35rem !important; }

      /* 탭/보조 텍스트 */
      .stTabs [role='tab']{ padding:10px 16px !important; font-size:1.02rem !important; }
      .grid-head{ font-size:.9rem; color:#6b7280; margin:.2rem 0 .5rem; }

      /* 앱 제목: 전용 클래스 사용 (글자 살짝 작게) */
      .app-title{
        font-size: 1.28rem;
        line-height: 1.45rem;
        margin: .2rem 0 .6rem;
        font-weight: 800;
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
@st.cache_data(ttl=60, show_spinner=True)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    ws = get_workbook().worksheet(sheet_name)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if "관리자여부" in df.columns: df["관리자여부"] = df["관리자여부"].map(_to_bool)
    if "재직여부" in df.columns: df["재직여부"] = df["재직여부"].map(_to_bool)
    for c in ["입사일","퇴사일"]: 
        if c in df.columns: df[c] = df[c].astype(str)
    for c in ["사번","이름","PIN_hash"]:
        if c not in df.columns: df[c] = ""
    if "사번" in df.columns: df["사번"] = df["사번"].astype(str)
    return df

def _get_ws_and_headers(sheet_name: str):
    ws = get_workbook().worksheet(sheet_name); header = ws.row_values(1) or []
    if not header: raise RuntimeError(f"'{sheet_name}' 헤더(1행) 없음")
    return ws, header, {n:i+1 for i,n in enumerate(header)}

def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    c = hmap.get("사번")
    if not c: return 0
    for i, v in enumerate(ws.col_values(c)[1:], start=2):
        if str(v).strip() == str(sabun).strip(): return i
    return 0

def _update_cell(ws, row, col, value): ws.update_cell(row, col, value)

def _hide_doctors(df: pd.DataFrame) -> pd.DataFrame:
    """직무가 '의사'인 행을 숨깁니다. (대소문자/공백 안전)"""
    if "직무" not in df.columns:
        return df
    col = df["직무"].astype(str).str.strip().str.lower()
    return df[~col.eq("의사")]

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

    # 폼으로 묶으면 Enter 로 제출 가능
    with st.form("login_form", clear_on_submit=False):
        sabun = st.text_input("사번", placeholder="예) 123456", key="login_sabun")
        pin   = st.text_input("PIN (숫자)", type="password", key="login_pin")
        submitted = st.form_submit_button("로그인", use_container_width=True, type="primary")

    # ⌨️ Enter UX 보강: 사번 → PIN, PIN → 로그인
    #   (DOM 탐색으로 해당 위젯을 찾아 포커스/클릭)
    components.html("""
    <script>
    (function(){
      function qdoc(){ try{ return window.frameElement?.ownerDocument || window.parent.document; }catch(e){ return document; } }
      function labelInput(labelText){
        const doc = qdoc();
        const label = [...doc.querySelectorAll('label')].find(l => l.textContent.trim() === labelText);
        if (!label) return null;
        // streamlit 구조상 label의 부모에 input이 하나 들어있음
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

    # 폼 제출 처리
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
        "관리자여부": False,  # 로그인 직후 main()에서 is_admin으로 재계산
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
def read_auth_df()->pd.DataFrame:
    ensure_auth_sheet(); ws=get_workbook().worksheet(AUTH_SHEET)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: df=pd.DataFrame(columns=AUTH_HEADERS)
    for c in ["사번","이름","역할","범위유형","부서1","부서2","대상사번","비고"]:
        if c in df.columns: df[c]=df[c].astype(str)
    if "활성" in df.columns: df["활성"]=df["활성"].map(_to_bool)
    return df

def is_admin(sabun:str)->bool:
    s=str(sabun).strip()
    if s in {a["사번"] for a in SEED_ADMINS}: return True
    df=read_auth_df()
    if df.empty: return False
    q=df[(df["사번"].astype(str)==s)&(df["역할"].str.lower()=="admin")&(df["활성"]==True)]
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
    ensure_settings_sheet()
    ws = get_workbook().worksheet(SETTINGS_SHEET)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty:
        return pd.DataFrame(columns=SETTINGS_HEADERS)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    for c in ["키", "값", "메모", "수정자사번", "수정자이름"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df

def get_setting(key: str, default: str = "") -> str:
    df = read_settings_df()
    if df.empty or "키" not in df.columns:
        return default
    q = df[(df["키"].astype(str) == str(key)) & (df.get("활성", True) == True)]
    if q.empty:
        return default
    return str(q.iloc[-1].get("값", default))

def set_setting(key: str, value: str, memo: str, editor_sabun: str, editor_name: str):
    ensure_settings_sheet()
    ws = get_workbook().worksheet(SETTINGS_SHEET)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}

    col_key = hmap.get("키")
    row_idx = 0
    if col_key:
        vals = ws.col_values(col_key)
        for i, v in enumerate(vals[1:], start=2):
            if str(v).strip() == str(key).strip():
                row_idx = i
                break

    now = kst_now_str()
    if row_idx == 0:
        row = [""] * len(header)
        def put(k, v):
            c = hmap.get(k)
            if c: row[c - 1] = v
        put("키", key); put("값", value); put("메모", memo); put("수정시각", now)
        put("수정자사번", editor_sabun); put("수정자이름", editor_name); put("활성", True)
        ws.append_row(row, value_input_option="USER_ENTERED")
    else:
        ws.update_cell(row_idx, hmap["값"], value)
        if "메모" in hmap: ws.update_cell(row_idx, hmap["메모"], memo)
        if "수정시각" in hmap: ws.update_cell(row_idx, hmap["수정시각"], now)
        if "수정자사번" in hmap: ws.update_cell(row_idx, hmap["수정자사번"], editor_sabun)
        if "수정자이름" in hmap: ws.update_cell(row_idx, hmap["수정자이름"], editor_name)
        if "활성" in hmap: ws.update_cell(row_idx, hmap["활성"], True)
    st.cache_data.clear()

# ── Status Line ───────────────────────────────────────────────────────────────
def render_status_line():
    try:
        # 연결만 점검(예외가 안 나면 OK)
        _ = get_workbook()
        # 간결한 상태 표기
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
    "연도","평가유형","평가대상사번","평가대상이름",
    "평가자사번","평가자이름","총점","상태","제출시각",
    "서명_대상","서명시각_대상","서명_평가자","서명시각_평가자","잠금"
]
EVAL_TYPES = ["자기", "1차", "2차"]  # UI 표시용 라벨

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
    ensure_eval_items_sheet()
    ws = get_workbook().worksheet(EVAL_ITEMS_SHEET)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty:
        df = pd.DataFrame(columns=EVAL_ITEM_HEADERS)
    if "순서" in df.columns:
        def _i(x):
            try:
                return int(float(str(x).strip()))
            except Exception:
                return 0
        df["순서"] = df["순서"].apply(_i)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)
    df = df.sort_values(["순서", "항목"]).reset_index(drop=True)
    if only_active and "활성" in df.columns:
        df = df[df["활성"] == True]
    return df

def _eval_sheet_name(year: int | str) -> str:
    return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"

def _ensure_eval_response_sheet(year: int, item_ids: list[str]) -> gspread.Worksheet:
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
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    if not row.empty:
        return str(row.iloc[0].get("이름", ""))
    # (필터링으로 못 찾을 때) 전체 시트 재조회
    try:
        full = read_sheet_df(EMP_SHEET)
        row2 = full.loc[full["사번"].astype(str) == str(sabun)]
        return "" if row2.empty else str(row2.iloc[0].get("이름", ""))
    except Exception:
        return ""

def upsert_eval_response(
    emp_df: pd.DataFrame,
    year: int,
    eval_type: str,
    target_sabun: str,
    evaluator_sabun: str,
    scores: dict[str, int],
    status: str = "제출",
) -> dict:
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_response_sheet(year, item_ids)
    header = ws.row_values(1)
    hmap = {n: i + 1 for i, n in enumerate(header)}

    # 점수(1~5) 보정 및 총점(100점 만점) 환산
    scores_list = [min(5, max(1, int(scores.get(iid, 3)))) for iid in item_ids]
    total_100 = round(sum(scores_list) * (100.0 / max(1, len(item_ids) * 5)), 1)

    # 메타
    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = kst_now_str()

    # 기존행 찾기
    values = ws.get_all_values()
    cY = hmap.get("연도"); cT = hmap.get("평가유형")
    cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    row_idx = 0
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            if (str(r[cY - 1]).strip() == str(year)
                and str(r[cT - 1]).strip() == str(eval_type)
                and str(r[cTS - 1]).strip() == str(target_sabun)
                and str(r[cES - 1]).strip() == str(evaluator_sabun)):
                row_idx = i
                break
        except Exception:
            pass

    if row_idx == 0:
        buf = [""] * len(header)
        def put(k, v):
            c = hmap.get(k)
            if c: buf[c - 1] = v
        put("연도", int(year)); put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("총점", total_100); put("상태", status); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c = hmap.get(f"점수_{iid}")
            if c: buf[c - 1] = sc
        ws.append_row(buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "row": None, "total": total_100}

    # 업데이트
    ws.update_cell(row_idx, hmap["총점"], total_100)
    ws.update_cell(row_idx, hmap["상태"], status)
    ws.update_cell(row_idx, hmap["제출시각"], now)
    ws.update_cell(row_idx, hmap["평가대상이름"], t_name)
    ws.update_cell(row_idx, hmap["평가자이름"], e_name)
    for iid, sc in zip(item_ids, scores_list):
        c = hmap.get(f"점수_{iid}")
        if c: ws.update_cell(row_idx, c, sc)
    st.cache_data.clear()
    return {"action": "update", "row": row_idx, "total": total_100}

@st.cache_data(ttl=60, show_spinner=False)
def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    name = _eval_sheet_name(year)
    try:
        ws = get_workbook().worksheet(name)
    except Exception:
        return pd.DataFrame(columns=EVAL_BASE_HEADERS)
    df = pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty:
        return df
    df = df[df["평가자사번"].astype(str) == str(sabun)]
    return df.sort_values(["평가유형", "평가대상사번", "제출시각"], ascending=[True, True, False])

def tab_eval_input(emp_df: pd.DataFrame):
    st.subheader("평가")

    # ── 최소 스타일: 한 줄(항목 | 내용 | 점수) 그리드
    st.markdown(
        """
        <style>
          .eval-row{
            display:grid; grid-template-columns:2fr 6fr 3fr; gap:12px;
            align-items:center; padding:8px 0; border-bottom:1px solid rgba(49,51,63,.08);
          }
          .eval-row .name{font-weight:600;}
          .eval-row .desc{color:#4b5563;}
          .eval-row .score .stRadio [role="radiogroup"]{
            display:flex; gap:8px; justify-content:center; align-items:center;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ── 연도
    this_year = datetime.now(tz=tz_kst()).year
    colY = st.columns([1, 3])
    with colY[0]:
        year = st.number_input("평가 연도", min_value=2000, max_value=2100,
                               value=int(this_year), step=1, key="eval_year")

    # ── 항목 로드
    items = read_eval_items_df(only_active=True)
    if items.empty:
        st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️")
        return

    # ── 권한/대상 선택
    u = st.session_state["user"]
    me_sabun = str(u["사번"])
    me_name = str(u["이름"])
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
            sel = st.selectbox("평가 대상자", ["(선택)"] + df["표시"].tolist(),
                               index=0, key="eval_target_select")
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

    # ── 상단 제목 + (슬라이더 + 일괄적용 버튼) : st.rerun() 없이 플래그로 반영
    c_head, c_slider, c_btn = st.columns([5, 2, 1])
    with c_head:
        st.markdown("#### 점수 입력 (각 1~5)")

    kbase = f"evalbulk_{year}_{evaluator_sabun}_{target_sabun}"
    slider_key = f"{kbase}_slider"
    if slider_key not in st.session_state:
        st.session_state[slider_key] = 3
    with c_slider:
        bulk_score = st.slider(
            "일괄 점수",
            min_value=1, max_value=5, step=1,
            key=slider_key
        )
    with c_btn:
        if st.button("일괄 적용", use_container_width=True, key=f"{kbase}_apply"):
            # 다음 렌더에서 항목 라디오 값 주입 (rerun 없이 탭 유지)
            st.session_state[f"__apply_bulk_{kbase}"] = int(bulk_score)
            st.toast(f"모든 항목에 {bulk_score}점 적용", icon="✅")

    # ── 일괄 적용 플래그 처리(라디오 생성 전에 값 세팅)
    apply_key = f"__apply_bulk_{kbase}"
    if st.session_state.get(apply_key) is not None:
        _v = int(st.session_state[apply_key])
        for _iid in items["항목ID"].astype(str):
            st.session_state[f"eval_seg_{_iid}_{kbase}"] = str(_v)
        del st.session_state[apply_key]

    # ── 항목 렌더링 (이름 | 내용 | 점수)
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    scores = {}

    for r in items_sorted.itertuples(index=False):
        iid  = str(getattr(r, "항목ID"))
        name = getattr(r, "항목") or ""
        desc = getattr(r, "내용") or ""

        rkey = f"eval_seg_{iid}_{kbase}"
        if rkey not in st.session_state:
            st.session_state[rkey] = "3"

        st.markdown('<div class="eval-row">', unsafe_allow_html=True)
        c1, c2, c3 = st.columns([2, 6, 3])
        with c1:
            st.markdown(f'<div class="name">{name}</div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="desc">{desc.replace(chr(10), "<br/>")}</div>', unsafe_allow_html=True)
        with c3:
            pick = st.radio(" ", ["1","2","3","4","5"],
                            horizontal=True, key=rkey, label_visibility="collapsed")
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
        if st.button("제출/저장", type="primary", use_container_width=True, key=f"eval_save_{kbase}"):
            try:
                rep = upsert_eval_response(
                    emp_df, int(year), eval_type,
                    str(target_sabun), str(evaluator_sabun),
                    scores, "제출"
                )
                st.success(
                    ("제출 완료" if rep["action"] == "insert" else "업데이트 완료")
                    + f" (총점 {rep['total']}점)",
                    icon="✅"
                )
                st.toast("평가 저장됨", icon="✅")
            except Exception as e:
                st.exception(e)

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
JOBDESC_HEADERS=[
    "사번","연도","버전","소속","부서1","부서2","작성자사번","작성자이름",
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

    # 로그인 사용자/권한
    u = st.session_state["user"]
    me_sabun = str(u["사번"])
    me_name = str(u["이름"])

    # 대상 선택(관리자/매니저는 범위 내에서 선택, 직원은 본인)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)
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
        target_name = _emp_name_by_sabun(emp_df, target_sabun)
    else:
        target_sabun = me_sabun
        target_name = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")

    # 연도
    this_year = datetime.now(tz=tz_kst()).year
    col_year = st.columns([1, 3])
    with col_year[0]:
        year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="job_year")

    # 대상자 기본값(조직/직무)
    tgt_row = emp_df.loc[emp_df["사번"].astype(str) == str(target_sabun)]
    pref_dept1  = ("" if tgt_row.empty else str(tgt_row.iloc[0].get("부서1", "")))
    pref_dept2  = ("" if tgt_row.empty else str(tgt_row.iloc[0].get("부서2", "")))
    pref_group  = ("" if tgt_row.empty else str(tgt_row.iloc[0].get("직군", "")))
    pref_series = ("" if tgt_row.empty else str(tgt_row.iloc[0].get("직종", "")))
    pref_jobname = ("" if tgt_row.empty else str(tgt_row.iloc[0].get("직무", "")))

    # 날짜 기본값(관리자 설정 → 없으면 오늘/1년)
    today = datetime.now(tz=tz_kst()).strftime("%Y-%m-%d")
    defval_create = get_setting("JD.제정일", today)
    defval_update = get_setting("JD.개정일", today)
    defval_review = get_setting("JD.검토주기", "1년")

    st.markdown("#### 기본 정보")

    # 조직/직무 기본값(자동 채움 — 필요 시 수정 가능)
    c2 = st.columns([1, 1, 1, 1])
    with c2[0]:
        dept1 = st.text_input("부서1", value=pref_dept1, key="job_dept1")
    with c2[1]:
        dept2 = st.text_input("부서2", value=pref_dept2, key="job_dept2")
    with c2[2]:
        group = st.text_input("직군", value=pref_group, key="job_group")
    with c2[3]:
        series = st.text_input("직종", value=pref_series, key="job_series")

    # 직무명 + 제정/검토
    c3 = st.columns([2, 2, 1])
    with c3[0]:
        jobname = st.text_input("직무명", value=pref_jobname, key="job_jobname")
    with c3[1]:
        d_create = st.text_input("제정일", value=st.session_state.get("job_d_create", defval_create), key="job_d_create")
    with c3[2]:
        review = st.text_input("검토주기", value=st.session_state.get("job_review", defval_review), key="job_review")

    # 개정/비고
    c4 = st.columns([2, 2])
    with c4[0]:
        d_update = st.text_input("개정일", value=st.session_state.get("job_d_update", defval_update), key="job_d_update")
    with c4[1]:
        memo = st.text_input("비고", value="", key="job_memo")

    # 본문
    st.markdown("#### 직무 내용")
    job_summary = st.text_area("직무개요", "", height=80, key="job_summary")
    job_main    = st.text_area("주업무", "", height=120, key="job_main")
    job_other   = st.text_area("기타업무", "", height=80, key="job_other")

    # 교육/자격 (직원공통필수교육: 크게)
    st.markdown("#### 요건/교육")
    c5 = st.columns([1, 1, 1])
    with c5[0]:
        edu_req   = st.text_input("필요학력", "", key="job_edu")
    with c5[1]:
        major_req = st.text_input("전공계열", "", key="job_major")
    with c5[2]:
        license_  = st.text_input("면허", "", key="job_license")

    c6 = st.columns([1, 1, 1])
    with c6[0]:
        edu_common = st.text_area("직원공통필수교육", "", height=200, key="job_edu_common")
    with c6[1]:
        edu_cont   = st.text_input("보수교육", "", key="job_edu_cont")
    with c6[2]:
        edu_spec   = st.text_input("특성화교육", "", key="job_edu_spec")

    c7 = st.columns([1, 1, 1])
    with c7[0]:
        edu_etc  = st.text_input("기타교육", "", key="job_edu_etc")
    with c7[1]:
        career   = st.text_input("경력(자격요건)", "", key="job_career")
    with c7[2]:
        pass

    # 서명(선택)
    st.markdown("#### 서명(선택)")
    c8 = st.columns([1, 2])
    with c8[0]:
        sign_type = st.selectbox("서명방식", ["", "text", "image"], index=0, key="job_sign_type")
    with c8[1]:
        sign_data = st.text_input("서명데이터", "", key="job_sign_data")

    # 저장 버튼
    st.markdown("---")
    col_btn = st.columns([1, 3])
    with col_btn[0]:
        do_save = st.button("저장/업서트", type="primary", use_container_width=True, key="job_save_btn")

    if do_save:
        # 기존 버전 존재 시 → 최신 버전 업데이트, 없으면 자동 신버전
        try:
            df_exist = read_jobdesc_df()
            latest_ver = 0
            if not df_exist.empty:
                sub = df_exist[(df_exist["사번"].astype(str) == str(target_sabun)) &
                               (df_exist["연도"].astype(int) == int(year))]
                if not sub.empty:
                    latest_ver = int(sub["버전"].astype(int).max())

            rec = {
                "사번": str(target_sabun),
                "연도": int(year),
                "버전": int(latest_ver),  # 0이면 upsert에서 자동 신버전, >0이면 해당 버전 업데이트
                # "소속": 생략(필요 시 추가)
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

            rep = upsert_jobdesc(rec, as_new_version=False)
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

# ── 부서이력/이동(필수 최소) ──────────────────────────────────────────────────
HIST_SHEET="부서이력"
def ensure_dept_history_sheet():
    wb=get_workbook()
    try: return wb.worksheet(HIST_SHEET)
    except WorksheetNotFound:
        ws=wb.add_worksheet(title=HIST_SHEET, rows=200, cols=10)
        ws.update("A1", [["사번","이름","부서1","부서2","시작일","종료일","변경사유","승인자","메모","등록시각"]]); return ws

@st.cache_data(ttl=60, show_spinner=False)
def read_dept_history_df()->pd.DataFrame:
    ensure_dept_history_sheet(); ws=get_workbook().worksheet(HIST_SHEET)
    df=pd.DataFrame(ws.get_all_records(numericise_ignore=["all"]))
    if df.empty: return df
    for c in ["시작일","종료일","등록시각"]: 
        if c in df.columns: df[c]=df[c].astype(str)
    if "사번" in df.columns: df["사번"]=df["사번"].astype(str)
    return df

def apply_department_change(emp_df:pd.DataFrame, sabun:str, new_dept1:str, new_dept2:str, start_date:datetime.date, reason:str="", approver:str="")->dict:
    ensure_dept_history_sheet()
    wb=get_workbook(); ws_hist=wb.worksheet(HIST_SHEET)
    start_str=start_date.strftime("%Y-%m-%d"); prev_end=(start_date-timedelta(days=1)).strftime("%Y-%m-%d")
    row=emp_df.loc[emp_df["사번"].astype(str)==str(sabun)]
    if row.empty: raise RuntimeError("사번을 찾지 못했습니다.")
    name=str(row.iloc[0].get("이름",""))
    header=ws_hist.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
    # close open range
    values=ws_hist.get_all_values(); cS=hmap.get("사번"); cE=hmap.get("종료일")
    for i in range(2,len(values)+1):
        if values[i-1][cS-1].strip()==str(sabun).strip() and values[i-1][cE-1].strip()=="":
            ws_hist.update_cell(i, cE, prev_end)
    # append new
    rec={"사번":str(sabun),"이름":name,"부서1":new_dept1,"부서2":new_dept2,"시작일":start_str,"종료일":"","변경사유":reason,"승인자":approver,"메모":"","등록시각":kst_now_str()}
    rowbuf=[rec.get(h,"") for h in header]; ws_hist.append_row(rowbuf, value_input_option="USER_ENTERED")
    # apply now
    applied=False
    if start_date<=datetime.now(tz=tz_kst()).date():
        ws_emp, header_emp, hmap_emp = _get_ws_and_headers(EMP_SHEET)
        row_idx=_find_row_by_sabun(ws_emp,hmap_emp,str(sabun))
        if row_idx>0:
            if "부서1" in hmap_emp: _update_cell(ws_emp,row_idx,hmap_emp["부서1"],new_dept1)
            if "부서2" in hmap_emp: _update_cell(ws_emp,row_idx,hmap_emp["부서2"],new_dept2)
            applied=True
    st.cache_data.clear()
    return {"applied_now":applied,"start_date":start_str,"new_dept1":new_dept1,"new_dept2":new_dept2}

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
            _update_cell(ws, r, hmap["PIN_hash"], _sha256_hex(pin1.strip()))
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
            if "PIN_hash" not in hmap or "사번" not in hmap: st.error(f"'{EMP_SHEET}' 시트에 '사번' 또는 'PIN_hash' 헤더가 없습니다."); return
            sabun_col=hmap["사번"]; pin_col=hmap["PIN_hash"]
            sabun_values=ws.col_values(sabun_col)[1:]; pos={str(v).strip():i for i,v in enumerate(sabun_values,start=2)}
            updates=[]
            for _, row in preview.iterrows():
                sabun=str(row["사번"]).strip(); r_idx=pos.get(sabun,0)
                if r_idx:
                    a1=gspread.utils.rowcol_to_a1(r_idx, pin_col)
                    updates.append({"range":a1,"values":[[_sha256_hex(row["새_PIN"])]]})
            if not updates: st.warning("업데이트할 대상이 없습니다.", icon="⚠️"); return
            CHUNK=100; pbar=st.progress(0.0, text="시트 업데이트(배치) 중...")
            for i in range(0,len(updates),CHUNK):
                ws.batch_update(updates[i:i+CHUNK]); pbar.progress(min(1.0,(i+CHUNK)/len(updates))); time.sleep(0.2)
            st.cache_data.clear(); st.success(f"일괄 발급 완료: {len(updates):,}명 반영", icon="✅"); st.toast("PIN 일괄 발급 반영됨", icon="✅")
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
    st.markdown("### 평가 항목 관리")
    df=read_eval_items_df(only_active=False)
    st.write(f"현재 등록: **{len(df)}개** (활성 {df[df['활성']==True].shape[0]}개)")
    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        edit_df=df[["항목ID","항목","순서","활성"]].copy().reset_index(drop=True)
        edited=st.data_editor(edit_df, use_container_width=True, height=380,
                              column_config={"항목ID":st.column_config.TextColumn(disabled=True),
                                             "항목":st.column_config.TextColumn(disabled=True),
                                             "활성":st.column_config.CheckboxColumn(disabled=True),
                                             "순서":st.column_config.NumberColumn(step=1, min_value=0)}, num_rows="fixed")
        if st.button("순서 일괄 저장", use_container_width=True, key="adm_eval_order_save"):
            try:
                # 간단 일괄 반영
                ws=get_workbook().worksheet(EVAL_ITEMS_SHEET); header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
                col_id=hmap.get("항목ID"); col_ord=hmap.get("순서")
                vals=ws.col_values(col_id); pos={str(v).strip():i for i,v in enumerate(vals[1:],start=2)}
                for _, r in edited.iterrows():
                    iid=str(r["항목ID"]).strip(); 
                    if iid in pos: ws.update_cell(pos[iid], col_ord, int(r["순서"]))
                st.cache_data.clear(); st.success("순서가 반영되었습니다."); st.rerun()
            except Exception as e:
                st.exception(e)
    st.divider()
    st.markdown("### 신규 등록 / 수정")
    choices=["(신규)"]+[f"{r['항목ID']} - {r['항목']}" for _,r in df.iterrows()]
    sel=st.selectbox("대상 선택", choices, index=0, key="adm_eval_pick")
    item_id=None; name=""; desc=""; order=(df["순서"].max()+1 if not df.empty else 1); active=True; memo=""
    if sel!="(신규)":
        iid=sel.split(" - ",1)[0]; row=df.loc[df["항목ID"]==iid].iloc[0]
        item_id=row["항목ID"]; name=str(row.get("항목","")); desc=str(row.get("내용","")); order=int(row.get("순서",0)); active=bool(row.get("활성",True)); memo=str(row.get("비고",""))
    c1,c2=st.columns([3,1])
    with c1:
        name=st.text_input("항목명", value=name, key="adm_eval_name")
        desc=st.text_area("설명(문항 내용)", value=desc, height=100, key="adm_eval_desc")
        memo=st.text_input("비고(선택)", value=memo, key="adm_eval_memo")
    with c2:
        order=st.number_input("순서", min_value=0, step=1, value=int(order), key="adm_eval_order")
        active=st.checkbox("활성", value=active, key="adm_eval_active")
        if st.button("저장(신규/수정)", type="primary", use_container_width=True, key="adm_eval_save"):
            if not name.strip(): st.error("항목명을 입력하세요.")
            else:
                try:
                    # upsert
                    ensure_eval_items_sheet(); ws=get_workbook().worksheet(EVAL_ITEMS_SHEET)
                    header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
                    if not item_id:
                        # new id
                        vals=ws.col_values(hmap.get("항목ID"))[1:]; nums=[]; 
                        for v in vals:
                            s=str(v).strip()
                            if s.startswith("ITM"):
                                try: nums.append(int(s[3:]))
                                except: pass
                        new_id=f"ITM{((max(nums)+1) if nums else 1):04d}"
                        rowbuf=[""]*len(header)
                        def put(k,v):
                            c=hmap.get(k)
                            if c: rowbuf[c-1]=v
                        put("항목ID", new_id); put("항목", name.strip()); put("내용", desc.strip()); put("순서", int(order)); put("활성", bool(active)); 
                        if "비고" in hmap: put("비고", memo.strip())
                        ws.append_row(rowbuf, value_input_option="USER_ENTERED")
                        st.success(f"저장 완료 (항목ID: {new_id})"); st.cache_data.clear(); st.rerun()
                    else:
                        idx=0; col_id=hmap.get("항목ID"); vals=ws.col_values(col_id); 
                        for i,v in enumerate(vals[1:], start=2):
                            if str(v).strip()==str(item_id).strip(): idx=i; break
                        if idx==0:
                            st.error("대상 항목을 찾을 수 없습니다.")
                        else:
                            ws.update_cell(idx, hmap["항목"], name.strip())
                            ws.update_cell(idx, hmap["내용"], desc.strip())
                            ws.update_cell(idx, hmap["순서"], int(order))
                            ws.update_cell(idx, hmap["활성"], bool(active))
                            if "비고" in hmap: ws.update_cell(idx, hmap["비고"], memo.strip())
                            st.success("업데이트 완료"); st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.exception(e)
        if item_id:
            if st.button("비활성화(소프트 삭제)", use_container_width=True, key="adm_eval_disable"):
                try:
                    ws=get_workbook().worksheet(EVAL_ITEMS_SHEET); header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
                    col_id=hmap.get("항목ID"); col_active=hmap.get("활성"); vals=ws.col_values(col_id)
                    for i,v in enumerate(vals[1:], start=2):
                        if str(v).strip()==str(item_id).strip(): ws.update_cell(i, col_active, False); break
                    st.success("비활성화 완료"); st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.exception(e)
            if st.button("행 삭제(완전 삭제)", use_container_width=True, key="adm_eval_delete"):
                try:
                    ws=get_workbook().worksheet(EVAL_ITEMS_SHEET); header=ws.row_values(1); hmap={n:i+1 for i,n in enumerate(header)}
                    col_id=hmap.get("항목ID"); vals=ws.col_values(col_id)
                    for i,v in enumerate(vals[1:], start=2):
                        if str(v).strip()==str(item_id).strip(): ws.delete_rows(i); break
                    st.success("삭제 완료"); st.cache_data.clear(); st.rerun()
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
    st.markdown("### 권한 관리")
    df_auth=read_auth_df()
    st.markdown("#### 권한 규칙 추가")
    df_pick=emp_df.copy(); df_pick["표시"]=df_pick.apply(lambda r:f"{str(r.get('사번',''))} - {str(r.get('이름',''))}",axis=1); df_pick=df_pick.sort_values(["사번"])
    c1,c2=st.columns([2,2])
    with c1: giver=st.selectbox("권한 주체(사번 - 이름)", ["(선택)"]+df_pick["표시"].tolist(), index=0, key="acl_giver")
    with c2: role=st.selectbox("역할", ["manager","admin"], index=0, key="acl_role")
    c3,c4=st.columns([1,3])
    with c3: scope_type=st.radio("범위유형", ["부서","개별"], horizontal=True, key="acl_scope_type")
    with c4: memo=st.text_input("비고(선택)", "", key="acl_memo")
    add_rows=[]
    if scope_type=="부서":
        cA,cB,cC=st.columns([1,1,1])
        with cA: dept1=st.selectbox("부서1", [""]+sorted([x for x in emp_df.get("부서1",[]).dropna().unique() if x]), index=0, key="acl_dept1")
        with cB:
            sub=emp_df.copy()
            if dept1: sub=sub[sub["부서1"].astype(str)==dept1]
            opt_d2=[""]+sorted([x for x in sub.get("부서2",[]).dropna().unique() if x])
            dept2=st.selectbox("부서2(선택)", opt_d2, index=0, key="acl_dept2")
        with cC: active=st.checkbox("활성", True, key="acl_active_dep")
        if st.button("➕ 부서 권한 추가", type="primary", use_container_width=True, key="acl_add_dep"):
            if giver!="(선택)":
                sab=giver.split(" - ",1)[0]; name=_emp_name_by_sabun(emp_df,sab)
                add_rows.append({"사번":sab,"이름":name,"역할":role,"범위유형":"부서","부서1":dept1,"부서2":dept2,"대상사번":"","활성":bool(active),"비고":memo.strip()})
            else: st.warning("권한 주체를 선택하세요.", icon="⚠️")
    else:
        cA,cB,cC=st.columns([2,2,1])
        with cA: targets=st.multiselect("대상자(여러 명 선택)", df_pick["표시"].tolist(), default=[], key="acl_targets")
        with cB: active=st.checkbox("활성", True, key="acl_active_ind")
        with cC: st.write("")
        if st.button("➕ 개별 권한 추가", type="primary", use_container_width=True, key="acl_add_ind"):
            if giver!="(선택)" and targets:
                sab=giver.split(" - ",1)[0]; name=_emp_name_by_sabun(emp_df,sab)
                for t in targets:
                    tsab=t.split(" - ",1)[0]
                    add_rows.append({"사번":sab,"이름":name,"역할":role,"범위유형":"개별","부서1":"","부서2":"","대상사번":tsab,"활성":bool(active),"비고":memo.strip()})
            else: st.warning("권한 주체/대상자를 선택하세요.", icon="⚠️")
    if add_rows:
        try:
            ws=get_workbook().worksheet(AUTH_SHEET); header=ws.row_values(1); rows=[[r.get(h,"") for h in header] for r in add_rows]
            ws.append_rows(rows, value_input_option="USER_ENTERED"); st.cache_data.clear(); st.success(f"규칙 {len(rows)}건 추가 완료", icon="✅"); st.rerun()
        except Exception as e:
            st.exception(e)
    st.divider()
    st.markdown("#### 권한 규칙 목록")
    if df_auth.empty: st.caption("권한 규칙이 없습니다.")
    else:
        view=df_auth.sort_values(["역할","사번","범위유형","부서1","부서2","대상사번"])
        st.dataframe(view, use_container_width=True, height=380)
    st.divider()
    st.markdown("#### 규칙 삭제 (행 번호)")
    del_row=st.number_input("삭제할 시트 행 번호 (헤더=1)", min_value=2, step=1, value=2, key="acl_del_row")
    if st.button("🗑️ 해당 행 삭제", use_container_width=True, key="acl_del_btn"):
        try:
            ws=get_workbook().worksheet(AUTH_SHEET); ws.delete_rows(int(del_row)); st.cache_data.clear(); st.success(f"{del_row}행 삭제 완료", icon="✅"); st.rerun()
        except Exception as e:
            st.exception(e)

# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    # 제목을 조금 작게 (H2)
    st.markdown(f"## {APP_TITLE}")
    render_status_line()

    # 1) 직원 시트 로딩
    try:
        emp_df_all = read_sheet_df(EMP_SHEET)
    except Exception as e:
        st.error(f"'{EMP_SHEET}' 시트 로딩 실패: {e}")
        return

    # 2) 로그인 요구
    require_login(emp_df_all)

    # 3) 로그인 직후: 관리자 플래그 최신화(시드/시트 기준)
    st.session_state["user"]["관리자여부"] = is_admin(st.session_state["user"]["사번"])

    # 4) 데이터 뷰 분기
    # - 직원 탭: 전체 데이터(의사 포함)
    # - 그 외 탭(평가/직무기술서/직무능력/관리자): '의사' 숨김
    emp_df_for_staff = emp_df_all
    emp_df_for_rest  = _hide_doctors(emp_df_all)

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

    # 6) 탭 구성 (한 번만!)
    if u.get("관리자여부", False):
        tabs = st.tabs(["직원", "평가", "직무기술서", "직무능력평가", "관리자", "도움말"])
    else:
        tabs = st.tabs(["직원", "평가", "직무기술서", "직무능력평가", "도움말"])

    # 직원 탭: 전체 데이터
    with tabs[0]:
        tab_staff(emp_df_for_staff)

    # 평가: 의사 숨김
    with tabs[1]:
        tab_eval_input(emp_df_for_rest)

    # 직무기술서: 의사 숨김
    with tabs[2]:
        tab_job_desc(emp_df_for_rest)

    # 직무능력평가: 의사 숨김
    with tabs[3]:
        tab_competency(emp_df_for_rest)

    # 관리자 탭: 의사 숨김 데이터 사용
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
                tab_admin_pin(emp_df_for_rest)
            elif admin_page == "부서(근무지) 이동":
                tab_admin_transfer(emp_df_for_rest)
            elif admin_page == "평가 항목 관리":
                tab_admin_eval_items()
            else:
                tab_admin_acl(emp_df_for_rest)

    # 도움말
    with tabs[-1]:
        st.markdown(
            """
            ### 사용 안내
            - 직원 탭: 전체 데이터(의사 포함), 권한에 따라 행 제한
            - 평가/직무기술서/직무능력평가/관리자: '의사' 직무는 숨김
            - 상태표시: 상단에 'DB연결 ... (KST)'
            """
        )
        # (선택) 원본 스프레드시트 빠른 이동 링크
        try:
            sheet_id = st.secrets["sheets"]["HR_SHEET_ID"]
            url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
            st.caption(f"📄 원본 스프레드시트: [{url}]({url})")
        except Exception:
            pass

# ── 엔트리포인트 ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()

