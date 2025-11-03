# -*- coding: utf-8 -*-
# HYBRID v0.3 — Static sheets (no "{year}" pattern)
#
# Master: Google Sheets → Mirror: Supabase (read/analytics)
# - App is READ-ONLY from DB (dashboards). Write happens ONLY via explicit [Sheet→DB Sync] button.
# - Secrets compatibility:
#   (A) Default: st.secrets["supabase"]{url, anon_key, service_key?}, st.secrets["gsheets"]{service_account, spreadsheet_key}, st.secrets["tables"]
#   (B) User TOML: [gcp_service_account]{...}, [sheets].HR_SHEET_ID, [app]{TITLE,TZ}, [supabase]{url,key}
# - Sheet names are FIXED (no dynamic year). Mapping is:
#       employees="직원", eval_items="평가항목", acl="권한"
#       eval_responses="인사평가"
#       job_specs="직무기술서", job_specs_approvals="직무기술서_승인"
#       competency_evals="직무능력평가"
#
# To change any sheet tab name, edit SHEETS constant below or set st.secrets["gsheets"]["sheets"].
#
import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
import gspread
from google.oauth2.service_account import Credentials

# ────────────────────────────────────────────────────────────────
# Secrets helpers (support both layouts)
# ────────────────────────────────────────────────────────────────
def _secrets_get(path, default=None):
    try:
        cur = st.secrets
        for part in path.split('.'):
            if not part: 
                continue
            cur = cur.get(part) if hasattr(cur, 'get') else cur[part]
        return cur
    except Exception:
        return default

def _detect_spreadsheet_key():
    key = _secrets_get("gsheets.spreadsheet_key")
    if key: return key
    return _secrets_get("sheets.HR_SHEET_ID")

def _detect_service_account():
    svc = _secrets_get("gsheets.service_account")
    if svc: return svc
    return _secrets_get("gcp_service_account")

def _detect_supabase_url():
    return _secrets_get("supabase.url")

def _detect_supabase_read_key():
    k = _secrets_get("supabase.anon_key")
    if k: return k
    return _secrets_get("supabase.key")

def _detect_supabase_service_key():
    svc = _secrets_get("supabase.service_key")
    if svc: return svc
    return _detect_supabase_read_key()

APP_TITLE = _secrets_get("app.TITLE", "HISMEDI - 인사/HR (Hybrid)")
st.set_page_config(page_title=APP_TITLE, layout="wide")

SPREADSHEET_KEY = _detect_spreadsheet_key()
SERVICE_ACCOUNT = _detect_service_account()

# ────────────────────────────────────────────────────────────────
# Fixed sheet mapping (no dynamic year)
# ────────────────────────────────────────────────────────────────
SHEETS = _secrets_get("gsheets.sheets") or {
    "employees": "직원",
    "eval_items": "평가항목",
    "acl": "권한",
    "eval_responses": "인사평가",
    "job_specs": "직무기술서",
    "job_specs_approvals": "직무기술서_승인",
    "competency_evals": "직무능력평가",
}

TABLES = _secrets_get("tables") or {
    "employees": {"pk": ["사번"]},
    "eval_items": {"pk": ["항목ID"]},
    "acl": {"pk": ["사번"]},
    "eval_responses": {"pk": ["연도","사번","항목ID","버전"]},
    "job_specs": {"pk": ["연도","사번","버전"]},
    "job_specs_approvals": {"pk": ["연도","사번","버전","승인자"]},
    "competency_evals": {"pk": ["연도","사번","항목ID","버전"]},
}

# ────────────────────────────────────────────────────────────────
# Clients
# ────────────────────────────────────────────────────────────────
def _get_supabase(readonly: bool = True) -> Client:
    url = _detect_supabase_url()
    if not url:
        st.error("Supabase URL이 누락되었습니다: st.secrets['supabase']['url']")
        st.stop()
    key = _detect_supabase_read_key() if readonly else _detect_supabase_service_key()
    if not key:
        st.error("Supabase 키가 누락되었습니다: anon_key/key 또는 service_key")
        st.stop()
    return create_client(url, key)

def _get_gspread_client() -> gspread.Client:
    svc = SERVICE_ACCOUNT
    if not isinstance(svc, dict) or "private_key" not in svc:
        st.error("Google Service Account가 누락되었습니다: [gsheets.service_account] 또는 [gcp_service_account]")
        st.stop()
    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

# ────────────────────────────────────────────────────────────────
# Utils
# ────────────────────────────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _sheet_title_for(table: str) -> str:
    return SHEETS.get(table, table)

# ────────────────────────────────────────────────────────────────
# Auth (stub)
# ────────────────────────────────────────────────────────────────
def _session_valid():
    return bool(st.session_state.get("user"))

def require_login():
    if _session_valid():
        return
    st.markdown(f"### {APP_TITLE}")
    st.info("임시 로그인 (사번/이름 아무거나). 배포 시 PIN/RLS로 교체하세요.")
    id_ = st.text_input("사번", key="tmp_id")
    nm_ = st.text_input("이름", key="tmp_nm")
    if st.button("로그인"):
        if id_ and nm_:
            st.session_state["user"] = {"사번": id_, "이름": nm_}
            st.rerun()
    st.stop()

def logout():
    st.session_state.pop("user", None)
    st.rerun()

# ────────────────────────────────────────────────────────────────
# Sheets → DataFrame
# ────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def read_sheet_df(sheet_title: str) -> pd.DataFrame:
    gc = _get_gspread_client()
    try:
        ws = gc.open_by_key(SPREADSHEET_KEY).worksheet(sheet_title)
    except Exception as e:
        st.error(f"시트를 찾을 수 없습니다: '{sheet_title}' (스프레드시트ID={SPREADSHEET_KEY})")
        raise
    rows = ws.get_all_records()
    df = pd.DataFrame(rows)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df

# ────────────────────────────────────────────────────────────────
# Supabase read helpers
# ────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=30)
def sb_count(table: str) -> int:
    sb = _get_supabase(readonly=True)
    try:
        res = sb.table(table).select("*", count="exact").limit(1).execute()
        return int(getattr(res, "count", 0) or 0)
    except Exception:
        return 0

@st.cache_data(show_spinner=False, ttl=30)
def sb_select_df(table: str, limit: int = 1000) -> pd.DataFrame:
    sb = _get_supabase(readonly=True)
    res = sb.table(table).select("*").limit(limit).execute()
    data = getattr(res, "data", []) or []
    return pd.DataFrame(data)

# ────────────────────────────────────────────────────────────────
# Upsert (Sheet → Supabase). One-shot, explicit button only.
# ────────────────────────────────────────────────────────────────
def _upsert_df(table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    pk = (TABLES.get(table, {}) or {}).get("pk", [])
    if not pk:
        st.error(f"{table}: 기본키(pk) 설정이 없습니다. st.secrets['tables'][table]['pk'] 확인.")
        return 0
    sbw = _get_supabase(readonly=False)
    payload = df.to_dict(orient="records")
    total = 0
    CHUNK = 500
    for i in range(0, len(payload), CHUNK):
        batch = payload[i:i+CHUNK]
        res = sbw.table(table).upsert(batch, on_conflict=",".join(pk)).execute()
        total += len(getattr(res, "data", []) or batch)
    return total

def sync_table(table: str) -> int:
    title = _sheet_title_for(table)
    df = read_sheet_df(title)
    return _upsert_df(table, df)

def sync_all() -> dict:
    order = ["employees","eval_items","acl","eval_responses","job_specs","job_specs_approvals","competency_evals"]
    out = {}
    for t in order:
        if t not in SHEETS:
            continue
        try:
            out[t] = sync_table(t)
        except Exception as e:
            out[t] = f"ERROR: {e}"
    sb_count.clear()
    sb_select_df.clear()
    return out

# ────────────────────────────────────────────────────────────────
# UI: Dashboards (read-only)
# ────────────────────────────────────────────────────────────────
def tab_eval():
    st.subheader("인사평가 (DB 읽기 전용)")
    df = sb_select_df("eval_responses", limit=5000)
    if df.empty:
        st.info("데이터가 없습니다.")
        return
    # 간단 요약
    group_cols = [c for c in ["연도","사번"] if c in df.columns]
    if group_cols:
        counts = df.groupby(group_cols).size().reset_index(name="응답수")
        st.dataframe(counts, use_container_width=True)
    st.dataframe(df.head(200), use_container_width=True)

def tab_job_desc():
    st.subheader("직무기술서 (DB 읽기 전용)")
    st.metric("등록건수", sb_count("job_specs"))
    df = sb_select_df("job_specs", limit=5000)
    st.dataframe(df.head(200), use_container_width=True)

def tab_competency():
    st.subheader("직무능력평가 (DB 읽기 전용)")
    st.metric("등록건수", sb_count("competency_evals"))
    df = sb_select_df("competency_evals", limit=5000)
    st.dataframe(df.head(200), use_container_width=True)

# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────
def main():
    require_login()
    user = st.session_state.get("user", {})
    st.markdown(f"<h2 style='margin-bottom:0'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    st.caption(f"사용자: {user.get('이름','')}({user.get('사번','')}) · {now_str()}")

    c1, c2, _ = st.columns([1,1,2], gap="small")
    with c1:
        if st.button("로그아웃", use_container_width=True):
            logout()
    with c2:
        if st.button("🔄 시트→DB 동기화", use_container_width=True, help="고정 시트들에서 Supabase로 업서트합니다."):
            with st.spinner("동기화 중..."):
                result = sync_all()
            st.success("동기화 완료")
            st.json(result)

    tabs = st.tabs(["인사평가","직무기술서","직무능력평가"])
    with tabs[0]:
        tab_eval()
    with tabs[1]:
        tab_job_desc()
    with tabs[2]:
        tab_competency()

if __name__ == "__main__":
    main()
