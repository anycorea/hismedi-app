# -*- coding: utf-8 -*-
import os, re, io, html, time, requests, urllib.parse
from pathlib import Path
from typing import List, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import text, create_engine
from pypdf import PdfReader

# ====================================================================================
# Edge Function 즉시 동기화
# ====================================================================================
# App settings → Secrets 에 아래 키 필요
#   SUPABASE_FUNC_BASE = "https://<PROJECT-REF>.supabase.co/functions/v1"
#   SUPABASE_ANON_KEY  = "<anon public token>"
SUPABASE_FUNC_BASE = (st.secrets.get("SUPABASE_FUNC_BASE") or os.getenv("SUPABASE_FUNC_BASE") or "").rstrip("/")
SUPABASE_ANON_KEY  = (st.secrets.get("SUPABASE_ANON_KEY")  or os.getenv("SUPABASE_ANON_KEY")  or "").strip()

def _trigger_edge_func(slug: str) -> dict:
    """Supabase Edge Function 호출 (예: 'sync_main', 'sync_qna')."""
    if not SUPABASE_FUNC_BASE or not SUPABASE_ANON_KEY:
        raise RuntimeError("SUPABASE_FUNC_BASE / SUPABASE_ANON_KEY 시크릿이 필요합니다.")
    url = f"{SUPABASE_FUNC_BASE}/{slug}"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {SUPABASE_ANON_KEY}", "Content-Type": "application/json"},
        json={}, timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"{slug} 호출 실패 {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        return {"ok": True, "raw": r.text}

# ====================================================================================
# 한 번에 동기화(전체/선택 포함)
# ====================================================================================
def _do_sync_all(include_pdf: bool = True) -> tuple[int, int, int]:
    """
    Main + QnA를 Edge Function으로 즉시 동기화하고,
    include_pdf=True면 Google Drive 규정(PDF) 인덱싱까지 수행.
    반환값: (main_count, qna_count, pdf_indexed)
    """
    # 1) Main
    resp_main = _trigger_edge_func("sync_main")
    c_main = int(resp_main.get("count", 0))

    # 2) QnA
    resp_qna = _trigger_edge_func("sync_qna")
    c_qna = int(resp_qna.get("count", 0))

    # 3) (선택) PDF 인덱싱
    c_pdf = 0
    if include_pdf and DRIVE_API_KEY and DRIVE_FOLDER_ID:
        res = index_pdfs_from_drive(eng, DRIVE_FOLDER_ID, DRIVE_API_KEY)
        c_pdf = int(res.get("indexed", 0))

    # 캐시/세션 청소 (최신 데이터 바로 보이게)
    st.cache_data.clear()
    for k in ("main_results", "qna_results", "pdf_results", "pdf_sel_idx", "pdf_kw_list"):
        st.session_state.pop(k, None)

    return (c_main, c_qna, c_pdf)

# ====================================================================================
# Google Drive 폴더/파일 ID 추출기
# ====================================================================================
def _extract_drive_id(value: str) -> str:
    """URL/공유링크/순수ID 입력 모두에서 구글 드라이브 ID만 추출."""
    v = (value or "").strip()
    if not v:
        return ""
    if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", v):
        return v
    try:
        parsed = urllib.parse.urlparse(v)
        m = re.search(r"/folders/([A-Za-z0-9_\-]{20,})", parsed.path)
        if not m:
            m = re.search(r"/file/d/([A-Za-z0-9_\-]{20,})", parsed.path)
        if m:
            return m.group(1)
        qs = urllib.parse.parse_qs(parsed.query)
        if "id" in qs and qs["id"]:
            cand = qs["id"][0]
            if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", cand):
                return cand
    except Exception:
        pass
    return re.sub(r"[^A-Za-z0-9_\-]", "", v)

# ====================================================================================
# 페이지 / 레이아웃
# ====================================================================================
st.set_page_config(page_title="★★★ HISMEDI 인증 ★★★", layout="wide")
st.markdown("""
<style>
mark { background:#ffe2a8; }
.main-title{ font-weight:800; font-size:26px; line-height:1.25; margin:4px 0 12px; }
@media (max-width: 768px){ .main-title{ font-size:22px; } }
</style>
""", unsafe_allow_html=True)
st.markdown('<div class="main-title">HISMEDI 인증</div>', unsafe_allow_html=True)

# ====================================================================================
# 간단 접근 비밀번호(선택)
# ====================================================================================
_APP_PW = (st.secrets.get("APP_PASSWORD") or os.getenv("APP_PASSWORD") or "").strip()

def _is_valid_pw_format(pw: str) -> bool:
    return bool(re.fullmatch(r"\d{8}", pw or ""))

if _APP_PW:
    if not _is_valid_pw_format(_APP_PW):
        st.error("APP_PASSWORD 는 8자리 숫자여야 합니다. (예: 12345678)")
        st.stop()
    if not st.session_state.get("pw_ok", False):
        now = time.time()
        locked_until = float(st.session_state.get("pw_locked_until", 0))
        if now < locked_until:
            st.warning(f"{int(locked_until - now)}초 후 재시도하세요.")
            st.stop()
        with st.form("pw_gate", clear_on_submit=False):
            pw = st.text_input("접속 비밀번호 (8자리 숫자)", type="password", max_chars=8, placeholder="예: 12345678")
            ok = st.form_submit_button("확인")
        if ok:
            if not _is_valid_pw_format(pw):
                st.error("8자리 숫자만 입력하세요.")
                st.stop()
            if pw == _APP_PW:
                st.session_state["pw_ok"] = True
                st.session_state.pop("pw_attempts", None)
                st.session_state.pop("pw_locked_until", None)
                st.rerun()
            else:
                attempts = int(st.session_state.get("pw_attempts", 0)) + 1
                st.session_state["pw_attempts"] = attempts
                if attempts >= 5:
                    st.session_state["pw_locked_until"] = time.time() + 60
                    st.session_state["pw_attempts"] = 0
                    st.error("실패가 많습니다. 60초 후 다시 시도하세요.")
                else:
                    st.error(f"비밀번호가 틀렸습니다. (시도 {attempts}/5)")
                st.stop()
        else:
            st.stop()

# ====================================================================================
# DB 연결 유틸
# ====================================================================================
def _load_database_url() -> str:
    url = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        st.error("DATABASE_URL 시크릿이 없습니다.")
        st.stop()
    return str(url).strip()

def _ensure_psycopg_url(url: str) -> str:
    u = url
    if u.startswith("postgresql://"):
        u = u.replace("postgresql://", "postgresql+psycopg://", 1)
    if u.startswith("postgres://"):
        u = u.replace("postgres://", "postgresql+psycopg://", 1)
    if "sslmode=" not in u:
        u += ("&" if ("?" in u) else "?") + "sslmode=require"
    return u

@st.cache_resource(show_spinner="DB 엔진 생성 중...")
def get_engine():
    url = _ensure_psycopg_url(_load_database_url())
    return create_engine(url, connect_args={"options": "-c statement_cache_mode=none"}, pool_pre_ping=True)

def _list_columns(eng, table: str) -> List[str]:
    sql = text("""
        select column_name
        from information_schema.columns
        where table_schema='public' and table_name=:t
        order by ordinal_position
    """)
    with eng.begin() as con:
        return [r[0] for r in con.execute(sql, {"t": table}).fetchall()]

def _table_exists(eng, table: str) -> bool:
    with eng.begin() as con:
        return con.execute(text("select to_regclass(:q)"), {"q": f"public.{table}"}).scalar() is not None

def _pick_table(eng, prefer_first: List[str]):
    for t in prefer_first:
        if _table_exists(eng, t):
            return t
    return None

def _qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

MAIN_DEFAULT_SEARCH_COLS = ["조사장소", "조사항목", "세부항목", "기준문구", "확인방법", "근거", "비고"]
QNA_DEFAULT_SEARCH_COLS  = ["조사위원 질문, 확인내용", "조사위원 질문", "확인내용", "조사장소"]

def _choose_search_cols(eng, table: str) -> List[str]:
    all_cols = _list_columns(eng, table)
    low = table.lower()
    if low.startswith("main"):
        pref = [c for c in MAIN_DEFAULT_SEARCH_COLS if c in all_cols]
    elif low.startswith("qna"):
        pref = [c for c in QNA_DEFAULT_SEARCH_COLS if c in all_cols]
    else:
        pref = []
    return pref if pref else all_cols

def search_table_any(eng, table: str, keywords: str, columns: List[str] = None, limit: int = 500) -> pd.DataFrame:
    """공백 AND, 다중 컬럼 OR, 모든 타입 ::text 캐스팅 안전 검색."""
    kw_list = [w for w in re.split(r"\s+", (keywords or "").strip()) if w]
    if not kw_list:
        return pd.DataFrame()
    select_cols = "*"
    if columns:
        select_cols = ", ".join(_qident(c) for c in columns)
    search_cols = _choose_search_cols(eng, table)
    params: Dict[str, str] = {}
    and_parts = []
    for i, kw in enumerate(kw_list):
        or_parts = []
        for j, col in enumerate(search_cols):
            p = f"kw_{i}_{j}"
            or_parts.append(f"COALESCE({_qident(col)}::text, '') ILIKE :{p}")
            params[p] = f"%{kw}%"
        and_parts.append("(" + " OR ".join(or_parts) + ")")
    where_clause = " AND ".join(and_parts)
    sql = text(f"SELECT {select_cols} FROM {_qident(table)} WHERE {where_clause} LIMIT :limit")
    params["limit"] = int(limit)
    with eng.begin() as con:
        return pd.read_sql_query(sql, con, params=params)

def run_select_query(eng, sql_text, params=None):
    q = (sql_text or "").strip().rstrip(";")
    if not re.match(r"^(select|with|explain)\b", q, re.I):
        raise ValueError("SELECT/WITH/EXPLAIN만 실행할 수 있습니다.")
    with eng.begin() as con:
        return pd.read_sql_query(text(q), con, params=params or {})

# ====================================================================================
# PDF 인덱싱/검색 (Drive 전용)
# ====================================================================================
REQUIRED_REG_COLUMNS = ["id", "filename", "page", "text", "file_mtime", "me"]

def ensure_reg_table(eng):
    with eng.begin() as con:
        try: con.execute(text("create extension if not exists pg_trgm"))
        except Exception: pass
        exists = con.execute(text("select to_regclass('public.regulations')")).scalar() is not None
        recreate = False
        if exists:
            cols = [r[0] for r in con.execute(text("""
                select column_name
                  from information_schema.columns
                 where table_schema='public' and table_name='regulations'
            """)).fetchall()]
            if set(REQUIRED_REG_COLUMNS) - set(cols):
                recreate = True
        else:
            recreate = True
        if recreate:
            con.execute(text("drop table if exists regulations cascade"))
            con.execute(text("""
                create table regulations (
                  id bigserial primary key,
                  filename text not null,
                  page int not null,
                  text text not null,
                  file_mtime bigint not null,
                  me text
                )
            """))
        con.execute(text("create index if not exists idx_reg_file on regulations(filename)"))
        con.execute(text("create index if not exists idx_reg_me   on regulations(me)"))
        try:
            con.execute(text("create index if not exists idx_reg_text_trgm on regulations using gin (text gin_trgm_ops)"))
        except Exception:
            pass

def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

@st.cache_data(ttl=600, show_spinner=False)
def _drive_list_all(folder_id: str, api_key: str):
    files = []
    def list_children(pid):
        page_token = None
        while True:
            params = {
                "q": f"'{pid}' in parents and trashed=false",
                "pageSize": 1000,
                "fields": "nextPageToken, files(id,name,mimeType,parents)",
                "key": api_key,
            }
            if page_token: params["pageToken"] = page_token
            r = requests.get("https://www.googleapis.com/drive/v3/files", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            batch = data.get("files", [])
            files.extend(batch)
            for f in batch:
                if f.get("mimeType") == "application/vnd.google-apps.folder":
                    list_children(f["id"])
            page_token = data.get("nextPageToken")
            if not page_token:
                break
    list_children(folder_id)
    return files

def _drive_path_map(folder_id: str, api_key: str):
    nodes = _drive_list_all(folder_id, api_key)
    by_id = {n["id"]: n for n in nodes}
    def path_of(fid):
        p, cur = [], by_id.get(fid)
        while cur:
            p.append(cur["name"])
            parents = cur.get("parents") or []
            cur = by_id.get(parents[0]) if parents else None
        return "/".join([x for x in reversed(p) if x])
    id_to_rel = {n["id"]: path_of(n["id"]) for n in nodes}
    rel_to_id = {v:k for k,v in id_to_rel.items()
                 if by_id[k].get("mimeType") == "application/pdf" or v.lower().endswith(".pdf")}
    return by_id, id_to_rel, rel_to_id

def _drive_download_pdf(file_id: str, api_key: str) -> bytes:
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    r = requests.get(url, params={"alt": "media", "key": api_key}, timeout=60)
    r.raise_for_status()
    return r.content

def index_pdfs_from_drive(eng, folder_id: str, api_key: str, limit_files: int = 0):
    ensure_reg_table(eng)
    by_id, id_to_rel, rel_to_id = _drive_path_map(folder_id, api_key)
    indexed = skipped = errors = 0
    done_files = []
    with eng.begin() as con:
        for rel, fid in rel_to_id.items():
            try:
                row = con.execute(text("select count(*) from regulations where filename=:fn"), {"fn": rel}).scalar()
                if row and row > 0:
                    skipped += 1; done_files.append((rel, "skip")); continue
                con.execute(text("delete from regulations where filename=:fn"), {"fn": rel})
                pdf_bytes = _drive_download_pdf(fid, api_key)
                reader = PdfReader(io.BytesIO(pdf_bytes))
                rows = []
                for pno, page in enumerate(reader.pages, start=1):
                    try: txt = page.extract_text() or ""
                    except Exception: txt = ""
                    txt = _clean_text(txt)
                    if not txt: continue
                    rows.append({"filename": rel, "page": pno, "text": txt, "file_mtime": 0, "me": fid})
                if rows:
                    pd.DataFrame(rows).to_sql("regulations", con, if_exists="append", index=False)
                    indexed += 1; done_files.append((rel, f"indexed {len(rows)}p"))
                else:
                    done_files.append((rel, "no-text"))
            except Exception as e:
                errors += 1; done_files.append((rel, f"error: {type(e).__name__}"))
            if limit_files and indexed >= limit_files:
                break
    return {"indexed": indexed, "skipped": skipped, "errors": errors, "files": done_files}

def make_snippet(text_: str, kw_list: List[str], width: int = 160) -> str:
    if not text_: return ""
    low, pos, hit = text_.lower(), -1, ""
    for k in kw_list:
        i = low.find(k.lower())
        if i != -1 and (pos == -1 or i < pos):
            pos, hit = i, k.lower()
    if pos == -1:
        return text_[:width] + ("..." if len(text_) > width else "")
    start = max(0, pos - width // 2)
    end   = min(len(text_), pos + len(hit) + width // 2)
    return ("..." if start > 0 else "") + text_[start:end] + ("..." if end < len(text_) else "")

def highlight_html(src_text: str, kw_list: List[str], width: int = 200) -> str:
    snippet = make_snippet(src_text, kw_list, width=width)
    esc = html.escape(snippet)
    for k in sorted({k for k in kw_list if k}, key=len, reverse=True):
        esc = re.compile(re.escape(k), re.IGNORECASE).sub(lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def search_regs(eng, keywords: str, filename_like: str = "", limit: int = 500, hide_ipynb_chk: bool = True):
    kw_list = [k.strip() for k in str(keywords).split() if k.strip()]
    if not kw_list:
        return pd.DataFrame()
    where_parts, params = [], {}
    for i, kw in enumerate(kw_list):
        where_parts.append(f"(text ILIKE :kw{i})")
        params[f"kw{i}"] = f"%{kw}%"
    if filename_like.strip():
        where_parts.append("filename ILIKE :fn")
        params["fn"] = f"%{filename_like.strip()}%"
    if hide_ipynb_chk:
        where_parts.append(r"(filename !~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)')")
    where_sql = " AND ".join(where_parts)
    sql = text(f"""
        select filename, page, me, text
          from regulations
         where {where_sql}
         order by filename, page
         limit :lim
    """)
    params["lim"] = int(limit)
    with eng.begin() as con:
        return pd.read_sql_query(sql, con, params=params)

# ====================================================================================
# DB 상태 표시
# ====================================================================================
from datetime import timezone, timedelta
def _to_kst(dt):
    try:
        kst = timezone(timedelta(hours=9))
        return dt.astimezone(kst) if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc).astimezone(kst)
    except Exception:
        return dt

SHOW_DB_BADGE = str(st.secrets.get("SHOW_DB_BADGE", os.getenv("SHOW_DB_BADGE", "0"))).strip().lower() in ("1", "true")
eng = get_engine()
try:
    with eng.begin() as con:
        user, port, now_utc = con.execute(text("select current_user, inet_server_port(), now()")).one()
    from datetime import datetime
    now_kst = _to_kst(now_utc)
    st.session_state["db_status"] = {"ok": True, "user": user, "port": port, "time_kst": now_kst.strftime("%Y-%m-%d %H:%M:%S (KST)")}
    if SHOW_DB_BADGE:
        st.markdown("""
<style>
.db-badge{position:fixed;top:8px;right:10px;z-index:9999;font-size:12px;color:#0a0;background:#f6fff6;border:1px solid #dfe3e8;border-radius:12px;padding:4px 8px;box-shadow:0 1px 2px rgba(0,0,0,.04);}
.db-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#28a745;vertical-align:middle;margin-right:6px;}
@media (max-width:768px){.db-badge{top:6px;right:8px;font-size:11px;}}
</style>
<div class="db-badge"><span class="db-dot"></span>DB OK</div>
""", unsafe_allow_html=True)
except Exception as e:
    st.error("DB 연결 실패")
    st.exception(e)
    st.stop()

# ====================================================================================
# Secrets(Drive) 읽기  (URL/ID 모두 허용)
# ====================================================================================
_raw_api_key = (st.secrets.get("DRIVE_API_KEY") or os.getenv("DRIVE_API_KEY") or "").strip()
_raw_folder  = (st.secrets.get("DRIVE_FOLDER_ID") or os.getenv("DRIVE_FOLDER_ID") or "").strip()
DRIVE_API_KEY   = _raw_api_key
DRIVE_FOLDER_ID = _extract_drive_id(_raw_folder)

# ====================================================================================
# 로그인 직후 상단: 단일 버튼 "데이터 전체 동기화"
#  - 박스 전체가 버튼으로 동작 (Main + QnA + PDF 인덱스)
# ====================================================================================
with st.container():
    st.markdown("""
<style>
.sync-wrap{border:1px solid #e9ecef;border-radius:12px;padding:12px;background:#fff;margin:8px 0;}
/* 버튼 스타일 */
.sync-btn .stButton{margin:0;}
.sync-btn .stButton>button{
  padding:14px 18px;border:1px solid #e9ecef;border-radius:10px;background:#fff;
  font-size:15px;font-weight:800;box-shadow:none;width:100%;
}
.sync-btn .stButton>button:hover{background:#f8f9fa;border-color:#dee2e6;}
/* 버튼 바로 아래 안내문 */
.sync-info{color:#6c757d;font-size:12px;line-height:1.4;margin-top:6px;}
</style>
    """, unsafe_allow_html=True)

    st.markdown('<div class="sync-wrap">', unsafe_allow_html=True)

    # 버튼 (제목 텍스트 없음)
    st.markdown('<div class="sync-btn">', unsafe_allow_html=True)
    run_all = st.button("데이터 전체 동기화", key="btn_sync_all_in_one", use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    # 버튼 바로 아래 안내문
    st.markdown(
        '<div class="sync-info">한 번 누르면 Main+QnA 동기화, '
        'PDF 키가 있으면 인덱싱까지 수행합니다.</div>',
        unsafe_allow_html=True
    )

    st.markdown('</div>', unsafe_allow_html=True)  # .sync-wrap

    # 실행
    if run_all:
        try:
            with st.spinner("동기화 중..."):
                c_main, c_qna, c_pdf = _do_sync_all(include_pdf=True)
            st.success(f"완료: Main {c_main:,} · QnA {c_qna:,} · PDF {c_pdf:,}")
            st.rerun()
        except Exception as e:
            if e.__class__.__name__ in ("RerunData", "RerunException"):
                raise
            st.error(f"동기화 실패: {e}")

# ====================================================================================
# 탭 UI
# ====================================================================================
tab_main, tab_qna, tab_pdf = st.tabs(["인증기준/조사지침", "조사위원 질문", "규정검색(PDF파일/본문)"])

# ====================================================================================
# 인증기준/조사지침 (필터 포함)
# ====================================================================================
with tab_main:
    st.write("")  # 큰 제목 제거
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 1) 사용할 테이블(뷰) 우선순위: main_sheet_v → main_v → main_raw
    main_table = _pick_table(eng, ["main_sheet_v", "main_v", "main_raw"]) or "main_raw"

    # 2) 고정 표시 순서(구글시트 원본 순서)
    MAIN_COLS = [
        "ME", "조사항목", "항목", "등급", "조사결과",
        "조사기준의 이해", "조사방법1", "조사방법2", "조사장소", "조사대상",
    ]
    # (표형) 열 너비 비율
    MAIN_COL_WEIGHTS = {
        "ME": 2, "조사항목": 8, "항목": 1, "등급": 1, "조사결과": 2,
        "조사기준의 이해": 12, "조사방법1": 10, "조사방법2": 5,
        "조사장소": 4, "조사대상": 4,
    }

    existing_cols = _list_columns(eng, main_table)
    show_cols = [c for c in MAIN_COLS if c in existing_cols]
    has_sort = all(x in existing_cols for x in ["sort1", "sort2", "sort3"])

    # ====== 검색 입력 (Enter 제출) ======
    with st.form("main_search_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            kw = st.text_input("키워드 (공백=AND)", st.session_state.get("main_kw", ""), key="main_kw")
        with c2:
            f_place = st.text_input("조사장소 (선택)", st.session_state.get("main_filter_place", ""), key="main_filter_place")
        with c3:
            f_target = st.text_input("조사대상 (선택)", st.session_state.get("main_filter_target", ""), key="main_filter_target")

        FIXED_LIMIT = 1000
        submitted_main = st.form_submit_button("검색")

    # ====== 검색 실행 ======
    results_df = pd.DataFrame()
    if submitted_main and (kw.strip() or f_place.strip() or f_target.strip()):
        kw_list = [k.strip() for k in kw.split() if k.strip()]
        where_parts, params = [], {}

        if kw_list and show_cols:
            for i, token in enumerate(kw_list):
                ors = " OR ".join([f'"{c}" ILIKE :kw{i}' for c in show_cols])
                where_parts.append(f"({ors})")
                params[f"kw{i}"] = f"%{token}%"

        if f_place.strip() and "조사장소" in existing_cols:
            where_parts.append('"조사장소" ILIKE :place')
            params["place"] = f"%{f_place.strip()}%"
        if f_target.strip() and "조사대상" in existing_cols:
            where_parts.append('"조사대상" ILIKE :target')
            params["target"] = f"%{f_target.strip()}%"

        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
        select_cols_sql = ", ".join([f'"{c}"' for c in show_cols])
        order_sql = "ORDER BY sort1, sort2, sort3" if has_sort else ""
        sql = text(f"""
            SELECT {select_cols_sql}
            FROM "{main_table}"
            WHERE {where_sql}
            {order_sql}
            LIMIT :lim
        """)
        params["lim"] = FIXED_LIMIT
        with eng.begin() as con:
            results_df = pd.read_sql_query(sql, con, params=params)

        if results_df.empty:
            st.info("결과 없음")
            st.session_state.pop("main_results", None)
        else:
            st.session_state["main_results"] = results_df.to_dict("records")

    # ====== 스타일(하이라이트 & 표형 기본 CSS) ======
    st.markdown("""
<style>
.hl-item{ color:#0d47a1; font-weight:800; }          /* 조사항목 파랑 굵게 */
.hl-required{ color:#b10000; font-weight:800; }      /* 등급=필수 빨강 굵게 */

/* 카드 */
.card{border:1px solid #e9ecef;border-radius:10px;padding:12px 14px;margin:8px 0;background:#fff}
.card h4{margin:0 0 8px 0;font-size:16px;line-height:1.3;word-break:break-word}
.card .row{margin:4px 0;font-size:13px;color:#333;word-break:break-word}
.card .lbl{display:inline-block;min-width:110px;color:#6c757d}

/* 표형 — colgroup 비율을 적용하기 위해 고정 레이아웃 사용 */
.table-wrap{ overflow-x:auto; }
.table-wrap table{
  width:100%; border-collapse:collapse; background:#fff; table-layout:fixed;
}
.table-wrap th, .table-wrap td{
  border:1px solid #e9ecef; padding:8px 10px; text-align:left; vertical-align:top;
  font-size:13px; line-height:1.45;
  white-space:normal; word-break:keep-all; overflow-wrap:anywhere;
}
.table-wrap th{ background:#f8f9fa; font-weight:700; }

/* 반응형 축소 */
@media (max-width: 1200px){
  .table-wrap th, .table-wrap td{ font-size:12px; padding:6px 8px; }
}
</style>
    """, unsafe_allow_html=True)

    # ====== 셀 포맷 ======
    def _fmt_cell(colname: str, value) -> str:
        s = html.escape("" if value is None else str(value))
        def _is_required(val: str) -> bool:
            t = (val or "").strip().replace(" ", "").lower()
            return t in ("필수", "必須")
        if colname == "조사항목" and s:
            return f'<span class="hl-item">{s}</span>'
        if colname == "등급" and _is_required(s):
            return f'<span class="hl-required">{s}</span>'
        return s

    # ====== 카드 렌더러(고정 순서) ======
    def render_cards(df_: pd.DataFrame, cols_order: list[str]):
        for _, r in df_.iterrows():
            title_html = _fmt_cell("조사항목", r.get("조사항목"))
            rows_html = []
            for c in cols_order:
                v_html = _fmt_cell(c, r.get(c))
                rows_html.append(f'<div class="row"><span class="lbl">{html.escape(str(c))}</span> {v_html}</div>')
            st.markdown(f'<div class="card"><h4>{title_html or "-"}</h4>' + "".join(rows_html) + '</div>', unsafe_allow_html=True)

    # ====== (표형) colgroup 생성 유틸 ======
    def _build_colgroup(cols, weights):
        w = [float(weights.get(str(c), 1)) for c in cols]
        tot = sum(w) or 1.0
        return "<colgroup>" + "".join(f'<col style="width:{(x/tot)*100:.3f}%">' for x in w) + "</colgroup>"

    # ====== 표형 렌더러(고정 순서 + 가중치 비율) ======
    def render_table(df_: pd.DataFrame, cols_order: list[str]):
        colgroup_html = _build_colgroup(cols_order, MAIN_COL_WEIGHTS)
        header_cells = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols_order)
        body_rows = []
        for _, r in df_.iterrows():
            cells = "".join(f"<td>{_fmt_cell(c, r.get(c))}</td>" for c in cols_order)
            body_rows.append(f"<tr>{cells}</tr>")
        st.markdown(
            f"""
<div class="table-wrap">
  <table>
    {colgroup_html}
    <thead><tr>{header_cells}</tr></thead>
    <tbody>
      {''.join(body_rows)}
    </tbody>
  </table>
</div>
""",
            unsafe_allow_html=True
        )

    # ====== 결과 출력 ======
    if "main_results" in st.session_state and st.session_state["main_results"]:
        df = pd.DataFrame(st.session_state["main_results"])
        cols_order = [c for c in MAIN_COLS if c in df.columns]  # 고정 순서

        st.write(f"결과: {len(df):,}건")
        view_mode = st.radio("보기 형식", ["카드형(모바일)", "표형"], index=0, horizontal=True, key="main_view_mode")

        if view_mode == "표형":
            render_table(df, cols_order)
        else:
            render_cards(df, cols_order)
    else:
        st.caption("힌트: 조사장소/조사대상은 메인 키워드와 AND 조건으로 결합되어 검색됩니다.")

# ====================================================================================
# 조사위원 질문
# ====================================================================================
with tab_qna:
    st.write("")  # 큰 제목 생략
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 뷰가 있으면 우선, 없으면 원본 테이블
    qna_table = _pick_table(eng, ["qna_sheet_v", "qna_v", "qna_raw"]) or "qna_raw"

    # ====== 입력폼 (Enter 제출) ======
    with st.form("qna_search_form", clear_on_submit=False):
        kw_q = st.text_input(
            "키워드 (공백=AND)",
            st.session_state.get("qna_kw", ""),
            key="qna_kw",
            placeholder="예) 낙상, 환자확인, 고객, 수술 체크리스트 등"
        )
        FIXED_LIMIT_QNA = 1000
        submitted_qna = st.form_submit_button("검색")  # 화면엔 숨김

    # ====== 검색 실행 ======
    if submitted_qna and kw_q.strip():
        with st.spinner("검색 중..."):
            df_q = search_table_any(eng, qna_table, kw_q, columns=None, limit=FIXED_LIMIT_QNA)
        if df_q.empty:
            st.info("결과 없음")
            st.session_state.pop("qna_results", None)
        else:
            st.session_state["qna_results"] = df_q.to_dict("records")

    # ===== 유틸: 컬럼 이름 느슨하게 매칭 + 최후보루 =====
    import html as _html

    def _norm_col(s: str) -> str:
        """공백/기호 제거 + 소문자 (한글은 그대로 유지)"""
        s = str(s or "")
        s = re.sub(r"[ \t\r\n/_\-:;.,(){}\[\]<>·•｜|]+", "", s)
        return s.lower()

    def _pick_col(cols: list[str], candidates: list[str]) -> str | None:
        """정확 일치 → 정규화 일치 → 부분 포함(정규화) 순으로 매칭"""
        # 1) 정확
        for w in candidates:
            if w in cols:
                return w
        # 2) 정규화 일치
        wants = [_norm_col(w) for w in candidates]
        for c in cols:
            if _norm_col(c) in wants:
                return c
        # 3) 부분 포함
        for w in wants:
            for c in cols:
                if w and w in _norm_col(c):
                    return c
        return None

    def _guess_long_text_col(df: pd.DataFrame, exclude: set[str]) -> str | None:
        """숫자/번호/정렬키를 제외하고 '평균 글자수'가 가장 긴 컬럼 추정"""
        cand = [c for c in df.columns if c not in exclude]
        if not cand:
            return None
        samp = df.head(50)
        best_col, best_len = None, -1.0
        for c in cand:
            try:
                vals = samp[c].astype(str)
            except Exception:
                vals = samp[c].map(lambda x: "" if x is None else str(x))
            lens = []
            for s in vals:
                s = ("" if s is None else str(s)).strip()
                if not s or re.fullmatch(r"\d{1,4}([./-]\d{1,2}([./-]\d{1,2})?)?$", s):  # 날짜/숫자
                    lens.append(0)
                else:
                    lens.append(len(s))
            avg = (sum(lens) / max(1, len(lens)))
            if avg > best_len:
                best_col, best_len = c, avg
        return best_col

    # ===== 카드 렌더러 (라벨 최소화) =====
    def render_qna_cards(df_: pd.DataFrame):
        st.markdown("""
<style>
.qcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.qtitle{font-size:15px;font-weight:800;margin-bottom:6px;word-break:break-word;color:#0d47a1}
.qbody{font-size:13px;color:#333;word-break:break-word}
</style>
        """, unsafe_allow_html=True)

        cols = list(df_.columns)

        # 장소/내용 컬럼 후보 (다양한 변형 대비)
        PLACE_CAND = ["조사장소", "장소", "부서/장소", "부서", "조사 장소", "조사 부서"]
        CONTENT_CAND = [
            "조사위원 질문(확인) 내용", "조사위원 질문(확인)내용",
            "조사위원 질문, 확인내용", "질문(확인) 내용",
            "질문/확인내용", "질문 확인내용", "조사위원 질문", "확인내용"
        ]

        place_col = _pick_col(cols, PLACE_CAND)
        content_col = _pick_col(cols, CONTENT_CAND)

        # 내용 컬럼을 못 찾으면: '가장 긴 텍스트' 컬럼을 추정
        exclude = set([place_col, "No", "no", "번호", "순번", "sort1", "sort2", "sort3"])
        content_col = content_col or _guess_long_text_col(df_, exclude)

        for _, r in df_.iterrows():
            # ---- 장소
            place = r.get(place_col, "") if place_col else ""
            place = "" if pd.isna(place) else str(place).strip()
            if not place:
                place = "조사장소 미지정"

            # ---- 내용 (우선: content_col → 최후: 행 전체에서 가장 긴 값)
            content = r.get(content_col, "") if content_col else ""
            content = "" if pd.isna(content) else str(content).strip()
            if not content:
                best_val, best_len = "", -1
                for c in cols:
                    if c in exclude:  # 번호/정렬/장소 제외
                        continue
                    v = r.get(c, "")
                    if pd.isna(v):
                        continue
                    s = str(v).strip()
                    if len(s) > best_len:
                        best_val, best_len = s, len(s)
                content = best_val

            st.markdown(
                f"""
<div class="qcard">
  <div class="qtitle">{_html.escape(place)}</div>
  <div class="qbody">{_html.escape(content) if content else "-"}</div>
</div>
                """,
                unsafe_allow_html=True
            )

    # ===== 결과 표시 =====
    if "qna_results" in st.session_state and st.session_state["qna_results"]:
        df = pd.DataFrame(st.session_state["qna_results"])
        st.write(f"결과: {len(df):,}건")
        render_qna_cards(df)
    else:
        st.caption("키워드를 입력하고 **Enter** 를 누르면 결과가 표시됩니다.")

# ====================================================================================
# 규정검색(PDF파일/본문) (Google Drive 전용)
# ====================================================================================
with tab_pdf:
    st.write("")  # 큰 제목 제거
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 검색 조건 (Enter 제출 폼)
    FIXED_LIMIT = 1000
    with st.form("pdf_search_form", clear_on_submit=False):
        kw_pdf  = st.text_input("키워드 (공백=AND)", "", key="pdf_kw")
        fn_like = st.text_input("파일명 필터(선택)", "", key="pdf_fn")
        submitted_pdf = st.form_submit_button("검색")

    # 제출 시 검색 → 세션 저장
    if submitted_pdf and kw_pdf.strip():
        with st.spinner("검색 중..."):
            _df = search_regs(eng, kw_pdf, filename_like=fn_like, limit=FIXED_LIMIT)
        if "me" in _df.columns:
            _df = _df[_df["me"].astype(str).str.strip() != ""]
        _df = _df.sort_values(["filename", "page"], kind="stable").reset_index(drop=True)

        if _df.empty:
            st.info("조건에 맞는 결과가 없습니다. 상단의 **데이터 전체 동기화** 버튼으로 PDF 인덱싱을 먼저 실행하세요.")
            st.session_state.pop("pdf_results", None)
            st.session_state.pop("pdf_sel_idx", None)
            st.session_state.pop("pdf_kw_list", None)
        else:
            st.session_state["pdf_results"] = _df.to_dict("records")
            st.session_state["pdf_sel_idx"] = 0
            st.session_state["pdf_kw_list"] = [k.strip() for k in kw_pdf.split() if k.strip()]

    # 결과 렌더링
    if "pdf_results" in st.session_state and st.session_state["pdf_results"]:
        df = pd.DataFrame(st.session_state["pdf_results"])
        kw_list = st.session_state.get("pdf_kw_list", [])
        st.write(f"결과: {len(df):,}건")

        def view_url(fid: str, page: int) -> str:
            fid = (fid or "").strip()
            return f"https://drive.google.com/file/d/{fid}/view#page={int(page)}"

        # 보기 형식 선택
        view_mode_pdf = st.radio("보기 형식", ["카드형(모바일)", "표형(간단)"], horizontal=True, key="pdf_view_mode")

        if view_mode_pdf.endswith("간단)"):
            st.caption("파일명/페이지 버튼을 클릭하면 아래 미리보기가 바뀝니다.")
            hdr = st.columns([7, 1, 1])
            hdr[0].markdown("**파일명**")
            hdr[1].markdown("**페이지**")
            hdr[2].markdown("**열기**")

            if "pdf_sel_idx" not in st.session_state:
                st.session_state["pdf_sel_idx"] = 0

            for i, row in df.iterrows():
                c1, c2, c3 = st.columns([7, 1, 1])
                if c1.button(str(row["filename"]), key=f"pick_file_{i}"):
                    st.session_state["pdf_sel_idx"] = int(i)
                if c2.button(str(int(row["page"])), key=f"pick_page_{i}"):
                    st.session_state["pdf_sel_idx"] = int(i)
                c3.markdown(
                    f'<a href="{view_url(row["me"], int(row["page"]))}" target="_blank" rel="noopener noreferrer" '
                    f'style="display:inline-block;padding:6px 12px;border:1px solid #ddd;border-radius:8px;'
                    f'background:#f8f9fa;text-decoration:none;color:#0d6efd;font-weight:600;">열기</a>',
                    unsafe_allow_html=True
                )
        else:
            st.markdown("""
<style>
.pcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.pcard .title{font-size:15px;font-weight:700;margin-bottom:10px;word-break:break-all}
.pbtn{display:inline-block;padding:8px 12px;border:1px solid #dee2e6;border-radius:10px;background:#f8f9fa;
      text-decoration:none;color:#0d6efd;font-weight:600}
.pbtn + .pbtn{margin-left:8px}
.pmeta{font-size:12px;color:#6c757d;margin-top:6px}
.rowbtn{display:flex;gap:8px;flex-wrap:wrap}
.rowbtn .stButton>button{width:100%}
</style>
            """, unsafe_allow_html=True)

            if "pdf_sel_idx" not in st.session_state:
                st.session_state["pdf_sel_idx"] = 0

            import html as _html
            for i, row in df.iterrows():
                fid_i = (row.get("me") or "").strip()
                fname = _html.escape(str(row["filename"]))
                pagei = int(row["page"])
                col1, = st.columns(1)
                with col1:
                    st.markdown(f'<div class="pcard"><div class="title">{fname}</div>', unsafe_allow_html=True)
                    if st.button("이 파일 미리보기", key=f"pick_file_card_{i}", use_container_width=True):
                        st.session_state["pdf_sel_idx"] = int(i)
                    cA, cB = st.columns(2)
                    with cA:
                        if st.button(f"페이지 {pagei}", key=f"pick_page_card_{i}", use_container_width=True):
                            st.session_state["pdf_sel_idx"] = int(i)
                    with cB:
                        st.markdown(
                            f'<a class="pbtn" href="{view_url(fid_i, pagei)}" target="_blank" rel="noopener noreferrer" '
                            f'style="display:block;text-align:center;padding:9px 12px;">열기</a>',
                            unsafe_allow_html=True
                        )
                    st.markdown(f'<div class="pmeta">file_id: {fid_i or "-"}</div></div>', unsafe_allow_html=True)

        # ---- 선택된 행으로 미리보기 (pdf.js 렌더)
        sel_idx = int(st.session_state.get("pdf_sel_idx", 0))
        sel_idx = max(0, min(sel_idx, len(df) - 1))
        sel = df.iloc[sel_idx]
        fid = (sel.get("me") or "").strip()
        sel_file = sel["filename"]
        sel_page = int(sel["page"])

        st.caption("텍스트 미리보기 & 문서 보기 (선택한 1건)")
        st.write(f"**파일**: {sel_file}  |  **페이지**: {sel_page}  |  **file_id**: {fid or '-'}")
        st.markdown(highlight_html(sel["text"], kw_list, width=200), unsafe_allow_html=True)

        # ---- PDF 바이트 캐시 + 내려받기
        import base64
        cache = st.session_state.setdefault("pdf_cache", {})
        b64 = cache.get(fid)
        if not b64:
            pdf_bytes = _drive_download_pdf(fid, DRIVE_API_KEY)
            b64 = base64.b64encode(pdf_bytes).decode("ascii")
            cache[fid] = b64

        # ---- 미리보기 컨트롤
        page_view = st.number_input("미리보기 페이지", 1, 9999, int(sel_page), step=1, key=f"pv_page_{fid}")
        zoom_pct  = st.slider("줌(%)", 30, 200, 80, step=5, key=f"pv_zoom_{fid}")
        height_px = st.slider("미리보기 높이(px)", 480, 1200, 640, step=40, key=f"pv_h_{fid}")

        # ---- pdf.js 렌더
        max_fit_width = 900
        viewer_html = f"""
<div id="pdf-root" style="width:100%;height:{height_px}px;max-height:80vh;background:#fafafa;overflow:auto;">
  <canvas id="pdf-canvas" style="display:block;margin:0 auto;background:#fff;box-shadow:0 0 4px rgba(0,0,0,0.08)"></canvas>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>
<script>
  if (window['pdfjsLib']) {{
    pdfjsLib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  }}
  function b64ToUint8Array(b64) {{
    const raw = atob(b64), len = raw.length;
    const arr = new Uint8Array(len);
    for (let i=0; i<len; i++) arr[i] = raw.charCodeAt(i);
    return arr;
  }}
  const pdfData    = b64ToUint8Array("{b64}");
  const targetPage = {int(page_view)};
  const sliderZoom = {float(zoom_pct)}/100.0;
  const maxFitW    = {int(max_fit_width)};
  pdfjsLib.getDocument({{ data: pdfData }}).promise.then(function(pdf) {{
    const pageNo = Math.min(Math.max(1, targetPage), pdf.numPages);
    return pdf.getPage(pageNo).then(function(page) {{
      const container = document.getElementById('pdf-root');
      const canvas = document.getElementById('pdf-canvas');
      const ctx = canvas.getContext('2d');
      const initialViewport = page.getViewport({{scale: 1}});
      const fitWidth = Math.min(container.clientWidth, maxFitW);
      const isPhone = fitWidth < 520;
      const mobileBoost = isPhone ? 1.15 : 1.0;
      const fitScale = fitWidth / initialViewport.width;
      const finalScale = fitScale * sliderZoom * mobileBoost;
      const viewport = page.getViewport({{scale: finalScale}});
      const dpr = (window.devicePixelRatio || 1);
      canvas.width  = Math.floor(viewport.width  * dpr);
      canvas.height = Math.floor(viewport.height * dpr);
      canvas.style.width  = Math.floor(viewport.width) + 'px';
      canvas.style.height = Math.floor(viewport.height) + 'px';
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      page.render({{ canvasContext: ctx, viewport: viewport }});
    }});
  }}).catch(function(e) {{
    const el = document.getElementById('pdf-root');
    el.innerHTML = '<div style="padding:16px;color:#d6336c;">PDF 미리보기를 표시할 수 없습니다.</div>';
    console.error(e);
  }});
</script>
"""
        st.components.v1.html(viewer_html, height=height_px + 40)
    else:
        st.caption("먼저 키워드를 입력하고 **Enter**를 누르세요. (PDF 인덱스가 필요하다면 상단의 **데이터 전체 동기화** 버튼을 사용하세요.)")



