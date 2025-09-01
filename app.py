# -*- coding: utf-8 -*-
import os, re, io, html, time, requests, urllib.parse, base64
from pathlib import Path
from typing import List, Dict

import pandas as pd
import streamlit as st
from sqlalchemy import text, create_engine
from pypdf import PdfReader
from datetime import timezone, timedelta, datetime

# =========================
# 기본 설정 & 전역 스타일(여백 타이트)
# =========================
st.set_page_config(page_title="★★★ HISMEDI 인증 ★★★", layout="wide")
st.markdown("""
<style>
/* 상단 여백: 제목이 가려지지 않도록 넉넉히 확보 (+ iOS safe-area 반영) */
section.main > div.block-container{
  padding-top: calc(env(safe-area-inset-top, 0px) + 56px);
  padding-bottom: 40px;
}
@media (max-width:768px){
  section.main > div.block-container{
    padding-top: calc(env(safe-area-inset-top, 0px) + 64px);
  }
}

/* 요소 간 간격은 타이트 유지 */
div[data-testid="stVerticalBlock"]{gap:.6rem;}
div[data-testid="stHorizontalBlock"]{gap:.6rem;}
h1, h2, h3, h4, h5, h6{margin:.2rem 0 .6rem 0}

/* 스크롤 시 앵커가려짐 방지(혹시 모를 내부 링크용) */
h1, h2, h3, .main-title{ scroll-margin-top: 80px; }

/* 제목 */
.main-title{
  font-weight:800; font-size:26px; line-height:1.25;
  margin:4px 0 8px; color:#111;
}
@media (max-width:768px){ .main-title{font-size:22px} }

/* 동기화 버튼 */
.stButton > button.sync-all{
  width:100%; border:1px solid #ffd5d5; border-radius:12px;
  background:#fff; color:#d6336c; font-weight:800; padding:10px 12px;
}
.stButton > button.sync-all:hover{background:#fff5f5;border-color:#ffb3b3}

/* 카드/표 공통 */
.card{border:1px solid #e9ecef;border-radius:10px;padding:12px 14px;margin:8px 0;background:#fff}
.card h4{margin:0 0 8px 0;font-size:16px;line-height:1.3;word-break:break-word}
.card .row{margin:4px 0;font-size:13px;color:#333;word-break:break-word}
.card .lbl{display:inline-block;min-width:110px;color:#6c757d}

/* 표형 */
.table-wrap{overflow-x:auto;}
.table-wrap table{width:100%;border-collapse:collapse;background:#fff;table-layout:fixed;}
.table-wrap th, .table-wrap td{
  border:1px solid #e9ecef;padding:8px 10px;text-align:left;vertical-align:top;
  font-size:13px;line-height:1.45;white-space:normal;word-break:keep-all;overflow-wrap:anywhere;
}
.table-wrap th{background:#f8f9fa;font-weight:700}
@media (max-width:1200px){
  .table-wrap th, .table-wrap td{font-size:12px;padding:6px 8px}
}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-title">HISMEDI 인증</div>', unsafe_allow_html=True)

# =========================
# Edge Function 즉시 동기화
# =========================
SUPABASE_FUNC_BASE = (st.secrets.get("SUPABASE_FUNC_BASE") or os.getenv("SUPABASE_FUNC_BASE") or "").rstrip("/")
SUPABASE_ANON_KEY  = (st.secrets.get("SUPABASE_ANON_KEY")  or os.getenv("SUPABASE_ANON_KEY")  or "").strip()

def _trigger_edge_func(slug: str) -> dict:
    if not SUPABASE_FUNC_BASE or not SUPABASE_ANON_KEY:
        raise RuntimeError("SUPABASE_FUNC_BASE / SUPABASE_ANON_KEY 시크릿이 필요합니다.")
    r = requests.post(
        f"{SUPABASE_FUNC_BASE}/{slug}",
        headers={"Authorization": f"Bearer {SUPABASE_ANON_KEY}", "Content-Type": "application/json"},
        json={}, timeout=60,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"{slug} 호출 실패 {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        return {"ok": True, "raw": r.text}

# =========================
# 간단 접근 비밀번호(선택)
# =========================
_APP_PW = (st.secrets.get("APP_PASSWORD") or os.getenv("APP_PASSWORD") or "").strip()
def _is_valid_pw_format(pw: str) -> bool: return bool(re.fullmatch(r"\d{8}", pw or ""))

if _APP_PW:
    if not _is_valid_pw_format(_APP_PW):
        st.error("APP_PASSWORD 는 8자리 숫자여야 합니다. (예: 12345678)")
        st.stop()
    if not st.session_state.get("pw_ok", False):
        with st.form("pw_gate", clear_on_submit=False):
            pw = st.text_input("접속 비밀번호 (8자리 숫자)", type="password", max_chars=8, placeholder="예: 12345678")
            ok = st.form_submit_button("확인")
        if ok:
            if not _is_valid_pw_format(pw):
                st.error("8자리 숫자만 입력하세요."); st.stop()
            if pw == _APP_PW:
                st.session_state["pw_ok"] = True; st.rerun()
            else:
                st.error("비밀번호가 틀렸습니다."); st.stop()
        else:
            st.stop()

# ===========
# DB 연결 유틸
# ===========
def _load_database_url() -> str:
    url = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url: st.error("DATABASE_URL 시크릿이 없습니다."); st.stop()
    return str(url).strip()

def _ensure_psycopg_url(url: str) -> str:
    u = url
    if u.startswith("postgresql://"): u = u.replace("postgresql://", "postgresql+psycopg://", 1)
    if u.startswith("postgres://"):  u = u.replace("postgres://", "postgresql+psycopg://", 1)
    if "sslmode=" not in u: u += ("&" if ("?" in u) else "?") + "sslmode=require"
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
        if _table_exists(eng, t): return t
    return None

def _qident(name: str) -> str: return '"' + str(name).replace('"', '""') + '"'

MAIN_DEFAULT_SEARCH_COLS = ["조사장소", "조사항목", "세부항목", "기준문구", "확인방법", "근거", "비고"]
QNA_DEFAULT_SEARCH_COLS  = ["조사위원 질문, 확인내용", "조사위원 질문", "확인내용", "조사장소"]

def _choose_search_cols(eng, table: str) -> List[str]:
    all_cols = _list_columns(eng, table); low = table.lower()
    if low.startswith("main"):
        pref = [c for c in MAIN_DEFAULT_SEARCH_COLS if c in all_cols]
    elif low.startswith("qna"):
        pref = [c for c in QNA_DEFAULT_SEARCH_COLS if c in all_cols]
    else:
        pref = []
    return pref if pref else all_cols

def search_table_any(
    eng,
    table: str,
    keywords: str,
    columns=None,          # 선택: SELECT 에서 보여줄 컬럼. None이면 전체(*)
    limit: int = 500
):
    """
    공백으로 구분된 키워드(AND)를, 여러 컬럼(OR)에 대해 부분일치(ILIKE) 검색합니다.
    **키워드가 비어 있으면 전체 조회( LIMIT 적용 )**.
    """
    kw_list = [w for w in re.split(r"\s+", (keywords or "").strip()) if w]

    # SELECT 절
    select_cols = "*"
    if columns:
        select_cols = ", ".join(_qident(c) for c in columns)

    # 키워드가 없으면 전체 조회
    if not kw_list:
        sql = text(f"SELECT {select_cols} FROM {_qident(table)} LIMIT :limit")
        with eng.begin() as con:
            return pd.read_sql_query(sql, con, params={"limit": int(limit)})

    # WHERE 절(AND x OR) + 파라미터
    search_cols = _choose_search_cols(eng, table)
    params: dict[str, str] = {}
    and_parts = []
    for i, kw in enumerate(kw_list):
        or_parts = []
        for j, col in enumerate(search_cols):
            p = f"kw_{i}_{j}"
            # 모든 타입 안전: ::text 로 캐스팅 + NULL 방지
            or_parts.append(f"COALESCE({_qident(col)}::text, '') ILIKE :{p}")
            params[p] = f"%{kw}%"
        and_parts.append("(" + " OR ".join(or_parts) + ")")
    where_clause = " AND ".join(and_parts)

    sql = text(f"""
        SELECT {select_cols}
        FROM {_qident(table)}
        WHERE {where_clause}
        LIMIT :limit
    """)
    params["limit"] = int(limit)

    with eng.begin() as con:
        return pd.read_sql_query(sql, con, params=params)

def run_select_query(eng, sql_text, params=None):
    q = (sql_text or "").strip().rstrip(";")
    if not re.match(r"^(select|with|explain)\b", q, re.I):
        raise ValueError("SELECT/WITH/EXPLAIN만 실행할 수 있습니다.")
    with eng.begin() as con:
        return pd.read_sql_query(text(q), con, params=params or {})

# ==========================
# PDF 인덱싱/검색 (Drive 전용)
# ==========================
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
            if set(REQUIRED_REG_COLUMNS) - set(cols): recreate = True
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

def _clean_text(s: str) -> str: return re.sub(r"\s+", " ", s or "").strip()

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
            data = r.json(); batch = data.get("files", [])
            files.extend(batch)
            for f in batch:
                if f.get("mimeType") == "application/vnd.google-apps.folder":
                    list_children(f["id"])
            page_token = data.get("nextPageToken")
            if not page_token: break
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
    indexed = skipped = errors = 0; done_files = []
    with eng.begin() as con:
        for rel, fid in rel_to_id.items():
            try:
                row = con.execute(text("select count(*) from regulations where filename=:fn"), {"fn": rel}).scalar()
                if row and row > 0: skipped += 1; done_files.append((rel, "skip")); continue
                con.execute(text("delete from regulations where filename=:fn"), {"fn": rel})
                reader = PdfReader(io.BytesIO(_drive_download_pdf(fid, api_key)))
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
            if limit_files and indexed >= limit_files: break
    return {"indexed": indexed, "skipped": skipped, "errors": errors, "files": done_files}

def make_snippet(text_: str, kw_list: List[str], width: int = 160) -> str:
    if not text_: return ""
    low, pos, hit = text_.lower(), -1, ""
    for k in kw_list:
        i = low.find(k.lower())
        if i != -1 and (pos == -1 or i < pos): pos, hit = i, k.lower()
    if pos == -1: return text_[:width] + ("..." if len(text_) > width else "")
    start = max(0, pos - width // 2); end = min(len(text_), pos + len(hit) + width // 2)
    return ("..." if start > 0 else "") + text_[start:end] + ("..." if end < len(text_) else "")

def highlight_html(src_text: str, kw_list: List[str], width: int = 200) -> str:
    esc = html.escape(make_snippet(src_text, kw_list, width=width))
    for k in sorted({k for k in kw_list if k}, key=len, reverse=True):
        esc = re.compile(re.escape(k), re.IGNORECASE).sub(lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def search_regs(eng, keywords: str, filename_like: str = "", limit: int = 500, hide_ipynb_chk: bool = True):
    """PDF 본문 검색. 키워드가 비어 있으면 전체 조회(옵션 필터만 적용)."""
    kw_list = [k.strip() for k in str(keywords or "").split() if k.strip()]

    where_parts, params = [], {}

    # (A) 키워드 AND
    if kw_list:
        for i, kw in enumerate(kw_list):
            where_parts.append(f"(text ILIKE :kw{i})")
            params[f"kw{i}"] = f"%{kw}%"

    # (B) 파일명 필터(선택)
    if filename_like.strip():
        where_parts.append("filename ILIKE :fn")
        params["fn"] = f"%{filename_like.strip()}%"

    # (C) .ipynb_checkpoints 숨기기
    if hide_ipynb_chk:
        where_parts.append(r"(filename !~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)')")

    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
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

# ============
# DB 연결 확인
# ============
eng = get_engine()
try:
    with eng.begin() as con:
        user, port, now_utc = con.execute(text("select current_user, inet_server_port(), now()")).one()
except Exception as e:
    st.error("DB 연결 실패"); st.exception(e); st.stop()

# ------------------------------------------------------------
# Drive 시크릿 (URL/ID 모두 허용)
# ------------------------------------------------------------
def _extract_drive_id(value: str) -> str:
    v = (value or "").strip()
    if not v: return ""
    if re.fullmatch(r"[A-Za-z0-9_\-]{20,}", v): return v
    try:
        parsed = urllib.parse.urlparse(v)
        m = re.search(r"/folders/([A-Za-z0-9_\-]{20,})", parsed.path) or re.search(r"/file/d/([A-Za-z0-9_\-]{20,})", parsed.path)
        if m: return m.group(1)
        qs = urllib.parse.parse_qs(parsed.query)
        if "id" in qs and qs["id"] and re.fullmatch(r"[A-Za-z0-9_\-]{20,}", qs["id"][0]): return qs["id"][0]
    except Exception:
        pass
    return re.sub(r"[^A-Za-z0-9_\-]", "", v)

DRIVE_API_KEY   = (st.secrets.get("DRIVE_API_KEY")   or os.getenv("DRIVE_API_KEY")   or "").strip()
DRIVE_FOLDER_ID = _extract_drive_id(st.secrets.get("DRIVE_FOLDER_ID") or os.getenv("DRIVE_FOLDER_ID") or "")

# ------------------------------------------------------------
# 상단: 한 번에 동기화 (Main+QnA+PDF) + 최근 동기화 표시
# ------------------------------------------------------------
show_sync_btn = (not _APP_PW) or st.session_state.get("pw_ok", False)  # 비번이 있으면 인증 후에만 노출
if show_sync_btn:
    clicked = st.button("데이터 전체 동기화", key="btn_sync_all_pdf", type="secondary",
                        help="Main+QnA 동기화, PDF 키가 있으면 인덱싱까지 수행합니다.")
    if clicked:
        try:
            # 1) Main + QnA
            r1 = _trigger_edge_func("sync_main"); cnt_main = int(r1.get("count", 0))
            r2 = _trigger_edge_func("sync_qna");  cnt_qna  = int(r2.get("count", 0))
            # 2) PDF (있을 때만)
            cnt_pdf = 0
            if DRIVE_API_KEY and DRIVE_FOLDER_ID:
                res = index_pdfs_from_drive(eng, DRIVE_FOLDER_ID, DRIVE_API_KEY)
                cnt_pdf = int(res.get("indexed", 0))
            # 캐시/세션 클리어
            st.cache_data.clear()
            for k in ("main_results","qna_results","pdf_results","pdf_sel_idx","pdf_kw_list"): st.session_state.pop(k, None)
            # 최근 동기화 시간/건수 저장
            st.session_state["last_sync_ts"] = time.time()
            st.session_state["last_sync_counts"] = {"main": cnt_main, "qna": cnt_qna, "pdf": cnt_pdf}
            st.success(f"완료: Main {cnt_main:,} · QnA {cnt_qna:,}" + (f" · PDF {cnt_pdf:,}" if cnt_pdf else ""))
            st.rerun()
        except Exception as e:
            if e.__class__.__name__ in ("RerunData","RerunException"): raise
            st.error(f"동기화 실패: {e}")

# 최근 동기화 안내(얇고 가까이)
def _fmt_ts(ts: float) -> str:
    try:
        kst = timezone(timedelta(hours=9))
        return datetime.fromtimestamp(ts, tz=kst).strftime("%Y-%m-%d %H:%M:%S (KST)")
    except Exception:
        return "-"

counts = st.session_state.get("last_sync_counts")
when   = st.session_state.get("last_sync_ts")
if counts and when:
    line = f"최근 동기화: Main {counts.get('main',0):,} · QnA {counts.get('qna',0):,}"
    if counts.get("pdf",0): line += f" · PDF {counts['pdf']:,}"
    line += f" · {_fmt_ts(when)}"
    st.caption(line)

# =====
# 탭 UI
# =====
tab_main, tab_qna, tab_pdf = st.tabs(["인증기준/조사지침", "조사위원 질문", "규정검색(PDF파일/본문)"])

# ========================== 인증기준/조사지침 탭 ==========================
with tab_main:
    # 큰 제목 여백 제거 + 폼 제출 버튼 숨김(Enter로 바로 검색)
    st.write("")
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 1) 사용할 테이블(뷰) 우선순위: main_sheet_v → main_v → main_raw
    main_table = _pick_table(eng, ["main_sheet_v", "main_v", "main_raw"]) or "main_raw"

    # 2) 고정 표시 순서(구글시트 원본 순서)
    MAIN_COLS = [
        "ME", "조사항목", "항목", "등급", "조사결과",
        "조사기준의 이해", "조사방법1", "조사방법2", "조사장소", "조사대상",
    ]

    # 표형 열 너비 비율
    MAIN_COL_WEIGHTS = {
        "ME": 2, "조사항목": 8, "항목": 1, "등급": 1, "조사결과": 2,
        "조사기준의 이해": 12, "조사방법1": 10, "조사방법2": 5,
        "조사장소": 4, "조사대상": 4,
    }

    # 테이블에 실제로 존재하는 컬럼/정렬키 확인
    existing_cols = _list_columns(eng, main_table)
    show_cols = [c for c in MAIN_COLS if c in existing_cols]
    has_sort = all(x in existing_cols for x in ["sort1", "sort2", "sort3"])

    # ====== 입력폼 (Enter 제출) ======
    with st.form("main_search_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            kw = st.text_input("키워드 (입력 없이 Enter=전체조회, 공백=AND)",
                               st.session_state.get("main_kw", ""), key="main_kw")
        with c2:
            f_place = st.text_input("조사장소 (선택)",
                                    st.session_state.get("main_filter_place", ""),
                                    key="main_filter_place",
                                    placeholder="예) 전 부서, 병동, 외래, 수술실, 검사실 등")
        with c3:
            f_target = st.text_input("조사대상 (선택)",
                                     st.session_state.get("main_filter_target", ""),
                                     key="main_filter_target",
                                     placeholder="예) 전 직원, 의사, 간호사, 의료기사, 원무 등")
        FIXED_LIMIT = 1000
        submitted_main = st.form_submit_button("검색")

    # ====== 검색 실행 ======
    results_df = pd.DataFrame()
    if submitted_main:  # 키워드 없이 Enter여도 전체 조회
        kw_list = [k.strip() for k in (kw or "").split() if k.strip()]
        where_parts, params = [], {}

        # 키워드(AND) → 각 키워드가 show_cols(OR) 중 하나에 매칭
        if kw_list and show_cols:
            for i, token in enumerate(kw_list):
                ors = " OR ".join([f'"{c}" ILIKE :kw{i}' for c in show_cols])
                where_parts.append(f"({ors})")
                params[f"kw{i}"] = f"%{token}%"

        # 조사장소/조사대상 개별 필터(선택)
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
            st.info("결과 없음 (키워드 없이 Enter=전체 조회)")
            st.session_state.pop("main_results", None)
        else:
            st.session_state["main_results"] = results_df.to_dict("records")

    # ====== 스타일(하이라이트 & 표형 기본 CSS) ======
    st.markdown("""
<style>
.hl-item{ color:#0d47a1; font-weight:800; }          /* 조사항목 파랑 굵게 */
.hl-required{ color:#b10000; font-weight:800; }      /* 등급=필수 빨강 굵게 */
.card{border:1px solid #e9ecef;border-radius:10px;padding:12px 14px;margin:8px 0;background:#fff}
.card h4{margin:0 0 8px 0;font-size:16px;line-height:1.3;word-break:break-word}
.card .row{margin:4px 0;font-size:13px;color:#333;word-break:break-word}
.card .lbl{display:inline-block;min-width:110px;color:#6c757d}
.table-wrap{ overflow-x:auto; }
.table-wrap table{ width:100%; border-collapse:collapse; background:#fff; table-layout:fixed; }
.table-wrap th, .table-wrap td{
  border:1px solid #e9ecef; padding:8px 10px; text-align:left; vertical-align:top;
  font-size:13px; line-height:1.45; white-space:normal; word-break:keep-all; overflow-wrap:anywhere;
}
.table-wrap th{ background:#f8f9fa; font-weight:700; }
@media (max-width: 1200px){ .table-wrap th, .table-wrap td{ font-size:12px; padding:6px 8px; } }
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
        # 👉 보기 형식 순서/기본값 변경: 표형(PC) 기본
        view_mode = st.radio("보기 형식", ["표형(PC)", "카드형(모바일)"], index=0, horizontal=True, key="main_view_mode")
        if view_mode == "표형(PC)":
            render_table(df, cols_order)
        else:
            render_cards(df, cols_order)

        # 👉 메인 탭: 결과 렌더 후 반드시 상단으로 스크롤 + 키워드 입력에 포커스
        st.components.v1.html("""
<script>
(function(){
  const LABEL = '키워드 (입력 없이 Enter=전체조회, 공백=AND)';
  const doc = window.parent?.document || document;
  function refocus(){
    // 스크롤 최상단
    try { window.scrollTo({top: 0, behavior: 'auto'}); } catch(e){ window.scrollTo(0,0); }
    // 키워드 input 찾기
    let input = null;
    const labels = Array.from(doc.querySelectorAll('label'));
    for (const lb of labels){
      if ((lb.textContent || '').trim().startsWith(LABEL)){
        input = lb.parentElement?.querySelector('input'); break;
      }
    }
    if (!input) input = doc.querySelector('input[aria-label="'+LABEL+'"]');
    if (input){
      input.focus();
      const len = input.value?.length || 0;
      try { input.setSelectionRange(len, len); } catch(e) {}
    }
  }
  // 렌더 직후 약간의 지연 후 실행
  setTimeout(refocus, 80);
})();
</script>
""", height=0)
    else:
        st.caption("힌트: 조사장소/조사대상은 메인 키워드와 AND 조건으로 결합되어 검색됩니다.")

# ============================ 조사위원 질문 탭 ============================
with tab_qna:
    st.write("")  # 큰 제목 생략
    st.markdown(
        "<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>",
        unsafe_allow_html=True
    )

    # 1) 사용할 테이블 (뷰 우선)
    qna_table = _pick_table(eng, ["qna_sheet_v", "qna_v", "qna_raw"]) or "qna_raw"

    # 2) 입력 폼
    with st.form("qna_search_form", clear_on_submit=False):
        kw_q = st.text_input(
            "키워드 (입력 없이 Enter=전체조회, 공백=AND)",
            st.session_state.get("qna_kw", ""),
            key="qna_kw",
            placeholder="예) 낙상, 환자확인, 고객, 수술 체크리스트 등"
        )
        FIXED_LIMIT_QNA = 2000
        submitted_qna = st.form_submit_button("검색")  # 화면엔 숨김

    # ===== 유틸: 컬럼 이름 느슨 매칭 + 최후보루 =====
    import html as _html
    def _norm_col(s: str) -> str:
        """공백/기호 삭제 + 소문자(한글은 그대로)"""
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
        """숫자/번호/정렬키 제외하고 평균 글자수 가장 긴 컬럼 추정"""
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

    # 3) 검색 실행 (여기서 직접 WHERE 구성해서 3개 필드 동시 검색)
    if submitted_qna:  # 키워드 없이 Enter여도 전체 조회
        with st.spinner("검색 중..."):
            # 현재 테이블의 실제 컬럼 목록
            existing_cols = _list_columns(eng, qna_table)

            # 우리가 찾고자 하는 컬럼 후보들(다양한 표기 허용)
            NUM_CAND = ["No.", "No", "no", "번호", "순번"]
            PLACE_CAND = ["조사장소", "장소", "부서/장소", "부서", "조사 장소", "조사 부서"]
            CONTENT_CAND = [
                "조사위원 질문(확인) 내용", "조사위원 질문(확인)내용",
                "조사위원 질문, 확인내용", "질문(확인) 내용",
                "질문/확인내용", "질문 확인내용", "조사위원 질문", "확인내용"
            ]

            num_col     = _pick_col(existing_cols, NUM_CAND)
            place_col   = _pick_col(existing_cols, PLACE_CAND)
            content_col = _pick_col(existing_cols, CONTENT_CAND)

            # 내용 컬럼을 못 찾으면: '가장 긴 텍스트' 컬럼을 추정
            exclude = set([place_col, num_col, "sort1", "sort2", "sort3"])
            content_col = content_col or _guess_long_text_col(
                # 전체를 훑을 샘플이 필요하므로, 잠시 한 번 전체 SELECT (LIMIT 200) 로 df 생성
                pd.read_sql_query(text(f'SELECT * FROM "{qna_table}" LIMIT 200'), eng),
                exclude
            )

            # 실제 검색에 사용할 컬럼(없으면 전체 컬럼 사용)
            search_cols = [c for c in [num_col, place_col, content_col] if c] or existing_cols

            kw_list = [w for w in re.split(r"\s+", (kw_q or "").strip()) if w]
            params: Dict[str, str] = {}
            if kw_list:
                and_parts = []
                for i, kw in enumerate(kw_list):
                    or_parts = []
                    for j, col in enumerate(search_cols):
                        p = f"kw_{i}_{j}"
                        or_parts.append(f"COALESCE(\"{col}\"::text,'') ILIKE :{p}")
                        params[p] = f"%{kw}%"
                    and_parts.append("(" + " OR ".join(or_parts) + ")")
                where_sql = " AND ".join(and_parts)
                sql = text(f'SELECT * FROM "{qna_table}" WHERE {where_sql} LIMIT :lim')
                params["lim"] = FIXED_LIMIT_QNA
            else:
                # 전체 조회
                sql = text(f'SELECT * FROM "{qna_table}" LIMIT :lim')
                params = {"lim": FIXED_LIMIT_QNA}

            with eng.begin() as con:
                df_q = pd.read_sql_query(sql, con, params=params)

        if df_q.empty:
            st.info("결과 없음 (키워드 없이 Enter=전체 조회)")
            st.session_state.pop("qna_results", None)
        else:
            st.session_state["qna_results"] = df_q.to_dict("records")

    # 4) 카드 렌더러 (라벨 최소화)
    def render_qna_cards(df_: pd.DataFrame):
        st.markdown("""
<style>
.qcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.qtitle{font-size:15px;font-weight:800;margin-bottom:6px;word-break:break-word;color:#0d47a1}
.qbody{font-size:13px;color:#333;word-break:break-word}
</style>
        """, unsafe_allow_html=True)

        cols = list(df_.columns)

        PLACE_CAND = ["조사장소", "장소", "부서/장소", "부서", "조사 장소", "조사 부서"]
        CONTENT_CAND = [
            "조사위원 질문(확인) 내용", "조사위원 질문(확인)내용",
            "조사위원 질문, 확인내용", "질문(확인) 내용",
            "질문/확인내용", "질문 확인내용", "조사위원 질문", "확인내용"
        ]
        NUM_CAND = ["No.", "No", "no", "번호", "순번"]

        num_col     = _pick_col(cols, NUM_CAND)
        place_col   = _pick_col(cols, PLACE_CAND)
        content_col = _pick_col(cols, CONTENT_CAND)

        # 내용 컬럼을 못 찾으면: '가장 긴 텍스트' 컬럼을 추정
        exclude = set([place_col, num_col, "sort1", "sort2", "sort3"])
        content_col = content_col or _guess_long_text_col(df_, exclude)

        for _, r in df_.iterrows():
            place = r.get(place_col, "") if place_col else ""
            place = "" if pd.isna(place) else str(place).strip()
            if not place:
                place = "조사장소 미지정"

            content = r.get(content_col, "") if content_col else ""
            content = "" if pd.isna(content) else str(content).strip()
            if not content:
                best_val, best_len = "", -1
                for c in cols:
                    if c in exclude:
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

    # 5) 결과 표시
    if "qna_results" in st.session_state and st.session_state["qna_results"]:
        df = pd.DataFrame(st.session_state["qna_results"])
        st.write(f"결과: {len(df):,}건")
        render_qna_cards(df)
    else:
        st.caption("키워드를 입력하고 **Enter** 를 누르면 결과가 표시됩니다. (입력 없이 Enter=전체 조회)")

# ============================ 규정검색(PDF파일/본문) 탭 ============================
with tab_pdf:
    # 큰 제목 생략
    st.write("")
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 내부 유틸: 전체 조회용 (키워드 없이 Enter일 때)
    def _fetch_all_regs(eng, filename_like: str = "", limit: int = 1000, hide_ipynb_chk: bool = True):
        where_parts, params = [], {}
        if filename_like.strip():
            where_parts.append("filename ILIKE :fn")
            params["fn"] = f"%{filename_like.strip()}%"
        if hide_ipynb_chk:
            where_parts.append(r"(filename !~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)')")
        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
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

    # ====== 검색 폼 (Enter 제출) ======
    FIXED_LIMIT = 2000  # 전체 조회 상한 (페이지네이션 있으니 넉넉히)
    with st.form("pdf_search_form", clear_on_submit=False):
        kw_pdf  = st.text_input("키워드 (입력 없이 Enter=전체조회, 공백=AND)", st.session_state.get("pdf_kw", ""), key="pdf_kw")
        fn_like = st.text_input("파일명 필터(선택)", st.session_state.get("pdf_fn_filter", ""), key="pdf_fn_filter")
        submitted_pdf = st.form_submit_button("검색")

    # ====== 검색 실행 → 세션 저장 ======
    if submitted_pdf:
        with st.spinner("검색 중..."):
            if kw_pdf.strip():
                _df = search_regs(eng, kw_pdf, filename_like=fn_like, limit=FIXED_LIMIT)
            else:
                _df = _fetch_all_regs(eng, filename_like=fn_like, limit=FIXED_LIMIT)

        # file_id(me) 없는 행은 제외
        if "me" in _df.columns:
            _df = _df[_df["me"].astype(str).str.strip() != ""]
        _df = _df.sort_values(["filename", "page"], kind="stable").reset_index(drop=True)

        if _df.empty:
            st.info("조건에 맞는 결과가 없습니다. (키워드 없이 Enter=전체 조회)")
            for k in ("pdf_results", "pdf_sel_idx", "pdf_kw_list", "pdf_page"):
                st.session_state.pop(k, None)
        else:
            st.session_state["pdf_results"] = _df.to_dict("records")
            st.session_state["pdf_sel_idx"] = 0          # 전역 인덱스(선택 행)
            st.session_state["pdf_kw_list"] = [k.strip() for k in kw_pdf.split() if k.strip()]
            st.session_state["pdf_page"] = 1             # 페이지네이션 초기화

    # ====== 결과 + 페이지네이션 ======
    if "pdf_results" in st.session_state and st.session_state["pdf_results"]:
        import math, base64, html as _html

        df_all   = pd.DataFrame(st.session_state["pdf_results"])
        kw_list  = st.session_state.get("pdf_kw_list", [])
        total    = len(df_all)

        # 페이지네이션 상태
        PAGE_KEY = "pdf_page"
        SIZE_KEY = "pdf_page_size"
        page_size = int(st.session_state.get(SIZE_KEY, 30))
        page      = int(st.session_state.get(PAGE_KEY, 1))
        total_pages = max(1, math.ceil(total / max(1, page_size)))
        page = min(max(1, page), total_pages)

        start = (page - 1) * page_size
        end   = min(start + page_size, total)
        df    = df_all.iloc[start:end].reset_index(drop=False)  # drop=False로 전역 인덱스 유지
        # df['index'] 가 원래 df_all의 전역 인덱스

        st.write(f"결과: {total:,}건  ·  페이지 {page}/{total_pages}  ·  표시 {start+1}–{end}")

        # 컨트롤 바 (페이지 크기 / 이전·다음 / 보기 형식)
        cA, cB, cC = st.columns([1.5, 2, 2])
        with cA:
            new_size = st.selectbox("페이지당 표시", [20, 30, 50, 100], index=[20,30,50,100].index(page_size), key=SIZE_KEY, help="한 페이지에 표시할 결과 개수")
            # 크기 변경 시 1페이지로
            if new_size != page_size:
                st.session_state[PAGE_KEY] = 1
                st.rerun()
        with cB:
            col_prev, col_next = st.columns(2)
            if col_prev.button("◀ 이전", use_container_width=True) and page > 1:
                st.session_state[PAGE_KEY] = page - 1
                st.rerun()
            if col_next.button("다음 ▶", use_container_width=True) and page < total_pages:
                st.session_state[PAGE_KEY] = page + 1
                st.rerun()
        with cC:
            view_mode_pdf = st.radio("보기 형식", ["카드형(모바일)", "표형(간단)"], horizontal=True, key="pdf_view_mode")

        # 링크 생성기
        def view_url(fid: str, page: int) -> str:
            fid = (fid or "").strip()
            return f"https://drive.google.com/file/d/{fid}/view#page={int(page)}"

        # ====== 리스트 렌더 (현재 페이지 df) ======
        if view_mode_pdf.endswith("간단)"):
            st.caption("파일명/페이지 버튼을 클릭하면 아래 미리보기가 바뀝니다.")
            hdr = st.columns([7, 1, 1])
            hdr[0].markdown("**파일명**")
            hdr[1].markdown("**페이지**")
            hdr[2].markdown("**열기**")

            if "pdf_sel_idx" not in st.session_state:
                st.session_state["pdf_sel_idx"] = 0

            for _, row in df.iterrows():
                global_i = int(row["index"])  # 전역 인덱스
                c1, c2, c3 = st.columns([7, 1, 1])
                if c1.button(str(row["filename"]), key=f"pick_file_{global_i}"):
                    st.session_state["pdf_sel_idx"] = global_i
                    st.rerun()
                if c2.button(str(int(row["page"])), key=f"pick_page_{global_i}"):
                    st.session_state["pdf_sel_idx"] = global_i
                    st.rerun()
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
.pmeta{font-size:12px;color:#6c757d;margin-top:6px}
</style>
            """, unsafe_allow_html=True)

            if "pdf_sel_idx" not in st.session_state:
                st.session_state["pdf_sel_idx"] = 0

            for _, row in df.iterrows():
                global_i = int(row["index"])
                fid_i  = (row.get("me") or "").strip()
                fname  = _html.escape(str(row["filename"]))
                pagei  = int(row["page"])

                st.markdown(f'<div class="pcard"><div class="title">{fname}</div>', unsafe_allow_html=True)
                cA, cB = st.columns(2)
                with cA:
                    if st.button(f"이 파일 미리보기 (p.{pagei})", key=f"pick_file_card_{global_i}", use_container_width=True):
                        st.session_state["pdf_sel_idx"] = global_i
                        st.rerun()
                with cB:
                    st.markdown(
                        f'<a class="pbtn" href="{view_url(fid_i, pagei)}" target="_blank" rel="noopener noreferrer" '
                        f'style="display:block;text-align:center;padding:9px 12px;">열기</a>',
                        unsafe_allow_html=True
                    )
                st.markdown(f'<div class="pmeta">file_id: {fid_i or "-"}</div></div>', unsafe_allow_html=True)

        # ====== 선택 행 미리보기 ======
        sel_idx = int(st.session_state.get("pdf_sel_idx", 0))
        sel_idx = max(0, min(sel_idx, len(df_all) - 1))
        sel = df_all.iloc[sel_idx]
        fid = (sel.get("me") or "").strip()
        sel_file = sel["filename"]
        sel_page = int(sel["page"])

        st.caption("텍스트 미리보기 & 문서 보기 (선택한 1건)")
        st.write(f"**파일**: {sel_file}  |  **페이지**: {sel_page}  |  **file_id**: {fid or '-'}")
        st.markdown(highlight_html(sel["text"], kw_list, width=200), unsafe_allow_html=True)

        # ---- PDF 바이트 캐시 + 내려받기
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
        st.caption("키워드를 입력하고 **Enter**를 누르면 결과가 표시됩니다. (입력 없이 Enter=전체 조회)")
