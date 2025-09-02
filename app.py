# -*- coding: utf-8 -*-
import os, re, io, html, time, requests, urllib.parse, base64
from typing import List, Dict
from datetime import timezone, timedelta, datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text, create_engine
from pypdf import PdfReader

# =========================
# 기본 설정 & 전역 스타일
# =========================
st.set_page_config(page_title="★★★ HISMEDI 인증 ★★★", layout="wide")
st.markdown("""
<style>
/* 레이아웃 여백(모바일 safe-area 포함) */
section.main > div.block-container{
  padding-top: calc(env(safe-area-inset-top, 0px) + 56px);
  padding-bottom: 36px;
}
@media (max-width:768px){
  section.main > div.block-container{
    padding-top: calc(env(safe-area-inset-top, 0px) + 64px);
  }
}
/* 요소 간 간격 타이트 */
div[data-testid="stVerticalBlock"]{gap:.6rem;}
div[data-testid="stHorizontalBlock"]{gap:.6rem;}
h1, h2, h3, h4, h5, h6{margin:.2rem 0 .6rem 0}
h1, h2, h3, .main-title{ scroll-margin-top: 80px; }

.main-title{font-weight:800;font-size:26px;line-height:1.25;margin:4px 0 8px;color:#111;}
@media (max-width:768px){ .main-title{font-size:22px} }

/* 공통 카드/표 */
.card{border:1px solid #e9ecef;border-radius:10px;padding:12px 14px;margin:8px 0;background:#fff}
.card h4{margin:0 0 8px 0;font-size:16px;line-height:1.3;word-break:break-word}
.card .row{margin:4px 0;font-size:13px;color:#333;word-break:break-word}
.card .lbl{display:inline-block;min-width:110px;color:#6c757d}
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

/* PDF 탭(리스트/카드) */
.pcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.pcard .title{font-size:15px;font-weight:700;margin-bottom:10px;word-break:break-all}
.pbtn{display:inline-block;padding:8px 12px;border:1px solid #dee2e6;border-radius:10px;background:#f8f9fa;
      text-decoration:none;color:#0d6efd;font-weight:600}
.pmeta{font-size:12px;color:#6c757d;margin-top:6px}

/* 동영상 탭(심플) */
.vlist .row{display:flex;align-items:center;gap:.5rem;margin:.6rem 0;} /* 간격 살짝 확대 */
.vlist .name{flex:1 1 auto; word-break:break-all; font-size:14px;}
.vlist .name a{text-decoration:none; font-weight:600; color:#0d6efd}

/* 상단 동기화 버튼 */
.stButton > button.sync-all{
  width:100%; border:1px solid #ffd5d5; border-radius:12px;
  background:#fff; color:#d6336c; font-weight:800; padding:10px 12px;
}
.stButton > button.sync-all:hover{background:#fff5f5;border-color:#ffb3b3}
</style>
""", unsafe_allow_html=True)

st.markdown('<div class="main-title">HISMEDI 인증</div>', unsafe_allow_html=True)

# =========================
# Edge Function 호출
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
        st.error("APP_PASSWORD 는 8자리 숫자여야 합니다. (예: 12345678)"); st.stop()
    if not st.session_state.get("pw_ok", False):
        with st.form("pw_gate", clear_on_submit=False):
            pw = st.text_input("접속 비밀번호 (입력 숫자 8자리)", type="password", max_chars=8, placeholder="예: 12345678")
            ok = st.form_submit_button("확인")
        if ok:
            if (not _is_valid_pw_format(pw)) or (pw != _APP_PW):
                st.error("비밀번호가 틀렸습니다."); st.stop()
            st.session_state["pw_ok"] = True; st.rerun()
        else:
            st.stop()

# -------------------------
# 관리자 토큰 핸들러 (?admin=...)
# -------------------------
ADMIN_TOKEN = (st.secrets.get("ADMIN_TOKEN") or os.getenv("ADMIN_TOKEN") or "").strip()

def _is_admin() -> bool:
    """URL의 ?admin= 토큰이 ADMIN_TOKEN과 일치하면 세션에 관리자 플래그 저장."""
    if not ADMIN_TOKEN:
        return False

    # 이미 인증된 세션이면 바로 True
    if st.session_state.get("_admin_ok"):
        return True

    # 쿼리파라미터에서 admin 값 읽기 (신/구 API 모두 대응)
    try:
        if hasattr(st, "query_params"):  # 최신
            qp = st.query_params
            tok = qp.get("admin", None)
        else:  # 구버전
            q = st.experimental_get_query_params()
            tok = (q.get("admin", [None]) or [None])[0]
    except Exception:
        tok = None

    if tok and str(tok).strip() == ADMIN_TOKEN:
        st.session_state["_admin_ok"] = True

        # (선택) URL에서 admin 파라미터 지우기 — 최신 API에서만 가능
        try:
            if hasattr(st, "query_params"):
                if "admin" in st.query_params:
                    del st.query_params["admin"]
        except Exception:
            pass

        return True

    return False

# =========== DB 유틸 ===========
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

def search_table_any(eng, table: str, keywords: str, columns=None, limit: int = 500):
    kw_list = [w for w in re.split(r"\s+", (keywords or "").strip()) if w]
    select_cols = "*"
    if columns: select_cols = ", ".join(_qident(c) for c in columns)

    if not kw_list:
        sql = text(f"SELECT {select_cols} FROM {_qident(table)} LIMIT :limit")
        with eng.begin() as con:
            return pd.read_sql_query(sql, con, params={"limit": int(limit)})

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

# ========== PDF 인덱싱/검색 ==========
REQUIRED_REG_COLUMNS = ["id", "filename", "page", "text", "file_mtime", "me"]

def ensure_reg_table(eng):
    with eng.begin() as con:
        try: con.execute(text("create extension if not exists pg_trgm"))
        except Exception: pass
        exists = con.execute(text("select to_regclass('public.regulations')")).scalar() is not None
        recreate = False
        if exists:
            cols = [r[0] for r in con.execute(text("""
                select column_name from information_schema.columns
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
                "fields": "nextPageToken, files(id,name,mimeType,parents,modifiedTime)",
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
            p.append(cur.get("name") or "")
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
    kw_list = [k.strip() for k in str(keywords or "").split() if k.strip()]
    where_parts, params = [], {}
    if kw_list:
        for i, kw in enumerate(kw_list):
            where_parts.append(f"(text ILIKE :kw{i})")
            params[f"kw{i}"] = f"%{kw}%"
    if filename_like.strip():
        where_parts.append("filename ILIKE :fn"); params["fn"] = f"%{filename_like.strip()}%"
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

# ============ DB 연결 ============
eng = get_engine()
try:
    with eng.begin() as con:
        user, port, now_utc = con.execute(text("select current_user, inet_server_port(), now()")).one()
except Exception as e:
    st.error("DB 연결 실패"); st.exception(e); st.stop()

# -------- Drive 시크릿 & 도우미 --------
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

# 인증교육자료(동영상) 폴더(시크릿 없으면 기본값 사용)
EDU_FOLDER_DEFAULT = "1AQkdgO3iVqzUta5LPTMl5qqUlppJ97Pn"
EDU_FOLDER_ID = _extract_drive_id(st.secrets.get("EDU_FOLDER_ID") or os.getenv("EDU_FOLDER_ID") or EDU_FOLDER_DEFAULT)

# ------------------------------------------------------------
# 상단: 데이터 전체 동기화 (Main+QnA + PDF 인덱스)
# ------------------------------------------------------------
# 관리자만 버튼 노출
if _is_admin():
    if st.button(
        "데이터 전체 동기화",
        key="btn_sync_all_pdf",
        type="secondary",
        help="Main+QnA 동기화, PDF 키가 있으면 인덱싱까지 수행합니다.",
        kwargs=None
    ):
        try:
            # 1) Main + QnA
            r1 = _trigger_edge_func("sync_main"); cnt_main = int(r1.get("count", 0))
            r2 = _trigger_edge_func("sync_qna");  cnt_qna  = int(r2.get("count", 0))

            # 2) PDF (Drive 키/폴더 있을 때만)
            cnt_pdf = skipped = errors = renamed = 0
            pdf_note = ""
            if DRIVE_API_KEY and DRIVE_FOLDER_ID:
                res = index_pdfs_from_drive(eng, DRIVE_FOLDER_ID, DRIVE_API_KEY)
                cnt_pdf  = int(res.get("indexed", 0))
                renamed  = int(res.get("renamed", 0))
                skipped  = int(res.get("skipped", 0))
                errors   = int(res.get("errors", 0))
                pdf_note = f" · PDF indexed {cnt_pdf:,}, renamed {renamed:,}, skipped {skipped:,}, errors {errors:,}"

            # 캐시/세션 정리 + 최근 동기화 기록
            st.cache_data.clear()
            for k in ("main_results","qna_results","pdf_results","pdf_sel_idx","pdf_kw_list"):
                st.session_state.pop(k, None)
            st.session_state["last_sync_ts"] = time.time()
            st.session_state["last_sync_counts"] = {"main": cnt_main, "qna": cnt_qna, "pdf": cnt_pdf}

            st.success(f"완료: Main {cnt_main:,} · QnA {cnt_qna:,}{pdf_note}")
            st.rerun()
        except Exception as e:
            if e.__class__.__name__ in ("RerunData","RerunException"):
                raise
            st.error(f"동기화 실패: {e}")

def _fmt_ts(ts: float) -> str:
    try:
        kst = timezone(timedelta(hours=9))
        return datetime.fromtimestamp(ts, tz=kst).strftime("%Y-%m-%d %H:%M:%S (KST)")
    except Exception:
        return "-"

counts = st.session_state.get("last_sync_counts"); when = st.session_state.get("last_sync_ts")
if counts and when:
    line = f"최근 동기화: Main {counts.get('main',0):,} · QnA {counts.get('qna',0):,}"
    if counts.get("pdf",0): line += f" · PDF {counts['pdf']:,}"
    line += f" · {_fmt_ts(when)}"
    st.caption(line)

# ===== 탭 =====
tab_main, tab_qna, tab_pdf, tab_edu = st.tabs(["기준/지침", "Q/n/A", "규정검색/PDF", "인증교육/영상"])

# ========================== 메인 탭 ==========================
with tab_main:
    # 탭 상단 앵커: 검색/보기 전환 후 항상 이 위치로 고정
    st.markdown('<div id="main-tab-top"></div>', unsafe_allow_html=True)

    # 폼 제출 버튼 숨김(Enter로 제출)
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 1) 사용할 테이블(뷰 우선)
    main_table = _pick_table(eng, ["main_sheet_v", "main_v", "main_raw"]) or "main_raw"

    # 2) 표시 순서 및 너비 비율
    MAIN_COLS = ["ME","조사항목","항목","등급","조사결과","조사기준의 이해","조사방법1","조사방법2","조사장소","조사대상"]
    MAIN_COL_WEIGHTS = {
        "ME":2,"조사장소":4,"조사대상":4,"조사방법1":10,"조사방법2":5,
        "조사기준의 이해":12,"조사항목":8,"항목":1,"등급":1,"조사결과":2
    }

    existing_cols = _list_columns(eng, main_table)
    show_cols = [c for c in MAIN_COLS if c in existing_cols]
    has_sort = all(x in existing_cols for x in ["sort1","sort2","sort3"])

    # ====== 입력 폼 (Enter 제출) ======
    with st.form("main_search_form", clear_on_submit=False):
        c1, c2, c3 = st.columns([2,1,1])
        with c1:
            kw = st.text_input(
                "키워드 (입력 없이 Enter=전체조회, 공백=AND)",
                st.session_state.get("main_kw", ""),
                key="main_kw",
                placeholder="예) 낙상, 환자 확인, 환자안전 지표 등"
            )
        with c2:
            f_place = st.text_input(
                "조사장소 (선택)",
                st.session_state.get("main_filter_place", ""),
                key="main_filter_place",
                placeholder="예) 전 부서, 병동, 외래, 수술실, 검사실 등"
            )
        with c3:
            f_target = st.text_input(
                "조사대상 (선택)",
                st.session_state.get("main_filter_target", ""),
                key="main_filter_target",
                placeholder="예) 전 직원, 의사, 간호사, 의료기사, 원무 등"
            )
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

        # ▶ 검색 직후 '무조건' 상단 고정 + 입력칸 포커스 (첫 검색 하단 튐 방지)
        st.session_state["main_scroll_and_focus"] = True

    # ====== 스타일(하이라이트) ======
    st.markdown("""
<style>
.hl-item{ color:#0d47a1; font-weight:800; }
.hl-required{ color:#b10000; font-weight:800; }
</style>
    """, unsafe_allow_html=True)

    # ====== 렌더 유틸 ======
    def _fmt_cell(colname: str, value) -> str:
        s = html.escape("" if value is None else str(value))
        def _is_required(val: str) -> bool:
            t = (val or "").strip().replace(" ", "").lower()
            return t in ("필수","必須")
        if colname == "조사항목" and s: return f'<span class="hl-item">{s}</span>'
        if colname == "등급" and _is_required(s): return f'<span class="hl-required">{s}</span>'
        return s

    def render_cards(df_: pd.DataFrame, cols_order: list[str]):
        for _, r in df_.iterrows():
            title_html = _fmt_cell("조사항목", r.get("조사항목"))
            rows_html = []
            for c in cols_order:
                v_html = _fmt_cell(c, r.get(c))
                rows_html.append(f'<div class="row"><span class="lbl">{html.escape(str(c))}</span> {v_html}</div>')
            st.markdown(f'<div class="card"><h4>{title_html or "-"}</h4>' + "".join(rows_html) + '</div>', unsafe_allow_html=True)

    def _build_colgroup(cols, weights):
        w = [float(weights.get(str(c), 1)) for c in cols]; tot = sum(w) or 1.0
        return "<colgroup>" + "".join(f'<col style="width:{(x/tot)*100:.3f}%">' for x in w) + "</colgroup>"

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
    <tbody>{''.join(body_rows)}</tbody>
  </table>
</div>
""", unsafe_allow_html=True)

    # 보기 형식 전환 시에도 상단 고정 + 포커스
    def _on_main_view_change():
        st.session_state["main_scroll_and_focus"] = True

    if "main_view_mode" not in st.session_state:
        st.session_state["main_view_mode"] = "표형(PC)"

    # ====== 결과 출력 ======
    if "main_results" in st.session_state and st.session_state["main_results"]:
        df = pd.DataFrame(st.session_state["main_results"])
        cols_order = [c for c in MAIN_COLS if c in df.columns]
        st.write(f"결과: {len(df):,}건")
        view_mode = st.radio("보기 형식", ["표형(PC)", "카드형(모바일)"], index=0, horizontal=True,
                             key="main_view_mode", on_change=_on_main_view_change)
        if view_mode.startswith("표형"):
            render_table(df, cols_order)
        else:
            render_cards(df, cols_order)
    else:
        st.caption("힌트: 조사장소/조사대상은 메인 키워드와 AND 조건으로 결합되어 검색됩니다.")

    # ====== (핵심) 검색/보기전환 이후 상단 고정 + 입력칸 포커스 ======
    if st.session_state.pop("main_scroll_and_focus", False):
        st.components.v1.html("""
<script>
(function(){
  const doc = window.parent?.document || document;

  // 1) 상단 앵커로 즉시 스크롤(하단 튐 방지)
  try{
    var el = doc.getElementById('main-tab-top');
    var top = 0;
    if(el){
      var rect = el.getBoundingClientRect();
      var scrollTop = (window.parent ? window.parent.pageYOffset : window.pageYOffset);
      top = rect.top + scrollTop - 90; // 상단 고정바 여유
    }
    if (window.parent) window.parent.scrollTo({top: top, left: 0, behavior: 'auto'});
    else window.scrollTo({top: top, left: 0, behavior: 'auto'});
  }catch(e){}

  // 2) 키워드 입력칸 포커스(+커서 끝)
  try{
    const LABEL = '키워드 (입력 없이 Enter=전체조회, 공백=AND)';
    let input = null;
    const labels = Array.from(doc.querySelectorAll('label'));
    for (const lb of labels){
      if ((lb.textContent||'').trim().startsWith(LABEL)){
        input = lb.parentElement?.querySelector('input'); if(input) break;
      }
    }
    if (!input){ input = doc.querySelector('input[aria-label="'+LABEL+'"]'); }
    if (input){
      input.focus();
      const len = (input.value||'').length;
      try{ input.setSelectionRange(len, len); }catch(e){}
    }
  }catch(e){}
})();
</script>
        """, height=0)

# ============================ QnA 탭 ============================
with tab_qna:
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    qna_table = _pick_table(eng, ["qna_sheet_v", "qna_v", "qna_raw"]) or "qna_raw"
    with st.form("qna_search_form", clear_on_submit=False):
        kw_q = st.text_input(
            "키워드 (입력 없이 Enter=전체조회, 공백=AND)",
            st.session_state.get("qna_kw", ""),
            key="qna_kw",
            placeholder="예) 낙상, 환자확인, 고객, 수술 체크리스트 등"
        )
        FIXED_LIMIT_QNA = 2000
        submitted_qna = st.form_submit_button("검색")

    import html as _html
    def _norm_col(s: str) -> str:
        s = str(s or ""); s = re.sub(r"[ \t\r\n/_\-:;.,(){}\[\]<>·•｜|]+", "", s); return s.lower()
    def _pick_col(cols: list[str], candidates: list[str]) -> str | None:
        for w in candidates:
            if w in cols: return w
        wants = [_norm_col(w) for w in candidates]
        for c in cols:
            if _norm_col(c) in wants: return c
        for w in wants:
            for c in cols:
                if w and w in _norm_col(c): return c
        return None
    def _guess_long_text_col(df: pd.DataFrame, exclude: set[str]) -> str | None:
        cand = [c for c in df.columns if c not in exclude]
        if not cand: return None
        samp = df.head(50); best_col, best_len = None, -1.0
        for c in cand:
            try: vals = samp[c].astype(str)
            except Exception: vals = samp[c].map(lambda x: "" if x is None else str(x))
            lens = []
            for s in vals:
                s = ("" if s is None else str(s)).strip()
                if not s or re.fullmatch(r"\d{1,4}([./-]\d{1,2}([./-]\d{1,2})?)?$", s):
                    lens.append(0)
                else:
                    lens.append(len(s))
            avg = (sum(lens)/max(1,len(lens)))
            if avg > best_len: best_col, best_len = c, avg
        return best_col

    if submitted_qna:
        with st.spinner("검색 중..."):
            existing_cols = _list_columns(eng, qna_table)
            NUM_CAND = ["No.","No","no","번호","순번"]
            PLACE_CAND = ["조사장소","장소","부서/장소","부서","조사 장소","조사 부서"]
            CONTENT_CAND = ["조사위원 질문(확인) 내용","조사위원 질문(확인)내용","조사위원 질문, 확인내용","질문(확인) 내용",
                            "질문/확인내용","질문 확인내용","조사위원 질문","확인내용"]
            num_col = _pick_col(existing_cols, NUM_CAND)
            place_col = _pick_col(existing_cols, PLACE_CAND)
            content_col = _pick_col(existing_cols, CONTENT_CAND)
            exclude = set([place_col, num_col, "sort1","sort2","sort3"])

            if not content_col:
                df_probe = pd.read_sql_query(text(f'SELECT * FROM "{qna_table}" LIMIT 200'), eng)
                content_col = _guess_long_text_col(df_probe, exclude)

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
                sql = text(f'SELECT * FROM "{qna_table}" LIMIT :lim')
                params = {"lim": FIXED_LIMIT_QNA}
            with eng.begin() as con:
                df_q = pd.read_sql_query(sql, con, params=params)

        if df_q.empty:
            st.info("결과 없음 (키워드 없이 Enter=전체 조회)")
            st.session_state.pop("qna_results", None)
        else:
            st.session_state["qna_results"] = df_q.to_dict("records")

    def render_qna_cards(df_: pd.DataFrame):
        st.markdown("""
<style>
.qcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.qtitle{font-size:15px;font-weight:800;margin-bottom:6px;word-break:break-word;color:#0d47a1}
.qbody{font-size:13px;color:#333;word-break:break-word}
</style>
        """, unsafe_allow_html=True)
        cols = list(df_.columns)
        PLACE_CAND = ["조사장소","장소","부서/장소","부서","조사 장소","조사 부서"]
        CONTENT_CAND = ["조사위원 질문(확인) 내용","조사위원 질문(확인)내용","조사위원 질문, 확인내용","질문(확인) 내용",
                        "질문/확인내용","질문 확인내용","조사위원 질문","확인내용"]
        NUM_CAND = ["No.","No","no","번호","순번"]
        num_col = _pick_col(cols, NUM_CAND)
        place_col = _pick_col(cols, PLACE_CAND)
        content_col = _pick_col(cols, CONTENT_CAND)
        exclude = set([place_col, num_col, "sort1","sort2","sort3"])
        content_col = content_col or _guess_long_text_col(df_, exclude)

        for _, r in df_.iterrows():
            place = r.get(place_col, "") if place_col else ""
            place = "" if pd.isna(place) else str(place).strip()
            if not place: place = "조사장소 미지정"
            content = r.get(content_col, "") if content_col else ""
            content = "" if pd.isna(content) else str(content).strip()
            if not content:
                best_val, best_len = "", -1
                for c in cols:
                    if c in exclude: continue
                    v = r.get(c, "")
                    if pd.isna(v): continue
                    s = str(v).strip()
                    if len(s) > best_len: best_val, best_len = s, len(s)
                content = best_val
            st.markdown(
                f"""
<div class="qcard">
  <div class="qtitle">{_html.escape(place)}</div>
  <div class="qbody">{_html.escape(content) if content else "-"}</div>
</div>
                """, unsafe_allow_html=True)

    if "qna_results" in st.session_state and st.session_state["qna_results"]:
        df = pd.DataFrame(st.session_state["qna_results"])
        st.write(f"결과: {len(df):,}건")
        render_qna_cards(df)
    else:
        st.caption("키워드를 입력하고 **Enter** 를 누르면 결과가 표시됩니다. (입력 없이 Enter=전체 조회)")

# ====================== PDF 탭 ============================
with tab_pdf:
    # 폼 제출 버튼 숨김 + 이 탭 전용 컴팩트/모바일 친화 CSS
    st.markdown("""
<style>
/* 컨트롤 컴팩트 */
.pdf-compact .stButton>button{
  padding:4px 10px !important; font-size:12px !important; border-radius:8px !important;
}
.pdf-compact [data-baseweb="select"]>div{
  min-height:30px !important; padding-top:2px !important; padding-bottom:2px !important;
}
/* 라벨-입력 간격 */
.stTextInput > label{ margin-bottom:6px !important; }

/* 모바일 카드 스타일(파일/페이지 공통) */
.pdf-card{
  border:1px solid #e9ecef; border-radius:12px; background:#fff;
  padding:10px 12px; margin:8px 0;
}
.pdf-card .fname{
  font-weight:800; font-size:14px; line-height:1.35; word-break:break-all;
}
.pdf-card .meta{
  font-size:12px; color:#6c757d; margin-top:4px;
}
.pdf-card .rowbtn{ display:flex; gap:6px; margin-top:8px; }
.open-btn{
  display:inline-block; padding:4px 10px !important; font-size:12px !important;
  border:1px solid #ddd; border-radius:8px; background:#f8f9fa;
  text-decoration:none; color:#0d6efd; font-weight:600;
}

/* 표형(PC) 간단 리스트 */
.pbtn{display:inline-block;padding:6px 10px;border:1px solid #dee2e6;border-radius:8px;background:#f8f9fa;
      text-decoration:none;color:#0d6efd;font-weight:600}
</style>
""", unsafe_allow_html=True)

    # ====== 검색 폼: 파일명 / 본문 분리 ======
    FIXED_LIMIT = 2000
    with st.form("pdf_search_form", clear_on_submit=False):
        c1, c2 = st.columns([1, 1])
        with c1:
            name_kw = st.text_input(
                "파일명 검색 (입력 없이 Enter=전체조회, 공백=AND)",
                st.session_state.get("pdf_name_kw", ""),
                key="pdf_name_kw",
                placeholder="예) 1.1, 환자 확인, 수술, 손위생 등"
            )
        with c2:
            body_kw = st.text_input(
                "본문(내용) 검색 (입력 없이 Enter=전체조회, 공백=AND)",
                st.session_state.get("pdf_body_kw", ""),
                key="pdf_body_kw",
                placeholder="예) 병동, 외래, 내시경실, 간호사 등"
            )
        submitted_pdf = st.form_submit_button("검색")

    # ====== 쿼리 유틸 ======
    def _query_pages(name_kw: str, body_kw: str, limit: int = 2000, hide_ipynb_chk: bool = True) -> pd.DataFrame:
        """페이지 단위 결과(본문 AND, 파일명 AND 필터)."""
        name_tokens = [t for t in re.split(r"\s+", (name_kw or "").strip()) if t]
        body_tokens = [t for t in re.split(r"\s+", (body_kw or "").strip()) if t]
        where_parts, params = [], {}

        # 본문 AND
        for i, kw in enumerate(body_tokens):
            where_parts.append(f"(text ILIKE :b{i})")
            params[f"b{i}"] = f"%{kw}%"
        # 파일명 AND(선택)
        for j, kw in enumerate(name_tokens):
            where_parts.append(f"(filename ILIKE :n{j})")
            params[f"n{j}"] = f"%{kw}%"

        if hide_ipynb_chk:
            where_parts.append(r"(filename !~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)')")

        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
        sql = text(f"""
            SELECT filename, page, me, text
              FROM regulations
             WHERE {where_sql}
             ORDER BY filename, page
             LIMIT :lim
        """)
        params["lim"] = int(limit)
        with eng.begin() as con:
            return pd.read_sql_query(sql, con, params=params)

    def _query_files(name_kw: str, hide_ipynb_chk: bool = True) -> pd.DataFrame:
        """
        파일명 전용: '파일' 목록(중복 제거).
        - name_kw 비어도 전체 파일 목록.
        - 파일 ID(me) 기준으로 dedupe (파일명 변경 이력 자동 통합).
        - any_name 은 DB 내 최근/사전식 우선 fallback 이름.
        """
        name_tokens = [t for t in re.split(r"\s+", (name_kw or "").strip()) if t]
        where_parts, params = ["(COALESCE(me,'') <> '')"], {}

        # 파일명 AND(선택)
        for j, kw in enumerate(name_tokens):
            where_parts.append(f"(filename ILIKE :n{j})")
            params[f"n{j}"] = f"%{kw}%"

        if hide_ipynb_chk:
            where_parts.append(r"(filename !~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)')")

        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

        # 파일 ID(me)당 1행 + 첫 페이지 + 총 페이지 수 + fallback 이름(최신/사전식)
        sql = text(f"""
            SELECT
                me,
                MIN(page) AS first_page,
                COUNT(*)  AS pages,
                MAX(file_mtime) AS mtime,
                (ARRAY_AGG(filename ORDER BY file_mtime DESC NULLS LAST, filename DESC))[1] AS any_name
              FROM regulations
             WHERE {where_sql}
          GROUP BY me
          ORDER BY any_name
        """)
        with eng.begin() as con:
            return pd.read_sql_query(sql, con, params=params)

    @st.cache_data(ttl=600, show_spinner=False)
    def _drive_name(fid: str, api_key: str) -> str | None:
        """Drive에서 실시간 파일명을 가져와 표시(변경 이력 반영). 실패 시 None."""
        try:
            r = requests.get(
                f"https://www.googleapis.com/drive/v3/files/{fid}",
                params={"fields": "name", "key": api_key},
                timeout=15
            )
            if r.status_code >= 400:
                return None
            return (r.json() or {}).get("name")
        except Exception:
            return None

    # ====== 검색 실행 → 세션 저장 ======
    if submitted_pdf:
        name_tokens = [t for t in re.split(r"\s+", (name_kw or "").strip()) if t]
        body_tokens = [t for t in re.split(r"\s+", (body_kw or "").strip()) if t]

        if body_tokens:
            # 본문 검색 모드(페이지 단위)
            with st.spinner("검색 중..."):
                _df = _query_pages(name_kw, body_kw, limit=FIXED_LIMIT)
            if "me" in _df.columns:
                _df = _df[_df["me"].astype(str).str.strip() != ""]
            _df = _df.sort_values(["filename", "page"], kind="stable").reset_index(drop=True)

            if _df.empty:
                st.info("결과 없음 (파일명/본문 둘 다 비워서 Enter=전체 파일 목록)")
                for k in ("pdf_results", "pdf_mode", "pdf_sel_idx", "pdf_body_tokens", "pdf_name_tokens",
                          "pdf_page", "pdf_page_size_label", "pdf_view_mode"):
                    st.session_state.pop(k, None)
            else:
                st.session_state["pdf_results"]      = _df.to_dict("records")
                st.session_state["pdf_mode"]         = "pages"
                st.session_state["pdf_sel_idx"]      = 0
                st.session_state["pdf_body_tokens"]  = body_tokens
                st.session_state["pdf_name_tokens"]  = name_tokens
                st.session_state["pdf_page"]         = 1
                st.session_state.setdefault("pdf_page_size_label", "10개/page")
                # pdf_view_mode 는 여기서 건드리지 않음(위젯 충돌 방지)
        else:
            # 파일명 모드: name_kw 유무와 무관하게 전체/필터된 "파일 목록" 반환
            with st.spinner("목록 불러오는 중..."):
                _df = _query_files(name_kw)
            _df = _df.reset_index(drop=True)

            if _df.empty:
                st.info("표시할 파일이 없습니다.")
                for k in ("pdf_results", "pdf_mode", "pdf_sel_idx", "pdf_body_tokens", "pdf_name_tokens",
                          "pdf_page", "pdf_page_size_label", "pdf_view_mode"):
                    st.session_state.pop(k, None)
            else:
                st.session_state["pdf_results"] = _df.to_dict("records")
                st.session_state["pdf_mode"]    = "files"
                for k in ("pdf_sel_idx","pdf_body_tokens","pdf_name_tokens"):
                    st.session_state.pop(k, None)
                st.session_state["pdf_page"] = 1
                st.session_state.setdefault("pdf_page_size_label", "10개/page")

    # ====== 결과 렌더 ======
    if "pdf_results" in st.session_state and st.session_state["pdf_results"]:
        import math, html as _html, base64 as _b64

        mode = st.session_state.get("pdf_mode", "files")  # 기본 files
        df_all = pd.DataFrame(st.session_state["pdf_results"])
        total = len(df_all)

        # --- 페이지 상태(파일/페이지 공통)
        PAGE_KEY = "pdf_page"
        SIZE_KEY = "pdf_page_size_label"   # '10개/page' | '30개/page' | '50개/page' | '전체 파일'
        size_labels = ["10개/page", "30개/page", "50개/page", "전체 파일"]
        prev_label = st.session_state.get(SIZE_KEY, "10개/page")
        try:
            idx_default = size_labels.index(prev_label)
        except ValueError:
            idx_default = 0

        # 상단 컨트롤 바: 드롭다운 옆 ◀/▶ (모바일 한 줄 유지)
        st.markdown('<div class="pdf-compact">', unsafe_allow_html=True)
        rowL, rowR = st.columns([1.8, 2])
        with rowL:
            sel_col, prev_col, next_col = st.columns([1.0, 0.32, 0.32])
            with sel_col:
                new_label = st.selectbox(
                    "",
                    size_labels,
                    index=idx_default,
                    key=SIZE_KEY,
                    label_visibility="collapsed",
                )
            with prev_col:
                page = int(st.session_state.get(PAGE_KEY, 1))
                if st.button("◀", key="pdf_prev_btn", use_container_width=True) and page > 1:
                    st.session_state[PAGE_KEY] = page - 1
                    st.rerun()
            with next_col:
                # new_label 기준 총 페이지 계산
                if new_label == "전체 파일":
                    total_pages_for_next = 1
                else:
                    ps_map = {"10개/page": 10, "30개/page": 30, "50개/page": 50}
                    ps = ps_map.get(new_label, 10)
                    total_pages_for_next = max(1, math.ceil(total / max(1, ps)))
                if st.button("▶", key="pdf_next_btn", use_container_width=True):
                    page = int(st.session_state.get(PAGE_KEY, 1))
                    if page < total_pages_for_next:
                        st.session_state[PAGE_KEY] = page + 1
                        st.rerun()

        with rowR:
            if mode == "pages":
                if "pdf_view_mode" not in st.session_state:
                    st.session_state["pdf_view_mode"] = "표형(PC)"  # 최초 기본값
                st.radio(
                    "보기",
                    ["카드형(모바일)", "표형(PC)"],
                    key="pdf_view_mode",
                    horizontal=True
                )
            else:
                st.caption("※ 파일명 검색 결과 — 파일 목록만 표시합니다. (본문 검색을 입력하면 페이지 단위 결과)")

        st.markdown('</div>', unsafe_allow_html=True)

        # 드롭다운 값 변경 시 1페이지로
        if new_label != prev_label:
            st.session_state[PAGE_KEY] = 1
            st.rerun()

        # 실제 page_size/total_pages 계산
        if new_label == "전체 파일":
            page_size   = max(1, total)
            total_pages = 1
            page        = 1
        else:
            ps_map = {"10개/page": 10, "30개/page": 30, "50개/page": 50}
            page_size   = int(ps_map.get(new_label, 10))
            page        = int(st.session_state.get(PAGE_KEY, 1))
            total_pages = max(1, math.ceil(total / max(1, page_size)))
            page        = min(max(1, page), total_pages)

        start = (page - 1) * page_size
        end   = min(start + page_size, total)
        df    = df_all.iloc[start:end].reset_index(drop=False)

        # 상단 카운터(모드별 레이블)
        if mode == "files":
            st.write(f"결과: {total:,}개 파일  ·  페이지 {page}/{total_pages}  ·  표시 {start+1}–{end}  ·  페이지당 {page_size:,}개")
        else:
            st.write(f"결과: {total:,}건  ·  페이지 {page}/{total_pages}  ·  표시 {start+1}–{end}  ·  페이지당 {page_size:,}건")

        # 링크 생성
        def view_url(fid: str, p: int) -> str:
            fid = (fid or "").strip()
            return f"https://drive.google.com/file/d/{fid}/view#page={int(p)}"

        # ===== (A) 파일명 모드 — 파일 1개당 1행 + 열기만 (ID 기준 dedupe + 실시간 Drive 이름) =====
        if mode == "files":
            st.markdown('<div class="pdf-compact">', unsafe_allow_html=True)
            for _, row in df.iterrows():
                fid_i  = (row.get("me") or "").strip()
                # 실시간 Drive 이름 우선, 실패 시 any_name fallback
                live_name = _drive_name(fid_i, DRIVE_API_KEY)
                fname  = live_name or str(row.get("any_name") or "")
                firstp = int(row.get("first_page", 1) or 1)
                pages  = int(row.get("pages", 0) or 0)
                st.markdown(
                    f"""
<div class="pdf-card">
  <div class="fname">{_html.escape(fname)}</div>
  <div class="meta">총 {pages:,} 페이지</div>
  <div class="rowbtn">
    <a class="open-btn" href="{view_url(fid_i, firstp)}" target="_blank" rel="noopener noreferrer">열기</a>
  </div>
</div>
""",
                    unsafe_allow_html=True
                )
            st.markdown('</div>', unsafe_allow_html=True)

        # ===== (B) 본문 검색 모드 — 페이지 단위 + (PC에서만) 미리보기 =====
        else:
            body_tok = st.session_state.get("pdf_body_tokens", [])

            if st.session_state.get("pdf_view_mode") == "카드형(모바일)":
                # 카드형(모바일): 페이지별 간단 카드 + 열기 버튼만
                st.markdown('<div class="pdf-compact">', unsafe_allow_html=True)
                for _, row in df.iterrows():
                    fname = str(row["filename"])
                    pagei = int(row["page"])
                    fid_i = (row.get("me") or "").strip()
                    st.markdown(
                        f"""
<div class="pdf-card">
  <div class="fname">{_html.escape(fname)}</div>
  <div class="meta">페이지 {pagei}</div>
  <div class="rowbtn">
    <a class="open-btn" href="{view_url(fid_i, pagei)}" target="_blank" rel="noopener noreferrer">열기</a>
  </div>
</div>
""",
                        unsafe_allow_html=True
                    )
                st.markdown('</div>', unsafe_allow_html=True)

            else:
                # 표형(PC): 파일명/페이지 버튼으로 '선택 1건 미리보기' 지원
                hdr = st.columns([7, 1, 1])
                hdr[0].markdown("**파일명**")
                hdr[1].markdown("**페이지**")
                hdr[2].markdown("**열기**")

                if "pdf_sel_idx" not in st.session_state:
                    st.session_state["pdf_sel_idx"] = 0

                for _, row in df.iterrows():
                    global_i = int(row["index"])
                    c1, c2, c3 = st.columns([7, 1, 1])
                    if c1.button(str(row["filename"]), key=f"pick_name_{global_i}"):
                        st.session_state["pdf_sel_idx"] = global_i
                        st.rerun()
                    if c2.button(str(int(row["page"])), key=f"pick_page_{global_i}"):
                        st.session_state["pdf_sel_idx"] = global_i
                        st.rerun()
                    c3.markdown(
                        f'<a class="open-btn" href="{view_url(row["me"], int(row["page"]))}" target="_blank" rel="noopener noreferrer">열기</a>',
                        unsafe_allow_html=True
                    )

                # ---- 선택 행 미리보기(PC에서만)
                df_all_pages = df_all
                sel_idx = int(st.session_state.get("pdf_sel_idx", 0))
                sel_idx = max(0, min(sel_idx, len(df_all_pages) - 1))
                sel = df_all_pages.iloc[sel_idx] if len(df_all_pages) else None
                if sel is not None:
                    fid = (sel.get("me") or "").strip()
                    sel_file = sel["filename"]; sel_page = int(sel["page"])
                    st.caption("텍스트 미리보기 & 문서 보기 (선택한 1건)")
                    st.write(f"**파일**: {sel_file}  |  **페이지**: {sel_page}  |  **file_id**: {fid or '-'}")
                    st.markdown(highlight_html(sel["text"], body_tok, width=200), unsafe_allow_html=True)

                    # pdf.js 미리보기
                    cache = st.session_state.setdefault("pdf_cache", {})
                    b64 = cache.get(fid)
                    if not b64:
                        pdf_bytes = _drive_download_pdf(fid, DRIVE_API_KEY)
                        b64 = _b64.b64encode(pdf_bytes).decode("ascii")
                        cache[fid] = b64

                    page_view = st.number_input("미리보기 페이지", 1, 9999, int(sel_page), step=1, key=f"pv_page_{fid}")
                    zoom_pct  = st.slider("줌(%)", 30, 200, 80, step=5, key=f"pv_zoom_{fid}")
                    height_px = st.slider("미리보기 높이(px)", 480, 1200, 640, step=40, key=f"pv_h_{fid}")

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
    const raw = atob(b64), len = raw.length, arr = new Uint8Array(len);
    for (let i=0; i<len; i++) arr[i] = raw.charCodeAt(i);
    return arr;
  }}
  const pdfData    = b64ToUint8Array("{b64}");
  const targetPage = {int(page_view)};
  const sliderZoom = {float(zoom_pct)}/100.0;
  const maxFitW    = 900;
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
        st.caption("파일명/본문 중 아무거나 입력하고 **Enter**를 누르세요. (아무것도 입력 안 하고 Enter=**전체 파일 목록**)")

# ============================ 인증교육자료(동영상) 탭 ============================
with tab_edu:
    # 상단: 관리자만 '목록 새로고침' 노출
    _show_video_refresh_admin = ((not _APP_PW) or st.session_state.get("pw_ok", False)) and _is_admin()

    cL, cR = st.columns([1, 3])
    with cL:
        if _is_admin() and st.button("목록 새로고침", key="edu_refresh", use_container_width=True):
            st.cache_data.clear(); st.rerun()
    with cR:
        if _show_video_refresh_admin:
            st.caption("공유폴더 변경 후 목록이 다르면 새로고침을 눌러주세요. (관리자 전용)")
        else:
            st.caption("공유폴더 변경 사항은 관리자가 새로고침하면 반영됩니다.")

    API_KEY = DRIVE_API_KEY
    if not API_KEY or not EDU_FOLDER_ID:
        st.warning("교육자료 폴더를 불러오려면 **DRIVE_API_KEY / EDU_FOLDER_ID** 시크릿이 필요합니다.")
    else:
        try:
            nodes = _drive_list_all(EDU_FOLDER_ID, API_KEY)
        except Exception as e:
            st.error("Google Drive 목록을 불러오지 못했습니다.")
            st.exception(e)
            st.stop()

        by_id = {n["id"]: n for n in nodes}

        def _path_of(fid: str) -> str:
            p, cur = [], by_id.get(fid)
            while cur:
                p.append(cur.get("name") or "")
                parents = cur.get("parents") or []
                cur = by_id.get(parents[0]) if parents else None
            return "/".join([x for x in reversed(p) if x])

        # 동영상만 선별
        VIDEO_EXT_RE = re.compile(r"\.(mp4|m4v|mov|avi|wmv|mkv|webm)$", re.I)
        items = []
        for n in nodes:
            mt = (n.get("mimeType") or "")
            name = (n.get("name") or "")
            if mt == "application/vnd.google-apps.folder":
                continue
            if mt.startswith("video/") or VIDEO_EXT_RE.search(name):
                items.append({
                    "id": n["id"],
                    "path": _path_of(n["id"]),
                    "name": name,
                    "ext": (os.path.splitext(name)[1] or "").lstrip(".").upper()
                })
        items.sort(key=lambda x: x["path"].lower())

        st.write(f"총 {len(items):,}개")

        # ---- 카드형 그리드 스타일
        st.markdown("""
<style>
/* 반응형 그리드: 모바일 1열, 태블릿 2열, PC 3열 */
.vgrid{display:grid;grid-template-columns:repeat(1,minmax(0,1fr));gap:10px;}
@media (min-width:640px){ .vgrid{grid-template-columns:repeat(2,1fr);} }
@media (min-width:1024px){ .vgrid{grid-template-columns:repeat(3,1fr);} }

/* 카드 전체가 링크(밑줄/파란색 제거) */
a.vcard{
  display:block; text-decoration:none; color:#111;
  border:1px solid #e9ecef; border-radius:12px; background:#fff;
  padding:12px 14px;
  transition: box-shadow .15s ease, border-color .15s ease, transform .02s ease;
}
a.vcard:hover{ border-color:#cfe2ff; box-shadow:0 6px 16px rgba(0,0,0,.06); }
a.vcard:active{ transform:translateY(1px); }

/* 제목(2줄 말줄임) */
.vtitle{
  font-weight:800; font-size:14px; line-height:1.35; word-break:break-all;
  display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden;
}

/* 보조 메타(확장자 표시) */
.vmeta{ margin-top:6px; font-size:12px; color:#6c757d; }

/* 여백 균형: 모바일에서도 너무 붙지 않도록 */
.vwrap{ margin-top:.4rem; }
</style>
        """, unsafe_allow_html=True)

        # ---- 렌더
        if not items:
            st.info("표시할 동영상이 없습니다.")
        else:
            st.markdown('<div class="vwrap"><div class="vgrid">', unsafe_allow_html=True)
            for it in items:
                url_preview = f"https://drive.google.com/file/d/{it['id']}/preview"
                title_html  = html.escape(it["path"])
                meta_html   = f"{it['ext'] or 'VIDEO'}"
                st.markdown(
                    f"""
<a class="vcard" href="{url_preview}" target="_blank" rel="noopener noreferrer">
  <div class="vtitle">{title_html}</div>
  <div class="vmeta">{meta_html}</div>
</a>
""",
                    unsafe_allow_html=True
                )
            st.markdown('</div></div>', unsafe_allow_html=True)
