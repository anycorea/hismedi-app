# -*- coding: utf-8 -*-
import os, re, io, html, time, requests
from pathlib import Path
from typing import List, Tuple, Dict
from urllib.parse import quote

import pandas as pd
import streamlit as st
from sqlalchemy import text, create_engine
from pypdf import PdfReader

# ------------------------------------------------------------
# 페이지/레이아웃
# ------------------------------------------------------------
st.set_page_config(page_title="★★★ HISMEDI 인증 ★★★", layout="wide")

# 하이라이트 색(선택) + 모바일 친화 제목 스타일
st.markdown("""
<style>
/* <mark> 스타일 */
mark { background:#ffe2a8; }
/* 제목(모바일에서 작게) */
.main-title{
  font-weight:800; font-size:26px; line-height:1.25;
  margin:4px 0 12px;
}
@media (max-width: 768px){
  .main-title{ font-size:22px; }
}
</style>
""", unsafe_allow_html=True)

# 큰 H1 대신 컴팩트한 커스텀 타이틀
st.markdown('<div class="main-title">HISMEDI 인증</div>', unsafe_allow_html=True)

# ------------------------------------------------------------
# 옵션: 간단 접근 비밀번호 (8자리 숫자 전용)
#  - secrets 또는 환경변수 APP_PASSWORD 에 8자리 숫자 설정 (예: "12345678")
#  - 5회 연속 실패 시 60초 잠금
# ------------------------------------------------------------
_APP_PW = (st.secrets.get("APP_PASSWORD") or os.getenv("APP_PASSWORD") or "").strip()

def _is_valid_pw_format(pw: str) -> bool:
    # 8자리 숫자만 허용
    return bool(re.fullmatch(r"\d{8}", pw or ""))

if _APP_PW:
    # 관리자 설정 검증 (8자리 숫자 아니면 앱 실행 중단)
    if not _is_valid_pw_format(_APP_PW):
        st.error("관리자 설정 오류: APP_PASSWORD 는 8자리 숫자여야 합니다. (예: 12345678)")
        st.stop()

    # 이미 인증됐는지 확인
    if not st.session_state.get("pw_ok", False):
        # 잠금 여부(과도한 실패 방지)
        now = time.time()
        locked_until = float(st.session_state.get("pw_locked_until", 0))
        if now < locked_until:
            st.warning(f"잠시 후 다시 시도하세요. {int(locked_until - now)}초 후 가능합니다.")
            st.stop()

        # 폼(모바일에서 Enter로 제출 가능)
        with st.form("pw_gate", clear_on_submit=False):
            pw = st.text_input("접속 비밀번호 (8자리 숫자)", type="password",
                               max_chars=8, placeholder="예: 12345678")
            ok = st.form_submit_button("확인")

        if ok:
            if not _is_valid_pw_format(pw):
                st.error("비밀번호 형식이 올바르지 않습니다. 8자리 숫자만 입력하세요.")
                st.stop()

            if pw == _APP_PW:
                st.session_state["pw_ok"] = True
                st.session_state.pop("pw_attempts", None)
                st.session_state.pop("pw_locked_until", None)
                st.rerun()  # 인증 성공 후 UI 갱신
            else:
                # 실패 횟수 증가 및 잠금
                attempts = int(st.session_state.get("pw_attempts", 0)) + 1
                st.session_state["pw_attempts"] = attempts
                if attempts >= 5:
                    st.session_state["pw_locked_until"] = time.time() + 60  # 60초 잠금
                    st.session_state["pw_attempts"] = 0
                    st.error("비밀번호 입력 실패가 많습니다. 60초 후 다시 시도하세요.")
                else:
                    st.error(f"비밀번호가 틀렸습니다. (시도 {attempts}/5)")
                st.stop()
        else:
            # 제출 전에는 아래 내용 노출 방지
            st.stop()
    # pw_ok=True 이면 더 이상 폼 노출되지 않음 (그대로 진행)

# ------------------------------------------------------------
# DB 연결 유틸
# ------------------------------------------------------------
def _load_database_url() -> str:
    url = st.secrets.get("DATABASE_URL") or os.getenv("DATABASE_URL")
    if not url:
        st.error("DATABASE_URL이 없습니다. `.streamlit/secrets.toml` 또는 환경변수에 설정해 주세요.")
        st.stop()
    return str(url).strip()

def _ensure_psycopg_url(url: str) -> str:
    u = url
    if u.startswith("postgresql://"):
        u = u.replace("postgresql://", "postgresql+psycopg://", 1)
    if u.startswith("postgres://"):
        u = u.replace("postgres://", "postgresql+psycopg://", 1)
    # Supabase pooler(6543) / direct(5432) 모두 허용. sslmode 없으면 추가.
    if "sslmode=" not in u:
        u += ("&" if ("?" in u) else "?") + "sslmode=require"
    return u

@st.cache_resource(show_spinner="DB 엔진 생성 중...")
def get_engine():
    url = _ensure_psycopg_url(_load_database_url())
    return create_engine(
        url,
        connect_args={"options": "-c statement_cache_mode=none"},
        pool_pre_ping=True,
    )

def _list_columns(eng, table):
    sql = text("""
        select column_name
        from information_schema.columns
        where table_schema='public' and table_name=:t
        order by ordinal_position
    """)
    with eng.begin() as con:
        return [r[0] for r in con.execute(sql, {"t": table}).fetchall()]

def _table_exists(eng, table):
    with eng.begin() as con:
        return con.execute(text("select to_regclass(:q)"),
                           {"q": f"public.{table}"}).scalar() is not None

def _pick_table(eng, prefer_first: List[str]):
    for t in prefer_first:
        if _table_exists(eng, t):
            return t
    return None

def search_table_any(eng, table, keywords, columns=None, limit=500):
    """공백으로 구분된 키워드 전부(AND)를, 지정된 컬럼들(OR)에서 검색"""
    kw_list = [k.strip() for k in str(keywords).split() if k.strip()]
    if not kw_list:
        return pd.DataFrame()
    cols = columns or _list_columns(eng, table)
    where_parts, params = [], {}
    for i, kw in enumerate(kw_list):
        ors = " OR ".join([f'"{c}" ILIKE :kw{i}' for c in cols])
        where_parts.append(f"({ors})")
        params[f"kw{i}"] = f"%{kw}%"
    where_sql = " AND ".join(where_parts)
    sql = text(f'SELECT * FROM "{table}" WHERE {where_sql} LIMIT :lim')
    params["lim"] = int(limit)
    with eng.begin() as con:
        return pd.read_sql_query(sql, con, params=params)

def run_select_query(eng, sql_text, params=None):
    q = (sql_text or "").strip().rstrip(";")
    if not re.match(r"^(select|with|explain)\b", q, re.I):
        raise ValueError("SELECT/WITH/EXPLAIN만 실행할 수 있습니다.")
    with eng.begin() as con:
        return pd.read_sql_query(text(q), con, params=params or {})

# ------------------------------------------------------------
# PDF 인덱싱/검색 (스키마 자동 교정 포함)
#  - local/HTTP 서버 방식 + Google Drive 방식 병행
# ------------------------------------------------------------
REQUIRED_REG_COLUMNS = ["id", "filename", "page", "text", "file_mtime", "me"]

def ensure_reg_table(eng):
    """regulations 스키마 점검 → 부족/불일치 시 안전하게 재생성"""
    with eng.begin() as con:
        try:
            con.execute(text("create extension if not exists pg_trgm"))
        except Exception:
            pass

        exists = con.execute(
            text("select to_regclass('public.regulations')")
        ).scalar() is not None

        recreate = False
        if exists:
            cols = [r[0] for r in con.execute(text("""
                select column_name
                from information_schema.columns
                where table_schema='public' and table_name='regulations'
            """)).fetchall()]
            need = set(REQUIRED_REG_COLUMNS) - set(cols)
            if need:
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
    s = s or ""
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def index_pdfs(eng, folder: Path):
    """로컬/서버 폴더의 PDF를 incremental 인덱싱 (Streamlit Cloud에선 보통 미사용)"""
    ensure_reg_table(eng)
    pdfs = sorted(folder.rglob("*.pdf"))
    if not pdfs:
        return {"indexed": 0, "skipped": 0, "errors": 0, "files": []}

    indexed = skipped = errors = 0
    done_files = []

    with eng.begin() as con:
        # 기존에 들어간 체크포인트 파일들 싹 정리(한 번 실행해두면 좋음)
        con.execute(text(r"delete from regulations where filename ~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)'"))

        for f in pdfs:
            # 체크포인트 폴더 제외
            if any(part == ".ipynb_checkpoints" for part in f.parts):
                skipped += 1
                try:
                    fn_skip = str(f.relative_to(folder))
                except Exception:
                    fn_skip = str(f)
                done_files.append((fn_skip, "skip(checkpoints)"))
                continue

            try:
                fn = str(f.relative_to(folder))
            except Exception:
                fn = str(f)
            mt = int(f.stat().st_mtime)

            # 최신이면 스킵
            row = con.execute(
                text("select count(*) from regulations where filename=:fn and file_mtime=:mt"),
                {"fn": fn, "mt": mt},
            ).scalar()
            if row and row > 0:
                skipped += 1
                done_files.append((fn, "skip"))
                continue

            # 기존 레코드 제거 후 재적재
            con.execute(text("delete from regulations where filename=:fn"), {"fn": fn})

            # PDF 읽기
            try:
                reader = PdfReader(str(f))
            except Exception as e:
                errors += 1
                done_files.append((fn, f"open-fail: {type(e).__name__}"))
                continue

            rows = []
            for pno, page in enumerate(reader.pages, start=1):
                try:
                    txt = page.extract_text() or ""
                except Exception:
                    txt = ""
                txt = _clean_text(txt)
                if not txt:
                    continue
                rows.append({"filename": fn, "page": pno, "text": txt, "file_mtime": mt, "me": None})

            if rows:
                df = pd.DataFrame(rows)
                df.to_sql("regulations", con, if_exists="append", index=False)
                indexed += 1
                done_files.append((fn, f"indexed {len(rows)}p"))
            else:
                done_files.append((fn, "no-text"))

    return {"indexed": indexed, "skipped": skipped, "errors": errors, "files": done_files}

# ---------------- Google Drive 크롤/인덱싱 ------------------

@st.cache_data(ttl=600, show_spinner=False)
def _drive_list_all(folder_id: str, api_key: str):
    """folder_id 이하 모든 파일/폴더 메타데이터(id,name,mimeType,parents)를 재귀 수집"""
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
    """id->node, id->상대경로, 상대경로->fileId 매핑 생성"""
    nodes = _drive_list_all(folder_id, api_key)
    by_id = {n["id"]: n for n in nodes}
    def path_of(fid):
        p = []
        cur = by_id.get(fid)
        while cur:
            p.append(cur["name"])
            parents = cur.get("parents") or []
            cur = by_id.get(parents[0]) if parents else None
        p = [x for x in reversed(p) if x]   # ['하위', '파일.pdf']
        return "/".join(p)
    id_to_rel = {n["id"]: path_of(n["id"]) for n in nodes}
    rel_to_id = {v:k for k,v in id_to_rel.items()
                 if by_id[k].get("mimeType") == "application/pdf" or v.lower().endswith(".pdf")}
    return by_id, id_to_rel, rel_to_id

def _drive_download_pdf(file_id: str, api_key: str) -> bytes:
    """공개 파일 직접 다운로드 (PDF 바이트)"""
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    r = requests.get(url, params={"alt": "media", "key": api_key}, timeout=60)
    r.raise_for_status()
    return r.content

def index_pdfs_from_drive(eng, folder_id: str, api_key: str, limit_files: int = 0):
    """구글드라이브 폴더에서 PDF를 내려받아 인덱싱 (me 칼럼에 file_id 저장)"""
    ensure_reg_table(eng)
    by_id, id_to_rel, rel_to_id = _drive_path_map(folder_id, api_key)

    indexed = skipped = errors = 0
    done_files = []

    with eng.begin() as con:
        for rel, fid in rel_to_id.items():
            try:
                # 간단화를 위해 mtime 비교 생략 → filename 단위로 새로고침
                row = con.execute(text("select count(*) from regulations where filename=:fn"),
                                  {"fn": rel}).scalar()
                if row and row > 0:
                    skipped += 1
                    done_files.append((rel, "skip"))
                    continue

                con.execute(text("delete from regulations where filename=:fn"), {"fn": rel})

                pdf_bytes = _drive_download_pdf(fid, api_key)
                reader = PdfReader(io.BytesIO(pdf_bytes))
                rows = []
                for pno, page in enumerate(reader.pages, start=1):
                    try:
                        txt = page.extract_text() or ""
                    except Exception:
                        txt = ""
                    txt = _clean_text(txt)
                    if not txt:
                        continue
                    rows.append({"filename": rel, "page": pno, "text": txt,
                                 "file_mtime": 0, "me": fid})  # me에 file_id 저장

                if rows:
                    pd.DataFrame(rows).to_sql("regulations", con, if_exists="append", index=False)
                    indexed += 1
                    done_files.append((rel, f"indexed {len(rows)}p"))
                else:
                    done_files.append((rel, "no-text"))
            except Exception as e:
                errors += 1
                done_files.append((rel, f"error: {type(e).__name__}"))

            if limit_files and indexed >= limit_files:
                break

    return {"indexed": indexed, "skipped": skipped, "errors": errors, "files": done_files}

def make_snippet(text_: str, kw_list: List[str], width: int = 160) -> str:
    """가장 먼저 맞은 키워드 주변으로 스니펫 생성"""
    if not text_:
        return ""
    low = text_.lower()
    pos = -1
    hit = ""
    for k in kw_list:
        k = k.lower()
        i = low.find(k)
        if i != -1 and (pos == -1 or i < pos):
            pos = i
            hit = k
    if pos == -1:
        return text_[:width] + ("..." if len(text_) > width else "")
    start = max(0, pos - width // 2)
    end = min(len(text_), pos + len(hit) + width // 2)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text_) else ""
    return f"{prefix}{text_[start:end]}{suffix}"

def highlight_html(src_text: str, kw_list: List[str], width: int = 200) -> str:
    """스니펫을 만든 뒤, 키워드를 <mark>로 강조해 HTML로 반환."""
    snippet = make_snippet(src_text, kw_list, width=width)
    esc = html.escape(snippet)
    for k in sorted({k for k in kw_list if k}, key=len, reverse=True):
        pattern = re.compile(re.escape(k), re.IGNORECASE)
        esc = pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", esc)
    return esc

def search_regs(eng, keywords: str, filename_like: str = "", limit: int = 500, hide_ipynb_chk: bool = True):
    """PDF 본문 검색. 기본값으로 .ipynb_checkpoints 결과를 숨김."""
    kw_list = [k.strip() for k in str(keywords).split() if k.strip()]
    if not kw_list:
        return pd.DataFrame()

    where_parts, params = [], {}

    # 본문 키워드 AND
    for i, kw in enumerate(kw_list):
        where_parts.append(f"(text ILIKE :kw{i})")
        params[f"kw{i}"] = f"%{kw}%"

    # 파일명 필터(선택)
    if filename_like.strip():
        where_parts.append("filename ILIKE :fn")
        params["fn"] = f"%{filename_like.strip()}%"

    # .ipynb_checkpoints 숨기기
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

# ------------------------------------------------------------
# DB 연결 (성공 시 화면에 표시하지 않음 / 실패 시만 에러 표시)
#  - 배지를 보고 싶으면 SHOW_DB_BADGE=1 (secrets 또는 env) 로 설정
# ------------------------------------------------------------
from datetime import timezone, timedelta

def _to_kst(dt):
    try:
        kst = timezone(timedelta(hours=9))
        return (dt.astimezone(kst) if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc).astimezone(kst))
    except Exception:
        return dt

SHOW_DB_BADGE = str(st.secrets.get("SHOW_DB_BADGE", os.getenv("SHOW_DB_BADGE", "0"))).strip() in ("1", "true", "True")

eng = get_engine()
try:
    with eng.begin() as con:
        user, port, now_utc = con.execute(
            text("select current_user, inet_server_port(), now()")
        ).one()
    now_kst = _to_kst(now_utc)
    # 상태를 세션에 저장 (필요 시 다른 곳에서 참고)
    st.session_state["db_status"] = {
        "ok": True,
        "user": user,
        "port": port,
        "time_kst": now_kst.strftime("%Y-%m-%d %H:%M:%S (KST)")
    }

    # --- 성공 시 기본은 '완전 숨김' ---
    # 배지를 켜고 싶다면 SHOW_DB_BADGE=1 로 설정
    if SHOW_DB_BADGE:
        st.markdown(f"""
<style>
.db-badge {{
  position: fixed; top: 8px; right: 10px; z-index: 9999;
  font-size: 12px; color: #0a0; background: #f6fff6;
  border: 1px solid #dfe3e8; border-radius: 12px; padding: 4px 8px;
  box-shadow: 0 1px 2px rgba(0,0,0,.04);
}}
.db-dot {{ display:inline-block;width:8px;height:8px;border-radius:50%;background:#28a745;vertical-align:middle;margin-right:6px;}}
@media (max-width: 768px) {{
  .db-badge {{ top: 6px; right: 8px; font-size: 11px; }}
}}
</style>
<div class="db-badge"><span class="db-dot"></span>DB OK</div>
        """, unsafe_allow_html=True)

except Exception as e:
    # 실패 시엔 명확히 표시하고 중단
    st.error("DB 연결 실패: 앱을 사용할 수 없습니다.")
    st.exception(e)
    st.stop()

# ------------------------------------------------------------
# Secrets(Drive) 읽기
# ------------------------------------------------------------
DRIVE_API_KEY = (st.secrets.get("DRIVE_API_KEY") or os.getenv("DRIVE_API_KEY") or "").strip()
DRIVE_FOLDER_ID = (st.secrets.get("DRIVE_FOLDER_ID") or os.getenv("DRIVE_FOLDER_ID") or "").strip()

# ------------------------------------------------------------
# 탭 UI
# ------------------------------------------------------------
tab_main, tab_qna, tab_pdf = st.tabs([
    "인증기준/조사지침",
    "조사위원 질문",
    "규정검색(PDF파일/본문)"
])

# --------------------- 인증기준/조사지침 (필터 포함) ----------------------------
with tab_main:
    # 큰 제목 제거
    st.write("")

    # 폼 제출 버튼 전역 숨김(Enter로 바로 검색)
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    main_table = _pick_table(eng, ["main_v", "main_raw"]) or "main_raw"
    all_cols = _list_columns(eng, main_table)

    # --- Enter로 제출되는 폼 ---
    with st.form("main_search_form", clear_on_submit=False):
        cols_top = st.columns([2, 1, 1])
        with cols_top[0]:
            kw = st.text_input("키워드 (공백=AND)", st.session_state.get("main_kw", ""), key="main_kw")
        with cols_top[1]:
            f_person = st.text_input("조사대상 (선택)", st.session_state.get("main_filter_person", ""), key="main_filter_person")
        with cols_top[2]:
            f_place = st.text_input("조사장소 (선택)", st.session_state.get("main_filter_place", ""), key="main_filter_place")

        mode = st.radio("검색 대상", ["전체 열", "특정 열 선택", "ME만"], horizontal=True, key="main_mode")
        if mode == "특정 열 선택":
            sel_cols = st.multiselect("검색할 열 선택", options=all_cols,
                                      default=st.session_state.get("main_cols", all_cols), key="main_cols")
        elif mode == "ME만":
            sel_cols = ["ME"] if "ME" in all_cols else all_cols[:1]
            if "ME" not in all_cols:
                st.info("ME 칼럼이 없어 첫 칼럼으로 대체합니다.")
        else:
            sel_cols = None

        FIXED_LIMIT = 1000  # 내부 고정 제한 (UI 노출 없음)
        submitted_main = st.form_submit_button("검색")  # 화면에서는 숨김 처리됨

    # ---- 검색 실행 → 세션 저장 ----
    combined_kw = " ".join([s for s in [
        st.session_state.get("main_kw",""),
        st.session_state.get("main_filter_person",""),
        st.session_state.get("main_filter_place","")
    ] if str(s).strip()]).strip()

    if submitted_main and combined_kw:
        with st.spinner("검색 중..."):
            _df = search_table_any(eng, main_table, combined_kw, columns=sel_cols, limit=FIXED_LIMIT)
        if _df.empty:
            st.info("결과 없음")
            st.session_state.pop("main_results", None)
        else:
            st.session_state["main_results"] = _df.to_dict("records")

    # ===== 하이라이트/탐지 규칙 =====
    # - '조사항목' 컬럼(이름에 '조사항목' 또는 '항목' 포함)을 파란색 굵게
    # - '등급' 값이 '필수' → 빨간색 굵게
    def _find_item_col(cols):
        lowers = [(c, str(c).lower()) for c in cols]
        for c, lc in lowers:
            if lc == "조사항목":
                return c
        for c, lc in lowers:
            if "조사항목" in lc:
                return c
        for c, lc in lowers:
            if "항목" in lc:
                return c
        return None

    def _find_grade_col(cols):
        lowers = [(c, str(c).lower()) for c in cols]
        for c, lc in lowers:
            if lc == "등급":
                return c
        for c, lc in lowers:
            if "등급" in lc:
                return c
        for c, lc in lowers:
            if "필수" in lc:
                return c
        return None

    ITEM_COL = _find_item_col(all_cols)
    GRADE_COL = _find_grade_col(all_cols)

    # ===== 공통 스타일(CSS) : 너비·최소폭 지정 없음, 반응형 미디어쿼리만 =====
    st.markdown("""
<style>
.hl-item{ color:#0d47a1; font-weight:800; }          /* 조사항목 파랑 굵게 */
.hl-required{ color:#b10000; font-weight:800; }      /* 등급=필수 빨강 굵게 */

/* 카드 */
.card{border:1px solid #e9ecef;border-radius:10px;padding:12px 14px;margin:8px 0;background:#fff}
.card h4{margin:0 0 8px 0;font-size:16px;line-height:1.3;word-break:break-word}
.card .row{margin:4px 0;font-size:13px;color:#333;word-break:break-word}
.card .lbl{display:inline-block;min-width:96px;color:#6c757d}

/* 표형(HTML 테이블) : 자동 레이아웃 + 줄바꿈 + 반응형 */
.table-wrap{ overflow-x:auto; }
.table-wrap table{
  width:100%; border-collapse:collapse; background:#fff; table-layout:auto;
}
.table-wrap th, .table-wrap td{
  border:1px solid #e9ecef; padding:8px 10px; text-align:left; vertical-align:top;
  font-size:13px; word-break:break-word; overflow-wrap:anywhere; white-space:normal;
}
.table-wrap th{ background:#f8f9fa; font-weight:700; }

/* ↓ 화면이 줄어들수록 글자/패딩 축소 */
@media (max-width: 1400px){
  .table-wrap th, .table-wrap td{ font-size:12px; padding:6px 8px; }
}
@media (max-width: 1200px){
  .table-wrap th, .table-wrap td{ font-size:11.5px; padding:5px 7px; }
}
@media (max-width: 1024px){
  .table-wrap th, .table-wrap td{ font-size:11px; padding:5px 6px; }
}
@media (max-width: 900px){
  .table-wrap th, .table-wrap td{ font-size:10.5px; padding:4px 5px; }
}
@media (max-width: 768px){
  .table-wrap th, .table-wrap td{ font-size:10px; padding:3px 4px; }
}
</style>
    """, unsafe_allow_html=True)

    # ===== 셀 포맷(강조 적용) =====
    def _fmt_cell(colname: str, value) -> str:
        s = html.escape("" if value is None else str(value))
        def _is_required(val: str) -> bool:
            t = (val or "").strip().replace(" ", "").lower()
            return t in ("필수", "必須")
        if ITEM_COL and colname == ITEM_COL and s:
            return f'<span class="hl-item">{s}</span>'
        if GRADE_COL and colname == GRADE_COL and _is_required(s):
            return f'<span class="hl-required">{s}</span>'
        return s

    # ===== 카드 렌더러 =====
    def render_cards(df_: pd.DataFrame):
        for _, r in df_.iterrows():
            title_val = r[df_.columns[0]]
            title_html = _fmt_cell(df_.columns[0], title_val)
            rows_html = []
            for c in df_.columns:
                v_html = _fmt_cell(c, r[c])
                rows_html.append(f'<div class="row"><span class="lbl">{html.escape(str(c))}</span> {v_html}</div>')
            st.markdown(f'<div class="card"><h4>{title_html}</h4>' + "".join(rows_html) + '</div>', unsafe_allow_html=True)

    # ===== 표형 렌더러 : colgroup/width 지정 없음(완전 자동) =====
    def render_table(df_: pd.DataFrame):
        cols = list(df_.columns)

        # 헤더
        header_cells = "".join(f"<th>{html.escape(str(c))}</th>" for c in cols)

        # 바디
        body_rows = []
        for _, r in df_.iterrows():
            cells = "".join(f"<td>{_fmt_cell(c, r[c])}</td>" for c in cols)
            body_rows.append(f"<tr>{cells}</tr>")

        table_html = f"""
<div class="table-wrap">
  <table>
    <thead><tr>{header_cells}</tr></thead>
    <tbody>
      {''.join(body_rows)}
    </tbody>
  </table>
</div>
"""
        st.markdown(table_html, unsafe_allow_html=True)

    # ---- 결과 렌더링 ----
    if "main_results" in st.session_state and st.session_state["main_results"]:
        df = pd.DataFrame(st.session_state["main_results"])
        st.write(f"결과: {len(df):,}건")

        view_mode = st.radio("보기 형식", ["카드형(모바일)", "표형"], horizontal=True, key="main_view_mode")
        if view_mode == "표형":
            render_table(df)      # 자동 너비 + 색 하이라이트
        else:
            render_cards(df)      # 카드에서 색 하이라이트
    else:
        st.caption("힌트: 조사대상/조사장소 단어는 메인 키워드와 AND로 결합되어 검색됩니다.")

# --------------------- 조사위원 질문 -----------------------------
with tab_qna:
    # 큰 제목은 생략
    st.write("")

    # 폼 제출 버튼 숨김(Enter로 검색)
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    qna_table = _pick_table(eng, ["qna_v", "qna_raw"]) or "qna_raw"

    # ====== 입력폼 (Enter 제출) ======
    with st.form("qna_search_form", clear_on_submit=False):
        kw_q = st.text_input(
            "키워드 (공백=AND)",
            st.session_state.get("qna_kw", ""),
            key="qna_kw",
            placeholder="예) 낙상, 환자확인, 수술 체크리스트 등"
        )
        FIXED_LIMIT_QNA = 1000   # 내부 고정 제한
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

    # ====== 컬럼 자동 매핑 헬퍼 ======
    def _norm_key(s: str) -> str:
        # 비교용: 공백/쉼표/슬래시/하이픈 제거 + 소문자
        return re.sub(r"[\s,/_\-]+", "", str(s or "")).lower()

    def _pick_col(cols, candidates):
        """cols 중 candidates와 '정확 일치' → '정규화 일치' → '부분 포함' 순으로 선택."""
        # 1) 정확 일치
        for want in candidates:
            for c in cols:
                if str(c).strip() == want:
                    return c
        # 2) 정규화 일치
        wants_norm = [_norm_key(want) for want in candidates]
        for c in cols:
            if _norm_key(c) in wants_norm:
                return c
        # 3) 부분 포함(완전 느슨)
        for want in candidates:
            w = _norm_key(want)
            for c in cols:
                if w and w in _norm_key(c):
                    return c
        return None

    # ====== 카드 렌더러 (표형 없음) ======
    def render_qna_cards(df_: pd.DataFrame, col_place, col_combined, col_q=None, col_c=None):
        st.markdown("""
<style>
.qcard{border:1px solid #e9ecef;border-radius:12px;padding:12px 14px;margin:10px 0;background:#fff}
.qtitle{font-size:15px;font-weight:800;margin-bottom:8px;word-break:break-word;color:#0d47a1}
.qbody{font-size:13px;color:#333}
.qline{margin:6px 0;word-break:break-word}
.qlbl{display:inline-block;min-width:132px;color:#6c757d;font-weight:700}
@media (max-width: 900px){
  .qlbl{min-width:110px}
}
</style>
        """, unsafe_allow_html=True)

        import html as _html

        for _, r in df_.iterrows():
            place_raw = "" if pd.isna(r.get(col_place)) else str(r.get(col_place))

            if col_combined is not None:
                # 1순위: '조사위원 질문, 확인내용' 단일 컬럼 그대로 사용
                content_raw = "" if pd.isna(r.get(col_combined)) else str(r.get(col_combined))
            else:
                # 2순위: 질문/확인내용 두 컬럼을 합쳐 한 줄
                q_raw = "" if (col_q is None or pd.isna(r.get(col_q))) else str(r.get(col_q))
                c_raw = "" if (col_c is None or pd.isna(r.get(col_c))) else str(r.get(col_c))
                qn = re.sub(r"\s+", " ", q_raw or "").strip()
                cn = re.sub(r"\s+", " ", c_raw or "").strip()
                if qn and cn:
                    if _norm_key(qn) == _norm_key(cn):
                        content_raw = qn
                    else:
                        # 서로 다르면 '질문 / 확인내용'을 합쳐 한 줄로
                        content_raw = f"{qn} / {cn}"
                elif qn:
                    content_raw = qn
                elif cn:
                    content_raw = cn
                else:
                    content_raw = "-"

            place = _html.escape(place_raw)
            content = _html.escape(content_raw)

            st.markdown(
                f"""
<div class="qcard">
  <div class="qtitle">{place if place else "조사장소 미지정"}</div>
  <div class="qbody">
    <div class="qline"><span class="qlbl">조사위원 질문, 확인내용</span> {content}</div>
  </div>
</div>
                """,
                unsafe_allow_html=True
            )

    # ====== 결과 표시 (카드형만) ======
    if "qna_results" in st.session_state and st.session_state["qna_results"]:
        df = pd.DataFrame(st.session_state["qna_results"])

        # 자동 인덱스는 표시하지 않으며, 'No/번호/순번' 유사 식별자 컬럼은 제거
        drop_like = [c for c in df.columns if _norm_key(c) in ("no", "번호", "순번")]
        if drop_like:
            df = df.drop(columns=drop_like)

        # ① 조사장소 컬럼 찾기
        COL_PLACE = _pick_col(df.columns, ["조사장소", "장소", "부서/장소", "부서", "조사 장소", "조사 부서"])

        # ② '조사위원 질문, 확인내용' 단일 컬럼 찾기(우선)
        COL_COMBINED = _pick_col(df.columns, [
            "조사위원 질문, 확인내용",
            "조사위원질문, 확인내용",
            "조사위원 질문,확인내용",
            "조사위원질문,확인내용",
            "질문, 확인내용",
            "질문/확인내용",
            "질문확인내용",
            "조사위원질문확인내용"
        ])

        # ③ 없으면 질문/확인내용을 각각 찾아서 합치기(후순위)
        COL_Q = None
        COL_C = None
        if COL_COMBINED is None:
            COL_Q = _pick_col(df.columns, ["조사위원 질문", "조사위원질문", "질문", "QnA", "질문사항"])
            COL_C = _pick_col(df.columns, ["확인내용", "확인 내용", "확인사항", "확인 내역", "확인내용(기록)", "확인 근거"])

        # 필수 보정
        if COL_PLACE is None:
            COL_PLACE = df.columns[0]
        # (COMBINED/Q/C 중 아무 것도 없으면 마지막 컬럼을 임시로 콘텐츠로 사용)
        if COL_COMBINED is None and COL_Q is None and COL_C is None:
            COL_COMBINED = df.columns[-1]

        st.write(f"결과: {len(df):,}건")
        render_qna_cards(df, COL_PLACE, COL_COMBINED, COL_Q, COL_C)
    # else: 별도 힌트는 생략

# --------------------- 규정검색(PDF파일/본문) (Google Drive 전용) -------
with tab_pdf:
    # 큰 제목 제거 + 상단 라인 제거
    st.write("")

    # 폼 제출 버튼 전역 숨김
    st.markdown("<style>div[data-testid='stFormSubmitButton']{display:none!important;}</style>", unsafe_allow_html=True)

    # 인덱스 버튼: 최초 접속 시에만 노출
    drive_done = st.session_state.get("drive_index_done", False)
    if not drive_done:
        cols1 = st.columns([1, 2, 1])
        with cols1[1]:
            if st.button("인덱스(Drive)", key="pdf_reindex_drive"):
                if not (DRIVE_API_KEY and DRIVE_FOLDER_ID and "?" not in DRIVE_FOLDER_ID):
                    st.error("Secrets에 DRIVE_API_KEY / DRIVE_FOLDER_ID(쿼리스트링 제거) 를 설정하세요.")
                else:
                    with st.spinner("Google Drive 인덱싱 중..."):
                        _ = index_pdfs_from_drive(eng, DRIVE_FOLDER_ID, DRIVE_API_KEY)
                    st.session_state["drive_index_done"] = True
                    st.rerun()

    # 2) 검색 조건 (Enter 제출 폼, 최대결과 입력 제거)
    FIXED_LIMIT = 1000  # 내부 고정 제한
    with st.form("pdf_search_form", clear_on_submit=False):
        kw_pdf  = st.text_input("키워드 (공백=AND)", "", key="pdf_kw")
        fn_like = st.text_input("파일명 필터(선택)", "", key="pdf_fn")
        submitted_pdf = st.form_submit_button("검색")  # 화면에는 숨김

    # 제출 시 검색 → 세션 저장
    if submitted_pdf and kw_pdf.strip():
        with st.spinner("검색 중..."):
            _df = search_regs(eng, kw_pdf, filename_like=fn_like, limit=FIXED_LIMIT)

        if "me" in _df.columns:
            _df = _df[_df["me"].astype(str).str.strip() != ""]
        _df = _df.sort_values(["filename", "page"], kind="stable").reset_index(drop=True)

        if _df.empty:
            st.info("조건에 맞는 결과가 없습니다. 먼저 [인덱스(Drive)]를 수행했는지 확인하세요.")
            st.session_state.pop("pdf_results", None)
            st.session_state.pop("pdf_sel_idx", None)
            st.session_state.pop("pdf_kw_list", None)
        else:
            st.session_state["pdf_results"] = _df.to_dict("records")
            st.session_state["pdf_sel_idx"] = 0
            st.session_state["pdf_kw_list"] = [k.strip() for k in kw_pdf.split() if k.strip()]

    # 3) 결과 렌더링 (이하 기존과 동일)
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

        # ---- 선택된 행으로 미리보기 (pdf.js 렌더 포함: 생략 없이 기존 동작 유지)
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

        # ---- 미리보기 컨트롤 (페이지/줌/높이)
        page_view = st.number_input("미리보기 페이지", 1, 9999, int(sel_page), step=1, key=f"pv_page_{fid}")
        zoom_pct  = st.slider("줌(%)", 30, 200, 80, step=5, key=f"pv_zoom_{fid}")
        height_px = st.slider("미리보기 높이(px)", 480, 1200, 640, step=40, key=f"pv_h_{fid}")

        # ---- pdf.js 렌더 (모바일 최적화 포함)
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
        st.caption("먼저 키워드를 입력하고 **키보드 Enter**를 누르면 결과가 표시됩니다.")











