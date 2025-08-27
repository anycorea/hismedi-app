# -*- coding: utf-8 -*-
import os, re, io, html, requests
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
st.set_page_config(page_title="★★★ HISMEDI 인증 준비 ★★★", layout="wide")
st.markdown("# HISMEDI - 지침/QnA/규정")
# 하이라이트 색(선택)
st.markdown("<style> mark { background: #ffe2a8; } </style>", unsafe_allow_html=True)

# ------------------------------------------------------------
# 옵션: 간단 접근 비밀번호 (Secrets에 APP_PASSWORD 넣었을 때만 작동)
# ------------------------------------------------------------
_APP_PW = (st.secrets.get("APP_PASSWORD") or os.getenv("APP_PASSWORD") or "").strip()
if _APP_PW:
    pw = st.text_input("접속 비밀번호", type="password")
    if pw.strip() != _APP_PW:
        st.stop()

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
# 연결 배너
# ------------------------------------------------------------
eng = get_engine()
try:
    with eng.begin() as con:
        user, port, now = con.execute(
            text("select current_user, inet_server_port(), now()")
        ).one()
    st.success(f"DB 연결 OK | user={user} | port={port} | time={now}")
except Exception as e:
    st.exception(e); st.stop()

# ------------------------------------------------------------
# Secrets(Drive) 읽기
# ------------------------------------------------------------
DRIVE_API_KEY = (st.secrets.get("DRIVE_API_KEY") or os.getenv("DRIVE_API_KEY") or "").strip()
DRIVE_FOLDER_ID = (st.secrets.get("DRIVE_FOLDER_ID") or os.getenv("DRIVE_FOLDER_ID") or "").strip()

# ------------------------------------------------------------
# 탭 UI
# ------------------------------------------------------------
tab_main, tab_qna, tab_pdf = st.tabs(["Main 조회", "QnA 조회", "PDF 검색"])

# --------------------- Main 조회 (필터 포함) ----------------------------
with tab_main:
    st.subheader("Main 조회")
    main_table = _pick_table(eng, ["main_v", "main_raw"]) or "main_raw"
    all_cols = _list_columns(eng, main_table)

    cols_top = st.columns([2, 1, 1])
    with cols_top[0]:
        kw = st.text_input("키워드 (공백=AND)", "", key="main_kw")
    with cols_top[1]:
        f_person = st.text_input("조사대상 (선택)", "", key="main_filter_person")
    with cols_top[2]:
        f_place = st.text_input("조사장소 (선택)", "", key="main_filter_place")

    mode = st.radio("검색 대상", ["전체 열", "특정 열 선택", "ME만"], horizontal=True, key="main_mode")
    if mode == "특정 열 선택":
        sel_cols = st.multiselect("검색할 열 선택", options=all_cols, default=all_cols, key="main_cols")
    elif mode == "ME만":
        sel_cols = ["ME"] if "ME" in all_cols else all_cols[:1]
        if "ME" not in all_cols:
            st.info("ME 칼럼이 없어 첫 칼럼으로 대체합니다.")
    else:
        sel_cols = None

    limit = st.number_input("최대 행수", 1, 5000, 500, step=100, key="main_lim")

    combined_kw = " ".join([s for s in [kw, f_person, f_place] if str(s).strip()]).strip()

    if st.button("검색", key="main_search") and combined_kw:
        with st.spinner("검색 중..."):
            df = search_table_any(eng, main_table, combined_kw, columns=sel_cols, limit=limit)
        st.write(f"결과: {len(df):,}건")
        if df.empty:
            st.info("결과 없음")
        else:
            st.dataframe(df, use_container_width=True, height=520)
    else:
        st.caption("힌트: 조사대상/조사장소 단어는 메인 키워드와 AND로 결합되어 검색됩니다.")

# --------------------- QnA 조회 -----------------------------
with tab_qna:
    st.subheader("QnA 조회")
    qna_table = _pick_table(eng, ["qna_v", "qna_raw"]) or "qna_raw"
    kw_q = st.text_input("키워드 (공백=AND)", "", key="qna_kw")
    limit_q = st.number_input("최대 행수", 1, 5000, 500, step=100, key="qna_lim")

    if st.button("검색", key="qna_search") and kw_q.strip():
        with st.spinner("검색 중..."):
            df = search_table_any(eng, qna_table, kw_q, columns=None, limit=limit_q)
        st.write(f"결과: {len(df):,}건")
        if df.empty:
            st.info("결과 없음")
        else:
            st.dataframe(df, use_container_width=True, height=520)
    else:
        st.caption("필요 시 ‘임시 SQL’ 탭에서 직접 SELECT 실행 가능합니다.")

# --------------------- PDF 검색 (Google Drive 전용) -------
with tab_pdf:
    st.subheader("PDF 검색 (Google Drive)")
    st.divider()

    # 1) 인덱싱: Google Drive만 사용
    cols1 = st.columns([1, 2, 1])
    with cols1[1]:
        if st.button("인덱스(Drive)", key="pdf_reindex_drive"):
            if not (DRIVE_API_KEY and DRIVE_FOLDER_ID and "?" not in DRIVE_FOLDER_ID):
                st.error("Secrets에 DRIVE_API_KEY / DRIVE_FOLDER_ID(쿼리스트링 제거) 를 설정하세요.")
            else:
                with st.spinner("Google Drive에서 인덱싱 중..."):
                    rep = index_pdfs_from_drive(eng, DRIVE_FOLDER_ID, DRIVE_API_KEY)
                st.success(f"Drive 인덱스 완료 | indexed={rep['indexed']} | skipped={rep['skipped']} | errors={rep['errors']}")
                with st.expander("상세 로그 보기"):
                    for fn, stat in rep["files"]:
                        st.write(f"- {fn}: {stat}")

    st.divider()

    # 2) 검색 조건
    kw_pdf   = st.text_input("키워드 (공백=AND)", "", key="pdf_kw")
    fn_like  = st.text_input("파일명 필터(선택)", "", key="pdf_fn")
    limit_pdf = st.number_input("최대 결과", 1, 5000, 500, step=100, key="pdf_lim")

    # 3) 검색 실행 (Drive 인덱스 레코드만 대상으로)
    if st.button("검색", key="pdf_search") and kw_pdf.strip():
        with st.spinner("검색 중..."):
            df = search_regs(eng, kw_pdf, filename_like=fn_like, limit=int(limit_pdf))

        # me(Drive file_id) 있는 행만 사용 → Drive 인덱싱 결과만
        if "me" in df.columns:
            df = df[df["me"].astype(str).str.strip() != ""]

        st.write(f"결과: {len(df):,}건")
        if df.empty:
            st.info("조건에 맞는 결과가 없습니다. 먼저 [인덱스(Drive)]를 수행했는지 확인하세요.")
        else:
            # --- 링크 유틸 ---
            # 클릭(새 탭)용: /view#page=n
            # iframe(앱 내 미리보기)용: /preview#page=n
            def make_click_url_from_vals(fid: str, page: int) -> str:
                fid = (fid or "").strip()
                return f"https://drive.google.com/file/d/{fid}/view#page={int(page)}"

            def make_iframe_url_from_vals(fid: str, page: int) -> str:
                fid = (fid or "").strip()
                return f"https://drive.google.com/file/d/{fid}/preview#page={int(page)}"

            # --- 표: HTML로 렌더링(의존성 없음, 링크 클릭 보장) ---
            view = df[["filename", "page", "me"]].copy()
            view.rename(columns={"filename": "파일명", "page": "페이지", "me": "file_id"}, inplace=True)

            view["열기"] = view.apply(
                lambda r: (
                    f'<a href="{make_click_url_from_vals(r["file_id"], r["페이지"])}" '
                    f'target="_blank" rel="noopener noreferrer">열기</a>'
                ),
                axis=1,
            )

            html_table = view[["파일명", "페이지", "열기"]].to_html(index=False, escape=False)
            st.write(html_table, unsafe_allow_html=True)

            # --- 미리보기(하이라이트 + iframe) ---
            kw_list = [k.strip() for k in kw_pdf.split() if k.strip()]

            st.caption("행 번호를 선택해 본문 스니펫과 문서 미리보기를 확인하세요.")
            with st.expander("텍스트 미리보기 & 문서 보기 (선택한 1건)"):
                idx = st.number_input(
                    "행 번호(0부터)",
                    min_value=0, max_value=len(df) - 1, value=0, step=1,
                    key="pdf_preview_idx"
                )
                row = df.iloc[int(idx)]
                st.write(
                    f"**파일**: {row['filename']}  |  **페이지**: {int(row['page'])}  |  **file_id**: {row.get('me') or '-'}"
                )

                st.markdown(
                    highlight_html(row["text"], kw_list, width=200),
                    unsafe_allow_html=True
                )

                iframe_src = make_iframe_url_from_vals(row.get("me"), int(row["page"]))  # /preview
                st.components.v1.html(
                    f'<iframe src="{iframe_src}" style="width:100%; height:720px;" frameborder="0"></iframe>',
                    height=740,
                )
    else:
        st.caption("먼저 [인덱스(Drive)] 버튼으로 Google Drive 내 PDF를 인덱싱하세요. 그 후 키워드로 검색할 수 있습니다.")

