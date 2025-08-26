# -*- coding: utf-8 -*-
import os, re, html
from pathlib import Path
from typing import List
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
    """폴더의 PDF를 파일별 최신 mtime 기준으로 incremental 인덱싱
       ('.ipynb_checkpoints' 경로는 자동 제외)"""
    ensure_reg_table(eng)
    pdfs = sorted(folder.rglob("*.pdf"))
    if not pdfs:
        return {"indexed": 0, "skipped": 0, "errors": 0, "files": []}

    indexed = skipped = errors = 0
    done_files = []

    with eng.begin() as con:
         # >>> 기존에 들어간 체크포인트 파일들 싹 정리(한 번 실행해두면 좋음)
        con.execute(text(r"delete from regulations where filename ~* '(^|[\\/])\.ipynb_checkpoints([\\/]|$)'"))

        for f in pdfs:
            # === 체크포인트 폴더 제외 ===
            if any(part == ".ipynb_checkpoints" for part in f.parts):
                skipped += 1
                try:
                    fn_skip = str(f.relative_to(folder))
                except Exception:
                    fn_skip = str(f)
                done_files.append((fn_skip, "skip(checkpoints)"))
                continue
            # =========================

            try:
                fn = str(f.relative_to(folder))
            except Exception:
                fn = str(f)
            mt = int(f.stat().st_mtime)

            # 이미 최신으로 인덱싱되어 있으면 스킵
            row = con.execute(
                text("select count(*) from regulations where filename=:fn and file_mtime=:mt"),
                {"fn": fn, "mt": mt},
            ).scalar()
            if row and row > 0:
                skipped += 1
                done_files.append((fn, "skip"))
                continue

            # 기존 파일 레코드 제거 후 재적재
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

    # >>> .ipynb_checkpoints 숨기기(정규식)
    # 슬래시/백슬래시 둘 다 경로 구분자로 인식
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
# 탭 UI
# ------------------------------------------------------------
tab_main, tab_qna, tab_pdf = st.tabs(
    ["Main 조회", "QnA 조회", "PDF 검색"]
)

# --------------------- Main 조회 (필터 포함 통합검색) ----------------------------
with tab_main:
    st.subheader("Main 조회")

    main_table = _pick_table(eng, ["main_v", "main_raw"]) or "main_raw"
    all_cols = _list_columns(eng, main_table)

    # --- (선택) 자동 컬럼 매핑: 실제 컬럼명이 다를 수 있으니 후보에서 1개 자동 선택
    CAND_PLACE  = ["조사장소", "장소", "location", "place", "부서", "dept"]
    CAND_TARGET = ["조사대상", "대상", "target", "role", "직군", "직무"]

    def _pick_col(cands):
        for c in cands:
            if c in all_cols:
                return c
        return None

    col_place  = _pick_col(CAND_PLACE)
    col_target = _pick_col(CAND_TARGET)

    # --- 검색 대상 열 선택(기존 기능 유지)
    kw = st.text_input("전체 키워드 (공백=AND · 선택된 열에서 검색)", "", key="main_kw")

    mode = st.radio(
        "전체 키워드 검색 대상",
        ["전체 열", "특정 열 선택", "ME만"],
        horizontal=True,
        key="main_mode"
    )

    if mode == "특정 열 선택":
        sel_cols = st.multiselect("검색할 열 선택", options=all_cols, default=all_cols, key="main_cols")
    elif mode == "ME만":
        sel_cols = ["ME"] if "ME" in all_cols else all_cols[:1]
        if "ME" not in all_cols:
            st.info("ME 칼럼이 없어 첫 칼럼으로 대체합니다.")
    else:
        sel_cols = None  # 전체 열

    st.divider()

    # --- 필터 조건(단어 1개, 부분일치) : 조사장소 / 조사대상
    c1, c2 = st.columns(2)
    with c1:
        place_word = st.text_input(
            f"조사장소 필터(단어 1개 · 부분일치) [{col_place or '컬럼없음'}]",
            "",
            key="flt_place_one",
            help="예: 원무  → 조사장소에 '원무'가 포함된 행만"
        )
    with c2:
        target_word = st.text_input(
            f"조사대상 필터(단어 1개 · 부분일치) [{col_target or '컬럼없음'}]",
            "",
            key="flt_target_one",
            help="예: 간호사 → 조사대상에 '간호사'가 포함된 행만"
        )

    # --- 추가 컬럼 필터: 필요한 컬럼을 선택하고, 각자에 부분일치 단어 1개 입력
    st.markdown("**추가 컬럼 필터(선택)** — 필요한 컬럼을 선택하고 각자 검색어(부분일치)를 입력하세요.")
    extra_cols = st.multiselect(
        "추가로 필터할 컬럼들 선택",
        options=[c for c in all_cols if c not in {col_place, col_target}],
        default=[],
        key="extra_cols"
    )
    extra_terms = {}
    if extra_cols:
        cols_box = st.columns(min(3, len(extra_cols)))
        for i, c in enumerate(extra_cols):
            with cols_box[i % len(cols_box)]:
                extra_terms[c] = st.text_input(f"{c} 포함 단어", "", key=f"extra_term_{c}")

    limit = st.number_input("최대 행수", 1, 10000, 1000, step=100, key="main_lim2")

    if st.button("검색", key="main_search2"):
        where_parts, params = [], {}

        # 1) 조사장소 필터
        if place_word.strip() and col_place:
            where_parts.append(f'"{col_place}" ILIKE :place')
            params["place"] = f"%{place_word.strip()}%"

        # 2) 조사대상 필터
        if target_word.strip() and col_target:
            where_parts.append(f'"{col_target}" ILIKE :target')
            params["target"] = f"%{target_word.strip()}%"

        # 3) 추가 컬럼 필터(각 컬럼별 부분일치)
        for c, term in (extra_terms or {}).items():
            t = (term or "").strip()
            if t:
                pkey = f"extra_{len(params)}"
                where_parts.append(f'"{c}" ILIKE :{pkey}')
                params[pkey] = f"%{t}%"

        # 4) 전체 키워드(공백=AND) — 선택된 열(sel_cols) 대상, 없으면 전체 열 대상
        if kw.strip():
            tokens = [t for t in kw.split() if t.strip()]
            cols_for_kw = sel_cols if sel_cols else all_cols
            for i, t in enumerate(tokens):
                # 선택된(또는 전체) 열 중 어느 하나에라도 포함되면 통과
                ors = " OR ".join([f'CAST("{c}" AS TEXT) ILIKE :kw{i}' for c in cols_for_kw])
                where_parts.append(f"({ors})")
                params[f"kw{i}"] = f"%{t}%"

        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
        sql = text(f'SELECT * FROM "{main_table}" WHERE {where_sql} LIMIT :lim')
        params["lim"] = int(limit)

        with st.spinner("검색 중..."):
            with eng.begin() as con:
                df = pd.read_sql_query(sql, con, params=params)

        st.write(f"결과: {len(df):,}건")
        if df.empty:
            st.info("결과 없음")
        else:
            st.dataframe(df, use_container_width=True, height=520)

            # (선택) CSV 다운로드
            csv = df.to_csv(index=False).encode("utf-8-sig")
            st.download_button("CSV 다운로드", csv, "main_filtered_search.csv", "text/csv")

    else:
        st.caption("순서: (필요시) 조사장소/조사대상/추가 컬럼 필터 입력 → 전체 키워드 입력 → [검색] 클릭")

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
        st.caption("필요 시 ‘임시 SQL’에서 직접 SELECT 실행 가능.")


# --------------------- PDF 검색 (HTTP 서버 방식 고정) -------
with tab_pdf:
    st.subheader("PDF 검색")

    # 1) 인덱싱 관련
    default_folder = str((Path.cwd() / "PDFs").resolve())
    folder_str = st.text_input("PDF 폴더 경로", default_folder, key="pdf_folder")
    folder = Path(folder_str)

    cols1 = st.columns(3)
    with cols1[0]:
        if st.button("인덱스 갱신", key="pdf_reindex"):
            if not folder.exists():
                st.error(f"폴더가 없습니다: {folder}")
            else:
                with st.spinner("PDF 인덱싱 중... (처음은 다소 걸릴 수 있습니다)"):
                    rep = index_pdfs(eng, folder)
                st.success(f"인덱스 완료 | indexed={rep['indexed']} | skipped={rep['skipped']} | errors={rep['errors']}")
                with st.expander("상세 로그 보기"):
                    for fn, stat in rep["files"]:
                        st.write(f"- {fn}: {stat}")

    st.markdown(
        "🔗 **링크 방식: HTTP 서버(권장)** &nbsp;&nbsp;"
        "`cd /d D:\\Anaconda\\PDFs && python -m http.server 8010` 을 별도 터미널에서 실행하세요."
    )
    http_base = st.text_input(
        "HTTP 서버 주소",
        value="http://localhost:8010",
        key="pdf_http_base",
        help="다른 PC에서 열려면 http://<내PC IP>:8010 로 변경 (예: http://192.168.0.23:8010)"
    )
    base_http = http_base.rstrip("/")

    st.divider()

    # 2) 검색 조건
    kw_pdf = st.text_input("키워드 (공백=AND)", "", key="pdf_kw")
    fn_like = st.text_input("파일명 필터(선택)", "", key="pdf_fn")
    limit_pdf = st.number_input("최대 결과", 1, 5000, 500, step=100, key="pdf_lim")

    # 3) 검색 실행
    if st.button("검색", key="pdf_search") and kw_pdf.strip():
        with st.spinner("검색 중..."):
            df = search_regs(eng, kw_pdf, filename_like=fn_like, limit=int(limit_pdf))
        st.write(f"결과: {len(df):,}건")
        if df.empty:
            st.info("조건에 맞는 결과가 없습니다.")
        else:
            kw_list = [k.strip() for k in kw_pdf.split() if k.strip()]

            # HTTP 링크 만들기 (상대경로 → URL 인코딩, 슬래시 유지)
            def make_href(row):
                rel_web = str(row["filename"]).replace("\\", "/")
                rel_enc = quote(rel_web, safe="/")  # '/'는 유지하고 한글/공백만 인코딩
                return f"{base_http}/{rel_enc}#page={int(row['page'])}"

            view = df[["filename", "page", "me"]].copy()
            view["open"] = df.apply(make_href, axis=1)

            st.dataframe(
                view,
                use_container_width=True,
                height=520,
                column_config={"open": st.column_config.LinkColumn("열기", display_text="열기")},
            )

            # 미리보기(하이라이트 + iframe)
            st.caption("행을 선택해 본문 스니펫과 문서 미리보기를 확인하세요.")
            with st.expander("텍스트 미리보기 & 문서 보기 (선택한 1건)"):
                idx = st.number_input(
                    "행 번호(0부터)",
                    min_value=0, max_value=len(df) - 1, value=0, step=1,
                    key="pdf_preview_idx"
                )
                row = df.iloc[int(idx)]
                st.write(f"**파일**: {row['filename']}  |  **페이지**: {int(row['page'])}  |  **ME**: {row.get('me') or '-'}")
                st.markdown(highlight_html(row["text"], kw_list, width=200), unsafe_allow_html=True)

                # iframe 미리보기 (HTTP 서버가 켜져 있어야 보임)
                href = make_href(row)
                st.components.v1.html(
                    f'<iframe src="{href}" style="width:100%; height:720px;" frameborder="0"></iframe>',
                    height=740,
                )

    else:
        st.caption("먼저 [인덱스 갱신]을 한 번 수행해야 검색이 잘 동작합니다.")