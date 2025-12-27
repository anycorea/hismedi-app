import os
import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# ---------------------------
# Google Sheets client
# ---------------------------
@st.cache_resource
def get_gspread_client():
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON in Streamlit secrets/env.")
    creds = Credentials.from_service_account_info(
        eval(sa_json) if sa_json.startswith("{") is False else None,  # fallback, will be overwritten below
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )

def _build_client():
    import streamlit as st

    if "gcp_service_account" not in st.secrets:
        raise RuntimeError("Missing [gcp_service_account] in Streamlit Secrets")

    info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

@st.cache_resource
def get_sheet():
    sheet_id = os.getenv("GSHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("Missing GSHEET_ID")
    gc = _build_client()
    return gc.open_by_key(sheet_id)

# ---------------------------
# Load data
# ---------------------------
@st.cache_data(ttl=60)
def load_news_and_meta():
    sh = get_sheet()
    ws_news = sh.worksheet("NEWS")
    ws_meta = sh.worksheet("META")

    news_values = ws_news.get_all_values()
    meta_values = ws_meta.get_all_values()

    df = pd.DataFrame(news_values[1:], columns=news_values[0]) if len(news_values) > 1 else pd.DataFrame(columns=news_values[0] if news_values else [])
    meta = {}
    if len(meta_values) > 1:
        for r in meta_values[1:]:
            if len(r) >= 2 and r[0]:
                meta[r[0]] = r[1]
    return df, meta

def parse_dt(s):
    if not s:
        return pd.NaT
    try:
        # ISO 8601 포함(UTC) 전제
        return pd.to_datetime(s, utc=True, errors="coerce").dt.tz_convert(KST)
    except Exception:
        return pd.NaT

def today_kst_date():
    return datetime.now(KST).date()

# ---------------------------
# UI
# ---------------------------
st.set_page_config(page_title="HISMEDI News Monitor", layout="wide")
st.title("📰 보건·의료·노동 뉴스 모니터")

with st.sidebar:
    st.subheader("데이터")
    if st.button("🔄 새로고침 (시트 다시 읽기)"):
        st.cache_data.clear()

df, meta = load_news_and_meta()

# 표준 컬럼 보정
if df.empty:
    st.info("NEWS 탭에 데이터가 아직 없습니다. (Actions가 기사 0건이거나 첫 실행 직후일 수 있어요)")
else:
    # 날짜 파싱
    if "published_at" in df.columns:
        df["published_at_dt"] = pd.to_datetime(df["published_at"], utc=True, errors="coerce").dt.tz_convert(KST)
    else:
        df["published_at_dt"] = pd.NaT

    # 태그 정리
    if "tags" not in df.columns:
        df["tags"] = ""

    # 중복 처리
    if "duplicate_of" not in df.columns:
        df["duplicate_of"] = ""

    # 기본 정렬
    df = df.sort_values("published_at_dt", ascending=False, na_position="last")

# ---- 상단 상태 카드 ----
colA, colB, colC, colD = st.columns(4)
colA.metric("마지막 실행(UTC)", meta.get("last_run_at", "-"))
colB.metric("마지막 적재 건수", meta.get("last_inserted_count", "-"))
colC.metric("마지막 에러", meta.get("last_error", "") or "없음")
colD.metric("총 기사 수(NEWS)", f"{len(df):,}" if not df.empty else "0")

st.divider()

# ---- 필터 ----
left, right = st.columns([2, 3])

with left:
    st.subheader("필터")

    only_today = st.toggle("오늘 들어온 기사만", value=True)
    dedup_toggle = st.toggle("중복 기사 제외(duplicate_of 비움)", value=True)

    # 태그 필터: 보건/의료/노동
    tag_options = ["전체", "보건", "의료", "노동"]
    tag_pick = st.radio("태그", tag_options, horizontal=True)

    # 소스 필터
    sources = ["전체"]
    if not df.empty and "source" in df.columns:
        sources += sorted([s for s in df["source"].dropna().unique().tolist() if s])
    source_pick = st.selectbox("언론사/소스", sources, index=0)

    limit = st.slider("표시 개수", 20, 300, 80, step=10)

with right:
    st.subheader("검색")
    q = st.text_input("제목/요약 검색", value="", placeholder="예: 전공의, 산재, 건강보험, 심평원 ...")

# ---- 필터 적용 ----
view = df.copy() if not df.empty else df

if not view.empty:
    if only_today:
        kst_today = today_kst_date()
        view = view[view["published_at_dt"].dt.date == kst_today]

    if dedup_toggle:
        view = view[view["duplicate_of"].fillna("").str.strip() == ""]

    if tag_pick != "전체":
        # tags 컬럼에 "보건/공공보건" 같은 값이 들어있을 수 있으니 포함검색
        view = view[view["tags"].fillna("").str.contains(tag_pick)]

    if source_pick != "전체" and "source" in view.columns:
        view = view[view["source"] == source_pick]

    if q.strip():
        qq = q.strip()
        view = view[
            view["title"].fillna("").str.contains(qq, case=False) |
            view["summary"].fillna("").str.contains(qq, case=False)
        ]

    view = view.head(limit)

# ---- 오늘 기사 수 ----
if not df.empty:
    kst_today = today_kst_date()
    today_count = (df["published_at_dt"].dt.date == kst_today).sum()
    st.caption(f"📌 오늘(KST) 들어온 전체 기사 수: **{today_count:,}건**")

st.divider()

# ---- 리스트 출력(클릭 가능한 원문 링크) ----
if view.empty:
    st.warning("조건에 맞는 기사가 없습니다.")
else:
    st.subheader("기사 목록")

    # 링크 컬럼(마크다운)
    def mk_link(row):
        title = row.get("title", "")
        url = row.get("url", "")
        if url:
            return f"[{title}]({url})"
        return title

    show = view.copy()
    show["원문"] = show.apply(mk_link, axis=1)

    cols = []
    for c in ["published_at_dt", "source", "tags", "원문", "summary"]:
        if c in show.columns:
            cols.append(c)

    show2 = show[cols].rename(columns={"published_at_dt": "발행(KST)", "source": "소스", "tags": "태그", "summary": "요약"})

    st.dataframe(
        show2,
        use_container_width=True,
        hide_index=True,
        column_config={
            "원문": st.column_config.MarkdownColumn("원문(클릭)", help="제목 클릭 → 원문"),
            "발행(KST)": st.column_config.DatetimeColumn("발행(KST)", format="YYYY-MM-DD HH:mm"),
        },
    )

st.divider()

# ---- 수동 실행 안내(기본) ----
with st.expander("수동 실행 / 자동 실행 시간 조정", expanded=False):
    st.markdown(
        """
### 수동 실행
- GitHub 레포 → **Actions** → `scrape-health-labor-news` → **Run workflow**

### 자동 실행 시간 조정
- `.github/workflows/news_scrape.yml`에서 `cron`을 바꾸면 됩니다.

예)
- 10분마다: `*/10 * * * *`
- 매일 08:05(KST 기준으로는 GitHub는 UTC라 변환 필요): `5 23 * * *` (KST 08:05 = UTC 23:05 전날)

> GitHub Actions cron은 기본적으로 **UTC** 기준입니다.
"""
    )
