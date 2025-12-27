import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

KST = ZoneInfo("Asia/Seoul")


# ---------------------------------------------------------------------
# Google Sheets (Service Account)
# ---------------------------------------------------------------------
def _normalize_private_key(info: dict) -> dict:
    """Return a copy of info with a normalized PEM private_key.

    This makes the app resilient to:
    - '\\n' vs '\n'
    - Windows line endings (\r\n)
    - Accidental leading/trailing spaces per line
    """
    info = dict(info)
    pk = info.get("private_key", "")

    if isinstance(pk, str) and pk:
        pk = pk.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
        lines = [ln.strip() for ln in pk.split("\n") if ln.strip()]
        info["private_key"] = "\n".join(lines) + "\n"

    return info


@st.cache_resource
def get_gspread_client() -> gspread.Client:
    """Build a cached gspread client.

    Priority:
    1) Streamlit secrets: [gcp_service_account]
    2) Env var: GOOGLE_SERVICE_ACCOUNT_JSON (JSON string)
    """
    if "gcp_service_account" in st.secrets:
        info = _normalize_private_key(dict(st.secrets["gcp_service_account"]))
    else:
        sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        if not sa_json:
            raise RuntimeError(
                "Missing [gcp_service_account] in Streamlit Secrets and GOOGLE_SERVICE_ACCOUNT_JSON env var."
            )
        try:
            info = json.loads(sa_json)
        except json.JSONDecodeError as e:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from e
        info = _normalize_private_key(info)

    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


@st.cache_resource
def get_sheet() -> gspread.Spreadsheet:
    """Open and cache the target spreadsheet."""
    sheet_id = os.getenv("GSHEET_ID", "").strip() or str(st.secrets.get("GSHEET_ID", "")).strip()
    if not sheet_id:
        raise RuntimeError("Missing GSHEET_ID (env or Streamlit secrets).")

    gc = get_gspread_client()
    return gc.open_by_key(sheet_id)


@st.cache_data(ttl=60)
def load_news_and_meta():
    """Load NEWS + META worksheets into (df, meta_dict)."""
    sh = get_sheet()
    ws_news = sh.worksheet("NEWS")
    ws_meta = sh.worksheet("META")

    news_values = ws_news.get_all_values()
    meta_values = ws_meta.get_all_values()

    if len(news_values) > 1:
        df = pd.DataFrame(news_values[1:], columns=news_values[0])
    else:
        df = pd.DataFrame(columns=(news_values[0] if news_values else []))

    meta = {}
    if len(meta_values) > 1:
        for r in meta_values[1:]:
            if len(r) >= 2 and r[0]:
                meta[r[0]] = r[1]

    return df, meta


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def parse_dt(x):
    """Parse datetime values that may arrive as strings."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, datetime):
        return x
    s = str(x).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def today_kst_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------
st.set_page_config(page_title="HISMEDI News Monitor", layout="wide")
st.title("📰 보건·의료·노동 뉴스 모니터")

with st.sidebar:
    st.subheader("데이터")
    if st.button("🔄 새로고침 (시트 다시 읽기)"):
        st.cache_data.clear()

df, meta = load_news_and_meta()

# 표준 컬럼 보정
if "published_at" in df.columns and "발행(KST)" not in df.columns:
    df["발행(KST)"] = df["published_at"].apply(parse_dt)

# 필터
with st.sidebar:
    st.subheader("필터")
    date_default = today_kst_date()
    date_from = st.date_input("시작일", value=datetime.strptime(date_default, "%Y-%m-%d").date())
    query = st.text_input("검색(제목/요약)", value="")

df_view = df.copy()

if "발행(KST)" in df_view.columns:
    df_view["발행(KST)"] = df_view["발행(KST)"].apply(parse_dt)
    df_view = df_view[df_view["발행(KST)"].notna()]
    df_view = df_view[df_view["발행(KST)"].dt.date >= date_from]

if query:
    q = query.strip()
    cols = [c for c in ["title", "summary", "언론사", "제목", "요약"] if c in df_view.columns]
    if cols:
        mask = False
        for c in cols:
            mask = mask | df_view[c].astype(str).str.contains(q, case=False, na=False)
        df_view = df_view[mask]

# 컬럼 이름 통일(가능한 경우)
rename_map = {}
if "title" in df_view.columns and "제목" not in df_view.columns:
    rename_map["title"] = "제목"
if "summary" in df_view.columns and "요약" not in df_view.columns:
    rename_map["summary"] = "요약"
if "url" in df_view.columns and "원문" not in df_view.columns:
    rename_map["url"] = "원문"
if rename_map:
    df_view = df_view.rename(columns=rename_map)

# 정렬
if "발행(KST)" in df_view.columns:
    df_view = df_view.sort_values("발행(KST)", ascending=False)

# 표시 컬럼
preferred_cols = []
for c in ["발행(KST)", "언론사", "제목", "요약", "원문"]:
    if c in df_view.columns:
        preferred_cols.append(c)

if preferred_cols:
    df_show = df_view[preferred_cols].copy()
else:
    df_show = df_view.copy()

st.subheader("기사 목록")

# Streamlit 버전에 따라 column_config가 다를 수 있어 안전하게 처리
colcfg = {}
if hasattr(st, "column_config") and hasattr(st.column_config, "DatetimeColumn") and "발행(KST)" in df_show.columns:
    colcfg["발행(KST)"] = st.column_config.DatetimeColumn("발행(KST)", format="YYYY-MM-DD HH:mm")

if hasattr(st, "column_config") and "원문" in df_show.columns:
    if hasattr(st.column_config, "LinkColumn"):
        colcfg["원문"] = st.column_config.LinkColumn("원문", help="클릭 → 원문 열기", display_text="열기")
    elif hasattr(st.column_config, "TextColumn"):
        colcfg["원문"] = st.column_config.TextColumn("원문(URL)", help="URL 복사해서 열기")

st.dataframe(
    df_show,
    use_container_width=True,
    hide_index=True,
    column_config=colcfg if colcfg else None,
)

with st.expander("ℹ️ 운영 안내", expanded=False):
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
