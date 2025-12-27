import json
import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials


# =========================================================
# Config
# =========================================================
APP_TITLE = "뉴스 모니터"
DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "").strip()


# =========================================================
# Google Sheets (Service Account)
# =========================================================
def _normalize_private_key(info: dict) -> dict:
    """Normalize PEM so cryptography can parse it reliably."""
    info = dict(info)
    pk = info.get("private_key", "")
    if isinstance(pk, str) and pk:
        pk = pk.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
        lines = [ln.strip() for ln in pk.split("\n") if ln.strip() != ""]
        info["private_key"] = "\n".join(lines) + "\n"
    return info


@st.cache_resource
def get_gspread_client():
    # 1) Streamlit secrets 우선
    if "gcp_service_account" in st.secrets:
        info = _normalize_private_key(dict(st.secrets["gcp_service_account"]))
        creds = Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        return gspread.authorize(creds)

    # 2) env var fallback (JSON 문자열)
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise RuntimeError("Missing [gcp_service_account] in secrets and GOOGLE_SERVICE_ACCOUNT_JSON env var.")
    info = json.loads(sa_json)
    info = _normalize_private_key(info)
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


@st.cache_data(ttl=120)
def load_news(sheet_id: str) -> pd.DataFrame:
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws_news = sh.get_worksheet(0)  # 첫 시트(왼쪽)
    rows = ws_news.get_all_records()
    return pd.DataFrame(rows)


def _to_kst_datetime(series: pd.Series) -> pd.Series:
    """ISO8601(+09:00) 문자열을 pandas datetime으로 변환."""
    s = pd.to_datetime(series, errors="coerce", utc=False)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    return s


# =========================================================
# UI
# =========================================================
st.set_page_config(page_title=APP_TITLE, layout="wide")

sheet_id = (st.secrets.get("GSHEET_ID", "").strip() or DEFAULT_SHEET_ID)
if not sheet_id:
    st.error("GSHEET_ID가 설정되지 않았습니다. Streamlit secrets 또는 환경변수로 설정하세요.")
    st.stop()

# ---------------- Filters: 최상단 한 줄 박스(동기화 포함) ----------------
try:
    box = st.container(border=True)
except TypeError:
    box = st.container()

with box:
    f0, f1, f2, f3 = st.columns([0.7, 1, 1, 2], vertical_alignment="center")
    with f0:
        if st.button("🔄 동기화"):
            load_news.clear()
    with f1:
        default_from = date.today() - timedelta(days=7)
        date_from = st.date_input("시작일", value=default_from)
    with f2:
        date_to = st.date_input("종료일", value=date.today())
    with f3:
        q = st.text_input("검색(제목/요약)", value="").strip()

# 데이터 로드(필터 바로 아래에 배치되도록)
df = load_news(sheet_id)
if df.empty:
    st.warning("시트에 데이터가 없습니다.")
    st.stop()

df.columns = [str(c).strip() for c in df.columns]

# published_at 찾기
published_col = None
for cand in ["published_at", "publishedAt", "pubDate", "date", "발행", "발행일"]:
    if cand in df.columns:
        published_col = cand
        break
if published_col is None:
    st.error("발행일 컬럼(published_at)을 찾지 못했습니다.")
    st.stop()

df["발행(KST)"] = _to_kst_datetime(df[published_col])

# 링크/제목 컬럼
url_col = "url_canonical" if "url_canonical" in df.columns else ("url" if "url" in df.columns else None)
title_col = "title" if "title" in df.columns else None

# 정렬
df = df.sort_values("발행(KST)", ascending=False, na_position="last").reset_index(drop=True)

# ---------------- Apply filters ----------------
df_view = df.copy()

df_view = df_view[pd.notna(df_view["발행(KST)"])]
df_view = df_view[df_view["발행(KST)"].dt.date >= date_from]
df_view = df_view[df_view["발행(KST)"].dt.date <= date_to]

if q:
    hay = ""
    if title_col:
        hay = df_view[title_col].fillna("").astype(str)
    if "summary" in df_view.columns:
        hay = hay + " " + df_view["summary"].fillna("").astype(str)
    df_view = df_view[hay.str.contains(q, case=False, na=False)]

if df_view.empty:
    st.info("선택한 조건에 해당하는 기사가 없습니다.")
    st.stop()

# ---------------- Table view ----------------
show_cols = ["발행(KST)"]
if "source" in df_view.columns:
    show_cols.append("source")
if title_col:
    show_cols.append(title_col)
if "summary" in df_view.columns:
    show_cols.append("summary")
if url_col:
    show_cols.append(url_col)

df_out = df_view[show_cols].copy()

rename_map = {"발행(KST)": "발행", "source": "출처"}
if title_col:
    rename_map[title_col] = "제목"
if "summary" in df_out.columns:
    rename_map["summary"] = "요약"
if url_col:
    rename_map[url_col] = "원문"

df_out = df_out.rename(columns=rename_map)

# 발행 포맷
df_out["발행"] = pd.to_datetime(df_out["발행"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

# 요약 줄바꿈/길이 정리
if "요약" in df_out.columns:
    df_out["요약"] = (
        df_out["요약"]
        .fillna("")
        .astype(str)
        .str.replace("\n", " ", regex=False)
        .str.slice(0, 180)
    )

# 링크 컬럼 클릭(가능하면 LinkColumn)
column_config = {}
if "원문" in df_out.columns:
    try:
        column_config["원문"] = st.column_config.LinkColumn("원문")
    except Exception:
        pass

st.dataframe(
    df_out,
    use_container_width=True,
    height=760,
    hide_index=True,
    column_config=column_config if column_config else None,
)
