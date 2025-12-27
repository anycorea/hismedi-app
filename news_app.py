import json
import os
import re
from datetime import date, timedelta

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials


APP_TITLE = "뉴스 모니터"
DEFAULT_SHEET_ID = os.getenv("GSHEET_ID", "").strip()


def _normalize_private_key(info: dict) -> dict:
    info = dict(info)
    pk = info.get("private_key", "")
    if isinstance(pk, str) and pk:
        pk = pk.replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
        lines = [ln.strip() for ln in pk.split("\n") if ln.strip()]
        info["private_key"] = "\n".join(lines) + "\n"
    return info


@st.cache_resource
def get_gspread_client():
    if "gcp_service_account" in st.secrets:
        info = _normalize_private_key(dict(st.secrets["gcp_service_account"]))
        creds = Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        return gspread.authorize(creds)

    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        raise RuntimeError("Missing service account credentials.")
    info = _normalize_private_key(json.loads(sa_json))
    creds = Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


@st.cache_data(ttl=120)
def load_news(sheet_id: str) -> pd.DataFrame:
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    ws = sh.get_worksheet(0)
    return pd.DataFrame(ws.get_all_records())


def _to_kst(series: pd.Series) -> pd.Series:
    """Convert various published date formats to KST-naive datetimes (display-friendly).

    - Accepts strings / mixed values / timezone-aware values.
    - Unparseable values become NaT (and can be filtered out safely).
    """
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    # Convert UTC -> KST, then drop tz for simpler display/filtering
    return dt.dt.tz_convert("Asia/Seoul").dt.tz_localize(None)


st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
      .block-container { padding-top: 0.7rem !important; }
      .top-box { border: 1px solid rgba(49,51,63,.14); border-radius: 14px; padding: 0.6rem 0.8rem; margin-bottom: 0.6rem; }
      .top-box button { height: 42px; }
      table.news { border-collapse:collapse; width:100%; font-size:14px; }
      table.news th, table.news td { padding:10px 12px; border-bottom:1px solid rgba(49,51,63,.08); text-align:left; white-space:nowrap; }
      table.news th { position:sticky; top:0; background:#fafafa; z-index:1; }
      table.news tr:hover td { background:rgba(49,51,63,.03); }
      a.newslink { text-decoration:none; }
      a.newslink:hover { text-decoration:underline; }
    </style>
    """
    ,
    unsafe_allow_html=True,
)

sheet_id = st.secrets.get("GSHEET_ID", "").strip() or DEFAULT_SHEET_ID
if not sheet_id:
    st.error("GSHEET_ID가 설정되지 않았습니다.")
    st.stop()

# ---------------- Top controls ----------------
with st.container():
    st.markdown('<div class="top-box">', unsafe_allow_html=True)

    # (요청 순서) 시작일 · 종료일 · 태그 · 검색(키워드) · 동기화
    df = load_news(sheet_id)
    if df.empty:
        st.warning("데이터가 없습니다.")
        st.stop()

    # 태그 옵션 준비(있을 때만)
    tag_col = "tags" if "tags" in df.columns else None
    tag_options = []
    if tag_col:
        _tags = (
            df[tag_col]
            .fillna("")
            .astype(str)
            .str.split(",")
            .explode()
            .str.strip()
        )
        tag_options = sorted([t for t in _tags.unique().tolist() if t])

    c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.2, 2.2, 0.9], vertical_alignment="bottom")
    with c1:
        date_from = st.date_input("시작일", value=date.today() - timedelta(days=7), key="date_from")
    with c2:
        date_to = st.date_input("종료일", value=date.today(), key="date_to")
    with c3:
        selected_tag = st.selectbox("태그", options=["전체"] + tag_options, index=0, key="selected_tag")
    with c4:
        keyword = st.text_input(
            "검색(키워드)",
            value="",
            placeholder="예: 전공의, 간호사, 수가, 고용유지지원금 ...",
            key="keyword",
        )
    with c5:
        if st.button("🔄 동기화", use_container_width=True):
            load_news.clear()
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)


df = load_news(sheet_id)
if df.empty:
    st.warning("데이터가 없습니다.")
    st.stop()

df.columns = [str(c).strip() for c in df.columns]

pub_col = next((c for c in ["published_at", "publishedAt", "pubDate", "date", "발행"] if c in df.columns), None)
if not pub_col:
    st.error("발행일 컬럼을 찾지 못했습니다.")
    st.stop()

df["발행"] = _to_kst(df[pub_col])
df = df[pd.notna(df["발행"])]

# 날짜 필터
df = df[(df["발행"].dt.date >= date_from) & (df["발행"].dt.date <= date_to)]

# 태그 필터(단일 선택)
if tag_col and selected_tag and selected_tag != "전체":
    df = df[df[tag_col].fillna("").astype(str).str.contains(re.escape(selected_tag))]

# 키워드 검색(선택 시): 제목/출처/태그에서 부분일치
kw = (keyword or "").strip().lower()
if kw:
    title_c = "title" if "title" in df.columns else None
    source_c = "source" if "source" in df.columns else None

    mask = pd.Series(False, index=df.index)
    if title_c:
        mask = mask | df[title_c].fillna("").astype(str).str.lower().str.contains(kw)
    if source_c:
        mask = mask | df[source_c].fillna("").astype(str).str.lower().str.contains(kw)
    if tag_col:
        mask = mask | df[tag_col].fillna("").astype(str).str.lower().str.contains(kw)

    df = df[mask]

df = df.sort_values("발행", ascending=False)

title_col = "title" if "title" in df.columns else None
url_col = "url_canonical" if "url_canonical" in df.columns else ("url" if "url" in df.columns else None)
if not title_col or not url_col:
    st.error("필수 컬럼(title, url/url_canonical)을 찾지 못했습니다.")
    st.stop()

rows = []
for _, r in df.iterrows():
    pub_str = r["발행"].strftime("%Y-%m-%d %H:%M")
    src = str(r.get("source", "")).strip()
    title = str(r.get(title_col, "")).strip()
    url = str(r.get(url_col, "")).strip()
    rows.append(
        "<tr>"
        f"<td>{pub_str}</td>"
        f"<td>{src}</td>"
        f"<td><a class='newslink' href='{url}' target='_blank' rel='noopener noreferrer'>{title}</a></td>"
        "</tr>"
    )

html = (
    "<div style='max-height:760px; overflow:auto; border:1px solid rgba(49,51,63,.14); border-radius:14px;'>"
    "<table class='news'>"
    "<thead><tr><th>발행</th><th>출처</th><th>제목</th></tr></thead>"
    "<tbody>"
    + "".join(rows)
    + "</tbody></table></div>"
)

st.markdown(html, unsafe_allow_html=True)
