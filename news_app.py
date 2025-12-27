import json
import os
import re
import html
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
        raise RuntimeError(
            "Missing [gcp_service_account] in secrets and GOOGLE_SERVICE_ACCOUNT_JSON env var."
        )
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


def _clean_lead(title: str, text: str) -> str:
    """요약이 안내문/메타/제목반복인 경우를 최대한 제거하고, '기사 첫부분'처럼 보이게 정리."""
    t = (title or "").strip()
    s = (text or "").strip()
    if not s:
        return ""

    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # 메타/안내 문구 제거(빈번 패턴)
    boilerplate_patterns = [
        r"입력\s*\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2}[^ ]*",
        r"기사\s*읽어주기\s*서비스는.*?브라우저에서만\s*사용[^.。]*\.?$",
        r"최근\s*24시간\s*이내\s*속보\s*및\s*알림을\s*표시합니다\.?$",
        r"※\s*이\s*사진은\s*기사\s*내용과\s*관련이\s*없습니다\.?$",
        r"^\s*사진\s*=\s*[^ ]+\s*",
    ]
    for p in boilerplate_patterns:
        s = re.sub(p, " ", s, flags=re.IGNORECASE).strip()
        s = re.sub(r"\s+", " ", s).strip()

    # 제목 반복 제거
    if t:
        if s.startswith(t):
            s = s[len(t):].lstrip(" -:·|」’'\"")
        if t in s:
            s = s.replace(t, " ").strip()
            s = re.sub(r"\s+", " ", s).strip()

    # 머리표/대괄호 접두 제거
    s = re.sub(r"^(\[.*?\]\s*)+", "", s).strip()

    return s


# =========================================================
# UI
# =========================================================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# --- Compact & modern spacing ---
st.markdown(
    """
    <style>
      .block-container { padding-top: 0.7rem !important; padding-bottom: 0.9rem !important; }
      .filter-box {
        border: 1px solid rgba(49, 51, 63, 0.14);
        border-radius: 16px;
        padding: 0.70rem 0.85rem;
        margin-bottom: 0.55rem;
        background: rgba(255, 255, 255, 0.02);
      }
      .filter-label {
        font-size: 0.85rem;
        font-weight: 650;
        color: rgba(49, 51, 63, 0.70);
        margin: 0 0 0.30rem 0;
      }
      div[data-testid="stButton"] > button { height: 42px; border-radius: 14px; padding: 0 14px; }
      div[data-baseweb="input"] input { height: 42px !important; }
      .news-wrap {
        border: 1px solid rgba(49, 51, 63, 0.14);
        border-radius: 16px;
        overflow: auto;
        max-height: 760px;
      }
      table.news {
        border-collapse: collapse;
        width: 100%;
        font-size: 14px;
      }
      table.news thead th {
        position: sticky;
        top: 0;
        background: rgba(250, 250, 250, 1);
        border-bottom: 1px solid rgba(49, 51, 63, 0.14);
        text-align: left;
        padding: 10px 12px;
        white-space: nowrap;
        z-index: 5;
      }
      table.news tbody td {
        border-bottom: 1px solid rgba(49, 51, 63, 0.08);
        padding: 10px 12px;
        vertical-align: top;
      }
      table.news tbody tr:hover td {
        background: rgba(49, 51, 63, 0.03);
      }
      .nowrap { white-space: nowrap; }
      a.newslink { text-decoration: none; }
      a.newslink:hover { text-decoration: underline; }
      .lead { color: rgba(49, 51, 63, 0.82); }
    </style>
    """,
    unsafe_allow_html=True,
)

sheet_id = st.secrets.get("GSHEET_ID", "").strip() or DEFAULT_SHEET_ID
if not sheet_id:
    st.error("GSHEET_ID가 설정되지 않았습니다. Streamlit secrets 또는 환경변수로 설정하세요.")
    st.stop()

# ---------------- Top filter row (동기화 포함) ----------------
st.markdown('<div class="filter-box">', unsafe_allow_html=True)

c0, c1, c2, c3 = st.columns([0.75, 1.15, 1.15, 2.2], vertical_alignment="center")

with c0:
    st.markdown('<div class="filter-label">&nbsp;</div>', unsafe_allow_html=True)
    if st.button("🔄 동기화", use_container_width=True):
        load_news.clear()

with c1:
    st.markdown('<div class="filter-label">시작일</div>', unsafe_allow_html=True)
    default_from = date.today() - timedelta(days=7)
    date_from = st.date_input("시작일", value=default_from, label_visibility="collapsed")

with c2:
    st.markdown('<div class="filter-label">종료일</div>', unsafe_allow_html=True)
    date_to = st.date_input("종료일", value=date.today(), label_visibility="collapsed")

with c3:
    st.markdown('<div class="filter-label">검색(제목/요약)</div>', unsafe_allow_html=True)
    q = st.text_input("검색(제목/요약)", value="", label_visibility="collapsed").strip()

st.markdown("</div>", unsafe_allow_html=True)

# ---------------- Load & normalize data ----------------
df = load_news(sheet_id)
if df.empty:
    st.warning("시트에 데이터가 없습니다.")
    st.stop()

df.columns = [str(c).strip() for c in df.columns]

published_col = None
for cand in ["published_at", "publishedAt", "pubDate", "date", "발행", "발행일"]:
    if cand in df.columns:
        published_col = cand
        break
if published_col is None:
    st.error("발행일 컬럼(published_at)을 찾지 못했습니다.")
    st.stop()

df["발행(KST)"] = _to_kst_datetime(df[published_col])

url_col = "url_canonical" if "url_canonical" in df.columns else ("url" if "url" in df.columns else None)
title_col = "title" if "title" in df.columns else None
summary_col = "summary" if "summary" in df.columns else None

if url_col is None or title_col is None:
    st.error("필수 컬럼(title, url/url_canonical)을 찾지 못했습니다.")
    st.stop()

df = df.sort_values("발행(KST)", ascending=False, na_position="last").reset_index(drop=True)

# ---------------- Apply filters ----------------
df_view = df[pd.notna(df["발행(KST)"])].copy()
df_view = df_view[df_view["발행(KST)"].dt.date >= date_from]
df_view = df_view[df_view["발행(KST)"].dt.date <= date_to]

if q:
    hay = df_view[title_col].fillna("").astype(str)
    if summary_col:
        hay = hay + " " + df_view[summary_col].fillna("").astype(str)
    df_view = df_view[hay.str.contains(q, case=False, na=False)]

if df_view.empty:
    st.info("선택한 조건에 해당하는 기사가 없습니다.")
    st.stop()

# ---------------- Build 'lead' preview (기사 시작부분) ----------------
titles = df_view[title_col].fillna("").astype(str).tolist()
summaries = df_view[summary_col].fillna("").astype(str).tolist() if summary_col else [""] * len(df_view)

leads = []
for t, s in zip(titles, summaries):
    lead = _clean_lead(t, s)
    lead = lead[:180].rstrip()
    leads.append(lead)

# ---------------- Render table (제목 클릭 = 원문) ----------------
rows_html = []
for idx, r in df_view.iterrows():
    pub = r.get("발행(KST)")
    pub_str = ""
    try:
        pub_ts = pd.to_datetime(pub, errors="coerce")
        if pd.notna(pub_ts):
            pub_str = pub_ts.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pub_str = ""

    src = str(r.get("source", "")).strip()
    title = str(r.get(title_col, "")).strip()
    url = str(r.get(url_col, "")).strip()
    lead = leads[df_view.index.get_loc(idx)] if len(leads) == len(df_view) else ""

    pub_html = html.escape(pub_str)
    src_html = html.escape(src)
    title_html = html.escape(title)
    url_html = html.escape(url, quote=True)
    lead_html = html.escape(lead)

    rows_html.append(
        f"""<tr>
  <td class='nowrap'>{pub_html}</td>
  <td class='nowrap'>{src_html}</td>
  <td><a class='newslink' href='{url_html}' target='_blank' rel='noopener noreferrer'>{title_html}</a></td>
  <td class='lead'>{lead_html}</td>
</tr>"""
    )

table_html = f"""
<div class='news-wrap'>
  <table class='news'>
    <thead>
      <tr>
        <th class='nowrap'>발행</th>
        <th class='nowrap'>출처</th>
        <th>제목</th>
        <th>기사 시작부분</th>
      </tr>
    </thead>
    <tbody>
      {''.join(rows_html)}
    </tbody>
  </table>
</div>
"""

st.markdown(table_html, unsafe_allow_html=True)
