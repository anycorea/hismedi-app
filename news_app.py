import json
import os
import re
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


def _clean_summary(title: str, summary: str) -> str:
    """
    '요약' 컬럼이 실제 요약이 아니라,
    - 제목 반복
    - '기사 읽어주기 서비스...', '최근 24시간...' 같은 고정 안내문
    - '입력 2025-...' 같은 메타 문구
    로 채워지는 경우가 많아 화면에서 제거/정리합니다.

    전략:
    1) 고정 안내/메타 문구 제거
    2) 제목 반복 제거
    3) 정리 후 너무 짧거나 안내문 성격이면 빈칸 처리
    """
    t = (title or "").strip()
    s = (summary or "").strip()
    if not s:
        return ""

    # normalize whitespace
    s = s.replace("\n", " ").replace("\r", " ")
    s = re.sub(r"\s+", " ", s).strip()

    # remove common boilerplate phrases (Korean news feeds)
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

    # remove title repetitions
    if t:
        if s.startswith(t):
            s = s[len(t):].lstrip(" -:·|」’'\"")
        if t in s:
            s = s.replace(t, " ").strip()
            s = re.sub(r"\s+", " ", s).strip()

    # remove bracketed prefixes like [단독], [리포트]
    s = re.sub(r"^(\[.*?\]\s*)+", "", s).strip()

    # If summary still looks like a notice, drop it
    if re.search(r"(읽어주기\s*서비스|브라우저에서만\s*사용|속보\s*및\s*알림)", s):
        return ""

    # too short => not a real summary
    if len(s) < 40:
        return ""

    return s


# =========================================================
# UI
# =========================================================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# --- Compact spacing ---
st.markdown(
    """
    <style>
      .block-container { padding-top: 0.8rem !important; padding-bottom: 1.0rem !important; }
      .filter-box {
        border: 1px solid rgba(49, 51, 63, 0.16);
        border-radius: 14px;
        padding: 0.75rem 0.85rem;
        margin-bottom: 0.6rem;
        background: rgba(255, 255, 255, 0.02);
      }
      .filter-label {
        font-size: 0.85rem;
        font-weight: 600;
        color: rgba(49, 51, 63, 0.75);
        margin: 0 0 0.35rem 0;
      }
      div[data-testid="stButton"] > button { height: 42px; border-radius: 12px; padding: 0 14px; }
      div[data-baseweb="input"] input { height: 42px !important; }
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

df = df.sort_values("발행(KST)", ascending=False, na_position="last").reset_index(drop=True)

# ---------------- Apply filters ----------------
df_view = df[pd.notna(df["발행(KST)"])].copy()
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

df_out["발행"] = pd.to_datetime(df_out["발행"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

if "요약" in df_out.columns:
    if "제목" in df_out.columns:
        df_out["요약"] = [
            _clean_summary(t, s)
            for t, s in zip(
                df_out["제목"].fillna("").astype(str),
                df_out["요약"].fillna("").astype(str),
            )
        ]
    else:
        df_out["요약"] = df_out["요약"].fillna("").astype(str)

    df_out["요약"] = (
        pd.Series(df_out["요약"])
        .fillna("")
        .astype(str)
        .str.replace("\n", " ", regex=False)
        .str.replace("\r", " ", regex=False)
        .str.replace("  ", " ", regex=False)
        .str.strip()
        .str.slice(0, 220)
    )

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
