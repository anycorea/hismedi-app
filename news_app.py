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
APP_TITLE = "뉴스 모니터"  # 브라우저 탭 제목(본문 제목은 표시 안 함)
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
        # strip per line to remove accidental leading/trailing spaces
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
def load_news_and_meta(sheet_id: str):
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)

    # News worksheet: 첫 시트(왼쪽) 우선
    ws_news = sh.get_worksheet(0)
    rows = ws_news.get_all_records()
    df = pd.DataFrame(rows)

    # Meta worksheet: "meta" 시트가 있으면 읽기(없으면 빈 dict)
    meta = {}
    try:
        ws_meta = sh.worksheet("meta")
        meta_rows = ws_meta.get_all_records()
        if meta_rows:
            meta_df = pd.DataFrame(meta_rows)
            if {"key", "value"}.issubset(meta_df.columns):
                meta = dict(zip(meta_df["key"].astype(str), meta_df["value"].astype(str)))
    except Exception:
        pass

    return df, meta


def _to_kst_datetime(series: pd.Series) -> pd.Series:
    """
    published_at 같은 ISO8601(+09:00) 문자열을 pandas datetime으로.
    tz-aware로 파싱된 경우 KST로 변환 후 tz 정보 제거(표시/필터 편의).
    """
    s = pd.to_datetime(series, errors="coerce", utc=False)
    # tz-aware면 KST로 변환 → naive로
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    return s


# =========================================================
# UI
# =========================================================
st.set_page_config(page_title=APP_TITLE, layout="wide")

# 타이틀(본문) 제거: 필요 최소 UI만
# st.title(...) 사용하지 않음

# Sheet ID 결정: secrets > env > 상수
sheet_id = (
    st.secrets.get("GSHEET_ID", "").strip()
    or DEFAULT_SHEET_ID
)

if not sheet_id:
    st.error("GSHEET_ID가 설정되지 않았습니다. Streamlit secrets 또는 환경변수로 설정하세요.")
    st.stop()

# 상단 컨트롤(모바일에서도 본문에 그대로 보이도록 sidebar 미사용)
top_left, top_right = st.columns([1, 2], vertical_alignment="center")
with top_left:
    if st.button("🔄 새로고침(시트 다시 읽기)"):
        load_news_and_meta.clear()

with top_right:
    st.caption("필터는 아래에서 조절할 수 있어요. (모바일에서도 본문에 표시됩니다)")

df, meta = load_news_and_meta(sheet_id)

if df.empty:
    st.warning("시트에 데이터가 없습니다.")
    st.stop()

# 컬럼 표준화
# (시트마다 대소문자/공백이 섞일 수 있어 방어)
df.columns = [str(c).strip() for c in df.columns]

# published_at -> 발행(KST)
published_col = "published_at" if "published_at" in df.columns else None
if not published_col:
    # 혹시 다른 이름이면 후보 탐색
    for cand in ["발행", "발행일", "publishedAt", "pubDate", "date"]:
        if cand in df.columns:
            published_col = cand
            break

if published_col is None:
    st.error("발행일 컬럼(published_at)을 찾지 못했습니다.")
    st.stop()

df["발행(KST)"] = _to_kst_datetime(df[published_col])

# 정렬
df = df.sort_values("발행(KST)", ascending=False, na_position="last").reset_index(drop=True)

# ---------------- Filters (본문에 배치) ----------------
with st.expander("필터", expanded=True):
    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        default_from = date.today() - timedelta(days=7)
        date_from = st.date_input("시작일", value=default_from)
    with f2:
        date_to = st.date_input("종료일", value=date.today())
    with f3:
        q = st.text_input("검색(제목/요약)", value="").strip()

    # 태그 필터(있으면)
    tag = None
    if "tags" in df.columns:
        tags = sorted({t.strip() for t in df["tags"].dropna().astype(str).tolist() if t.strip()})
        if tags:
            tag = st.selectbox("태그", options=["(전체)"] + tags, index=0)

# 필터 적용
df_view = df.copy()

if pd.notna(df_view["발행(KST)"]).any():
    df_view = df_view[df_view["발행(KST)"].dt.date >= date_from]
    df_view = df_view[df_view["발행(KST)"].dt.date <= date_to]

if q:
    hay = ""
    if "title" in df_view.columns:
        hay = df_view["title"].fillna("").astype(str)
    if "summary" in df_view.columns:
        hay = hay + " " + df_view["summary"].fillna("").astype(str)
    df_view = df_view[hay.str.contains(q, case=False, na=False)]

if tag and tag != "(전체)" and "tags" in df_view.columns:
    df_view = df_view[df_view["tags"].fillna("").astype(str).str.contains(tag, na=False)]

# ---------------- Result ----------------
st.subheader("기사 목록")

if df_view.empty:
    st.info("선택한 조건에 해당하는 기사가 없습니다. 시작일/종료일 또는 검색어를 조정해 보세요.")
    st.stop()

# 링크: url_canonical 우선, 없으면 url
url_col = "url_canonical" if "url_canonical" in df_view.columns else ("url" if "url" in df_view.columns else None)
title_col = "title" if "title" in df_view.columns else None

# 모바일/클릭 UX 최우선: DataFrame 대신 '리스트 카드' 형태로 출력 (클릭하면 바로 원문 열림)
for _, r in df_view.iterrows():
    t = str(r.get(title_col, "")).strip() if title_col else ""
    u = str(r.get(url_col, "")).strip() if url_col else ""
    src = str(r.get("source", "")).strip()
    tags = str(r.get("tags", "")).strip()
    summ = str(r.get("summary", "")).strip() if "summary" in df_view.columns else ""

    if not t and not u:
        continue

    # 제목 클릭 → 원문 열기 (새 탭)
    if t and u:
        st.markdown(f"**[{t}]({u})**")
    elif u:
        st.markdown(f"**[원문 열기]({u})**")
    else:
        st.markdown(f"**{t}**")

        pub = r.get("발행(KST)")
    pub_str = ""
    try:
        if pd.notna(pub):
            # pub can be pandas Timestamp/datetime
            pub_str = pd.to_datetime(pub, errors="coerce")
            if pd.notna(pub_str):
                pub_str = pub_str.strftime("%Y-%m-%d %H:%M")
            else:
                pub_str = ""
        else:
            pub_str = ""
    except Exception:
        pub_str = ""

    meta_parts = [p for p in [pub_str, (src if src else ""), (tags if tags else "")] if str(p).strip() != ""]
    meta_line = " · ".join(meta_parts)

    if meta_line:
        st.caption(meta_line)

    if summ:
        st.write(summ)

    st.divider()

# ---------------- 운영 안내 ----------------
with st.expander("운영 안내", expanded=False):
    st.write("GitHub Actions 또는 별도 수집 작업이 시트를 갱신하면 자동으로 반영됩니다.")
    if meta:
        st.caption("메타 정보")
        st.json(meta)
