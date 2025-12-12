import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
from typing import Any, Optional
import html
import calendar
import re
import streamlit.components.v1 as components

# ======================================================
# App / Secrets
# ======================================================

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")  # reserved

# Daily
SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

# Weekly
WEEKLY_SPREADSHEET_ID = st.secrets["weekly_board"]["spreadsheet_id"]
WEEKLY_WORKSHEET_NAME = st.secrets["weekly_board"]["worksheet_name"]

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

WEEK_COL = "WEEK"
HEADER_ROW = 1
DATA_START_ROW = HEADER_ROW + 1
WEEKDAY_MAP = ["월", "화", "수", "목", "금", "토", "일"]

# ======================================================
# Layout
# ======================================================

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
      /* Keep main safe spacing (do not crop) */
      .block-container { padding-top: 2.3rem; padding-bottom: 1rem; }

      /* Sidebar: bring it UP to match main top */
      section[data-testid="stSidebar"] .block-container { padding-top: 0rem!important; padding-bottom: 0.55rem!important; }
      section[data-testid="stSidebar"] div[data-testid="stSidebarContent"]{ margin-top:-1.2rem!important; }
      section[data-testid="stSidebar"] div[data-testid="stSidebarUserContent"]{ margin-top:-1.2rem!important; }

      /* Some themes wrap sidebar content in additional divs */
      section[data-testid="stSidebar"] > div { padding-top: 0rem!important; }
      section[data-testid="stSidebar"] h2 { margin: -0.10rem 0 0.20rem 0!important; }
      section[data-testid="stSidebar"] h3 { margin: 0.18rem 0 0.12rem 0!important; }
      section[data-testid="stSidebar"] .stMarkdown { margin-bottom: 0.10rem; }
      section[data-testid="stSidebar"] hr { margin: 0.30rem 0; }

      /* Highlighted inputs (sidebar + main select) */
      section[data-testid="stSidebar"] div[data-testid="stSelectbox"] div[role="combobox"],
      section[data-testid="stSidebar"] div[data-testid="stDateInput"] input,
      section[data-testid="stSidebar"] div[data-testid="stTextArea"] textarea,
      section.main div[data-testid="stSelectbox"] div[role="combobox"]{ background: #eef4ff !important; border: 1px solid #c7d2fe !important; }

      /* Center text inside date input */
      section[data-testid="stSidebar"] div[data-testid="stDateInput"] input { text-align:center !important; }

      /* Timetable link styled like a light button */
      .sidebar-linkbtn {
        display: inline-flex; align-items: center; justify-content: center;
        width: 100%; height: 2.45rem; padding: 0 0.65rem;
        border-radius: 0.5rem;
        border: 1px solid rgba(49, 51, 63, 0.18);
        background: rgba(248, 249, 251, 1);
        color: rgba(49, 51, 63, 0.75) !important;
        font-weight: 500;
        text-decoration: none !important;
        white-space: nowrap;
        box-sizing: border-box;
      }
      .sidebar-linkbtn:hover { background: rgba(243, 244, 246, 1); }

      /* Monthly horizontal table */
      .month-wrap { overflow-x:auto; border:1px solid #e5e7eb; border-radius:0.75rem; }
      .month-table { border-collapse:collapse; width:max-content; min-width:100%; font-size:0.82rem; }
      .month-table td { border-bottom:1px solid #f3f4f6; padding:0.55rem 0.65rem; vertical-align:top; }
      .month-date { background:#f9fafb; font-weight:700; white-space:nowrap; }
      .month-cell { min-width:14rem; white-space:pre-wrap; }

      /* Main titles */
      .main-title { font-size: 1.15rem; font-weight: 850; margin: 0.2rem 0 0.35rem 0; }
      .sub-title { font-size: 1.05rem; font-weight: 850; margin: 0.1rem 0 0.2rem 0; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ======================================================
# Google Sheets connection
# ======================================================

@st.cache_resource
def get_gspread_client() -> gspread.Client:
    credentials = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPE,
    )
    return gspread.authorize(credentials)


def get_worksheet() -> gspread.Worksheet:
    client = get_gspread_client()
    sh = client.open_by_key(SPREADSHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)


@st.cache_resource
def get_weekly_worksheet() -> gspread.Worksheet:
    client = get_gspread_client()
    sh = client.open_by_key(WEEKLY_SPREADSHEET_ID)
    return sh.worksheet(WEEKLY_WORKSHEET_NAME)


# ======================================================
# Date utils
# ======================================================

def parse_date_cell(v: Any) -> Optional[date]:
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None

        try:
            return date.fromisoformat(s)
        except Exception:
            pass

        m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
        if m:
            y, mth, d = map(int, m.groups())
            try:
                return date(y, mth, d)
            except Exception:
                return None

    return None


def format_date_with_weekday(d: Any) -> str:
    if isinstance(d, datetime):
        d = d.date()
    if not isinstance(d, date):
        return str(d)
    w = WEEKDAY_MAP[d.weekday()]
    return d.strftime("%Y-%m-%d") + f" ({w})"


def escape_html(text: Any) -> str:
    if text is None:
        return ""
    s = html.escape(str(text))
    return s.replace("\n", "<br>")


# ======================================================
# Daily DF
# ======================================================

@st.cache_data(ttl=60)
def load_daily_df() -> pd.DataFrame:
    ws = get_worksheet()
    records = ws.get_all_records()

    if not records:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df = pd.DataFrame(records)

    if "DATE" not in df.columns:
        st.error("Daily 시트의 헤더에 'DATE' 열이 필요합니다.")
        st.stop()

    df["__row"] = df.index + DATA_START_ROW

    parsed = df["DATE"].apply(parse_date_cell)
    invalid_mask = parsed.isna()
    if invalid_mask.any():
        invalid_values = df.loc[invalid_mask, "DATE"].astype(str).unique()
        st.warning("파싱할 수 없는 DATE 값이 있어 제외되었습니다: " + ", ".join(invalid_values))

    valid_mask = ~invalid_mask
    df = df[valid_mask].copy()
    if df.empty:
        return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])

    df["DATE"] = parsed[valid_mask].values

    for col in ["내용", "비고"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    return df


def save_daily_entry(selected_date: date, content: str, note: str, df: pd.DataFrame) -> None:
    ws = get_worksheet()
    mask = (df["DATE"] == selected_date) if not df.empty else pd.Series([], dtype=bool)

    if (not df.empty) and mask.any():
        row_number = int(df.loc[mask, "__row"].iloc[0])
        ws.update(f"B{row_number}:C{row_number}", [[content, note]])
    else:
        ws.append_row([selected_date.isoformat(), content, note], value_input_option="USER_ENTERED")

    load_daily_df.clear()


# ======================================================
# Weekly DF + cards
# ======================================================

@st.cache_data(ttl=300)
def load_weekly_df() -> pd.DataFrame:
    ws = get_weekly_worksheet()
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return pd.DataFrame()

    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header).replace("", pd.NA).dropna(how="all").fillna("")

    if WEEK_COL not in df.columns:
        st.warning("주간업무 시트에 'WEEK' 열이 없습니다.")
        return pd.DataFrame()

    def parse_start(week_str: str) -> Optional[date]:
        try:
            s = str(week_str).split("~")[0].strip()
            return datetime.strptime(s, "%Y.%m.%d").date()
        except Exception:
            return None

    df["_start"] = df[WEEK_COL].astype(str).apply(parse_start)
    df = df.dropna(subset=["_start"]).sort_values("_start", ascending=False).reset_index(drop=True)
    return df


def render_weekly_cards(df_weekly: pd.DataFrame, week_str: str, ncols: int = 3) -> None:
    row_df = df_weekly[df_weekly[WEEK_COL] == week_str]
    if row_df.empty:
        st.info("선택한 기간의 부서별 업무 데이터가 없습니다.")
        return

    row = row_df.iloc[0]
    dept_cols = [c for c in df_weekly.columns if c not in [WEEK_COL, "_start"] and not c.startswith("Unnamed")]

    cols = st.columns(ncols)
    card_idx = 0

    for dept in dept_cols:
        text = str(row.get(dept, "")).strip()
        if not text:
            continue

        with cols[card_idx % ncols]:
            with st.container(border=True):
                st.markdown(
                    f"<div style='font-size:0.85rem; font-weight:850; margin:-0.05rem 0 0.15rem 0;'>{dept}</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"""<div style="
                        background:#f8fafc; border-radius:0.6rem;
                        padding:0.35rem 0.65rem;
                        font-size:0.80rem; line-height:1.35; color:#111827;
                        white-space:pre-wrap;
                    ">{escape_html(text)}</div>""",
                    unsafe_allow_html=True,
                )
        card_idx += 1

    if card_idx == 0:
        st.info("선택한 기간에 작성된 부서별 업무 내용이 없습니다.")


# ======================================================
# Timetable preview
# ======================================================

def render_sheet_preview() -> None:
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_view = f"https://docs.google.com/spreadsheets/d/{sheet_id}/htmlview?gid={gid}&rm=minimal"

    components.html(
        f"""
        <div id="wrap" style="position:relative; width:100%; height:1100px;">
          <!-- Loading overlay -->
          <div id="overlay" style="
              position:absolute; inset:0;
              display:flex; align-items:center; justify-content:center;
              background: rgba(255,255,255,0.78);
              backdrop-filter: blur(2px);
              border: 1px solid #ddd;
              border-radius: 0.75rem;
              z-index: 10;
            ">
            <div style="display:flex; align-items:center; gap:0.55rem; color:#111827; font-size:0.92rem; font-weight:650;">
              <div style="
                width:14px; height:14px; border-radius:999px;
                border: 2px solid rgba(17,24,39,0.25);
                border-top-color: rgba(17,24,39,0.85);
                animation: spin 0.8s linear infinite;
              "></div>
              진료시간표를 불러오는 중...
            </div>
          </div>

          <iframe
            id="sheet_iframe"
            src="{src_view}"
            style="
              width:100%;
              height:1100px;
              border:1px solid #ddd;
              border-radius:0.75rem;
              background:#fff;
            "
          ></iframe>
        </div>

        <style>
          @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
        </style>

        <script>
          const iframe = document.getElementById("sheet_iframe");
          const overlay = document.getElementById("overlay");

          // iframe가 로드되면 오버레이 숨김
          iframe.addEventListener("load", () => {{
            overlay.style.display = "none";
          }});

          // 혹시 load 이벤트가 안 잡히는 환경 대비: 8초 후 강제 숨김(선택)
          setTimeout(() => {{
            if (overlay.style.display !== "none") overlay.style.display = "none";
          }}, 8000);
        </script>
        """,
        height=1120,
        scrolling=True,
    )


# ======================================================
# UI State
# ======================================================

df_daily = load_daily_df()

if "flash" not in st.session_state:
    st.session_state["flash"] = None

if "timetable_open" not in st.session_state:
    st.session_state["timetable_open"] = False

today = date.today()
default_single = today

if "memo_date" not in st.session_state:
    st.session_state["memo_date"] = default_single


# ======================================================
# Sidebar
# ======================================================

with st.sidebar:
    st.markdown(f"<h2 style='font-size:1.45rem; font-weight:850;'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    st.divider()

    # 업무현황(월)
    st.markdown("### 업무현황 (월)")

    if df_daily.empty:
        st.caption("아직 작성된 보고가 없어 월 선택 옵션이 없습니다.")
        selected_ym = None
        ym_options = []
    else:
        ym_set = {(d.year, d.month) for d in df_daily["DATE"]}
        ym_options = sorted(ym_set, reverse=True)

        default_ym = (today.year, today.month)
        default_index = ym_options.index(default_ym) if default_ym in ym_options else 0

        if "ym_index" not in st.session_state:
            st.session_state["ym_index"] = default_index

        st.session_state["ym_index"] = max(0, min(st.session_state["ym_index"], len(ym_options) - 1))

        m1, m2, m3 = st.columns([1, 4, 1], vertical_alignment="center")
        with m1:
            if st.button("◀", use_container_width=True, key="ym_prev"):
                st.session_state["ym_index"] = min(len(ym_options) - 1, st.session_state["ym_index"] + 1)
                st.rerun()
        with m2:
            selected_ym = st.selectbox(
                "월 선택",
                ym_options,
                index=st.session_state["ym_index"],
                key="ym_selectbox",
                format_func=lambda ym: f"{ym[0]}년 {ym[1]:02d}월",
                label_visibility="collapsed",
            )
            st.session_state["ym_index"] = ym_options.index(selected_ym)
        with m3:
            if st.button("▶", use_container_width=True, key="ym_next"):
                st.session_state["ym_index"] = max(0, st.session_state["ym_index"] - 1)
                st.rerun()

    st.divider()

    # 진료시간표
    st.markdown("### 진료시간표")

    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_open = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"

    t1, t2, t3 = st.columns(3)
    with t1:
        if st.button("열기", use_container_width=True, disabled=st.session_state["timetable_open"]):
            st.session_state["timetable_open"] = True
            st.rerun()
    with t2:
        if st.button("닫기", use_container_width=True, disabled=not st.session_state["timetable_open"]):
            st.session_state["timetable_open"] = False
            st.rerun()
    with t3:
        st.markdown(f'<a class="sidebar-linkbtn" href="{src_open}" target="_blank">새창 열기↗</a>', unsafe_allow_html=True)

    st.divider()

    # 1일 업무 메모
    st.markdown("### 1일 업무 메모")

    d1, d2, d3 = st.columns([1, 4, 1], vertical_alignment="center")
    with d1:
        if st.button("◀", use_container_width=True, key="day_prev"):
            st.session_state["memo_date"] = st.session_state["memo_date"] - timedelta(days=1)
            st.rerun()
    with d2:
        picked = st.date_input(
            "날짜",
            value=st.session_state["memo_date"],
            key="memo_date_input",
            label_visibility="collapsed",
        )
        if isinstance(picked, (list, tuple)):
            picked = picked[0]
        if picked != st.session_state["memo_date"]:
            st.session_state["memo_date"] = picked
    with d3:
        if st.button("▶", use_container_width=True, key="day_next"):
            st.session_state["memo_date"] = st.session_state["memo_date"] + timedelta(days=1)
            st.rerun()

    selected_date = st.session_state["memo_date"]

    default_content = ""
    if not df_daily.empty and (df_daily["DATE"] == selected_date).any():
        row = df_daily[df_daily["DATE"] == selected_date].iloc[0]
        default_content = row.get("내용", "")

    content = st.text_area(
        "내용",
        height=150,
        value=default_content,
        key="sidebar_daily_memo",
        label_visibility="collapsed",
    )

    b1, b2 = st.columns(2)
    with b1:
        if st.button("저장", use_container_width=True):
            save_daily_entry(selected_date, content, "", df_daily)
            st.session_state["flash"] = ("success", "저장되었습니다.")
            st.rerun()
    with b2:
        if st.button("동기화", use_container_width=True):
            load_daily_df.clear()
            try:
                load_weekly_df.clear()
            except Exception:
                pass
            st.session_state["flash"] = ("success", "동기화되었습니다.")
            st.rerun()


# ======================================================
# Main
# ======================================================

def render_month_overview_horizontal(period_df: pd.DataFrame) -> None:
    if period_df.empty:
        st.info("해당 월에 작성된 보고가 없습니다.")
        return

    dates = [format_date_with_weekday(d) for d in period_df["DATE"].tolist()]
    contents = [escape_html(str(x)) for x in period_df["내용"].tolist()]

    date_row = "".join([f"<td class='month-date'>{escape_html(d)}</td>" for d in dates])
    content_row = "".join([f"<td class='month-cell'>{c}</td>" for c in contents])

    st.markdown(
        f"""
        <div class="month-wrap">
          <table class="month-table">
            <tbody>
              <tr>{date_row}</tr>
              <tr>{content_row}</tr>
            </tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )

if st.session_state.get("timetable_open", False):
    render_sheet_preview()

else:
    if df_daily.empty or selected_ym is None:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        year, month = selected_ym
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])

        st.markdown('<div class="main-title">주요 업무 현황</div>', unsafe_allow_html=True)

        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask, ["DATE", "내용"]].copy().sort_values("DATE").reset_index(drop=True)
        render_month_overview_horizontal(period_df)

        st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

        try:
            weekly_df = load_weekly_df()
        except Exception:
            weekly_df = pd.DataFrame()

        head1, head2, head3 = st.columns([0.14, 0.24, 0.62], vertical_alignment="center")
        with head1:
            st.markdown('<div class="sub-title">부서별 업무 현황</div>', unsafe_allow_html=True)
        with head2:
            if not weekly_df.empty:
                week_options = weekly_df[WEEK_COL].astype(str).tolist()
                default_week_idx = 0
                prev_week = st.session_state.get("weekly_week_select")
                if prev_week in week_options:
                    default_week_idx = week_options.index(prev_week)

                selected_week = st.selectbox(
                    "기간선택",
                    options=week_options,
                    index=default_week_idx,
                    key="weekly_week_select",
                    label_visibility="collapsed",
                )
            else:
                selected_week = None
                st.caption("")
        with head3:
            st.caption("")

        if weekly_df.empty:
            st.info("부서별 업무 데이터가 없습니다.")
        else:
            render_weekly_cards(weekly_df, selected_week, ncols=3)


# ======================================================
# Flash messages
# ======================================================

flash = st.session_state.get("flash")
if flash:
    level, msg = flash
    if level == "success":
        st.success(msg)
    elif level == "info":
        st.info(msg)
    elif level == "warning":
        st.warning(msg)
    st.session_state["flash"] = None
