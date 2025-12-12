import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime
from typing import Any, Optional
import html
import calendar
import re
import streamlit.components.v1 as components

# ======================================================
# 1. App / Secrets
# ======================================================

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")  # reserved

# Daily report sheet
SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

# Weekly board sheet
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
# 2. Layout / CSS
# ======================================================

st.set_page_config(page_title=APP_TITLE, layout="wide")

st.markdown(
    """
    <style>
      .block-container { padding-top: 2.3rem; padding-bottom: 1rem; }

      /* Sidebar link button (pill) */
      .pill-link {
        display:block; text-align:center;
        font-size:0.86rem; font-weight:600;
        padding:0.45rem 0.9rem;
        border-radius:999px;
        border:1px solid #4f46e5;
        color:#4f46e5 !important;
        background:#eef2ff;
        text-decoration:none !important;
        line-height:1.1;
      }
      .pill-link:hover { filter: brightness(0.98); }

      /* Monthly horizontal table */
      .month-wrap { overflow-x:auto; border:1px solid #e5e7eb; border-radius:0.75rem; }
      .month-table { border-collapse:collapse; width:max-content; min-width:100%; font-size:0.82rem; }
      .month-table th, .month-table td { border-bottom:1px solid #f3f4f6; padding:0.45rem 0.6rem; vertical-align:top; }
      .month-table th { background:#f9fafb; font-weight:700; white-space:nowrap; }
      .month-rowhead { background:#ffffff; font-weight:700; white-space:nowrap; width:5.8rem; }
      .month-cell { min-width:12rem; white-space:pre-wrap; }
      .month-date-link { color:#111827; text-decoration:none; }
      .month-date-link:hover { text-decoration:underline; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ======================================================
# 3. Google Sheets connections
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
# 4. Date / text utils
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
            return date.fromisoformat(s)  # 2025-11-24
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
    return d.strftime("%Y-%m-%d") + f" ({WEEKDAY_MAP[d.weekday()]})"

def escape_html(text: Any) -> str:
    if text is None:
        return ""
    return html.escape(str(text)).replace("\n", "<br>")

# ======================================================
# 5. Daily DF (load/save)
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

    if not df.empty and mask.any():
        row_number = int(df.loc[mask, "__row"].iloc[0])
        ws.update(f"B{row_number}:C{row_number}", [[content, note]])
    else:
        ws.append_row([selected_date.isoformat(), content, note], value_input_option="USER_ENTERED")

    load_daily_df.clear()

# ======================================================
# 6. Weekly DF & cards
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

def render_weekly_cards(df_weekly: pd.DataFrame, week_str: str) -> None:
    row_df = df_weekly[df_weekly[WEEK_COL] == week_str]
    if row_df.empty:
        st.info("선택한 기간의 주간업무 데이터가 없습니다.")
        return

    row = row_df.iloc[0]
    dept_cols = [c for c in df_weekly.columns if c not in [WEEK_COL, "_start"] and not c.startswith("Unnamed")]

    col_a, col_b = st.columns(2)
    card_idx = 0

    for dept in dept_cols:
        text = str(row.get(dept, "")).strip()
        if not text:
            continue

        target_col = col_a if card_idx % 2 == 0 else col_b
        card_idx += 1

        with target_col:
            with st.container(border=True):
                st.markdown(
                    f"<div style='font-size:0.82rem; font-weight:700; margin:-0.18rem 0 0.04rem 0;'>{dept}</div>",
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"""<div style="
                        background:#f3f4f6; border-radius:0.5rem;
                        padding:0.16rem 0.60rem; margin-bottom:0.18rem;
                        font-size:0.80rem; line-height:1.30; color:#111827;
                        white-space:pre-wrap; text-align:left;
                    ">{escape_html(text)}</div>""",
                    unsafe_allow_html=True,
                )

    if card_idx == 0:
        st.info("선택한 기간에 작성된 부서별 업무 내용이 없습니다.")

# ======================================================
# 7. Timetable preview
# ======================================================

def render_sheet_preview() -> None:
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_view = f"https://docs.google.com/spreadsheets/d/{sheet_id}/htmlview?gid={gid}&rm=minimal"

    components.html(
        f"""
        <iframe
          src="{src_view}"
          style="width:100%; height:1100px; border:1px solid #ddd; border-radius:0.75rem; background:#fff;"
        ></iframe>
        """,
        height=1120,
        scrolling=True,
    )

# ======================================================
# 8. Monthly overview (horizontal + clickable)
# ======================================================

def render_month_overview_horizontal(period_df: pd.DataFrame) -> None:
    if period_df.empty:
        st.info("해당 월에 작성된 보고가 없습니다.")
        return

    # build links that set query param pick=YYYY-MM-DD
    dates_iso = [d.isoformat() if isinstance(d, date) else str(d) for d in period_df["DATE"].tolist()]
    dates_label = [format_date_with_weekday(d) for d in period_df["DATE"].tolist()]
    contents = [escape_html(str(x)) for x in period_df["내용"].tolist()]

    th_html = "".join(
        [
            f"<th><a class='month-date-link' href='?pick={escape_html(iso)}'>{escape_html(lbl)}</a></th>"
            for iso, lbl in zip(dates_iso, dates_label)
        ]
    )
    td_html = "".join(
        [
            f"<td class='month-cell'><a class='month-date-link' href='?pick={escape_html(iso)}'>{c}</a></td>"
            for iso, c in zip(dates_iso, contents)
        ]
    )

    st.markdown(
        f"""
        <div class="month-wrap">
          <table class="month-table">
            <thead>
              <tr><th class="month-rowhead">DATE</th>{th_html}</tr>
            </thead>
            <tbody>
              <tr><td class="month-rowhead">내용</td>{td_html}</tr>
            </tbody>
          </table>
        </div>
        """,
        unsafe_allow_html=True,
    )

# ======================================================
# 9. UI state
# ======================================================

df_daily = load_daily_df()

if "flash" not in st.session_state:
    st.session_state["flash"] = None
if "timetable_open" not in st.session_state:
    st.session_state["timetable_open"] = False
if "sidebar_date" not in st.session_state:
    st.session_state["sidebar_date"] = date.today()

today = date.today()
default_single = today

# Handle clickable month table -> set sidebar date
pick = st.query_params.get("pick")
if pick:
    try:
        # pick might be list-like depending on streamlit version
        pick_str = pick[0] if isinstance(pick, list) else str(pick)
        st.session_state["sidebar_date"] = date.fromisoformat(pick_str)
    except Exception:
        pass
    # clear the param to keep URL clean
    try:
        st.query_params.pop("pick", None)
    except Exception:
        pass

# ======================================================
# 10. Sidebar
# ======================================================

with st.sidebar:
    st.markdown(f"<h2 style='font-size:1.6rem; font-weight:700;'>{APP_TITLE}</h2>", unsafe_allow_html=True)

    is_timetable_open = st.session_state.get("timetable_open", False)
    st.divider()

    # --------------------------
    # Daily memo
    # --------------------------
    st.markdown("### 1일 업무 메모")

    st.date_input("날짜", key="sidebar_date")
    selected_date = st.session_state["sidebar_date"]

    default_content, has_existing = "", False
    if not df_daily.empty and (df_daily["DATE"] == selected_date).any():
        row = df_daily[df_daily["DATE"] == selected_date].iloc[0]
        default_content, has_existing = row.get("내용", ""), True

    content = st.text_area(
        "내용",
        height=140,
        value=default_content,
        placeholder="짤막하게 메모를 남겨두세요.\n(예: 정대표 미팅 - 14시)",
        key="sidebar_daily_memo",
    )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("저장", use_container_width=True):
            save_daily_entry(selected_date, content, "", df_daily)
            st.session_state["flash"] = ("success", "저장되었습니다.")
            st.rerun()
    with c2:
        if has_existing and st.button("비우기", use_container_width=True):
            save_daily_entry(selected_date, "", "", df_daily)
            st.session_state["flash"] = ("info", "이 날짜의 메모를 모두 비웠습니다.")
            st.rerun()

    st.divider()

    # --------------------------
    # Sync
    # --------------------------
    if st.button("🔄 구글시트 동기화", use_container_width=True):
        load_daily_df.clear()
        try:
            load_weekly_df.clear()
        except Exception:
            pass
        st.session_state["flash"] = ("success", "동기화되었습니다.")
        st.rerun()

    st.divider()

    # --------------------------
    # Timetable
    # --------------------------
    st.markdown("### 진료시간표")

    b1, b2 = st.columns([1, 1])
    with b1:
        label = "진료시간표 닫기" if is_timetable_open else "진료시간표 열기"
        if st.button(label, use_container_width=True):
            st.session_state["timetable_open"] = not is_timetable_open
            st.rerun()

    with b2:
        sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
        gid = st.secrets["gsheet_preview"].get("gid", "0")
        src_open = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"
        st.markdown(f'<a class="pill-link" href="{src_open}" target="_blank">새 창에서 열기 ↗</a>', unsafe_allow_html=True)

    st.divider()

    # --------------------------
    # Monthly selector (hidden while timetable open)
    # --------------------------
    if not is_timetable_open:
        st.markdown("### 업무현황 (월)")

        if df_daily.empty:
            st.caption("아직 작성된 보고가 없어 월 선택 옵션이 없습니다.")
            selected_ym = None
        else:
            ym_set = {(d.year, d.month) for d in df_daily["DATE"]}
            ym_options = sorted(ym_set, reverse=True)
            default_ym = (today.year, today.month)
            default_index = ym_options.index(default_ym) if default_ym in ym_options else 0

            selected_ym = st.selectbox(
                "월 선택",
                ym_options,
                index=default_index,
                format_func=lambda ym: f"{ym[0]}년 {ym[1]:02d}월",
            )
    else:
        selected_ym = None

# ======================================================
# 11. Main
# ======================================================

if st.session_state.get("timetable_open", False):
    with st.spinner("진료시간표를 불러오는 중..."):
        render_sheet_preview()

else:
    if df_daily.empty or selected_ym is None:
        st.info("아직 작성된 보고가 없습니다.")
    else:
        year, month = selected_ym
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])

        st.markdown(f"## {year}년 {month:02d}월")

        mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
        period_df = df_daily.loc[mask, ["DATE", "내용"]].copy().sort_values("DATE").reset_index(drop=True)

        render_month_overview_horizontal(period_df)

        st.markdown("### 주간업무")
        try:
            weekly_df = load_weekly_df()
        except Exception:
            weekly_df = pd.DataFrame()

        if weekly_df.empty:
            st.info("주간업무 데이터가 없습니다.")
        else:
            week_options = weekly_df[WEEK_COL].astype(str).tolist()
            prev_week = st.session_state.get("weekly_week_select")
            default_week_idx = week_options.index(prev_week) if prev_week in week_options else 0

            selected_week = st.selectbox(
                "기간선택 (주간업무)",
                options=week_options,
                index=default_week_idx,
                key="weekly_week_select",
            )
            render_weekly_cards(weekly_df, selected_week)

# ======================================================
# 12. Flash message
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
