import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date, datetime, timedelta
from typing import Any, Optional
import html, calendar, re
import streamlit.components.v1 as components

# ======================================================
# 0) App / Secrets
# ======================================================

APP_TITLE = st.secrets["app"].get("TITLE", "HISMEDI † Daily report")
TZ = st.secrets["app"].get("TZ", "Asia/Seoul")  # reserved

# Daily
SPREADSHEET_ID = st.secrets["gsheet"]["spreadsheet_id"]
WORKSHEET_NAME = st.secrets["gsheet"]["worksheet_name"]

# Weekly
WEEKLY_SPREADSHEET_ID = st.secrets["weekly_board"]["spreadsheet_id"]
WEEKLY_WORKSHEET_NAME = st.secrets["weekly_board"]["worksheet_name"]

SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
WEEK_COL, HEADER_ROW = "WEEK", 1
DATA_START_ROW = HEADER_ROW + 1
WEEKDAY_MAP = ["월", "화", "수", "목", "금", "토", "일"]

# ======================================================
# 1) Layout
# ======================================================

st.set_page_config(page_title=APP_TITLE, layout="wide", initial_sidebar_state="collapsed")

st.markdown(
    """
        <style>
      /* Main spacing (safe) */
      .block-container{padding-top:2.5rem;padding-bottom:1.0rem;}

      /* Hide native sidebar completely (we use main 2-column panel) */
      section[data-testid="stSidebar"]{display:none!important;}

      /* ===========================
         LEFT column as a "card"
         =========================== */
      /* Make the entire LEFT column block look like a sidebar card */
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stVerticalBlock"]{
        position:sticky;
        top:0.65rem;

        background:#f6f7f9;
        border:1px solid rgba(49,51,63,0.16);
        border-radius:0.85rem;
        padding:0.95rem 0.95rem;
      }

      /* Left titles */
      .left-title{font-size:1.65rem;font-weight:850;margin:0 0 0.55rem 0;}
      .left-h3,
      .left-h3 *{font-size:1.02rem;font-weight:850;color:#2563eb !important;margin:0.15rem 0 0.45rem 0;}
      .left-hr{margin:0.75rem 0;border:none;border-top:1px solid rgba(49,51,63,0.14);}

      /* Left: tighten widget spacing (only in LEFT column) */
      div[data-testid="column"]:nth-of-type(1) .stElementContainer{margin:0.10rem 0!important;}
      div[data-testid="column"]:nth-of-type(1) .stMarkdown{margin:0.05rem 0!important;}
      div[data-testid="column"]:nth-of-type(1) [data-testid="stBlock"],
      div[data-testid="column"]:nth-of-type(1) .stBlock{padding:0!important;margin:0!important;}
      div[data-testid="column"]:nth-of-type(1) .stButton,
      div[data-testid="column"]:nth-of-type(1) .stSelectbox,
      div[data-testid="column"]:nth-of-type(1) .stDateInput,
      div[data-testid="column"]:nth-of-type(1) .stTextArea{margin:0.10rem 0!important;}

      /* Highlighted inputs (left + main select) */
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stSelectbox"] div[role="combobox"],
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stDateInput"] input,
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stTextArea"] textarea,
      section.main div[data-testid="stSelectbox"] div[role="combobox"]{background:#eef4ff!important;border:1px solid #c7d2fe!important;}

      /* Left memo textarea */
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stDateInput"] input{text-align:center!important;}
      div[data-testid="column"]:nth-of-type(1) div[data-testid="stTextArea"] textarea{font-size:0.85rem!important;line-height:1.15!important;min-height:10.5rem!important;}

      /* "새창 열기" button-like link */
      .sidebar-linkbtn{display:inline-flex;align-items:center;justify-content:center;width:100%;height:2.45rem;padding:0 0.65rem;border-radius:0.5rem;border:1px solid rgba(49,51,63,0.18);}
      .sidebar-linkbtn{background:rgba(248,249,251,1);color:rgba(49,51,63,0.75)!important;font-weight:500;text-decoration:none!important;white-space:nowrap;box-sizing:border-box;}
      .sidebar-linkbtn:hover{background:rgba(243,244,246,1);}

      /* Monthly wrap grid (NO horizontal scroll) */
      .month-grid{display:grid!important;grid-template-columns:repeat(auto-fit,minmax(260px,1fr))!important;gap:.75rem!important;width:100%!important;}
      .month-item{border:1px solid #e5e7eb;border-radius:.75rem;background:#fff;overflow:hidden;min-width:0;}
      .month-item-date{background:#f9fafb;font-weight:800;padding:.55rem .75rem;border-bottom:1px solid #f3f4f6;white-space:nowrap;}
      .month-item-body{padding:.65rem .75rem;white-space:pre-wrap;word-break:break-word;line-height:1.35;}


      /* Main titles */
      .main-title{font-size:1.15rem;font-weight:850;color:#2563eb;margin:0.2rem 0 0.35rem 0;}
      .sub-title{font-size:1.05rem;font-weight:850;color:#2563eb;margin:0.1rem 0 0.2rem 0;}

      /* Monthly: wrap grid (no horizontal scroll) */
      .month-grid{display:flex;flex-wrap:wrap;gap:0.75rem;}
      .month-item{flex: 1 1 260px;border:1px solid #e5e7eb;border-radius:0.75rem;background:#fff;overflow:hidden;}
      .month-item-date{background:#f9fafb;font-weight:800;padding:0.55rem 0.75rem;border-bottom:1px solid #f3f4f6;white-space:nowrap;}
      .month-item-body{padding:0.65rem 0.75rem;white-space:pre-wrap;line-height:1.35;}

      /* Border container padding control (works for st.container(border=True)) */
      div[data-testid="stVerticalBlockBorderWrapper"]{padding:.35rem .45rem!important;}
      div[data-testid="stVerticalBlockBorderWrapper"] > div{padding:0!important;margin:0!important;}

      /* Weekly cards: pull grey box up a bit */
      div[style*="background:#f8fafc"]{margin-top:0!important;}

      /* Border wrapper: allow children to stay inside, no spill */
      div[data-testid="stVerticalBlockBorderWrapper"]{overflow:visible!important;}
      
    </style>
    """,
    unsafe_allow_html=True,
)

# ======================================================
# 2) Google Sheets connection
# ======================================================

@st.cache_resource
def get_gspread_client() -> gspread.Client:
    credentials = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPE)
    return gspread.authorize(credentials)

def get_worksheet() -> gspread.Worksheet:
    sh = get_gspread_client().open_by_key(SPREADSHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)

@st.cache_resource
def get_weekly_worksheet() -> gspread.Worksheet:
    sh = get_gspread_client().open_by_key(WEEKLY_SPREADSHEET_ID)
    return sh.worksheet(WEEKLY_WORKSHEET_NAME)

# ======================================================
# 3) Date utils
# ======================================================

def parse_date_cell(v: Any) -> Optional[date]:
    if isinstance(v, date) and not isinstance(v, datetime): return v
    if isinstance(v, datetime): return v.date()
    if isinstance(v, str):
        s = v.strip()
        if not s: return None
        try: return date.fromisoformat(s)
        except Exception: pass
        m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
        if m:
            y, mth, d = map(int, m.groups())
            try: return date(y, mth, d)
            except Exception: return None
    return None

def format_date_with_weekday(d: Any) -> str:
    if isinstance(d, datetime): d = d.date()
    if not isinstance(d, date): return str(d)
    return d.strftime("%Y-%m-%d") + f" ({WEEKDAY_MAP[d.weekday()]})"

def escape_html(text: Any) -> str:
    if text is None: return ""
    return html.escape(str(text)).replace("\n", "<br>")

# ======================================================
# 4) Daily DF + save
# ======================================================

@st.cache_data(ttl=60)
def load_daily_df() -> pd.DataFrame:
    records = get_worksheet().get_all_records()
    if not records: return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])
    df = pd.DataFrame(records)
    if "DATE" not in df.columns:
        st.error("Daily 시트의 헤더에 'DATE' 열이 필요합니다."); st.stop()
    df["__row"] = df.index + DATA_START_ROW
    parsed = df["DATE"].apply(parse_date_cell)
    invalid = parsed.isna()
    if invalid.any():
        bad = df.loc[invalid, "DATE"].astype(str).unique()
        st.warning("파싱할 수 없는 DATE 값이 있어 제외되었습니다: " + ", ".join(bad))
    df = df[~invalid].copy()
    if df.empty: return pd.DataFrame(columns=["DATE", "내용", "비고", "__row"])
    df["DATE"] = parsed[~invalid].values
    for col in ["내용", "비고"]:
        if col not in df.columns: df[col] = ""
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
# 5) Weekly DF + cards
# ======================================================

@st.cache_data(ttl=300)
def load_weekly_df() -> pd.DataFrame:
    values = get_weekly_worksheet().get_all_values()
    if not values or len(values) < 2: return pd.DataFrame()
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header).replace("", pd.NA).dropna(how="all").fillna("")
    if WEEK_COL not in df.columns:
        st.warning("주간업무 시트에 'WEEK' 열이 없습니다."); return pd.DataFrame()

    def parse_start(week_str: str) -> Optional[date]:
        try: return datetime.strptime(str(week_str).split("~")[0].strip(), "%Y.%m.%d").date()
        except Exception: return None

    df["_start"] = df[WEEK_COL].astype(str).apply(parse_start)
    return df.dropna(subset=["_start"]).sort_values("_start", ascending=False).reset_index(drop=True)

def render_weekly_cards(df_weekly: pd.DataFrame, week_str: str, ncols: int = 3) -> None:
    row_df = df_weekly[df_weekly[WEEK_COL] == week_str]
    if row_df.empty: st.info("선택한 기간의 부서별 업무 데이터가 없습니다."); return
    row = row_df.iloc[0]
    dept_cols = [c for c in df_weekly.columns if c not in [WEEK_COL, "_start"] and not c.startswith("Unnamed")]
    cols = st.columns(ncols); idx = 0
    for dept in dept_cols:
        text = str(row.get(dept, "")).strip()
        if not text: continue
        with cols[idx % ncols]:
            with st.container(border=True):
                st.markdown(f"<div style='font-size:0.85rem;font-weight:850;margin:-0.05rem 0 0.15rem 0;'>{dept}</div>", unsafe_allow_html=True)
                st.markdown(
                    f"<div style='background:#f8fafc;border-radius:0.6rem;padding:0.25rem 0.45rem;font-size:0.80rem;line-height:1.35;color:#111827;white-space:pre-wrap;'>{escape_html(text)}</div>",
                    unsafe_allow_html=True,
                )
        idx += 1
    if idx == 0: st.info("선택한 기간에 작성된 부서별 업무 내용이 없습니다.")

# ======================================================
# 6) Timetable preview (iframe + loading overlay)
# ======================================================

def render_sheet_preview() -> None:
    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_view = f"https://docs.google.com/spreadsheets/d/{sheet_id}/htmlview?gid={gid}&rm=minimal"

    components.html(
        f"""
        <div style="position:relative;width:100%;height:1100px;">
          <div id="overlay" style="position:absolute;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,0.78);backdrop-filter:blur(2px);border:1px solid #ddd;border-radius:0.75rem;z-index:10;">
            <div style="display:flex;align-items:center;gap:0.55rem;color:#111827;font-size:0.92rem;font-weight:650;">
              <div style="width:14px;height:14px;border-radius:999px;border:2px solid rgba(17,24,39,0.25);border-top-color:rgba(17,24,39,0.85);animation:spin 0.8s linear infinite;"></div>
              진료시간표를 불러오는 중...
            </div>
          </div>
          <iframe id="sheet_iframe" src="{src_view}" style="width:100%;height:1100px;border:1px solid #ddd;border-radius:0.75rem;background:#fff;"></iframe>
        </div>
            
        <script>
          const iframe=document.getElementById("sheet_iframe"), overlay=document.getElementById("overlay");
          iframe.addEventListener("load",()=>{{overlay.style.display="none";}});
          setTimeout(()=>{{if(overlay.style.display!=="none") overlay.style.display="none";}},8000);
        </script>
        """,
        height=1120,
        scrolling=True,
    )

# ======================================================
# 7) UI State
# ======================================================

df_daily = load_daily_df()
if "flash" not in st.session_state: st.session_state["flash"] = None
if "timetable_open" not in st.session_state: st.session_state["timetable_open"] = False
today = date.today()

# memo date
if "memo_date" not in st.session_state: st.session_state["memo_date"] = today

# ym index
if "ym_index" not in st.session_state: st.session_state["ym_index"] = 0

# ======================================================
# 8) Main 2-column layout (LEFT panel + RIGHT content)
# ======================================================

col_left, col_right = st.columns([0.2, 0.8], gap="large")

# ---------------------------
# 8-1) LEFT: sidebar-like panel
# ---------------------------

with col_left:
    st.markdown(f"<div class='left-title'>{APP_TITLE}</div>", unsafe_allow_html=True)
    st.markdown("<hr class='left-hr'>", unsafe_allow_html=True)

    # (1) 업무현황(월)
    st.markdown("<div class='left-h3'>업무현황 (월)</div>", unsafe_allow_html=True)

    if df_daily.empty:
        st.caption("아직 작성된 보고가 없어 월 선택 옵션이 없습니다.")
        selected_ym, ym_options = None, []
    else:
        ym_options = sorted({(d.year, d.month) for d in df_daily["DATE"]}, reverse=True)
        default_ym = (today.year, today.month)
        default_index = ym_options.index(default_ym) if default_ym in ym_options else 0
        if st.session_state["ym_index"] == 0: st.session_state["ym_index"] = default_index
        st.session_state["ym_index"] = max(0, min(st.session_state["ym_index"], len(ym_options) - 1))

        m1, m2, m3 = st.columns([1, 4, 1], vertical_alignment="center")
        with m1:
            if st.button("◀", use_container_width=True, key="ym_prev"):
                st.session_state["ym_index"] = min(len(ym_options) - 1, st.session_state["ym_index"] + 1); st.rerun()
        with m2:
            selected_ym = st.selectbox(
                "월 선택", ym_options, index=st.session_state["ym_index"], key="ym_selectbox",
                format_func=lambda ym: f"{ym[0]}년 {ym[1]:02d}월", label_visibility="collapsed",
            )
            st.session_state["ym_index"] = ym_options.index(selected_ym)
        with m3:
            if st.button("▶", use_container_width=True, key="ym_next"):
                st.session_state["ym_index"] = max(0, st.session_state["ym_index"] - 1); st.rerun()

    st.markdown("<hr class='left-hr'>", unsafe_allow_html=True)

    # (2) 진료시간표
    st.markdown("<div class='left-h3'>진료시간표</div>", unsafe_allow_html=True)

    sheet_id = st.secrets["gsheet_preview"]["spreadsheet_id"]
    gid = st.secrets["gsheet_preview"].get("gid", "0")
    src_open = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={gid}"

    t1, t2, t3 = st.columns(3)
    with t1:
        if st.button("열기", use_container_width=True, disabled=st.session_state["timetable_open"]):
            st.session_state["timetable_open"] = True; st.rerun()
    with t2:
        if st.button("닫기", use_container_width=True, disabled=not st.session_state["timetable_open"]):
            st.session_state["timetable_open"] = False; st.rerun()
    with t3:
        st.markdown(f'<a class="sidebar-linkbtn" href="{src_open}" target="_blank">새창 열기↗</a>', unsafe_allow_html=True)

    st.markdown("<hr class='left-hr'>", unsafe_allow_html=True)

    # (3) 1일 업무 메모
    st.markdown("<div class='left-h3'>1일 업무 메모</div>", unsafe_allow_html=True)

    d1, d2, d3 = st.columns([1, 4, 1], vertical_alignment="center")
    with d1:
        if st.button("◀", use_container_width=True, key="day_prev"):
            st.session_state["memo_date"] = st.session_state["memo_date"] - timedelta(days=1); st.rerun()
    with d2:
        picked = st.date_input("날짜", value=st.session_state["memo_date"], key="memo_date_input", label_visibility="collapsed")
        if isinstance(picked, (list, tuple)): picked = picked[0]
        if picked != st.session_state["memo_date"]: st.session_state["memo_date"] = picked
    with d3:
        if st.button("▶", use_container_width=True, key="day_next"):
            st.session_state["memo_date"] = st.session_state["memo_date"] + timedelta(days=1); st.rerun()

    selected_date = st.session_state["memo_date"]
    default_content = ""
    if not df_daily.empty and (df_daily["DATE"] == selected_date).any():
        default_content = df_daily[df_daily["DATE"] == selected_date].iloc[0].get("내용", "")

    content = st.text_area("내용", value=default_content, key="left_daily_memo", label_visibility="collapsed", height=200)

    b1, b2 = st.columns(2)
    with b1:
        if st.button("저장", use_container_width=True):
            save_daily_entry(selected_date, content, "", df_daily)
            st.session_state["flash"] = ("success", "저장되었습니다."); st.rerun()
    with b2:
        if st.button("동기화", use_container_width=True):
            load_daily_df.clear()
            try: load_weekly_df.clear()
            except Exception: pass
            st.session_state["flash"] = ("success", "동기화되었습니다."); st.rerun()

    
# ---------------------------
# 8-2) RIGHT: main content
# ---------------------------

def render_month_overview_horizontal(period_df: pd.DataFrame) -> None:
    if period_df.empty: st.info("해당 월에 작성된 보고가 없습니다."); return
    parts=[]
    for _, r in period_df.sort_values("DATE").iterrows():
        d=format_date_with_weekday(r["DATE"]); c=escape_html(str(r.get("내용","")))
        parts.append(f"<div class='month-item'><div class='month-item-date'>{escape_html(d)}</div><div class='month-item-body'>{c}</div></div>")
    st.markdown(f"<div class='month-grid'>{''.join(parts)}</div>", unsafe_allow_html=True)

with col_right:
    if st.session_state.get("timetable_open", False):
        render_sheet_preview()
    else:
        if df_daily.empty or (selected_ym is None):
            st.info("아직 작성된 보고가 없습니다.")
        else:
            year, month = selected_ym
            start_date, end_date = date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])

            st.markdown("<div class='main-title'>주요 업무 현황</div>", unsafe_allow_html=True)
            mask = (df_daily["DATE"] >= start_date) & (df_daily["DATE"] <= end_date)
            period_df = df_daily.loc[mask, ["DATE", "내용"]].copy().sort_values("DATE").reset_index(drop=True)
            render_month_overview_horizontal(period_df)

            st.markdown("<div style='height:0.9rem'></div>", unsafe_allow_html=True)

            try: weekly_df = load_weekly_df()
            except Exception: weekly_df = pd.DataFrame()

            head1, head2, head3 = st.columns([0.14, 0.24, 0.62], vertical_alignment="center")
            with head1:
                st.markdown("<div class='sub-title'>부서별 업무 현황</div>", unsafe_allow_html=True)
            with head2:
                if not weekly_df.empty:
                    week_options = weekly_df[WEEK_COL].astype(str).tolist()
                    default_week_idx = 0
                    prev_week = st.session_state.get("weekly_week_select")
                    if prev_week in week_options: default_week_idx = week_options.index(prev_week)
                    selected_week = st.selectbox("기간선택", options=week_options, index=default_week_idx, key="weekly_week_select", label_visibility="collapsed")
                else:
                    selected_week = None
                    st.caption("")
            with head3:
                st.caption("")

            if weekly_df.empty: st.info("부서별 업무 데이터가 없습니다.")
            else: render_weekly_cards(weekly_df, selected_week, ncols=3)

# ======================================================
# 9) Flash messages
# ======================================================

flash = st.session_state.get("flash")
if flash:
    level, msg = flash
    if level == "success": st.success(msg)
    elif level == "info": st.info(msg)
    elif level == "warning": st.warning(msg)
    st.session_state["flash"] = None
