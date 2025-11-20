import re
from datetime import datetime, timedelta

import gspread
from gspread.cell import Cell
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
import streamlit.components.v1 as components

WEEK_COL = "WEEK"


@st.cache_resource(show_spinner=False)
def get_worksheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=scopes,
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    ws = sh.worksheet(st.secrets["gsheet"]["worksheet_name"])
    return ws


@st.cache_data(show_spinner=False)
def load_data():
    ws = get_worksheet()
    values = ws.get_all_values()

    if not values or len(values) < 2:
        return pd.DataFrame()

    raw_header = values[0]
    rows = values[1:]

    header = []
    for i, h in enumerate(raw_header):
        h = str(h).strip()
        if not h:
            h = f"Unnamed_{i+1}"
        header.append(h)

    n_cols = len(header)

    normalized_rows = []
    for r in rows:
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        normalized_rows.append(r)

    df = pd.DataFrame(normalized_rows, columns=header)

    for c in [c for c in df.columns if c.startswith("Unnamed_")]:
        if df[c].replace("", pd.NA).isna().all():
            df.drop(columns=[c], inplace=True)

    df["_sheet_row"] = df.index + 2

    pattern = re.compile(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}")
    week_col_name = None
    for col in df.columns:
        s = df[col].astype(str)
        if s.apply(lambda x: bool(pattern.fullmatch(x.strip()))).any():
            week_col_name = col
            break

    if week_col_name is None:
        return df

    if WEEK_COL not in df.columns:
        df[WEEK_COL] = df[week_col_name]

    def parse_start_date(week_str: str) -> datetime:
        try:
            start = str(week_str).split("~")[0].strip()
            return datetime.strptime(start, "%Y.%m.%d")
        except Exception:
            return datetime.min

    df["_start_date"] = df[WEEK_COL].astype(str).apply(parse_start_date)
    df = df.sort_values("_start_date", ascending=False).reset_index(drop=True)

    return df


def get_dept_columns(df: pd.DataFrame):
    return [c for c in df.columns if c not in [WEEK_COL] and not c.startswith("_")]


def parse_week_range(week_str: str):
    try:
        s, e = week_str.split("~")
        start = datetime.strptime(s.strip(), "%Y.%m.%d")
        end = datetime.strptime(e.strip(), "%Y.%m.%d")
        return start, end
    except Exception:
        return None, None


def get_col_index(ws, col_name: str):
    headers = ws.row_values(1)
    try:
        return headers.index(col_name) + 1
    except ValueError:
        return None


def escape_html(text: str) -> str:
    if text is None:
        return ""
    text = str(text)
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace("\n", "<br>")
    return text


def main():
    app_title = "HISMEDI † Weekly report"
    try:
        app_title = st.secrets["app"].get("TITLE", app_title)
    except Exception:
        pass

    st.set_page_config(page_title=app_title, layout="wide")

    # Global layout & spacing styles - 최대한 상단으로, 간격 압축
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            min-width: 360px;
            max-width: 380px;
            padding-top: 0;
        }
        [data-testid="stSidebar"] * {
            line-height: 1.03;
        }
        [data-testid="stSidebar"] .stButton {
            margin-bottom: 0.2rem;
        }
        [data-testid="stSidebar"] button {
            font-size: 0.8rem;
            padding-top: 0.18rem;
            padding-bottom: 0.18rem;
        }
        /* 부서 선택 영역(컬럼 안 버튼)은 글자 조금 더 작게, 박스는 약간 높게 */
        [data-testid="stSidebar"] [data-testid="column"] button {
            font-size: 0.75rem;
            padding-top: 0.26rem;
            padding-bottom: 0.26rem;
        }
        [data-testid="stSidebar"] [data-testid="column"] .stButton {
            margin-bottom: 0.15rem;
        }
        [data-testid="block-container"] {
            padding-top: 0;
            padding-left: 1.1rem;
            padding-right: 1.1rem;
        }
        h4 {
            margin-top: 0.15rem;
            margin-bottom: 0.35rem;
        }
        textarea {
            line-height: 1.3;
        }
        /* 기간 선택 드롭다운 텍스트를 더 굵고 크게 */
        [data-testid="stSidebar"] div[data-baseweb="select"] span {
            font-size: 0.9rem;
            font-weight: 700;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    df = load_data()
    if df.empty:
        st.warning("구글시트에 데이터가 없습니다.")
        return

    if WEEK_COL not in df.columns:
        st.error("기간(WEEK) 컬럼을 찾지 못했습니다. 시트의 기간 형식을 확인해 주세요.")
        st.write("현재 열 목록:", list(df.columns))
        return

    ws = get_worksheet()
    dept_cols = get_dept_columns(df)

    if "selected_dept" not in st.session_state:
        st.session_state["selected_dept"] = "전체 부서"

    # ---------------------- Sidebar ----------------------
    with st.sidebar:
        # Title at very top - 글자 크게, 여백 최소
        st.markdown(
            f"<div style='margin-top:0; margin-bottom:0.2rem; font-size:1.5rem; font-weight:700;'>{app_title}</div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<hr style='margin:0.25rem 0; border:0; border-top:1px solid #e0e0e0;' />",
            unsafe_allow_html=True,
        )

        # 인쇄 / 동기화 (섹션 제목 없이 버튼만)
        if st.button("🖨 인쇄 미리보기", use_container_width=True):
            st.session_state["print_requested"] = True

        if st.button("🔄 데이터 동기화", use_container_width=True):
            load_data.clear()
            st.rerun()

        st.markdown(
            "<hr style='margin:0.35rem 0; border:0; border-top:1px solid #e0e0e0;' />",
            unsafe_allow_html=True,
        )

        # 기간 관리 - 섹션 제목 없이 바로 위젯
        week_options = df[WEEK_COL].astype(str).tolist()
        selected_week = st.selectbox(
            "기간 선택",
            options=week_options,
            index=0,
            key="week_select",
        )

        last_week_str = df[WEEK_COL].astype(str).iloc[0]
        last_start, last_end = parse_week_range(last_week_str)
        if last_start and last_end:
            span_days = (last_end - last_start).days + 1
            default_weeks = 1 if span_days <= 7 else 2
        else:
            default_weeks = 2

        st.markdown(
            "<div style='font-size:0.8rem; margin-top:0.1rem; margin-bottom:0.1rem;'>새 기간 길이</div>",
            unsafe_allow_html=True,
        )
        unit_choice = st.radio(
            "",
            ["직전 기간과 동일", "1주", "2주"],
            index=0,
            horizontal=True,
            label_visibility="collapsed",
        )

        if unit_choice == "1주":
            weeks_to_add = 1
        elif unit_choice == "2주":
            weeks_to_add = 2
        else:
            weeks_to_add = default_weeks

        if last_start and last_end:
            new_start = last_end + timedelta(days=1)
        else:
            new_start = datetime.today()

        new_end = new_start + timedelta(days=7 * weeks_to_add - 1)
        new_week_str = f"{new_start:%Y.%m.%d}~{new_end:%Y.%m.%d}"
        st.caption(f"새 기간 미리보기: **{new_week_str}**")

        if st.button("새 기간 행 추가", use_container_width=True):
            headers = ws.row_values(1)
            new_row = ["" for _ in headers]
            if WEEK_COL in headers:
                idx = headers.index(WEEK_COL)
                new_row[idx] = new_week_str
            else:
                ws.insert_cols([WEEK_COL], 1)
                headers = ws.row_values(1)
                new_row = ["" for _ in headers]
                new_row[0] = new_week_str

            ws.insert_row(new_row, index=2, value_input_option="USER_ENTERED")

            load_data.clear()
            st.success(f"새 기간 {new_week_str} 이(가) 추가되었습니다.")
            st.rerun()

        st.markdown(
            "<hr style='margin:0.35rem 0; border:0; border-top:1px solid #e0e0e0;' />",
            unsafe_allow_html=True,
        )

        # 부서 선택 - 섹션 제목 제거, 버튼만
        all_depts = ["전체 부서"] + dept_cols
        current_dept = st.session_state.get("selected_dept", "전체 부서")

        n_cols = 3 if len(all_depts) >= 3 else len(all_depts)
        dept_cols_ui = st.columns(n_cols)

        for i, dept in enumerate(all_depts):
            col = dept_cols_ui[i % n_cols]
            button_type = "primary" if dept == current_dept else "secondary"
            with col:
                if st.button(
                    dept,
                    key=f"dept_btn_{dept}",
                    use_container_width=True,
                    type=button_type,
                ):
                    st.session_state["selected_dept"] = dept
                    current_dept = dept

        dept_filter = current_dept

        st.markdown(
            "<hr style='margin:0.35rem 0; border:0; border-top:1px solid #e0e0e0;' />",
            unsafe_allow_html=True,
        )
        # 부서 관리는 제목 그대로 유지
        st.markdown(
            "<div style='font-weight:600; margin:0.05rem 0 0.2rem;'>부서 관리</div>",
            unsafe_allow_html=True,
        )
        st.caption("표에서 부서명을 직접 수정·추가·삭제 후, 아래 저장 버튼을 눌러주세요.")

        dept_df = pd.DataFrame({"부서": dept_cols})
        edited_dept_df = st.data_editor(
            dept_df,
            num_rows="dynamic",
            use_container_width=True,
            key="dept_editor",
        )

        if st.button("부서 변경 사항 저장", use_container_width=True):
            original = dept_cols
            new_list = [
                str(x).strip()
                for x in edited_dept_df["부서"].tolist()
                if str(x).strip()
            ]

            max_len = max(len(original), len(new_list))
            renames = []
            to_delete = []
            to_add = []

            for i in range(max_len):
                old = original[i] if i < len(original) else None
                new_name = new_list[i] if i < len(new_list) else None

                if old and new_name:
                    if old != new_name:
                        renames.append((old, new_name))
                elif old and not new_name:
                    to_delete.append(old)
                elif new_name and not old:
                    to_add.append(new_name)

            for old, new_name in renames:
                col_idx = get_col_index(ws, old)
                if col_idx is not None:
                    ws.update_cell(1, col_idx, new_name)

            if to_delete:
                col_indices = []
                for name in to_delete:
                    idx = get_col_index(ws, name)
                    if idx is not None:
                        col_indices.append(idx)
                for idx in sorted(col_indices, reverse=True):
                    ws.delete_columns(idx)

            for name in to_add:
                headers_now = ws.row_values(1)
                ws.add_cols(1)
                new_idx = len(headers_now) + 1
                ws.update_cell(1, new_idx, name)

            load_data.clear()
            st.success("부서 설정이 저장되었습니다.")
            st.rerun()

    # ---------------------- Main content ----------------------
    row_df = df[df[WEEK_COL] == selected_week]
    if row_df.empty:
        st.error("선택한 기간의 데이터를 찾을 수 없습니다.")
        return

    row = row_df.iloc[0]
    sheet_row = int(row["_sheet_row"])

    st.markdown(f"#### {selected_week} 업무 내용")

    edited_values = {}

    if dept_filter == "전체 부서":
        # 항상 2열, 화면이 좁으면 자동으로 1열로 떨어짐
        cols_main = st.columns(2)

        for i, dept in enumerate(dept_cols):
            current_text = ""
            if dept in row.index and pd.notna(row[dept]):
                current_text = str(row[dept])

            col = cols_main[i % 2]
            with col:
                with st.container(border=True):
                    st.markdown(f"**{dept}**")
                    edited = st.text_area(
                        label="",
                        value=current_text,
                        height=320,
                        key=f"ta_{dept}",
                        label_visibility="collapsed",
                    )
                    edited_values[dept] = edited
    else:
        dept = dept_filter
        current_text = ""
        if dept in row.index and pd.notna(row[dept]):
            current_text = str(row[dept])

        with st.container(border=True):
            st.markdown(f"**{dept}**")
            edited = st.text_area(
                label="",
                value=current_text,
                height=450,
                key=f"ta_{dept}",
                label_visibility="collapsed",
            )
            edited_values[dept] = edited

    # 저장 버튼
    if st.button("변경 내용 저장", type="primary"):
        cells = []
        for dept, val in edited_values.items():
            col_idx = get_col_index(ws, dept)
            if col_idx is not None:
                cells.append(Cell(row=sheet_row, col=col_idx, value=val))

        if not cells:
            st.error("저장할 대상 부서를 찾지 못했습니다. 헤더 이름을 확인해 주세요.")
        else:
            ws.update_cells(cells)
            load_data.clear()
            st.success("구글 시트에 저장되었습니다.")
            st.rerun()

    # ---------------------- Print preview (separate HTML) ----------------------
    if st.session_state.get("print_requested"):
        # Build printable HTML with selected period & departments
        title_html = escape_html(app_title)
        week_html = escape_html(selected_week)

        sections_html = ""

        if dept_filter == "전체 부서":
            for dept in dept_cols:
                content = ""
                if dept in row.index and pd.notna(row[dept]):
                    content = str(row[dept])
                content_html = escape_html(content)
                sections_html += f"""
                <section style='margin-bottom: 0.8rem;'>
                    <h3 style='margin:0 0 0.15rem 0;'>{escape_html(dept)}</h3>
                    <div style='white-space:normal;font-size:0.85rem;'>{content_html}</div>
                </section>
                """
        else:
            dept = dept_filter
            content = ""
            if dept in row.index and pd.notna(row[dept]):
                content = str(row[dept])
            content_html = escape_html(content)
            sections_html += f"""
            <section style='margin-bottom: 1rem;'>
                <h3 style='margin:0 0 0.2rem 0;'>{escape_html(dept)}</h3>
                <div style='white-space:normal;font-size:0.9rem;'>{content_html}</div>
            </section>
            """

        html = f"""
        <html>
          <head>
            <meta charset="utf-8" />
            <title>{title_html}</title>
            <style>
              @page {{
                size: A4;
                margin: 10mm;
              }}
              body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                font-size: 11px;
                color: #111;
              }}
              h1 {{
                font-size: 16px;
                margin-bottom: 0.3rem;
              }}
              h2 {{
                font-size: 13px;
                margin: 0 0 0.6rem 0;
              }}
              h3 {{
                font-size: 12px;
              }}
            </style>
          </head>
          <body>
            <h1>{title_html}</h1>
            <h2>{week_html}</h2>
            {sections_html}
            <script>
              window.print();
            </script>
          </body>
        </html>
        """
        # Render hidden printable HTML (separate document) and trigger print
        components.html(html, height=0, width=0)
        st.session_state["print_requested"] = False


if __name__ == "__main__":
    main()
