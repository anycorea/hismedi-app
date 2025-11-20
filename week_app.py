import re
from datetime import datetime, timedelta

import gspread
from gspread.cell import Cell
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# ===== 기본 설정 =====
WEEK_COL = "WEEK"  # 시트에서 기간이 들어있는 열 이름

# ===== 구글 시트 연결 =====
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
    """
    시트 전체를 DataFrame으로 변환.
    - get_all_values()로 값만 가져온 뒤
    - 빈 헤더 처리 / 행 길이 정규화
    - 기간 패턴 열을 찾아 WEEK 컬럼으로 보정
    """
    ws = get_worksheet()
    values = ws.get_all_values()

    if not values or len(values) < 2:
        return pd.DataFrame()

    raw_header = values[0]
    rows = values[1:]

    # 1) 헤더 정리
    header = []
    for i, h in enumerate(raw_header):
        h = str(h).strip()
        if not h:
            h = f"Unnamed_{i+1}"
        header.append(h)

    n_cols = len(header)

    # 2) 행 길이 맞추기
    normalized_rows = []
    for r in rows:
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        normalized_rows.append(r)

    df = pd.DataFrame(normalized_rows, columns=header)

    # 3) 완전히 빈 Unnamed_* 컬럼 제거
    for c in [c for c in df.columns if c.startswith("Unnamed_")]:
        if df[c].replace("", pd.NA).isna().all():
            df.drop(columns=[c], inplace=True)

    # 4) 기간 컬럼 자동 탐지 (YYYY.MM.DD~YYYY.MM.DD)
    pattern = re.compile(r"\d{4}\.\d{2}\.\d{2}\s*~\s*\d{4}\.\d{2}\.\d{2}")
    week_col_name = None
    for col in df.columns:
        s = df[col].astype(str)
        if s.apply(lambda x: bool(pattern.fullmatch(x.strip()))).any():
            week_col_name = col
            break

    if week_col_name is None:
        # 기간 정보를 찾을 수 없으면 그대로 반환
        return df

    if WEEK_COL not in df.columns:
        df[WEEK_COL] = df[week_col_name]

    # 5) WEEK 기준 최신순 정렬
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
    """부서(열) 목록: WEEK와 내부 컬럼 제외"""
    return [c for c in df.columns if c not in [WEEK_COL] and not c.startswith("_")]


def parse_week_range(week_str: str):
    """'YYYY.MM.DD~YYYY.MM.DD' -> (start, end)"""
    try:
        s, e = week_str.split("~")
        start = datetime.strptime(s.strip(), "%Y.%m.%d")
        end = datetime.strptime(e.strip(), "%Y.%m.%d")
        return start, end
    except Exception:
        return None, None


def main():
    # 앱 제목 (secrets에 있으면 사용)
    app_title = "HISMEDI † Weekly report"
    try:
        app_title = st.secrets["app"].get("TITLE", app_title)
    except Exception:
        pass

    st.set_page_config(page_title=app_title, layout="wide")
    st.title(app_title)

    df = load_data()
    if df.empty:
        st.warning("구글시트에 데이터가 없습니다.")
        return

    if WEEK_COL not in df.columns:
        st.error("기간(WEEK) 컬럼을 찾지 못했습니다. 시트의 기간 형식을 확인해 주세요.")
        st.write("현재 열 목록:", list(df.columns))
        return

    dept_cols = get_dept_columns(df)
    ws = get_worksheet()
    headers = ws.row_values(1)

    def get_col_index(col_name: str):
        try:
            return headers.index(col_name) + 1  # 1-based
        except ValueError:
            return None

    # -----------------------
    # 사이드바: 조건 / 새 기간 / 부서 관리
    # -----------------------
    with st.sidebar:
        st.markdown("### 조건 선택")

        week_options = df[WEEK_COL].astype(str).tolist()
        selected_week = st.selectbox(
            "기간 선택",
            options=week_options,
            index=0,
        )

        dept_filter = st.radio(
            "부서 선택",
            options=["전체 부서"] + dept_cols,
            index=0,
        )

        st.markdown("---")
        st.markdown("### 새 기간 추가")

        # 마지막 기간 기준으로 다음 기간 계산
        last_week_str = df[WEEK_COL].astype(str).iloc[0]
        last_start, last_end = parse_week_range(last_week_str)
        if last_start and last_end:
            span_days = (last_end - last_start).days + 1
            default_weeks = 1 if span_days <= 7 else 2
        else:
            default_weeks = 2

        unit_choice = st.radio(
            "기간 단위",
            ["직전 기간과 동일", "1주", "2주"],
            index=0,
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

        if st.button("새 기간 행 추가"):
            # 헤더 개수만큼 빈 문자열 생성 후 WEEK 위치에만 값 세팅
            headers = ws.row_values(1)
            new_row = ["" for _ in headers]
            if WEEK_COL in headers:
                idx = headers.index(WEEK_COL)
                new_row[idx] = new_week_str
            else:
                # WEEK 열이 없다면 맨 앞에 추가
                ws.insert_cols([WEEK_COL], 1)
                headers = ws.row_values(1)
                new_row = ["" for _ in headers]
                new_row[0] = new_week_str

            # 항상 append_row로 마지막에 추가 → 기존 데이터 덮어쓰지 않음
            ws.append_row(new_row, value_input_option="USER_ENTERED")
            load_data.clear()
            st.success(f"새 기간 {new_week_str} 이(가) 추가되었습니다.")
            st.rerun()

        st.markdown("---")
        st.markdown("### 부서 관리")

        st.caption("현재 부서 목록")
        st.table(pd.DataFrame({"부서": dept_cols}))

        manage_mode = st.radio(
            "작업 선택",
            ["부서 추가", "부서 이름 변경", "부서 삭제"],
            index=0,
        )

        if manage_mode == "부서 추가":
            new_dept = st.text_input("새 부서 이름")
            if st.button("부서 추가 실행"):
                if not new_dept:
                    st.warning("부서 이름을 입력해 주세요.")
                elif new_dept in headers:
                    st.warning("이미 존재하는 부서입니다.")
                else:
                    ws.add_cols(1)
                    headers_now = ws.row_values(1)
                    new_col_idx = len(headers_now) + 1
                    ws.update_cell(1, new_col_idx, new_dept)
                    load_data.clear()
                    st.success(f"부서 '{new_dept}' 열이 추가되었습니다.")
                    st.rerun()

        elif manage_mode == "부서 이름 변경":
            target = st.selectbox("변경할 부서", dept_cols, key="rename_target")
            new_name = st.text_input("새 부서 이름", key="rename_new")
            if st.button("부서 이름 변경 실행"):
                if not new_name:
                    st.warning("새 이름을 입력해 주세요.")
                else:
                    col_idx = get_col_index(target)
                    if col_idx is None:
                        st.error("해당 부서를 찾을 수 없습니다.")
                    else:
                        ws.update_cell(1, col_idx, new_name)
                        load_data.clear()
                        st.success(f"'{target}' → '{new_name}' 으로 변경되었습니다.")
                        st.rerun()

        else:  # 부서 삭제
            target = st.selectbox("삭제할 부서", dept_cols, key="delete_target")
            if st.button("부서 삭제 실행"):
                col_idx = get_col_index(target)
                if col_idx is None:
                    st.error("해당 부서를 찾을 수 없습니다.")
                else:
                    ws.delete_columns(col_idx)
                    load_data.clear()
                    st.success(f"부서 '{target}' 열이 삭제되었습니다.")
                    st.rerun()

    # -----------------------
    # 메인 영역: 인쇄 + 내용 편집
    # -----------------------
    # 인쇄 버튼 (브라우저 프린트)
    col_print, _ = st.columns([1, 5])
    with col_print:
        if st.button("🖨 인쇄"):
            st.markdown(
                """
                <script>
                window.print();
                </script>
                """,
                unsafe_allow_html=True,
            )

    # 선택한 기간 한 행 가져오기
    row_df = df[df[WEEK_COL] == selected_week]
    if row_df.empty:
        st.error("선택한 기간의 데이터를 찾을 수 없습니다.")
        return

    row = row_df.iloc[0]
    sheet_row = row.name + 2  # 헤더 1행 보정

    st.markdown(f"### {selected_week} 업무 내용")

    # 편집용 텍스트 영역들
    edited_values = {}

    if dept_filter == "전체 부서":
        # 모든 부서를 카드처럼 나열
        for dept in dept_cols:
            current_text = ""
            if dept in row.index and pd.notna(row[dept]):
                current_text = str(row[dept])

            with st.expander(dept, expanded=True):
                edited = st.text_area(
                    label=dept,
                    value=current_text,
                    height=200,
                    key=f"ta_{dept}",
                )
                edited_values[dept] = edited
    else:
        # 선택한 부서만 크게 표시
        dept = dept_filter
        current_text = ""
        if dept in row.index and pd.notna(row[dept]):
            current_text = str(row[dept])

        edited = st.text_area(
            label=dept,
            value=current_text,
            height=400,
            key=f"ta_{dept}",
        )
        edited_values[dept] = edited

    if st.button("변경 내용 저장", type="primary"):
        cells: list[Cell] = []
        for dept, val in edited_values.items():
            col_idx = get_col_index(dept)
            if col_idx is not None:
                cells.append(Cell(row=sheet_row, col=col_idx, value=val))

        if not cells:
            st.error("저장할 대상 부서를 찾지 못했습니다. 헤더 이름을 확인해 주세요.")
        else:
            ws.update_cells(cells)
            load_data.clear()
            st.success("구글 시트에 저장되었습니다.")
            st.rerun()


if __name__ == "__main__":
    main()
