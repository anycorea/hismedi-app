import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from io import BytesIO
from datetime import datetime, timedelta
import tempfile
import os

# =========================
# 설정값
# =========================
NOTICE_COL = "공지·결정사항"   # 공지 칼럼 이름
WEEK_COL = "WEEK"             # 주 표시 칼럼 이름

# =========================
# 구글시트 연결
# =========================
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
    구글시트 전체를 DataFrame으로 가져오기.
    - get_all_values()로 값만 가져온 뒤
    - 헤더의 빈 칸을 'Unnamed_n' 으로 치환
    - 행마다 칼럼 개수 맞춰서 DataFrame 생성
    """
    ws = get_worksheet()
    values = ws.get_all_values()   # [[헤더...], [row1...], [row2...], ...]

    if not values or len(values) < 2:
        return pd.DataFrame()

    raw_header = values[0]
    rows = values[1:]

    # 1) 헤더 정리: 빈 헤더("")는 Unnamed_1, Unnamed_2 ... 로 바꿔서 중복 제거
    header = []
    for i, h in enumerate(raw_header):
        h = str(h).strip()
        if not h:
            h = f"Unnamed_{i+1}"
        header.append(h)

    n_cols = len(header)

    # 2) 각 행 길이를 헤더 길이에 맞게 패딩/자르기
    normalized_rows = []
    for r in rows:
        if len(r) < n_cols:
            r = r + [""] * (n_cols - len(r))
        elif len(r) > n_cols:
            r = r[:n_cols]
        normalized_rows.append(r)

    df = pd.DataFrame(normalized_rows, columns=header)

    # 3) 완전히 빈 'Unnamed_*' 컬럼은 정리해서 없애기 (헤더만 있고 내용이 전부 공백이면 삭제)
    unnamed_cols = [c for c in df.columns if c.startswith("Unnamed_")]
    for c in unnamed_cols:
        if df[c].replace("", pd.NA).isna().all():
            df.drop(columns=[c], inplace=True)

    # 4) WEEK 기준 최신순 정렬
    def parse_start_date(week_str):
        try:
            start = str(week_str).split("~")[0].strip()
            return datetime.strptime(start, "%Y.%m.%d")
        except Exception:
            return datetime.min

    if WEEK_COL in df.columns:
        df["_start_date"] = df[WEEK_COL].astype(str).apply(parse_start_date)
        df = df.sort_values("_start_date", ascending=False).reset_index(drop=True)

    return df


def get_dept_columns(df: pd.DataFrame):
    """부서 컬럼 목록 (WEEK, 공지 제외)"""
    exclude = {WEEK_COL, NOTICE_COL}
    return [c for c in df.columns if c not in exclude and not c.startswith("_")]


def parse_week_range(week_str: str):
    """'YYYY.MM.DD~YYYY.MM.DD' -> (start, end)"""
    try:
        start_str, end_str = str(week_str).split("~")
        start = datetime.strptime(start_str.strip(), "%Y.%m.%d")
        end = datetime.strptime(end_str.strip(), "%Y.%m.%d")
        return start, end
    except Exception:
        return None, None


def get_row_info(df: pd.DataFrame, selected_week: str):
    """선택한 주에 해당하는 df row와, 시트 row 번호"""
    row = df[df[WEEK_COL] == selected_week].head(1)
    if row.empty:
        return None, None
    row = row.iloc[0]
    sheet_row = row.name + 2  # 헤더 1행 + index 보정
    return row, sheet_row


def export_to_excel(df: pd.DataFrame) -> BytesIO:
    """필터링된 df를 엑셀 파일로 변환"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="주간업무")
    output.seek(0)
    return output


def export_to_pdf(df: pd.DataFrame) -> BytesIO:
    """
    DataFrame -> 간단한 PDF 변환
    pdfkit + wkhtmltopdf 가 설치돼 있어야 동작합니다.
    (설치 안돼 있으면 호출 시 예외 발생)
    """
    import pdfkit

    html = df.to_html(index=False, border=0)
    tmp_html = tempfile.NamedTemporaryFile(suffix=".html", delete=False)
    tmp_html.write(html.encode("utf-8"))
    tmp_html.close()

    pdf_path = tmp_html.name.replace(".html", ".pdf")
    pdfkit.from_file(tmp_html.name, pdf_path)

    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    os.remove(tmp_html.name)
    os.remove(pdf_path)

    return BytesIO(pdf_bytes)


def main():
    st.set_page_config(page_title="주간 업무 관리", layout="wide")
    st.title("주간 업무 관리 (Google Sheets 연동)")

    df = load_data()
    if df.empty:
        st.warning("구글시트에 데이터가 없습니다.")
        return

    dept_cols = get_dept_columns(df)
    ws = get_worksheet()
    headers = ws.row_values(1)

    def get_col_index(col_name: str):
        try:
            return headers.index(col_name) + 1
        except ValueError:
            return None

    # ---------------------
    # 왼쪽 사이드바
    # ---------------------
    with st.sidebar:
        st.header("조건 선택")

        week_options = df[WEEK_COL].astype(str).tolist()
        selected_week = st.selectbox(
            "주(기간) 선택",
            options=week_options,
            index=0,  # 최신
        )

        dept_options = ["전체 보기"] + dept_cols
        selected_dept = st.radio(
            "부서 선택",
            options=dept_options,
            horizontal=True,
        )

        st.markdown("---")
        st.subheader("엑셀 / PDF")

        range_opt = st.radio(
            "출력 범위",
            ["선택한 기간만", "최근 2개 기간", "전체"],
            index=0,
        )

        if range_opt == "선택한 기간만":
            export_df = df[df[WEEK_COL] == selected_week].copy()
        elif range_opt == "최근 2개 기간":
            export_df = df.head(2).copy()
        else:
            export_df = df.copy()

        if selected_dept != "전체 보기":
            keep_cols = [c for c in [WEEK_COL, NOTICE_COL, selected_dept] if c in export_df.columns]
            export_df = export_df[keep_cols]

        excel_data = export_to_excel(export_df)
        st.download_button(
            "엑셀 다운로드",
            data=excel_data,
            file_name="주간업무.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # PDF (선택)
        try:
            pdf_data = export_to_pdf(export_df)
            st.download_button(
                "PDF 다운로드",
                data=pdf_data,
                file_name="주간업무.pdf",
                mime="application/pdf",
            )
        except Exception:
            st.caption("※ PDF는 pdfkit / wkhtmltopdf 설치 후 사용 가능합니다.")

        st.info("PDF를 열어서 프린터로 출력하시면 됩니다.")

        # ---------------------
        # 새 기간(1주/2주) 자동 추가
        # ---------------------
        st.markdown("---")
        st.subheader("새 기간 추가")

        # 마지막 기간 파싱
        last_week_str = df[WEEK_COL].astype(str).iloc[0]
        last_start, last_end = parse_week_range(last_week_str)
        last_span_days = (last_end - last_start).days + 1 if last_start and last_end else 14
        last_weeks = 1 if last_span_days <= 7 else 2

        unit_choice = st.radio(
            "작성 단위",
            ["자동(직전 기간과 동일)", "1주", "2주"],
            index=0,
        )

        if unit_choice == "1주":
            weeks_to_add = 1
        elif unit_choice == "2주":
            weeks_to_add = 2
        else:
            weeks_to_add = last_weeks

        if last_start and last_end:
            new_start = last_end + timedelta(days=1)
        else:
            # 만약 파싱 실패 시 오늘 기준으로 시작
            new_start = datetime.today()

        new_end = new_start + timedelta(days=7 * weeks_to_add - 1)
        new_week_str = f"{new_start:%Y.%m.%d}~{new_end:%Y.%m.%d}"

        st.caption(f"미리보기: 새 기간 → **{new_week_str}**")

        if st.button("새 기간 행 추가"):
            # 새 행: 기존 헤더 길이에 맞게 모두 빈 문자열, WEEK만 채우기
            headers = ws.row_values(1)
            new_row = ["" for _ in headers]

            if WEEK_COL in headers:
                idx = headers.index(WEEK_COL)
                new_row[idx] = new_week_str
            else:
                # WEEK 컬럼이 없으면 맨 앞에 추가
                ws.insert_cols([WEEK_COL], 1)
                headers = ws.row_values(1)
                new_row = ["" for _ in headers]
                new_row[0] = new_week_str

            ws.append_row(new_row)
            load_data.clear()
            st.success(f"새 기간 {new_week_str} 행이 추가되었습니다.")
            st.rerun()

        # ---------------------
        # 부서(열) 관리
        # ---------------------
        st.markdown("---")
        st.subheader("부서 관리")

        st.caption("※ 실제로는 시트의 열을 추가/이름변경/삭제합니다.")

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
                    headers = ws.row_values(1)
                    new_col_idx = len(headers) + 1
                    ws.update_cell(1, new_col_idx, new_dept)
                    load_data.clear()
                    st.success(f"부서 '{new_dept}' 열이 추가되었습니다.")
                    st.rerun()

        elif manage_mode == "부서 이름 변경":
            target = st.selectbox("대상 부서", dept_cols)
            new_name = st.text_input("새 부서 이름")
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
            target = st.selectbox("삭제할 부서", dept_cols)
            if st.button("부서 삭제 실행"):
                col_idx = get_col_index(target)
                if col_idx is None:
                    st.error("해당 부서를 찾을 수 없습니다.")
                else:
                    ws.delete_columns(col_idx)
                    load_data.clear()
                    st.success(f"부서 '{target}' 열이 삭제되었습니다.")
                    st.rerun()

    # ---------------------
    # 오른쪽: 조회 / 입력
    # ---------------------
    col1, col2 = st.columns([1.2, 1.8])

    with col1:
        st.subheader("기간·부서 요약")

        st.markdown(f"- **선택한 기간**: {selected_week}")
        st.markdown(f"- **선택한 부서**: {selected_dept}")

        display_cols = [WEEK_COL]
        if NOTICE_COL in df.columns:
            display_cols.append(NOTICE_COL)
        display_cols += dept_cols

        week_df = df[df[WEEK_COL] == selected_week][display_cols]
        st.dataframe(week_df, use_container_width=True)

    with col2:
        st.subheader("내용 입력 / 수정")

        row, sheet_row = get_row_info(df, selected_week)
        if row is None:
            st.error("선택한 기간에 해당하는 행을 찾을 수 없습니다.")
            return

        # 공지·결정사항
        notice_col_idx = get_col_index(NOTICE_COL)
        notice_text = ""
        if NOTICE_COL in row.index and pd.notna(row[NOTICE_COL]):
            notice_text = str(row[NOTICE_COL])

        notice_text_new = st.text_area(
            "공지·결정사항",
            value=notice_text,
            height=180,
            placeholder="공통 공지, 결정사항을 입력하세요.",
        )

        # 선택된 부서 내용
        dept_text_new = None
        dept_col_idx = None
        if selected_dept != "전체 보기":
            dept_col_idx = get_col_index(selected_dept)
            existing = ""
            if selected_dept in row.index and pd.notna(row[selected_dept]):
                existing = str(row[selected_dept])

            dept_text_new = st.text_area(
                f"{selected_dept} 내용",
                value=existing,
                height=260,
                placeholder="해당 부서의 업무 내용을 입력하세요.\n예) - 회의 일정\n- 보고서 작성\n- 링크: https://...",
            )
        else:
            st.info("특정 부서를 수정하려면 왼쪽에서 부서를 선택하세요.")

        if st.button("현재 내용 저장", type="primary"):
            updates = []

            if notice_col_idx is not None:
                updates.append(gspread.cell.Cell(row=sheet_row, col=notice_col_idx, value=notice_text_new))

            if selected_dept != "전체 보기" and dept_col_idx is not None:
                updates.append(gspread.cell.Cell(row=sheet_row, col=dept_col_idx, value=dept_text_new))

            if not updates:
                st.error("수정할 칼럼을 찾지 못했습니다. 헤더 이름을 확인하세요.")
            else:
                ws.update_cells(updates)
                load_data.clear()
                st.success("구글 시트에 저장되었습니다.")
                st.rerun()


if __name__ == "__main__":
    main()
