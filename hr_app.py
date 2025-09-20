# HISMEDI_full_0920_tabs5_fix8_with_perm_admin
# Streamlit app single-file version with "권한관리" admin page restored.
# - Uses Google Sheets "권한" sheet for role-based access control (RBAC).
# - 직무능력평가 탭의 접근권한은 인사평가 탭과 동일하게 강제 동기화됩니다.
# - 첫 줄 공백 제거 (파일 시작 공백 없음).
# - Replace placeholders in st.secrets with your actual values.

import os
import io
import time
from datetime import datetime
from typing import Dict, Any, Tuple

import streamlit as st
import pandas as pd

# ====== Google Sheets (gspread) setup ======
# Expecting st.secrets["gcp_service_account"] (service account json) and st.secrets["google_sheet_key"]
# The "권한" worksheet must exist with headers as defined in REQUIRED_COLS below.
def _lazy_import_gspread():
    import gspread
    from google.oauth2.service_account import Credentials
    return gspread, Credentials

@st.cache_resource(show_spinner=False)
def connect_gsheet():
    gspread, Credentials = _lazy_import_gspread()
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = st.secrets.get("gcp_service_account", None)
    if not info:
        raise RuntimeError("st.secrets['gcp_service_account']가 설정되어 있지 않습니다.")
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    key = st.secrets.get("google_sheet_key", None)
    if not key:
        raise RuntimeError("st.secrets['google_sheet_key']가 설정되어 있지 않습니다.")
    sh = client.open_by_key(key)
    return client, sh

REQUIRED_COLS = ["사번","이름","부서","역할","인사평가","직무능력평가","관리자","비고"]

@st.cache_data(ttl=60, show_spinner=False)
def load_permissions() -> pd.DataFrame:
    _, sh = connect_gsheet()
    try:
        ws = sh.worksheet("권한")
    except Exception as e:
        raise RuntimeError('구글시트에 "권한" 워크시트가 존재하지 않습니다.') from e
    df = pd.DataFrame(ws.get_all_records())
    # Normalize columns
    if df.empty:
        df = pd.DataFrame(columns=REQUIRED_COLS)
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = "" if c in ["사번","이름","부서","역할","비고"] else False
    # Coerce boolean-like
    for c in ["인사평가","직무능력평가","관리자"]:
        df[c] = df[c].apply(lambda x: True if str(x).strip().lower() in ["true","1","y","yes","t","on"] else False)
    # 강제 동기화: 직무능력평가 = 인사평가
    df["직무능력평가"] = df["인사평가"]
    # 사번을 문자열로 정규화
    df["사번"] = df["사번"].astype(str).str.strip()
    return df[REQUIRED_COLS].copy()

def save_permissions(df: pd.DataFrame) -> None:
    # Enforce schema and sync rules before saving
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = "" if c in ["사번","이름","부서","역할","비고"] else False
    df = df[REQUIRED_COLS].copy()
    df["사번"] = df["사번"].astype(str).str.strip()
    # Mirror rule
    df["직무능력평가"] = df["인사평가"]
    # Write back
    _, sh = connect_gsheet()
    ws = sh.worksheet("권한")
    # Prepare payload with header
    payload = [REQUIRED_COLS] + df.fillna("").values.tolist()
    ws.clear()
    ws.update("A1", payload, value_input_option="RAW")

# ====== Auth / RBAC helpers ======
def get_user_session() -> Dict[str, Any]:
    if "auth" not in st.session_state:
        st.session_state["auth"] = {"logged_in": False, "사번":"", "이름":"", "역할":"", "is_admin": False}
    return st.session_state["auth"]

def resolve_user(사번: str, df_perm: pd.DataFrame) -> Dict[str, Any]:
    row = df_perm[df_perm["사번"] == str(사번).strip()]
    if row.empty:
        return {"사번":사번, "이름":"", "역할":"", "is_admin": False, "tabs": {"인사평가": False, "직무능력평가": False}}
    r = row.iloc[0].to_dict()
    return {
        "사번": r.get("사번",""),
        "이름": r.get("이름",""),
        "역할": r.get("역할",""),
        "is_admin": bool(r.get("관리자", False)),
        "tabs": {
            "인사평가": bool(r.get("인사평가", False)),
            # 동기화 규칙: 직무능력평가 = 인사평가
            "직무능력평가": bool(r.get("인사평가", False)),
        },
    }

def has_access(user: Dict[str, Any], tab_key: str) -> bool:
    if user.get("is_admin"):
        return True
    tabs = user.get("tabs", {})
    return bool(tabs.get(tab_key, False))

# ====== UI Sections ======
def ui_login(df_perm: pd.DataFrame):
    auth = get_user_session()
    st.title("HISMEDI 로그인")
    col1, col2 = st.columns([1,1], gap="large")
    with col1:
        사번 = st.text_input("사번", key="login_empno", placeholder="예: 12345", help="숫자만 입력", autocomplete="off")
    with col2:
        pin = st.text_input("PIN", key="login_pin", type="password", placeholder="****", help="사내 개인 PIN")
    # 로그인 버튼만으로 제출 (엔터키 혼란 방지)
    if st.button("로그인", type="primary", use_container_width=True):
        if not 사번 or not pin:
            st.error("사번과 PIN을 모두 입력해주세요.")
            return
        # 여기서는 PIN 검증 로직을 생략하고, 권한표 존재 여부만 확인
        user = resolve_user(사번, df_perm)
        if not user.get("이름"):
            st.error("권한 표에 사번이 등록되어 있지 않습니다. 관리자에게 문의하세요.")
            return
        auth.update({"logged_in": True, **user})
        st.success(f"{user.get('이름','')}님 환영합니다.")
        st.rerun()

def ui_topbar(user: Dict[str, Any]):
    st.sidebar.success(f"접속: {user.get('이름','')} ({user.get('사번','')})")
    st.sidebar.caption(f"역할: {user.get('역할','')}  •  관리자: {'예' if user.get('is_admin') else '아니오'}")
    if st.sidebar.button("로그아웃", type="secondary"):
        st.session_state.clear()
        st.rerun()

def ui_tab_main():
    st.header("메인")
    st.info("여기는 대시보드/요약 정보를 표시하는 자리입니다.")
    # TODO: 실제 메트릭/차트 배치

def ui_tab_hr_eval(user: Dict[str, Any]):
    st.header("인사평가")
    if not has_access(user, "인사평가"):
        st.error("접근 권한이 없습니다. 관리자에게 문의하세요.")
        return
    st.write("인사평가 폼/리스트가 여기에 표시됩니다.")
    # TODO: 구현

def ui_tab_job_eval(user: Dict[str, Any]):
    st.header("직무능력평가")
    # 권한은 인사평가와 동일하게 적용
    if not has_access(user, "직무능력평가"):
        st.error("접근 권한이 없습니다. (인사평가 권한과 동일)")
        return
    st.write("직무능력평가 내용이 여기에 표시됩니다.")
    # TODO: 구현

def ui_admin_permissions():
    st.header("관리자 메뉴 · 권한관리")
    st.caption('데이터 소스: 구글시트 "권한" 워크시트')

    df_perm = load_permissions().copy()

    # 안내
    with st.expander("권한 표 구조 및 규칙", expanded=False):
        st.markdown(
            """
            - 필수 열: **사번, 이름, 부서, 역할, 인사평가, 직무능력평가, 관리자, 비고**
            - **직무능력평가 = 인사평가** (자동 동기화 · 편집 비활성 권장)
            - 관리자=True 인 사용자는 모든 탭 접근 가능
            """
        )

    # 편집 가능한 테이블
    edit_cols = {
        "사번": st.column_config.TextColumn(required=True, width="small"),
        "이름": st.column_config.TextColumn(required=True, width="small"),
        "부서": st.column_config.TextColumn(width="small"),
        "역할": st.column_config.TextColumn(width="small", help="예: 직원/매니저/마스터 등"),
        "인사평가": st.column_config.CheckboxColumn(help="체크 시 인사평가/직무능력평가 접근 허용"),
        # 직무능력평가는 미러 규칙, 편집 비활성화를 위해 읽기 표시만
        "직무능력평가": st.column_config.CheckboxColumn(disabled=True, help="인사평가와 자동 동기화"),
        "관리자": st.column_config.CheckboxColumn(help="체크시 모든 탭 접근 허용"),
        "비고": st.column_config.TextColumn(width="medium"),
    }
    st.subheader("권한 표 편집")
    edited = st.data_editor(
        df_perm,
        key="perm_editor",
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config=edit_cols,
    )

    # 자동 동기화 미러 반영
    edited["직무능력평가"] = edited["인사평가"]

    cols = st.columns([1,1,1,2])
    with cols[0]:
        if st.button("변경 저장", type="primary"):
            try:
                save_permissions(edited)
                st.success("저장 완료 · 구글시트에 반영되었습니다.")
                st.cache_data.clear()
                time.sleep(0.3)
                st.rerun()
            except Exception as e:
                st.exception(e)
    with cols[1]:
        if st.button("새로고침"):
            st.cache_data.clear()
            st.rerun()
    with cols[2]:
        # 백업 다운로드
        csv_bytes = edited.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "CSV로 내보내기",
            data=csv_bytes,
            file_name=f"권한_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    with cols[3]:
        up = st.file_uploader("CSV 업로드(헤더 포함)", type=["csv"], accept_multiple_files=False)
        if up is not None:
            try:
                df_up = pd.read_csv(up)
                # 스키마 보정
                for c in REQUIRED_COLS:
                    if c not in df_up.columns:
                        df_up[c] = "" if c in ["사번","이름","부서","역할","비고"] else False
                df_up = df_up[REQUIRED_COLS]
                df_up["직무능력평가"] = df_up["인사평가"]
                st.dataframe(df_up, use_container_width=True, hide_index=True)
                if st.button("업로드 내용을 저장", type="primary"):
                    save_permissions(df_up)
                    st.success("업로드 내용을 저장했습니다.")
                    st.cache_data.clear()
                    st.rerun()
            except Exception as e:
                st.error(f"업로드 실패: {e}")

# ====== Main Router ======
def main():
    st.set_page_config(page_title="HISMEDI", page_icon="🩺", layout="wide")
    try:
        df_perm = load_permissions()
    except Exception as e:
        st.error("권한 데이터 로드 실패")
        st.exception(e)
        st.stop()

    auth = get_user_session()
    if not auth["logged_in"]:
        ui_login(df_perm)
        st.stop()

    user = get_user_session()
    ui_topbar(user)

    # 탭 구성 (필요시 추가/정렬 변경 가능)
    tabs = st.tabs(["메인","인사평가","직무능력평가","관리자"])

    with tabs[0]:
        ui_tab_main()
    with tabs[1]:
        ui_tab_hr_eval(user)
    with tabs[2]:
        ui_tab_job_eval(user)
    with tabs[3]:
        if user.get("is_admin"):
            ui_admin_permissions()
        else:
            st.error("관리자 전용 메뉴입니다.")

if __name__ == "__main__":
    main()
