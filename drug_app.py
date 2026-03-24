import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1.5rem !important; }
    [data-testid="stHeader"] { display: none; }
    .stTabs [data-baseweb="tab"] { font-weight: 700; }
    .section-header { font-size: 1.1rem; font-weight: 800; color: #1E3A8A; margin: 20px 0 10px 0; border-bottom: 2px solid #1E3A8A; }
    .drug-table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; padding: 8px; border: 1px solid #e2e8f0; text-align: center; font-weight: 600; }
    /* 상태별 색상 */
    .status-ready { color: #2563eb; } /* 신청완료 - 파랑 */
    .status-ing { color: #d97706; }   /* 처리중 - 주황 */
    .status-done { color: #059669; }  /* 처리완료 - 초록 */
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
@st.cache_resource
def get_spreadsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=60) # 대시보드를 위해 캐시 시간을 줄임
def load_master_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("Master").get_all_records())
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=10) # 상태 시트는 더 자주 새로고침
def load_status_data():
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("Status")
        data = ws.get_all_records()
        return pd.DataFrame(data)
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 공통 옵션 ---
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.title("HISMEDI Drug")
    st.divider()
    user_name = st.text_input("접속자 성명", key="user_name")
    access_mode = st.radio("접속 모드", ["신청부서", "처리부서"])

# --- 5. 헬퍼 함수 ---
def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, label_title="약제 정보"):
    m = get_drug_info(edi_val, master_df)
    st.markdown(f"**{label_title}**")
    price = str(m.get("상한금액", "-")).replace(',', '')
    table_html = f"""<table class="drug-table">
        <tr><th>제품코드</th><th>제품명</th><th>업체명</th><th>규격</th></tr>
        <tr><td>{edi_val if edi_val else "-"}</td><td style="color:#1E40AF">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위</th><th>상한금액</th><th>주성분명</th><th>의약품 구분</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td style="color:#dc2626">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>"""
    st.markdown(table_html, unsafe_allow_html=True)
    return [edi_val, m.get("제품명", ""), m.get("전일", "")]

def save_to_status(category, p1_info, p2_info, stop_date="", start_date=""):
    """ Status 시트에 신청 정보 저장 """
    if not user_name: 
        st.error("접속자 성명을 입력해야 제출이 가능합니다."); return False
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("Status")
        # 헤더: 진행상황-신청일-신청자-처리자-신청구분-제품코드1-제품명1-사용중지일1-제품코드2-제품명2-사용시작일2
        new_row = [
            "신청완료", 
            datetime.now().strftime('%Y-%m-%d'), 
            user_name, 
            "", 
            category, 
            p1_info[0], p1_info[1], stop_date,
            p2_info[0], p2_info[1], start_date
        ]
        ws.append_row(new_row)
        return True
    except Exception as e:
        st.error(f"저장 실패: {e}"); return False

# --- 6. 메인 탭 구성 ---
tabs = st.tabs(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가변경", "🔍 약가조회", "📊 진행현황"])

# [Tab 0-4] 기존 신청 양식 (간소화 요약)
with tabs[0]:
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t0_edi")
    info = render_drug_table(edi)
    stop_d = st.date_input("사용중지일", key="t0_d").strftime('%Y-%m-%d')
    if st.button("🚀 사용중지 제출"):
        if save_to_status("사용중지", info, ["",""], stop_date=stop_d): st.success("접수되었습니다."); st.balloons()

with tabs[1]:
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t1_edi")
    info = render_drug_table(edi)
    start_d = st.date_input("입고일", key="t1_d").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 제출"):
        if save_to_status("신규입고", info, ["",""], start_date=start_d): st.success("접수되었습니다.")

with tabs[2]:
    st.markdown('<div class="section-header">대체입고 신청</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1: e1 = st.text_input("기존 제품코드", key="t2_e1"); info1 = render_drug_table(e1, "기존 약제")
    with c2: e2 = st.text_input("대체 제품코드", key="t2_e2"); info2 = render_drug_table(e2, "대체 약제")
    d1 = st.date_input("중지일", key="t2_d1").strftime('%Y-%m-%d')
    d2 = st.date_input("입고일", key="t2_d2").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 제출"):
        if save_to_status("대체입고", info1, info2, d1, d2): st.success("접수되었습니다.")

# ... (나머지 신청 탭들은 유사한 방식으로 save_to_status 호출) ...

# [Tab 6] 진행현황 Dashboard (핵심)
with tabs[6]:
    st.markdown('<div class="section-header">📊 실시간 진행현황 대시보드</div>', unsafe_allow_html=True)
    
    # 1. 데이터 로드
    status_df = load_status_data()
    
    if status_df.empty:
        st.info("현재 신청된 내역이 없습니다.")
    else:
        # 2. 통합 검색 기능
        search_q = st.text_input("🔍 검색 (제품명, 신청자, 코드 등 무엇이든 입력)", key="dash_search")
        if search_q:
            status_df = status_df[status_df.apply(lambda row: row.astype(str).str.contains(search_q).any(), axis=1)]

        # 3. 요약 테이블 표시
        st.dataframe(status_df, use_container_width=True, hide_index=False)

        st.divider()

        # 4. 상세보기 및 상태 변경 (처리부서용)
        st.subheader("⚙️ 항목 상세 처리")
        selected_index = st.number_input("처리할 행 번호(왼쪽 숫자)를 입력하세요", min_value=0, max_value=len(status_df)-1, step=1)
        
        if st.button("✅ 선택한 항목 상세보기"):
            selected_row = status_df.iloc[selected_index]
            st.write(selected_row) # 상세 데이터 표시
            
            if access_mode == "처리부서":
                col1, col2 = st.columns(2)
                new_stat = col1.selectbox("상태 변경", ["신청완료", "처리중", "처리완료"], index=["신청완료", "처리중", "처리완료"].index(selected_row['진행상황']))
                proc_name = col2.text_input("처리자 성명 기록", value=user_name)
                
                if st.button("💾 상태 업데이트 저장"):
                    try:
                        ss = get_spreadsheet()
                        ws = ss.worksheet("Status")
                        # 구글 시트는 1부터 시작하고 헤더가 있으므로 index + 2
                        row_num = selected_index + 2 
                        ws.update_cell(row_num, 1, new_stat) # 진행상황 컬럼(1번) 업데이트
                        ws.update_cell(row_num, 4, proc_name) # 처리자 컬럼(4번) 업데이트
                        st.success(f"{row_num}번 행의 상태가 {new_stat}(으)로 변경되었습니다.")
                        st.cache_data.clear() # 데이터 새로고침
                    except Exception as e:
                        st.error(f"업데이트 실패: {e}")
