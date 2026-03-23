import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f0f2f6; border-radius: 5px; padding: 10px 15px; font-weight: bold;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    .section-box { background-color: #f8f9fa; padding: 20px; border-radius: 10px; border: 1px solid #dee2e6; margin-bottom: 20px; }
    .section-title { font-size: 1.1rem; font-weight: bold; color: #1E40AF; margin-bottom: 15px; border-left: 5px solid #1E40AF; padding-left: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 심평원 원본 CSV 데이터 조회 함수 ---
@st.cache_data
def get_drug_info(edi_code):
    """심평원 엑셀 원본 헤더를 그대로 사용하는 조회 함수"""
    if not edi_code: return {}
    try:
        # 심평원 원본 CSV 읽기 (인코딩은 엑셀 저장 방식에 따라 utf-8-sig 또는 cp949)
        df = pd.read_csv("drug_master.csv", encoding='utf-8-sig')
        
        # 제품코드를 문자열로 변환 (0 누락 방지)
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        target = df[df['제품코드'] == str(edi_code).strip()]
        
        if not target.empty:
            res = target.iloc[0]
            # 상한금액 콤마 제거 및 정수화
            price_val = str(res['상한금액']).replace(',', '')
            
            return {
                "name": res['제품명'],
                "comp": res['업체명'],
                "price": price_val,
                "spec": f"{res['규격']} {res['단위']}", # 규격과 단위를 합침
                "date": str(res['전일']), # 이미지의 '전일' 열 사용
                "status": "급여" # 기본값
            }
    except Exception as e:
        st.error(f"데이터 조회 오류: drug_master.csv 파일을 확인해주세요. ({e})")
    return {}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_sheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except: return None

sheet = get_sheet()

# --- 4. 왼쪽 사이드바 ---
with st.sidebar:
    st.markdown("# 💊 HISMEDI\n### Drug Service")
    st.divider()
    app_user = st.text_input("👤 신청자 성명")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (기타 요청사항)")

# --- 5. 공통 저장 함수 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    if sheet:
        row_data[1] = app_date
        row_data[2] = app_user
        row_data[54] = app_remark
        row_data[55] = app_status
        sheet.append_row(row_data)
        st.success("데이터베이스에 성공적으로 저장되었습니다!"); st.balloons()

# --- 6. 탭 구성 (대체입고 예시) ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

# [대체입고 탭] - 비포/애프터 정보를 동시에 보여줌
with tabs[2]:
    st.markdown("### ③ 대체입고 신청")
    data = [""] * 56
    data[0] = "대체입고"

    # 1. 기존 약제 구역 (D~M열)
    st.markdown('<div class="section-box"><div class="section-title">기존 약제 정보 (D~M열 반영)</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi_old = c1.text_input("기존 EDI 코드 (제품코드)", key="edi_old")
    m_old = get_drug_info(edi_old) 
    
    name_old = c2.text_input("기존 제품명", value=m_old.get("name", ""), key="n_old")
    comp_old = c3.text_input("기존 업체명", value=m_old.get("comp", ""), key="c_old")
    
    c4, c5, c6 = st.columns(3)
    price_old = c4.text_input("기존 상한금액", value=m_old.get("price", ""), key="p_old")
    spec_old = c5.text_input("기존 규격_단위", value=m_old.get("spec", ""), key="s_old")
    date_old = c6.text_input("기존 적용일(전일)", value=m_old.get("date", ""), key="d_old")
    
    # 구글 시트 D, E, F, H, K, J열 매핑
    data[3], data[4], data[5], data[7], data[10], data[9] = edi_old, name_old, comp_old, price_old, spec_old, date_old
    st.markdown('</div>', unsafe_allow_html=True)

    # 2. 대체 약제 구역 (AK~AT열)
    st.markdown('<div class="section-box"><div class="section-title">대체 약제 정보 (AK~AT열 반영)</div>', unsafe_allow_html=True)
    c7, c8, c9 = st.columns([1, 2, 1])
    edi_new = c7.text_input("대체 EDI 코드 (제품코드)", key="edi_new")
    m_new = get_drug_info(edi_new) 
    
    name_new = c8.text_input("대체 제품명", value=m_new.get("name", ""), key="n_new")
    comp_new = c9.text_input("대체 업체명", value=m_new.get("comp", ""), key="c_new")
    
    c10, c11, c12 = st.columns(3)
    price_new = c10.text_input("대체 상한금액", value=m_new.get("price", ""), key="p_new")
    spec_new = c11.text_input("대체 규격_단위", value=m_new.get("spec", ""), key="s_new")
    date_new = c12.text_input("대체 적용일(전일)", value=m_new.get("date", ""), key="d_new")

    # 구글 시트 AK, AL, AM, AO, AR, AQ열 매핑 (인덱스 36~)
    data[36], data[37], data[38], data[40], data[43], data[42] = edi_new, name_new, comp_new, price_new, spec_new, date_new
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("🚀 대체입고 신청서 제출", use_container_width=True):
        handle_submit(data)
