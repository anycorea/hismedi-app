import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Noto Sans KR', sans-serif; }
    
    .main-title { font-size: 2.5rem; font-weight: 800; color: #1E3A8A; margin-bottom: 0.5rem; text-align: center; }
    .sub-title { font-size: 1.1rem; color: #64748B; text-align: center; margin-bottom: 2rem; }
    
    /* 카드 스타일 */
    .stForm { background-color: #F8FAFC; padding: 2rem; border-radius: 15px; border: 1px solid #E2E8F0; }
    .section-header { font-size: 1.2rem; font-weight: 700; color: #1E40AF; border-left: 5px solid #1E40AF; padding-left: 10px; margin: 1.5rem 0 1rem 0; }
    
    /* 탭 스타일 */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #F1F5F9; border-radius: 8px 8px 0 0; padding: 10px 20px; font-weight: 600;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. API 및 시트 연결 함수 ---
@st.cache_data(ttl=3600)
def get_hira_info(edi_code):
    """심평원 API 연동"""
    if not edi_code: return {}
    api_key = st.secrets.get("hira_api_key", "")
    url = "http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList"
    params = {'serviceKey': api_key, 'mdsCd': edi_code, 'numOfRows': '1', 'pageNo': '1'}
    try:
        res = requests.get(url, params=params, timeout=5)
        root = ET.fromstring(res.text)
        item = root.find(".//item")
        if item is not None:
            return {
                "name": item.findtext("itmNm", ""), "comp": item.findtext("enpNm", ""),
                "price": item.findtext("maxAmt", "0"), "spec": item.findtext("unit", ""),
                "date": item.findtext("applcStdt", ""), "status": "급여"
            }
    except: pass
    return {}

@st.cache_resource
def get_sheet():
    """구글 시트 연결"""
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        return client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except Exception as e:
        st.error(f"시트 연결 실패: {e}")
        return None

sheet = get_sheet()

# --- 3. 공통 UI 및 데이터 처리 ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">약제 사용중지, 신규/대체 입고 및 단가 관리 시스템</div>', unsafe_allow_html=True)

# 시트 열 인덱스 매핑 (A=0, B=1 ... BD=55)
COL = {
    '신청구분':0, '신청일':1, '신청자':2, '비고':54, '진행상황':55,
    'EDI_신규':3, '약명_신규':4, '업체_신규':5, '급여_신규':6, '상한가_신규':7, '상태_신규':8, '적용일_신규':9, '규격_신규':10, '구입처_신규':11, '입고가_신규':12,
    '중지일':13, '중지사유':14, '중지사유기타':15, '변경내용':16, '재고여부':17, '재고처리':18, '재고량':19, '반품가능':20, '반품예정일':21, '반품량':22,
    '중지일2':23, '병용여부':25, '원내구분':26, '급여구분':27, '요청과':28, '원내유무':29, '사용기간':30, '입고일':31, '사용시작일':32, '상한가외사유':33,
    'EDI_대체':36, '약명_대체':37, '업체_대체':38, '급여_대체':39, '상한가_대체':40, '상태_대체':41, '적용일_대체':42, '규격_대체':43, '구입처_대체':44, '입고가_대체':45,
    '원내_대체':46, '급여_대체2':47, '요청사유':48, '시작일_대체':49, '상한가외_대체':50, '병용_대체':51, '기간_대체':52, '입고_대체':53
}

# 공통 정보 (Sidebar)
with st.sidebar:
    st.header("📋 공통 신청 정보")
    app_user = st.text_input("신청자 명", placeholder="성함을 입력하세요")
    app_date = st.date_input("신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("진행 상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("비고 (기타 요청사항)")
    st.divider()
    st.info("EDI 코드를 입력하면 심평원 데이터베이스에서 약품 정보를 자동으로 불러옵니다.")

# --- 4. 메인 폼 (탭 구성) ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

def save_data(row_data):
    if not app_user:
        st.error("신청자 이름을 입력해주세요!"); return
    try:
        row_data[COL['신청일']] = app_date
        row_data[COL['신청자']] = app_user
        row_data[COL['비고']] = app_remark
        row_data[COL['진행상황']] = app_status
        sheet.append_row(row_data)
        st.success("데이터가 성공적으로 저장되었습니다!"); st.balloons()
    except Exception as e:
        st.error(f"저장 실패: {e}")

# 탭별 레이아웃 정의
for i, tab_name in enumerate(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가변경(인하)", "단가변경(인상)"]):
    with tabs[i]:
        row = [""] * 56
        row[COL['신청구분']] = tab_name
        
        st.markdown(f'<div class="section-header">{tab_name} 정보 입력</div>', unsafe_allow_html=True)
        
        # [공통/기본 약제 정보 섹션]
        c1, c2, c3 = st.columns([1, 2, 1])
        edi_main = c1.text_input("EDI 코드", key=f"edi_{i}")
        m = get_hira_info(edi_main)
        
        name_main = c2.text_input("약품명", value=m.get("name", ""), key=f"nm_{i}")
        comp_main = c3.text_input("제조/업체명", value=m.get("comp", ""), key=f"cp_{i}")
        
        c4, c5, c6, c7 = st.columns(4)
        price_main = c4.text_input("상한금액", value=m.get("price", ""), key=f"pr_{i}")
        spec_main = c5.text_input("규격/단위", value=m.get("spec", ""), key=f"sp_{i}")
        date_main = c6.text_input("적용일자", value=m.get("date", ""), key=f"dt_{i}")
        status_main = c7.selectbox("현재상태", ["급여", "비급여", "저가", "퇴장방지"], key=f"st_{i}")

        # 기본 정보 매핑 (D~M열 공통)
        row[COL['EDI_신규']], row[COL['약명_신규']], row[COL['업체_신규']] = edi_main, name_main, comp_main
        row[COL['상한가_신규']], row[COL['규격_신규']], row[COL['적용일_신규']] = price_main, spec_main, date_main
        row[COL['상태_신규']] = status_main

        # [상세 정보 섹션 - 신청종류별 가변]
        st.markdown('<div class="section-header">세부 항목</div>', unsafe_allow_html=True)
        
        if i == 0: # 사용중지
            cc1, cc2, cc3 = st.columns(3)
            row[COL['중지일']] = cc1.date_input("사용중지일").strftime('%Y-%m-%d')
            row[COL['중지사유']] = cc2.selectbox("중지사유", ["자진취하", "생산중단", "대체품목발생", "기타"])
            row[COL['중지사유기타']] = cc3.text_input("중지사유(기타)")
            
            sc1, sc2, sc3 = st.columns(3)
            row[COL['재고여부']] = sc1.radio("재고여부", ["유", "무"], horizontal=True)
            row[COL['재고처리']] = sc2.selectbox("재고처리방법", ["반품", "소진시까지사용", "폐기"])
            row[COL['재고량']] = sc3.text_input("현재 재고량")

        elif i == 1: # 신규입고
            nc1, nc2, nc3 = st.columns(3)
            row[COL['요청과']] = nc1.text_input("입고요청 진료과")
            row[COL['원내유무']] = nc2.selectbox("원내유무(동일성분)", ["유", "무"])
            row[COL['입고일']] = nc3.date_input("입고일").strftime('%Y-%m-%d')
            
            nc4, nc5 = st.columns(2)
            row[COL['사용기간']] = nc4.text_input("사용기간(신규)")
            row[COL['상한가외사유']] = nc5.text_input("상한가 외 입고사유")

        elif i == 2: # 대체입고
            st.info("💡 아래에 대체/변경될 약제 정보를 입력하세요.")
            # 대체 약제 정보 (AK~BB열 등)
            dc1, dc2, dc3 = st.columns([1,2,1])
            edi_sub = dc1.text_input("대체 EDI 코드", key=f"edi_sub_{i}")
            sm = get_hira_info(edi_sub)
            row[COL['EDI_대체']], row[COL['약명_대체']], row[COL['업체_대체']] = edi_sub, dc2.text_input("대체 약명", value=sm.get("name","")), dc3.text_input("대체 업체", value=sm.get("comp",""))
            
            row[COL['요청사유']] = st.text_area("입고요청사유")

        elif i in [3, 4, 5]: # 급여코드/단가변경
            row[COL['변경내용']] = st.text_area("변경 세부 내용")
            row[COL['재고여부']] = st.radio("재고여부", ["유", "무"], horizontal=True, key=f"inv_{i}")
            row[COL['반품량']] = st.text_input("반품 예정량", key=f"ret_{i}")

        # 제출 버튼
        st.divider()
        if st.button(f"🚀 {tab_name} 신청서 제출", use_container_width=True, key=f"btn_{i}"):
            save_data(row)
