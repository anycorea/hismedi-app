import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    .main-title { font-size: 2.2rem !important; font-weight: 800; color: #1E3A8A; margin-bottom: 1.5rem; }
    .stTabs [data-baseweb="tab"] { height: 55px; font-size: 1rem !important; font-weight: bold !important; }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 약가정보 조회 함수 (교정된 파라미터 반영) ---
@st.cache_data(ttl=3600)
def get_hira_drug_info(edi_code):
    if not edi_code or len(edi_code) < 5: return {}
    
    # [중요] 공공데이터포털에서 발급받은 본인의 인증키를 입력하세요. 
    # (st.secrets["hira_api_key"] 로 관리하는 것을 추천합니다)
    try:
        api_key = st.secrets.get("hira_api_key", "YOUR_DECODING_KEY_HERE")
    except:
        api_key = "YOUR_DECODING_KEY_HERE"

    url = "http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList"
    
    # 매개변수 수정: EDI코드는 mdsCd를 사용해야 합니다.
    params = {
        'serviceKey': api_key,
        'mdsCd': edi_code,      # 제품코드(EDI)
        'numOfRows': '1',
        'pageNo': '1'
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            item = root.find(".//item")
            if item is not None:
                # XML 태그명 수정: itmNm(약품명), enpNm(업체명), maxAmt(금액)
                return {
                    "name": item.findtext("itmNm", ""),     
                    "comp": item.findtext("enpNm", ""),    
                    "price": item.findtext("maxAmt", "0"),   
                    "spec": item.findtext("unit", ""),      
                    "date": item.findtext("applcStdt", "")  
                }
    except Exception as e:
        print(f"Error: {e}")
    return {}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_gsheet():
    try:
        creds = Credentials.from_service_account_info(
            st.secrets["gcp_service_account"], 
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except:
        return None

sheet = get_gsheet()

# --- 4. 메인 UI ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)

# 데이터 저장용 리스트 (A~BD열)
if 'data_row' not in st.session_state:
    st.session_state.data_row = [""] * 56

col_left, col_right = st.columns([1, 3.2], gap="large")

with col_left:
    st.subheader("📋 공통 신청 정보")
    app_date = st.date_input("신청일", datetime.now()).strftime('%Y-%m-%d')
    app_user = st.text_input("신청자")
    app_status = st.selectbox("진행상황", ["신청완료", "처리완료"])
    
    st.divider()
    st.subheader("🔍 EDI 빠른 확인")
    test_code = st.text_input("코드를 입력하세요", value="648500030") # 타이레놀 예시
    if test_code:
        res = get_hira_drug_info(test_code)
        if res.get("name"):
            st.success(f"**{res['name']}**")
            st.caption(f"제조사: {res['comp']} | 상한가: {res['price']}원")
        else:
            st.warning("정보를 찾을 수 없습니다. (인증키 확인 필요)")

with col_right:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    def render_form(tid, e_idx, n_idx, c_idx, p_idx):
        c1, c2, c3 = st.columns([1, 2, 1])
        edi = c1.text_input(f"EDI코드", key=f"e_{tid}")
        
        # 데이터 자동 조회
        m = get_hira_drug_info(edi) if edi else {}
        
        # 조회된 데이터가 있으면 자동 입력, 없으면 수동 입력 가능
        name = c2.text_input(f"약품명", value=m.get("name", ""), key=f"n_{tid}")
        comp = c3.text_input(f"업체명", value=m.get("comp", ""), key=f"c_{tid}")
        
        c4, c5 = st.columns(2)
        price = c4.text_input(f"상한금액(원)", value=m.get("price", ""), key=f"p_{tid}")
        status = c5.text_input(f"비고/상태", key=f"s_{tid}")
        
        # 세션 데이터 업데이트
        st.session_state.data_row[e_idx] = edi
        st.session_state.data_row[n_idx] = name
        st.session_state.data_row[c_idx] = comp
        st.session_state.data_row[p_idx] = price

    with tabs[0]: # 사용중지
        st.session_state.data_row[0] = "사용중지"
        render_form("stop", 3, 4, 5, 7)
    
    with tabs[1]: # 신규입고
        st.session_state.data_row[0] = "신규입고"
        render_form("new", 11, 12, 13, 15)

    # ... 다른 탭들도 동일한 방식으로 추가 가능

    st.divider()
    if st.button("🚀 신청서 제출", use_container_width=True):
        if sheet and app_user:
            st.session_state.data_row[1] = app_date
            st.session_state.data_row[2] = app_user
            sheet.append_row(st.session_state.data_row)
            st.success("성공적으로 데이터베이스에 저장되었습니다!")
            st.balloons()
        else:
            st.error("신청자 이름을 입력하거나 구글 시트 설정을 확인하세요.")
