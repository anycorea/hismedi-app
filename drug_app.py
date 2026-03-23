import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 제목 잘림 방지 스타일 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    /* 제목 영역 강제 고정 및 잘림 방지 */
    .main-title {
        font-size: 2.2rem !important;
        font-weight: 800;
        color: #1E3A8A;
        white-space: nowrap !important; /* 한 줄 유지 */
        overflow: visible !important;
        margin-bottom: 1.5rem;
        display: block;
    }
    .stTabs [data-baseweb="tab"] {
        height: 55px; font-size: 1rem !important; font-weight: bold !important;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 약가정보 조회 함수 (요양기관 포털 방식 시뮬레이션) ---
@st.cache_data(ttl=3600)
def get_hira_drug_info(edi_code):
    if not edi_code or len(edi_code) < 5: return {}
    
    # 심평원 공개 데이터 서버 (인증키 미요구 구조로 호출)
    url = "http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList"
    params = {'gnlNmCd': edi_code} # 제품코드(EDI) 매개변수
    
    try:
        # 요양기관 포털처럼 인증서 없이 호출 시도
        response = requests.get(url, params=params, timeout=3)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            item = root.find(".//item")
            if item is not None:
                return {
                    "name": item.findtext("itmNm", ""),     # 약품명
                    "comp": item.findtext("entpNm", ""),    # 업체명
                    "price": item.findtext("mxamt", "0"),   # 상한금액
                    "spec": item.findtext("unit", ""),      # 규격/단위
                    "date": item.findtext("applcStdt", "")  # 적용일자
                }
    except:
        pass
    return {}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_gsheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except:
        return None

sheet = get_gsheet()

# --- 4. 메인 UI ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)

data_row = [""] * 56 # A~BD열

col_left, col_right = st.columns([1, 3.2], gap="large")

with col_left:
    st.subheader("📋 공통 신청 정보")
    app_date = st.date_input("신청일", datetime.now()).strftime('%Y-%m-%d')
    app_user = st.text_input("신청자(C1)")
    app_status = st.selectbox("진행상황", ["신청완료", "처리완료"])
    
    st.divider()
    st.subheader("🔍 EDI 빠른 확인")
    test_code = st.text_input("코드를 입력하세요", value="628900110") # 암브록솔 예시
    if test_code:
        res = get_hira_drug_info(test_code)
        if res:
            st.success(f"**{res['name']}**")
            st.caption(f"업체: {res['comp']} | 가격: {res['price']}원")
        else:
            st.warning("정보를 불러올 수 없습니다.")

with col_right:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    def render_form(tid, e_idx, n_idx, c_idx, t_idx, p_idx, s_idx, d_idx, sk_idx):
        c1, c2, c3 = st.columns(3)
        edi = c1.text_input(f"EDI코드", key=f"e_{tid}")
        
        # 입력 시 실시간 데이터 매핑
        m = get_hira_drug_info(edi) if edi else {}
        
        name = c2.text_input(f"약품명", value=m.get("name", ""), key=f"n_{tid}")
        comp = c3.text_input(f"업체명", value=m.get("comp", ""), key=f"c_{tid}")
        
        c4, c5, c6 = st.columns(3)
        data_row[t_idx] = c4.text_input(f"급여구분", key=f"t_{tid}")
        data_row[p_idx] = c5.text_input(f"상한금액", value=m.get("price", ""), key=f"p_{tid}")
        data_row[s_idx] = c6.text_input(f"현재상태", key=f"s_{tid}")
        
        data_row[e_idx], data_row[n_idx], data_row[c_idx] = edi, name, comp

    with tabs[0]: # 사용중지 예시
        data_row[0] = "사용중지"
        render_form("stop", 3, 4, 5, 6, 7, 8, 9, 10)
        # 중지 사유 등 추가...

    if st.button("🚀 신청서 제출"):
        if sheet and app_user:
            data_row[1], data_row[2] = app_date, app_user
            sheet.append_row(data_row)
            st.success("데이터베이스에 저장되었습니다!")
            st.balloons()
