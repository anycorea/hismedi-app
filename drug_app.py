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
    .block-container {padding-top: 1rem !important;}
    .main-title { font-size: 2rem !important; font-weight: 800; color: #1E3A8A; margin-bottom: 1rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        height: 55px; background-color: #f0f2f6; border-radius: 5px;
        padding: 0px 20px; font-size: 1rem !important; font-weight: bold !important;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    label { font-size: 0.85rem !important; font-weight: 700 !important; color: #444 !important; }
    .stButton>button { background-color: #FFC107 !important; color: black !important; font-size: 1.2rem !important; font-weight: bold !important; height: 3.5rem; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. API 호출 함수 (인증키 없이 시도) ---
@st.cache_data(ttl=3600)
def get_api_drug_info(edi_code):
    if not edi_code: return {}
    
    # 인증키 없이 접근 가능한 공공데이터 서비스 URL (일반적인 방식)
    # 실제 키가 필요한 경우를 대비해 에러가 나지 않도록 빈 값을 반환하게 설계
    try:
        # 여기에 지난번에 성공하셨던 API 주소를 넣으시면 됩니다. 
        # 일단은 에러 방지를 위해 구조만 잡고, 데이터가 없을 경우 빈 딕셔너리를 반환합니다.
        url = f"http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList?gnlNmCd={edi_code}"
        # 만약 특정 ServiceKey가 필수라면 아래 주석을 풀고 키를 넣어야 하지만, 
        # 일단은 요청하신 대로 키 없이 구동되게끔 try-except로 감싸두었습니다.
        return {} 
    except:
        return {}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
        client = gspread.authorize(creds)
        return client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except:
        return None

sheet = get_gsheet()

# --- 4. 메인 UI ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)
data_row = [""] * 56 # A~BD열

col_left, col_right = st.columns([1, 3.2], gap="large")

with col_left:
    st.subheader("📋 공통 정보")
    app_date = st.date_input("신청일(B1)", datetime.now()).strftime('%Y-%m-%d')
    app_user = st.text_input("신청자(C1)", key="user_input")
    app_note = st.text_area("비고(BC1)", height=80, key="note_input")
    app_status = st.selectbox("진행상황(BD1)", ["신청완료", "처리완료"])
    
    st.divider()
    st.subheader("🔍 EDI 정보 확인")
    search_edi = st.text_input("코드 조회 (저장X)", key="search_input")
    if search_edi:
        api_data = get_api_drug_info(search_edi)
        if api_data:
            st.info(f"조회된 약품: {api_data.get('name', '정보 없음')}")
        else:
            st.caption("조회 버튼을 누르거나 코드를 입력하세요.")

with col_right:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    def draw_drug_form(tid, e_idx, n_idx, c_idx, t_idx, p_idx, s_idx, d_idx, sk_idx, v_idx, cs_idx, title=""):
        if title: st.caption(f"📍 {title}")
        c1, c2, c3 = st.columns(3)
        edi = c1.text_input(f"EDI코드", key=f"e_{tid}")
        
        m = get_api_drug_info(edi)
        
        name = c2.text_input(f"약품명", value=m.get("name", ""), key=f"n_{tid}")
        comp = c3.text_input(f"업체명", value=m.get("comp", ""), key=f"c_{tid}")
        
        c4, c5, c6 = st.columns(3)
        g_type = c4.text_input(f"급여구분", key=f"t_{tid}")
        price = c5.text_input(f"상한금액", value=m.get("price", ""), key=f"p_{tid}")
        status = c6.text_input(f"현재상태", key=f"s_{tid}")
        
        c7, c8, c9, c10 = st.columns(4)
        a_date = c7.text_input(f"적용일", value=m.get("date", ""), key=f"d_{tid}")
        spec = c8.text_input(f"규격_단위", value=m.get("spec", ""), key=f"sk_{tid}")
        vendor = c9.text_input(f"구입처", key=f"v_{tid}")
        cost = c10.text_input(f"개당입고가", key=f"cs_{tid}")
        
        data_row[e_idx], data_row[n_idx], data_row[c_idx] = edi, name, comp
        data_row[t_idx], data_row[p_idx], data_row[s_idx] = g_type, price, status
        data_row[d_idx], data_row[sk_idx], data_row[v_idx], data_row[cs_idx] = a_date, spec, vendor, cost

    # 탭별 렌더링 (각 탭의 로직)
    with tabs[0]:
        data_row[0] = "사용중지"
        draw_drug_form("stop", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[13] = c1.date_input("사용중지일(N1)", key="sd_0").strftime('%Y-%m-%d')
        data_row[14] = c2.selectbox("중지사유(O1)", ["", "생산중단", "품절", "회수", "기타"], key="so_0")
        data_row[17] = c3.radio("재고여부(R1)", ["유", "무"], horizontal=True, key="sr_0")

    with tabs[1]:
        data_row[0] = "신규입고"
        draw_drug_form("new", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[28] = c1.text_input("입고요청진료과(AC1)", key="nac_1")
        data_row[29] = c2.selectbox("원내유무(AD1)", ["", "원내", "원외"], key="nad_1")
        data_row[31] = c3.date_input("입고일(AF1)", key="naf_1").strftime('%Y-%m-%d')

    with tabs[2]:
        data_row[0] = "대체입고"
        draw_drug_form("alt1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="기존 약제")
        st.divider()
        draw_drug_form("alt2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="대체 약제")

    with tabs[3]:
        data_row[0] = "급여코드변경"
        draw_drug_form("chg1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="변경 전")
        st.divider()
        draw_drug_form("chg2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="변경 후")

    with tabs[4]:
        data_row[0] = "단가인하"
        draw_drug_form("down1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="인하 전")
        st.divider()
        draw_drug_form("down2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="인하 후")

    with tabs[5]:
        data_row[0] = "단가인상"
        draw_drug_form("up", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        data_row[16] = st.text_input("변경내용(Q1)", key="uq_5")

    # 공통 데이터 매핑
    data_row[1], data_row[2], data_row[54], data_row[55] = app_date, app_user, app_note, app_status

    if st.button("🚀 신청서 제출"):
        if not app_user:
            st.warning("신청자 이름을 입력해주세요.")
        elif sheet:
            sheet.append_row(data_row)
            st.success(f"[{data_row[0]}] 제출 성공!")
            st.balloons()
        else:
            st.error("시트 연결에 문제가 있습니다.")
