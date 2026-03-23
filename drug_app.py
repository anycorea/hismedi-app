import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 (제목 잘림 해결) ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    /* 상단 여백 확보 및 전체 배경색 */
    .block-container { padding-top: 1.5rem !important; background-color: #f8f9fa; }
    
    /* 제목이 절대 잘리지 않도록 설정 */
    .main-title {
        font-size: 2.2rem !important;
        font-weight: 800;
        color: #1E3A8A;
        white-space: nowrap !important;
        overflow: visible !important;
        display: block !important;
        margin-bottom: 1.5rem;
    }
    
    /* 탭 디자인 (박스형 버튼 스타일) */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 60px; background-color: #e9ecef; border-radius: 8px 8px 0px 0px;
        padding: 0px 25px; font-size: 1.1rem !important; font-weight: bold !important;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }

    /* 입력창 라벨 */
    label { font-size: 0.9rem !important; font-weight: 700 !important; color: #333 !important; }
    
    /* 제출 버튼 스타일 */
    .stButton>button {
        background-color: #FFC107 !important; color: black !important;
        font-size: 1.2rem !important; font-weight: bold !important;
        height: 4rem; border-radius: 8px; border: none; margin-top: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 인증키 없는 약제 정보 조회 함수 ---
@st.cache_data(ttl=3600)
def fetch_drug_data(edi_code):
    if not edi_code or len(edi_code) < 5: return {}
    
    # 요양기관업무포털/심평원 공개 API 경로 (인증키 미요구 파라미터 구조)
    # 실제 지난번에 성공했던 그 공개 URL 구조를 적용합니다.
    url = f"http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList"
    # 인증키 없이도 응답을 주는 특정 모드로 호출 시도
    params = {'gnlNmCd': edi_code} 
    
    try:
        # timeout을 짧게 설정해 반응이 없으면 바로 넘어가도록 함
        response = requests.get(url, params=params, timeout=2)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            item = root.find(".//item")
            if item is not None:
                return {
                    "name": item.findtext("itmNm", ""),
                    "comp": item.findtext("entpNm", ""),
                    "price": item.findtext("mxamt", "0"),
                    "date": item.findtext("applcStdt", ""),
                    "spec": item.findtext("unit", "")
                }
    except:
        pass
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

# --- 4. 메인 UI 구성 ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)

data_row = [""] * 56 # A~BD열

col_info, col_form = st.columns([1, 3], gap="large")

with col_info:
    st.subheader("📋 기본 정보")
    with st.container(border=True):
        app_date = st.date_input("신청일(B1)", datetime.now()).strftime('%Y-%m-%d')
        app_user = st.text_input("신청자(C1)", placeholder="성함 입력")
        app_note = st.text_area("비고(BC1)", height=100)
        app_status = st.selectbox("진행상황(BD1)", ["신청완료", "처리완료"])

    st.divider()
    st.subheader("🔍 EDI 빠른 확인")
    check_edi = st.text_input("코드 입력 (예: 648500030)")
    if check_edi:
        res = fetch_drug_data(check_edi)
        if res:
            st.success(f"**품명:** {res['name']}\n\n**업체:** {res['comp']}")
        else:
            st.caption("데이터가 없거나 조회 중입니다.")

with col_form:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    # 입력창 통합 함수
    def build_inputs(tid, e_idx, n_idx, c_idx, t_idx, p_idx, s_idx, d_idx, sk_idx, v_idx, cs_idx, title=""):
        if title: st.markdown(f"**📍 {title}**")
        c1, c2, c3 = st.columns(3)
        edi = c1.text_input(f"EDI코드", key=f"edi_{tid}")
        
        # EDI 코드 입력 시 자동 조회
        api_info = fetch_drug_data(edi) if edi else {}
        
        name = c2.text_input(f"약품명", value=api_info.get("name", ""), key=f"name_{tid}")
        comp = c3.text_input(f"업체명", value=api_info.get("comp", ""), key=f"comp_{tid}")
        
        c4, c5, c6 = st.columns(3)
        data_row[t_idx] = c4.text_input(f"급여구분", key=f"type_{tid}")
        data_row[p_idx] = c5.text_input(f"상한금액", value=api_info.get("price", ""), key=f"price_{tid}")
        data_row[s_idx] = c6.text_input(f"현재상태", key=f"status_{tid}")
        
        c7, c8, c9, c10 = st.columns(4)
        data_row[d_idx] = c7.text_input(f"적용일", value=api_info.get("date", ""), key=f"date_{tid}")
        data_row[sk_idx] = c8.text_input(f"규격_단위", value=api_info.get("spec", ""), key=f"spec_{tid}")
        data_row[v_idx] = c9.text_input(f"구입처", key=f"vend_{tid}")
        data_row[cs_idx] = c10.text_input(f"개당입고가", key=f"cost_{tid}")
        
        data_row[e_idx], data_row[n_idx], data_row[c_idx] = edi, name, comp

    # 각 탭별 렌더링
    with tabs[0]: # 사용중지
        data_row[0] = "사용중지"
        build_inputs("t0", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[13] = c1.date_input("중지일(N1)", key="d0").strftime('%Y-%m-%d')
        data_row[14] = c2.selectbox("중지사유(O1)", ["", "생산중단", "품절", "회수", "기타"], key="s0")
        data_row[17] = c3.radio("재고여부(R1)", ["유", "무"], horizontal=True, key="r0")

    with tabs[1]: # 신규입고
        data_row[0] = "신규입고"
        build_inputs("t1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2 = st.columns(2)
        data_row[28] = c1.text_input("진료과(AC1)", key="ac1")
        data_row[31] = c2.date_input("입고일(AF1)", key="af1").strftime('%Y-%m-%d')

    with tabs[2]: # 대체입고
        data_row[0] = "대체입고"
        build_inputs("t2_1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, "기존 약제")
        st.divider()
        build_inputs("t2_2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, "대체 약제")

    with tabs[3]: # 급여코드변경
        data_row[0] = "급여코드변경"
        build_inputs("t3_1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, "변경 전")
        st.divider()
        build_inputs("t3_2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, "변경 후")

    with tabs[4]: # 단가인하
        data_row[0] = "단가인하"
        build_inputs("t4_1", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, "인하 전")
        st.divider()
        build_inputs("t4_2", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, "인하 후")

    with tabs[5]: # 단가인상
        data_row[0] = "단가인상"
        build_inputs("t5", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        data_row[16] = st.text_input("변경내용(Q1)", key="q5")

    # 공통 데이터 세팅
    data_row[1], data_row[2], data_row[54], data_row[55] = app_date, app_user, app_note, app_status

    if st.button("🚀 신청서 제출"):
        if not app_user:
            st.warning("신청자 성함을 입력해주세요.")
        elif sheet:
            sheet.append_row(data_row)
            st.success(f"[{data_row[0]}] 신청이 완료되었습니다!")
            st.balloons()
