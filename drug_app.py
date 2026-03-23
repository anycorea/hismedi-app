import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide", initial_sidebar_state="collapsed")

# 디자인 개선 CSS (제목, 탭, 여백)
st.markdown("""
    <style>
    /* 상단 여백 제거 */
    .block-container {padding-top: 1rem !important; padding-bottom: 0rem !important;}
    
    /* 제목 스타일 */
    .main-title { font-size: 2.2rem !important; font-weight: 800; color: #1E3A8A; margin-bottom: 1.5rem; white-space: nowrap; }
    
    /* 박스형 탭 스타일 커스텀 */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 60px;
        background-color: #f0f2f6;
        border-radius: 8px 8px 0px 0px;
        padding: 0px 20px;
        font-size: 1.1rem !important;
        font-weight: bold !important;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }

    /* 입력창 라벨 및 텍스트 */
    label { font-size: 0.9rem !important; font-weight: 700 !important; color: #444 !important; }
    input { font-size: 1rem !important; }
    
    /* 하단 제출 버튼 */
    .stButton>button {
        background-color: #FFC107 !important;
        color: black !important;
        font-size: 1.2rem !important;
        font-weight: bold !important;
        height: 4rem;
        border: none;
        margin-top: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 시트 연결 ---
@st.cache_resource
def get_gsheet():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    client = gspread.authorize(creds)
    return client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])

try:
    sheet = get_gsheet()
except Exception as e:
    st.error(f"시트 연결 실패: {e}")
    st.stop()

# --- 3. EDI 마스터 데이터 ---
MASTER_DATA = {
    "644504100": {"name": "타이레놀정500mg", "comp": "(주)한국존슨앤드존슨", "type": "급여", "price": "51", "status": "정상", "date": "2024-01-01", "spec": "500mg/1T", "vendor": "직거래", "cost": "45"},
    "642102570": {"name": "아모디핀정", "comp": "한미약품(주)", "type": "급여", "price": "364", "status": "정상", "date": "2023-10-01", "spec": "5mg/1T", "vendor": "제이에스팜", "cost": "320"}
}

# --- 4. 메인 UI 구성 ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)

# 데이터 저장용 리스트 초기화 (A~BD: 56열)
data_row = [""] * 56

col_left, col_right = st.columns([1, 3.2], gap="large")

with col_left:
    st.subheader("📋 공통 정보")
    app_date = st.date_input("신청일(B1)", datetime.now()).strftime('%Y-%m-%d')
    app_user = st.text_input("신청자(C1)", placeholder="성함 입력")
    app_note = st.text_area("비고(BC1)", placeholder="기타 요청사항", height=100)
    app_status = st.selectbox("진행상황(BD1)", ["신청완료", "처리완료"])
    
    st.divider()
    st.subheader("🔍 EDI 빠른 조회")
    search_code = st.text_input("코드 입력 후 엔터")
    if search_code in MASTER_DATA:
        d = MASTER_DATA[search_code]
        st.info(f"**약품명:** {d['name']}\n\n**업체:** {d['comp']}")
    
    data_row[1], data_row[2], data_row[54], data_row[55] = app_date, app_user, app_note, app_status

with col_right:
    # 탭 생성 (글자 크기 및 박스형 디자인은 CSS에서 처리)
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    # 입력 UI 헬퍼 함수 (Key 중복 방지를 위해 tab_id 추가)
    def draw_drug_inputs(tab_id, edi_idx, name_idx, comp_idx, type_idx, price_idx, status_idx, date_idx, spec_idx, vendor_idx, cost_idx, title=""):
        if title: st.caption(f"📍 {title}")
        c1, c2, c3 = st.columns(3)
        edi = c1.text_input(f"EDI코드(D1/AK1)", key=f"edi_{tab_id}")
        m = MASTER_DATA.get(edi, {})
        name = c2.text_input(f"약품명", value=m.get("name", ""), key=f"name_{tab_id}")
        comp = c3.text_input(f"업체명", value=m.get("comp", ""), key=f"comp_{tab_id}")
        
        c4, c5, c6 = st.columns(3)
        g_type = c4.text_input(f"급여구분", value=m.get("type", ""), key=f"type_{tab_id}")
        price = c5.text_input(f"상한금액", value=m.get("price", ""), key=f"price_{tab_id}")
        status = c6.text_input(f"현재상태", value=m.get("status", ""), key=f"status_{tab_id}")
        
        c7, c8, c9, c10 = st.columns(4)
        a_date = c7.text_input(f"적용일", value=m.get("date", ""), key=f"date_{tab_id}")
        spec = c8.text_input(f"규격_단위", value=m.get("spec", ""), key=f"spec_{tab_id}")
        vendor = c9.text_input(f"구입처", value=m.get("vendor", ""), key=f"vendor_{tab_id}")
        cost = c10.text_input(f"개당입고가", value=m.get("cost", ""), key=f"cost_{tab_id}")
        
        data_row[edi_idx], data_row[name_idx], data_row[comp_idx] = edi, name, comp
        data_row[type_idx], data_row[price_idx], data_row[status_idx] = g_type, price, status
        data_row[date_idx], data_row[spec_idx], data_row[vendor_idx], data_row[cost_idx] = a_date, spec, vendor, cost

    # 1. 사용중지
    with tabs[0]:
        data_row[0] = "사용중지"
        draw_drug_inputs("stop", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[13] = c1.date_input("사용중지일(N1)", key="d1").strftime('%Y-%m-%d')
        data_row[14] = c2.selectbox("중지사유(O1)", ["", "생산중단", "품절", "회수", "기타"], key="s1")
        data_row[15] = c3.text_input("기타사유(P1)", key="p1")

    # 2. 신규입고
    with tabs[1]:
        data_row[0] = "신규입고"
        draw_drug_inputs("new", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[28] = c1.text_input("입고요청진료과(AC1)", key="ac1")
        data_row[29] = c2.selectbox("원내유무(AD1)", ["", "원내", "원외"], key="ad1")
        data_row[31] = c3.date_input("입고일(AF1)", key="af1").strftime('%Y-%m-%d')

    # 3. 대체입고
    with tabs[2]:
        data_row[0] = "대체입고"
        draw_drug_inputs("alt_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="기본 약제 정보")
        st.divider()
        draw_drug_inputs("alt_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="대체 약제 정보")

    # 4. 급여코드변경
    with tabs[3]:
        data_row[0] = "급여코드변경"
        draw_drug_inputs("chg_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="변경 전")
        st.divider()
        draw_drug_inputs("chg_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="변경 후")

    # 5. 단가인하
    with tabs[4]:
        data_row[0] = "단가인하"
        draw_drug_inputs("down_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="인하 전")
        st.divider()
        draw_drug_inputs("down_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="인하 후")

    # 6. 단가인상
    with tabs[5]:
        data_row[0] = "단가인상"
        draw_drug_inputs("up", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        data_row[16] = st.text_input("변경내용(Q1)", key="q1_up")

    # 제출 버튼
    if st.button("🚀 신청서 제출"):
        if not app_user:
            st.warning("신청자 이름을 입력해주세요.")
        else:
            sheet.append_row(data_row)
            st.success(f"[{data_row[0]}] 신청이 성공적으로 제출되었습니다!")
            st.balloons()
