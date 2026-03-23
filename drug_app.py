import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 제목 잘림 방지 CSS ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
    <style>
    /* 상단 여백 최소화 및 제목 영역 확보 */
    .block-container {padding-top: 1rem !important; padding-left: 2rem !important; padding-right: 2rem !important;}
    
    /* 제목이 잘리지 않도록 폰트 크기 조절 및 줄바꿈 방지 */
    .main-title {
        font-size: 2.2rem !important;
        font-weight: 800;
        color: #1E3A8A;
        white-space: nowrap !important;
        overflow: visible !important;
        margin-bottom: 1.5rem;
    }
    
    /* 탭 버튼 스타일 (박스형) */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 60px; background-color: #f0f2f6; border-radius: 8px;
        padding: 0px 20px; font-size: 1.1rem !important; font-weight: bold !important;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    
    /* 입력 라벨 스타일 */
    label { font-size: 0.9rem !important; font-weight: 700 !important; color: #333 !important; }
    
    /* 제출 버튼 */
    .stButton>button {
        background-color: #FFC107 !important; color: black !important;
        font-size: 1.2rem !important; font-weight: bold !important;
        height: 4rem; width: 100%; border: none; margin-top: 2rem;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. API 데이터 호출 (인증키 미사용 구조) ---
@st.cache_data(ttl=3600)
def get_api_data(edi_code):
    if not edi_code or len(edi_code) < 5: return {}
    
    # 심평원 약제 정보 조회 오픈 URL (인증키 없이 시도 가능한 구조)
    url = f"http://apis.data.go.kr/B551182/dgamtInfoService/getMdclSvcGrdeList"
    params = {'gnlNmCd': edi_code} # EDI 코드를 매개변수로 전달
    
    try:
        # 인증키 없이 호출하거나, 아주 기본적인 키로 호출 시도
        response = requests.get(url, params=params, timeout=3)
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            item = root.find(".//item")
            if item is not None:
                return {
                    "name": item.findtext("itmNm", ""), # 약품명
                    "comp": item.findtext("entpNm", ""), # 업체명
                    "price": item.findtext("mxamt", "0"), # 상한금액
                    "date": item.findtext("applcStdt", ""), # 적용일
                    "spec": item.findtext("unit", ""), # 규격
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

# --- 4. 메인 UI 및 로직 ---
st.markdown('<div class="main-title">💊 HISMEDI Drug Service Subscription</div>', unsafe_allow_html=True)

data_row = [""] * 56 # A~BD열 초기화

col_left, col_right = st.columns([1, 3.2], gap="large")

with col_left:
    st.subheader("📌 공통 신청 정보")
    app_date = st.date_input("신청일(B1)", datetime.now()).strftime('%Y-%m-%d')
    app_user = st.text_input("신청자(C1)", placeholder="성함 입력")
    app_note = st.text_area("비고(BC1)", placeholder="기타 요청사항", height=100)
    app_status = st.selectbox("진행상황(BD1)", ["신청완료", "처리완료"])
    
    st.divider()
    st.subheader("🔍 EDI 빠른 조회")
    search_input = st.text_input("코드를 넣고 엔터를 치세요 (조회용)")
    if search_input:
        res = get_api_data(search_input)
        if res:
            st.success(f"**{res['name']}**")
            st.caption(f"업체: {res['comp']} | 가격: {res['price']}원")
        else:
            st.warning("조회된 정보가 없습니다.")

with col_right:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])

    # 입력 폼 생성 함수
    def draw_form(tab_key, e_idx, n_idx, c_idx, t_idx, p_idx, s_idx, d_idx, sk_idx, v_idx, cs_idx, title=""):
        if title: st.info(f"📍 {title}")
        c1, c2, c3 = st.columns(3)
        edi_val = c1.text_input(f"EDI코드", key=f"e_{tab_key}")
        
        # 입력 시 API에서 데이터 가져오기
        api_res = get_api_data(edi_val) if edi_val else {}
        
        name_val = c2.text_input(f"약품명", value=api_res.get("name", ""), key=f"n_{tab_key}")
        comp_val = c3.text_input(f"업체명", value=api_res.get("comp", ""), key=f"c_{tab_key}")
        
        c4, c5, c6 = st.columns(3)
        data_row[t_idx] = c4.text_input(f"급여구분", key=f"t_{tab_key}")
        data_row[p_idx] = c5.text_input(f"상한금액", value=api_res.get("price", ""), key=f"p_{tab_key}")
        data_row[s_idx] = c6.text_input(f"현재상태", key=f"s_{tab_key}")
        
        c7, c8, c9, c10 = st.columns(4)
        data_row[d_idx] = c7.text_input(f"적용일", value=api_res.get("date", ""), key=f"d_{tab_key}")
        data_row[sk_idx] = c8.text_input(f"규격_단위", value=api_res.get("spec", ""), key=f"sk_{tab_key}")
        data_row[v_idx] = c9.text_input(f"구입처", key=f"v_{tab_key}")
        data_row[cs_idx] = c10.text_input(f"개당입고가", key=f"cs_{tab_key}")
        
        data_row[e_idx], data_row[n_idx], data_row[c_idx] = edi_val, name_val, comp_val

    # 1. 사용중지
    with tabs[0]:
        data_row[0] = "사용중지"
        draw_form("stop", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[13] = c1.date_input("사용중지일(N1)", key="sd0").strftime('%Y-%m-%d')
        data_row[14] = c2.selectbox("중지사유(O1)", ["", "생산중단", "품절", "회수", "기타"], key="so0")
        data_row[17] = c3.radio("재고여부(R1)", ["유", "무"], horizontal=True, key="sr0")

    # 2. 신규입고
    with tabs[1]:
        data_row[0] = "신규입고"
        draw_form("new", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2 = st.columns(2)
        data_row[28] = c1.text_input("입고요청진료과(AC1)", key="nac1")
        data_row[31] = c2.date_input("입고일(AF1)", key="naf1").strftime('%Y-%m-%d')

    # 3. 대체입고
    with tabs[2]:
        data_row[0] = "대체입고"
        draw_form("alt_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="기존 약제")
        st.divider()
        draw_form("alt_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="대체 약제")

    # 4. 급여코드변경
    with tabs[3]:
        data_row[0] = "급여코드변경"
        draw_form("chg_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="변경 전")
        st.divider()
        draw_form("chg_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="변경 후")

    # 5. 단가인하
    with tabs[4]:
        data_row[0] = "단가인하"
        draw_form("down_old", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, title="인하 전")
        st.divider()
        draw_form("down_new", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, title="인하 후")

    # 6. 단가인상
    with tabs[5]:
        data_row[0] = "단가인상"
        draw_form("up", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        data_row[16] = st.text_input("변경내용(Q1)", key="uq_up")

    # 데이터 저장 실행
    data_row[1], data_row[2], data_row[54], data_row[55] = app_date, app_user, app_note, app_status

    if st.button("🚀 신청서 제출"):
        if not app_user:
            st.warning("신청자 성함을 입력해주세요.")
        elif sheet:
            sheet.append_row(data_row)
            st.success(f"✅ {data_row[0]} 신청 완료!")
            st.balloons()
        else:
            st.error("시트 연결을 확인해주세요.")
