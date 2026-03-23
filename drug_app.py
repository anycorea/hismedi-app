import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 (PC 전용 와이드 모드) ---
st.set_page_config(page_title="HISMEDI 약무 서비스 신청서", layout="wide", initial_sidebar_state="collapsed")

# 디자인 커스텀 CSS
st.markdown("""
    <style>
    .block-container {padding-top: 0.5rem; padding-bottom: 0.5rem;}
    h1 {font-size: 1.8rem !important; margin-bottom: 1rem !important;}
    .stButton>button {width: 100%; height: 3.5rem; background-color: #FFC107; color: black; font-weight: bold; font-size: 1.1rem; border: none; margin-top: 10px;}
    .stButton>button:hover {background-color: #FFA000; color: white;}
    .tab-content { border: 1px solid #dee2e6; padding: 15px; border-radius: 8px; background-color: #fdfdfd; min-height: 450px; }
    label { font-weight: bold !important; color: #333 !important; font-size: 0.85rem !important; }
    input { color: black !important; font-weight: 500 !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 시트 연결 ---
def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
    return gspread.authorize(creds)

try:
    client = get_gspread_client()
    doc = client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    sheet = doc.worksheet(st.secrets["gsheet"]["worksheet_name"])
except Exception as e:
    st.error(f"⚠️ 시트 연결 실패: {e}")
    st.stop()

# --- 3. EDI 마스터 데이터 (자동 완성용) ---
# 실제 운영 시에는 별도의 'Master' 시트를 읽어오도록 구현하면 편리합니다.
MASTER_DATA = {
    "644504100": {"name": "타이레놀정500mg", "comp": "(주)한국존슨앤드존슨", "type": "급여", "price": "51", "status": "정상", "date": "2024-01-01", "spec": "500mg/1T", "vendor": "직거래", "cost": "45"},
    "642102570": {"name": "아모디핀정", "comp": "한미약품(주)", "type": "급여", "price": "364", "status": "정상", "date": "2023-10-01", "spec": "5mg/1T", "vendor": "제이에스팜", "cost": "320"}
}

# --- 4. 메인 UI ---
st.title("💊 HISMEDI Drug Service Subscription")

# 왼쪽(기본 정보/조회) & 오른쪽(신청 항목) 분할
col_base, col_main = st.columns([1, 3.5], gap="medium")

with col_base:
    st.subheader("📌 공통 신청 정보")
    with st.container(border=True):
        app_date = st.date_input("신청일(B1)", datetime.now()).strftime('%Y-%m-%d')
        app_user = st.text_input("신청자(C1)", placeholder="성함 입력")
        app_note = st.text_area("비고(BC1)", placeholder="기타 요청사항", height=80)
        app_process = st.selectbox("진행상황(BD1)", ["신청완료", "처리완료"])

    st.subheader("🔍 EDI 빠른 조회")
    search_code = st.text_input("코드를 입력하고 엔터를 치세요")
    if search_code in MASTER_DATA:
        d = MASTER_DATA[search_code]
        st.success(f"**{d['name']}** 확인됨")
        st.caption(f"업체: {d['comp']} | 상한가: {d['price']}원")
    elif search_code:
        st.error("데이터 없음")

with col_main:
    tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"])
    
    # 데이터 저장을 위한 공통 리스트 (A~BD까지 총 56열 공백 초기화)
    data_row = [""] * 56
    data_row[1] = app_date    # B1
    data_row[2] = app_user    # C1
    data_row[54] = app_note   # BC1
    data_row[55] = app_process # BD1

    # 공통 입력 헬퍼 함수
    def drug_input_ui(key_prefix, edi_idx, name_idx, comp_idx, type_idx, price_idx, status_idx, date_idx, spec_idx, vendor_idx, cost_idx):
        c1, c2, c3 = st.columns(3)
        edi = c1.text_input(f"EDI코드({key_prefix})(D1/AK1)", key=f"edi_{key_prefix}")
        m = MASTER_DATA.get(edi, {})
        name = c2.text_input(f"약품명({key_prefix})", value=m.get("name", ""), key=f"name_{key_prefix}")
        comp = c3.text_input(f"업체명({key_prefix})", value=m.get("comp", ""), key=f"comp_{key_prefix}")
        
        c4, c5, c6 = st.columns(3)
        g_type = c4.text_input(f"급여구분({key_prefix})", value=m.get("type", ""), key=f"type_{key_prefix}")
        price = c5.text_input(f"상한금액({key_prefix})", value=m.get("price", ""), key=f"price_{key_prefix}")
        status = c6.text_input(f"현재상태({key_prefix})", value=m.get("status", ""), key=f"status_{key_prefix}")
        
        c7, c8, c9, c10 = st.columns(4)
        apply_date = c7.text_input(f"적용일({key_prefix})", value=m.get("date", ""), key=f"date_{key_prefix}")
        spec = c8.text_input(f"규격_단위({key_prefix})", value=m.get("spec", ""), key=f"spec_{key_prefix}")
        vendor = c9.text_input(f"구입처({key_prefix})", value=m.get("vendor", ""), key=f"vendor_{key_prefix}")
        cost = c10.text_input(f"개당입고가({key_prefix})", value=m.get("cost", ""), key=f"cost_{key_prefix}")
        
        # 데이터 매핑
        data_row[edi_idx], data_row[name_idx], data_row[comp_idx] = edi, name, comp
        data_row[type_idx], data_row[price_idx], data_row[status_idx] = g_type, price, status
        data_row[date_idx], data_row[spec_idx], data_row[vendor_idx], data_row[cost_idx] = apply_date, spec, vendor, cost

    # --- 1) 사용중지 ---
    with tabs[0]:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        data_row[0] = "사용중지"
        drug_input_ui("신규·기존", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[13] = c1.date_input("사용중지일(N1)").strftime('%Y-%m-%d')
        data_row[14] = c2.selectbox("중지사유(O1)", ["", "생산중단", "품절", "회수", "기타"], key="s1")
        data_row[15] = c3.text_input("기타사유(P1)")
        c4, c5, c6 = st.columns(3)
        data_row[17] = c4.radio("재고여부(R1)", ["유", "무"], horizontal=True, key="r1")
        data_row[18] = c5.text_input("재고처리방법(S1)")
        data_row[19] = c6.text_input("재고량(T1)")
        st.markdown('</div>', unsafe_allow_html=True)

    # --- 2) 신규입고 ---
    with tabs[1]:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        data_row[0] = "신규입고"
        drug_input_ui("신규", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        c1, c2, c3 = st.columns(3)
        data_row[28] = c1.text_input("입고요청진료과(AC1)")
        data_row[29] = c2.selectbox("원내유무(AD1)", ["", "원내", "원외", "병용"], key="s2")
        data_row[30] = c3.text_input("사용기간(AE1)")
        c4, c5, c6 = st.columns(3)
        data_row[31] = c4.date_input("입고일(AF1)").strftime('%Y-%m-%d')
        data_row[32] = c5.date_input("코드사용시작일(AG1)").strftime('%Y-%m-%d')
        data_row[33] = c6.text_input("상한가외입고사유(AH1)")
        st.markdown('</div>', unsafe_allow_html=True)

    # --- 3) 대체입고 ---
    with tabs[2]:
        st.markdown('<div class="tab-content">', unsafe_allow_html=True)
        data_row[0] = "대체입고"
        st.info("🔄 기존 약제 정보 (신규·기존·반품)")
        drug_input_ui("기존", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        st.info("🆕 대체/변경될 약제 정보")
        drug_input_ui("대체", 36, 37, 38, 39, 40, 41, 42, 43, 44, 45)
        st.markdown('</div>', unsafe_allow_html=True)

    # --- 4~6) 나머지 항목 (구조 동일, 컬럼 인덱스만 매칭) ---
    for i, title in enumerate(["④ 급여코드변경", "⑤ 단가인하▼", "⑥ 단가인상▲"], 3):
        with tabs[i]:
            st.markdown('<div class="tab-content">', unsafe_allow_html=True)
            data_row[0] = title
            drug_input_ui("정보", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
            data_row[16] = st.text_input(f"변경내용(Q1) - {title}", key=f"change_{i}")
            st.markdown('</div>', unsafe_allow_html=True)

# --- 5. 제출 및 저장 ---
if st.button("🚀 신청서 제출 (Submit Application)"):
    if not app_user:
        st.error("신청자 이름을 입력해주세요.")
    else:
        try:
            # 시트에 데이터 추가 (A열~BD열 한 번에 저장)
            sheet.append_row(data_row)
            st.balloons()
            st.success(f"✅ {data_row[0]} 신청이 완료되었습니다! (시트에 정상 기록됨)")
        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")
