import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 프리미엄 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 상단 여백 확보 및 배경 설정 */
    .block-container { padding-top: 5rem !important; background-color: #F8FAFC; }
    
    /* 사이드바 제목 및 간격 조정 */
    .sidebar-title { font-size: 1.6rem; font-weight: 800; color: #1E3A8A; margin-bottom: 25px; line-height: 1.2; }
    [data-testid="stSidebar"] { border-right: 1px solid #E2E8F0; }

    /* 메인 타이틀 */
    .main-header { font-size: 2rem; font-weight: 800; color: #1E3A8A; margin-bottom: 10px; }

    /* 탭 디자인 개편 (고급스러운 세그먼트 스타일) */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 12px; margin-bottom: 30px; 
        background-color: #EDF2F7; padding: 10px; border-radius: 15px;
    }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; min-width: 140px; 
        background-color: transparent; border: none !important;
        border-radius: 10px !important;
        font-weight: 700; color: #4A5568; transition: all 0.3s;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #FFFFFF !important; color: #1E3A8A !important; 
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    }
    .stTabs [data-baseweb="tab-border"] { display: none; }

    /* 자동 가져오기 필드 (ReadOnly) 스타일 */
    input:disabled {
        -webkit-text-fill-color: #000000 !important;
        background-color: #EBF5FF !important;
        border: 1px solid #BFDBFE !important;
        font-weight: 600 !important;
    }

    /* 섹션 제목 */
    .section-label { 
        font-size: 1.1rem; font-weight: 700; color: #1E3A8A; 
        margin: 30px 0 15px 0; border-left: 6px solid #1E3A8A; padding-left: 12px;
    }

    /* 제출 버튼 강조 */
    div.stButton > button {
        background-color: #1E3A8A; color: white; border-radius: 10px; height: 50px; font-weight: 700;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
@st.cache_resource
def get_spreadsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=3600)
def load_master_data():
    try:
        ss = get_spreadsheet()
        master_ws = ss.worksheet("Master")
        data = master_ws.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        df.columns = [c.strip() for c in df.columns]
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    if not target.empty:
        res = target.iloc[0].to_dict()
        res['상한금액'] = str(res.get('상한금액', '0')).replace(',', '').strip()
        return res
    return {}

# --- 3. 드롭다운 항목 정의 ---
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_STOP_CRITERIA = ["즉시", "재고소진후"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_IN_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI †<br>Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("👤 신청자 성명", placeholder="입력하세요", key="sb_user")
    app_date = st.date_input("📅 신청 일자", datetime.now()).strftime('%Y-%m-%d')
    comp_user = st.text_input("✅ 완료자 성명", placeholder="처리자 입력", key="sb_comp")
    app_status = st.selectbox("⚙️ 진행 상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 공통 비고", placeholder="비고 입력", height=100)

# --- 5. 공통 UI 헬퍼 ---
def render_drug_info(edi_val, key_id):
    m = get_drug_info(edi_val, master_df)
    c1, c2, c3, c4 = st.columns([1.5, 3, 1.5, 1])
    name = c2.text_input("제품명", value=m.get("제품명", ""), key=f"nm_{key_id}", disabled=True)
    comp = c3.text_input("업체명", value=m.get("업체명", ""), key=f"cp_{key_id}", disabled=True)
    spec = c4.text_input("규격", value=m.get("규격", ""), key=f"sp_{key_id}", disabled=True)
    
    c5, c6, c7, c8 = st.columns([1, 1.5, 3.5, 1])
    unit = c5.text_input("단위", value=m.get("단위", ""), key=f"un_{key_id}", disabled=True)
    price = c6.text_input("상한금액", value=m.get("상한금액", ""), key=f"pr_{key_id}", disabled=True)
    j_name = c7.text_input("주성분명", value=m.get("주성분명", ""), key=f"jn_{key_id}", disabled=True)
    date_v = c8.text_input("전일", value=m.get("전일", ""), key=f"dt_{key_id}", disabled=True)
    return [edi_val, name, comp, spec, unit, price, j_name, date_v]

# --- 6. 탭 구성 (7개 탭) ---
tab_titles = ["사용중지", "신규입고", "대체입고", "급여코드변경", "단가변경적용(상한가인하▼)", "단가변경적용(상한가인상▲)", "약가조회"]
tabs = st.tabs(tab_titles)

def handle_submit(data_row, category):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        data_row[0], data_row[1], data_row[2], data_row[54], data_row[55] = category, app_date, app_user, app_remark, app_status
        ws.append_row(data_row)
        st.success(f"[{category}] 제출 완료!"); st.balloons()
    except Exception as e: st.error(f"오류: {e}")

# [사용중지]
with tabs[0]:
    row = [""] * 56
    st.markdown('<div class="section-label">제품 정보</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드1", key="t0_edi")
    auto = render_drug_info(edi, "t0")
    row[3:11] = auto
    
    st.markdown('<div class="section-label">상세 입력</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26] = c1.selectbox("원내구분", ["원내", "원외", "원내/외"], key="t0_1")
    row[27] = c2.selectbox("급여구분", ["급여", "비급여"], key="t0_2")
    row[11] = c3.text_input("구입처", key="t0_3")
    row[12] = c4.number_input("개당입고가", value=0, key="t0_4")
    
    c5, c6, c7, c8 = st.columns(4)
    row[13] = c5.date_input("사용중지일", key="t0_5").strftime('%Y-%m-%d')
    row[14] = c6.selectbox("사용중지사유", OP_STOP_REASON, key="t0_6")
    row[15] = c7.text_input("사용중지사유_기타", key="t0_7")
    row[17] = c8.selectbox("재고여부", ["유", "무"], key="t0_8")
    
    c9, c10, c11 = st.columns(3)
    row[18] = c9.selectbox("재고처리방법", OP_STOCK_METHOD, key="t0_9")
    row[19] = c10.number_input("재고량", value=0, key="t0_10")
    row[22] = c11.number_input("반품량", value=0, key="t0_11")
    if st.button("🚀 사용중지 제출"): handle_submit(row, tab_titles[0])

# [신규입고]
with tabs[1]:
    row = [""] * 56
    st.markdown('<div class="section-label">제품 정보</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드1", key="t1_edi")
    auto = render_drug_info(edi, "t1")
    row[3:11] = auto
    
    st.markdown('<div class="section-label">상세 입력</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외"], key="t1_1"), c2.selectbox("급여구분", ["급여", "비급여"], key="t1_2"), c3.text_input("구입처", key="t1_3"), c4.number_input("개당입고가", value=0, key="t1_4")
    
    c5, c6, c7, c8 = st.columns(4)
    row[33], row[32], row[28], row[29] = c5.text_input("상한가외입고사유", key="t1_5"), c6.date_input("코드사용시작일", key="t1_6").strftime('%Y-%m-%d'), c7.selectbox("입고요청진료과", OP_DEPT, key="t1_7"), c8.selectbox("원내유무(동일성분)", ["유", "무"], key="t1_8")
    
    c9, c10 = st.columns(2)
    row[30], row[31] = c9.selectbox("사용기간", OP_USE_PERIOD, key="t1_9"), c10.date_input("입고일", key="t1_10").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 제출"): handle_submit(row, tab_titles[1])

# [대체입고]
with tabs[2]:
    row = [""] * 56
    st.markdown('<div class="section-label">기존 약제 정보</div>', unsafe_allow_html=True)
    edi1 = st.text_input("제품코드1", key="t2_edi1")
    auto1 = render_drug_info(edi1, "t2_1")
    row[3:11] = auto1
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[25], row[17] = c1.selectbox("원내구분1", ["원내", "원외"], key="t2_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_2"), c3.selectbox("신규약제와병용사용", ["Y", "N"], key="t2_3"), c4.selectbox("재고여부1", ["유", "무"], key="t2_4")
    c5, c6, c7, c8 = st.columns(4)
    row[20], row[21], row[22], row[23] = c5.selectbox("반품가능여부", ["가능", "불가"], key="t2_5"), c6.date_input("반품예정일", key="t2_6").strftime('%Y-%m-%d'), c7.number_input("반품량1", value=0, key="t2_7"), c8.selectbox("코드중지기준", OP_STOP_CRITERIA, key="t2_8")

    st.markdown('<div class="section-label">대체 약제 정보</div>', unsafe_allow_html=True)
    edi2 = st.text_input("제품코드2", key="t2_edi2")
    m2 = get_drug_info(edi2, master_df)
    row[36:44] = [edi2, m2.get("제품명",""), m2.get("업체명",""), m2.get("규격",""), m2.get("단위",""), m2.get("상한금액",""), m2.get("주성분명",""), m2.get("전일","")]
    st.columns(4)[0].text_input("제품명2 (자동)", value=row[37], disabled=True)
    
    c9, c10, c11, c12 = st.columns(4)
    row[46], row[47], row[44], row[45] = c9.selectbox("원내구분2", ["원내", "원외"], key="t2_9"), c10.selectbox("급여구분2", ["급여", "비급여"], key="t2_10"), c11.text_input("구입처2", key="t2_11"), c12.number_input("개당입고가2", value=0, key="t2_12")
    
    c13, c14, c15 = st.columns(3)
    row[50], row[51], row[48] = c13.text_input("상한가외입고사유2", key="t2_13"), c14.selectbox("기존약제와병용사용", ["Y", "N"], key="t2_14"), c15.selectbox("입고요청사유", OP_IN_REASON, key="t2_15")
    
    c16, c17, c18 = st.columns(3)
    row[49], row[52], row[53] = c16.date_input("코드사용시작일2", key="t2_16").strftime('%Y-%m-%d'), c17.selectbox("사용기간2", OP_USE_PERIOD, key="t2_17"), c18.date_input("입고일2", key="t2_18").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 제출"): handle_submit(row, tab_titles[2])

# [급여코드변경] & [단가인하]
for i in [3, 4]:
    with tabs[i]:
        row = [""] * 56
        st.markdown('<div class="section-label">반품 약제 정보</div>', unsafe_allow_html=True)
        edi1 = st.text_input(f"제품코드1_{i}", key=f"t{i}_edi1")
        row[3:11] = render_drug_info(edi1, f"t{i}_1")
        c1, c2, c3, c4 = st.columns(4)
        row[26], row[27], row[16], row[17] = c1.selectbox("원내구분1", ["원내", "원외"], key=f"t{i}_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key=f"t{i}_2"), c3.selectbox("변경내용", OP_CHANGE_CONTENT, key=f"t{i}_3"), c4.selectbox("재고여부1", ["유", "무"], key=f"t{i}_4")
        c5, c6, c7 = st.columns(3)
        row[21], row[22], row[23] = c5.date_input("반품예정일", key=f"t{i}_5").strftime('%Y-%m-%d'), c6.number_input("반품량", value=0, key=f"t{i}_6"), c7.selectbox("코드중지기준", OP_STOP_CRITERIA, key=f"t{i}_7")
        
        st.markdown('<div class="section-label">변경 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input(f"제품코드2_{i}", key=f"t{i}_edi2")
        m2 = get_drug_info(edi2, master_df)
        row[36:44] = [edi2, m2.get("제품명",""), m2.get("업체명",""), m2.get("규격",""), m2.get("단위",""), m2.get("상한금액",""), m2.get("주성분명",""), m2.get("전일","")]
        st.columns(4)[0].text_input("제품명2", value=row[37], disabled=True, key=f"t{i}_nm2")
        c8, c9, c10, c11, c12 = st.columns(5)
        row[46], row[47], row[44], row[45], row[50] = c8.selectbox("원내구분2", ["원내", "원외"], key=f"t{i}_8"), c9.selectbox("급여구분2", ["급여", "비급여"], key=f"t{i}_9"), c10.text_input("구입처2", key=f"t{i}_10"), c11.number_input("개당입고가2", value=0, key=f"t{i}_11"), c12.text_input("상한가외사유2", key=f"t{i}_12")
        if st.button(f"🚀 {tab_titles[i]} 제출"): handle_submit(row, tab_titles[i])

# [단가인상]
with tabs[5]:
    row = [""] * 56
    st.markdown('<div class="section-label">약제 정보</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드1", key="t5_edi")
    row[3:11] = render_drug_info(edi, "t5")
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외"], key="t5_1"), c2.selectbox("급여구분", ["급여", "비급여"], key="t5_2"), c3.text_input("구입처", key="t5_3"), c4.number_input("개당입고가", value=0, key="t5_4")
    c5, c6 = st.columns(2)
    row[13], row[16] = c5.date_input("단가변경_품절일", key="t5_5").strftime('%Y-%m-%d'), c6.selectbox("변경내용", OP_CHANGE_CONTENT, key="t5_6")
    if st.button("🚀 단가변경적용(상한가인상▲) 제출"): handle_submit(row, tab_titles[5])

# [약가조회]
with tabs[6]:
    st.markdown('<div class="section-label">Master DB 약제 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("EDI 코드 입력", placeholder="예: 648500030")
    if s_edi:
        m = get_drug_info(s_edi, master_df)
        if m:
            st.info(f"🔍 검색 결과: **{m.get('제품명')}**")
            col_a, col_b = st.columns(2)
            with col_a:
                st.write(f"**업체명:** {m.get('업체명')}")
                st.write(f"**상한금액:** :red[{m.get('상한금액')}] 원")
                st.write(f"**규격/단위:** {m.get('규격')} / {m.get('단위')}")
            with col_b:
                st.write(f"**주성분명:** {m.get('주성분명')}")
                st.write(f"**적용일:** {m.get('전일')}")
                st.write(f"**투여/분류:** {m.get('투여')} / {m.get('분류')}")
        else: st.error("해당 코드를 찾을 수 없습니다.")
