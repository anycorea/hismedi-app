import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 고해상도 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 메인 배경색 제거 및 여백 최소화 */
    .block-container { padding: 1rem 3rem !important; background-color: transparent !important; }
    [data-testid="stHeader"] { display: none; }
    
    /* 사이드바 스타일링 */
    [data-testid="stSidebar"] { border-right: 1px solid #e2e8f0; }
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: 10px; line-height: 1.2; }

    /* 6개 신청 탭 디자인 (크기 및 색상 차별화) */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; margin-bottom: 10px; }
    .stTabs [data-baseweb="tab"] { 
        height: 40px; padding: 0 15px; background-color: #f1f5f9; 
        border-radius: 6px 6px 0 0 !important; font-size: 0.9rem; font-weight: 700; color: #475569;
    }
    /* 각 탭별 포인트 컬러 (CSS 선택자) */
    .stTabs [data-baseweb="tab"]:nth-child(1) { border-top: 3px solid #ef4444; } /* 사용중지-빨강 */
    .stTabs [data-baseweb="tab"]:nth-child(2) { border-top: 3px solid #3b82f6; } /* 신규입고-파랑 */
    .stTabs [data-baseweb="tab"]:nth-child(3) { border-top: 3px solid #10b981; } /* 대체입고-초록 */
    .stTabs [data-baseweb="tab"]:nth-child(7) { background-color: #1e293b; color: #ffffff; border-radius: 6px !important; margin-left: 20px; } /* 약가조회-차별화 */

    .stTabs [aria-selected="true"] { background-color: #ffffff !important; color: #1E3A8A !important; border: 1px solid #cbd5e1 !important; border-bottom: none !important; }

    /* 제품코드(EDI) 입력창 강조 - 필수 입력 표시 */
    div[data-testid="stVerticalBlock"] > div:has(input[aria-label*="제품코드"]) input {
        background-color: #FFFBEB !important; /* 연한 노랑 */
        border: 2px solid #F59E0B !important; /* 오렌지색 테두리 */
        font-weight: 800 !important;
        color: #B45309 !important;
    }

    /* 자동 가져오기 필드 (ReadOnly) 스타일 */
    input:disabled {
        -webkit-text-fill-color: #000000 !important;
        background-color: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
        font-weight: 600 !important;
    }

    /* 섹션 간격 축소 */
    .section-label { font-size: 0.95rem; font-weight: 800; color: #1E3A8A; margin: 10px 0 5px 0; border-left: 4px solid #1E3A8A; padding-left: 8px; }
    .stDivider { margin: 10px 0 !important; }
    .row-widget.stButton { margin-top: 10px; }
    
    /* 입력창 레이블 크기 축소 */
    div[data-testid="stWidgetLabel"] p { font-size: 0.85rem !important; margin-bottom: 2px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 및 로드 ---
@st.cache_resource
def get_spreadsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=3600)
def load_master_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("Master").get_all_records())
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

# --- 3. 드롭다운 데이터 ---
DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
CHANGE = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
STOCK = ["재고 소진", "반품", "폐기"]
CRIT = ["즉시", "재고소진후"]
PERIOD = ["한시적 사용", "지속적 사용"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI †<br>Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("👤 신청자", placeholder="성명")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    comp_user = st.text_input("✅ 처리자", placeholder="완료자 성명")
    app_status = st.selectbox("진행", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("비고", placeholder="공통 비고", height=70)

# --- 5. UI 유틸리티 ---
def render_auto_info(edi_val, key_id):
    m = get_drug_info(edi_val, master_df)
    # 제품코드는 좁게, 제품명은 넓게
    c1, c2, c3 = st.columns([1.5, 4, 2])
    edi = c1.text_input("제품코드", value=edi_val, key=f"edi_in_{key_id}")
    name = c2.text_input("제품명 (자동)", value=m.get("제품명", ""), disabled=True, key=f"nm_{key_id}")
    comp = c3.text_input("업체명 (자동)", value=m.get("업체명", ""), disabled=True, key=f"cp_{key_id}")
    
    c4, c5, c6, c7 = st.columns([1, 1, 3, 1])
    spec = c4.text_input("규격", value=m.get("규격", ""), disabled=True, key=f"sp_{key_id}")
    unit = c5.text_input("단위", value=m.get("단위", ""), disabled=True, key=f"un_{key_id}")
    jnam = c6.text_input("주성분명", value=m.get("주성분명", ""), disabled=True, key=f"jn_{key_id}")
    price = c7.text_input("상한금액", value=m.get("상한금액", ""), disabled=True, key=f"pr_{key_id}")
    
    # 리턴 데이터: 제품코드, 제품명, 업체명, 규격, 단위, 상한금액, 주성분명, 전일
    return [edi, name, comp, spec, unit, price, jnam, m.get("전일", "")]

def handle_submit(data, cat):
    if not app_user: st.error("신청자 성명을 입력하세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        data[0], data[1], data[2], data[54], data[55] = cat, app_date, app_user, app_remark, app_status
        ws.append_row(data)
        st.success(f"[{cat}] 데이터가 시트에 기록되었습니다."); st.balloons()
    except Exception as e: st.error(f"오류: {e}")

# --- 6. 메인 탭 구현 ---
tabs = st.tabs(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "🔍 약가조회"])

# [사용중지]
with tabs[0]:
    row = [""] * 56
    st.markdown('<div class="section-label">약제 정보 (필수 입력)</div>', unsafe_allow_html=True)
    edi_val = st.text_input("제품코드(EDI) 입력", key="t0_edi", label_visibility="collapsed", placeholder="숫자 9자리 입력")
    res = render_auto_info(edi_val, "t0")
    row[3:11] = res
    st.markdown('<div class="section-label">상세 내용</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외", "원내/외"], key="t0_1"), c2.selectbox("급여구분", ["급여", "비급여"], key="t0_2"), c3.text_input("구입처", key="t0_3"), c4.number_input("개당입고가", 0, key="t0_4")
    c5, c6, c7, c8 = st.columns(4)
    row[13], row[14], row[15], row[17] = c5.date_input("사용중지일", key="t0_5").strftime('%Y-%m-%d'), c6.selectbox("중지사유", REASON, key="t0_6"), c7.text_input("중지사유(기타)", key="t0_7"), c8.selectbox("재고여부", ["유", "무"], key="t0_8")
    c9, c10, c11 = st.columns(3)
    row[18], row[19], row[22] = c9.selectbox("재고처리방법", STOCK, key="t0_9"), c10.number_input("재고량", 0, key="t0_10"), c11.number_input("반품량", 0, key="t0_11")
    if st.button("🚀 사용중지 신청서 제출"): handle_submit(row, "사용중지")

# [신규입고]
with tabs[1]:
    row = [""] * 56
    edi_val = st.text_input("제품코드(EDI) 입력", key="t1_edi", label_visibility="collapsed", placeholder="숫자 9자리 입력")
    row[3:11] = render_auto_info(edi_val, "t1")
    st.markdown('<div class="section-label">상세 내용</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외"], key="t1_1"), c2.selectbox("급여구분", ["급여", "비급여"], key="t1_2"), c3.text_input("구입처", key="t1_3"), c4.number_input("개당입고가", 0, key="t1_4")
    c5, c6, c7, c8 = st.columns(4)
    row[33], row[32], row[28], row[29] = c5.text_input("상한가외입고사유", key="t1_5"), c6.date_input("코드사용시작일", key="t1_6").strftime('%Y-%m-%d'), c7.selectbox("입고요청진료과", DEPT, key="t1_7"), c8.selectbox("원내유무", ["유", "무"], key="t1_8")
    c9, c10 = st.columns(2)
    row[30], row[31] = c9.selectbox("사용기간", PERIOD, key="t1_9"), c10.date_input("입고일", key="t1_10").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 신청서 제출"): handle_submit(row, "신규입고")

# [대체입고]
with tabs[2]:
    row = [""] * 56
    st.markdown('<div class="section-label">기존 약제</div>', unsafe_allow_html=True)
    e1 = st.text_input("기존 제품코드", key="t2_e1", placeholder="9자리")
    row[3:11] = render_auto_info(e1, "t2_1")
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[25], row[17] = c1.selectbox("원내1", ["원내", "원외"], key="t2_1"), c2.selectbox("급여1", ["급여", "비급여"], key="t2_2"), c3.selectbox("병용사용", ["Y", "N"], key="t2_3"), c4.selectbox("재고여부", ["유", "무"], key="t2_4")
    c5, c6, c7, c8 = st.columns(4)
    row[20], row[21], row[22], row[23] = c5.selectbox("반품가능", ["가능", "불가"], key="t2_5"), c6.date_input("반품예정일", key="t2_6").strftime('%Y-%m-%d'), c7.number_input("반품량", 0, key="t2_7"), c8.selectbox("중지기준", CRIT, key="t2_8")
    st.divider()
    st.markdown('<div class="section-label">대체 약제</div>', unsafe_allow_html=True)
    e2 = st.text_input("대체 제품코드", key="t2_e2", placeholder="9자리")
    m2 = get_drug_info(e2, master_df)
    row[36:44] = [e2, m2.get("제품명",""), m2.get("업체명",""), m2.get("규격",""), m2.get("단위",""), m2.get("상한금액",""), m2.get("주성분명",""), m2.get("전일","")]
    st.columns([1,3])[0].text_input("제품명(자동)", value=row[37], disabled=True)
    c9, c10, c11, c12 = st.columns(4)
    row[46], row[47], row[44], row[45] = c9.selectbox("원내2", ["원내", "원외"], key="t2_9"), c10.selectbox("급여2", ["급여", "비급여"], key="t2_10"), c11.text_input("구입처2", key="t2_11"), c12.number_input("입고가2", 0, key="t2_12")
    c13, c14, c15 = st.columns(3)
    row[50], row[51], row[48] = c13.text_input("상한가외사유2", key="t2_13"), c14.selectbox("병용여부2", ["Y", "N"], key="t2_14"), c15.selectbox("입고요청사유", REASON, key="t2_15")
    c16, c17, c18 = st.columns(3)
    row[49], row[52], row[53] = c16.date_input("사용시작일2", key="t2_16").strftime('%Y-%m-%d'), c17.selectbox("사용기간2", PERIOD, key="t2_17"), c18.date_input("입고일2", key="t2_18").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 신청서 제출"): handle_submit(row, "대체입고")

# [급여코드변경 / 단가인하]
for i, name in enumerate(["급여코드변경", "단가변경적용(상한가인하▼)"], 3):
    with tabs[i]:
        row = [""] * 56
        st.markdown('<div class="section-label">반품(기존) 약제</div>', unsafe_allow_html=True)
        e1 = st.text_input(f"제품코드_{i}", key=f"t{i}_e1")
        row[3:11] = render_auto_info(e1, f"t{i}_1")
        c1, c2, c3, c4 = st.columns(4)
        row[26], row[27], row[16], row[17] = c1.selectbox("원내1", ["원내", "원외"], key=f"t{i}_1"), c2.selectbox("급여1", ["급여", "비급여"], key=f"t{i}_2"), c3.selectbox("변경내용", CHANGE, key=f"t{i}_3"), c4.selectbox("재고여부", ["유", "무"], key=f"t{i}_4")
        c5, c6, c7 = st.columns(3)
        row[21], row[22], row[23] = c5.date_input("반품예정일", key=f"t{i}_5").strftime('%Y-%m-%d'), c6.number_input("반품량", 0, key=f"t{i}_6"), c7.selectbox("중지기준", CRIT, key=f"t{i}_7")
        st.divider()
        st.markdown('<div class="section-label">변경 약제</div>', unsafe_allow_html=True)
        e2 = st.text_input(f"변경 제품코드_{i}", key=f"t{i}_e2")
        m2 = get_drug_info(e2, master_df)
        row[36:44] = [e2, m2.get("제품명",""), m2.get("업체명",""), m2.get("규격",""), m2.get("단위",""), m2.get("상한금액",""), m2.get("주성분명",""), m2.get("전일","")]
        c8, c9, c10, c11, c12 = st.columns(5)
        row[46], row[47], row[44], row[45], row[50] = c8.selectbox("원내2", ["원내", "원외"], key=f"t{i}_8"), c9.selectbox("급여2", ["급여", "비급여"], key=f"t{i}_9"), c10.text_input("구입처2", key=f"t{i}_10"), c11.number_input("입고가2", 0, key=f"t{i}_11"), c12.text_input("상한가외사유2", key=f"t{i}_12")
        if st.button(f"🚀 {name} 제출"): handle_submit(row, name)

# [단가인상]
with tabs[5]:
    row = [""] * 56
    e1 = st.text_input("제품코드", key="t5_e1")
    row[3:11] = render_auto_info(e1, "t5")
    st.markdown('<div class="section-label">상세 정보</div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내", ["원내", "원외"], key="t5_1"), c2.selectbox("급여", ["급여", "비급여"], key="t5_2"), c3.text_input("구입처", key="t5_3"), c4.number_input("입고가", 0, key="t5_4")
    c5, c6 = st.columns(2)
    row[13], row[16] = c5.date_input("단가변경_품절일", key="t5_5").strftime('%Y-%m-%d'), c6.selectbox("변경내용", CHANGE, key="t5_6")
    if st.button("🚀 단가인상 제출"): handle_submit(row, "단가변경적용(상한가인상▲)")

# [약가조회 - 차별화된 디자인]
with tabs[6]:
    st.markdown("### 🔍 Master DB 약제 검색")
    s_edi = st.text_input("조회할 제품코드를 입력하세요", placeholder="숫자 9자리")
    if s_edi:
        m = get_drug_info(s_edi, master_df)
        if m:
            st.success(f"**제품명:** {m.get('제품명')}")
            col1, col2, col3 = st.columns(3)
            col1.metric("상한금액", f"{m.get('상한금액')}원")
            col2.write(f"**업체명:** {m.get('업체명')}")
            col2.write(f"**주성분명:** {m.get('주성분명')}")
            col3.write(f"**규격/단위:** {m.get('규격')} {m.get('단위')}")
            col3.write(f"**투여/분류:** {m.get('투여')} / {m.get('분류')}")
            st.caption(f"적용일: {m.get('전일')} | 비고: {m.get('비고')}")
        else: st.error("해당 코드가 Master DB에 없습니다.")
