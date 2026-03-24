import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 초정밀 레이아웃 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 전체 배경 및 상단 여백 최적화 */
    .block-container { padding: 1.5rem 2rem !important; background-color: #ffffff !important; }
    [data-testid="stHeader"] { display: none; }
    
    /* 사이드바 스타일 */
    .sidebar-title { font-size: 1.3rem; font-weight: 800; color: #1E3A8A; margin-bottom: 15px; }

    /* 7개 탭 디자인 표준화 */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; margin-bottom: 15px; }
    .stTabs [data-baseweb="tab"] { 
        height: 42px; min-width: 110px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 6px 6px 0 0 !important;
        font-size: 0.85rem; font-weight: 700; color: #64748b;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; color: #ffffff !important; 
        border: 1px solid #1E3A8A !important;
    }

    /* 엑셀 스타일 테이블 레이블 (2줄 압축용) */
    .table-label {
        font-size: 0.8rem; font-weight: 700; color: #475569;
        background-color: #f1f5f9; padding: 3px 10px;
        border-radius: 4px 4px 0 0; border: 1px solid #e2e8f0;
        margin-bottom: -1px; text-align: center;
    }

    /* 제품코드(EDI) 입력창 - 노란색 강조 */
    div[data-testid="stVerticalBlock"] div:has(input[aria-label*="EDI"]) input {
        background-color: #fffdec !important; border: 1px solid #fbbf24 !important;
        font-weight: 700 !important; height: 38px !important;
    }

    /* 자동 완성 필드 (ReadOnly) 스타일 */
    input:disabled {
        -webkit-text-fill-color: #000000 !important;
        background-color: #f8fafc !important; border: 1px solid #e2e8f0 !important;
        opacity: 1 !important; height: 38px !important; font-size: 0.9rem !important;
    }

    /* 섹션 헤더 */
    .section-header { 
        font-size: 1rem; font-weight: 800; color: #1E3A8A; 
        margin: 15px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A;
    }
    
    /* 간격 조정 */
    .stDivider { margin: 15px 0 !important; }
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
        df = pd.DataFrame(ss.worksheet("Master").get_all_records())
        df.columns = [c.strip() for c in df.columns]
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

# --- 3. 공통 옵션 리스트 ---
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK = ["재고 소진", "반품", "폐기"]
OP_STOP_CRIT = ["즉시", "재고소진후"]
OP_PERIOD = ["한시적 사용", "지속적 사용"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("👤 신청자 성명", key="sb_u")
    app_date = st.date_input("📅 신청 일자", datetime.now()).strftime('%Y-%m-%d')
    st.divider()
    comp_user = st.text_input("✅ 완료자(약사)", key="sb_c")
    app_status = st.selectbox("⚙️ 진행 상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 공통 비고", height=80)

# --- 5. 표준화된 제품 정보 렌더링 함수 (2줄 압축 표 형식) ---
def render_drug_row(edi_val, key_id):
    """모든 탭에서 동일하게 사용하는 2줄 압축 제품 정보 레이아웃"""
    m = get_drug_info(edi_val, master_df)
    
    # [표 1층: 제품코드, 제품명, 업체명, 규격]
    c1, c2, c3, c4 = st.columns([1.5, 4, 2, 1.5])
    with c1: 
        st.markdown('<div class="table-label">EDI 제품코드</div>', unsafe_allow_html=True)
        edi = st.text_input("edi", value=edi_val, key=f"e_{key_id}", label_visibility="collapsed")
    with c2:
        st.markdown('<div class="table-label">제품명 (자동)</div>', unsafe_allow_html=True)
        name = st.text_input("nm", value=m.get("제품명", ""), disabled=True, key=f"n_{key_id}", label_visibility="collapsed")
    with c3:
        st.markdown('<div class="table-label">업체명 (자동)</div>', unsafe_allow_html=True)
        comp = st.text_input("cp", value=m.get("업체명", ""), disabled=True, key=f"c_{key_id}", label_visibility="collapsed")
    with c4:
        st.markdown('<div class="table-label">규격</div>', unsafe_allow_html=True)
        spec = st.text_input("sp", value=m.get("규격", ""), disabled=True, key=f"s_{key_id}", label_visibility="collapsed")

    # [표 2층: 단위, 상한금액, 주성분명, 전일(적용일)]
    c5, c6, c7, c8 = st.columns([1, 1.5, 4, 1.5])
    with c5:
        st.markdown('<div class="table-label">단위</div>', unsafe_allow_html=True)
        unit = st.text_input("un", value=m.get("단위", ""), disabled=True, key=f"u_{key_id}", label_visibility="collapsed")
    with c6:
        st.markdown('<div class="table-label">상한금액</div>', unsafe_allow_html=True)
        price = st.text_input("pr", value=str(m.get("상한금액", "")).replace(',', ''), disabled=True, key=f"p_{key_id}", label_visibility="collapsed")
    with c7:
        st.markdown('<div class="table-label">주성분명</div>', unsafe_allow_html=True)
        jname = st.text_input("jn", value=m.get("주성분명", ""), disabled=True, key=f"j_{key_id}", label_visibility="collapsed")
    with c8:
        st.markdown('<div class="table-label">적용일(전일)</div>', unsafe_allow_html=True)
        date_v = st.text_input("dt", value=m.get("전일", ""), disabled=True, key=f"d_{key_id}", label_visibility="collapsed")
    
    return [edi, name, comp, spec, unit, price, jname, date_v]

def handle_submit(row_data, category):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        row_data[0], row_data[1], row_data[2], row_data[54], row_data[55] = category, app_date, app_user, app_remark, app_status
        ws.append_row(row_data)
        st.success(f"[{category}] 저장 완료!"); st.balloons()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 (7개 전 탭 표준화) ---
tab_names = ["사용중지", "신규입고", "대체입고", "급여코드변경", "단가변경적용(인하▼)", "단가변경적용(인상▲)", "🔍 약가조회"]
tabs = st.tabs(tab_names)

# [Group A: 사용중지, 신규입고, 단가인상]
for i in [0, 1, 5]:
    with tabs[i]:
        title = tab_names[i]
        st.markdown(f'<div class="section-header">{title} 신청 약제</div>', unsafe_allow_html=True)
        edi_val = st.text_input("제품코드 입력", key=f"main_edi_{i}", placeholder="9자리 코드 입력 후 엔터")
        res = render_drug_row(edi_val, f"tab{i}")
        
        row = [""] * 60
        row[3:11] = res # D~K열 매핑
        
        st.markdown('<div class="section-header">상세 내용 입력</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        if i == 0: # 사용중지
            row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외", "원내/외"], key=f"v{i}_1"), c2.selectbox("급여구분", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처", key=f"v{i}_3"), c4.number_input("개당입고가", 0, key=f"v{i}_4")
            c5, c6, c7, c8 = st.columns(4)
            row[13], row[14], row[15], row[17] = c5.date_input("중지일", key=f"v{i}_5").strftime('%Y-%m-%d'), c6.selectbox("중지사유", OP_REASON, key=f"v{i}_6"), c7.text_input("중지사유(기타)", key=f"v{i}_7"), c8.selectbox("재고여부", ["유", "무"], key=f"v{i}_8")
            c9, c10, c11 = st.columns(3)
            row[18], row[19], row[22] = c9.selectbox("처리방법", OP_STOCK, key=f"v{i}_9"), c10.number_input("재고량", 0, key=f"v{i}_10"), c11.number_input("반품량", 0, key=f"v{i}_11")
        elif i == 1: # 신규입고
            row[26], row[27], row[11], row[12] = c1.selectbox("원내구분", ["원내", "원외"], key=f"v{i}_1"), c2.selectbox("급여구분", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처", key=f"v{i}_3"), c4.number_input("개당입고가", 0, key=f"v{i}_4")
            c5, c6, c7, c8 = st.columns(4)
            row[33], row[32], row[28], row[29] = c5.text_input("상한가외사유", key=f"v{i}_5"), c6.date_input("사용시작일", key=f"v{i}_6").strftime('%Y-%m-%d'), c7.selectbox("요청진료과", OP_DEPT, key=f"v{i}_7"), c8.selectbox("원내유무", ["유", "무"], key=f"v{i}_8")
            c9, c10 = st.columns(2)
            row[30], row[31] = c9.selectbox("사용기간", OP_USE_PERIOD, key=f"v{i}_9"), c10.date_input("입고일", key=f"v{i}_10").strftime('%Y-%m-%d')
        elif i == 5: # 단가인상
            row[26], row[27], row[11], row[12] = c1.selectbox("원내", ["원내", "원외"], key=f"v{i}_1"), c2.selectbox("급여", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처", key=f"v{i}_3"), c4.number_input("개당입고가", 0, key=f"v{i}_4")
            c5, c6 = st.columns(2)
            row[13], row[16] = c5.date_input("단가변경_품절일", key=f"v{i}_5").strftime('%Y-%m-%d'), c6.selectbox("변경내용", OP_CHANGE, key=f"v{i}_6")

        if st.button(f"🚀 {title} 제출", use_container_width=True, key=f"btn_{i}"): handle_submit(row, title)

# [Group B: 대체입고, 급여코드변경, 단가인하]
for i in [2, 3, 4]:
    with tabs[i]:
        title = tab_names[i]
        row = [""] * 60
        st.markdown(f'<div class="section-header">기존/반품 약제 정보</div>', unsafe_allow_html=True)
        e1 = st.text_input("기존 제품코드", key=f"groupb_e1_{i}")
        res1 = render_drug_row(e1, f"tab{i}_1")
        row[3:11] = res1
        c1, c2, c3, c4 = st.columns(4)
        row[26], row[27], row[17] = c1.selectbox("원내1", ["원내", "원외"], key=f"v{i}_1"), c2.selectbox("급여1", ["급여", "비급여"], key=f"v{i}_2"), c4.selectbox("재고여부1", ["유", "무"], key=f"v{i}_4")
        if i == 2: row[25] = c3.selectbox("신규병용", ["Y", "N"], key=f"v{i}_3")
        else: row[16] = c3.selectbox("변경내용", OP_CHANGE, key=f"v{i}_3")
        c5, c6, c7, c8 = st.columns(4)
        if i == 2: row[20], row[21], row[22], row[23] = c5.selectbox("반품가능", ["가능", "불가"], key=f"v{i}_5"), c6.date_input("반품일", key=f"v{i}_6").strftime('%Y-%m-%d'), c7.number_input("반품량", 0, key=f"v{i}_7"), c8.selectbox("중지기준", OP_STOP_CRIT, key=f"v{i}_8")
        else: row[21], row[22], row[23] = c5.date_input("반품일", key=f"v{i}_5").strftime('%Y-%m-%d'), c6.number_input("반품량", 0, key=f"v{i}_6"), c7.selectbox("중지기준", OP_STOP_CRIT, key=f"v{i}_7")

        st.markdown(f'<div class="section-header">변경/대체 약제 정보</div>', unsafe_allow_html=True)
        e2 = st.text_input("대체 제품코드", key=f"groupb_e2_{i}")
        res2 = render_drug_row(e2, f"tab{i}_2")
        row[36:44] = res2
        c9, c10, c11, c12 = st.columns(4)
        row[46], row[47], row[44], row[45] = c9.selectbox("원내2", ["원내", "원외"], key=f"v{i}_9"), c10.selectbox("급여2", ["급여", "비급여"], key=f"v{i}_10"), c11.text_input("구입처2", key=f"v{i}_11"), c12.number_input("개당입고가2", 0, key=f"v{i}_12")
        
        if i == 2:
            c13, c14, c15 = st.columns(3)
            row[50], row[51], row[48] = c13.text_input("상한가외사유2", key=f"v{i}_13"), c14.selectbox("기존병용2", ["Y", "N"], key=f"v{i}_14"), c15.selectbox("입고요청사유", OP_REASON, key=f"v{i}_15")
            c16, c17, c18 = st.columns(3)
            row[49], row[52], row[53] = c16.date_input("시작일2", key=f"v{i}_16").strftime('%Y-%m-%d'), c17.selectbox("사용기간2", OP_USE_PERIOD, key=f"v{i}_17"), c18.date_input("입고일2", key=f"v{i}_18").strftime('%Y-%m-%d')
        else:
            row[50] = st.text_input("상한가외사유2", key=f"v{i}_13")

        if st.button(f"🚀 {title} 제출", use_container_width=True, key=f"btn_{i}"): handle_submit(row, title)

# [탭 7: 약가조회 표준화]
with tabs[6]:
    st.markdown('<div class="section-header">Master DB 약제 통합 조회</div>', unsafe_allow_html=True)
    search_edi = st.text_input("🔍 조회할 제품코드 입력", key="final_search_edi")
    if search_edi:
        # 신청서와 100% 동일한 2줄 압축 레이아웃 사용
        render_drug_row(search_edi, "search_final")
        m = get_drug_info(search_edi, master_df)
        if m:
            c1, c2 = st.columns(2)
            c1.info(f"**투여 경로:** {m.get('투여', '-')}")
            c2.info(f"**분류:** {m.get('분류', '-')}")
            st.warning(f"**비고:** {m.get('비고', '특이사항 없음')}")
        else:
            st.error("마스터 시트에서 해당 코드를 찾을 수 없습니다.")
