import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 표준화 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 전체 배경 및 상단 여백 최적화 */
    .block-container { padding-top: 2rem !important; background-color: #ffffff !important; }
    [data-testid="stHeader"] { display: none; }
    
    /* 사이드바 디자인 (차분한 그레이) */
    [data-testid="stSidebar"] { background-color: #fcfcfc; border-right: 1px solid #efefef; }
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }

    /* 탭 디자인 (일관된 둥근 스타일) */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 45px; min-width: 120px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 8px !important;
        font-size: 0.95rem; font-weight: 600; color: #64748b;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; color: #ffffff !important; 
        border: 1px solid #1E3A8A !important;
    }

    /* 제품코드(EDI) 입력창 강조 (부드러운 파스텔톤) */
    div[data-testid="stVerticalBlock"] div:has(input[aria-label*="EDI"]) input {
        background-color: #fffdec !important; /* 매우 연한 노랑 */
        border: 1.5px solid #d4d4d4 !important;
        font-weight: 700 !important;
    }

    /* 자동 완성 필드 (읽기전용) 표준 스타일 */
    input:disabled {
        -webkit-text-fill-color: #000000 !important;
        background-color: #f1f5f9 !important; /* 연한 블루그레이 */
        border: 1px solid #e2e8f0 !important;
        opacity: 1 !important;
    }

    /* 항목 레이블 간소화 */
    div[data-testid="stWidgetLabel"] p { font-size: 0.9rem !important; font-weight: 600 !important; color: #334155 !important; margin-bottom: 2px !important; }
    
    /* 섹션 구분선 및 여백 최소화 */
    .section-line { border-bottom: 2px solid #f1f5f9; margin: 15px 0 10px 0; }
    .stDivider { margin: 10px 0 !important; }
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
    if not target.empty:
        res = target.iloc[0].to_dict()
        res['상한금액'] = str(res.get('상한금액', '0')).replace(',', '').strip()
        return res
    return {}

# --- 3. 공통 데이터 리스트 ---
DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
CHANGE = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
STOCK_PROC = ["재고 소진", "반품", "폐기"]
STOP_CRIT = ["즉시", "재고소진후"]
PERIOD = ["한시적 사용", "지속적 사용"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI †<br>Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("👤 신청자 성명", key="sb_u")
    app_date = st.date_input("📅 신청 일자", datetime.now()).strftime('%Y-%m-%d')
    st.divider()
    comp_user = st.text_input("✅ 완료자 성명", key="sb_c")
    app_status = st.selectbox("⚙️ 진행 상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 공통 비고", height=80)

# --- 5. 표준화된 제품 정보 렌더링 함수 ---
def render_standard_drug_info(edi_val, key_id, is_search=False):
    """모든 탭에서 동일하게 사용하는 표준 정보 그리드"""
    m = get_drug_info(edi_val, master_df)
    
    # 1행: 코드(좁게), 명칭, 업체
    c1, c2, c3 = st.columns([1.5, 4, 2])
    # 검색용일 때는 배경을 다르게 하지 않음
    edi = c1.text_input("EDI 제품코드", value=edi_val, key=f"edi_std_{key_id}")
    name = c2.text_input("제품명 (자동)", value=m.get("제품명", ""), disabled=True, key=f"nm_std_{key_id}")
    comp = c3.text_input("업체명 (자동)", value=m.get("업체명", ""), disabled=True, key=f"cp_std_{key_id}")
    
    # 2행: 규격, 단위, 상한금액, 적용일
    c4, c5, c6, c7 = st.columns(4)
    spec = c4.text_input("규격", value=m.get("규격", ""), disabled=True, key=f"sp_std_{key_id}")
    unit = c5.text_input("단위", value=m.get("단위", ""), disabled=True, key=f"un_std_{key_id}")
    price = c6.text_input("상한금액", value=m.get("상한금액", ""), disabled=True, key=f"pr_std_{key_id}")
    date_v = c7.text_input("전일(적용일)", value=m.get("전일", ""), disabled=True, key=f"dt_std_{key_id}")
    
    # 3행: 주성분명, 투여/분류(검색시에만 노출)
    c8, c9 = st.columns([5, 3])
    j_name = c8.text_input("주성분명", value=m.get("주성분명", ""), disabled=True, key=f"jn_std_{key_id}")
    if is_search:
        c9.text_input("투여 / 분류", value=f"{m.get('투여','-')} / {m.get('분류','-')}", disabled=True, key=f"ext_std_{key_id}")
    
    return [edi, name, comp, spec, unit, price, j_name, m.get("전일", ""), m.get("투여",""), m.get("주성분명",""), m.get("비고","")]

def handle_submit(row_data, category):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        row_data[0], row_data[1], row_data[2], row_data[54], row_data[55] = category, app_date, app_user, app_remark, app_status
        ws.append_row(row_data)
        st.success(f"[{category}] 저장 완료!"); st.balloons()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "약가조회"]
tabs = st.tabs(tab_names)

# [사용중지]
with tabs[0]:
    row = [""] * 60
    st.write("**[제품 정보]**")
    edi = st.text_input("조회할 제품코드1", key="t0_edi", label_visibility="collapsed", placeholder="코드 9자리 입력")
    res = render_standard_drug_info(edi, "t0")
    row[3:11] = res[0:8]
    st.markdown('<div class="section-line"></div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분1", ["원내", "원외", "원내/외"], key="t0_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t0_2"), c3.text_input("구입처1", key="t0_3"), c4.number_input("개당입고가1", 0, key="t0_4")
    c5, c6, c7, c8 = st.columns(4)
    row[13], row[14], row[15], row[17] = c5.date_input("사용중지일1", key="t0_5").strftime('%Y-%m-%d'), c6.selectbox("사용중지사유", REASON, key="t0_6"), c7.text_input("중지사유_기타", key="t0_7"), c8.selectbox("재고여부1", ["유", "무"], key="t0_8")
    c9, c10, c11 = st.columns(3)
    row[18], row[19], row[22] = c9.selectbox("재고처리방법", STOCK_PROC, key="t0_9"), c10.number_input("재고량1", 0, key="t0_10"), c11.number_input("반품량1", 0, key="t0_11")
    if st.button("🚀 사용중지 신청 제출", use_container_width=True): handle_submit(row, "사용중지")

# [신규입고]
with tabs[1]:
    row = [""] * 60
    edi = st.text_input("신청할 제품코드1", key="t1_edi", label_visibility="collapsed", placeholder="코드 9자리 입력")
    res = render_standard_drug_info(edi, "t1")
    row[3:11] = res[0:8]
    st.markdown('<div class="section-line"></div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내구분1", ["원내", "원외"], key="t1_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_2"), c3.text_input("구입처1", key="t1_3"), c4.number_input("개당입고가1", 0, key="t1_4")
    c5, c6, c7, c8 = st.columns(4)
    row[33], row[32], row[28], row[29] = c5.text_input("상한가외입고사유1", key="t1_5"), c6.date_input("코드사용시작일1", key="t1_6").strftime('%Y-%m-%d'), c7.selectbox("입고요청진료과1", DEPT, key="t1_7"), c8.selectbox("원내유무1", ["유", "무"], key="t1_8")
    c9, c10 = st.columns(2)
    row[30], row[31] = c9.selectbox("사용기간1", PERIOD, key="t1_9"), c10.date_input("입고일1", key="t1_10").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 신청 제출", use_container_width=True): handle_submit(row, "신규입고")

# [대체입고]
with tabs[2]:
    row = [""] * 60
    st.write("**[기존 약제 정보]**")
    e1 = st.text_input("기존 제품코드", key="t2_e1")
    res1 = render_standard_drug_info(e1, "t2_1")
    row[3:11] = res1[0:8]
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[25], row[17] = c1.selectbox("원내1", ["원내", "원외"], key="t2_1"), c2.selectbox("급여1", ["급여", "비급여"], key="t2_2"), c3.selectbox("신규약제와병용1", ["Y", "N"], key="t2_3"), c4.selectbox("재고여부1", ["유", "무"], key="t2_4")
    c5, c6, c7, c8 = st.columns(4)
    row[20], row[21], row[22], row[23] = c5.selectbox("반품가능여부1", ["가능", "불가"], key="t2_5"), c6.date_input("반품예정일1", key="t2_6").strftime('%Y-%m-%d'), c7.number_input("반품량1", 0, key="t2_7"), c8.selectbox("코드사용중지일1", STOP_CRIT, key="t2_8")
    st.markdown('<div class="section-line"></div>', unsafe_allow_html=True)
    st.write("**[대체 약제 정보]**")
    e2 = st.text_input("대체 제품코드", key="t2_e2")
    res2 = render_standard_drug_info(e2, "t2_2")
    row[36:44] = res2[0:8]
    c9, c10, c11, c12 = st.columns(4)
    row[46], row[47], row[44], row[45] = c9.selectbox("원내2", ["원내", "원외"], key="t2_9"), c10.selectbox("급여2", ["급여", "비급여"], key="t2_10"), c11.text_input("구입처2", key="t2_11"), c12.number_input("개당입고가2", 0, key="t2_12")
    c13, c14, c15 = st.columns(3)
    row[50], row[51], row[48] = c13.text_input("상한가외입고사유2", key="t2_13"), c14.selectbox("기존약제와병용2", ["Y", "N"], key="t2_14"), c15.selectbox("입고요청사유2", REASON, key="t2_15")
    c16, c17, c18 = st.columns(3)
    row[49], row[52], row[53] = c16.date_input("코드사용시작일2", key="t2_16").strftime('%Y-%m-%d'), c17.selectbox("사용기간2", PERIOD, key="t2_17"), c18.date_input("입고일2", key="t2_18").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 신청 제출", use_container_width=True): handle_submit(row, "대체입고")

# [급여코드변경 / 단가인하]
for i, name in enumerate(["급여코드변경", "단가변경적용(상한가인하▼)"], 3):
    with tabs[i]:
        row = [""] * 60
        st.write("**[반품(기존) 정보]**")
        e1 = st.text_input(f"기존 제품코드_{i}", key=f"t{i}_e1")
        row[3:11] = render_standard_drug_info(e1, f"t{i}_1")[0:8]
        c1, c2, c3, c4 = st.columns(4)
        row[26], row[27], row[16], row[17] = c1.selectbox("원내1", ["원내", "원외"], key=f"t{i}_1"), c2.selectbox("급여1", ["급여", "비급여"], key=f"t{i}_2"), c3.selectbox("변경내용1", CHANGE, key=f"t{i}_3"), c4.selectbox("재고여부1", ["유", "무"], key=f"t{i}_4")
        c5, c6, c7 = st.columns(3)
        row[21], row[22], row[23] = c5.date_input("반품예정일1", key=f"t{i}_5").strftime('%Y-%m-%d'), c6.number_input("반품량1", 0, key=f"t{i}_6"), c7.selectbox("코드사용중지일1", STOP_CRIT, key=f"t{i}_7")
        st.markdown('<div class="section-line"></div>', unsafe_allow_html=True)
        st.write("**[변경(신규) 정보]**")
        e2 = st.text_input(f"변경 제품코드_{i}", key=f"t{i}_e2")
        row[36:44] = render_standard_drug_info(e2, f"t{i}_2")[0:8]
        c8, c9, c10, c11, c12 = st.columns(5)
        row[46], row[47], row[44], row[45], row[50] = c8.selectbox("원내2", ["원내", "원외"], key=f"t{i}_8"), c9.selectbox("급여2", ["급여", "비급여"], key=f"t{i}_9"), c10.text_input("구입처2", key=f"t{i}_10"), c11.number_input("개당입고가2", 0, key=f"t{i}_11"), c12.text_input("상한가외입고사유2", key=f"t{i}_12")
        if st.button(f"🚀 {name} 신청 제출", use_container_width=True): handle_submit(row, name)

# [단가인상]
with tabs[5]:
    row = [""] * 60
    e1 = st.text_input("인상 대상 제품코드", key="t5_e1")
    row[3:11] = render_standard_drug_info(e1, "t5")[0:8]
    st.markdown('<div class="section-line"></div>', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    row[26], row[27], row[11], row[12] = c1.selectbox("원내", ["원내", "원외"], key="t5_1"), c2.selectbox("급여", ["급여", "비급여"], key="t5_2"), c3.text_input("구입처", key="t5_3"), c4.number_input("개당입고가", 0, key="t5_4")
    c5, c6 = st.columns(2)
    row[13], row[16] = c5.date_input("단가변경_품절일", key="t5_5").strftime('%Y-%m-%d'), c6.selectbox("변경내용", CHANGE, key="t5_6")
    if st.button("🚀 단가인상 신청 제출", use_container_width=True): handle_submit(row, "단가변경적용(상한가인상▲)")

# [약가조회 - 표준 디자인 적용]
with tabs[6]:
    st.markdown("### 🔍 약제 정보 마스터 통합 조회")
    search_edi = st.text_input("조회할 제품코드를 입력하세요 (9자리)", placeholder="예: 648500030", key="search_tab_edi")
    if search_edi:
        # 신청 양식과 동일한 함수 호출 (is_search=True로 추가 정보 표시)
        res = render_standard_drug_info(search_edi, "search", is_search=True)
        if res[1]: # 제품명이 검색된 경우
            st.info(f"**비고:** {res[10] if res[10] else '특이사항 없음'}")
        else:
            st.error("해당 코드가 마스터 데이터에 존재하지 않습니다.")
