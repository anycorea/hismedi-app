import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 (이미지 기반 표 스타일) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 여백 및 메인 폰트 */
    .block-container { padding-top: 2rem !important; background-color: #ffffff !important; }
    [data-testid="stHeader"] { display: none; }
    
    /* 사이드바 스타일 */
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }
    [data-testid="stSidebar"] hr { margin: 15px 0px !important; }

    /* 7개 탭 디자인 표준화 */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 45px; min-width: 120px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 8px 8px 0 0 !important;
        font-size: 0.9rem; font-weight: 700; color: #64748b;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; color: #ffffff !important; 
        border: 1px solid #1E3A8A !important;
    }

    /* [이미지 스타일 반영] 자동입력 구역 테이블 디자인 */
    .drug-table {
        width: 100%; border-collapse: collapse; margin-bottom: 15px;
        border: 1px solid #e2e8f0; font-size: 0.85rem;
    }
    .drug-table th {
        background-color: #f1f5f9; color: #475569; font-weight: 700;
        padding: 6px; border: 1px solid #e2e8f0; text-align: center;
    }
    .drug-table td {
        background-color: #ffffff; color: #000000; font-weight: 600;
        padding: 8px; border: 1px solid #e2e8f0; text-align: center;
    }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }

    /* 제품코드 입력창 노란색 강조 */
    div[data-testid="stVerticalBlock"] div:has(input[aria-label*="제품코드"]) input {
        background-color: #fffdec !important; border: 1px solid #fbbf24 !important;
        font-weight: 700 !important;
    }

    /* 섹션 헤더 */
    .section-header { 
        font-size: 1rem; font-weight: 800; color: #1E3A8A; 
        margin: 15px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A;
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

# --- 3. 드롭다운 옵션 리스트 ---
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_STOP_CRIT = ["즉시", "재고소진후"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_IN_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]

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

# --- 5. [이미지 스타일] 약제 정보 테이블 렌더링 함수 ---
def render_drug_table(edi_val, label_title="약제 정보"):
    m = get_drug_info(edi_val, master_df)
    st.markdown(f"**{label_title}**")
    
    price = str(m.get("상한금액", "-")).replace(',', '')
    
    table_html = f"""
    <table class="drug-table">
        <tr><th>제품코드</th><th>제품명</th><th>업체명</th><th>규격</th></tr>
        <tr><td>{edi_val if edi_val else "-"}</td><td class="blue-cell">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위</th><th>상한금액</th><th>주성분명</th><th>적용일(전일)</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td class="red-cell">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)
    return [edi_val, m.get("제품명", ""), m.get("업체명", ""), m.get("규격", ""), m.get("단위", ""), price, m.get("주성분명", ""), m.get("전일", ""), m.get("투여",""), m.get("분류",""), m.get("비고","")]

def handle_submit(row_data, category):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        row_data[0], row_data[1], row_data[2], row_data[54], row_data[55] = category, app_date, app_user, app_remark, app_status
        ws.append_row(row_data)
        st.success(f"[{category}] 저장 완료!"); st.balloons()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 (7개 탭 전체 완성) ---
tab_names = ["사용중지", "신규입고", "대체입고", "급여코드변경", "단가변경적용(상한가인하▼)", "단가변경적용(상한가인상▲)", "🔍 약가조회"]
tabs = st.tabs(tab_names)

# [Group A: 사용중지, 신규입고, 단가인상]
for i in [0, 1, 5]:
    with tabs[i]:
        title = tab_names[i]
        st.markdown(f'<div class="section-header">{title} 신청</div>', unsafe_allow_html=True)
        edi_val = st.text_input("제품코드1 입력", key=f"main_edi_{i}", placeholder="9자리 코드 입력 후 엔터")
        res = render_drug_table(edi_val)
        row = [""] * 60
        row[3:11] = res[0:8]
        
        st.markdown('<div class="section-header">상세 내용 입력</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        if i == 0: # 사용중지
            row[26], row[27], row[11], row[12] = c1.selectbox("원내구분1", ["원내", "원외", "원내/외"], key=f"v{i}_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처1", key=f"v{i}_3"), c4.number_input("개당입고가1", 0, key=f"v{i}_4")
            c5, c6, c7, c8 = st.columns(4)
            row[13], row[14], row[15], row[17] = c5.date_input("사용중지일1", key=f"v{i}_5").strftime('%Y-%m-%d'), c6.selectbox("사용중지사유1", OP_STOP_REASON, key=f"v{i}_6"), c7.text_input("사유기타", key=f"v{i}_7"), c8.selectbox("재고여부1", ["유", "무"], key=f"v{i}_8")
            c9, c10, c11 = st.columns(3)
            row[18], row[19], row[22] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key=f"v{i}_9"), c10.number_input("재고량1", 0, key=f"v{i}_10"), c11.number_input("반품량1", 0, key=f"v{i}_11")
        elif i == 1: # 신규입고
            row[26], row[27], row[11], row[12] = c1.selectbox("원내구분1", ["원내", "원외"], key=f"v{i}_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처1", key=f"v{i}_3"), c4.number_input("개당입고가1", 0, key=f"v{i}_4")
            c5, c6, c7, c8 = st.columns(4)
            row[33], row[32], row[28], row[29] = c5.text_input("상한가외입고사유1", key=f"v{i}_5"), c6.date_input("코드사용시작일1", key=f"v{i}_6").strftime('%Y-%m-%d'), c7.selectbox("입고요청진료과1", OP_DEPT, key=f"v{i}_7"), c8.selectbox("원내유무1", ["유", "무"], key=f"v{i}_8")
            c9, c10 = st.columns(2)
            row[30], row[31] = c9.selectbox("사용기간1", OP_USE_PERIOD, key=f"v{i}_9"), c10.date_input("입고일1", key=f"v{i}_10").strftime('%Y-%m-%d')
        elif i == 5: # 단가인상
            row[26], row[27], row[11], row[12] = c1.selectbox("원내구분1", ["원내", "원외"], key=f"v{i}_1"), c2.selectbox("급여구분1", ["급여", "비급여"], key=f"v{i}_2"), c3.text_input("구입처1", key=f"v{i}_3"), c4.number_input("개당입고가1", 0, key=f"v{i}_4")
            c5, c6 = st.columns(2)
            row[13], row[16] = c5.date_input("단가변경_품절일1", key=f"v{i}_5").strftime('%Y-%m-%d'), c6.selectbox("변경내용1", OP_CHANGE_CONTENT, key=f"v{i}_6")
        
        if st.button(f"🚀 {title} 제출", key=f"btn_{i}", use_container_width=True): handle_submit(row, title)

# [Group B: 대체입고, 급여코드변경, 단가인하]
for i in [2, 3, 4]:
    with tabs[i]:
        title = tab_names[i]
        row = [""] * 60
        st.markdown(f'<div class="section-header">{title} 신청</div>', unsafe_allow_html=True)
        e1 = st.text_input("제품코드1 입력 (기존/반품)", key=f"e1_{i}")
        res1 = render_drug_table(e1, "[기존/반품 약제 정보]")
        row[3:11] = res1[0:8]
        
        c1, c2, c3, c4 = st.columns(4)
        row[26], row[27], row[17] = c1.selectbox("원내1", ["원내", "원외"], key=f"v1_{i}"), c2.selectbox("급여1", ["급여", "비급여"], key=f"v2_{i}"), c4.selectbox("재고여부1", ["유", "무"], key=f"v3_{i}")
        if i == 2: row[25] = c3.selectbox("신규병용", ["Y", "N"], key=f"v4_{i}")
        else: row[16] = c3.selectbox("변경내용", OP_CHANGE_CONTENT, key=f"v4_{i}")
        
        c5, c6, c7, c8 = st.columns(4)
        if i == 2: row[20], row[21], row[22], row[23] = c5.selectbox("반품가능", ["가능", "불가"], key=f"v5_{i}"), c6.date_input("반품일", key=f"v6_{i}").strftime('%Y-%m-%d'), c7.number_input("반품량", 0, key=f"v7_{i}"), c8.selectbox("중지기준", OP_STOP_CRIT, key=f"v8_{i}")
        else: row[21], row[22], row[23] = c5.date_input("반품일", key=f"v6_{i}").strftime('%Y-%m-%d'), c6.number_input("반품량", 0, key=f"v7_{i}"), c7.selectbox("중지기준", OP_STOP_CRIT, key=f"v8_{i}")

        st.divider()
        e2 = st.text_input("제품코드2 입력 (대체/변경)", key=f"e2_{i}")
        res2 = render_drug_table(e2, "[대체/변경 약제 정보]")
        row[36:44] = res2[0:8]
        
        c9, c10, c11, c12 = st.columns(4)
        row[46], row[47], row[44], row[45] = c9.selectbox("원내2", ["원내", "원외"], key=f"v9_{i}"), c10.selectbox("급여2", ["급여", "비급여"], key=f"v10_{i}"), c11.text_input("구입처2", key=f"v11_{i}"), c12.number_input("개당입고가2", 0, key=f"v12_{i}")
        
        if i == 2:
            c13, c14, c15 = st.columns(3)
            row[50], row[51], row[48] = c13.text_input("상한가외사유2", key=f"v13_{i}"), c14.selectbox("기존병용", ["Y", "N"], key=f"v14_{i}"), c15.selectbox("입고요청사유", OP_IN_REASON, key=f"v15_{i}")
            c16, c17, c18 = st.columns(3)
            row[49], row[52], row[53] = c16.date_input("사용시작일", key=f"v16_{i}").strftime('%Y-%m-%d'), c17.selectbox("사용기간", OP_USE_PERIOD, key=f"v17_{i}"), c18.date_input("입고일", key=f"v18_{i}").strftime('%Y-%m-%d')
        else:
            row[50] = st.text_input("상한가외입고사유2", key=f"v13_{i}")

        if st.button(f"🚀 {title} 제출", key=f"btn_{i}", use_container_width=True): handle_submit(row, title)

# [약가조회]
with tabs[6]:
    st.markdown('<div class="section-header">Master DB 통합 조회</div>', unsafe_allow_html=True)
    search_edi = st.text_input("조회할 제품코드 입력", key="final_search_edi")
    if search_edi:
        res = render_drug_table(search_edi)
        if res[1]: 
            st.info(f"**투여:** {res[8]} | **분류:** {res[9]} | **비고:** {res[10]}")
        else:
            st.error("Master DB에 해당 코드가 없습니다.")
