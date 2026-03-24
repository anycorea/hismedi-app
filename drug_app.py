import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 (CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1.5rem !important; background-color: #ffffff !important; }
    [data-testid="stHeader"] { display: none; }
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab-list"] { gap: 5px; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 45px; min-width: 110px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 8px 8px 0 0 !important;
        font-size: 0.9rem; font-weight: 700; color: #64748b;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: #ffffff !important; }
    
    /* 제품코드 입력창 강조 */
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important;
    }

    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 20px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
    
    /* 상태 뱃지 스타일 */
    .status-badge { padding: 4px 8px; border-radius: 4px; font-weight: 700; font-size: 0.8rem; }
    .status-done { background-color: #dcfce7; color: #166534; }
    .status-ing { background-color: #fef9c3; color: #854d0e; }
    .status-ready { background-color: #dbeafe; color: #1e40af; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
@st.cache_resource
def get_spreadsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=60)
def load_master_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("Master").get_all_records())
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=5) # 대시보드 데이터는 짧은 캐시 적용
def load_all_data(sheet_name):
    try:
        ss = get_spreadsheet()
        return pd.DataFrame(ss.worksheet(sheet_name).get_all_records())
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_IN_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("접속자 성명", key="global_user")
    app_date = st.date_input("오늘 날짜", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.info("신청 시에는 '신청자 성명'을,\n처리 시에는 '완료자 성명'을 입력하세요.")

# --- 5. 헬퍼 함수 ---
def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, label_title="약제 정보"):
    m = get_drug_info(edi_val, master_df)
    st.markdown(f"**{label_title}**")
    price = str(m.get("상한금액", "-")).replace(',', '')
    table_html = f"""
    <table class="drug-table">
        <tr><th>제품코드</th><th>제품명</th><th>업체명</th><th>규격</th></tr>
        <tr><td>{edi_val if edi_val else "-"}</td><td class="blue-cell">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위</th><th>상한금액</th><th>주성분명</th><th>의약품 구분</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td class="red-cell">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>
    """
    st.markdown(table_html, unsafe_allow_html=True)
    return [edi_val, m.get("제품명", ""), m.get("업체명", ""), m.get("규격", ""), m.get("단위", ""), price, m.get("주성분명", ""), m.get("전일", "")]

def handle_submit(row_data, category, summary_data):
    if not app_user: 
        st.error("성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        # 1. New_stop (상세) 저장
        ws_detail = ss.worksheet("New_stop")
        row_data[0], row_data[1], row_data[2], row_data[5] = category, app_date, app_user, "신청완료"
        ws_detail.append_row(row_data)
        
        # 2. Status (요약) 저장
        ws_status = ss.worksheet("Status")
        status_row = ["신청완료", app_date, app_user, "", category] + summary_data
        ws_status.append_row(status_row)
        
        st.success("성공적으로 접수되었습니다!"); st.balloons()
        st.cache_data.clear() # 캐시 즉시 삭제
        st.rerun() # 화면 갱신
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["🔍 진행현황(Dash)", "사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "약가조회"]
tabs = st.tabs(tab_names)

# [Tab 0] 진행현황 Dashboard (실시간 & 상호작용)
with tabs[0]:
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    
    st_df = load_all_data("Status")
    if st_df.empty:
        st.info("현재 접수된 내역이 없습니다.")
    else:
        # 통합 검색
        search_q = st.text_input("🔍 통합 검색 (코드, 제품명, 신청자 등)", key="dash_search")
        if search_q:
            st_df = st_df[st_df.apply(lambda r: r.astype(str).str.contains(search_q).any(), axis=1)]
        
        # 데이터 표시 (최신순 역순 정렬)
        display_df = st_df.iloc[::-1].reset_index()
        st.dataframe(display_df.drop(columns=['index']), use_container_width=True, height=300)
        
        st.divider()
        
        # 상세 항목 처리 섹션
        st.subheader("📝 선택 항목 상세보기 및 처리")
        selected_idx = st.selectbox("수정 또는 상세 확인을 위해 항목을 선택하세요.", 
                                    options=display_df.index,
                                    format_func=lambda i: f"[{display_df.loc[i, '신청구분']}] {display_df.loc[i, '제품명1']}{display_df.loc[i, '제품명2']} - {display_df.loc[i, '신청자']} ({display_df.loc[i, '신청일']})")
        
        if selected_idx is not None:
            sel_row = display_df.loc[selected_idx]
            original_row_index = sel_row['index'] + 2 # 구글 시트 실제 행 번호
            
            # 레이아웃 구성
            col_info, col_action = st.columns([3, 1])
            with col_info:
                st.info(f"**상세 정보** | {sel_row['신청구분']} (Row: {original_row_index})")
                c1, c2, c3 = st.columns(3)
                c1.metric("진행 상황", sel_row['진행상황'])
                c2.metric("신청자", sel_row['신청자'])
                c3.metric("신청일", sel_row['신청일'])
                
                if sel_row['제품코드1']: st.code(f"기존/대상: {sel_row['제품코드1']} | {sel_row['제품명1']} | {sel_row['사용중지일1']}")
                if sel_row['제품코드2']: st.code(f"대체/신규: {sel_row['제품코드2']} | {sel_row['제품명2']} | {sel_row['사용시작일2']}")
            
            with col_action:
                st.write("**상태 업데이트**")
                if st.button("🟠 처리중으로 변경", use_container_width=True):
                    ss = get_spreadsheet()
                    ss.worksheet("Status").update_cell(original_row_index, 1, "처리중")
                    ss.worksheet("Status").update_cell(original_row_index, 4, app_user)
                    st.success("상태가 변경되었습니다."); st.cache_data.clear(); st.rerun()
                
                if st.button("🟢 처리완료로 변경", use_container_width=True):
                    ss = get_spreadsheet()
                    now_str = datetime.now().strftime('%Y-%m-%d')
                    # Status 시트 업데이트
                    ss.worksheet("Status").update_cell(original_row_index, 1, "처리완료")
                    ss.worksheet("Status").update_cell(original_row_index, 4, app_user)
                    # New_stop (상세) 시트의 상태와 완료정보도 업데이트 하려면 추가 로직 필요 (이름/날짜 기준 매칭 등)
                    st.success("완료 처리되었습니다."); st.cache_data.clear(); st.rerun()

# [1] 사용중지
with tabs[1]:
    row = [""] * 58
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t1_edi")
    drug_info = render_drug_table(edi)
    row[6:14] = drug_info
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t1_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t1_p"), c3.text_input("구입처", key="t1_q"), c4.number_input("개당입고가", 0, key="t1_r")
    c5, c6, c7, c8 = st.columns(4)
    row[18] = c5.date_input("사용중지일", key="t1_s").strftime('%Y-%m-%d')
    row[19], row[20], row[22] = c6.selectbox("사유", OP_STOP_REASON, key="t1_t"), c7.text_input("사유(기타)", key="t1_u"), c8.selectbox("재고여부", ["유", "무"], key="t1_w")
    row[38] = st.text_area("비고(기타)", key="t1_am")
    if st.button("🚀 사용중지 제출", key="b1", use_container_width=True):
        handle_submit(row, "사용중지", [edi, drug_info[1], row[18], "", "", ""])

# [2] 신규입고
with tabs[2]:
    row = [""] * 58
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력 ", key="t2_edi")
    drug_info = render_drug_table(edi)
    row[6:14] = drug_info
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t2_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t2_p"), c3.text_input("구입처", key="t2_q"), c4.number_input("개당입고가", 0, key="t2_r")
    c5 = st.columns(1)[0]
    row[34] = c5.date_input("입고일", key="t2_ai").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 제출", key="b2", use_container_width=True):
        handle_submit(row, "신규입고", ["", "", "", edi, drug_info[1], row[34]])

# [3] 대체입고
with tabs[3]:
    row = [""] * 58
    st.markdown('<div class="section-header">기존 약제</div>', unsafe_allow_html=True)
    edi1 = st.text_input("기존 제품코드", key="t3_e1")
    drug_info1 = render_drug_table(edi1)
    row[6:14] = drug_info1
    row[28] = st.date_input("중지일", key="t3_ac").strftime('%Y-%m-%d')
    st.divider()
    st.markdown('<div class="section-header">대체 약제</div>', unsafe_allow_html=True)
    edi2 = st.text_input("대체 제품코드", key="t3_e2")
    drug_info2 = render_drug_table(edi2, "대체 약제")
    row[39:47] = drug_info2
    row[56] = st.date_input("입고일", key="t3_be").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 제출", key="b3", use_container_width=True):
        handle_submit(row, "대체입고", [edi1, drug_info1[1], row[28], edi2, drug_info2[1], row[56]])

# [4] 급여코드변경 / [5] 단가인하▼
for idx, tab_name in zip([4, 5], ["급여코드변경", "단가인하▼"]):
    with tabs[idx]:
        row = [""] * 58
        st.markdown(f'<div class="section-header">정보 입력</div>', unsafe_allow_html=True)
        e1 = st.text_input(f"현재 제품코드", key=f"t{idx}_e1")
        info1 = render_drug_table(e1)
        row[28] = st.date_input("변경(중지)일", key=f"t{idx}_ac").strftime('%Y-%m-%d')
        e2 = st.text_input(f"새 제품코드", key=f"t{idx}_e2")
        info2 = render_drug_table(e2, "변경 약제")
        if st.button(f"🚀 {tab_name} 제출", key=f"b{idx}", use_container_width=True):
            handle_submit(row, tab_name, [e1, info1[1], row[28], e2, info2[1], ""])

# [6] 단가인상▲
with tabs[6]:
    row = [""] * 58
    edi = st.text_input("인상 대상 코드", key="t6_edi")
    info = render_drug_table(edi)
    row[37] = st.date_input("변경일", key="t6_al").strftime('%Y-%m-%d')
    if st.button("🚀 단가인상 제출", key="b6", use_container_width=True):
        handle_submit(row, "단가인상", [edi, info[1], "", "", "", row[37]])

# [7] 약가조회
with tabs[7]:
    s_edi = st.text_input("조회할 제품코드", key="search_edi")
    if s_edi:
        render_drug_table(s_edi, "검색 결과")
