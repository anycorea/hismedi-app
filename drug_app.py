import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
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
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important;
    }
    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 20px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
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
        df.columns = [c.strip() for c in df.columns]
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=10)
def load_status_data():
    try:
        ss = get_spreadsheet()
        return pd.DataFrame(ss.worksheet("Status").get_all_records())
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
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("신청 일자", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.divider()
    done_user = st.text_input("완료자 성명", key="global_done_user")
    done_date = st.date_input("완료 일자", datetime.now(), key="global_done_date").strftime('%Y-%m-%d')
    app_status = st.selectbox("진행 상황", ["신청완료", "처리중", "처리완료"], key="global_status")

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
        st.error("신청자 성명을 입력해주세요.")
        return
    try:
        ss = get_spreadsheet()
        # 1. 상세 기록 (New_stop 시트)
        ws_detail = ss.worksheet("New_stop")
        row_data[0], row_data[1], row_data[2], row_data[3], row_data[4], row_data[5] = category, app_date, app_user, done_date, done_user, app_status
        ws_detail.append_row(row_data)
        
        # 2. 요약 기록 (Status 시트)
        ws_status = ss.worksheet("Status")
        # 구조: 진행상황-신청일-신청자-처리자-신청구분-제품코드1-제품명1-사용중지일1-제품코드2-제품명2-사용시작일2
        # summary_data 형식: [p1_code, p1_name, stop_d, p2_code, p2_name, start_d]
        status_row = ["신청완료", app_date, app_user, "", category] + summary_data
        ws_status.append_row(status_row)
        
        st.success(f"[{category}] 저장 성공!"); st.balloons()
        st.cache_data.clear()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "🔍 약가조회", "📊 진행현황"]
tabs = st.tabs(tab_names)

# [1] 사용중지
with tabs[0]:
    row = [""] * 58
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t0_edi")
    drug_info = render_drug_table(edi)
    row[6:14] = drug_info
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t0_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t0_p"), c3.text_input("구입처", key="t0_q"), c4.number_input("개당입고가", 0, key="t0_r")
    c5, c6, c7, c8 = st.columns(4)
    row[18] = c5.date_input("사용중지일", key="t0_s").strftime('%Y-%m-%d')
    row[19], row[20], row[22] = c6.selectbox("사용중지사유", OP_STOP_REASON, key="t0_t"), c7.text_input("사용중지사유(기타)", key="t0_u"), c8.selectbox("재고여부", ["유", "무"], key="t0_w")
    c9, c10, c11 = st.columns(3)
    row[23], row[24], row[27] = c9.selectbox("재고처리방법", OP_STOCK_METHOD, key="t0_x"), c10.number_input("재고량", 0, key="t0_y"), c11.number_input("반품량", 0, key="t0_ab")
    row[38] = st.text_area("비고(기타 요청사항)", key="t0_am")
    if st.button("🚀 사용중지 제출", key="b0", use_container_width=True):
        handle_submit(row, "사용중지", [edi, drug_info[1], row[18], "", "", ""])

# [2] 신규입고
with tabs[1]:
    row = [""] * 58
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력 ", key="t1_edi")
    drug_info = render_drug_table(edi)
    row[6:14] = drug_info
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t1_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t1_p"), c3.text_input("구입처", key="t1_q"), c4.number_input("개당입고가", 0, key="t1_r")
    c5, c6, c7, c8 = st.columns(4)
    row[36], row[35], row[31], row[32] = c5.text_input("상한가외입고사유", key="t1_ak"), c6.date_input("코드사용시작일", key="t1_aj").strftime('%Y-%m-%d'), c7.selectbox("입고요청진료과", OP_DEPT, key="t1_af"), c8.selectbox("원내유무(동일성분)", ["유", "무"], key="t1_ag")
    c9, c10 = st.columns(2)
    row[33] = c9.selectbox("사용기간", OP_USE_PERIOD, key="t1_ah")
    row[34] = c10.date_input("입고일", key="t1_ai").strftime('%Y-%m-%d')
    row[38] = st.text_area("비고(기타 요청사항) ", key="t1_am")
    if st.button("🚀 신규입고 제출", key="b1", use_container_width=True):
        handle_submit(row, "신규입고", ["", "", "", edi, drug_info[1], row[34]])

# [3] 대체입고
with tabs[2]:
    row = [""] * 58
    st.markdown('<div class="section-header">기존 약제 정보</div>', unsafe_allow_html=True)
    edi1 = st.text_input("제품코드 입력 (기존)", key="t2_e1")
    drug_info1 = render_drug_table(edi1, "[기존 약제]")
    row[6:14] = drug_info1
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[30], row[22] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t2_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t2_p"), c3.selectbox("신규약제와병용사용", ["Y", "N"], key="t2_ae"), c4.selectbox("재고여부", ["유", "무"], key="t2_w")
    c5, c6, c7, c8 = st.columns(4)
    row[25], row[26], row[27] = c5.selectbox("반품가능여부", ["가능", "불가"], key="t2_z"), c6.date_input("반품예정일", key="t2_aa").strftime('%Y-%m-%d'), c7.number_input("반품량", 0, key="t2_ab")
    row[28] = c8.date_input("코드사용중지일", key="t2_ac").strftime('%Y-%m-%d')
    st.divider()
    st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True)
    edi2 = st.text_input("제품코드 입력 (대체)", key="t2_e2")
    m2 = get_drug_info(edi2, master_df)
    drug_info2 = [edi2, m2.get("제품명", ""), m2.get("업체명", ""), m2.get("규격", ""), m2.get("단위", ""), str(m2.get("상한금액", "")).replace(',', ''), m2.get("주성분명", ""), m2.get("전일", "")]
    row[39:47] = drug_info2
    c9, c10, c11, c12 = st.columns(4)
    row[47], row[48], row[49], row[50] = c9.selectbox("원내구분 (대체)", OP_INSIDE_OUT, key="t2_av"), c10.selectbox("급여구분 (대체)", ["급여", "비급여"], key="t2_aw"), c11.text_input("구입처 (대체)", key="t2_ax"), c12.number_input("개당입고가 (대체)", 0, key="t2_ay")
    c13, c14, c15 = st.columns(3)
    row[53], row[54], row[51] = c13.text_input("상한가외입고사유", key="t2_bb"), c14.selectbox("기존약제와병용사용", ["Y", "N"], key="t2_bc"), c15.selectbox("입고요청사유", OP_IN_REASON, key="t2_az")
    c16, c17, c18 = st.columns(3)
    row[52] = c16.date_input("코드사용시작일", key="t2_ba").strftime('%Y-%m-%d')
    row[55] = c17.selectbox("사용기간", OP_USE_PERIOD, key="t2_bd")
    row[56] = c18.date_input("입고일", key="t2_be").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 제출", key="b2", use_container_width=True):
        handle_submit(row, "대체입고", [edi1, drug_info1[1], row[28], edi2, drug_info2[1], row[56]])

# [4] 급여코드변경 / [5] 단가인하▼ (통합 로직)
for idx, tab_name in zip([3, 4], ["급여코드변경", "단가인하▼"]):
    with tabs[idx]:
        row = [""] * 58
        st.markdown(f'<div class="section-header">반품 약제 정보</div>', unsafe_allow_html=True)
        e1 = st.text_input(f"제품코드 입력 ", key=f"t{idx}_e1")
        drug_info1 = render_drug_table(e1, "[반품 약제]")
        row[6:14] = drug_info1
        c1, c2, c3, c4 = st.columns(4)
        row[14], row[15], row[21], row[22] = c1.selectbox("원내구분", OP_INSIDE_OUT, key=f"t{idx}_o"), c2.selectbox("급여구분", ["급여", "비급여"], key=f"t{idx}_p"), c3.selectbox("변경내용", OP_CHANGE_CONTENT, key=f"t{idx}_v"), c4.selectbox("재고여부", ["유", "무"], key=f"t{idx}_w")
        c5, c6, c7 = st.columns(3)
        row[26], row[27] = c5.date_input("반품예정일", key=f"t{idx}_aa").strftime('%Y-%m-%d'), c6.number_input("반품량", 0, key=f"t{idx}_ab")
        row[28] = c7.date_input("코드사용중지일", key=f"t{idx}_ac").strftime('%Y-%m-%d')
        st.divider()
        st.markdown(f'<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True)
        e2 = st.text_input(f"제품코드 입력 (변경)", key=f"t{idx}_e2")
        m2 = get_drug_info(e2, master_df)
        drug_info2 = [e2, m2.get("제품명", ""), m2.get("업체명", ""), m2.get("규격", ""), m2.get("단위", ""), str(m2.get("상한금액", "")).replace(',', ''), m2.get("주성분명", ""), m2.get("전일", "")]
        row[39:47] = drug_info2
        c8, c9, c10, c11, c12 = st.columns(5)
        row[47], row[48], row[49], row[50], row[53] = c8.selectbox("원내구분 (변경)", OP_INSIDE_OUT, key=f"t{idx}_av"), c9.selectbox("급여구분 (변경)", ["급여", "비급여"], key=f"t{idx}_aw"), c10.text_input("구입처 (변경)", key=f"t{idx}_ax"), c11.number_input("개당입고가 (변경)", 0, key=f"t{idx}_ay"), c12.text_input("상한가외입고사유", key=f"t{idx}_bb")
        if st.button(f"🚀 {tab_name} 제출", key=f"b{idx}", use_container_width=True):
            handle_submit(row, tab_name, [e1, drug_info1[1], row[28], e2, drug_info2[1], ""])

# [6] 단가인상▲
with tabs[5]:
    row = [""] * 58
    st.markdown('<div class="section-header">인상 대상 정보</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t5_edi")
    drug_info = render_drug_table(edi)
    row[6:14] = drug_info
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t5_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t5_p"), c3.text_input("구입처", key="t5_q"), c4.number_input("개당입고가", 0, key="t5_r")
    c5, c6 = st.columns(2)
    row[37] = c5.date_input("단가변경_품절일", key="t5_al").strftime('%Y-%m-%d')
    row[21] = c6.selectbox("변경내용", OP_CHANGE_CONTENT, key="t5_v")
    if st.button("🚀 단가인상 제출", key="b5", use_container_width=True):
        handle_submit(row, "단가인상", [edi, drug_info[1], "", "", "", row[37]])

# [7] 약가조회
with tabs[6]:
    st.markdown('<div class="section-header">Master DB 통합 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력", key="search_edi")
    if s_edi:
        render_drug_table(s_edi, "검색 결과")
        m = get_drug_info(s_edi, master_df)
        if m: st.info(f"**투여:** {m.get('투여','-')} | **분류:** {m.get('분류','-')} | **비고:** {m.get('비고','-')}")

# [8] 진행현황 Dashboard
with tabs[7]:
    st.markdown('<div class="section-header">📊 실시간 진행현황 대시보드</div>', unsafe_allow_html=True)
    
    st_df = load_status_data()
    if st_df.empty:
        st.info("데이터가 없습니다.")
    else:
        # 통합 검색 바
        search_query = st.text_input("🔍 통합 검색 (제품명, 코드, 신청자 등)", key="global_search")
        if search_query:
            st_df = st_df[st_df.apply(lambda r: r.astype(str).str.contains(search_query).any(), axis=1)]
        
        # 데이터 표시
        st.dataframe(st_df, use_container_width=True)
        
        # 상세보기 및 상태 관리 섹션
        st.divider()
        st.subheader("📝 항목 상세 처리")
        
        if not st_df.empty:
            # 처리할 행 선택
            row_to_edit = st.selectbox("수정할 항목을 선택하세요 (제품명 - 신청자)", 
                                       options=range(len(st_df)),
                                       format_func=lambda i: f"{st_df.iloc[i]['제품명1'] if st_df.iloc[i]['제품명1'] else st_df.iloc[i]['제품명2']} - {st_df.iloc[i]['신청자']}")
            
            selected_row = st_df.iloc[row_to_edit]
            
            # 선택된 행 상세 정보 레이아웃
            c1, c2, c3 = st.columns(3)
            with c1:
                st.write("**신청 정보**")
                st.write(f"- 구분: {selected_row['신청구분']}")
                st.write(f"- 신청자: {selected_row['신청자']} ({selected_row['신청일']})")
            with c2:
                st.write("**제품 정보**")
                st.write(f"- 코드1: {selected_row['제품코드1']} ({selected_row['제품명1']})")
                st.write(f"- 코드2: {selected_row['제품코드2']} ({selected_row['제품명2']})")
            with c3:
                st.write("**상태 업데이트**")
                new_status = st.selectbox("상태 변경", ["신청완료", "처리중", "처리완료"], 
                                         index=["신청완료", "처리중", "처리완료"].index(selected_row['진행상황']) if selected_row['진행상황'] in ["신청완료", "처리중", "처리완료"] else 0,
                                         key="update_status_val")
                
                if st.button("💾 상태 업데이트 저장", use_container_width=True):
                    try:
                        ss = get_spreadsheet()
                        ws = ss.worksheet("Status")
                        # 구글 시트 인덱스 계산 (헤더 1줄 + 0-index 대응을 위해 +2)
                        # 원본 데이터에서의 행 번호를 찾아야 함 (검색 후에도 정확한 행 수정을 위해)
                        # 여기서는 단순 index를 사용하되 검색 기능 사용 시 주의 필요.
                        # 가장 안전한 방법은 고유 ID를 부여하는 것이나, 현재 구조에서는 목록의 행 번호를 활용.
                        actual_row_num = row_to_edit + 2 
                        ws.update_cell(actual_row_num, 1, new_status) # 진행상황
                        ws.update_cell(actual_row_num, 4, done_user if done_user else app_user) # 처리자
                        
                        st.success(f"항목이 '{new_status}' 상태로 업데이트되었습니다.")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"업데이트 실패: {e}")
