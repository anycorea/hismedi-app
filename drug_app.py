import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 (CSS 유지) ---
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
    
    /* 제품코드 입력창 오렌지색 강조 유지 */
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important;
        color: #000000 !important;
    }

    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 20px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
    
    .detail-view { background-color: #f8fafc; border: 1px solid #1E3A8A; border-radius: 8px; padding: 20px; margin-top: 10px; }
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

@st.cache_data(ttl=2) # 실시간 데이터 로드
def load_db_data():
    try:
        ss = get_spreadsheet()
        return pd.DataFrame(ss.worksheet("New_stop").get_all_records())
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]
OP_STATUS = ["신청완료", "처리중", "처리완료"]
# [요청사항 반영] 처리자(완료자) 리스트
OP_PROCESSORS = ["한승주 팀장", "이소영 대리", "변혜진 주임"]
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    # [요청사항 반영] 사용자 성명 -> 신청자 성명
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("오늘 날짜", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.info("신청 시 성명을 입력해 주세요.")

# --- 5. 헬퍼 함수 ---
def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, label_title="약제 정보"):
    m = get_drug_info(edi_val, master_df)
    st.markdown(f"**{label_title}**")
    price = str(m.get("상한금액", "-")).replace(',', '')
    table_html = f"""<table class="drug-table">
        <tr><th>제품코드</th><th>제품명</th><th>업체명</th><th>규격</th></tr>
        <tr><td>{edi_val if edi_val else "-"}</td><td class="blue-cell">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위</th><th>상한금액</th><th>주성분명</th><th>의약품 구분</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td class="red-cell">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>"""
    st.markdown(table_html, unsafe_allow_html=True)
    return [edi_val, m.get("제품명", ""), m.get("업체명", ""), m.get("규격", ""), m.get("단위", ""), price, m.get("주성분명", ""), m.get("전일", "")]

def handle_submit(row_data, category):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        # 컬럼 매칭: 0:구분, 1:신청일, 2:신청자, 5:상태
        row_data[0], row_data[1], row_data[2], row_data[5] = category, app_date, app_user, "신청완료"
        ws.append_row(row_data)
        st.success("성공적으로 접수되었습니다!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["📊 진행현황 대시보드", "사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "약가조회"]
tabs = st.tabs(tab_names)

# [Tab 0] 진행현황 Dashboard
with tabs[0]:
    st.markdown('<div class="section-header">📊 통합 신청 현황 및 실시간 처리</div>', unsafe_allow_html=True)
    
    raw_df = load_db_data()
    if raw_df.empty:
        st.info("데이터가 없습니다.")
    else:
        # 편집용 데이터프레임 구성
        cols = raw_df.columns
        edit_df = raw_df.copy()
        edit_df.insert(0, "상세조회", False) # 체크박스 추가
        
        # 통합 검색
        search_bar = st.text_input("🔍 검색 (제품명, 코드, 신청자, 상태 등 입력)", key="global_search_bar")
        if search_bar:
            edit_df = edit_df[edit_df.apply(lambda r: r.astype(str).str.contains(search_bar).any(), axis=1)]

        st.write("💡 **진행상황** 또는 **완료자**를 직접 수정한 후 하단 버튼을 눌러주세요.")
        
        # [요청사항 반영] st.data_editor 설정
        edited_df = st.data_editor(
            edit_df.iloc[::-1], # 최신순 표시
            column_config={
                "상세조회": st.column_config.CheckboxColumn("조회", default=False),
                cols[5]: st.column_config.SelectboxColumn("진행상황", options=OP_STATUS, required=True),
                # 완료자(처리자) 드롭다운 적용
                cols[4]: st.column_config.SelectboxColumn("완료자(처리자)", options=OP_PROCESSORS),
                cols[0]: st.column_config.TextColumn("구분", disabled=True),
                cols[1]: st.column_config.TextColumn("신청일", disabled=True),
                cols[2]: st.column_config.TextColumn("신청자", disabled=True),
                cols[7]: st.column_config.TextColumn("제품명1", disabled=True),
                cols[40]: st.column_config.TextColumn("제품명2", disabled=True),
            },
            hide_index=True,
            use_container_width=True,
            height=400,
            key="main_data_editor"
        )

        # 일괄 저장 기능
        if st.button("💾 변경사항 구글 시트에 최종 반영하기", use_container_width=True):
            try:
                ss = get_spreadsheet()
                ws = ss.worksheet("New_stop")
                # 편집된 데이터를 시트에 반영
                for idx, row in edited_df.iterrows():
                    real_row_idx = row.name + 2 # 헤더 보정
                    # 진행상황(6번컬럼), 완료자(5번컬럼) 업데이트
                    ws.update_cell(real_row_idx, 6, row[cols[5]]) 
                    ws.update_cell(real_row_idx, 5, row[cols[4]])
                    # 완료 처리 시 완료일자 자동 입력
                    if row[cols[5]] == "처리완료":
                         ws.update_cell(real_row_idx, 4, datetime.now().strftime('%Y-%m-%d'))
                st.success("시트에 성공적으로 반영되었습니다."); st.cache_data.clear(); st.rerun()
            except Exception as e: st.error(f"저장 실패: {e}")

        # 선택 항목 상세 보기
        selected_rows = edited_df[edited_df["상세조회"] == True]
        if not selected_rows.empty:
            st.markdown('<div class="section-header">🔍 선택 항목 상세 내역</div>', unsafe_allow_html=True)
            for _, sel in selected_rows.iterrows():
                with st.container():
                    st.markdown('<div class="detail-view">', unsafe_allow_html=True)
                    c1, c2, c3, c4 = st.columns(4)
                    c1.write(f"**구분:** {sel[cols[0]]}"); c2.write(f"**신청자:** {sel[cols[2]]}"); c3.write(f"**상태:** {sel[cols[5]]}"); c4.write(f"**신청일:** {sel[cols[1]]}")
                    st.divider()
                    s1, s2 = st.columns(2)
                    with s1:
                        st.write("**[대상 약제]**")
                        st.write(f"코드: {sel[cols[6]]} | 명칭: {sel[cols[7]]}")
                        st.write(f"규격: {sel[cols[9]]} | 중지일: {sel[cols[18]]}")
                    with s2:
                        st.write("**[신규/대체 약제]**")
                        st.write(f"코드: {sel[cols[39]]} | 명칭: {sel[cols[40]]}")
                        st.write(f"규격: {sel[cols[42]]} | 입고일: {sel[cols[56]]}")
                    st.divider()
                    st.write(f"**비고:** {sel[cols[38]]}")
                    st.markdown('</div>', unsafe_allow_html=True)

# [Tab 1-7] 신청 양식 로직 (기존 58개 컬럼 유지)
with tabs[1]: # 사용중지
    row = [""] * 58
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력", key="t1_edi")
    row[6:14] = render_drug_table(edi)
    c1, c2, c3, c4 = st.columns(4)
    row[14], row[15], row[16], row[17] = c1.selectbox("원내구분", OP_INSIDE_OUT, key="t1_o"), c2.selectbox("급여구분", ["급여", "비급여"], key="t1_p"), c3.text_input("구입처", key="t1_q"), c4.number_input("개당입고가", 0, key="t1_r")
    row[18] = st.date_input("사용중지일", key="t1_s").strftime('%Y-%m-%d')
    row[38] = st.text_area("비고(기타)", key="t1_am")
    if st.button("🚀 사용중지 제출", key="b1", use_container_width=True): handle_submit(row, "사용중지")

with tabs[2]: # 신규입고
    row = [""] * 58
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi = st.text_input("제품코드 입력 ", key="t2_edi")
    row[6:14] = render_drug_table(edi)
    row[34] = st.date_input("입고일", key="t2_ai").strftime('%Y-%m-%d')
    if st.button("🚀 신규입고 제출", key="b2", use_container_width=True): handle_submit(row, "신규입고")

with tabs[3]: # 대체입고
    row = [""] * 58
    st.markdown('<div class="section-header">기존 약제</div>', unsafe_allow_html=True)
    edi1 = st.text_input("기존 제품코드", key="t3_e1")
    row[6:14] = render_drug_table(edi1)
    row[28] = st.date_input("중지일", key="t3_ac").strftime('%Y-%m-%d')
    st.divider()
    st.markdown('<div class="section-header">대체 약제</div>', unsafe_allow_html=True)
    edi2 = st.text_input("대체 제품코드", key="t3_e2")
    row[39:47] = render_drug_table(edi2, "대체 약제")
    row[56] = st.date_input("입고일", key="t3_be").strftime('%Y-%m-%d')
    if st.button("🚀 대체입고 제출", key="b3", use_container_width=True): handle_submit(row, "대체입고")

for idx, tab_name in zip([4, 5], ["급여코드변경", "단가인하▼"]):
    with tabs[idx]:
        row = [""] * 58
        e1 = st.text_input(f"현재 제품코드", key=f"t{idx}_e1"); row[6:14] = render_drug_table(e1)
        e2 = st.text_input(f"새 제품코드", key=f"t{idx}_e2"); row[39:47] = render_drug_table(e2, "변경 약제")
        if st.button(f"🚀 {tab_name} 제출", key=f"b{idx}", use_container_width=True): handle_submit(row, tab_name)

with tabs[6]: # 단가인상
    row = [""] * 58
    edi = st.text_input("인상 대상 코드", key="t6_edi"); row[6:14] = render_drug_table(edi)
    if st.button("🚀 단가인상 제출", key="b6", use_container_width=True): handle_submit(row, "단가인상")

with tabs[7]: # 약가조회
    s_edi = st.text_input("조회할 제품코드", key="search_edi")
    if s_edi: render_drug_table(s_edi, "검색 결과")
