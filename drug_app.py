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
    
    /* 탭 디자인 */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 45px; min-width: 100px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 8px 8px 0 0 !important;
        font-size: 0.9rem; font-weight: 700; color: #64748b;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: #ffffff !important; }
    
    /* 배경색 지정 클래스 */
    .status-container { background-color: #f0f7ff; padding: 20px; border-radius: 12px; border: 1px solid #dbeafe; }
    .search-container { background-color: #fffef0; padding: 20px; border-radius: 12px; border: 1px solid #fef3c7; }

    /* 제품코드 입력창 오렌지색 강조 */
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important;
        color: #000000 !important;
    }

    /* 약제 정보 테이블 디자인 */
    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 10px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
    
    .detail-view { background-color: #ffffff; border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; margin-top: 10px; }
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
        # 제품코드를 확실하게 텍스트(문자열)로 변환
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=2)
def load_db_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("New_stop").get_all_records())
        # 모든 코드성 컬럼은 문자열로 변환 (콤마 방지)
        for col in df.columns:
            if "코드" in str(col):
                df[col] = df[col].astype(str).str.replace(".0", "", regex=False)
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]
OP_STATUS = ["신청완료", "처리중", "처리완료"]
OP_PROCESSORS = ["한승주 팀장", "이소영 대리", "변혜진 주임"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("오늘 날짜", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.info("신청 시 성명을 정확히 입력하세요.")

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
        row_data[0], row_data[1], row_data[2], row_data[5] = category, app_date, app_user, "신청완료"
        ws.append_row(row_data)
        st.success("성공적으로 접수되었습니다!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 실패: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["📊 진행현황", "사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "🔍 약가조회"]
tabs = st.tabs(tab_names)

# [Tab 0] 진행현황 (연한 파랑 배경)
with tabs[0]:
    st.markdown('<div class="status-container">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">📊 실시간 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    
    raw_df = load_db_data()
    if raw_df.empty:
        st.info("데이터가 없습니다.")
    else:
        cols = raw_df.columns
        edit_df = raw_df.copy()
        edit_df.insert(0, "상세조회", False)
        
        search_bar = st.text_input("🔍 통합 검색 (제품명, 코드, 신청자 등)", key="global_search_bar")
        if search_bar:
            edit_df = edit_df[edit_df.apply(lambda r: r.astype(str).str.contains(search_bar).any(), axis=1)]

        # 데이터 에디터 설정 (코드: 텍스트 / 금액&수량: 숫자 포맷팅)
        edited_df = st.data_editor(
            edit_df.iloc[::-1],
            column_config={
                "상세조회": st.column_config.CheckboxColumn("조회"),
                cols[5]: st.column_config.SelectboxColumn("진행상황", options=OP_STATUS),
                cols[4]: st.column_config.SelectboxColumn("완료자", options=OP_PROCESSORS),
                cols[6]: st.column_config.TextColumn("제품코드1"), # 텍스트 형식 강제
                cols[39]: st.column_config.TextColumn("제품코드2"), # 텍스트 형식 강제
                cols[17]: st.column_config.NumberColumn("입고가", format="%d"),
                cols[24]: st.column_config.NumberColumn("재고량", format="%d"),
                cols[27]: st.column_config.NumberColumn("반품량", format="%d"),
                cols[0]: st.column_config.TextColumn("구분", disabled=True),
                cols[1]: st.column_config.TextColumn("신청일", disabled=True),
                cols[7]: st.column_config.TextColumn("제품명1", disabled=True),
            },
            hide_index=True,
            use_container_width=True,
            height=400
        )

        if st.button("💾 변경사항 최종 반영하기", use_container_width=True):
            try:
                ss = get_spreadsheet()
                ws = ss.worksheet("New_stop")
                for idx, row in edited_df.iterrows():
                    real_row_idx = row.name + 2 
                    ws.update_cell(real_row_idx, 6, row[cols[5]]) # 상태
                    ws.update_cell(real_row_idx, 5, row[cols[4]]) # 완료자
                    if row[cols[5]] == "처리완료":
                         ws.update_cell(real_row_idx, 4, datetime.now().strftime('%Y-%m-%d'))
                st.success("반영 완료!"); st.cache_data.clear(); st.rerun()
            except Exception as e: st.error(f"저장 실패: {e}")

        # 상세조회 섹션
        selected = edited_df[edited_df["상세조회"] == True]
        if not selected.empty:
            for _, sel in selected.iterrows():
                st.markdown('<div class="detail-view">', unsafe_allow_html=True)
                st.write(f"**[{sel[cols[0]]}] 상세 정보** | 신청자: {sel[cols[2]]} ({sel[cols[1]]})")
                c1, c2 = st.columns(2)
                c1.write(f"대상: {sel[cols[6]]} - {sel[cols[7]]} | {sel[cols[9]]}")
                c2.write(f"신규: {sel[cols[39]]} - {sel[cols[40]]} | {sel[cols[42]]}")
                st.write(f"**비고:** {sel[cols[38]]}")
                st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# [Tab 1-6] 신청 양식 (기본 흰색 배경 유지)
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
    st.markdown('<div class="section-header">기존/대체 약제 입력</div>', unsafe_allow_html=True)
    e1 = st.text_input("기존 제품코드", key="t3_e1"); row[6:14] = render_drug_table(e1)
    e2 = st.text_input("대체 제품코드", key="t3_e2"); row[39:47] = render_drug_table(e2, "대체 약제")
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

# [Tab 7] 약가조회 (연한 오렌지 배경)
with tabs[7]:
    st.markdown('<div class="search-container">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">🔍 Master DB 통합 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (콤마 없이 숫자만)", key="search_edi")
    if s_edi:
        render_drug_table(s_edi, "검색 결과")
        m = get_drug_info(s_edi, master_df)
        if m: st.info(f"투여: {m.get('투여','-')} | 분류: {m.get('분류','-')} | 비고: {m.get('비고','-')}")
    st.markdown('</div>', unsafe_allow_html=True)
