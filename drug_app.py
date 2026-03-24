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
        height: 45px; min-width: 100px; background-color: #f8fafc; 
        border: 1px solid #e2e8f0 !important; border-radius: 8px 8px 0 0 !important;
        font-size: 0.9rem; font-weight: 700; color: #64748b;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: #ffffff !important; }
    .status-container { background-color: #f0f7ff; padding: 20px; border-radius: 12px; border: 1px solid #dbeafe; }
    .search-container { background-color: #fffef0; padding: 20px; border-radius: 12px; border: 1px solid #fef3c7; }
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important; color: #000000 !important;
    }
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
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=2)
def load_db_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("New_stop").get_all_records())
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]
OP_STATUS = ["신청완료", "처리중", "처리완료"]
OP_PROCESSORS = ["한승주 팀장", "이소영 대리", "변혜진 주임"]
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_YN = ["Y", "N"]
OP_POSSIBLE = ["가능", "불가"]

# --- 4. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("오늘 날짜", datetime.now(), key="global_date").strftime('%Y-%m-%d')

# --- 5. 헬퍼 함수 ---
def get_drug_info(edi_code):
    if not edi_code or master_df.empty: return {}
    target = master_df[master_df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, drug_num=1, label="약제 정보"):
    m = get_drug_info(edi_val)
    st.markdown(f"**{label}**")
    price = str(m.get("상한금액", "-")).replace(',', '')
    table_html = f"""<table class="drug-table">
        <tr><th>제품코드{drug_num}</th><th>제품명{drug_num}</th><th>업체명{drug_num}</th><th>규격{drug_num}</th></tr>
        <tr><td>{edi_val if edi_val else "-"}</td><td class="blue-cell">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위{drug_num}</th><th>상한금액{drug_num}</th><th>주성분명{drug_num}</th><th>의약품 구분{drug_num}</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td class="red-cell">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>"""
    st.markdown(table_html, unsafe_allow_html=True)
    return {
        f"제품코드{drug_num}": edi_val,
        f"제품명{drug_num}": m.get("제품명", ""),
        f"업체명{drug_num}": m.get("업체명", ""),
        f"규격{drug_num}": m.get("규격", ""),
        f"단위{drug_num}": m.get("단위", ""),
        f"상한금액{drug_num}": price,
        f"주성분명{drug_num}": m.get("주성분명", ""),
        f"의약품 구분{drug_num}": m.get("전일", "")
    }

def handle_safe_submit(category, data_dict):
    """헤더 이름을 기준으로 구글 시트에 안전하게 저장"""
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        ws = ss.worksheet("New_stop")
        headers = ws.row_values(1) # 첫 번째 줄 헤더 가져오기
        
        # 공통 정보 추가
        data_dict.update({
            "신청구분": category, "신청일": app_date, "신청자": app_user, "진행상황": "신청완료"
        })
        
        # 헤더 순서에 맞춰 리스트 생성
        row_to_append = [str(data_dict.get(h, "")) for h in headers]
        ws.append_row(row_to_append)
        
        st.success(f"[{category}] 접수 완료!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 중 오류 발생: {e}")

# --- 6. 메인 탭 구현 ---
tab_names = ["📊 진행현황", "사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하▼", "단가인상▲", "🔍 약가조회"]
tabs = st.tabs(tab_names)

# [Tab 0] 진행현황
with tabs[0]:
    st.markdown('<div class="status-container">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    if not db_df.empty:
        search = st.text_input("🔍 검색 (제품명, 신청자 등)", key="dash_search")
        if search: db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        
        # 에디터 설정
        edited_df = st.data_editor(
            db_df.iloc[::-1],
            column_config={
                "진행상황": st.column_config.SelectboxColumn("진행상황", options=OP_STATUS),
                "완료자": st.column_config.SelectboxColumn("완료자", options=OP_PROCESSORS),
                "제품코드1": st.column_config.TextColumn("제품코드1"),
                "제품코드2": st.column_config.TextColumn("제품코드2")
            },
            hide_index=True, use_container_width=True, height=400
        )
        if st.button("💾 변경사항 최종 반영하기", use_container_width=True):
            try:
                ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
                headers = ws.row_values(1)
                st_idx = headers.index("진행상황") + 1
                ed_idx = headers.index("완료자") + 1
                for idx, row in edited_df.iterrows():
                    real_idx = row.name + 2
                    ws.update_cell(real_idx, st_idx, row["진행상황"])
                    ws.update_cell(real_idx, ed_idx, row["완료자"])
                    if row["진행상황"] == "처리완료": 
                        ws.update_cell(real_idx, headers.index("완료일") + 1, datetime.now().strftime('%Y-%m-%d'))
                st.success("반영 완료!"); st.cache_data.clear(); st.rerun()
            except Exception as e: st.error(f"오류: {e}")
    st.markdown('</div>', unsafe_allow_html=True)

# [Tab 1] 사용중지 (그룹1: 약제1)
with tabs[1]:
    d = {}
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("제품코드 입력", key="t1_edi")
    d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4)
    d["원내구분1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t1_inout")
    d["급여구분1"] = c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay")
    d["구입처1"] = c3.text_input("구입처1", key="t1_vendor")
    d["개당입고가1"] = c4.number_input("개당입고가1", 0, key="t1_price")
    
    c5, c6, c7, c8 = st.columns(4)
    d["사용중지일1"] = c5.date_input("사용중지일1", key="t1_stopd").strftime('%Y-%m-%d')
    d["사용중지사유1"] = c6.selectbox("사용중지사유1", OP_STOP_REASON, key="t1_reason")
    d["사용중지사유_기타1"] = c7.text_input("사용중지사유_기타1", key="t1_etc_reason")
    d["재고여부1"] = c8.selectbox("재고여부1", ["유", "무"], key="t1_stock_yn")
    
    c9, c10, c11, c12 = st.columns(4)
    d["재고처리방법1"] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t1_method")
    d["재고량1"] = c10.number_input("재고량1", 0, key="t1_stock_vol")
    d["반품가능여부1"] = c11.selectbox("반품가능여부1", OP_POSSIBLE, key="t1_ret_yn")
    d["반품예정일1"] = c12.date_input("반품예정일1", key="t1_retd").strftime('%Y-%m-%d')
    d["반품량1"] = st.number_input("반품량1", 0, key="t1_ret_vol")
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t1_memo")
    if st.button("🚀 사용중지 제출", use_container_width=True): handle_safe_submit("사용중지", d)

# [Tab 2] 신규입고 (그룹1: 약제1)
with tabs[2]:
    d = {}
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("제품코드 입력", key="t2_edi")
    d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4)
    d["원내구분1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t2_inout")
    d["급여구분1"] = c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay")
    d["구입처1"] = c3.text_input("구입처1", key="t2_vendor")
    d["개당입고가1"] = c4.number_input("개당입고가1", 0, key="t2_price")
    
    c5, c6, c7, c8 = st.columns(4)
    d["입고요청진료과1"] = c5.selectbox("입고요청진료과1", OP_DEPT, key="t2_dept")
    d["원내유무(동일성분)1"] = c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_same_yn")
    d["입고요청사유1"] = c7.selectbox("입고요청사유1", OP_STOP_REASON, key="t2_reason")
    d["사용기간1"] = c8.selectbox("사용기간1", OP_USE_PERIOD, key="t2_period")
    
    c9, c10, c11 = st.columns(3)
    d["입고일1"] = c9.date_input("입고일1", key="t2_ind").strftime('%Y-%m-%d')
    d["코드사용시작일1"] = c10.date_input("코드사용시작일1", key="t2_startd").strftime('%Y-%m-%d')
    d["상한가외입고사유1"] = c11.text_input("상한가외입고사유1", key="t2_over_reason")
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t2_memo")
    if st.button("🚀 신규입고 제출", use_container_width=True): handle_safe_submit("신규입고", d)

# [Tab 3] 대체입고 (그룹2: 약제1 & 2)
with tabs[3]:
    d = {}
    st.markdown('<div class="section-header">기존 약제 정보</div>', unsafe_allow_html=True)
    edi1 = st.text_input("기존 제품코드 입력", key="t3_edi1")
    d.update(render_drug_table(edi1, 1, "(기존약제)"))
    c1, c2, c3, c4 = st.columns(4)
    d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.number_input("개당입고가1", 0, key="t3_pr1")
    c5, c6, c7, c8 = st.columns(4)
    d["재고여부1"], d["재고처리방법1"], d["재고량1"], d["반품가능여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1"), c6.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t3_m1"), c7.number_input("재고량1", 0, key="t3_sv1"), c8.selectbox("반품가능여부1", OP_POSSIBLE, key="t3_py1")
    c9, c10, c11, c12 = st.columns(4)
    d["반품예정일1"], d["반품량1"], d["코드중지기준1"], d["사용중지일1"] = c9.date_input("반품예정일1", key="t3_rd1").strftime('%Y-%m-%d'), c10.number_input("반품량1", 0, key="t3_rv1"), c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1"), c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
    d["신규약제와병용사용1"] = st.selectbox("신규약제와병용사용1", OP_YN, key="t3_co1")
    
    st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True)
    edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2")
    d.update(render_drug_table(edi2, 2, "(대체약제)"))
    c13, c14, c15, c16 = st.columns(4)
    d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.number_input("개당입고가2", 0, key="t3_pr2")
    c17, c18, c19, c20 = st.columns(4)
    d["입고요청사유2"], d["사용기간2"], d["입고일2"], d["코드사용시작일2"] = c17.selectbox("입고요청사유2", OP_STOP_REASON, key="t3_rs2"), c18.selectbox("사용기간2", OP_USE_PERIOD, key="t3_pd2"), c19.date_input("입고일2", key="t3_id2").strftime('%Y-%m-%d'), c20.date_input("코드사용시작일2", key="t3_ss2").strftime('%Y-%m-%d')
    d["기존약제와병용사용2"] = st.selectbox("기존약제와병용사용2", OP_YN, key="t3_co2")
    d["상한가외입고사유2"] = st.text_input("상한가외입고사유2", key="t3_ov2")
    d["비고(기타 요청사항)2"] = st.text_area("비고(기타 요청사항)2", key="t3_memo2")
    if st.button("🚀 대체입고 제출", use_container_width=True): handle_safe_submit("대체입고", d)

# [Tab 4/5] 급여코드변경 & 단가인하 (그룹2)
for idx, title in zip([4, 5], ["급여코드변경", "단가인하▼"]):
    with tabs[idx]:
        d = {}
        st.markdown(f'<div class="section-header">기존 약제 ({title})</div>', unsafe_allow_html=True)
        edi1 = st.text_input(f"{title} 대상 코드", key=f"t{idx}_edi1")
        d.update(render_drug_table(edi1, 1, "(기존약제)"))
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["변경내용1"], d["재고여부1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key=f"t{idx}_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key=f"t{idx}_p1"), c3.selectbox("변경내용1", OP_CHANGE_CONTENT, key=f"t{idx}_v1"), c4.selectbox("재고여부1", ["유", "무"], key=f"t{idx}_s1")
        c5, c6, c7, c8 = st.columns(4)
        d["재고처리방법1"], d["재고량1"], d["반품가능여부1"], d["반품예정일1"] = c5.selectbox("재고처리방법1", OP_STOCK_METHOD, key=f"t{idx}_m1"), c6.number_input("재고량1", 0, key=f"t{idx}_sv1"), c7.selectbox("반품가능여부1", OP_POSSIBLE, key=f"t{idx}_py1"), c8.date_input("반품예정일1", key=f"t{idx}_rd1").strftime('%Y-%m-%d')
        d["반품량1"] = st.number_input("반품량1", 0, key=f"t{idx}_rv1")
        
        st.markdown(f'<div class="section-header">변경 약제 ({title})</div>', unsafe_allow_html=True)
        edi2 = st.text_input(f"{title} 변경 코드", key=f"t{idx}_edi2")
        d.update(render_drug_table(edi2, 2, "(변경약제)"))
        c9, c10, c11 = st.columns(3)
        d["코드사용시작일2"] = c9.date_input("코드사용시작일2", key=f"t{idx}_ss2").strftime('%Y-%m-%d')
        d["상한가외입고사유2"] = c10.text_input("상한가외입고사유2", key=f"t{idx}_ov2")
        d["비고(기타 요청사항)2"] = st.text_area("비고(기타 요청사항)2", key=f"t{idx}_memo2")
        if st.button(f"🚀 {title} 제출", use_container_width=True): handle_safe_submit(title, d)

# [Tab 6] 단가인상 (그룹1)
with tabs[6]:
    d = {}
    st.markdown('<div class="section-header">단가인상 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("인상 대상 코드", key="t6_edi1")
    d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4)
    d["원내구분1"], d["급여구분1"], d["변경내용1"], d["사용중지일1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t6_v1"), c4.date_input("사용중지일1", key="t6_sd1").strftime('%Y-%m-%d')
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t6_memo")
    if st.button("🚀 단가인상 제출", use_container_width=True): handle_safe_submit("단가인상", d)

# [Tab 7] 약가조회
with tabs[7]:
    st.markdown('<div class="search-container">', unsafe_allow_html=True)
    st.markdown('<div class="section-header">🔍 Master DB 통합 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (숫자만)", key="search_edi")
    if s_edi:
        render_drug_table(s_edi, 1, "검색 결과")
        m = get_drug_info(s_edi)
        if m: st.info(f"투여: {m.get('투여','-')} | 분류: {m.get('분류','-')} | 비고: {m.get('비고','-')}")
    st.markdown('</div>', unsafe_allow_html=True)
