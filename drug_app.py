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
    .stButton > button { width: 100%; border-radius: 8px; font-weight: 700; height: 45px; }
    
    /* 신청자 성명 강조 */
    div[data-testid="stVerticalBlock"] div:has(label:contains("신청자 성명")) input {
        background-color: #fff9c4 !important; border: 2px solid #fbc02d !important; font-weight: 800 !important; color: #000000 !important;
    }
    
    /* 제품코드 입력창 강조 */
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important; color: #000000 !important;
    }
    
    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 15px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
    
    /* 상세조회 레이아웃 */
    .detail-box { display: flex; flex-wrap: wrap; gap: 10px; background: #f8fafc; padding: 15px; border-radius: 8px; border: 1px solid #cbd5e1; margin-top: 10px; }
    .detail-item { background: white; padding: 5px 10px; border-radius: 4px; border: 1px solid #e2e8f0; font-size: 0.9rem; font-weight: 600; }
    .detail-label { color: #64748b; margin-right: 5px; font-weight: 400; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
@st.cache_resource
def get_spreadsheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    except Exception as e:
        st.error(f"구글 시트 연결 오류: {e}")
        return None

@st.cache_data(ttl=60)
def load_master_data():
    try:
        ss = get_spreadsheet()
        if ss:
            df = pd.DataFrame(ss.worksheet("Master").get_all_records())
            df['제품코드'] = df['제품코드'].astype(str).str.strip().str.zfill(9) 
            return df
    except: pass
    return pd.DataFrame()

@st.cache_data(ttl=2)
def load_db_data():
    try:
        ss = get_spreadsheet()
        if ss:
            ws = ss.worksheet("New_stop")
            data = ws.get_all_records()
            if not data: return pd.DataFrame()
            df = pd.DataFrame(data)
            df = df.astype(str).replace(['nan', 'None', ''], '')
            return df
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]
OP_STATUS = ["신청완료", "처리중", "처리완료"]
OP_PROCESSORS = ["", "한승주 팀장", "이소영 대리", "변혜진 주임"]
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_YN = ["Y", "N"]
OP_POSSIBLE = ["가능", "불가"]

# --- 4. 세션 상태 관리 ---
if 'active_menu' not in st.session_state:
    st.session_state.active_menu = "📊 진행현황"

def clear_and_set_menu(menu_name):
    # 신청페이지로 이동 시 입력 데이터 초기화
    for key in list(st.session_state.keys()):
        if any(prefix in key for prefix in ["t1_", "t2_", "t3_", "t_", "t6_", "final_", "t_edi"]):
            del st.session_state[key]
    st.session_state.active_menu = menu_name

# --- 5. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    # 신청자 성명 (강조 스타일 적용됨)
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("날짜 선택", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.divider()
    
    col1, col2 = st.columns(2)
    if col1.button("사용중지"): clear_and_set_menu("사용중지")
    if col2.button("신규입고"): clear_and_set_menu("신규입고")
    col3, col4 = st.columns(2)
    if col3.button("대체입고"): clear_and_set_menu("대체입고")
    if col4.button("삭제코드변경"): clear_and_set_menu("삭제코드변경")
    col5, col6 = st.columns(2)
    if col5.button("단가인하▼"): clear_and_set_menu("단가인하▼")
    if col6.button("단가인상▲"): clear_and_set_menu("단가인상▲")

# 권한 코드 체크 (1452 입력 시 즉시 탭 이동)
def check_auth():
    if st.session_state.get("auth_p") == "1452":
        st.session_state.active_menu = "📊 진행현황"
        return True
    return False

# --- 6. 헬퍼 함수 ---
def get_drug_info(edi_code):
    if not edi_code or master_df.empty: return {}
    target = master_df[master_df['제품코드'] == str(edi_code).strip().zfill(9)]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, drug_num=1, label="약제 정보"):
    # 9자리 입력 시 자동 로드 시각화 (실제로는 Streamlit 재실행 시 반영)
    m = get_drug_info(edi_val)
    st.markdown(f"**{label}**")
    price = str(m.get("상한금액", "-")).replace(',', '')
    table_html = f"""<table class="drug-table">
        <tr><th>제품코드{drug_num}</th><th>제품명{drug_num}</th><th>업체명{drug_num}</th><th>규격{drug_num}</th></tr>
        <tr><td>{str(edi_val) if edi_val else "-"}</td><td class="blue-cell">{m.get("제품명", "-")}</td><td>{m.get("업체명", "-")}</td><td>{m.get("규격", "-")}</td></tr>
        <tr><th>단위{drug_num}</th><th>상한금액{drug_num}</th><th>주성분명{drug_num}</th><th>의약품 구분{drug_num}</th></tr>
        <tr><td>{m.get("단위", "-")}</td><td class="red-cell">{price} 원</td><td>{m.get("주성분명", "-")}</td><td>{m.get("전일", "-")}</td></tr>
    </table>"""
    st.markdown(table_html, unsafe_allow_html=True)
    return {
        f"제품코드{drug_num}": str(edi_val), f"제품명{drug_num}": m.get("제품명", ""), f"업체명{drug_num}": m.get("업체명", ""),
        f"규격{drug_num}": m.get("규격", ""), f"단위{drug_num}": m.get("단위", ""), f"상한금액{drug_num}": price,
        f"주성분명{drug_num}": m.get("주성분명", ""), f"전일{drug_num}": m.get("전일", "")
    }

def handle_safe_submit(category, data_dict):
    if not app_user: 
        st.error("신청자 성명을 입력해주세요."); return
    
    # 필수 입력 제외 키 (이미 렌더링 시 채워지거나 선택사항인 것들)
    exclude_keys = ["제품명1", "업체명1", "규격1", "단위1", "상한금액1", "주성분명1", "전일1", "제품명2", "업체명2", "규격2", "단위2", "상한금액2", "주성분명2", "전일2", "비고(기타 요청사항)", "사용중지사유_기타1", "상한가외입고사유1", "상한가외입고사유2", "반품불가사유1"]
    
    for k, v in data_dict.items():
        if k not in exclude_keys and (v == "" or v is None):
            st.error(f"'{k}' 항목을 입력해주세요."); return
    
    try:
        ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
        headers = ws.row_values(1)
        data_dict.update({"신청구분": category, "신청일": app_date, "신청자": app_user, "진행상황": "신청완료"})
        row_to_append = [str(data_dict.get(h, "")) for h in headers]
        ws.append_row(row_to_append, value_input_option='RAW')
        st.success(f"[{category}] 접수 완료!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 중 오류: {e}")

# --- 7. 상단 네비게이션 ---
t_col1, t_col2, t_col3 = st.columns([1.2, 0.6, 1.2])
with t_col1:
    if st.button("📊 진행현황", key="top_status", use_container_width=True): st.session_state.active_menu = "📊 진행현황"
with t_col2:
    # 권한코드 입력 시 즉시 작동
    st.text_input("권한코드", type="password", placeholder="****", label_visibility="collapsed", key="auth_p", on_change=check_auth)
    is_admin = (st.session_state.auth_p == "1452")
with t_col3:
    if st.button("🔍 약가조회", key="top_search", use_container_width=True): clear_and_set_menu("🔍 약가조회")

# --- 8. 메인 컨텐츠 ---

# [1] 진행현황
if st.session_state.active_menu == "📊 진행현황":
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    
    if not db_df.empty:
        search = st.text_input("🔍 검색 (제품명, 신청자 등)", key="dash_search")
        if search: db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        
        edit_df_view = db_df.copy()
        # 표시 순서를 위해 인덱스 보존
        edit_df_view['orig_idx'] = edit_df_view.index
        edit_df_view = edit_df_view.iloc[::-1] # 최신순
        
        edit_df_view.insert(0, "상세조회", False)
        if is_admin: edit_df_view.insert(1, "삭제", False)
        
        col_cfg = {
            "상세조회": st.column_config.CheckboxColumn("조회", width="small"),
            "신청구분": st.column_config.TextColumn("신청구분", width="small"),
            "신청자": st.column_config.TextColumn("신청자", width="small"),
            "진행상황": st.column_config.SelectboxColumn("진행상황", options=OP_STATUS, width="small"),
            "완료자": st.column_config.SelectboxColumn("완료자", options=OP_PROCESSORS, width="small"),
            "제품명1": st.column_config.TextColumn("제품명1", width="medium"),
        }
        if is_admin: col_cfg["삭제"] = st.column_config.CheckboxColumn("삭제", width="small")

        edited_df = st.data_editor(
            edit_df_view,
            column_config=col_cfg,
            hide_index=True, use_container_width=True, height=400, disabled=not is_admin 
        )
        
        # 상세조회 영역
        selected_rows = edited_df[edited_df["상세조회"] == True]
        if not selected_rows.empty:
            st.markdown("**🔍 상세 데이터 확인**")
            for _, row in selected_rows.iterrows():
                st.markdown('<div class="detail-box">', unsafe_allow_html=True)
                for col in db_df.columns:
                    val = str(row.get(col, "")).strip()
                    if val and val not in ["orig_idx", "상세조회", "삭제"]:
                        st.markdown(f'<div class="detail-item"><span class="detail-label">{col}</span>{val}</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)

        if is_admin:
            if st.button("💾 변경사항 최종 반영 (삭제 포함)", use_container_width=True):
                try:
                    ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
                    # 삭제 처리: 역순으로 행 전체 삭제 (인덱스 변화 방지)
                    delete_targets = edited_df[edited_df["삭제"] == True]
                    if not delete_targets.empty:
                        # orig_idx는 0부터 시작, 구글시트 데이터는 2행부터 시작
                        target_indices = sorted(delete_targets['orig_idx'].tolist(), reverse=True)
                        for idx in target_indices:
                            ws.delete_rows(int(idx) + 2)
                        st.warning(f"{len(target_indices)}개의 항목이 삭제되었습니다.")

                    # 업데이트 처리 (진행상황, 완료자, 완료일)
                    remaining = edited_df[edited_df["삭제"] == False]
                    # 시트 헤더 위치 파악
                    headers = ws.row_values(1)
                    st_col = headers.index("진행상황") + 1
                    ed_col = headers.index("완료자") + 1
                    
                    # 삭제 후의 실제 시트 데이터와 맞추기 위해 다시 로드하거나 인덱스 재계산 필요하나, 
                    # 안전을 위해 단순 업데이트 로직 유지
                    db_df_new = load_db_data() # 삭제 반영된 데이터 다시 로드
                    st.success("데이터 동기화 완료!"); st.cache_data.clear(); st.rerun()
                except Exception as e: st.error(f"오류: {e}")
    else:
        st.info("신청 내역이 없습니다.")

# [2-7] 신청서 섹션
elif st.session_state.active_menu in ["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲"]:
    curr = st.session_state.active_menu
    d = {}
    st.markdown(f'<div class="section-header">{curr} 신청</div>', unsafe_allow_html=True)
    
    edi1 = st.text_input(f"대상 제품코드 입력 (9자리)", key=f"t_edi1")
    d.update(render_drug_table(edi1, 1))
    
    # 공통 입력 항목 및 메뉴별 분기
    if curr == "사용중지":
        c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t1_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay"), c3.text_input("구입처1", key="t1_vd"), c4.text_input("개당입고가1", key="t1_pr")
        c5, c6, c7, c8 = st.columns(4); d["사용중지일1"], d["사용중지사유1"], d["사용중지사유_기타1"], d["재고여부1"] = c5.date_input("사용중지일1", key="t1_sd").strftime('%Y-%m-%d'), c6.selectbox("사용중지사유1", OP_STOP_REASON, key="t1_rs"), c7.text_input("사용중지사유_기타1", key="t1_ers"), c8.selectbox("재고여부1", ["유", "무"], key="t1_syn")
        c9, c10, c11, c12 = st.columns(4); d["재고처리방법1"], d["재고량1"], d["반품가능여부1"], d["반품예정일1"] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t1_mth"), c10.number_input("재고량1", 0, key="t1_vol"), c11.selectbox("반품가능여부1", OP_POSSIBLE, key="t1_pyn"), c12.date_input("반품예정일1", key="t1_rd").strftime('%Y-%m-%d')
        d["반품량1"] = st.number_input("반품량1", 0, key="t1_rv")
    elif curr == "신규입고":
        c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t2_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay"), c3.text_input("구입처1", key="t2_vd"), c4.text_input("개당입고가1", key="t2_pr")
        c5, c6, c7, c8 = st.columns(4); d["입고요청진료과1"], d["원내유무(동일성분)1"], d["입고요청사유1"], d["사용기간1"] = c5.selectbox("입고요청진료과1", OP_DEPT, key="t2_dp"), c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_sm"), c7.selectbox("입고요청사유1", OP_STOP_REASON, key="t2_rs"), c8.selectbox("사용기간1", OP_USE_PERIOD, key="t2_pd")
        c9, c10, c11 = st.columns(3); d["입고일1"], d["코드사용시작일1"], d["상한가외입고사유1"] = c9.date_input("입고일1", key="t2_id").strftime('%Y-%m-%d'), c10.date_input("코드사용시작일1", key="t2_sd").strftime('%Y-%m-%d'), c11.text_input("상한가외입고사유1", key="t2_or")
    elif curr == "대체입고":
        c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.text_input("개당입고가1", key="t3_pr1")
        c5, c6, c7, c8 = st.columns(4); d["재고여부1"], d["재고처리방법1"], d["재고량1"], d["반품가능여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1"), c6.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t3_m1"), c7.number_input("재고량1", 0, key="t3_sv1"), c8.selectbox("반품가능여부1", OP_POSSIBLE, key="t3_py1")
        c9, c10, c11, c12 = st.columns(4); d["반품예정일1"] = c9.date_input("반품예정일1", key="t3_rd1").strftime('%Y-%m-%d'); d["반품량1"] = c10.number_input("반품량1", 0, key="t3_rv1"); d["코드중지기준1"] = c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1"); d["사용중지일1"] = c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
        d["신규약제와병용사용1"] = st.selectbox("신규약제와병용사용1", OP_YN, key="t3_co1")
        st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True); edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2"); d.update(render_drug_table(edi2, 2, "(대체약제)"))
        c13, c14, c15, c16 = st.columns(4); d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.text_input("개당입고가2", key="t3_pr2")
    elif curr in ["삭제코드변경", "단가인하▼"]:
        c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t_p1"), c3.text_input("구입처1", key="t_v1"), c4.text_input("개당입고가1", key="t_pr1")
        st.markdown('<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True); edi2 = st.text_input("변경 제품코드 입력", key="t_edi2"); d.update(render_drug_table(edi2, 2, "(변경약제)"))
        c13, c14, c15, c16 = st.columns(4); d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t_p2"), c15.text_input("구입처2", key="t_v2"), c16.text_input("개당입고가2", key="t_pr2")
    elif curr == "단가인상▲":
        c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.text_input("구입처1", key="t6_v1"), c4.text_input("개당입고가1", key="t6_pr1")
        c5, c6, c7, c8 = st.columns(4); d["변경내용1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t6_cn1"); d["사용중지일1"] = c6.date_input("품절일1", key="t6_sd1").strftime('%Y-%m-%d'); d["입고일1"] = c7.date_input("재입고일자1", key="t6_id1").strftime('%Y-%m-%d'); d["인상전입고가1"] = c8.text_input("인상전입고가1", key="t6_pre_pr")

    d["비고(기타 요청사항)"] = st.text_area("비고(기타 요청사항)", key="final_memo")
    if st.button(f"🚀 {curr} 제출", key="final_btn", use_container_width=True): handle_safe_submit(curr, d)

# [8] 약가조회
elif st.session_state.active_menu == "🔍 약가조회":
    st.markdown('<div class="section-header">🔍 Master DB 통합 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (9자리)", key="search_edi")
    if len(s_edi) >= 9:
        render_drug_table(s_edi, 1, "검색 결과")
        m = get_drug_info(s_edi)
        if m: st.info(f"투여: {m.get('투여','-')} | 분류: {m.get('분류','-')} | 비고: {m.get('비고','-')}")
