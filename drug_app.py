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
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important; color: #000000 !important;
    }
    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 15px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
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
        # 사용자 원복 로직 적용: 문자열 변환 및 9자리 유지 (앞자리 0 보존)
        df['제품코드'] = df['제품코드'].astype(str).str.strip().str.zfill(9) 
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=2)
def load_db_data():
    try:
        ss = get_spreadsheet()
        df = pd.DataFrame(ss.worksheet("New_stop").get_all_records())
        # 모든 데이터를 문자열로 처리하여 '0'으로 시작하는 코드 보존
        df = df.astype(str).replace(['nan', 'None', 'None', ''], '')
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

# --- 5. 사이드바 (왼쪽 메뉴 - 2x3 배치) ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("날짜 선택", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.divider()
    
    col1, col2 = st.columns(2)
    if col1.button("사용중지"): st.session_state.active_menu = "사용중지"
    if col2.button("신규입고"): st.session_state.active_menu = "신규입고"
    col3, col4 = st.columns(2)
    if col3.button("대체입고"): st.session_state.active_menu = "대체입고"
    if col4.button("삭제코드변경"): st.session_state.active_menu = "삭제코드변경"
    col5, col6 = st.columns(2)
    if col5.button("단가인하▼"): st.session_state.active_menu = "단가인하▼"
    if col6.button("단가인상▲"): st.session_state.active_menu = "단가인상▲"

# --- 6. 헬퍼 함수 ---
def get_drug_info(edi_code):
    if not edi_code or master_df.empty: return {}
    # 입력된 코드를 문자열로 취급하여 검색
    target = master_df[master_df['제품코드'] == str(edi_code).strip()]
    return target.iloc[0].to_dict() if not target.empty else {}

def render_drug_table(edi_val, drug_num=1, label="약제 정보"):
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
        f"주성분명{drug_num}": m.get("주성분명", ""), f"의약품 구분{drug_num}": m.get("전일", "")
    }

def handle_safe_submit(category, data_dict):
    if not app_user: 
        st.error("신청자 성명을 입력해주세요."); return
    
    # 필수 입력 예외 항목 (약제표 자동완성항목, 비고, 특정 기타항목)
    exclude_keys = [
        "제품명1", "업체명1", "규격1", "단위1", "상한금액1", "주성분명1", "의약품 구분1",
        "제품명2", "업체명2", "규격2", "단위2", "상한금액2", "주성분명2", "의약품 구분2",
        "비고(기타 요청사항)1", "비고(기타 요청사항)2",
        "사용중지사유_기타1", "상한가외입고사유1", "상한가외입고사유2"
    ]
    
    for k, v in data_dict.items():
        if k not in exclude_keys:
            if k == "반품불가사유1" and "반품불가사유1_skip" in st.session_state and st.session_state["반품불가사유1_skip"]:
                continue
            if v == "" or v is None:
                st.error(f"'{k}' 항목을 입력하거나 선택해주세요."); return
    
    try:
        ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
        headers = ws.row_values(1)
        data_dict.update({"신청구분": category, "신청일": app_date, "신청자": app_user, "진행상황": "신청완료"})
        # RAW 옵션으로 전송하여 0 누락 방지
        row_to_append = [str(data_dict.get(h, "")) for h in headers]
        ws.append_row(row_to_append, value_input_option='RAW')
        st.success(f"[{category}] 접수 완료!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 중 오류 발생: {e}")

# --- 7. 상단 네비게이션 (2개 메뉴 + 권한박스) ---
t_col1, t_col2, t_col3 = st.columns([1.2, 0.6, 1.2])
with t_col1:
    if st.button("📊 진행현황", key="top_status", use_container_width=True): st.session_state.active_menu = "📊 진행현황"
with t_col2:
    auth_code = st.text_input("권한코드", type="password", placeholder="****", label_visibility="collapsed", key="auth_p")
    is_admin = (auth_code == "1452")
with t_col3:
    if st.button("🔍 약가조회", key="top_search", use_container_width=True): st.session_state.active_menu = "🔍 약가조회"

st.divider()

# --- 8. 메인 컨텐츠 영역 ---

# [1] 진행현황 (삭제 기능 포함, 조회 체크박스 제거)
if st.session_state.active_menu == "📊 진행현황":
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    if not db_df.empty:
        search = st.text_input("🔍 검색 (제품명, 신청자 등)", key="dash_search")
        if search: db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        edit_df_view = db_df.copy()
        edit_df_view['완료일'] = pd.to_datetime(edit_df_view['완료일'], errors='coerce').dt.date
        
        # 관리자일 경우 삭제 체크박스 추가
        if is_admin:
            edit_df_view.insert(0, "삭제", False)
        
        col_cfg = {
            "진행상황": st.column_config.SelectboxColumn("진행상황", options=OP_STATUS, width="medium"),
            "완료자": st.column_config.SelectboxColumn("완료자", options=OP_PROCESSORS, width="medium"),
            "완료일": st.column_config.DateColumn("완료일", format="YYYY-MM-DD", default=datetime.now().date(), width="medium"),
            "제품코드1": st.column_config.TextColumn("제품코드1", disabled=True),
            "제품명1": st.column_config.TextColumn("제품명1", disabled=True)
        }
        if is_admin:
            col_cfg["삭제"] = st.column_config.CheckboxColumn("삭제", width="small")

        edited_df = st.data_editor(
            edit_df_view.iloc[::-1],
            column_config=col_cfg,
            hide_index=True, use_container_width=True, height=520, disabled=not is_admin 
        )
        
        if is_admin:
            if st.button("💾 변경사항 최종 반영하기 (삭제 포함)", use_container_width=True):
                try:
                    ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
                    headers = ws.row_values(1)
                    
                    rows_to_delete = edited_df[edited_df["삭제"] == True]
                    if not rows_to_delete.empty:
                        delete_indices = sorted(rows_to_delete.index.tolist(), reverse=True)
                        for idx in delete_indices:
                            ws.delete_rows(idx + 2)
                    
                    remaining_df = edited_df[edited_df["삭제"] == False]
                    st_col, ed_col, dt_col = headers.index("진행상황")+1, headers.index("완료자")+1, headers.index("완료일")+1
                    
                    for idx, row in remaining_df.iterrows():
                        real_idx = idx + 2 - len([i for i in delete_indices if i < idx])
                        ws.update_cell(real_idx, st_col, row["진행상황"])
                        ws.update_cell(real_idx, ed_col, row["완료자"])
                        if row["완료일"]: 
                            # 업데이트 시에도 텍스트 형식 유지를 위해 RAW 형태 고려 (update 메서드 권장)
                            ws.update(f"{gspread.utils.rowcol_to_a1(real_idx, dt_col)}", [[str(row['완료일'])]], value_input_option='RAW')

                    st.success("데이터가 성공적으로 동기화되었습니다."); st.cache_data.clear(); st.rerun()
                except Exception as e: st.error(f"오류: {e}")

# [2] 사용중지
elif st.session_state.active_menu == "사용중지":
    d = {}
    st.markdown('<div class="section-header">사용중지 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("대상 제품코드 입력 (텍스트)", key="t1_edi")
    d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t1_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay"), c3.text_input("구입처1", key="t1_vd"), c4.number_input("개당입고가1", 0, key="t1_pr")
    c5, c6, c7, c8 = st.columns(4); d["사용중지일1"], d["사용중지사유1"], d["사용중지사유_기타1"], d["재고여부1"] = c5.date_input("사용중지일1", key="t1_sd").strftime('%Y-%m-%d'), c6.selectbox("사용중지사유1", OP_STOP_REASON, key="t1_rs"), c7.text_input("사용중지사유_기타1", key="t1_ers"), c8.selectbox("재고여부1", ["유", "무"], key="t1_syn")
    c9, c10, c11, c12 = st.columns(4); d["재고처리방법1"], d["재고량1"], d["반품가능여부1"], d["반품예정일1"] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t1_mth"), c10.number_input("재고량1", 0, key="t1_vol"), c11.selectbox("반품가능여부1", OP_POSSIBLE, key="t1_pyn"), c12.date_input("반품예정일1", key="t1_rd").strftime('%Y-%m-%d')
    d["반품량1"] = st.number_input("반품량1", 0, key="t1_rv")
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t1_mm")
    if st.button("🚀 사용중지 제출", key="btn_t1", use_container_width=True): handle_safe_submit("사용중지", d)

# [3] 신규입고
elif st.session_state.active_menu == "신규입고":
    d = {}
    st.markdown('<div class="section-header">신규입고 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("대상 제품코드 입력 (텍스트)", key="t2_edi")
    d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t2_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay"), c3.text_input("구입처1", key="t2_vd"), c4.number_input("개당입고가1", 0, key="t2_pr")
    c5, c6, c7, c8 = st.columns(4); d["입고요청진료과1"], d["원내유무(동일성분)1"], d["입고요청사유1"], d["사용기간1"] = c5.selectbox("입고요청진료과1", OP_DEPT, key="t2_dp"), c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_sm"), c7.selectbox("입고요청사유1", OP_STOP_REASON, key="t2_rs"), c8.selectbox("사용기간1", OP_USE_PERIOD, key="t2_pd")
    c9, c10, c11 = st.columns(3); d["입고일1"], d["코드사용시작일1"], d["상한가외입고사유1"] = c9.date_input("입고일1", key="t2_id").strftime('%Y-%m-%d'), c10.date_input("코드사용시작일1", key="t2_sd").strftime('%Y-%m-%d'), c11.text_input("상한가외입고사유1", key="t2_or")
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t2_mm")
    if st.button("🚀 신규입고 제출", key="btn_t2", use_container_width=True): handle_safe_submit("신규입고", d)

# [4] 대체입고
elif st.session_state.active_menu == "대체입고":
    d = {}
    st.markdown('<div class="section-header">대체입고 신청 (기존 약제)</div>', unsafe_allow_html=True)
    edi1 = st.text_input("대상 제품코드 입력 (텍스트)", key="t3_edi1"); d.update(render_drug_table(edi1, 1, "(기존약제)"))
    c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.number_input("개당입고가1", 0, key="t3_pr1")
    c5, c6, c7, c8 = st.columns(4); d["재고여부1"], d["재고처리방법1"], d["재고량1"], d["반품가능여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1"), c6.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t3_m1"), c7.number_input("재고량1", 0, key="t3_sv1"), c8.selectbox("반품가능여부1", OP_POSSIBLE, key="t3_py1")
    c9, c10, c11, c12 = st.columns(4); d["반품/폐기 예정일1"] = c9.date_input("반품/폐기 예정일1", key="t3_rd1").strftime('%Y-%m-%d'); d["반품량1"] = c10.number_input("반품량1", 0, key="t3_rv1"); d["코드중지기준1"] = c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1"); d["사용중지일1"] = c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
    d["신규약제와병용사용1"] = st.selectbox("신규약제와병용사용1", OP_YN, key="t3_co1")
    
    st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True); edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2"); d.update(render_drug_table(edi2, 2, "(대체약제)"))
    c13, c14, c15, c16 = st.columns(4); d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.number_input("개당입고가2", 0, key="t3_pr2")
    c17, c18, c19, c20 = st.columns(4); d["입고요청사유2"], d["사용기간2"], d["입고일2"], d["코드사용시작일2"] = c17.selectbox("입고요청사유2", OP_STOP_REASON, key="t3_rs2"), c18.selectbox("사용기간2", OP_USE_PERIOD, key="t3_pd2"), c19.date_input("입고일2", key="t3_id2").strftime('%Y-%m-%d'), c20.date_input("코드사용시작일2", key="t3_ss2").strftime('%Y-%m-%d')
    cs1, cs2 = st.columns(2); d["기존약제와병용사용2"], d["상한가외입고사유2"] = cs1.selectbox("기존약제와병용사용2", OP_YN, key="t3_co2"), cs2.text_input("상한가외입고사유2", key="t3_ov2")
    d["비고(기타 요청사항)2"] = st.text_area("비고(기타 요청사항)2", key="t3_mm2")
    if st.button("🚀 대체입고 제출", key="btn_t3", use_container_width=True): handle_safe_submit("대체입고", d)

# [5/6] 삭제코드변경 & 단가인하▼
elif st.session_state.active_menu in ["삭제코드변경", "단가인하▼"]:
    curr = st.session_state.active_menu
    d = {}
    st.markdown(f'<div class="section-header">{curr} 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("대상 제품코드 입력 (텍스트)", key="t_edi1"); d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t_p1"), c3.text_input("구입처1", key="t_v1"), c4.number_input("개당입고가1", 0, key="t_pr1")
    c5, c6, c7, c8 = st.columns(4); d["변경내용1"], d["재고여부1"], d["재고처리방법1"], d["재고량1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t_cn1"), c6.selectbox("재고여부1", ["유", "무"], key="t_s1"), c7.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t_m1"), c8.number_input("재고량1", 0, key="t_sv1")
    c9, c10, c11, c12 = st.columns(4); d["반품가능여부1"] = c9.selectbox("반품가능여부1", OP_POSSIBLE, key="t_py1")
    if d["반품가능여부1"] == "불가": d["반품불가사유1"] = c10.text_input("반품불가사유1 (필수)", key="t_nrs1"); st.session_state["반품불가사유1_skip"] = False
    else: d["반품불가사유1"] = c10.text_input("반품불가사유1 (비활성)", key="t_nrs1", disabled=True); st.session_state["반품불가사유1_skip"] = True
    d["반품예정일1"], d["반품량1"] = c11.date_input("반품예정일1", key="t_rd1").strftime('%Y-%m-%d'), c12.number_input("반품량1", 0, key="t_rv1")
    
    st.markdown('<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True); edi2 = st.text_input("변경 제품코드 입력", key="t_edi2"); d.update(render_drug_table(edi2, 2, "(변경약제)"))
    c13, c14, c15, c16 = st.columns(4); d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t_p2"), c15.text_input("구입처2", key="t_v2"), c16.number_input("개당입고가2", 0, key="t_pr2")
    c17, c18 = st.columns(2); d["코드사용시작일2"], d["상한가외입고사유2"] = c17.date_input("코드사용시작일2", key="t_ss2").strftime('%Y-%m-%d'), c18.text_input("상한가외입고사유2", key="t_ov2")
    d["비고(기타 요청사항)2"] = st.text_area("비고(기타 요청사항)2", key="t_mm2")
    if st.button(f"🚀 {curr} 제출", key="btn_t_sub", use_container_width=True): handle_safe_submit(curr, d)

# [7] 단가인상▲
elif st.session_state.active_menu == "단가인상▲":
    d = {}
    st.markdown('<div class="section-header">단가인상▲ 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input("대상 제품코드 입력 (텍스트)", key="t6_edi1"); d.update(render_drug_table(edi1, 1))
    c1, c2, c3, c4 = st.columns(4); d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.text_input("구입처1", key="t6_v1"), c4.number_input("개당입고가1", 0, key="t6_pr1")
    c5, c6, c7, c8 = st.columns(4); d["변경내용1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t6_cn1"); d["사용중지일1"] = c6.date_input("품절일1", key="t6_sd1").strftime('%Y-%m-%d'); d["입고일1"] = c7.date_input("재입고일자1", key="t6_id1").strftime('%Y-%m-%d'); d["인상전입고가1"] = c8.number_input("인상전입고가1", 0, key="t6_pre_pr")
    c9 = st.columns(1)[0]; d["코드사용시작일1"] = c9.date_input("코드사용시작일1", key="t6_ss1").strftime('%Y-%m-%d')
    d["비고(기타 요청사항)1"] = st.text_area("비고(기타 요청사항)1", key="t6_memo")
    if st.button("🚀 단가인상▲ 제출", key="btn_t6", use_container_width=True): handle_safe_submit("단가인상▲", d)

# [8] 약가조회
elif st.session_state.active_menu == "🔍 약가조회":
    st.markdown('<div class="section-header">🔍 Master DB 통합 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (숫자만)", key="search_edi")
    if s_edi:
        render_drug_table(s_edi, 1, "검색 결과")
        m = get_drug_info(s_edi)
        if m: st.info(f"투여: {m.get('투여','-')} | 분류: {m.get('분류','-')} | 비고: {m.get('비고','-')}")
