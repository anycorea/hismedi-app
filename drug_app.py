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
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: 5px; }
    
    .stButton > button { 
        width: 100%; border-radius: 12px; font-weight: 700; height: 48px; 
        transition: all 0.3s; border: 1px solid #e2e8f0; background-color: #ffffff;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stButton > button:hover { border-color: #1E3A8A; color: #1E3A8A; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }

    div[data-testid="column"] label {
        font-size: 0.75rem !important; font-weight: 800 !important; color: #ffffff !important;
        background-color: #1E3A8A; padding: 2px 10px !important; border-radius: 6px !important;
        margin-bottom: 6px !important; display: inline-block !important; letter-spacing: 0.5px;
    }
    div[data-testid="column"] [data-testid="stTextInput"] > div { border-radius: 12px !important; height: 48px !important; background-color: #f8fafc !important; border: 1px solid #e2e8f0 !important; }
    div[data-testid="column"] input { height: 48px !important; font-weight: 600 !important; font-size: 1rem !important; }
    div[data-testid="column"] { display: flex; flex-direction: column; justify-content: flex-end; }

    .drug-table { width: 100%; border-collapse: collapse; margin-bottom: 15px; border: 1px solid #e2e8f0; font-size: 0.85rem; }
    .drug-table th { background-color: #f1f5f9; color: #475569; font-weight: 700; padding: 6px; border: 1px solid #e2e8f0; text-align: center; }
    .drug-table td { background-color: #ffffff; color: #000000; font-weight: 600; padding: 8px; border: 1px solid #e2e8f0; text-align: center; }
    .blue-cell { background-color: #f0f7ff !important; color: #1E40AF !important; font-weight: 800 !important; }
    .red-cell { color: #dc2626 !important; font-weight: 800 !important; }
    .section-header { font-size: 1rem; font-weight: 800; color: #1E3A8A; margin: 15px 0 10px 0; padding-bottom: 5px; border-bottom: 2px solid #1E3A8A; }
    .detail-card { background-color: #f8fafc; border: 1px solid #e2e8f0; padding: 10px; border-radius: 6px; margin-bottom: 5px; min-height: 65px; }
    .detail-label { font-size: 0.75rem; color: #64748b; font-weight: 400; margin-bottom: 2px; }
    .detail-value { font-size: 0.9rem; color: #1e293b; font-weight: 700; word-break: break-all; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
@st.cache_resource
def get_spreadsheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/drive"])
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
            if "진행상황" in df.columns:
                def map_status(x):
                    if "신청완료" in x: return "🔴신청완료"
                    if "처리중" in x: return "🟡처리중"
                    if "처리완료" in x: return "🟢처리완료"
                    return x
                df["진행상황"] = df["진행상황"].apply(map_status)
            for col in df.columns:
                if "제품코드" in col:
                    df[col] = df[col].apply(lambda x: x.strip().zfill(9) if x and x.strip() and x != '0' else x)
            return df
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_INSIDE_OUT = ["원내", "원외", "원내/외"]
OP_STATUS = ["🔴신청완료", "🟡처리중", "🟢처리완료"]
OP_PROCESSORS = ["", "한승주 팀장", "이소영 대리", "변혜진 주임"]
OP_DEPT = ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"]
OP_STOP_REASON = ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"]
OP_CHANGE_CONTENT = ["급여코드 삭제", "상한가 인하", "상한가 인상"]
OP_STOCK_METHOD = ["재고 소진", "반품", "폐기"]
OP_USE_PERIOD = ["한시적 사용", "지속적 사용"]
OP_YN = ["Y", "N"]
OP_POSSIBLE = ["가능", "불가"]

# --- 4. 세션 상태 및 메뉴 관리 ---
if 'active_menu' not in st.session_state:
    st.session_state.active_menu = "📊 진행현황"

def clear_form_data():
    keys_to_reset = [k for k in st.session_state.keys() if k.startswith(('t1_', 't2_', 't3_', 't_', 't6_', 't_edi', 'final_', 'search_edi', 't_edi1'))]
    for key in keys_to_reset:
        del st.session_state[key]

def set_menu(menu_name):
    st.session_state.active_menu = menu_name
    clear_form_data()

# --- 5. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear(); st.rerun()
    st.divider()
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("날짜 선택", datetime.now(), key="global_date").strftime('%Y-%m-%d')
    st.divider()
    col1, col2 = st.columns(2)
    if col1.button("사용중지"): set_menu("사용중지")
    if col2.button("신규입고"): set_menu("신규입고")
    col3, col4 = st.columns(2)
    if col3.button("대체입고"): set_menu("대체입고")
    if col4.button("삭제코드변경"): set_menu("삭제코드변경")
    col5, col6 = st.columns(2)
    if col5.button("단가인하▼"): set_menu("단가인하▼")
    if col6.button("단가인상▲"): set_menu("단가인상▲")

# --- 6. 헬퍼 함수 ---
def get_drug_info(edi_code):
    if not edi_code or master_df.empty: return {}
    target = master_df[master_df['제품코드'] == str(edi_code).strip().zfill(9)]
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
    return {f"제품코드{drug_num}": str(edi_val), f"제품명{drug_num}": m.get("제품명", ""), f"업체명{drug_num}": m.get("업체명", ""), f"규격{drug_num}": m.get("규격", ""), f"단위{drug_num}": m.get("단위", ""), f"상한금액{drug_num}": price, f"주성분명{drug_num}": m.get("주성분명", ""), f"전일{drug_num}": m.get("전일", "")}

def handle_safe_submit(category, data_dict):
    if not app_user: st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
        headers = ws.row_values(1)
        data_dict.update({"신청구분": category, "신청일": app_date, "신청자": app_user, "진행상황": "신청완료"})
        row_to_append = [str(data_dict.get(h, "")) for h in headers]
        ws.append_row(row_to_append, value_input_option='RAW')
        st.success(f"[{category}] 접수 완료!"); st.balloons(); st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 중 오류 발생: {e}")

# --- 7. 상단 네비게이션 ---
t_col1, t_col2, t_col3, t_col4 = st.columns([1.2, 1.0, 1.0, 1.2])
with t_col1:
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("📊 진행현황", key="top_status", use_container_width=True): set_menu("📊 진행현황")
with t_col2: auth_req = st.text_input("신청부서 권한 🔒", type="password", placeholder="****", key="auth_req")
with t_col3: auth_admin = st.text_input("완료부서 권한 🔑", type="password", placeholder="****", key="auth_admin")
with t_col4:
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("🔍 약가조회", key="top_search", use_container_width=True): set_menu("🔍 약가조회")

is_requester = (auth_req == "7410")
is_admin = (auth_admin == "1452")

# --- 8. 메인 컨텐츠 영역 ---
if st.session_state.active_menu == "📊 진행현황":
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    if not db_df.empty:
        search = st.text_input("🔍 검색 (제품명, 신청자 등)", key="dash_search")
        if search: db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        
        edit_df_view = db_df.copy()
        edit_df_view['완료일'] = pd.to_datetime(edit_df_view['완료일'], errors='coerce').dt.date
        edit_df_view['신청일'] = pd.to_datetime(edit_df_view['신청일'], errors='coerce').dt.date
        edit_df_view['sheet_row'] = range(2, len(edit_df_view) + 2)
        edit_df_view = edit_df_view.iloc[::-1]

        if "진행상황" in edit_df_view.columns and "거래명세표" in edit_df_view.columns:
            all_cols = edit_df_view.columns.tolist()
            all_cols.remove("거래명세표")
            idx_status = all_cols.index("진행상황")
            all_cols.insert(idx_status + 1, "거래명세표")
            edit_df_view = edit_df_view[all_cols]

        # 기능 컬럼 강제 삽입
        edit_df_view.insert(0, "상세조회", False)
        if is_admin: edit_df_view.insert(1, "삭제", False)
        
        col_cfg = {
            "상세조회": st.column_config.CheckboxColumn("조회", width="small"),
            "진행상황": st.column_config.SelectboxColumn("진행상황", options=OP_STATUS, width="100", disabled=not is_admin),
            "완료자": st.column_config.SelectboxColumn("완료자", options=OP_PROCESSORS, width="100", disabled=not is_admin),
            "완료일": st.column_config.DateColumn("완료일", format="YYYY-MM-DD", width="small", disabled=not is_admin),
            "거래명세표": st.column_config.LinkColumn("거래명세표🔗", width="100", display_text="파일 열기", disabled=not is_requester),
            "신청구분": st.column_config.SelectboxColumn("신청구분", options=["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲"], width="100", disabled=not is_requester),
            "신청일": st.column_config.DateColumn("신청일", format="YYYY-MM-DD", width="small", disabled=not is_requester),
            "신청자": st.column_config.TextColumn("신청자", width="small", disabled=not is_requester),
            "제품코드1": st.column_config.TextColumn("제품코드1", width="small", disabled=not is_requester),
            "제품명1": st.column_config.TextColumn("제품명1", width="250", disabled=not is_requester),
            "sheet_row": None
        }
        if is_admin: col_cfg["삭제"] = st.column_config.CheckboxColumn("삭제", width="small")

        edited_df = st.data_editor(edit_df_view, column_config=col_cfg, hide_index=True, use_container_width=True, height=520, key="main_editor_v10")
        
        selected_rows = edited_df[edited_df["상세조회"] == True]
        if not selected_rows.empty:
            st.markdown('<div class="section-header">🔍 선택 항목 상세 정보</div>', unsafe_allow_html=True)
            for _, row in selected_rows.iterrows():
                valid_items = [(k, v) for k, v in row.items() if str(v).strip() and k not in ["상세조회", "삭제", "sheet_row"]]
                cols = st.columns(4)
                for idx, (lbl, val) in enumerate(valid_items):
                    with cols[idx % 4]: st.markdown(f'<div class="detail-card"><div class="detail-label">{lbl}</div><div class="detail-value">{val}</div></div>', unsafe_allow_html=True)
                st.divider()

        if (is_admin or is_requester) and st.button("💾 변경사항 최종 반영하기", use_container_width=True):
            try:
                ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
                headers = ws.row_values(1)
                
                # 삭제 처리 (관리자 전용)
                deleted_rows_list = []
                if is_admin and "삭제" in edited_df.columns:
                    rows_to_delete = edited_df[edited_df["삭제"] == True]
                    if not rows_to_delete.empty:
                        deleted_rows_list = sorted(rows_to_delete['sheet_row'].tolist(), reverse=True)
                        for r in deleted_rows_list: ws.delete_rows(int(r))
                
                # 업데이트 처리
                remaining_df = edited_df.copy()
                if "삭제" in remaining_df.columns:
                    remaining_df = remaining_df[remaining_df["삭제"] == False]
                
                for _, row in remaining_df.iterrows():
                    offset = sum(1 for r in deleted_rows_list if int(r) < int(row['sheet_row']))
                    actual_r = int(row['sheet_row']) - offset
                    
                    if is_admin:
                        ws.update_cell(actual_r, headers.index("진행상황")+1, row["진행상황"])
                        ws.update_cell(actual_r, headers.index("완료자")+1, row["완료자"])
                        ws.update_cell(actual_r, headers.index("완료일")+1, str(row["완료일"]) if row["완료일"] else "")
                    
                    if is_requester:
                        for col_name in ["신청구분", "신청일", "신청자", "제품코드1", "제품명1", "거래명세표"]:
                            if col_name in headers:
                                ws.update_cell(actual_r, headers.index(col_name)+1, str(row[col_name]) if row[col_name] else "")
                            
                st.success("성공적으로 저장되었습니다!"); st.cache_data.clear(); st.rerun()
            except Exception as e: st.error(f"오류: {e}")
    else: st.info("신청 내역이 없습니다.")

# --- 신청서 양식 섹션 ---
elif st.session_state.active_menu in ["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲"]:
    curr = st.session_state.active_menu
    d = {}
    st.markdown(f'<div class="section-header">{curr} 신청</div>', unsafe_allow_html=True)
    edi1 = st.text_input(f"대상 제품코드 입력", key=f"t_edi1")
    d.update(render_drug_table(edi1, 1))
    
    if curr == "사용중지":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t1_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay"), c3.text_input("구입처1", key="t1_vd"), c4.text_input("개당입고가1", key="t1_pr")
        c5, c6, c7, c8 = st.columns(4)
        d["사용중지일1"], d["사용중지사유1"] = c5.date_input("사용중지일1", key="t1_sd").strftime('%Y-%m-%d'), c6.selectbox("사용중지사유1", OP_STOP_REASON, key="t1_rs")
        d["사용중지사유_기타1"] = c7.text_input("사용중지사유_기타1", key="t1_ers", disabled=(d["사용중지사유1"] != "기타"))
        d["재고여부1"] = c8.selectbox("재고여부1", ["유", "무"], key="t1_syn")
        has_s = (d["재고여부1"]=="유")
        c9, c10, c11, c12 = st.columns(4)
        d["재고처리방법1"], d["재고량1"], d["반품가능여부1"], d["반품예정일1"] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t1_mth", disabled=not has_s), c10.number_input("재고량1", 0, key="t1_vol", disabled=not has_s), c11.selectbox("반품가능여부1", OP_POSSIBLE, key="t1_pyn", disabled=not has_s), c12.date_input("반품예정일1", key="t1_rd", disabled=not has_s).strftime('%Y-%m-%d')
        d["반품량1"] = st.number_input("반품량1", 0, key="t1_rv", disabled=not has_s)

    elif curr == "신규입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t2_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay"), c3.text_input("구입처1", key="t2_vd"), c4.text_input("개당입고가1", key="t2_pr")
        c5, c6, c7, c8 = st.columns(4)
        d["입고요청진료과1"], d["원내유무(동일성분)1"], d["입고요청사유1"], d["사용기간1"] = c5.selectbox("입고요청진료과1", OP_DEPT, key="t2_dp"), c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_sm"), c7.selectbox("입고요청사유1", OP_STOP_REASON, key="t2_rs"), c8.selectbox("사용기간1", OP_USE_PERIOD, key="t2_pd")
        c9, c10, c11 = st.columns(3)
        d["입고일1"], d["코드사용시작일1"], d["상한가외입고사유1"] = c9.date_input("입고일1", key="t2_id").strftime('%Y-%m-%d'), c10.date_input("코드사용시작일1", key="t2_sd").strftime('%Y-%m-%d'), c11.text_input("상한가외입고사유1", key="t2_or")

    elif curr == "대체입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.text_input("개당입고가1", key="t3_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["재고여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1")
        has_s = (d["재고여부1"]=="유")
        d["재고처리방법1"], d["재고량1"], d["반품가능여부1"] = c6.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t3_m1", disabled=not has_s), c7.number_input("재고량1", 0, key="t3_sv1", disabled=not has_s), c8.selectbox("반품가능여부1", OP_POSSIBLE, key="t3_py1", disabled=not has_s)
        c9, c10, c11, c12 = st.columns(4)
        d["반품예정일1"], d["반품량1"], d["코드중지기준1"], d["사용중지일1"] = c9.date_input("반품예정일1", key="t3_rd1", disabled=not has_s).strftime('%Y-%m-%d'), c10.number_input("반품량1", 0, key="t3_rv1", disabled=not has_s), c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1"), c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
        st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2")
        d.update(render_drug_table(edi2, 2, "(대체약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.text_input("개당입고가2", key="t3_pr2")

    elif curr in ["삭제코드변경", "단가인하▼"]:
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t_p1"), c3.text_input("구입처1", key="t_v1"), c4.text_input("개당입고가1", key="t_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"], d["재고여부1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t_cn1"), c6.selectbox("재고여부1", ["유", "무"], key="t_s1")
        has_t = (d["재고여부1"]=="유")
        d["재고처리방법1"], d["재고량1"] = c7.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t_m1", disabled=not has_t), c8.number_input("재고량1", 0, key="t_sv1", disabled=not has_t)
        c9, c10, c11, c12 = st.columns(4)
        d["반품가능여부1"] = c9.selectbox("반품가능여부1", OP_POSSIBLE, key="t_py1", disabled=not has_t)
        d["반품불가사유1"] = c10.text_input("반품불가사유1", key="t_nrs1", disabled=not (has_t and d["반품가능여부1"]=="불가"))
        d["반품예정일1"], d["반품량1"] = c11.date_input("반품예정일1", key="t_rd1", disabled=not has_t).strftime('%Y-%m-%d'), c12.number_input("반품량1", 0, key="t_rv1", disabled=not has_t)
        st.markdown('<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("변경 제품코드 입력", key="t_edi2")
        d.update(render_drug_table(edi2, 2, "(변경약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t_p2"), c15.text_input("구입처2", key="t_v2"), c16.text_input("개당입고가2", key="t_pr2")

    elif curr == "단가인상▲":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.text_input("구입처1", key="t6_v1"), c4.text_input("개당입고가1", key="t6_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"], d["사용중지일1"], d["입고일1"], d["인상전입고가1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t6_cn1"), c6.date_input("품절일1", key="t6_sd1").strftime('%Y-%m-%d'), c7.date_input("재입고일자1", key="t6_id1").strftime('%Y-%m-%d'), c8.text_input("인상전입고가1", key="t6_pre_pr")
        d["코드사용시작일1"] = st.date_input("코드사용시작일1", key="t6_ss1").strftime('%Y-%m-%d')

    st.divider()
    col_memo, col_file = st.columns([2, 1])
    with col_memo:
        d["비고(기타 요청사항)"] = st.text_area("비고(기타 요청사항)", key="final_memo")
    with col_file:
        d["거래명세표"] = st.text_input("거래명세표 URL", placeholder="http://...", key="final_file")
    
    if st.button(f"🚀 {curr} 제출", key="final_btn", use_container_width=True):
        handle_safe_submit(curr, d)

elif st.session_state.active_menu == "🔍 약가조회":
    st.markdown('<div class="section-header">🔍 약가 상세 정보 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (9자리)", key="search_edi")
    if s_edi:
        m = get_drug_info(s_edi)
        if m:
            disp = ["연번", "제품코드", "제품명", "업체명", "규격", "단위", "상한금액", "전일", "투여", "분류", "식약분류", "주성분명"]
            cols = st.columns(4)
            for idx, field in enumerate(disp):
                with cols[idx % 4]:
                    val = m.get(field, "-")
                    if field == "상한금액" and val != "-":
                        try: val = "{:,} 원".format(int(str(val).replace(',', '')))
                        except: pass
                    st.markdown(f'<div class="detail-card"><div class="detail-label">{field}</div><div class="detail-value">{val}</div></div>', unsafe_allow_html=True)
        else: st.error("해당 제품코드를 찾을 수 없습니다.")
