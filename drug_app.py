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
    
    /* --- 모던 대시보드 네비게이션 스타일 --- */
    /* 모든 버튼 공통: 세련된 그림자와 둥근 모서리 */
    .stButton > button { 
        width: 100%; border-radius: 12px; font-weight: 700; height: 48px; 
        transition: all 0.3s; border: 1px solid #e2e8f0; background-color: #ffffff;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stButton > button:hover { border-color: #1E3A8A; color: #1E3A8A; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }

    /* 입력창 라벨을 작고 세련된 '배지' 형태로 변경 */
    div[data-testid="column"] label {
        font-size: 0.75rem !important;
        font-weight: 800 !important;
        color: #ffffff !important;
        background-color: #1E3A8A; /* 진한 파란색 배지 */
        padding: 2px 10px !important;
        border-radius: 6px !important;
        margin-bottom: 6px !important;
        display: inline-block !important;
        letter-spacing: 0.5px;
    }

    /* 입력창 자체 스타일: 버튼과 높이를 맞추고 깔끔하게 처리 */
    div[data-testid="column"] [data-testid="stTextInput"] > div {
        border-radius: 12px !important;
        height: 48px !important;
        background-color: #f8fafc !important;
        border: 1px solid #e2e8f0 !important;
    }
    div[data-testid="column"] input {
        height: 48px !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
    }

    /* 하단 여백 및 정렬 맞춤 */
    div[data-testid="column"] {
        display: flex;
        flex-direction: column;
        justify-content: flex-end; /* 버튼과 높이 정렬을 위해 하단 정렬 */
    }

    /* --- 기존 스타일 유지 (약제 테이블 등) --- */
    section[data-testid="stSidebar"] div[data-testid="stTextInput"] input {
        background-color: #fff9c4 !important; border: 2px solid #fbc02d !important; font-weight: 800 !important; color: #000000 !important;
    }
    div[data-testid="stVerticalBlock"] div:has(label:contains("제품코드")) input {
        background-color: #fffdec !important; border: 2px solid #fbbf24 !important; font-weight: 700 !important; color: #000000 !important;
    }
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
            # 모든 데이터를 문자열로 변환하고 빈 값 처리
            df = df.astype(str).replace(['nan', 'None', 'None', ''], '')
            
            # [추가된 부분] 구글 시트의 진행상황 텍스트를 이모지 옵션과 매칭
            if "진행상황" in df.columns:
                def map_status(x):
                    if x == "신청완료": return "🔴신청완료"
                    if x == "처리중": return "🟡처리중"
                    if x == "처리완료": return "🟢처리완료"
                    return x
                df["진행상황"] = df["진행상황"].apply(map_status)

            # [유지된 부분] 제품코드가 포함된 모든 컬럼의 앞 '0'을 보존 (9자리)
            for col in df.columns:
                if "제품코드" in col:
                    # 값이 있는 경우에만 작동 (빈 칸에 000000000이 생기는 것 방지)
                    df[col] = df[col].apply(lambda x: x.strip().zfill(9) if x and x.strip() and x != '0' else x)
            
            return df
    except Exception as e:
        return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP_USERS = ["", "김영국", "허은아", "한승주", "이소영", "변혜진", "직접 입력"] # 신청자 리스트 추가
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

# --- 4. 세션 상태 관리 ---
if 'active_menu' not in st.session_state:
    st.session_state.active_menu = "📊 진행현황"

def clear_form_data():
    keys_to_reset = [k for k in st.session_state.keys() if k.startswith(('t1_', 't2_', 't3_', 't_', 't6_', 't_edi', 'final_', 'search_edi', 't_edi1'))]
    for key in keys_to_reset:
        del st.session_state[key]

def set_menu(menu_name):
    st.session_state.active_menu = menu_name
    clear_form_data()

def check_auth_auto():
    if st.session_state.get("auth_p") == "1452":
        st.session_state.active_menu = "📊 진행현황"

# --- 5. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()

    # 신청자 성명 선택/입력 로직
    user_sel = st.selectbox("신청자 성명", options=OP_USERS, key="user_dropdown")
    if user_sel == "직접 입력":
        app_user = st.text_input("성명을 입력하세요", key="manual_user")
    else:
        app_user = user_sel # 드롭다운에서 선택한 값이 그대로 app_user가 됨

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
    if st.button("📝 거래명세표요청", use_container_width=True): set_menu("거래명세표요청")

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
    return {
        f"제품코드{drug_num}": str(edi_val), f"제품명{drug_num}": m.get("제품명", ""), f"업체명{drug_num}": m.get("업체명", ""),
        f"규격{drug_num}": m.get("규격", ""), f"단위{drug_num}": m.get("단위", ""), f"상한금액{drug_num}": price,
        f"주성분명{drug_num}": m.get("주성분명", ""), f"전일{drug_num}": m.get("전일", "")
    }

def handle_safe_submit(category, data_dict):
    if not app_user: 
        st.error("신청자 성명을 입력해주세요."); return
    
    # 처리 중 상태 표시 시작
    with st.status(f"🚀 [{category}] 신청서를 접수 중입니다...", expanded=True) as status:
        try:
            ss = get_spreadsheet()
            ws = ss.worksheet("New_stop")
            headers = ws.row_values(1)
            
            data_dict.update({
                "신청구분": category, 
                "신청일": app_date, 
                "신청자": app_user, 
                "진행상황": "신청완료"
            })
            
            row_to_append = [str(data_dict.get(h, "")) for h in headers]
            ws.append_row(row_to_append, value_input_option='RAW')
            
            # 완료 상태로 업데이트
            status.update(label=f"✅ [{category}] 접수가 완료되었습니다!", state="complete", expanded=False)
            st.success(f"[{category}] 접수 완료!"); st.balloons()
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            status.update(label="❌ 저장 중 오류가 발생했습니다.", state="error")
            st.error(f"저장 중 오류 발생: {e}")

# --- 7. 상단 네비게이션 ---
is_requester = (st.session_state.get("auth_req") == "7410")
is_admin = (st.session_state.get("auth_admin") == "1452")

# 버튼과 입력창의 높이 균형을 위해 columns 설정
t_col1, t_col2, t_col3, t_col4 = st.columns([1.2, 1.0, 1.0, 1.2])

with t_col1:
    # 버튼 위에 여백을 주어 입력창 배지와 높이를 맞춤
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("📊 진행현황", key="top_status", use_container_width=True): 
        set_menu("📊 진행현황")

with t_col2:
    st.text_input("신청부서 권한 🔒", type="password", placeholder="****", 
                  key="auth_req", on_change=check_auth_auto)

with t_col3:
    st.text_input("처리부서 권한 🔑", type="password", placeholder="****", 
                  key="auth_admin", on_change=check_auth_auto)

with t_col4:
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("🔍 약가조회", key="top_search", use_container_width=True): 
        set_menu("🔍 약가조회")

# --- 8. 메인 컨텐츠 영역 ---

# [1] 진행현황 (위에서 수정된 내용 유지)
if st.session_state.active_menu == "📊 진행현황":
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    
    # 권한 설정 확인
    is_requester = (st.session_state.get("auth_req") == "7410")
    is_admin = (st.session_state.get("auth_admin") == "1452")
    
    if not db_df.empty:
        # 검색 기능
        search = st.text_input("🔍 검색 (제품명, 신청자 등)", key="dash_search")
        if search: 
            db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        
        edit_df_view = db_df.copy()

        # --- [1. 타입 호환성 에러 해결을 위한 데이터 전처리] ---
        date_cols = ['처리일', '신청일', '사용중지일1', '반품예정일1', '입고일1', '코드사용시작일1', '입고일2', '코드사용시작일2']
        for col in date_cols:
            if col in edit_df_view.columns:
                edit_df_view[col] = pd.to_datetime(edit_df_view[col], errors='coerce').dt.date

        for col in edit_df_view.columns:
            if col in ["진행상황", "신청구분", "처리자", "원내구분1", "재고여부1", "급여구분1"]:
                edit_df_view[col] = edit_df_view[col].astype(str).str.strip()

        edit_df_view['sheet_row'] = range(2, len(edit_df_view) + 2)
        edit_df_view = edit_df_view.iloc[::-1] 
        
        # --- [2. 컬럼 순서 재배치] ---
        core_fields = ["신청구분", "신청일", "신청자", "처리일", "처리자", "진행상황", "요청사항(신청부서)", "전달사항(처리부서)", "거래명세표", "제품코드1", "제품명1"]
        extra_fields = [
            "업체명1", "규격1", "단위1", "상한금액1", "주성분명1", "전일1", "원내구분1", "급여구분1", "구입처1", "개당입고가1", 
            "사용중지일1", "사용중지사유1", "사용중지사유_기타1", "변경내용1", "재고여부1", "재고처리방법1", "재고량1", "반품가능여부1", 
            "반품불가사유1", "반품예정일1", "반품량1", "코드중지기준1", "신규약제와병용사용1", "입고요청진료과1", "원내유무(동일성분)1", 
            "입고요청사유1", "사용기간1", "입고일1", "인상전입고가1", "코드사용시작일1", "상한가외입고사유1", "제품코드2", "제품명2", 
            "업체명2", "규격2", "단위2", "상한금액2", "주성분명2", "전일2", "원내구분2", "급여구분2", "구입처2", "개당입고가2", 
            "입고요청사유2", "코드사용시작일2", "상한가외입고사유2", "기존약제와병용사용2", "사용기간2", "입고일2"
        ]
        
        all_existing = edit_df_view.columns.tolist()
        final_order = [c for c in core_fields if c in all_existing] + \
                      [c for c in extra_fields if c in all_existing and c not in core_fields]
        
        remains = [c for c in all_existing if c not in final_order and c != 'sheet_row']
        edit_df_view = edit_df_view[final_order + remains + ['sheet_row']]
        
        edit_df_view.insert(0, "상세조회", False)
        if is_admin: edit_df_view.insert(1, "삭제", False)
        
        # --- [3. 컬럼 설정 및 너비 설정] ---
        c_cfg = {"상세조회": st.column_config.CheckboxColumn("조회", width="small"), "sheet_row": None}
        admin_fields = ["진행상황", "처리자", "처리일", "전달사항(처리부서)"]
        
        for f in [c for c in edit_df_view.columns if c not in ["상세조회", "삭제", "sheet_row"]]:
            is_admin_col = f in admin_fields
            can_edit = (is_admin and is_admin_col) or (is_requester and not is_admin_col)
            
            if f == "진행상황": 
                c_cfg[f] = st.column_config.SelectboxColumn(f, options=OP_STATUS, disabled=not can_edit)
            elif f == "처리자": 
                c_cfg[f] = st.column_config.SelectboxColumn(f, options=OP_PROCESSORS, disabled=not can_edit)
            elif f == "신청구분": 
                c_cfg[f] = st.column_config.SelectboxColumn(f, options=["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲", "거래명세표요청"], disabled=not can_edit)
            elif f in date_cols: 
                c_cfg[f] = st.column_config.DateColumn(f, format="YYYY-MM-DD", disabled=not can_edit)
            elif f == "요청사항(신청부서)":
                c_cfg[f] = st.column_config.TextColumn(f, width="medium", disabled=not can_edit)
            elif f == "전달사항(처리부서)":
                c_cfg[f] = st.column_config.TextColumn(f, width="medium", disabled=not can_edit)
            elif f in ["원내구분1", "원내구분2"]:
                c_cfg[f] = st.column_config.SelectboxColumn(f, options=OP_INSIDE_OUT, disabled=not can_edit)
            elif f in ["재고여부1", "급여구분1", "급여구분2"]:
                c_cfg[f] = st.column_config.SelectboxColumn(f, options=["유", "무"] if "재고" in f else ["급여", "비급여"], disabled=not can_edit)
            elif f == "거래명세표":
                c_cfg[f] = st.column_config.LinkColumn(f, display_text="파일 열기", disabled=not can_edit)
            else:
                c_cfg[f] = st.column_config.TextColumn(f, disabled=not can_edit)

        if is_admin: c_cfg["삭제"] = st.column_config.CheckboxColumn("삭제", width="small")

        edited_df = st.data_editor(
            edit_df_view, 
            column_config=c_cfg, 
            hide_index=True, 
            use_container_width=True, 
            height=600, 
            key="main_editor_v15"
        )
        
        # 상세 조회 처리
        selected_rows = edited_df[edited_df["상세조회"] == True]
        if not selected_rows.empty:
            st.markdown('<div class="section-header">🔍 선택 항목 상세 정보</div>', unsafe_allow_html=True)
            for _, row in selected_rows.iterrows():
                valid_items = [(k, v) for k, v in row.items() if str(v).strip() and k not in ["상세조회", "삭제", "sheet_row"]]
                cols = st.columns(4)
                for idx, (lbl, val) in enumerate(valid_items):
                    with cols[idx % 4]:
                        st.markdown(f'<div class="detail-card"><div class="detail-label">{lbl}</div><div class="detail-value">{val}</div></div>', unsafe_allow_html=True)
                st.divider()

        # [저장 로직]
        if is_admin or is_requester:
            if st.button("💾 변경사항 DB에 통합 저장하기", use_container_width=True):
                # 저장 프로세스 상태 표시
                with st.status("🔄 DB에 변경사항을 저장 중입니다...", expanded=True) as status:
                    try:
                        ss = get_spreadsheet()
                        ws = ss.worksheet("New_stop")
                        all_data = ws.get_all_values()
                        headers = all_data[0]
                        
                        deleted_rows_nums = [int(r) for r in edited_df[edited_df.get("삭제", False) == True]['sheet_row'].tolist()] if is_admin else []
                        
                        remaining_df = edited_df.copy()
                        if is_admin: remaining_df = remaining_df[remaining_df.get("삭제", False) == False]
                        
                        for _, row in remaining_df.iterrows():
                            r_idx = int(row['sheet_row']) - 1
                            for col_name in headers:
                                if col_name in row:
                                    is_adm_col = col_name in admin_fields
                                    if (is_admin and is_adm_col) or (is_requester and not is_adm_col):
                                        val = row[col_name]
                                        all_data[r_idx][headers.index(col_name)] = str(val) if val is not None else ""
                        
                        if deleted_rows_nums:
                            for r_num in sorted(deleted_rows_nums, reverse=True):
                                del all_data[r_num - 1]
                        
                        ws.clear()
                        ws.update('A1', all_data)
                        
                        status.update(label="✅ 모든 변경사항이 성공적으로 저장되었습니다!", state="complete", expanded=False)
                        st.success("변경사항 저장 완료!"); st.cache_data.clear(); st.rerun()
                    except Exception as e:
                        status.update(label="❌ 저장 실패", state="error")
                        st.error(f"저장 중 오류 발생: {e}")

# [2-7] 신청서 섹션들 (거래명세표요청 추가 포함)
elif st.session_state.active_menu in ["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲", "거래명세표요청"]:
    curr = st.session_state.active_menu
    d = {}
    st.markdown(f'<div class="section-header">{curr} 신청</div>', unsafe_allow_html=True)
    
    # 공통: 첫 번째 약제 코드 입력 및 정보 표시
    edi1 = st.text_input(f"대상 제품코드 입력", key=f"t_edi1")
    d.update(render_drug_table(edi1, 1))
    
    # --- 1. 사용중지 섹션 ---
    if curr == "사용중지":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t1_io")
        d["급여구분1"] = c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay")
        d["구입처1"] = c3.text_input("구입처1", key="t1_vd")
        d["개당입고가1"] = c4.text_input("개당입고가1", key="t1_pr")
        
        c5, c6, c7, c8 = st.columns(4)
        d["사용중지일1"] = c5.date_input("사용중지일1", key="t1_sd").strftime('%Y-%m-%d')
        d["사용중지사유1"] = c6.selectbox("사용중지사유1", OP_STOP_REASON, key="t1_rs")
        
        is_other_reason = (d["사용중지사유1"] == "기타")
        d["사용중지사유_기타1"] = c7.text_input("사용중지사유_기타1", key="t1_ers", disabled=not is_other_reason)
        d["재고여부1"] = c8.selectbox("재고여부1", ["유", "무"], key="t1_syn")
        
        has_stock = (d["재고여부1"] == "유")
        c9, c10, c11, c12 = st.columns(4)
        d["재고처리방법1"] = c9.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t1_mth", disabled=not has_stock)
        d["재고량1"] = c10.number_input("재고량1", 0, key="t1_vol", disabled=not has_stock)
        d["반품가능여부1"] = c11.selectbox("반품가능여부1", OP_POSSIBLE, key="t1_pyn", disabled=not has_stock)
        d["반품예정일1"] = c12.date_input("반품예정일1", key="t1_rd", disabled=not has_stock).strftime('%Y-%m-%d')
        d["반품량1"] = st.number_input("반품량1", 0, key="t1_rv", disabled=not has_stock)

        if not is_other_reason: d["사용중지사유_기타1"] = ""
        if not has_stock:
            d["재고처리방법1"] = ""; d["재고량1"] = 0; d["반품가능여부1"] = ""; d["반품예정일1"] = ""; d["반품량1"] = 0

    # --- 2. 신규입고 섹션 ---
    elif curr == "신규입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t2_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay"), c3.text_input("구입처1", key="t2_vd"), c4.text_input("개당입고가1", key="t2_pr")
        c5, c6, c7, c8 = st.columns(4)
        d["입고요청진료과1"], d["원내유무(동일성분)1"], d["입고요청사유1"], d["사용기간1"] = c5.selectbox("입고요청진료과1", OP_DEPT, key="t2_dp"), c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_sm"), c7.selectbox("입고요청사유1", OP_STOP_REASON, key="t2_rs"), c8.selectbox("사용기간1", OP_USE_PERIOD, key="t2_pd")
        c9, c10, c11 = st.columns(3)
        d["입고일1"], d["코드사용시작일1"], d["상한가외입고사유1"] = c9.date_input("입고일1", key="t2_id").strftime('%Y-%m-%d'), c10.date_input("코드사용시작일1", key="t2_sd").strftime('%Y-%m-%d'), c11.text_input("상한가외입고사유1", key="t2_or")

    # --- 3. 대체입고 섹션 ---
    elif curr == "대체입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.text_input("개당입고가1", key="t3_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["재고여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1")
        has_stock_t3 = (d["재고여부1"] == "유")
        d["재고처리방법1"] = c6.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t3_m1", disabled=not has_stock_t3)
        d["재고량1"] = c7.number_input("재고량1", 0, key="t3_sv1", disabled=not has_stock_t3)
        d["반품가능여부1"] = c8.selectbox("반품가능여부1", OP_POSSIBLE, key="t3_py1", disabled=not has_stock_t3)
        c9, c10, c11, c12 = st.columns(4)
        d["반품예정일1"] = c9.date_input("반품예정일1", key="t3_rd1", disabled=not has_stock_t3).strftime('%Y-%m-%d')
        d["반품량1"] = c10.number_input("반품량1", 0, key="t3_rv1", disabled=not has_stock_t3)
        d["코드중지기준1"] = c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1")
        d["사용중지일1"] = c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
        d["신규약제와병용사용1"] = st.selectbox("신규약제와병용사용1", OP_YN, key="t3_co1")
        if not has_stock_t3: d["재고처리방법1"] = ""; d["재고량1"] = 0; d["반품가능여부1"] = ""; d["반품예정일1"] = ""; d["반품량1"] = 0

        st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2")
        d.update(render_drug_table(edi2, 2, "(대체약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.text_input("개당입고가2", key="t3_pr2")
        c17, c18, c19, c20 = st.columns(4)
        d["입고요청사유2"], d["사용기간2"], d["입고일2"], d["코드사용시작일2"] = c17.selectbox("입고요청사유2", OP_STOP_REASON, key="t3_rs2"), c18.selectbox("사용기간2", OP_USE_PERIOD, key="t3_pd2"), c19.date_input("입고일2", key="t3_id2").strftime('%Y-%m-%d'), c20.date_input("코드사용시작일2", key="t3_ss2").strftime('%Y-%m-%d')
        cs1, cs2 = st.columns(2)
        d["기존약제와병용사용2"], d["상한가외입고사유2"] = cs1.selectbox("기존약제와병용사용2", OP_YN, key="t3_co2"), cs2.text_input("상한가외입고사유2", key="t3_ov2")

    # --- 4. 삭제코드변경 / 단가인하▼ 섹션 ---
    elif curr in ["삭제코드변경", "단가인하▼"]:
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t_p1"), c3.text_input("구입처1", key="t_v1"), c4.text_input("개당입고가1", key="t_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t_cn1")
        d["재고여부1"] = c6.selectbox("재고여부1", ["유", "무"], key="t_s1")
        has_stock_t = (d["재고여부1"] == "유")
        d["재고처리방법1"] = c7.selectbox("재고처리방법1", OP_STOCK_METHOD, key="t_m1", disabled=not has_stock_t)
        d["재고량1"] = c8.number_input("재고량1", 0, key="t_sv1", disabled=not has_stock_t)
        c9, c10, c11, c12 = st.columns(4)
        d["반품가능여부1"] = c9.selectbox("반품가능여부1", OP_POSSIBLE, key="t_py1", disabled=not has_stock_t)
        is_return_impossible = (has_stock_t and d["반품가능여부1"] == "불가")
        d["반품불가사유1"] = c10.text_input("반품불가사유1 (필수)", key="t_nrs1", disabled=not is_return_impossible)
        d["반품예정일1"] = c11.date_input("반품예정일1", key="t_rd1", disabled=not has_stock_t).strftime('%Y-%m-%d')
        d["반품량1"] = c12.number_input("반품량1", 0, key="t_rv1", disabled=not has_stock_t)
        if not has_stock_t: d["재고처리방법1"] = ""; d["재고량1"] = 0; d["반품가능여부1"] = ""; d["반품불가사유1"] = ""; d["반품예정일1"] = ""; d["반품량1"] = 0
        elif not is_return_impossible: d["반품불가사유1"] = ""

        st.markdown('<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("변경 제품코드 입력", key="t_edi2")
        d.update(render_drug_table(edi2, 2, "(변경약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP_INSIDE_OUT, key="t_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t_p2"), c15.text_input("구입처2", key="t_v2"), c16.text_input("개당입고가2", key="t_pr2")
        c17, c18 = st.columns(2)
        d["코드사용시작일2"], d["상한가외입고사유2"] = c17.date_input("코드사용시작일2", key="t_ss2").strftime('%Y-%m-%d'), c18.text_input("상한가외입고사유2", key="t_ov2")

    # --- 5. 단가인상▲ 섹션 ---
    elif curr == "단가인상▲":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP_INSIDE_OUT, key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.text_input("구입처1", key="t6_v1"), c4.text_input("개당입고가1", key="t6_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"] = c5.selectbox("변경내용1", OP_CHANGE_CONTENT, key="t6_cn1")
        d["사용중지일1"] = c6.date_input("품절일1", key="t6_sd1").strftime('%Y-%m-%d')
        d["입고일1"] = c7.date_input("재입고일자1", key="t6_id1").strftime('%Y-%m-%d')
        d["인상전입고가1"] = c8.text_input("인상전입고가1", key="t6_pre_pr")
        d["코드사용시작일1"] = st.date_input("코드사용시작일1", key="t6_ss1").strftime('%Y-%m-%d')

    # --- 6. 거래명세표요청 섹션 (추가) ---
    elif curr == "거래명세표요청":
        # 별도의 필드 없이 약제정보만 출력 후 바로 하단 공통 영역으로 이동
        st.info("선택한 약제의 거래명세표를 요청합니다. 하단의 요청사항과 URL을 작성해주세요.")

    # 하단 공통: 비고란 및 제출 버튼
    st.divider()
    col_memo, col_file = st.columns([2, 1])
    with col_memo: d["요청사항(신청부서)"] = st.text_area("요청사항(신청부서)", key="final_memo")
    with col_file: d["거래명세표"] = st.text_input("거래명세표 URL (구글/네이버 등)", placeholder="http://...", key="final_file")
    
    if st.button(f"🚀 {curr} 제출", key="final_btn", use_container_width=True):
        handle_safe_submit(curr, d)

# [8] 약가조회
elif st.session_state.active_menu == "🔍 약가조회":
    st.markdown('<div class="section-header">🔍 약가 상세 정보 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (9자리)", key="search_edi")
    if s_edi:
        m = get_drug_info(s_edi)
        if m:
            disp_fields = ["연번", "제품코드", "제품명", "업체명", "규격", "단위", "상한금액", "전일", "투여", "분류", "식약분류", "주성분코드_동일제형", "주성분코드", "주성분갯수", "주성분명"]
            cols = st.columns(4)
            for idx, field in enumerate(disp_fields):
                with cols[idx % 4]:
                    val = m.get(field, "-")
                    if field == "상한금액" and val != "-":
                        try: val = "{:,} 원".format(int(str(val).replace(',', '')))
                        except: pass
                    st.markdown(f'<div class="detail-card"><div class="detail-label">{field}</div><div class="detail-value">{val}</div></div>', unsafe_allow_html=True)
        else: st.error("해당 제품코드를 찾을 수 없습니다.")
