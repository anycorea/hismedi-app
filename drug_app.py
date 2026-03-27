import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 (기존 스타일 완전 유지) ---
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

# --- 2. 데이터 처리 함수 (GSpread 효율화) ---
@st.cache_resource
def get_spreadsheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
    except Exception as e:
        st.error(f"구글 시트 연결 오류: {e}")
        return None

@st.cache_data(ttl=300)
def load_master_data():
    try:
        ss = get_spreadsheet()
        if ss:
            df = pd.DataFrame(ss.worksheet("Master").get_all_records())
            df['제품코드'] = df['제품코드'].astype(str).str.strip().str.zfill(9) 
            return df
    except: pass
    return pd.DataFrame()

@st.cache_data(ttl=10) # 속도 개선을 위해 TTL을 10초로 약간 늘림
def load_db_data():
    try:
        ss = get_spreadsheet()
        if not ss: return pd.DataFrame()
        ws = ss.worksheet("New_stop")
        data = ws.get_all_records()
        if not data: return pd.DataFrame()
        df = pd.DataFrame(data).astype(str).replace(['nan', 'None', ''], '')
        
        # 진행상황 이모지 매핑
        status_map = {"신청완료": "🔴신청완료", "처리중": "🟡처리중", "처리완료": "🟢처리완료"}
        if "진행상황" in df.columns:
            df["진행상황"] = df["진행상황"].replace(status_map)

        # 제품코드 9자리 보존
        for col in [c for c in df.columns if "제품코드" in c]:
            df[col] = df[col].apply(lambda x: x.strip().zfill(9) if x and x != '0' else x)
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

# --- 3. 옵션 리스트 ---
OP = {
    "io": ["원내", "원외", "원내/외"],
    "status": ["🔴신청완료", "🟡처리중", "🟢처리완료"],
    "processors": ["", "한승주 팀장", "이소영 대리", "변혜진 주임"],
    "dept": ["내과", "신장내과", "소아청소년과", "외과", "정형외과", "신경외과", "비뇨의학과", "산부인과", "이비인후과", "가정의학과", "마취통증의학과", "영상의학과"],
    "reason": ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"],
    "change": ["급여코드 삭제", "상한가 인하", "상한가 인상"],
    "stock_mth": ["재고 소진", "반품", "폐기"],
    "period": ["한시적 사용", "지속적 사용"],
    "yn": ["Y", "N"],
    "possible": ["가능", "불가"]
}

# --- 4. 세션 관리 및 메뉴 함수 ---
if 'active_menu' not in st.session_state: st.session_state.active_menu = "📊 진행현황"

def set_menu(menu_name):
    st.session_state.active_menu = menu_name
    # 전체 삭제 대신 신청 관련 키만 선별 삭제 (속도 최적화)
    keys_to_del = [k for k in st.session_state.keys() if k.startswith(('t1_', 't2_', 't3_', 't_', 't6_', 'final_', 'search_edi'))]
    for k in keys_to_del: del st.session_state[k]

# --- 5. 헬퍼 함수 (기존 테이블 유지) ---
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
    if not st.session_state.get("global_user"): 
        st.error("신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet(); ws = ss.worksheet("New_stop")
        headers = ws.row_values(1)
        data_dict.update({
            "신청구분": category, 
            "신청일": st.session_state.global_date.strftime('%Y-%m-%d'), 
            "신청자": st.session_state.global_user, 
            "진행상황": "신청완료"
        })
        row_to_append = [str(data_dict.get(h, "")) for h in headers]
        ws.append_row(row_to_append, value_input_option='RAW')
        st.success(f"[{category}] 접수 완료!"); st.balloons()
        st.cache_data.clear(); st.rerun()
    except Exception as e: st.error(f"저장 오류: {e}")

# --- 6. 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    if st.button("🔄 새로고침", use_container_width=True): st.cache_data.clear(); st.rerun()
    st.divider()
    app_user = st.text_input("신청자 성명", key="global_user")
    app_date = st.date_input("날짜 선택", datetime.now(), key="global_date")
    st.divider()
    
    c1, c2 = st.columns(2)
    if c1.button("사용중지"): set_menu("사용중지")
    if c2.button("신규입고"): set_menu("신규입고")
    c3, c4 = st.columns(2)
    if c3.button("대체입고"): set_menu("대체입고")
    if c4.button("삭제코드변경"): set_menu("삭제코드변경")
    c5, c6 = st.columns(2)
    if c5.button("단가인하▼"): set_menu("단가인하▼")
    if c6.button("단가인상▲"): set_menu("단가인상▲")

# 상단 네비게이션
is_req = (st.session_state.get("auth_req") == "7410")
is_admin = (st.session_state.get("auth_admin") == "1452")

t_col = st.columns([1.2, 1.0, 1.0, 1.2])
with t_col[0]:
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("📊 진행현황", key="top_status", use_container_width=True): set_menu("📊 진행현황")
with t_col[1]: st.text_input("신청부서 권한 🔒", type="password", key="auth_req")
with t_col[2]: st.text_input("완료부서 권한 🔑", type="password", key="auth_admin")
with t_col[3]:
    st.markdown("<div style='margin-top:28px;'></div>", unsafe_allow_html=True)
    if st.button("🔍 약가조회", key="top_search", use_container_width=True): set_menu("🔍 약가조회")

# --- 7. 메인 컨텐츠 ---

# [1] 진행현황
if st.session_state.active_menu == "📊 진행현황":
    st.markdown('<div class="section-header">📊 통합 신청 및 처리 현황</div>', unsafe_allow_html=True)
    db_df = load_db_data()
    if not db_df.empty:
        search = st.text_input("🔍 검색", key="dash_search")
        if search: db_df = db_df[db_df.apply(lambda r: r.astype(str).str.contains(search).any(), axis=1)]
        
        edit_view = db_df.copy()
        edit_view['sheet_row'] = range(2, len(edit_view) + 2)
        edit_view = edit_view.iloc[::-1]
        
        # 상세/삭제 컬럼
        edit_view.insert(0, "상세조회", False)
        if is_admin: edit_view.insert(1, "삭제", False)
        
        col_cfg = {
            "상세조회": st.column_config.CheckboxColumn("조회", width="small"),
            "진행상황": st.column_config.SelectboxColumn("진행상황", options=OP["status"], disabled=not is_admin),
            "완료자": st.column_config.SelectboxColumn("완료자", options=OP["processors"], disabled=not is_admin),
            "완료일": st.column_config.DateColumn("완료일", format="YYYY-MM-DD", disabled=not is_admin),
            "거래명세표": st.column_config.LinkColumn("거래명세표🔗", display_text="파일", disabled=not is_req),
            "sheet_row": None
        }
        if is_admin: col_cfg["삭제"] = st.column_config.CheckboxColumn("삭제", width="small")

        edited_df = st.data_editor(edit_view, column_config=col_cfg, hide_index=True, use_container_width=True, height=500, key="editor_final")

        # 저장 버튼
        if (is_admin or is_req) and st.button("💾 변경사항 최종 반영하기", use_container_width=True):
            try:
                ws = get_spreadsheet().worksheet("New_stop")
                all_data = ws.get_all_values()
                headers = all_data[0]
                
                # 삭제 대상 수집
                deleted_rows = []
                if is_admin and "삭제" in edited_df.columns:
                    deleted_rows = edited_df[edited_df["삭제"] == True]['sheet_row'].tolist()

                # 수정 대상 반영
                for _, row in edited_df[edited_df.get("삭제", False) == False].iterrows():
                    r_idx = int(row['sheet_row']) - 1
                    if is_admin:
                        for f in ["진행상황", "완료자", "완료일"]:
                            if f in headers: all_data[r_idx][headers.index(f)] = str(row[f])
                    if is_req:
                        for f in ["신청구분", "신청자", "제품코드1", "거래명세표"]:
                            if f in headers: all_data[r_idx][headers.index(f)] = str(row[f])
                
                # 역순 삭제
                for r_num in sorted([int(r) for r in deleted_rows], reverse=True): del all_data[r_num-1]
                
                ws.clear(); ws.update('A1', all_data)
                st.success("저장 완료!"); st.cache_data.clear(); st.rerun()
            except Exception as e: st.error(f"저장 중 오류: {e}")

# [2-7] 신청서 섹션 (모든 필드 100% 복구)
elif st.session_state.active_menu in ["사용중지", "신규입고", "대체입고", "삭제코드변경", "단가인하▼", "단가인상▲"]:
    curr = st.session_state.active_menu
    d = {}
    st.markdown(f'<div class="section-header">{curr} 신청</div>', unsafe_allow_html=True)
    
    edi1 = st.text_input(f"대상 제품코드 입력", key=f"t_edi1")
    d.update(render_drug_table(edi1, 1))
    
    if curr == "사용중지":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"] = c1.selectbox("원내구분1", OP["io"], key="t1_io")
        d["급여구분1"] = c2.selectbox("급여구분1", ["급여", "비급여"], key="t1_pay")
        d["구입처1"] = c3.text_input("구입처1", key="t1_vd")
        d["개당입고가1"] = c4.text_input("개당입고가1", key="t1_pr")
        c5, c6, c7, c8 = st.columns(4)
        d["사용중지일1"] = c5.date_input("사용중지일1", key="t1_sd").strftime('%Y-%m-%d')
        d["사용중지사유1"] = c6.selectbox("사용중지사유1", OP["reason"], key="t1_rs")
        d["사용중지사유_기타1"] = c7.text_input("사용중지사유_기타1", key="t1_ers", disabled=(d["사용중지사유1"] != "기타"))
        d["재고여부1"] = c8.selectbox("재고여부1", ["유", "무"], key="t1_syn")
        has_stock = (d["재고여부1"] == "유")
        c9, c10, c11, c12 = st.columns(4)
        d["재고처리방법1"] = c9.selectbox("재고처리방법1", OP["stock_mth"], key="t1_mth", disabled=not has_stock)
        d["재고량1"] = c10.number_input("재고량1", 0, key="t1_vol", disabled=not has_stock)
        d["반품가능여부1"] = c11.selectbox("반품가능여부1", OP["possible"], key="t1_pyn", disabled=not has_stock)
        d["반품예정일1"] = c12.date_input("반품예정일1", key="t1_rd", disabled=not has_stock).strftime('%Y-%m-%d')
        d["반품량1"] = st.number_input("반품량1", 0, key="t1_rv", disabled=not has_stock)

    elif curr == "신규입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP["io"], key="t2_io"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t2_pay"), c3.text_input("구입처1", key="t2_vd"), c4.text_input("개당입고가1", key="t2_pr")
        c5, c6, c7, c8 = st.columns(4)
        d["입고요청진료과1"], d["원내유무(동일성분)1"], d["입고요청사유1"], d["사용기간1"] = c5.selectbox("입고요청진료과1", OP["dept"], key="t2_dp"), c6.selectbox("원내유무(동일성분)1", ["유", "무"], key="t2_sm"), c7.selectbox("입고요청사유1", OP["reason"], key="t2_rs"), c8.selectbox("사용기간1", OP["period"], key="t2_pd")
        c9, c10, c11 = st.columns(3)
        d["입고일1"], d["코드사용시작일1"], d["상한가외입고사유1"] = c9.date_input("입고일1", key="t2_id").strftime('%Y-%m-%d'), c10.date_input("코드사용시작일1", key="t2_sd").strftime('%Y-%m-%d'), c11.text_input("상한가외입고사유1", key="t2_or")

    elif curr == "대체입고":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP["io"], key="t3_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t3_p1"), c3.text_input("구입처1", key="t3_v1"), c4.text_input("개당입고가1", key="t3_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["재고여부1"] = c5.selectbox("재고여부1", ["유", "무"], key="t3_s1")
        hs3 = (d["재고여부1"] == "유")
        d["재고처리방법1"], d["재고량1"], d["반품가능여부1"] = c6.selectbox("재고처리방법1", OP["stock_mth"], key="t3_m1", disabled=not hs3), c7.number_input("재고량1", 0, key="t3_sv1", disabled=not hs3), c8.selectbox("반품가능여부1", OP["possible"], key="t3_py1", disabled=not hs3)
        c9, c10, c11, c12 = st.columns(4)
        d["반품예정일1"], d["반품량1"], d["코드중지기준1"], d["사용중지일1"] = c9.date_input("반품예정일1", key="t3_rd1", disabled=not hs3).strftime('%Y-%m-%d'), c10.number_input("반품량1", 0, key="t3_rv1", disabled=not hs3), c11.selectbox("코드중지기준1", ["즉시", "재고소진후"], key="t3_cs1"), c12.date_input("사용중지일1", key="t3_sd1").strftime('%Y-%m-%d')
        d["신규약제와병용사용1"] = st.selectbox("신규약제와병용사용1", OP["yn"], key="t3_co1")
        st.markdown('<div class="section-header">대체 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("대체 제품코드 입력", key="t3_edi2")
        d.update(render_drug_table(edi2, 2, "(대체약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP["io"], key="t3_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t3_p2"), c15.text_input("구입처2", key="t3_v2"), c16.text_input("개당입고가2", key="t3_pr2")
        c17, c18, c19, c20 = st.columns(4)
        d["입고요청사유2"], d["사용기간2"], d["입고일2"], d["코드사용시작일2"] = c17.selectbox("입고요청사유2", OP["reason"], key="t3_rs2"), c18.selectbox("사용기간2", OP["period"], key="t3_pd2"), c19.date_input("입고일2", key="t3_id2").strftime('%Y-%m-%d'), c20.date_input("코드사용시작일2", key="t3_ss2").strftime('%Y-%m-%d')
        cs1, cs2 = st.columns(2)
        d["기존약제와병용사용2"], d["상한가외입고사유2"] = cs1.selectbox("기존약제와병용사용2", OP["yn"], key="t3_co2"), cs2.text_input("상한가외입고사유2", key="t3_ov2")

    elif curr in ["삭제코드변경", "단가인하▼"]:
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP["io"], key="t_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t_p1"), c3.text_input("구입처1", key="t_v1"), c4.text_input("개당입고가1", key="t_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"], d["재고여부1"] = c5.selectbox("변경내용1", OP["change"], key="t_cn1"), c6.selectbox("재고여부1", ["유", "무"], key="t_s1")
        hst = (d["재고여부1"] == "유")
        d["재고처리방법1"], d["재고량1"] = c7.selectbox("재고처리방법1", OP["stock_mth"], key="t_m1", disabled=not hst), c8.number_input("재고량1", 0, key="t_sv1", disabled=not hst)
        c9, c10, c11, c12 = st.columns(4)
        d["반품가능여부1"] = c9.selectbox("반품가능여부1", OP["possible"], key="t_py1", disabled=not hst)
        d["반품불가사유1"] = c10.text_input("반품불가사유1 (필수)", key="t_nrs1", disabled=not (hst and d["반품가능여부1"] == "불가"))
        d["반품예정일1"], d["반품량1"] = c11.date_input("반품예정일1", key="t_rd1", disabled=not hst).strftime('%Y-%m-%d'), c12.number_input("반품량1", 0, key="t_rv1", disabled=not hst)
        st.markdown('<div class="section-header">변경 약제 정보</div>', unsafe_allow_html=True)
        edi2 = st.text_input("변경 제품코드 입력", key="t_edi2")
        d.update(render_drug_table(edi2, 2, "(변경약제)"))
        c13, c14, c15, c16 = st.columns(4)
        d["원내구분2"], d["급여구분2"], d["구입처2"], d["개당입고가2"] = c13.selectbox("원내구분2", OP["io"], key="t_o2"), c14.selectbox("급여구분2", ["급여", "비급여"], key="t_p2"), c15.text_input("구입처2", key="t_v2"), c16.text_input("개당입고가2", key="t_pr2")
        c17, c18 = st.columns(2)
        d["코드사용시작일2"], d["상한가외입고사유2"] = c17.date_input("코드사용시작일2", key="t_ss2").strftime('%Y-%m-%d'), c18.text_input("상한가외입고사유2", key="t_ov2")

    elif curr == "단가인상▲":
        c1, c2, c3, c4 = st.columns(4)
        d["원내구분1"], d["급여구분1"], d["구입처1"], d["개당입고가1"] = c1.selectbox("원내구분1", OP["io"], key="t6_o1"), c2.selectbox("급여구분1", ["급여", "비급여"], key="t6_p1"), c3.text_input("구입처1", key="t6_v1"), c4.text_input("개당입고가1", key="t6_pr1")
        c5, c6, c7, c8 = st.columns(4)
        d["변경내용1"], d["사용중지일1"], d["입고일1"], d["인상전입고가1"] = c5.selectbox("변경내용1", OP["change"], key="t6_cn1"), c6.date_input("품절일1", key="t6_sd1").strftime('%Y-%m-%d'), c7.date_input("재입고일자1", key="t6_id1").strftime('%Y-%m-%d'), c8.text_input("인상전입고가1", key="t6_pre_pr")
        d["코드사용시작일1"] = st.date_input("코드사용시작일1", key="t6_ss1").strftime('%Y-%m-%d')

    st.divider()
    col_memo, col_file = st.columns([2, 1])
    with col_memo: d["비고(기타 요청사항)"] = st.text_area("비고(기타 요청사항)", key="final_memo")
    with col_file: d["거래명세표"] = st.text_input("거래명세표 URL", placeholder="http://...", key="final_file")
    if st.button(f"🚀 {curr} 제출", key="final_btn", use_container_width=True): handle_safe_submit(curr, d)

# [8] 약가조회
elif st.session_state.active_menu == "🔍 약가조회":
    st.markdown('<div class="section-header">🔍 약가 상세 정보 조회</div>', unsafe_allow_html=True)
    s_edi = st.text_input("조회할 제품코드 입력 (9자리)", key="search_edi")
    if s_edi:
        m = get_drug_info(s_edi)
        if m:
            disp_fields = ["연번", "제품코드", "제품명", "업체명", "규격", "단위", "상한금액", "전일", "투여", "분류", "식약분류", "주성분코드", "주성분명"]
            cols = st.columns(4)
            for idx, field in enumerate(disp_fields):
                with cols[idx % 4]:
                    val = m.get(field, "-")
                    if field == "상한금액" and val != "-":
                        try: val = "{:,} 원".format(int(str(val).replace(',', '')))
                        except: pass
                    st.markdown(f'<div class="detail-card"><div class="detail-label">{field}</div><div class="detail-value">{val}</div></div>', unsafe_allow_html=True)
        else: st.error("제품코드를 찾을 수 없습니다.")
