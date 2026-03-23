import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    /* 사이드바 스타일링 */
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f0f2f6; border-radius: 5px; padding: 10px 15px; font-weight: bold;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    .section-box { background-color: #ffffff; padding: 20px; border-radius: 10px; border: 1px solid #dee2e6; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    .section-title { font-size: 1.1rem; font-weight: bold; color: #1E40AF; margin-bottom: 15px; border-left: 5px solid #1E40AF; padding-left: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 심평원 원본 CSV 데이터 조회 함수 ---
@st.cache_data
def get_drug_info(edi_code):
    if not edi_code: return {}
    try:
        # CSV 로드 및 헤더 공백 제거
        df = pd.read_csv("drug_master.csv", encoding='utf-8-sig')
        df.columns = df.columns.str.strip()
        
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        target = df[df['제품코드'] == str(edi_code).strip()]
        
        if not target.empty:
            res = target.iloc[0]
            # 상한금액에서 콤마 제거 및 공백 제거
            price_raw = str(res.get('상한금액', '0')).replace(',', '').strip()
            return {
                "name": res.get('제품명', ''),
                "comp": res.get('업체명', ''),
                "price": price_raw,
                "spec": f"{res.get('규격', '')} {res.get('단위', '')}".strip(),
                "date": str(res.get('전일', ''))
            }
    except Exception as e:
        return {"error": str(e)}
    return {}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_sheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except: return None

sheet = get_sheet()

# --- 4. 왼쪽 사이드바 (제목 + 공통 정보 + EDI 조회기) ---
with st.sidebar:
    st.markdown("# 💊 HISMEDI\n### Drug Service")
    st.divider()
    
    # [1] EDI 빠른 조회기 (저장 목적 아님)
    st.subheader("🔍 EDI 정보 조회기")
    search_code = st.text_input("제품코드 입력", placeholder="예: 648500030", key="sidebar_search")
    if search_code:
        s_res = get_drug_info(search_code)
        if "error" in s_res: st.error(f"오류: {s_res['error']}")
        elif s_res:
            st.success(f"**{s_res['name']}**")
            st.caption(f"업체: {s_res['comp']}")
            st.caption(f"금액: {s_res['price']}원 | 규격: {s_res['spec']}")
        else: st.warning("정보를 찾을 수 없습니다.")
    
    st.divider()
    
    # [2] 신청 공통 정보
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명", placeholder="성명을 입력하세요")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통 요청사항)")

# --- 5. 공통 저장 함수 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    if sheet:
        row_data[1] = app_date    # B열
        row_data[2] = app_user    # C열
        row_data[54] = app_remark # BC열
        row_data[55] = app_status # BD열
        try:
            sheet.append_row(row_data)
            st.success("데이터베이스에 성공적으로 저장되었습니다!"); st.balloons()
        except Exception as e:
            st.error(f"저장 중 오류 발생: {e}")

# --- 6. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

# [탭별 공통 입력 헬퍼]
def render_drug_info_section(prefix, title, key_id):
    st.markdown(f'<div class="section-box"><div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드 (제품코드)", key=f"edi_{key_id}")
    m = get_drug_info(edi)
    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일(전일)", value=m.get("date", ""), key=f"dt_{key_id}")
    st.markdown('</div>', unsafe_allow_html=True)
    return edi, name, comp, price, spec, date

# --- ① 사용중지 ---
with tabs[0]:
    st.markdown("### ① 사용중지 신청")
    data0 = [""] * 56
    data0[0] = "사용중지"
    edi, nm, cp, pr, sp, dt = render_drug_info_section("중지", "중지 약제 정보 (D~M열)", "tab0")
    data0[3], data0[4], data0[5], data0[7], data0[10], data0[9] = edi, nm, cp, pr, sp, dt
    
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    sc1, sc2, sc3 = st.columns(3)
    data0[13] = sc1.date_input("사용중지일", key="stop_d").strftime('%Y-%m-%d')
    data0[14] = sc2.selectbox("사유", ["생산중단", "자진취하", "대체발생", "기타"], key="stop_r")
    data0[19] = sc3.text_input("현재 재고량", key="stop_v")
    st.markdown('</div>', unsafe_allow_html=True)
    if st.button("🚀 사용중지 신청서 제출"): handle_submit(data0)

# --- ② 신규입고 ---
with tabs[1]:
    st.markdown("### ② 신규입고 신청")
    data1 = [""] * 56
    data1[0] = "신규입고"
    edi, nm, cp, pr, sp, dt = render_drug_info_section("신규", "신입고 약제 정보 (D~M열)", "tab1")
    data1[3], data1[4], data1[5], data1[7], data1[10], data1[9] = edi, nm, cp, pr, sp, dt
    
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    nc1, nc2, nc3 = st.columns(3)
    data1[28] = nc1.text_input("요청 진료과", key="new_dept")
    data1[31] = nc2.date_input("입고 희망일", key="new_in").strftime('%Y-%m-%d')
    data1[33] = nc3.text_input("상한가외 입고사유", key="new_extra")
    st.markdown('</div>', unsafe_allow_html=True)
    if st.button("🚀 신규입고 신청서 제출"): handle_submit(data1)

# --- ③ 대체입고 ---
with tabs[2]:
    st.markdown("### ③ 대체입고 신청")
    data2 = [""] * 56
    data2[0] = "대체입고"
    # 기존 약제 (D~M열)
    edi1, nm1, cp1, pr1, sp1, dt1 = render_drug_info_section("기존", "기존 약제 (D~M열)", "tab2_old")
    data2[3], data2[4], data2[5], data2[7], data2[10], data2[9] = edi1, nm1, cp1, pr1, sp1, dt1
    # 대체 약제 (AK~AT열)
    edi2, nm2, cp2, pr2, sp2, dt2 = render_drug_info_section("대체", "대체 약제 (AK~AT열)", "tab2_new")
    data2[36], data2[37], data2[38], data2[40], data2[43], data2[42] = edi2, nm2, cp2, pr2, sp2, dt2
    
    data2[48] = st.text_area("입고 요청 사유 (AW열)", key="tab2_reason")
    if st.button("🚀 대체입고 신청서 제출"): handle_submit(data2)

# --- ④ 급여코드변경 ---
with tabs[3]:
    st.markdown("### ④ 급여코드변경 신청")
    data3 = [""] * 56
    data3[0] = "급여코드변경"
    edi1, nm1, cp1, pr1, sp1, dt1 = render_drug_info_section("이전", "이전 코드 정보 (D~M열)", "tab3_old")
    data3[3], data3[4], data3[5], data3[7], data3[10], data3[9] = edi1, nm1, cp1, pr1, sp1, dt1
    edi2, nm2, cp2, pr2, sp2, dt2 = render_drug_info_section("변경", "변경 코드 정보 (AK~AT열)", "tab3_new")
    data3[36], data3[37], data3[38], data3[40], data3[43], data3[42] = edi2, nm2, cp2, pr2, sp2, dt2
    
    if st.button("🚀 급여코드변경 신청서 제출"): handle_submit(data3)

# --- ⑤ 단가인하 ---
with tabs[4]:
    st.markdown("### ⑤ 단가인하 신청")
    data4 = [""] * 56
    data4[0] = "단가인하"
    edi1, nm1, cp1, pr1, sp1, dt1 = render_drug_info_section("이전", "이전 단가 (D~M열)", "tab4_old")
    data4[3], data4[4], data4[5], data4[7], data4[10], data4[9] = edi1, nm1, cp1, pr1, sp1, dt1
    edi2, nm2, cp2, pr2, sp2, dt2 = render_drug_info_section("인하", "인하 단가 (AK~AT열)", "tab4_new")
    data4[36], data4[37], data4[38], data4[40], data4[43], data4[42] = edi2, nm2, cp2, pr2, sp2, dt2
    
    if st.button("🚀 단가인하 신청서 제출"): handle_submit(data4)

# --- ⑥ 단가인상 ---
with tabs[5]:
    st.markdown("### ⑥ 단가인상 신청")
    data5 = [""] * 56
    data5[0] = "단가인상"
    edi1, nm1, cp1, pr1, sp1, dt1 = render_drug_info_section("이전", "이전 단가 (D~M열)", "tab5_old")
    data5[3], data5[4], data5[5], data5[7], data5[10], data5[9] = edi1, nm1, cp1, pr1, sp1, dt1
    edi2, nm2, cp2, pr2, sp2, dt2 = render_drug_info_section("인상", "인상 단가 (AK~AT열)", "tab5_new")
    data5[36], data5[37], data5[38], data5[40], data5[43], data5[42] = edi2, nm2, cp2, pr2, sp2, dt2
    
    if st.button("🚀 단가인상 신청서 제출"): handle_submit(data5)
