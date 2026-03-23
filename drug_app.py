import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    /* 사이드바 배경색 및 간격 초밀착 조정 */
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: 0.3rem !important; padding-top: 1rem !important; }
    
    /* 사이드바 내부 위젯 간격 줄이기 */
    .st-emotion-cache-16idsys p { margin-bottom: 0px !important; }
    div[data-testid="stVerticalBlock"] > div { margin-top: -5px !important; }
    
    /* 제목 스타일 */
    .main-header { font-size: 1.6rem; font-weight: 800; color: #1E3A8A; margin-bottom: 15px; }
    
    /* 탭 디자인 */
    .stTabs [data-baseweb="tab-list"] { gap: 5px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f1f3f5; border-radius: 5px; padding: 8px 15px; font-weight: bold; color: #495057; font-size: 0.9rem;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    
    /* 섹션 제목 스타일 (박스 제거 후 간결하게) */
    .section-title { 
        font-size: 1.0rem; font-weight: bold; color: #1E40AF; 
        margin: 20px 0 10px 0; border-bottom: 2px solid #e9ecef; padding-bottom: 3px;
    }
    
    /* 입력 필드 높이 살짝 조절 */
    div[data-baseweb="input"] { min-height: 35px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 조회 함수 ---
@st.cache_data
def get_drug_info(edi_code):
    if not edi_code: return {}
    try:
        df = pd.read_csv("drug_master.csv", encoding='utf-8-sig')
        df.columns = [c.strip() for c in df.columns] 
        
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        target = df[df['제품코드'] == str(edi_code).strip()]
        
        if not target.empty:
            res = target.iloc[0]
            col_price = next((c for c in df.columns if '상한금액' in c), None)
            price_val = str(res[col_price]).replace(',', '').strip() if col_price else "0"
            col_date = next((c for c in df.columns if '전일' in c), None)
            date_val = str(res[col_date]).strip() if col_date else ""

            return {
                "name": res.get('제품명', ''),
                "comp": res.get('업체명', ''),
                "price": price_val,
                "spec": f"{res.get('규격', '')} {res.get('단위', '')}".strip(),
                "date": date_val
            }
    except: return {}
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

# --- 4. 왼쪽 사이드바 (신청자 정보 우선 + 간격 축소) ---
with st.sidebar:
    st.markdown("### HISMEDI † Drug Service")
    
    # [1] 신청자 정보 (최상단)
    st.markdown("**📋 신청자 정보**")
    app_user = st.text_input("성명", placeholder="성명 입력", label_visibility="collapsed", key="u_name")
    app_date = st.date_input("신청일", datetime.now(), label_visibility="collapsed", key="u_date").strftime('%Y-%m-%d')
    app_status = st.selectbox("진행상황", ["신청완료", "처리중", "처리완료"], label_visibility="collapsed", key="u_stat")
    app_remark = st.text_area("비고(공통)", placeholder="기타 요청사항", label_visibility="collapsed", key="u_rem", height=80)
    
    st.markdown("<hr style='margin: 10px 0;'>", unsafe_allow_html=True) # 좁은 구분선
    
    # [2] EDI 정보 조회기 (하단)
    st.markdown("**🔍 EDI 정보 조회기**")
    search_code = st.text_input("제품코드", placeholder="조회용 코드 입력", label_visibility="collapsed", key="sb_search")
    if search_code:
        s_res = get_drug_info(search_code)
        if s_res:
            st.info(f"**{s_res['name']}**\n\n{s_res['comp']}\n\n{s_res['price']}원 | {s_res['spec']}")
        else: st.warning("정보 없음")

# --- 5. 공통 함수 및 입력 헬퍼 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    if sheet:
        row_data[1], row_data[2], row_data[54], row_data[55] = app_date, app_user, app_remark, app_status
        sheet.append_row(row_data)
        st.success("데이터베이스에 저장되었습니다."); st.balloons()

def render_drug_input(prefix, title, key_id):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi)
    
    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일", value=m.get("date", ""), key=f"dt_{key_id}")
    return [edi, name, comp, "", price, "", date, spec]

# --- 6. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

# ① 사용중지
with tabs[0]:
    st.markdown('<div class="main-header">① 사용중지 신청</div>', unsafe_allow_html=True)
    data = [""] * 56
    data[0] = "사용중지"
    res = render_drug_input("중지", "중지 약제 정보", "t1")
    data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
    
    cc1, cc2, cc3 = st.columns(3)
    data[13] = cc1.date_input("사용중지일", key="t1_d1").strftime('%Y-%m-%d')
    data[14] = cc2.selectbox("중지 사유", ["생산중단", "자진취하", "대체발생", "기타"], key="t1_s1")
    data[19] = cc3.text_input("현재 재고량", key="t1_v1")
    
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🚀 사용중지 신청 제출", use_container_width=True): handle_submit(data)

# ② 신규입고
with tabs[1]:
    st.markdown('<div class="main-header">② 신규입고 신청</div>', unsafe_allow_html=True)
    data = [""] * 56
    data[0] = "신규입고"
    res = render_drug_input("신규", "신규 입고 약제 정보", "t2")
    data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
    
    cc1, cc2, cc3 = st.columns(3)
    data[28] = cc1.text_input("요청 진료과", key="t2_v1")
    data[31] = cc2.date_input("입고 희망일", key="t2_v2").strftime('%Y-%m-%d')
    data[33] = cc3.text_input("상한가 외 사유", key="t2_v3")
    
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🚀 신규입고 신청 제출", use_container_width=True): handle_submit(data)

# ③ 대체입고 ~ ⑥ 단가인상
for i, title in enumerate(["대체입고", "급여코드변경", "단가인하", "단가인상"], start=2):
    with tabs[i]:
        st.markdown(f'<div class="main-header">{i+1}. {title} 신청</div>', unsafe_allow_html=True)
        data = [""] * 56
        data[0] = title
        
        # 기존 정보
        res1 = render_drug_input("기존/이전", "기존 약제 정보", f"t{i}_old")
        data[3], data[4], data[5], data[7], data[10], data[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
        
        # 대체 정보
        res2 = render_drug_input("대체/변경", "대체 약제 정보", f"t{i}_new")
        data[36], data[37], data[38], data[40], data[43], data[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
        
        st.markdown('<div class="section-title">추가 정보</div>', unsafe_allow_html=True)
        data[48] = st.text_area("변경 및 입고 요청 사유", key=f"t{i}_area", height=100)
        
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button(f"🚀 {title} 신청 제출", use_container_width=True): handle_submit(data)
