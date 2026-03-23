import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    /* 사이드바 배경색 및 구분선 */
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    
    /* 탭 디자인 */
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f0f2f6; border-radius: 5px; padding: 10px 15px; font-weight: bold; color: #495057;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    
    /* 입력창 박스 디자인 */
    .section-box { 
        background-color: #ffffff; padding: 20px; border-radius: 10px; 
        border: 1px solid #dee2e6; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); 
    }
    .section-title { 
        font-size: 1.1rem; font-weight: bold; color: #1E40AF; margin-bottom: 15px; 
        border-left: 5px solid #1E40AF; padding-left: 10px; 
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 심평원 원본 CSV 데이터 조회 함수 (공백 및 특수문자 완벽 대응) ---
@st.cache_data
def get_drug_info(edi_code):
    if not edi_code: return {}
    try:
        # CSV 로드
        df = pd.read_csv("drug_master.csv", encoding='utf-8-sig')
        
        # [핵심] 모든 헤더의 앞뒤 공백 및 보이지 않는 특수문자 제거
        df.columns = [c.strip() for c in df.columns]
        
        # 제품코드 열에서 검색 (문자열로 변환하여 비교)
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        target = df[df['제품코드'] == str(edi_code).strip()]
        
        if not target.empty:
            res = target.iloc[0]
            
            # 상한금액 열 찾기 (이름에 '상한금액'이 포함된 열 검색)
            col_price = next((c for c in df.columns if '상한금액' in c), None)
            price_val = str(res[col_price]).replace(',', '').strip() if col_price else "0"
            
            # 적용일자(전일) 열 찾기
            col_date = next((c for c in df.columns if '전일' in c), None)
            date_val = str(res[col_date]).strip() if col_date else ""

            return {
                "name": res.get('제품명', ''),
                "comp": res.get('업체명', ''),
                "price": price_val,
                "spec": f"{res.get('규격', '')} {res.get('단위', '')}".strip(),
                "date": date_val
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

# --- 4. 왼쪽 사이드바 (조회기 + 신청자 공통 정보) ---
with st.sidebar:
    st.markdown("# 💊 HISMEDI\n### Drug Service")
    st.divider()
    
    # [1] 사이드바 EDI 빠른 조회기
    st.subheader("🔍 EDI 정보 조회기")
    search_code = st.text_input("제품코드 입력", placeholder="예: 648500030", key="sb_search")
    if search_code:
        s_res = get_drug_info(search_code)
        if "error" in s_res: st.error(f"조회 실패: {s_res['error']}")
        elif s_res:
            st.success(f"**{s_res['name']}**")
            st.caption(f"업체: {s_res['comp']}")
            st.caption(f"상한금액: {s_res['price']}원")
            st.caption(f"규격: {s_res['spec']} | 적용일: {s_res['date']}")
        else: st.warning("정보를 찾을 수 없습니다.")
    
    st.divider()
    
    # [2] 신청 공통 정보
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통 요청사항)")

# --- 5. 공통 함수 및 매핑 헬퍼 ---
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
            st.error(f"저장 실패: {e}")

def render_drug_input(prefix, title, key_id):
    """약제 정보 입력 섹션 렌더링"""
    st.markdown(f'<div class="section-box"><div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi)
    
    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한가", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일", value=m.get("date", ""), key=f"dt_{key_id}")
    st.markdown('</div>', unsafe_allow_html=True)
    return [edi, name, comp, "", price, "", date, spec] # 매핑용 리스트

# --- 6. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

# ① 사용중지
with tabs[0]:
    st.markdown("### ① 사용중지 신청")
    data = [""] * 56
    data[0] = "사용중지"
    res = render_drug_input("중지", "중지 약제 정보 (D~M열 반영)", "t1")
    # D, E, F, H, K, J열 매핑
    data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
    
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    cc1, cc2, cc3 = st.columns(3)
    data[13] = cc1.date_input("사용중지일", key="t1_d1").strftime('%Y-%m-%d')
    data[14] = cc2.selectbox("중지 사유", ["생산중단", "자진취하", "대체품목발생", "기타"], key="t1_s1")
    data[19] = cc3.text_input("현재 재고량", key="t1_v1")
    st.markdown('</div>', unsafe_allow_html=True)
    if st.button("🚀 사용중지 신청서 제출", use_container_width=True): handle_submit(data)

# ② 신규입고
with tabs[1]:
    st.markdown("### ② 신규입고 신청")
    data = [""] * 56
    data[0] = "신규입고"
    res = render_drug_input("신규", "신규 입고 약제 정보 (D~M열 반영)", "t2")
    data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
    
    st.markdown('<div class="section-box">', unsafe_allow_html=True)
    cc1, cc2, cc3 = st.columns(3)
    data[28] = cc1.text_input("요청 진료과", key="t2_v1")
    data[31] = cc2.date_input("입고 희망일", key="t2_v2").strftime('%Y-%m-%d')
    data[33] = cc3.text_input("상한가 외 입고사유", key="t2_v3")
    st.markdown('</div>', unsafe_allow_html=True)
    if st.button("🚀 신규입고 신청서 제출", use_container_width=True): handle_submit(data)

# ③ 대체입고 / ④ 급여코드변경 / ⑤ 단가인하 / ⑥ 단가인상 (공통 구조)
for i, title in enumerate(["대체입고", "급여코드변경", "단가인하", "단가인상"], start=2):
    with tabs[i]:
        st.markdown(f"### {i+1}. {title} 신청")
        data = [""] * 56
        data[0] = title
        
        # [기존 약제 - D~M열]
        res1 = render_drug_input("기존/이전", f"기존 약제 정보 (D~M열 반영)", f"t{i}_old")
        data[3], data[4], data[5], data[7], data[10], data[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
        
        # [대체/신규 약제 - AK~AT열]
        res2 = render_drug_input("대체/변경", f"대체/변경 약제 정보 (AK~AT열 반영)", f"t{i}_new")
        data[36], data[37], data[38], data[40], data[43], data[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
        
        # 추가 사유
        data[48] = st.text_area("입고/변경 요청 사유 (AW열)", key=f"t{i}_area")
        
        if st.button(f"🚀 {title} 신청서 제출", use_container_width=True): handle_submit(data)
