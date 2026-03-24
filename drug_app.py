import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    .main-header { font-size: 1.8rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { background-color: #f1f3f5; border-radius: 5px; font-weight: bold; }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    .section-title { font-size: 1.05rem; font-weight: bold; color: #1E40AF; margin: 25px 0 15px 0; border-bottom: 2px solid #e9ecef; padding-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 구글 시트 연결 및 데이터 로드 (캐싱 적용) ---
@st.cache_resource
def get_spreadsheet():
    """구글 시트 파일 자체를 연결"""
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    client = gspread.authorize(creds)
    return client.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=3600) # 20,000행 데이터를 1시간 동안 메모리에 저장
def load_master_data():
    """Master 시트의 20,000행 데이터를 한꺼번에 읽어와서 검색 준비"""
    try:
        ss = get_spreadsheet()
        master_ws = ss.worksheet("Master")
        # 모든 데이터를 가져와서 DataFrame으로 변환
        data = master_ws.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0]) # 첫 줄은 헤더
        
        # 헤더 공백 제거 (사용자님의 ' 상한금액 ' 공백 대응)
        df.columns = [c.strip() for c in df.columns]
        
        # 검색용 제품코드 인덱싱
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except Exception as e:
        st.error(f"Master 데이터 로드 실패: {e}")
        return pd.DataFrame()

def get_drug_info(edi_code, master_df):
    """메모리에 저장된 20,000개 데이터 중에서 빛의 속도로 검색"""
    if not edi_code or master_df.empty: return {}
    
    clean_code = str(edi_code).strip()
    target = master_df[master_df['제품코드'] == clean_code]
    
    if not target.empty:
        res = target.iloc[0]
        # 사용자님이 주신 헤더 명칭대로 데이터 추출
        return {
            "name": res.get('제품명', ''),
            "comp": res.get('업체명', ''),
            "price": str(res.get('상한금액', '0')).replace(',', '').strip(),
            "spec": f"{res.get('규격', '')} {res.get('단위', '')}".strip(),
            "date": res.get('전일', ''),
            "found": True
        }
    return {"found": False}

# 데이터 미리 로드
master_df = load_master_data()

# --- 3. 사이드바 (신청자 정보 및 조회기) ---
with st.sidebar:
    st.markdown("## HISMEDI † Drug Service")
    st.divider()
    
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명", placeholder="성명 입력")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통)")
    
    st.divider()
    st.subheader("🔍 EDI 정보 조회기")
    search_code = st.text_input("제품코드 입력", placeholder="Master 시트 검색", key="sb_search")
    if search_code:
        s_res = get_drug_info(search_code, master_df)
        if s_res.get("found"):
            st.info(f"**{s_res['name']}**\n\n{s_res['comp']}\n\n{s_res['price']}원 | {s_res['spec']}\n\n적용일: {s_res['date']}")
        else:
            st.warning("Master 시트에서 정보를 찾을 수 없습니다.")

# --- 4. 공통 함수 및 입력 헬퍼 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        main_ws = ss.worksheet(st.secrets["gsheet"]["worksheet_name"])
        row_data[1], row_data[2], row_data[54], row_data[55] = app_date, app_user, app_remark, app_status
        main_ws.append_row(row_data)
        st.success("데이터베이스에 저장되었습니다."); st.balloons()
    except Exception as e:
        st.error(f"저장 중 오류 발생: {e}")

def render_drug_input(prefix, title, key_id):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    
    m = get_drug_info(edi, master_df) if edi else {}
    
    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일", value=m.get("date", ""), key=f"dt_{key_id}")
    return [edi, name, comp, "", price, "", date, spec]

# --- 5. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하", "단가인상"]):
    with tabs[i]:
        st.markdown(f'<div class="main-header">{i+1}. {title} 신청</div>', unsafe_allow_html=True)
        data = [""] * 56
        data[0] = title
        
        if i < 2: # ①사용중지, ②신규입고
            res = render_drug_input("신청", f"{title} 약제 정보", f"t{i}")
            data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
            if i == 0:
                cc1, cc2 = st.columns(2)
                data[13] = cc1.date_input("사용중지일", key="t0_d").strftime('%Y-%m-%d')
                data[14] = cc2.selectbox("사유", ["생산중단", "자진취하", "대체발생", "기타"], key="t0_s")
        else: # ③대체입고 ~ ⑥단가변경
            res1 = render_drug_input("기존/이전", "기존 약제 정보", f"t{i}_old")
            data[3], data[4], data[5], data[7], data[10], data[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
            
            res2 = render_drug_input("대체/변경", "대체/변경 약제 정보", f"t{i}_new")
            data[36], data[37], data[38], data[40], data[43], data[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
            
            st.markdown('<div class="section-title">추가 정보</div>', unsafe_allow_html=True)
            data[48] = st.text_area("변경 및 입고 요청 사유", key=f"t{i}_area", height=100)
            
        st.divider()
        if st.button(f"🚀 {title} 신청 제출", key=f"btn_{i}", use_container_width=True):
            handle_submit(data)
