import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 고급 CSS 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    /* 전체 상단 여백 제거 및 제목 잘림 방지 */
    .block-container { padding-top: 1.5rem !important; padding-bottom: 0rem !important; }
    [data-testid="stHeader"] { background: rgba(0,0,0,0); }
    
    /* 사이드바 제목 및 간격 조절 */
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: 0.5rem !important; }
    .sidebar-title { font-size: 1.4rem; font-weight: 800; color: #1E3A8A; margin-bottom: -10px; white-space: nowrap; }
    hr { margin: 5px 0px !important; padding: 0px !important; }

    /* 메인 타이틀 */
    .main-header { font-size: 1.8rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; border-left: 8px solid #1E3A8A; padding-left: 15px; }

    /* 탭 디자인 개선 (세련된 블루 테마) */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; width: 100%; }
    .stTabs [data-baseweb="tab"] { 
        height: 50px; 
        width: 13.5%; /* 탭 7개를 위해 넓게 설정 */
        background-color: #F8FAFC; 
        border-radius: 8px 8px 0px 0px; 
        border: 1px solid #E2E8F0;
        font-weight: 700; 
        color: #64748B;
        transition: all 0.3s;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; 
        color: white !important; 
        border: 1px solid #1E3A8A !important;
        box-shadow: 0px 4px 10px rgba(30, 58, 138, 0.2);
    }
    .stTabs [data-baseweb="tab"]:hover { background-color: #E2E8F0; color: #1E3A8A; }

    /* 입력 필드 섹션 타이틀 */
    .section-title { 
        font-size: 1.05rem; font-weight: bold; color: #1E40AF; 
        margin: 20px 0 10px 0; border-bottom: 2px solid #E2E8F0; padding-bottom: 5px;
    }

    /* 약가 조회 결과 카드 스타일 */
    .search-card {
        background-color: #F1F5F9; padding: 20px; border-radius: 12px; border: 1px solid #CBD5E1; margin-top: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 로드 및 캐싱 (Master 시트) ---
@st.cache_resource
def get_spreadsheet():
    creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                 scopes=["https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"])

@st.cache_data(ttl=3600)
def load_master_data():
    try:
        ss = get_spreadsheet()
        master_ws = ss.worksheet("Master")
        data = master_ws.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        df.columns = [c.strip() for c in df.columns]
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        return df
    except: return pd.DataFrame()

master_df = load_master_data()

def get_drug_info(edi_code, df):
    if not edi_code or df.empty: return {}
    target = df[df['제품코드'] == str(edi_code).strip()]
    if not target.empty:
        res = target.iloc[0].to_dict()
        # 상한금액 콤마 제거
        res['상한금액'] = str(res.get('상한금액', '0')).replace(',', '').strip()
        return res
    return {}

# --- 3. 왼쪽 사이드바 ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI † Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명", placeholder="성명 입력")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통 요청사항)")

# --- 4. 공통 함수 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    try:
        ss = get_spreadsheet()
        main_ws = ss.worksheet(st.secrets["gsheet"]["worksheet_name"])
        row_data[1], row_data[2], row_data[54], row_data[55] = app_date, app_user, app_remark, app_status
        main_ws.append_row(row_data)
        st.success("데이터베이스에 성공적으로 저장되었습니다."); st.balloons()
    except Exception as e: st.error(f"저장 오류: {e}")

def render_drug_input(prefix, title, key_id):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi, master_df)
    
    name = c2.text_input(f"{prefix} 제품명", value=m.get("제품명", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("업체명", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("상한금액", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=f"{m.get('규격','')} {m.get('단위','')}".strip(), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일(전일)", value=m.get("전일", ""), key=f"dt_{key_id}")
    return [edi, name, comp, "", price, "", date, spec]

# --- 5. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여변경", "⑤ 단가인하", "⑥ 단가인상", "⑦ 약가 조회"])

# 탭 1~6: 신청서 양식
for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하", "단가인상"]):
    with tabs[i]:
        st.markdown(f'<div class="main-header">{i+1}. {title} 신청</div>', unsafe_allow_html=True)
        data = [""] * 56
        data[0] = title
        if i < 2:
            res = render_drug_input("신청", "약제 정보", f"t{i}")
            data[3], data[4], data[5], data[7], data[10], data[9] = res[0], res[1], res[2], res[4], res[7], res[6]
        else:
            res1 = render_drug_input("기존", "기존 약제 정보", f"t{i}_old")
            data[3], data[4], data[5], data[7], data[10], data[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
            res2 = render_drug_input("대체", "대체/변경 정보", f"t{i}_new")
            data[36], data[37], data[38], data[40], data[43], data[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
        
        st.divider()
        if st.button(f"🚀 {title} 신청 제출", key=f"btn_{i}", use_container_width=True): handle_submit(data)

# 탭 7: EDI 정보 조회기 (메인 영역)
with tabs[6]:
    st.markdown('<div class="main-header">⑦ 마스터 약가 조회</div>', unsafe_allow_html=True)
    st.info("Master 시트에 등록된 20,000여 개의 약제 정보를 실시간으로 검색합니다.")
    
    search_edi = st.text_input("🔍 제품코드(EDI)를 입력하세요", placeholder="예: 648500030", key="main_search")
    
    if search_edi:
        res = get_drug_info(search_edi, master_df)
        if res:
            st.markdown('<div class="search-card">', unsafe_allow_html=True)
            # 요청하신 10개 항목 출력
            col_a, col_b = st.columns(2)
            with col_a:
                st.write(f"**투여:** {res.get('투여', '-')}")
                st.write(f"**주성분명:** {res.get('주성분명', '-')}")
                st.write(f"**제품코드:** {res.get('제품코드', '-')}")
                st.write(f"**제품명:** :blue[{res.get('제품명', '-')}]")
                st.write(f"**업체명:** {res.get('업체명', '-')}")
            with col_b:
                st.write(f"**규격:** {res.get('규격', '-')}")
                st.write(f"**단위:** {res.get('단위', '-')}")
                st.write(f"**상한금액:** :red[{res.get('상한금액', '0')}] 원")
                st.write(f"**적용일(전일):** {res.get('전일', '-')}")
                st.write(f"**비고:** {res.get('비고', '-')}")
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.error("해당 코드를 Master 시트에서 찾을 수 없습니다.")

    st.divider()
    st.caption("※ Master 데이터 업데이트는 구글 시트의 'Master' 탭에서 진행하세요.")
