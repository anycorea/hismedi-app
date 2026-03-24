import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 커스텀 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 전체 배경 및 메인 상단 여백 조정 */
    .block-container { padding-top: 3rem !important; }
    
    /* 사이드바 제목 및 간격 */
    .sidebar-title { 
        font-size: 1.6rem; font-weight: 800; color: #1E3A8A; 
        margin-bottom: 25px; /* 제목과 아래 항목 간격 확보 */
        line-height: 1.2;
    }
    [data-testid="stSidebar"] hr { margin: 20px 0px !important; }

    /* 메인 헤더 */
    .main-header { font-size: 2rem; font-weight: 800; color: #1E3A8A; margin-bottom: 30px; }

    /* 탭 디자인 전면 개편 (세련된 필 버튼 스타일) */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 12px; 
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] { 
        height: 45px; 
        min-width: 120px;
        background-color: #F1F5F9; 
        border-radius: 30px !important; /* 둥근 버튼 모양 */
        border: none !important;
        font-weight: 600; 
        color: #64748B;
        padding: 0 25px;
        transition: all 0.2s ease;
    }
    /* 탭 호버 효과 */
    .stTabs [data-baseweb="tab"]:hover { background-color: #E2E8F0; color: #1E3A8A; }
    /* 활성화된 탭 */
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; 
        color: white !important; 
        box-shadow: 0 4px 12px rgba(30, 58, 138, 0.25);
    }
    /* 탭 아래 기본 밑줄 제거 */
    .stTabs [data-baseweb="tab-border"] { display: none; }

    /* 입력 섹션 구분선 */
    .section-label { 
        font-size: 1rem; font-weight: 700; color: #334155; 
        margin: 30px 0 15px 0; border-left: 5px solid #1E3A8A; padding-left: 12px;
    }
    
    /* 조회 결과 레이아웃 일관성 유지용 */
    .info-box {
        background-color: #F8FAFC; padding: 20px; border-radius: 12px; border: 1px solid #E2E8F0;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 데이터 처리 함수 ---
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
        res['상한금액'] = str(res.get('상한금액', '0')).replace(',', '').strip()
        return res
    return {}

# --- 3. 왼쪽 사이드바 (구조 조정) ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI †<br>Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    
    # 신청 정보
    st.subheader("📝 접수 정보")
    app_user = st.text_input("신청자 성명", placeholder="입력하세요", key="sb_app_user")
    app_date = st.date_input("신청 일자", datetime.now(), key="sb_app_date").strftime('%Y-%m-%d')
    
    st.divider()
    
    # 처리 정보 (새로 추가)
    st.subheader("✅ 처리 정보")
    comp_user = st.text_input("완료자(담당자)", placeholder="입력하세요", key="sb_comp_user")
    app_status = st.selectbox("현재 진행상황", ["신청완료", "처리중", "처리완료"], index=0)
    app_remark = st.text_area("공통 비고", placeholder="특이사항 기록", key="sb_remark")

# --- 4. 메인 화면 구성 ---
st.markdown('<div class="main-header">약무 서비스 관리 시스템</div>', unsafe_allow_html=True)

# 세련된 탭 구성 (번호 제거)
tabs = st.tabs(["사용중지", "신규입고", "대체입고", "급여변경", "단가인하", "단가인상", "약가조회"])

def render_drug_form(prefix, title, key_id, is_readonly=False):
    """표준화된 3열 그리드 폼 (신청서 및 조회 공용)"""
    st.markdown(f'<div class="section-label">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi, master_df)
    
    # 조회 결과 매핑
    nm_val = m.get("제품명", "")
    cp_val = m.get("업체명", "")
    pr_val = m.get("상한금액", "")
    sp_val = f"{m.get('규격','')} {m.get('단위','')}".strip()
    dt_val = m.get("전일", "")
    
    c1, c2, c3 = st.columns([1, 2, 1])
    name = c2.text_input(f"{prefix} 제품명", value=nm_val, key=f"nm_{key_id}", disabled=is_readonly)
    comp = c3.text_input(f"{prefix} 업체명", value=cp_val, key=f"cp_{key_id}", disabled=is_readonly)
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=pr_val, key=f"pr_{key_id}", disabled=is_readonly)
    spec = c5.text_input(f"{prefix} 규격_단위", value=sp_val, key=f"sp_{key_id}", disabled=is_readonly)
    date = c6.text_input(f"{prefix} 적용일자", value=dt_val, key=f"dt_{key_id}", disabled=is_readonly)
    
    return [edi, name, comp, price, spec, date]

# --- [1~6번 탭] 신청서 양식 ---
for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여변경", "단가인하", "단가인상"]):
    with tabs[i]:
        row = [""] * 60 # 넉넉히 60열 확보 (완료자 등 추가 대비)
        row[0] = title
        
        if i < 2: # 단일 정보
            res = render_drug_form("신청", "기본 약제 정보", f"tab_{i}")
            # 시트 위치 매핑 (사용자 이전 요청 기준 준수)
            row[3], row[4], row[5], row[7], row[10], row[9] = res[0], res[1], res[2], res[3], res[4], res[5]
        else: # 비포/애프터 정보
            res1 = render_drug_form("기존", "기존 약제 정보", f"tab_{i}_old")
            row[3], row[4], row[5], row[7], row[10], row[9] = res1[0], res1[1], res1[2], res1[3], res1[4], res1[5]
            
            res2 = render_drug_form("변경", "변경/대체 약제 정보", f"tab_{i}_new")
            row[36], row[37], row[38], row[40], row[43], row[42] = res2[0], res2[1], res2[2], res2[3], res2[4], res2[5]
        
        st.divider()
        if st.button(f"🚀 {title} 신청서 제출", key=f"btn_{i}", use_container_width=True):
            # 저장 로직 (사이드바 정보 포함)
            try:
                ss = get_spreadsheet()
                ws = ss.worksheet(st.secrets["gsheet"]["worksheet_name"])
                row[1], row[2], row[54], row[55] = app_date, app_user, app_remark, app_status
                # 추가 데이터 (완료자 정보 등 - 시트헤더 수정에 따라 인덱스 조정 필요)
                # row[56] = comp_user 
                ws.append_row(row)
                st.success(f"{title} 접수가 완료되었습니다."); st.balloons()
            except Exception as e: st.error(f"저장 오류: {e}")

# --- [7번 탭] 약가 조회 (디자인 통일) ---
with tabs[6]:
    st.markdown('<div class="section-label">Master DB 약제 통합 조회</div>', unsafe_allow_html=True)
    c1, c2 = st.columns([1, 3])
    search_edi = c1.text_input("🔍 EDI 코드 입력", placeholder="예: 648500030", key="search_tab_edi")
    
    if search_edi:
        m = get_drug_info(search_edi, master_df)
        if m:
            # 1~6번 탭과 동일한 그리드 형식으로 정보 출력
            st.markdown('<div class="info-box">', unsafe_allow_html=True)
            
            # 첫 번째 줄 (투여, 제품명, 업체명)
            ca1, ca2, ca3 = st.columns([1, 2, 1])
            ca1.text_input("투여 경로", value=m.get("투여", ""), disabled=True, key="s_1")
            ca2.text_input("제품명(품목명)", value=m.get("제품명", ""), disabled=True, key="s_2")
            ca3.text_input("업체명", value=m.get("업체명", ""), disabled=True, key="s_3")
            
            # 두 번째 줄 (상한금액, 규격/단위, 적용일)
            cb1, cb2, cb3 = st.columns(3)
            cb1.text_input("상한금액", value=f"{m.get('상한금액','0')} 원", disabled=True, key="s_4")
            cb2.text_input("규격 및 단위", value=f"{m.get('규격','')} {m.get('단위','')}", disabled=True, key="s_5")
            cb3.text_input("적용일(전일)", value=m.get("전일", ""), disabled=True, key="s_6")
            
            # 세 번째 줄 (주성분명, 비고)
            cc1, cc2 = st.columns([2, 2])
            cc1.text_input("주성분명", value=m.get("주성분명", ""), disabled=True, key="s_7")
            cc2.text_input("비고", value=m.get("비고", ""), disabled=True, key="s_8")
            
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.error("해당 코드가 Master DB에 존재하지 않습니다.")
