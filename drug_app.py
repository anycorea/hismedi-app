import streamlit as st
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# --- 1. 페이지 설정 및 세련된 디자인(CSS) ---
st.set_page_config(page_title="HISMEDI Drug Service", layout="wide")

st.markdown("""
    <style>
    /* 상단 여백 및 메인 레이아웃 */
    .block-container { padding-top: 2.5rem !important; }
    
    /* 사이드바 제목 및 간격 */
    .sidebar-title { font-size: 1.6rem; font-weight: 800; color: #1E3A8A; margin-bottom: 25px; line-height: 1.2; }
    [data-testid="stSidebar"] hr { margin: 20px 0px !important; }

    /* 메인 헤더 */
    .main-header { font-size: 1.8rem; font-weight: 800; color: #1E3A8A; margin-bottom: 25px; }

    /* 7개 탭 디자인 (둥근 필 스타일) */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { 
        height: 48px; min-width: 120px; background-color: #F1F5F9; 
        border-radius: 12px !important; border: none !important;
        font-weight: 700; color: #64748B; transition: all 0.2s;
    }
    .stTabs [aria-selected="true"] { 
        background-color: #1E3A8A !important; color: white !important; 
        box-shadow: 0 4px 12px rgba(30, 58, 138, 0.2);
    }

    /* 섹션 제목 */
    .section-label { 
        font-size: 1.1rem; font-weight: 700; color: #1E3A8A; 
        margin: 25px 0 15px 0; border-left: 5px solid #1E3A8A; padding-left: 12px;
    }

    /* 조회 결과 강조 스타일 */
    .blue-bold { color: #1E40AF !important; font-weight: 800 !important; }
    .red-bold { color: #E11D48 !important; font-weight: 800 !important; }
    
    /* 조회 결과 텍스트 박스 형태 (신청서 양식과 통일) */
    .display-box {
        background-color: #F8FAFC; padding: 10px; border-radius: 8px; 
        border: 1px solid #E2E8F0; min-height: 42px; display: flex; align-items: center;
        font-size: 1rem; color: #334155;
    }
    .display-label { font-size: 0.85rem; color: #64748B; margin-bottom: 4px; font-weight: 600; }
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

# --- 3. 왼쪽 사이드바 (구조 유지) ---
with st.sidebar:
    st.markdown('<p class="sidebar-title">HISMEDI †<br>Drug Service</p>', unsafe_allow_html=True)
    st.divider()
    
    st.subheader("👤 신청 정보")
    app_user = st.text_input("신청자 성명", placeholder="입력하세요", key="sb_user")
    app_date = st.date_input("신청 일자", datetime.now()).strftime('%Y-%m-%d')
    
    st.divider()
    
    st.subheader("✅ 처리 정보")
    comp_user = st.text_input("완료자 명", placeholder="처리자 성명", key="sb_comp")
    app_status = st.selectbox("진행 상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("비고 사항", placeholder="특이사항 입력")

# --- 4. 메인 화면 ---
st.markdown('<div class="main-header">약무 서비스 관리 시스템</div>', unsafe_allow_html=True)

# 7개 탭 구성 (원위치)
tabs = st.tabs(["사용중지", "신규입고", "대체입고", "급여변경", "단가인하", "단가인상", "약가조회"])

def render_drug_form(prefix, title, key_id):
    st.markdown(f'<div class="section-label">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi, master_df)
    
    c1, c2, c3 = st.columns([1, 2, 1])
    name = c2.text_input(f"{prefix} 제품명", value=m.get("제품명", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("업체명", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("상한금액", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=f"{m.get('규격','')} {m.get('단위','')}".strip(), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일자", value=m.get("전일", ""), key=f"dt_{key_id}")
    return [edi, name, comp, price, spec, date]

# 1~6번 신청 탭
for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여변경", "단가인하", "단가인상"]):
    with tabs[i]:
        row = [""] * 60
        row[0] = title
        if i < 2:
            res = render_drug_form("신청", "약제 정보 입력", f"tab_{i}")
            row[3], row[4], row[5], row[7], row[10], row[9] = res[0], res[1], res[2], res[3], res[4], res[5]
        else:
            res1 = render_drug_form("기존", "기존 약제 정보", f"tab_{i}_old")
            row[3], row[4], row[5], row[7], row[10], row[9] = res1[0], res1[1], res1[2], res1[3], res1[4], res1[5]
            res2 = render_drug_form("대체", "변경/대체 정보", f"tab_{i}_new")
            row[36], row[37], row[38], row[40], row[43], row[42] = res2[0], res2[1], res2[2], res2[3], res2[4], res2[5]
        
        st.divider()
        if st.button(f"🚀 {title} 신청 제출", key=f"btn_{i}", use_container_width=True):
            try:
                ss = get_spreadsheet()
                ws = ss.worksheet(st.secrets["gsheet"]["worksheet_name"])
                row[1], row[2], row[54], row[55] = app_date, app_user, app_remark, app_status
                ws.append_row(row)
                st.success(f"{title} 접수 완료!"); st.balloons()
            except Exception as e: st.error(f"저장 실패: {e}")

# 7번 약가조회 탭 (분리된 느낌의 고해상도 디자인)
with tabs[6]:
    st.markdown('<div class="section-label">Master DB 실시간 조회</div>', unsafe_allow_html=True)
    c1, c2 = st.columns([1, 3])
    search_edi = c1.text_input("🔍 EDI 코드 입력", placeholder="코드를 넣고 엔터를 치세요", key="search_tab_edi")
    
    if search_edi:
        m = get_drug_info(search_edi, master_df)
        if m:
            st.markdown("<br>", unsafe_allow_html=True)
            # 3x3 그리드 레이아웃
            r1c1, r1c2, r1c3 = st.columns([1, 2, 1])
            with r1c1:
                st.markdown(f'<p class="display-label">투여</p><div class="display-box">{m.get("투여","-")}</div>', unsafe_allow_html=True)
                st.markdown(f'<p class="display-label">제품코드</p><div class="display-box">{m.get("제품코드","-")}</div>', unsafe_allow_html=True)
            with r1c2:
                st.markdown(f'<p class="display-label">제품명</p><div class="display-box blue-bold">{m.get("제품명","-")}</div>', unsafe_allow_html=True)
                st.markdown(f'<p class="display-label">주성분명</p><div class="display-box">{m.get("주성분명","-")}</div>', unsafe_allow_html=True)
            with r1c3:
                st.markdown(f'<p class="display-label">업체명</p><div class="display-box">{m.get("업체명","-")}</div>', unsafe_allow_html=True)
                st.markdown(f'<p class="display-label">단위</p><div class="display-box">{m.get("단위","-")}</div>', unsafe_allow_html=True)
            
            r2c1, r2c2, r2c3 = st.columns(3)
            with r2c1:
                st.markdown(f'<p class="display-label">상한금액</p><div class="display-box red-bold">{m.get("상한금액","0")} 원</div>', unsafe_allow_html=True)
            with r2c2:
                st.markdown(f'<p class="display-label">규격</p><div class="display-box">{m.get("규격","-")}</div>', unsafe_allow_html=True)
            with r2c3:
                st.markdown(f'<p class="display-label">적용일(전일)</p><div class="display-box">{m.get("전일","-")}</div>', unsafe_allow_html=True)
            
            st.markdown(f'<p class="display-label">비고</p><div class="display-box" style="min-height:80px;">{m.get("비고","-")}</div>', unsafe_allow_html=True)
        else:
            st.error("Master 데이터에서 해당 코드를 찾을 수 없습니다.")
