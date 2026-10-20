import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# --- 1. 페이지 설정 ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. 여백 및 UI 스타일 최적화 ---
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        max-width: 1400px;
    }
    .header-text {
        font-size: 15px;
        color: #444444;
        line-height: 1.6;
        margin-bottom: 0px;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 심사평가원/병원 데이터 실시간 파싱 함수 ---
@st.cache_data(ttl=300)
def fetch_hira_unpaid_data():
    url = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            return pd.DataFrame()
            
        data = []
        headers_list = []
        
        for table in tables:
            rows = table.find_all('tr')
            for i, row in enumerate(rows):
                cols = row.find_all(['th', 'td'])
                cols_text = [c.get_text(strip=True) for c in cols]
                
                if not headers_list and i == 0 and len(cols_text) > 1:
                    headers_list = cols_text
                else:
                    if len(cols_text) >= 2:
                        data.append(cols_text)
                        
        if not data:
            return pd.DataFrame()
            
        if not headers_list or len(headers_list) < 5:
            headers_list = ["대분류", "중분류", "소분류", "코드", "명칭", "구분", "비용", "최저비용", "최고비용", "치료재료대포함", "약제비포함", "특이사항", "최종변경일"]
            
        cleaned_data = []
        for row in data:
            if len(row) == len(headers_list):
                cleaned_data.append(row)
            elif len(row) > len(headers_list):
                cleaned_data.append(row[:len(headers_list)])
            else:
                cleaned_data.append(row + [''] * (len(headers_list) - len(row)))
                
        df = pd.DataFrame(cleaned_data, columns=headers_list)
        return df

    except Exception:
        return pd.DataFrame()

# --- 4. 상단 레이아웃 (로고 좌측 작게 + 지정 안내문구 배치) ---
col_logo, col_desc = st.columns([1, 4])

with col_logo:
    logo_path = "Hismedi_logo.png"
    if os.path.exists(logo_path):
        st.image(logo_path, width=180)
    else:
        st.subheader("🏥 히즈메디병원")

with col_desc:
    st.markdown("""
        <div style="display: flex; align-items: center; height: 100%; padding-top: 10px;">
            <p class="header-text">
                📢 <b>의료법 제45조 제1항 및 제2항과 동법 시행규칙 제 42조의 2 제1항, 제2항 및 제3항에 의거하여 비급여 진료비용을 고지합니다.</b>
            </p>
        </div>
    """, unsafe_allow_html=True)

st.markdown("<div style='margin-bottom: 15px;'></div>", unsafe_allow_html=True)

# --- 5. 데이터 로드 및 탭 구성 ---
df = fetch_hira_unpaid_data()

tab_names = ["행위료", "치료재료대", "약제비", "제증명수수료"]
tabs = st.tabs(tab_names)

for i, tab_title in enumerate(tab_names):
    with tabs[i]:
        if df.empty:
            st.info("💡 병원 서버에서 실시간 데이터를 확인 중입니다.")
            st.markdown(f"🔗 [심사평가원 원본 페이지 직접 보기](https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy)")
            continue

        # 대분류/구분 필터링
        tab_df = df.copy()
        if "대분류" in tab_df.columns:
            matched_df = tab_df[tab_df["대분류"].str.contains(tab_title, na=False)]
            if not matched_df.empty:
                tab_df = matched_df
                
        # 중분류 필터링 추출
        target_col = None
        for col in tab_df.columns:
            if "중분류" in col:
                target_col = col
                break
                
        sub_categories = ["전체"] + list(tab_df[target_col].dropna().unique()) if target_col else ["전체"]
        
        # 검색바 및 중분류 필터
        col_sub, col_search = st.columns([3, 1])
        
        with col_sub:
            selected_sub = st.radio(
                f"[{tab_title}] 중분류 선택", 
                sub_categories, 
                horizontal=True, 
                key=f"radio_{i}",
                label_visibility="collapsed"
            )
            
        with col_search:
            search_keyword = st.text_input(
                "검색어", 
                placeholder="🔍 검색어 (예: 독감, MRI)", 
                key=f"search_{i}",
                label_visibility="collapsed"
            )
            
        # 필터 로직 적용
        filtered_df = tab_df.copy()
        if selected_sub != "전체" and target_col:
            filtered_df = filtered_df[filtered_df[target_col] == selected_sub]
            
        if search_keyword:
            mask = filtered_df.apply(lambda row: row.astype(str).str.contains(search_keyword, case=False).any(), axis=1)
            filtered_df = filtered_df[mask]
            
        st.caption(f"총 **{len(filtered_df)}** 건의 항목이 조회되었습니다.")
        
        # 표 출력
        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True,
            height=550
        )

# --- 6. 하단 푸터 ---
st.markdown("---")
col_f1, col_f2 = st.columns([3, 1])
with col_f1:
    st.caption("※ 본 비급여 진료비용은 심사평가원 및 병원 데이터와 실시간 연동되며, 환자의 상태나 진료 내용에 따라 차이가 있을 수 있습니다.")
with col_f2:
    if st.button("🔄 즉시 새로고침"):
        st.cache_data.clear()
        st.rerun()
