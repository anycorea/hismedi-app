import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- 1. 페이지 설정 (PC 최적화 와이드 레이아웃) ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. 스타일링 적용 ---
st.markdown("""
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 1400px;
    }
    h1 {
        text-align: center;
        color: #2b4c7e;
        font-weight: 700;
    }
    .sub-text {
        text-align: center;
        color: #666;
        margin-bottom: 2rem;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 병원 홈페이지 실시간 데이터 연동 (안정형 크롤링) ---
@st.cache_data(ttl=300)
def fetch_live_unpaid_data():
    url = "http://hismedi.kr/etc/unpaid.asp"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        # GET 방식으로 페이지 전체 소스 요청 (내부 탭 및 전체 데이터가 포함된 기본 페이지)
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        
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
            
        # 기본 헤더가 없을 경우 병원 홈페이지 표준 컬럼 매칭
        if not headers_list or len(headers_list) < 5:
            headers_list = ["중분류", "소분류", "코드", "명칭", "구분", "비용", "최저비용", "최고비용", "치료재료대포함", "약제비포함", "특이사항", "최종변경일"]
            
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

    except Exception as e:
        return pd.DataFrame()

# --- 4. 타이틀 및 안내 문구 ---
st.markdown("<h1>🏥 히즈메디병원 비급여 진료비 안내</h1>", unsafe_allow_html=True)
st.markdown("<div class='sub-text'>의료법 제45조에 의거하여 병원 홈페이지와 실시간 연동되는 비급여 진료비용 고지 화면입니다.</div>", unsafe_allow_html=True)

# 데이터 로드
with st.spinner("병원 홈페이지에서 최신 비급여 정보를 실시간으로 불러오는 중..."):
    df = fetch_live_unpaid_data()

if df.empty:
    st.warning("⚠️ 실시간 데이터를 가져오는 중 문제가 발생했습니다. 하단의 [즉시 새로고침] 버튼을 눌러주세요.")
    st.markdown("🔗 [히즈메디병원 원본 페이지 직접 보기](http://hismedi.kr/etc/unpaid.asp)")
else:
    # --- 5. 상단 4대 대분류 탭 구성 ---
    tab_names = ["행위료", "치료재료대", "약제비", "제증명수수료"]
    tabs = st.tabs(tab_names)

    for i, tab_title in enumerate(tab_names):
        with tabs[i]:
            st.markdown(f"### 📌 {tab_title} 실시간 안내")
            
            # 대분류 컬럼이 있다면 해당 키워드로 필터링, 없으면 전체 데이터 활용
            tab_df = df.copy()
            if "대분류" in tab_df.columns:
                matched_df = tab_df[tab_df["대분류"].str.contains(tab_title, na=False)]
                if not matched_df.empty:
                    tab_df = matched_df
                    
            # --- 6. 중분류 라디오 버튼 필터 ---
            target_col = tab_df.columns[0] if len(tab_df.columns) > 0 else None
            for col in tab_df.columns:
                if "중분류" in col:
                    target_col = col
                    break
                    
            sub_categories = ["전체"] + list(tab_df[target_col].dropna().unique()) if target_col else ["전체"]
            
            selected_sub = st.radio(
                "중분류 선택", 
                sub_categories, 
                horizontal=True, 
                key=f"radio_{i}",
                label_visibility="collapsed"
            )
            
            st.markdown("---")
            
            # --- 7. 검색바 ---
            c1, c2 = st.columns([3, 1])
            with c2:
                search_keyword = st.text_input(
                    "검색", 
                    placeholder="검색어를 입력하세요 (예: 도수치료, MRI)", 
                    key=f"search_{i}",
                    label_visibility="collapsed"
                )
                
            # 필터 적용
            filtered_df = tab_df.copy()
            if selected_sub != "전체" and target_col:
                filtered_df = filtered_df[filtered_df[target_col] == selected_sub]
                
            if search_keyword:
                mask = filtered_df.apply(lambda row: row.astype(str).str.contains(search_keyword, case=False).any(), axis=1)
                filtered_df = filtered_df[mask]
                
            st.caption(f"검색 결과: 총 **{len(filtered_df)}** 건 (실시간 연동됨)")
            
            # --- 8. 테이블 렌더링 ---
            st.dataframe(
                filtered_df,
                use_container_width=True,
                hide_index=True,
                height=500
            )

# --- 9. 하단 푸터 및 새로고침 버튼 ---
st.markdown("---")
col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
with col_f1:
    st.caption("※ 본 데이터는 히즈메디병원 공식 홈페이지와 실시간 연동되어 제공됩니다.")
with col_f2:
    if st.button("🔄 즉시 새로고침 (캐시 초기화)"):
        st.cache_data.clear()
        st.rerun()
with col_f3:
    st.markdown("🔗 [원본 페이지 보기](http://hismedi.kr/etc/unpaid.asp)")
