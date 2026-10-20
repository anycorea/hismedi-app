import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- 1. 페이지 설정 ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

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

# --- 2. 실시간 크롤링 함수 (에러 내용 반환 기능 추가) ---
@st.cache_data(ttl=300)
def fetch_live_unpaid_data():
    # https로 변경하여 보안 접속 시도
    url = "https://hismedi.kr/etc/unpaid.asp"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            return None, "페이지 내에서 <table> 태그를 찾지 못했습니다."
            
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
            return None, "테이블 내에 추출할 데이터 행이 없습니다."
            
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
        return df, None

    except Exception as e:
        return None, str(e)

# --- 3. 메인 UI ---
st.markdown("<h1>🏥 히즈메디병원 비급여 진료비 안내</h1>", unsafe_allow_html=True)
st.markdown("<div class='sub-text'>의료법 제45조에 의거하여 병원 홈페이지와 실시간 연동되는 비급여 진료비용 고지 화면입니다.</div>", unsafe_allow_html=True)

with st.spinner("병원 홈페이지 데이터를 실시간으로 불러오는 중..."):
    df, err = fetch_live_unpaid_data()

if err or df is None or df.empty:
    st.error(f"⚠️ 데이터를 불러오지 못했습니다. (원인: {err})")
    st.info("💡 클라우드 서버(Streamlit Cloud)의 IP 대역이 병원 방화벽에 차단되었거나, 외부 스크래핑을 허용하지 않는 구조일 수 있습니다.")
    st.markdown("👉 원본 페이지 확인: [히즈메디병원 비급여진료비 바로가기](http://hismedi.kr/etc/unpaid.asp)")
else:
    # 탭 구성
    tab_names = ["행위료", "치료재료대", "약제비", "제증명수수료"]
    tabs = st.tabs(tab_names)

    for i, tab_title in enumerate(tab_names):
        with tabs[i]:
            st.markdown(f"### 📌 {tab_title} 실시간 안내")
            
            tab_df = df.copy()
            if "대분류" in tab_df.columns:
                matched_df = tab_df[tab_df["대분류"].str.contains(tab_title, na=False)]
                if not matched_df.empty:
                    tab_df = matched_df
                    
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
            
            c1, c2 = st.columns([3, 1])
            with c2:
                search_keyword = st.text_input(
                    "검색", 
                    placeholder="검색어 입력", 
                    key=f"search_{i}",
                    label_visibility="collapsed"
                )
                
            filtered_df = tab_df.copy()
            if selected_sub != "전체" and target_col:
                filtered_df = filtered_df[filtered_df[target_col] == selected_sub]
                
            if search_keyword:
                mask = filtered_df.apply(lambda row: row.astype(str).str.contains(search_keyword, case=False).any(), axis=1)
                filtered_df = filtered_df[mask]
                
            st.caption(f"검색 결과: 총 **{len(filtered_df)}** 건")
            st.dataframe(filtered_df, use_container_width=True, hide_index=True, height=500)

st.markdown("---")
col_f1, col_f2, col_f3 = st.columns([2, 1, 1])
with col_f1:
    st.caption("※ 본 데이터는 히즈메디병원 공식 홈페이지와 실시간 연동되어 제공됩니다.")
with col_f2:
    if st.button("🔄 즉시 새로고침"):
        st.cache_data.clear()
        st.rerun()
with col_f3:
    st.markdown("🔗 [원본 페이지 보기](http://hismedi.kr/etc/unpaid.asp)")
