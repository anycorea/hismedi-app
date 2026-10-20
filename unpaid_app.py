import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup

# --- 1. 페이지 기본 설정 (모바일/PC 반응형) ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. 모바일 가독성 최적화 Custom CSS ---
st.markdown("""
    <style>
    /* 상단 헤더 간격 축소 및 모바일 여백 최적화 */
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    /* 검색창 및 안내 문구 스타일 */
    .stTextInput > div > div > input {
        font-size: 16px;
        border-radius: 8px;
    }
    /* 카드형/테이블 반응형 여백 */
    .element-container {
        margin-bottom: 0.5rem;
    }
    </style>
""", unsafe_unsafe_html=True)


# --- 3. 실시간 홈페이지 크롤링 및 캐싱 ---
# 10분(600초) 캐싱 적용: 속도 향상 및 기존 서버 부담 최소화
@st.cache_data(ttl=600)
def fetch_unpaid_data():
    url = "http://hismedi.kr/etc/unpaid.asp"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'  # 기존 ASP 페이지 인코딩에 맞게 설정 (필요시 utf-8로 변경)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 테이블 추출 (홈페이지 내 비급여 표 구조에 맞춰 파싱)
        table = soup.find('table')
        if not table:
            return None, "표 데이터를 찾을 수 없습니다."
            
        rows = table.find_all('tr')
        data = []
        headers_list = []
        
        for i, row in enumerate(rows):
            cols = row.find_all(['th', 'td'])
            cols_text = [c.get_text(strip=True) for c in cols]
            
            if i == 0:
                headers_list = cols_text
            else:
                if cols_text:
                    data.append(cols_text)
                    
        df = pd.DataFrame(data, columns=headers_list if headers_list else None)
        return df, None

    except Exception as e:
        return None, str(e)


# --- 4. 메인 UI 구성 ---
st.title("🏥 비급여 진료비 안내")
st.caption("의료법 제45조에 의거한 히즈메디병원 비급여 진료비용입니다. (기존 홈페이지 실시간 연동)")

# 데이터 로딩
with st.spinner("최신 비급여 정보를 불러오는 중..."):
    df, err = fetch_unpaid_data()

if err or df is None:
    st.error(f"데이터를 불러오는데 실패했습니다: {err}")
    st.info("원래 페이지 직접 보기: http://hismedi.kr/etc/unpaid.asp")
else:
    # --- 5. 초고속 검색 필터 ---
    search_term = st.text_input(
        "🔍 진료 항목 검색", 
        placeholder="예: MRI, 초음파, 도수치료, 예방접종 등",
        help="원하시는 비급여 항목이나 키워드를 입력하세요."
    )

    # 검색어 필터링 (전체 컬럼 대상)
    if search_term:
        mask = df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
        filtered_df = df[mask]
        st.write(f"검색 결과: **{len(filtered_df)}** 건")
    else:
        filtered_df = df

    # --- 6. 모바일 최적화 데이터 표시 ---
    # Streamlit의 dataframe/table을 활용하거나 컬럼별 검색 가능
    st.dataframe(
        filtered_df,
        use_container_width=True,
        hide_index=True
    )

    # --- 7. 하단 안내 및 새로고침 ---
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("🔄 정보 새로고침"):
            st.cache_data.clear()
            st.rerun()
    with col2:
        st.caption("※ 실제 진료비용은 환자의 상태나 진료 내용에 따라 차이가 있을 수 있습니다.")
