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
    .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    .stTextInput > div > div > input {
        font-size: 16px;
        border-radius: 8px;
    }
    .element-container {
        margin-bottom: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 실시간 홈페이지 크롤링 및 데이터 가공 함수 ---
@st.cache_data(ttl=600)
def fetch_unpaid_data():
    url = "http://hismedi.kr/etc/unpaid.asp"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'euc-kr'  # ASP 페이지 인코딩 대응
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 페이지 내 모든 테이블 탐색
        tables = soup.find_all('table')
        if not tables:
            return None, "HTML 내에 표(Table) 태그가 존재하지 않습니다."
            
        data = []
        headers_list = []
        
        # 모든 테이블의 행을 순회하며 데이터 수집
        for table in tables:
            rows = table.find_all('tr')
            for i, row in enumerate(rows):
                cols = row.find_all(['th', 'td'])
                cols_text = [c.get_text(strip=True) for c in cols]
                
                if not headers_list and i == 0 and len(cols_text) > 1:
                    headers_list = cols_text
                else:
                    if len(cols_text) >= 2:  # 유의미한 데이터 행만 추가
                        data.append(cols_text)
                        
        if not data:
            return None, "표에서 추출할 데이터가 없습니다."
            
        # 컬럼명이 없거나 맞지 않을 경우 기본값 지정
        if not headers_list or len(headers_list) < 2:
            headers_list = ["진료항목", "구분", "가격(원)"]
            
        # 데이터프레임 변환 시 컬럼 길이 맞추기
        cleaned_data = []
        for row in data:
            if len(row) == len(headers_list):
                cleaned_data.append(row)
            elif len(row) > len(headers_list):
                cleaned_data.append(row[:len(headers_list)])
            else:
                # 모자란 컬럼은 빈 값으로 채움
                cleaned_data.append(row + [''] * (len(headers_list) - len(row)))
                
        df = pd.DataFrame(cleaned_data, columns=headers_list)
        return df, None

    except Exception as e:
        return None, str(e)

# --- 4. 메인 UI 구성 ---
st.title("🏥 비급여 진료비 안내")
st.caption("의료법 제45조에 의거한 히즈메디병원 비급여 진료비용입니다. (기존 홈페이지 실시간 연동)")

# 데이터 로딩
with st.spinner("최신 비급여 정보를 불러오는 중..."):
    df, err = fetch_unpaid_data()

# 파싱 실패 시 안내 및 원본 링크 제공 (앱이 죽지 않도록 방어 코드 적용)
if err or df is None or df.empty:
    st.warning("⚠️ 실시간 페이지 구조 변경 등으로 데이터를 직접 불러오지 못했습니다.")
    st.info(f"상세 에러 내용: {err}")
    st.markdown("👉 정확한 내용은 아래 히즈메디병원 공식 홈페이지에서 확인하실 수 있습니다.")
    st.markdown("[히즈메디병원 비급여진료비 페이지 바로가기](http://hismedi.kr/etc/unpaid.asp)")
else:
    # --- 5. 초고속 검색 필터 ---
    search_term = st.text_input(
        "🔍 진료 항목 검색", 
        placeholder="예: MRI, 초음파, 도수치료, 예방접종 등",
        help="원하시는 비급여 항목이나 키워드를 입력하세요."
    )

    # 검색어 필터링
    if search_term:
        mask = df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
        filtered_df = df[mask]
        st.write(f"검색 결과: **{len(filtered_df)}** 건")
    else:
        filtered_df = df

    # --- 6. 모바일 최적화 데이터 표시 ---
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
