import streamlit as st
import pandas as pd

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

# --- 3. 비급여 진료비 데이터 정의 (필요시 항목 추가/수정 용이) ---
@st.cache_data
def get_unpaid_data():
    data = [
        # 도수치료 및 물리치료
        {"분류": "도수치료", "항목명": "도수치료 (1일당)", "가격(원)": "100,000 ~ 150,000"},
        {"분류": "도수치료", "항목명": "도수윈백운동치료 (1일당)", "가격(원)": "100,000 ~ 150,000"},
        {"분류": "도수치료", "항목명": "도수운동치료 (1일당)", "가격(원)": "100,000 ~ 150,000"},
        {"분류": "주사료", "항목명": "프롤로테라피 - 사지관절 (1부위)", "가격(원)": "50,000 ~ 80,000"},
        {"분류": "주사료", "항목명": "프롤로테라피 - 사지관절 (2부위 이상)", "가격(원)": "50,000 ~ 80,000"},
        
        # 예방접종
        {"분류": "예방접종", "항목명": "가다실 4가 프리필드시린지", "가격(원)": "180,000"},
        {"분류": "예방접종", "항목명": "가다실 9가 프리필드시린지", "가격(원)": "변동문의"},
        {"분류": "예방접종", "항목명": "서바릭스 프리필드시린지", "가격(원)": "150,000"},
        {"분류": "예방접종", "항목명": "지씨플루쿼드리밸런트 (4가 독감)", "가격(원)": "35,000"},
        {"분류": "예방접종", "항목명": "플루아릭스테트라 (4가 독감)", "가격(원)": "35,000"},
        {"분류": "예방접종", "항목명": "부스트릭스 프리필드시린지 (Tdap)", "가격(원)": "50,000"},
        {"분류": "예방접종", "항목명": "아다셀주 (Tdap)", "가격(원)": "50,000"},
        {"분류": "예방접종", "항목명": "프리베나13주 (폐렴구균)", "가격(원)": "150,000"},
        {"분류": "예방접종", "항목명": "하브릭스주 1.0ml (성인 A형간염)", "가격(원)": "80,000"},
        
        # MRI 검사
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 두경부 (부비동/안와 등)", "가격(원)": "580,000"},
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 척추 (경추/흉추/요천추)", "가격(원)": "600,000"},
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 근골격계 (견관절/주관절 등 일반)", "가격(원)": "500,000"},
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 근골격계 (제한적 MRI)", "가격(원)": "170,000"},
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 뇌혈관 (Brain MRA)", "가격(원)": "400,000"},
        {"분류": "MRI", "항목명": "기본자기공명영상진단 - 경부혈관 (Neck MRA)", "가격(원)": "400,000"},
        
        # 초음파 검사
        {"분류": "초음파", "항목명": "단순초음파 (Ⅰ)", "가격(원)": "30,000"},
        {"분류": "초음파", "항목명": "단순초음파 (Ⅱ)", "가격(원)": "50,000"},
        {"분류": "초음파", "항목명": "갑상선·부갑상선 초음파", "가격(원)": "130,000"},
        {"분류": "초음파", "항목명": "유방·액와부 초음파", "가격(원)": "150,000"},
    ]
    return pd.DataFrame(data)

# --- 4. 메인 UI 구성 ---
st.title("🏥 비급여 진료비 안내")
st.caption("의료법 제45조에 의거한 히즈메디병원 비급여 진료비용 안내입니다.")

# 데이터 불러오기
df = get_unpaid_data()

# --- 5. 카테고리 필터 및 검색창 ---
col1, col2 = st.columns([1, 2])
with col1:
    categories = ["전체"] + list(df["분류"].unique())
    selected_category = st.selectbox("📂 분류 선택", categories)

with col2:
    search_term = st.text_input(
        "🔍 진료 항목 검색", 
        placeholder="예: 도수치료, MRI, 가다실, 초음파 등",
        help="원하시는 비급여 항목이나 키워드를 입력하세요."
    )

# --- 6. 필터링 로직 ---
filtered_df = df.copy()

if selected_category != "전체":
    filtered_df = filtered_df[filtered_df["분류"] == selected_category]

if search_term:
    mask = filtered_df.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
    filtered_df = filtered_df[mask]

st.write(f"검색 결과: **{len(filtered_df)}** 건")

# --- 7. 모바일 최적화 테이블 표시 ---
st.dataframe(
    filtered_df,
    use_container_width=True,
    hide_index=True
)

# --- 8. 하단 안내 및 원본 링크 ---
st.markdown("---")
st.caption("※ 실제 진료비용은 환자의 상태나 진료 내용에 따라 차이가 있을 수 있습니다.")
st.markdown("🔗 [히즈메디병원 공식 홈페이지 비급여진료비 원본 보기](http://hismedi.kr/etc/unpaid.asp)")
