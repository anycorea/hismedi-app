import streamlit as st
import pandas as pd

# --- 1. 페이지 설정 (PC 최적화 와이드 레이아웃) ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. PC 화면 가독성 극대화 CSS ---
st.markdown("""
    <style>
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }
    /* 테이블 행 높이 및 폰트 크기 최적화 */
    dataframe {
        font-size: 14px;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 비급여 데이터 로드 (CSV 연동) ---
@st.cache_data
def load_unpaid_data():
    try:
        # UTF-8 또는 CP949 인코딩 자동 대응
        df = pd.read_csv("unpaid_data.csv", encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv("unpaid_data.csv", encoding="cp949")
    except FileNotFoundError:
        # 파일이 없을 경우 테스트용 샘플 데이터 반환
        sample_data = [
            {"구분": "행위료", "중분류": "상급병실료(차액)", "소분류": "상급병실료(차액)", "코드": "ABZ010001", "명칭": "상급병실료(1인실)", "구분상세": "상급병실료", "비용": "220,000", "최저비용": "-", "최고비용": "-", "특이사항": "", "최종변경일": "21.01.01"},
            {"구분": "행위료", "중분류": "검사료", "소분류": "일반화학검사", "코드": "CZ2420000", "명칭": "아밀로이드 A", "구분상세": "", "비용": "50,000", "최저비용": "-", "최고비용": "-", "특이사항": "", "최종변경일": "23.08.11"},
            {"구분": "치료재료대", "중분류": "처치 및 수술용", "소분류": "기타재료", "코드": "T001", "명칭": "특수 드레싱 재료", "구분상세": "치료재료", "비용": "15,000", "최저비용": "-", "최고비용": "-", "특이사항": "", "최종변경일": "24.01.01"},
            {"구분": "약제비", "중분류": "주사제", "소분류": "원내약품", "코드": "D001", "명칭": "비급여 영양수액", "구분상세": "약제", "비용": "80,000", "최저비용": "-", "최고비용": "-", "특이사항": "", "최종변경일": "24.05.01"},
            {"구분": "제증명수수료", "중분류": "제증명", "소분류": "진단서", "코드": "C001", "명칭": "일반진단서", "구분상세": "수수료", "비용": "20,000", "최저비용": "-", "최고비용": "-", "특이사항": "", "최종변경일": "22.01.01"}
        ]
        df = pd.DataFrame(sample_data)
    return df

df = load_unpaid_data()

# --- 4. 메인 타이틀 ---
st.title("🏥 히즈메디병원 비급여 진료비 안내")
st.caption("의료법 제45조에 의거한 비급여 진료비용 안내입니다. (PC 최적화 버전)")

# --- 5. 병원 홈페이지와 동일한 상단 탭 구성 ---
tab_list = ["행위료", "치료재료대", "약제비", "제증명수수료"]
tabs = st.tabs(tab_list)

# 데이터에 '구분' 컬럼이 없으면 기본값 부여
if "구분" not in df.columns:
    df["구분"] = "행위료"

for i, tab_name in enumerate(tab_list):
    with tabs[i]:
        # 해당 탭에 맞는 데이터 필터링
        tab_df = df[df["구분"] == tab_name]
        if tab_df.empty:
            tab_df = df # 데이터가 부족할 경우 전체 표시 방어 코드
            
        # PC 화면에 어울리는 2단 레이아웃 (중분류 필터 + 키워드 검색창)
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if "중분류" in tab_df.columns:
                sub_categories = ["전체"] + list(tab_df["중분류"].dropna().unique())
                selected_sub = st.selectbox(f"📂 [{tab_name}] 중분류 선택", sub_categories, key=f"sub_{i}")
            else:
                selected_sub = "전체"
                
        with col2:
            search_query = st.text_input(
                f"🔍 [{tab_name}] 항목명 또는 코드 검색", 
                placeholder="예: MRI, 도수치료, 1인실, 수액 등", 
                key=f"search_{i}"
            )
            
        # 필터 적용
        current_df = tab_df.copy()
        if selected_sub != "전체" and "중분류" in current_df.columns:
            current_df = current_df[current_df["중분류"] == selected_sub]
            
        if search_query:
            mask = current_df.apply(lambda row: row.astype(str).str.contains(search_query, case=False).any(), axis=1)
            current_df = current_df[mask]
            
        st.write(f"검색 결과: **{len(current_df)}** 건")
        
        # 넓고 깔끔한 PC용 와이드 데이터프레임 (모바일에서도 가로 스크롤로 대응됨)
        st.dataframe(
            current_df,
            use_container_width=True,
            hide_index=True,
            height=500
        )

# --- 6. 하단 푸터 ---
st.markdown("---")
col_f1, col_f2 = st.columns([3, 1])
with col_f1:
    st.caption("※ 실제 진료비용은 환자의 상태, 진료 내용 및 선택 사항에 따라 차이가 발생할 수 있습니다.")
with col_f2:
    st.markdown("🔗 [병원 원본 페이지 보기](http://hismedi.kr/etc/unpaid.asp)")
