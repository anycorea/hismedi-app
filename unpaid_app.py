import streamlit as st
import streamlit.components.v1 as components

# --- 1. 페이지 기본 설정 ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. Streamlit 상단 여백 완전 제거 ---
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    
    .block-container {
        padding-top: 0rem !important;
        padding-bottom: 0rem !important;
        padding-left: 0rem !important;
        padding-right: 0rem !important;
        max-width: 100% !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 원하는 상단 위치("히즈메디병원"에서 시행중인... 텍스트)부터 출력되도록 상단 크롭 처리 ---
hira_iframe_url = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"

# 상단 80px 영역(최상단 타이틀)을 위로 끌어올려서 잘라내고, 로고 및 고지 문구부터 바로 노출
custom_html = f"""
<div style="overflow: hidden; width: 100%; height: 1300px; position: relative;">
    <iframe src="{hira_iframe_url}" 
            style="position: absolute; top: -80px; left: 0; width: 100%; height: 1380px; border: none; scrolling: yes;">
    </iframe>
</div>
"""

# HTML 컴포넌트로 렌더링
components.html(custom_html, height=1300, scrolling=True)
