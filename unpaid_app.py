import streamlit as st
import streamlit.components.v1 as components

# --- 1. 페이지 기본 설정 ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. Streamlit 상단 여백 및 헤더 완전히 제거하는 Custom CSS ---
st.markdown("""
    <style>
    /* 상단 기본 여백 및 헤더/메뉴 제거 */
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
    
    iframe {
        border: none !important;
        width: 100% !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. 심사평가원 공식 비급여 iframe 임베드 (상단부터 시작) ---
hira_iframe_url = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"

# 로고 위치부터 시원하게 보이도록 높이 지정
components.iframe(hira_iframe_url, height=1300, scrolling=True)
