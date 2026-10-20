import streamlit as st
import streamlit.components.v1 as components

# --- 1. 페이지 기본 설정 (와이드 레이아웃 및 여백 최적화) ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. Streamlit 상단/하단 여백 및 헤더 완전 제거 ---
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

# --- 3. 심사평가원 iframe 상단 정교한 크롭 (스크린샷 위치 맞춤) ---
hira_iframe_url = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"

# top: -35px 위치로 맞춰 최상단 불필요 여백을 숨기고 로고부터 바로 나오도록 설정
crop_html = f"""
<div style="overflow: hidden; width: 100%; height: 1350px; position: relative;">
    <iframe src="{hira_iframe_url}" 
            style="position: absolute; top: -35px; left: 0; width: 100%; height: 1400px; border: none; scrolling: yes;">
    </iframe>
</div>
"""

# Streamlit HTML 컴포넌트 렌더링
components.html(crop_html, height=1350, scrolling=True)
