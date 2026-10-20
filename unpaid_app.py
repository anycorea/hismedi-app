import streamlit as st
import streamlit.components.v1 as components

# --- 1. 페이지 설정 (PC 최적화 와이드 레이아웃) ---
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- 2. 상단 타이틀 및 안내 ---
st.title("🏥 히즈메디병원 비급여 진료비 안내")
st.caption("의료법 제45조에 의거한 건강보험심사평가원 연동 히즈메디병원 비급여 진료비용입니다. (실시간 자동 연동)")

# --- 3. 심사평가원 공식 비급여 iframe 임베드 (https 적용) ---
hira_iframe_url = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"

# 브라우저 보안에 걸리지 않도록 https 주소로 렌더링
components.iframe(hira_iframe_url, height=1100, scrolling=True)

# --- 4. 하단 푸터 및 원본 링크 ---
st.markdown("---")
col1, col2 = st.columns([3, 1])
with col1:
    st.caption("※ 본 비급여 진료비용은 심사평가원 및 병원 데이터와 실시간 연동되며, 환자의 상태나 진료 내용에 따라 차이가 있을 수 있습니다.")
with col2:
    st.markdown("🔗 [히즈메디병원 원본 보기](http://hismedi.kr/etc/unpaid.asp)")
