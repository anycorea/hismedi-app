import streamlit as st
import streamlit.components.v1 as components

# --------------------------------------------------
# 기본 설정
# --------------------------------------------------
st.set_page_config(
    page_title="히즈메디병원 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# HIRA 원본 주소 (해당 페이지가 외부 iframe을 허용해야 표시됩니다.)
HIRA_URL = (
    "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do"
    "?yadmSbstKey=rbGHaZKha4UZWTsy"
)

# 조정 가능한 화면 값 (px)
CROP_TOP = 50       # HIRA 상단에서 숨길 높이. 로고가 잘리면 줄이세요.
VIEW_HEIGHT = 900    # 화면에 보이는 HIRA 영역 높이

st.markdown(
    """
    <style>
    #MainMenu, footer, header, [data-testid="stToolbar"],
    [data-testid="stDecoration"] {
        display: none !important;
    }
    html, body {
        margin: 0;
        padding: 0;
    }
    .stApp {
        background: #ffffff;
    }
    [data-testid="stAppViewBlockContainer"],
    .block-container {
        max-width: 100% !important;
        padding: 0 !important;
        margin: 0 !important;
    }
    [data-testid="stVerticalBlock"] {
        gap: 0 !important;
    }
    [data-testid="stElementContainer"] {
        margin: 0 !important;
    }
    iframe[title="streamlit_components.v1.html"] {
        display: block;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Streamlit의 추가 스크롤 영역은 만들지 않고,
# 실제 HIRA 페이지의 iframe 내부 스크롤만 사용합니다.
iframe_html = f"""
<!doctype html>
<html lang="ko">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * {{ box-sizing: border-box; }}
        html, body {{
            width: 100%;
            height: 100%;
            margin: 0;
            padding: 0;
            overflow: hidden;
            background: white;
        }}
        .frame-window {{
            position: relative;
            width: 100%;
            height: 100%;
            overflow: hidden;
        }}
        .hira-frame {{
            position: absolute;
            top: -{CROP_TOP}px;
            left: 0;
            width: 100%;
            height: calc(100% + {CROP_TOP}px);
            border: 0;
            display: block;
            background: white;
        }}
    </style>
</head>
<body>
    <div class="frame-window">
        <iframe
            class="hira-frame"
            src="{HIRA_URL}"
            title="히즈메디병원 비급여 진료비 원본 안내"
            loading="eager"
            referrerpolicy="strict-origin-when-cross-origin"
        ></iframe>
    </div>
</body>
</html>
"""

components.html(iframe_html, height=VIEW_HEIGHT, scrolling=False)
