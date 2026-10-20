import streamlit as st
import streamlit.components.v1 as components
from pathlib import Path
from html import escape

st.set_page_config(
    page_title="히즈메디병원 | 비급여 진료비 안내",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# HIRA 원본 페이지: 병원 정보가 연결된 URL을 그대로 사용합니다.
HIRA_URL = "https://www.hira.or.kr/re/diag/getNpayNotiCsuiSvcFom.do?yadmSbstKey=rbGHaZKha4UZWTsy"

# 원본 화면 상단의 로고·안내문을 가리고 '행위료' 탭부터 표시
# HIRA 페이지 구성과 브라우저에 따라 180~240 정도로 조절하세요.
CROP_TOP = 195
# 데이터 영역의 표시 높이(PC 화면 기준). HIRA 내부 스크롤은 그대로 사용합니다.
DATA_HEIGHT = 1050

st.markdown("""
<style>
  #MainMenu, footer, header[data-testid="stHeader"] {display:none !important;}
  [data-testid="stAppViewContainer"] {background:#fff;}
  .block-container {
    padding: 0 14px 20px !important;
    max-width: 100% !important;
  }
  [data-testid="stVerticalBlock"] {gap:0 !important;}
  [data-testid="stElementContainer"] {margin-bottom:0 !important;}
  .hospital-head {
    display:flex; align-items:center; gap:22px;
    border-bottom:2px solid #1762af; padding:18px 12px 15px;
    margin-bottom:4px;
  }
  .hospital-mark {font-size:38px; flex-shrink:0;}
  .hospital-title {font-size:32px; font-weight:750; color:#183d75; line-height:1.25;}
  .hospital-subtitle {font-size:15px; color:#4b5563; padding-top:7px; line-height:1.6;}
  @media(max-width:650px) {
    .hospital-head {gap:12px;padding:12px 4px;}
    .hospital-mark {font-size:27px;}
    .hospital-title {font-size:22px;}
    .hospital-subtitle {font-size:12px;}
  }
</style>
""", unsafe_allow_html=True)

# 'logo.png'를 앱 파일과 같은 폴더에 두면 실제 로고로 교체할 수 있습니다.
logo_path = Path(__file__).with_name("logo.png")
if logo_path.exists():
    import base64
    encoded = base64.b64encode(logo_path.read_bytes()).decode("ascii")
    logo_html = f'<img src="data:image/png;base64,{encoded}" alt="히즈메디병원" style="width:105px;max-width:20vw;height:auto">'
else:
    logo_html = '<span class="hospital-mark" aria-hidden="true">🏥</span>'

st.markdown(f"""
<div class="hospital-head">
  {logo_html}
  <div>
    <div class="hospital-title">비급여 진료비용 안내</div>
    <div class="hospital-subtitle">의료법 제45조 제1항 및 제2항과 동법 시행규칙 제42조의2에 따라 비급여 진료비용을 고지합니다.</div>
  </div>
</div>
""", unsafe_allow_html=True)

# iframe 내부 페이지는 HIRA 서버에서 직접 로딩됩니다.
# 다른 출처의 iframe 내부 DOM에 접근하지 않고, 상단만 시각적으로 가립니다.
# scrolling=False 는 Streamlit 바깥쪽 컴포넌트의 스크롤만 비활성화합니다.
# HIRA 원본 iframe의 스크롤은 활성화합니다.
frame_html = f"""
<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<style>
  html, body {{margin:0; padding:0; overflow:hidden; background:#fff;}}
  .hira-window {{
    position:relative;
    overflow:hidden;
    width:100%;
    height:{DATA_HEIGHT}px;
  }}
  .hira-window iframe {{
    position:absolute;
    left:0;
    top:-{CROP_TOP}px;
    width:100%;
    height:{DATA_HEIGHT + CROP_TOP}px;
    border:0;
    display:block;
  }}
</style>
</head>
<body>
  <div class="hira-window">
    <iframe src="{escape(HIRA_URL, quote=True)}"
            title="건강보험심사평가원 비급여 진료비용 원본 데이터"
            scrolling="yes" loading="eager" referrerpolicy="strict-origin-when-cross-origin"></iframe>
  </div>
</body>
</html>
"""
components.html(frame_html, height=DATA_HEIGHT, scrolling=False)
