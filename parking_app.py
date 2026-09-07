import datetime
import requests
import streamlit as st

# 1. 페이지 및 UI 기본 설정
st.set_page_config(
    page_title="히즈메디병원 주차등록 시스템",
    page_icon="🚗",
    layout="centered",
)

# UI 요소 및 관리자 툴바 숨김 CSS
hide_ui_style = """
    <style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stAppHeader {display: none !important;}
    [data-testid="stHeader"] {display: none !important;}
    [data-testid="stToolbar"] {display: none !important;}
    [data-testid="stDecoration"] {display: none !important;}
    .stActionButton {display: none !important;}
    [data-testid="stActionButton"] {display: none !important;}
    .viewerBadge_container__1S-is {display: none !important;}
    div[class*="viewerBadge"] {display: none !important;}
    div[class*="profile"] {display: none !important;}
    [data-testid="stAppViewBlockContainer"] ~ div {display: none !important;}
    [data-testid="stStatusWidget"],
    .stAppToolbar,
    div[class*="StyledEmbedToolbar"],
    div[class*="EmbedToolbar"],
    div[data-testid="stEmbedToolbar"] {
        display: none !important;
        visibility: hidden !important;
        height: 0px !important;
    }
    #root > div:nth-child(2) {display: none !important;}
    </style>
"""
st.markdown(hide_ui_style, unsafe_allow_html=True)

st.title("🚗 히즈메디병원 주차등록")
st.caption("진료 및 검진 방문객 전용 셀프 주차등록 시스템")

# 계정 정보 설정 (필요 시 수정)
LOGIN_URL = "http://115.21.205.117/login/loginProc"  # 업체 로그인 URL
USER_ID = "YOUR_ID"  # 사용 중이신 아이디 입력
USER_PW = "YOUR_PASSWORD"  # 사용 중이신 비밀번호 입력

if "search_results" not in st.session_state:
  st.session_state.search_results = None

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


# 공통 세션 및 로그인 함수
def get_authenticated_session():
  session = requests.Session()
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      ),
      "Referer": "http://115.21.205.117/login/index",
  }
  session.headers.update(headers)

  # 로그인 시도 (계정 정보가 설정되어 있는 경우)
  if USER_ID != "YOUR_ID":
    login_payload = {
        "userId": USER_ID,
        "userPw": USER_PW,
    }
    try:
      session.post(LOGIN_URL, data=login_payload, timeout=5)
    except Exception:
      pass

  return session


# 2. 입력 폼
with st.form("parking_form"):
  car_no = st.text_input(
      "차량번호 뒤 4자리", max_chars=4, placeholder="예: 5661"
  )
  receipt_no = st.text_input(
      "환자 확인번호 (접수증 참조)",
      placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
  )

  submitted = st.form_submit_button(
      "내 차량 조회하기", use_container_width=True
  )

# 3. 차량 조회 로직
if submitted:
  raw_input = receipt_no.strip()

  if not raw_input.startswith(today_day):
    st.error(
        f"❌ 올바른 확인번호가 아닙니다. (오늘 일자 [{today_day}]로"
        " 시작해야 합니다)"
    )
    st.session_state.search_results = None
  elif len(car_no) != 4 or not car_no.isdigit():
    st.error("❌ 차량번호 4자리를 정확히 입력해 주세요.")
    st.session_state.search_results = None
  else:
    patient_seq = raw_input[2:]

    if not patient_seq.isdigit():
      st.error("❌ 환자 확인번호는 숫자만 입력 가능합니다.")
      st.session_state.search_results = None
    else:
      session = get_authenticated_session()

      list_url = "http://115.21.205.117/discount/registration/listForDiscount"
      list_payload = {
          "iLotArea": "621",
          "entryDate": today_yyyymmdd,
          "carNo": car_no,
      }

      try:
        res = session.post(list_url, data=list_payload, timeout=5)
        items = res.json()

        if items:
          st.session_state.search_results = items
        else:
          st.warning(
              "⚠️ 입차된 차량을 찾을 수 없습니다. 입차 여부 및 번호를 확인해"
              " 주세요."
          )
          st.session_state.search_results = None
      except Exception as e:
        st.error(f"주차 시스템 통신 오류: {e}")

# 4. 차량 선택 및 3시간 자동 할인 등록
if st.session_state.search_results:
  st.write("---")
  st.subheader("📋 본인 차량 선택 (3시간 할인 적용)")

  for item in st.session_state.search_results:
    car_full_no = item.get("carNo", "차량번호 없음")

    # 정확한 입차시간 Key (`entryDateToString`) 적용
    in_time = item.get("entryDateToString") or "입차시간 없음"
    pe_id = item.get("id")

    btn_label = f"🚘 {car_full_no} (입차: {in_time}) ➔ 3시간 할인 등록"

    if st.button(btn_label, key=f"btn_{pe_id}", use_container_width=True):
      session = get_authenticated_session()

      save_url = "http://115.21.205.117/discount/registration/save"
      save_payload = {
          "peId": pe_id,
          "discountType": "2",  # 3시간 할인 코드
          "saveCnt": "1",
          "iCardType": "0",
          "carNo": car_full_no,
          "acPlate2": "",
          "memo": "",
      }

      try:
        save_res = session.post(save_url, data=save_payload, timeout=5)

        # 성공 처리 판단
        if save_res.status_code == 200:
          st.success(
              f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
              " 적용되었습니다!"
          )
          st.session_state.search_results = None
        else:
          st.error("❌ 주차 할인 처리에 실패했습니다. 카운터에 문의해 주세요.")
      except Exception as e:
        st.error(f"할인 적용 통신 오류: {e}")
