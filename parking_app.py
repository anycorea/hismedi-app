import datetime
import hashlib
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

# ==========================================
# 🔑 계정 정보 (실제 아이디/비밀번호로 수정해 주세요)
# ==========================================
USER_ID = "YOUR_ID"  # 사용 중이신 아이디
USER_PW = "YOUR_PASSWORD"  # 사용 중이신 비밀번호
# ==========================================

BASE_URL = "http://115.21.205.117"

if "search_results" not in st.session_state:
  st.session_state.search_results = None

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


# SHA-256 암호화 적용 및 로그인 세션 생성 함수
def get_authenticated_session():
  session = requests.Session()
  headers = {
      "User-Agent": (
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      ),
      "Referer": f"{BASE_URL}/login",
      "X-Requested-With": "XMLHttpRequest",
  }
  session.headers.update(headers)

  if USER_ID != "YOUR_ID":
    # 비밀번호 SHA-256 암호화 처리
    hashed_pw = hashlib.sha256(USER_PW.encode("utf-8")).hexdigest()

    login_url = f"{BASE_URL}/login"
    login_payload = {
        "userId": USER_ID,
        "userPwd": hashed_pw,
    }
    try:
      # 로그인 요청 실행 (세션 쿠키 획득)
      res = session.post(login_url, data=login_payload, timeout=5)
    except Exception as e:
      st.error(f"로그인 통신 오류: {e}")

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

      list_url = f"{BASE_URL}/discount/registration/listForDiscount"
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
    in_time = item.get("entryDateToString") or "입차시간 없음"
    pe_id = item.get("id")
    lot_area = item.get("iLotArea", "621")

    btn_label = f"🚘 {car_full_no} (입차: {in_time}) ➔ 3시간 할인 등록"

    if st.button(btn_label, key=f"btn_{pe_id}", use_container_width=True):
      session = get_authenticated_session()

      save_url = f"{BASE_URL}/discount/registration/save"
      save_payload = {
          "peId": pe_id,
          "discountType": "2",  # 3시간 할인 코드
          "saveCnt": "1",
          "iCardType": "0",
          "carNo": car_full_no,
          "iLotArea": lot_area,
          "acPlate2": "",
          "memo": "",
      }

      try:
        save_res = session.post(save_url, data=save_payload, timeout=5)

        try:
          res_json = save_res.json()
          # 서버 응답 검증
          if (
              res_json.get("result") == True
              or res_json.get("code") == 0
              or res_json.get("code") == "200"
              or res_json.get("status") == "SUCCESS"
          ):
            st.success(
                f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
                " 적용되었습니다!"
            )
            st.session_state.search_results = None
          else:
            err_msg = (
                res_json.get("msg")
                or res_json.get("errorMsg")
                or res_json.get("message")
                or res_json
            )
            st.error(f"❌ 주차 할인 등록 실패: {err_msg}")
        except Exception:
          # HTML(로그인창)이 돌아오는지 검사
          if "<title>히즈메디병원</title>" in save_res.text:
            st.error(
                "❌ 아이디 또는 비밀번호가 틀렸거나 로그인 세션 생성에"
                " 실패했습니다. 상단 계정 정보를 다시 확인해 주세요."
            )
          elif "성공" in save_res.text or "ok" in save_res.text.lower():
            st.success(
                f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
                " 적용되었습니다!"
            )
            st.session_state.search_results = None
          else:
            st.error("❌ 처리 결과 응답 해석 실패")

      except Exception as e:
        st.error(f"할인 적용 통신 오류: {e}")
