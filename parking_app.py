import datetime
import hashlib
import json
import requests
import streamlit as st

# 1. 페이지 및 UI 기본 설정
st.set_page_config(
    page_title="히즈메디병원 주차등록 시스템",
    page_icon="🚗",
    layout="centered",
)

# UI 요소 숨김 CSS
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
# 🔑 계정 정보 (실제 아이디/비밀번호 입력)
# ==========================================
USER_ID = "001"
USER_PW = "1588"
# ==========================================

BASE_URL = "http://115.21.205.117"

if "search_results" not in st.session_state:
  st.session_state.search_results = None

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


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
    hashed_pw = hashlib.sha256(USER_PW.encode("utf-8")).hexdigest()
    login_url = f"{BASE_URL}/login"
    login_payload = {"userId": USER_ID, "userPwd": hashed_pw}
    try:
      session.post(login_url, data=login_payload, timeout=5)
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

# 3. 차량 조회 로직 (강력한 할인 중복 검증)
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
          has_discount = False

          for item in items:
            # 1) 다양한 형태의 기존 할인 관련 필드 조사
            dc_list = (
                item.get("dcDetailList")
                or item.get("dscntList")
                or item.get("discountList")
                or []
            )
            dc_cnt = (
                item.get("discountCnt")
                or item.get("dscntCnt")
                or item.get("dcCnt")
                or 0
            )
            dc_name = (
                item.get("discountName")
                or item.get("dscntName")
                or item.get("dcName")
                or ""
            )

            # 2) 문자열 전체에서 "할인" 단어나 수치 존재 여부 정밀 탐색
            item_str = json.dumps(item, ensure_ascii=False)

            if (
                dc_list
                or int(dc_cnt) > 0
                or bool(dc_name)
                or "할인" in item_str
                or "3시간" in item_str
            ):
              has_discount = True
              break

          if has_discount:
            st.warning(
                "⚠️ 이미 주차할인 등록이 되어 있습니다. 추가 등록이"
                " 불가능합니다."
            )
            st.session_state.search_results = None
          else:
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
        res_text = save_res.text.strip()

        # 서버 응답 분석
        # 1. 이미 등록되어 있거나 중복 관련 텍스트가 포함된 경우
        if any(
            msg in res_text
            for msg in [
                "이미",
                "중복",
                "exist",
                "already",
                "dup",
                "초과",
                "제한",
            ]
        ):
          st.warning("⚠️ 이미 주차할인이 등록되어 있는 차량입니다.")
          st.session_state.search_results = None  # 초기 화면으로 이동

        # 2. 정상 성공 응답 ("true", "1", "ok" 등)
        elif res_text.lower() in ["true", "1", "ok", "success"]:
          st.success(
              f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
              " 적용되었습니다!"
          )
          st.session_state.search_results = None  # 성공 후 초기화

        # 3. 그 외 예상치 못한 응답
        else:
          st.error(f"❌ 할인 등록 실패 (응답: {res_text})")

      except Exception as e:
        st.error(f"할인 적용 통신 오류: {e}")
