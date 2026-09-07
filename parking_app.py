import datetime
import hashlib
import requests
import streamlit as st
import streamlit.components.v1 as components

# 1. UI 설정
st.set_page_config(
    page_title="히즈메디병원 주차등록", page_icon="🚗", layout="centered"
)

st.markdown(
    """
    <style>
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"] { display: none !important; }
    .custom-title {
        font-size: 1.3rem !important;
        font-weight: 700;
        color: #1E293B;
        text-align: center;
        white-space: nowrap;
        margin-bottom: 2px;
    }
    .custom-sub {
        font-size: 0.85rem;
        color: #64748B;
        text-align: center;
        margin-bottom: 20px;
    }
    .info-notice {
        background-color: #FEF3C7;
        color: #92400E;
        padding: 10px 14px;
        border-radius: 8px;
        font-size: 0.82rem;
        font-weight: 600;
        text-align: center;
        margin-top: 15px;
    }
    </style>
""",
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="custom-title">🚗 히즈메디병원 주차등록</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="custom-sub">진료 및 검진 방문객 셀프 주차등록</div>',
    unsafe_allow_html=True,
)

# 2. 주차 시스템 로그인 설정
USER_ID = "001"
USER_PW = "1588"
BASE_URL = "http://115.21.205.117"

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


def get_session():
  s = requests.Session()
  s.headers.update({
      "User-Agent": "Mozilla/5.0",
      "Referer": f"{BASE_URL}/login",
      "X-Requested-With": "XMLHttpRequest",
  })
  if USER_ID != "YOUR_ID":
    hashed_pw = hashlib.sha256(USER_PW.encode()).hexdigest()
    s.post(
        f"{BASE_URL}/login",
        data={"userId": USER_ID, "userPwd": hashed_pw},
        timeout=5,
    )
  return s


if "searched_cars" not in st.session_state:
  st.session_state.searched_cars = None
if "selected_car" not in st.session_state:
  st.session_state.selected_car = None

# ----------------------------------------------------
# [1단계] 차량번호 뒤 4자리 입력
# ----------------------------------------------------
car_no = st.text_input(
    "1️⃣ 차량번호 (뒤 4자리)",
    max_chars=4,
    placeholder="예: 5661",
    key="input_car_no",
)

if len(car_no) == 4 and car_no.isdigit():
  try:
    res = get_session().post(
        f"{BASE_URL}/discount/registration/listForDiscount",
        data={
            "iLotArea": "621",
            "entryDate": today_yyyymmdd,
            "carNo": car_no,
        },
        timeout=5,
    )
    items = res.json()
    if items:
      st.session_state.searched_cars = items
    else:
      st.error("❌ 입차된 차량이 없습니다. 번호를 다시 확인해 주세요.")
      st.session_state.searched_cars = None
  except Exception as e:
    st.error(f"주차 서버 통신 오류: {e}")
else:
  st.session_state.searched_cars = None

# ----------------------------------------------------
# [2단계] 입차 차량 확인 및 중복 할인 상태 검사
# ----------------------------------------------------
if st.session_state.searched_cars:
  cars = st.session_state.searched_cars

  # 1대만 조회된 경우
  if len(cars) == 1:
    car = cars[0]
    st.session_state.selected_car = car
  else:
    options = {
        f"{c.get('carNo')} (입차: {c.get('entryDateToString')})": c for c in cars
    }
    selected_label = st.selectbox("2️⃣ 주차 차량 선택", list(options.keys()))
    st.session_state.selected_car = options[selected_label]

  target_car = st.session_state.selected_car

  # 🔍 서버 응답 데이터에서 기존 할인 등록 여부 필드 확인
  # (주차 서버 API마다 필드명이 다를 수 있으나 보통 discountYn, isDiscount, totalDiscountAmt 등을 사용)
  is_already_discounted = (
      target_car.get("discountYn") == "Y"
      or target_car.get("isDiscount") == True
      or int(target_car.get("iDiscountAmt", 0)) > 0
  )

  if is_already_discounted:
    st.warning(
        f"⚠️ [{target_car.get('carNo')}] 차량은 이미 주차 할인이 적용되어"
        " 있습니다."
    )
  else:
    st.success(
        f"🚘 **조회 차량:** {target_car.get('carNo')} (입차시간:"
        f" {target_car.get('entryDateToString')})"
    )

    # ----------------------------------------------------
    # [3단계] 환자 확인번호 입력
    # ----------------------------------------------------
    receipt_no = st.text_input(
        "3️⃣ 환자 확인번호 (접수증 참조)",
        placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
        key="input_receipt_no",
    )

    # ----------------------------------------------------
    # [4단계] 주차 등록 버튼
    # ----------------------------------------------------
    if st.button("주차 등록하기 (3시간 할인)", use_container_width=True):
      raw_input = receipt_no.strip()

      if not raw_input.startswith(today_day):
        st.error(
            f"❌ 환자 확인번호가 올바르지 않습니다. (오늘 일자 [{today_day}]로"
            " 시작)"
        )
      else:
        car_full_no = target_car.get("carNo")

        try:
          save_res = get_session().post(
              f"{BASE_URL}/discount/registration/save",
              data={
                  "peId": target_car.get("id"),
                  "discountType": "2",
                  "saveCnt": "1",
                  "iCardType": "0",
                  "carNo": car_full_no,
                  "iLotArea": target_car.get("iLotArea", "621"),
              },
              timeout=5,
          )

          st.balloons()
          st.success(
              f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
              " 적용되었습니다!"
          )

          st.session_state.searched_cars = None
          st.session_state.selected_car = None

        except Exception as e:
          st.error(f"등록 통신 오류: {e}")

st.markdown(
    '<div class="info-notice">※ 3시간 이상이 필요한 경우 접수창구에 말씀해'
    " 주십시오.</div>",
    unsafe_allow_html=True,
)

components.html(
    """
<script>
const doc = window.parent.document;
doc.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') {
        const inputs = Array.from(doc.querySelectorAll('input[type="text"]'));
        const activeEl = doc.activeElement;
        const index = inputs.indexOf(activeEl);
        if (index > -1 && index < inputs.length - 1) {
            e.preventDefault();
            inputs[index + 1].focus();
        }
    }
});
</script>
""",
    height=0,
)
