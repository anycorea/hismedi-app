import datetime
import hashlib
import requests
import streamlit as st
import streamlit.components.v1 as components

# 1. UI 및 스타일 설정
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
    
    /* 주차 등록 버튼 스타일 */
    div.stButton > button {
        background-color: #2563EB !important;
        color: white !important;
        font-weight: 700 !important;
        font-size: 1rem !important;
        border-radius: 8px !important;
        border: none !important;
        padding: 12px 0px !important;
        margin-top: 5px !important;
    }
    div.stButton > button:hover {
        background-color: #1D4ED8 !important;
    }
    
    .info-notice {
        background-color: #FEF3C7;
        color: #92400E;
        padding: 10px 12px;
        border-radius: 8px;
        font-size: 0.82rem;
        font-weight: 600;
        text-align: center;
        white-space: nowrap;
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

# 2. 계정 및 서버 설정
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


# ----------------------------------------------------
# 🔍 할인 내역 상세 조회 함수 (서버 API 호출)
# ----------------------------------------------------
def get_existing_discount(session, pe_id):
  """peId(입차ID)를 기반으로 해당 차량에 등록된 기존 할인 내역을 조회합니다."""
  endpoints = [
      f"{BASE_URL}/discount/registration/discountList",
      f"{BASE_URL}/discount/registration/detail",
      f"{BASE_URL}/discount/registration/selectDiscountList",
  ]

  for url in endpoints:
    try:
      res = session.post(url, data={"peId": pe_id, "id": pe_id}, timeout=3)
      if res.status_code == 200:
        data = res.json()

        # 데이터가 리스트 형태인 경우
        if isinstance(data, list) and len(data) > 0:
          first_dc = data[0]
          return (
              first_dc.get("discountVal")
              or first_dc.get("discountName")
              or first_dc.get("dcName")
              or "기존 주차할인"
          )

        # 데이터가 딕셔너리 형태인 경우
        elif isinstance(data, dict):
          dc_list = data.get("list") or data.get("discountList") or []
          if dc_list and len(dc_list) > 0:
            return (
                dc_list[0].get("discountVal")
                or dc_list[0].get("discountName")
                or dc_list[0].get("dcName")
                or "기존 주차할인"
            )
          elif data.get("discountVal") or data.get("discountName"):
            return (
                data.get("discountVal")
                or data.get("discountName")
                or "기존 주차할인"
            )
    except Exception:
      continue
  return None


if "searched_cars" not in st.session_state:
  st.session_state.searched_cars = None
if "selected_car" not in st.session_state:
  st.session_state.selected_car = None

# ----------------------------------------------------
# 🔹 차량번호 (뒤 4자리) 입력
# ----------------------------------------------------
car_no = st.text_input(
    "🔹 차량번호 (뒤 4자리)",
    max_chars=4,
    placeholder="예: 5661",
    key="input_car_no",
)

if len(car_no) == 4 and car_no.isdigit():
  session = get_session()
  try:
    res = session.post(
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
# 차량 선택 및 기존 할인 내역 상세 검사
# ----------------------------------------------------
if st.session_state.searched_cars:
  cars = st.session_state.searched_cars

  if len(cars) == 1:
    car = cars[0]
    st.session_state.selected_car = car
  else:
    options = {
        f"{c.get('carNo')} (입차: {c.get('entryDateToString')})": c for c in cars
    }
    selected_label = st.selectbox("주차 차량 선택", list(options.keys()))
    st.session_state.selected_car = options[selected_label]

  target_car = st.session_state.selected_car
  pe_id = target_car.get("id")

  # 1) 차량 기본 정보에서 1차 확인
  existing_dc_name = (
      target_car.get("discountName")
      or target_car.get("dcName")
      or target_car.get("discountVal")
  )

  # 2) 기본 정보에 없으면 상세 할인 내역 API 호출해서 2차 확인
  session = get_session()
  if not existing_dc_name and pe_id:
    existing_dc_name = get_existing_discount(session, pe_id)

  # 기존 할인이 존재하는 경우 🛑 (등록 완전 차단)
  if existing_dc_name:
    st.warning(
        f"⚠️ [{target_car.get('carNo')}] 차량은 이미"
        f" **[{existing_dc_name}]**이(가) 등록되어 있습니다."
    )
    st.info("💡 추가 할인이 필요한 경우 원무팀에 문의해 주세요.")

  # 기존 할인이 없는 경우만 등록 절차 진행 ✅
  else:
    st.success(
        f"🚘 **조회 차량:** {target_car.get('carNo')} (입차시간:"
        f" {target_car.get('entryDateToString')})"
    )

    # ----------------------------------------------------
    # 🔹 환자 확인번호 입력
    # ----------------------------------------------------
    receipt_no = st.text_input(
        "🔹 환자 확인번호 (접수증 참조)",
        placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
        key="input_receipt_no",
    )

    # ----------------------------------------------------
    # 주차 등록 버튼
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
          save_res = session.post(
              f"{BASE_URL}/discount/registration/save",
              data={
                  "peId": pe_id,
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

# 하단 안내 문구
st.markdown(
    '<div class="info-notice">※ 3시간 이상 주차 시 원무팀에 문의해'
    " 주세요.</div>",
    unsafe_allow_html=True,
)

# Enter 키 입력 이동 JS
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
