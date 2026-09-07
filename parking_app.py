import datetime
import requests
import streamlit as st

# 모바일 UI 최적화
st.set_page_config(
    page_title="허즈메디병원 주차등록 시스템",
    page_icon="🚗",
    layout="centered",
)

st.title("🚗 허즈메디병원 주차등록")
st.caption("진료 및 검진 방문객 전용 셀프 주차등록 시스템")

# 세션 상태 초기화
if "search_results" not in st.session_state:
  st.session_state.search_results = None

# 1. 입력 폼
with st.form("parking_form"):
  car_no = st.text_input(
      "차량번호 뒤 4자리", max_chars=4, placeholder="예: 6347"
  )
  receipt_no = st.text_input(
      "접수/영수증 번호 (6자리)",
      max_chars=6,
      placeholder="오늘 일자(2자리) + 숫자 4자리",
  )

  submitted = st.form_submit_button(
      "내 차량 조회하기", use_container_width=True
  )

# 2. 차량 조회 로직
if submitted:
  today = datetime.datetime.now()
  today_day = today.strftime("%d")
  today_yyyymmdd = today.strftime("%Y%m%d")

  # 접수/영수증 번호 규칙 검증 (오늘 일자로 시작하는 6자리 숫자)
  if len(receipt_no) != 6 or not receipt_no.startswith(today_day):
    st.error("❌ 올바른 접수/영수증 번호가 아닙니다. (당일 번호 확인)")
    st.session_state.search_results = None
  elif len(car_no) != 4 or not car_no.isdigit():
    st.error("❌ 차량번호 4자리를 정확히 입력해 주세요.")
    st.session_state.search_results = None
  else:
    session = requests.Session()
    list_url = "http://115.21.205.117/discount/registration/listForDiscount"
    list_payload = {
        "iLotArea": "621",
        "entryDate": today_yyyymmdd,
        "carNo": car_no,
    }

    try:
      res = session.post(list_url, data=list_payload)
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

# 3. 조회 결과 목록에서 본인 차량 선택 시 3시간 자동 할인
if st.session_state.search_results:
  st.write("---")
  st.subheader("📋 본인 차량 선택 (3시간 할인 적용)")

  for item in st.session_state.search_results:
    car_full_no = item.get("carNo", "차량번호 없음")
    in_time = item.get("inTime", "")
    pe_id = item.get("id")  # 입차 ID

    btn_label = f"🚘 {car_full_no} (입차: {in_time}) ➔ 3시간 할인 등록"

    if st.button(btn_label, key=f"btn_{pe_id}", use_container_width=True):
      session = requests.Session()
      save_url = "http://115.21.205.117/discount/registration/save"

      # 수집된 save API Payload 매핑
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
        save_res = session.post(save_url, data=save_payload)
        if save_res.status_code == 200:
          st.success(
              f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상"
              " 적용되었습니다!"
          )
          st.session_state.search_results = None
        else:
          st.error("할인 등록 실패. 카운터에 문의해 주세요.")
      except Exception as e:
        st.error(f"할인 적용 통신 오류: {e}")
