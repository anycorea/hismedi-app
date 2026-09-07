import datetime
import requests
import streamlit as st

st.set_page_config(
    page_title="히즈메디병원 주차등록 시스템",
    page_icon="🚗",
    layout="centered",
)

st.title("🚗 히즈메디병원 주차등록")
st.caption("진료 및 검진 방문객 전용 셀프 주차등록 시스템")

if "search_results" not in st.session_state:
  st.session_state.search_results = None

# 1. 입력 폼
with st.form("parking_form"):
  car_no = st.text_input(
      "차량번호 뒤 4자리", max_chars=4, placeholder="예: 6347"
  )
  receipt_no = st.text_input(
      "환자등록번호 (오늘 일자 + 환자번호)",
      placeholder="예: 오늘 일자 2자리 + 숫자를 입력하세요",
  )

  submitted = st.form_submit_button(
      "내 차량 조회하기", use_container_width=True
  )

# 2. 차량 조회 및 검증 로직
if submitted:
  today = datetime.datetime.now()
  today_day = today.strftime("%d")  # 오늘 일자 (2자리 문자열, 예: '07')
  today_yyyymmdd = today.strftime("%Y%m%d")

  raw_input = receipt_no.strip()

  # A. 환자등록번호 검증 및 앞자리 0 자동 채움 처리
  if not raw_input.startswith(today_day):
    st.error(
        f"❌ 올바른 환자등록번호가 아닙니다. (오늘 일자 [{today_day}]로"
        " 시작해야 합니다)"
    )
    st.session_state.search_results = None
  elif len(car_no) != 4 or not car_no.isdigit():
    st.error("❌ 차량번호 4자리를 정확히 입력해 주세요.")
    st.session_state.search_results = None
  else:
    # 오늘 일자(2자리)를 제외한 순수 환자번호 추출
    patient_seq = raw_input[2:]

    # 숫자로만 구성되어 있는지 확인
    if not patient_seq.isdigit():
      st.error("❌ 환자등록번호는 숫자만 입력 가능합니다.")
      st.session_state.search_results = None
    else:
      # 앞자리를 '0'으로 채워 총 10자리 환자번호로 보정 (zfill)
      formatted_patient_id = patient_seq.zfill(10)

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
          st.warning("⚠️ 입차된 차량을 찾을 수 없습니다.")
          st.session_state.search_results = None
      except Exception as e:
        st.error(f"주차 시스템 통신 오류: {e}")

# 3. 차량 선택 및 3시간 자동 할인
if st.session_state.search_results:
  st.write("---")
  st.subheader("📋 본인 차량 선택 (3시간 할인 적용)")

  for item in st.session_state.search_results:
    car_full_no = item.get("carNo", "차량번호 없음")
    in_time = item.get("inTime", "")
    pe_id = item.get("id")

    btn_label = f"🚘 {car_full_no} (입차: {in_time}) ➔ 3시간 할인 등록"

    if st.button(btn_label, key=f"btn_{pe_id}", use_container_width=True):
      session = requests.Session()
      save_url = "http://115.21.205.117/discount/registration/save"

      save_payload = {
          "peId": pe_id,
          "discountType": "2",  # 3시간 할인
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
