import datetime
import hashlib
import sqlite3
import requests
import streamlit as st

# 1. UI 및 기본 설정
st.set_page_config(
    page_title="히즈메디병원 주차등록", page_icon="🚗", layout="centered"
)
st.markdown(
    "<style>#MainMenu, header, footer, .stAppHeader,"
    ' [data-testid="stHeader"] {display: none !important;}</style>',
    unsafe_allow_html=True,
)

st.title("🚗 히즈메디병원 주차등록")

USER_ID = "001"
USER_PW = "1588"
BASE_URL = "http://115.21.205.117"


# 2. SQLite 데이터베이스 관리 (오늘 할인 차량 이력 저장)
def init_db():
  conn = sqlite3.connect("parking_log.db")
  c = conn.cursor()
  c.execute("""
        CREATE TABLE IF NOT EXISTS discount_log (
            car_no TEXT,
            reg_date TEXT,
            PRIMARY KEY (car_no, reg_date)
        )
    """)
  conn.commit()
  conn.close()


def is_already_registered(car_no, date_str):
  conn = sqlite3.connect("parking_log.db")
  c = conn.cursor()
  c.execute(
      "SELECT 1 FROM discount_log WHERE car_no = ? AND reg_date = ?",
      (car_no, date_str),
  )
  row = c.fetchone()
  conn.close()
  return row is not None


def add_registration(car_no, date_str):
  conn = sqlite3.connect("parking_log.db")
  c = conn.cursor()
  c.execute(
      "INSERT OR IGNORE INTO discount_log VALUES (?, ?)", (car_no, date_str)
  )
  conn.commit()
  conn.close()


init_db()

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


# 3. 로그인 및 세션 관리
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


# 4. 입력 폼
with st.form("parking_form"):
  car_no = st.text_input("차량번호 뒤 4자리", max_chars=4, placeholder="5661")
  receipt_no = st.text_input(
      "환자 확인번호", placeholder=f"{today_day} + 환자번호"
  )
  submitted = st.form_submit_button("조회하기", use_container_width=True)

# 5. 조회 및 검증
if submitted:
  raw_input = receipt_no.strip()
  if not raw_input.startswith(today_day) or len(car_no) != 4:
    st.error("❌ 입력 정보를 다시 확인해 주세요.")
  elif is_already_registered(car_no, today_yyyymmdd):
    st.warning(
        f"⚠️ [{car_no}] 차량은 오늘 이미 할인 등록이 완료되었습니다."
    )
  else:
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
        st.session_state.search_results = items
      else:
        st.warning("⚠️ 입차 내역이 없습니다.")
    except Exception as e:
        st.error(f"통신 오류: {e}")

# 6. 할인 등록 실행
if st.session_state.get("search_results"):
  st.write("---")
  for item in st.session_state.search_results:
    car_full_no = item.get("carNo")
    in_time = item.get("entryDateToString")
    pe_id = item.get("id")

    btn_label = f"🚘 {car_full_no} ({in_time}) ➔ 3시간 할인 적용"

    if st.button(btn_label, key=f"btn_{pe_id}", use_container_width=True):
      try:
        save_res = get_session().post(
            f"{BASE_URL}/discount/registration/save",
            data={
                "peId": pe_id,
                "discountType": "2",
                "saveCnt": "1",
                "iCardType": "0",
                "carNo": car_full_no,
                "iLotArea": item.get("iLotArea", "621"),
            },
            timeout=5,
        )

        # DB에 등록 처리 후 성공 안내
        add_registration(car_no, today_yyyymmdd)
        st.success(f"🎉 [{car_full_no}] 3시간 주차 할인이 완료되었습니다.")
        st.session_state.search_results = None
        st.rerun()
      except Exception as e:
        st.error(f"등록 실패: {e}")
