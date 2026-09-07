import datetime
import hashlib
import requests
import streamlit as st
import streamlit.components.v1 as components

# ----------------------------------------------------
# 1. UI 및 스타일 설정
# ----------------------------------------------------
st.set_page_config(
    page_title="히즈메디병원 주차등록", page_icon="🚗", layout="centered"
)

st.markdown(
    """
    <style>
    /* 기본 헤더/메뉴 숨김 */
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
    
    /* 하단 안내문구 (한 줄 처리) */
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

# ----------------------------------------------------
# 2. 계정 및 서버 설정
# ----------------------------------------------------
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
# 🔍 getForDiscount API로 할인 상세 내역 정밀 조회 (수정됨)
# ----------------------------------------------------
def check_existing_discount(session, pe_id, car_no="", entry_date=""):
    """
    getForDiscount API를 호출하여 해당 차량의 parkVisitCar 목록 내 기존 할인명을 확인합니다.
    (이미지 상의 하단 '할인내역' 영역인 '5시간할인' 등 감지)
    """
    try:
        url = f"{BASE_URL}/discount/registration/getForDiscount"
        payload = {
            "peId": pe_id,
            "carNo": car_no,
            "entryDate": entry_date,
            "iLotArea": "621"
        }
        res = session.post(url, data=payload, timeout=3)
        if res.status_code == 200:
            data = res.json()

            # parkVisitCar 배열 검사
            park_visit_car = data.get("parkVisitCar", [])
            if isinstance(park_visit_car, list) and len(park_visit_car) > 0:
                first_dc = park_visit_car[0]
                dc_name = first_dc.get("discount_name") # 예: "5시간할인", "3시간할인"
                if dc_name:
                    return dc_name
                    
    except Exception as e:
        pass
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
    placeholder="예: 2684",
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
# 차량 조회 결과 및 기존 할인 여부 상세 검사
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
    car_full_no = target_car.get("carNo", "")
    entry_date = target_car.get("entryDate", "")

    # 🔍 getForDiscount API 호출하여 기존 할인 존재 여부 확인
    session = get_session()
    existing_dc_name = check_existing_discount(session, pe_id, car_no=car_full_no, entry_date=entry_date)

    # 🛑 이미 할인이 적용된 경우 (차단 안내 표시)
    if existing_dc_name:
        st.warning(
            f"⚠️ [{car_full_no}] 차량은 이미"
            f" **[{existing_dc_name}]**이(가) 등록되어 있습니다."
        )

    # ✅ 기존 할인이 없는 경우에만 입력 필드 및 등록 버튼 노출
    else:
        st.success(
            f"🚘 **조회 차량:** {car_full_no} (입차시간:"
            f" {target_car.get('entryDateToString')})"
        )

        # ----------------------------------------------------
        # 🔹 환자 확인번호 (접수증 참조)
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

# Enter 키 입력 순차 이동 JS
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
