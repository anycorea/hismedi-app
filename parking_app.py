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
    
    /* 하단 안내문구 */
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


def fetch_car_details(session, pe_id, car_no, entry_date):
    """
    getForDiscount API를 직접 호출하여 차량 입차 상세 및 이미 적용된 할인 내역(parkVisitCar)을 추출합니다.
    """
    try:
        url = f"{BASE_URL}/discount/registration/getForDiscount"
        payload = {
            "peId": str(pe_id),
            "carNo": car_no,
            "entryDate": entry_date,
            "iLotArea": "621"
        }
        res = session.post(url, data=payload, timeout=5)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        st.error(f"상세 조회 네트워크 오류: {e}")
    return None


# 세션 상태 초기화
if "target_car" not in st.session_state:
    st.session_state.target_car = None
if "existing_dc" not in st.session_state:
    st.session_state.existing_dc = None

# ----------------------------------------------------
# 🔹 차량번호 (뒤 4자리) 입력
# ----------------------------------------------------
car_no_input = st.text_input(
    "🔹 차량번호 (뒤 4자리)",
    max_chars=4,
    placeholder="예: 2684",
    key="input_car_no",
)

# 4자리 숫자 입력 시 조회 수행
if len(car_no_input) == 4 and car_no_input.isdigit():
    session = get_session()
    try:
        # 1차: 차량 목록 조회
        list_res = session.post(
            f"{BASE_URL}/discount/registration/listForDiscount",
            data={
                "iLotArea": "621",
                "entryDate": today_yyyymmdd,
                "carNo": car_no_input,
            },
            timeout=5,
        )
        items = list_res.json()

        if items and isinstance(items, list) and len(items) > 0:
            # 기본적으로 첫 번째 검색된 차량 선택
            selected = items[0]
            pe_id = selected.get("id") or selected.get("iID")
            car_full = selected.get("carNo") or selected.get("acPlate1")
            entry_dt = selected.get("entryDate") or selected.get("dtInDateStr")
            
            # 2차: getForDiscount 호출로 parkVisitCar (기존 할인내역) 정밀 확인
            detail_data = fetch_car_details(session, pe_id, car_full, entry_dt)
            
            existing_discount_name = None
            if detail_data:
                park_visit_car = detail_data.get("parkVisitCar", [])
                if isinstance(park_visit_car, list) and len(park_visit_car) > 0:
                    # 첫 번째 등록된 할인 이름 추출 (예: "5시간할인")
                    existing_discount_name = park_visit_car[0].get("discount_name") or park_visit_car[0].get("discountName")

            # 상태 저장
            st.session_state.target_car = {
                "peId": pe_id,
                "carNo": car_full,
                "entryDate": entry_dt,
                "entryStr": selected.get("entryDateToString", ""),
                "iLotArea": selected.get("iLotArea", "621")
            }
            st.session_state.existing_dc = existing_discount_name

        else:
            st.error("❌ 입차된 차량이 없습니다. 번호를 다시 확인해 주세요.")
            st.session_state.target_car = None
            st.session_state.existing_dc = None

    except Exception as e:
        st.error(f"주차 서버 통신 오류: {e}")
        st.session_state.target_car = None
        st.session_state.existing_dc = None
else:
    st.session_state.target_car = None
    st.session_state.existing_dc = None

# ----------------------------------------------------
# 결과 화면 표시
# ----------------------------------------------------
if st.session_state.target_car:
    target = st.session_state.target_car
    existing_dc = st.session_state.existing_dc

    # 🛑 이미 할인이 적용된 경우 (차단)
    if existing_dc:
        st.warning(
            f"⚠️ [{target['carNo']}] 차량은 이미 **[{existing_dc}]**이(가) 등록되어 있습니다."
        )
    # ✅ 할인이 없는 경우 (등록 절차 진행)
    else:
        st.success(
            f"🚘 **조회 차량:** {target['carNo']} (입차시간: {target['entryStr']})"
        )

        receipt_no = st.text_input(
            "🔹 환자 확인번호 (접수증 참조)",
            placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
            key="input_receipt_no",
        )

        if st.button("주차 등록하기 (3시간 할인)", use_container_width=True):
            raw_input = receipt_no.strip()

            if not raw_input.startswith(today_day):
                st.error(
                    f"❌ 환자 확인번호가 올바르지 않습니다. (오늘 일자 [{today_day}]로 시작)"
                )
            else:
                session = get_session()
                try:
                    save_res = session.post(
                        f"{BASE_URL}/discount/registration/save",
                        data={
                            "peId": target["peId"],
                            "discountType": "2",
                            "saveCnt": "1",
                            "iCardType": "0",
                            "carNo": target["carNo"],
                            "iLotArea": target["iLotArea"],
                        },
                        timeout=5,
                    )

                    if save_res.status_code == 200:
                        st.balloons()
                        st.success(
                            f"🎉 [{target['carNo']}] 차량에 3시간 주차 할인이 정상 적용되었습니다!"
                        )
                        st.session_state.target_car = None
                        st.session_state.existing_dc = None
                    else:
                        st.error(f"등록 실패 (상태 코드: {save_res.status_code})")

                except Exception as e:
                    st.error(f"등록 통신 오류: {e}")

# 하단 안내 문구
st.markdown(
    '<div class="info-notice">※ 3시간 이상 주차 시 원무팀에 문의해 주세요.</div>',
    unsafe_allow_html=True,
)

# Enter 키 이동 스크립트
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
