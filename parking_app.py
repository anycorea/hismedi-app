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
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"] { display: none !important; }
    
    .custom-title {
        font-size: 1.3rem !important;
        font-weight: 700;
        color: #1E293B;
        text-align: center;
        margin-bottom: 2px;
    }
    .custom-sub {
        font-size: 0.85rem;
        color: #64748B;
        text-align: center;
        margin-bottom: 20px;
    }
    
    div.stButton > button {
        background-color: #2563EB !important;
        color: white !important;
        font-weight: 700 !important;
        font-size: 1rem !important;
        border-radius: 8px !important;
        border: none !important;
        padding: 12px 0px !important;
    }
    
    .info-notice {
        background-color: #FEF3C7;
        color: #92400E;
        padding: 10px 12px;
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

st.markdown('<div class="custom-title">🚗 히즈메디병원 주차등록</div>', unsafe_allow_html=True)
st.markdown('<div class="custom-sub">진료 및 검진 방문객 셀프 주차등록</div>', unsafe_allow_html=True)

# ----------------------------------------------------
# 2. 서버 및 세션 관리 (세션 지속 유지)
# ----------------------------------------------------
USER_ID = "001"
USER_PW = "1588"
BASE_URL = "http://115.21.205.117"

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")

def init_session():
    """Streamlit 세션 상태에 Requests Session을 생성하고 로그인 쿠키를 유지시킵니다."""
    if "http_session" not in st.session_state:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"{BASE_URL}/login",
            "X-Requested-With": "XMLHttpRequest",
        })
        if USER_ID != "***":
            hashed_pw = hashlib.sha256(USER_PW.encode()).hexdigest()
            try:
                s.post(
                    f"{BASE_URL}/login",
                    data={"userId": USER_ID, "userPwd": hashed_pw},
                    timeout=5,
                )
            except Exception as e:
                st.error(f"로그인 처리 중 오류: {e}")
        st.session_state.http_session = s
    return st.session_state.http_session

session = init_session()

# ----------------------------------------------------
# 3. API 호출 함수 (상세 로그 디버깅 지원)
# ----------------------------------------------------
def get_car_discount_info(pe_id, car_no, entry_date):
    """getForDiscount를 호출하여 parkVisitCar 및 전체 데이터를 받아옵니다."""
    url = f"{BASE_URL}/discount/registration/getForDiscount"
    payload = {
        "peId": str(pe_id),
        "carNo": str(car_no),
        "entryDate": str(entry_date),
        "iLotArea": "621"
    }
    try:
        res = session.post(url, data=payload, timeout=5)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        st.error(f"getForDiscount 통신 오류: {e}")
    return None

# ----------------------------------------------------
# 4. 화면 로직
# ----------------------------------------------------
car_no_input = st.text_input(
    "🔹 차량번호 (뒤 4자리)",
    max_chars=4,
    placeholder="예: 2684",
    key="input_car_no",
)

if len(car_no_input) == 4 and car_no_input.isdigit():
    try:
        # 1. 차량 검색 (listForDiscount)
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
            target = items[0]
            
            # 파라미터 값 추출 (키 명칭 다각도 대응)
            pe_id = target.get("id") or target.get("iID") or target.get("peId")
            car_full = target.get("carNo") or target.get("acPlate1")
            entry_date = target.get("entryDate") or target.get("dtInDateStr")
            entry_str = target.get("entryDateToString", "")

            # 2. 할인 내역 상세 조회 (getForDiscount)
            discount_data = get_car_discount_info(pe_id, car_full, entry_date)
            
            # 🔍 [디버깅용 로그] 서버 응답 데이터를 직접 확인 (확인 후 삭제 가능)
            with st.expander("🛠️ 서버 응답 데이터 확인 (디버그)"):
                st.write("listForDiscount 데이터:", target)
                st.write("getForDiscount 데이터:", discount_data)

            # 3. 기존 할인 존재 여부 체크
            existing_dc_name = None
            if discount_data:
                park_visit_car = discount_data.get("parkVisitCar", [])
                if isinstance(park_visit_car, list) and len(park_visit_car) > 0:
                    first_item = park_visit_car[0]
                    existing_dc_name = first_item.get("discount_name") or first_item.get("discountName") or first_item.get("dc_name")

            # 4. 결과에 따른 UI 분기
            if existing_dc_name:
                st.warning(f"⚠️ [{car_full}] 차량은 이미 **[{existing_dc_name}]**이(가) 등록되어 있습니다.")
            else:
                st.success(f"🚘 **조회 차량:** {car_full} (입차시간: {entry_str})")

                receipt_no = st.text_input(
                    "🔹 환자 확인번호 (접수증 참조)",
                    placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
                    key="input_receipt_no",
                )

                if st.button("주차 등록하기 (3시간 할인)", use_container_width=True):
                    raw_input = receipt_no.strip()
                    if not raw_input.startswith(today_day):
                        st.error(f"❌ 환자 확인번호가 올바르지 않습니다. (오늘 일자 [{today_day}]로 시작)")
                    else:
                        save_res = session.post(
                            f"{BASE_URL}/discount/registration/save",
                            data={
                                "peId": pe_id,
                                "discountType": "2",
                                "saveCnt": "1",
                                "iCardType": "0",
                                "carNo": car_full,
                                "iLotArea": target.get("iLotArea", "621"),
                            },
                            timeout=5,
                        )
                        if save_res.status_code == 200:
                            st.balloons()
                            st.success(f"🎉 [{car_full}] 차량에 3시간 주차 할인이 정상 적용되었습니다!")
                        else:
                            st.error("등록에 실패했습니다.")
        else:
            st.error("❌ 입차된 차량이 없습니다. 번호를 다시 확인해 주세요.")

    except Exception as e:
        st.error(f"처리 중 오류 발생: {e}")

st.markdown('<div class="info-notice">※ 3시간 이상 주차 시 원무팀에 문의해 주세요.</div>', unsafe_allow_html=True)

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
