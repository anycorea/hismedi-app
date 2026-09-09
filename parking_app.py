import datetime
import hashlib
import json
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

# 2. 계정 및 기본 설정
USER_ID = "001"
USER_PW = "1588"
BASE_URL = "http://115.21.205.117"

if "search_results" not in st.session_state:
    st.session_state.search_results = None

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


def get_authenticated_session():
    """요청 시마다 안정적인 세션과 인증을 보장합니다."""
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


# 3. 입력 폼
with st.form("parking_form"):
    car_no = st.text_input(
        "🔹 차량번호 (뒤 4자리)", max_chars=4, placeholder="예: 5661"
    )
    receipt_no = st.text_input(
        "🔹 환자 확인번호 (접수증 참조)",
        placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)",
    )

    submitted = st.form_submit_button(
        "🚘 차량 조회하기", use_container_width=True
    )

# 4. 차량 조회 및 정밀 검증
if submitted:
    raw_input = receipt_no.strip()

    if not raw_input.startswith(today_day):
        st.error(
            f"❌ 올바른 확인번호가 아닙니다. (오늘 일자 [{today_day}]로 시작)"
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
                            "⚠️ 이미 주차할인 등록이 되어 있습니다. 추가 등록이 불가능합니다."
                        )
                        st.session_state.search_results = None
                    else:
                        st.session_state.search_results = items
                else:
                    st.warning(
                        "⚠️ 입차된 차량을 찾을 수 없습니다. 입차 여부 및 번호를 확인해 주세요."
                    )
                    st.session_state.search_results = None
            except Exception as e:
                st.error(f"주차 시스템 통신 오류: {e}")

# 5. 차량 선택 및 자동 할인 등록
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
            
            # 백엔드 필수 키값 규격 전송
            save_payload = {
                "peId": pe_id,
                "discountType": "2",
                "saveCnt": "1",
                "iCardType": "0",
                "carNo": car_full_no,
                "iLotArea": lot_area,
                "acPlate2": "",
                "memo": "",
            }

            try:
                save_res = session.post(save_url, data=save_payload, timeout=5)
                res_text = save_res.text.strip().lower()

                if "true" in res_text or "ok" in res_text or "성공" in res_text:
                    st.balloons()
                    st.success(
                        f"🎉 [{car_full_no}] 차량에 3시간 주차 할인이 정상 적용되었습니다!"
                    )
                    st.session_state.search_results = None
                elif "<title>히즈메디병원</title>" in save_res.text:
                    st.error("❌ 로그인 세션이 유효하지 않습니다. 계정을 확인해 주세요.")
                else:
                    st.error(f"❌ 주차 할인 등록 실패: {save_res.text}")

            except Exception as e:
                st.error(f"할인 적용 통신 오류: {e}")

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
