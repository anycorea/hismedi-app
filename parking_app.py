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

# 2. 계정 및 서버 설정
USER_ID = "001"
USER_PW = "1588"
BASE_URL = "http://115.21.205.117"

today = datetime.datetime.now()
today_day = today.strftime("%d")
today_yyyymmdd = today.strftime("%Y%m%d")


def do_login(session):
    """서버에 로그인하여 세션 쿠키를 획득합니다."""
    if USER_ID != "***":
        hashed_pw = hashlib.sha256(USER_PW.encode()).hexdigest()
        try:
            session.post(
                f"{BASE_URL}/login",
                data={"userId": USER_ID, "userPwd": hashed_pw},
                timeout=5,
            )
        except Exception as e:
            st.error(f"로그인 처리 중 오류: {e}")


def get_session():
    if "http_session" not in st.session_state:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"{BASE_URL}/login",
            "X-Requested-With": "XMLHttpRequest",
        })
        do_login(s)
        st.session_state.http_session = s
    return st.session_state.http_session


session = get_session()

# 3. 화면 및 등록 로직
car_no_input = st.text_input(
    "🔹 차량번호 (뒤 4자리)",
    max_chars=4,
    placeholder="예: 2684",
    key="input_car_no",
)

if len(car_no_input) == 4 and car_no_input.isdigit():
    try:
        # 차량 검색 요청
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

            pe_id = target.get("id")
            car_full = target.get("carNo", "")
            entry_str = target.get("entryDateToString", "")
            entry_date_raw = target.get("entryDate", "")

            # dscnt_cnt 체크
            dscnt_cnt_val = str(target.get("dscnt_cnt", "0"))

            if dscnt_cnt_val not in ["0", "None", ""]:
                st.warning(
                    f"⚠️ [{car_full}] 차량은 이미 **주차 할인이 등록되어 있습니다.** ({dscnt_cnt_val}건 적용됨)"
                )
                st.info("※ 추가 할인이 필요한 경우 원무팀에 문의해 주세요.")

            else:
                st.success(
                    f"🚘 **조회 차량:** {car_full} (입차시간: {entry_str})"
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
                        # 저장 전 세션 안전 보장 (재로그인)
                        do_login(session)

                        # 필수 및 보완 파라미터 구성
                        payload = {
                            "peId": str(pe_id),
                            "discountType": "2",
                            "saveCnt": "1",
                            "iCardType": "0",
                            "carNo": str(car_full),
                            "entryDate": str(entry_date_raw),
                            "iLotArea": str(target.get("iLotArea", "621")),
                        }

                        save_res = session.post(
                            f"{BASE_URL}/discount/registration/save",
                            data=payload,
                            timeout=5,
                        )

                        if save_res.status_code == 200:
                            st.balloons()
                            st.success(
                                f"🎉 [{car_full}] 차량에 3시간 주차 할인이 정상 적용되었습니다!"
                            )
                        else:
                            st.error(
                                f"등록 실패 (서버 응답: {save_res.status_code} - {save_res.text})"
                            )
        else:
            st.error("❌ 입차된 차량이 없습니다. 번호를 다시 확인해 주세요.")

    except Exception as e:
        st.error(f"처리 중 오류 발생: {e}")

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
