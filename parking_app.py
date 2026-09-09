import datetime, hashlib, json, requests, streamlit as st, streamlit.components.v1 as components

# 1. UI 및 스타일 설정
st.set_page_config(page_title="히즈메디병원 주차등록", page_icon="🚗", layout="centered")
st.markdown("""<style>
#MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"] { display: none !important; }
.custom-title { font-size: 1.3rem !important; font-weight: 700; color: #1E293B; text-align: center; margin-bottom: 2px; }
.custom-sub { font-size: 0.85rem; color: #64748B; text-align: center; margin-bottom: 20px; }
div.stButton > button { background-color: #2563EB !important; color: white !important; font-weight: 700 !important; font-size: 1rem !important; border-radius: 8px !important; border: none !important; padding: 12px 0px !important; }
</style>""", unsafe_allow_html=True)

st.markdown('<div class="custom-title">🚗 히즈메디병원 주차등록</div>', unsafe_allow_html=True)
st.markdown('<div class="custom-sub">진료 및 검진 방문객 셀프 주차등록</div>', unsafe_allow_html=True)

USER_ID, USER_PW, BASE_URL = "001", "1588", "http://115.21.205.117"
today = datetime.datetime.now()
today_day, today_yyyymmdd = today.strftime("%d"), today.strftime("%Y%m%d")

def get_authenticated_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0", "Referer": f"{BASE_URL}/login", "X-Requested-With": "XMLHttpRequest"})
    if USER_ID != "***":
        hashed_pw = hashlib.sha256(USER_PW.encode("utf-8")).hexdigest()
        try: s.post(f"{BASE_URL}/login", data={"userId": USER_ID, "userPwd": hashed_pw}, timeout=5)
        except Exception as e: st.error(f"로그인 통신 오류: {e}")
    return s

car_no_input = st.text_input("◆ 차량번호 (뒤 4자리)", max_chars=4, placeholder="예: 2684", key="input_car_no")

if len(car_no_input) == 4 and car_no_input.isdigit():
    try:
        session = get_authenticated_session()
        list_res = session.post(f"{BASE_URL}/discount/registration/listForDiscount", data={"iLotArea": "621", "entryDate": today_yyyymmdd, "carNo": car_no_input}, timeout=5)
        items = list_res.json()

        if items and isinstance(items, list) and len(items) > 0:
            target = items[0]
            pe_id, car_full, entry_str, lot_area = target.get("id"), target.get("carNo", ""), target.get("entryDateToString", ""), target.get("iLotArea", "621")

            # 🛑 [정밀 중복 감지 로직] 모든 형태의 기존 할인 내역 탐지
            dc_cnt_val = str(target.get("dscnt_cnt") or target.get("dscntCnt") or target.get("discountCnt") or "0")
            dc_list = target.get("dcDetailList") or target.get("dscntList") or target.get("discountList") or target.get("dcList") or []
            
            # JSON 텍스트 내에서 할인 관련 키워드 정밀 체크 ("시간" 또는 "할인" 또는 리스트 항목 존재)
            item_str = json.dumps(target, ensure_ascii=False)
            has_dc_keyword = any(k in item_str for k in ["시간", "할인", "dscnt", "discount"]) and not item_str.endswith('[]')

            has_discount = (dc_cnt_val not in ["0", "None", "", "null"]) or len(dc_list) > 0 or has_dc_keyword

            if has_discount:
                st.warning(f"⚠️ [{car_full}] 차량은 이미 주차 할인이 등록되어 있습니다.")
                st.info("※ 주차시간 조정은 원무팀에 문의해 주세요.")
            else:
                st.success(f"🚘 **조회 차량:** {car_full} (입차시간: {entry_str})")
                receipt_no = st.text_input("🔹 환자 확인번호 (접수증 참조)", placeholder=f"예: {today_day} + 환자번호 (오늘 일자 {today_day}로 시작)", key="input_receipt_no")

                if st.button("주차 등록하기 (3시간 할인)", use_container_width=True):
                    raw_input = receipt_no.strip()
                    if not raw_input.startswith(today_day):
                        st.error(f"❌ 환자 확인번호가 올바르지 않습니다. (오늘 일자 [{today_day}]로 시작)")
                    elif not raw_input[2:].isdigit():
                        st.error("❌ 환자 확인번호는 숫자만 입력 가능합니다.")
                    else:
                        save_session = get_authenticated_session()
                        save_payload = {"peId": pe_id, "discountType": "2", "saveCnt": "1", "iCardType": "0", "carNo": car_full, "iLotArea": lot_area, "acPlate2": "", "memo": ""}
                        save_res = save_session.post(f"{BASE_URL}/discount/registration/save", data=save_payload, timeout=5)
                        res_text = save_res.text.strip().lower()

                        if "true" in res_text or "ok" in res_text or "성공" in res_text:
                            st.balloons()
                            st.success(f"🎉 [{car_full}] 차량에 3시간 주차 할인이 정상 적용되었습니다!")
                            st.info("※ 주차시간 조정은 원무팀에 문의해 주세요.")
                        elif "<title>히즈메디병원</title>" in save_res.text:
                            st.error("❌ 로그인 세션이 유효하지 않습니다.")
                        else:
                            st.error(f"❌ 주차 할인 등록 실패: {save_res.text}")
        else:
            st.error("❌ 입차된 차량이 없습니다. 번호를 다시 확인해 주세요.")
    except Exception as e:
        st.error(f"처리 중 오류 발생: {e}")

components.html("""<script>
const doc = window.parent.document;
doc.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') {
        const inputs = Array.from(doc.querySelectorAll('input[type="text"]'));
        const index = inputs.indexOf(doc.activeElement);
        if (index > -1 && index < inputs.length - 1) { e.preventDefault(); inputs[index + 1].focus(); }
    }
});
</script>""", height=0)
