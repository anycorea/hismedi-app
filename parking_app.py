import datetime
import hashlib
import html
import json
import uuid
from zoneinfo import ZoneInfo

import gspread
import requests
import streamlit as st
import streamlit.components.v1 as components
from google.oauth2.service_account import Credentials

# 1. UI 및 페이지 기본 설정
st.set_page_config(page_title="히즈메디병원 주차등록", page_icon="🏥", layout="centered")

# 세션 상태 초기화 (결과 화면 전환 플래그 및 메시지 저장)
if "result_state" not in st.session_state:
    st.session_state.result_state = None  # None, "success", "already", "not_found"
if "result_message" not in st.session_state:
    st.session_state.result_message = ""

# 2. 어르신 배려 & 모바일 최적화 & 다크모드 방지 CSS 스타일링
st.markdown("""
<style>
    /* ----------------------------------------------------
       다크모드 강제 오버라이드 및 기본 앱 배경 고정
    ---------------------------------------------------- */
    .stApp {
        background-color: #FFFFFF !important;
        color: #1E293B !important;
    }
    
    /* 상단 헤더 및 기본 메뉴 숨기기 */
    #MainMenu, header, footer, .stAppHeader, [data-testid="stHeader"] { display: none !important; }

    /* 여백 및 전체 배경 조정 */
    .block-container { 
        padding-top: 1rem !important; 
        padding-bottom: 2rem !important; 
        background-color: #FFFFFF !important;
    }
    
    /* 타이틀 및 로고 상단 영역 */
    .header-box {
        background-color: #FFFFFF !important;
        border: 2px solid #E2E8F0;
        border-radius: 16px;
        padding: 20px 15px 15px 15px;
        text-align: center;
        margin-bottom: 24px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
    }
    .header-box img {
        max-width: 220px;
        height: auto;
        margin-bottom: 8px;
    }
    .main-title { 
        font-size: 1.5rem !important; 
        font-weight: 800; 
        color: #1E293B !important; 
        margin-top: 5px; 
    }
    .sub-title { 
        font-size: 1rem; 
        color: #64748B !important; 
        margin-top: 4px; 
        font-weight: 600; 
    }
    
    /* 라벨 및 안내 문구 폰트 고정 */
    label, div[data-testid="stMarkdownContainer"] p, div[data-testid="stMarkdownContainer"] span {
        font-size: 1.15rem !important;
        font-weight: 700 !important;
        color: #1E293B !important;
    }
    
    /* 입력창(Text Input) 디자인 및 다크모드 글자색 고정 */
    div[data-baseweb="input"] {
        border-radius: 12px !important;
        border: 2px solid #CBD5E1 !important;
        height: 56px !important;
        background-color: #FFFFFF !important;
    }
    div[data-baseweb="input"] input {
        font-size: 1.3rem !important;
        font-weight: 700 !important;
        text-align: center !important;
        color: #0F172A !important;
        background-color: #FFFFFF !important;
    }
    div[data-baseweb="input"]:focus-within {
        border-color: #2563EB !important;
        box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.15) !important;
    }
    
    /* 제출 버튼 시인성 및 크기 극대화 */
    div.stButton > button {
        background-color: #2563EB !important;
        color: #FFFFFF !important;
        font-weight: 800 !important;
        font-size: 1.35rem !important;
        border-radius: 12px !important;
        border: none !important;
        padding: 16px 0px !important;
        height: 60px !important;
        margin-top: 10px !important;
        box-shadow: 0 4px 10px rgba(37, 99, 235, 0.25) !important;
    }
    div.stButton > button:hover {
        background-color: #1D4ED8 !important;
    }

    /* 경고 및 성공 메시지 박스 텍스트 크기 확대 및 배경 고정 */
    .stAlert {
        border-radius: 12px !important;
        padding: 15px !important;
    }
    .stAlert div[data-testid="stMarkdownContainer"] p {
        font-size: 1.1rem !important;
        line-height: 1.5 !important;
    }
    
    /* 입차 정보 요약 카드 스타일 */
    .info-card {
        background-color: #F8FAFC !important;
        border: 2px solid #E2E8F0;
        border-radius: 12px;
        padding: 16px;
        margin-top: 15px;
        margin-bottom: 15px;
        text-align: center;
    }
    .info-card .car-num { font-size: 1.4rem; font-weight: 800; color: #0F172A !important; }
    .info-card .entry-time { font-size: 1.05rem; color: #475569 !important; margin-top: 4px; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# 3. 로고를 포함한 헤더 UI
LOGO_URL = "https://lh3.googleusercontent.com/d/1O7VZsctdhhpxyRXaORKLJEL6LL738ivs"

st.markdown(f"""
<div class="header-box">
    <img src="{LOGO_URL}" alt="히즈메디병원 로고">
    <div class="main-title">무료 주차 등록</div>
    <div class="sub-title">진료 및 검진 방문객 셀프 서비스</div>
</div>
""", unsafe_allow_html=True)

USER_ID, USER_PW, BASE_URL = "001", "1588", "http://115.21.205.117"
today = datetime.datetime.now(ZoneInfo("Asia/Seoul"))
today_yyyymmdd = today.strftime("%Y%m%d")

# 시트 1행의 A:K 헤더와 순서까지 일치해야 합니다.
SHEET_HEADERS = [
    "기록ID", "등록요청일시", "환자등록번호", "차량뒷번호", "차량번호",
    "입차일시", "입차ID", "무료주차시간", "처리결과", "최종갱신일시", "비고",
]


def now_kst():
    return datetime.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")


def get_worksheet():
    # 인증 객체를 사용자 간 공유하지 않습니다. 실제 키는 Secrets에서만 읽습니다.
    info = dict(st.secrets["gcp_service_account"])
    info["private_key"] = info["private_key"].replace("\\n", "\n")
    credentials = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    client = gspread.authorize(credentials)
    client.set_timeout(10)
    config = st.secrets["gsheet"]
    worksheet = client.open_by_key(config["spreadsheet_id"]).worksheet(
        config.get("worksheet_name", "parking_logs")
    )
    if worksheet.row_values(1) != SHEET_HEADERS:
        raise ValueError("구글시트 1행의 A:K 헤더가 설정과 다릅니다.")
    return worksheet


def ensure_request_logged(record):
    """할인 요청 전에 기록. 응답 유실 후 재시도 시 같은 기록ID를 확인합니다."""
    worksheet = get_worksheet()
    cell = worksheet.find(record[0], in_column=1)
    if cell is None:
        # 서버의 append로 여러 사용자 기록이 같은 행을 덮어쓰지 않도록 합니다.
        # RAW로 환자등록번호/뒷번호의 선행 0과 문자열을 보존합니다.
        worksheet.append_row(
            record, value_input_option="RAW", insert_data_option="INSERT_ROWS",
            table_range="A:K",
        )


def update_result_record(record):
    worksheet = get_worksheet()
    cell = worksheet.find(record[0], in_column=1)
    if cell is None:
        raise ValueError("등록 요청 기록을 찾을 수 없습니다.")
    # 기존 행의 결과만 갱신. 이 함수는 주차 할인 API를 호출하지 않습니다.
    worksheet.update(
        range_name=f"I{cell.row}:K{cell.row}",
        values=[record[8:11]], value_input_option="RAW",
    )


def get_authenticated_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0", "Referer": f"{BASE_URL}/login",
        "X-Requested-With": "XMLHttpRequest",
    })
    hashed_pw = hashlib.sha256(USER_PW.encode("utf-8")).hexdigest()
    try:
        response = session.post(
            f"{BASE_URL}/login", data={"userId": USER_ID, "userPwd": hashed_pw}, timeout=5
        )
        response.raise_for_status()
    except requests.RequestException:
        session.close()
        raise
    return session


def is_confirmed_success(response):
    # 'token' 등의 임의 문자열에 'ok'가 포함됐다고 성공으로 기록하지 않습니다.
    text = response.text.strip()
    if text.lower() in {"true", "ok", "success", "성공"}:
        return True
    try:
        value = response.json()
    except ValueError:
        return False
    if value is True:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "ok", "success", "성공"}
    if isinstance(value, dict):
        return any(
            value.get(key) is True or (
                isinstance(value.get(key), str)
                and value[key].strip().lower() in {"true", "ok", "success", "성공"}
            ) for key in ("success", "result")
        )
    return False


def submit_registration(raw_receipt, car_suffix, pe_id, car_full, entry_str, lot_area):
    signature = (raw_receipt, car_suffix, str(pe_id), car_full)
    pending = st.session_state.get("pending_request")
    if pending is None or pending["signature"] != signature:
        timestamp = now_kst()
        record = [
            str(uuid.uuid4()), timestamp, raw_receipt, car_suffix, car_full,
            entry_str, str(pe_id), "3", "처리중", timestamp,
            "결과 갱신 전에는 할인 적용 여부를 확인해 주세요.",
        ]
        pending = {"signature": signature, "record": record}
        st.session_state.pending_request = pending
    record = pending["record"]
    try:
        ensure_request_logged(record)
    except Exception:
        st.error("등록 내역을 저장할 수 없어 주차 할인을 요청하지 않았습니다. 잠시 후 다시 시도하거나 원무팀에 문의해 주세요.")
        return

    state = "check_required"
    status = "확인필요"
    note = "주차 시스템에서 실제 할인 적용 여부 확인 필요"
    message = "주차 할인 처리 결과를 확인하지 못했습니다. 중복 신청하지 마시고 원무팀에 문의해 주세요."
    try:
        with get_authenticated_session() as save_session:
            save_payload = {
                "peId": pe_id, "discountType": "2", "saveCnt": "1", "iCardType": "0",
                "carNo": car_full, "iLotArea": lot_area, "acPlate2": "", "memo": "",
            }
            response = save_session.post(
                f"{BASE_URL}/discount/registration/save", data=save_payload, timeout=5
            )
            response.raise_for_status()
            if is_confirmed_success(response):
                state, status, note = "success", "등록완료", "3시간 할인 적용 완료"
                message = f"🎉 [{car_full}] 차량에 3시간 주차 할인이 완료되었습니다."
            elif "<title>히즈메디병원</title>" in response.text:
                note = "로그인 세션 만료 응답; 실제 적용 여부 확인 필요"
    except requests.RequestException:
        note = "주차 시스템 통신 오류; 실제 적용 여부 확인 필요"
    except Exception:
        note = "처리 오류; 실제 적용 여부 확인 필요"

    # 먼저 결과 화면으로 잠금. DB 재시도는 할인 요청과 분리합니다.
    record[8:11] = [status, now_kst(), note]
    st.session_state.result_state = state
    st.session_state.result_message = message
    st.session_state.unsaved_record = record
    try:
        update_result_record(record)
        st.session_state.unsaved_record = None
    except Exception:
        pass  # 결과 화면에서 기록만 재저장할 수 있습니다.
    st.rerun()

# ----------------------------------------------------
# 4. 결과 화면 처리 (성공/중복/입차없음 상태 시 입력폼 가림)
# ----------------------------------------------------
if st.session_state.result_state == "success":
    st.success(st.session_state.result_message)
    st.info("💡 처리가 완료되었습니다. 이 창을 닫아주시기 바랍니다.")

elif st.session_state.result_state == "check_required":
    st.warning(st.session_state.result_message)

elif st.session_state.result_state == "already":
    st.warning(st.session_state.result_message)
    st.info("💡 처리가 완료되었습니다. 이 창을 닫아주시기 바랍니다.")

elif st.session_state.result_state == "not_found":
    st.error(st.session_state.result_message)
    st.info("💡 이 창을 닫고 번호를 다시 확인한 후 재시도해 주세요.")

# ----------------------------------------------------
# 5. 미완료 상태 시 기존 입력 및 등록 절차 실행
# ----------------------------------------------------
else:
    # 1) 환자등록번호 입력
    receipt_no = st.text_input(
        "1. 환자등록번호를 입력하세요", 
        max_chars=6, 
        placeholder="접수증/영수증의 번호 입력", 
        key="input_receipt_no"
    )

    # 2) 차량 번호 입력
    car_no_input = st.text_input(
        "2. 차량 뒷번호 4자리를 입력하세요", 
        max_chars=4, 
        placeholder="예: 1234", 
        key="input_car_no"
    )

    # 차량 번호 입력 시 조회 및 등록 절차 실행
    if len(car_no_input) == 4 and car_no_input.isdigit():
        raw_receipt = receipt_no.strip()
        
        # 환자등록번호 유효성 사전 체크
        if not (raw_receipt.isdigit() and len(raw_receipt) in [5, 6]):
            st.error("❌ 먼저 '환자등록번호'를 정확히 입력해 주세요 (숫자 5자리 또는 6자리).")
        else:
            try:
                with get_authenticated_session() as session:
                    list_res = session.post(
                        f"{BASE_URL}/discount/registration/listForDiscount",
                        data={"iLotArea": "621", "entryDate": today_yyyymmdd, "carNo": car_no_input},
                        timeout=5,
                    )
                    list_res.raise_for_status()
                    items = list_res.json()
                if not isinstance(items, list):
                    raise ValueError("차량 조회 응답 형식 오류")

                if items and isinstance(items, list) and len(items) > 0:
                    target = items[0]
                    pe_id, car_full, entry_str, lot_area = target.get("id"), target.get("carNo", ""), target.get("entryDateToString", ""), target.get("iLotArea", "621")

                    # 할인 중복 검증
                    dc_cnt_raw = target.get("dscnt_cnt") or target.get("dscntCnt") or target.get("discountCnt") or 0
                    try: dc_cnt_num = int(dc_cnt_raw)
                    except: dc_cnt_num = 0

                    dc_list = target.get("dcDetailList") or target.get("dscntList") or target.get("discountList") or target.get("dcList") or []
                    dc_name = target.get("discountName") or target.get("dscntName") or target.get("dcName") or ""

                    has_discount = (dc_cnt_num > 0) or (len(dc_list) > 0) or bool(dc_name)

                    if has_discount:
                        # 이미 할인된 경우 -> 전용 완료 화면으로 전환
                        st.session_state.result_state = "already"
                        st.session_state.result_message = f"⚠️ [{car_full}] 차량은 이미 주차 할인이 적용되어 있습니다.\n\n※ 조정이 필요하시면 원무팀에 문의해 주세요."
                        st.rerun()
                    else:
                        # 입차 정보 표시 카드
                        st.markdown(f"""
                        <div class="info-card">
                            <div class="car-num">차량번호: {html.escape(str(car_full))}</div>
                            <div class="entry-time">입차시간: {html.escape(str(entry_str))}</div>
                        </div>
                        """, unsafe_allow_html=True)

                        if st.button("주차 등록하기 (3시간 무료)", use_container_width=True):
                            submit_registration(
                                raw_receipt, car_no_input, pe_id, car_full, entry_str, lot_area
                            )
                else:
                    # 입차 차량 없음 -> 전용 완료 화면으로 전환
                    st.session_state.result_state = "not_found"
                    st.session_state.result_message = "❌ 입차된 차량이 없습니다. 차량 번호를 다시 확인해 주세요."
                    st.rerun()
            except Exception as e:
                st.error("차량 조회 중 오류가 발생했습니다. 잠시 후 다시 시도하거나 원무팀에 문의해 주세요.")

# 할인 재요청 없이 기록 갱신만 재시도합니다.
if st.session_state.get("unsaved_record"):
    st.warning("처리 결과 기록이 아직 저장되지 않았습니다. 아래 버튼을 눌러 주세요.")
    if st.button("처리 기록 다시 저장", use_container_width=True):
        try:
            update_result_record(st.session_state.unsaved_record)
        except Exception:
            st.error("기록을 저장하지 못했습니다. 이 화면을 유지하고 원무팀에 문의해 주세요.")
        else:
            st.session_state.unsaved_record = None
            st.rerun()

# Enter 키 입력 시 다음 Input 포커스 자동 이동
components.html("""<script>
const doc = window.parent.document;
doc.addEventListener('keydown', function(e) {
    if (e.key === 'Enter') {
        const inputs = Array.from(doc.querySelectorAll('input[type="text"]'));
        const index = inputs.indexOf(doc.activeElement);
        if (index > -1 && index < inputs.length - 1) { 
            e.preventDefault(); 
            inputs[index + 1].focus(); 
        }
    }
});
</script>""", height=0)
