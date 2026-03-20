import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import datetime
import json

# 1. 페이지 설정 (모바일/PC 겸용)
st.set_page_config(page_title="HisMedi 약무신청", page_icon="💊", layout="wide")

# UI 스타일링 (가독성 향상)
st.markdown("""
    <style>
    .stRadio [data-testid="stMarkdownContainer"] { font-weight: bold; }
    .stButton>button { width: 100%; height: 3.5em; border-radius: 10px; font-weight: bold; }
    div[data-testid="stForm"] { border: 1px solid #ddd; padding: 20px; border-radius: 15px; background-color: white; }
    </style>
    """, unsafe_allow_html=True)

# 2. 구글 시트 연결 (Secrets 활용)
@st.cache_resource
def get_sheet():
    try:
        # 깃허브 Secrets에 저장된 GOOGLE_CREDENTIALS 사용
        creds_json = json.loads(st.secrets["GOOGLE_CREDENTIALS"])
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
        client = gspread.authorize(creds)
        # 지정하신 시트 및 워크시트명 연결
        return client.open_by_key("1JL8mtgT1-h9gJWPtY2GO-Ydqjw-4dxb9ykyWfVUXseU").worksheet("약무신청서 - New_stop")
    except Exception as e:
        st.error(f"⚠️ 시트 연결 실패: {e}")
        return None

sheet = get_sheet()

# 3. 메인 레이아웃
st.title("🏥 약무 서비스 신청서 작성")

# 상단 메뉴 탭
tab1, tab2 = st.tabs(["📋 신청서 작성", "🔍 EDI 정보 확인"])

with tab1:
    if sheet:
        # [A2] 신청구분 선택 (드롭다운)
        req_type = st.selectbox("1. 신청 구분을 선택하세요", ["신규입고", "사용중지", "대체입고"], index=None, placeholder="여기에서 선택하세요")

        if req_type:
            with st.form("medicine_form", clear_on_submit=True):
                st.info(f"📍 현재 **[{req_type}]** 신청서를 작성 중입니다.")
                
                # --- 공통 항목: EDI 코드 및 자동조회 결과 구역 ---
                edi_code = st.text_input("2. EDI 코드 입력", placeholder="9자리 숫자 입력 (예: 648500030)")
                
                # (추후 API 연동될 자동조회 예시 데이터)
                drug_info = {"name": "", "company": "", "price": "", "status": "", "date": "", "unit": ""}
                if edi_code == "648500030":
                    drug_info = {"name": "타이레놀정500mg", "company": "(주)한국얀센", "price": "51", "status": "정상", "date": "2024-01-01", "unit": "500mg/1정"}

                # 레이아웃 구성 (2열)
                col1, col2 = st.columns(2)
                
                with col1:
                    st.text_input("약품명 (자동)", value=drug_info["name"], disabled=True)
                    st.text_input("업체명 (자동)", value=drug_info["company"], disabled=True)
                    st.text_input("상한금액 (자동)", value=drug_info["price"], disabled=True)
                    
                    # [구입처] - 신규입고, 대체입고 시에만 노출
                    purchase = ""
                    if req_type in ["신규입고", "대체입고"]:
                        purchase = st.text_input("구입처 입력")

                with col2:
                    st.text_input("현재상태 (자동)", value=drug_info["status"], disabled=True)
                    # 급여/비급여, 원내/원외 (모든 신청 공통)
                    pay_cat = st.radio("급여구분", ["급여", "비급여", "100대100"], horizontal=True)
                    in_out = st.radio("원내/원외 구분", ["원내", "원외", "원내/원외"], horizontal=True)

                st.markdown("---")

                # --- [사용중지] 전용 동적 항목 (3열 로직) ---
                stop_date, stop_reason, stock_yn, stock_how = "", "", "", ""
                if req_type == "사용중지":
                    st.warning("⚠️ 사용중지 상세 정보를 입력하세요.")
                    c1, c2 = st.columns(2)
                    with c1:
                        stop_date = st.date_input("사용중지일", datetime.date.today())
                        stop_reason = st.selectbox("사용중지 사유", ["생산중단", "품절", "대체 약제로 변경 예정", "회수약품", "제조사 변경", "EDI 코드 삭제", "유통기한 만료", "기타"])
                    with c2:
                        stock_yn = st.radio("재고여부", ["유", "무"], horizontal=True)
                        stock_how = st.selectbox("재고처리방법", ["재고 소진", "반품", "폐기", "해당없음"])

                # 비고 (공통 입력창)
                note = st.text_area("비고 (기타 요청사항)")

                # 제출 버튼
                if st.form_submit_button("📩 구글 시트에 신청서 제출"):
                    if not edi_code or not drug_info["name"]:
                        st.error("EDI 코드를 확인하여 정보를 불러와주세요.")
                    else:
                        # 시트 헤더 순서(A~S열)에 맞춰 데이터 구성
                        row_data = [
                            req_type, edi_code, drug_info["name"], drug_info["company"],
                            "급여", drug_info["price"], drug_info["status"], drug_info["date"], drug_info["unit"],
                            purchase, pay_cat, in_out, str(stop_date), stop_reason, note,
                            stock_yn, stock_how, "0", ""
                        ]
                        try:
                            sheet.append_row(row_data)
                            st.success("🎉 신청서 제출 완료!")
                            st.balloons()
                        except Exception as e:
                            st.error(f"저장 오류: {e}")
    else:
        st.error("데이터베이스 연결 대기 중...")

with tab2:
    st.subheader("🔎 EDI 코드 간편 확인")
    st.info("신청서 작성 전 정보를 미리 확인하는 조회기입니다.")
