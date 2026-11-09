import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from PIL import Image

# 1. 페이지 설정 (와이드 레이아웃)
st.set_page_config(
    page_title="Hismedi 진료현황",
    page_icon="🏥",
    layout="wide"
)

# 세련된 UI를 위한 커스텀 스타일 적용
st.markdown("""
    <style>
    .main-title { font-size: 24px; font-weight: bold; color: #1f77b4; margin-bottom: 0px; }
    .sub-title { font-size: 14px; color: #666; margin-bottom: 20px; }
    .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border: 1px solid #e9ecef; }
    </style>
""", unsafe_allow_html=True)

# 2. 구글 시트 데이터 로드 및 업데이트 함수 (캐시 적용)
@st.cache_data(ttl=10)
def load_all_data():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = dict(st.secrets["gcp_service_account"])
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    
    spreadsheet_id = st.secrets["gsheet"]["spreadsheet_id"]
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 일자별_통계_DB 탭 로드
    try:
        db_ws = spreadsheet.worksheet("일자별_통계_DB")
        db_rows = db_ws.get_all_values()
        if len(db_rows) > 2:
            headers = db_rows[1]  # 2행을 헤더로 사용
            df_db = pd.DataFrame(db_rows[2:], columns=headers[:len(db_rows[2])])
        else:
            df_db = pd.DataFrame()
    except:
        df_db = pd.DataFrame()

    # PIN_관리 탭 로드 (A:진료과, B:의사, C:PIN, D:권한)
    try:
        pin_ws = spreadsheet.worksheet("PIN_관리")
        pin_records = pin_ws.get_all_records()
        df_pin = pd.DataFrame(pin_records)
    except:
        df_pin = pd.DataFrame(columns=["진료과", "의사", "PIN", "권한"])
        
    return df_db, df_pin

# 데이터 불러오기
try:
    df_main, df_pins = load_all_data()
except Exception as e:
    st.error(f"구글 시트 연동 오류: {e}")
    st.stop()

# 3. 세션 상태 기반 PIN 인증 로직 (엔터키 지원)
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
    st.session_state["user_info"] = None

if not st.session_state["authenticated"]:
    col_l, col_r = st.columns([1, 2])
    with col_l:
        try:
            logo_img = Image.open("Hismedi_logo.png")
            st.image(logo_img, width=180)
        except:
            st.markdown("### 🏥 HISMEDI")
            
        st.markdown("<p class='main-title'>진료현황 시스템</p>", unsafe_allow_html=True)
        st.markdown("<p class='sub-title'>PIN 번호 입력 후 엔터를 누르세요.</p>", unsafe_allow_html=True)
        
        # 엔터키 로그인을 위한 form 적용
        with st.form("login_form"):
            pin_input = st.text_input("PIN 코드 (4자리)", type="password", max_chars=4)
            submit_btn = st.form_submit_button("로그인", use_container_width=True)
            
            if submit_btn:
                pin_str = str(pin_input).strip()
                if pin_str == "0000":
                    st.session_state["authenticated"] = True
                    st.session_state["user_info"] = {"진료과": "전체", "의사": "마스터", "PIN": "0000", "권한": "master"}
                    st.rerun()
                elif not df_pins.empty:
                    # B열(의사) 기준 매칭
                    matched = df_pins[df_pins["PIN"].astype(str).str.strip() == pin_str]
                    if not matched.empty:
                        user_row = matched.iloc[0]
                        st.session_state["authenticated"] = True
                        st.session_state["user_info"] = {
                            "진료과": str(user_row.get("진료과", "전체")),
                            "의사": str(user_row.get("의사", "사용자")),
                            "PIN": str(user_row.get("PIN", "")),
                            "권한": str(user_row.get("권한", "user"))
                        }
                        st.rerun()
                    else:
                        st.error("일치하지 않는 PIN 코드입니다.")
                else:
                    st.error("PIN_관리 시트를 읽어오지 못했습니다.")
    st.stop()

user = st.session_state["user_info"]

# 4. 왼쪽 메뉴 (사이드바) 구성
with st.sidebar:
    try:
        logo_img = Image.open("Hismedi_logo.png")
        st.image(logo_img, width=140)
    except:
        st.markdown("### 🏥 HISMEDI")
        
    st.markdown(f"👤 **접속자:** {user['의사']} ({user['진료과']})")
    
    if st.button("🔄 구글시트 동기화", use_container_width=True):
        st.cache_data.clear()
        st.success("동기화 완료!")
        st.rerun()
        
    st.markdown("---")
    st.markdown("### 🔒 PIN 번호 변경")
    with st.form("change_pin_form"):
        new_pin = st.text_input("새로운 PIN (4자리)", type="password", max_chars=4)
        change_btn = st.form_submit_button("PIN 변경", use_container_width=True)
        if change_btn:
            if len(new_pin.strip()) == 4:
                try:
                    # 구글 시트 연결하여 PIN 번호 업데이트
                    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
                    creds_dict = dict(st.secrets["gcp_service_account"])
                    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                    gc = gspread.authorize(credentials)
                    spreadsheet = gc.open_by_key(st.secrets["gsheet"]["spreadsheet_id"])
                    pin_ws = spreadsheet.worksheet("PIN_관리")
                    
                    # 해당 의사 행 찾기
                    cell = pin_ws.find(user["의사"])
                    if cell:
                        # C열(3번째 열)이 PIN
                        pin_ws.update_cell(cell.row, 3, new_pin.strip())
                        st.success("PIN 번호가 변경되었습니다!")
                        st.cache_data.clear()
                    else:
                        st.error("시트에서 사용자를 찾을 수 없습니다.")
                except Exception as ex:
                    st.error(f"변경 실패: {ex}")
            else:
                st.warning("4자리 숫자를 입력해주세요.")

    st.markdown("---")
    st.markdown("### 🔍 조회 필터")
    
    if not df_main.empty:
        df_main.columns = [str(c).strip() for c in df_main.columns]
        
        if "일자" in df_main.columns and "진료과" in df_main.columns and "의사명" in df_main.columns:
            df_main = df_main[df_main['일자'].astype(str).str.strip() != ""]
            df_main = df_main[~df_main['진료과'].astype(str).str.contains("합계", na=False)]
            
            dates = sorted(df_main['일자'].unique(), reverse=True)
            selected_date = st.selectbox("📅 조회 일자", dates)
            
            # 권한별 필터 제어
            if user["권한"] == "master":
                depts = ["전체"] + sorted(df_main['진료과'].unique().tolist())
                selected_dept = st.selectbox("🩺 진료과 선택", depts)
                
                if selected_dept == "전체":
                    doctors = ["전체"] + sorted(df_main['의사명'].unique().tolist())
                else:
                    doctors = ["전체"] + sorted(df_main[df_main['진료과'] == selected_dept]['의사명'].unique().tolist())
                selected_doctor = st.selectbox("👨‍⚕️ 의사명 선택", doctors)
            else:
                # 일반 사용자: 본인 소속 진료과 기준
                selected_dept = user["진료과"]
                st.info(f"🩺 소속 진료과: {selected_dept}")
                
                dept_doctors = df_main[df_main['진료과'] == selected_dept]['의사명'].unique().tolist()
                user_name = user["의사"]
                doctors = ["전체", user_name] + [d for d in dept_doctors if d != user_name]
                selected_doctor = st.selectbox("👨‍⚕️ 의사명 선택", list(dict.fromkeys(doctors)))
        else:
            st.error("시트 컬럼명 형식을 확인해주세요.")
            st.stop()
    else:
        st.warning("데이터가 없습니다.")
        st.stop()
        
    st.markdown("---")
    if st.button("로그아웃", use_container_width=True):
        st.session_state["authenticated"] = False
        st.rerun()

# 5. 오른쪽 메인 영역 (세련된 KPI 카드 및 테이블)
st.markdown(f"<p class='main-title'>📊 Hismedi 진료현황 리포트</p>", unsafe_allow_html=True)
st.markdown(f"<p class='sub-title'>조회일자: <b>{selected_date}</b> | 소속/진료과: <b>{selected_dept}</b></p>", unsafe_allow_html=True)

# 필터링 적용
mask = (df_main['일자'] == selected_date)
if user["권한"] != "master" and selected_dept != "전체":
    mask &= (df_main['진료과'] == selected_dept)
elif selected_dept != "전체":
    mask &= (df_main['진료과'] == selected_dept)

if selected_doctor != "전체":
    mask &= (df_main['의사명'] == selected_doctor)

view_df = df_main[mask].copy()

# 숫자형 데이터 정제
for col in view_df.columns:
    if col not in ['일자', '진료과', '의사명']:
        view_df[col] = pd.to_numeric(view_df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

# 주요 지표 (KPI) 카드
col1, col2, col3, col4 = st.columns(4)
t_out = view_df['외래'].sum() if '외래' in view_df.columns else 0
t_in = view_df['입원'].sum() if '입원' in view_df.columns else 0
t_rev_ex = view_df['검진제외'].sum() if '검진제외' in view_df.columns else 0
t_rev_inc = view_df['검진포함'].sum() if '검진포함' in view_df.columns else 0

with col1:
    st.metric(label="외래 환자수", value=f"{int(t_out):,} 명")
with col2:
    st.metric(label="입원 환자수", value=f"{int(t_in):,} 명")
with col3:
    st.metric(label="진료수입 (검진제외)", value=f"{int(t_rev_ex):,} 원")
with col4:
    st.metric(label="진료수입 (검진포함)", value=f"{int(t_rev_inc):,} 원")

st.markdown("### 📋 상세 실적 데이터")
if not view_df.empty:
    st.dataframe(view_df, use_container_width=True, hide_index=True)
else:
    st.info("선택하신 조건에 해당하는 실적 데이터가 없습니다.")
