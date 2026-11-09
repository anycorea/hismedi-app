import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# 1. 페이지 기본 설정
st.set_page_config(
    page_title=st.secrets["app"]["TITLE"],
    layout="wide"
)

st.title(f"🏥 {st.secrets['app']['TITLE']} 대시보드")
st.markdown("구글 시트(`2026_Q4` 및 `RAW_검진`) 데이터를 기반으로 집계된 실적 현황입니다.")

# 2. 구글 시트 데이터 로드 함수
@st.cache_data(ttl=60)
def load_data():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds_dict = dict(st.secrets["gcp_service_account"])
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    
    spreadsheet_id = st.secrets["gsheet"]["spreadsheet_id"]
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 임상 데이터 로드 (분기별 시트: 2026_Q4)
    try:
        clinical_ws = spreadsheet.worksheet("2026_Q4")
        clinical_data = clinical_ws.get_all_records()
        df_clinical = pd.DataFrame(clinical_data)
    except Exception as e:
        df_clinical = pd.DataFrame()

    # 검진 데이터 로드 (RAW_검진)
    try:
        exam_ws = spreadsheet.worksheet("RAW_검진")
        exam_data = exam_ws.get_all_records()
        df_exam = pd.DataFrame(exam_data)
    except Exception as e:
        df_exam = pd.DataFrame()
        
    return df_clinical, df_exam

try:
    df_cli, df_exm = load_data()
except Exception as e:
    st.error(f"구글 시트 연동 중 오류가 발생했습니다: {e}")
    st.stop()

# 3. 데이터 전처리 및 의사명/진료과 정제 함수
def parse_doctor_and_dept(row_text):
    text = str(row_text).strip()
    
    # 진료과 분류 매핑
    if "외과 치료" in text or "일반외과" in text or "외과" in text:
        dept = "외과"
    elif "정형외과" in text:
        dept = "정형외과"
    elif "소아청소년과" in text:
        dept = "소아청소년과"
    elif "내과" in text:
        dept = "내과"
    elif "마취통증의학과" in text:
        dept = "마취통증의학과"
    elif "진단검사의학과" in text:
        dept = "진단검사의학과"
    else:
        dept = "기타"

    # 의사명 추출 (괄호 안의 이름 기준, 없으면 '공통')
    if "(" in text and ")" in text:
        start = text.find("(")
        end = text.find(")")
        doc_raw = text[start+1:end].replace("PD", "").strip()
        doc = doc_raw if doc_raw else "공통"
    else:
        doc = "공통"
        
    return pd.Series([dept, doc])

# 데이터 정제 실행
if not df_cli.empty:
    # 컬럼명 공백 제거 및 정렬 표준화
    df_cli.columns = [str(c).strip() for c in df_cli.columns]
    
    # 날짜 컬럼 확인 (일자함수 또는 일자 활용)
    date_col = "일자함수" if "일자함수" in df_cli.columns else ("일자" if "일자" in df_cli.columns else None)
    
    if date_col and "진료과" in df_cli.columns:
        df_cli['정제_일자'] = pd.to_datetime(df_cli[date_col], errors='coerce').dt.strftime('%Y-%m-%d')
        df_cli[['정제_진료과', '정제_의사명']] = df_cli['진료과'].apply(parse_doctor_and_dept)
        
        # 사이드바 날짜 필터
        valid_dates = sorted(df_cli['정제_일자'].dropna().unique(), reverse=True)
        if valid_dates:
            selected_date = st.sidebar.selectbox("📅 조회 일자 선택", valid_dates)
            
            filtered_df = df_cli[df_cli['정제_일자'] == selected_date]
            
            st.subheader(f"📌 [{selected_date}] 병원 진료 실적 리포트")
            
            tab1, tab2, tab3 = st.tabs(["📊 진료과별 요약", "👨‍⚕️ 의사별 상세 실적", "🩺 건강검진 현황"])
            
            with tab1:
                st.markdown("### 진료과별 실적 현황")
                st.dataframe(filtered_df, use_container_width=True)
                
            with tab2:
                st.markdown("### 의사별 상세 실적 조회")
                selected_dept_filter = st.selectbox("진료과 선택", filtered_df['정제_진료과'].unique())
                dept_filtered_df = filtered_df[filtered_df['정제_진료과'] == selected_dept_filter]
                st.dataframe(dept_filtered_df, use_container_width=True)
                
            with tab3:
                st.markdown("### 건강검진 일자별 현황")
                if not df_exm.empty:
                    df_exm.columns = [str(c).strip() for c in df_exm.columns]
                    # 검진 일자 표준화
                    if "일자" in df_exm.columns:
                        df_exm['정제_일자'] = pd.to_datetime(df_exm['일자'].astype(str), format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
                        filtered_exam = df_exm[df_exm['정제_일자'] == selected_date]
                        st.dataframe(filtered_exam, use_container_width=True)
                    else:
                        st.dataframe(df_exm, use_container_width=True)
                else:
                    st.info("RAW_검진 시트에 데이터가 없습니다.")
        else:
            st.warning("유효한 날짜 데이터가 없습니다. 시트의 일자 컬럼을 확인해주세요.")
    else:
        st.error("시트 내에 '일자' 또는 '진료과' 컬럼을 찾을 수 없습니다.")
else:
    st.info("`2026_Q4` 시트에 입력된 데이터가 없습니다. 병원 통계 데이터를 붙여넣어 주세요.")
