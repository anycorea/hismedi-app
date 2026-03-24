import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime
import urllib.parse

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    .main-header { font-size: 1.8rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f1f3f5; border-radius: 5px; padding: 10px 18px; font-weight: bold; color: #495057;
    }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    .section-title { 
        font-size: 1.05rem; font-weight: bold; color: #1E40AF; 
        margin: 25px 0 15px 0; border-bottom: 2px solid #e9ecef; padding-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. KPIS SOAP API 조회 함수 (정밀 교정) ---
@st.cache_data(ttl=600)
def get_drug_info(edi_code):
    if not edi_code: return {}
    
    # Secret에서 키 가져오기 (루트, API, app 섹션 모두 체크)
    api_key = st.secrets.get("hira_api_key") or st.secrets.get("API", {}).get("hira_api_key") or st.secrets.get("app", {}).get("hira_api_key")
    if not api_key: return {"error": "Secret에 API 키가 설정되지 않았습니다."}

    # KPIS는 Decoding된 키를 사용해야 특수문자(+, / 등) 에러가 나지 않습니다.
    decoded_key = urllib.parse.unquote(api_key)

    url = "http://openapi.kpis.or.kr/services/msupStdCdInfo"
    
    # [교정] KPIS 서버가 요구하는 가장 표준적인 SOAP 1.1 구조
    # 파라미터 태그(serviceKey, stdCd)에 접두사(ser:)를 붙이지 않는 것이 포인트일 수 있습니다.
    soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://service.kpis.or.kr/">
    <soapenv:Header/>
    <soapenv:Body>
        <ser:getMsupStdCdInfo>
            <serviceKey>{decoded_key}</serviceKey>
            <stdCd>{edi_code}</stdCd>
        </ser:getMsupStdCdInfo>
    </soapenv:Body>
</soapenv:Envelope>"""

    headers = {
        "Content-Type": "text/xml;charset=UTF-8",
        "User-Agent": "Mozilla/5.0",
        "SOAPAction": ""
    }

    try:
        response = requests.post(url, data=soap_envelope.encode('utf-8'), headers=headers, timeout=10)
        
        # 500 에러 발생 시 서버가 보낸 XML 내부의 <faultstring> 추출
        if response.status_code == 500:
            try:
                fault_root = ET.fromstring(response.content)
                fault_msg = fault_root.find(".//faultstring")
                detail = fault_msg.text if fault_msg is not None else "원인 불명의 서버 오류"
                return {"error": f"KPIS 서버 응답(500): {detail}"}
            except:
                return {"error": "KPIS 서버 내부 오류(500)가 발생했습니다. 키 또는 형식을 확인하세요."}
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            # KPIS 응답의 return 항목 찾기
            item = root.find(".//return")
            
            if item is not None:
                item_nm = item.findtext("itemNm")
                if not item_nm:
                    return {"error": "조회 결과가 없습니다. (코드 확인 필요)"}

                return {
                    "name": item_nm,
                    "comp": item.findtext("entpNm", ""),
                    "price": item.findtext("maxAmt", "0"),
                    "spec": f"{item.findtext('spec', '')} {item.findtext('unit', '')}".strip(),
                    "date": item.findtext("applcStdt", "")
                }
            else:
                return {"error": "데이터 형식이 올바르지 않습니다."}
        else:
            return {"error": f"HTTP 오류: {response.status_code}"}
            
    except Exception as e:
        return {"error": f"연결 실패: {str(e)}"}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_sheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except: return None

sheet = get_sheet()

# --- 4. 왼쪽 사이드바 (신청자 정보 우선 + 조회기 하단) ---
with st.sidebar:
    st.markdown("## HISMEDI † Drug Service")
    st.divider()
    
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명", placeholder="성명 입력")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통 요청사항)")
    
    st.divider()
    
    st.subheader("🔍 EDI 정보 조회기")
    search_code = st.text_input("제품코드 입력", placeholder="예: 648500030", key="sb_search")
    if search_code:
        with st.spinner('KPIS 데이터 조회 중...'):
            s_res = get_drug_info(search_code)
            if "error" in s_res: 
                st.error(s_res["error"])
            elif s_res:
                st.info(f"**{s_res['name']}**\n\n{s_res['comp']}\n\n{s_res['price']}원 | {s_res['spec']}\n\n적용일: {s_res['date']}")

# --- 5. 공통 함수 및 입력 헬퍼 ---
def handle_submit(row_data):
    if not app_user:
        st.error("왼쪽 메뉴에서 신청자 성명을 입력해주세요."); return
    if sheet:
        row_data[1], row_data[2], row_data[54], row_data[55] = app_date, app_user, app_remark, app_status
        try:
            sheet.append_row(row_data)
            st.success("데이터베이스에 저장되었습니다."); st.balloons()
        except Exception as e:
            st.error(f"저장 오류: {e}")

def render_drug_input(prefix, title, key_id):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    
    m = get_drug_info(edi) if edi else {}
    if "error" in m:
        st.caption(f":red[{m['error']}]")
        m = {}

    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한금액", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일", value=m.get("date", ""), key=f"dt_{key_id}")
    return [edi, name, comp, "", price, "", date, spec]

# --- 6. 메인 화면 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하", "단가인상"]):
    with tabs[i]:
        st.markdown(f'<div class="main-header">{i+1}. {title} 신청</div>', unsafe_allow_html=True)
        row = [""] * 56
        row[0] = title
        
        if i < 2: # ①사용중지, ②신규입고
            res = render_drug_input("신청", f"{title} 약제 정보", f"t{i}")
            row[3], row[4], row[5], row[7], row[10], row[9] = res[0], res[1], res[2], res[4], res[7], res[6]
            if i == 0:
                cc1, cc2 = st.columns(2)
                row[13] = cc1.date_input("사용중지일", key="t0_d").strftime('%Y-%m-%d')
                row[14] = cc2.selectbox("사유", ["생산중단", "자진취하", "대체발생", "기타"], key="t0_s")
        else: # ③대체입고 ~ ⑥단가변경
            res1 = render_drug_input("기존/이전", "기존 약제 정보", f"t{i}_old")
            row[3], row[4], row[5], row[7], row[10], row[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
            
            res2 = render_drug_input("대체/변경", "대체/변경 약제 정보", f"t{i}_new")
            row[36], row[37], row[38], row[40], row[43], row[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
            
            st.markdown('<div class="section-title">추가 정보</div>', unsafe_allow_html=True)
            row[48] = st.text_area("변경 및 입고 요청 사유", key=f"t{i}_area", height=100)
            
        st.divider()
        if st.button(f"🚀 {title} 신청 제출", key=f"btn_{i}", use_container_width=True):
            handle_submit(row)
