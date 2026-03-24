import streamlit as st
import gspread
import requests
import xml.etree.ElementTree as ET
from google.oauth2.service_account import Credentials
from datetime import datetime
import urllib.parse
import pandas as pd # CSV 조회를 위해 추가

# --- 1. 페이지 설정 및 디자인 ---
st.set_page_config(page_title="HISMEDI 약무 서비스", layout="wide")

st.markdown("""
    <style>
    [data-testid="stSidebar"] { background-color: #f8f9fa; border-right: 1px solid #dee2e6; }
    .main-header { font-size: 1.8rem; font-weight: 800; color: #1E3A8A; margin-bottom: 20px; }
    .stTabs [data-baseweb="tab"] { background-color: #f1f3f5; border-radius: 5px; font-weight: bold; }
    .stTabs [aria-selected="true"] { background-color: #1E3A8A !important; color: white !important; }
    .section-title { font-size: 1.05rem; font-weight: bold; color: #1E40AF; margin: 25px 0 15px 0; border-bottom: 2px solid #e9ecef; padding-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 통합 조회 함수 (API 우선, 실패 시 CSV) ---
@st.cache_data(ttl=600)
def get_drug_info(edi_code):
    if not edi_code: return {}
    
    # [A] 먼저 KPIS 실시간 API 시도
    api_key = st.secrets.get("hira_api_key") or st.secrets.get("API", {}).get("hira_api_key") or st.secrets.get("app", {}).get("hira_api_key")
    
    if api_key:
        decoded_key = urllib.parse.unquote(api_key)
        # HTTPS로 시도하여 보안 차단 회피 노력
        url = "https://openapi.kpis.or.kr/services/msupStdCdInfo"
        
        soap_envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://ws.skf.sk.biz.kpis.or.kr/">
            <soapenv:Header/><soapenv:Body>
                <ns1:getMsupStdCdInfo>
                    <arg0>{decoded_key}</arg0>
                    <arg1>{edi_code}</arg1>
                </ns1:getMsupStdCdInfo>
            </soapenv:Body>
        </soapenv:Envelope>"""

        headers = {"Content-Type": "text/xml;charset=UTF-8", "SOAPAction": ""}

        try:
            # 타임아웃을 5초로 줄이고 실패 시 즉시 CSV로 전환
            response = requests.post(url, data=soap_envelope.encode('utf-8'), headers=headers, timeout=5)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                item = root.find(".//return")
                if item is not None and item.findtext("itemNm"):
                    return {
                        "name": item.findtext("itemNm"), "comp": item.findtext("entpNm", ""),
                        "price": item.findtext("maxAmt", "0"), "spec": f"{item.findtext('spec', '')} {item.findtext('unit', '')}".strip(),
                        "date": item.findtext("applcStdt", ""), "source": "실시간 API"
                    }
        except Exception:
            pass # API 연결 실패 시 아래 CSV 조회로 넘어감

    # [B] API 실패 시 로컬 drug_master.csv에서 조회 (안전장치)
    try:
        df = pd.read_csv("drug_master.csv", encoding='utf-8-sig')
        df.columns = [c.strip() for c in df.columns]
        df['제품코드'] = df['제품코드'].astype(str).str.strip()
        target = df[df['제품코드'] == str(edi_code).strip()]
        
        if not target.empty:
            res = target.iloc[0]
            col_price = next((c for c in df.columns if '상한금액' in c), None)
            return {
                "name": res.get('제품명', ''), "comp": res.get('업체명', ''),
                "price": str(res[col_price]).replace(',', '').strip() if col_price else "0",
                "spec": f"{res.get('규격', '')} {res.get('단위', '')}".strip(),
                "date": str(res.get('전일', '')), "source": "로컬 CSV"
            }
    except:
        pass
        
    return {"error": "API 연결 불가 및 CSV 파일 정보 없음"}

# --- 3. 구글 시트 연결 ---
@st.cache_resource
def get_sheet():
    try:
        creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], 
                                                     scopes=["https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds).open_by_key(st.secrets["gsheet"]["spreadsheet_id"]).worksheet(st.secrets["gsheet"]["worksheet_name"])
    except: return None

sheet = get_sheet()

# --- 4. 왼쪽 사이드바 ---
with st.sidebar:
    st.markdown("## HISMEDI † Drug Service")
    st.divider()
    st.subheader("📋 신청자 정보")
    app_user = st.text_input("👤 신청자 성명", placeholder="성명 입력")
    app_date = st.date_input("📅 신청일", datetime.now()).strftime('%Y-%m-%d')
    app_status = st.selectbox("⚙️ 진행상황", ["신청완료", "처리중", "처리완료"])
    app_remark = st.text_area("📝 비고 (공통)")
    st.divider()
    st.subheader("🔍 EDI 정보 조회기")
    search_code = st.text_input("제품코드 입력", placeholder="예: 648500030", key="sb_search")
    if search_code:
        s_res = get_drug_info(search_code)
        if "error" in s_res: st.warning(s_res["error"])
        elif s_res:
            st.info(f"**{s_res['name']}** ({s_res['source']})\n\n{s_res['comp']}\n\n{s_res['price']}원 | {s_res['spec']}")

# --- 5. 저장 및 입력 헬퍼 ---
def handle_submit(row_data):
    if not app_user:
        st.error("신청자 성명을 입력해주세요."); return
    if sheet:
        row_data[1], row_data[2], row_data[54], row_data[55] = app_date, app_user, app_remark, app_status
        try:
            sheet.append_row(row_data)
            st.success("데이터베이스에 저장되었습니다."); st.balloons()
        except Exception as e: st.error(f"저장 오류: {e}")

def render_drug_input(prefix, title, key_id):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    edi = c1.text_input(f"{prefix} EDI 코드", key=f"edi_{key_id}")
    m = get_drug_info(edi) if edi else {}
    name = c2.text_input(f"{prefix} 제품명", value=m.get("name", ""), key=f"nm_{key_id}")
    comp = c3.text_input(f"{prefix} 업체명", value=m.get("comp", ""), key=f"cp_{key_id}")
    c4, c5, c6 = st.columns(3)
    price = c4.text_input(f"{prefix} 상한가", value=m.get("price", ""), key=f"pr_{key_id}")
    spec = c5.text_input(f"{prefix} 규격_단위", value=m.get("spec", ""), key=f"sp_{key_id}")
    date = c6.text_input(f"{prefix} 적용일", value=m.get("date", ""), key=f"dt_{key_id}")
    return [edi, name, comp, "", price, "", date, spec]

# --- 6. 탭 구성 ---
tabs = st.tabs(["① 사용중지", "② 신규입고", "③ 대체입고", "④ 급여코드변경", "⑤ 단가인하", "⑥ 단가인상"])

for i, title in enumerate(["사용중지", "신규입고", "대체입고", "급여코드변경", "단가인하", "단가인상"]):
    with tabs[i]:
        st.markdown(f'<div class="main-header">{i+1}. {title} 신청</div>', unsafe_allow_html=True)
        row = [""] * 56
        row[0] = title
        if i < 2:
            res = render_drug_input("신청", f"{title} 약제 정보", f"t{i}")
            row[3], row[4], row[5], row[7], row[10], row[9] = res[0], res[1], res[2], res[4], res[7], res[6]
        else:
            res1 = render_drug_input("기존", "기존 약제 정보", f"t{i}_old")
            row[3], row[4], row[5], row[7], row[10], row[9] = res1[0], res1[1], res1[2], res1[4], res1[7], res1[6]
            res2 = render_drug_input("대체/변경", "대체/변경 정보", f"t{i}_new")
            row[36], row[37], row[38], row[40], row[43], row[42] = res2[0], res2[1], res2[2], res2[4], res2[7], res2[6]
        
        st.divider()
        if st.button(f"🚀 {title} 신청 제출", key=f"btn_{i}", use_container_width=True): handle_submit(row)
