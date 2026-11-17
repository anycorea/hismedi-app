"""HISMEDI 심평원 OpenAPI 연결 확인용 독립 Streamlit 페이지."""
import xml.etree.ElementTree as ET
import requests
import pandas as pd
import streamlit as st

st.set_page_config(page_title="HISMEDI 심평원 API 테스트", layout="wide")
st.markdown("### 심평원 치료재료 OpenAPI 연결 테스트")
st.caption("이 테스트는 Google Sheets를 수정하지 않습니다. 인증키는 화면에 표시하거나 로그에 기록하지 않습니다.")

ENDPOINT = "https://apis.data.go.kr/B551182/mcatInfoService1.2/getPaymentNonPaymentList1.2"

try:
    raw_key = str(st.secrets["hira_api"]["service_key"]).strip()
except (KeyError, TypeError):
    st.error("Streamlit Secrets에 [hira_api] service_key가 없습니다.")
    st.stop()
if not raw_key:
    st.error("service_key가 비어 있습니다.")
    st.stop()

st.success("Secrets의 API 인증키 항목을 읽었습니다. (실제 인증 성공 여부는 아래 조회에서 확인)")
a, b = st.columns([1, 2])
with a:
    kind = st.radio("검색 방식", ["치료재료코드", "품목명"], horizontal=True)
with b:
    term = st.text_input("검색어", value="G8207118" if kind == "치료재료코드" else "COROX", key=f"api_term_{kind}")
if st.button("API 연결 및 품목 조회", type="primary"):
    if not term.strip():
        st.warning("검색어를 입력하세요.")
        st.stop()
    key_name = "mcatCd" if kind == "치료재료코드" else "itmNm"
    # 공공데이터포털 일반(Decoding) 인증키를 권장합니다.
    # 인코딩 인증키 입력 시 이중 인코딩 위험이 있으므로 키 종류를 확인하세요.
    params = {"ServiceKey": raw_key, "pageNo": 1, "numOfRows": 5, key_name: term.strip()}
    try:
        with st.spinner("심평원 API 조회 중..."):
            resp = requests.get(ENDPOINT, params=params, timeout=25)
        st.write(f"HTTP 상태: **{resp.status_code}**")
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        # 공공데이터포털 오류는 HTTP 200 XML로 돌아올 수도 있습니다.
        code = root.findtext(".//resultCode") or root.findtext(".//returnReasonCode")
        message = root.findtext(".//resultMsg") or root.findtext(".//returnAuthMsg")
        if root.tag.lower() == "openapi_serviceresponse" or (code and str(code) not in ("00", "0", "NORMAL_SERVICE", "INFO-000")):
            st.error(f"API 응답 오류: {code or '알 수 없음'} / {message or '메시지 없음'}")
            st.stop()
        items = root.findall(".//item")
        total = root.findtext(".//totalCount")
        if items:
            data = [{c.tag: (c.text or "") for c in item} for item in items]
            st.success(f"API 조회 성공 · 전체 결과 {total or '확인 불가'}건 · 이번 응답 {len(data)}건")
            st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
        else:
            st.warning("API 응답은 받았으나 조회 품목이 없습니다. 검색어와 키의 승인 상태를 확인하세요.")
            st.caption("응답 XML 구조: " + root.tag)
    except requests.exceptions.Timeout:
        st.error("API 응답시간 초과(25초). 잠시 후 재시도하세요.")
    except requests.exceptions.HTTPError:
        st.error("HTTP 오류가 발생했습니다. 인증키 및 API 호출 가능 상태를 확인하세요.")
    except (requests.exceptions.RequestException, ET.ParseError):
        st.error("연결 또는 XML 응답 처리 오류입니다. 앱 로그에서 네트워크 상태를 확인하세요.")
