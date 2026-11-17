"""HISMEDI 심평원 치료재료 OpenAPI 조회 및 공식 변경이력 연결 테스트.

실행: streamlit run hira_api_test_full.py
설정: Streamlit Secrets의 [hira_api] service_key
Google Sheets/Drive에 읽기·쓰기 요청을 하지 않습니다.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from urllib.parse import unquote

import pandas as pd
import requests
import streamlit as st

API_URL = "https://apis.data.go.kr/B551182/mcatInfoService1.2/getPaymentNonPaymentList1.2"
HISTORY_URL = (
    "https://biz.hira.or.kr/popup.ndo?"
    "formname=qya_bizcom%3A%3AInfoBank.xfdl&framename=InfoBank"
)
FIELDS = {
    "mcatCd": "치료재료코드",
    "itmNm": "품목명",
    "nomNm": "규격",
    "unit": "단위(원본)",
    "mxUnprc": "상한금액",
    "payTpNm": "급여구분",
    "mnfEntpNm": "제조업체명",
    "impEntpNm": "수입업체명",
    "mdivCd": "중분류코드",
    "mdivCdNm": "중분류명",
    "ldivCd": "대분류코드",
    "ldivCdNm": "대분류명",
    "ldgrpCd": "대분류군코드",
    "ldgrpCdNm": "대분류군명",
}
MAIN_COLUMNS = [
    "치료재료코드", "품목명", "규격", "단위", "급여구분", "상한금액",
    "중분류명", "제조업체명", "수입업체명",
]

st.set_page_config(
    page_title="HISMEDI 심평원 치료재료 조회", page_icon="🏥", layout="wide"
)
st.markdown(
    """<style>
    .block-container {max-width: 1600px; padding-top: 1.3rem; padding-bottom: 2rem;}
    h1 {font-size: 1.65rem !important;}
    div[data-testid="stButton"] button, div[data-testid="stLinkButton"] a {
      min-height: 2.6rem; border-radius: 8px;
    }
    </style>""",
    unsafe_allow_html=True,
)
st.title("심평원 치료재료 조회 · 변경이력 확인")
st.caption("공식 OpenAPI로 현재 목록을 조회하고, 과거 변경이력은 심평원 업무포털에서 확인합니다.")


def norm_unit(text: str) -> str:
    """병원 표시용 단위. API 원문은 별도 열에 보존합니다."""
    value = str(text or "").strip().upper()
    return re.sub(r"^1\s*(EA|SET|KIT|BOX|PACK|UNIT|ROLL)$", r"\1", value)


def parse_response(blob: bytes) -> tuple[list[dict[str, str]], int | None]:
    root = ET.fromstring(blob)
    if root.tag.lower().endswith("openapi_serviceresponse"):
        raise ValueError("공공데이터포털 API 인증 또는 호출 오류가 반환되었습니다.")
    code = (root.findtext(".//resultCode") or "").strip()
    if code and code not in {"00", "0", "NORMAL_SERVICE", "INFO-000"}:
        # Error bodies may contain request URLs including credentials: don't display raw response.
        raise ValueError(f"API 응답 오류 코드: {code}")
    items = [
        {child.tag.split("}")[-1]: (child.text or "").strip() for child in item}
        for item in root.findall(".//item")
    ]
    count_text = root.findtext(".//totalCount")
    total = int(count_text) if count_text and count_text.isdigit() else None
    return items, total


@st.cache_data(ttl=300, show_spinner=False)
def query_hira(search_by: str, keyword: str, page: int, rows: int) -> tuple[list[dict], int | None]:
    # 인증키는 캐시 키/데이터 프레임/화면에 포함하지 않습니다.
    raw_key = str(st.secrets["hira_api"]["service_key"]).strip()
    if not raw_key:
        raise ValueError("API 인증키가 비어 있습니다.")
    # Encoding 키가 실수로 등록된 경우 1회만 디코딩한 후 requests가 인코딩하도록 합니다.
    key = unquote(raw_key) if "%" in raw_key else raw_key
    params = {
        "ServiceKey": key,
        "pageNo": page,
        "numOfRows": rows,
        "mcatCd" if search_by == "치료재료코드" else "itmNm": keyword,
    }
    response = requests.get(API_URL, params=params, timeout=(8, 30))
    response.raise_for_status()
    return parse_response(response.content)


try:
    configured = bool(str(st.secrets["hira_api"]["service_key"]).strip())
except (KeyError, TypeError):
    configured = False
if not configured:
    st.error("Streamlit Secrets에 `[hira_api]`의 `service_key`를 등록하세요.")
    st.stop()

with st.form("hira_search", clear_on_submit=False):
    col1, col2, col3 = st.columns([1.2, 3, 1], vertical_alignment="bottom")
    with col1:
        kind = st.radio("검색 기준", ["치료재료코드", "품목명"], horizontal=True)
    with col2:
        term = st.text_input("검색어", value="G8207118", placeholder="심평원 코드 또는 품목명")
    with col3:
        rows = st.selectbox("조회 건수", [10, 20, 50, 100], index=1)
    submitted = st.form_submit_button("치료재료 검색", type="primary", use_container_width=True)

if submitted:
    if not term.strip():
        st.warning("검색어를 입력해 주세요.")
    else:
        st.session_state["hira_query"] = {"kind": kind, "term": term.strip(), "rows": rows, "page": 1}
        st.session_state.pop("hira_selected", None)

query = st.session_state.get("hira_query")
if query:
    try:
        with st.spinner("공식 심평원 API 조회 중..."):
            items, total = query_hira(query["kind"], query["term"], query["page"], query["rows"])
    except requests.exceptions.Timeout:
        st.error("심평원 API 응답 시간이 초과되었습니다. 잠시 후 다시 시도해 주세요.")
        st.stop()
    except requests.exceptions.HTTPError as ex:
        code = ex.response.status_code if ex.response is not None else "알 수 없음"
        st.error(f"심평원 API HTTP 오류: {code}. 인증·서비스 사용 가능 여부를 확인하세요.")
        st.stop()
    except requests.exceptions.RequestException:
        st.error("심평원 API 네트워크 연결에 실패했습니다.")
        st.stop()
    except (ET.ParseError, ValueError) as ex:
        st.error(f"API 응답 확인 실패: {ex}")
        st.stop()

    st.success(f"조회 성공 · 전체 {total if total is not None else '확인 불가'}건 · 현재 페이지 {len(items)}건")
    if not items:
        st.info("검색 결과가 없습니다. 검색어를 바꾸어 보세요.")
    else:
        df = pd.DataFrame(items).rename(columns=FIELDS)
        df["단위"] = df.get("단위(원본)", pd.Series([""] * len(df))).map(norm_unit)
        if "상한금액" in df.columns:
            df["상한금액"] = df["상한금액"].map(
                lambda x: f"{int(x):,}" if re.fullmatch(r"\d+", str(x)) else str(x)
            )
        for col in MAIN_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        with st.container():
            st.markdown("#### 조회된 품목")
            st.caption("표에서 품목을 선택하면 아래에 공식 변경이력 조회 링크가 표시됩니다.")
            event = st.dataframe(
                df[MAIN_COLUMNS],
                use_container_width=True,
                hide_index=True,
                height=min(520, 50 + len(df) * 35),
                on_select="rerun",
                selection_mode="single-row",
                key=f"hira_result_{query['kind']}_{query['term']}_{query['page']}",
                column_config={
                    "품목명": st.column_config.TextColumn(width="large"),
                    "규격": st.column_config.TextColumn(width="medium"),
                    "상한금액": st.column_config.TextColumn(width="small"),
                },
            )
        selected_indexes = event.selection.rows
        if selected_indexes:
            idx = selected_indexes[0]
            selected = df.iloc[idx].to_dict()
            st.session_state["hira_selected"] = selected
        else:
            selected = st.session_state.get("hira_selected")
            if selected and selected.get("치료재료코드") not in set(df["치료재료코드"]):
                selected = None

        if selected:
            st.divider()
            st.markdown("#### 선택 품목 · 변경이력 조회")
            c1, c2, c3 = st.columns([1.1, 2.5, 1])
            with c1:
                st.text_input("치료재료코드", value=selected.get("치료재료코드", ""), disabled=True)
            with c2:
                st.text_input("품목명", value=selected.get("품목명", ""), disabled=True)
            with c3:
                st.text_input("급여구분", value=selected.get("급여구분", ""), disabled=True)
            a, b = st.columns([1, 2], vertical_alignment="center")
            with a:
                st.link_button("심평원 변경이력 조회 ↗", HISTORY_URL, use_container_width=True)
            with b:
                st.code(selected.get("치료재료코드", ""), language=None)
                st.caption("오른쪽 코드의 복사 아이콘을 누른 후, 열린 심평원 ‘치료재료대정보’에서 코드로 조회하세요.")
            with st.expander("API 원본 제공항목 전체 보기"):
                # Display source columns in Korean; no secret or URL included.
                st.dataframe(pd.DataFrame([selected]).T.rename(columns={0: "조회값"}), use_container_width=True)

        if total is not None and total > query["rows"]:
            max_page = max(1, (total + query["rows"] - 1) // query["rows"])
            left, middle, right = st.columns([1, 2, 1])
            with left:
                if st.button("이전 페이지", disabled=query["page"] <= 1):
                    st.session_state["hira_query"]["page"] -= 1
                    st.session_state.pop("hira_selected", None)
                    st.rerun()
            with middle:
                st.markdown(f"<div style='text-align:center;padding-top:9px'>페이지 {query['page']:,} / {max_page:,}</div>", unsafe_allow_html=True)
            with right:
                if st.button("다음 페이지", disabled=query["page"] >= max_page):
                    st.session_state["hira_query"]["page"] += 1
                    st.session_state.pop("hira_selected", None)
                    st.rerun()
else:
    st.info("치료재료 코드 또는 품목명을 입력한 뒤 **치료재료 검색**을 누르세요.")

st.divider()
st.caption("이 테스트 앱은 심평원 API 조회만 수행합니다. Google Sheets 및 병원 재고·신청 데이터는 변경하지 않습니다.")
st.caption("변경이력 조회 버튼은 심평원 업무포털을 엽니다. 해당 치료재료코드의 자동 입력은 지원하지 않습니다.")
