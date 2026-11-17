"""HISMEDI: HIRA SSV change history connectivity test.
Does not modify Sheets/Drive. Direct portal requests are unverified and may require
an initialized browser session; official OpenAPI remains a read-only fallback.
"""
from __future__ import annotations

import csv
import io
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import unquote

import pandas as pd
import requests
import streamlit as st

PORTAL_URL = 'https://biz.hira.or.kr/qyc/infmrm/tretMatCat.ndo'
API_URL = 'https://apis.data.go.kr/B551182/mcatInfoService1.2/getPaymentNonPaymentList1.2'
RS = '\x1e'
US = '\x1f'
ETX = '\x03'

FIELDS = {
    'rn': '순번', 'mcatCd': '치료재료코드', 'impEntpNm': '수입(판매)업소',
    'adtStaDd': '적용시작일', 'adtEndDd': '적용종료일', 'itmNm': '품목명',
    'nomNm': '규격', 'unit': '단위(원본)', 'mnfEntpNm': '제조회사',
    'matNm': '재질', 'tmcatPayTpCd': '급여구분',
    'sellEptUnprc': '업체판매단가', 'mxUnprc': '상한금액',
    'crspMcatCd': '대응치료재료코드',
    'slfBrdnRtCzaApvYn': '본인부담률 A 인정여부',
    'slfBrdnRtCzbApvYn': '본인부담률 B 인정여부',
    'slfBrdnRtCzeApvYn': '본인부담률 E 인정여부',
    'essSelPayDpApvYn': '중복인정여부',
    'totCnt': '전체건수', 'currentPage': '현재페이지',
    'recordCountPerPage': '페이지당건수',
    # Official OpenAPI additional fields
    'payTpNm': '급여구분', 'mdivCd': '중분류코드',
    'mdivCdNm': '중분류명', 'ldivCd': '대분류코드',
    'ldivCdNm': '대분류명', 'ldgrpCd': '대분류군코드',
    'ldgrpCdNm': '대분류군명',
}
DISPLAY_ORDER = [
    '치료재료코드', '품목명', '규격', '단위', '급여구분', '상한금액',
    '적용시작일', '적용종료일', '제조회사', '수입(판매)업소', '재질',
    '업체판매단가', '대응치료재료코드', '본인부담률 A 인정여부',
    '본인부담률 B 인정여부', '본인부담률 E 인정여부', '중복인정여부',
]


def clean_value(v):
    return (v or '').replace(ETX, '').strip()


def parse_ssv(body: bytes | str) -> tuple[list[dict], dict]:
    """Decode Nexacro SSV datasets; no credentials are interpreted or logged."""
    raw = body.decode('utf-8-sig', 'replace') if isinstance(body, bytes) else body
    raw = raw.replace('\r\n', RS).replace('\n', RS) if RS not in raw else raw
    if not raw.lstrip().lower().startswith('ssv:'):
        raise ValueError('Nexacro SSV가 아닌 응답입니다. 인증/접근 제한 또는 다른 오류 응답일 수 있습니다.')
    rows: list[dict] = []
    meta: dict = {'datasets': []}
    current = None
    names = []
    for token in raw.split(RS):
        token = token.strip('\r\n')
        if not token:
            continue
        if token.startswith('ErrorCode='):
            meta['error_code'] = clean_value(token.split('=', 1)[1])
            continue
        if token.startswith('ErrorMsg='):
            meta['error_message_present'] = True  # avoid echoing raw server errors
            continue
        if token.startswith('Dataset:'):
            current = token.split(':', 1)[1].strip()
            meta['datasets'].append(current)
            names = []
            continue
        if token.startswith('_RowType_'):
            cells = token.split(US)[1:]
            names = [c.split(':', 1)[0] for c in cells]
            continue
        if current != 'dsMain' or not names:
            continue
        cells = token.split(US)
        if cells and cells[0] in ('N', 'I', 'U', 'D', ''):
            cells = cells[1:]
        if not cells:
            continue
        entry = {k: clean_value(cells[i]) if i < len(cells) else '' for i, k in enumerate(names)}
        if entry.get('mcatCd'):
            rows.append(entry)
    return rows, meta


def make_ssv_request(code: str, per_page: int = 100, page: int = 1) -> bytes:
    """Best-effort unauthenticated SSV payload modeled on observed dsParam.
    This does not copy Chrome cookies, session identifiers, or security tokens.
    """
    names = [
        'mcatCd', 'vendNm', 'mnfEntpNm', 'impEntpNm', 'tmcatPayTpCd',
        'currentPage', 'recordCountPerPage', 'firstIndex', 'totCnt', 'recCnt', 'addLastIndex',
    ]
    vals = [code, '', '', '', '', str(page), str(per_page), str((page-1)*per_page), '0', '0', '0']
    # Nexacro SSV column schema; row values are delimited by unit separators.
    columns = '_RowType_' + ''.join(US + col + ':STRING(256)' for col in names)
    row = US.join(['N', *vals])
    payload = RS.join(['SSV:utf-8', 'Dataset:dsParam', columns, row, '', ''])
    return payload.encode('utf-8')


def fmt_date(value):
    s = str(value or '').strip()
    if re.fullmatch(r'\d{8}', s):
        return f'{s[:4]}-{s[4:6]}-{s[6:]}'
    return s


def normalize_unit(raw):
    s = str(raw or '').strip().upper()
    return re.sub(r'^1\s*(EA|SET|KIT|BOX|PACK|UNIT|ROLL)$', r'\1', s)


def view_frame(items):
    frame = pd.DataFrame(items).rename(columns=FIELDS)
    if frame.empty:
        return frame
    frame['단위'] = frame.get('단위(원본)', pd.Series([''] * len(frame))).map(normalize_unit)
    for col in ('적용시작일', '적용종료일'):
        if col in frame:
            frame[col] = frame[col].map(fmt_date)
    for col in ('상한금액', '업체판매단가'):
        if col in frame:
            frame[col] = frame[col].map(lambda x: f'{int(x):,}' if str(x).isdigit() else x)
    order = [x for x in DISPLAY_ORDER if x in frame.columns]
    return frame[order + [x for x in frame.columns if x not in order]]


def portal_request(code: str):
    headers = {
        'Content-Type': 'application/octet-stream; charset=utf-8',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://biz.hira.or.kr',
        'Referer': 'https://biz.hira.or.kr/index.do',
    }
    result = requests.post(PORTAL_URL, data=make_ssv_request(code), headers=headers, timeout=(8, 22))
    result.raise_for_status()
    items, meta = parse_ssv(result.content)
    if meta.get('error_code') not in (None, '', '0'):
        raise ValueError(f"업무포털 응답 오류 코드 {meta['error_code']}")
    return items, meta


def openapi_request(code: str, key: str):
    if not key:
        raise ValueError('Secrets에 [hira_api] service_key가 없습니다.')
    params = {'ServiceKey': unquote(key), 'mcatCd': code, 'pageNo': 1, 'numOfRows': 20}
    result = requests.get(API_URL, params=params, timeout=(8, 22))
    result.raise_for_status()
    root = ET.fromstring(result.content)
    error = root.findtext('.//resultCode')
    if error and error not in ('00', '0', 'NORMAL_SERVICE', 'INFO-000'):
        raise ValueError(f'공식 API 오류 코드: {error}')
    return [{child.tag.split('}')[-1]: child.text or '' for child in item} for item in root.findall('.//item')]


st.set_page_config(page_title='HISMEDI 심평원 통합조회 시험', layout='wide')
st.markdown('''<style>
.block-container{max-width:1600px;padding-top:1.1rem;padding-bottom:2rem}
[data-testid="stDataFrame"]{font-size:13px}
</style>''', unsafe_allow_html=True)
st.title('심평원 치료재료 통합조회 · 변경이력')
st.caption('업무포털 SSV 직접 조회를 시험합니다. Google Sheets·재고·직원정보는 변경하지 않습니다.')

with st.form('lookup'):
    left, right = st.columns([3, 1], vertical_alignment='bottom')
    with left:
        code = st.text_input('치료재료코드', value='G8207118').strip().upper()
    with right:
        submitted = st.form_submit_button('현재정보 + 변경이력 조회', type='primary', use_container_width=True)

if submitted:
    if not re.fullmatch(r'[A-Z0-9-]{3,32}', code):
        st.error('치료재료코드를 확인하세요. 영문·숫자·하이픈만 입력할 수 있습니다.')
    else:
        try:
            with st.spinner('심평원 업무포털 직접 조회 시험 중...'):
                data, details = portal_request(code)
            st.session_state['hira_direct_results'] = data
            st.session_state['hira_direct_code'] = code
            st.session_state['hira_direct_metadata'] = details
            st.session_state['hira_direct_error'] = ''
        except requests.exceptions.Timeout:
            st.session_state['hira_direct_error'] = '업무포털 응답시간 초과. Streamlit 서버에서 직접 접속할 수 없거나 초기화 과정이 필요할 수 있습니다.'
            st.session_state['hira_direct_results'] = []
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else '알 수 없음'
            st.session_state['hira_direct_error'] = f'업무포털 HTTP {status}. 외부 서버 호출이 제한될 수 있습니다.'
            st.session_state['hira_direct_results'] = []
        except (requests.exceptions.RequestException, ValueError) as exc:
            st.session_state['hira_direct_error'] = f'업무포털 직접 조회 실패: {type(exc).__name__}. 요청 초기화·세션 또는 형식 차이 가능성이 있습니다.'
            st.session_state['hira_direct_results'] = []

if st.session_state.get('hira_direct_error'):
    st.warning(st.session_state['hira_direct_error'])
    st.info('직접 연동 실패 시 공식 OpenAPI로 현재 정보는 조회할 수 있습니다. 이력 자동수집은 아직 검증 전입니다.')

items = st.session_state.get('hira_direct_results', [])
if items:
    df = view_frame(items)
    st.success(f'업무포털 SSV 직접 조회 성공 · 이력 {len(df)}건')
    current = df.iloc[0]
    cols = st.columns(4)
    for col, key in zip(cols, ['치료재료코드', '급여구분', '상한금액', '적용시작일']):
        col.metric(key, str(current.get(key, '')))
    st.subheader('변경이력 전체 — 심평원 응답 항목 모두 표시')
    st.dataframe(df, hide_index=True, use_container_width=True, height=min(620, 90 + len(df)*36))
    st.download_button('조회결과 CSV 다운로드', data=df.to_csv(index=False).encode('utf-8-sig'),
                       file_name=f'HIRA_history_{st.session_state.get("hira_direct_code", "item")}.csv',
                       mime='text/csv')

st.divider()
st.subheader('공식 OpenAPI 현재정보 비교 (보조 테스트)')
st.caption('기존에 발급받은 API 인증키로 현재 정보만 확인합니다. 이력 조회는 포함되지 않습니다.')
try:
    api_key = str(st.secrets['hira_api']['service_key']).strip()
except (KeyError, TypeError):
    api_key = ''
if st.button('공식 OpenAPI로 현재정보 확인', disabled=not bool(api_key)):
    try:
        query_code = code if re.fullmatch(r'[A-Z0-9-]{3,32}', code) else 'G8207118'
        result_items = openapi_request(query_code, api_key)
        if result_items:
            st.dataframe(view_frame(result_items), hide_index=True, use_container_width=True)
        else:
            st.info('검색 결과가 없습니다.')
    except Exception as exc:
        st.error(f'공식 API 조회 실패: {type(exc).__name__}. 서비스 상태·인증키를 확인하세요.')

with st.expander('SSV 응답 붙여넣기 분석 (직접 조회가 실패했을 때)'):
    text = st.text_area('Chrome Network > Response에서 복사한 SSV 텍스트', height=100)
    if st.button('붙여넣은 응답 분석'):
        try:
            entries, meta = parse_ssv(text)
            if entries:
                parsed = view_frame(entries)
                st.success(f'SSV 응답 분석 성공 · {len(parsed)}건')
                st.dataframe(parsed, hide_index=True, use_container_width=True)
            else:
                st.warning(f'응답에서 dsMain 품목 레코드를 찾지 못했습니다. 데이터셋: {meta.get("datasets")}')
        except ValueError as exc:
            st.error(str(exc))
