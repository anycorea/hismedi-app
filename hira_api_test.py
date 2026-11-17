# -*- coding: utf-8 -*-
"""HISMEDI 심평원 치료재료대정보 PC용 직접조회 테스트.

실행: streamlit run hira_history_streamlit_test.py
구글시트 / 기존 HISMEDI 운영 DB에 기록하지 않습니다.
업무포털 내부 인터페이스는 공식 외부 API가 아니므로 변경될 수 있습니다.
"""
from __future__ import annotations

import re
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from urllib.parse import unquote

import pandas as pd
import streamlit as st

PORTAL_URL = 'https://biz.hira.or.kr/qyc/infmrm/tretMatCat.ndo'
API_URL = 'https://apis.data.go.kr/B551182/mcatInfoService1.2/getPaymentNonPaymentList1.2'
RS, US, STX, ETX = '\x1e', '\x1f', '\x02', '\x03'

FIELD_NAMES = {
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
}
FIELD_ORDER = list(FIELD_NAMES)
MAIN_FIELDS = ['치료재료코드', '품목명', '규격', '단위', '급여구분', '상한금액',
               '제조회사', '수입(판매)업소', '재질', '적용시작일', '적용종료일']
HISTORY_FIELDS = ['적용시작일', '적용종료일', '급여구분', '상한금액', '업체판매단가',
                  '품목명', '규격', '단위', '재질', '제조회사', '수입(판매)업소']


def make_request(code: str, page: int = 1, per_page: int = 20) -> bytes:
    """병원 PC에서 성공한 최소 SSV 요청 그대로 이용(인증정보 없음)."""
    names = ['mcatCd', 'engNm', 'mnfEntpNm', 'impEntpNm', 'tmcatPayTpCd',
             'currentPage', 'recordCountPerPage', 'firstIndex', 'totCnt', 'rvcCnt',
             'addLastIndex']
    values = [code, '', '', '', '', str(page), str(per_page),
              str((page - 1) * per_page), '0', '0', '0']
    return (RS.join(['SSV:utf-8', 'Dataset:dsParam',
                     '_RowType_' + US + US.join(f'{name}:STRING(256)' for name in names),
                     US.join(['N', *values]), '', ''])).encode('utf-8')


def parse_ssv(content: bytes) -> tuple[list[dict[str, str]], dict]:
    raw = content.decode('utf-8-sig', errors='replace')
    if not raw.lstrip().upper().startswith('SSV:'):
        raise ValueError('업무포털에서 SSV가 아닌 응답을 받았습니다.')
    rows: list[dict[str, str]] = []
    meta: dict = {}
    dataset = ''
    fields: list[str] = []
    for token in raw.split(RS):
        token = token.strip('\r\n')
        if not token:
            continue
        if token.startswith('ErrorCode='):
            meta['error_code'] = token.partition('=')[2]
        elif token.startswith('Dataset:'):
            dataset = token.partition(':')[2].strip()
            fields = []
        elif token.startswith('_RowType_'):
            fields = [part.partition(':')[0] for part in token.split(US)[1:]]
        elif dataset == 'dsMain' and fields:
            values = token.split(US)
            if not values or values[0] not in ('N', 'I', 'U'):
                continue
            values = [v.replace(STX, '').replace(ETX, '').strip() for v in values[1:]]
            values.extend([''] * max(0, len(fields) - len(values)))
            item = dict(zip(fields, values))
            if item.get('mcatCd'):
                rows.append(item)
    if rows:
        meta['total'] = _to_int(rows[0].get('totCnt'))
    return rows, meta


def _to_int(value: str | None) -> int | None:
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return None


def format_date(s: str) -> str:
    if re.fullmatch(r'\d{8}', s):
        return f'{s[:4]}-{s[4:6]}-{s[6:]}'
    return s


def normalize_unit(s: str) -> str:
    return re.sub(r'^1\s*(EA|SET|KIT|UNIT|BOX|PACK|ROLL)$', r'\1', str(s).strip().upper())


def view_table(items: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(items)
    if df.empty:
        return df
    df = df.rename(columns=FIELD_NAMES)
    df['단위'] = df.get('단위(원본)', pd.Series([''] * len(df))).map(normalize_unit)
    for col in ('적용시작일', '적용종료일'):
        if col in df.columns:
            df[col] = df[col].map(format_date)
    for col in ('상한금액', '업체판매단가'):
        if col in df.columns:
            df[col] = df[col].map(lambda x: f'{int(x):,}' if str(x).isdigit() else str(x))
    ordered = [FIELD_NAMES[k] for k in FIELD_ORDER if FIELD_NAMES[k] in df.columns]
    ordered.insert(ordered.index('단위(원본)') + 1, '단위')
    return df[[*dict.fromkeys(ordered), *[c for c in df.columns if c not in ordered]]]


@st.cache_data(ttl=180, show_spinner=False)
def direct_lookup(code: str, page: int = 1) -> tuple[list[dict], dict]:
    req = urllib.request.Request(
        PORTAL_URL, data=make_request(code, page), method='POST',
        headers={
            'Content-Type': 'text/plain; charset=UTF-8',
            'Accept': '*/*',
            'User-Agent': 'HISMEDI-SSV-Connection-Test/1.0',
        },
    )
    with urllib.request.urlopen(req, timeout=18) as resp:
        body = resp.read(2_000_000)
    rows, meta = parse_ssv(body)
    if meta.get('error_code') not in (None, '', '0'):
        raise ValueError('업무포털 응답에서 오류코드를 반환했습니다.')
    return rows, meta


def official_lookup(code: str) -> tuple[list[dict], int | None]:
    try:
        key = str(st.secrets['hira_api']['service_key']).strip()
    except (KeyError, TypeError) as exc:
        raise ValueError('공식 OpenAPI 인증키가 설정되지 않았습니다.') from exc
    if not key:
        raise ValueError('공식 OpenAPI 인증키가 비어 있습니다.')
    import requests
    response = requests.get(API_URL, params={
        'ServiceKey': unquote(key), 'mcatCd': code,
        'pageNo': 1, 'numOfRows': 20,
    }, timeout=(8, 25))
    response.raise_for_status()
    root = ET.fromstring(response.content)
    status = root.findtext('.//resultCode')
    if status and status not in ('00', '0', 'NORMAL_SERVICE', 'INFO-000'):
        raise ValueError(f'공식 OpenAPI 응답 코드: {status}')
    items = [{c.tag.split('}')[-1]: c.text or '' for c in item}
             for item in root.findall('.//item')]
    count = _to_int(root.findtext('.//totalCount'))
    return items, count


st.set_page_config(page_title='HISMEDI 치료재료 이력 직접조회', layout='wide')
st.markdown('''<style>
/* Keep a safe top zone below Streamlit's floating toolbar on varying monitors. */
[data-testid="stMainBlockContainer"], .block-container {
    max-width:1500px!important;
    padding-top:4.75rem!important;
    padding-bottom:2.5rem!important;
}
/* Avoid collapsing the first heading into the Streamlit toolbar. */
[data-testid="stMain"] .block-container > div:first-child {margin-top:0!important;}
[data-testid="stTabs"] button p {font-size:1.05rem!important;font-weight:700!important;}
[data-testid="stTabs"] [role="tablist"] {gap:1.25rem!important;flex-wrap:wrap;}
@media (max-height:760px) {
    [data-testid="stMainBlockContainer"], .block-container {padding-top:3.5rem!important;}
}
</style>''', unsafe_allow_html=True)
st.markdown('### 심평원 치료재료대정보 · 현재 정보 및 변경이력')
st.caption('병원 PC 직접 POST 조회 시험 · Google Sheets 및 기존 HISMEDI 업무앱은 변경하지 않습니다.')

with st.form('lookup_form'):
    ca, cb = st.columns([4, 1], vertical_alignment='bottom')
    with ca:
        code = st.text_input('치료재료코드', value='G8207118', max_chars=32)
    with cb:
        submit = st.form_submit_button('현재 정보 + 변경이력 조회', type='primary', use_container_width=True)
if submit:
    code = code.strip().upper()
    if not re.fullmatch(r'[A-Z0-9-]{4,30}', code):
        st.error('코드는 영문·숫자·하이픈 4~30자로 입력하세요.')
    else:
        st.session_state['target_code'] = code
        st.session_state.pop('direct_error', None)
        st.session_state.pop('current_api_result', None)

query_code = st.session_state.get('target_code')
if query_code:
    try:
        with st.spinner('심평원 업무포털 직접조회 중...'):
            rows, meta = direct_lookup(query_code, 1)
        if not rows:
            st.warning('통신은 성공했지만 해당 코드의 이력이 없습니다.')
        else:
            total = meta.get('total') or len(rows)
            all_rows = list(rows)
            # Read additional pages if necessary, with a bounded number of calls.
            if total > len(all_rows):
                st.info(f'이력 {total}건 중 첫 {len(all_rows)}건을 받았습니다. 남은 페이지를 조회합니다.')
                for pg in range(2, min(26, (total + 19) // 20 + 1)):
                    nxt, _ = direct_lookup(query_code, pg)
                    if not nxt:
                        break
                    all_rows.extend(nxt)
            # Remove duplicate history rows returned by overlapping pages.
            unique = {}
            for item in all_rows:
                key = (item.get('mcatCd'), item.get('adtStaDd'), item.get('adtEndDd'),
                       item.get('tmcatPayTpCd'), item.get('mxUnprc'), item.get('itmNm'))
                unique[key] = item
            all_rows = list(unique.values())
            all_rows.sort(key=lambda x: x.get('adtStaDd', ''), reverse=True)
            df = view_table(all_rows)
            st.success(f'업무포털 직접조회 성공 · 변경이력 {len(df)}건' + (f' / 전체 {total}건' if total else ''))
            current = df.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric('치료재료코드', current.get('치료재료코드', ''))
            c2.metric('최신 급여상태', current.get('급여구분', ''))
            c3.metric('최신 상한금액', current.get('상한금액', ''))
            tab1, tab2, tab3 = st.tabs(['현재 적용정보', f'변경이력 ({len(df)}건)', '전체 응답항목'])
            with tab1:
                fields = [f for f in MAIN_FIELDS if f in df.columns]
                st.dataframe(pd.DataFrame({'항목': fields, '조회값': [current[f] for f in fields]}),
                             hide_index=True, use_container_width=True)
            with tab2:
                st.dataframe(df[[f for f in HISTORY_FIELDS if f in df.columns]],
                             hide_index=True, use_container_width=True, height=min(600, 80+len(df)*35))
            with tab3:
                st.dataframe(df, hide_index=True, use_container_width=True, height=min(700, 80+len(df)*35))
            st.download_button('전체 변경이력 CSV 다운로드', df.to_csv(index=False).encode('utf-8-sig'),
                               file_name=f'HIRA_history_{query_code}.csv', mime='text/csv')
            if total and len(df) < total:
                st.warning('서버가 보고한 전체 이력 건수보다 적게 조회되었습니다. 페이지별 응답을 확인하세요.')
    except Exception as exc:
        if isinstance(exc, urllib.error.HTTPError):
            st.warning(f'업무포털 응답 오류 HTTP {exc.code}')
        elif isinstance(exc, (TimeoutError, urllib.error.URLError)):
            st.warning('직접조회 응답시간 초과 또는 네트워크 접근 실패입니다. PC 로컬 실행에서 확인해 주세요.')
        else:
            st.warning(f'업무포털 직접조회 실패: {type(exc).__name__} (요청 형식 또는 응답을 확인하세요).')
        st.info('Streamlit Cloud에서는 앞서 시간 초과가 발생했습니다. PC에서 이 파일을 로컬로 실행하면 PC 네트워크로 직접 조회합니다.')
        with st.expander('공식 OpenAPI로 현재정보만 조회 (선택)'):
            if st.button('공식 OpenAPI 조회'):
                try:
                    items, n = official_lookup(query_code)
                    if items:
                        # API field names are a different schema from SSV.
                        api_names = {'mcatCd':'치료재료코드','itmNm':'품목명','nomNm':'규격','unit':'단위(원본)',
                                     'payTpNm':'급여구분','mxUnprc':'상한금액','mnfEntpNm':'제조회사',
                                     'impEntpNm':'수입(판매)업소','mdivCd':'중분류코드','mdivCdNm':'중분류명',
                                     'ldivCd':'대분류코드','ldivCdNm':'대분류명',
                                     'ldgrpCd':'대분류군코드','ldgrpCdNm':'대분류군명'}
                        api_df = pd.DataFrame(items).rename(columns=api_names)
                        st.dataframe(api_df, hide_index=True, use_container_width=True)
                    else:
                        st.info('공식 OpenAPI에서도 결과가 없습니다.')
                except Exception as api_exc:
                    st.error(f'OpenAPI 조회 실패: {type(api_exc).__name__}. 인증키 및 이용상태를 확인하세요.')
