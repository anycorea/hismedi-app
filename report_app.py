"""실행: streamlit run report_app.py"""
from __future__ import annotations

import io
import html
from datetime import datetime

import pandas as pd
import streamlit as st

from report_core import (ALL, SHEETS, SEOUL, METRICS, GROUPS, available_years,
                         available_months, doctors, parse_source, month_values,
                         cumulative, coverage, make_pdf, format_value)

DEFAULT_SHEET_ID = "1UDyUYa-v-pWsQJbf4x4cpWc0m2FZSWASgIMqt2iHIag"
st.set_page_config(page_title="히즈메디 실적 보고서", page_icon="📋", layout="wide")
st.markdown("""<style>
.stApp{background:#f4f6f8;color:#172d42}
.block-container{padding-top:2rem;max-width:1500px}
h1,h2,h3{color:#172d42;letter-spacing:-.035em}
[data-testid="stMetric"]{background:white;padding:20px;border:1px solid #dce4e9;border-radius:12px}
[data-testid="stMetricLabel"]{font-size:17px}
[data-testid="stMetricValue"]{font-size:28px}
div.stButton>button[kind="primary"]{background:#165c71;border-color:#165c71}
.report-table{width:100%;border-collapse:collapse;background:white;font-size:17px;margin:6px 0 24px}
.report-table th{background:#172d42;color:white;padding:13px 16px;text-align:right;white-space:nowrap}
.report-table th:first-child,.report-table td:first-child{text-align:left}
.report-table td{padding:10px 16px;border-bottom:1px solid #e1e7ec;text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}
.report-table .section td{background:#edf4f6;color:#165c71;font-weight:700;text-align:left;font-size:17px}
.report-table .total td{font-weight:700;background:#f7f9fb}
.report-wrap{overflow-x:auto;border-radius:10px;border:1px solid #dce4e9;margin-bottom:22px}
.eyebrow{font-size:15px;font-weight:700;color:#165c71;letter-spacing:.05em}
</style>""", unsafe_allow_html=True)


def settings():
    try:
        return dict(st.secrets)
    except FileNotFoundError:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def load_online(sheet_id, credential_identity, _account):
    import gspread
    from google.oauth2.service_account import Credentials
    credentials = Credentials.from_service_account_info(
        _account, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    client = gspread.authorize(credentials)
    client.set_timeout(30)
    book = client.open_by_key(sheet_id)
    # 원본 6개 시트를 한 번에 조회합니다. S시트 셀을 변경하지 않습니다.
    response = book.values_batch_get(
        [f"'{name}'!A:Q" for name in SHEETS],
        params={"valueRenderOption": "UNFORMATTED_VALUE"})
    raw = {name: item.get("values", []) for name, item in zip(SHEETS, response["valueRanges"])}
    return parse_source(raw), datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(show_spinner=False)
def load_excel(content):
    import openpyxl
    book = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    try:
        missing = set(SHEETS) - set(book.sheetnames)
        if missing:
            raise ValueError("필수 시트가 없습니다: " + ", ".join(sorted(missing)))
        return parse_source({n: list(book[n].values) for n in SHEETS})
    finally:
        book.close()


def render_table(metrics, columns, datasets):
    bits = ['<div class="report-wrap"><table class="report-table"><thead><tr><th>항목 / 단위</th>']
    bits.extend(f"<th>{html.escape(label)}</th>" for label in columns)
    bits.append("</tr></thead><tbody>")
    previous = None
    for metric in metrics:
        if metric.section != previous:
            bits.append(f'<tr class="section"><td colspan="{len(columns)+1}">{html.escape(metric.section)}</td></tr>')
            previous = metric.section
        style = ' class="total"' if metric.emphasis else ""
        bits.append(f'<tr{style}><td>{html.escape(metric.label)} <small>({metric.unit})</small></td>')
        for values in datasets:
            bits.append(f"<td>{format_value(values[metric.key])}</td>")
        bits.append("</tr>")
    bits.append("</tbody></table></div>")
    st.markdown("".join(bits), unsafe_allow_html=True)


config = settings()
hospital = config.get("hospital_name", "히즈메디병원")
st.markdown(f'<div class="eyebrow">{html.escape(hospital)} · 경영통계</div>', unsafe_allow_html=True)
st.title("실적 보고서")
st.caption("연도와 의사를 선택하고, 필요한 월의 보고서를 출력하세요.")

with st.sidebar:
    st.header("보고서 조회")
    mode_source = st.radio("데이터 연결", ["구글시트", "Excel 파일"], horizontal=True)

data, retrieved = None, None
if mode_source == "구글시트":
    account = config.get("gcp_service_account")
    if not account:
        st.info("Streamlit Secrets에 기존 [gcp_service_account] 설정을 등록해주세요. 또는 왼쪽에서 Excel 파일을 선택해 미리 확인할 수 있습니다.")
        st.stop()
    with st.sidebar:
        if st.button("데이터 새로고침", type="primary", use_container_width=True):
            load_online.clear()
    try:
        with st.spinner("구글시트에서 실적을 불러오는 중입니다..."):
            ident = str(account.get("client_email", "")) + ":" + str(account.get("private_key_id", ""))
            data, retrieved = load_online(config.get("spreadsheet_id", DEFAULT_SHEET_ID), ident, dict(account))
    except Exception as exc:
        # Credentials, tokens and raw API responses are never shown to viewers.
        st.error("구글시트를 읽지 못했습니다. 서비스 계정 뷰어 권한, Google Sheets API 사용 설정, Secrets를 확인한 뒤 새로고침해주세요.")
        st.caption("오류 종류: " + type(exc).__name__)
        if isinstance(exc, ValueError):
            st.error(str(exc))
        st.stop()
else:
    uploaded = st.sidebar.file_uploader("전체 구글시트를 내려받은 Excel", type=["xlsx"])
    if uploaded is None:
        st.info("Na, Da, Ca, Ea, Za, H 시트가 포함된 Excel 파일을 선택해주세요.")
        st.stop()
    try:
        data = load_excel(uploaded.getvalue())
        retrieved = datetime.now(SEOUL).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as exc:
        st.error(str(exc))
        st.stop()

years = available_years(data)
if not years:
    st.warning("조회 가능한 연월 자료가 없습니다.")
    st.stop()
with st.sidebar:
    year = st.selectbox("연도", years, format_func=lambda y: f"{y}년")
    doctor = st.selectbox("의사", [ALL] + doctors(data, year))
    available = available_months(data, year)
    end_month = st.selectbox("기준 월", available, index=len(available)-1, format_func=lambda m: f"{m}월", key=f"end_{year}")
    mode_label = st.radio("보고서 형식", ["선택월 + 누계 · A4 세로", "월별 비교 · A4 가로"])
    mode = "summary" if mode_label.startswith("선택월") else "trend"
    eligible = [m for m in available if m <= end_month]
    if mode == "trend":
        months = st.multiselect("출력할 월", eligible, default=eligible, format_func=lambda m: f"{m}월", key=f"months_{year}_{end_month}")
        months = sorted(months)
    else:
        months = eligible
    st.caption("원본에 자료가 있는 월만 선택할 수 있습니다. 0건인 월도 포함됩니다.")
    st.caption(f"데이터 조회: {retrieved}")
    if mode_source == "구글시트":
        st.caption("조회 결과는 최대 5분간 재사용합니다. 최신 자료는 새로고침으로 즉시 반영합니다.")

if not months:
    st.info("출력할 월을 한 개 이상 선택해주세요.")
    st.stop()
try:
    wanted = sorted(set(months + [end_month]))
    values = {m: month_values(data, year, m, doctor) for m in wanted}
    totals = cumulative(values, months)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

cov = coverage(data, year, months)
missing = {n: [m for m in months if m not in ms] for n, ms in cov.items() if len(ms) < len(months) and (n != "H" or doctor == ALL)}
if missing:
    st.warning("일부 원본의 자료가 없습니다. 해당 값은 ‘—’로 표시하며 누계는 자료가 있는 월만 계산합니다. " +
               " / ".join(f"{n}: {', '.join(str(m) for m in ms)}월" for n, ms in missing.items()))

st.subheader(f"{year}년 {end_month}월 · {doctor}")
card_values = values[end_month] if mode == "summary" else totals
cards = st.columns(3)
for col, key, label, unit in zip(cards, ("revenue", "patients_out", "patients_in"),
                                ("진료수입 · 검진 제외", "외래 환자수", "입원 · 재원 환자수"), ("원", "명", "명")):
    with col:
        st.metric(label, format_value(card_values[key]) + (f" {unit}" if card_values[key] is not None else ""))
st.caption("상단 수치: " + (f"{end_month}월 실적" if mode == "summary" else "선택한 월의 누계"))

try:
    pdf = make_pdf(data, year, doctor, months, end_month, mode, hospital)
    filename = f"실적보고서_{year}_{end_month:02d}_{doctor}_{'월별' if mode == 'trend' else '월간'}.pdf"
    st.download_button("A4 보고서 PDF 다운로드", pdf, filename, "application/pdf", type="primary", use_container_width=True)
    st.caption("PDF를 열고 Ctrl+P로 인쇄하세요. 용지는 A4, 배율은 실제 크기(100%)를 권장합니다.")
except Exception as exc:
    st.error("PDF를 만들지 못했습니다. fonts 폴더의 글꼴 파일이 함께 배포되었는지 확인해주세요.")
    st.caption(type(exc).__name__ + ": " + str(exc))

if mode == "summary":
    columns = [f"{end_month}월", f"1~{end_month}월 누계"]
    datasets = [values[end_month], totals]
else:
    columns = [f"{m}월" for m in months] + ["선택월 누계"]
    datasets = [values[m] for m in months] + [totals]
for title, sections in GROUPS:
    st.markdown(f"### {title}")
    render_table([m for m in METRICS if m.section in sections], columns, datasets)

with st.expander("집계 기준 및 원본별 자료 월"):
    st.write("병원 전체 합계는 Na의 H열, Da·Ca·Ea·Za의 F열에 ‘합계’가 적힌 행을 사용합니다. 개별 의사는 G열 이름이 정확히 일치하는 상세 행만 합산합니다.")
    st.write("소아는 Na F열에 ‘소아청소년과’가 포함된 행, 성인은 그 외 상세 행입니다. Na의 건강검진도 성인에 포함하며, H시트 검진은 별도 집계합니다. H는 개별 의사로 배분하지 않습니다.")
    st.write("일당진료비 누계는 같은 기간의 수입 합계 ÷ 환자수 합계입니다. Na 재원(M열)을 입원 환자수로 사용합니다. PT는 PT+PT2입니다.")
    st.write("자료가 없는 월·해당 없는 항목·분모가 0인 일당진료비는 ‘—’, 자료가 있으나 실적이 없으면 0으로 표시합니다. 검진 포함 수입은 Na와 H가 모두 있는 월에만 표시합니다.")
    st.dataframe(pd.DataFrame([{"원본": n, "자료 있는 월": ", ".join(f"{m}월" for m in ms) or "없음"} for n, ms in cov.items()]), hide_index=True, use_container_width=True)
