"""히즈메디병원 의료진 안내 — streamlit run medical_app.py"""
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo
import html
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
import tempfile
import requests
import streamlit as st
from bs4 import BeautifulSoup

# 이 목록의 줄 순서를 바꾸면 화면의 진료과 순서가 바뀝니다.
DEPARTMENTS = [
    ('소아청소년과', 5), ('내과', 2), ('외과', 6), ('정형외과', 7),
    ('영상의학과', 14), ('진단검사의학과', 17), ('마취통증의학과', 13),
]
UPDATE_SECONDS = 600  # 홈페이지 재확인 간격 (초)
BASE_URL = 'http://hismedi.kr'
# 통합 안내 화면을 만든 후 주소를 입력하면 상단 버튼이 나타납니다.
HOME_URL = ''
CACHE_DIR = Path(__file__).parent / '.medical_cache'
DAYS = ['월', '화', '수', '목', '금', '토', '일']


def safe_url(path):
    url = urljoin(BASE_URL + '/medical/', path or '')
    p = urlparse(url)
    return url if p.scheme in ('http', 'https') and p.hostname in ('hismedi.kr', 'www.hismedi.kr') else ''


def parse_doctors(markup):
    soup = BeautifulSoup(markup, 'html.parser')
    doctors = []
    for row in soup.select('.doctorTbNew > table > tbody > tr'):
        name = row.select_one('.textBox .name')
        if not name or not name.get_text(strip=True):
            continue
        fields = []
        for group in row.select('.textBox dl.category'):
            title, value = group.find('dt'), group.find('dd')
            if title and value and '진료분야' in title.get_text():
                fields.append(value.get_text(' ', strip=True))
        table = row.select_one('table.doctorInTb')
        schedule = []
        if table:
            headings = [x.get_text(' ', strip=True) for x in table.select('thead th')]
            for tr in table.select('tbody tr'):
                cells = [x.get_text(' ', strip=True) for x in tr.find_all('td', recursive=False)]
                if len(headings) == len(cells):
                    schedule.append(dict(zip(headings, cells)))
        photo = row.select_one('.photo img')
        doctors.append(dict(name=name.get_text(' ', strip=True),
                            field=' / '.join(fields),
                            photo=safe_url(photo.get('src')) if photo else '',
                            schedule=schedule))
    if not doctors:
        raise ValueError('홈페이지에서 의료진 목록을 확인하지 못했습니다.')
    return doctors


def download_photo(url):
    """사진을 서버에서 수신하여 HTTPS 화면에서도 표시 가능한 data URI로 만듭니다."""
    if not url or safe_url(url) != url:
        return ''
    try:
        with requests.get(url, headers={'Referer': BASE_URL + '/'},
                          timeout=(5, 12), stream=True) as response:
            response.raise_for_status()
            parts, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > 8 * 1024 * 1024:
                    return ''
                parts.append(chunk)
        content = b''.join(parts)
        if content.startswith(b'\xff\xd8\xff'):
            mime = 'image/jpeg'
        elif content.startswith(b'\x89PNG\r\n\x1a\n'):
            mime = 'image/png'
        elif content.startswith((b'GIF87a', b'GIF89a')):
            mime = 'image/gif'
        elif content.startswith(b'RIFF') and content[8:12] == b'WEBP':
            mime = 'image/webp'
        else:
            return ''
        return f'data:{mime};base64,' + base64.b64encode(content).decode('ascii')
    except requests.RequestException:
        return ''


def fetch_doctors(sidx):
    response = requests.get(BASE_URL + '/medical/section_view.asp', params={'sidx': sidx}, timeout=(5, 15))
    response.raise_for_status()
    response.encoding = 'utf-8'
    doctors = parse_doctors(response.text)
    with ThreadPoolExecutor(max_workers=4) as pool:
        images = pool.map(download_photo, [doctor['photo'] for doctor in doctors])
        for doctor, image in zip(doctors, images):
            doctor['photo_data'] = image
    return doctors


@st.cache_data(ttl=UPDATE_SECONDS, show_spinner=False)
def load_department(sidx):
    cache_file = CACHE_DIR / f'{sidx}.json'
    try:
        doctors = fetch_doctors(sidx)
        # 사진 서버의 일시 장애 시 이전에 수신한 같은 사진을 유지합니다.
        try:
            previous = json.loads(cache_file.read_text(encoding='utf-8'))
            photos = {d.get('photo'): d.get('photo_data', '') for d in previous.get('doctors', [])}
            for doctor in doctors:
                if not doctor.get('photo_data'):
                    doctor['photo_data'] = photos.get(doctor.get('photo'), '')
        except (OSError, ValueError, TypeError, AttributeError):
            pass
        data = {'doctors': doctors, 'checked_at': datetime.now(ZoneInfo('Asia/Seoul')).strftime('%Y-%m-%d %H:%M')}
        try:
            CACHE_DIR.mkdir(exist_ok=True)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=CACHE_DIR, delete=False) as f:
                json.dump(data, f, ensure_ascii=False)
                tmp = f.name
            os.replace(tmp, cache_file)
        except OSError:
            pass
        return data, False
    except (requests.RequestException, ValueError) as error:
        try:
            data = json.loads(cache_file.read_text(encoding='utf-8'))
            if not data.get('doctors'):
                raise ValueError('empty cache')
            return data, True
        except (OSError, ValueError):
            raise RuntimeError('홈페이지에 연결하지 못했습니다. 잠시 후 다시 시도해 주세요.') from error


def e(value):
    return html.escape(str(value), quote=True)


def doctor_card(doctor):
    source = doctor.get('photo_data', '')
    photo = (f'<img class="portrait" src="{e(source)}" alt="{e(doctor["name"])} 사진">'
             if source.startswith(('data:image/jpeg;base64,', 'data:image/png;base64,',
                                   'data:image/gif;base64,', 'data:image/webp;base64,'))
             else '<div class="portrait" style="display:flex;align-items:center;justify-content:center;color:#587482">사진 준비 중</div>')
    rows = ''
    today = datetime.now(ZoneInfo('Asia/Seoul')).weekday()
    for schedule in doctor['schedule']:
        for i, day in enumerate(DAYS):
            time = schedule.get(day, '미등록')
            state = ' closed' if '휴진' in time else ''
            rows += f'<div class="day{state}{" today" if i == today else ""}"><strong>{day}{" · 오늘" if i == today else ""}</strong><span>{e(time)}</span></div>'
    schedule_html = f'<div class="week">{rows}</div>' if rows else '<p>진료일정은 병원에 문의해 주세요.</p>'
    return f'<article class="doctor"><div class="intro">{photo}<div><h2>{e(doctor["name"])}</h2><p class="label">진료 분야</p><p class="field">{e(doctor["field"] or "병원에 문의해 주세요.")}</p></div></div><h3>주간 진료일정</h3>{schedule_html}</article>'


def main():
    st.set_page_config(page_title='히즈메디병원 · 의료진 안내', page_icon='🏥', layout='wide', initial_sidebar_state='collapsed')
    st.markdown('''<style>
    #MainMenu,header,footer{visibility:hidden} .block-container{padding:1.5rem 2rem 3rem;max-width:1500px}
    .stApp{background:#f3f7fa;color:#182d3c} h1{font-size:2.5rem!important} 
    .stButton button{min-height:76px;font-size:1.3rem!important;border-radius:14px;font-weight:700}
    .stButton button p{font-size:1.25rem!important} .doctor{background:white;border:1px solid #dce6ec;border-radius:22px;padding:28px;margin:22px 0}
    .intro{display:flex;gap:28px;align-items:center}.portrait{width:150px;height:180px;object-fit:contain;background:#f3f6f8;border-radius:14px}
    .doctor h2{font-size:1.9rem;margin:0 0 16px}.label{color:#587482;margin:0;font-size:1rem}.field{font-size:1.3rem;line-height:1.6;margin:6px 0}
    .doctor h3{font-size:1.25rem;margin:24px 0 12px}.week{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:8px}
    .day{border:1px solid #dce6ec;border-radius:12px;overflow:hidden;text-align:center}.day strong{display:block;background:#eef4f7;padding:10px}
    .day span{display:block;padding:16px 5px;font-size:1.08rem;overflow-wrap:anywhere}.closed span{color:#b23c45}.today{border:2px solid #137f94}.today strong{background:#137f94;color:white}
    @media(max-width:700px){.block-container{padding:1rem!important}.intro{gap:16px;align-items:flex-start}.portrait{width:95px;height:125px}.doctor{padding:18px}.doctor h2{font-size:1.45rem}.field{font-size:1.1rem}.week{grid-template-columns:1fr}.day{display:flex;text-align:left}.day strong{width:90px;flex-shrink:0}.day span{padding:10px;font-size:1.1rem}h1{font-size:1.9rem!important}}
    </style>''', unsafe_allow_html=True)
    st.title('히즈메디병원 의료진 안내')
    st.write('진료과를 선택하시면 의료진과 진료일정을 확인하실 수 있습니다.')
    if HOME_URL:
        st.link_button('⌂ 통합 안내 처음으로', HOME_URL)
    if 'department' not in st.session_state:
        st.session_state.department = 5
    for departments in (DEPARTMENTS[:4], DEPARTMENTS[4:]):
        columns = st.columns(len(departments))
        for column, (name, sidx) in zip(columns, departments):
            with column:
                if st.button(name, key=f'dep_{sidx}', use_container_width=True,
                             type='primary' if st.session_state.department == sidx else 'secondary'):
                    st.session_state.department = sidx
                    st.rerun()
    render_department()


@st.fragment(run_every='60s')
def render_department():
    sidx = st.session_state.department
    try:
        with st.spinner('의료진 정보를 확인하고 있습니다…'):
            data, stale = load_department(sidx)
    except RuntimeError as error:
        st.error(str(error))
        if st.button('다시 시도', use_container_width=True):
            load_department.clear(sidx)
            st.rerun()
        return
    if stale:
        st.warning('홈페이지 연결이 원활하지 않아 마지막으로 확인한 정보를 표시합니다.')
    st.info('아래 일정은 홈페이지에 게시된 주간 진료일정입니다. 임시 휴진·공휴일 일정은 병원에 확인해 주세요.')
    for doctor in data['doctors']:
        st.markdown(doctor_card(doctor), unsafe_allow_html=True)


if __name__ == '__main__':
    main()
