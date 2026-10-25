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
    response = requests.get(f'{BASE_URL}/medical/section_view.asp',
