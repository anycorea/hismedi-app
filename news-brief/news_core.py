"""News_Brief shared Google Sheets + RSS routines; no service keys in source."""
import calendar
import hashlib
import html
import json
import os
import re
import time
import random
from difflib import SequenceMatcher
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from zoneinfo import ZoneInfo

import feedparser
import gspread
import requests
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup

TZ = ZoneInfo('Asia/Seoul')
HEADERS = {
 'Category':['카테고리코드','카테고리명','상위카테고리','정렬순서','사용여부','비고'],
 'Media':['언론사코드','언론사명','언론사분류','홈페이지URL','RSS_URL','로고URL','수집방식','수집주기분','정렬순서','사용여부','비고'],
 'News':['뉴스ID','언론사코드','카테고리코드','제목','원문URL','이미지URL','발행일시','수집일시','요약','키워드','이슈ID','중복여부','노출여부'],
 'Collection_Log':['로그ID','실행일시','언론사코드','수집방식','수집건수','신규건수','중복건수','처리결과','오류내용','소요시간초'],
 'Settings':['설정키','설정값','설명','수정일시'],
}
DEFAULT_CATEGORIES = [
 ('ALL','전체','',0),('POL','정치','',10),('ECO','경제','',20),('SOC','사회','',30),
 ('WORLD','국제','',40),('MED','의료·건강','',50),('SCI','IT·과학','',60),
 ('SPORT','스포츠','',70),('AUTO','자동차','',75),('CULT','문화·연예','',80),('LOCAL','지역','',90),('LIFE','생활','',100),
]
# Initial sources are discoverability feeds, not a representation of all Korean news outlets.
def google_search(query):
    return f'https://news.google.com/rss/search?q={quote(query)}&hl=ko&gl=KR&ceid=KR:ko'


def google_topic(topic):
    return f'https://news.google.com/rss/headlines/section/topic/{topic}?hl=ko&gl=KR&ceid=KR:ko'


# Source entries are RSS aggregations; publishers are extracted from each article.
# This does not imply coverage of every Korean newspaper.
DEFAULT_MEDIA = [
 ('GOOGLE_POL','정치 뉴스 모음','정치','https://news.google.com/',google_search('정치 국회 정부 when:3d'),'POL'),
 ('GOOGLE_BUS','경제 뉴스 모음','경제','https://news.google.com/',google_topic('BUSINESS'),'ECO'),
 ('GOOGLE_SOC','사회 뉴스 모음','사회','https://news.google.com/',google_search('사회 사건 교육 법원 when:3d'),'SOC'),
 ('GOOGLE_WORLD','국제 뉴스 모음','국제','https://news.google.com/',google_topic('WORLD'),'WORLD'),
 ('GOOGLE_HEALTH','의료·건강 뉴스 모음','의료','https://news.google.com/',google_search('의료 병원 질병 건강 when:3d'),'MED'),
 ('GOOGLE_TECH','IT·과학 뉴스 모음','IT','https://news.google.com/',google_topic('TECHNOLOGY'),'SCI'),
 ('GOOGLE_SPORT','스포츠 뉴스 모음','스포츠','https://news.google.com/',google_topic('SPORTS'),'SPORT'),
 ('GOOGLE_AUTO','자동차 뉴스 모음','자동차','https://news.google.com/',google_search('자동차 신차 전기차 완성차 모빌리티 when:3d'),'AUTO'),
 ('GOOGLE_CULT','문화·연예 뉴스 모음','문화','https://news.google.com/',google_topic('ENTERTAINMENT'),'CULT'),
 ('GOOGLE_LOCAL','지역 뉴스 모음','지역','https://news.google.com/',google_search('지역 지방자치 지자체 when:3d'),'LOCAL'),
 ('GOOGLE_LIFE','생활 뉴스 모음','생활','https://news.google.com/',google_search('생활 소비자 날씨 교통 when:3d'),'LIFE'),
 ('GOOGLE_TOP','종합 뉴스 모음','종합','https://news.google.com/','https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko','ALL'),
]


# Curated publisher-targeted Google News searches. A Google News result is not
# guaranteed for every publisher; queries complement the topical feeds.
# Media rows remain editable in the app, and manual spreadsheet headers stay intact.
PRIMARY_MEDIA = [
 ('PUB_JOONGANG','중앙일보','종합','https://www.joongang.co.kr','joongang.co.kr','ALL'),
 ('PUB_CHOSUN','조선일보','종합','https://www.chosun.com','chosun.com','ALL'),
 ('PUB_DONGA','동아일보','종합','https://www.donga.com','donga.com','ALL'),
 ('PUB_HANI','한겨레','종합','https://www.hani.co.kr','hani.co.kr','ALL'),
 ('PUB_KHAN','경향신문','종합','https://www.khan.co.kr','khan.co.kr','ALL'),
 ('PUB_HANKOOK','한국일보','종합','https://www.hankookilbo.com','hankookilbo.com','ALL'),
 ('PUB_YNA','연합뉴스','통신','https://www.yna.co.kr','yna.co.kr','ALL'),
 ('PUB_NEWSIS','뉴시스','통신','https://www.newsis.com','newsis.com','ALL'),
 ('PUB_NEWS1','뉴스1','통신','https://www.news1.kr','news1.kr','ALL'),
 ('PUB_MK','매일경제','경제','https://www.mk.co.kr','mk.co.kr','ECO'),
 ('PUB_HK','한국경제','경제','https://www.hankyung.com','hankyung.com','ECO'),
 ('PUB_SE','서울경제','경제','https://www.sedaily.com','sedaily.com','ECO'),
 ('PUB_MT','머니투데이','경제','https://www.mt.co.kr','mt.co.kr','ECO'),
 ('PUB_EDAILY','이데일리','경제','https://www.edaily.co.kr','edaily.co.kr','ECO'),
 ('PUB_KBS','KBS','방송','https://news.kbs.co.kr','kbs.co.kr','ALL'),
 ('PUB_MBC','MBC','방송','https://imnews.imbc.com','imbc.com','ALL'),
 ('PUB_SBS','SBS','방송','https://news.sbs.co.kr','sbs.co.kr','ALL'),
 ('PUB_YTN','YTN','방송','https://www.ytn.co.kr','ytn.co.kr','ALL'),
 ('PUB_JTBC','JTBC','방송','https://news.jtbc.co.kr','jtbc.co.kr','ALL'),
 ('PUB_ETNEWS','전자신문','IT','https://www.etnews.com','etnews.com','SCI'),
 ('PUB_ZDNET','지디넷코리아','IT','https://zdnet.co.kr','zdnet.co.kr','SCI'),
 ('PUB_MEDTIMES','메디칼타임즈','의료','https://www.medicaltimes.com','medicaltimes.com','MED'),
 ('PUB_DOCNEWS','의협신문','의료','https://www.doctorsnews.co.kr','doctorsnews.co.kr','MED'),
 ('PUB_DAILYMEDI','데일리메디','의료','https://www.dailymedi.com','dailymedi.com','MED'),
 ('PUB_SPORTSSEOUL','스포츠서울','스포츠','https://www.sportsseoul.com','sportsseoul.com','SPORT'),
 ('PUB_SPORTSCHOSUN','스포츠조선','스포츠','https://sports.chosun.com','sports.chosun.com','SPORT'),
 ('PUB_BUSAN','부산일보','지역','https://www.busan.com','busan.com','LOCAL'),
 ('PUB_KUKJE','국제신문','지역','https://www.kookje.co.kr','kookje.co.kr','LOCAL'),
]

# Prefer publisher RSS feeds with direct article URLs. This avoids the Google
# News intermediate link for newly collected stories. The list is deliberately
# small: only sources with externally documented feeds are enabled here.
DIRECT_PUBLISHER_RSS = {
    'PUB_DONGA': 'https://rss.donga.com/total.xml',
    'PUB_YNA': 'https://www.yna.co.kr/rss/news.xml',
    'PUB_JTBC': 'https://fs.jtbc.co.kr/RSS/newsflash.xml',
}



def now(): return datetime.now(TZ).strftime('%Y-%m-%d %H:%M:%S')
def enabled(v): return str(v or '').strip().upper() not in ('N','NO','FALSE','0','OFF','미사용')
def uid(s): return hashlib.sha256(str(s).encode('utf-8')).hexdigest()[:24]
def safe_url(v):
    try:
        u=urlparse(str(v).strip())
        return str(v).strip() if u.scheme in ('https','http') and u.netloc else ''
    except Exception: return ''

def get_config(secret=None):
    if secret is not None:
        sa = dict(secret['gcp_service_account'])
        sheet_id = str(secret.get('gsheet',{}).get('spreadsheet_id') or secret.get('news_brief',{}).get('spreadsheet_id') or '').strip()
    else:
        sa = json.loads(os.environ['GCP_SERVICE_ACCOUNT_JSON'])
        sheet_id = os.environ['NEWS_SPREADSHEET_ID'].strip()
    if not sheet_id: raise ValueError('News_Brief spreadsheet_id가 설정되지 않았습니다.')
    sa['private_key'] = str(sa.get('private_key','')).replace('\\n','\n')
    return sa, sheet_id

def connect(secret=None):
    account, sheet_id = get_config(secret)
    creds=Credentials.from_service_account_info(account,scopes=['https://www.googleapis.com/auth/spreadsheets'])
    return gspread.authorize(creds).open_by_key(sheet_id)

def with_retry(action):
    """Short, bounded retries for transient Sheets API quota/server errors."""
    for attempt in range(4):
        try:
            return action()
        except gspread.exceptions.APIError as exc:
            status = getattr(getattr(exc, 'response', None), 'status_code', None)
            if status not in (429, 500, 502, 503, 504) or attempt == 3:
                raise
            time.sleep(min(1.5 * (2 ** attempt) + random.random(), 8))


def rows(book, sheet):
    """Read data using the manually maintained header; handle header-only sheets.

    `get_all_records` can be sensitive to empty rows/columns or gspread
    version-specific record conversion. Keep everything as original text.
    """
    values = with_retry(lambda: book.worksheet(sheet).get_all_values())
    if not values:
        raise ValueError(f'{sheet}: 1행에 헤더가 없습니다.')
    headers = HEADERS[sheet]
    if values[0] != headers:
        raise ValueError(f'{sheet}: 1행 헤더 또는 열 순서가 등록된 구조와 다릅니다.')
    result = []
    for line in values[1:]:
        if not any(str(cell).strip() for cell in line):
            continue
        padded = (line + [''] * len(headers))[:len(headers)]
        result.append(dict(zip(headers, padded)))
    return result

def validate(book):
    bad=[]
    for sheet, headers in HEADERS.items():
        try:
            actual=with_retry(lambda: book.worksheet(sheet).row_values(1))
            if actual != headers: bad.append(f'{sheet}: 헤더 또는 순서가 다릅니다.')
        except gspread.WorksheetNotFound: bad.append(f'{sheet}: 시트가 없습니다.')
    return bad

def append_records(book, sheet, records):
    if records:
        headers=HEADERS[sheet]
        with_retry(lambda: book.worksheet(sheet).append_rows([[str(record.get(k,'')) for k in headers] for record in records], value_input_option='RAW'))

def patch_record(book, sheet, key_name, key_value, updates):
    """Update the matching physical row without shifting when blank rows exist."""
    ws = book.worksheet(sheet)
    headers = HEADERS[sheet]
    values = with_retry(ws.get_all_values)
    if not values or values[0] != headers:
        raise ValueError(f'{sheet}: 헤더 구조가 다릅니다.')
    if key_name not in headers:
        raise ValueError(f'{sheet}: 키 열이 없습니다: {key_name}')
    key_pos = headers.index(key_name)
    for physical_row, cells in enumerate(values[1:], start=2):
        if len(cells) > key_pos and str(cells[key_pos]) == str(key_value):
            rec = dict(zip(headers, (cells + [''] * len(headers))[:len(headers)]))
            rec.update(updates)
            line = [str(rec.get(k, '')) for k in headers]
            with_retry(lambda: ws.update(range_name=f'A{physical_row}',
                                        values=[line], value_input_option='RAW'))
            return True
    return False

def initialize(book):
    """Seed only missing category/media codes; never modify manual formatting."""
    problems = validate(book)
    if problems:
        raise ValueError('구글시트 헤더를 확인하세요: ' + '; '.join(problems))
    known_categories = {r['카테고리코드'] for r in rows(book, 'Category')}
    cat_records = [dict(zip(HEADERS['Category'], [code,name,parent,order,'Y','']))
                   for code,name,parent,order in DEFAULT_CATEGORIES if code not in known_categories]
    append_records(book, 'Category', cat_records)
    known_media = {r['언론사코드'] for r in rows(book, 'Media')}
    media_records = [dict(zip(HEADERS['Media'], [code,name,group,home,rss,'','RSS',30,order*10,'Y',cat]))
                     for order,(code,name,group,home,rss,cat) in enumerate(DEFAULT_MEDIA,1)
                     if code not in known_media]
    # Register important individual outlets without overwriting existing rows.
    for idx, (code, name, group, home, domain, cat) in enumerate(PRIMARY_MEDIA, 1):
        if code not in known_media:
            media_records.append(dict(zip(HEADERS['Media'], [
                code, name, group, home, google_search(f'site:{domain} when:3d'), '',
                'Google News RSS 검색', 60, (len(DEFAULT_MEDIA) + idx) * 10, 'Y', cat
            ])))
    append_records(book, 'Media', media_records)
    return {'categories_added': len(cat_records), 'sources_added': len(media_records)}


def clean_text(raw):
    txt=BeautifulSoup(str(raw or ''),'html.parser').get_text(' ',strip=True)
    return re.sub(r'\s+',' ',html.unescape(txt)).strip()

def display_time(entry):
    parsed=entry.get('published_parsed') or entry.get('updated_parsed')
    if parsed:
        try: return datetime.fromtimestamp(calendar.timegm(parsed),timezone.utc).astimezone(TZ).strftime('%Y-%m-%d %H:%M:%S')
        except Exception: pass
    return now()

def parse_source(entry, media,cat):
    title=clean_text(entry.get('title',''))
    original=safe_url(entry.get('link',''))
    if not title or not original: return None
    summary_html = str(entry.get('summary',''))
    # Some publisher RSS feeds include article content separately. Prefer it
    # when it is genuinely publisher-authored text (not Google News link lists).
    content_blocks = entry.get('content') or []
    if content_blocks and isinstance(content_blocks, list):
        candidate = max((str(c.get('value','')) for c in content_blocks if isinstance(c,dict)),
                        key=lambda t: len(clean_text(t)), default='')
        if len(clean_text(candidate)) > len(clean_text(summary_html)):
            summary_html = candidate
    # Preserve a substantive publisher excerpt when available, but never a Google
    # News related-headline list. This makes genuine RSS descriptions usable.
    snippet = clean_text(summary_html)[:2400]
    linked_headlines = len(BeautifulSoup(summary_html, 'html.parser').find_all('a'))
    if 'news.google.com' in str(media.get('RSS_URL', '')) and linked_headlines >= 2:
        snippet = ''  # these are unrelated headlines, not article evidence
    media_code=str(media.get('언론사코드','')).strip()
    # Google RSS embeds original publisher name after the title; keep original title unmodified.
    publisher=clean_text(entry.get('source',{}).get('title','')) if entry.get('source') else ''
    # Individual publisher searches can omit the RSS <source> element.
    # In that case the configured publisher remains the correct display name.
    if not publisher and media_code.startswith('PUB_'):
        publisher = str(media.get('언론사명', '')).strip()
    media_code=media_code if not publisher else 'SRC_'+uid(publisher)[:12].upper()
    # Name of the actual publisher is retained in keywords (displayed separately in the app).
    keyword=f'출처:{publisher}' if publisher else ''
    image=''
    if entry.get('media_thumbnail'):
        image=safe_url(entry.media_thumbnail[0].get('url',''))
    return dict(zip(HEADERS['News'],[
        uid(original),media_code,cat,title,original,image,display_time(entry),now(),snippet,keyword,'','N','Y'
    ]))

def media_name_map(book):
    return {r['언론사코드']:r['언론사명'] for r in rows(book,'Media')}

def collect(book, limit_per_feed=22, source_codes=None):
    """Fetch feeds concurrently; write each sheet in batches to limit API calls."""
    start = time.monotonic()
    known = rows(book, 'News')
    existing = {str(x.get('뉴스ID')) for x in known}
    existing_titles = {(re.sub(r'\W+', '', str(x.get('제목',''))).casefold(),
                        str(x.get('발행일시',''))[:10]) for x in known}
    sources = [m for m in rows(book, 'Media') if enabled(m.get('사용여부'))
               and safe_url(m.get('RSS_URL'))
               and (source_codes is None or str(m.get('언론사코드')) in source_codes)]

    # A legacy installation may list GOOGLE_TOP first; preserve topical labels.
    sources.sort(key=lambda m: str(m.get('비고', 'ALL')).upper() == 'ALL')

    def fetch(item):
        t = time.monotonic()
        try:
            code = str(item.get('언론사코드', ''))
            url = DIRECT_PUBLISHER_RSS.get(code) or safe_url(item['RSS_URL'])
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 NewsBrief/1.0 RSS reader'}, timeout=(5, 10))
            r.raise_for_status()
            parsed = feedparser.parse(r.content)
            if not parsed.entries:
                raise ValueError('RSS에서 기사를 찾을 수 없습니다')
            records = [parse_source(e,item,clean_text(item.get('비고')) or 'ALL')
                       for e in parsed.entries[:limit_per_feed]]
            recent = [x for x in records if x and recent_news(x, days=1)]
            return item, recent, '', time.monotonic()-t
        except Exception as exc:
            return item, [], f'{type(exc).__name__}: {exc}'[:250], time.monotonic()-t

    fetched = {}
    if sources:
        with ThreadPoolExecutor(max_workers=min(6, len(sources))) as pool:
            futures = [pool.submit(fetch, m) for m in sources]
            for f in as_completed(futures):
                media, articles, err, seconds = f.result()
                fetched[str(media['언론사코드'])] = (articles, err, seconds)

    new, logs = [], []
    # Topic feeds first, general feed last; category labels remain useful.
    for media in sources:
        code = str(media['언론사코드'])
        articles, err, seconds = fetched.get(code, ([], '수집 결과 없음', 0.0))
        added, duplicates = 0, 0
        for article in articles:
            title_key = (re.sub(r'\W+', '', article['제목']).casefold(), article['발행일시'][:10])
            if article['뉴스ID'] in existing or title_key in existing_titles:
                duplicates += 1
                continue
            existing.add(article['뉴스ID'])
            existing_titles.add(title_key)
            new.append(article)
            added += 1
        logs.append(dict(zip(HEADERS['Collection_Log'], [
            uid(now()+code), now(), code, 'RSS', len(articles), added, duplicates,
            '오류' if err else '성공', err, round(seconds, 2)])))
    for i in range(0, len(new), 150):
        append_records(book, 'News', new[i:i+150])
    append_records(book, 'Collection_Log', logs)
    return {'sources': len(logs), 'new': len(new),
            'errors': sum(bool(x['오류내용']) for x in logs),
            'seconds': round(time.monotonic()-start, 1)}


def _stamp(text):
    """Return an aware Asia/Seoul datetime or None for malformed timestamps."""
    try:
        stamp = datetime.fromisoformat(str(text or '').strip().replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=TZ)
        return stamp.astimezone(TZ)
    except (TypeError, ValueError):
        return None


def recent_news(item, days=7, reference=None):
    """Published time wins; missing/invalid publish time falls back to collected time."""
    date = _stamp(item.get('발행일시')) or _stamp(item.get('수집일시'))
    reference = reference or datetime.now(TZ)
    return date is not None and reference - timedelta(days=days) <= date <= reference + timedelta(hours=1)


def clean_old_rows(book, days=7):
    """Bulk-delete expired rows, preserving manual headers, styles and all Settings.

    Runs only on explicit refresh. BatchUpdate uses reverse contiguous row ranges,
    so indices remain correct when Google executes the requests sequentially.
    """
    reference = datetime.now(TZ)
    threshold = reference - timedelta(days=days)
    spec = {'News': ('발행일시', '수집일시'),
            'Collection_Log': ('실행일시',)}
    result = {}
    requests_to_send = []
    for sheet, date_keys in spec.items():
        ws = book.worksheet(sheet)
        values = with_retry(ws.get_all_values)
        headers = HEADERS[sheet]
        if not values or values[0] != headers:
            raise ValueError(f'{sheet}: 헤더 구조가 달라서 과거 데이터 정리를 중단했습니다.')
        expired = []
        for physical_row, cells in enumerate(values[1:], start=2):
            rec = dict(zip(headers, (cells + [''] * len(headers))[:len(headers)]))
            if not any(rec.values()):
                continue
            date = next((d for key in date_keys if (d := _stamp(rec.get(key))) is not None), None)
            # Rows without a valid date are kept for safety rather than erased.
            if date is not None and date < threshold:
                expired.append(physical_row)
        result[sheet] = len(expired)
        if not expired:
            continue
        groups = []
        first = last = expired[0]
        for n in expired[1:]:
            if n == last + 1:
                last = n
            else:
                groups.append((first, last))
                first = last = n
        groups.append((first, last))
        # Reverse within each sheet; deletes below cannot move rows above.
        for first, last in reversed(groups):
            requests_to_send.append({'deleteDimension': {'range': {
                'sheetId': ws.id, 'dimension': 'ROWS',
                'startIndex': first - 1, 'endIndex': last}}})
    if requests_to_send:
        # Avoid overly large request bodies; all row indexes calculated before
        # deleting and reversed independently within each sheet.
        for index in range(0, len(requests_to_send), 100):
            batch = requests_to_send[index:index + 100]
            with_retry(lambda batch=batch: book.batch_update({'requests': batch}))
    return result




# Resolve headlines from a publisher's own feed before using Google's
# undocumented intermediate-link decoder. This is particularly useful for
# older News rows that were originally collected from Google News.
DIRECT_RSS_BY_PUBLISHER = {
    '연합뉴스': DIRECT_PUBLISHER_RSS['PUB_YNA'],
    '동아일보': DIRECT_PUBLISHER_RSS['PUB_DONGA'],
    'JTBC': DIRECT_PUBLISHER_RSS['PUB_JTBC'],
}


def _headline_normalized(title):
    title = clean_text(title).casefold()
    # Google News appends the publisher after a separator; do not treat it
    # as part of the article title when matching to a direct feed.
    title = re.sub(r'\s+[-–|]\s+(?:연합뉴스|동아일보|jtbc)(?:\s.*)?$', '', title, flags=re.I)
    return re.sub(r'[^0-9a-z가-힣]+', '', title)


@lru_cache(maxsize=40)
def _publisher_feed_items(rss_url, window):
    # `window` is a 15-minute time bucket. It prevents repeated RSS fetches
    # on repeated clicks while allowing refreshed publisher feeds later.
    try:
        response = requests.get(rss_url, timeout=(3, 7),
                                headers={'User-Agent': 'Mozilla/5.0 NewsBrief RSS Reader'})
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
        return [(clean_text(entry.get('title', '')), safe_url(entry.get('link', '')))
                for entry in parsed.entries[:200]]
    except (requests.RequestException, ValueError):
        return []


def _find_publisher_feed_url(article):
    # This match is intentionally conservative: an unrelated story is much
    # worse than saying no article body is available.
    hint = str(article.get('키워드', ''))
    name = hint.removeprefix('출처:').strip() if hint.startswith('출처:') else ''
    if not name:
        code = str(article.get('언론사코드', ''))
        name = {'PUB_YNA': '연합뉴스', 'PUB_DONGA': '동아일보',
                'PUB_JTBC': 'JTBC'}.get(code, '')
    rss_url = DIRECT_RSS_BY_PUBLISHER.get(name)
    wanted = _headline_normalized(article.get('제목', ''))
    if not rss_url or len(wanted) < 12:
        return ''
    best_score, best_url = 0.0, ''
    for headline, link in _publisher_feed_items(rss_url, int(time.time() // 900)):
        other = _headline_normalized(headline)
        if not other or not link:
            continue
        if other == wanted:
            return link
        score = SequenceMatcher(None, wanted, other).ratio()
        if score > best_score:
            best_score, best_url = score, link
    return best_url if best_score >= 0.84 else ''


def _resolve_google_news_url(url):
    """Best-effort publisher URL for current Google News RSS opaque links.

    Google uses an undocumented page signature + Fbv4je RPC. Only attempted
    when someone opens a specific article; failure retains original link.
    """
    parsed = urlparse(url)
    if (parsed.hostname or '').lower() != 'news.google.com':
        return url
    match = re.search(r'/(?:rss/)?(?:articles|read)/([^/?#]+)', parsed.path)
    if not match:
        return url
    article_id = match.group(1)
    if not re.fullmatch(r'[A-Za-z0-9_-]{12,2048}', article_id):
        return url
    try:
        page = requests.get('https://news.google.com/rss/articles/' + article_id,
                            timeout=(3, 7), headers={'User-Agent':'Mozilla/5.0', 'Accept-Language':'ko-KR,ko;q=0.9'})
        if page.status_code != 200:
            return url
        sg = re.search(r'data-n-a-sg="([^"]+)"', page.text)
        ts = re.search(r'data-n-a-ts="(\d+)"', page.text)
        if not sg or not ts:
            return url
        inner = ['garturlreq',
                 [['X','X',['X','X'],None,None,1,1,'KR:ko',None,1,None,None,None,None,None,0,1],
                  'X','X',1,[1,1,1],1,1,None,0,0,None,0],
                 article_id,int(ts.group(1)),sg.group(1)]
        payload = {'f.req': json.dumps([[['Fbv4je',json.dumps(inner),None,'generic']]])}
        result = requests.post('https://news.google.com/_/DotsSplashUi/data/batchexecute',
                               data=payload, timeout=(3,8), headers={'User-Agent':'Mozilla/5.0'})
        if result.status_code != 200:
            return url
        # XSSI prefix / framing is not a stable published API: fail closed.
        for line in result.text.splitlines():
            if 'Fbv4je' not in line:
                continue
            try:
                parsed_rpc = json.loads(line)
                for entry in parsed_rpc:
                    if isinstance(entry,list) and len(entry)>2 and entry[0]=='wrb.fr':
                        decoded = json.loads(entry[2])
                        found = decoded[1] if isinstance(decoded,list) and len(decoded)>1 else ''
                        valid = safe_url(found)
                        if valid and (urlparse(valid).hostname or '').lower() != 'news.google.com':
                            return valid
            except (ValueError, TypeError, IndexError):
                continue
    except (requests.RequestException, TypeError, ValueError):
        pass
    return url


def _title_matches(requested_title, page_title):
    """Reject clearly unrelated pages; allow publisher prefixes and headline edits."""
    def normalize(value):
        value = clean_text(value).casefold()
        value = re.sub(r'\s*[-|–]\s*(연합뉴스|중앙일보|동아일보|mbc|jtbc|네이버 뉴스).*$', '', value, flags=re.I)
        return re.sub(r'[^0-9a-z가-힣]+', '', value)
    a, b = normalize(requested_title), normalize(page_title)
    if not a or not b:
        return False
    if len(a) >= 14 and (a in b or b in a):
        return True
    return SequenceMatcher(None, a, b).ratio() >= 0.53


def _article_context(article):
    """Read a short extract of publicly accessible publisher HTML, never paywalls.

    Does not attempt to bypass subscriptions, Google News redirects or access controls.
    Returns a source label and a bounded extract, not persistent full-text storage.
    """
    raw = str(article.get('요약') or '').strip()
    if raw.startswith('AI_SUMMARY_V4::'):
        return 'cached', raw.split('::', 1)[1]
    if raw.startswith(('AI_SUMMARY_V1::', 'AI_SUMMARY_V2::', 'AI_SUMMARY_V3::')):
        raw = ''  # Older summaries were not validated against publisher headlines.
    url = safe_url(article.get('원문URL', ''))
    if not url:
        return '', ''
    if (urlparse(url).hostname or '').lower() == 'news.google.com':
        direct_url = _find_publisher_feed_url(article)
        url = direct_url or _resolve_google_news_url(url)
    article['_resolved_url'] = url  # transient, not a stored full article body
    if (urlparse(url).hostname or '').lower() == 'news.google.com':
        if len(raw) >= 180:
            return 'rss', raw[:1800]
        raise ValueError('ARTICLE_SOURCE_LINK_UNRESOLVED')
    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()
    # An aggregator URL may redirect to a publisher; verify the final host.
    # Never treat the aggregator's own page as article body.
    # Avoid local/private/loopback IP targets in an untrusted feed.
    import ipaddress
    try:
        addr = ipaddress.ip_address(host)
        if not addr.is_global:
            return '', ''
    except ValueError:
        if not host or host in {'localhost', 'localhost.localdomain'} or host.endswith(('.local', '.internal')):
            return '', ''
    if parsed.port not in (None, 80, 443):
        return '', ''
    try:
        r = requests.get(url, timeout=(4, 9), allow_redirects=True,
                         headers={'User-Agent': 'Mozilla/5.0 (compatible; NewsBriefReader/1.0)'},
                         stream=True)
        r.raise_for_status()
        final_host = (urlparse(r.url).hostname or '').lower()
        if final_host == 'news.google.com' or final_host.endswith('.news.google.com'):
            return 'rss', raw[:1800] if len(raw) >= 220 else ''
        ct = r.headers.get('Content-Type', '').lower()
        if 'html' not in ct:
            return 'rss', raw[:1800] if len(raw) >= 220 else ''
        max_bytes = 700_000
        blob = bytearray()
        for chunk in r.iter_content(chunk_size=32768):
            blob.extend(chunk)
            if len(blob) >= max_bytes:
                break
        soup = BeautifulSoup(bytes(blob[:max_bytes]), 'html.parser')
        # Google News aggregations can land on a different story; never summarize
        # such a page or cache its body under the clicked article ID.
        expected_title = clean_text(article.get('제목', ''))
        meta_title = soup.select_one('meta[property="og:title"]')
        found_title = clean_text(meta_title.get('content', '')) if meta_title else ''
        if not found_title and soup.title:
            found_title = clean_text(soup.title.get_text(' ', strip=True))
        if found_title and not _title_matches(expected_title, found_title):
            raise ValueError('ARTICLE_TITLE_MISMATCH')
        # Reject obvious paywall/blocked responses rather than inventing content.
        head_desc = ''
        meta = soup.select_one('meta[property="og:description"],meta[name="description"]')
        if meta:
            head_desc = clean_text(meta.get('content', ''))
        # A number of publishers expose the editorial text in schema.org
        # NewsArticle JSON-LD even when their CSS class names change.
        jsonld_bodies = []
        for script in soup.select('script[type="application/ld+json"]'):
            if not script.string:
                continue
            try:
                structured_data = json.loads(script.string)
            except (TypeError, ValueError):
                continue
            queue = structured_data if isinstance(structured_data, list) else [structured_data]
            while queue:
                node = queue.pop()
                if isinstance(node, list):
                    queue.extend(node)
                elif isinstance(node, dict):
                    if isinstance(node.get('@graph'), list):
                        queue.extend(node['@graph'])
                    article_body = node.get('articleBody')
                    if isinstance(article_body, str):
                        body = clean_text(article_body)
                        if 280 <= len(body) <= 100_000:
                            jsonld_bodies.append(body[:6500])
        if jsonld_bodies:
            return 'article', max(jsonld_bodies, key=len)
        for tag in soup.select('script,style,nav,footer,header,aside,form,button,figcaption'):
            tag.decompose()
        structured = []
        for selector in ('article p', '[itemprop="articleBody"] p', '.article-body p',
                         '.article_view p', '.news_body p', '#articleBody p',
                         '.article_txt p', '.article-body-content p', '.article_content p',
                         '.news_cnt_detail_wrap p', '#article-view-content-div p',
                         '#newsct_article p', '#articleBodyContents p',
                         '.news-article p', '.article_view_content p',
                         '.article-body-wrap p', '.article-body-text p',
                         '.view_txt p', '#articeBody p', 'main p'):
            parts = [clean_text(p.get_text(' ', strip=True)) for p in soup.select(selector)]
            parts = [x for x in parts if 28 <= len(x) <= 1800]
            if sum(map(len, parts)) >= 300:
                structured = parts
                break
        text = ' '.join(structured)[:6500]
        if len(text) >= 280:
            return 'article', text
        if len(head_desc) >= 120:
            return 'description', head_desc[:2300]
        if len(raw) >= 180:
            return 'rss', raw[:1800]
    except ValueError as exc:
        if str(exc) == 'ARTICLE_TITLE_MISMATCH':
            raise
    except (requests.RequestException, TypeError):
        pass
    return ('rss', raw[:1800]) if len(raw) >= 180 else ('', '')


def _summary_quality(output, source, title):
    """Reject excerpts, truncated output, echoing headlines and hollow summaries.

    These checks limit false positives; they do not prove factual accuracy.
    """
    text = clean_text(output).strip().strip('"“”')
    source = clean_text(source)
    title = clean_text(title)
    if len(text) < 115 or len(text) > 1100:
        return False, 'TOO_SHORT_OR_LONG'
    # Do not show a partial quote that ends mid-sentence.
    if not re.search(r'[.!?。！？][\"”’)]?\s*$', text):
        return False, 'TRUNCATED'
    # Typical free-model responses are sometimes copied directly from the source.
    normalized_text = re.sub(r'\s+', '', text)
    normalized_source = re.sub(r'\s+', '', source)
    if len(normalized_text) >= 65 and normalized_text[:min(110,len(normalized_text))] in normalized_source:
        return False, 'COPIED_EXCERPT'
    if len(normalized_text) >= 85 and normalized_text in normalized_source:
        return False, 'COPIED_EXCERPT'
    # Reject a headline-only rewrite without article detail.
    if len(text) < 175 and SequenceMatcher(None, _headline_normalized(text),
                                           _headline_normalized(title)).ratio() > .63:
        return False, 'HEADLINE_REWRITE'
    sentences = [x.strip() for x in re.split(r'(?<=[.!?。！？])\s+', text) if x.strip()]
    normalized_sentences = [re.sub(r'\W+', '', t) for t in sentences]
    if len(normalized_sentences) > 1 and len(set(normalized_sentences)) < len(normalized_sentences):
        return False, 'REPEATED_SENTENCE'
    if len(sentences) < 2:
        return False, 'NOT_ENOUGH_SENTENCES'
    return True, ''


def summarize_article(article, api_key, model='openrouter/free'):
    """Only summarize adequately sourced article text, not just a headline.

    Fails closed and never stores model output that does not pass quality checks.
    """
    if model != 'openrouter/free':
        raise ValueError('무료 모델만 허용합니다.')
    raw = str(article.get('요약') or '').strip()
    if raw.startswith('AI_SUMMARY_V4::'):
        return raw.split('::', 1)[1]
    title = clean_text(article.get('제목', ''))[:220]
    if not title:
        raise ValueError('기사 제목이 없습니다.')
    source_kind, source_text = _article_context(article)
    if source_kind == 'cached':
        return source_text
    # A short description cannot support the detailed contextual summary the user requested.
    minimum = 430 if source_kind == 'article' else 350
    if len(clean_text(source_text)) < minimum:
        raise ValueError('ARTICLE_CONTENT_INSUFFICIENT')
    prompt = (
        '한국어 뉴스 전문 편집자 역할입니다. 주어진 본문에서 확인할 수 있는 내용만 사용하세요.\n'
        '독자는 제목을 이미 읽었습니다. 제목을 다시 말하지 말고, 본문에서 확인된 구체적 사실을 설명하세요.\n'
        '첫 문장에는 가장 중요한 실제 사실을, 이후에는 배경 및 경과, 수치·일정·발언, 직접적 영향 중 '
        '본문에 나온 사실만 간결하게 작성하세요. 3~5개의 완결된 한국어 문장, 약 200~400자.\n'
        '자료에 없는 사실이나 예상, 일반론은 추가하지 마세요. 문장을 원문에서 복사하지 마세요.\n'
        '짧은 자료라면 있는 사실만 사용하되 실질적 핵심 정보가 없다면 정확히 내용 부족이라고 답하세요.\n'
        '제목·머리말·불릿·마크다운·출처 URL 없이 본문 문장만 반환하세요.\n'
        + json.dumps({'기사제목': title, '자료유형': source_kind,
                      '확인된기사내용': source_text[:5700]}, ensure_ascii=False)
    )
    try:
        response = requests.post('https://openrouter.ai/api/v1/chat/completions',
            headers={'Authorization': 'Bearer ' + api_key.strip(),
                     'Content-Type': 'application/json', 'X-Title': 'News Brief'},
            json={'model': model, 'messages': [{'role': 'user', 'content': prompt}],
                  'temperature': .15, 'max_tokens': 950, 'stream': False},
            timeout=(5, 40))
        if response.status_code in (402, 429):
            raise RuntimeError('무료 AI의 사용 한도에 도달했습니다.')
        response.raise_for_status()
        message = response.json()['choices'][0]['message']['content']
        if isinstance(message, list):
            message = ' '.join(str(part.get('text', '')) for part in message if isinstance(part, dict))
        output = clean_text(message).strip().strip('"“”')
        if output == '내용 부족':
            raise ValueError('ARTICLE_CONTENT_INSUFFICIENT')
        ok, reason = _summary_quality(output, source_text, title)
        if not ok:
            raise ValueError('ARTICLE_SUMMARY_QUALITY_' + reason)
        return output
    except (requests.RequestException, KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError('기사 요약 생성 실패') from exc
