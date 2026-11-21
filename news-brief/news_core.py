"""News_Brief shared Google Sheets + RSS routines; no service keys in source."""
import calendar
import hashlib
import html
import json
import os
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from datetime import datetime, timezone
from urllib.parse import urlparse
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
 'Briefing':['브리핑ID','브리핑일시','카테고리코드','브리핑제목','핵심요약','주요이슈','관련뉴스ID','생성모델','생성일시','노출여부'],
 'Keywords':['키워드ID','키워드','카테고리코드','우선순위','사용여부','등록일시','비고'],
 'Collection_Log':['로그ID','실행일시','언론사코드','수집방식','수집건수','신규건수','중복건수','처리결과','오류내용','소요시간초'],
 'Settings':['설정키','설정값','설명','수정일시'],
 'Admin_Log':['로그ID','작업일시','작업자','작업구분','대상시트','대상ID','변경내용'],
}
DEFAULT_CATEGORIES = [
 ('ALL','전체','',0),('POL','정치','',10),('ECO','경제','',20),('SOC','사회','',30),
 ('WORLD','국제','',40),('MED','의료·건강','',50),('SCI','IT·과학','',60),
 ('SPORT','스포츠','',70),('CULT','문화·연예','',80),('LOCAL','지역','',90),('LIFE','생활','',100),
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

def patch_record(book,sheet,key_name,key_value,data):
    ws=book.worksheet(sheet)
    records=rows(book,sheet)
    for index, rec in enumerate(records,2):
        if str(rec.get(key_name,'')) == str(key_value):
            merged={**rec,**data}
            with_retry(lambda: ws.update(range_name=f'A{index}',values=[[str(merged.get(k,'')) for k in HEADERS[sheet]]],value_input_option='RAW'))
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
    # Google News summaries are often a list of other headlines rather than a synopsis.
    # Never misrepresent that list as a verified article summary.
    snippet = '' if 'news.google.com' in str(media.get('RSS_URL','')) else clean_text(summary_html)[:450]
    media_code=str(media.get('언론사코드','')).strip()
    # Google RSS embeds original publisher name after the title; keep original title unmodified.
    publisher=clean_text(entry.get('source',{}).get('title','')) if entry.get('source') else ''
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
            url = safe_url(item['RSS_URL'])
            r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 NewsBrief/1.0 RSS reader'}, timeout=(5, 10))
            r.raise_for_status()
            parsed = feedparser.parse(r.content)
            if not parsed.entries:
                raise ValueError('RSS에서 기사를 찾을 수 없습니다')
            records = [parse_source(e,item,clean_text(item.get('비고')) or 'ALL')
                       for e in parsed.entries[:limit_per_feed]]
            return item, [x for x in records if x], '', time.monotonic()-t
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


def ai_briefing(news, api_key='', model='gemini-2.5-flash'):
    """Build a source-bound Gemini digest from public RSS metadata only.

    No article bodies, private information or Sheets credentials are sent.
    This is deliberately one API request, shared via the Briefing sheet.
    """
    items = [x for x in news if str(x.get('제목', '')).strip()][:35]
    if not items:
        raise ValueError('브리핑을 생성할 뉴스가 없습니다.')
    if not api_key:
        raise ValueError('Google AI Studio에서 Gemini API 키를 발급받아 Streamlit Secrets에 등록하세요.')
    # Never allow user-configured URLs or an arbitrary model endpoint.
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{1,90}', str(model)):
        raise ValueError('Gemini 모델 이름이 올바르지 않습니다.')
    payload = [
        {'id': x['뉴스ID'], 'title': str(x['제목'])[:200],
         'rss_description': str(x.get('요약', ''))[:380],
         'publisher': str(x.get('키워드', ''))[:90],
         'published': str(x.get('발행일시', ''))[:19]}
        for x in items
    ]
    instruction = (
        '당신은 대한민국 뉴스 편집자입니다. 아래 공개 RSS의 기사 제목, 출처, 제공된 설명만 근거로 한국어 뉴스브리핑을 만드세요. '
        '기사 본문을 직접 읽었다고 주장하지 말고 제공되지 않은 사실, 인용, 수치, 원인 또는 전망을 창작하지 마세요. '
        '유사한 사건은 묶고 중요도와 분야 균형을 고려해 핵심 이슈 최대 7개를 선별하세요. '
        '각 이슈의 point는 짧지만 의미 있는 1~2문장으로 작성하세요. 정보가 부족하면 제목에 따르면이라고 명시하세요. '
        '기사 간 연관성을 확신할 수 없으면 묶지 마세요. issues의 ids에는 실제 입력된 id만 사용하세요. '
        '최상위 JSON 객체만 반환하세요. 형식: '
        '{"title":"오늘의 주요 뉴스","summary":"전체 흐름 2~3문장",'
        '"issues":[{"headline":"핵심 제목","point":"근거 있는 설명","ids":["기사ID"]}]}.'
    )
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent'
    body = {
        'contents': [{'role': 'user', 'parts': [{'text': instruction + '\n\n[공개 RSS 데이터]\n' + json.dumps(payload, ensure_ascii=False)}]}],
        'generationConfig': {'responseMimeType': 'application/json', 'temperature': 0.2,
                             'maxOutputTokens': 2400}
    }
    try:
        resp = requests.post(url, headers={'x-goog-api-key': api_key, 'Content-Type': 'application/json'},
                             json=body, timeout=(8, 90))
    except requests.RequestException as exc:
        raise RuntimeError('Gemini API 연결에 실패했습니다. 네트워크 연결을 확인하세요.') from exc
    if resp.status_code == 429:
        raise RuntimeError('Gemini 무료 사용량 또는 호출 빈도 제한에 도달했습니다. 잠시 후 다시 시도하세요.')
    if resp.status_code in (401, 403):
        raise RuntimeError('Gemini API 키가 유효하지 않거나 프로젝트에서 해당 모델을 사용할 수 없습니다.')
    if resp.status_code >= 400:
        # Avoid exposing request or credential details from upstream responses.
        raise RuntimeError(f'Gemini API 요청 실패 (HTTP {resp.status_code}). Google AI Studio의 모델/사용량 설정을 확인하세요.')
    try:
        candidates = resp.json().get('candidates', [])
        parts = candidates[0].get('content', {}).get('parts', []) if candidates else []
        answer = ''.join(part.get('text', '') for part in parts)
        parsed = json.loads(answer)
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise RuntimeError('Gemini가 올바른 JSON 브리핑을 반환하지 않았습니다.') from exc
    allowable = {p['id'] for p in payload}
    issues = []
    for issue in parsed.get('issues', [])[:7]:
        if not isinstance(issue, dict):
            continue
        ids = [str(i) for i in issue.get('ids', []) if str(i) in allowable]
        if not ids:
            continue
        headline = clean_text(issue.get('headline', ''))[:110]
        point = clean_text(issue.get('point', ''))[:350]
        if headline:
            issues.append({'headline': headline, 'point': point, 'ids': ids})
    if not issues:
        raise ValueError('유효한 기사 출처가 연결된 이슈를 생성하지 못했습니다.')
    return {'title': clean_text(parsed.get('title', '오늘의 핵심 뉴스'))[:120],
            'summary': clean_text(parsed.get('summary', ''))[:700],
            'issues': issues, 'model': model}

def save_briefing(book,result,cat='ALL'):
    ids=list(dict.fromkeys(id for issue in result['issues'] for id in issue['ids']))
    record=dict(zip(HEADERS['Briefing'],[
        uid(now()+cat),now(),cat,result['title'],result['summary'],json.dumps(result['issues'],ensure_ascii=False),
        ','.join(ids),result['model'],now(),'Y']))
    append_records(book,'Briefing',[record]);return record
