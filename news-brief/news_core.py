"""News_Brief shared Google Sheets + RSS routines; no service keys in source."""
import calendar
import hashlib
import html
import json
import os
import re
import time
import random
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
DEFAULT_MEDIA = [
 ('GOOGLE_TOP','종합 뉴스 모음','종합','https://news.google.com/','https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko','ALL'),
 ('GOOGLE_BUS','경제 뉴스 모음','경제','https://news.google.com/','https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=ko&gl=KR&ceid=KR:ko','ECO'),
 ('GOOGLE_TECH','IT·과학 뉴스 모음','IT·과학','https://news.google.com/','https://news.google.com/rss/headlines/section/topic/TECHNOLOGY?hl=ko&gl=KR&ceid=KR:ko','SCI'),
 ('GOOGLE_SPORT','스포츠 뉴스 모음','스포츠','https://news.google.com/','https://news.google.com/rss/headlines/section/topic/SPORTS?hl=ko&gl=KR&ceid=KR:ko','SPORT'),
 ('GOOGLE_HEALTH','의료·건강 뉴스 모음','의료·건강','https://news.google.com/','https://news.google.com/rss/search?q=%EC%9D%98%EB%A3%8C+OR+%EB%B3%91%EC%9B%90+OR+%EA%B1%B4%EA%B0%95&hl=ko&gl=KR&ceid=KR:ko','MED'),
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
    """Insert rows only; never create sheets or format any cells."""
    problems = validate(book)
    if problems: raise ValueError('구글시트의 헤더를 먼저 확인하세요: '+ '; '.join(problems))
    if not rows(book,'Category'):
        append_records(book,'Category',[dict(zip(HEADERS['Category'],[code,name,parent,order,'Y',''])) for code,name,parent,order in DEFAULT_CATEGORIES])
    if not rows(book,'Media'):
        append_records(book,'Media',[dict(zip(HEADERS['Media'],[code,name,group,home,rss,'','RSS',30,order*10,'Y',cat])) for order,(code,name,group,home,rss,cat) in enumerate(DEFAULT_MEDIA,1)])

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
    snippet=clean_text(entry.get('summary',''))
    snippet=re.sub(r'\s+',' ',snippet)[:450]
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

def collect(book, limit_per_feed=35, source_codes=None):
    start=time.monotonic()
    existing={str(x.get('뉴스ID')) for x in rows(book,'News')}
    new=[]; logs=[]; sources=rows(book,'Media')
    for media in sources:
        if not enabled(media.get('사용여부')): continue
        code=str(media.get('언론사코드',''))
        if source_codes is not None and code not in source_codes: continue
        feed=safe_url(media.get('RSS_URL',''))
        if not feed: continue
        t=time.monotonic(); count=0;added=0;dup=0;err=''
        try:
            response=requests.get(feed,headers={'User-Agent':'NewsBrief/1.0 (+RSS reader)'},timeout=15)
            response.raise_for_status()
            parsed=feedparser.parse(response.content)
            if parsed.bozo and not parsed.entries: raise ValueError('RSS 형식을 읽을 수 없습니다.')
            cat=clean_text(media.get('비고','')) or 'ALL'
            for entry in parsed.entries[:limit_per_feed]:
                record=parse_source(entry,media,cat)
                if record is None: continue
                count+=1
                if record['뉴스ID'] in existing: dup+=1; continue
                existing.add(record['뉴스ID']);new.append(record);added+=1
        except Exception as exc: err=str(exc)[:250]
        logs.append(dict(zip(HEADERS['Collection_Log'],[
            uid(now()+code),now(),code,'RSS',count,added,dup,'오류' if err else '성공',err,round(time.monotonic()-t,2)])))
    # Chunk writes to avoid excessively large request payloads.
    for i in range(0,len(new),200):append_records(book,'News',new[i:i+200])
    append_records(book,'Collection_Log',logs)
    return {'sources':len(logs),'new':len(new),'errors':sum(bool(x['오류내용']) for x in logs),'seconds':round(time.monotonic()-start,1)}

def ai_briefing(news, api_key='', model='gpt-4.1-mini'):
    """Summarize only RSS metadata supplied. Never claim article body verified."""
    items=[x for x in news if str(x.get('제목','')).strip()][:15]
    if not items: raise ValueError('브리핑을 생성할 뉴스가 없습니다.')
    if not api_key: raise ValueError('AI API 키가 없습니다. 관리자에게 키 등록을 요청하세요.')
    payload=[{'title':x['제목'],'summary':str(x.get('요약',''))[:450], 'source':str(x.get('키워드','')), 'id':x['뉴스ID']} for x in items]
    instruction=('너는 뉴스 편집자다. 오직 제공된 제목과 RSS 요약만 근거로 한국어 핵심 뉴스 브리핑을 작성한다. '
      '검증하지 못한 숫자, 날짜, 관계, 직접인용과 원문 내용을 절대 추가하지 않는다. '
      '관련 뉴스 3~5개를 선정해 각 이슈를 한 문장으로 명료하게 요약한다. '
      '단정할 수 없는 경우 제목에 따르면/보도에 따르면 표현을 사용한다. '
      '출력은 JSON 객체로만: {"title":"...","summary":"세 문장 이하","issues":[{"headline":"...","point":"...","ids":["뉴스ID"]}]}')
    body={'model':model,'instructions':instruction,'input':json.dumps(payload,ensure_ascii=False),'text':{'format':{'type':'json_object'}},'max_output_tokens':1100}
    r=requests.post('https://api.openai.com/v1/responses',headers={'Authorization':f'Bearer {api_key}','Content-Type':'application/json'},json=body,timeout=80)
    r.raise_for_status()
    result=r.json(); output=''.join(t.get('text','') for item in result.get('output',[]) for t in item.get('content',[]) if t.get('type')=='output_text')
    data=json.loads(output)
    allowable={p['id'] for p in payload}
    issues=[]
    for issue in data.get('issues',[])[:5]:
        ids=[s for s in issue.get('ids',[]) if s in allowable]
        issues.append({'headline':str(issue.get('headline',''))[:110],'point':str(issue.get('point',''))[:350],'ids':ids})
    if not issues:raise ValueError('AI가 유효한 브리핑을 생성하지 못했습니다.')
    return {'title':str(data.get('title','오늘의 핵심 뉴스'))[:120], 'summary':str(data.get('summary',''))[:700], 'issues':issues,'model':model}

def save_briefing(book,result,cat='ALL'):
    ids=list(dict.fromkeys(id for issue in result['issues'] for id in issue['ids']))
    record=dict(zip(HEADERS['Briefing'],[
        uid(now()+cat),now(),cat,result['title'],result['summary'],json.dumps(result['issues'],ensure_ascii=False),
        ','.join(ids),result['model'],now(),'Y']))
    append_records(book,'Briefing',[record]);return record
