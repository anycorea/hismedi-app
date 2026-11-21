"""News Brief: compact article list with on-demand summaries."""
import html
import json
from datetime import datetime, timedelta
import threading
import time
import re
from difflib import SequenceMatcher
from collections import Counter, defaultdict
from zoneinfo import ZoneInfo

import streamlit as st
from news_core import (HEADERS, append_records, collect, connect, enabled,
                       initialize, now, patch_record, rows, safe_url,
                       clean_old_rows, recent_news, summarize_article)

st.set_page_config(page_title='News Brief | AI 뉴스브리핑', page_icon='📰',
                   layout='wide', initial_sidebar_state='collapsed')

# App-only setting; does not modify other apps in the shared GitHub repository.
st.set_option('client.toolbarMode', 'minimal')

# Compact, stable layout for a standalone direct URL (not iframe embed mode).
# Streamlit's own app header is hidden; the app provides its own navigation.
# No fixed-position elements or viewport/zoom restrictions are introduced.
st.markdown('''<style>
/* Keep a small, stable top clearance. Never position the app title over the
   Streamlit toolbar, including Safari home-screen and iPad split view. */
.stApp {background:#f7f9fc;color:#15253b}
.block-container {max-width:1880px;padding-top:1.25rem!important;padding-bottom:1.5rem}
/* Hide Streamlit's chrome, not the app controls. Host-provided overlays outside
   this document (e.g. Community Cloud widgets) cannot be controlled here. */
header[data-testid="stHeader"], [data-testid="stToolbar"], #MainMenu,
footer, div[data-testid="stDecoration"] {display:none!important}
.stButton button, .stLinkButton a {min-height:42px;border-radius:11px}
.brand {display:flex;align-items:center;gap:9px;font-size:1.25rem;font-weight:840;color:#10263f;letter-spacing:-.035em}
.brand-mark {width:29px;height:29px;display:inline-flex;align-items:center;justify-content:center;background:#0d765a;color:white;border-radius:9px;font-size:1rem}
.brand-small {font-size:.72rem;color:#7c8a99;font-weight:600;letter-spacing:0}
.page-note {color:#7a8899;font-size:.76rem;margin:0 0 7px}
.header-updated{color:#718195;font-size:.77rem;white-space:nowrap;text-align:left}
@media(max-width:900px){.header-updated{font-size:.72rem}}
.pill-status {color:#65778c;font-size:.77rem;white-space:nowrap}
.editor-heading {font-size:1.03rem;font-weight:800;color:#152941;letter-spacing:-.025em;margin:4px 0 7px}
.news-grid {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 12px;align-items:stretch}
.news-tile {background:#fff;border:1px solid #e2e9ef;border-radius:9px;padding:11px 15px;min-width:0;transition:border-color .15s,box-shadow .15s}
.news-tile:hover {border-color:#b8cbd8;box-shadow:0 4px 15px rgba(17,39,63,.055)}
.news-meta {display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#718091;font-size:.72rem;margin-top:4px;margin-bottom:2px}
.news-source {color:#63758a;font-weight:600}
.news-category {color:#116b53;font-weight:750}
.news-title {font-size:1.03rem;line-height:1.4;letter-spacing:-.018em;font-weight:735;overflow-wrap:anywhere;word-break:keep-all}
.news-title a {color:#1b2d42;text-decoration:none}
.news-title a:hover {color:#08785b;text-decoration:underline;text-underline-offset:3px}
.news-desc {color:#647789;font-size:.87rem;line-height:1.6;margin-top:8px;word-break:keep-all}
.related{margin-top:8px;font-size:.78rem;color:#168162}.related summary{cursor:pointer;font-weight:650}.related a{color:#0d765a;text-decoration:none}.related a:hover{text-decoration:underline}
.anycorea {text-align:center;color:#9aa5b0;font-size:.70rem;padding:14px 0}
/* Related reports: tiny understated control, not another section heading. */
[class*="st-key-article_tile_"] [data-testid="stExpander"] {border:none!important;background:transparent!important;margin-top:2px!important}
[class*="st-key-article_tile_"] [data-testid="stExpander"] summary {padding:2px 0!important;color:#77879a!important;font-size:.73rem!important;min-height:0!important}
[class*="st-key-article_tile_"] [data-testid="stExpander"] summary p {font-size:.73rem!important}
/* Headlines lead. A compact metadata column stays right-aligned on desktop. */
[class*="st-key-article_tile_"] {background:#fff;border:1px solid #e1e8ee;border-radius:10px;padding:10px 14px;margin-bottom:8px;box-shadow:0 1px 1px rgba(10,35,52,.018)}
[class*="st-key-article_tile_"] [data-testid="stHorizontalBlock"] {align-items:center;gap:12px}
[class*="st-key-article_tile_"] .stButton, [class*="st-key-article_tile_"] .stButton > div {width:100%}
[class*="st-key-article_tile_"] .stButton button {display:block!important;background:transparent!important;border:0!important;box-shadow:none!important;min-height:0!important;padding:0!important;width:100%!important;justify-content:flex-start!important;text-align:left!important;color:#1b2d42!important;font-size:1.04rem!important;font-weight:760!important;line-height:1.45!important;white-space:normal!important;height:auto!important}
[class*="st-key-article_tile_"] .stButton button p {white-space:normal!important;text-align:left!important;word-break:keep-all!important;font-weight:760!important;font-size:inherit!important;line-height:inherit!important}
[class*="st-key-article_tile_"] .stButton button:hover {color:#08785b!important;text-decoration:underline;text-underline-offset:3px}
.news-right-meta {text-align:right;font-size:.71rem;line-height:1.4;color:#758396;white-space:nowrap}
.news-right-meta .meta-category {color:#607689;font-weight:700}
.article-explainer {font-size:.94rem;line-height:1.8;color:#263b51;background:#f2f7f5;border-left:3px solid #116b53;padding:12px 14px;border-radius:5px;word-break:keep-all;margin:9px 0}
.article-source-note {font-size:.74rem;color:#758499;margin:7px 0}
.article-original a {display:inline-block;font-size:.85rem;font-weight:750;color:#08785b;text-decoration:none;padding:7px 0}
.article-original a:hover{text-decoration:underline}
@media(max-width:700px){
 [class*="st-key-article_tile_"] {background:transparent;border:0;border-bottom:1px solid #e1e6ed;border-radius:0;padding:9px 2px;margin-bottom:2px}
 [class*="st-key-article_tile_"] .stButton button {font-size:1.07rem!important}
 .news-right-meta{text-align:left;font-size:.67rem;margin-top:-4px;white-space:normal}
}
/* Avoid decorative wrappers: the headline and settings are the only toolbar. */
.brand{white-space:nowrap}
.news-grid{gap:8px 10px}

@media(max-width:700px){.brand{font-size:1.09rem}.brand-mark{width:27px;height:27px}}

/* Masthead: logo + updated at left, one menu at right. */
.st-key-news_header [data-testid="stHorizontalBlock"] {
 display:grid!important;grid-template-columns:minmax(0,1fr) minmax(94px,max-content)!important;
 align-items:center;gap:10px;
}
.st-key-news_header [data-testid="stHorizontalBlock"] > div {
 width:100%!important;min-width:0!important;max-width:none!important;flex:none!important;
}
.brand{display:flex;align-items:center;flex-wrap:wrap;gap:5px 10px}
.brand-updated{color:#8290a0;font-size:.71rem;font-weight:500;white-space:nowrap}
@media(max-width:700px){.brand{gap:3px 7px}.brand-updated{font-size:.66rem}}

@media(max-width:850px){.news-grid,.issue-grid{grid-template-columns:1fr}}
@media(max-width:700px){
 .block-container{padding-left:.72rem;padding-right:.72rem;padding-bottom:1.5rem}
 .brand{font-size:1.13rem}
 .news-tile{padding:12px 13px;border-radius:9px}
 .news-title{font-size:1.05rem}
 .summary-hero{padding:16px}
 .brand-small{display:none}
}
</style>''', unsafe_allow_html=True)


def esc(v):
    return html.escape(str(v or ''), quote=True)


def setting(group, key, default=''):
    try:
        return st.secrets.get(group, {}).get(key, default)
    except Exception:
        return default


@st.cache_resource(ttl=1800)
def book():
    return connect(st.secrets)


@st.cache_data(ttl=150, show_spinner=False)
def data(sheet):
    return rows(book(), sheet)



def refresh():
    data.clear()


def handle_error(exc):
    st.error('뉴스 데이터를 읽는 중 문제가 발생했습니다. 잠시 후 다시 접속해 주세요.')
    # Exception text may contain request metadata; keep it out of public UI.
    st.caption(f'오류 종류: {type(exc).__name__} · 자세한 원인은 Streamlit 앱 로그에서 확인')


@st.cache_resource
def news_update_guard():
    # Process-wide, so simultaneous iPhone/iPad/PC visits cannot launch multiple collects.
    return {'lock': threading.Lock(), 'state_lock': threading.Lock(),
            'running': False, 'last_start': 0.0, 'last_end': 0.0,
            'last_report': None, 'last_error': '', 'version': 0, 'source_signature': ''}



def maybe_update_news(last_time):
    """Start a bounded non-blocking update only if data is stale.

    Do not run on every Streamlit rerun. The background worker is intentionally
    isolated from Streamlit context and never touches st.session_state or st.secrets.
    """
    guard = news_update_guard()
    st.session_state.setdefault('seen_collect_version', guard['version'])
    request = bool(st.session_state.pop('news_user_requested_latest', False))
    if not last_time:
        stale = True
    else:
        try:
            stamp = datetime.fromisoformat(last_time.strip().replace('Z', '+00:00'))
            # Sheets stores local timestamps without an offset. Mixing these
            # with timezone-aware datetimes previously raised TypeError and
            # prevented the entire news page from rendering.
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=ZoneInfo('Asia/Seoul'))
            stale = (datetime.now(ZoneInfo('Asia/Seoul')) - stamp).total_seconds() >= 45 * 60
        except (ValueError, TypeError, AttributeError):
            stale = True
    codes = target_source_codes()
    signature = ','.join(sorted(codes))
    if not codes:
        return
    source_changed = guard['source_signature'] != signature
    if not stale and not request and not source_changed:
        return
    # User explicitly tapping latest may refresh sooner, but never more often than 5 min.
    elapsed = time.monotonic() - guard['last_start']
    if guard['last_start'] and elapsed < (300 if request or source_changed else 900):
        return
    if not guard['lock'].acquire(blocking=False):
        return
    # Everything consumed by the background thread is copied before it starts.
    source_codes = codes
    guard['source_signature'] = signature
    config = {'gcp_service_account': dict(st.secrets['gcp_service_account']),
              'gsheet': {'spreadsheet_id': str(st.secrets['gsheet']['spreadsheet_id'])}}
    with guard['state_lock']:
        guard['running'] = True
        guard['last_start'] = time.monotonic()
        guard['last_error'] = ''

    def worker():
        try:
            b = connect(config)
            result = collect(b, limit_per_feed=12, source_codes=source_codes)
            deleted = clean_old_rows(b, days=7)
            with guard['state_lock']:
                guard['last_report'] = {**result, 'deleted': sum(deleted.values())}
                guard['last_error'] = ''
        except Exception as exc:
            with guard['state_lock']:
                guard['last_error'] = f'{type(exc).__name__}: {str(exc)[:220]}'
        finally:
            with guard['state_lock']:
                guard['running'] = False
                guard['last_end'] = time.monotonic()
                guard['version'] += 1
            guard['lock'].release()

    threading.Thread(target=worker, name='NewsBrief-RSS', daemon=True).start()


@st.fragment(run_every='20s')
def update_indicator():
    """Refresh the whole screen only when a background collection finishes."""
    guard = news_update_guard()
    with guard['state_lock']:
        running = guard['running']
        version = guard['version']
        error = guard['last_error']
    if running:
        st.caption('새 뉴스를 확인하는 중입니다. 기존 기사는 계속 이용할 수 있습니다.')
    elif st.session_state.get('seen_collect_version', version) != version:
        st.session_state.seen_collect_version = version
        refresh()
        st.rerun(scope='app')
    elif error and st.session_state.get('seen_error_version') != version:
        st.session_state.seen_error_version = version
        st.caption('최근 뉴스 갱신에 실패했습니다. 저장된 뉴스를 표시합니다.')


@st.cache_data(ttl=1800, show_spinner=False)
def ensure_setup():
    """Seed missing categories/sources only; never fetch RSS on page open."""
    b = book()
    cats = rows(b, 'Category')
    media = rows(b, 'Media')
    if not cats or not media or not any(c.get('카테고리코드') == 'AUTO' for c in cats):
        initialize(b)
        refresh()
    return True


def preference(name, default):
    record = next((r for r in data('Settings') if r.get('설정키') == name), None)
    try:
        v = json.loads(record['설정값']) if record else default
        return v if type(v) is type(default) else default
    except (ValueError, TypeError, KeyError):
        return default


def save_preference(name, value, explanation):
    item = dict(zip(HEADERS['Settings'], [name, json.dumps(value, ensure_ascii=False),
                                         explanation, now()]))
    if not patch_record(book(), 'Settings', '설정키', name, item):
        append_records(book(), 'Settings', [item])
    refresh()


def cluster_titles(articles):
    """Conservatively group reports of the *same event*, not an entire topic.

    Candidate indexing avoids pairwise comparisons against every saved article.
    Publication dates, distinct numbers and a high Korean-headline similarity
    threshold keep separate events from being silently hidden.

    Every report remains in its original Google Sheet row. Grouping changes
    presentation only; expanding a group exposes the other publisher links.
    """
    generic = {
        '오늘', '뉴스', '속보', '단독', '종합', '관련', '기자', '대한', '한국',
        '정부', '최근', '이번', '최신', '보도', '발표', '논란', '확인', '전망',
        '시장', '국내', '올해', '내년', '어제', '다시', '주요', '문제',
    }

    def normalized(title):
        title = str(title or '').casefold()
        for old, new in (('영업이익', '영업익'), ('예상치', '예상'), ('전망치', '예상'), ('내년부터', '내년'), ('올해부터', '올해')):
            title = title.replace(old, new)
        title = re.sub(r'^\s*[\[【][^\]】]{1,16}[\]】]\s*', '', title)
        title = re.sub(r'\s+[-–—]\s+[^-–—]{2,20}$', '', title)
        return re.sub(r'[^가-힣a-z0-9]+', ' ', title).strip()

    def tokens(text):
        return {w for w in text.split() if len(w) >= 2 and w not in generic}

    def grams(text):
        compact = text.replace(' ', '')
        return {compact[i:i+3] for i in range(max(0, len(compact)-2))}

    def info(article):
        headline = normalized(article.get('제목', ''))
        date = str(article.get('발행일시') or article.get('수집일시') or '')
        try:
            day = datetime.fromisoformat(date.replace('Z', '+00:00'))
            day = day.date()
        except (ValueError, TypeError):
            day = None
        return {'article': article, 'text': headline,
                'words': tokens(headline), 'grams': grams(headline),
                'numbers': set(re.findall(r'\d+(?:\.\d+)?', headline)),
                'date': day, 'category': str(article.get('카테고리코드') or 'ALL')}

    def same_event(a, b):
        if not a['text'] or not b['text']:
            return False
        if a['date'] and b['date'] and abs((a['date'] - b['date']).days) > 2:
            return False
        if (a['category'] != b['category'] and 'ALL' not in
                (a['category'], b['category'])):
            return False
        # Two articles with incompatible key figures often concern different
        # dates/quarters, even when the rest of a headline is identical.
        if a['numbers'] and b['numbers'] and not (
                a['numbers'] <= b['numbers'] or b['numbers'] <= a['numbers']):
            return False
        # The most common dangerous false positives: same general headline
        # about different cities, different markets, or opposing positions.
        exclusive_sets = (
            {'서울', '부산', '대구', '인천', '광주', '대전', '울산', '제주',
             '수원', '성남', '고양', '용인', '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남'},
            {'매매', '전세', '월세'},
            {'찬성', '반대'},
        )
        for options in exclusive_sets:
            ea = {w for w in options if w in a['text']}
            eb = {w for w in options if w in b['text']}
            if ea and eb and not (ea & eb):
                return False
        common = a['words'] & b['words']
        informative = [w for w in common if len(w) >= 3]
        if len(common) < 2 or not informative:
            return False
        union = a['words'] | b['words']
        token_jaccard = len(common) / len(union) if union else 0
        shared_grams = a['grams'] & b['grams']
        dice = 2 * len(shared_grams) / max(1, len(a['grams']) + len(b['grams']))
        ratio = SequenceMatcher(None, a['text'], b['text']).ratio()
        return bool(
            (ratio >= .85 and token_jaccard >= .48 and dice >= .54) or
            (dice >= .75 and token_jaccard >= .38) or
            (token_jaccard >= .62 and dice >= .57 and ratio >= .74) or
            (ratio >= .76 and dice >= .52 and len(common) >= 2
             and len(a['numbers'] & b['numbers']) >= 2)
        )

    groups, anchors = [], []
    inverted = defaultdict(set)
    for article in articles:
        candidate = info(article)
        # Only consider groups sharing an informative token; avoids O(n^2)
        # comparisons against the entire 7-day archive.
        possible = set()
        for token in candidate['words']:
            if len(token) >= 3:
                possible.update(inverted.get(token, ()))
        matched = None
        for group_i in sorted(possible):
            if same_event(candidate, anchors[group_i]):
                matched = group_i
                break
        if matched is None:
            matched = len(groups)
            groups.append([article])
            anchors.append(candidate)
        else:
            groups[matched].append(article)
        for token in candidate['words']:
            if len(token) >= 3:
                inverted[token].add(matched)
    return groups


def publisher(item, known):
    keyword = str(item.get('키워드', ''))
    if keyword.startswith('출처:'):
        return keyword[3:].strip() or '출처 미상'
    return known.get(item.get('언론사코드'), item.get('언론사코드') or '출처 미상')


DEFAULT_FAVORITES = ['MBC', 'JTBC', '중앙일보', '동아일보', '연합뉴스']


def effective_favorites():
    """Saved favorites win; a fresh installation starts with five publishers."""
    return favorite_names() or DEFAULT_FAVORITES


def target_source_codes():
    """Fetch only sources corresponding to the active favorite publisher list.

    Prefer Media tab rows (allows custom registered feeds), and use the
    built-in code list for the five default outlets. Do not fall back to the
    entire national news catalog on initial load.
    """
    selected = {name.casefold().strip() for name in effective_favorites()}
    return [str(row.get('언론사코드')) for row in data('Media')
            if str(row.get('언론사명', '')).casefold().strip() in selected
            and enabled(row.get('사용여부'))]


def favorite_names():
    """A shared favorite list backed by the existing Settings sheet."""
    return preference('favorite_publishers', [])


def favorites_editor(articles, known, inline=False):
    """Public app preference editor: no separate administrator page."""
    names = sorted({publisher(r, known) for r in articles})
    saved = favorite_names()
    st.caption('선택한 언론사는 모든 기기에서 공통으로 사용합니다.')
    with st.form('favorite_editor'):
        chosen = st.multiselect('즐겨찾기 언론사', sorted(set(names + saved)), default=(saved or DEFAULT_FAVORITES),
                                placeholder='언론사 검색·선택')
        submit = st.form_submit_button('즐겨찾기 저장', use_container_width=True)
    if submit:
        save_preference('favorite_publishers', chosen, '공통 즐겨찾기 언론사')
        st.rerun()


def header_menu():
    # One menu instead of a full-width toolbar; this is especially important on iPad.
    with st.popover('☰ 메뉴', use_container_width=True):
        categories = [r for r in data('Category') if enabled(r.get('사용여부'))]
        categories.sort(key=lambda r: int(r.get('정렬순서') or 999))
        cmap = {r['카테고리코드']: r['카테고리명'] for r in categories}
        codes = list(cmap) or ['ALL']
        saved = st.session_state.get('category_selection', 'ALL')
        if saved not in codes:
            st.session_state['category_selection'] = 'ALL' if 'ALL' in codes else codes[0]
        st.selectbox('뉴스 분야', codes, format_func=lambda c: cmap.get(c,c),
                     key='category_selection')
        st.session_state.setdefault('news_favorite_mode', '⭐ 즐겨찾기')
        st.selectbox('언론사 보기', ['⭐ 즐겨찾기', '전체 언론사'],
                     key='news_favorite_mode')
        with st.expander('언론사 즐겨찾기 관리'):
            articles = [r for r in data('News') if recent_news(r, 7)]
            known = {r['언론사코드']: r['언론사명'] for r in data('Media')}
            favorites_editor(articles, known, inline=True)
        st.text_input('뉴스 검색', placeholder='제목 또는 언론사', key='news_search')
        st.selectbox('기간', ['오늘', '어제', '최근 7일'], index=2, key='news_date')
        st.selectbox('정렬', ['최신순', '언론사순', '관련 보도 많은 순'], key='news_sort')
        st.toggle('유사 기사 묶기', value=True, key='news_group')
        st.toggle('RSS 설명 표시', value=False, key='news_excerpts')
        st.select_slider('한 화면 기사 수', options=[20,40,60], value=40,
                         key='news_limit')
        with st.expander('관심 / 제외 키워드'):
            saved_include = preference('priority_keywords_v1', [])
            saved_exclude = preference('excluded_keywords_v1', [])
            with st.form('keywords_form'):
                include = st.text_input('관심 키워드', value=', '.join(saved_include))
                exclude = st.text_input('제외 키워드', value=', '.join(saved_exclude))
                if st.form_submit_button('키워드 저장'):
                    def terms(raw):
                        return list(dict.fromkeys(t.strip() for t in raw.split(',') if t.strip()))[:25]
                    save_preference('priority_keywords_v1', terms(include), '관심 키워드')
                    save_preference('excluded_keywords_v1', terms(exclude), '제외 키워드')
                    st.rerun()


def render_article_summary(article):
    """Run AI only on an explicit article click; persist once per article."""
    value = str(article.get('요약') or '').strip()
    cached = value.startswith('AI_SUMMARY_V2::')
    if cached:
        st.markdown('<div class="article-explainer">' + esc(value.split('::', 1)[1])
                    + '</div>', unsafe_allow_html=True)
    else:
        # An RSS snippet is not labeled or presented as a generated summary.
        token = str(setting('news_brief', 'openrouter_api_key', '')).strip()
        if not token:
            st.caption('AI 연결 정보가 없어 원문을 확인해 주세요.')
        else:
            # A shared, persistent limit conserves the free tier. Avoid any
            # model calls on page loads or repeated clicks of saved summaries.
            today = now()[:10]
            records = data('Collection_Log')
            used = sum(1 for r in records if str(r.get('언론사코드')) == 'ARTICLE_AI'
                       and str(r.get('실행일시', '')).startswith(today))
            if used >= 12:
                st.caption('오늘의 기사 요약 한도에 도달했습니다. 원문을 이용해 주세요.')
            else:
                try:
                    with st.spinner('기사를 간추리는 중…'):
                        brief = summarize_article(article, token)
                    persisted = patch_record(book(), 'News', '뉴스ID', article['뉴스ID'],
                                             {'요약': 'AI_SUMMARY_V2::' + brief,
                                              '원문URL': article.get('_resolved_url') or article.get('원문URL', '')})
                    if persisted:
                        append_records(book(), 'Collection_Log', [dict(zip(HEADERS['Collection_Log'],
                            [str(article['뉴스ID']) + '-' + now(), now(), 'ARTICLE_AI', 'AI',
                             1, 1, 0, '성공', '', 0]))])
                        data.clear()
                    st.markdown('<div class="article-explainer">' + esc(brief)
                                + '</div>', unsafe_allow_html=True)
                except ValueError as exc:
                    if str(exc) == 'ARTICLE_CONTENT_UNAVAILABLE':
                        st.caption('이 기사는 제목 외에 확인할 수 있는 본문이 없어 정확한 요약을 만들 수 없습니다.')
                    elif str(exc) == 'ARTICLE_CONTENT_INSUFFICIENT':
                        st.caption('확인된 내용이 부족해 요약을 제공하지 않습니다.')
                    else:
                        st.caption('요약에 필요한 기사 내용을 확인하지 못했습니다.')
                except Exception:
                    st.caption('지금은 요약을 생성하지 못했습니다. 원문을 이용해 주세요.')
    if cached or 'brief' in locals():
        st.markdown('<div class="article-source-note">공개 기사 자료 기준 · 원문에서 사실 확인 권장</div>',
                    unsafe_allow_html=True)
    url = safe_url(article.get('_resolved_url') or article.get('원문URL', ''))
    if url:
        st.markdown('<div class="article-original"><a href="' + esc(url)
                    + '" target="_blank" rel="noopener noreferrer">원문 보기 ↗</a></div>',
                    unsafe_allow_html=True)


def render_news_tile(group, index, known, cmap):
    article = group[0]
    key = str(article.get('뉴스ID'))
    actual = publisher(article, known)
    display_title = str(article.get('제목', ''))
    if display_title.endswith(' - ' + actual):
        display_title = display_title[:-(len(actual) + 3)]
    published = str(article.get('발행일시', ''))[5:16]
    category = cmap.get(article.get('카테고리코드'), '')
    with st.container(key=f'article_tile_{index}'):
        title_col, meta_col = st.columns([4.6, 1.5], vertical_alignment='center', gap='small')
        with title_col:
            if st.button(display_title, key=f'article_open_{key}', use_container_width=True):
                st.session_state['opened_news_id'] = (None if st.session_state.get('opened_news_id') == key else key)
        with meta_col:
            metadata = ' · '.join(x for x in (category, actual, published) if x)
            st.markdown('<div class="news-right-meta">' + esc(metadata) + '</div>', unsafe_allow_html=True)
        if st.session_state.get('opened_news_id') == key:
            render_article_summary(article)
        if len(group) > 1:
            # Related publishers are supplementary; never an additional card.
            with st.expander(f'관련 {len(group)-1}건', expanded=False):
                for other in group[1:]:
                    u = safe_url(other.get('원문URL', ''))
                    if u:
                        safe_label = (publisher(other, known) + ' · ' +
                                      str(other.get('제목', ''))[:90]).replace('[', '').replace(']', '')
                        st.markdown('[' + safe_label.replace('(', '（').replace(')', '）')
                                    + '](' + u + ')')


def app_home():
    ensure_setup()
    categories = [r for r in data('Category') if enabled(r.get('사용여부'))]
    categories.sort(key=lambda r: int(r.get('정렬순서') or 999))
    cmap = {r['카테고리코드']: r['카테고리명'] for r in categories}
    if not cmap:
        st.info('카테고리가 아직 없습니다. 새로고침해 주세요.')
        return
    # On reads, expire visually; real deletions run on explicit refresh.
    all_items = [r for r in data('News') if enabled(r.get('노출여부'))
                 and str(r.get('중복여부', 'N')).upper() != 'Y' and recent_news(r, 7)]
    all_items.sort(key=lambda r: str(r.get('발행일시', '')), reverse=True)
    known = {r['언론사코드']: r['언론사명'] for r in data('Media')}
    last_logs = data('Collection_Log')
    last_time = max((str(r.get('실행일시','')) for r in last_logs
                     if str(r.get('수집방식','')).upper() == 'RSS'), default='')
    try:
        maybe_update_news(last_time)
        update_indicator()
    except Exception as exc:
        # News should remain readable even when background refresh is unavailable.
        st.caption('최신 기사 자동 확인이 잠시 지연되고 있습니다. 저장된 뉴스를 표시합니다.')

    # The primary layout deliberately contains no collection button, counts or
    # explanatory title. News collection is lazy and background-only.
    if not all_items and not news_update_guard()['running']:
        st.caption('최근 7일 뉴스가 없습니다. 최신 기사 확인을 시도하고 있습니다.')

    # Category, publisher selection, favorites and all filters are contained in
    # the single compact header menu (rendered by main()).
    category = st.session_state.get('category_selection', 'ALL')
    if category not in cmap:
        category = 'ALL'
    favorite_mode = st.session_state.get('news_favorite_mode', '⭐ 즐겨찾기')


    favorites = effective_favorites() if favorite_mode == '⭐ 즐겨찾기' else []
    items = [r for r in all_items if category == 'ALL' or r.get('카테고리코드') == category]
    if favorite_mode == '⭐ 즐겨찾기':
        items = [r for r in items if publisher(r, known) in favorites]
    search = str(st.session_state.get('news_search', '')).strip().casefold()
    if search:
        items = [r for r in items if search in (str(r.get('제목','')) + ' ' + publisher(r, known)).casefold()]
    date_choice = st.session_state.get('news_date', '최근 7일')
    today = datetime.now(ZoneInfo('Asia/Seoul')).date()
    if date_choice != '최근 7일':
        date_wanted = today if date_choice == '오늘' else today - timedelta(days=1)
        items = [r for r in items if str(r.get('발행일시') or r.get('수집일시', ''))[:10] == date_wanted.isoformat()]
    exclude = [x.casefold() for x in preference('excluded_keywords_v1', []) if x]
    if exclude:
        items = [r for r in items if not any(word in str(r.get('제목','')).casefold() for word in exclude)]
    priority = [x.casefold() for x in preference('priority_keywords_v1', []) if x]
    clusters = cluster_titles(items) if st.session_state.get('news_group', True) else [[r] for r in items]
    sorting = st.session_state.get('news_sort', '최신순')
    if sorting == '언론사순':
        clusters.sort(key=lambda g: publisher(g[0], known))
    elif sorting == '관련 보도 많은 순':
        clusters.sort(key=lambda g: (-len({publisher(r, known) for r in g}), -len(g)))
    elif priority:
        clusters.sort(key=lambda g: (not any(x in str(r.get('제목','')).casefold() for r in g for x in priority),
                                     str(g[0].get('발행일시',''))), reverse=False)
        # Preserve headline priority but restore newest first within each group.
        clusters.sort(key=lambda g: str(g[0].get('발행일시','')), reverse=True)
        clusters.sort(key=lambda g: not any(x in str(r.get('제목','')).casefold() for r in g for x in priority))


    if not items:
        st.info('선택한 조건의 뉴스가 없습니다. ☰ 메뉴에서 언론사나 기간을 확인해 주세요.')
        return
    limit = int(st.session_state.get('news_limit',40))
    pages = max(1, (len(clusters) + limit - 1)//limit)
    if st.session_state.get('news_page',1) > pages:
        st.session_state.news_page = 1
    page = int(st.session_state.get('news_page',1))
    shown = clusters[(page-1)*limit:page*limit]
    # Native Streamlit buttons allow inline on-demand AI without leaving the app.
    # Two columns on wide screens; Streamlit stacks them on phone widths.
    for i in range(0, len(shown), 2):
        left, right = st.columns(2, gap='small')
        with left:
            render_news_tile(shown[i], i, known, cmap)
        if i + 1 < len(shown):
            with right:
                render_news_tile(shown[i+1], i+1, known, cmap)

    if pages > 1:
        st.number_input('페이지 이동', min_value=1, max_value=pages, step=1, key='news_page')
    


def main():
    try:
        ensure_setup()
        # Avoid Streamlit embed=true: it removes
        # the header clearance on some mobile browsers.
        logs = data('Collection_Log')
        last_stamp = max((str(r.get('실행일시','')) for r in logs
                          if str(r.get('수집방식','')).upper() == 'RSS'), default='')
        timestamp = last_stamp[5:16].replace('-', '.') if last_stamp else '기록 없음'
        with st.container(key='news_header'):
            brandcol, menucol = st.columns([7, 1], vertical_alignment='center', gap='small')
            with brandcol:
                st.markdown('<div class="brand"><span class="brand-mark">N</span>'
                            '<span>간추린 뉴스</span>'
                            f'<span class="brand-updated">Updated {esc(timestamp)}</span></div>',
                            unsafe_allow_html=True)
            with menucol:
                header_menu()
        app_home()
    except Exception as exc:
        handle_error(exc)
    st.markdown('<div class="anycorea">Created by anycorea</div>', unsafe_allow_html=True)


if __name__ == '__main__':
    main()
