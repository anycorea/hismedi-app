"""News Brief: readable news + RSS-grounded AI briefings, Google Sheets backend."""
import html
import json
from datetime import datetime, timedelta
import threading
import time
import re
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

import streamlit as st
from news_core import (HEADERS, ai_briefing, append_records, collect, connect, enabled,
                       initialize, now, patch_record, rows, safe_url, save_briefing, validate,
                       clean_old_rows, recent_news)

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
.news-meta {display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#7c8a98;font-size:.74rem;margin-bottom:5px}
.news-source {display:inline-block;color:#167352;background:#e8f5ee;font-weight:750;border-radius:6px;padding:3px 8px;overflow-wrap:anywhere}
.news-category {color:#617b92;font-weight:700}
.news-title {font-size:1.03rem;line-height:1.4;letter-spacing:-.018em;font-weight:735;overflow-wrap:anywhere;word-break:keep-all}
.news-title a {color:#1b2d42;text-decoration:none}
.news-title a:hover {color:#08785b;text-decoration:underline;text-underline-offset:3px}
.news-desc {color:#647789;font-size:.87rem;line-height:1.6;margin-top:8px;word-break:keep-all}
.summary-hero {background:#fff;border:1px solid #e0e8ee;border-left:4px solid #148669;border-radius:13px;padding:20px 22px;margin:12px 0 18px}
.summary-hero h3 {font-size:1.13rem;margin:0 0 8px;letter-spacing:-.015em;color:#182c42}
.summary-hero p {color:#53677b;line-height:1.7;font-size:.94rem;margin:0}
.issue-grid {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.issue-card {background:white;border:1px solid #e2e9ef;border-radius:12px;padding:17px 18px}
.issue-number {font-weight:850;font-size:.76rem;color:#148669;margin-bottom:6px}
.issue-title {font-size:1rem;font-weight:760;color:#1b2e43;line-height:1.48;word-break:keep-all}
.issue-point {color:#566a7d;line-height:1.65;font-size:.88rem;margin:10px 0 9px;word-break:keep-all}
.issue-links {font-size:.78rem;color:#237a62;line-height:1.6}
.issue-links a {color:#087859;text-decoration:none;margin-right:12px}
.issue-links a:hover {text-decoration:underline}
.empty-ai {background:#fff;border:1px solid #e2e9ef;border-radius:12px;padding:22px;color:#52677c}
.related{margin-top:8px;font-size:.78rem;color:#168162}.related summary{cursor:pointer;font-weight:650}.related a{color:#0d765a;text-decoration:none}.related a:hover{text-decoration:underline}
.anycorea {text-align:center;color:#9aa5b0;font-size:.70rem;padding:14px 0}
/* Avoid decorative wrappers: the headline and settings are the only toolbar. */
.brand{white-space:nowrap}
.news-grid{gap:8px 10px}
.st-key-news_view_mode button{white-space:nowrap}
@media(max-width:700px){.brand{font-size:1.09rem}.brand-mark{width:27px;height:27px}}

/* Layout only this app's masthead, not Streamlit columns used elsewhere.
   Desktop: single line. Mobile/tablet portrait: two clean rows. */
.st-key-news_header [data-testid="stHorizontalBlock"] {
 display:grid!important;
 grid-template-columns:minmax(145px,1.8fr) minmax(310px,3.2fr) minmax(130px,1.25fr) minmax(98px,.85fr);
 gap:12px;align-items:center;
}
.st-key-news_header [data-testid="stHorizontalBlock"] > div {
 width:100%!important;min-width:0!important;max-width:none!important;flex:none!important;
}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(1) {grid-area:brand}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(2) {grid-area:nav}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(3) {grid-area:updated}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(4) {grid-area:menu}
.st-key-news_header [data-testid="stHorizontalBlock"] {
 grid-template-areas:"brand nav updated menu";
}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(2) {overflow-x:auto;scrollbar-width:none}
.st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(2)::-webkit-scrollbar {display:none}
.mobile-updated{display:none}
@media(max-width:900px){
 .st-key-news_header [data-testid="stHorizontalBlock"] {
  grid-template-columns:minmax(0,1fr) minmax(88px,max-content)!important;
  grid-template-areas:"brand menu" "nav nav";
  gap:6px 12px;
 }
 .st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(3){display:none}
 .mobile-updated{display:block;font-size:.69rem;color:#8290a0;margin-top:2px;font-weight:500;white-space:nowrap}
 .st-key-news_header [data-testid="stHorizontalBlock"] > div:nth-child(2){overflow-x:auto;white-space:nowrap}
 .block-container{padding-top:1.1rem!important}
}
@media(max-width:450px){
 .st-key-news_header [data-testid="stHorizontalBlock"] {gap:3px 8px}
 .st-key-news_header .stButton button {font-size:.85rem}
}


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


@st.cache_data(ttl=300, show_spinner=False)
def sheet_problems():
    return validate(book())


@st.cache_resource
def briefing_guard():
    # Shared per running app process; Google Sheets remains the durable successful cache.
    return {'lock': threading.Lock(), 'failed_until': {}}


def latest_brief(category):
    exact = [r for r in data('Briefing') if enabled(r.get('노출여부'))
             and str(r.get('카테고리코드')) == category]
    exact.sort(key=lambda r: str(r.get('생성일시', '')), reverse=True)
    return exact[0] if exact else None


def briefing_is_fresh(brief):
    if not brief:
        return False
    try:
        stamp = datetime.fromisoformat(str(brief.get('생성일시', '')).strip())
        return datetime.fromisoformat(now()) - stamp < timedelta(hours=6)
    except (ValueError, TypeError):
        return False


def auto_briefing(category, articles):
    """Generate only for missing/old briefings; one generation at a time per process."""
    token = str(setting('news_brief', 'openrouter_api_key', '')).strip()
    if not token:
        return 'OpenRouter 무료 API 키가 등록되지 않아 저장된 브리핑만 표시합니다.'
    if not articles:
        return '요약할 뉴스가 아직 없습니다.'
    guard = briefing_guard()
    key = str(category)
    current = datetime.fromisoformat(now())
    with guard['lock']:
        # Re-read the persistent sheet after obtaining the lock so other sessions reuse output.
        data.clear()
        if briefing_is_fresh(latest_brief(category)):
            return None
        # Cap successful automatic briefs per day across all categories.
        today = now()[:10]
        daily_count = sum(1 for r in data('Briefing')
                          if str(r.get('생성일시', '')).startswith(today)
                          and str(r.get('생성모델', '')).startswith('openrouter/'))
        if daily_count >= 8:
            return '오늘의 자동 AI 브리핑 생성 한도(8회)에 도달했습니다. 저장된 브리핑을 표시합니다.'
        if guard['failed_until'].get(key, datetime.min) > current:
            return '무료 AI가 일시적으로 응답하지 않아 기존 브리핑을 표시합니다. 잠시 후 다시 이용할 수 있습니다.'
        try:
            with st.spinner('최신 뉴스를 AI가 간추리고 있습니다…'):
                result = ai_briefing(articles, api_key=token, model='openrouter/free')
                save_briefing(book(), result, category)
            data.clear()
            return None
        except Exception as exc:
            # A 30 minute failure cooldown avoids repeated free API calls on page refresh.
            guard['failed_until'][key] = current + timedelta(minutes=30)
            return 'AI 브리핑 자동 생성에 실패했습니다. 기존 브리핑을 표시합니다. (' + str(exc)[:145] + ')'


def refresh():
    data.clear()
    sheet_problems.clear()


def handle_error(exc):
    st.error('뉴스 데이터를 읽는 중 문제가 발생했습니다. 잠시 후 다시 접속해 주세요.')
    # Exception text may contain request metadata; keep it out of public UI.
    st.caption(f'오류 종류: {type(exc).__name__} · 자세한 원인은 Streamlit 앱 로그에서 확인')


@st.cache_resource
def news_update_guard():
    # Process-wide, so simultaneous iPhone/iPad/PC visits cannot launch multiple collects.
    return {'lock': threading.Lock(), 'state_lock': threading.Lock(),
            'running': False, 'last_start': 0.0, 'last_end': 0.0,
            'last_report': None, 'last_error': '', 'version': 0}


def on_view_change():
    """Selecting Latest News explicitly requests a fresh check, not a forced RSS call."""
    if st.session_state.get('news_view_mode') == '최신 뉴스':
        st.session_state['news_user_requested_latest'] = True


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
    if not stale and not request:
        return
    # User explicitly tapping latest may refresh sooner, but never more often than 5 min.
    elapsed = time.monotonic() - guard['last_start']
    if guard['last_start'] and elapsed < (300 if request else 900):
        return
    if not guard['lock'].acquire(blocking=False):
        return
    # Everything consumed by the background thread is copied before it starts.
    config = {'gcp_service_account': dict(st.secrets['gcp_service_account']),
              'gsheet': {'spreadsheet_id': str(st.secrets['gsheet']['spreadsheet_id'])}}
    with guard['state_lock']:
        guard['running'] = True
        guard['last_start'] = time.monotonic()
        guard['last_error'] = ''

    def worker():
        try:
            b = connect(config)
            result = collect(b, limit_per_feed=20)
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
    """Conservative same-event grouping, not a semantic fact-check.
    Avoid combining articles from the same publisher and require unusually high
    headline similarity AND shared meaningful tokens.
    """
    noise = {'오늘','뉴스','속보','단독','종합','관련','발표','기자','대한','한국','정부'}
    def words(title):
        return {w for w in re.findall(r'[가-힣A-Za-z0-9]{2,}', str(title).lower())
                if w not in noise}
    groups = []
    for article in articles:
        tokens = words(article.get('제목', ''))
        best = None
        for group in groups:
            root = group[0]
            other = words(root.get('제목', ''))
            common = tokens & other
            union = tokens | other
            if len(common) < 3 or not union:
                continue
            jaccard = len(common) / len(union)
            ratio = SequenceMatcher(None, str(article.get('제목', '')),
                                    str(root.get('제목', ''))).ratio()
            if jaccard >= .72 and ratio >= .68:
                best = group
                break
        if best is None:
            groups.append([article])
        else:
            best.append(article)
    return groups


def save_bookmarks(article_ids):
    news = {str(r.get('뉴스ID')): r for r in data('News')}
    existing = preference('bookmarked_articles_v1', [])
    saved = {str(v.get('id')): v for v in existing if isinstance(v, dict)}
    for news_id in article_ids:
        r = news.get(str(news_id))
        if r:
            saved[str(news_id)] = {'id': str(news_id), 'title': str(r.get('제목',''))[:220],
                                   'url': safe_url(r.get('원문URL')),
                                   'publisher': str(r.get('키워드','')).removeprefix('출처:'),
                                   'saved': now()}
    save_preference('bookmarked_articles_v1', list(saved.values())[-120:],
                    '7일 이후에도 원문 링크를 유지하는 북마크')


def publisher(item, known):
    keyword = str(item.get('키워드', ''))
    if keyword.startswith('출처:'):
        return keyword[3:].strip() or '출처 미상'
    return known.get(item.get('언론사코드'), item.get('언론사코드') or '출처 미상')


def favorite_names():
    """A shared favorite list backed by the existing Settings sheet."""
    return preference('favorite_publishers', [])


def favorites_editor(articles, known, inline=False):
    """Public app preference editor: no separate administrator page."""
    names = sorted({publisher(r, known) for r in articles})
    saved = favorite_names()
    st.caption('선택한 언론사는 모든 기기에서 공통으로 사용합니다.')
    with st.form('favorite_editor'):
        chosen = st.multiselect('즐겨찾기 언론사', sorted(set(names + saved)), default=saved,
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
        st.selectbox('언론사 보기', ['전체 언론사', '⭐ 즐겨찾기'],
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
    last_time = max((str(r.get('실행일시','')) for r in last_logs), default='')
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

    old = st.session_state.get('news_category', 'ALL')
    catcodes = list(cmap)
    if old not in catcodes:
        old = catcodes[0]
    mode = st.session_state.get('news_view_mode', '최신 뉴스') or '최신 뉴스'
    # Category, publisher selection, favorites and all filters are contained in
    # the single compact header menu (rendered by main()).
    category = st.session_state.get('category_selection', 'ALL')
    if category not in cmap:
        category = 'ALL'
    favorite_mode = st.session_state.get('news_favorite_mode', '전체 언론사')

    # Bookmarks do not get removed with 7-day news retention.
    if mode == '🔖 보관함':
        saved = preference('bookmarked_articles_v1', [])
        if not saved:
            st.info('저장한 기사가 없습니다. 최신 뉴스의 🔖 저장 메뉴에서 추가할 수 있습니다.')
            return
        with st.popover('북마크 관리'):
            opts = {str(i): str(a.get('title', ''))[:95] for i, a in enumerate(saved) if isinstance(a, dict)}
            with st.form('remove_saved'):
                removing = st.multiselect('삭제할 북마크', list(opts), format_func=lambda x: opts[x])
                if st.form_submit_button('선택 삭제'):
                    save_preference('bookmarked_articles_v1', [a for i, a in enumerate(saved) if str(i) not in removing], '저장한 기사')
                    st.rerun()
        tiles = []
        for a in reversed(saved):
            url = safe_url(a.get('url',''))
            if url:
                tiles.append('<article class="news-tile"><div class="news-meta">🔖 '
                             + esc(a.get('publisher','')) + '</div><div class="news-title"><a href="'
                             + esc(url) + '" target="_blank" rel="noopener noreferrer">'
                             + esc(a.get('title','')) + ' ↗</a></div></article>')
        st.markdown('<div class="news-grid">' + ''.join(tiles) + '</div>', unsafe_allow_html=True)
        return

    favorites = favorite_names() if favorite_mode == '⭐ 즐겨찾기' else []
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

    if mode == '✨ AI 브리핑':
        brief = latest_brief(category)
        if not briefing_is_fresh(brief):
            if category == 'ALL':
                buckets = {}
                for r in items:
                    buckets.setdefault(str(r.get('카테고리코드','ALL')), []).append(r)
                balanced = []
                while len(balanced) < 30 and any(buckets.values()):
                    for group in buckets.values():
                        if group and len(balanced) < 30:
                            balanced.append(group.pop(0))
                source_items = balanced
            else:
                source_items = items[:30]
            message = auto_briefing(category, source_items)
            if message:
                st.caption(message)
        brief = latest_brief(category) or (latest_brief('ALL') if category != 'ALL' else None)
        if not brief:
            st.info('AI 브리핑이 아직 없습니다. 무료 AI가 가능한 경우 이 화면에서 생성됩니다.')
            return
        st.markdown('<div class="summary-hero"><h3>' + esc(brief.get('브리핑제목'))
                    + '</h3><p>' + esc(brief.get('핵심요약')) + '</p></div>', unsafe_allow_html=True)
        try:
            issues = json.loads(brief.get('주요이슈') or '[]')
        except (ValueError, TypeError):
            issues = []
        idmap = {str(r.get('뉴스ID')): r for r in all_items}
        cards = []
        for i, issue in enumerate(issues[:10], 1):
            related = [idmap[str(k)] for k in issue.get('ids', []) if str(k) in idmap]
            links = []
            for r in related[:3]:
                url = safe_url(r.get('원문URL',''))
                if url:
                    links.append(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">'
                                 f'{esc(publisher(r, known))} ↗</a>')
            cards.append('<article class="issue-card"><div class="issue-number">ISSUE '
                         + str(i).zfill(2) + '</div><div class="issue-title">'
                         + esc(issue.get('headline')) + '</div><p class="issue-point">'
                         + esc(issue.get('point')) + '</p><div class="issue-links">'
                         + ' '.join(links) + '</div></article>')
        st.markdown('<div class="issue-grid">' + ''.join(cards) + '</div>', unsafe_allow_html=True)
        st.caption('OpenRouter 무료 AI · RSS 제목·설명 기반 (본문 검증 아님) · 생성 ' + str(brief.get('생성일시',''))[:16])
        return

    if not items:
        st.info('선택한 조건의 뉴스가 없습니다. 필터를 바꾸거나 최신 뉴스 갱신을 눌러 주세요.')
        return
    limit = int(st.session_state.get('news_limit',40))
    pages = max(1, (len(clusters) + limit - 1)//limit)
    if st.session_state.get('news_page',1) > pages:
        st.session_state.news_page = 1
    page = int(st.session_state.get('news_page',1))
    shown = clusters[(page-1)*limit:page*limit]
    excerpts = bool(st.session_state.get('news_excerpts',False))
    tiles = []
    for group in shown:
        article = group[0]
        url = safe_url(article.get('원문URL',''))
        display_title = str(article.get('제목',''))
        actual = publisher(article, known)
        if display_title.endswith(' - ' + actual):
            display_title = display_title[:-(len(actual)+3)]
        title = esc(display_title)
        name = esc(actual)
        cat = esc(cmap.get(article.get('카테고리코드',''),''))
        published = esc(str(article.get('발행일시',''))[5:16])
        title_html = (f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title} ↗</a>' if url else title)
        desc = (f'<div class="news-desc">{esc(str(article.get("요약",""))[:220])}</div>'
                if excerpts and article.get('요약') else '')
        media_links = []
        if len(group) > 1:
            for other in group[1:8]:
                other_url = safe_url(other.get('원문URL',''))
                if other_url:
                    media_links.append(f'<a href="{esc(other_url)}" target="_blank" rel="noopener noreferrer">'
                                       + esc(publisher(other, known)) + ' ↗</a>')
        related = ('<details class="related"><summary>관련 보도 ' + str(len(group)-1)
                   + '건 펼치기</summary>' + ' · '.join(media_links) + '</details>') if media_links else ''
        tiles.append('<article class="news-tile"><div class="news-meta"><span class="news-source">'
                     + name + '</span><span class="news-category">' + cat
                     + '</span><span>' + published + '</span></div><div class="news-title">'
                     + title_html + '</div>' + desc + related + '</article>')
    st.markdown('<div class="news-grid">' + ''.join(tiles) + '</div>', unsafe_allow_html=True)
    # Shared bookmark UI stays compact and works even with HTML-rendered news cards.
    with st.popover('🔖 기사 저장'):
        lookup = {str(group[0]['뉴스ID']): group[0] for group in shown}
        with st.form('bookmark_add'):
            chosen = st.multiselect('보관할 기사', list(lookup),
                                    format_func=lambda x: str(lookup[x].get('제목',''))[:100])
            if st.form_submit_button('북마크에 저장') and chosen:
                save_bookmarks(chosen)
                st.success(f'{len(chosen)}개 저장했습니다.')
    if pages > 1:
        st.number_input('페이지 이동', min_value=1, max_value=pages, step=1, key='news_page')
    st.caption('기사 클릭 시 원문으로 이동합니다. 유사 기사 묶음은 제목 비교에 의한 추정입니다.')


def main():
    try:
        ensure_setup()
        # The latest update is a single understated text element, placed directly
        # after the three navigation choices. Avoid Streamlit embed=true: it removes
        # the header clearance on some mobile browsers.
        logs = data('Collection_Log')
        last_stamp = max((str(r.get('실행일시','')) for r in logs), default='')
        timestamp = last_stamp[5:16].replace('-', '.') if last_stamp else '기록 없음'
        with st.container(key='news_header'):
            brandcol, navcol, timecol, menucol = st.columns(
                [2.1, 4.2, 1.7, 1.1], vertical_alignment='center', gap='small')
            with brandcol:
                st.markdown('<div class="brand"><span class="brand-mark">N</span>'
                            '<span>간추린 뉴스<span class="mobile-updated">Updated '
                            + esc(timestamp) + '</span></span></div>',
                            unsafe_allow_html=True)
            with navcol:
                st.segmented_control('화면', ['최신 뉴스', '✨ AI 브리핑', '🔖 보관함'],
                                     default='최신 뉴스', label_visibility='collapsed',
                                     key='news_view_mode', on_change=on_view_change)
            with timecol:
                st.markdown(f'<div class="header-updated">Updated {esc(timestamp)}</div>',
                            unsafe_allow_html=True)
            with menucol:
                header_menu()
        app_home()
    except Exception as exc:
        handle_error(exc)
    st.markdown('<div class="anycorea">Created by anycorea</div>', unsafe_allow_html=True)


if __name__ == '__main__':
    main()
