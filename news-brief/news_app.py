"""News Brief: readable news + RSS-grounded AI briefings, Google Sheets backend."""
import html
import json
from datetime import datetime, timedelta
import threading
import re
from difflib import SequenceMatcher
from zoneinfo import ZoneInfo

import streamlit as st
from news_core import (HEADERS, ai_briefing, append_records, collect, connect, enabled,
                       initialize, now, patch_record, rows, safe_url, save_briefing, validate,
                       clean_old_rows, recent_news)

st.set_page_config(page_title='News Brief | AI 뉴스브리핑', page_icon='📰',
                   layout='wide', initial_sidebar_state='collapsed')

# No custom fixed heading positioning. Retain the Streamlit default content padding.
st.markdown('''<style>
/* Default Streamlit top offset prevents header clipping across releases. */
.stApp {background:#f7f9fc;color:#15253b}
.block-container {max-width:1880px;padding-bottom:1.5rem}
header[data-testid="stHeader"] {background:#f7f9fc}
#MainMenu, footer, div[data-testid="stDecoration"] {visibility:hidden}
.stButton button, .stLinkButton a {min-height:42px;border-radius:11px}
.brand {display:flex;align-items:center;gap:9px;font-size:1.25rem;font-weight:840;color:#10263f;letter-spacing:-.035em}
.brand-mark {width:29px;height:29px;display:inline-flex;align-items:center;justify-content:center;background:#0d765a;color:white;border-radius:9px;font-size:1rem}
.brand-small {font-size:.72rem;color:#7c8a99;font-weight:600;letter-spacing:0}
.page-note {color:#7a8899;font-size:.76rem;margin:0 0 7px}
.header-stats {color:#7a8899;font-size:.76rem;text-align:right;white-space:nowrap;line-height:1.35}
@media(max-width:900px){.header-stats {font-size:.69rem;white-space:normal}}
.pill-status {color:#65778c;font-size:.77rem;white-space:nowrap}
.editor-heading {font-size:1.03rem;font-weight:800;color:#152941;letter-spacing:-.025em;margin:4px 0 7px}
.news-grid {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 12px;align-items:stretch}
.news-tile {background:#fff;border:1px solid #e2e9ef;border-radius:9px;padding:11px 15px;min-width:0;transition:border-color .15s,box-shadow .15s}
.news-tile:hover {border-color:#b8cbd8;box-shadow:0 4px 15px rgba(17,39,63,.055)}
.news-meta {display:flex;align-items:center;gap:8px;flex-wrap:wrap;color:#7c8a98;font-size:.74rem;margin-bottom:5px}
.news-source {display:inline-block;color:#167352;background:#e8f5ee;font-weight:750;border-radius:6px;padding:3px 8px;overflow-wrap:anywhere}
.news-category {color:#617b92;font-weight:700}
.news-title {font-size:var(--news-title-size,1.03rem);line-height:1.4;letter-spacing:-.018em;font-weight:735;overflow-wrap:anywhere;word-break:keep-all}
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
.anycorea {text-align:center;color:#9aa5b0;font-size:.70rem;padding:22px 0}
@media(max-width:850px){.news-grid,.issue-grid{grid-template-columns:1fr}}
@media(max-width:700px){
 .block-container{padding-left:.72rem;padding-right:.72rem;padding-bottom:1.5rem}
 .brand{font-size:1.13rem}
 .news-tile{padding:12px 13px;border-radius:9px}
 .news-title{font-size:max(var(--news-title-size,1.03rem),1.04rem)}
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
    st.error('데이터를 불러오지 못했습니다. 잠시 후 새로고침해 주세요.')
    with st.expander('오류 정보'):
        st.code(f'{type(exc).__name__}: {exc}', language='text')


@st.cache_resource
def news_update_guard():
    return {'lock': threading.Lock(), 'last': None}


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


def collect_on_click():
    """Single explicit action; no background scheduler or implicit AI charges."""
    guard = news_update_guard()
    if not guard['lock'].acquire(blocking=False):
        return {'busy': True}
    try:
        b = book()
        ensure_setup()
        result = collect(b, limit_per_feed=20)
        deleted = clean_old_rows(b, days=7)
        refresh()
        return {**result, 'deleted': sum(deleted.values()), 'busy': False}
    finally:
        guard['lock'].release()


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


def favorites_editor(articles, known):
    """Public app preference editor: no separate administrator page."""
    names = sorted({publisher(r, known) for r in articles})
    saved = favorite_names()
    with st.popover('☆ 언론사 선택', use_container_width=True):
        st.caption('표시할 언론사를 선택합니다. 저장하면 모든 기기에 공통 적용됩니다.')
        with st.form('favorite_editor'):
            chosen = st.multiselect('즐겨찾기 언론사', sorted(set(names + saved)), default=saved,
                                    placeholder='검색해서 여러 언론사 선택')
            submit = st.form_submit_button('즐겨찾기 저장', type='primary', use_container_width=True)
        if submit:
            save_preference('favorite_publishers', chosen, '공통 즐겨찾기 언론사')
            st.rerun()


def app_home(stats_placeholder=None):
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
    if stats_placeholder is not None:
        stats_placeholder.markdown(
            f'<div class="header-stats">{len(all_items):,}개 기사 · '
            f'{len({publisher(r, known) for r in all_items}):,}개 언론사 · '
            f'마지막 갱신 {esc(last_time[5:16]) or "기록 없음"}</div>',
            unsafe_allow_html=True)

    # Collection is triggered only by the compact header button.
    report = st.session_state.pop('news_refresh_report', None)
    if report:
        st.caption(f"갱신 완료 · 신규 {report['new']}건 · 오류 소스 {report['errors']}곳 · "
                   f"7일 경과 정리 {report['deleted']}행 · {report['seconds']}초")
    elif not all_items:
        st.caption('저장된 최근 뉴스가 없습니다. 상단의 ↻ 갱신을 눌러 주세요.')
    if st.session_state.pop('news_refresh_busy', False):
        st.warning('다른 뉴스 갱신 작업이 실행 중입니다.')
    error = st.session_state.pop('news_refresh_error', '')
    if error:
        st.error('수집이 완료되지 않았습니다. 기존 뉴스는 유지됩니다.')
        with st.expander('수집 오류 내용'):
            st.code(error)

    old = st.session_state.get('news_category', 'ALL')
    catcodes = list(cmap)
    if old not in catcodes:
        old = catcodes[0]
    tcol, catcol, favcol, editcol, filtercol = st.columns(
        [2.1, 1.8, 1.5, 1.3, .85], vertical_alignment='center', gap='small')
    with tcol:
        mode = st.segmented_control('보기', ['최신 뉴스', '✨ AI 브리핑', '🔖 보관함'],
                                    default='최신 뉴스', label_visibility='collapsed',
                                    key='news_view_mode') or '최신 뉴스'
    with catcol:
        category = st.selectbox('뉴스 분야', catcodes, index=catcodes.index(old),
                                format_func=lambda c: cmap[c], label_visibility='collapsed',
                                key='category_selection')
        st.session_state.news_category = category
    with favcol:
        favorite_mode = st.selectbox('언론사', ['전체 언론사', '⭐ 즐겨찾기'],
                                     label_visibility='collapsed', key='news_favorite_mode')
    with editcol:
        favorites_editor(all_items, known)
    with filtercol:
        with st.popover('☰ 필터', use_container_width=True):
            st.text_input('검색', placeholder='제목·언론사', key='news_search')
            st.selectbox('기간', ['오늘', '어제', '최근 7일'], index=2, key='news_date')
            st.selectbox('정렬', ['최신순', '언론사순', '관련 보도 많은 순'], key='news_sort')
            st.toggle('유사 기사 묶어보기', value=True, key='news_group')
            st.toggle('RSS 요약 설명', value=False, key='news_excerpts')
            st.select_slider('페이지당 기사', options=[20, 40, 60], value=40, key='news_limit')
            saved_include = preference('priority_keywords_v1', [])
            saved_exclude = preference('excluded_keywords_v1', [])
            with st.form('keywords_form'):
                include = st.text_input('관심 키워드 (쉼표 구분)', value=', '.join(saved_include))
                exclude = st.text_input('제외 키워드 (쉼표 구분)', value=', '.join(saved_exclude))
                if st.form_submit_button('키워드 저장'):
                    def terms(raw):
                        return list(dict.fromkeys(t.strip() for t in raw.split(',') if t.strip()))[:25]
                    save_preference('priority_keywords_v1', terms(include), '관심 뉴스 키워드')
                    save_preference('excluded_keywords_v1', terms(exclude), '숨길 뉴스 키워드')
                    st.rerun()

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
    # Shared bookmark UI stays compact and works even with HTML-rendered news cards.
    with st.popover('🔖 기사 저장'):
        lookup = {str(group[0]['뉴스ID']): group[0] for group in shown}
        with st.form('bookmark_add'):
            chosen = st.multiselect('보관할 기사', list(lookup),
                                    format_func=lambda x: str(lookup[x].get('제목',''))[:100])
            if st.form_submit_button('북마크에 저장') and chosen:
                save_bookmarks(chosen)
                st.success(f'{len(chosen)}개 저장했습니다.')
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
    if pages > 1:
        st.number_input('페이지 이동', min_value=1, max_value=pages, step=1, key='news_page')
    st.caption('기사 클릭 시 원문으로 이동합니다. 유사 기사 묶음은 제목 비교에 의한 추정입니다.')


def main():
    # Header stays on a single row on wide displays; no forced top-padding overrides.
    brandcol, refreshcol, statscol, fontcol = st.columns(
        [2.2, 1.3, 5.8, .55], vertical_alignment='center', gap='small')
    with brandcol:
        st.markdown('<div class="brand"><span class="brand-mark">N</span> 간추린 뉴스'
                    '<span class="brand-small">NEWS BRIEF</span></div>', unsafe_allow_html=True)
    with refreshcol:
        if st.button('↻ 최신 뉴스 갱신', type='primary', use_container_width=True,
                     key='header_news_refresh'):
            with st.spinner('최근 24시간 뉴스 수집 · 7일 경과 기사 정리 중…'):
                try:
                    report = collect_on_click()
                    if report.get('busy'):
                        st.session_state['news_refresh_busy'] = True
                    else:
                        st.session_state['news_refresh_report'] = report
                except Exception as exc:
                    st.session_state['news_refresh_error'] = f'{type(exc).__name__}: {exc}'
            st.rerun()
    with statscol:
        stats_placeholder = st.empty()
    with fontcol:
        with st.popover('Aa', help='글자 크기 설정', use_container_width=True):
            font = st.radio('글자 크기', ['기본', '크게', '아주 크게'], key='font_pref')
            if st.button('새로고침'):
                refresh()
                st.rerun()
    sizes = {'기본': '1.03rem', '크게': '1.2rem', '아주 크게': '1.42rem'}
    font = st.session_state.get('font_pref', '기본')
    st.markdown('<style>:root{--news-title-size:' + sizes[font] + '}</style>',
                unsafe_allow_html=True)
    try:
        app_home(stats_placeholder)
    except Exception as exc:
        handle_error(exc)
    st.markdown('<div class="anycorea">Created by anycorea</div>', unsafe_allow_html=True)


if __name__ == '__main__':
    main()
