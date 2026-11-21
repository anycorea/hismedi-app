"""News Brief: readable news + RSS-grounded AI briefings, Google Sheets backend."""
import html
import json
from datetime import datetime, timedelta
import threading

import streamlit as st
from news_core import (HEADERS, ai_briefing, append_records, collect, connect, enabled,
                       initialize, now, patch_record, rows, safe_url, save_briefing, validate)

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


@st.cache_data(ttl=1800, show_spinner=False)
def ensure_first_news():
    """One-time setup for empty sheets, not a continuous crawler.

    Cache prevents repeated requests on each visitor's rerun. Existing articles
    are never wiped or force-refetched; recurring collection comes later.
    """
    b = book()
    existing_categories = rows(b, 'Category')
    existing_media = rows(b, 'Media')
    existing_news = rows(b, 'News')
    result = {'sources': 0, 'new': 0, 'errors': 0}
    if not existing_categories or not existing_media or not existing_news:
        initialize(b)
        refresh()
    if not existing_news:
        result = collect(b, 18)
        refresh()
        record = dict(zip(HEADERS['Settings'], ['publisher_seed_v2', 'done',
                                              '초기 수집 시 주요 언론사 포함', now()]))
        if not patch_record(b, 'Settings', '설정키', 'publisher_seed_v2', record):
            append_records(b, 'Settings', [record])
        refresh()
    else:
        # Initial one-time targeted media discovery. No recurring collection:
        # existing publisher-specific source codes are not fetched again here.
        # Keep this in a separate Settings flag, so viewers do not repeatedly
        # trigger network calls on reruns.
        setting_rows = rows(b, 'Settings')
        done = any(r.get('설정키') == 'publisher_seed_v2' and
                   r.get('설정값') == 'done' for r in setting_rows)
        if not done:
            from news_core import PRIMARY_MEDIA
            existing_codes = {str(m.get('언론사코드')) for m in existing_media}
            missing_codes = {m[0] for m in PRIMARY_MEDIA} - existing_codes
            initialize(b)
            refresh()
            if missing_codes:
                result = collect(b, 12, source_codes=missing_codes)
                refresh()
            # If any error occurs before this write, the next visit may retry.
            record = dict(zip(HEADERS['Settings'], ['publisher_seed_v2', 'done',
                                                   '주요 언론사 최초 검색 완료', now()]))
            if not patch_record(b, 'Settings', '설정키', 'publisher_seed_v2', record):
                append_records(b, 'Settings', [record])
            refresh()
    # An existing installation needs the new automobile category and feed too.
    # Register only missing records; avoid reinitializing any existing rows.
    if not any(r.get('카테고리코드') == 'AUTO' for r in rows(b, 'Category')) or not any(
        r.get('언론사코드') == 'GOOGLE_AUTO' for r in rows(b, 'Media')
    ):
        initialize(b)
        refresh()
    settings = rows(b, 'Settings')
    if not any(r.get('설정키') == 'automobile_seed_v1' and r.get('설정값') == 'done' for r in settings):
        # Do not run a general collection: only the new automobile feed.
        auto_result = collect(b, 16, source_codes={'GOOGLE_AUTO'})
        result['sources'] += auto_result.get('sources', 0)
        result['new'] += auto_result.get('new', 0)
        result['errors'] += auto_result.get('errors', 0)
        rec = dict(zip(HEADERS['Settings'], ['automobile_seed_v1', 'done',
                                             '자동차 카테고리 최초 뉴스 수집', now()]))
        if not patch_record(b, 'Settings', '설정키', 'automobile_seed_v1', rec):
            append_records(b, 'Settings', [rec])
        refresh()
    # AI generation remains lazy and cached in the AI briefing view.
    return result


def publisher(item, known):
    keyword = str(item.get('키워드', ''))
    if keyword.startswith('출처:'):
        return keyword[3:].strip() or '출처 미상'
    return known.get(item.get('언론사코드'), item.get('언론사코드') or '출처 미상')


def favorite_names():
    """A shared favorite list backed by the existing Settings sheet."""
    entry = next((r for r in data('Settings') if r.get('설정키') == 'favorite_publishers'), None)
    try:
        val = json.loads((entry or {}).get('설정값') or '[]')
        return [str(x) for x in val if isinstance(x, str)] if isinstance(val, list) else []
    except (ValueError, TypeError):
        return []


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
            record = dict(zip(HEADERS['Settings'], [
                'favorite_publishers', json.dumps(chosen, ensure_ascii=False),
                '공통 즐겨찾기 언론사', now()]))
            if not patch_record(book(), 'Settings', '설정키', 'favorite_publishers', record):
                append_records(book(), 'Settings', [record])
            refresh()
            st.rerun()


def app_home(stats_placeholder=None):
    # Preserve the working first-run initialization, without a separate collection button.
    with st.spinner('뉴스를 불러오는 중…'):
        status = ensure_first_news()
    if status.get('errors') and not data('News'):
        st.warning('초기 뉴스 수집에 문제가 있습니다. 뉴스 목록은 계속 이용할 수 있습니다.')

    categories = [r for r in data('Category') if enabled(r.get('사용여부'))]
    def order(row):
        try:
            return int(row.get('정렬순서') or 999)
        except (TypeError, ValueError):
            return 999
    categories.sort(key=order)
    cmap = {r['카테고리코드']: r['카테고리명'] for r in categories}
    if not cmap:
        st.info('표시할 뉴스 분야가 없습니다.')
        return

    all_items = [r for r in data('News') if enabled(r.get('노출여부'))
                 and str(r.get('중복여부', 'N')).upper() != 'Y']
    all_items.sort(key=lambda r: str(r.get('발행일시', '')), reverse=True)
    known = {r['언론사코드']: r['언론사명'] for r in data('Media')}
    # Global collection status belongs in the compact upper-right header, not the article area.
    if stats_placeholder is not None:
        total_sources = len({publisher(r, known) for r in all_items})
        newest = max((str(r.get('수집일시', '')) for r in all_items), default='')
        stats_placeholder.markdown(
            f'<div class="header-stats">{len(all_items):,}개 기사 · {total_sources:,}개 언론사'
            f' · 마지막 수집 {esc(newest[:16]) or "기록 없음"}</div>',
            unsafe_allow_html=True)

    catcodes = list(cmap)
    old = st.session_state.get('news_category', 'ALL')
    if old not in catcodes:
        old = catcodes[0]
    # A single desktop toolbar; Streamlit stacks columns on narrow screens.
    tabcol, catcol, favcol, editcol, filtercol = st.columns([2.1, 2.2, 1.6, 1.2, .8], vertical_alignment='center', gap='small')
    with tabcol:
        if hasattr(st, 'segmented_control'):
            mode = st.segmented_control('뉴스 화면', ['최신 뉴스', '✨ AI 브리핑'],
                                        default='최신 뉴스', label_visibility='collapsed',
                                        key='news_view_mode') or '최신 뉴스'
        else:
            mode = st.radio('뉴스 화면', ['최신 뉴스', '✨ AI 브리핑'],
                            horizontal=True, label_visibility='collapsed', key='news_view_mode')
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
            st.text_input('기사 검색', placeholder='기사 제목·언론사 검색', key='news_search')
            st.toggle('RSS 설명 함께 표시', value=False, key='news_excerpts')
            st.select_slider('페이지당 기사', options=[20, 40, 60], value=40, key='news_limit')

    favorites = favorite_names() if favorite_mode == '⭐ 즐겨찾기' else []
    items = [r for r in all_items if category == 'ALL' or r.get('카테고리코드') == category]
    if favorite_mode == '⭐ 즐겨찾기':
        items = [r for r in items if publisher(r, known) in favorites]
    search = str(st.session_state.get('news_search', '')).strip().casefold()
    if search:
        items = [r for r in items if search in (str(r.get('제목', '')) + ' ' + publisher(r, known)).casefold()]

    if mode == '✨ AI 브리핑':
        # Opening the AI tab uses the stored briefing if newer than 6 hours.
        # Only a stale/missing category triggers one free AI request.
        existing = latest_brief(category)
        if not briefing_is_fresh(existing):
            # Round-robin across categories for a broader briefing than the 30 newest articles.
            if category == 'ALL':
                buckets = {}
                for item in items:
                    buckets.setdefault(str(item.get('카테고리코드', 'ALL')), []).append(item)
                balanced = []
                while len(balanced) < 30 and any(buckets.values()):
                    for group in buckets.values():
                        if group and len(balanced) < 30:
                            balanced.append(group.pop(0))
                input_items = balanced
            else:
                input_items = items[:30]
            message = auto_briefing(category, input_items)
            if message:
                st.caption(message)
        brief = latest_brief(category)
        if not brief and category != 'ALL':
            brief = latest_brief('ALL')
        if not brief:
            st.markdown('<div class="empty-ai"><strong>아직 생성된 AI 브리핑이 없습니다.</strong>'
                        '<p>무료 AI가 사용 가능해지면 이 화면에서 자동 생성됩니다. 기사 목록은 정상 이용할 수 있습니다.</p></div>',
                        unsafe_allow_html=True)
            return
        st.markdown('<div class="summary-hero"><h3>' + esc(brief.get('브리핑제목')) +
                    '</h3><p>' + esc(brief.get('핵심요약')) + '</p></div>', unsafe_allow_html=True)
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
                url = safe_url(r.get('원문URL', ''))
                if url:
                    links.append(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">'
                                 f'{esc(publisher(r, known))} ↗</a>')
            cards.append('<article class="issue-card"><div class="issue-number">ISSUE '
                         + str(i).zfill(2) + '</div><div class="issue-title">'
                         + esc(issue.get('headline')) + '</div><p class="issue-point">'
                         + esc(issue.get('point')) + '</p><div class="issue-links">'
                         + ' '.join(links) + '</div></article>')
        st.markdown('<div class="issue-grid">' + ''.join(cards) + '</div>', unsafe_allow_html=True)
        st.caption('OpenRouter 무료 AI 작성 · 최근 브리핑 재사용(최대 6시간) · RSS 제목과 설명만 분석 · 기사 본문 전체 검증이 아닙니다 · '
                   + str(brief.get('생성일시', ''))[:16])
        return

    if not items:
        if favorite_mode == '⭐ 즐겨찾기' and not favorites:
            st.info('상단 ☆ 언론사 선택에서 즐겨찾기를 지정해 주세요.')
        else:
            st.info('선택한 조건의 기사가 없습니다. 분야나 검색 조건을 변경해 주세요.')
        return

    limit = int(st.session_state.get('news_limit', 40))
    pages = max(1, (len(items) + limit - 1) // limit)
    if st.session_state.get('news_page', 1) > pages:
        st.session_state.news_page = 1
    page = int(st.session_state.get('news_page', 1))
    excerpts = bool(st.session_state.get('news_excerpts', False))
    tiles = []
    for r in items[(page - 1) * limit:page * limit]:
        url = safe_url(r.get('원문URL', ''))
        display_title = str(r.get('제목', ''))
        actual_publisher = publisher(r, known)
        if display_title.endswith(' - ' + actual_publisher):
            display_title = display_title[:-(len(actual_publisher) + 3)]
        title = esc(display_title)
        name = esc(actual_publisher)
        cat = esc(cmap.get(r.get('카테고리코드', ''), ''))
        published = esc(str(r.get('발행일시', ''))[5:16])
        title_html = (f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title} ↗</a>'
                      if url else title)
        desc = (f'<div class="news-desc">{esc(str(r.get("요약", ""))[:220])}</div>'
                if excerpts and r.get('요약') else '')
        tiles.append('<article class="news-tile"><div class="news-meta"><span class="news-source">'
                     + name + '</span><span class="news-category">' + cat
                     + '</span><span>' + published + '</span></div><div class="news-title">'
                     + title_html + '</div>' + desc + '</article>')
    st.markdown('<div class="news-grid">' + ''.join(tiles) + '</div>', unsafe_allow_html=True)
    if pages > 1:
        st.number_input('페이지 이동', min_value=1, max_value=pages, step=1, key='news_page')
    st.caption('기사 클릭 시 해당 매체 원문으로 이동합니다. 기사 저작권은 해당 언론사에 있습니다.')


def main():
    brandcol, statscol, fontcol = st.columns([4.5, 4.8, .7],
                                             vertical_alignment='center', gap='small')
    with brandcol:
        st.markdown('<div class="brand"><span class="brand-mark">N</span> 간추린 뉴스'
                    '<span class="brand-small">NEWS BRIEF</span></div>', unsafe_allow_html=True)
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
