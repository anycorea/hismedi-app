"""News Brief: readable news + RSS-grounded AI briefings, Google Sheets backend."""
import hashlib
import hmac
import html
import json
from datetime import datetime

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


def refresh():
    data.clear()
    sheet_problems.clear()


def logged_in():
    return st.session_state.get('news_admin', False)


def write_log(action, sheet, key='', detail=''):
    record = dict(zip(HEADERS['Admin_Log'], [
        hashlib.sha256((now()+action+str(key)).encode()).hexdigest()[:24],
        now(), 'MASTER', action, sheet, str(key), str(detail)[:350]
    ]))
    try:
        append_records(book(), 'Admin_Log', [record])
    except Exception:
        # An audit logging failure must not falsely report that a completed
        # primary write failed and encourage duplicate submissions.
        st.warning('변경은 처리되었으나 관리자 작업 기록 저장에 실패했습니다.')


def publisher(item, known):
    keyword = str(item.get('키워드', ''))
    if keyword.startswith('출처:'):
        return keyword[3:].strip() or '출처 미상'
    return known.get(item.get('언론사코드'), item.get('언론사코드') or '출처 미상')


def news_card(item, known, compact=False):
    title = esc(item.get('제목'))
    if not title:
        return
    original = safe_url(item.get('원문URL', ''))
    link = (f'<a href="{esc(original)}" target="_blank" '
            f'rel="noopener noreferrer">{title} ↗</a>') if original else title
    source = esc(publisher(item, known))
    when = esc(str(item.get('발행일시', ''))[:16])
    summary = esc(str(item.get('요약', '')).strip()[:280])
    short = '' if compact or not summary else f'<div class="summary">{summary}</div>'
    st.markdown(f'<article class="news-card"><div class="meta">{source} · {when}</div>'
                f'<div class="headline">{link}</div>{short}</article>',
                unsafe_allow_html=True)


def handle_error(exc):
    st.error('데이터 처리 오류가 발생했습니다.')
    if logged_in():
        st.code(f'{type(exc).__name__}: {exc}', language='text')
        if st.checkbox('상세 오류 보기', key='show_diagnostics'):
            st.exception(exc)
    else:
        st.caption('관리자 화면에서 오류 내용을 확인할 수 있습니다.')


def start_data():
    st.caption('초기 소스 등록과 첫 기사 수집은 뉴스 화면 접속 시 자동으로 진행됩니다.')
    items = data('Category')
    media = data('Media')
    articles = data('News')
    left, middle, right = st.columns(3)
    left.metric('카테고리', len(items))
    middle.metric('뉴스 소스', len(media))
    right.metric('저장된 뉴스', len(articles))
    st.caption('기본 뉴스가 보인 뒤 정기 수집과 별도 수집 기능은 마지막 단계에 설정합니다.')


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
    # AI summaries are optional and require a configured API key.
    api_key = setting('news_brief', 'gemini_api_key', '')
    if api_key and not rows(b, 'Briefing'):
        articles = [x for x in rows(b, 'News') if enabled(x.get('노출여부'))]
        articles.sort(key=lambda x: str(x.get('발행일시','')), reverse=True)
        if articles:
            try:
                briefing = ai_briefing(articles, api_key=api_key,
                     model=setting('news_brief','gemini_model','gemini-2.5-flash'))
                save_briefing(b, briefing, 'ALL')
                refresh()
            except Exception:
                # News should remain readable if the optional AI service fails.
                pass
    return result


def manage_media():
    media = data('Media')
    if media:
        st.dataframe(media, hide_index=True, use_container_width=True)
    selection = st.selectbox('편집할 소스', ['새로 등록'] + [r['언론사코드'] for r in media])
    old = next((r for r in media if r['언론사코드'] == selection), {})
    with st.form('media_form'):
        code = st.text_input('언론사코드', old.get('언론사코드', ''))
        name = st.text_input('언론사명', old.get('언론사명', ''))
        group = st.text_input('언론사분류', old.get('언론사분류', '종합'))
        homepage = st.text_input('홈페이지 URL', old.get('홈페이지URL', ''))
        rss = st.text_input('RSS 주소', old.get('RSS_URL', ''))
        cats = [r['카테고리코드'] for r in data('Category')]
        previous = old.get('비고', 'ALL')
        category = st.selectbox('기본 카테고리', cats or ['ALL'], index=(cats.index(previous) if previous in cats else 0))
        on = st.checkbox('사용', value=enabled(old.get('사용여부', 'Y')))
        submit = st.form_submit_button('저장', type='primary')
    if submit:
        code = code.strip()
        name = name.strip()
        if not code or not name or not safe_url(rss):
            st.error('언론사코드, 언론사명 및 올바른 RSS 주소가 필요합니다.')
            return
        if selection != '새로 등록' and selection != code:
            st.error('기존 언론사코드는 변경할 수 없습니다.')
            return
        if selection == '새로 등록' and any(r['언론사코드'] == code for r in media):
            st.error('중복 언론사코드입니다.')
            return
        record = dict(zip(HEADERS['Media'], [
            code, name, group, homepage, rss, old.get('로고URL', ''), 'RSS',
            old.get('수집주기분', '30'), old.get('정렬순서', str((len(media)+1)*10)),
            'Y' if on else 'N', category
        ]))
        if selection == '새로 등록':
            append_records(book(), 'Media', [record])
        else:
            patch_record(book(), 'Media', '언론사코드', selection, record)
        refresh()
        write_log('저장', 'Media', code, name)
        st.success('저장했습니다.')


def favorite_names():
    """Shared editorial favorites in existing Settings sheet; no new columns."""
    row = next((r for r in data('Settings') if r.get('설정키') == 'favorite_publishers'), None)
    if not row:
        return []
    try:
        value = json.loads(row.get('설정값') or '[]')
        return [str(n) for n in value if isinstance(n, str)] if isinstance(value, list) else []
    except (ValueError, TypeError):
        return []


def manage_favorites():
    """Admin-curated publisher favorites; actual RSS article publishers, not feed groups."""
    articles = data('News')
    known = {r['언론사코드']: r['언론사명'] for r in data('Media')}
    names = sorted({publisher(item, known) for item in articles if enabled(item.get('노출여부'))})
    saved = favorite_names()
    st.caption('기사에 표시된 실제 언론사를 선택합니다. 수집용 RSS 소스와는 별도로 관리됩니다.')
    with st.form('favorites_form'):
        chosen = st.multiselect('즐겨찾기 언론사', sorted(set(names + saved)), default=saved,
                                placeholder='언론사 검색 후 여러 곳 선택')
        submitted = st.form_submit_button('⭐ 즐겨찾기 저장', type='primary')
    if submitted:
        rec = dict(zip(HEADERS['Settings'], ['favorite_publishers', json.dumps(chosen, ensure_ascii=False),
                                              '뉴스 화면 공동 즐겨찾기 언론사', now()]))
        if not patch_record(book(), 'Settings', '설정키', 'favorite_publishers', rec):
            append_records(book(), 'Settings', [rec])
        refresh()
        write_log('즐겨찾기', 'Settings', 'favorite_publishers', f'{len(chosen)}개 언론사')
        st.success(f'언론사 {len(chosen)}곳의 즐겨찾기를 저장했습니다.')
        st.rerun()
    st.caption('저장된 즐겨찾기는 모든 기기에서 공통으로 표시됩니다. 개인별 계정 즐겨찾기는 아직 제공하지 않습니다.')


def manage_categories():
    categories = data('Category')
    if categories:
        st.dataframe(categories, hide_index=True, use_container_width=True)
    selection = st.selectbox('편집할 카테고리', ['새로 등록'] + [r['카테고리코드'] for r in categories])
    old = next((r for r in categories if r['카테고리코드'] == selection), {})
    with st.form('cat_form'):
        code = st.text_input('카테고리코드', old.get('카테고리코드', ''))
        name = st.text_input('카테고리명', old.get('카테고리명', ''))
        order = st.number_input('정렬순서', 0, 9999, int(old.get('정렬순서') or 100), step=10)
        on = st.checkbox('사용', value=enabled(old.get('사용여부', 'Y')))
        submit = st.form_submit_button('저장', type='primary')
    if submit:
        code = code.strip()
        name = name.strip()
        if not code or not name:
            st.error('코드와 이름이 필요합니다.')
            return
        if selection != '새로 등록' and selection != code:
            st.error('기존 카테고리코드는 변경할 수 없습니다.')
            return
        if selection == '새로 등록' and any(r['카테고리코드'] == code for r in categories):
            st.error('중복 카테고리코드입니다.')
            return
        record = dict(zip(HEADERS['Category'], [
            code, name, old.get('상위카테고리', ''), order,
            'Y' if on else 'N', old.get('비고', '')
        ]))
        if selection == '새로 등록':
            append_records(book(), 'Category', [record])
        else:
            patch_record(book(), 'Category', '카테고리코드', selection, record)
        refresh()
        write_log('저장', 'Category', code, name)
        st.success('저장했습니다.')


def manual_collection():
    st.info('별도 수집 버튼과 GitHub 정기 수집은 뉴스 화면 검수 후 마지막 단계에 연결합니다.')
    st.caption('첫 기사 수집은 뉴스 화면에 접속하면 자동 진행됩니다.')


def make_brief():
    st.caption('RSS에서 제공된 제목·요약에 근거합니다. 기사 본문을 검증한 것으로 표현하지 않습니다.')
    token = setting('news_brief', 'gemini_api_key', '')
    model = setting('news_brief', 'gemini_model', 'gemini-2.5-flash')
    if not token:
        st.info('AI 브리핑을 사용하려면 Streamlit Secrets의 [news_brief]에 gemini_api_key를 등록하세요. OpenAI API 키는 사용하지 않습니다.')
    cats = [r for r in data('Category') if enabled(r.get('사용여부'))]
    selected = st.selectbox('브리핑 분야', [r['카테고리코드'] for r in cats] or ['ALL'])
    if st.button('AI 브리핑 생성·저장', type='primary', disabled=not bool(token)):
        articles = [r for r in data('News') if enabled(r.get('노출여부'))
                    and (selected == 'ALL' or r.get('카테고리코드') == selected)]
        articles.sort(key=lambda r: str(r.get('발행일시', '')), reverse=True)
        with st.spinner('핵심 이슈를 정리하고 있습니다'):
            result = ai_briefing(articles, api_key=token, model=model)
            save_briefing(book(), result, selected)
        refresh()
        write_log('AI브리핑', 'Briefing', selected, result['title'])
        st.success('Gemini 브리핑 생성·저장 완료')
        st.json(result)


def settings_panel():
    st.caption('설정은 앱에서 관리하고 구글시트의 서식·헤더는 변경하지 않습니다.')
    with st.form('setting_form'):
        key = st.text_input('설정키')
        value = st.text_input('설정값')
        description = st.text_input('설명')
        submitted = st.form_submit_button('설정 저장')
    if submitted and key.strip():
        record = dict(zip(HEADERS['Settings'], [key.strip(), value, description, now()]))
        if not patch_record(book(), 'Settings', '설정키', key.strip(), record):
            append_records(book(), 'Settings', [record])
        refresh()
        write_log('설정', 'Settings', key, description)
        st.success('설정을 저장했습니다.')
    selected = st.selectbox('기록 조회', ['Collection_Log', 'Settings', 'Admin_Log'])
    st.dataframe(data(selected)[-50:], hide_index=True, use_container_width=True)


def app_admin():
    password = str(setting('news_brief', 'admin_password', ''))
    if not password:
        st.warning('Streamlit Secrets의 [news_brief] admin_password를 설정해야 관리자 기능을 사용할 수 있습니다.')
        return
    if not logged_in():
        with st.form('login'):
            entered = st.text_input('관리자 비밀번호', type='password')
            submit = st.form_submit_button('로그인', type='primary')
        if submit:
            if hmac.compare_digest(entered, password):
                st.session_state.news_admin = True
                st.rerun()
            else:
                st.error('비밀번호가 일치하지 않습니다.')
        return
    left, right = st.columns([5, 1], vertical_alignment='center')
    left.markdown('**관리자 센터**')
    if right.button('로그아웃'):
        st.session_state.news_admin = False
        st.rerun()
    # st.tabs evaluates ALL panels on every rerun. A single selector avoids
    # numerous Google Sheets API reads, quota errors and lengthy loading.
    sections = ['상태·초기화', '⭐ 언론사 즐겨찾기', '언론사 관리', '카테고리 관리', '뉴스 수집', 'AI 브리핑', '설정·기록']
    section = st.selectbox('관리 메뉴', sections, label_visibility='collapsed')
    if section == '상태·초기화':
        problems = sheet_problems()
        if problems:
            st.error('헤더 확인 필요: ' + ' / '.join(problems))
        else:
            st.success('8개 시트 헤더 정상 · 서식 변경 없음')
            start_data()
    elif section == '⭐ 언론사 즐겨찾기':
        manage_favorites()
    elif section == '언론사 관리':
        manage_media()
    elif section == '카테고리 관리':
        manage_categories()
    elif section == '뉴스 수집':
        manual_collection()
    elif section == 'AI 브리핑':
        make_brief()
    elif section == '설정·기록':
        settings_panel()


def app_home(stats_placeholder=None):
    # Preserve the working first-run initialization, without a separate collection button.
    with st.spinner('뉴스를 불러오는 중…'):
        status = ensure_first_news()
    if status.get('errors') and not data('News'):
        st.warning('기사 수집에 문제가 있습니다. 관리자 기록을 확인해 주세요.')

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
    tabcol, catcol, favcol, filtercol = st.columns([2.0, 2.4, 1.75, .85], vertical_alignment='center', gap='small')
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
        # AI API calls remain manual/controlled. Display only genuinely generated briefings.
        recent = [r for r in data('Briefing') if enabled(r.get('노출여부'))
                  and (r.get('카테고리코드') == category or
                       (category != 'ALL' and r.get('카테고리코드') == 'ALL'))]
        recent.sort(key=lambda r: str(r.get('생성일시', '')), reverse=True)
        if not recent:
            st.markdown('<div class="empty-ai"><strong>아직 생성된 AI 브리핑이 없습니다.</strong>'
                        '<p>관리자 → AI 브리핑에서 생성하면 이곳에만 표시됩니다. '
                        '헤드라인을 AI 요약인 것처럼 반복하지 않습니다.</p></div>',
                        unsafe_allow_html=True)
            return
        brief = recent[0]
        st.markdown('<div class="summary-hero"><h3>' + esc(brief.get('브리핑제목')) +
                    '</h3><p>' + esc(brief.get('핵심요약')) + '</p></div>', unsafe_allow_html=True)
        try:
            issues = json.loads(brief.get('주요이슈') or '[]')
        except (ValueError, TypeError):
            issues = []
        idmap = {str(r.get('뉴스ID')): r for r in all_items}
        cards = []
        for i, issue in enumerate(issues[:7], 1):
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
        st.caption('Gemini AI 작성 · RSS 제목과 설명만 분석 · 기사 본문 전체 검증이 아닙니다 · '
                   + str(brief.get('생성일시', ''))[:16])
        return

    if not items:
        if favorite_mode == '⭐ 즐겨찾기' and not favorites:
            st.info('관리자 → ⭐ 언론사 즐겨찾기에서 관심 언론사를 지정해 주세요.')
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
    # Keep all always-visible controls in one row on wide screens.
    brandcol, statscol, fontcol, viewcol = st.columns([3.25, 3.8, .55, 1.3],
                                                    vertical_alignment='center', gap='small')
    with brandcol:
        st.markdown('<div class="brand"><span class="brand-mark">N</span> 간추린 뉴스'
                    '<span class="brand-small">NEWS BRIEF</span></div>', unsafe_allow_html=True)
    with statscol:
        stats_placeholder = st.empty()
    with fontcol:
        with st.popover('Aa', help='글자 크기 설정', use_container_width=True):
            font = st.radio('글자 크기', ['기본', '크게', '아주 크게'], horizontal=False)
            if st.button('새로고침'):
                refresh()
                st.rerun()
    with viewcol:
        view = st.selectbox('화면', ['뉴스', '관리자'], label_visibility='collapsed')
    sizes = {'기본': '1.03rem', '크게': '1.2rem', '아주 크게': '1.42rem'}
    st.markdown('<style>:root{--news-title-size:' + sizes[font] + '}</style>',
                unsafe_allow_html=True)
    try:
        if view == '관리자':
            app_admin()
        else:
            app_home(stats_placeholder)
    except Exception as exc:
        handle_error(exc)
    st.markdown('<div class="anycorea">Created by anycorea</div>', unsafe_allow_html=True)


if __name__ == '__main__':
    main()
