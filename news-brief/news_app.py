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
/* Use Streamlit default top spacing; do not override viewport zoom. */
.stApp{background:#f8fafc;color:#17263a}
.block-container{max-width:1320px}
header[data-testid="stHeader"]{background:#f8fafc}
#MainMenu,footer,div[data-testid="stDecoration"]{visibility:hidden}
.stButton button,.stLinkButton a{min-height:44px;border-radius:10px}
.news-card{background:#fff;border:1px solid #dce5ee;border-radius:12px;
 padding:1rem 1.15rem;margin:.52rem 0;overflow-wrap:anywhere}
.news-card .meta{font-size:.86rem;color:#52677b;margin-bottom:.38rem}
.news-card .headline{font-size:var(--news-title-size,1.19rem);font-weight:750;line-height:1.55}
.news-card .summary{font-size:var(--news-body-size,1rem);color:#40536b;line-height:1.65;margin-top:.35rem}
.news-card a{color:#142e4c;text-decoration:none}
.news-card a:hover{text-decoration:underline;color:#1761a4}
.brief-box{border:1px solid #d3e2f5;border-radius:12px;background:#eef5ff;
 padding:1rem 1.1rem;line-height:1.68;color:#233e5b;margin:.6rem 0}
.brief-box .brief-title{font-weight:750;font-size:1.05rem}
.brief-box .brief-note{color:#64748b;font-size:.82rem;margin-top:.4rem}
.anycorea{text-align:center;color:#708198;font-size:.73rem;padding:1rem 0}
.brand{font-weight:800;font-size:1.2rem;line-height:1.5;color:#172e4b;padding:.1rem 0 .3rem}
@media(max-width:800px){
 .block-container{padding-left:.8rem;padding-right:.8rem}
 .news-card{padding:.85rem .95rem}
 .news-card .headline{font-size:max(var(--news-title-size,1.19rem),1.13rem)}
 .brand{font-size:1.09rem}
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
    # AI summaries are optional and require a configured API key.
    api_key = setting('news_brief', 'openai_api_key', '')
    if api_key and not rows(b, 'Briefing'):
        articles = [x for x in rows(b, 'News') if enabled(x.get('노출여부'))]
        articles.sort(key=lambda x: str(x.get('발행일시','')), reverse=True)
        if articles:
            try:
                briefing = ai_briefing(articles, api_key=api_key,
                     model=setting('news_brief','ai_model','gpt-4.1-mini'))
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
    token = setting('news_brief', 'openai_api_key', '')
    model = setting('news_brief', 'ai_model', 'gpt-4.1-mini')
    if not token:
        st.info('AI 기능을 쓰려면 이 앱의 Streamlit Secrets에 [news_brief] openai_api_key를 등록하세요.')
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
        st.success('AI 브리핑 생성 완료')
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
    sections = ['상태·초기화', '언론사 관리', '카테고리 관리', '뉴스 수집', 'AI 브리핑', '설정·기록']
    section = st.selectbox('관리 메뉴', sections, label_visibility='collapsed')
    if section == '상태·초기화':
        problems = sheet_problems()
        if problems:
            st.error('헤더 확인 필요: ' + ' / '.join(problems))
        else:
            st.success('8개 시트 헤더 정상 · 서식 변경 없음')
            start_data()
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


def app_home():
    # Load actual articles on first visit; no initial button needed.
    with st.spinner('첫 뉴스 데이터를 준비하고 있습니다…'):
        outcome = ensure_first_news()
    if outcome.get('errors') and outcome.get('new') == 0:
        st.warning('일부 뉴스 소스에 접속하지 못했습니다. 관리자 기록에서 원인을 확인할 수 있습니다.')
    categories = [r for r in data('Category') if enabled(r.get('사용여부'))]
    categories.sort(key=lambda r: int(r.get('정렬순서') or 999))
    cmap = {r['카테고리코드']: r['카테고리명'] for r in categories}
    if not categories:
        st.info('아직 뉴스 카테고리가 없습니다. 관리자에서 기본 데이터 등록을 실행하세요.')
        return
    cat_options = list(cmap)
    default = st.session_state.get('news_category', 'ALL')
    if default not in cmap:
        default = cat_options[0]
    header_left, header_right = st.columns([3, 1], vertical_alignment='bottom')
    with header_left:
        selected = st.selectbox('뉴스 분야', cat_options,
                                index=cat_options.index(default),
                                format_func=lambda c: cmap[c],
                                label_visibility='collapsed')
        st.session_state.news_category = selected
    with header_right:
        if st.button('☰ 검색·필터', use_container_width=True):
            st.session_state.news_filters = not st.session_state.get('news_filters', False)
    search, source, no_dup, compact, amount = '', '전체', True, False, 20
    known = {r['언론사코드']: r['언론사명'] for r in data('Media')}
    if st.session_state.get('news_filters', False):
        with st.container(border=True):
            search = st.text_input('기사 검색', placeholder='제목·기사 요약·언론사')
            source_rows = data('News')
            available = sorted({publisher(r, known) for r in source_rows})
            source = st.selectbox('언론사', ['전체'] + available)
            a, b = st.columns(2)
            no_dup = a.checkbox('중복 숨김', True)
            compact = b.checkbox('제목만 보기', False)
            amount = st.select_slider('페이지당 기사', [10, 20, 30, 50], value=20)

    briefs = [r for r in data('Briefing') if enabled(r.get('노출여부')) and
              r.get('카테고리코드') == selected]
    briefs.sort(key=lambda r: str(r.get('생성일시', '')), reverse=True)
    if briefs:
        latest = briefs[0]
        st.markdown(f'<div class="brief-box"><div class="brief-title">AI 핵심 브리핑 · {esc(cmap[selected])}</div>'
                    f'<strong>{esc(latest.get("브리핑제목"))}</strong><div>{esc(latest.get("핵심요약"))}</div>'
                    f'<div class="brief-note">{esc(latest.get("생성일시"))} · RSS 제목·요약 기반</div></div>',
                    unsafe_allow_html=True)
        try:
            issues = json.loads(latest.get('주요이슈') or '[]')
        except (ValueError, TypeError):
            issues = []
        for index, issue in enumerate(issues, 1):
            with st.expander(f'{index}. {issue.get("headline", "주요 이슈")}'):
                st.write(issue.get('point', ''))
                articles = {r['뉴스ID']: r for r in data('News')}
                for article_id in issue.get('ids', []):
                    record = articles.get(article_id)
                    if record and safe_url(record.get('원문URL', '')):
                        st.link_button(str(record.get('제목', '기사 보기'))[:75], record['원문URL'])
    elif selected == 'ALL':
        st.caption('AI 브리핑은 API 키가 설정되면 생성됩니다. 아래의 기사는 실제 RSS 수집 결과입니다.')
    st.markdown(f'**{esc(cmap[selected])} 최신 뉴스**')
    news = [r for r in data('News') if enabled(r.get('노출여부'))
            and (selected == 'ALL' or r.get('카테고리코드') == selected)]
    if no_dup:
        news = [r for r in news if str(r.get('중복여부', 'N')).upper() != 'Y']
    if source != '전체':
        news = [r for r in news if publisher(r, known) == source]
    if search.strip():
        q = search.strip().casefold()
        news = [r for r in news if q in ' '.join([
            str(r.get('제목', '')), str(r.get('요약', '')), publisher(r, known)]).casefold()]
    news.sort(key=lambda r: str(r.get('발행일시', '')), reverse=True)
    if not news:
        st.info('표시할 뉴스가 없습니다. 관리자에서 뉴스 수집을 실행하거나 필터를 확인하세요.')
        return
    pages = max(1, (len(news) + amount - 1) // amount)
    page = st.number_input(f'{len(news):,}건 · 페이지', 1, pages, value=1, step=1)
    for item in news[(page-1)*amount:page*amount]:
        news_card(item, known, compact)
    st.caption(f'{page}/{pages} 페이지 · 제목을 누르면 기사 제공 링크로 이동합니다.')


def main():
    left, right = st.columns([4, 2], vertical_alignment='center')
    with left:
        st.markdown('<div class="brand">📰 News Brief <span style="font-size:.78rem;font-weight:500;color:#718096">AI 뉴스브리핑</span></div>', unsafe_allow_html=True)
    with right:
        view = st.selectbox('화면', ['뉴스', '관리자'], label_visibility='collapsed')
    # Compact options: readable news first, settings only when requested.
    with st.popover('Aa · 화면 설정'):
        font = st.radio('글자 크기', ['기본', '크게', '아주 크게'], horizontal=True)
        if st.button('새로고침'):
            refresh()
            st.rerun()
    titles = {'기본': ('1.18rem', '1rem'), '크게': ('1.45rem', '1.13rem'),
              '아주 크게': ('1.73rem', '1.3rem')}
    tsize, bsize = titles[font]
    st.markdown(f'<style>:root{{--news-title-size:{tsize};--news-body-size:{bsize}}}</style>', unsafe_allow_html=True)
    try:
        if view == '관리자':
            app_admin()
        else:
            app_home()
    except Exception as exc:
        handle_error(exc)
    st.markdown('<div class="anycorea">Created by anycorea · 기사 저작권은 각 언론사에 있습니다.</div>',
                unsafe_allow_html=True)


if __name__ == '__main__':
    main()
