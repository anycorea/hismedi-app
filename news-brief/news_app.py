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
/* Preserve Streamlit's standard document top spacing and native zoom. */
.stApp{background:#f7f9fc;color:#13243a}
.block-container{max-width:1450px}
header[data-testid="stHeader"]{background:#f7f9fc}
#MainMenu,footer,div[data-testid="stDecoration"]{visibility:hidden}
.stButton button,.stLinkButton a{min-height:42px;border-radius:10px}
.brand{font-weight:850;font-size:1.34rem;letter-spacing:-.035em;color:#132c47}
.brand span{font-size:.75rem;font-weight:600;letter-spacing:0;color:#708196;margin-left:.4rem}
.section-head{font-size:1.08rem;font-weight:800;color:#102e4a;margin:.65rem 0 .45rem}
.headline-row{display:flex;gap:12px;align-items:flex-start;border-bottom:1px solid #e7edf3;padding:12px 4px;line-height:1.55}
.headline-row:hover{background:#f1f7f5}
.media-badge{flex:0 0 96px;text-align:center;border-radius:5px;background:#e5f6eb;color:#168149;font-size:.78rem;font-weight:750;padding:4px 6px;overflow-wrap:anywhere}
.headline-main{flex:1;min-width:0}
.headline-main a{font-size:var(--news-title-size,1.07rem);line-height:1.52;font-weight:680;color:#152c43;text-decoration:none;overflow-wrap:anywhere}
.headline-main a:hover{color:#13804b;text-decoration:underline}
.headline-time{font-size:.78rem;color:#77889b;margin-top:3px}
.headline-summary{font-size:.88rem;color:#516579;margin-top:5px;line-height:1.5}
.panel{background:white;border:1px solid #e3eaf1;border-radius:12px;padding:14px 16px;margin-bottom:12px}
.issue{padding:11px 0;border-bottom:1px solid #e9edf2}
.issue:last-child{border:0}
.issue .num{font-size:.82rem;color:#148d58;font-weight:800;vertical-align:top;margin-right:8px}
.issue a{font-weight:650;font-size:.98rem;color:#21374d;line-height:1.52;text-decoration:none}
.issue a:hover{text-decoration:underline;color:#168149}
.issue .source{font-size:.78rem;color:#7b8996;margin:4px 0 0 23px}
.notice{font-size:.79rem;color:#6c7c8a;line-height:1.5}
.anycorea{text-align:center;color:#8793a0;font-size:.73rem;padding:1rem 0}
@media(max-width:800px){
 .block-container{padding-left:.65rem;padding-right:.65rem}
 .headline-row{padding:12px 1px;gap:8px}
 .media-badge{flex-basis:65px;font-size:.7rem;padding:4px 3px}
 .headline-main a{font-size:max(var(--news-title-size,1.07rem),1.02rem)}
 .panel{padding:11px 12px}
 .brand{font-size:1.15rem}
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


def app_home():
    # News can be used immediately without running a separate collection command.
    with st.spinner('뉴스를 불러오는 중…'):
        status = ensure_first_news()
    if status.get('errors') and not data('News'):
        st.warning('뉴스 RSS 접속에 실패했습니다. 나중에 다시 접속하거나 관리자 기록을 확인하세요.')

    categories = [r for r in data('Category') if enabled(r.get('사용여부'))]
    def order(row):
        try: return int(row.get('정렬순서') or 999)
        except (TypeError, ValueError): return 999
    categories.sort(key=order)
    cmap = {r['카테고리코드']: r['카테고리명'] for r in categories}
    if not cmap:
        st.info('표시할 카테고리가 없습니다.'); return
    catcodes = list(cmap)
    old = st.session_state.get('news_category', 'ALL')
    if old not in catcodes: old = catcodes[0]
    topcat, tools = st.columns([5, 1], vertical_alignment='bottom')
    with topcat:
        category = st.selectbox('분야', catcodes, index=catcodes.index(old),
                                format_func=lambda c:cmap[c], label_visibility='collapsed')
        st.session_state.news_category = category
    with tools:
        with st.popover('☰ 필터', use_container_width=True):
            search = st.text_input('기사 검색', placeholder='제목 또는 언론사', key='news_search')
            show_excerpt = st.toggle('RSS 설명 표시', value=False, key='news_excerpts')
            limit = st.select_slider('한 페이지 기사 수', options=[15, 30, 50], value=30,
                                     key='news_limit')
            st.caption('기본 화면은 뉴스를 읽는 데 집중하도록 구성했습니다.')
    # Keep filtering outside the expander for one-tap access on all devices.
    favorite_mode = st.radio('언론사 보기', ['전체 언론사', '⭐ 즐겨찾기'],
                             horizontal=True, label_visibility='collapsed',
                             key='news_favorite_mode')
    saved_favorites = favorite_names() if favorite_mode == '⭐ 즐겨찾기' else []
    search = st.session_state.get('news_search', '')
    show_excerpt = st.session_state.get('news_excerpts', False)
    limit = st.session_state.get('news_limit', 30)

    all_items = [x for x in data('News') if enabled(x.get('노출여부'))
                 and str(x.get('중복여부','N')).upper() != 'Y']
    known = {r['언론사코드']:r['언론사명'] for r in data('Media')}
    all_items.sort(key=lambda x:str(x.get('발행일시','')),reverse=True)
    items = [r for r in all_items if category == 'ALL' or r.get('카테고리코드') == category]
    if favorite_mode == '⭐ 즐겨찾기':
        items = [r for r in items if publisher(r, known) in saved_favorites]
    if search.strip():
        q=search.strip().casefold()
        items=[r for r in items if q in (str(r.get('제목',''))+' '+publisher(r,known)).casefold()]
    unique_sources = len({publisher(r,known) for r in items})
    last_updated = max((r.get('수집일시','') for r in all_items), default='')
    st.caption(f'기사 {len(items):,}건 · 언론사 {unique_sources:,}곳 · 마지막 수집 {last_updated[:16] or "기록 없음"}')
    if not items:
        if favorite_mode == '⭐ 즐겨찾기' and not saved_favorites:
            st.info('즐겨찾기 언론사가 아직 없습니다. 관리자 → ⭐ 언론사 즐겨찾기에서 먼저 선택해 주세요.')
        else:
            st.info('선택한 조건의 뉴스가 없습니다. 분야·검색어 또는 즐겨찾기를 변경해 보세요.')
        return

    # Digest is deterministic editorial selection from exact article headlines.
    # It is not labelled as AI, nor does it invent unseen article-body facts.
    brief_rows = [r for r in data('Briefing') if enabled(r.get('노출여부'))
                  and (r.get('카테고리코드') == category or (category != 'ALL' and r.get('카테고리코드') == 'ALL'))]
    brief_rows.sort(key=lambda x:str(x.get('생성일시','')),reverse=True)
    left,right=st.columns([1.75,1],gap='large')
    with left:
        st.markdown(f'<div class="section-head">📰 {esc(cmap.get(category,"전체"))} 헤드라인</div>',unsafe_allow_html=True)
        pages=max(1,(len(items)+limit-1)//limit)
        if st.session_state.get('news_page',1)>pages:st.session_state.news_page=1
        page=st.number_input('페이지',min_value=1,max_value=pages,value=1,step=1,key='news_page',label_visibility='collapsed') if pages>1 else 1
        lines=[]
        for item in items[(page-1)*limit:page*limit]:
            url=safe_url(item.get('원문URL',''))
            title=esc(item.get('제목',''))
            name=esc(publisher(item,known))
            when=esc(str(item.get('발행일시',''))[5:16])
            tag=(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title}</a>' if url else title)
            snippet=(f'<div class="headline-summary">{esc(str(item.get("요약", ""))[:200])}</div>'
                     if show_excerpt and item.get('요약') else '')
            lines.append(f'<div class="headline-row"><span class="media-badge">{name}</span>'
                         f'<div class="headline-main">{tag}<div class="headline-time">{when}</div>{snippet}</div></div>')
        st.markdown('<div class="panel">'+''.join(lines)+'</div>',unsafe_allow_html=True)
        if pages>1:st.caption(f'{page} / {pages} 페이지')
    with right:
        st.markdown('<div class="section-head">✨ AI 브리핑 · 핵심 이슈</div>' if brief_rows else '<div class="section-head">📌 주요 헤드라인 7</div>',unsafe_allow_html=True)
        if brief_rows:
            latest=brief_rows[0]
            if latest.get('카테고리코드') != category:
                st.caption('전체 뉴스 AI 브리핑 · 현재 선택한 분야 또는 즐겨찾기만의 분석은 아닙니다.')
            st.markdown(f'<div class="panel"><strong>{esc(latest.get("브리핑제목"))}</strong>'
                        f'<div class="notice">{esc(latest.get("핵심요약"))}</div></div>',unsafe_allow_html=True)
            try: issues=json.loads(latest.get('주요이슈') or '[]')
            except (ValueError,TypeError):issues=[]
            id_map={r.get('뉴스ID'):r for r in all_items}
            cards=[]
            for ix,issue in enumerate(issues[:7],1):
                target=next((id_map[k] for k in issue.get('ids',[]) if k in id_map),None)
                url=safe_url(target.get('원문URL','')) if target else ''
                headline=esc(issue.get('headline',''))
                headline=(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{headline}</a>' if url else headline)
                cards.append(f'<div class="issue"><span class="num">{ix}.</span>{headline}'
                             f'<div class="source">{esc(issue.get("point",""))}</div></div>')
            st.markdown('<div class="panel">'+''.join(cards)+'</div>',unsafe_allow_html=True)
            st.caption('AI 작성 · 제공된 RSS 제목·설명 기반 · 원문 사실검증 아님')
        else:
            # One strong headline per category, then fill remaining slots by recency.
            chosen=[]; cats_seen=set(); ids=set()
            for r in items:
                cat=r.get('카테고리코드','')
                if cat not in cats_seen:
                    chosen.append(r);cats_seen.add(cat);ids.add(r.get('뉴스ID'))
                if len(chosen)>=7:break
            for r in items:
                if len(chosen)>=7:break
                if r.get('뉴스ID') not in ids:
                    chosen.append(r);ids.add(r.get('뉴스ID'))
            cards=[]
            for ix,r in enumerate(chosen,1):
                title=esc(r.get('제목',''))
                url=safe_url(r.get('원문URL',''))
                link=(f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title}</a>' if url else title)
                cards.append(f'<div class="issue"><span class="num">{ix}.</span>{link}'
                             f'<div class="source">{esc(publisher(r,known))} · {esc(str(r.get("발행일시",""))[5:16])}</div></div>')
            st.markdown('<div class="panel">'+''.join(cards)+'</div>',unsafe_allow_html=True)
            st.caption('헤드라인 기반 자동 선별 · AI 요약이 아닙니다. 제목을 누르면 기사로 이동합니다.')
        st.markdown('<div class="section-head">📊 분야별 뉴스</div>',unsafe_allow_html=True)
        counts={key:sum(1 for r in all_items if r.get('카테고리코드')==key) for key in catcodes if key!='ALL'}
        top=sorted(counts.items(),key=lambda x:x[1],reverse=True)[:6]
        st.markdown('<div class="panel">'+''.join(
            f'<div class="issue">{esc(cmap.get(k,k))} <span style="float:right;color:#168149;font-weight:750">{v:,}건</span></div>'
            for k,v in top)+'</div>',unsafe_allow_html=True)
    st.caption('원문 및 기사 저작권은 해당 언론사에 있습니다. Google 뉴스 RSS 제공 링크가 원문으로 연결될 수 있습니다.')


def main():
    left, right = st.columns([4, 2], vertical_alignment='center')
    with left:
        st.markdown('<div class="brand">📰 간추린 뉴스 <span>News Brief</span></div>', unsafe_allow_html=True)
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
