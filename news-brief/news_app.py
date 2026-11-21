"""News_Brief: accessible multi-device Streamlit RSS reader and AI briefings."""
import hashlib
import hmac
import html
import json
from datetime import datetime

import streamlit as st
from news_core import (HEADERS, ai_briefing, append_records, collect, connect, enabled, initialize,
                       media_name_map, now, patch_record, rows, safe_url, save_briefing, validate)

st.set_page_config(page_title='AI 뉴스브리핑 | News_Brief',page_icon='📰',layout='wide',initial_sidebar_state='collapsed')
st.markdown('''<style>
:root{color-scheme:light} .stApp{background:#f8fafc;color:#102033}
.block-container{padding-top:1.05rem;max-width:1450px;padding-bottom:4rem}
header[data-testid="stHeader"]{background:#f8fafc}
#MainMenu, footer,div[data-testid="stDecoration"]{visibility:hidden}
.stButton button,.stLinkButton a{min-height:44px;border-radius:10px;font-weight:700}
.stTextInput input,.stSelectbox>div{min-height:44px}
.main-brand{border-bottom:3px solid #123e78;padding:0 0 .8rem;margin-bottom:1rem}
.main-brand h1{font-size:clamp(1.7rem,3.6vw,2.65rem);color:#102c52;margin:0;font-weight:850;letter-spacing:-.03em}
.main-brand p{font-size:1rem;color:#52637b;margin:.25rem 0 0}
.news-card{background:#fff;border:1px solid #dbe4f0;border-left:4px solid #3976c4;border-radius:12px;padding:1.1rem 1.15rem;margin:0 0 .85rem;box-shadow:0 2px 8px #14375a08;overflow-wrap:anywhere}
.news-card .meta{font-size:.9rem;color:#52637b;margin-bottom:.5rem;font-weight:650}
.news-card .headline{font-size:var(--news-title-size,1.25rem);font-weight:800;line-height:1.47;margin:.1rem 0 .6rem}
.news-card a,.brief-box a{color:#123f7a;text-decoration:none}
.news-card a:hover,.brief-box a:hover{text-decoration:underline}
.news-card .summary{font-size:var(--news-body-size,1rem);line-height:1.7;color:#34455e}
.brief-box{background:#ecf4ff;border:1px solid #bfd6f9;border-radius:14px;padding:1rem 1.2rem;margin-bottom:1rem;line-height:1.7;color:#183755}
.brief-box strong{font-size:1.14rem;color:#0b376b}
.news-badge{display:inline-block;border-radius:5px;padding:3px 8px;background:#dceafc;color:#194981;font-size:.8rem;font-weight:750;margin-right:.35rem}
.anycorea{text-align:center;color:#62748c;font-size:.85rem;padding:1.8rem 0 0}
div[data-testid="stVerticalBlock"]{gap:.7rem}
@media(max-width:800px){.block-container{padding:1rem .85rem 4rem}.main-brand h1{font-size:1.65rem}.news-card{padding:1rem}.news-card .headline{font-size:max(var(--news-title-size,1.25rem),1.15rem)}.news-card .summary{line-height:1.8}.main-brand p{font-size:.9rem}}
</style>''',unsafe_allow_html=True)

def esc(value):return html.escape(str(value or ''),quote=True)

def secrets_value(group,key,default=''):
    try:return st.secrets.get(group,{}).get(key,default)
    except Exception:return default

@st.cache_resource(ttl=600)
def get_book():return connect(st.secrets)

@st.cache_data(ttl=120,show_spinner=False)
def get_data(name):return rows(get_book(),name)

def invalidate():get_data.clear()

def authorized():return bool(st.session_state.get('admin',False))

def publisher(record,names):
    kw=str(record.get('키워드',''))
    if kw.startswith('출처:'):return kw[3:].strip()
    return names.get(record.get('언론사코드'),str(record.get('언론사코드','출처 미상')))

def make_news_card(item,names,condensed=False):
    url=safe_url(item.get('원문URL',''))
    title=esc(item.get('제목'))
    if not title:return
    when=esc(str(item.get('발행일시',''))[:16])
    summary=esc(str(item.get('요약',''))[:250])
    source=esc(publisher(item,names))
    category=esc(item.get('카테고리코드',''))
    link=f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title} ↗</a>' if url else title
    st.markdown(f'''<article class="news-card"><div class="meta"><span class="news-badge">{category}</span>{source} · {when}</div>
        <div class="headline">{link}</div>
        {'' if condensed else f'<div class="summary">{summary or "RSS 요약이 제공되지 않은 기사입니다."}</div>'}
        </article>''',unsafe_allow_html=True)

def admin_log(action,sheet,key,details):
    append_records(get_book(),'Admin_Log',[dict(zip(HEADERS['Admin_Log'],[
      hashlib.sha256((now()+action+key).encode()).hexdigest()[:24],now(),'MASTER',action,sheet,key,str(details)[:400]]))])

def app_admin():
    st.markdown('### 🔐 관리자 센터')
    pw=str(secrets_value('news_brief','admin_password',''))
    if not pw:
        st.warning('관리자 비밀번호가 설정되지 않았습니다. Streamlit Secrets의 [news_brief] admin_password를 등록하세요.');return
    if not authorized():
        with st.form('login'):
            password=st.text_input('관리자 비밀번호',type='password')
            if st.form_submit_button('관리자 로그인',type='primary'):
                if hmac.compare_digest(password,pw):st.session_state['admin']=True;st.rerun()
                else:st.error('비밀번호가 일치하지 않습니다.')
        return
    if st.button('로그아웃'):st.session_state['admin']=False;st.rerun()
    tabs=st.tabs(['상태·초기화','언론사 관리','카테고리 관리','뉴스 수집','AI 브리핑','설정·기록'])
    with tabs[0]:
        st.write('구글시트 구조 검사: 데이터와 서식은 수정하지 않습니다.')
        faults=validate(get_book())
        if faults:st.error(' / '.join(faults))
        else:st.success('8개 탭 헤더가 정상입니다.')
        st.caption('초기 데이터 버튼은 Category와 Media가 비어 있을 때만 기본 목록을 넣습니다.')
        if st.button('카테고리·기본 뉴스 소스 초기 등록',disabled=bool(faults)):
            try:initialize(get_book());admin_log('초기등록','Category/Media','','초기 데이터 등록');invalidate();st.success('등록 완료')
            except Exception as e:st.error(str(e))
        for sheet in ['Category','Media','News','Briefing','Collection_Log']:
            st.metric(sheet,len(get_data(sheet)))
    with tabs[1]:
        st.write('언론사별 RSS를 등록할 수 있습니다. 등록 후 뉴스 수집 탭에서 수동 수집 또는 정기 작업을 이용합니다.')
        media=get_data('Media')
        if media:st.dataframe(media,hide_index=True)
        code_options=['(새로 등록)']+[r['언론사코드'] for r in media]
        chosen=st.selectbox('편집 대상',code_options,key='media_select')
        row=next((r for r in media if r.get('언론사코드')==chosen),{})
        with st.form('media_edit'):
            code=st.text_input('언론사코드',value=row.get('언론사코드',''))
            name=st.text_input('언론사명',value=row.get('언론사명',''))
            group=st.text_input('언론사분류',value=row.get('언론사분류','종합'))
            home=st.text_input('홈페이지 URL',value=row.get('홈페이지URL',''))
            rss=st.text_input('RSS URL',value=row.get('RSS_URL',''))
            active=st.checkbox('수집 사용',value=enabled(row.get('사용여부','Y')))
            cat=st.selectbox('기본 카테고리',options=[r['카테고리코드'] for r in get_data('Category') if r['카테고리코드']!='ALL']+['ALL'],index=0)
            submitted=st.form_submit_button('저장',type='primary')
            if submitted:
                if not code.strip() or not name.strip() or not safe_url(rss):st.error('코드, 이름, 올바른 RSS URL이 필요합니다.')
                elif chosen=='(새로 등록)' and any(x['언론사코드']==code for x in media):st.error('이미 존재하는 코드입니다.')
                else:
                    data=dict(zip(HEADERS['Media'],[code.strip(),name.strip(),group,home,rss,'','RSS',30,len(media)*10+10,'Y' if active else 'N',cat]))
                    if chosen=='(새로 등록)':append_records(get_book(),'Media',[data])
                    elif code != chosen:st.error('기존 언론사코드는 변경할 수 없습니다. 새로 등록하세요.');st.stop()
                    else:patch_record(get_book(),'Media','언론사코드',chosen,data)
                    admin_log('저장','Media',code,name);invalidate();st.success('저장했습니다.')
    with tabs[2]:
        categories=get_data('Category')
        st.dataframe(categories,hide_index=True)
        chosen=st.selectbox('카테고리 편집', ['(새로 등록)']+[x['카테고리코드'] for x in categories],key='category_select')
        old=next((x for x in categories if x['카테고리코드']==chosen),{})
        with st.form('cat_edit'):
            code=st.text_input('카테고리코드',value=old.get('카테고리코드',''))
            name=st.text_input('카테고리명',value=old.get('카테고리명',''))
            sort=st.number_input('정렬순서',min_value=0,max_value=9999,value=int(old.get('정렬순서') or 100),step=10)
            use=st.checkbox('사용',value=enabled(old.get('사용여부','Y')))
            if st.form_submit_button('카테고리 저장',type='primary'):
                if not code.strip() or not name.strip():st.error('코드와 이름이 필요합니다.')
                elif chosen=='(새로 등록)' and any(x['카테고리코드']==code for x in categories):st.error('이미 등록된 코드입니다.')
                elif chosen!='(새로 등록)' and code!=chosen:st.error('기존 카테고리 코드는 변경할 수 없습니다.')
                else:
                    data=dict(zip(HEADERS['Category'],[code,name,old.get('상위카테고리',''),sort,'Y' if use else 'N',old.get('비고','')]))
                    if chosen=='(새로 등록)':append_records(get_book(),'Category',[data])
                    else:patch_record(get_book(),'Category','카테고리코드',chosen,data)
                    admin_log('저장','Category',code,name);invalidate();st.success('저장했습니다.')
    with tabs[3]:
        st.info('RSS 피드에 접속해서 신규 기사만 수집합니다. 자동 수집은 GitHub Actions에서 실행됩니다.')
        media=[r for r in get_data('Media') if enabled(r.get('사용여부'))]
        targets=st.multiselect('수집할 뉴스 소스',options=[r['언론사코드'] for r in media],default=[r['언론사코드'] for r in media])
        cap=st.slider('소스별 최대 수집 기사',5,80,30,5)
        if st.button('지금 뉴스 수집',type='primary',disabled=not bool(targets)):
            with st.spinner('뉴스 수집 중'):
                try:
                    result=collect(get_book(),cap,set(targets));admin_log('수동수집','News','',result);invalidate();st.success(f"완료 · 신규 {result['new']}건 / 오류 {result['errors']}개 소스")
                except Exception as e:st.error(f'수집 실패: {e}')
    with tabs[4]:
        st.caption('AI는 RSS 제목과 제공된 짧은 요약만 읽습니다. 기사 본문 확인이나 사실 검증을 수행하지 않습니다.')
        ai_key=str(secrets_value('news_brief','openai_api_key',''))
        model=str(secrets_value('news_brief','ai_model','gpt-4.1-mini'))
        if not ai_key:st.warning('AI 브리핑을 사용하려면 Streamlit Secrets에 openai_api_key를 추가하세요.')
        categories=[r for r in get_data('Category') if enabled(r.get('사용여부'))]
        pick=st.selectbox('브리핑 카테고리',options=[r['카테고리코드'] for r in categories])
        if st.button('AI 브리핑 생성·저장',type='primary',disabled=not ai_key):
            raw=[r for r in get_data('News') if enabled(r.get('노출여부')) and (pick=='ALL' or r.get('카테고리코드')==pick)]
            raw.sort(key=lambda x:x.get('발행일시',''),reverse=True)
            try:
                with st.spinner('AI가 기사 메타데이터를 검토하고 있습니다'):
                    output=ai_briefing(raw,api_key=ai_key,model=model)
                    save_briefing(get_book(),output,pick)
                    admin_log('AI브리핑','Briefing',pick,output['title']);invalidate()
                st.success('AI 브리핑 저장 완료');st.json(output)
            except Exception as e:st.error('AI 생성 실패: '+str(e)[:300])
    with tabs[5]:
        st.write('운영 기록 및 설정 테이블입니다.')
        for sheet in ['Settings','Collection_Log','Admin_Log']:
            with st.expander(sheet,expanded=(sheet=='Collection_Log')):
                st.dataframe(get_data(sheet)[-50:],hide_index=True)
        with st.form('settings_edit'):
            key=st.text_input('설정키 (기존 설정은 덮어쓰기)')
            value=st.text_input('설정값')
            detail=st.text_input('설명')
            if st.form_submit_button('설정 저장'):
                if key:
                    r={'설정키':key,'설정값':value,'설명':detail,'수정일시':now()}
                    if not patch_record(get_book(),'Settings','설정키',key,r):append_records(get_book(),'Settings',[r])
                    admin_log('설정','Settings',key,detail);invalidate();st.success('저장 완료')

def app_home():
    categories=[r for r in get_data('Category') if enabled(r.get('사용여부'))]
    cats=sorted(categories,key=lambda x:int(x.get('정렬순서') or 999))
    cmap={r['카테고리코드']:r['카테고리명'] for r in cats}
    names=media_name_map(get_book())
    selected=st.session_state.get('category','ALL')
    if selected not in cmap:selected='ALL'
    with st.expander('☰  뉴스 분야 · 언론사 · 검색 필터',expanded=False):
        options=list(cmap)
        selected=st.selectbox('뉴스 분야',options,index=options.index(selected),format_func=lambda v:cmap[v])
        st.session_state['category']=selected
        search=st.text_input('기사 검색',placeholder='제목·요약·언론사 이름')
        all_media=sorted({publisher(x,names) for x in get_data('News')})
        source=st.selectbox('언론사', ['전체 언론사']+all_media)
        hide_duplicate=st.checkbox('중복 기사 숨기기',value=True)
        compact=st.checkbox('제목만 간단히 보기',value=False)
        items_per_page=st.select_slider('페이지당 기사',options=[10,20,30,50],value=20)
    st.markdown('#### 🧠 AI 오늘의 핵심 브리핑')
    briefs=[x for x in get_data('Briefing') if enabled(x.get('노출여부')) and (selected=='ALL' and x.get('카테고리코드')=='ALL' or x.get('카테고리코드')==selected)]
    briefs.sort(key=lambda x:x.get('생성일시',''),reverse=True)
    if briefs:
        latest=briefs[0]
        st.markdown(f'<div class="brief-box"><strong>{esc(latest.get("브리핑제목"))}</strong><p>{esc(latest.get("핵심요약"))}</p><small>생성: {esc(latest.get("생성일시"))} · RSS 제목/요약 기반 (기사 본문 미검증)</small></div>',unsafe_allow_html=True)
        try:issues=json.loads(latest.get('주요이슈','[]'))
        except Exception:issues=[]
        for n,issue in enumerate(issues,1):
            with st.expander(f'{n}. {issue.get("headline", "주요 이슈")}',expanded=n==1):
                st.write(issue.get('point',''))
                links={r['뉴스ID']:r for r in get_data('News')}
                for id_ in issue.get('ids',[]):
                    record=links.get(id_)
                    if record and safe_url(record.get('원문URL','')):st.link_button('원문 · '+str(record['제목'])[:48],record['원문URL'])
    else:st.info('아직 생성된 AI 브리핑이 없습니다. 관리자가 뉴스 수집 후 브리핑을 생성하면 여기에 표시됩니다.')
    st.markdown(f'#### 📰 {esc(cmap.get(selected,"전체"))} 최신 뉴스')
    all_news=[r for r in get_data('News') if enabled(r.get('노출여부')) and (not hide_duplicate or str(r.get('중복여부','N')).upper()!='Y')]
    if selected!='ALL':all_news=[r for r in all_news if r.get('카테고리코드')==selected]
    if source!='전체 언론사':all_news=[r for r in all_news if publisher(r,names)==source]
    if search.strip():
        q=search.strip().casefold()
        all_news=[r for r in all_news if q in (' '.join([str(r.get('제목','')),str(r.get('요약','')),publisher(r,names)])).casefold()]
    all_news.sort(key=lambda r:str(r.get('발행일시','')),reverse=True)
    total=len(all_news)
    if not total:st.info('조건에 해당하는 기사가 없습니다. 뉴스 수집 또는 필터 변경을 확인하세요.');return
    maxpage=max(1,(total+items_per_page-1)//items_per_page)
    page=st.number_input(f'총 {total:,}건 · 페이지',min_value=1,max_value=maxpage,step=1,value=1)
    for item in all_news[(page-1)*items_per_page:page*items_per_page]:make_news_card(item,names,compact)
    st.caption(f'{page} / {maxpage} 페이지 · 기사 제목을 누르면 제공된 원문 URL이 새 창으로 열립니다.')

def main():
    st.markdown('<div class="main-brand"><h1>📰 AI 뉴스브리핑</h1><p>읽기 편한 뉴스, 짧고 명확한 핵심 정리 · News_Brief</p></div>',unsafe_allow_html=True)
    st.caption('iPhone · iPad · PC 대응 | 브라우저 핀치 확대 허용 · 글자 크기 조절')
    a,b,c=st.columns([2,2,1.5],vertical_alignment='bottom')
    with a:mode=st.segmented_control('화면',options=['뉴스','관리자'],default='뉴스',label_visibility='collapsed')
    with b:font=st.selectbox('글자 크기',['기본','크게','아주 크게'],label_visibility='collapsed')
    with c:
        if st.button('↻ 새로고침'):invalidate();st.rerun()
    sizes={'기본':('1.25rem','1rem'),'크게':('1.52rem','1.15rem'),'아주 크게':('1.85rem','1.35rem')}
    title_size,body_size=sizes[font]
    st.markdown(f'<style>:root{{--news-title-size:{title_size};--news-body-size:{body_size}}}</style>',unsafe_allow_html=True)
    try:
        problems=validate(get_book())
        if problems:
            st.error('구글시트 구조 확인 필요: '+' / '.join(problems))
            return
        if mode=='관리자':app_admin()
        else:app_home()
    except Exception as exc:
        st.error('Google Sheets 연결 또는 처리에 문제가 발생했습니다. 비밀키·공유 권한·시트 ID를 확인하세요.')
        if authorized():st.exception(exc)
    st.markdown('<div class="anycorea">Created by anycorea · 기사 저작권은 각 언론사에 있습니다.</div>',unsafe_allow_html=True)

if __name__=='__main__':main()
