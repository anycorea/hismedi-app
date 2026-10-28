"""키오스크 전용 통합 진입점. 모바일 앱과 별도로 배포하세요."""
import streamlit as st

st.set_page_config(page_title='히즈메디병원 안내',page_icon='🏥',layout='wide',initial_sidebar_state='collapsed')

def home():
    from pathlib import Path
    import streamlit.components.v1 as components
    for key in ('suffix','cars','selected','result','request_record','sending','notice','photo','photo_note'):
        st.session_state.pop(key,None)
    st.markdown('<style>#MainMenu,footer,header[data-testid="stHeader"]{display:none!important}.block-container{padding:0!important;max-width:100%!important}</style>',unsafe_allow_html=True)
    components.html(Path(__file__).with_name('kiosk_home.html').read_text(encoding='utf-8'),height=900,scrolling=False)

main=st.Page(home,title='처음 화면',default=True)
medical=st.Page('medical_kiosk_view.py',title='의료진 안내',url_path='medical')
parking=st.Page('parking_kiosk_app.py',title='주차 등록',url_path='parking')
unpaid=st.Page('unpaid_kiosk_view.py',title='비급여 안내',url_path='unpaid')
page=st.navigation([main,medical,parking,unpaid],position='hidden')
if page.url_path!='parking':
    st.markdown('<div id="hismedi-kiosk-state" data-state="ready" style="display:none"></div>',unsafe_allow_html=True)
page.run()
