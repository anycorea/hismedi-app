"""키오스크 전용 앱. 모바일 parking_app.py와 별도로 배포합니다."""
import html
import streamlit as st
from parking_kiosk_service import ParkingService

st.set_page_config(page_title='히즈메디병원 · 주차등록',page_icon='🏥',layout='wide',initial_sidebar_state='collapsed')
st.markdown('''<style>
#MainMenu,footer,header[data-testid="stHeader"],[data-testid="stToolbar"],[data-testid="stAppDeployButton"],[data-testid="stStatusWidget"],[class*="viewerBadge"]{display:none!important}
.stApp{background:#eef4f8;color:#17354b}.block-container{padding:.6rem 1.2rem 1rem;max-width:1400px}
h1{font-size:30px!important;padding:.2rem 0!important}h2{font-size:24px!important;padding:.2rem 0!important}
[data-testid="stVerticalBlock"]{gap:.65rem}
[data-testid="stImage"] img{max-height:210px;object-fit:contain;border-radius:10px}
.st-key-parking-choice button{border:2px solid #078aab;background:#e2f4f7;text-align:left}
.st-key-parking-choice button p{font-size:21px!important}
div.stButton>button{min-height:54px;width:100%;font-size:23px!important;border-radius:12px;touch-action:manipulation}
div.stButton>button p{font-size:23px!important;font-weight:700}
.kiosk-number{background:white;border:3px solid #078aab;border-radius:14px;padding:8px;text-align:center;font:700 40px/1.3 sans-serif;letter-spacing:10px;margin-bottom:12px}
.kiosk-card{background:white;padding:12px;border-radius:14px;border:1px solid #ccdce5;margin-bottom:12px;font-size:21px;line-height:1.4}
.kiosk-next{background:#fff4d6;border-left:6px solid #d99000;border-radius:10px;padding:10px 14px;font-size:22px;font-weight:800;margin:0 0 10px;line-height:1.5}
.kiosk-step{font-size:24px;font-weight:800;margin:8px 0 6px;color:#17354b}
.kiosk-note{font-size:20px;color:#45616f;margin-bottom:18px}
</style>''',unsafe_allow_html=True)

def worksheet_factory():
    import gspread
    from google.oauth2.service_account import Credentials
    info = dict(st.secrets['gcp_service_account'])
    info['private_key'] = info['private_key'].replace('\\n','\n')
    creds = Credentials.from_service_account_info(info,scopes=['https://www.googleapis.com/auth/spreadsheets'])
    client = gspread.authorize(creds)
    client.set_timeout(10)
    config = st.secrets['gsheet']
    return client.open_by_key(config['spreadsheet_id']).worksheet(config.get('worksheet_name','parking_logs'))

try:
    settings = st.secrets['amano']
    service = ParkingService(settings['base_url'],settings['user_id'],settings['password'],worksheet_factory)
except Exception:
    st.error('주차 시스템 설정이 필요합니다. 관리자에게 문의해 주세요.')
    st.stop()

# 운영자가 최초에만 입력하는 선택적 보호 코드. 환자번호 입력은 없습니다.
access_code = str(st.secrets.get('kiosk_access_code',''))
if access_code and not st.session_state.get('kiosk_unlocked'):
    st.title('히즈메디병원 · 키오스크 시작')
    entered = st.text_input('운영자 시작 코드',type='password')
    if st.button('키오스크 시작',type='primary'):
        import hmac
        if hmac.compare_digest(entered,access_code):
            st.session_state.kiosk_unlocked=True
            st.rerun()
        else:
            st.error('시작 코드가 일치하지 않습니다.')
    st.stop()

for key,value in {'suffix':'','cars':[],'selected':None,'result':None,'request_record':None,'sending':False,'notice':'','photo':None,'photo_note':''}.items():
    if key not in st.session_state: st.session_state[key]=value

def reset():
    for key,value in {'suffix':'','cars':[],'selected':None,'result':None,'request_record':None,'sending':False,'notice':'','photo':None,'photo_note':''}.items():
        st.session_state[key]=value

def digit(value):
    if st.session_state.sending:return
    if value == 'clear': st.session_state.suffix=''
    elif value == 'back': st.session_state.suffix=st.session_state.suffix[:-1]
    elif len(st.session_state.suffix)<4: st.session_state.suffix+=value
    st.session_state.cars=[];st.session_state.selected=None;st.session_state.request_record=None;st.session_state.notice=''
    st.session_state.photo=None;st.session_state.photo_note=''

st.title('히즈메디병원 · 주차등록')
st.markdown('<div class="kiosk-note">① 번호 입력 → ② 차량 조회 → ③ 차량 선택 → ④ 할인 등록</div>',unsafe_allow_html=True)
result=st.session_state.result
st.markdown('<div id="hismedi-kiosk-state" data-state="'+('hold' if result and result.get('record') is not None else 'done' if result else 'ready')+'" style="display:none"></div>',unsafe_allow_html=True)
if result:
    if result['state']=='success': st.success(result['message'])
    elif result['state']=='stale': st.warning(result['message'])
    else: st.warning(result['message'])
    if result.get('record') is not None:
        st.warning('주차 요청 결과의 구글시트 기록이 아직 갱신되지 않았습니다.')
        if st.button('기록 저장만 다시 시도'):
            try:
                service.update_record(result['record'])
                result['record']=None
                st.rerun()
            except Exception:
                st.error('기록 저장에 실패했습니다. 원무팀에 문의해 주세요.')
    if st.button('다음 차량 등록',type='primary',disabled=result.get('record') is not None):
        reset();st.rerun()
    st.stop()

if st.session_state.selected:
    next_action='④ 차량번호와 사진을 확인한 뒤, 오른쪽 ‘3시간 할인 등록’을 눌러 주세요.'
elif st.session_state.cars:
    next_action='③ 오른쪽 목록에서 본인 차량번호를 눌러 주세요.'
elif len(st.session_state.suffix)==4:
    next_action='② 왼쪽 아래 ‘차량 조회’ 버튼을 눌러 주세요.'
else:
    next_action='① 왼쪽 숫자 버튼으로 차량번호 뒤 4자리를 입력해 주세요.'
st.markdown('<div class="kiosk-next">'+next_action+'</div>',unsafe_allow_html=True)
left,right=st.columns([.85,1.45],gap='medium')
with left:
    st.subheader('① 차량번호 입력')
    st.markdown('<div class="kiosk-number">'+html.escape(st.session_state.suffix.ljust(4,'·'))+'</div>',unsafe_allow_html=True)
    for row in [('1','2','3'),('4','5','6'),('7','8','9'),('clear','0','back')]:
        for col,key in zip(st.columns(3),row):
            col.button({'clear':'전체삭제','back':'⌫'}.get(key,key),key='key_'+key,on_click=digit,args=(key,),disabled=st.session_state.sending,use_container_width=True)
    st.markdown('<div class="kiosk-step">② 입력한 번호로 차량 조회</div>',unsafe_allow_html=True)
    if st.button('차량 조회',type='primary',use_container_width=True,disabled=len(st.session_state.suffix)!=4 or st.session_state.sending):
        st.session_state.sending=True
        try:
            with st.spinner('차량을 조회하고 있습니다. 잠시만 기다려 주세요.'):
                st.session_state.cars=service.lookup(st.session_state.suffix)
                st.session_state.selected=None
                st.session_state.request_record=None
                st.session_state.notice='' if st.session_state.cars else '오늘 입차한 차량이 없습니다. 번호를 확인해 주세요.'
        except Exception:
            st.session_state.notice='차량 조회가 지연되거나 연결되지 않았습니다. 잠시 후 조회를 다시 눌러 주세요.'
            st.session_state.cars=[];st.session_state.selected=None
        finally:
            st.session_state.sending=False
        st.rerun()
    if st.button('입력 초기화',disabled=st.session_state.sending):
        reset();st.rerun()
with right:
    st.subheader('③ 차량 선택')
    if st.session_state.notice:st.info(st.session_state.notice)
    if not st.session_state.cars and not st.session_state.notice:
        st.info('왼쪽 숫자 키패드로 차량번호 뒤 4자리를 입력해 주세요.')
    if st.session_state.cars:
        st.markdown('**본인 차량의 파란색 ‘이 차량 선택’ 버튼을 눌러 주세요.**')
    with st.container(key='parking-choice'):
        for index,car in enumerate(st.session_state.cars):
            selected=st.session_state.selected
            chosen=selected is not None and str(selected.get('id'))==str(car.get('id')) and selected.get('carNo')==car.get('carNo')
            label=f"{'✓ 선택됨' if chosen else '👉 이 차량 선택'} · {car['carNo']}  |  입차 {car.get('entryDateToString','')}"
            if st.button(label,key='car_'+str(index),disabled=st.session_state.sending,use_container_width=True):
                st.session_state.selected=car;st.session_state.request_record=None
                st.session_state.photo=None;st.session_state.photo_note=''
                try:
                    with st.spinner('입차 사진을 확인하고 있습니다.'):
                        st.session_state.photo=service.entry_photo(car)
                    if st.session_state.photo is None:st.session_state.photo_note='등록된 입차 사진이 없습니다.'
                except Exception:
                    st.session_state.photo_note='입차 사진을 불러오지 못했습니다. 차량번호와 입차시간을 확인해 주세요.'
                st.rerun()
    car=st.session_state.selected
    if car:
        photo_col,action_col=st.columns([1,1.1],gap='medium')
        with photo_col:
            st.markdown('<div class="kiosk-card"><b>'+html.escape(str(car['carNo']))+'</b><br>입차시간: '+html.escape(str(car.get('entryDateToString','')))+'</div>',unsafe_allow_html=True)
            if st.session_state.photo:
                st.image(st.session_state.photo,width=280)
            elif st.session_state.photo_note:
                st.caption(st.session_state.photo_note)
        with action_col:
            st.markdown('<div class="kiosk-step">④ 3시간 할인 등록</div>',unsafe_allow_html=True)
            st.info('본인 차량이 맞으면 아래 ‘3시간 할인 등록’을 눌러 주세요.')
            if st.button('3시간 할인 등록',type='primary',use_container_width=True,disabled=st.session_state.sending):
                st.session_state.sending=True
                st.markdown('<div id="hismedi-kiosk-state" data-state="busy" style="display:none"></div>',unsafe_allow_html=True)
                try:
                    if st.session_state.request_record is None:
                        st.session_state.request_record=service.prepare_record(st.session_state.suffix,car)
                    with st.spinner('할인을 등록하고 있습니다. 다시 누르지 말고 기다려 주세요.'):
                        st.session_state.result=service.submit(st.session_state.suffix,car,st.session_state.request_record)
                except Exception:
                    st.session_state.notice='조회 또는 기록 저장을 확인하지 못했습니다. 새로 조회하거나 원무팀에 문의해 주세요.'
                finally:
                    st.session_state.sending=False
                st.rerun()
