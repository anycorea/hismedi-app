"""모바일 앱의 아마노 요청 형식을 재사용한 키오스크 서비스.
실제 차량 조회/할인 요청은 사용자가 실행할 때에만 수행합니다.
"""
import datetime
import hashlib
import threading
import uuid
from contextlib import contextmanager
from zoneinfo import ZoneInfo
import requests
from urllib.parse import quote

HEADERS = ['기록ID','등록요청일시','환자등록번호','차량뒷번호','차량번호','입차일시','입차ID','무료주차시간','처리결과','최종갱신일시','비고']
_LOCK = threading.Lock()
_ENTRY_LOCKS = {}
UNCERTAIN = {'처리중', '확인필요'}

def now():
    return datetime.datetime.now(ZoneInfo('Asia/Seoul'))

def stamp():
    return now().strftime('%Y-%m-%d %H:%M:%S')

def confirmed(response):
    text = response.text.strip()
    if text.lower() in {'true','ok','success','성공'}:
        return True
    try:
        value = response.json()
    except ValueError:
        return False
    if value is True:
        return True
    if isinstance(value,str):
        return value.strip().lower() in {'true','ok','success','성공'}
    if isinstance(value,dict):
        return any(value.get(k) is True or isinstance(value.get(k),str) and value[k].strip().lower() in {'true','ok','success','성공'} for k in ('success','result'))
    return False

def has_discount(car):
    for key in ('dscnt_cnt','dscntCnt','discountCnt'):
        try:
            if int(car.get(key) or 0) > 0:
                return True
        except (TypeError,ValueError):
            pass
    return any(bool(car.get(k)) for k in ('dcDetailList','dscntList','discountList','dcList','discountName','dscntName','dcName'))

class ParkingService:
    def __init__(self, base_url, user_id, password, worksheet_factory, session_factory=requests.Session):
        self.base = base_url.rstrip('/')
        self.user_id = str(user_id)
        self.password = str(password)
        self.worksheet_factory = worksheet_factory
        self.session_factory = session_factory

    @contextmanager
    def session(self):
        session = self.session_factory()
        session.headers.update({'User-Agent':'Mozilla/5.0','Referer':self.base+'/login','X-Requested-With':'XMLHttpRequest'})
        try:
            response = session.post(self.base+'/login', data={'userId':self.user_id,'userPwd':hashlib.sha256(self.password.encode()).hexdigest()}, timeout=(5,10))
            response.raise_for_status()
            yield session
        finally:
            session.close()

    def lookup(self, suffix):
        if len(suffix) != 4 or not suffix.isascii() or not suffix.isdigit():
            raise ValueError('차량번호 뒤 4자리를 입력해 주세요.')
        with self.session() as session:
            response = session.post(self.base+'/discount/registration/listForDiscount',data={'iLotArea':'621','entryDate':now().strftime('%Y%m%d'),'carNo':suffix},timeout=(5,10))
            response.raise_for_status()
            items = response.json()
        if not isinstance(items,list) or any(not isinstance(x,dict) or x.get('id') is None or not x.get('carNo') for x in items):
            raise ValueError('차량 조회 응답을 확인할 수 없습니다.')
        # 아마노가 반환한 같은 입차 ID의 중복 행만 제거합니다.
        unique = {}
        for item in items:
            unique[str(item['id'])] = item
        return list(unique.values())

    def entry_photo(self, selected):
        """원본 화면의 상세조회/사진 경로. 사진은 메모리로만 읽고 저장하지 않습니다."""
        with self.session() as session:
            response = session.post(self.base+'/discount/registration/getForDiscount',
                data={'id':selected['id'],'iCardType':selected.get('iCardType') or '0',
                      'member_id':self.user_id,'startDate':now().strftime('%Y%m%d')},timeout=(5,10))
            response.raise_for_status()
            payload = response.json()
            entry = payload.get('parkEntry') if isinstance(payload,dict) else None
            if not isinstance(entry,dict) or str(entry.get('iID')) != str(selected['id']):
                raise ValueError('입차 사진의 차량 식별 정보가 일치하지 않습니다.')
            if entry.get('acPlate1') and str(entry['acPlate1']) != str(selected['carNo']):
                raise ValueError('입차 사진의 차량번호가 일치하지 않습니다.')
            path = entry.get('acEntrancePicName')
            if not isinstance(path,str) or not path:
                return None
            # 외부 주소가 아닌 로그인한 주차 서버의 사진 경로만 읽습니다.
            url = self.base + '/image' + quote(path,safe='/')
            with session.get(url,timeout=(5,10),stream=True,allow_redirects=False) as image:
                image.raise_for_status()
                if not image.headers.get('Content-Type','').lower().startswith('image/'):
                    raise ValueError('입차 사진 응답 형식이 올바르지 않습니다.')
                chunks=[];size=0
                for chunk in image.iter_content(65536):
                    size+=len(chunk)
                    if size>4*1024*1024:raise ValueError('입차 사진 용량이 제한을 초과했습니다.')
                    chunks.append(chunk)
                return b''.join(chunks) or None

    def prepare_record(self, suffix, car):
        timestamp = stamp()
        return [str(uuid.uuid4()),timestamp,'',suffix,str(car['carNo']),str(car.get('entryDateToString','')),str(car['id']),'3','처리중',timestamp,'키오스크 / 할인 요청 전 기록']

    def update_record(self, record):
        sheet = self.worksheet_factory()
        cell = sheet.find(record[0], in_column=1)
        if cell is None:
            raise ValueError('등록 기록을 찾지 못했습니다.')
        sheet.update(range_name=f'I{cell.row}:K{cell.row}', values=[record[8:11]], value_input_option='RAW')

    def submit(self, suffix, selected, record):
        """응답 불명확 시 동일 기록으로 재전송하지 않습니다.
        동일 입차ID의 동시 실행은 이 Python 프로세스 안에서 직렬화합니다.
        """
        entry_id = str(selected['id'])
        if record[6] != entry_id or record[4] != str(selected['carNo']) or record[3] != suffix:
            raise ValueError('선택 차량과 기록이 일치하지 않습니다.')
        with _LOCK:
            lock = _ENTRY_LOCKS.setdefault(entry_id,threading.Lock())
        with lock:
            # 등록 직전 재조회: 오래된 선택과 이미 적용된 할인 확인.
            latest = next((x for x in self.lookup(suffix) if str(x['id']) == entry_id and str(x['carNo']) == str(selected['carNo'])), None)
            if latest is None:
                return {'state':'stale','message':'차량 입차 정보가 변경되었습니다. 새로 조회해 주세요.','record':None}
            if has_discount(latest):
                return {'state':'already','message':'이미 주차 할인이 적용된 차량입니다. 조정이 필요하면 원무팀에 문의해 주세요.','record':None}
            sheet = self.worksheet_factory()
            # 원본 모바일 앱과 동일한 A:K 형식. 기존 열/행은 변경하지 않습니다.
            if sheet.row_values(1) != HEADERS:
                raise ValueError('구글시트의 A:K 헤더가 기존 형식과 다릅니다.')
            for cell in sheet.findall(entry_id,in_column=7):
                row = sheet.row_values(cell.row)
                if len(row) >= 9 and row[6] == entry_id and row[8] in UNCERTAIN:
                    return {'state':'check_required','message':'이 차량의 이전 등록 결과를 확인해야 합니다. 중복 신청하지 말고 원무팀에 문의해 주세요.','record':None}
            if sheet.find(record[0],in_column=1) is not None:
                return {'state':'check_required','message':'이미 요청 기록이 있습니다. 원무팀에서 할인 적용 여부를 확인해 주세요.','record':None}
            # 저장 실패 시 할인 요청을 보내지 않습니다.
            sheet.append_row(record,value_input_option='RAW',insert_data_option='INSERT_ROWS',table_range='A:K')
            result = {'state':'check_required','message':'등록 결과를 확인하지 못했습니다. 다시 등록하지 말고 원무팀에 문의해 주세요.','record':record}
            record[8:11] = ['확인필요',stamp(),'키오스크 / 실제 할인 적용 여부 확인 필요']
            try:
                with self.session() as session:
                    payload = {'peId':latest['id'],'discountType':'2','saveCnt':'1','iCardType':'0','carNo':latest['carNo'],'iLotArea':latest.get('iLotArea') or '621','acPlate2':'','memo':''}
                    response = session.post(self.base+'/discount/registration/save',data=payload,timeout=(5,10))
                    response.raise_for_status()
                    if confirmed(response):
                        record[8:11] = ['등록완료',stamp(),'키오스크 / 3시간 할인 적용 완료']
                        result.update(state='success',message='3시간 주차 할인 등록이 완료되었습니다.')
            except Exception:
                # 할인 요청의 전송 여부가 불명확할 수 있으므로 자동 재시도 금지.
                pass
            try:
                self.update_record(record)
                result['record'] = None
            except Exception:
                pass
            return result
