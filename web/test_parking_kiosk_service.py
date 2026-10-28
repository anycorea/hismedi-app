import unittest
from types import SimpleNamespace
from parking_kiosk_service import ParkingService, HEADERS, confirmed

CAR={'id':'entry-10','carNo':'123가4567','entryDateToString':'2026-10-28 09:00','iLotArea':'621','dscnt_cnt':0}
class Response:
    def __init__(self,value=None,text='',error=None):self.value=value;self.text=text;self.error=error
    def raise_for_status(self):
        if self.error:raise self.error
    def json(self):return self.value
class Sheet:
    def __init__(self):self.rows=[HEADERS.copy()];self.fail=False
    def row_values(self,row):return self.rows[row-1].copy()
    def find(self,value,in_column):return next((SimpleNamespace(row=i+1) for i,row in enumerate(self.rows) if len(row)>=in_column and row[in_column-1]==value),None)
    def findall(self,value,in_column):return [SimpleNamespace(row=i+1) for i,row in enumerate(self.rows) if len(row)>=in_column and row[in_column-1]==value]
    def append_row(self,record,**kw):
        if self.fail:raise RuntimeError('offline')
        self.rows.append(record.copy())
    def update(self,range_name,values,**kw):
        row=int(range_name.split(':')[0][1:]);self.rows[row-1][8:11]=values[0].copy()
class Session:
    def __init__(self,engine):self.engine=engine;self.headers={}
    def post(self,url,data,**kw):
        if url.endswith('listForDiscount'):return Response(self.engine.cars)
        if url.endswith('/save'):
            self.engine.saves+=1
            if self.engine.timeout:raise TimeoutError('network timeout')
            return Response({'success':True} if self.engine.success else {'token':'looks-ok'},text='')
        return Response()
    def close(self):pass
class Engine:
    def __init__(self):self.sheet=Sheet();self.saves=0;self.success=True;self.timeout=False;self.cars=[CAR.copy()]
    def service(self):return ParkingService('http://example.invalid','test','dummy',lambda:self.sheet,lambda:Session(self))
class Tests(unittest.TestCase):
    def test_multiple_cars_not_first_only(self):
        e=Engine();e.cars.append({**CAR,'id':'entry-11','carNo':'999나4567'});self.assertEqual(len(e.service().lookup('4567')),2)
    def test_success_audited_and_no_patient_number(self):
        e=Engine();s=e.service();r=s.prepare_record('4567',CAR);out=s.submit('4567',CAR,r)
        self.assertEqual(out['state'],'success');self.assertEqual(e.saves,1);self.assertEqual(e.sheet.rows[1][2],'');self.assertEqual(e.sheet.rows[1][8],'등록완료')
    def test_log_failure_no_discount(self):
        e=Engine();e.sheet.fail=True;s=e.service()
        with self.assertRaises(RuntimeError):s.submit('4567',CAR,s.prepare_record('4567',CAR))
        self.assertEqual(e.saves,0)
    def test_timeout_never_resubmits(self):
        e=Engine();e.timeout=True;s=e.service();r=s.prepare_record('4567',CAR)
        self.assertEqual(s.submit('4567',CAR,r)['state'],'check_required')
        self.assertEqual(s.submit('4567',CAR,s.prepare_record('4567',CAR))['state'],'check_required');self.assertEqual(e.saves,1)
    def test_discounted_car_no_save(self):
        e=Engine();e.cars[0]['dscnt_cnt']=1;s=e.service()
        self.assertEqual(s.submit('4567',CAR,s.prepare_record('4567',CAR))['state'],'already');self.assertEqual(e.saves,0)
    def test_unrecognized_success_response_not_success(self):
        e=Engine();e.success=False;s=e.service()
        self.assertEqual(s.submit('4567',CAR,s.prepare_record('4567',CAR))['state'],'check_required')
    def test_stale_selection_no_save(self):
        e=Engine();e.cars=[];s=e.service()
        self.assertEqual(s.submit('4567',CAR,s.prepare_record('4567',CAR))['state'],'stale');self.assertEqual(e.saves,0)
class PhotoTests(unittest.TestCase):
    def service_with_photo(self,entry,content=None):
        from unittest.mock import MagicMock
        session=MagicMock();session.headers={}
        session.post.side_effect=[Response(),Response({'parkEntry':entry})]
        image=MagicMock();image.headers={'Content-Type':'image/jpeg'}
        image.iter_content.return_value=[content or b'example-image-bytes']
        image.__enter__.return_value=image;session.get.return_value=image
        service=ParkingService('http://example.invalid','test','dummy',lambda:None,lambda:session)
        return service,session
    def test_same_entry_image_requested(self):
        service,session=self.service_with_photo({'iID':CAR['id'],'acPlate1':CAR['carNo'],'acEntrancePicName':'/2026/test image.jpg'})
        self.assertEqual(service.entry_photo(CAR),b'example-image-bytes')
        self.assertEqual(session.get.call_args.args[0],'http://example.invalid/image/2026/test%20image.jpg')
    def test_wrong_entry_photo_rejected(self):
        service,session=self.service_with_photo({'iID':'other','acEntrancePicName':'/test.jpg'})
        with self.assertRaises(ValueError):service.entry_photo(CAR)
        session.get.assert_not_called()
    def test_missing_photo(self):
        service,session=self.service_with_photo({'iID':CAR['id'],'acPlate1':CAR['carNo']})
        self.assertIsNone(service.entry_photo(CAR));session.get.assert_not_called()
if __name__=='__main__':unittest.main()
