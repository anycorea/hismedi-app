"""실제 아마노/구글시트 호출 없이 Streamlit 화면 흐름 확인."""
import unittest
from pathlib import Path
from unittest.mock import patch
from streamlit.testing.v1 import AppTest

APP=str(Path(__file__).with_name('parking_kiosk_app.py'))
CAR={'id':'ui-test','carNo':'123가4567','entryDateToString':'2026-10-28 09:00','dscnt_cnt':0}
class Tests(unittest.TestCase):
    def app(self):
        at=AppTest.from_file(APP)
        at.secrets['amano']={'base_url':'http://example.invalid','user_id':'test','password':'dummy'}
        at.run()
        self.assertFalse(at.exception)
        return at
    def test_vehicle_only_flow(self):
        at=self.app()
        self.assertEqual(len(at.text_input),0)
        for digit in '4567':at.button(key='key_'+digit).click().run()
        with patch('parking_kiosk_service.ParkingService.lookup',return_value=[CAR]):
            next(b for b in at.button if b.label=='차량 조회').click().run()
        self.assertFalse(at.exception)
        with patch('parking_kiosk_service.ParkingService.entry_photo',return_value=None):
            at.button(key='car_0').click().run()
        with patch('parking_kiosk_service.ParkingService.submit',return_value={'state':'success','message':'시험 등록 완료','record':None}) as submit:
            next(b for b in at.button if b.label=='3시간 할인 등록').click().run()
        self.assertEqual(submit.call_count,1)
        self.assertFalse(at.exception)
        self.assertEqual(at.success[0].value,'시험 등록 완료')
        next(b for b in at.button if b.label=='다음 차량 등록').click().run()
        self.assertEqual(at.session_state['suffix'],'')
        self.assertIsNone(at.session_state['selected'])
    def test_requires_four_digits(self):
        at=self.app()
        self.assertTrue(next(b for b in at.button if b.label=='차량 조회').disabled)
        at.button(key='key_1').click().run()
        self.assertTrue(next(b for b in at.button if b.label=='차량 조회').disabled)
if __name__=='__main__':unittest.main()
