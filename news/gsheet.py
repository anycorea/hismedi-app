# hismedi-app/news/gsheet.py
import os, json
import gspread
from google.oauth2.service_account import Credentials

# ✅ summary 제거
NEWS_HEADERS = [
  "published_at","source","title","url","url_canonical","tags",
  "title_hash","simhash","duplicate_of"
]

META_HEADERS = ["key","value"]

def _client():
    sa_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if not sa_json:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    info = json.loads(sa_json)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_sheet():
    sheet_id = os.getenv("GSHEET_ID","").strip()
    if not sheet_id:
        raise RuntimeError("Missing GSHEET_ID")
    gc = _client()
    sh = gc.open_by_key(sheet_id)
    return sh

def ensure_tabs(sh):
    """
    - NEWS / META 탭이 없으면 생성
    - NEWS 탭이 새로 생성될 때는 summary 없는 헤더로 생성
    - 기존 NEWS 탭이 이미 있으면 헤더는 건드리지 않음(사용자가 직접 정리한다고 하셨으므로)
    """
    try:
        ws_news = sh.worksheet("NEWS")
    except Exception:
        # cols는 헤더 수에 맞게 충분히(여유 포함)
        ws_news = sh.add_worksheet(title="NEWS", rows=2000, cols=max(20, len(NEWS_HEADERS) + 5))
        ws_news.append_row(NEWS_HEADERS, value_input_option="RAW")

    try:
        ws_meta = sh.worksheet("META")
    except Exception:
        ws_meta = sh.add_worksheet(title="META", rows=200, cols=5)
        ws_meta.append_row(META_HEADERS, value_input_option="RAW")

    return ws_news, ws_meta

def meta_get(ws_meta, key: str):
    rows = ws_meta.get_all_values()
    for r in rows[1:]:
        if len(r) >= 2 and r[0] == key:
            return r[1]
    return ""

def meta_set(ws_meta, key: str, value: str):
    rows = ws_meta.get_all_values()
    for i, r in enumerate(rows[1:], start=2):
        if len(r) >= 1 and r[0] == key:
            ws_meta.update(f"B{i}", [[value]])
            return
    ws_meta.append_row([key, value], value_input_option="RAW")
