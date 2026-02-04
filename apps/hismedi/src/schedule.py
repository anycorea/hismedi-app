import streamlit as st
import requests
from bs4 import BeautifulSoup

def _fetch_schedule(url: str):
    if not url:
        return None

    r = requests.get(url, timeout=10)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.doctorInTb")
    if not table:
        return None

    ths = [th.get_text(strip=True) for th in table.select("thead th")]
    tds = [td.get_text(" ", strip=True) for td in table.select("tbody td")]

    if len(ths) < 7 or len(tds) < 7:
        return None

    return dict(zip(ths[:7], tds[:7]))

@st.cache_data(ttl=21600)  # 6시간 캐시
def get_schedule(url: str):
    try:
        return _fetch_schedule(url)
    except Exception:
        return None
