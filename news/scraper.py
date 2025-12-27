import os, re, time, hashlib
from datetime import datetime, timezone
from dateutil import parser as dateparser

import feedparser
import requests
from bs4 import BeautifulSoup

from news.config import KEYWORDS, NEGATIVE_HINTS, RSS_SOURCES, DEFAULTS
from news.gsheet import open_sheet, ensure_tabs, meta_get, meta_set

# ----------------------------
# 유틸
# ----------------------------
def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def canonicalize_url(url: str) -> str:
    url = (url or "").strip()
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"[?&]utm_[^=&]+=[^&]+", "", url)
    url = re.sub(r"\?&", "?", url)
    return url.rstrip("?&")

def sha256_hex(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()


# ----------------------------
# HTML 크롤링 보조
# ----------------------------
def _parse_date_any(s: str) -> str:
    """YYYY.MM.DD / YYYY-MM-DD 형태를 UTC ISO로 변환(실패 시 '')."""
    s = (s or "").strip().replace(".", "-")
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if not m:
        return ""
    try:
        dt = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return ""


# ----------------------------
# 정부/기관/협회: HTML 목록 크롤러
# ----------------------------
def _emit_item(source: str, title: str, link: str, published_at: str):
    title = normalize_ws(title).replace("새글", "").strip()
    link = canonicalize_url(link)
    if not title or not link:
        return None
    tags = pick_tags(title)
    if not tags:
        return None
    return {
        "published_at": published_at,
        "source": source,
        "title": title,
        "url": link,
        "url_canonical": link,
        "tags": ",".join(tags),
    }

def crawl_mohw_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    base = "https://www.mohw.go.kr/board.es?mid=a10503010100&bid=0027"
    out = []
    for p in range(1, max(1, pages) + 1):
        url = f"{base}&nPage={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        for tr in soup.select("table tbody tr"):
            a = tr.select_one("a[href]")
            if not a:
                continue
            href = (a.get("href") or "").strip()
            link = urljoin("https://www.mohw.go.kr/", href)
            title = a.get_text(" ", strip=True)
            published_at = ""
            for td in tr.select("td"):
                published_at = _parse_date_any(td.get_text(" ", strip=True))
                if published_at:
                    break
            it = _emit_item("보건복지부-보도자료", title, link, published_at)
            if it:
                out.append(it)
    return out

def crawl_moel_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    base = "https://www.moel.go.kr/news/enews/report/enewsList.do"
    out = []
    for p in range(1, max(1, pages) + 1):
        url = f"{base}?pageIndex={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        for tr in soup.select("table tbody tr"):
            a = tr.select_one('a[href*="enewsView.do"]')
            if not a:
                continue
            href = (a.get("href") or "").strip()
            link = urljoin("https://www.moel.go.kr/", href)
            title = a.get_text(" ", strip=True)
            published_at = ""
            for td in tr.select("td"):
                published_at = _parse_date_any(td.get_text(" ", strip=True))
                if published_at:
                    break
            it = _emit_item("고용노동부-보도자료", title, link, published_at)
            if it:
                out.append(it)
    return out

def crawl_dailymedi_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    # 데일리메디는 RSS가 불안정/차단되는 경우가 있어 목록 HTML에서 기사 링크를 직접 수집
    # 목록: /news/news_list.php?page=1
    base = "https://www.dailymedi.com/news/news_list.php"
    out = []
    for p in range(1, max(1, pages) + 1):
        url = f"{base}?page={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        # 기사 링크 패턴: /news/news_view.php?wr_id=...
        for a in soup.select('a[href*="news_view.php"]'):
            href = (a.get("href") or "").strip()
            if "wr_id=" not in href:
                continue
            title = a.get_text(" ", strip=True)
            link = urljoin("https://www.dailymedi.com/", href)
            # 날짜는 목록 구조가 자주 바뀌어서: a 주변의 동일 row/parent에서 날짜 탐색
            published_at = ""
            parent_text = a.find_parent().get_text(" ", strip=True) if a.find_parent() else ""
            published_at = _parse_date_any(parent_text)
            it = _emit_item("데일리메디", title, link, published_at)
            if it:
                out.append(it)
    # 중복 제거
    seen=set(); ded=[]
    for it in out:
        if it["url_canonical"] in seen: 
            continue
        seen.add(it["url_canonical"]); ded.append(it)
    return ded

def crawl_ksmha_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    base = "http://www.ksmha.or.kr/bbs/board.php?bo_table=6_6"
    out=[]
    for p in range(1, max(1, pages) + 1):
        url = f"{base}&page={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        # 링크 패턴: wr_id=
        for a in soup.select('a[href*="wr_id="]'):
            href = (a.get("href") or "").strip()
            if "bo_table=6_6" not in href:
                # 상대경로일 수 있음
                pass
            title = a.get_text(" ", strip=True)
            link = urljoin("http://www.ksmha.or.kr/", href)
            published_at = ""
            parent_tr = a.find_parent("tr")
            if parent_tr:
                published_at = _parse_date_any(parent_tr.get_text(" ", strip=True))
            it = _emit_item("대한중소병원협회", title, link, published_at)
            if it:
                out.append(it)
    seen=set(); ded=[]
    for it in out:
        if it["url_canonical"] in seen: 
            continue
        seen.add(it["url_canonical"]); ded.append(it)
    return ded

def crawl_kha_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    # KHA 보도/뉴스 게시판(구조 변동 가능) - href에 bbs/board.php?bo_table=...
    base = "https://www.kha.or.kr"
    list_url = "https://www.kha.or.kr/bbs/board.php"
    out=[]
    for p in range(1, max(1, pages) + 1):
        url = f"{list_url}?bo_table=news&page={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select('a[href*="wr_id="]'):
            href=(a.get("href") or "").strip()
            title=a.get_text(" ", strip=True)
            link=urljoin(base+"/", href)
            published_at=_parse_date_any((a.find_parent("tr").get_text(" ", strip=True) if a.find_parent("tr") else ""))
            it=_emit_item("대한병원협회", title, link, published_at)
            if it: out.append(it)
    seen=set(); ded=[]
    for it in out:
        if it["url_canonical"] in seen: continue
        seen.add(it["url_canonical"]); ded.append(it)
    return ded

def crawl_nhis_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    # NHIS 보도자료 페이지(구조 변동 가능): 링크에 bbsId 또는 articleNo 등이 포함될 수 있어 광범위 탐색
    base = "https://www.nhis.or.kr"
    list_url = "https://www.nhis.or.kr/nhis/together/wbhaec06300m01.do"
    out=[]
    for p in range(1, max(1, pages) + 1):
        url = f"{list_url}?pageIndex={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        # 테이블 기반 우선
        for tr in soup.select("table tbody tr"):
            a = tr.select_one("a[href]")
            if not a:
                continue
            href=(a.get("href") or "").strip()
            title=a.get_text(" ", strip=True)
            link=urljoin(base+"/", href)
            published_at=_parse_date_any(tr.get_text(" ", strip=True))
            it=_emit_item("국민건강보험공단", title, link, published_at)
            if it: out.append(it)
    seen=set(); ded=[]
    for it in out:
        if it["url_canonical"] in seen: continue
        seen.add(it["url_canonical"]); ded.append(it)
    return ded

def crawl_hira_press(ua: str, timeout_sec: int, retries: int, backoff_sec: float, pages: int = 1):
    base = "https://www.hira.or.kr"
    list_url = "https://www.hira.or.kr/bbsDummy.do?pgmid=HIRAA020002000000"
    out=[]
    for p in range(1, max(1, pages) + 1):
        url = f"{list_url}&pageIndex={p}"
        r = http_get(url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
        soup = BeautifulSoup(r.text, "html.parser")
        for tr in soup.select("table tbody tr"):
            a=tr.select_one("a[href]")
            if not a: 
                continue
            href=(a.get("href") or "").strip()
            title=a.get_text(" ", strip=True)
            link=urljoin(base+"/", href)
            published_at=_parse_date_any(tr.get_text(" ", strip=True))
            it=_emit_item("건강보험심사평가원", title, link, published_at)
            if it: out.append(it)
    seen=set(); ded=[]
    for it in out:
        if it["url_canonical"] in seen: continue
        seen.add(it["url_canonical"]); ded.append(it)
    return ded

# ----------------------------
# 태그 분류
# ----------------------------
def pick_tags(text: str):
    t = text or ""
    if any(h in t for h in NEGATIVE_HINTS):
        return []
    tags = []
    for tag, kws in KEYWORDS.items():
        if any(k in t for k in kws):
            tags.append(tag)
    return tags

# ----------------------------
# SimHash (제목 기반)
# ----------------------------
def tokenize(text: str):
    text = normalize_ws(text).lower()
    text = re.sub(r"[^0-9a-z가-힣 ]+", " ", text)
    toks = [w for w in text.split() if len(w) >= 2]
    return toks[:200]

def simhash64(text: str):
    toks = tokenize(text)
    if not toks:
        return ""
    v = [0]*64
    for tok in toks:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        for i in range(64):
            v[i] += 1 if ((h >> i) & 1) else -1
    out = 0
    for i in range(64):
        if v[i] >= 0:
            out |= (1 << i)
    return str(out)

def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()

# ----------------------------
# HTTP GET helper (retry / timeout / UA)
# ----------------------------
def http_get(url: str, ua: str, timeout_sec: int, retries: int, backoff_sec: float):
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout_sec, allow_redirects=True)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_sec * (attempt + 1))
            else:
                break
    raise last_err

# ----------------------------
# 기존 인덱스 로드
# ----------------------------
def load_indexes(ws_news, recent_sim_n: int):
    values = ws_news.get_all_values()
    if len(values) <= 1:
        return set(), set(), []

    body = values[1:]
    url_set = set()
    titlehash_set = set()

    recent = body[-recent_sim_n:] if len(body) > recent_sim_n else body
    recent_sim = []  # (sim_int, url)
    for row in recent:
        url_c = row[4] if len(row) > 4 else ""
        th = row[6] if len(row) > 6 else ""
        sh = row[7] if len(row) > 7 else ""
        url = row[3] if len(row) > 3 else ""
        if url_c:
            url_set.add(url_c)
        if th:
            titlehash_set.add(th)
        if sh.isdigit():
            recent_sim.append((int(sh), url))
    return url_set, titlehash_set, recent_sim

def find_near_duplicate(sim_int: int, recent_sim, max_hamming: int):
    for s, url in recent_sim:
        if hamming(sim_int, s) <= max_hamming:
            return url
    return ""

# ----------------------------
# RSS 수집(UA + requests → feedparser)
# ----------------------------
def collect_rss(ua: str, timeout_sec: int, retries: int, backoff_sec: float, gov_pages: int):
    out = []
    for source_name, feed_url in RSS_SOURCES:
        # HTML 토큰 소스 처리
        if (feed_url or '').startswith('HTML:'):
            try:
                if feed_url == 'HTML:mohw':
                    out.extend(crawl_mohw_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:moel':
                    out.extend(crawl_moel_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:dailymedi':
                    out.extend(crawl_dailymedi_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:ksmha':
                    out.extend(crawl_ksmha_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:kha':
                    out.extend(crawl_kha_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:nhis':
                    out.extend(crawl_nhis_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
                elif feed_url == 'HTML:hira':
                    out.extend(crawl_hira_press(ua, timeout_sec, retries, backoff_sec, pages=gov_pages))
            except Exception:
                pass
            continue

        try:
            r = http_get(feed_url, ua=ua, timeout_sec=timeout_sec, retries=retries, backoff_sec=backoff_sec)
            fp = feedparser.parse(r.content)
        except Exception:
            continue

        for e in getattr(fp, "entries", [])[:50]:
            title = normalize_ws(getattr(e, "title", ""))
            link = canonicalize_url(getattr(e, "link", ""))
            if not title or not link:
                continue

            dt_raw = getattr(e, "published", None) or getattr(e, "updated", None) or getattr(e, "pubDate", None)
            published_at = ""
            if dt_raw:
                try:
                    dt = dateparser.parse(dt_raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    published_at = dt.isoformat()
                except Exception:
                    published_at = ""

            tags = pick_tags(title)
            if not tags:
                continue

            out.append({
                "published_at": published_at,
                "source": source_name,
                "title": title,
                "url": link,
                "url_canonical": link,
                "tags": ",".join(tags),
            })
    return out

# ----------------------------
# 메인
# ----------------------------
def main():
    sh = open_sheet()
    ws_news, ws_meta = ensure_tabs(sh)

    # META 설정값 읽기(없으면 기본값)
    max_hamming = int(meta_get(ws_meta, "max_hamming") or DEFAULTS["max_hamming"])
    recent_sim_n = int(meta_get(ws_meta, "recent_sim_n") or DEFAULTS["recent_sim_n"])
    fetch_timeout_sec = int(meta_get(ws_meta, "fetch_timeout_sec") or DEFAULTS["fetch_timeout_sec"])
    rss_enabled_raw = meta_get(ws_meta, "rss_enabled")
    gov_pages_raw = meta_get(ws_meta, "gov_pages")
    rss_enabled = DEFAULTS["rss_enabled"] if rss_enabled_raw == "" else (rss_enabled_raw.strip().upper() == "TRUE")
    if gov_pages_raw == "":
        meta_set(ws_meta, "gov_pages", str(DEFAULTS.get("gov_pages", 1)))

    ua = DEFAULTS["user_agent"]
    retries = int(DEFAULTS.get("http_retries", 2))
    backoff = float(DEFAULTS.get("http_backoff_sec", 1.2))

    meta_set(ws_meta, "last_run_at", datetime.now(timezone.utc).isoformat())
    meta_set(ws_meta, "last_error", "")

    if not rss_enabled:
        meta_set(ws_meta, "last_inserted_count", "0")
        return

    url_set, titlehash_set, recent_sim = load_indexes(ws_news, recent_sim_n)

    inserted = 0
    new_rows = []

    # 1) RSS(전문지) + HTML 크롤링(정부/공공/협회)
    gov_pages = int(meta_get(ws_meta, "gov_pages") or DEFAULTS.get("gov_pages", 1))
    items = collect_rss(ua=ua, timeout_sec=fetch_timeout_sec, retries=retries, backoff_sec=backoff, gov_pages=gov_pages)

    for it in items:
        title_hash = sha256_hex(normalize_ws(it["title"]).lower())

        if it["url_canonical"] in url_set:
            continue
        if title_hash in titlehash_set:
            continue

        sh_str = simhash64(it["title"])
        dup_of = ""
        if sh_str.isdigit():
            dup_of = find_near_duplicate(int(sh_str), recent_sim, max_hamming)

        # ✅ summary 컬럼 없음(9열)
        row = [
            it["published_at"],
            it["source"],
            it["title"],
            it["url"],
            it["url_canonical"],
            it["tags"],
            title_hash,
            sh_str,
            dup_of,
        ]
        new_rows.append(row)
        inserted += 1

        url_set.add(it["url_canonical"])
        titlehash_set.add(title_hash)
        if sh_str.isdigit():
            recent_sim.append((int(sh_str), it["url"]))
            if len(recent_sim) > recent_sim_n:
                recent_sim = recent_sim[-recent_sim_n:]

        time.sleep(0.12)

    if new_rows:
        ws_news.append_rows(new_rows, value_input_option="RAW")

    meta_set(ws_meta, "last_inserted_count", str(inserted))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # META에 에러 기록
        try:
            sh = open_sheet()
            _, ws_meta = ensure_tabs(sh)
            meta_set(ws_meta, "last_error", repr(e))
            meta_set(ws_meta, "last_run_at", datetime.now(timezone.utc).isoformat())
        except Exception:
            pass
        raise
