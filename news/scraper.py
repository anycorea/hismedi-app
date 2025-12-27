import os, re, time, hashlib
from datetime import datetime, timezone
from dateutil import parser as dateparser

import feedparser
import requests

from news.config import KEYWORDS, NEGATIVE_HINTS, RSS_SOURCES, DEFAULTS, NAVER_API_QUERIES
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
# HTTP GET (재시도 포함) — RSS/GoogleNews 안정화
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
def collect_rss(ua: str, timeout_sec: int, retries: int, backoff_sec: float):
    out = []
    for source_name, feed_url in RSS_SOURCES:
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
# Naver News Search API (가장 안정적인 네이버 보강)
# ----------------------------
def collect_naver_news_api(ua: str, timeout_sec: int):
    cid = os.getenv("NAVER_CLIENT_ID", "").strip()
    csec = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not cid or not csec:
        return []

    out = []
    headers = {
        "X-Naver-Client-Id": cid,
        "X-Naver-Client-Secret": csec,
        "User-Agent": ua,
    }

    for q in NAVER_API_QUERIES:
        try:
            r = requests.get(
                "https://openapi.naver.com/v1/search/news.json",
                headers=headers,
                params={"query": q, "display": 100, "sort": "date"},
                timeout=timeout_sec,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for it in data.get("items", []):
            title = normalize_ws(re.sub(r"<.*?>", "", it.get("title", "")))
            link = canonicalize_url(it.get("originallink") or it.get("link") or "")
            if not title or not link:
                continue

            published_at = ""
            dt_raw = it.get("pubDate")
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
                "source": "NaverAPI",
                "title": title,
                "url": link,
                "url_canonical": link,
                "tags": ",".join(tags),
            })

        time.sleep(0.12)

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
    rss_enabled = DEFAULTS["rss_enabled"] if rss_enabled_raw == "" else (rss_enabled_raw.strip().upper() == "TRUE")

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

    # 1) RSS(전문지/정부) + Google News RSS(검색 보강)
    items = collect_rss(ua=ua, timeout_sec=fetch_timeout_sec, retries=retries, backoff_sec=backoff)
    # 2) Naver는 RSS 대신 API(키가 있을 때만)
    items += collect_naver_news_api(ua=ua, timeout_sec=fetch_timeout_sec)

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
