import re, time, hashlib
from datetime import datetime, timezone
from dateutil import parser as dateparser

import feedparser
import requests
from bs4 import BeautifulSoup

from news.config import KEYWORDS, NEGATIVE_HINTS, RSS_SOURCES, DEFAULTS
from news.gsheet import open_sheet, ensure_tabs, meta_get, meta_set

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

def pick_tags(text: str):
    t = text or ""
    if any(h in t for h in NEGATIVE_HINTS):
        return []
    tags = []
    for tag, kws in KEYWORDS.items():
        if any(k in t for k in kws):
            tags.append(tag)
    return tags

def tokenize(text: str):
    text = normalize_ws(text).lower()
    text = re.sub(r"[^0-9a-z가-힣 ]+", " ", text)
    toks = [w for w in text.split() if len(w) >= 2]
    return toks[:500]

def simhash64(text: str):
    toks = tokenize(text)
    if not toks:
        return ""
    v = [0]*64
    for tok in toks:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        for i in range(64):
            v[i] += 1 if ((h>>i)&1) else -1
    out = 0
    for i in range(64):
        if v[i] >= 0:
            out |= (1<<i)
    return str(out)

def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()

def fetch_article_text(url: str, ua: str, timeout_sec: int) -> str:
    try:
        r = requests.get(url, headers={"User-Agent": ua}, timeout=timeout_sec)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        for t in soup(["script","style","noscript"]):
            t.decompose()
        text = soup.get_text("\n")
        lines = [normalize_ws(x) for x in text.split("\n")]
        lines = [x for x in lines if len(x) >= 20]
        return "\n".join(lines[:200])
    except Exception:
        return ""

def summarize_local(text: str) -> str:
    text = normalize_ws(text)
    sents = re.split(r"[.!?。]\s+", text)
    sents = [normalize_ws(s) for s in sents if len(s) >= 20]
    return " ".join(sents[:4])[:800]

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
        if url_c: url_set.add(url_c)
        if th: titlehash_set.add(th)
        if sh.isdigit(): recent_sim.append((int(sh), url))
    return url_set, titlehash_set, recent_sim

def find_near_duplicate(sim_int: int, recent_sim, max_hamming: int):
    for s, url in recent_sim:
        if hamming(sim_int, s) <= max_hamming:
            return url
    return ""

def collect_rss():
    out = []
    for source_name, feed_url in RSS_SOURCES:
        fp = feedparser.parse(feed_url)
        for e in fp.entries[:50]:
            title = normalize_ws(getattr(e, "title", ""))
            link = canonicalize_url(getattr(e, "link", ""))
            if not title or not link:
                continue

            dt_raw = getattr(e, "published", None) or getattr(e, "updated", None)
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

    meta_set(ws_meta, "last_run_at", datetime.now(timezone.utc).isoformat())
    meta_set(ws_meta, "last_error", "")

    if not rss_enabled:
        meta_set(ws_meta, "last_inserted_count", "0")
        return

    url_set, titlehash_set, recent_sim = load_indexes(ws_news, recent_sim_n=recent_sim_n)

    inserted = 0
    new_rows = []

    items = collect_rss()
    for it in items:
        title_hash = sha256_hex(normalize_ws(it["title"]).lower())

        if it["url_canonical"] in url_set:
            continue
        if title_hash in titlehash_set:
            continue

        content = fetch_article_text(it["url"], ua=ua, timeout_sec=fetch_timeout_sec)
        summary = summarize_local(content)
        sh_str = simhash64(it["title"] + " " + summary)

        dup_of = ""
        if sh_str.isdigit():
            dup_of = find_near_duplicate(int(sh_str), recent_sim, max_hamming=max_hamming)

        row = [
            it["published_at"], it["source"], it["title"], it["url"], it["url_canonical"],
            it["tags"], title_hash, sh_str, dup_of, summary
        ]
        new_rows.append(row)
        inserted += 1

        url_set.add(it["url_canonical"])
        titlehash_set.add(title_hash)
        if sh_str.isdigit():
            recent_sim.append((int(sh_str), it["url"]))
            if len(recent_sim) > recent_sim_n:
                recent_sim = recent_sim[-recent_sim_n:]

        time.sleep(0.2)

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
