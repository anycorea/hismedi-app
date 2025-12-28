# hismedi-app/news/config.py
# -*- coding: utf-8 -*-

from urllib.parse import quote

# ----------------------------
# 키워드(태그) 분류용
# ----------------------------
KEYWORDS = {
    "보건/공공보건": [
        "보건","질병","감염병","역학","방역","백신","접종","검역",
        "보건소","질병관리청","KDCA","공중보건","건강보험","건보","수가","심평원"
    ],
    "의료/의료정책": [
        "의료","병원","의원","의사","간호사","의료기사","응급","중환자","진료","환자",
        "의료법","의대","전공의","수련","필수의료",
        "원격의료","비대면진료","의료인력","의료사고","환자안전"
    ],
    "노동/산재/고용": [
        "노동","근로","고용","임금","최저임금","노조","파업",
        "근로기준법","산재","산업재해","중대재해","중대재해처벌법",
        "직장내괴롭힘","노동시간","안전보건","감독","고용노동부"
    ],
    "인력/채용지원정책": [
        "인력","인력난","인력지원","채용지원","고용지원","고용 지원",
        "인건비","인건비 지원","일자리","일자리사업",
        "고용유지","고용유지지원금","지원금","보조금",
        "근무시간","근로시간","교대제",
        "전공의","수련","수련비","교육비 지원"
    ],
}

NEGATIVE_HINTS = ["연예","스포츠","게임","가십","패션"]

# ----------------------------
# RSS 고정 소스(전문지/정부 원문)
# ----------------------------
RSS_SOURCES = [
    # 정부·공공기관
    ("보건복지부(보도)", "http://www.mohw.go.kr/rss/board.es?mid=a10503000000&bid=0027"),
    ("식약처(약/기기)", "https://www.mfds.go.kr/rss/news.do"),
    ("심평원(공지)", "https://www.hira.or.kr/rss/board.do?bid=notice"),
    ("고용노동부(정책)", "https://www.moel.go.kr/rss/policy.do"),

    # 병원·의료 전문 언론(직접 RSS)
    ("병원신문", "https://www.khanews.com/rss/allArticle.xml"),
    ("의학신문", "http://www.bosa.co.kr/rss/allArticle.xml"),
    ("청년의사", "https://www.docdocdoc.co.kr/rss/allArticle.xml"),
    ("의협신문", "https://www.doctorsnews.co.kr/rss/allArticle.xml"),
    ("데일리메디", "https://news.google.com/rss/search?q=source:데일리메디+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("메디게이트", "https://news.google.com/rss/search?q=source:메디게이트뉴스+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("메디칼타임즈", "https://news.google.com/rss/search?q=source:메디칼타임즈+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("라포르시안", "https://news.google.com/rss/search?q=source:라포르시안+when:7d&hl=ko&gl=KR&ceid=KR:ko"),

    # 병원·의료 전문 언론(최근 7일)
    ("데일리메디(G)", "https://news.google.com/rss/search?q=source:데일리메디+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("메디게이트(G)", "https://news.google.com/rss/search?q=source:메디게이트뉴스+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("메디칼타임즈(G)", "https://news.google.com/rss/search?q=source:메디칼타임즈+when:7d&hl=ko&gl=KR&ceid=KR:ko"),
    ("라포르시안(G)", "https://news.google.com/rss/search?q=source:라포르시안+when:7d&hl=ko&gl=KR&ceid=KR:ko"), 
]

# ----------------------------
# 기본 설정
# ----------------------------
DEFAULTS = {
    "max_hamming": 6,
    "recent_sim_n": 800,
    "fetch_timeout_sec": 10,
    # HTML 크롤링 페이지 수(1페이지=최신 약 10~20건)
    "gov_pages": 1,
    "rss_enabled": True,
    # 일부 환경에서 RSS가 403/리다이렉트 나는 것을 줄이기 위해 UA는 꼭 씁니다.
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36 (compatible; NewsSheetBot/1.0)",
    # requests 재시도/백오프(스크래퍼에서 사용)
    "http_retries": 2,
    "http_backoff_sec": 1.2,
}
