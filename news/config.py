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
# 검색 쿼리(보강용) — 폭넓게 커버
# ----------------------------
# Google News RSS / Naver News API(권장) 모두 같은 쿼리를 사용합니다.
SEARCH_QUERIES = [
    "(보건 OR 의료 OR 병원 OR 의원 OR 진료 OR 환자 OR 감염병 OR 백신) (정책 OR 제도 OR 지원 OR 대책 OR 지침)",
    "(의사 OR 간호사 OR 의료기사 OR 전공의 OR 수련) (정책 OR 제도 OR 지원 OR 처우 OR 인력 OR 수가)",
    "(병원 OR 의료) (고용 OR 채용 OR 인건비 OR 고용유지지원금 OR 노동 OR 근로 OR 임금 OR 산재 OR 근로기준법) (정책 OR 제도 OR 지원)",
    "(환자안전 OR 의료사고 OR 의료법 OR 응급 OR 중환자) (대책 OR 개정 OR 정책 OR 제도)",
]

# ----------------------------
# Google News RSS
# ----------------------------
def GOOGLE_NEWS_RSS(query: str) -> str:
    # 한국 고정 파라미터 포함
    return f"https://news.google.com/rss/search?q={quote(query)}&hl=ko&gl=KR&ceid=KR:ko"

GOOGLE_SOURCES = [(f"GoogleNews-{i+1}", GOOGLE_NEWS_RSS(q)) for i, q in enumerate(SEARCH_QUERIES)]

# ----------------------------
# Naver News는 RSS가 아니라 API를 권장(가장 안정적)
# - scraper.py에서 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 있으면 호출합니다.
# ----------------------------
NAVER_API_QUERIES = [
    "보건 정책",
    "의료 정책",
    "병원 진료 정책",
    "의료인력 지원",
    "간호사 인력 지원",
    "의사 인력 정책",
    "의료기사 처우",
    "환자안전 대책",
    "고용 지원 병원",
    "채용 지원 병원",
    "인건비 지원 병원",
    "고용유지지원금 병원",
    "근로기준법 병원",
    "산재 병원",
]

# ----------------------------
# RSS 고정 소스(전문지/정부 원문)
# ----------------------------
RSS_SOURCES = [
    # 1) 병원·의료 전문 언론
    ("병원신문", "https://www.khanews.com/rss/allArticle.xml"),
    ("의학신문-전체", "http://www.bosa.co.kr/rss/allArticle.xml"),
    ("의학신문-병원경영", "http://www.bosa.co.kr/rss/section.xml?section=010"),
    ("청년의사", "https://www.docdocdoc.co.kr/rss/allArticle.xml"),

    # 2) 정부기관(원문)
    ("보건복지부-보도자료", "https://www.mohw.go.kr/rss/board.es?mid=a10101000000&bid=0015"),
    ("보건복지부-공지사항", "https://www.mohw.go.kr/rss/board.es?mid=a10401000000&bid=0025"),
    ("고용노동부-보도자료", "https://www.moel.go.kr/rss/news.do"),
    ("고용노동부-정책자료", "https://www.moel.go.kr/rss/policy.do"),
    ("고용노동부-공지사항", "https://www.moel.go.kr/rss/notice.do"),
] + GOOGLE_SOURCES

# ----------------------------
# 기본 설정
# ----------------------------
DEFAULTS = {
    "max_hamming": 6,
    "recent_sim_n": 800,
    "fetch_timeout_sec": 10,
    "rss_enabled": True,
    # 일부 환경에서 RSS가 403/리다이렉트 나는 것을 줄이기 위해 UA는 꼭 씁니다.
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36 (compatible; NewsSheetBot/1.0)",
    # requests 재시도/백오프(스크래퍼에서 사용)
    "http_retries": 2,
    "http_backoff_sec": 1.2,
}
