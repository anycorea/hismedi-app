KEYWORDS = {
  "보건/공공보건": [
    "보건","질병","감염병","역학","방역","백신","접종","검역",
    "보건소","질병관리청","KDCA","공중보건","건강보험","건보","수가","심평원"
  ],
  "의료/의료정책": [
    "의료","병원","의사","간호사","응급","중환자","진료",
    "의료법","의대","전공의","수련","필수의료",
    "원격의료","비대면진료","의료인력","의료사고","환자안전"
  ],
  "노동/산재/고용": [
    "노동","근로","고용","임금","최저임금","노조","파업",
    "근로기준법","산재","산업재해","중대재해","중대재해처벌법",
    "직장내괴롭힘","노동시간","안전보건","감독","고용노동부"
  ]
}

NEGATIVE_HINTS = ["연예","스포츠","게임","가십","패션"]

RSS_SOURCES = [
    # ─────────────────────────
    # 1. 병원 · 의료 전문 언론
    # ─────────────────────────
    ("병원신문-전체", "https://www.khanews.com/rss/allArticle.xml"),
    ("의학신문-전체", "http://www.bosa.co.kr/rss/allArticle.xml"),
    ("의학신문-병원경영", "http://www.bosa.co.kr/rss/section.xml?section=010"),
    ("청년의사-전체", "https://www.docdocdoc.co.kr/rss/allArticle.xml"),

    # ─────────────────────────
    # 2. 정부기관 (정책·지원·제도 원문)
    # ─────────────────────────
    ("보건복지부-보도자료", "https://www.mohw.go.kr/rss/board.es?mid=a10101000000&bid=0015"),
    ("보건복지부-공지사항", "https://www.mohw.go.kr/rss/board.es?mid=a10401000000&bid=0025"),

    ("고용노동부-보도자료", "https://www.moel.go.kr/rss/news.do"),
    ("고용노동부-정책자료", "https://www.moel.go.kr/rss/policy.do"),
    ("고용노동부-공지사항", "https://www.moel.go.kr/rss/notice.do"),

    # ─────────────────────────
    # 3. Google News RSS (정책·지원 누락 방지용)
    # ─────────────────────────
    (
        "GoogleNews-병원채용지원정책",
        "https://news.google.com/rss/search?q=병원+채용+지원+정책"
    ),
    (
        "GoogleNews-의료인력지원",
        "https://news.google.com/rss/search?q=의료인력+지원+정부"
    ),
    (
        "GoogleNews-간호사인력정책",
        "https://news.google.com/rss/search?q=간호사+인력+지원+정책"
    ),
    (
        "GoogleNews-병원고용노동정책",
        "https://news.google.com/rss/search?q=병원+고용노동부+지원"
    ),

    # ─────────────────────────
    # 4. 네이버 뉴스 검색 RSS (국내 기사 보강)
    # ─────────────────────────
    (
        "NaverNews-병원채용지원",
        "https://rss.naver.com/search/news.xml?query=병원%20채용%20지원"
    ),
    (
        "NaverNews-의료인력정책",
        "https://rss.naver.com/search/news.xml?query=의료인력%20지원%20정책"
    ),
    (
        "NaverNews-간호사인력지원",
        "https://rss.naver.com/search/news.xml?query=간호사%20인력%20지원"
    ),
]

DEFAULTS = {
  "max_hamming": 6,
  "recent_sim_n": 800,
  "fetch_timeout_sec": 10,
  "rss_enabled": True,
  "user_agent": "Mozilla/5.0 (compatible; NewsSheetBot/1.0)"
}
