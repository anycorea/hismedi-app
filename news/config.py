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
  ("SBS-속보", "https://news.sbs.co.kr/news/newsflashRssFeed.do?plink=RSSREADER"),
  ("연합뉴스TV-사회", "http://www.yonhapnewstv.co.kr/category/news/society/feed/"),
  ("연합뉴스TV-최신", "http://www.yonhapnewstv.co.kr/browse/feed/"),
  ("한겨레-전체", "http://www.hani.co.kr/rss/"),
]

DEFAULTS = {
  "max_hamming": 6,
  "recent_sim_n": 800,
  "fetch_timeout_sec": 10,
  "rss_enabled": True,
  "user_agent": "Mozilla/5.0 (compatible; NewsSheetBot/1.0)"
}
