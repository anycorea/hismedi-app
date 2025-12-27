# hismedi-app/news/config.py
# -*- coding: utf-8 -*-

from urllib.parse import quote

# ----------------------------
# 키워드(태그) 분류용
# ----------------------------
KEYWORDS = {
    "보건/공공보건": [
        "보건", "질병", "감염병", "역학", "방역", "백신", "접종", "검역",
        "보건소", "질병관리청", "KDCA", "공중보건", "건강보험", "건보", "수가", "심평원"
    ],
    "의료/의료정책": [
        "의료", "병원", "의사", "간호사", "응급", "중환자", "진료",
        "의료법", "의대", "전공의", "수련", "필수의료",
        "원격의료", "비대면진료", "의료인력", "의료사고", "환자안전"
    ],
    "노동/산재/고용": [
        "노동", "근로", "고용", "임금", "최저임금", "노조", "파업",
        "근로기준법", "산재", "산업재해", "중대재해", "중대재해처벌법",
        "직장내괴롭힘", "노동시간", "안전보건", "감독", "고용노동부"
    ],
    # 병원 “직원 채용지원/정책” 관점(채용공고 X, 제도/지원 O)
    "인력/채용지원정책": [
        "인력", "인력난", "인력지원", "채용지원", "채용 지원", "고용지원", "고용 지원",
        "인건비", "인건비 지원", "인건비지원", "일자리", "일자리사업",
        "고용유지", "고용유지지원금", "지원금", "보조금",
        "근무시간", "근로시간", "교대제", "간호 인력", "간호사 인력", "간호인력",
        "전공의 지원", "수련 지원", "수련비", "교육비 지원"
    ],
}

NEGATIVE_HINTS = ["연예", "스포츠", "게임", "가십", "패션"]


# ----------------------------
# RSS 헬퍼
# ----------------------------
def NAVER_NEWS_RSS(query: str, sort_type: int = 1) -> str:
    """
    (구)rss.naver.com/search/... 는 대부분 동작하지 않습니다.
    대신 아래 형태를 사용합니다.
    sort_type=1: 최신순(모니터링에 유리)
    """
    return (
        "https://newssearch.naver.com/search.naver"
        f"?where=rss&sort_type={sort_type}&query={quote(query)}"
    )


def GOOGLE_NEWS_RSS(query: str) -> str:
    """
    Google News RSS는 hl/gl/ceid를 붙이면 한국판 고정이 됩니다.
    (환경에 따라 User-Agent 헤더 필요할 수 있으니 수집 코드에서 DEFAULTS['user_agent']를
    requests 헤더로 실제 적용하세요.)
    """
    return (
        "https://news.google.com/rss/search"
        f"?q={quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    )


# ----------------------------
# 수집 소스
# ----------------------------
RSS_SOURCES = [
    # ─────────────────────────
    # 1. 병원 · 의료 전문 언론
    # ─────────────────────────
    ("병원신문", "https://www.khanews.com/rss/allArticle.xml"),
    ("의학신문-전체", "http://www.bosa.co.kr/rss/allArticle.xml"),
    ("의학신문-병원경영", "http://www.bosa.co.kr/rss/section.xml?section=010"),
    ("청년의사", "https://www.docdocdoc.co.kr/rss/allArticle.xml"),

    # ─────────────────────────
    # 2. 정부기관 (정책·지원·제도 원문)
    # ─────────────────────────
    ("보건복지부-보도자료", "https://www.mohw.go.kr/rss/board.es?mid=a10101000000&bid=0015"),
    ("보건복지부-공지사항", "https://www.mohw.go.kr/rss/board.es?mid=a10401000000&bid=0025"),
    ("고용노동부-보도자료", "https://www.moel.go.kr/rss/news.do"),
    ("고용노동부-정책자료", "https://www.moel.go.kr/rss/policy.do"),

    # ─────────────────────────
    # 3. 네이버 뉴스 RSS (검색 기반 보강)
    #    - "직원 채용 공고"가 아니라 "병원이 활용할 수 있는 인력/채용지원 정책" 중심
    # ─────────────────────────
    ("Naver-병원_채용지원정책", NAVER_NEWS_RSS("병원 채용 지원 정책")),
    ("Naver-의료인력_지원제도", NAVER_NEWS_RSS("의료인력 지원 정책")),
    ("Naver-간호사_인력지원", NAVER_NEWS_RSS("간호사 인력 지원")),
    ("Naver-병원_인건비지원", NAVER_NEWS_RSS("병원 인건비 지원")),
    ("Naver-고용유지지원금_병원", NAVER_NEWS_RSS("고용유지지원금 병원")),
    ("Naver-전공의_수련지원", NAVER_NEWS_RSS("전공의 수련 지원")),

    # ─────────────────────────
    # 4. Google News RSS (백업/누락 방지용)
    #    - 네이버만 쓰고 싶으면 아래 블록을 통째로 삭제해도 됩니다.
    # ─────────────────────────
    ("Google-병원_채용지원정책", GOOGLE_NEWS_RSS("병원 채용 지원 정책")),
    ("Google-의료인력_지원제도", GOOGLE_NEWS_RSS("의료인력 지원 정책")),
    ("Google-간호사_인력지원", GOOGLE_NEWS_RSS("간호사 인력 지원")),
    ("Google-병원_인건비지원", GOOGLE_NEWS_RSS("병원 인건비 지원")),
    ("Google-고용유지지원금_병원", GOOGLE_NEWS_RSS("고용유지지원금 병원")),
    ("Google-전공의_수련지원", GOOGLE_NEWS_RSS("전공의 수련 지원")),
]


# ----------------------------
# 기본 설정
# ----------------------------
DEFAULTS = {
    "max_hamming": 6,
    "recent_sim_n": 800,
    "fetch_timeout_sec": 10,
    "rss_enabled": True,
    "user_agent": "Mozilla/5.0 (compatible; NewsSheetBot/1.0; +https://example.invalid/bot)",
}
