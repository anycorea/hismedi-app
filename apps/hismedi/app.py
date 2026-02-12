import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from src.sheets import load_departments

# 페이지 기본 설정 (타이틀, 레이아웃)
st.set_page_config(page_title="히즈메디병원", layout="wide")

# 상수 및 함수 정의
CALL = "1588-0223"  # 병원 대표 번호

# None 값을 빈 문자열로 변환하고, 문자열 앞뒤 공백 제거
S = lambda x: "" if x is None else str(x).strip()

# 문자열을 소문자로 변환하여 True/False 값 판단
def ok(x):
    v = S(x).lower()
    return v in ("true", "1", "y", "yes") if v in ("true","false","1","0","y","n","yes","no","") else bool(x)

# URL에 'sidx' 파라미터 추가 또는 업데이트
def sidx(url, did):
    url = S(url)
    if not url.startswith("http"): return None
    u = urlparse(url); q = parse_qs(u.query)
    if S(did): q["sidx"] = [S(did)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

# URL에 앵커(#) 추가
def anc(url, a): return None if not url else (url if a in url else url + a)

# HTML 버튼 생성 함수
def A(lbl, url, cls):
    if url and url.startswith("http"):
        return f'<a class="hm-btn {cls}" href="{url}" target="_blank" rel="noopener noreferrer">{lbl}</a>'
    return f'<span class="hm-btn {cls} hm-dis">{lbl}</span>'

# CSS 스타일 정의 (폰트 크기, 디자인 변경)
st.markdown("""
<style>
/* 전체 폰트 사이즈 설정 */
body {
    font-size: 16px;
}

/* 상단 여백 조정 */
header {
    padding-top: calc(env(safe-area-inset-top) + 0.6rem);
}

/* 블록 컨테이너 여백 조정 */
div.block-container {
    padding-top: 0.6rem;
    padding-bottom: 2rem;
}

/* 전화 걸기 버튼 스타일 */
.hm-call {
    display: block;
    margin: 0.2rem 0 0.55rem;
    padding: 12px;
    border-radius: 14px;
    text-decoration: none;
    font-weight: 900;
    text-align: center;
    border: 1px solid rgba(49, 51, 63, 0.18);
    background: rgba(49, 51, 63, 0.05);
    color: inherit;
    transition: background-color 0.3s ease; /* 호버 효과 추가 */
}

.hm-call:hover {
    background-color: rgba(49, 51, 63, 0.15); /* 호버 시 배경색 변경 */
}

/* 정보 섹션 스타일 */
.hm-info {
    margin: 0.35rem 0 1rem;
    padding: 1.1rem 1rem;  /* 패딩 값 증가 */
    border-radius: 15px;  /* border-radius 값 증가 */
    border: 1px solid rgba(49, 51, 63, 0.10);
    background: rgba(49, 51, 63, 0.02);
    font-size: 1rem;  /* 폰트 크기 증가 */
    line-height: 1.6;  /* 줄 간격 조정 */
    color: rgba(49, 51, 63, 0.86);
}

.hm-info .title {
    font-weight: bold;  /* 폰트 굵게 */
    margin-bottom: 0.5rem;
    font-size: 1.2rem;  /* 제목 폰트 크기 증가 */
}

.hm-info .section {
    margin-top: 1rem;
    padding-top: 0.8rem;
    border-top: 1px solid rgba(49, 51, 63, 0.08);
}

.hm-info .label {
    font-weight: 900;
}

.hm-info ul {
    margin: 0.5rem 0 0 1.5rem;  /* 들여쓰기 조정 */
    padding-left: 0; /* 기본 ul 스타일 제거 */
    list-style-type: disc; /* 글머리 기호 추가 */
}

.hm-info li {
    margin: 0.3rem 0;
}

.hm-info .muted {
    color: rgba(49, 51, 63, 0.66);
    font-size: 0.9rem;
}

/* 진료과 스타일 */
.hm-dept {
    padding: 16px 0;  /* 패딩 값 증가 */
    border-bottom: 1px solid rgba(49, 51, 63, 0.08);
}

.hm-title {
    font-size: 1.3rem;  /* 폰트 크기 증가 */
    font-weight: bold;
    margin: 0 0 12px;
}

.hm-row {
    display: flex;
    gap: 12px;  /* 간격 증가 */
    flex-wrap: nowrap;
    width: 100%;
}

/* 버튼 스타일 */
.hm-btn {
    flex: 1 1 0;
    text-align: center;
    padding: 12px 10px;  /* 패딩 값 증가 */
    border-radius: 12px;  /* border-radius 값 증가 */
    white-space: nowrap;
    text-decoration: none;
    font-weight: 800;
    font-size: 1rem;  /* 폰트 크기 증가 */
    color: inherit;
    border: 1px solid rgba(49, 51, 63, 0.18);
    background: rgba(49, 51, 63, 0.02);
    transition: background-color 0.3s ease; /* 호버 효과 추가 */
}

.hm-btn:hover {
    background-color: rgba(49, 51, 63, 0.15); /* 호버 시 배경색 변경 */
}

.hm-r {
    border-color: rgba(255, 75, 75, 0.65);
}

.hm-dis {
    opacity: 0.45;
    cursor: not-allowed;
}

.hm-sub {
    margin-top: 10px;
    font-size: 0.9rem;
    color: rgba(49, 51, 63, 0.55);
}
</style>
""", unsafe_allow_html=True)

# 약간의 여백 추가 (선택 사항)
st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

# 제목 및 안내 문구
st.title("히즈메디병원")
st.markdown(f'<a class="hm-call" href="tel:{CALL}">📞 대표번호 전화하기 · {CALL}</a>', unsafe_allow_html=True)

# 병원 정보
st.markdown(f"""
<div class="hm-info">
  <div class="title">안내</div>

  <div>
    <span class="label">직원 추천 진료과</span> <b>[외과]</b><br/>
    하지정맥류·탈장 진료: <b>최영수 과장</b>
  </div>

  <div class="section">
    <span class="label">진료시간</span> <span class="muted">(소아청소년과 제외)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 17:30</li>
      <li><b>토요일</b> 08:30 ~ 12:30</li>
      <li><b>점심</b> 12:30 ~ 13:30</li>
    </ul>
  </div>

  <div class="section">
    <span class="label">온라인 예약 신청 절차</span>
    <ul>
      <li>예약 신청 → 전문 상담원 콜백 → 예약 확정</li>
      <li>상담원과 통화 후 예약이 확정됩니다.</li>
      <li>야간 및 주말에는 연락드리지 않습니다.</li>
      <li>당일 예약은 불가하며, 익일부터 가능합니다.</li>
    </ul>
  </div>

  <div class="section">
    <span class="label">소아청소년과 안내</span> <span class="muted">(예약 없이 당일 진료 / 달빛어린이병원)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 23:00</li>
      <li><b>주말·공휴일</b> 09:00 ~ 18:00</li>
      <li>영유아검진·검사 예약: <b>☎ {CALL}</b></li>
    </ul>
  </div>
</div>
""", unsafe_allow_html=True)

# 진료과 정보 로드
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다."); st.stop()

# 데이터프레임 컬럼 확인 및 초기화
for c in ("dept_id","dept_name","dept_reservation_url","dept_schedule_detail_url","display_order","is_active"):
    if c not in df.columns: df[c] = ""

# 활성 진료과 필터링
df = df[df["is_active"].apply(ok)]

# 정렬
if "display_order" in df.columns:
    df = df.sort_values("display_order", na_position="last")

# 각 진료과 정보 표시
for i, (_, r) in enumerate(df.iterrows()):
    did = r.get("dept_id")
    name = S(r.get("dept_name")) or "진료과"
    ped = "소아청소년과" in name.replace(" ", "")

    reserve = None if ped else anc(sidx(r.get("dept_reservation_url"), did), "#boardfrm")
    doc_sched = sidx(r.get("dept_schedule_detail_url"), did) if S(r.get("dept_schedule_detail_url")).startswith("http") else S(r.get("dept_schedule_detail_url"))

    st.markdown(f"""
<div class="hm-dept">
  <div class="hm-title">{name}</div>
  <div class="hm-row">
    {A("예약", reserve, "hm-r")}
    {A("의사정보·진료시간표", doc_sched, "")}
  </div>
  {('<div class="hm-sub">예약 없이 당일진료</div>' if ped else '')}
</div>
""", unsafe_allow_html=True)
