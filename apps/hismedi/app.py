import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from src.sheets import load_departments

# 페이지 기본 설정
st.set_page_config(page_title="히즈메디병원", layout="wide")

# 상수 및 함수 정의 (이전 코드와 동일)
CALL = "1588-0223"
S = lambda x: "" if x is None else str(x).strip()
ok = lambda x: x.lower() in ("true", "1", "y", "yes") if isinstance(x, str) else bool(x)
sidx = lambda url, did: urlunparse((urlparse(url).scheme, urlparse(url).netloc, urlparse(url).path, urlparse(url).params, urlencode({"sidx": [did]}, doseq=True), urlparse(url).fragment)) if url and url.startswith("http") and did else None
anc = lambda url, a: url + a if url and a not in url else url if url else None

# HTML 버튼 생성 함수 (스타일 변경)
def A(lbl, url, cls):
    if url and url.startswith("http"):
        return f'<a class="hm-btn {cls}" href="{url}" target="_blank" rel="noopener noreferrer">{lbl}</a>'
    return f'<span class="hm-btn {cls} hm-dis">{lbl}</span>'

# CSS 스타일 정의 (폰트, 색상, 버튼 스타일 조정)
st.markdown("""
<style>
/* 전체 폰트 사이즈 및 색상 설정 */
body {
    font-size: 16px;
    color: #333;
    font-family: sans-serif; /* 기본 폰트 변경 */
}

/* 초기화 스타일 */
ul {
    list-style: none;
    padding: 0;
    margin: 0;
}

/* 상단 여백 제거 (최대) */
.appview-container .main .block-container {
    padding-top: 0.2rem !important;
    margin-top: -2rem !important;
}

/* Streamlit 앱 전체 컨테이너 조정 */
.stApp {
    margin-top: -40px;
}

/* 제목 스타일 */
h1 {
    font-size: 28px !important;
    font-weight: bold;
    color: #3498db; /* 색상 변경 */
    margin-bottom: 10px;
    text-align: center; /* 가운데 정렬 */
}

/* 대표번호 전화하기 스타일 */
.hm-call {
    display: block;
    margin: 10px auto; /* 가운데 정렬 및 상하 간격 */
    padding: 12px 20px;
    border-radius: 8px;
    text-decoration: none;
    font-weight: bold;
    text-align: center;
    border: 2px solid #3498db; /* 테두리 색상 변경 */
    background: white;
    color: #3498db; /* 글자 색상 변경 */
    transition: background-color 0.3s ease, color 0.3s ease;
    font-size: 18px; /* 크게 */
    width: 80%; /* 너비 조정 */
    max-width: 400px; /* 최대 너비 설정 */
}

.hm-call:hover {
    background-color: #3498db;
    color: white;
}

/* 안내 문구 스타일 */
.hm-info {
    margin: 1rem auto;
    padding: 1rem;
    border-radius: 10px;
    border: 1px solid #ddd;
    background: #f9f9f9;
    font-size: 1rem;
    line-height: 1.6;
    color: #555;
    width: 90%;
    max-width: 600px;
}

.hm-info .title {
    font-size: 1.2rem;
    font-weight: bold;
    color: #333;
    margin-bottom: 0.5rem;
}

.hm-info .section {
    margin-top: 1rem;
    padding-top: 0.8rem;
    border-top: 1px solid #eee;
}

.hm-info .label {
    font-weight: bold;
    color: #333;
}

.hm-info ul {
    margin-left: 1.5rem;
}

.hm-info li {
    margin-bottom: 0.3rem;
}

.hm-info .muted {
    color: #777;
    font-size: 0.9rem;
}

/* 진료과 선택 안내 문구 스타일 */
.hm-dept-info {
    font-size: 1rem;
    color: #555;
    margin-bottom: 1rem;
    text-align: center;
}

/* Expander 스타일 조정 */
.streamlit .stExpander {
    border: 1px solid #ddd;
    border-radius: 8px;
    margin-bottom: 0.5rem;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

.streamlit .stExpander:last-child {
    margin-bottom: 0;
}

.streamlit .stExpander > div[data-baseweb="expandable-container"] > div {
    padding: 1rem;
}

/* 진료과 이름 스타일 조정 (Expander summary) */
.streamlit .stExpander > div[data-baseweb="expandable-container"] > div[data-testid="stExpanderInnerContainer"] > summary {
    font-size: 1.1rem;
    font-weight: 600;
    color: #333;
}

/* Expander 내부 hm-dept 스타일 조정 */
.streamlit .stExpander .hm-dept {
    padding: 0;
    border-bottom: none;
}

.streamlit .stExpander .hm-row {
    display: flex;
    flex-direction: column;
    align-items: stretch; /* 버튼 너비를 맞춤 */
    gap: 0.5rem; /* 버튼 간 위아래 간격 */
    margin-top: 0.5rem;
}

/* 버튼 스타일 (글자 크기, 굵기 조정) */
.hm-btn {
    display: block;
    padding: 12px 15px;
    border-radius: 6px;
    text-decoration: none;
    font-weight: 600;
    font-size: 1rem; /* 글자 크기 키움 */
    color: #333;
    border: 1px solid #ccc;
    background: #f0f0f0;
    transition: background-color 0.3s ease;
    text-align: center;
    width: 100%; /* Expander 너비에 맞춤 */
    box-sizing: border-box; /* 패딩, border 포함 */
}

.hm-btn:hover {
    background-color: #ddd;
}

.hm-r {
    border-color: #e74c3c;
    color: #e74c3c;
}

.hm-r:hover {
    background-color: #e74c3c;
    color: white;
}

.hm-dis {
    opacity: 0.5;
    cursor: not-allowed;
}

.hm-sub {
    margin-top: 0.5rem;
    font-size: 0.9rem;
    color: #777;
    text-align: center;
}
</style>
""", unsafe_allow_html=True)

# 초기 화면 설정
st.markdown("# 히즈메디병원")
st.markdown(f'<a class="hm-call" href="tel:{CALL}">📞 대표번호 전화하기 · {CALL}</a>', unsafe_allow_html=True)

# 안내 문구
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

# 진료과 선택 안내 문구 추가
st.markdown('<div class="hm-dept-info">아래 진료과를 선택하시면 예약, 의사정보 등을 볼 수 있습니다.</div>', unsafe_allow_html=True)

# 데이터 로드 및 전처리
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다.");
    st.stop()

for c in ("dept_id", "dept_name", "dept_reservation_url", "dept_schedule_detail_url", "display_order", "is_active"):
    if c not in df.columns:
        df[c] = ""

df = df[df["is_active"].apply(ok)]
if "display_order" in df.columns:
    df = df.sort_values("display_order", na_position="last")

# 컬럼 나누기
cols = st.columns(3)

# 각 진료과 정보를 컬럼에 번갈아 배치
for i, (_, r) in enumerate(df.iterrows()):
    col = cols[i % 3]
    did = r.get("dept_id")
    name = S(r.get("dept_name")) or "진료과"
    ped = "소아청소년과" in name.replace(" ", "")

    reserve = None if ped else anc(sidx(r.get("dept_reservation_url"), did), "#boardfrm")
    doc_sched = sidx(r.get("dept_schedule_detail_url"), did) if S(r.get("dept_schedule_detail_url")).startswith("http") else S(r.get("dept_schedule_detail_url"))

    with col:
        with st.expander(name):
            st.markdown(f"""
            <div class="hm-dept">
              <div class="hm-row">
                {A("예약", reserve, "hm-r")}
                {A("의사정보·진료시간표", doc_sched, "")}
              </div>
              {('<div class="hm-sub">예약 없이 당일진료</div>' if ped else '')}
            </div>
            """, unsafe_allow_html=True)
