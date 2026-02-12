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
    font-size: 15px;
    color: #333; /* 기본 글자색 */
}

/* 페이지 상단 여백 제거 (더욱 줄임) */
.appview-container .main .block-container {
    padding-top: 0.5rem !important; /* 더 적은 값으로 조정 */
}

/* 제목 스타일 */
h1 {
    font-size: 24px !important;
    font-weight: bold;
    color: #377ba8; /* 제목 색상 */
    margin-bottom: 12px; /* 간격 조정 */
}

/* 대표번호 전화하기 스타일 (이전 스타일로 복원) */
.hm-call {
    display: block;
    margin: 0.2rem 0 0.55rem;
    padding: 8px; /* 패딩 값 조정 */
    border-radius: 12px;
    text-decoration: none;
    font-weight: 900;
    text-align: center;
    border: 1px solid rgba(49, 51, 63, 0.18);
    background: rgba(49, 51, 63, 0.05);
    color: inherit;
    transition: background-color 0.3s ease;
    font-size: 14px; /* 폰트 크기 조정 */
}

.hm-call:hover {
    background-color: rgba(49, 51, 63, 0.15);
}

/* 안내 문구 스타일 */
.hm-info {
    margin: 0.5rem 0 1.5rem;
    padding: 0.8rem; /* 패딩 값 조정 */
    border-radius: 12px;
    border: 1px solid rgba(49, 51, 63, 0.1);
    background: rgba(49, 51, 63, 0.02);
    font-size: 0.9rem;
    line-height: 1.5;
    color: rgba(49, 51, 63, 0.8);
}

.hm-info .title {
    font-size: 1.0rem; /* 폰트 크기 조정 */
    font-weight: bold;
    color: #555; /* 소제목 색상 */
    margin-bottom: 0.4rem; /* 간격 조정 */
}

.hm-info .section {
    margin-top: 0.6rem; /* 간격 조정 */
    padding-top: 0.5rem; /* 패딩 값 조정 */
    border-top: 1px solid rgba(49, 51, 63, 0.08);
}

.hm-info .label {
    font-weight: bold;
}

.hm-info ul {
    margin: 0.3rem 0 0 1.2rem; /* 간격 조정 */
    padding-left: 0;
    list-style-type: disc;
}

.hm-info li {
    margin: 0.15rem 0; /* 간격 조정 */
}

.hm-info .muted {
    color: rgba(49, 51, 63, 0.6);
    font-size: 0.8rem;
}

/* 진료과 선택 안내 문구 스타일 */
.hm-dept-info {
    font-size: 0.9rem;
    color: #777;
    margin-bottom: 0.8rem; /* 간격 조정 */
    text-align: center;
}

/* 버튼 스타일 (글자 크기, 굵기 조정) */
.hm-btn {
    flex: 1 1 0;
    text-align: center;
    padding: 0.5rem 0.4rem; /* 패딩 값 조정 */
    border-radius: 8px;
    white-space: nowrap;
    text-decoration: none;
    font-weight: 600; /* 굵기 조정 */
    font-size: 0.7rem; /* 크기 조정 */
    color: inherit;
    border: 1px solid rgba(49, 51, 63, 0.15);
    background: rgba(49, 51, 63, 0.03);
    transition: background-color 0.3s ease;
}

.hm-btn:hover {
    background-color: rgba(49, 51, 63, 0.1);
}

.hm-r {
    border-color: rgba(255, 75, 75, 0.5);
}

.hm-dis {
    opacity: 0.45;
    cursor: not-allowed;
}

.hm-sub {
    margin-top: 0.4rem; /* 간격 조정 */
    font-size: 0.7rem; /* 크기 조정 */
    color: rgba(49, 51, 63, 0.5);
}

/* Expander 스타일 조정 */
.streamlit .stExpander {
    border: 1px solid rgba(49, 51, 63, 0.1);
    border-radius: 8px; /* 둥글기 조정 */
    margin-bottom: 0.4rem; /* Expander 간 간격 조정 */
}

.streamlit .stExpander:last-child {
    margin-bottom: 0; /* 마지막 Expander 간 간격 제거 */
}

.streamlit .stExpander > div[data-baseweb="expandable-container"] > div {
    padding: 0.6rem; /* 내용 padding 조정 */
}

/* 진료과 이름 스타일 조정 (Expander summary) */
.streamlit .stExpander > div[data-baseweb="expandable-container"] > div[data-testid="stExpanderInnerContainer"] > summary {
    font-size: 0.9rem; /* 폰트 크기 조정 */
    font-weight: bold; /* 폰트 굵기 조정 */
    color: #444; /* 폰트 색상 조정 */
}

/* Expander 내부 hm-dept 스타일 조정 (겹침 문제 해결) */
.streamlit .stExpander .hm-dept {
    padding: 0; /* 내부 padding 제거 */
    border-bottom: none; /* border 제거 */
}

.streamlit .stExpander .hm-row {
    margin-top: 0.2rem; /* 상단 margin 추가 */
}
</style>
""", unsafe_allow_html=True)

# 초기 화면 설정 (제목 스타일 변경)
st.markdown("# 히즈메디병원")  # 제목 스타일 적용
st.markdown(f'<a class="hm-call" href="tel:{CALL}">📞 대표번호 전화하기 · {CALL}</a>', unsafe_allow_html=True)

# 안내 문구 (스타일 변경)
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

df = df[df["is_active"].apply(lambda x: ok(S(x)))]
if "display_order" in df.columns:
    df = df.sort_values("display_order", na_position="last")

# 컬럼 나누기
cols = st.columns(3)  # 3개의 컬럼으로 나눔

# 각 진료과 정보를 컬럼에 번갈아 배치
for i, (_, r) in enumerate(df.iterrows()):
    col = cols[i % 3]  # 컬럼 번호 선택
    did = r.get("dept_id")
    name = S(r.get("dept_name")) or "진료과"
    ped = "소아청소년과" in name.replace(" ", "")

    reserve = None if ped else anc(sidx(r.get("dept_reservation_url"), did), "#boardfrm")
    doc_sched = sidx(r.get("dept_schedule_detail_url"), did) if S(r.get("dept_schedule_detail_url")).startswith("http") else S(r.get("dept_schedule_detail_url"))

    with col:
        with st.expander(name):  # expander를 사용하여 확장/축소 가능
            st.markdown(f"""
            <div class="hm-dept">
              <div class="hm-row">
                {A("예약", reserve, "hm-r")}
                {A("의사정보·진료시간표", doc_sched, "")}
              </div>
              {('<div class="hm-sub">예약 없이 당일진료</div>' if ped else '')}
            </div>
            """, unsafe_allow_html=True)
