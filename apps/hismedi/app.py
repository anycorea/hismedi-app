import streamlit as st

st.set_page_config(page_title="히즈메디병원 모바일 예약", layout="centered")

# ✅ UI 정리용 CSS (Fork/툴바, 하단 아이콘/푸터, 상단 여백)
st.markdown("""
<style>
/* 상단 툴바(Fork/Deploy 등) 숨김 */
[data-testid="stToolbar"] {display: none !important;}
/* 상단 얇은 장식 라인 숨김 */
[data-testid="stDecoration"] {display: none !important;}
/* 햄버거 메뉴 숨김 */
#MainMenu {visibility: hidden;}
/* 하단 footer(스트림릿 관련) 숨김 */
footer {visibility: hidden;}
/* 제목 위 여백(전체 컨테이너 패딩) 줄이기 */
div.block-container {padding-top: 0.8rem; padding-bottom: 2rem;}
/* h1(제목) 위쪽 마진 줄이기 */
h1 {margin-top: 0.2rem; padding-top: 0;}
/* 안내문 스타일 */
.hismedi-info{
  font-size: 0.90rem;           /* 제목보다 훨씬 작게 */
  line-height: 1.45;            /* 촘촘하지만 답답하지 않게 */
  color: rgba(0,0,0,0.78);
  margin-top: 0.35rem;
}
.hismedi-info .section{
  margin-top: 0.65rem;
  padding-top: 0.55rem;
  border-top: 1px solid rgba(0,0,0,0.08);
}
.hismedi-info .tag{
  display: inline-block;
  font-weight: 700;
  margin-bottom: 0.2rem;
}
.hismedi-info ul{
  margin: 0.2rem 0 0.1rem 1.1rem;
}
.hismedi-info li{
  margin: 0.15rem 0;
}
.hismedi-info .note{
  font-size: 0.86rem;
  color: rgba(0,0,0,0.68);
}
</style>
""", unsafe_allow_html=True)

st.title("히즈메디병원 모바일 예약")

# (예) 전화하기 버튼/링크가 여기 있다고 가정
# st.link_button("전화하기", "tel:1588-0223")

# ✅ '전화하기' 바로 아래 안내문(교체용)
st.markdown("""
<div class="hismedi-info">
  <div>
    <span class="tag">직원 추천 진료과</span> <b>외과</b><br/>
    하지정맥류·탈장 진료: <b>최영수 과장</b>
  </div>

  <div class="section">
    <span class="tag">진료시간</span> <span class="note">(소아청소년과 제외)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 17:30</li>
      <li><b>토요일</b> 08:30 ~ 12:30</li>
      <li><b>점심</b> 12:30 ~ 13:30</li>
    </ul>
  </div>

  <div class="section">
    <span class="tag">온라인 예약 신청 절차</span>
    <ul>
      <li>예약 신청 → 전문 상담원 콜백 → 예약 확정</li>
      <li>상담원과 통화 후 예약이 확정됩니다.</li>
      <li>야간 및 주말에는 연락드리지 않습니다.</li>
      <li>당일 예약은 불가하며, 익일부터 가능합니다.</li>
    </ul>
  </div>

  <div class="section">
    <span class="tag">소아청소년과 안내</span> <span class="note">(예약 없이 당일 진료 / 달빛어린이병원)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 23:00</li>
      <li><b>주말·공휴일</b> 09:00 ~ 18:00</li>
      <li>영유아검진·검사 예약: <b>☎ 1588-0223</b></li>
    </ul>
  </div>
</div>
""", unsafe_allow_html=True)
