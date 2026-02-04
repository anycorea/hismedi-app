import streamlit as st
from src.sheets import load_departments, load_doctors
from src.schedule import get_schedule  # ✅ 진료일정 자동 추출

st.set_page_config(page_title="Hismedi", layout="wide")

# =========================
# 스타일 (모바일에서 "예약 최우선" 느낌)
# =========================
st.markdown(
    """
<style>
/* 상단 여백 조금 줄이기 */
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }

/* 큰 CTA 버튼(HTML 링크) */
.big-cta a{
  display:block;
  text-align:center;
  padding:14px 16px;
  border-radius:14px;
  font-weight:800;
  text-decoration:none !important;
  border:1px solid rgba(0,0,0,.12);
  box-shadow: 0 6px 18px rgba(0,0,0,.06);
}

/* 카드 간격 */
.card {
  padding: 10px 4px 2px 4px;
}
</style>
""",
    unsafe_allow_html=True,
)

st.title("Hismedi")
st.caption("진료 예약을 가장 빠르게")

# =========================
# 데이터 로드
# =========================
departments = load_departments()
doctors = load_doctors()

# 공용(상단) 예약 링크: departments 첫 행을 공용으로 사용 중이라면 그대로 OK
global_reserve_url = ""
if not departments.empty:
    global_reserve_url = str(departments.iloc[0].get("dept_reservation_url") or "").strip()

st.divider()

# =========================
# 예약 CTA (최상단, 크게)
# =========================
if global_reserve_url:
    st.markdown(
        f'<div class="big-cta"><a href="{global_reserve_url}" target="_blank">예약하기</a></div>',
        unsafe_allow_html=True,
    )
else:
    # 공용 예약이 없다면 안내만
    st.info("예약 링크가 설정되지 않았습니다. (departments의 dept_reservation_url 확인)")

st.divider()

# =========================
# 검색
# =========================
q = st.text_input("의사 / 진료과 검색", placeholder="예) 정형외과, 문종렬").strip()

if q:
    filtered = doctors[
        doctors["doctor_name"].str.contains(q, na=False)
        | doctors["dept_name"].str.contains(q, na=False)
    ]
else:
    filtered = doctors

# =========================
# 유틸: 안전 이미지
# =========================
def render_photo(photo_url: str):
    photo = str(photo_url or "").strip()
    if not (photo.startswith("http://") or photo.startswith("https://")):
        return
    try:
        st.image(photo, use_container_width=True)
    except Exception:
        # 이미지 깨져도 앱은 계속 동작
        pass

# =========================
# 의사 리스트
# =========================
for _, row in filtered.iterrows():
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)

        c1, c2 = st.columns([1, 4])

        with c1:
            render_photo(row.get("photo_url"))

        with c2:
            doctor_name = row.get("doctor_name", "")
            title = row.get("title", "")
            dept_name = row.get("dept_name", "")

            st.markdown(f"**{doctor_name} {title}**")
            if dept_name:
                st.caption(dept_name)

            reserve = row.get("doctor_reservation_url") or row.get("dept_reservation_url")
            detail = row.get("doctor_detail_url")

            b1, b2 = st.columns(2)
            with b1:
                if reserve:
                    st.link_button("예약하기", reserve, use_container_width=True)
            with b2:
                if detail:
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

            # =========================
            # 진료일정 자동 추출 (요약)
            # =========================
            if detail:
                schedule = get_schedule(detail)  # schedule.py의 캐시 적용됨
                if schedule:
                    # 너무 길어지면 모바일에서 가독성 떨어져서 줄바꿈 형태로 표시
                    lines = [f"- {k}: {v}" for k, v in schedule.items()]
                    st.markdown("**진료일정(요약)**")
                    st.markdown("\n".join(lines))
                else:
                    st.caption("진료일정: 상세보기에서 확인")

        st.markdown("</div>", unsafe_allow_html=True)
        st.divider()
