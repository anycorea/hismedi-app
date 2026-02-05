import streamlit as st
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode

from src.sheets import load_departments, load_doctors
from src.schedule import get_schedule  # 진료일정 자동추출 사용 중이면 유지

st.set_page_config(page_title="Hismedi", layout="wide")

# =========================
# 스타일 (모바일 예약 최우선)
# =========================
st.markdown(
    """
<style>
.block-container { padding-top: 1.2rem; padding-bottom: 2rem; }
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
.card { padding: 10px 4px 2px 4px; }
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

# =========================
# 유틸: 예약 URL 생성
# - anchor: boardfrm / bbsagree
# - dept_id가 있으면 ?sidx=dept_id 를 넣어 "진료과 자동선택" 유도
# =========================
DEFAULT_RESERVE_BASE = "http://www.hismedi.kr/medical/reservation_form.asp"

def build_reserve_url(base_url: str | None = None, dept_id: int | str | None = None, anchor: str | None = None) -> str:
    url = (base_url or DEFAULT_RESERVE_BASE).strip()
    if not url:
        url = DEFAULT_RESERVE_BASE

    parts = urlsplit(url)
    q = parse_qs(parts.query)

    # dept_id가 있으면 sidx를 진료과로 강제(자동선택 목적)
    if dept_id is not None and str(dept_id).strip():
        q["sidx"] = [str(dept_id).strip()]

    new_query = urlencode(q, doseq=True)
    rebuilt = urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, ""))

    if anchor:
        return rebuilt + f"#{anchor}"
    return rebuilt

# =========================
# 유틸: 안전 이미지
# =========================
def render_photo(photo_url: str | None):
    photo = str(photo_url or "").strip()
    if not (photo.startswith("http://") or photo.startswith("https://")):
        return
    try:
        st.image(photo, use_container_width=True)
    except Exception:
        pass

# =========================
# 상단 예약 CTA (앵커 점프 2버튼)
# =========================
st.divider()

# 공용 예약 링크가 시트에 있다면 base로 사용, 없으면 DEFAULT 사용
global_base = ""
if not departments.empty:
    global_base = str(departments.iloc[0].get("dept_reservation_url") or "").strip()
if not global_base:
    global_base = DEFAULT_RESERVE_BASE

reserve_form_url = build_reserve_url(global_base, dept_id=None, anchor="boardfrm")
privacy_url = build_reserve_url(global_base, dept_id=None, anchor="bbsagree")

c1, c2 = st.columns(2)
with c1:
    st.markdown(f'<div class="big-cta"><a href="{reserve_form_url}" target="_blank">예약 폼 바로가기</a></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="big-cta"><a href="{privacy_url}" target="_blank">개인정보 동의/처리방침</a></div>', unsafe_allow_html=True)

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
# 의사 리스트
# - 예약하기: 해당 의사의 dept_id로 ?sidx=dept_id 만들어서 "진료과 자동선택" 유도
# - 앵커: #boardfrm 로 폼 위치 바로 점프
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
            dept_id = row.get("dept_id", None)

            st.markdown(f"**{doctor_name} {title}**")
            if dept_name:
                st.caption(dept_name)

            # 상세 링크는 그대로 사용
            detail = str(row.get("doctor_detail_url") or "").strip()

            # 예약 링크: 서버가 sidx로 진료과를 선택하는 구조를 사용하므로 dept_id로 생성
            # (row의 doctor_reservation_url이 staffidx 기반으로 잘못 채워져 있어도 안전)
            reserve_url = build_reserve_url(global_base, dept_id=dept_id, anchor="boardfrm")
            privacy_url2 = build_reserve_url(global_base, dept_id=dept_id, anchor="bbsagree")

            b1, b2, b3 = st.columns(3)
            with b1:
                st.link_button("예약하기", reserve_url, use_container_width=True)
            with b2:
                st.link_button("개인정보", privacy_url2, use_container_width=True)
            with b3:
                if detail:
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

            # 진료일정 자동 요약 (detail이 있을 때만)
            if detail:
                schedule = get_schedule(detail)
                if schedule:
                    lines = [f"- {k}: {v}" for k, v in schedule.items()]
                    st.markdown("**진료일정(요약)**")
                    st.markdown("\n".join(lines))
                else:
                    st.caption("진료일정: 상세보기에서 확인")

        st.markdown("</div>", unsafe_allow_html=True)
        st.divider()
