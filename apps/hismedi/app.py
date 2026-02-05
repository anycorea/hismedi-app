import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from src.sheets import load_departments, load_doctors

st.set_page_config(page_title="Hismedi", layout="wide")

st.title("Hismedi")
st.caption("진료 예약을 가장 빠르게")

# -----------------------------
# helpers
# -----------------------------
def build_reservation_url(base_url: str, dept_id: str | int | None = None) -> str | None:
    """
    base_url: 예) https://hismedi.kr/medical/reservation_form.asp
             예) https://hismedi.kr/medical/reservation_form.asp?sidx=7
    dept_id: departments.dept_id (sidx로 사용)
    """
    if not base_url:
        return None

    base_url = str(base_url).strip()
    if not base_url.startswith("http"):
        return None

    u = urlparse(base_url)
    qs = parse_qs(u.query)

    # dept_id가 있으면 sidx를 보장
    if dept_id is not None and str(dept_id).strip() != "":
        qs["sidx"] = [str(dept_id).strip()]

    new_query = urlencode(qs, doseq=True)
    out = urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

    # 폼으로 바로 스크롤 (HTML에 form id="boardfrm" 가 있음)
    if "#boardfrm" not in out:
        out = out + "#boardfrm"

    return out


# -----------------------------
# data
# -----------------------------
departments = load_departments()
doctors = load_doctors()

st.divider()

# -----------------------------
# TOP: 진료과 선택 + 진료 예약하기 (가장 우선)
# -----------------------------
if not departments.empty:
    active_depts = departments[departments.get("is_active", True) == True].copy()

    # 표시순서 있으면 정렬
    if "display_order" in active_depts.columns:
        active_depts = active_depts.sort_values("display_order", na_position="last")

    dept_names = active_depts["dept_name"].fillna("").tolist()
    dept_map = dict(zip(active_depts["dept_name"], active_depts["dept_id"]))

    colA, colB = st.columns([3, 2])
    with colA:
        selected_dept_name = st.selectbox("진료과 선택", dept_names, index=0)
    with colB:
        dept_id = dept_map.get(selected_dept_name)

        # dept_reservation_url이 시트에 있고, 거기에 sidx를 강제 주입
        base = active_depts[active_depts["dept_name"] == selected_dept_name].iloc[0].get("dept_reservation_url")
        reserve_url = build_reservation_url(base, dept_id=dept_id)

        if reserve_url:
            st.link_button("진료 예약하기", reserve_url, use_container_width=True)

st.divider()

# -----------------------------
# 검색
# -----------------------------
q = st.text_input("의사 / 진료과 검색", placeholder="예) 정형외과, 문종렬")

filtered = doctors
if q:
    q = q.strip()
    name_ok = doctors.get("doctor_name", "").astype(str).str.contains(q, na=False)
    dept_ok = doctors.get("dept_name", "").astype(str).str.contains(q, na=False)
    filtered = doctors[name_ok | dept_ok]

# 표시순서가 있으면 정렬
if "display_order" in filtered.columns:
    filtered = filtered.sort_values("display_order", na_position="last")

# -----------------------------
# 의사 리스트
# -----------------------------
for _, row in filtered.iterrows():
    with st.container():
        c1, c2 = st.columns([1, 4])

        # 사진
        with c1:
            photo = str(row.get("photo_url", "")).strip()
            if photo.startswith("http"):
                try:
                    st.image(photo, use_container_width=True)
                except Exception:
                    st.empty()

        with c2:
            doctor_name = str(row.get("doctor_name", "")).strip()
            title = str(row.get("title", "")).strip()
            dept_name = str(row.get("dept_name", "")).strip()

            st.markdown(f"**{doctor_name} {title}**" if title else f"**{doctor_name}**")
            if dept_name:
                st.caption(dept_name)

            # 예약: 의사별 링크 우선, 없으면 진료과 링크(+sidx 강제)
            doctor_reserve = str(row.get("doctor_reservation_url", "")).strip()
            dept_reserve = str(row.get("dept_reservation_url", "")).strip()
            dept_id = row.get("dept_id")

            reserve_url = None
            if doctor_reserve.startswith("http"):
                reserve_url = build_reservation_url(doctor_reserve, dept_id=None)  # doctor_reserve에 sidx가 있으면 그대로
            else:
                reserve_url = build_reservation_url(dept_reserve, dept_id=dept_id)

            # 상세(진료일정/상세)
            detail = str(row.get("doctor_detail_url", "")).strip()
            if detail and not detail.startswith("http"):
                detail = ""

            b1, b2 = st.columns(2)
            with b1:
                if reserve_url:
                    st.link_button("진료 예약하기", reserve_url, use_container_width=True)
            with b2:
                if detail:
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

        st.divider()
