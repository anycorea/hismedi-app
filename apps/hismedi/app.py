import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from src.sheets import load_departments, load_doctors

st.set_page_config(page_title="Hismedi", layout="wide")

st.title("Hismedi")
st.caption("진료 예약을 가장 빠르게")

# -----------------------------
# helpers
# -----------------------------
def build_reservation_url(base_url: str, dept_id=None):
    if not base_url:
        return None

    base_url = str(base_url).strip()
    if not base_url.startswith("http"):
        return None

    u = urlparse(base_url)
    qs = parse_qs(u.query)

    if dept_id not in (None, "", float("nan")):
        qs["sidx"] = [str(dept_id)]

    new_query = urlencode(qs, doseq=True)
    out = urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

    if "#boardfrm" not in out:
        out += "#boardfrm"

    return out


# -----------------------------
# data
# -----------------------------
departments = load_departments()
doctors = load_doctors()

st.divider()

# -----------------------------
# TOP: 진료과 선택 + 진료 예약하기
# -----------------------------
if departments is not None and not departments.empty and "dept_name" in departments.columns:

    dept_df = departments.copy()

    # 표시용 리스트
    dept_df["dept_name"] = dept_df["dept_name"].astype(str)
    dept_names = dept_df["dept_name"].tolist()

    selected_dept_name = st.selectbox("진료과 선택", dept_names)

    selected_row = dept_df[dept_df["dept_name"] == selected_dept_name].head(1)

    if not selected_row.empty:
        dept_id = selected_row.iloc[0].get("dept_id")
        base_url = selected_row.iloc[0].get("dept_reservation_url")

        reserve_url = build_reservation_url(base_url, dept_id)

        if reserve_url:
            st.link_button("진료 예약하기", reserve_url, use_container_width=True)

else:
    st.info("현재 선택 가능한 진료과 정보가 없습니다.")

st.divider()

# -----------------------------
# 검색
# -----------------------------
q = st.text_input("의사 / 진료과 검색", placeholder="예) 정형외과, 문종렬")

filtered = doctors
if q:
    q = q.strip()
    filtered = doctors[
        doctors["doctor_name"].astype(str).str.contains(q, na=False)
        | doctors["dept_name"].astype(str).str.contains(q, na=False)
    ]

# -----------------------------
# 의사 리스트
# -----------------------------
for _, row in filtered.iterrows():
    with st.container():
        c1, c2 = st.columns([1, 4])

        with c1:
            photo = str(row.get("photo_url", "")).strip()
            if photo.startswith("http"):
                try:
                    st.image(photo, use_container_width=True)
                except Exception:
                    pass

        with c2:
            st.markdown(f"**{row.get('doctor_name','')} {row.get('title','')}**")
            st.caption(row.get("dept_name", ""))

            dept_id = row.get("dept_id")
            base_url = row.get("dept_reservation_url")
            reserve_url = build_reservation_url(base_url, dept_id)

            detail = str(row.get("doctor_detail_url", "")).strip()

            b1, b2 = st.columns(2)
            with b1:
                if reserve_url:
                    st.link_button("진료 예약하기", reserve_url, use_container_width=True)
            with b2:
                if detail.startswith("http"):
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

        st.divider()
