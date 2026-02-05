import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from src.sheets import load_departments, load_doctors

st.set_page_config(page_title="Hismedi", layout="wide")

st.title("Hismedi")
st.caption("진료 예약을 가장 빠르게")

# -----------------------------
# helpers
# -----------------------------
def safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def build_reservation_url(base_url: str, dept_id=None) -> str | None:
    base_url = safe_str(base_url)
    if not base_url or not base_url.startswith("http"):
        return None

    u = urlparse(base_url)
    qs = parse_qs(u.query)

    # dept_id가 있으면 sidx로 강제
    if dept_id is not None and safe_str(dept_id) != "":
        qs["sidx"] = [safe_str(dept_id)]

    new_query = urlencode(qs, doseq=True)
    out = urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

    # 예약 폼 위치로 점프 (HTML 수정 X, 단순 anchor 이동)
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
if departments is None or departments.empty or "dept_name" not in departments.columns:
    st.info("현재 선택 가능한 진료과 정보가 없습니다.")
else:
    dept_df = departments.copy()

    # 정렬(있으면) / 표시용
    if "display_order" in dept_df.columns:
        dept_df = dept_df.sort_values("display_order", na_position="last")

    dept_df["dept_name"] = dept_df["dept_name"].astype(str)

    dept_names = dept_df["dept_name"].tolist()
    if not dept_names:
        st.info("현재 선택 가능한 진료과 정보가 없습니다.")
    else:
        selected_dept_name = st.selectbox("진료과 선택", dept_names)

        selected_row = dept_df[dept_df["dept_name"] == selected_dept_name].head(1)
        if not selected_row.empty:
            dept_id = selected_row.iloc[0].get("dept_id")
            base_url = selected_row.iloc[0].get("dept_reservation_url")
            reserve_url = build_reservation_url(base_url, dept_id)

            if reserve_url:
                st.link_button("진료 예약하기", reserve_url, use_container_width=True)

st.divider()

# -----------------------------
# 검색
# -----------------------------
q = st.text_input("의사 / 진료과 검색", placeholder="예) 정형외과, 문종렬")

filtered = doctors
if doctors is None or doctors.empty:
    st.info("현재 의료진 정보가 없습니다.")
    st.stop()

# 컬럼이 없을 수도 있으니 안전처리
for col in ["doctor_name", "dept_name"]:
    if col not in filtered.columns:
        filtered[col] = ""

if q:
    q = q.strip()
    filtered = filtered[
        filtered["doctor_name"].astype(str).str.contains(q, na=False)
        | filtered["dept_name"].astype(str).str.contains(q, na=False)
    ]

# -----------------------------
# 의사 리스트 (예약은 '진료과 예약'로만)
# -----------------------------
for _, row in filtered.iterrows():
    with st.container():
        c1, c2 = st.columns([1, 4])

        with c1:
            photo = safe_str(row.get("photo_url"))
            if photo.startswith("http"):
                try:
                    st.image(photo, use_container_width=True)
                except Exception:
                    pass

        with c2:
            name = safe_str(row.get("doctor_name"))
            title = safe_str(row.get("title"))
            dept_name = safe_str(row.get("dept_name"))

            st.markdown(f"**{name} {title}**".strip())
            if dept_name:
                st.caption(dept_name)

            # 예약은 '의사별'이 아니라 '진료과별'로만
            dept_id = row.get("dept_id")
            base_url = row.get("dept_reservation_url")
            reserve_url = build_reservation_url(base_url, dept_id)

            # 상세(진료일정/상세) 링크는 유지
            detail = safe_str(row.get("doctor_detail_url"))

            b1, b2 = st.columns(2)
            with b1:
                if reserve_url:
                    st.link_button("진료 예약하기", reserve_url, use_container_width=True)
            with b2:
                if detail.startswith("http"):
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

        st.divider()
