import streamlit as st
from src.sheets import load_departments, load_doctors

st.set_page_config(page_title="Hismedi", layout="wide")

st.title("Hismedi")
st.caption("진료 예약을 가장 빠르게")

# 데이터 로드
departments = load_departments()
doctors = load_doctors()

st.divider()

# 예약 CTA (최상단)
if not departments.empty:
    reserve_url = departments.iloc[0]["dept_reservation_url"]
    if reserve_url:
        st.link_button("예약하기", reserve_url, use_container_width=True)

st.divider()

# 검색
q = st.text_input("의사 / 진료과 검색", placeholder="예) 정형외과, 문종렬")

if q:
    q = q.strip()
    filtered = doctors[
        doctors["doctor_name"].str.contains(q, na=False) |
        doctors["dept_name"].str.contains(q, na=False)
    ]
else:
    filtered = doctors

# 의사 리스트
for _, row in filtered.iterrows():
    with st.container():
        c1, c2 = st.columns([1, 4])

        with c1:
            if row.get("photo_url"):
                photo = str(row.get("photo_url", "")).strip()
                if photo.startswith("http"):
                    try:
                        st.image(photo, use_container_width=True)
                    except Exception:
                        pass

        with c2:
            st.markdown(f"**{row['doctor_name']} {row.get('title','')}**")
            st.caption(row["dept_name"])

            reserve = row.get("doctor_reservation_url") or row.get("dept_reservation_url")
            detail = row.get("doctor_detail_url")

            b1, b2 = st.columns(2)
            with b1:
                if reserve:
                    st.link_button("예약하기", reserve, use_container_width=True)
            with b2:
                if detail:
                    st.link_button("진료일정 / 상세", detail, use_container_width=True)

        st.divider()
