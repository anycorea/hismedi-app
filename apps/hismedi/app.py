import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from src.sheets import load_departments

st.set_page_config(page_title="히즈메디병원", layout="wide")

# -----------------------------
# helpers
# -----------------------------
def safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def truthy(x) -> bool:
    s = safe_str(x).lower()
    if s in ("true", "1", "y", "yes"):
        return True
    if s in ("false", "0", "n", "no", ""):
        return False
    return bool(x)

def build_url_with_sidx(base_url: str, dept_id=None) -> str | None:
    base_url = safe_str(base_url)
    if not base_url or not base_url.startswith("http"):
        return None

    u = urlparse(base_url)
    qs = parse_qs(u.query)

    if dept_id is not None and safe_str(dept_id) != "":
        qs["sidx"] = [safe_str(dept_id)]

    new_query = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))

def add_anchor(url: str | None, anchor: str) -> str | None:
    if not url:
        return None
    return url if anchor in url else f"{url}{anchor}"

# -----------------------------
# header (텍스트만)
# -----------------------------
st.title("히즈메디병원")

# -----------------------------
# 안내문
# -----------------------------
st.markdown(
    """
## ※ 예약 절차 안내

**ㅇ 예약신청 → 전문상담원 콜백 → 예약확정**  
- 상담원과 통화 후 예약 확정  
- 당일 예약은 불가하고, 익일부터 가능  

**ㅇ 소아청소년과**  
- 예약 없이 당일진료(달빛어린이병원)  
  - [평일] 08:30~23:00  
  - [주말·공휴일] 09:00~18:00  
- 영유아검진, 검사 예약 : **☏1588-0223**

**ㅇ 점심시간 12:30~13:30**

**※ 대표번호 : ☏1588-0223**
""",
)

st.divider()

# -----------------------------
# data
# -----------------------------
departments = load_departments()

if departments is None or departments.empty:
    st.info("현재 진료과 정보가 없습니다.")
    st.stop()

dept_df = departments.copy()

need_cols = [
    "dept_id",
    "dept_name",
    "dept_reservation_url",
    "dept_detail_url",
    "dept_schedule_url",
    "display_order",
    "is_active",
]
for c in need_cols:
    if c not in dept_df.columns:
        dept_df[c] = ""

# 활성만 노출
dept_df = dept_df[dept_df["is_active"].apply(truthy)]

# 정렬
if "display_order" in dept_df.columns:
    dept_df = dept_df.sort_values("display_order", na_position="last")

dept_df["dept_name"] = dept_df["dept_name"].astype(str)

# -----------------------------
# 진료과 카드: 버튼 3개를 카드 안에 배치
# -----------------------------
cols = st.columns(2, gap="large")

for i, (_, row) in enumerate(dept_df.iterrows()):
    dept_id = row.get("dept_id")
    dept_name = safe_str(row.get("dept_name"))

    base_reserve = row.get("dept_reservation_url")
    base_detail = row.get("dept_detail_url")
    base_schedule = row.get("dept_schedule_url")

    # 예약: sidx 포함 + 폼으로 점프(#boardfrm)
    reserve_url = add_anchor(build_url_with_sidx(base_reserve, dept_id), "#boardfrm")

    # 의료진/일정: 시트에 URL 있으면 그대로(필요 시 sidx도 보강)
    detail_url = build_url_with_sidx(base_detail, dept_id) if safe_str(base_detail).startswith("http") else safe_str(base_detail)
    schedule_url = build_url_with_sidx(base_schedule, dept_id) if safe_str(base_schedule).startswith("http") else safe_str(base_schedule)

    with cols[i % 2]:
        with st.container(border=True):
            st.markdown(f"### {dept_name}")

            b1, b2, b3 = st.columns(3, gap="small")

            with b1:
                if reserve_url:
                    st.link_button("예약", reserve_url, use_container_width=True)
                else:
                    st.button("예약", use_container_width=True, disabled=True)

            with b2:
                if detail_url and detail_url.startswith("http"):
                    st.link_button("의료진", detail_url, use_container_width=True)
                else:
                    st.button("의료진", use_container_width=True, disabled=True)

            with b3:
                if schedule_url and schedule_url.startswith("http"):
                    st.link_button("진료일정", schedule_url, use_container_width=True)
                else:
                    st.button("진료일정", use_container_width=True, disabled=True)
