import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from src.sheets import load_departments

st.set_page_config(page_title="히즈메디병원", layout="wide")

# -----------------------------
# helpers
# -----------------------------
def safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def build_url_with_sidx(base_url: str, dept_id=None) -> str | None:
    """
    base_url에 sidx=dept_id를 보장해서 붙입니다.
    - base_url이 이미 ?sidx=... 를 갖고 있어도 dept_id로 덮어씁니다.
    """
    base_url = safe_str(base_url)
    if not base_url or not base_url.startswith("http"):
        return None

    u = urlparse(base_url)
    qs = parse_qs(u.query)

    if dept_id is not None and safe_str(dept_id) != "":
        qs["sidx"] = [safe_str(dept_id)]

    new_query = urlencode(qs, doseq=True)
    out = urlunparse((u.scheme, u.netloc, u.path, u.params, new_query, u.fragment))
    return out

def add_anchor(url: str, anchor: str) -> str:
    if not url:
        return url
    return url if anchor in url else f"{url}{anchor}"

# -----------------------------
# header (로고)
# -----------------------------
LOGO_URL = "http://www.hismedi.kr/images/h1_logo.gif"
st.image(LOGO_URL, width=220)

# -----------------------------
# 안내문 (요청 문구 그대로)
# -----------------------------
st.markdown(
    """
### ※ 예약 절차 안내

ㅇ **예약신청 → 전문상담원 콜백 → 예약확정**  
  - 상담원과 통화 후 예약 확정  
  - 당일 예약은 불가하고, 익일부터 가능  

ㅇ **소아청소년과**  
  - 예약 없이 당일진료(달빛어린이병원)  
    [평일] 08:30~23:00  
    [주말·공휴일] 09:00~18:00  
  - 영유아검진, 검사 예약 : **☏1588-0223**

ㅇ **점심시간 12:30~13:30**

**※ 대표번호 : ☏1588-0223**
""",
    unsafe_allow_html=False,
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

# is_active가 있으면 TRUE만 노출
if "is_active" in dept_df.columns:
    dept_df = dept_df[dept_df["is_active"] == True]

# 정렬(있으면)
if "display_order" in dept_df.columns:
    dept_df = dept_df.sort_values("display_order", na_position="last")

# 필수 컬럼 보정
for col in ["dept_id", "dept_name", "dept_reservation_url", "dept_detail_url", "dept_schedule_url"]:
    if col not in dept_df.columns:
        dept_df[col] = ""

dept_df["dept_name"] = dept_df["dept_name"].astype(str)

# -----------------------------
# 진료과 카드 전체 노출 + 선택 시 하단 버튼 펼침
# -----------------------------
st.subheader("진료과")

# 선택 상태 유지
if "selected_dept_id" not in st.session_state:
    st.session_state.selected_dept_id = None

# 모바일도 고려해서 2열(너무 작으면 1열로 자동 줄바꿈)
cols = st.columns(2, gap="large")

for i, (_, row) in enumerate(dept_df.iterrows()):
    dept_id = row.get("dept_id")
    dept_name = safe_str(row.get("dept_name"))

    with cols[i % 2]:
        # 카드 느낌을 내기 위해 container + 버튼
        with st.container(border=True):
            st.markdown(f"### {dept_name}")
            selected = (st.session_state.selected_dept_id == dept_id)

            # 선택 버튼
            if st.button("선택하기" if not selected else "선택됨", key=f"pick_{dept_id}", use_container_width=True, disabled=selected):
                st.session_state.selected_dept_id = dept_id
                st.rerun()

st.divider()

# -----------------------------
# 선택된 진료과 버튼(예약하기/의료진 정보/진료일정)
# -----------------------------
selected_dept_id = st.session_state.selected_dept_id

if selected_dept_id is None:
    st.info("원하시는 진료과를 선택해 주세요.")
    st.stop()

selected_row = dept_df[dept_df["dept_id"] == selected_dept_id].head(1)
if selected_row.empty:
    st.info("선택된 진료과 정보를 찾을 수 없습니다.")
    st.stop()

dept_name = safe_str(selected_row.iloc[0].get("dept_name"))

st.subheader(f"선택된 진료과: {dept_name}")

base_reserve = selected_row.iloc[0].get("dept_reservation_url")
base_detail = selected_row.iloc[0].get("dept_detail_url")
base_schedule = selected_row.iloc[0].get("dept_schedule_url")

reserve_url = build_url_with_sidx(base_reserve, selected_dept_id)
detail_url = build_url_with_sidx(base_detail, selected_dept_id) if safe_str(base_detail).startswith("http") else safe_str(base_detail)
schedule_url = build_url_with_sidx(base_schedule, selected_dept_id) if safe_str(base_schedule).startswith("http") else safe_str(base_schedule)

# 예약폼 위치로 점프 (있으면만)
reserve_url = add_anchor(reserve_url, "#boardfrm") if reserve_url else None

b1, b2, b3 = st.columns(3, gap="large")

with b1:
    if reserve_url:
        st.link_button("예약하기", reserve_url, use_container_width=True)
    else:
        st.button("예약하기", use_container_width=True, disabled=True)

with b2:
    if detail_url and detail_url.startswith("http"):
        st.link_button("의료진 정보", detail_url, use_container_width=True)
    else:
        st.button("의료진 정보", use_container_width=True, disabled=True)

with b3:
    if schedule_url and schedule_url.startswith("http"):
        st.link_button("진료일정", schedule_url, use_container_width=True)
    else:
        st.button("진료일정", use_container_width=True, disabled=True)
