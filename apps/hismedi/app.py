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

def add_anchor(url: str | None, anchor: str) -> str | None:
    if not url:
        return None
    return url if anchor in url else f"{url}{anchor}"

def truthy(x) -> bool:
    # 구글시트에서 TRUE/FALSE, True/False, 빈칸 등 섞일 수 있어서 안전 처리
    s = safe_str(x).lower()
    if s in ("true", "1", "y", "yes"):
        return True
    if s in ("false", "0", "n", "no", ""):
        return False
    return bool(x)

# -----------------------------
# header (로고 -> 실패 시 텍스트)
# -----------------------------
LOGO_URL = "http://www.hismedi.kr/images/h1_logo.gif"

try:
    st.image(LOGO_URL, width=220)
except Exception:
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

# 필수 컬럼 보정(없어도 앱이 죽지 않게)
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

# 활성만 노출(컬럼이 있는 경우)
dept_df = dept_df[dept_df["is_active"].apply(truthy)]

# 정렬(있으면)
if "display_order" in dept_df.columns:
    dept_df = dept_df.sort_values("display_order", na_position="last")

dept_df["dept_name"] = dept_df["dept_name"].astype(str)

st.subheader("진료과")

# 선택 상태
if "selected_dept_id" not in st.session_state:
    st.session_state.selected_dept_id = None

# 모바일 고려: 2열 카드
cols = st.columns(2, gap="large")

# 카드 렌더
for i, (_, row) in enumerate(dept_df.iterrows()):
    dept_id = row.get("dept_id")
    dept_name = safe_str(row.get("dept_name"))

    with cols[i % 2]:
        with st.container(border=True):
            st.markdown(f"### {dept_name}")

            selected = (st.session_state.selected_dept_id == dept_id)

            if st.button(
                "선택됨" if selected else "선택하기",
                key=f"pick_{dept_id}",
                use_container_width=True,
                disabled=selected,
            ):
                st.session_state.selected_dept_id = dept_id
                st.rerun()

st.divider()

# -----------------------------
# 선택된 진료과 -> 버튼 3개 펼치기
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

# 예약: sidx 강제 + 폼으로 점프(#boardfrm)
reserve_url = build_url_with_sidx(base_reserve, selected_dept_id)
reserve_url = add_anchor(reserve_url, "#boardfrm")

# 의료진/일정: 시트에 URL이 있으면 그대로 사용, 없으면 비활성
detail_url = build_url_with_sidx(base_detail, selected_dept_id) if safe_str(base_detail).startswith("http") else safe_str(base_detail)
schedule_url = build_url_with_sidx(base_schedule, selected_dept_id) if safe_str(base_schedule).startswith("http") else safe_str(base_schedule)

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
