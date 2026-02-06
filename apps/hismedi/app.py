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

def btn_html(label: str, url: str | None, kind: str, key: str) -> str:
    """
    kind: reserve / info / schedule
    url 없으면 비활성 버튼처럼 보이게
    key는 HTML id에 넣어서 유니크하게
    """
    cls = f"hm-btn hm-{kind}"
    if url and url.startswith("http"):
        return f'<a id="{key}" class="{cls}" href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
    return f'<span id="{key}" class="{cls} hm-disabled">{label}</span>'

# -----------------------------
# CSS (모바일에서도 1줄 유지: nowrap)
# -----------------------------
st.markdown(
    """
<style>
/* 카드 내부 버튼줄: 항상 한 줄 */
.hm-row{
  display:flex;
  gap:10px;
  flex-wrap:nowrap;          /* 줄바꿈 금지 */
  width:100%;
  margin-top:10px;
}

/* 버튼 공통 */
.hm-btn{
  flex:1 1 0;                /* 3개가 동일 비율로 가로폭 분배 */
  text-align:center;
  padding:10px 8px;
  border-radius:10px;
  text-decoration:none;
  font-weight:600;
  font-size:14px;
  border:1px solid rgba(49,51,63,0.2);
  background: rgba(49,51,63,0.03);
  color: inherit;
  white-space:nowrap;        /* 글자도 줄바꿈 금지 */
}

/* 강조(예약) */
.hm-reserve{
  border:1px solid rgba(255, 75, 75, 0.6);
}

/* 비활성 */
.hm-disabled{
  opacity:0.45;
  cursor:not-allowed;
}

/* 아주 작은 화면에서 글자가 너무 길면 축약 느낌(잘림) */
@media (max-width: 360px){
  .hm-btn{ font-size:13px; padding:9px 6px; }
}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# header
# -----------------------------
st.title("히즈메디병원")

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

dept_df = dept_df[dept_df["is_active"].apply(truthy)]

if dept_df.empty:
    st.info("현재 활성화된 진료과가 없습니다.")
    st.stop()

if "display_order" in dept_df.columns:
    dept_df = dept_df.sort_values("display_order", na_position="last")

dept_df["dept_name"] = dept_df["dept_name"].astype(str)

# -----------------------------
# cards (2열)
# -----------------------------
cols = st.columns(2, gap="large")

for i, (_, row) in enumerate(dept_df.iterrows()):
    dept_id = row.get("dept_id")
    dept_name = safe_str(row.get("dept_name"))

    base_reserve = row.get("dept_reservation_url")
    base_detail = row.get("dept_detail_url")
    base_schedule = row.get("dept_schedule_url")

    reserve_url = add_anchor(build_url_with_sidx(base_reserve, dept_id), "#boardfrm")
    detail_url = build_url_with_sidx(base_detail, dept_id) if safe_str(base_detail).startswith("http") else safe_str(base_detail)
    schedule_url = build_url_with_sidx(base_schedule, dept_id) if safe_str(base_schedule).startswith("http") else safe_str(base_schedule)

    # 유니크 key 만들기 (중복 방지)
    k_base = f"dept_{safe_str(dept_id) or i}"

    with cols[i % 2]:
        with st.container(border=True):
            st.markdown(f"### {dept_name}")

            # 버튼 3개를 "항상 한 줄"로
            row_html = f"""
<div class="hm-row">
  {btn_html("예약", reserve_url, "reserve", f"{k_base}_reserve")}
  {btn_html("의료진", detail_url, "info", f"{k_base}_detail")}
  {btn_html("진료일정", schedule_url, "schedule", f"{k_base}_schedule")}
</div>
"""
            st.markdown(row_html, unsafe_allow_html=True)
