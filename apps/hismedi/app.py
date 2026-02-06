import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from src.sheets import load_departments

st.set_page_config(page_title="히즈메디병원", layout="wide")

# ---- tiny utils ----
S = lambda x: "" if x is None else str(x).strip()

def ok(x):
    v = S(x).lower()
    return v in ("true", "1", "y", "yes") if v in ("true","false","1","0","y","n","yes","no","") else bool(x)

def sidx(url, did):
    url = S(url)
    if not url.startswith("http"):
        return None
    u = urlparse(url); q = parse_qs(u.query)
    if S(did):
        q["sidx"] = [S(did)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def anc(url, a):
    return None if not url else (url if a in url else url + a)

CALL = "1588-0223"

# ---- header ----
st.title("히즈메디병원")
st.markdown(f"**📞 대표번호 전화하기 · [{CALL}](tel:{CALL})**")

# ---- 안내문 (기본 스타일만 사용) ----
st.markdown("""
**직원 추천 진료과: 외과**  
하지정맥류·탈장 진료: **최영수 과장**

**진료시간** *(소아청소년과 제외)*  
- 평일 08:30 ~ 17:30  
- 토요일 08:30 ~ 12:30  
- 점심 12:30 ~ 13:30  

**온라인 예약 신청 절차**  
- 예약 신청 → 전문 상담원 콜백 → 예약 확정  
- 상담원과 통화 후 예약이 확정됩니다.  
- 야간 및 주말에는 연락드리지 않습니다.  
- 당일 예약은 불가하며, 익일부터 가능합니다.  

**소아청소년과 안내** *(예약 없이 당일 진료 / 달빛어린이병원)*  
- 평일 08:30 ~ 23:00  
- 주말·공휴일 09:00 ~ 18:00  
- 영유아검진·검사 예약: **☎ 1588-0223**
""")

st.divider()

# ---- data ----
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다.")
    st.stop()

for c in ("dept_id","dept_name","dept_reservation_url","dept_detail_url","dept_schedule_url","display_order","is_active"):
    if c not in df.columns:
        df[c] = ""

df = df[df["is_active"].apply(ok)]
if "display_order" in df.columns:
    df = df.sort_values("display_order", na_position="last")

# ---- render ----
for i, (_, r) in enumerate(df.iterrows()):
    did = r.get("dept_id")
    name = S(r.get("dept_name")) or "진료과"
    ped = "소아청소년과" in name.replace(" ", "")

    reserve = None if ped else anc(sidx(r.get("dept_reservation_url"), did), "#boardfrm")
    detail  = sidx(r.get("dept_detail_url"), did) if S(r.get("dept_detail_url")).startswith("http") else S(r.get("dept_detail_url"))
    sched   = sidx(r.get("dept_schedule_url"), did) if S(r.get("dept_schedule_url")).startswith("http") else S(r.get("dept_schedule_url"))

    # 진료과 이름: title보다 작은 기본 헤더 사용
    st.subheader(name)

    c1, c2, c3 = st.columns(3)

    with c1:
        if reserve:
            st.link_button("예약", reserve)
        else:
            st.button("예약", disabled=True)

    with c2:
        if detail and detail.startswith("http"):
            st.link_button("의료진", detail)
        else:
            st.button("의료진", disabled=True)

    with c3:
        if sched and sched.startswith("http"):
            st.link_button("진료일정", sched)
        else:
            st.button("진료일정", disabled=True)

    if ped:
        st.caption("예약 없이 당일진료")

    st.divider()
