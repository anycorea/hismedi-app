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
    if not url.startswith("http"): return None
    u = urlparse(url); q = parse_qs(u.query)
    if S(did): q["sidx"] = [S(did)]
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def anc(url, a): return None if not url else (url if a in url else url + a)

def A(lbl, url, cls, k):
    if url and url.startswith("http"):
        return f'<a id="{k}" class="hm-btn {cls}" href="{url}" target="_blank" rel="noopener noreferrer">{lbl}</a>'
    return f'<span id="{k}" class="hm-btn {cls} hm-dis">{lbl}</span>'

# ---- CSS (minimal) ----
st.markdown("""
<style>
.hm-call{display:block; margin:6px 0 14px; padding:12px 12px; border-radius:14px;
  text-decoration:none; font-weight:900; text-align:center;
  border:1px solid rgba(49,51,63,.18); background:rgba(49,51,63,.05); color:inherit;}
.hm-dept{padding:14px 0; border-bottom:1px solid rgba(49,51,63,.08);}
.hm-title{font-size:22px; font-weight:900; margin:0 0 10px;}
.hm-row{display:flex; gap:10px; flex-wrap:nowrap; width:100%;}
.hm-btn{flex:1 1 0; text-align:center; padding:10px 8px; border-radius:10px; white-space:nowrap;
  text-decoration:none; font-weight:800; font-size:14px; color:inherit;
  border:1px solid rgba(49,51,63,.18); background:rgba(49,51,63,.02);}
.hm-r{border-color:rgba(255,75,75,.65);}
.hm-dis{opacity:.45; cursor:not-allowed;}
.hm-sub{margin-top:8px; font-size:12px; color:rgba(49,51,63,.55);}
</style>
""", unsafe_allow_html=True)

# ---- header + call button (under title) ----
st.title("히즈메디병원")
CALL = "1588-0223"
st.markdown(f'<a class="hm-call" href="tel:{CALL}">📞 대표번호 전화하기 · {CALL}</a>', unsafe_allow_html=True)

# ---- 안내문 ----
st.markdown("""
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
""")

# ---- data ----
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다."); st.stop()

for c in ("dept_id","dept_name","dept_reservation_url","dept_detail_url","dept_schedule_url","display_order","is_active"):
    if c not in df.columns: df[c] = ""

df = df[df["is_active"].apply(ok)]
if "display_order" in df.columns: df = df.sort_values("display_order", na_position="last")

# ---- render (mobile-first 1 column) ----
for i, (_, r) in enumerate(df.iterrows()):
    did, name = r.get("dept_id"), (S(r.get("dept_name")) or "진료과")
    ped = "소아청소년과" in name.replace(" ", "")
    reserve = None if ped else anc(sidx(r.get("dept_reservation_url"), did), "#boardfrm")
    detail  = sidx(r.get("dept_detail_url"), did) if S(r.get("dept_detail_url")).startswith("http") else S(r.get("dept_detail_url"))
    sched   = sidx(r.get("dept_schedule_url"), did) if S(r.get("dept_schedule_url")).startswith("http") else S(r.get("dept_schedule_url"))
    k = f"dept_{S(did) or i}"
    st.markdown(f"""
<div class="hm-dept">
  <div class="hm-title">{name}</div>
  <div class="hm-row">
    {A("예약", reserve, "hm-r", f"{k}_r")}
    {A("의료진", detail, "", f"{k}_d")}
    {A("진료일정", sched, "", f"{k}_s")}
  </div>
  {('<div class="hm-sub">예약 없이 당일진료</div>' if ped else '')}
</div>
""", unsafe_allow_html=True)
