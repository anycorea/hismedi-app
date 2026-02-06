import streamlit as st
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from src.sheets import load_departments

st.set_page_config(page_title="히즈메디병원", layout="wide")

# -----------------------------
# helpers
# -----------------------------
def s(x):
    return "" if x is None else str(x).strip()

def norm(x):
    return s(x).replace(" ", "").lower()

def is_true(x):
    v = s(x).lower()
    if v in ("true", "1", "y", "yes"):
        return True
    if v in ("false", "0", "n", "no", ""):
        return False
    return bool(x)

def with_sidx(url, dept_id):
    url = s(url)
    if not url.startswith("http"):
        return None
    u = urlparse(url)
    qs = parse_qs(u.query)
    if s(dept_id):
        qs["sidx"] = [s(dept_id)]
    q = urlencode(qs, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, u.params, q, u.fragment))

def with_anchor(url, anchor):
    if not url:
        return None
    return url if anchor in url else url + anchor

def btn(label, url, cls, key):
    if url and url.startswith("http"):
        return f'<a id="{key}" class="hm-btn {cls}" href="{url}" target="_blank" rel="noopener noreferrer">{label}</a>'
    return f'<span id="{key}" class="hm-btn {cls} hm-disabled">{label}</span>'

# -----------------------------
# CSS (모바일 1열 + 하단 고정 전화버튼 + 3버튼 한줄)
# -----------------------------
st.markdown(
    """
<style>
/* 하단 고정바 공간 확보: Streamlit 버전별로 두 군데 모두 지정 */
div.block-container { padding-bottom: 92px !important; }
section.main > div { padding-bottom: 92px !important; }

.hm-dept{padding:14px 0; border-bottom:1px solid rgba(49,51,63,0.08);}
.hm-title{font-size:22px; font-weight:800; margin:0 0 10px 0;}

.hm-row{display:flex; gap:10px; flex-wrap:nowrap; width:100%;}

.hm-btn{
  flex:1 1 0;
  text-align:center;
  padding:10px 8px;
  border-radius:10px;
  text-decoration:none;
  font-weight:800;
  font-size:14px;
  border:1px solid rgba(49,51,63,0.18);
  background:rgba(49,51,63,0.02);
  color:inherit;
  white-space:nowrap;
}
.hm-reserve{border-color:rgba(255,75,75,0.65);}
.hm-disabled{opacity:0.45; cursor:not-allowed;}

.hm-sub{margin-top:8px; font-size:12px; color:rgba(49,51,63,0.55);}

/* 하단 고정 전화 버튼 */
.hm-callbar{
  position:fixed;
  left:0; right:0; bottom:0;
  padding:10px 14px 12px;
  background:rgba(255,255,255,0.94);
  backdrop-filter: blur(10px);
  border-top:1px solid rgba(49,51,63,0.10);
  z-index:99999;
}
.hm-callbtn{
  width:100%;
  display:flex;
  align-items:center;
  justify-content:center;
  gap:10px;
  padding:14px 12px;
  border-radius:14px;
  text-decoration:none;
  font-weight:900;
  font-size:16px;
  border:1px solid rgba(49,51,63,0.18);
  background:rgba(49,51,63,0.05);
  color:inherit;
}
.hm-ico{font-size:18px; line-height:1;}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# header + 안내문
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
"""
)

# -----------------------------
# 고정 전화 버튼 (항상 보이게)
# -----------------------------
CALL_NUMBER = "1588-0223"
st.markdown(
    f"""
<div class="hm-callbar">
  <a class="hm-callbtn" href="tel:{CALL_NUMBER}">
    <span class="hm-ico">📞</span>
    대표번호 전화하기 · {CALL_NUMBER}
  </a>
</div>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# data
# -----------------------------
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다.")
    st.stop()

for c in ("dept_id","dept_name","dept_reservation_url","dept_detail_url","dept_schedule_url","display_order","is_active"):
    if c not in df.columns:
        df[c] = ""

df = df[df["is_active"].apply(is_true)]
if "display_order" in df.columns:
    df = df.sort_values("display_order", na_position="last")

# -----------------------------
# render (모바일 1열)
# -----------------------------
for i, (_, r) in enumerate(df.iterrows()):
    dept_id = r.get("dept_id")
    dept_name = s(r.get("dept_name")) or "진료과"

    # ✅ “소아청소년과” 포함이면 예약 비활성 + 안내문 표시
    is_pediatric = ("소아청소년과" in dept_name) or ("pediatric" in norm(dept_name))

    reserve = None
    if not is_pediatric:
        reserve = with_anchor(with_sidx(r.get("dept_reservation_url"), dept_id), "#boardfrm")

    detail = with_sidx(r.get("dept_detail_url"), dept_id) if s(r.get("dept_detail_url")).startswith("http") else s(r.get("dept_detail_url"))
    sched  = with_sidx(r.get("dept_schedule_url"), dept_id) if s(r.get("dept_schedule_url")).startswith("http") else s(r.get("dept_schedule_url"))

    k = f"dept_{s(dept_id) or i}"
    pediatric_hint = '<div class="hm-sub">예약 없이 당일진료</div>' if is_pediatric else ""

    st.markdown(
        f"""
<div class="hm-dept">
  <div class="hm-title">{dept_name}</div>
  <div class="hm-row">
    {btn("예약", reserve, "hm-reserve", f"{k}_r")}
    {btn("의료진", detail, "", f"{k}_d")}
    {btn("진료일정", sched, "", f"{k}_s")}
  </div>
  {pediatric_hint}
</div>
""",
        unsafe_allow_html=True,
    )
