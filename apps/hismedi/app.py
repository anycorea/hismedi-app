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

CALL = "1588-0223"

# ---- CSS (minimal + mobile polish) ----
st.markdown("""
<style>
/* Streamlit UI hide (may vary by deploy env) */
[data-testid="stToolbar"]{display:none !important;}
[data-testid="stDecoration"]{display:none !important;}
#MainMenu{visibility:hidden;}
footer{visibility:hidden;}
.viewerBadge_container__1QSob,.viewerBadge_link__1S137,.viewerBadge_text__1JaDK{display:none !important;}

/* Top spacing (avoid title crop) */
div.block-container{padding-top:.75rem; padding-bottom:2rem;}
h1{margin-top:.15rem; padding-top:0; line-height:1.15;}

/* Call button */
.hm-call{display:block; margin:.35rem 0 .55rem; padding:12px 12px; border-radius:14px;
  text-decoration:none; font-weight:900; text-align:center;
  border:1px solid rgba(49,51,63,.18); background:rgba(49,51,63,.05); color:inherit;}

/* Info box */
.hm-info{margin:.35rem 0 1rem; padding:.85rem .9rem; border-radius:14px;
  border:1px solid rgba(49,51,63,.10); background:rgba(49,51,63,.02);
  font-size:.90rem; line-height:1.48; color:rgba(49,51,63,.86);}
.hm-info .title{font-weight:900; margin-bottom:.25rem;}
.hm-info .section{margin-top:.70rem; padding-top:.60rem; border-top:1px solid rgba(49,51,63,.08);}
.hm-info .label{font-weight:900;}
.hm-info ul{margin:.25rem 0 0 1.05rem;}
.hm-info li{margin:.18rem 0;}
.hm-info .muted{color:rgba(49,51,63,.66); font-size:.86rem;}

/* Dept cards */
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

# ---- header + call button ----
st.title("히즈메디병원")
st.markdown(f'<a class="hm-call" href="tel:{CALL}">📞 대표번호 전화하기 · {CALL}</a>', unsafe_allow_html=True)

# ---- 안내문 (replaced) ----
st.markdown(f"""
<div class="hm-info">
  <div class="title">안내</div>

  <div>
    <span class="label">직원 추천 진료과</span> <b>[외과]</b><br/>
    하지정맥류·탈장 진료: <b>최영수 과장</b>
  </div>

  <div class="section">
    <span class="label">진료시간</span> <span class="muted">(소아청소년과 제외)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 17:30</li>
      <li><b>토요일</b> 08:30 ~ 12:30</li>
      <li><b>점심</b> 12:30 ~ 13:30</li>
    </ul>
  </div>

  <div class="section">
    <span class="label">온라인 예약 신청 절차</span>
    <ul>
      <li>예약 신청 → 전문 상담원 콜백 → 예약 확정</li>
      <li>상담원과 통화 후 예약이 확정됩니다.</li>
      <li>야간 및 주말에는 연락드리지 않습니다.</li>
      <li>당일 예약은 불가하며, 익일부터 가능합니다.</li>
    </ul>
  </div>

  <div class="section">
    <span class="label">소아청소년과 안내</span> <span class="muted">(예약 없이 당일 진료 / 달빛어린이병원)</span>
    <ul>
      <li><b>평일</b> 08:30 ~ 23:00</li>
      <li><b>주말·공휴일</b> 09:00 ~ 18:00</li>
      <li>영유아검진·검사 예약: <b>☎ {CALL}</b></li>
    </ul>
  </div>
</div>
""", unsafe_allow_html=True)

# ---- data ----
df = load_departments()
if df is None or df.empty:
    st.info("현재 진료과 정보가 없습니다."); st.stop()

for c in ("dept_id","dept_name","dept_reservation_url","dept_detail_url","dept_schedule_url","display_order","is_active"):
    if c not in df.columns: df[c] = ""

df = df[df["is_active"].apply(ok)]
if "display_order" in df.columns: df = df.sort_values("display_order", na_position="last")

# ---- render ----
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
