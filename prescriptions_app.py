import os
import streamlit as st
import pandas as pd

# ==================== MUST be first Streamlit command ====================
st.set_page_config(page_title="내과 처방 조회(타병원)", page_icon="💊", layout="wide")

# -------------------- Optional Supabase import --------------------
try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None

# ==================== Frequent Dx (code ↔ name) ====================
FREQUENT_DIAG_ITEMS = [
    ("E785", "상세불명의 고지질혈증"),
    ("K210", "식도염을 동반한 위-식도역류병"),
    ("I109", "기타 및 상세불명의 원발성 고혈압"),
    ("K635", "결장의 폴립"),
    ("E119", "합병증을 동반하지 않은 2형 당뇨병"),
    ("K294", "만성 위축성 위염"),
    ("R739", "상세불명의 고혈당증"),
    ("E789", "Disorder of lipoprotein metabolism, unspecified"),
    ("R1049", "상세불명의 복통"),
    ("A099", "Gastroenteritis and colitis of unspecified origin"),
    ("J209", "상세불명의 급성 기관지염"),
    ("E784", "Other hyperlipidaemia"),
    ("K269", "출혈 또는 천공이 없는 급성인지 만성인지 상세불명인 십이지장궤양"),
    ("K208", "Other and unspecified oesophagitis"),
    ("E559", "상세불명의 비타민D결핍"),
    ("R194", "배변습관 변화"),
    ("J303", "기타 앨러지비염"),
    ("K297", "상세불명의 위염"),
    ("J304", "상세불명의 앨러지비염"),
    ("R51", "두통"),
    ("K317", "위 및 십이지장의 폴립"),
    ("E039", "상세불명의 갑상선기능저하증"),
    ("B980", "다른 장에서 분류된 질환의 원인으로서의 헬리코박터 파일로리균"),
    ("K759", "간염 NOS"),
    ("K5909", "기타 및 상세불명 변비"),
    ("R074", "상세불명의 흉통"),
    ("K599", "상세불명의 기능성 장장애"),
    ("D122", "상행결장의 양성 신생물"),
    ("A049", "세균성 장염 NOS"),
    ("J189", "상세불명의 폐렴"),
    ("Z000", "일반적 의학검사"),
    ("E079", "상세불명의 갑상선의 장애"),
    ("E041", "갑상선 (낭성) 결절 NOS"),
    ("K291", "기타 급성 위염"),
    ("J9840", "고립성 폐결절"),
    ("I652", "경동맥의 폐쇄 및 협착"),
    ("D123", "횡행결장의 양성 신생물"),
    ("D125", "구불결장의 양성 신생물"),
    ("K8080", "폐색의 언급이 없는 기타 담석증"),
    ("D126", "대장의 양성 신생물 NOS"),
    ("D509", "상세불명의 철결핍빈혈"),
    ("K2531", "출혈 또는 천공이 없는 급성 위궤양"),
    ("J399", "Disease of upper respiratory tract, unspecified"),
    ("K769", "상세불명의 간질환"),
    ("A090", "감염성 기원의 기타 및 상세불명의 위장염 및 결장염"),
    ("K716", "Toxic liver disease with hepatitis, NEC"),
    ("K293", "만성 표재성 위염"),
    ("K267", "출혈 또는 천공이 없는 만성 십이지장궤양"),
    ("I209", "상세불명의 협심증"),
    ("K8280", "담낭 또는 담낭관의 폴립"),
    ("K296", "기타 위염"),
    ("R53", "Malaise and fatigue"),
    ("E669", "상세불명의 비만"),
    ("K219", "식도염을 동반하지 않은 위-식도역류병"),
    ("E782", "Mixed hyperlipidaemia"),
    ("G319", "신경계통의 상세불명 퇴행성 질환"),
    ("D124", "하행결장의 양성 신생물"),
    ("R42", "어지럼증 및 어지럼"),
    ("G470", "수면 개시 및 유지 장애[불면증]"),
    ("R945", "간기능검사의 이상결과"),
    ("K758", "Other specified inflammatory liver diseases"),
    ("L500", "앨러지성 두드러기"),
    ("J459", "상세불명의 천식"),
    ("I70990", "괴저를 동반하지 않은 상세불명의 죽상경화증"),
    ("M8109", "폐경후골다공증, 상세불명 부분"),
]
DIAG_CODE2NAME = {c: n for c, n in FREQUENT_DIAG_ITEMS}

# ==================== Minimal CSS (incl. requests) ====================
st.markdown(
    """
    <style>
    /* Header spacing */
    [data-testid="stHeader"] {height:34px; padding:0; background:transparent;}
    section.main > div, .block-container {padding-top:10px !important;}

    .page-title {margin:2px 0 6px 0; font-weight:700;}

    /* Toolbar chips (same height) */
    .toolbar {display:flex; gap:8px; align-items:center; flex-wrap:nowrap;}
    .chip {display:inline-flex; align-items:center; height:28px; padding:0 10px; border-radius:10px; font-size:12px; border:1px solid;}
    .chip.grey {background:#f8fafc; border-color:#e2e8f0; color:#0f172a;}
    .chip.blue {background:#eff6ff; border-color:#bfdbfe; color:#1e40af;}

    /* Popover trigger wider */
    div[data-testid="stPopover"] > button { width: 100% !important; }

    /* Make the only button (검색 초기화) light blue */
    .stButton > button {
        background:#e0f2fe !important;
        border:1px solid #bfdbfe !important;
        color:#1e40af !important;
    }

    /* Dataframe spacing */
    [data-testid="stDataFrame"] {margin-top:6px;}
    </style>
    """,
    unsafe_allow_html=True,
)

# ==================== Supabase helpers ====================
@st.cache_resource(show_spinner=False)
def get_supabase():
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    if not url or not key or create_client is None:
        return None
    try:
        return create_client(url, key)
    except Exception:
        return None

sb: "Client | None" = get_supabase()
TABLE = "prescriptions"  # <-- 테이블명에 맞게 사용하세요.

def get_distinct(column: str, eq_filters: dict, limit: int = 10000):
    if sb is None:
        return ["전체"]
    q = sb.table(TABLE).select(column)
    for k, v in eq_filters.items():
        if v and v != "전체":
            q = q.eq(k, v)
    try:
        data = q.limit(limit).execute()
        vals = [row.get(column) for row in (data.data or []) if row.get(column)]
    except Exception:
        return ["전체"]
    vals = sorted(set([v for v in vals if v not in (None, "")]))
    return ["전체"] + vals if vals else ["전체"]

def run_query(filters: dict, limit: int = 1000):
    if sb is None:
        return pd.DataFrame(), 0
    q = sb.table(TABLE).select("*", count="exact").order("created_at", desc=True)
    for k, v in filters.items():
        if v and v != "전체":
            q = q.eq(k, v)
    res = q.limit(limit).execute()
    rows = res.data or []
    total = res.count or 0
    return pd.DataFrame(rows), total

def run_count_only(filters: dict):
    if sb is None:
        return 0
    q = sb.table(TABLE).select("id", count="exact")
    for k, v in filters.items():
        if v and v != "전체":
            q = q.eq(k, v)
    res = q.limit(1).execute()
    return res.count or 0

# ==================== Session defaults ====================
defaults = {"sel_code": "전체", "sel_rx": "전체", "sel_pt": "전체", "sel_visit": "전체", "free_q": ""}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ==================== Layout ====================
st.markdown("<h4 class='page-title'>💊 내과 처방 조회(타병원)</h4>", unsafe_allow_html=True)

left, right = st.columns([1.2, 2.4])

with left:
    st.write("드롭다운을 추가로 선택하면 조건이 누적됩니다.")

    # 상단 툴바: 좌측(Hismedi Dx 팝오버 길게), 우측(검색 초기화 파랑 버튼)
    lc, rc = st.columns([1.9, 0.6])
    with lc:
        diag_df = pd.DataFrame(FREQUENT_DIAG_ITEMS, columns=["진단코드", "진단명"])
        try:
            pop = st.popover("Hismedi Dx(다빈도순)")
            with pop:
                st.dataframe(diag_df, use_container_width=True, hide_index=True, height=420)
        except Exception:
            with st.expander("Hismedi Dx(다빈도순)"):
                st.dataframe(diag_df, use_container_width=True, hide_index=True, height=420)

    with rc:
        if st.button("검색 초기화", use_container_width=True):
            for k in ["sel_code", "sel_rx", "sel_pt", "sel_visit", "free_q"]:
                st.session_state[k] = "전체" if k != "free_q" else ""
            st.rerun()

    # 필터 드롭다운
    code_options = ["전체"] + [c for c, _ in FREQUENT_DIAG_ITEMS]
    st.selectbox(
        "진단코드",
        code_options,
        index=code_options.index(st.session_state.sel_code) if st.session_state.sel_code in code_options else 0,
        format_func=lambda c: "전체" if c == "전체" else f"{c} · {DIAG_CODE2NAME.get(c, '')}",
        key="sel_code",
    )

    rx_options = get_distinct("처방구분", {"진단코드": st.session_state.sel_code})
    st.selectbox("처방구분", rx_options,
                 index=rx_options.index(st.session_state.sel_rx) if st.session_state.sel_rx in rx_options else 0,
                 key="sel_rx")

    pt_options = get_distinct("환자번호", {"진단코드": st.session_state.sel_code, "처방구분": st.session_state.sel_rx})
    st.selectbox("환자번호", pt_options,
                 index=pt_options.index(st.session_state.sel_pt) if st.session_state.sel_pt in pt_options else 0,
                 key="sel_pt")

    visit_options = get_distinct("진료일", {
        "진단코드": st.session_state.sel_code,
        "처방구분": st.session_state.sel_rx,
        "환자번호": st.session_state.sel_pt
    })
    st.selectbox("진료일", visit_options,
                 index=visit_options.index(st.session_state.sel_visit) if st.session_state.sel_visit in visit_options else 0,
                 key="sel_visit")

    st.text_input("통합검색(일부 단어 입력) ", key="free_q", placeholder="진단코드·진단명·처방구분·처방명·환자번호·진료일 중 일부 입력")

with right:
    any_filter = any([
        st.session_state.sel_code != "전체",
        st.session_state.sel_rx != "전체",
        st.session_state.sel_pt != "전체",
        st.session_state.sel_visit != "전체",
        st.session_state.free_q.strip() != ""
    ])
    filters = {
        "진단코드": st.session_state.sel_code,
        "처방구분": st.session_state.sel_rx,
        "환자번호": st.session_state.sel_pt,
        "진료일":   st.session_state.sel_visit,
    }

    if not any_filter:
        total = run_count_only(filters)
        shown = 0
        # 같은 줄, 같은 높이의 칩 1개만 노출
        bar = f"<div class='toolbar'><span class='chip grey'>총 {total:,}건 / 표시 {shown:,}건</span></div>"
        st.markdown(bar, unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(columns=["진료과","진료일","환자번호","처방구분","처방명"]),
                     use_container_width=True, hide_index=True, height=640)
    else:
        df, total = run_query(filters, limit=1000)

        # free text filter
        if st.session_state.free_q.strip() and not df.empty:
            q = st.session_state.free_q.strip().lower()
            def match_row(row):
                values = [
                    row.get("진단코드", ""),
                    DIAG_CODE2NAME.get(row.get("진단코드",""), ""),
                    row.get("처방구분",""),
                    row.get("처방명",""),
                    row.get("환자번호",""),
                    row.get("진료일",""),
                ]
                return any(q in str(v).lower() for v in values)
            df = df[df.apply(match_row, axis=1)]

        shown = 0 if df.empty else len(df)

        # 같은 줄, 같은 높이 칩 2개(카운트 + 진단바)
        chips = [f"<span class='chip grey'>총 {total:,}건 / 표시 {shown:,}건</span>"]
        if st.session_state.sel_code and st.session_state.sel_code != "전체":
            sel_name = DIAG_CODE2NAME.get(st.session_state.sel_code, "")
            chips.append(f"<span class='chip blue'>{st.session_state.sel_code} · {sel_name}</span>")
        st.markdown(f"<div class='toolbar'>{''.join(chips)}</div>", unsafe_allow_html=True)

        if df.empty:
            st.info("검색(필터) 결과가 없습니다.")
        else:
            if "진단명" not in df.columns and "진단코드" in df.columns:
                df["진단명"] = df["진단코드"].map(DIAG_CODE2NAME).fillna(df.get("진단명"))
            drop_cols = [c for c in ["id", "created_at", "진단코드", "진단명"] if c in df.columns]
            df_show = df.drop(columns=drop_cols)

            preferred = ["진료과","진료일","환자번호","처방구분","처방명"]
            ordered = [c for c in preferred if c in df_show.columns] + [c for c in df_show.columns if c not in preferred]

            col_config = {}
            if "진료과" in ordered: col_config["진료과"] = st.column_config.TextColumn("진료과", width="small")
            if "진료일" in ordered: col_config["진료일"] = st.column_config.TextColumn("진료일", width="small")
            if "환자번호" in ordered: col_config["환자번호"] = st.column_config.TextColumn("환자번호", width="small")
            if "처방구분" in ordered: col_config["처방구분"] = st.column_config.TextColumn("처방구분", width="small")
            if "처방명" in ordered: col_config["처방명"] = st.column_config.TextColumn("처방명", width="large")

            st.dataframe(
                df_show[ordered],
                use_container_width=True,
                hide_index=True,
                column_config=col_config,
                height=640
            )
