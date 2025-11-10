import os
import streamlit as st
import pandas as pd

try:
    from supabase import create_client, Client
except Exception:
    create_client = None
    Client = None

# =========================
# 다빈도 진단 목록 (코드-명 매핑)
# =========================
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

# =========================
# 기본 UI
# =========================
st.set_page_config(page_title="내과 처방 조회", page_icon="💊", layout="wide")
st.title("내과 처방 조회")

# =========================
# Supabase 연결
# =========================
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

sb: Client = get_supabase()
TABLE = "prescriptions"  # 실제 테이블명으로 맞추세요.

# =========================
# 공통 함수
# =========================
@st.cache_data(show_spinner=False)
def distinct_values(column: str, base_filters: dict, limit: int = 5000):
    """
    현재 필터 상황(진단코드 포함)에 맞춰 column의 고유값 목록을 가져옵니다.
    - Supabase distinct 옵션 사용
    - base_filters: {"진단코드": "E119", "처방구분": "..."} 등 eq 필터용
    """
    if sb is None:
        return ["전체"]
    q = sb.table(TABLE).select(column, distinct=True).order(column)
    for k, v in base_filters.items():
        if v and v != "전체":
            q = q.eq(k, v)
    try:
        data = q.limit(limit).execute()
        vals = [row.get(column) for row in (data.data or []) if row.get(column)]
        vals = sorted(set(vals))
        return ["전체"] + vals
    except Exception:
        return ["전체"]

def run_query(filters: dict, limit: int = 5000):
    """
    선택한 드롭다운(진단코드/처방구분/환자번호/진료일)로 AND 필터 조회.
    """
    if sb is None:
        return pd.DataFrame(), 0

    q = sb.table(TABLE).select("*", count="exact").order("created_at", desc=True)
    for k, v in filters.items():
        if v and v != "전체":
            q = q.eq(k, v)

    # 서버 과부하 방지용 상한
    res = q.limit(limit).execute()
    rows = res.data or []
    total = res.count or 0
    return pd.DataFrame(rows), total

def chip(text: str):
    st.markdown(
        f"""
        <span style="display:inline-block;padding:4px 10px;border-radius:999px;
        background:#f1f5f9;border:1px solid #e2e8f0;font-size:12px;">
        {text}
        </span>
        """,
        unsafe_allow_html=True,
    )

# =========================
# 세션 상태 (선택 유지)
# =========================
if "sel_code" not in st.session_state:
    st.session_state.sel_code = "전체"
if "sel_rx" not in st.session_state:
    st.session_state.sel_rx = "전체"
if "sel_pt" not in st.session_state:
    st.session_state.sel_pt = "전체"
if "sel_visit" not in st.session_state:
    st.session_state.sel_visit = "전체"

# =========================
# 탭
# =========================
tab_view, tab_info = st.tabs(["조회", "설명(다빈도 진단)"])

with tab_view:
    st.caption("진단코드 선택 후, 다른 드롭다운을 추가로 선택하면 조건이 누적되어 결과가 줄어듭니다.")

    # 상단 선택 영역
    c1, c2, c3, c4 = st.columns([1.4, 1.4, 1.2, 1.2])

    # (1) 진단코드 — 선택 시 진단명은 자동 동기화(표시만)
    code_options = ["전체"] + [c for c, _ in FREQUENT_DIAG_ITEMS]
    st.session_state.sel_code = c1.selectbox("진단코드", code_options, index=code_options.index(st.session_state.sel_code) if st.session_state.sel_code in code_options else 0)
    diag_name = "" if st.session_state.sel_code == "전체" else DIAG_CODE2NAME.get(st.session_state.sel_code, "")
    c1.caption(f"진단명: {diag_name or '-'}")

    # 현재까지의 기준 필터(진단코드만 사용)로 나머지 옵션 리스트 가져오기
    base = {"진단코드": st.session_state.sel_code}

    # (2) 처방구분 — 드롭다운
    rx_options = distinct_values("처방구분", base)
    st.session_state.sel_rx = c2.selectbox("처방구분", rx_options, index=rx_options.index(st.session_state.sel_rx) if st.session_state.sel_rx in rx_options else 0)

    # (3) 환자번호 — 드롭다운
    base_rx = {**base, "처방구분": st.session_state.sel_rx}
    pt_options = distinct_values("환자번호", base_rx)
    st.session_state.sel_pt = c3.selectbox("환자번호", pt_options, index=pt_options.index(st.session_state.sel_pt) if st.session_state.sel_pt in pt_options else 0)

    # (4) 진료일(텍스트) — 드롭다운
    base_pt = {**base_rx, "환자번호": st.session_state.sel_pt}
    visit_options = distinct_values("진료일", base_pt)
    st.session_state.sel_visit = c4.selectbox("진료일", visit_options, index=visit_options.index(st.session_state.sel_visit) if st.session_state.sel_visit in visit_options else 0)

    st.divider()

    # 통합 자유검색 (클라이언트 단 부분일치)
    free_q = st.text_input("통합 검색(선택): 진단코드·진단명·처방구분·환자번호·진료일 텍스트 전체에 부분일치")

    # 조회 버튼
    run = st.button("조회", type="primary", use_container_width=True)

    if run:
        filters = {
            "진단코드": st.session_state.sel_code,
            "처방구분": st.session_state.sel_rx,
            "환자번호": st.session_state.sel_pt,
            "진료일": st.session_state.sel_visit,
        }
        df, total = run_query(filters)

        # 통합검색 적용
        if free_q.strip() and not df.empty:
            q = free_q.strip().lower()
            def match_row(row):
                values = [
                    row.get("진단코드", ""),
                    DIAG_CODE2NAME.get(row.get("진단코드", ""), ""),
                    row.get("처방구분", ""),
                    row.get("환자번호", ""),
                    row.get("진료일", ""),
                ]
                return any(q in str(v).lower() for v in values)
            df = df[df.apply(match_row, axis=1)]

        # 헤더/요약
        left, right = st.columns([3, 2], vertical_alignment="center")
        with left:
            chip(f"총 {total:,}건")
            chip(f"표시 {0 if df.empty else len(df):,}건")
        with right:
            if st.session_state.sel_code != "전체":
                chip(f"{st.session_state.sel_code} · {diag_name}")

        # 테이블
        if df.empty:
            st.info("조회 결과가 없습니다.")
        else:
            preferred = ["id", "진단코드", "진단명", "진료과", "진료일", "환자번호", "처방구분", "처방명", "created_at"]
            if "진단명" not in df.columns:
                # 테이블에 진단명이 없다면 표시용으로 추가
                df["진단명"] = df["진단코드"].map(DIAG_CODE2NAME).fillna(df.get("진단명"))
            ordered = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
            st.dataframe(df[ordered], use_container_width=True, hide_index=True)

    elif sb is None:
        st.warning("Supabase 연결이 설정되지 않았습니다. 환경변수(SUPABASE_URL, SUPABASE_KEY)를 확인하세요.")

with tab_info:
    st.subheader("우리병원의 다빈도 진단명")
    st.caption("진단코드는 아래 목록을 기반으로 선택하며, 진단명은 자동 동기화되어 표시됩니다.")
    df_info = pd.DataFrame(FREQUENT_DIAG_ITEMS, columns=["진단코드", "진단명"])
    q = st.text_input("다빈도 목록 검색", placeholder="코드 또는 명으로 검색 (부분일치)")
    if q.strip():
        ql = q.strip().lower()
        df_show = df_info[
            df_info["진단코드"].str.lower().str.contains(ql) | df_info["진단명"].str.lower().str.contains(ql)
        ]
    else:
        df_show = df_info
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.markdown(
        """
        - 진단명은 선택 대상이 아니며, **진단코드 선택에 따라 자동 표시**됩니다.  
        - **처방구분 → 환자번호 → 진료일** 순서로 드롭다운을 선택하면 조건이 누적되어 좁혀집니다.  
        - 상단의 **통합 검색**은 결과표에서 부분일치로 추가 필터합니다.
        - 엑셀 다운로드 기능은 제공하지 않습니다.
        """
    )
