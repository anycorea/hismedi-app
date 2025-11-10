import os
import math
import datetime as dt
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
DIAG_NAME2CODE = {n: c for c, n in FREQUENT_DIAG_ITEMS}

# =========================
# 기본 UI 설정
# =========================
st.set_page_config(
    page_title="처방 조회",
    page_icon="💊",
    layout="wide",
)

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
TABLE = "prescriptions"  # 실제 테이블명 사용

# =========================
# 공통 함수
# =========================
def query_prescriptions(
    diag_code: str | None = None,
    diag_name: str | None = None,
    rx_type: str | None = None,
    patient_no: str | None = None,
    visit_text: str | None = None,
    page: int = 1,
    page_size: int = 50,
):
    """
    Supabase에서 조건 조회.
    - 진단코드/진단명은 '함께' 필터 (둘 다 동일 로우 매칭)
    - 처방구분/환자번호/진료일(텍스트)은 부분일치(ilike)
    """
    if sb is None:
        return pd.DataFrame(), 0

    start = (page - 1) * page_size
    end = start + page_size - 1

    q = sb.table(TABLE).select("*", count="exact")

    if diag_code and diag_code != "전체":
        q = q.eq("진단코드", diag_code)
    if diag_name and diag_name != "전체":
        q = q.eq("진단명", diag_name)

    if rx_type:
        q = q.ilike("처방구분", f"%{rx_type}%")
    if patient_no:
        q = q.ilike("환자번호", f"%{patient_no}%")
    if visit_text:
        # 진료일이 텍스트 컬럼인 전제
        q = q.ilike("진료일", f"%{visit_text}%")

    # 정렬 (최신 생성순)
    q = q.order("created_at", desc=True)

    # 페이지네이션
    data = q.range(start, end).execute()

    rows = data.data or []
    total = data.count or 0
    df = pd.DataFrame(rows)
    return df, total

def chips(text: str):
    st.markdown(
        f"""
        <span style="
            display:inline-block;padding:4px 10px;border-radius:999px;
            background:#eef2ff;border:1px solid #c7d2fe;font-size:12px;">
            {text}
        </span>
        """,
        unsafe_allow_html=True,
    )

# =========================
# 세션 상태 (동기화용)
# =========================
if "sel_diag_code" not in st.session_state:
    st.session_state.sel_diag_code = "전체"
if "sel_diag_name" not in st.session_state:
    st.session_state.sel_diag_name = "전체"

def on_change_code():
    code = st.session_state.sel_diag_code
    if code == "전체":
        st.session_state.sel_diag_name = "전체"
    else:
        st.session_state.sel_diag_name = DIAG_CODE2NAME.get(code, st.session_state.sel_diag_name)

def on_change_name():
    name = st.session_state.sel_diag_name
    if name == "전체":
        st.session_state.sel_diag_code = "전체"
    else:
        st.session_state.sel_diag_code = DIAG_NAME2CODE.get(name, st.session_state.sel_diag_code)

# =========================
# 레이아웃
# =========================
tab_view, tab_info = st.tabs(["조회", "설명(다빈도 진단)"])

with tab_view:
    st.subheader("처방 조회")

    st.caption("진단코드와 진단명은 함께 선택됩니다. (전체/각각)")

    colA, colB, colC, colD = st.columns([2.2, 1.2, 1.5, 1.5])

    # (1) 진단코드/진단명 — 함께 움직임
    with colA:
        left, right = st.columns(2)
        code_options = ["전체"] + [c for c, _ in FREQUENT_DIAG_ITEMS]
        name_options = ["전체"] + [n for _, n in FREQUENT_DIAG_ITEMS]

        st.selectbox(
            "진단코드",
            code_options,
            key="sel_diag_code",
            on_change=on_change_code,
            help="다빈도 목록 기준. 선택 시 '진단명'이 자동 동기화됩니다.",
        )
        st.selectbox(
            "진단명",
            name_options,
            key="sel_diag_name",
            on_change=on_change_name,
            help="다빈도 목록 기준. 선택 시 '진단코드'가 자동 동기화됩니다.",
        )

    # (2) 처방구분
    with colB:
        rx_type = st.text_input("처방구분 (부분일치)", placeholder="예: 일반, 조제, 외래 등")

    # (3) 환자번호
    with colC:
        patient_no = st.text_input("환자번호 (부분일치)", placeholder="예: 2300***")

    # (4) 진료일(텍스트)
    with colD:
        visit_text = st.text_input("진료일(텍스트)", placeholder="예: 2025-10, 2025/10/03, 10-03 등")

    # 추가: 자유 텍스트 통합검색(코드/명 포함)
    st.divider()
    free_q = st.text_input(
        "통합 검색(선택): 진단코드·진단명·처방구분·환자번호·진료일 텍스트 전체에 부분일치",
        placeholder="예: E119 또는 '위염' 또는 '2025-10'"
    )

    # 페이지네이션
    st.divider()
    colP1, colP2, colP3 = st.columns([1, 1, 6])
    page_size = colP1.selectbox("페이지 크기", [25, 50, 100, 200], index=1)
    page = colP2.number_input("페이지", min_value=1, step=1, value=1)

    # 조회 버튼
    run = st.button("조회", type="primary", use_container_width=True)

    # 쿼리 실행
    if run:
        # 우선 기본 조건으로 조회
        df, total = query_prescriptions(
            diag_code=None if st.session_state.sel_diag_code == "전체" else st.session_state.sel_diag_code,
            diag_name=None if st.session_state.sel_diag_name == "전체" else st.session_state.sel_diag_name,
            rx_type=rx_type.strip() or None,
            patient_no=patient_no.strip() or None,
            visit_text=visit_text.strip() or None,
            page=page,
            page_size=page_size,
        )

        # 통합 자유검색(free_q) 적용: 클라이언트 단 필터(부분일치)
        if free_q.strip():
            q = free_q.strip().lower()
            def match_any(cell):
                try:
                    return q in str(cell).lower()
                except Exception:
                    return False
            if not df.empty:
                df = df[df.apply(lambda r: any(match_any(x) for x in r.values), axis=1)]

        # 헤더 & 요약
        left, right = st.columns([3, 2], vertical_alignment="center")
        with left:
            chips(f"총 {total:,}건")
            if not df.empty:
                chips(f"현재 페이지 {len(df):,}건 표시")
        with right:
            st.write("")

        # 테이블 표시
        if df.empty:
            st.info("조회 결과가 없습니다.")
        else:
            # 컬럼 정렬 가독성(존재하는 컬럼만 유지)
            preferred = ["id", "진단코드", "진단명", "진료과", "진료일", "환자번호", "처방구분", "처방명", "created_at"]
            ordered_cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
            df = df[ordered_cols]
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
            )

            # 페이지네이션 안내
            total_pages = max(1, math.ceil(total / page_size))
            st.caption(f"페이지 {page} / {total_pages} (총 {total:,}건)")

    elif sb is None:
        st.warning("Supabase 연결이 설정되지 않았습니다. 환경변수(SUPABASE_URL, SUPABASE_KEY)를 확인하세요.")

with tab_info:
    st.subheader("우리병원의 다빈도 진단명")
    st.caption("아래 목록은 코드–명 쌍으로 제공됩니다. 상단 ‘조회’ 탭의 동기화 선택박스도 이 목록을 사용합니다.")

    df_info = pd.DataFrame(FREQUENT_DIAG_ITEMS, columns=["진단코드", "진단명"])

    # 간단 검색
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
        - ‘조회’ 탭에서 **진단코드** 또는 **진단명** 중 하나를 선택하면 다른 항목이 자동으로 맞춰집니다.
        - **전체**를 선택하면 해당 조건은 해제됩니다.
        - **처방구분 · 환자번호 · 진료일(텍스트)** 는 부분일치로 검색됩니다.
        - 엑셀 다운로드 기능은 제거되었습니다.
        """
    )
