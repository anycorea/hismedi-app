# -*- coding: utf-8 -*-
from __future__ import annotations

def _ensure_capacity(ws, min_row: int | None, min_col: int | None, *, max_cells: int | None = 9_000_000) -> bool:
    """
    Ensure worksheet grid is at least (min_row x min_col).
    Uses a single resize(rows=..., cols=...) call if expansion is needed.
    Returns True if resized, False otherwise or on failure.
    Set max_cells=None to disable the total-cells guard.
    """
    try:
        r_needed = int(min_row) if min_row else 0
        c_needed = int(min_col) if min_col else 0

        cur_r = int(getattr(ws, "row_count", 0) or 0)
        cur_c = int(getattr(ws, "col_count", 0) or 0)

        new_r = max(cur_r, r_needed)
        new_c = max(cur_c, c_needed)

        # No change needed
        if new_r == cur_r and new_c == cur_c:
            return False

        # Prevent accidental huge expansions (Google Sheets total ~10M cells limit)
        if max_cells is not None and new_r > 0 and new_c > 0:
            if new_r * new_c > max_cells:
                return False

        rows_arg = new_r if new_r > cur_r else None
        cols_arg = new_c if new_c > cur_c else None

        # Use retry wrapper if available
        try:
            _retry(ws.resize, rows=rows_arg, cols=cols_arg)  # network/429 tolerant
        except NameError:
            ws.resize(rows=rows_arg, cols=cols_arg)

        return True
    except Exception:
        # Non-fatal: callers may attempt again or handle errors upstream.
        return False

# HISMEDI HR App
# Tabs: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말

# ═════════════════════════════════════════════════════════════════════════════
# Imports
# ═════════════════════════════════════════════════════════════════════════════
import re, time, random, hashlib, secrets as pysecrets
from datetime import datetime, timedelta
from typing import Any, Tuple
import pandas as pd
import streamlit as st

# Header auto-fix toggle (user manages Google Sheet headers)

# ═════════════════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════════════════

# --- Google Sheets 429(quota) 감지 ------------------------------------------------
def _is_quota_429(err) -> bool:
    try:
        from gspread.exceptions import APIError as _APIError
        if isinstance(err, _APIError):
            resp = getattr(err, "response", None)
            return getattr(resp, "status_code", None) == 429
    except Exception:
        pass
    return False
# --- end ---------------------------------------------------------------------

AUTO_FIX_HEADERS = False

# ===== Cached summary helpers (performance) ==================================

@st.cache_data(ttl=300, show_spinner=False)
def get_eval_summary_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {(사번, 평가유형) -> (총점, 제출시각)} for given year.
    Fast path: 필수 컬럼 확인 후, 헤더 인덱스/연도 문자열/열 인덱스를 캐싱하여
    루프 내부 분기와 예외를 최소화.
    """
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()] if not items.empty else []
    try:
        ws = _ensure_eval_resp_sheet(int(_year), item_ids)
        header = _retry(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _ws_values(ws)
    except Exception:
        return {}

    # 필수 컬럼 확인 (연도/평가유형/평가대상사번)
    cY = hmap.get("연도")
    cT = hmap.get("평가유형")
    cTS = hmap.get("평가대상사번")
    if not (cY and cT and cTS):
        return {}

    # 인덱스·캐시
    cy = cY - 1
    ct = cT - 1
    cts = cTS - 1
    cTot = hmap.get("총점")
    cSub = hmap.get("제출시각")
    ctot = (cTot - 1) if cTot else None
    csub = (cSub - 1) if cSub else None
    year_s = str(_year)

    out: dict = {}
    # values[0]는 헤더, 데이터는 1행부터
    for r in values[1:]:
        # 연도 필터
        if cy >= len(r) or str(r[cy]).strip() != year_s:
            continue
        # 키 구성에 필요한 열 존재 확인
        if cts >= len(r) or ct >= len(r):
            continue

        key = (str(r[cts]).strip(), str(r[ct]).strip())
        tot = r[ctot] if (ctot is not None and ctot < len(r)) else ""
        sub = r[csub] if (csub is not None and csub < len(r)) else ""

        prev = out.get(key)
        # 제출시각 최신 값만 유지(문자 비교 기반: 기존 로직 유지)
        if prev is None or str(prev[1]) < str(sub):
            out[key] = (tot, sub)
    return out


@st.cache_data(ttl=300, show_spinner=False)
def get_comp_summary_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {사번 -> (주업무, 기타업무, 자격유지, 제출시각)} for given year.
    불필요한 예외 처리 제거, 인덱스 캐싱.
    """
    try:
        ws = _ensure_comp_simple_sheet(int(_year))
        header = _retry(ws.row_values, 1) or []
        hmap = {n: i + 1 for i, n in enumerate(header)}
        values = _ws_values(ws)
    except Exception:
        return {}

    cY = hmap.get("연도")
    cTS = hmap.get("평가대상사번")
    if not (cY and cTS):
        return {}

    cy = cY - 1
    cts = cTS - 1
    cMain = hmap.get("주업무평가")
    cExtra = hmap.get("기타업무평가")
    cQual = hmap.get("자격유지")
    cSub = hmap.get("제출시각")
    cmain = (cMain - 1) if cMain else None
    cextra = (cExtra - 1) if cExtra else None
    cqual = (cQual - 1) if cQual else None
    csub = (cSub - 1) if cSub else None
    year_s = str(_year)

    out: dict = {}
    for r in values[1:]:
        if cy >= len(r) or str(r[cy]).strip() != year_s:
            continue
        if cts >= len(r):
            continue

        sab = str(r[cts]).strip()
        main = r[cmain] if (cmain is not None and cmain < len(r)) else ""
        extra = r[cextra] if (cextra is not None and cextra < len(r)) else ""
        qual = r[cqual] if (cqual is not None and cqual < len(r)) else ""
        sub = r[csub] if (csub is not None and csub < len(r)) else ""

        prev = out.get(sab)
        if prev is None or str(prev[3]) < str(sub):
            out[sab] = (main, extra, qual, sub)
    return out


@st.cache_data(ttl=120, show_spinner=False)
def get_jd_approval_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    {(사번, 최신버전) -> (상태, 승인시각)} from 직무기술서_승인 for the year.
    정렬 대신 1패스(max)로 최신 승인시각만 선별하여 O(n) 처리.
    """
    try:
        ws = _ws("직무기술서_승인")
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        df = pd.DataFrame(columns=["연도", "사번", "버전", "상태", "승인시각"])

    # 필요한 컬럼만 유지(없으면 생성)
    need_cols = ["연도", "사번", "버전", "상태", "승인시각"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = pd.Series(dtype="object")
    df = df[need_cols]

    # 타입 정리
    df["연도"] = pd.to_numeric(df["연도"], errors="coerce").fillna(-1).astype(int)
    df["버전"] = pd.to_numeric(df["버전"], errors="coerce").fillna(0).astype(int)
    for c in ["사번", "상태", "승인시각"]:
        df[c] = df[c].astype(str)

    # 연도 필터
    df = df[df["연도"] == int(_year)]
    if df.empty:
        return {}

    # 1패스 최신값 선택
    out: dict = {}
    # itertuples가 iterrows보다 빠름
    for rr in df.itertuples(index=False, name=None):
        # 순서: 연도, 사번, 버전, 상태, 승인시각
        _, sab, ver, stat, when = rr
        key = (str(sab), int(ver))
        prev = out.get(key)
        if prev is None or str(prev[1]) < str(when):
            out[key] = (str(stat), str(when))
    return out

from html import escape as _html_escape

# --- Timezone helper (KST) ---------------------------------------------------
try:
    from zoneinfo import ZoneInfo
    def tz_kst():
        return ZoneInfo(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
except Exception:
    import pytz
    def tz_kst():
        return pytz.timezone(st.secrets.get("app", {}).get("TZ", "Asia/Seoul"))
# --- end ---------------------------------------------------------------------

# --- gspread imports ---------------------------------------------------------
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound, APIError
from gspread.utils import rowcol_to_a1
# --- end ---------------------------------------------------------------------

# --- Batch helper shims (fallback only; real batch utils가 없을 때 사용) -----
try:
    _ = rowcol_to_a1  # ensure imported

    if "gs_enqueue_range" not in globals():
        def gs_enqueue_range(ws, a1, values, value_input_option="USER_ENTERED"):
            ws.update(a1, values, value_input_option=value_input_option)

    if "gs_enqueue_cell" not in globals():
        def gs_enqueue_cell(ws, row, col, value, value_input_option="USER_ENTERED"):
            ws.update(rowcol_to_a1(int(row), int(col)), [[value]], value_input_option=value_input_option)

    if "gs_flush" not in globals():
        def gs_flush():
            return  # no-op
except Exception:
    pass
# --- end ---------------------------------------------------------------------

# ═════════════════════════════════════════════════════════════════════════════
# Sync Utility (session-scoped, throttled)
# ═════════════════════════════════════════════════════════════════════════════
SYNC_THROTTLE_SEC = 8  # 연타 방지 쿨다운(초)

def _cooldown_remaining() -> float:
    last = float(st.session_state.get("_last_sync_ts", 0) or 0)
    return SYNC_THROTTLE_SEC - (time.time() - last)

def force_sync(*, clear_resource: bool = False, clear_all_session: bool = False, hard: bool = False):
    """세션 한정 동기화(전역 캐시 보존)."""
    wait = _cooldown_remaining()
    if wait > 0:
        # 안내는 전역 토스트에서 처리 → 여기서는 종료
        return

    # 쿨다운 시작
    now = time.time()
    st.session_state["_last_sync_ts"] = now

    # 세션 전용 캐시 버스터 증가(나만 새로고침)
    st.session_state["cache_rev"] = int(st.session_state.get("cache_rev", 0)) + 1
    for k in ("jobdesc_rev", "comp_rev", "eval_rev"):
        st.session_state[k] = int(st.session_state.get(k, 0)) + 1

    # 모듈 딕셔너리 캐시만 정리
    try:
        if "_WS_CACHE"  in globals() and isinstance(globals()["_WS_CACHE"],  dict): globals()["_WS_CACHE"].clear()
        if "_HDR_CACHE" in globals() and isinstance(globals()["_HDR_CACHE"], dict): globals()["_HDR_CACHE"].clear()
        if "_VAL_CACHE" in globals() and isinstance(globals()["_VAL_CACHE"], dict): globals()["_VAL_CACHE"].clear()
    except Exception:
        globals().setdefault("_WS_CACHE", {})
        globals().setdefault("_HDR_CACHE", {})
        globals().setdefault("_VAL_CACHE", {})

    # 세션 상태 중 캐시/편집성 키만 선별 삭제(로그인/토큰 보존)
    SAFE_KEEP = {"user","access_token","refresh_token","login_time","login_provider",
                 "authed","auth_expires_at","_state_owner_sabun","_last_sync_ts"}
    PREFIXES  = ("__cache_","_df_","_cache_","gs_","eval2_","bulk_score_","cmpS_","jd2_","adm_","acl_")
    ACL_KEYS  = {"acl_df","acl_header","acl_editor","auth_editor","auth_editor_df","__auth_sab_sig"}
    try:
        ss = st.session_state
        if clear_all_session:
            to_del = [k for k in list(ss.keys()) if k not in SAFE_KEEP]
        else:
            to_del = []
            for k in list(ss.keys()):
                if k in SAFE_KEEP: continue
                if k in ACL_KEYS: to_del.append(k); continue
                if any(k.startswith(p) for p in PREFIXES): to_del.append(k); continue
        for k in to_del: ss.pop(k, None)
    except Exception:
        pass

    # 정말 필요할 때만 전역 캐시 비우기
    if hard:
        try: st.cache_data.clear()
        except Exception: pass
        if clear_resource:
            try: st.cache_resource.clear()
            except Exception: pass

    st.rerun()

# ────────────────────────────────────────────────────────────────────────────
# Global toast mount: 왼쪽 패널 "동기화" 버튼 위에 딱 붙는 고정 토스트
# ────────────────────────────────────────────────────────────────────────────
def mount_sync_toast():
    """세션 쿨다운 중이면 '동기화' 버튼 위에 토스트를 띄우고 초를 갱신한다."""
    try:
        import streamlit.components.v1 as components
        cool = _cooldown_remaining()

        # 쿨다운 종료 상태면 기존 토스트 제거
        if cool <= 0:
            components.html("""
            <script>(function(){
              const d = window.parent.document;
              const el = d.getElementById('sync_toast_fixed');
              if (el) { try{ el.remove(); }catch(e){} }
            })();</script>
            """, height=0, width=0)
            return

        end_ts_ms = int((float(st.session_state.get("_last_sync_ts", 0) or 0) + SYNC_THROTTLE_SEC) * 1000)
        components.html(f"""
        <script>
        (function(){{
          const d = window.parent.document;

          // 토스트 엘리먼트 생성/획득
          let toast = d.getElementById('sync_toast_fixed');
          if(!toast){{
            toast = d.createElement('div');
            toast.id = 'sync_toast_fixed';
            d.body.appendChild(toast);
          }}

          // "동기화" 버튼을 찾아 위치를 버튼 상단-우측으로 고정
          function anchorToSync(){{
            // 왼쪽 패널 안의 버튼 중 텍스트에 '동기화' 포함을 찾음
            const btns = Array.from(d.querySelectorAll('.stButton button'));
            const syncBtn = btns.find(b => (b.textContent||'').trim().includes('동기화'));
            if(!syncBtn) return false;

            const r = syncBtn.getBoundingClientRect();
            // 토스트 크기 측정 후 버튼 우측 상단에 맞춤 (5px 위로 띄움)
            const tw = toast.offsetWidth || 260;
            const th = toast.offsetHeight || 32;
            const top  = Math.max(8, r.top - th - 5) + window.scrollY;
            const left = (r.left + r.width - tw) + window.scrollX;

            toast.style.top  = top + 'px';
            toast.style.left = left + 'px';
            return true;
          }}

          // 카운트다운 + 위치 갱신 루프
          const end = {end_ts_ms};
          function tick(){{
            const now = Date.now();
            let r = Math.ceil((end - now)/1000);

            // 매 프레임 위치 갱신(창 리플로우/리사이즈 대응)
            anchorToSync() || (toast.style.top='84px', toast.style.right='22px');

            if (r <= 0){{
              toast.style.transition = 'opacity .2s ease'; toast.style.opacity = '0';
              setTimeout(()=>{{ try{{ toast.remove(); }}catch(_){{}} }}, 240);
              return;
            }}
            toast.textContent = "⏳ 잠시만요… " + r + "초 후 다시 시도해 주세요.";
            requestAnimationFrame(tick);
          }}
          tick();
        }})();
        </script>
        """, height=0, width=0)
    except Exception:
        pass

# ═════════════════════════════════════════════════════════════════════════════
# App Config / Style (compact header + top 0)
# ═════════════════════════════════════════════════════════════════════════════
APP_TITLE = st.secrets.get("app", {}).get("TITLE", "HISMEDI - 인사/HR")
st.set_page_config(page_title=APP_TITLE, layout="wide")

# === Anti-scroll-jump: definitive block ======================================
import streamlit.components.v1 as components
components.html("""
<script>
(function(){
  const d = window.parent?.document || document;
  const w = window;
  try{ if ('scrollRestoration' in w.history){ w.history.scrollRestoration = 'manual'; } }catch(_){}

  // CSS hardening (no anchor-based reflow jumps, no smooth scrolling)
  try{
    const style = d.createElement('style');
    style.textContent = `
      html, body{ scroll-behavior:auto !important; overflow-anchor:none !important; }
      .main{ scroll-behavior:auto !important; }
      [autofocus]{ outline: none !important; }
    `;
    d.head.appendChild(style);
  }catch(_){}

  // Remove autofocus attrs that may pull viewport
  function stripAutofocus(root){
    try{
      root.querySelectorAll('[autofocus]').forEach(el=>{ el.removeAttribute('autofocus'); });
    }catch(_){}
  }

  // Build a hard "no scroll" window to neutralize Streamlit focus/scrollIntoView
  const HOLD_MS = 3800;  // Tweak if needed
  const until = Date.now() + HOLD_MS;

  // Freeze current top
  try{ w.scrollTo(0,0); }catch(_){}

  // Monkey-patch scrolling primitives during hold window
  const _scrollTo = w.scrollTo.bind(w);
  w.scrollTo = function(x,y){
    if (Date.now() < until) { try{ _scrollTo(0,0); }catch(_){ } return; }
    return _scrollTo(x,y);
  };

  const _elScrollIntoView = Element.prototype.scrollIntoView;
  Element.prototype.scrollIntoView = function(){
    if (Date.now() < until){ return; }
    try{ return _elScrollIntoView.apply(this, arguments); }catch(_){}
  };

  // Kill focus-driven jumps
  const _focus = Element.prototype.focus;
  Element.prototype.focus = function(){
    if (Date.now() < until){ return; }
    try{ return _focus.apply(this, arguments); }catch(_){}
  };

  // Prevent manual attempts during hold
  function block(e){
    if (Date.now() < until){
      try{ e.preventDefault(); e.stopPropagation(); }catch(_){}
      try{ _scrollTo(0,0); }catch(_){}
    }
  }
  ['wheel','touchmove','keydown'].forEach(ev => d.addEventListener(ev, block, {capture:true, passive:false}));

  // Mutation observer to clean late autofocus
  const mo = new MutationObserver(()=>{
    if (Date.now() < until){
      stripAutofocus(d);
      try{ _scrollTo(0,0); }catch(_){}
    }else{
      try{ mo.disconnect(); }catch(_){}
    }
  });
  mo.observe(d.body, {childList:true, subtree:true, attributes:true, attributeFilter:['autofocus']});
  stripAutofocus(d);

  // After hold, restore patched APIs
  function release(){
    if (Date.now() >= until){
      try{ window.scrollTo = _scrollTo; }catch(_){}
      try{ Element.prototype.scrollIntoView = _elScrollIntoView; }catch(_){}
      try{ Element.prototype.focus = _focus; }catch(_){}
      return;
    }
    requestAnimationFrame(release);
  }
  requestAnimationFrame(release);
})();
</script>
""", height=0, width=0)
# =============================================================================

# st.help 출력 무력화
if not getattr(st, "_help_disabled", False):
    def _noop_help(*_a, **_kw): return None
    st.help = _noop_help
    st._help_disabled = True

COMPACT_HEADER_H = 32  # px: 헤더 높이 (아이콘 보이도록 최소값)

_CSS_GLOBAL = f"""
<style>
  /* ── Top spacing 0 ─────────────────────────────────────────────────────── */
  div.block-container{{padding-top:0!important;margin-top:0!important}}
  section[data-testid="stSidebar"] .block-container{{padding-top:0!important}}
  html, body {{ scroll-behavior: auto !important; }}
  div.block-container > :first-child {{ margin-top: 0!important; }}

  /* ── Compact Header (메뉴 아이콘 유지, 높이만 최소화) ──────────────── */
  header[data-testid="stHeader"]{{
    display:flex!important; align-items:center!important; justify-content:flex-end!important;
    height:{COMPACT_HEADER_H}px!important; min-height:{COMPACT_HEADER_H}px!important;
    padding:0 10px!important; margin:0!important; background:transparent!important;
    border:0!important; box-shadow:none!important;
  }}
  /* 상단 데코 바 얇게(점프 방지) */
  div[data-testid="stDecoration"]{{height:2px!important}}

  /* 빈 단락 제거 (과거 True/False 잔상 방지) */
  div.block-container > p:empty{{display:none!important;margin:0!important;padding:0!important}}

  /* 버튼 클릭/포커스 흔들림 방지 */
  .stButton>button:focus, .stButton>button:focus-visible{{outline:none!important;box-shadow:none!important}}
  .stButton>button{{border-width:1px!important;border-color:#e5e7eb!important;transition:none!important;transform:none!important}}

  /* 토스트(위치는 JS에서 "동기화" 버튼 위로 고정) */
  #sync_toast_fixed{{
    position:fixed; z-index:9999;
    background:#eef2ff; color:#1e3a8a;
    border:1px solid #c7d2fe; border-radius:12px;
    padding:8px 12px; font-weight:700; line-height:1.2;
    box-shadow:0 6px 18px rgba(0,0,0,.08);
    pointer-events:none; white-space:nowrap;
  }}

  /* ── 기존 스타일 유지 ─────────────────────────────────────────────── */
  .stTabs [role='tab']{{padding:10px 16px!important;font-size:1.02rem!important}}
  .badge{{display:inline-block;padding:.25rem .5rem;border-radius:.5rem;border:1px solid #9ae6b4;background:#e6ffed;color:#0f5132;font-weight:600}}
  section[data-testid="stHelp"],div[data-testid="stHelp"]{{display:none!important}}
  .muted{{color:#6b7280}}
  .app-title-hero{{font-weight:800;font-size:1.6rem;line-height:1.15;margin:.1rem 0 .5rem}}
  @media (min-width:1400px){{.app-title-hero{{font-size:1.8rem}}}}
  div[data-testid="stFormSubmitButton"] button[kind="secondary"]{{padding:.35rem .5rem;font-size:.82rem}}
  .scrollbox{{max-height:280px;overflow-y:auto;padding:.6rem .75rem;background:#fafafa;border:1px solid #e5e7eb;border-radius:.5rem}}
  .scrollbox .kv{{margin-bottom:.6rem}}
  .scrollbox .k{{font-weight:700;margin-bottom:.2rem}}
  .scrollbox .v{{white-space:pre-wrap;word-break:break-word;line-height:1.42}}
  .jd-tight{{line-height:1.42}}
  .jd-tight p,.jd-tight ul,.jd-tight ol,.jd-tight li{{margin:0;padding:0}}
  .submit-banner{{background:#FEF3C7;border:1px solid #FDE68A;padding:.55rem .8rem;border-radius:.5rem;font-weight:600;line-height:1.35;margin:4px 0 14px;display:block}}
  div[data-testid="stDataFrame"]>div{{overflow-x:visible!important}}
  div[data-testid="stDataFrame"] [role="grid"]{{overflow-x:auto!important}}
  div[data-testid="stDataFrame"]{{padding-bottom:10px}}
</style>
"""
st.markdown(_CSS_GLOBAL, unsafe_allow_html=True)

# 스크롤 최상단 고정 (rerun 및 버튼 클릭 뒤 점프 억제: 항상 TOP)
# ────────────────────────────────────────────────────────────────────────────
# Stray True/False suppressor (safely hides random boolean paragraphs)
# ────────────────────────────────────────────────────────────────────────────
def _suppress_magic_booleans():
    try:
        import streamlit.components.v1 as components
        components.html(
            """
            <script>
            (function(){
              const doc = window.parent.document;
              const ps = Array.from(doc.querySelectorAll('div.block-container p'));
              for (const p of ps){
                const t = (p.textContent||"").trim();
                if ((t==="True"||t==="False") && !p.closest('[role="grid"]')){
                  p.style.display = "none";
                }
              }
            })();
            </script>
            """,
            height=0, width=0
        )
    except Exception:
        pass


# ═════════════════════════════════════════════════════════════════════════════
# Utils
# ═════════════════════════════════════════════════════════════════════════════
from html import escape as _html_escape  # ← 직접 임포트로 고정 (화면 출력 없음)
_sha256 = hashlib.sha256                 # 속도 미세 최적화: 룩업 캐시

def current_year() -> int:
    """KST 기준 현재 연도(실패 시 시스템 연도)."""
    try:
        return datetime.now(tz=tz_kst()).year
    except Exception:
        return datetime.now().year

def kst_now_str() -> str:
    """KST 현재 시각 문자열."""
    try:
        return datetime.now(tz=tz_kst()).strftime("%Y-%m-%d %H:%M:%S (%Z)")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _jd_plain_html(text: str) -> str:
    """JD 요약 HTML(개행 유지, 안전 이스케이프)."""
    s = "" if text is None else str(text)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    return '<div class="jd-tight">' + _html_escape(s).replace("\n", "<br>") + "</div>"

def _sha256_hex(s: str) -> str:
    return _sha256(str(s).encode()).hexdigest()

def _to_bool(x) -> bool:
    return str(x).strip().lower() in ("true", "1", "y", "yes", "t")

def _normalize_private_key(raw: str) -> str:
    """Multiline private key 문자열에서 \\n → 개행 복원(조건부)."""
    if not raw:
        return raw
    return raw.replace("\\n", "\n") if "\\n" in raw and "BEGIN PRIVATE KEY" in raw else raw

def _pin_hash(pin: str, sabun: str) -> str:
    """사번+PIN 기반 고정 솔팅 SHA-256."""
    return _sha256(f"{str(sabun).strip()}:{str(pin).strip()}".encode()).hexdigest()

def show_submit_banner(text: str) -> None:
    """상단 제출 안내 배너."""
    try:
        st.markdown(f"<div class='submit-banner'>{text}</div>", unsafe_allow_html=True)
    except Exception:
        st.info(text)

# ────────────────────────────────────────────────────────────────────────────
# PIN Utilities (clean)
# ────────────────────────────────────────────────────────────────────────────
def verify_pin(user_sabun: str, pin: str) -> bool:
    """
    제출 직전 PIN 재인증 (로그인 로직과 동일한 허용 범위).
    저장 포맷 허용:
      - SHA256(pin)
      - SHA256(sabun:pin)
    우선순위:
      1) st.session_state["pin_map"]             → 평문 비교
      2) st.session_state["pin_hash_map"]        → 해시 비교(단일/솔트)
      3) st.session_state["user"]                → pin / pin_hash 필드
      4) st.session_state["emp_df"].PIN_hash     → 보조 검증
    """
    sabun = str(user_sabun).strip()
    val = str(pin).strip()
    if not sabun or not val:
        return False

    # 해시는 1회만 계산해서 재사용
    plain_hash = _sha256_hex(val).lower()
    salted_hash = _pin_hash(val, sabun).lower()

    ss = st.session_state  # 로컬 바인딩(미세 가속)

    # 1) 평문 맵
    pin_map = ss.get("pin_map")
    if isinstance(pin_map, dict):
        stored_plain = pin_map.get(sabun)
        if stored_plain is not None and str(stored_plain) == val:
            return True

    # 2) 해시 맵
    pin_hash_map = ss.get("pin_hash_map")
    if isinstance(pin_hash_map, dict):
        stored_hash = str(pin_hash_map.get(sabun, "")).lower().strip()
        if stored_hash and (stored_hash == plain_hash or stored_hash == salted_hash):
            return True

    # 3) 세션 사용자 객체
    u = ss.get("user") or {}
    if str(u.get("사번", "")).strip() == sabun:
        up = u.get("pin")
        if up is not None and str(up) == val:
            return True
        uph = str(u.get("pin_hash", "")).lower().strip()
        if uph and (uph == plain_hash or uph == salted_hash):
            return True

    # 4) 직원 DF 기반 보조 검증 (emp_df 조회)
    try:
        emp_df = ss.get("emp_df")
        if emp_df is not None and "사번" in emp_df.columns:
            row = None

            # 먼저 숫자 비교(열 dtype이 숫자일 때 빠름)
            if sabun.isdigit():
                m_num = emp_df["사번"].eq(int(sabun))
                if m_num.any():
                    row = emp_df.loc[m_num]

            # 실패 시 문자열 비교(폴백)
            if row is None or row.empty:
                m_str = emp_df["사번"].astype(str).eq(sabun)
                if m_str.any():
                    row = emp_df.loc[m_str]

            if row is not None and not row.empty:
                stored_hash = str(row.iloc[0].get("PIN_hash", "")).lower().strip()
                if stored_hash and (stored_hash == plain_hash or stored_hash == salted_hash):
                    return True
    except Exception:
        # emp_df 부재/형식 이슈 등은 조용히 무시
        pass

    return False

# ═════════════════════════════════════════════════════════════════════════════
# Google Auth / Sheets
# ═════════════════════════════════════════════════════════════════════════════

# 지수 백오프(초) — 첫 시도는 즉시, 이후 점진 대기
API_BACKOFF_SEC = (0.0, 0.8, 1.6, 3.2, 6.4, 9.6)

def _retry(fn, *args, **kwargs):
    """
    gspread API 호출용 재시도 유틸.
    - 400/401/403/404: 즉시 예외 전파(비재시도)
    - 429/5xx 등: Retry-After 헤더를 우선, 없으면 지수 백오프 + 지터
    """
    last = None
    for base in API_BACKOFF_SEC:
        try:
            return fn(*args, **kwargs)
        except APIError as e:
            resp = getattr(e, "response", None)
            status = getattr(resp, "status_code", None)
            # 클라이언트/권한/존재 오류는 재시도 무의미
            if status in (400, 401, 403, 404):
                raise
            # 서버/쿼터 오류: 대기 후 재시도
            try:
                ra = resp.headers.get("Retry-After") if resp else None
            except Exception:
                ra = None
            wait = float(ra) if ra else (base + random.uniform(0, 0.5))
            time.sleep(max(0.2, wait))
            last = e
        except Exception as e:
            # 일시 네트워크 오류 등
            last = e
            time.sleep(base + random.uniform(0, 0.5))
    # 모든 시도 실패
    raise last if last else RuntimeError("Retry failed without exception context")

@st.cache_resource(show_spinner=False)
def get_client():
    """gspread 클라이언트(리소스 캐시)."""
    svc = dict(st.secrets["gcp_service_account"])
    svc["private_key"] = _normalize_private_key(svc.get("private_key", ""))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(svc, scopes=scopes)
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def get_book():
    """스프레드시트 핸들(리소스 캐시)."""
    return get_client().open_by_key(st.secrets["sheets"]["HR_SHEET_ID"])

EMP_SHEET = st.secrets.get("sheets", {}).get("EMP_SHEET", "직원")

# 워크시트/헤더/값 캐시(모듈 레벨)
_WS_CACHE: dict[str, Tuple[float, Any]] = {}
_HDR_CACHE: dict[str, Tuple[float, list[str], dict]] = {}
_VAL_CACHE: dict[str, Tuple[float, list]] = {}

# 429 대비 '마지막 정상값' 폴백 저장소 (권장)
_VAL_LASTGOOD: dict[str, list] = {}
_HDR_LASTGOOD: dict[str, list[str]] = {}

_WS_TTL, _HDR_TTL = 120, 120
_VAL_TTL = 200  # 조회 체감 개선(운영 중 수동 동기화 버튼과 병행)

def _ws_values(ws, key: str | None = None):
    """
    get_all_values 결과를 TTL 캐시(+세션 스코프)로 관리.
    - 캐시 키에 cache_rev를 섞어 한 세션의 동기화가 타 세션 캐시에 영향 주지 않음.
    - 429(할당량 초과) 시 마지막 정상값으로 폴백하여 UX/쿼터 보호.
    """
    global _VAL_CACHE, _VAL_TTL

    base_key = key or getattr(ws, "title", "") or "ws_values"
    bust = int(st.session_state.get("cache_rev", 0))  # 세션 전용 캐시버스터
    cache_key = f"{base_key}#r{bust}"

    # 마지막 정상값 저장소(없으면 생성)
    if "_VAL_LASTGOOD" not in globals():
        globals()["_VAL_LASTGOOD"] = {}
    _VAL_LASTGOOD = globals()["_VAL_LASTGOOD"]

    now = time.time()

    # 세션 스코프 캐시 히트
    hit = _VAL_CACHE.get(cache_key)
    if hit and (now - hit[0] < _VAL_TTL):
        return hit[1]

    # 읽기
    try:
        vals = _retry(ws.get_all_values)
    except APIError as e:
        # 읽기 할당량 초과 → 마지막 정상값 폴백
        if _is_quota_429(e):
            try:
                st.info("⏳구글시트 읽기 할당량(1분) 초과. 잠시 후 다시 시도해 주세요.", icon="⏳")
            except Exception:
                pass
            # 1) 같은 세션 키의 직전 값
            if hit:
                return hit[1]
            # 2) 베이스 키의 마지막 정상값(세션 무관)
            prev = _VAL_LASTGOOD.get(base_key)
            return prev if prev is not None else []
        # 그 외 오류는 상위에서 처리
        raise

    # 캐시/라스트굿 갱신
    _VAL_CACHE[cache_key] = (now, vals)
    _VAL_LASTGOOD[base_key] = vals

    # 같은 base_key의 오래된 리비전 엔트리 청소(메모리 보호)
    try:
        for k, (ts, _) in list(_VAL_CACHE.items()):
            if k.startswith(base_key + "#") and (now - ts >= _VAL_TTL):
                _VAL_CACHE.pop(k, None)
    except Exception:
        pass

    return vals

def _ws(title: str):
    """제목으로 워크시트 가져오기(핸들 TTL 캐시)."""
    now = time.time()
    hit = _WS_CACHE.get(title)
    if hit and (now - hit[0] < _WS_TTL):
        return hit[1]
    ws = _retry(get_book().worksheet, title)
    _WS_CACHE[title] = (now, ws)
    return ws

def _hdr(ws, key: str) -> Tuple[list[str], dict]:
    """
    헤더 1행과 이름→열번호 매핑(hmap) 반환(캐시, 세션 스코프).
    - 캐시 키에 cache_rev를 섞어 한 세션의 동기화가 타 세션에 영향 주지 않음.
    - 가능하면 _ws_values(ws, key)의 첫 행을 헤더로 재사용(추가 read 호출 회피).
    - 429(할당량 초과) 시 마지막 정상 헤더로 폴백.
    """
    global _HDR_CACHE, _HDR_TTL

    now = time.time()
    bust = int(st.session_state.get("cache_rev", 0))
    cache_key = f"{key}#r{bust}"

    # 세션 스코프 캐시 히트
    hit = _HDR_CACHE.get(cache_key)
    if hit and (now - hit[0] < _HDR_TTL):
        return hit[1], hit[2]

    # 마지막 정상 헤더 저장소
    if "_HDR_LASTGOOD" not in globals():
        globals()["_HDR_LASTGOOD"] = {}
    _HDR_LASTGOOD = globals()["_HDR_LASTGOOD"]

    header: list[str] = []

    # 1) values 캐시에서 헤더 재사용 시도
    try:
        vals = _ws_values(ws, key)
        if vals:
            header = [str(x).strip() for x in (vals[0] or [])]
    except Exception:
        header = []

    # 2) 실패/공백 시에만 row_values(1) 호출
    need_fallback = (not header) or all(str(x).strip() == "" for x in header)
    if need_fallback:
        try:
            header = _retry(ws.row_values, 1) or []
        except APIError as e:
            # 429 → 마지막 정상 헤더 폴백
            if _is_quota_429(e):
                try:
                    st.info("⏳구글시트 읽기 할당량(1분) 초과. 잠시 후 다시 시도해 주세요.", icon="⏳")
                except Exception:
                    pass
                header = _HDR_LASTGOOD.get(key, [])
            else:
                raise
        except Exception:
            header = _HDR_LASTGOOD.get(key, [])

    # 매핑 구성 및 캐시 반영
    hmap = {n: i + 1 for i, n in enumerate(header)}
    _HDR_CACHE[cache_key] = (now, header, hmap)
    _HDR_LASTGOOD[key] = header

    # 같은 base key의 오래된 리비전 엔트리 정리(메모리 보호)
    try:
        for k, (ts, _, _) in list(_HDR_CACHE.items()):
            if k.startswith(key + "#") and (now - ts >= _HDR_TTL):
                _HDR_CACHE.pop(k, None)
    except Exception:
        pass

    return header, hmap

def _ws_get_all_records(ws):
    """
    get_all_values 캐시를 활용해 dict 레코드 리스트 생성.
    (속도 우선: 필요 최소 변환만 수행)
    """
    try:
        title = getattr(ws, "title", "") or ""
        vals = _ws_values(ws, title)
        if not vals:
            return []
        header = [str(x).strip() for x in vals[0]]
        hlen = len(header)
        out = []
        append = out.append  # 로컬 바인딩(미세 가속)
        for row in vals[1:]:
            rec = {}
            # enumerate가 zip보다 안전(행 길이 짧을 때 공백 채우기)
            for j, h in enumerate(header):
                rec[h] = row[j] if j < len(row) else ""
            append(rec)
        return out
    except Exception:
        # gspread의 내부 변환 사용(폴백)
        try:
            return _retry(ws.get_all_records, numericise_ignore=["all"])
        except TypeError:
            return _retry(ws.get_all_records)

# ═════════════════════════════════════════════════════════════════════════════
# Sheet Readers (TTL↑)
# ═════════════════════════════════════════════════════════════════════════════
LAST_GOOD: dict[str, pd.DataFrame] = {}

@st.cache_data(ttl=600, show_spinner=False)
def read_sheet_df(sheet_name: str) -> pd.DataFrame:
    """
    구글시트 → DataFrame
    - '사번'은 문자열 키로 고정
    - '재직여부': 공백(빈칸)은 True, 그 외는 _to_bool 규칙과 동일 처리
    """
    try:
        ws = _ws(sheet_name)
        df = pd.DataFrame(_ws_get_all_records(ws))
        if df.empty:
            return df

        # 사번: 문자열 키로 고정
        if "사번" in df.columns:
            df["사번"] = df["사번"].astype(str)

        # 재직여부: 공백 True, 그 외는 토큰 집합으로 벡터화 판정
        if "재직여부" in df.columns:
            s = df["재직여부"].astype(str).str.strip()
            # _to_bool(x) == x.lower() in {"true","1","y","yes","t"}
            truthy = {"true", "1", "y", "yes", "t"}
            df["재직여부"] = (s.eq("") | s.str.lower().isin(truthy)).astype(bool)

        # 최신 정상값 캐시
        LAST_GOOD[sheet_name] = df.copy()
        return df

    except APIError as e:
        # 429(쿼터) → 캐시 폴백 우선
        if _is_quota_429(e):
            if sheet_name in LAST_GOOD:
                try:
                    st.info(f"할당량 초과로 캐시 데이터를 표시합니다: {sheet_name}", icon="⏳")
                except Exception:
                    pass
                return LAST_GOOD[sheet_name]
            # 캐시가 없으면 안내 후 빈 DF
            try:
                st.warning(
                    "구글시트 읽기 할당량(1분) 초과. 잠시 후 좌측 '동기화'를 눌러 다시 시도해 주세요.",
                    icon="⏳",
                )
            except Exception:
                pass
            return pd.DataFrame()

        # 기타 네트워크/서버 이슈 → 캐시 폴백
        if sheet_name in LAST_GOOD:
            try:
                st.info(f"네트워크 혼잡으로 캐시 데이터를 표시합니다: {sheet_name}")
            except Exception:
                pass
            return LAST_GOOD[sheet_name]
        raise


@st.cache_data(ttl=600, show_spinner=False)
def read_emp_df() -> pd.DataFrame:
    """
    직원 시트 표준화:
    - 필수 컬럼 보강: 사번/이름/PIN_hash/재직여부/적용여부
    - dtype 정리: 사번=str, 불리언 컬럼은 공백 True + 허용 토큰 벡터화
    """
    df = read_sheet_df(EMP_SHEET)

    # 필수 컬럼 보강(없으면 생성)
    required_defaults = {
        "사번": "",
        "이름": "",
        "PIN_hash": "",
        "재직여부": True,
        "적용여부": True,
    }
    for col, default in required_defaults.items():
        if col not in df.columns:
            df[col] = default

    # dtype 정리
    df["사번"] = df["사번"].astype(str)

    # 불리언 컬럼 통일 규칙: 공백 True + {"true","1","y","yes","t"} 허용
    truthy = {"true", "1", "y", "yes", "t"}
    for col in ("재직여부", "적용여부"):
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            df[col] = (s.eq("") | s.str.lower().isin(truthy)).astype(bool)

    return df

# ═════════════════════════════════════════════════════════════════════════════
# Login + Session
# ═════════════════════════════════════════════════════════════════════════════
SESSION_TTL_MIN = 30
_SESSION_KEYS_KEEP_ON_OWNER_SWITCH = {"authed", "user", "auth_expires_at", "_state_owner_sabun"}

def _session_valid() -> bool:
    """현재 세션이 유효한가? (로그인 + 만료시간 내)"""
    ss = st.session_state
    if not ss.get("authed", False):
        return False
    exp = ss.get("auth_expires_at")
    return bool(isinstance(exp, (int, float)) and time.time() < exp)

def _start_session(user: dict) -> None:
    """로그인 성공 시 세션 시작."""
    ss = st.session_state
    ss["authed"] = True
    ss["user"] = user
    ss["auth_expires_at"] = time.time() + SESSION_TTL_MIN * 60
    ss["_state_owner_sabun"] = str(user.get("사번", ""))

def _ensure_state_owner() -> None:
    """
    세션 소유자(사번)가 바뀌면, 로그인/만료 키만 남기고 나머지 상태는 정리.
    - 다른 사람으로 로그인 전환 시, 이전 사용자의 편집/캐시 상태 잔존 방지.
    """
    try:
        ss = st.session_state
        cur = str((ss.get("user") or {}).get("사번", "") or "")
        owner = str(ss.get("_state_owner_sabun", "") or "")
        if owner and owner != cur:
            to_del = [k for k in list(ss.keys()) if k not in _SESSION_KEYS_KEEP_ON_OWNER_SWITCH]
            for k in to_del:
                ss.pop(k, None)
            ss["_state_owner_sabun"] = cur
    except Exception:
        pass

def logout() -> None:
    """로그아웃(세션/캐시 정리 후 리런)."""
    try:
        for k in list(st.session_state.keys()):
            try:
                del st.session_state[k]
            except Exception:
                pass
        try:
            st.cache_data.clear()
        except Exception:
            pass
    finally:
        st.rerun()

# ── Enter Key Binder: 사번→PIN, PIN→로그인 ─────────────────────────────────
import streamlit.components.v1 as components
def _inject_login_keybinder() -> None:
    components.html(
        """
        <script>
        (function(){
          const doc = window.parent.document;
          function byLabelStartsWith(txt){
            const labels = Array.from(doc.querySelectorAll('label'));
            const lab = labels.find(l => (l.innerText||"").trim().startsWith(txt));
            if(!lab) return null;
            const root = lab.closest('div[data-testid="stTextInput"]') || lab.parentElement;
            return root ? root.querySelector('input') : null;
          }
          function findLoginBtn(){
            const btns = Array.from(doc.querySelectorAll('button'));
            return btns.find(b => (b.textContent||"").trim() === '로그인');
          }
          function commit(el){
            if(!el) return;
            el.dispatchEvent(new Event('input',{bubbles:true}));
            el.dispatchEvent(new Event('change',{bubbles:true}));
            el.blur();
          }
          function bind(){
            const sab = byLabelStartsWith('사번');
            const pin = byLabelStartsWith('PIN');
            if(!sab || !pin) return false;

            if(!sab._bound){
              sab._bound = true;
              sab.addEventListener('keydown', function(e){
                if(e.key==='Enter'){ e.preventDefault(); commit(sab); setTimeout(()=>{ try{ pin.focus(); pin.select(); }catch(_){}} ,0); }
              });
            }
            if(!pin._bound){
              pin._bound = true;
              pin.addEventListener('keydown', function(e){
                if(e.key==='Enter'){ e.preventDefault(); commit(pin); setTimeout(()=>{ try{ (findLoginBtn()||{}).click(); }catch(_){}} ,50); }
              });
            }
            return true;
          }
          bind();
          const mo = new MutationObserver(bind);
          mo.observe(doc.body, { childList:true, subtree:true });
          setTimeout(()=>{ try{ mo.disconnect(); }catch(e){} }, 8000);
        })();
        </script>
        """,
        height=0, width=0
    )

def show_login(emp_df: pd.DataFrame) -> None:
    """로그인 폼 + 검증."""
    st.markdown("### 로그인")
    sabun_input = st.text_input("사번", key="login_sabun")
    pin_input   = st.text_input("PIN (숫자)", type="password", key="login_pin")
    _inject_login_keybinder()

    if st.button("로그인", type="primary"):
        sabun = str(sabun_input).strip()
        pin   = str(pin_input).strip()
        if not sabun or not pin:
            st.error("사번과 PIN을 입력하세요."); st.stop()

        # 사번 매칭: 정수 비교(빠름) → 실패 시 문자열 비교
        row = None
        try:
            if sabun.isdigit() and pd.api.types.is_integer_dtype(emp_df["사번"]):
                m = emp_df["사번"].eq(int(sabun))
                if m.any(): row = emp_df.loc[m]
        except Exception:
            row = None
        if row is None or row.empty:
            m = emp_df["사번"].astype(str).eq(sabun)
            if m.any(): row = emp_df.loc[m]

        if row is None or row.empty:
            st.error("사번을 찾을 수 없습니다."); st.stop()

        r = row.iloc[0]
        if not _to_bool(r.get("재직여부", True)):
            st.error("재직 상태가 아닙니다."); st.stop()

        stored = str(r.get("PIN_hash", "")).strip().lower()
        entered_plain  = _sha256_hex(pin).lower()
        entered_salted = _pin_hash(pin, sabun).lower()

        if stored not in (entered_plain, entered_salted):
            st.error("PIN이 올바르지 않습니다."); st.stop()

        _start_session({"사번": sabun, "이름": str(r.get("이름", ""))})
        st.success("환영합니다!")
        st.rerun()

def require_login(emp_df: pd.DataFrame) -> None:
    """세션이 없으면 로그인 화면으로 전환."""
    if not _session_valid():
        for k in ("authed", "user", "auth_expires_at", "_state_owner_sabun"):
            st.session_state.pop(k, None)
        show_login(emp_df)
        st.stop()
    else:
        _ensure_state_owner()

# ═════════════════════════════════════════════════════════════════════════════
# ACL (권한) + Staff Filters (TTL↑)
# ═════════════════════════════════════════════════════════════════════════════
AUTH_SHEET = "권한"

EVAL_ITEMS_SHEET = st.secrets.get("sheets", {}).get("EVAL_ITEMS_SHEET", "평가_항목")
EVAL_ITEM_HEADERS = ["항목ID", "항목", "내용", "순서", "활성", "비고", "설명", "유형", "구분"]
EVAL_RESP_SHEET_PREFIX = "인사평가_"
EVAL_BASE_HEADERS = ["연도", "평가유형", "평가대상사번", "평가대상이름", "평가자사번", "평가자이름", "총점", "상태", "제출시각", "잠금"]

AUTH_HEADERS = ["사번", "이름", "역할", "범위유형", "부서1", "부서2", "대상사번", "활성", "비고"]

@st.cache_data(ttl=300, show_spinner=False)
def read_auth_df() -> pd.DataFrame:
    """권한 시트 로드 + 표준화(누락 컬럼 보강, dtype 정리)."""
    try:
        ws = _ws(AUTH_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=AUTH_HEADERS)

    if df.empty:
        return pd.DataFrame(columns=AUTH_HEADERS)

    # 누락 컬럼 보강
    for c in AUTH_HEADERS:
        if c not in df.columns:
            df[c] = ""

    # dtype 정리
    df["사번"] = df["사번"].astype(str)

    # 활성: _to_bool과 동일 의미(빈칸 False)로 벡터화 처리
    truthy = {"true", "1", "y", "yes", "t"}
    s = df["활성"].astype(str).str.strip().str.lower()
    df["활성"] = s.isin(truthy)

    return df

def is_admin(sabun: str) -> bool:
    """사번이 admin 권한(+활성)인지 여부."""
    try:
        sab = str(sabun)
        df = read_auth_df()
        if df.empty:
            return False
        q = (df["사번"].eq(sab)) & (df["역할"].astype(str).str.lower().eq("admin")) & (df["활성"])
        return bool(q.any())
    except Exception:
        return False

def get_allowed_sabuns(emp_df: pd.DataFrame, sabun: str, include_self: bool = True) -> set[str]:
    """
    현재 사용자(sabun)가 접근 가능한 사번 집합.
    - include_self=True면 자신의 사번 항상 포함
    - 범위유형 공란이 1개라도 있으면 전체 허용(기존 로직 유지)
    - 범위유형=부서 → 부서1/부서2 기준 필터
    - 범위유형=개별 → 대상사번 목록 추가
    """
    sab = str(sabun)
    allowed: set[str] = {sab} if include_self else set()

    df = read_auth_df()
    if df.empty:
        return allowed

    mine = df[(df["사번"].eq(sab)) & (df["활성"])]
    if mine.empty:
        return allowed

    # 공란 범위유형 있으면 전체 허용
    if (mine["범위유형"].astype(str).str.strip() == "").any():
        return set(emp_df["사번"].astype(str).tolist())

    # 부서/개별 규칙 적용
    has_d1 = "부서1" in emp_df.columns
    has_d2 = "부서2" in emp_df.columns

    for _, r in mine.iterrows():
        t = str(r.get("범위유형", "")).strip()
        if t == "부서":
            # 불필요한 df.copy() 제거 → 마스크로 필터
            d1 = str(r.get("부서1", "")).strip()
            d2 = str(r.get("부서2", "")).strip()

            m = pd.Series(True, index=emp_df.index)
            if has_d1 and d1:
                m &= emp_df["부서1"].astype(str).eq(d1)
            if has_d2 and d2:
                m &= emp_df["부서2"].astype(str).eq(d2)

            if m.any():
                allowed.update(emp_df.loc[m, "사번"].astype(str).tolist())

        elif t == "개별":
            raw = str(r.get("대상사번", "")).strip()
            if raw:
                # 공백/쉼표 구분 → 공란 제거 후 추가
                for p in re.split(r"[,\s]+", raw):
                    if p:
                        allowed.add(p)

    return allowed

# ═════════════════════════════════════════════════════════════════════════════
# Global Target Sync
# ═════════════════════════════════════════════════════════════════════════════
def set_global_target(sabun: str, name: str = "") -> None:
    st.session_state["glob_target_sabun"] = str(sabun).strip()
    st.session_state["glob_target_name"] = str(name).strip()

def get_global_target() -> Tuple[str, str]:
    return (
        str(st.session_state.get("glob_target_sabun", "") or ""),
        str(st.session_state.get("glob_target_name", "") or ""),
    )

# ═════════════════════════════════════════════════════════════════════════════
# Left: 직원선택 (Enter 동기화)
# ═════════════════════════════════════════════════════════════════════════════
def render_staff_picker_left(emp_df: pd.DataFrame):
    """
    좌측 패널: 직원 검색/선택 + 대시보드(요약 컬럼) 토글 표시.
    - 검색어: 사번/이름 부분일치(Enter로 적용)
    - 선택 시: 글로벌 타깃 동기화
    - '필터 초기화' 버튼: 검색/선택/탭별 타깃 상태 초기화
    """
    ss = st.session_state

    # ── 필터 초기화 플래그(위젯 생성 전에 처리) ───────────────────────────────
    if ss.get("_left_reset", False):
        u0 = ss.get("user", {}) or {}
        me0 = str(u0.get("사번", ""))
        nm0 = str(u0.get("이름", ""))

        # 검색/대상선택 리셋
        ss["pick_q"] = ""
        ss["left_pick"] = "(선택)"
        ss["left_preselect_sabun"] = ""

        # 탭별 대상자도 초기화
        try:
            set_global_target("", "")
        except Exception:
            pass
        ss["eval2_target_sabun"] = ""
        ss["eval2_target_name"]  = ""
        ss["jd2_target_sabun"]   = ""
        ss["jd2_target_name"]    = ""
        ss["cmpS_target_sabun"]  = ""
        ss["cmpS_target_name"]   = ""
        ss["_left_reset"] = False

    u = ss.get("user", {}) or {}
    me = str(u.get("사번", "")).strip()

    # 적용여부=True만 좌측 목록에 노출
    df = emp_df
    if "적용여부" in df.columns:
        df = df[df["적용여부"] == True]

    # 권한 범위 필터(관리자라도 범위유형이 부서/개별이면 그 범위만)
    allowed = get_allowed_sabuns(emp_df, me, include_self=True)
    df = df[df["사번"].astype(str).isin(allowed)]

    # ── 검색 폼 ──────────────────────────────────────────────────────────────
    with st.form("left_search_form", clear_on_submit=False):
        q = st.text_input("검색(사번/이름)", key="pick_q", placeholder="사번 또는 이름")
        submitted = st.form_submit_button("검색 적용(Enter)")

    view = df
    if q.strip():
        k = q.strip()
        # regex=False로 안전한 부분일치 검색(속도/안정성)
        cond_id = df["사번"].astype(str).str.contains(k, case=False, na=False, regex=False) if "사번" in df.columns else False
        cond_nm = df["이름"].astype(str).str.contains(k, case=False, na=False, regex=False) if "이름" in df.columns else False
        view = df[cond_id | cond_nm]

    # 정렬 및 옵션 구성
    if "사번" in view.columns:
        view = view.sort_values("사번")
    sabuns = view["사번"].astype(str).tolist()
    names = (view["이름"].astype(str).tolist()) if "이름" in view.columns else [""] * len(sabuns)
    opts = [f"{s} - {n}" for s, n in zip(sabuns, names)]

    # Enter로 정확히 일치하면 해당 대상 선선택
    pre_sel_sab = ss.get("left_preselect_sabun", "")
    if submitted:
        exact_idx = -1
        if q.strip():
            qs = q.strip()
            for i, (s, n) in enumerate(zip(sabuns, names)):
                if qs == s or qs == n:
                    exact_idx = i
                    break
        target_idx = exact_idx if exact_idx >= 0 else (0 if sabuns else -1)
        if target_idx >= 0:
            pre_sel_sab = sabuns[target_idx]
            ss["left_preselect_sabun"] = pre_sel_sab

    idx0 = 0
    if pre_sel_sab:
        try:
            idx0 = 1 + sabuns.index(pre_sel_sab)
        except ValueError:
            idx0 = 0

    picked = st.selectbox("**대상 선택**", ["(선택)"] + opts, index=idx0, key="left_pick")

    # 필터 초기화 버튼
    if st.button("필터 초기화", use_container_width=True):
        ss["_left_reset"] = True
        st.rerun()

    # 선택 시 글로벌 타깃 동기화 및 표 1명 필터
    if picked and picked != "(선택)":
        sab = picked.split(" - ", 1)[0].strip()
        name = picked.split(" - ", 1)[1].strip() if " - " in picked else ""
        set_global_target(sab, name)
        ss["eval2_target_sabun"] = sab
        ss["eval2_target_name"] = name
        ss["jd2_target_sabun"] = sab
        ss["jd2_target_name"] = name
        ss["cmpS_target_sabun"] = sab
        ss["cmpS_target_name"] = name

        if "사번" in view.columns:
            view = view[view["사번"].astype(str) == sab]

    cols = [c for c in ["사번", "이름", "부서1", "부서2", "직급"] if c in view.columns]
    st.caption(f"총 {len(view)}명")

    # ── 관리자/부서장: 대시보드 요약 열 표시 ──────────────────────────────────
    show_dashboard_cols = st.checkbox(
        "대시보드 보기(요약 컬럼 표시)",
        value=False,
        help="끄면 기본 직원표만 빠르게 표시됩니다.",
    )
    try:
        am_admin_or_mgr = (is_admin(me) or len(get_allowed_sabuns(emp_df, me, include_self=False)) > 0)
    except Exception:
        am_admin_or_mgr = False

    if am_admin_or_mgr and not view.empty and show_dashboard_cols:
        # 연도 선택(기본=올해)
        this_year = current_year()
        dash_year = st.number_input(
            "연도(현황판)", min_value=2000, max_value=2100, value=int(this_year), step=1, key="left_dash_year"
        )

        eval_map = get_eval_summary_map_cached(int(dash_year), st.session_state.get("eval_rev", 0))
        comp_map = get_comp_summary_map_cached(int(dash_year), st.session_state.get("comp_rev", 0))
        appr_map = get_jd_approval_map_cached(int(dash_year), st.session_state.get("appr_rev", 0))

        # view에 요약 컬럼 병합
        ext_rows = []
        for _, r in view.iterrows():
            sab = str(r.get("사번", "")).strip()

            # 인사평가 점수
            s_self = eval_map.get((sab, "자기"), ("", ""))[0]
            s_mgr  = eval_map.get((sab, "1차"), ("", ""))[0]
            s_adm  = eval_map.get((sab, "2차"), ("", ""))[0]

            # JD 작성/승인
            latest = _jd_latest_for(sab, int(dash_year))
            jd_write = "완료" if latest else ""
            jd_appr  = ""
            if latest:
                try:
                    ver = int(str(latest.get("버전", 0)).strip() or "0")
                except Exception:
                    ver = 0
                st_ap = appr_map.get((sab, ver), ("", ""))[0] if ver else ""
                jd_appr = st_ap or ""

            # 직무능력평가
            main, extra, qual = "", "", ""
            if sab in comp_map:
                main, extra, qual, _ = comp_map[sab]

            ext_rows.append({
                "사번": sab,
                "인사평가(자기)": s_self, "인사평가(1차)": s_mgr, "인사평가(2차)": s_adm,
                "직무기술서(작성)": jd_write, "직무기술서(승인)": jd_appr,
                "직무능력평가(주업무)": main, "직무능력평가(기타업무)": extra, "직무능력평가(자격유지)": qual
            })

        add_df = pd.DataFrame(ext_rows)
        add_df["사번"] = add_df["사번"].astype(str)
        view2 = view.copy()
        view2["사번"] = view2["사번"].astype(str)
        view2 = view2.merge(add_df, on="사번", how="left")

        ext_cols = cols + [
            "인사평가(자기)", "인사평가(1차)", "인사평가(2차)",
            "직무기술서(작성)", "직무기술서(승인)",
            "직무능력평가(주업무)", "직무능력평가(기타업무)", "직무능력평가(자격유지)"
        ]
        st.dataframe(
            view2[ext_cols],
            use_container_width=True,
            height=420,
            hide_index=True,
            column_config={
                "인사평가(자기)": st.column_config.TextColumn("자기"),
                "인사평가(1차)": st.column_config.TextColumn("1차"),
                "인사평가(2차)": st.column_config.TextColumn("2차"),
                "직무기술서(작성)": st.column_config.TextColumn("JD작성"),
                "직무기술서(승인)": st.column_config.TextColumn("JD승인"),
                "직무능력평가(주업무)": st.column_config.TextColumn("주업무"),
                "직무능력평가(기타업무)": st.column_config.TextColumn("기타업무"),
                "직무능력평가(자격유지)": st.column_config.TextColumn("자격유지"),
            }
        )
    else:
        st.dataframe(
            view[cols],
            use_container_width=True,
            height=(360 if not show_dashboard_cols else 420),
            hide_index=True
        )


# ═════════════════════════════════════════════════════════════════════════════
# Eval Sheet Helpers
# ═════════════════════════════════════════════════════════════════════════════
def _eval_sheet_name(year: int | str) -> str:
    return f"{EVAL_RESP_SHEET_PREFIX}{int(year)}"


def ensure_eval_items_sheet() -> None:
    """평가_항목 시트 존재/헤더 보장."""
    wb = get_book()
    try:
        ws = wb.worksheet(EVAL_ITEMS_SHEET)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=EVAL_ITEMS_SHEET, rows=200, cols=10)
        _retry(ws.update, "A1", [EVAL_ITEM_HEADERS])
        return

    try:
        header = _retry(ws.row_values, 1) or []
    except Exception as e:
        if _is_quota_429(e):
            try:
                st.warning('구글시트 읽기 할당량(1분) 초과. 잠시 후 좌측 "동기화"를 눌러 다시 시도해 주세요.', icon='⏳')
            except Exception:
                pass
            return
        raise

    need = [h for h in EVAL_ITEM_HEADERS if h not in header]
    if need:
        try:
            _retry(ws.update, "1:1", [header + need])
        except Exception as e:
            if _is_quota_429(e):
                try:
                    st.warning('구글시트 쓰기 할당량(1분) 초과. 잠시 후 좌측 "동기화" 후 다시 시도해 주세요.', icon='⏳')
                except Exception:
                    pass
                return
            raise


@st.cache_data(ttl=300, show_spinner=False)
def read_eval_items_df(only_active: bool = True) -> pd.DataFrame:
    """평가_항목 → DF(순서/활성 정리)."""
    ensure_eval_items_sheet()
    ws = _ws(EVAL_ITEMS_SHEET)
    try:
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        if _is_quota_429(e):
            try:
                st.warning('구글시트 읽기 할당량(1분) 초과. 잠시 후 "동기화"를 눌러 다시 시도해 주세요.', icon="⏳")
            except Exception:
                pass
            return pd.DataFrame(columns=EVAL_ITEM_HEADERS)
        raise

    if df.empty:
        return pd.DataFrame(columns=EVAL_ITEM_HEADERS)

    # 순서 정리(숫자 변환 실패→0)
    if "순서" in df.columns:
        df["순서"] = pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)

    # 활성 플래그
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool)

    # 정렬/필터
    sort_cols = [c for c in ["순서", "항목"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    if only_active and "활성" in df.columns:
        df = df[df["활성"] == True]
    return df


def _ensure_eval_resp_sheet(year: int, item_ids: list[str]):
    """연도별 응답 시트 존재/헤더 보장."""
    name = _eval_sheet_name(year)
    wb = get_book()
    try:
        ws = _ws(name)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=name, rows=5000, cols=max(50, len(item_ids) + 16))
        _WS_CACHE[name] = (time.time(), ws)

    need = list(EVAL_BASE_HEADERS) + [f"점수_{iid}" for iid in item_ids]
    header, _ = _hdr(ws, name)

    if not header:
        _retry(ws.update, "1:1", [need])
        _HDR_CACHE[name] = (time.time(), need, {n: i + 1 for i, n in enumerate(need)})
    else:
        miss = [h for h in need if h not in header]
        if miss:
            new = header + miss
            _retry(ws.update, "1:1", [new])
            _HDR_CACHE[name] = (time.time(), new, {n: i + 1 for i, n in enumerate(new)})
    return ws


def _emp_name_by_sabun(emp_df: pd.DataFrame, sabun: str) -> str:
    row = emp_df.loc[emp_df["사번"].astype(str) == str(sabun)]
    return "" if row.empty else str(row.iloc[0].get("이름", ""))


def upsert_eval_response(
    emp_df: pd.DataFrame,
    year: int,
    eval_type: str,
    target_sabun: str,
    evaluator_sabun: str,
    scores: dict[str, int],
    status: str = "제출",
) -> dict:
    """인사평가 응답 upsert(존재 시 update, 없으면 append)."""
    items = read_eval_items_df(True)
    item_ids = [str(x) for x in items["항목ID"].tolist()]
    ws = _ensure_eval_resp_sheet(year, item_ids)
    header = _retry(ws.row_values, 1) or []
    hmap = {n: i + 1 for i, n in enumerate(header)}

    # 점수 정규화(1~5)
    def c5(v):
        try:
            v = int(v)
        except Exception:
            v = 3
        return min(5, max(1, v))

    scores_list = [c5(scores.get(i, 3)) for i in item_ids]
    total = round(sum(scores_list) * (100.0 / max(1, len(item_ids) * 5)), 1)
    tname = _emp_name_by_sabun(emp_df, target_sabun)
    ename = _emp_name_by_sabun(emp_df, evaluator_sabun)
    now = kst_now_str()

    values = _ws_values(ws)
    cY = hmap.get("연도"); cT = hmap.get("평가유형"); cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")

    # 동일 키 검색(연/유형/대상/평가자)
    row_idx = 0
    for i in range(2, len(values) + 1):
        r = values[i - 1]
        try:
            if (
                str(r[cY - 1]).strip() == str(year) and
                str(r[cT - 1]).strip() == eval_type and
                str(r[cTS - 1]).strip() == str(target_sabun) and
                str(r[cES - 1]).strip() == str(evaluator_sabun)
            ):
                row_idx = i
                break
        except Exception:
            pass

    if row_idx == 0:
        # INSERT
        buf = [""] * len(header)
        def put(k, v):
            c = hmap.get(k)
            if c:
                buf[c - 1] = v
        put("연도", int(year)); put("평가유형", eval_type)
        put("평가대상사번", str(target_sabun)); put("평가대상이름", tname)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", ename)
        put("총점", total); put("상태", status); put("제출시각", now)
        for iid, sc in zip(item_ids, scores_list):
            c = hmap.get(f"점수_{iid}")
            if c:
                buf[c - 1] = sc
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        st.cache_data.clear()
        return {"action": "insert", "total": total}
    else:
        # UPDATE
        payload = {"총점": total, "상태": status, "제출시각": now, "평가대상이름": tname, "평가자이름": ename}
        for iid, sc in zip(item_ids, scores_list):
            payload[f"점수_{iid}"] = sc

        def _batch_row(ws, idx, hmap, kv):
            upd = []
            for k, v in kv.items():
                c = hmap.get(k)
                if c:
                    a1 = gspread.utils.rowcol_to_a1(idx, c)
                    upd.append({"range": a1, "values": [[v]]})
            if upd:
                _retry(ws.batch_update, upd)
        _batch_row(ws, row_idx, hmap, payload)
        st.cache_data.clear()
        return {"action": "update", "total": total}


@st.cache_data(ttl=300, show_spinner=False)
def read_my_eval_rows(year: int, sabun: str) -> pd.DataFrame:
    """특정 평가자가 입력한 응답행 조회(최근순 정렬)."""
    name = _eval_sheet_name(year)
    try:
        ws = _ws(name)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=EVAL_BASE_HEADERS)

    if df.empty:
        return df

    if "평가자사번" in df.columns:
        df = df[df["평가자사번"].astype(str) == str(sabun)]

    sort_cols = [c for c in ["평가유형", "평가대상사번", "제출시각"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True, True, False]).reset_index(drop=True)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# Eval Tab (자동 라우팅)
# ═════════════════════════════════════════════════════════════════════════════
def tab_eval(emp_df: pd.DataFrame):
    """
    인사평가 탭
    - 역할: employee / manager / admin
    - 유형 자동결정:
        employee: 본인=자기
        manager : 본인=자기, 부서원=1차(부서원의 자기 '제출' 후 입력 가능)
        admin   : 대상이 manager면 1차(그 manager의 자기 '제출' 후), 그 외(직원)는 2차(1차 '제출' 후)
    - 직원 자기평가: 제출 후 수정 불가(자동 잠금)
    """
    # ── 기본값/데이터 로드 ────────────────────────────────────────────────────
    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="eval2_year")

    u = st.session_state["user"]
    me_sabun = str(u["사번"]); me_name = str(u["이름"])

    items = read_eval_items_df(True)
    if items.empty:
        st.warning("활성화된 평가 항목이 없습니다.", icon="⚠️")
        return
    items_sorted = items.sort_values(["순서", "항목"]).reset_index(drop=True)
    item_ids = [str(x) for x in items_sorted["항목ID"].tolist()]

    # ── 역할 판정 ────────────────────────────────────────────────────────────
    def is_manager_role(_sabun: str) -> bool:
        # 본인 제외 부하가 1명이라도 있으면 manager (admin 제외)
        return (not is_admin(_sabun)) and len(get_allowed_sabuns(emp_df, _sabun, include_self=False)) > 0

    def role_of(_sabun: str) -> str:
        if is_admin(_sabun): return "admin"
        if is_manager_role(_sabun): return "manager"
        return "employee"

    my_role = role_of(me_sabun)

    # ── 대상 후보 목록 ──────────────────────────────────────────────────────
    def list_targets_for(me_role: str) -> pd.DataFrame:
        base = emp_df.copy(); base["사번"] = base["사번"].astype(str)
        if "재직여부" in base.columns:
            base = base[base["재직여부"] == True]
        if me_role == "employee":
            return base[base["사번"] == me_sabun]
        elif me_role == "manager":
            allowed = set(str(x) for x in get_allowed_sabuns(emp_df, me_sabun, include_self=True))
            return base[base["사번"].isin(allowed)]
        else:  # admin
            # 관리자도 범위 규칙을 따르되, 자기 자신은 제외(자기평가 없음)
            allowed = set(str(x) for x in get_allowed_sabuns(emp_df, me_sabun, include_self=True))
            return base[base["사번"].isin(allowed - {me_sabun})]

    view = list_targets_for(my_role)[["사번","이름","부서1","부서2","직급"]].copy().sort_values(["사번"]).reset_index(drop=True)

    # ── 제출 여부 / 저장값 조회 ──────────────────────────────────────────────
    def has_submitted(_year: int, _type: str, _target_sabun: str) -> bool:
        """해당 연도+유형+대상자의 '상태'가 제출/완료인지 검사(평가자 무관)."""
        try:
            ws = _ensure_eval_resp_sheet(int(_year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i + 1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cS=hmap.get("상태")
            if not all([cY, cT, cTS, cS]): return False
            for r in values[1:]:
                try:
                    if (str(r[cY-1]).strip()==str(_year)
                        and str(r[cT-1]).strip()==str(_type)
                        and str(r[cTS-1]).strip()==str(_target_sabun)):
                        if str(r[cS-1]).strip() in {"제출","완료"}: return True
                except Exception:
                    pass
        except Exception:
            pass
        return False

    def read_eval_saved_scores(year: int, eval_type: str, target_sabun: str, evaluator_sabun: str) -> Tuple[dict, dict]:
        """현 평가자 기준 저장된 점수/메타 로드."""
        try:
            ws = _ensure_eval_resp_sheet(int(year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i + 1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cES=hmap.get("평가자사번")
            row_idx = 0
            for i in range(2, len(values)+1):
                r = values[i-1]
                try:
                    if (str(r[cY-1]).strip()==str(year) and str(r[cT-1]).strip()==str(eval_type)
                        and str(r[cTS-1]).strip()==str(target_sabun) and str(r[cES-1]).strip()==str(evaluator_sabun)):
                        row_idx = i; break
                except Exception:
                    pass
            if row_idx == 0: return {}, {}
            row = values[row_idx-1]
            scores = {}
            for iid in item_ids:
                col = hmap.get(f"점수_{iid}")
                if col:
                    try:
                        v = int(str(row[col-1]).strip() or "0")
                        if v: scores[iid] = v
                    except Exception:
                        pass
            meta = {}
            for k in ["상태","잠금","제출시각","총점"]:
                c = hmap.get(k)
                if c and c-1 < len(row): meta[k] = row[c-1]
            return scores, meta
        except Exception:
            return {}, {}

    # ── 대상 선택 + 유형 자동결정 ───────────────────────────────────────────
    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("eval2_target_sabun", (glob_sab if my_role!="employee" else me_sabun))
    st.session_state.setdefault("eval2_target_name",  (glob_name if my_role!="employee" else me_name))
    st.session_state.setdefault("eval2_edit_mode",    False)

    if my_role == "employee":
        target_sabun, target_name = me_sabun, me_name
    else:
        _sabuns = view["사번"].astype(str).tolist()
        _names  = view["이름"].astype(str).tolist()
        _d2     = view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""] * len(_sabuns)
        _opts   = [f"{s} - {n} - {d2}" for s, n, d2 in zip(_sabuns, _names, _d2)]
        _target = st.session_state.get(
            "eval2_target_sabun",
            (_sabuns[_sabuns.index(me_sabun)] if (my_role=="manager" and me_sabun in _sabuns) else (_sabuns[0] if _sabuns else ""))
        )
        _idx2 = (1 + _sabuns.index(_target)) if (_target in _sabuns) else 0
        _sel = st.selectbox("대상자 선택", ["(선택)"] + _opts, index=_idx2, key="eval2_pick_editor_select")
        if _sel == "(선택)":
            st.session_state["eval2_target_sabun"] = ""
            st.session_state["eval2_target_name"]  = ""
            st.info("대상자를 선택하세요.", icon="👈")
            return
        _sel_sab = _sel.split(" - ", 1)[0] if isinstance(_sel, str) and " - " in _sel else (_target if _target else "")
        st.session_state["eval2_target_sabun"] = str(_sel_sab)
        try:
            st.session_state["eval2_target_name"] = str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["eval2_target_name"] = ""
        target_sabun = st.session_state["eval2_target_sabun"]
        target_name  = st.session_state["eval2_target_name"]

    st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    # 제출시각 배너
    try:
        _emap = get_eval_summary_map_cached(int(year), st.session_state.get('eval_rev', 0))
        def _b(stage: str) -> str:
            try:
                return (str(_emap.get((str(target_sabun), stage), ("",""))[1]).strip() or "미제출")
            except Exception:
                return "미제출"
        _banner = f"🕒 제출시각  |  [자기] {_b('자기')}  |  [1차] {_b('1차')}  |  [2차] {_b('2차')}"
        show_submit_banner(_banner)
    except Exception:
        pass

    # 평가유형 자동결정
    target_role = role_of(target_sabun)
    if my_role == "employee":
        eval_type = "자기"
    elif my_role == "manager":
        eval_type = "자기" if target_sabun == me_sabun else "1차"
    else:
        eval_type = "1차" if target_role == "manager" else "2차"

    st.info(f"평가유형: **{eval_type}** (자동 결정)", icon="ℹ️")

    # ── 선행조건/잠금 ────────────────────────────────────────────────────────
    prereq_ok, prereq_msg = True, ""
    if eval_type == "1차":
        if not has_submitted(year, "자기", target_sabun):
            prereq_ok = False; prereq_msg = "대상자의 '자기평가'가 제출되어야 1차평가를 입력할 수 있습니다."
    elif eval_type == "2차":
        if not has_submitted(year, "1차", target_sabun):
            prereq_ok = False; prereq_msg = "대상자의 '1차평가'가 제출되어야 2차평가를 입력할 수 있습니다."

    saved_scores, saved_meta = read_eval_saved_scores(int(year), eval_type, target_sabun, me_sabun)
    is_locked = (str(saved_meta.get("잠금","")).upper()=="Y") or (str(saved_meta.get("상태","")).strip() in {"제출","완료"})
    # 직원 자기평가: 제출되어 있으면 항상 잠금
    if my_role=="employee" and eval_type=="자기" and has_submitted(year,"자기",me_sabun):
        is_locked = True

    if is_locked:
        st.info("이 응답은 잠겨 있습니다.", icon="🔒")
    if not prereq_ok:
        st.warning(prereq_msg, icon="🧩")

    # ── 보기/수정 모드 ───────────────────────────────────────────────────────
    if st.button(("수정모드로 전환" if not st.session_state["eval2_edit_mode"] else "보기모드로 전환"),
                 use_container_width=True, key="eval2_toggle"):
        st.session_state["eval2_edit_mode"] = not st.session_state["eval2_edit_mode"]
        st.rerun()

    # '실제' 편집 가능 여부는 선행조건/잠금도 반영
    requested_edit = bool(st.session_state["eval2_edit_mode"])
    edit_mode = requested_edit and prereq_ok and (not is_locked)
    st.caption(f"현재: **{'수정모드' if edit_mode else '보기모드'}**")

    # ── 점수 입력 UI(표) ────────────────────────────────────────────────────
    st.markdown("#### 점수 입력 (자기/1차/2차) — 표에서 직접 수정하세요.")

    # Helper: 특정 평가유형(자기/1차/2차)의 '대상자 기준' 최신 점수(평가자 무관) 로드
    def _stage_scores_any_evaluator(_year: int, _etype: str, _target_sabun: str) -> dict[str, int]:
        try:
            ws = _ensure_eval_resp_sheet(int(_year), item_ids)
            header = _retry(ws.row_values, 1) or []; hmap = {n: i + 1 for i, n in enumerate(header)}
            values = _ws_values(ws)
            cY=hmap.get("연도"); cT=hmap.get("평가유형"); cTS=hmap.get("평가대상사번"); cDT=hmap.get("제출시각")
            picked = None; picked_dt = ""
            for r in values[1:]:
                try:
                    if (str(r[cY-1]).strip()==str(_year)
                        and str(r[cT-1]).strip()==str(_etype)
                        and str(r[cTS-1]).strip()==str(_target_sabun)):
                        ts = str(r[cDT-1]) if (cDT and cDT-1 < len(r)) else ""
                        if ts >= (picked_dt or ""):
                            picked = r; picked_dt = ts or ""
                except Exception:
                    pass
            if not picked: return {}
            out: dict[str,int] = {}
            for iid in item_ids:
                col = hmap.get(f"점수_{iid}")
                if col and col-1 < len(picked):
                    try:
                        v = int(str(picked[col-1]).strip() or "0")
                        if v: out[iid] = v
                    except Exception:
                        pass
            return out
        except Exception:
            return {}

    # 일괄 적용(현재 편집 컬럼만)
    _year_safe = int(st.session_state.get("eval2_year", datetime.now(tz=tz_kst()).year))
    _eval_type_safe = str(st.session_state.get("eval_type") or st.session_state.get("eval2_type") or ("자기"))
    kbase = f"E2_{_year_safe}_{_eval_type_safe}_{me_sabun}_{target_sabun}"
    bulk_map_key = f"eval2_bulkmap_{kbase}"
    bulk_map = st.session_state.get(bulk_map_key, {})

    slider_key = f"{kbase}_slider_multi"
    if slider_key not in st.session_state:
        if saved_scores:
            avg = round(sum(saved_scores.values()) / max(1, len(saved_scores)))
            st.session_state[slider_key] = int(min(5, max(1, avg)))
        else:
            st.session_state[slider_key] = 3
    bulk_score = st.slider("일괄 점수(현재 편집 컬럼)", 1, 5, step=1, key=slider_key, disabled=not edit_mode)
    if st.button("일괄 적용", use_container_width=True, disabled=not edit_mode, key=f"apply_bulk_{kbase}"):
        st.session_state[bulk_map_key] = {iid: int(bulk_score) for iid in item_ids}
        st.toast(f"{len(item_ids)}개 항목에 {int(bulk_score)}점 적용", icon="✅")
        st.rerun()

    # 현재 편집 대상 컬럼/표시 컬럼
    editable_col_name = {"자기":"자기평가","1차":"1차평가","2차":"2차평가"}.get(str(eval_type), "자기평가")
    if my_role == "employee":
        visible_cols = ["자기평가"]
    elif eval_type == "1차":
        visible_cols = ["자기평가","1차평가"]
    else:
        visible_cols = ["자기평가","1차평가","2차평가"]

    # 참조 점수 계산(가장 최근 제출된 이전 단계 점수)
    stage_self = _stage_scores_any_evaluator(int(year), "자기", str(target_sabun)) if "자기평가" in visible_cols else {}
    stage_1st  = _stage_scores_any_evaluator(int(year), "1차", str(target_sabun))  if "1차평가" in visible_cols else {}

    # 편집 시드 값
    def _seed_for_editable(iid: str):
        # 1) 일괄 적용 맵
        if iid in bulk_map:
            try: return int(bulk_map[iid])
            except Exception: return None
        # 2) 개별 위젯 상태
        rkey = f"eval2_seg_{iid}_{kbase}"
        if rkey in st.session_state:
            try:
                v = st.session_state[rkey]
                return int(v) if (v is not None and str(v).strip()!="") else None
            except Exception:
                return None
        # 3) 기존 저장값
        if iid in saved_scores:
            try: return int(saved_scores[iid])
            except Exception: return None
        return None

    # 표 데이터 구성
    rows = []
    for r in items_sorted.itertuples(index=False):
        iid = str(getattr(r, "항목ID"))
        row = {
            "항목": getattr(r, "항목") or "",
            "내용": getattr(r, "내용") or "",
            "자기평가": None,
            "1차평가": None,
            "2차평가": None
        }
        # 참조 점수(읽기 컬럼) + 편집 컬럼 시드
        if "자기평가" in visible_cols:
            if editable_col_name == "자기평가":
                row["자기평가"] = _seed_for_editable(iid)
            else:
                v = stage_self.get(iid, None)
                row["자기평가"] = int(v) if v is not None else None
        if "1차평가" in visible_cols:
            if editable_col_name == "1차평가":
                row["1차평가"] = _seed_for_editable(iid)
            else:
                v = stage_1st.get(iid, None)
                row["1차평가"] = int(v) if v is not None else None
        if "2차평가" in visible_cols and editable_col_name == "2차평가":
            row["2차평가"] = _seed_for_editable(iid)
        rows.append(row)

    df_tbl = pd.DataFrame(rows, index=item_ids)

    # 합계 행(표 안)
    def _col_sum(col: str) -> int:
        if col not in df_tbl.columns: return 0
        s = pd.to_numeric(df_tbl[col], errors="coerce").fillna(0).astype(int).sum()
        return int(s)

    sum_row = {"항목": "합계", "내용": ""}
    for c in ["자기평가","1차평가","2차평가"]:
        if c in visible_cols:
            sum_row[c] = _col_sum(c)
    df_tbl_with_sum = pd.concat([df_tbl, pd.DataFrame([sum_row], columns=["항목","내용"]+visible_cols)], ignore_index=True)

    # 데이터 에디터
    col_cfg = {
        "항목": st.column_config.TextColumn("항목", disabled=True),
        "내용": st.column_config.TextColumn("내용", disabled=True),
    }
    if "자기평가" in visible_cols:
        col_cfg["자기평가"] = st.column_config.NumberColumn("자기평가", min_value=1, max_value=5, step=1, help="자기평가 1~5점", disabled=(editable_col_name!="자기평가" or not edit_mode))
    if "1차평가" in visible_cols:
        col_cfg["1차평가"] = st.column_config.NumberColumn("1차평가", min_value=1, max_value=5, step=1, help="1차평가 1~5점", disabled=(editable_col_name!="1차평가" or not edit_mode))
    if "2차평가" in visible_cols:
        col_cfg["2차평가"] = st.column_config.NumberColumn("2차평가", min_value=1, max_value=5, step=1, help="2차평가 1~5점", disabled=(editable_col_name!="2차평가" or not edit_mode))

    edited = st.data_editor(
        df_tbl_with_sum[["항목","내용"] + visible_cols],
        hide_index=True,
        use_container_width=True,
        disabled=False,  # 일부 컬럼만 disabled
        num_rows="fixed",
        column_config=col_cfg,
        height=min(560, 64 + 36 * len(df_tbl_with_sum))
    )

    # 점수 dict(합계 행 제외, 편집 컬럼만 저장) — 공란은 저장하지 않음
    scores = {}
    if editable_col_name in edited.columns:
        values = list(edited[editable_col_name].tolist())[:-1]  # 마지막 행은 합계
        for iid, v in zip(item_ids, values):
            if v is None or str(v).strip() == "":
                continue
            try:
                val = int(v)
            except Exception:
                continue
            st.session_state[f"eval2_seg_{iid}_{kbase}"] = str(val)
            scores[iid] = val

    # ── 제출 확인 ────────────────────────────────────────────────────────────
    st.markdown("#### 제출 확인")
    cb1, cb2 = st.columns([2, 1])
    with cb1:
        attest_ok = st.checkbox(
            "본인은 입력한 내용이 사실이며, 회사의 인사평가 정책에 따라 제출함을 확인합니다.",
            key=f"eval_attest_ok_{kbase}",
            disabled=not edit_mode
        )
    with cb2:
        pin_input = st.text_input(
            "PIN 재입력",
            value="",
            type="password",
            key=f"eval_attest_pin_{kbase}",
            disabled=not edit_mode
        )

    # PIN 검증 대상: 자기평가는 대상자 사번, 1차/2차는 평가자(본인) 사번
    sabun_for_pin = str(target_sabun) if str(eval_type) == "자기" else str(me_sabun)

    cbtn = st.columns([1, 1, 3])
    with cbtn[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True,
                            key=f"eval_save_{kbase}", disabled=not edit_mode)
    with cbtn[1]:
        do_reset = st.button("초기화", use_container_width=True,
                             key=f"eval_reset_{kbase}", disabled=not edit_mode)

    if do_reset:
        for _iid in item_ids:
            _k = f"eval2_seg_{_iid}_{kbase}"
            if _k in st.session_state:
                del st.session_state[_k]
        # 일괄 적용 맵 초기화
        st.session_state.pop(bulk_map_key, None)
        st.rerun()

    if do_save:
        if not attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        elif not verify_pin(sabun_for_pin, pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            try:
                rep = upsert_eval_response(
                    emp_df, int(year), eval_type, str(target_sabun), str(me_sabun), scores, "제출"
                )
                st.success(
                    ("제출 완료" if rep.get("action") == "insert" else "업데이트 완료")
                    + f" (총점 {rep.get('total','?')}점)",
                    icon="✅",
                )
                st.session_state["eval2_edit_mode"] = False
                st.session_state['eval_rev'] = st.session_state.get('eval_rev', 0) + 1
                st.rerun()
            except Exception as e:
                st.exception(e)

# ═════════════════════════════════════════════════════════════════════════════
# 직무기술서 (Job Description) — Performance‑tuned & Comment‑standardized
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
JOBDESC_SHEET = "직무기술서"
JOBDESC_HEADERS = [
    "사번","이름","연도","버전","부서1","부서2","작성자사번","작성자이름",
    "직군","직종","직무명","제정일","개정일","검토주기",
    "직무개요","주업무","기타업무",
    "필요학력","전공계열","직원공통필수교육","보수교육","기타교육","특성화교육",
    "면허","경력(자격요건)","비고","제출시각"
]

JD_APPROVAL_SHEET  = "직무기술서_승인"
JD_APPROVAL_HEADERS = ["연도","사번","이름","버전","승인자사번","승인자이름","상태","승인시각","비고"]

# ═════════════════════════════════════════════════════════════════════════════
# Sheet Ensure / Readers
# ═════════════════════════════════════════════════════════════════════════════
def ensure_jobdesc_sheet():
    """Ensure 직무기술서 시트와 헤더 존재. AUTO_FIX_HEADERS가 True면 자동 보강."""
    wb = get_book()
    try:
        ws = wb.worksheet(JOBDESC_SHEET)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=JOBDESC_SHEET, rows=2000, cols=max(80, len(JOBDESC_HEADERS)+8))
        _retry(ws.update, "A1", [JOBDESC_HEADERS])
        return ws

    try:
        header = _retry(ws.row_values, 1) or []
    except Exception as e:
        if _is_quota_429(e):
            try: st.warning("구글시트 읽기 할당량(1분) 초과. 잠시 후 좌측 '동기화'를 눌러주세요.", icon="⏳")
            except Exception: pass
            return ws
        raise

    need = [h for h in JOBDESC_HEADERS if h not in header]
    if need:
        if AUTO_FIX_HEADERS:
            _retry(ws.update, "1:1", [header + need])
        else:
            try:
                st.warning("시트 헤더에 다음 컬럼이 없습니다: " + ", ".join(need) +
                           "\n→ 시트를 직접 수정한 뒤 좌측 🔄 동기화 버튼을 눌러주세요.", icon="⚠️")
            except Exception:
                pass
    return ws

@st.cache_data(ttl=600, show_spinner=False)
def read_jobdesc_df(_rev: int = 0) -> pd.DataFrame:
    """직무기술서 시트를 DataFrame으로 읽기 (dtype 정리 포함)."""
    ensure_jobdesc_sheet()
    ws = _ws(JOBDESC_SHEET)
    try:
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception as e:
        if _is_quota_429(e):
            try: st.warning("구글시트 읽기 할당량(1분) 초과. 잠시 후 '동기화'를 눌러주세요.", icon="⏳")
            except Exception: pass
            return pd.DataFrame(columns=JOBDESC_HEADERS)
        raise

    if df.empty:
        return pd.DataFrame(columns=JOBDESC_HEADERS)

    # 타입 정리 (필드 존재할 때만)
    for c in JOBDESC_HEADERS:
        if c in df.columns:
            df[c] = df[c].astype(str)

    # 연/버전 → int
    for c in ("연도","버전"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)

    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)

    return df

def _jd_latest_for(sabun: str, year: int) -> dict | None:
    """해당 (사번, 연도)의 최신 버전 레코드 1건 반환 (없으면 None)."""
    df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
    if df.empty:
        return None

    sabun_s = str(sabun)
    year_i  = int(year)

    # ⚠️ copy()로 경고 제거
    mask = (df["사번"].astype(str) == sabun_s) & (
        pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int) == year_i
    )
    sub = df.loc[mask].copy()
    if sub.empty:
        return None

    # 안전한 정수 변환
    sub["버전"] = pd.to_numeric(sub["버전"], errors="coerce").fillna(0).astype(int)

    row = sub.sort_values(["버전"], ascending=[False]).iloc[0].to_dict()
    # 문자열화
    return {k: ("" if v is None else str(v)) for k, v in row.items()}

def _jobdesc_next_version(sabun: str, year: int) -> int:
    """다음 버전 번호 산출 (해당 키가 없으면 1)."""
    df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
    if df.empty:
        return 1

    sabun_s = str(sabun)
    year_i  = int(year)

    # ⚠️ copy()로 경고 제거
    mask = (df["사번"].astype(str) == sabun_s) & (
        pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int) == year_i
    )
    sub_ver = df.loc[mask, "버전"].copy()
    if sub_ver.empty:
        return 1

    ver_max = pd.to_numeric(sub_ver, errors="coerce").fillna(0).astype(int).max()
    return int(ver_max) + 1

# ═════════════════════════════════════════════════════════════════════════════
# Write helpers (batch‑safe)
# ═════════════════════════════════════════════════════════════════════════════
def _ws_batch_row(ws, idx: int, hmap: dict, kv: dict):
    """한 행의 여러 셀을 values_batch_update로 일괄 업데이트."""
    if not kv:
        return
    updates = []
    max_c = 0
    for k, v in kv.items():
        c = hmap.get(k)
        if not c:
            continue
        try:
            cc = int(c)
        except Exception:
            continue
        max_c = max(max_c, cc)
        a1 = gspread.utils.rowcol_to_a1(int(idx), cc)
        updates.append({"range": f"'{ws.title}'!{a1}", "values": [[v]]})

    if updates:
        # 그리드 확장 보장 (희귀 케이스 방지)
        _ensure_capacity(ws, int(idx), int(max_c) if max_c else None)
        body = {"valueInputOption": "USER_ENTERED", "data": updates}
        _retry(ws.spreadsheet.values_batch_update, body)

def upsert_jobdesc(rec: dict, as_new_version: bool = False) -> dict:
    """
    직무기술서 upsert.
    - 키: (사번, 연도, 버전)
    - as_new_version=True 이면 버전 자동증가
    """
    ensure_jobdesc_sheet()
    ws = _ws(JOBDESC_SHEET)
    header = _retry(ws.row_values, 1) or JOBDESC_HEADERS
    hmap = {n: i + 1 for i, n in enumerate(header)}

    sabun = str(rec.get("사번", "")).strip()
    year  = int(rec.get("연도", 0))

    # 이름 자동 채움 (read_emp_df 1회만 호출)
    try:
        _edf = read_emp_df()
        rec["이름"] = _emp_name_by_sabun(_edf, sabun)
    except Exception:
        rec["이름"] = rec.get("이름", "")

    # 버전 결정
    if as_new_version:
        ver = _jobdesc_next_version(sabun, year)
    else:
        try_ver = int(str(rec.get("버전", 0) or 0))
        if try_ver <= 0:
            ver = _jobdesc_next_version(sabun, year)
        else:
            df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
            exist = (not df[
                (df["사번"].astype(str) == sabun) &
                (df["연도"].astype(int) == year) &
                (df["버전"].astype(int) == try_ver)
            ].empty)
            ver = try_ver if exist else 1

    rec["버전"] = int(ver)
    rec["제출시각"] = kst_now_str()

    # 기존 행 검색
    values = _ws_values(ws)
    row_idx = 0
    cS, cY, cV = hmap.get("사번"), hmap.get("연도"), hmap.get("버전")
    if all([cS, cY, cV]):
        for i in range(2, len(values) + 1):
            row = values[i - 1] if i - 1 < len(values) else []
            try:
                if (str(row[cS - 1]).strip() == sabun and
                    str(row[cY - 1]).strip() == str(year) and
                    str(row[cV - 1]).strip() == str(ver)):
                    row_idx = i; break
            except Exception:
                pass

    def _build_row() -> list[str]:
        buf = [""] * len(header)
        for k, v in rec.items():
            c = hmap.get(k)
            if c:
                buf[c - 1] = v
        return buf

    if row_idx == 0:
        _retry(ws.append_row, _build_row(), value_input_option="USER_ENTERED")
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "insert", "version": ver}
    else:
        _ws_batch_row(ws, row_idx, hmap, rec)
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "update", "version": ver}

# ═════════════════════════════════════════════════════════════════════════════
# Approval sheet (ensure / read / set)
# ═════════════════════════════════════════════════════════════════════════════
def ensure_jd_approval_sheet():
    """Ensure 직무기술서_승인 시트/헤더 존재."""
    wb = get_book()
    try:
        _ = wb.worksheet(JD_APPROVAL_SHEET)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=JD_APPROVAL_SHEET, rows=3000, cols=max(20, len(JD_APPROVAL_HEADERS)+4))
        _retry(ws.update, "1:1", [JD_APPROVAL_HEADERS])

@st.cache_data(ttl=300, show_spinner=False)
def read_jd_approval_df(_rev: int = 0) -> pd.DataFrame:
    """직무기술서_승인 DF (dtype 정리)."""
    ensure_jd_approval_sheet()
    try:
        ws = _ws(JD_APPROVAL_SHEET)
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        df = pd.DataFrame(columns=JD_APPROVAL_HEADERS)

    for c in JD_APPROVAL_HEADERS:
        if c not in df.columns:
            df[c] = ""

    if "연도" in df.columns:
        df["연도"] = pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int)
    if "버전" in df.columns:
        df["버전"] = pd.to_numeric(df["버전"], errors="coerce").fillna(0).astype(int)
    if "사번" in df.columns:
        df["사번"] = df["사번"].astype(str)

    return df

def _jd_latest_version_for(sabun: str, year: int) -> int:
    row = _jd_latest_for(sabun, int(year)) or {}
    try:
        return int(row.get("버전", 0) or 0)
    except Exception:
        return 0

def set_jd_approval(year: int, sabun: str, name: str, version: int,
                    approver_sabun: str, approver_name: str, status: str, remark: str = "") -> dict:
    """
    (연도, 사번, 버전) 기준 upsert. status: '승인' | '반려'
    """
    ensure_jd_approval_sheet()
    ws = _ws(JD_APPROVAL_SHEET)
    header = _retry(ws.row_values, 1) or JD_APPROVAL_HEADERS
    hmap = {n: i+1 for i, n in enumerate(header)}
    values = _ws_values(ws)

    cY = hmap.get("연도"); cS = hmap.get("사번"); cV = hmap.get("버전")
    target_row = 0
    if all([cY, cS, cV]):
        for i in range(2, len(values)+1):
            r = values[i-1] if i-1 < len(values) else []
            try:
                if (str(r[cY-1]).strip()==str(year) and
                    str(r[cS-1]).strip()==str(sabun) and
                    str(r[cV-1]).strip()==str(version)):
                    target_row = i; break
            except Exception:
                pass

    now = kst_now_str() if "kst_now_str" in globals() else str(pd.Timestamp.now()).split(".")[0]
    payload = {
        "연도": int(year),
        "사번": str(sabun),
        "이름": str(name),
        "버전": int(version),
        "승인자사번": str(approver_sabun),
        "승인자이름": str(approver_name),
        "상태": str(status),
        "승인시각": now,
        "비고": str(remark or ""),
    }

    if target_row > 0:
        _ws_batch_row(ws, target_row, hmap, payload)
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "update", "row": target_row}
    else:
        rowbuf = [""] * len(header)
        for k, v in payload.items():
            c = hmap.get(k)
            if c:
                rowbuf[c-1] = v
        _retry(ws.append_row, rowbuf, value_input_option="USER_ENTERED")
        try: st.cache_data.clear()
        except Exception: pass
        return {"action": "insert", "row": len(values) + 1}

# ═════════════════════════════════════════════════════════════════════════════
# Print (HTML)
# ═════════════════════════════════════════════════════════════════════════════
def _jd_print_html(jd: dict, meta: dict) -> str:
    """인쇄용 단일 페이지 HTML. 모든 섹션 포함, 자체 '인쇄' 버튼 노출."""
    import html as _html
    def g(k): return _html.escape((str(jd.get(k, "")) or "—").strip())
    def m(k): return _html.escape((str(meta.get(k, "")) or "—").strip())

    # Combine departments
    dept = m('부서1')
    if (meta.get('부서2') or "").strip():
        dept2 = m('부서2')
        dept = f"{dept} / {dept2}" if dept and dept != '—' else dept2

    # Meta rows
    row1 = [("사번", m('사번')), ("이름", m('이름')), ("부서", dept or "—")]
    row2 = [("직종", m('직종')), ("직군", m('직군')), ("직무명", m('직무명'))]
    row3 = [("연도", m('연도')), ("버전", m('버전')), ("제정일", m('제정일')), ("개정일", m('개정일')), ("검토주기", m('검토주기'))]

    def trow_3cols_kvk(title_pairs, wide_last=True):
        cells = []
        for i, (k, v) in enumerate(title_pairs):
            wide_cls = " wide" if (wide_last and i == 2) else ""
            cells.append(f"<td class='k'>{k}</td><td class='v{wide_cls}'>{v}</td>")
        return "<tr>" + "".join(cells) + "</tr>"

    def trow_5cols_kvk(title_pairs):
        cells = []
        for (k, v) in title_pairs:
            cells.append(f"<td class='k'>{k}</td><td class='v'>{v}</td>")
        return "<tr>" + "".join(cells) + "</tr>"

    def block(title, body):
        body_val = (body or "").strip() or "—"
        return f"""
        <section class="blk">
          <div class="cap">{title}</div>
          <div class="body">{body_val}</div>
        </section>
        """

    body_html = (
        block("직무개요", g("직무개요")) +
        block("주요업무", g("주업무")) +
        block("기타업무", g("기타업무")) +
        block("자격교육요건", f"""
            <div class="grid edu">
              <div class="cell"><b>필요학력</b><div>{g("필요학력")}</div></div>
              <div class="cell"><b>전공계열</b><div>{g("전공계열")}</div></div>
              <div class="cell"><b>면허</b><div>{g("면허")}</div></div>
              <div class="cell"><b>경력(자격요건)</b><div>{g("경력(자격요건)")}</div></div>

              <div class="cell span2"><b>직원공통필수교육</b><div>{g("직원공통필수교육")}</div></div>
              <div class="cell span2"><b>특성화교육</b><div>{g("특성화교육")}</div></div>
              <div class="cell span2"><b>보수교육</b><div>{g("보수교육")}</div></div>
              <div class="cell span2"><b>기타교육</b><div>{g("기타교육")}</div></div>
            </div>
        """)
    )

    html = f"""
    <html>
    <head>
      <meta charset="utf-8" />
      <title>직무기술서 출력</title>
      <style>
        :root {{ --fg:#111; --muted:#666; --line:#e5e7eb; }}
        html, body {{
          color: var(--fg);
          font-family: 'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic','Segoe UI',Roboto,system-ui,sans-serif;
          -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
          orphans: 2; widows: 2; word-break: keep-all; overflow-wrap: anywhere;
        }}
        .print-wrap {{ max-width: 900px; margin: 0 auto; padding: 28px 24px; background:#fff; }}
        .actionbar {{ display:flex; justify-content:flex-end; margin-bottom:12px; }}
        .actionbar button {{ padding:6px 12px; border:1px solid var(--line); background:#fff; border-radius:6px; cursor:pointer; }}
        header {{ border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:18px; }}
        header h1 {{ margin:0; font-size: 22px; }}

        table.meta6 {{ width:100%; border-collapse:collapse; margin-top:4px; font-size:13px; color:var(--muted); table-layout:fixed; }}
        table.meta6 td {{ padding:4px 6px; vertical-align:top; border-bottom:1px dashed var(--line); }}
        table.meta6 td.k {{ width:10%; color:#111; font-weight:700; white-space:nowrap; }}
        table.meta6 td.v {{ width:20%; color:#333; overflow:hidden; text-overflow:ellipsis; }}
        table.meta6 td.v.wide {{ width:30%; }}

        table.meta10 {{ width:100%; border-collapse:collapse; margin-top:4px; font-size:13px; color:var(--muted); table-layout:fixed; }}
        table.meta10 td {{ padding:4px 6px; vertical-align:top; border-bottom:1px dashed var(--line); }}
        table.meta10 td.k {{ width:10%; color:#111; font-weight:700; white-space:nowrap; }}
        table.meta10 td.v {{ width:10%; color:#333; overflow:hidden; text-overflow:ellipsis; }}

        .blk {{ break-inside: auto; page-break-inside: auto; margin: 12px 0 16px; }}
        .blk .cap {{ font-size:13px; color:#111; font-weight:700; margin: 2px 0 6px; }}
        .blk .body {{ white-space: pre-wrap; font-size:11px; line-height: 1.55; border:1px solid var(--line); padding:10px; border-radius:8px; min-height:60px; }}

        .grid.edu {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:8px; }}
        .grid.edu .cell {{ border:1px solid var(--line); border-radius:8px; padding:8px; }}
        .grid.edu .cell > b {{ font-size:12px; color:#111; }}
        .grid.edu .cell > div {{ font-size:11px; line-height:1.55; color:#333; }}
        .grid.edu .cell.span2 {{ grid-column: 1 / -1; }}

        .sign {{ margin-top:20px; display:flex; gap:16px; }}
        .sign > div {{ flex:1; border:1px dashed var(--line); border-radius:8px; padding:10px; min-height:70px; }}
        .sign .cap {{ font-size:13px; color:#111; font-weight:700; margin-bottom:6px; }}
        .sign .body {{ font-size:11px; line-height:1.55; color:#333; }}

        @media print {{
          @page {{ size: A4; margin: 18mm 14mm; }}
          body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
          .actionbar {{ display:none !important; }}
        }}
      </style>
    </head>
    <body>
      <div class="print-wrap">
        <div class="actionbar"><button onclick="window.print()">인쇄</button></div>
        <header>
          <h1>직무기술서 (Job Description)</h1>
          <table class="meta6">{trow_3cols_kvk(row1, wide_last=True)}</table>
          <table class="meta6">{trow_3cols_kvk(row2, wide_last=True)}</table>
          <table class="meta10">{trow_5cols_kvk(row3)}</table>
        </header>
        {body_html}
        <div class="sign">
          <div><div class="cap">직원 확인 서명</div><div class="body"></div></div>
          <div><div class="cap">부서장 확인 서명</div><div class="body"></div></div>
        </div>
      </div>
    </body>
    </html>
    """
    return html

# ═════════════════════════════════════════════════════════════════════════════
# Main Tab — 직무기술서
# ═════════════════════════════════════════════════════════════════════════════
def tab_job_desc(emp_df: pd.DataFrame):
    """
    JD 편집기
      - 2단 헤더 + 4단 교육영역 레이아웃
      - 인쇄 미리보기/버튼 (_jd_print_html 사용)
      - 관리자/부서장 승인 섹션 포함
    """
    # ── 기본 정보 ────────────────────────────────────────────────────────────
    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="jd2_year")

    u = st.session_state["user"]
    me_sabun = str(u["사번"]); me_name = str(u["이름"])

    am_admin_or_mgr = (is_admin(me_sabun) or len(get_allowed_sabuns(emp_df, me_sabun, include_self=False)) > 0)
    allowed = get_allowed_sabuns(emp_df, me_sabun, include_self=True)

    glob_sab, glob_name = get_global_target()
    st.session_state.setdefault("jd2_target_sabun", glob_sab or "")
    st.session_state.setdefault("jd2_target_name",  glob_name or me_name)
    st.session_state.setdefault("jd2_edit_mode",    False)

    # ── 대상자 선택 ─────────────────────────────────────────────────────────
    if not am_admin_or_mgr:
        target_sabun = me_sabun; target_name = me_name
        st.info(f"대상자: {target_name} ({target_sabun})", icon="👤")
    else:
        base = emp_df.copy()
        base["사번"] = base["사번"].astype(str)
        base = base[base["사번"].isin({str(s) for s in allowed})]
        if "재직여부" in base.columns:
            base = base[base["재직여부"] == True]
        cols = ["사번", "이름", "부서1", "부서2"]
        if "직급" in base.columns: cols.append("직급")
        view = base[cols].copy().sort_values(["사번"]).reset_index(drop=True)

        _sabuns = view["사번"].astype(str).tolist()
        _names  = view["이름"].astype(str).tolist()
        _d2     = view["부서2"].astype(str).tolist() if "부서2" in view.columns else [""] * len(_sabuns)
        _opts   = [f"{s} - {n} - {d2}" for s, n, d2 in zip(_sabuns, _names, _d2)]

        _target = st.session_state.get("jd2_target_sabun", glob_sab or "")
        _idx2   = (1 + _sabuns.index(_target)) if (_target in _sabuns) else 0
        _sel    = st.selectbox("대상자 선택", ["(선택)"] + _opts, index=_idx2, key="jd2_pick_editor_select")
        if _sel == "(선택)":
            st.session_state["jd2_target_sabun"] = ""
            st.session_state["jd2_target_name"]  = ""
            st.info("대상자를 선택하세요.", icon="👈")
            return
        _sel_sab = _sel.split(" - ", 1)[0] if isinstance(_sel, str) and " - " in _sel else (_sabuns[0] if _sabuns else "")
        st.session_state["jd2_target_sabun"] = str(_sel_sab)
        try:
            st.session_state["jd2_target_name"] = str(_names[_sabuns.index(_sel_sab)]) if _sel_sab in _sabuns else ""
        except Exception:
            st.session_state["jd2_target_name"] = ""

        target_sabun = st.session_state["jd2_target_sabun"]
        target_name  = st.session_state["jd2_target_name"]
        st.success(f"대상자: {target_name} ({target_sabun})", icon="✅")

    # ── 제출/승인 현황 배너 ─────────────────────────────────────────────────
    try:
        latest = _jd_latest_for(str(target_sabun), int(year)) or {}
        _sub_ts = (str(latest.get('제출시각','')).strip() or "미제출")
        latest_ver = _jd_latest_version_for(str(target_sabun), int(year))

        appr_df = read_jd_approval_df(st.session_state.get('appr_rev', 0))
        _appr_status, _appr_time = "미제출", ""
        if latest_ver > 0 and not appr_df.empty:
            sub = appr_df[(appr_df['연도'] == int(year)) &
                          (appr_df['사번'].astype(str) == str(target_sabun)) &
                          (appr_df['버전'] == int(latest_ver))].copy()
            if not sub.empty:
                if '승인시각' in sub.columns:
                    sub = sub.sort_values(['승인시각'], ascending=[False]).reset_index(drop=True)
                srow = sub.iloc[0].to_dict()
                _appr_status = str(srow.get('상태','')).strip() or "미제출"     # 승인 / 반려 / (없음)
                _appr_time   = str(srow.get('승인시각','')).strip()

        _appr_right = _appr_status + (f" {_appr_time}" if _appr_time else "")
        show_submit_banner(f"🕒 제출시각  |  {_sub_ts}  |  [부서장 승인여부] {_appr_right}")
    except Exception:
        pass

    # ── 모드 토글 ───────────────────────────────────────────────────────────
    if st.button(("수정모드로 전환" if not st.session_state["jd2_edit_mode"] else "보기모드로 전환"),
                 use_container_width=True, key="jd2_toggle"):
        st.session_state["jd2_edit_mode"] = not st.session_state["jd2_edit_mode"]
        st.rerun()
    st.caption(f"현재: **{'수정모드' if st.session_state['jd2_edit_mode'] else '보기모드'}**")
    edit_mode = bool(st.session_state["jd2_edit_mode"])

    # ── 현재/초기 레코드 ────────────────────────────────────────────────────
    jd_saved = _jd_latest_for(target_sabun, int(year)) or {}

    def _safe_get(col, default=""):
        try:
            return emp_df.loc[emp_df["사번"].astype(str) == str(target_sabun)].get(col, default).values[0] if col in emp_df.columns else default
        except Exception:
            return default

    jd_current = {
        "사번": str(target_sabun), "연도": int(year), "버전": int(jd_saved.get("버전", 0) or 0),
        "부서1": jd_saved.get("부서1", _safe_get("부서1","")), "부서2": jd_saved.get("부서2", _safe_get("부서2","")),
        "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
        "직군": jd_saved.get("직군",""), "직종": jd_saved.get("직종",""), "직무명": jd_saved.get("직무명",""),
        "제정일": jd_saved.get("제정일",""), "개정일": jd_saved.get("개정일",""), "검토주기": jd_saved.get("검토주기","1년"),
        "직무개요": jd_saved.get("직무개요",""), "주업무": jd_saved.get("주업무",""), "기타업무": jd_saved.get("기타업무",""),
        "필요학력": jd_saved.get("필요학력",""), "전공계열": jd_saved.get("전공계열",""),
        "직원공통필수교육": jd_saved.get("직원공통필수교육",""), "보수교육": jd_saved.get("보수교육",""),
        "기타교육": jd_saved.get("기타교육",""), "특성화교육": jd_saved.get("특성화교육",""),
        "면허": jd_saved.get("면허",""), "경력(자격요건)": jd_saved.get("경력(자격요건)",""), "비고": jd_saved.get("비고",""),
    }

    # ── 현재 저장분 요약 (접이식) ────────────────────────────────────────────
    with st.expander("현재 저장된 직무기술서 요약", expanded=False):
        st.write(f"**직무명:** {jd_saved.get('직무명', '')}")
        cc = st.columns(2)
        with cc[0]:
            st.markdown("**직무개요**")
            st.markdown(_jd_plain_html(jd_saved.get("직무개요","") or "—"), unsafe_allow_html=True)
            st.markdown("**주업무**")
            st.markdown(_jd_plain_html(jd_saved.get("주업무","") or "—"), unsafe_allow_html=True)
        with cc[1]:
            st.markdown("**기타업무**")
            st.markdown(_jd_plain_html(jd_saved.get("기타업무","") or "—"), unsafe_allow_html=True)

    # ── 입력 폼: 헤더 2줄 ───────────────────────────────────────────────────
    r1 = st.columns([1, 1, 1, 1, 1.6])
    with r1[0]:
        version = st.number_input("버전(없으면 자동)", min_value=0, max_value=999,
                                  value=int(jd_current["버전"]), step=1,
                                  key="jd2_ver", disabled=not edit_mode)
    with r1[1]:
        d_create = st.text_input("제정일", value=jd_current["제정일"], key="jd2_d_create", disabled=not edit_mode)
    with r1[2]:
        d_update = st.text_input("개정일", value=jd_current["개정일"], key="jd2_d_update", disabled=not edit_mode)
    with r1[3]:
        review = st.text_input("검토주기", value=jd_current["검토주기"], key="jd2_review", disabled=not edit_mode)
    with r1[4]:
        memo = st.text_input("비고", value=jd_current["비고"], key="jd2_memo", disabled=not edit_mode)

    r2 = st.columns([1, 1, 1, 1, 1.6])
    with r2[0]:
        dept1  = st.text_input("부서1", value=jd_current["부서1"], key="jd2_dept1", disabled=not edit_mode)
    with r2[1]:
        dept2  = st.text_input("부서2", value=jd_current["부서2"], key="jd2_dept2", disabled=not edit_mode)
    with r2[2]:
        group  = st.text_input("직군",  value=jd_current["직군"],  key="jd2_group",  disabled=not edit_mode)
    with r2[3]:
        series = st.text_input("직종",  value=jd_current["직종"],  key="jd2_series", disabled=not edit_mode)
    with r2[4]:
        jobname= st.text_input("직무명", value=jd_current["직무명"], key="jd2_jobname", disabled=not edit_mode)

    # 본문
    job_summary = st.text_area("직무개요", value=jd_current["직무개요"], height=80,  key="jd2_summary", disabled=not edit_mode)
    job_main    = st.text_area("주업무",   value=jd_current["주업무"],   height=120, key="jd2_main",    disabled=not edit_mode)
    job_other   = st.text_area("기타업무", value=jd_current["기타업무"], height=80,  key="jd2_other",   disabled=not edit_mode)

    # 교육/자격 4행
    e1 = st.columns([1,1,1,1])
    with e1[0]: edu_req    = st.text_input("필요학력",        value=jd_current["필요학력"],        key="jd2_edu",        disabled=not edit_mode)
    with e1[1]: major_req  = st.text_input("전공계열",        value=jd_current["전공계열"],        key="jd2_major",      disabled=not edit_mode)
    with e1[2]: license_   = st.text_input("면허",            value=jd_current["면허"],            key="jd2_license",    disabled=not edit_mode)
    with e1[3]: career     = st.text_input("경력(자격요건)", value=jd_current["경력(자격요건)"], key="jd2_career",     disabled=not edit_mode)
    edu_common = st.text_input("직원공통필수교육", value=jd_current["직원공통필수교육"], key="jd2_edu_common", disabled=not edit_mode)
    edu_spec   = st.text_input("특성화교육",       value=jd_current["특성화교육"],       key="jd2_edu_spec",   disabled=not edit_mode)
    e4 = st.columns([1,1])
    with e4[0]: edu_cont   = st.text_input("보수교육",        value=jd_current["보수교육"],        key="jd2_edu_cont",   disabled=not edit_mode)
    with e4[1]: edu_etc    = st.text_input("기타교육",        value=jd_current["기타교육"],        key="jd2_edu_etc",    disabled=not edit_mode)

    # ── 제출 확인 ───────────────────────────────────────────────────────────
    ca1, ca2 = st.columns([2, 1])
    with ca1:
        jd_attest_ok = st.checkbox(
            "본인은 입력한 직무기술서 내용이 사실이며, 회사 정책에 따라 제출함을 확인합니다.",
            key=f"jd_attest_ok_{year}_{target_sabun}_{me_sabun}",
            disabled=not edit_mode
        )
    with ca2:
        jd_pin_input = st.text_input(
            "PIN 재입력", value="", type="password",
            key=f"jd_attest_pin_{year}_{target_sabun}_{me_sabun}",
            disabled=not edit_mode
        )

    # 버튼
    cbtn = st.columns([1, 1])
    with cbtn[0]:
        do_save = st.button("제출/저장", type="primary", use_container_width=True, key="jd2_save", disabled=not edit_mode)
    with cbtn[1]:
        do_print = st.button("인쇄", type="secondary", use_container_width=True, key="jd2_print", disabled=False)

    # 현재 입력값(저장/인쇄 공용)
    current_input = {
        "사번": str(target_sabun), "연도": int(year), "버전": int(version or 0),
        "부서1": dept1, "부서2": dept2, "작성자사번": me_sabun, "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
        "직군": group, "직종": series, "직무명": jobname,
        "제정일": d_create, "개정일": d_update, "검토주기": review,
        "직무개요": job_summary, "주업무": job_main, "기타업무": job_other,
        "필요학력": edu_req, "전공계열": major_req,
        "직원공통필수교육": edu_common, "보수교육": edu_cont, "기타교육": edu_etc, "특성화교육": edu_spec,
        "면허": license_, "경력(자격요건)": career, "비고": memo
    }

    # 저장
    if do_save:
        if not jd_attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        elif not verify_pin(me_sabun, jd_pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            try:
                rep = upsert_jobdesc(current_input, as_new_version=(version == 0))
                st.success(f"저장 완료 (버전 {rep['version']})", icon="✅")
                st.session_state['jobdesc_rev'] = st.session_state.get('jobdesc_rev', 0) + 1
                st.rerun()
            except Exception as e:
                st.exception(e)

    # 인쇄
    if do_print:
        meta = {
            "사번": str(target_sabun), "이름": str(target_name),
            "부서1": str(dept1), "부서2": str(dept2),
            "연도": int(year), "버전": int(version or (jd_current.get("버전") or 1)),
            "작성자이름": _emp_name_by_sabun(emp_df, me_sabun),
            "제정일": str(d_create), "개정일": str(d_update),
            "검토주기": str(review),
            "직종": str(series), "직군": str(group), "직무명": str(jobname),
        }
        html = _jd_print_html(current_input, meta)  # ← 현재 입력값으로 바로 인쇄
        import streamlit.components.v1 as components
        components.html(html, height=1000, scrolling=True)

    # ── (관리자/부서장) 승인 처리 ────────────────────────────────────────────
    if am_admin_or_mgr:
        st.markdown("### 부서장 승인")
        appr_df = read_jd_approval_df(st.session_state.get("appr_rev", 0))
        latest_ver = _jd_latest_version_for(target_sabun, int(year))

        _approved = False
        if latest_ver > 0 and not appr_df.empty:
            _ok = appr_df[(appr_df['연도'] == int(year)) &
                          (appr_df['사번'].astype(str) == str(target_sabun)) &
                          (appr_df['버전'] == int(latest_ver)) &
                          (appr_df['상태'].astype(str) == '승인')]
            _approved = not _ok.empty

        cur_status = cur_when = cur_who = ""
        if latest_ver > 0 and not appr_df.empty:
            sub = appr_df[(appr_df["연도"]==int(year)) &
                          (appr_df["사번"].astype(str)==str(target_sabun)) &
                          (appr_df["버전"]==int(latest_ver))]
            if not sub.empty:
                srow = sub.sort_values(["승인시각"], ascending=[False]).iloc[0].to_dict()
                cur_status = str(srow.get("상태","")); cur_when = str(srow.get("승인시각","")); cur_who = str(srow.get("승인자이름",""))

        c_remark, c_pin = st.columns([4,1])
        if _approved:
            with c_remark:
                st.markdown("<div class='approval-dim'>부서장 승인이 완료된 대상자입니다. (수정/변경 불가)</div>", unsafe_allow_html=True)
            with c_pin:
                st.text_input("부서장 PIN 재입력", type="password", key=f"jd_appr_pin_{year}_{target_sabun}", disabled=True)
        else:
            with c_remark:
                appr_remark = st.text_input("부서장 의견", key=f"jd_appr_remark_{year}_{target_sabun}")
            with c_pin:
                appr_pin = st.text_input("부서장 PIN 재입력", type="password", key=f"jd_appr_pin_{year}_{target_sabun}")

            b1, b2 = st.columns([1,1])
            with b1:
                do_ok = st.button("승인", type="primary", use_container_width=True, disabled=not (latest_ver>0))
            with b2:
                do_rej = st.button("반려", use_container_width=True, disabled=not (latest_ver>0))

            if (do_ok or do_rej):
                if not verify_pin(me_sabun, appr_pin):
                    st.error("부서장 PIN 이 올바르지 않습니다.", icon="🚫")
                else:
                    status = "승인" if do_ok else "반려"
                    with st.spinner("처리 중..."):
                        res = set_jd_approval(
                            year=int(year),
                            sabun=str(target_sabun), name=str(target_name), version=int(latest_ver),
                            approver_sabun=str(me_sabun), approver_name=str(me_name),
                            status=status, remark=appr_remark
                        )
                        st.session_state["appr_rev"] = st.session_state.get("appr_rev", 0) + 1
                    st.success(f"{status} 처리되었습니다. ({res.get('action')})", icon="✅")

# ═════════════════════════════════════════════════════════════════════════════
# 직무능력평가 (Competency) — 성능 최적화 & 주석/구획 표준화
# ═════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
COMP_SIMPLE_PREFIX = "직무능력평가_"
COMP_SIMPLE_HEADERS = [
    "연도","평가대상사번","평가대상이름","평가자사번","평가자이름",
    "평가일자","주업무평가","기타업무평가","교육이수","자격유지","종합의견",
    "상태","제출시각","잠금"
]

# AUTO_FIX_HEADERS가 상위 블록에서 정의되지 않은 배포 환경 고려
try:
    AUTO_FIX_HEADERS  # noqa: F401
except NameError:
    AUTO_FIX_HEADERS = False

# ─────────────────────────────────────────────────────────────────────────────
# Sheet name helper
# ─────────────────────────────────────────────────────────────────────────────
def _simp_sheet_name(year: int | str) -> str:
    return f"{COMP_SIMPLE_PREFIX}{int(year)}"


# ═════════════════════════════════════════════════════════════════════════════
# Ensure / Readers
# ═════════════════════════════════════════════════════════════════════════════
def _ensure_comp_simple_sheet(year: int):
    """연도별 직무능력평가 시트 보장 + (선택) 헤더 자동 보강."""
    wb = get_book(); name = _simp_sheet_name(year)
    try:
        ws = wb.worksheet(name)
    except WorksheetNotFound:
        ws = _retry(wb.add_worksheet, title=name, rows=1200, cols=max(40, len(COMP_SIMPLE_HEADERS) + 6))
        _retry(ws.update, "1:1", [COMP_SIMPLE_HEADERS])
        return ws

    header = _retry(ws.row_values, 1) or []
    need = [h for h in COMP_SIMPLE_HEADERS if h not in header]
    if need:
        if AUTO_FIX_HEADERS:
            _retry(ws.update, "1:1", [header + need])
        else:
            try:
                st.warning(
                    "직무능력평가 시트 헤더에 누락된 컬럼이 있습니다: " + ", ".join(need) +
                    "\n→ 시트를 보강한 뒤 좌측 🔄 동기화를 눌러주세요.", icon="⚠️"
                )
            except Exception:
                pass
    return ws


def _jd_latest_for_comp(sabun: str, year: int) -> dict:
    """(사번, 연도)의 최신 JD 1건을 dict로 반환. 없으면 {}."""
    try:
        df = read_jobdesc_df(st.session_state.get("jobdesc_rev", 0))
        if df is None or df.empty:
            return {}
        mask = (df["사번"].astype(str) == str(sabun)) & (
            pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int) == int(year)
        )
        q = df.loc[mask].copy()
        if q.empty:
            return {}
        if "버전" in q.columns:
            q["버전"] = pd.to_numeric(q["버전"], errors="coerce").fillna(0).astype(int)
            q = q.sort_values("버전").iloc[-1]
        else:
            q = q.iloc[-1]
        return {c: q.get(c, "") for c in q.index}
    except Exception:
        return {}


def _edu_completion_from_jd(jd_row: dict) -> str:
    """JD의 '직원공통필수교육' 유무로 교육이수 상태 유추."""
    val = str(jd_row.get("직원공통필수교육", "")).strip()
    return "완료" if val else "미완료"


# ═════════════════════════════════════════════════════════════════════════════
# Write: Upsert (배치 쓰기 사용)
# ═════════════════════════════════════════════════════════════════════════════
def upsert_comp_simple_response(
    emp_df: pd.DataFrame,
    year: int,
    target_sabun: str,
    evaluator_sabun: str,
    main_grade: str,
    extra_grade: str,
    qual_status: str,
    opinion: str,
    eval_date: str = ""
) -> dict:
    """
    직무능력평가 기본형 응답 upsert.
    키: (연도, 평가대상사번, 평가자사번)
    - 배치 업데이트로 API 호출 최소화
    - eval_date 미지정/빈 문자열이면 KST 오늘 날짜로 자동 기록
    """
    ws = _ensure_comp_simple_sheet(year)
    header = _retry(ws.row_values, 1) or COMP_SIMPLE_HEADERS
    hmap = {n: i + 1 for i, n in enumerate(header)}

    jd = _jd_latest_for_comp(target_sabun, int(year))
    edu_status = _edu_completion_from_jd(jd)
    t_name = _emp_name_by_sabun(emp_df, target_sabun)
    e_name = _emp_name_by_sabun(emp_df, evaluator_sabun)

    now_ts = kst_now_str()
    eval_date_eff = (str(eval_date).strip() or now_ts.split(" ", 1)[0])  # YYYY-MM-DD

    # 기존 행 탐색
    values = _ws_values(ws)
    cY = hmap.get("연도"); cTS = hmap.get("평가대상사번"); cES = hmap.get("평가자사번")
    row_idx = 0
    if all([cY, cTS, cES]):
        for i in range(2, len(values) + 1):
            r = values[i - 1] if i - 1 < len(values) else []
            try:
                if (str(r[cY - 1]).strip() == str(year) and
                    str(r[cTS - 1]).strip() == str(target_sabun) and
                    str(r[cES - 1]).strip() == str(evaluator_sabun)):
                    row_idx = i; break
            except Exception:
                pass

    if row_idx == 0:
        # INSERT (append_row 한 번 호출로 완료)
        buf = [""] * len(header)
        def put(k, v): 
            c = hmap.get(k)
            if c: buf[c - 1] = v
        put("연도", int(year))
        put("평가대상사번", str(target_sabun)); put("평가대상이름", t_name)
        put("평가자사번", str(evaluator_sabun)); put("평가자이름", e_name)
        put("평가일자", eval_date_eff)
        put("주업무평가", main_grade); put("기타업무평가", extra_grade)
        put("교육이수", edu_status); put("자격유지", qual_status); put("종합의견", opinion)
        put("상태", "제출"); put("제출시각", now_ts); put("잠금", "")
        _retry(ws.append_row, buf, value_input_option="USER_ENTERED")
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action": "insert"}
    else:
        # UPDATE (배치 업데이트)
        _ws_batch_row(ws, row_idx, hmap, {
            "평가일자": eval_date_eff,
            "주업무평가": main_grade,
            "기타업무평가": extra_grade,
            "교육이수": edu_status,
            "자격유지": qual_status,
            "종합의견": opinion,
            "상태": "제출",
            "제출시각": now_ts,
        })
        try: read_my_comp_simple_rows.clear()
        except Exception: pass
        return {"action": "update"}


# ═════════════════════════════════════════════════════════════════════════════
# Reader: 내 제출 목록 (평가자 기준)
# ═════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def read_my_comp_simple_rows(year: int, sabun: str) -> pd.DataFrame:
    try:
        ws = get_book().worksheet(_simp_sheet_name(year))
        df = pd.DataFrame(_ws_get_all_records(ws))
    except Exception:
        return pd.DataFrame(columns=COMP_SIMPLE_HEADERS)
    if df.empty:
        return df
    df = df[df["평가자사번"].astype(str) == str(sabun)]
    sort_cols = [c for c in ["평가대상사번", "평가일자", "제출시각"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True, False, False])
    return df.reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# Tab UI
# ═════════════════════════════════════════════════════════════════════════════
def tab_competency(emp_df: pd.DataFrame):
    """직무능력평가 입력 탭 (관리자/권한자 전용)."""
    # ── 접근 권한 ───────────────────────────────────────────────────────────
    u_check = st.session_state.get("user", {})
    me_check = str(u_check.get("사번", ""))
    am_admin_or_mgr = (is_admin(me_check) or len(get_allowed_sabuns(emp_df, me_check, include_self=False)) > 0)
    if not am_admin_or_mgr:
        st.warning("권한이 없습니다. 관리자/평가 권한자만 접근할 수 있습니다.", icon="🔒")
        return

    # ── 연도 & 대상자 선택 ─────────────────────────────────────────────────
    this_year = current_year()
    year = st.number_input("연도", min_value=2000, max_value=2100, value=int(this_year), step=1, key="cmpS_year")

    u = st.session_state.get("user", {})
    me_sabun = str(u.get("사번", "")); me_name = str(u.get("이름", ""))

    allowed = set(map(str, get_allowed_sabuns(emp_df, me_sabun, include_self=True)))
    df = emp_df.copy()

    if "사번" not in df.columns:
        st.info("직원 데이터에 '사번' 컬럼이 없습니다.", icon="ℹ️")
        return

    df["사번"] = df["사번"].astype(str)
    df = df[df["사번"].isin(allowed)].copy()
    if "재직여부" in df.columns:
        df = df[df["재직여부"] == True]

    for c in ["이름", "부서1", "부서2", "직급"]:
        if c not in df.columns:
            df[c] = ""

    try:
        df["사번_sort"] = df["사번"].astype(int)
    except Exception:
        df["사번_sort"] = df["사번"].astype(str)

    df = df.sort_values(["사번_sort", "이름"]).reset_index(drop=True)

    glob_sab, _ = get_global_target()
    default_sab = glob_sab if glob_sab in set(df["사번"].astype(str)) else (me_sabun if me_sabun in set(df["사번"]) else (df["사번"].astype(str).tolist()[0] if not df.empty else ""))

    sabuns = df["사번"].astype(str).tolist()
    names  = df["이름"].astype(str).tolist()
    d2s    = df["부서2"].astype(str).tolist() if "부서2" in df.columns else [""] * len(sabuns)
    opts   = [f"{s} - {n} - {d2}" for s, n, d2 in zip(sabuns, names, d2s)]

    prev_sel = st.session_state.get("cmpS_target_sabun", default_sab)
    idx_prev = (1 + sabuns.index(prev_sel)) if prev_sel in sabuns else 0

    sel_label = st.selectbox("대상자 선택", ["(선택)"] + opts, index=idx_prev, key="cmpS_pick_select")
    if sel_label == "(선택)":
        st.session_state["cmpS_target_sabun"] = ""
        st.session_state["cmpS_target_name"] = ""
        st.info("대상자를 선택하세요.", icon="👈")
        return

    sel_sab = sel_label.split(" - ", 1)[0] if isinstance(sel_label, str) else (sabuns[0] if sabuns else "")
    st.session_state["cmpS_target_sabun"] = str(sel_sab)
    st.session_state["cmpS_target_name"] = _emp_name_by_sabun(emp_df, str(sel_sab))

    st.success(f"대상자: {_emp_name_by_sabun(emp_df, sel_sab)} ({sel_sab})", icon="✅")

    # ── 제출시각 배너 & 잠금 ────────────────────────────────────────────────
    comp_locked = False
    try:
        _cmap = get_comp_summary_map_cached(int(year), st.session_state.get("comp_rev", 0))
        _cts = (str(_cmap.get(str(sel_sab), ("", "", "", ""))[3]).strip())
        show_submit_banner(f"🕒 제출시각  |  {_cts if _cts else '미제출'}")
        comp_locked = bool(_cts)
    except Exception:
        pass

    # ── JD 요약 (스크롤 박스) ──────────────────────────────────────────────
    with st.expander("직무기술서 요약", expanded=True):
        jd = _jd_latest_for_comp(sel_sab, int(year))
        if jd:
            def V(key): 
                try:
                    from html import escape as _html_escape
                    return (_html_escape((jd.get(key, "") or "").strip()) or "—")
                except Exception:
                    return (str(jd.get(key, "") or "").strip() or "—")
            html = f"""
            <div class="scrollbox">
              <div class="kv"><div class="k">직무명</div><div class="v">{V('직무명')}</div></div>
              <div class="kv"><div class="k">직무개요</div><div class="v">{_jd_plain_html(jd.get('직무개요', '') or '—')}</div></div>
              <div class="kv"><div class="k">주요 업무</div><div class="v">{_jd_plain_html(jd.get('주업무', '') or '—')}</div></div>
              <div class="kv"><div class="k">기타업무</div><div class="v">{_jd_plain_html(jd.get('기타업무', '') or '—')}</div></div>
              <div class="kv"><div class="k">필요학력 / 전공</div><div class="v">{V('필요학력')} / {V('전공계열')}</div></div>
              <div class="kv"><div class="k">면허 / 경력(자격요건)</div><div class="v">{V('면허')} / {V('경력(자격요건)')}</div></div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
        else:
            st.caption("직무기술서가 없습니다. JD 없이도 평가를 진행할 수 있습니다.")

    # ── 평가 입력 ───────────────────────────────────────────────────────────
    st.markdown("### 평가 입력")
    grade_options = ["우수", "양호", "보통", "미흡"]
    colG = st.columns(4)
    with colG[0]: g_main  = st.radio("주업무 평가", grade_options, index=2, key="cmpS_main",  horizontal=False, disabled=comp_locked)
    with colG[1]: g_extra = st.radio("기타업무 평가", grade_options, index=2, key="cmpS_extra", horizontal=False, disabled=comp_locked)
    with colG[2]: qual    = st.radio("직무 자격 유지 여부", ["직무 유지","직무 변경","직무비부여"], index=0, key="cmpS_qual", disabled=comp_locked)
    with colG[3]:
        eval_date = ""  # 입력란 제거: 제출시각(KST) 날짜로 자동 기록

    try:
        edu_status = _edu_completion_from_jd(_jd_latest_for_comp(sel_sab, int(year)))
    except Exception:
        edu_status = "미완료"
    st.metric("교육이수 (자동)", edu_status)

    opinion = st.text_area("종합평가 의견", value="", height=150, key="cmpS_opinion", disabled=comp_locked)

    # ── 제출 확인(체크 + PIN) ──────────────────────────────────────────────
    cb1, cb2 = st.columns([2, 1])
    with cb1:
        comp_attest_ok = st.checkbox(
            "본인은 입력한 직무능력평가 내용이 사실이며, 회사 정책에 따라 제출함을 확인합니다.",
            key=f"comp_attest_ok_{year}_{sel_sab}_{me_sabun}",
        )
    with cb2:
        comp_pin_input = st.text_input("PIN 재입력", value="", type="password", key=f"comp_attest_pin_{year}_{sel_sab}_{me_sabun}")

    cbtn = st.columns([1, 1, 3])
    with cbtn[0]:
        do_save  = st.button("제출/저장", type="primary", use_container_width=True, key="cmpS_save",  disabled=comp_locked)
    with cbtn[1]:
        do_reset = st.button("초기화",     use_container_width=True,               key="cmpS_reset")

    if do_reset:
        for k in ["cmpS_main", "cmpS_extra", "cmpS_qual", "cmpS_opinion"]:
            st.session_state.pop(k, None)
        st.rerun()

    if do_save:
        if not comp_attest_ok:
            st.error("제출 전에 확인란에 체크해주세요.")
        elif not verify_pin(me_sabun, comp_pin_input):
            st.error("PIN이 올바르지 않습니다.")
        else:
            rep = upsert_comp_simple_response(
                emp_df, int(year), str(sel_sab), str(me_sabun),
                g_main, g_extra, qual, opinion, eval_date
            )
            st.success(("제출 완료" if rep.get("action") == "insert" else "업데이트 완료"), icon="✅")
            st.session_state["comp_rev"] = st.session_state.get("comp_rev", 0) + 1
            st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# 관리자 섹션 — 직원/ PIN / 평가항목 / 권한 관리 (성능 최적화 & 주석 표준화)
# ═════════════════════════════════════════════════════════════════════════════
# 이 파일은 기존 "관리자" 블럭을 통째로 교체할 수 있는 드롭인입니다.
# - PIN 저장/초기화: 항상 배치 쓰기 사용
# - 직원 저장: 다건 변경을 단일 values_batch_update로 처리
# - 헤더 보강: AUTO_FIX_HEADERS 정책 준수, 중복 if 제거
# - SettingWithCopyWarning 회피: .copy() 사용
# 주의: 본 파일은 상위 모듈의 유틸( _ws, _hdr, _retry, _ws_values, get_book, ensure_eval_items_sheet,
#       read_eval_items_df, _ensure_capacity, gs_enqueue_cell, gs_flush 등)을 사용합니다.

# ─────────────────────────────────────────────────────────────────────────────
# 필수 컬럼
# ─────────────────────────────────────────────────────────────────────────────
REQ_EMP_COLS = [
    "사번","이름","부서1","부서2","직급","직무","직군","입사일","퇴사일",
    "기타1","기타2","재직여부","적용여부","PIN_hash","PIN_No"
]

# ─────────────────────────────────────────────────────────────────────────────
# 내부 유틸
# ─────────────────────────────────────────────────────────────────────────────
def _get_ws_and_headers(sheet_name: str):
    ws = _ws(sheet_name)
    header, hmap = _hdr(ws, sheet_name)
    if not header:
        raise RuntimeError(f"'{sheet_name}' 헤더(1행) 없음")
    return ws, header, hmap


def ensure_emp_sheet_columns():
    """직원 시트의 필수 컬럼을 보장. AUTO_FIX_HEADERS=True면 자동 보강."""
    ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
    need = [c for c in REQ_EMP_COLS if c not in header]
    if need:
        if AUTO_FIX_HEADERS:
            _retry(ws.update, "1:1", [header + need])
            # 보강 후 최신 헤더 재조회
            ws, header, hmap = _get_ws_and_headers(EMP_SHEET)
        else:
            try:
                st.warning(
                    "직원 시트 헤더에 다음 컬럼이 없습니다: " + ", ".join(need) +
                    "\n→ 시트를 직접 수정한 뒤 좌측 🔄 동기화 버튼을 눌러주세요.", icon="⚠️"
                )
            except Exception:
                pass
    return ws, header, hmap


def _find_row_by_sabun(ws, hmap, sabun: str) -> int:
    """사번으로 행 인덱스(1‑based)를 찾음. 못 찾으면 0."""
    c = hmap.get("사번")
    if not c:
        return 0
    vals = _retry(ws.col_values, c)[1:]  # 2행부터
    s = str(sabun).strip()
    for i, v in enumerate(vals, start=2):
        if str(v).strip() == s:
            return i
    return 0


def _ws_batch_rows(ws, hmap, row_payloads):
    """여러 행에 대한 부분 갱신을 단일 values_batch_update로 처리.
    row_payloads: Iterable[(row_idx:int, kv:dict[colName->value])]
    """
    updates = []
    max_row = 0
    max_col = 0
    title = getattr(ws, "title", "")
    for row_idx, kv in row_payloads:
        if not kv:
            continue
        max_row = max(max_row, int(row_idx))
        for k, v in kv.items():
            c = hmap.get(k)
            if not c:
                continue
            cc = int(c)
            max_col = max(max_col, cc)
            a1 = gspread.utils.rowcol_to_a1(int(row_idx), cc)
            updates.append({"range": f"'{title}'!{a1}", "values": [[v]]})

    if not updates:
        return 0

    # 안전하게 그리드 확장
    try:
        _ensure_capacity(ws, max_row, max_col)
    except Exception:
        pass

    body = {"valueInputOption": "USER_ENTERED", "data": updates}
    _retry(ws.spreadsheet.values_batch_update, body)
    return len(updates)

# ═════════════════════════════════════════════════════════════════════════════
# 직원 관리
# ═════════════════════════════════════════════════════════════════════════════
def tab_staff_admin(emp_df: pd.DataFrame):
    """직원 시트 편집: 드롭다운 + 체크박스 저장(변경분만 일괄 갱신)."""
    # 1) 시트/헤더 확보
    ws, header, hmap = ensure_emp_sheet_columns()
    view = emp_df.copy()

    # 2) 민감 컬럼 숨기기
    for c in ["PIN_hash", "PIN_No"]:
        view = view.drop(columns=[c], errors="ignore")

    st.write(f"결과: **{len(view):,}명**")

    # 3) 드롭다운 옵션(직원 시트 유니크)
    try:
        dept1_options = [""] + sorted({
            str(x).strip() for x in emp_df.get("부서1", pd.Series(dtype=str)).dropna().unique().tolist()
            if str(x).strip()
        })
    except Exception:
        dept1_options = [""]
    try:
        dept2_options = [""] + sorted({
            str(x).strip() for x in emp_df.get("부서2", pd.Series(dtype=str)).dropna().unique().tolist()
            if str(x).strip()
        })
    except Exception:
        dept2_options = [""]

    # 4) 에디터
    colcfg = {
        "사번": st.column_config.TextColumn("사번", disabled=True),
        "이름": st.column_config.TextColumn("이름"),
        "부서1": st.column_config.SelectboxColumn("부서1", options=dept1_options),
        "부서2": st.column_config.SelectboxColumn("부서2", options=dept2_options),
        "직급": st.column_config.TextColumn("직급"),
        "직무": st.column_config.TextColumn("직무"),
        "직군": st.column_config.TextColumn("직군"),
        "입사일": st.column_config.TextColumn("입사일"),
        "퇴사일": st.column_config.TextColumn("퇴사일"),
        "기타1": st.column_config.TextColumn("기타1"),
        "기타2": st.column_config.TextColumn("기타2"),
        "재직여부": st.column_config.CheckboxColumn("재직여부"),
        "적용여부": st.column_config.CheckboxColumn("적용여부"),
    }

    edited = st.data_editor(
        view,
        use_container_width=True,
        height=560,
        hide_index=True,
        num_rows="fixed",
        column_config=colcfg,
    )

    # 5) 저장(변경된 칼럼만 일괄 갱신)
    if st.button("변경사항 저장", type="primary", use_container_width=True):
        try:
            before = view.set_index("사번")
            after  = edited.set_index("사번")

            # 안전장치: 빈 키 제거
            before = before[before.index.astype(str) != ""]
            after  = after[after.index.astype(str) != ""]

            payloads = []  # (row_idx, {col:value})
            change_rows = 0

            for sabun in after.index:
                if sabun not in before.index:
                    continue  # num_rows="fixed" 환경에서는 거의 없음

                row_payload = {}
                for c in after.columns:
                    if c not in before.columns:
                        continue
                    v0 = before.loc[sabun, c]
                    v1 = after.loc[sabun, c]
                    if str(v0) != str(v1):
                        if c in ("재직여부", "적용여부"):
                            row_payload[c] = bool(v1)
                        else:
                            row_payload[c] = v1

                if not row_payload:
                    continue

                row_idx = _find_row_by_sabun(ws, hmap, str(sabun))
                if row_idx > 0:
                    payloads.append((row_idx, row_payload))
                    change_rows += 1

            if payloads:
                _ws_batch_rows(ws, hmap, payloads)

            try:
                st.cache_data.clear()
            except Exception:
                pass

            st.success(f"저장 완료: {change_rows}명 반영", icon="✅")
        except Exception as e:
            st.exception(e)

# ═════════════════════════════════════════════════════════════════════════════
# PIN 관리
# ═════════════════════════════════════════════════════════════════════════════
def reissue_pin_inline(sabun: str, length: int = 4):
    """사번의 PIN을 재발급하고 즉시 저장(배치 쓰기). 반환: 평문 PIN / 해시."""
    ws, header, hmap = ensure_emp_sheet_columns()
    if "PIN_hash" not in hmap or "PIN_No" not in hmap:
        raise RuntimeError("PIN_hash/PIN_No 필요")

    row_idx = _find_row_by_sabun(ws, hmap, str(sabun))
    if row_idx == 0:
        raise RuntimeError("사번을 찾지 못했습니다.")

    pin = "".join(pysecrets.choice("0123456789") for _ in range(length))
    ph  = _pin_hash(pin, str(sabun))

    # ✅ 항상 배치 쓰기
    gs_enqueue_cell(ws, row_idx, hmap["PIN_hash"], ph)
    gs_enqueue_cell(ws, row_idx, hmap["PIN_No"],   pin)
    gs_flush()

    try:
        st.cache_data.clear()
    except Exception:
        pass
    return {"PIN_No": pin, "PIN_hash": ph}


def tab_admin_pin(emp_df):
    """관리자 PIN 저장/초기화 — 배치 쓰기 고정."""
    ws, header, hmap = ensure_emp_sheet_columns()
    df = emp_df.copy()

    # 적용여부 필터
    if "적용여부" in df.columns:
        df = df[df["적용여부"] == True].copy()

    # 표시용 라벨
    df["표시"] = df.apply(lambda r: f"{str(r.get('사번',''))} - {str(r.get('이름',''))}", axis=1)
    if "사번" in df.columns:
        df = df.sort_values(["사번"])

    sel = st.selectbox(
        "직원 선택(사번 - 이름)",
        ["(선택)"] + df.get("표시", pd.Series(dtype=str)).tolist(),
        index=0,
        key="adm_pin_pick"
    )
    if sel == "(선택)":
        return

    sabun = sel.split(" - ", 1)[0]
    row   = df.loc[df["사번"].astype(str) == str(sabun)].iloc[0]
    st.write(f"사번: **{sabun}** / 이름: **{row.get('이름','')}**")

    pin1 = st.text_input("새 PIN (숫자)", type="password", key="adm_pin1")
    pin2 = st.text_input("새 PIN 확인", type="password", key="adm_pin2")

    col = st.columns([1, 1, 2])
    with col[0]:
        do_save = st.button("PIN 저장/변경", type="primary", use_container_width=True, key="adm_pin_save")
    with col[1]:
        do_clear = st.button("PIN 비우기", use_container_width=True, key="adm_pin_clear")

    # 공통 컬럼 체크
    if "PIN_hash" not in hmap or "PIN_No" not in hmap:
        st.error(f"'{EMP_SHEET}' 시트에 PIN_hash/PIN_No가 없습니다.")
        return

    # 대상 행 찾기
    r = _find_row_by_sabun(ws, hmap, sabun)
    if r == 0:
        st.error("시트에서 사번을 찾지 못했습니다.")
        return

    # 저장(변경): 배치 쓰기
    if do_save:
        if not pin1 or not pin2:
            st.error("PIN을 두 번 모두 입력하세요."); return
        if pin1 != pin2:
            st.error("PIN 확인이 일치하지 않습니다."); return
        if not pin1.isdigit():
            st.error("PIN은 숫자만 입력하세요."); return
        if not _to_bool(row.get("재직여부", False)):
            st.error("퇴직자는 변경할 수 없습니다."); return

        hashed = _pin_hash(pin1.strip(), str(sabun))
        gs_enqueue_cell(ws, r, hmap["PIN_hash"], hashed)
        gs_enqueue_cell(ws, r, hmap["PIN_No"],   pin1.strip())
        gs_flush()

        st.cache_data.clear()
        st.success("PIN 저장 완료", icon="✅")
        st.session_state.pop("adm_pin1", None); st.session_state.pop("adm_pin2", None)
        st.rerun()

    # 초기화(비우기): 배치 쓰기
    if do_clear:
        gs_enqueue_cell(ws, r, hmap["PIN_hash"], "")
        gs_enqueue_cell(ws, r, hmap["PIN_No"],   "")
        gs_flush()

        st.cache_data.clear()
        st.success("PIN 초기화 완료", icon="🧹")
        st.session_state.pop("adm_pin1", None); st.session_state.pop("adm_pin2", None)
        st.rerun()

# ═════════════════════════════════════════════════════════════════════════════
# 평가 항목 관리
# ═════════════════════════════════════════════════════════════════════════════
def tab_admin_eval_items():
    df = read_eval_items_df(only_active=False).copy()
    for c in ["항목ID", "항목", "내용", "비고"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
    if "순서" in df.columns:
        df["순서"] = pd.to_numeric(df["순서"], errors="coerce").fillna(0).astype(int)
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(lambda x: str(x).strip().lower() in ("true","1","y","yes","t"))

    st.write(f"현재 등록: **{len(df)}개** (활성 {df[df.get('활성', False)==True].shape[0]}개)")

    # ── 목록 보기 / 일괄 편집 ──────────────────────────────────────────────
    with st.expander("목록 보기 / 순서 일괄 편집", expanded=True):
        edit_df = df[["항목ID","항목","순서","활성"]].copy().reset_index(drop=True)
        edited = st.data_editor(
            edit_df, use_container_width=True, height=420, hide_index=True,
            column_order=["항목ID","항목","순서","활성"],
            column_config={
                "항목ID": st.column_config.TextColumn(disabled=True),
                "항목":   st.column_config.TextColumn(disabled=True),
                "활성":   st.column_config.CheckboxColumn(),
                "순서":   st.column_config.NumberColumn(step=1, min_value=0),
            },
        )

        if st.button("순서 일괄 저장", type="primary", use_container_width=True):
            try:
                ws = get_book().worksheet(EVAL_ITEMS_SHEET)
                header = _retry(ws.row_values, 1) or []
                hmap   = {n: i+1 for i, n in enumerate(header)}

                # 기본 컬럼 확인
                col_id  = hmap.get("항목ID")
                col_ord = hmap.get("순서")
                col_act = hmap.get("활성")
                if not (col_id and col_ord):
                    st.error("'항목ID' 또는 '순서' 헤더가 없습니다."); st.stop()

                # 현재 시트의 항목ID 목록(2행부터)
                id_vals = _retry(ws.col_values, col_id)[1:]
                n = len(id_vals)

                # 편집 결과 매핑
                def _to_bool_local(x):
                    if isinstance(x, bool): return x
                    if x is None: return False
                    s = str(x).strip().lower()
                    return s in ("1","y","yes","true","t","on","checked")

                edited_map_order  = { str(r["항목ID"]).strip(): int(r["순서"])  for _, r in edited.iterrows() }
                edited_map_active = { str(r["항목ID"]).strip(): _to_bool_local(r["활성"]) for _, r in edited.iterrows() } if "활성" in edited.columns else {}

                # 컬럼 범위 문자열
                import re as _re_local
                def _col_range(col_idx: int, start_row: int, end_row: int) -> str:
                    letters = _re_local.match(r"([A-Z]+)", gspread.utils.rowcol_to_a1(1, col_idx)).group(1)
                    return f"{letters}{start_row}:{letters}{end_row}"

                # 일괄 덮어쓰기
                if n > 0:
                    order_values  = [[ int(edited_map_order.get(iid, 0)) ] for iid in id_vals ]
                    _retry(ws.update, _col_range(col_ord, 2, n+1), order_values, value_input_option="USER_ENTERED")

                    if col_act:
                        active_values = [[ bool(edited_map_active.get(iid, False)) ] for iid in id_vals ]
                        _retry(ws.update, _col_range(col_act, 2, n+1), active_values, value_input_option="USER_ENTERED")

                st.success("업데이트 완료", icon="✅")
            except Exception as e:
                st.exception(e)

    # ── 신규 등록 / 수정 ───────────────────────────────────────────────────
    st.divider()
    st.markdown("### 신규 등록 / 수정")
    choices = ["(신규)"] + ([f"{r['항목ID']} - {r['항목']}" for _, r in df.iterrows()] if not df.empty else [])
    sel = st.selectbox("대상 선택", choices, index=0, key="adm_eval_pick")

    item_id = None
    name = ""; desc = ""; memo = ""
    order = int(df["순서"].max()+1) if ("순서" in df.columns and not df.empty) else 1
    active = True

    if sel != "(신규)" and not df.empty:
        iid = sel.split(" - ", 1)[0]
        row = df.loc[df["항목ID"] == iid]
        if not row.empty:
            row = row.iloc[0]
            item_id = str(row.get("항목ID",""))
            name    = str(row.get("항목",""))
            desc    = str(row.get("내용",""))
            memo    = str(row.get("비고",""))
            try:    order   = int(row.get("순서",0) or 0)
            except Exception: order = 0
            active = (str(row.get("활성","")).strip().lower() in ("true","1","y","yes","t"))

    c1, c2 = st.columns([3,1])
    with c1:
        name = st.text_input("항목명", value=name, key="adm_eval_name")
        desc = st.text_area("설명(문항 내용)", value=desc, height=100, key="adm_eval_desc")
        memo = st.text_input("비고(선택)", value=memo, key="adm_eval_memo")
    with c2:
        order  = st.number_input("순서", min_value=0, step=1, value=int(order), key="adm_eval_order")
        active = st.checkbox("활성", value=bool(active), key="adm_eval_active")

        if st.button("저장(신규/수정)", type="primary", use_container_width=True, key="adm_eval_save_v3"):
            if not name.strip():
                st.error("항목명을 입력하세요.")
            else:
                try:
                    ensure_eval_items_sheet()
                    ws     = get_book().worksheet(EVAL_ITEMS_SHEET)
                    header = _retry(ws.row_values, 1) or EVAL_ITEM_HEADERS
                    hmap   = {n: i + 1 for i, n in enumerate(header)}

                    if not item_id:
                        # 신규
                        col_id = hmap.get("항목ID"); nums = []
                        if col_id:
                            vals = _retry(ws.col_values, col_id)[1:]
                            for v in vals:
                                s = str(v).strip()
                                if s.startswith("ITM"):
                                    try: nums.append(int(s[3:]))
                                    except Exception: pass
                        new_id = f"ITM{((max(nums)+1) if nums else 1):04d}"

                        rowbuf = [""] * len(header)
                        def put(k, v):
                            c = hmap.get(k)
                            if c: rowbuf[c-1] = v
                        put("항목ID", new_id)
                        put("항목",   name.strip())
                        put("내용",   desc.strip())
                        put("순서",   int(order))
                        put("활성",   bool(active))
                        if "비고" in hmap: put("비고", memo.strip())

                        _retry(ws.append_row, rowbuf, value_input_option="USER_ENTERED")
                        st.cache_data.clear()
                        st.success(f"저장 완료 (항목ID: {new_id})")
                        st.rerun()
                    else:
                        # 수정 — ✅ 배치 쓰기로 변경
                        col_id = hmap.get("항목ID"); idx = 0
                        if col_id:
                            vals = _retry(ws.col_values, col_id)
                            for i, v in enumerate(vals[1:], start=2):
                                if str(v).strip() == str(item_id).strip():
                                    idx = i; break
                        if idx == 0:
                            st.error("대상 항목을 찾을 수 없습니다.")
                        else:
                            payload = {
                                "항목": name.strip(),
                                "내용": desc.strip(),
                                "순서": int(order),
                                "활성": bool(active),
                            }
                            if "비고" in hmap:
                                payload["비고"] = memo.strip()

                            _ws_batch_rows(ws, hmap, [(idx, payload)])
                            st.success("업데이트 완료", icon="✅")
                except Exception as e:
                    st.exception(e)

# ═════════════════════════════════════════════════════════════════════════════
# 권한(ACL) 관리
# ═════════════════════════════════════════════════════════════════════════════
def tab_admin_acl(emp_df: pd.DataFrame):
    """권한 관리(간소/고속): 편집 시 세션 DF 정규화로 NaN/None 문제 방지."""
    me = st.session_state.get("user", {})
    am_admin = is_admin(str(me.get("사번","")))
    if not am_admin:
        st.error("Master만 저장할 수 있습니다. (표/저장 모두 비활성화)", icon="🛡️")

    # 직원 라벨/룩업
    base = emp_df[["사번","이름","부서1","부서2"]].copy() if not emp_df.empty else pd.DataFrame(columns=["사번","이름","부서1","부서2"])
    base["사번"] = base["사번"].astype(str).str.strip()
    emp_lookup = {str(r["사번"]).strip(): str(r.get("이름","")).strip() for _, r in base.iterrows()}
    sabuns = sorted(emp_lookup.keys())
    labels, label_by_sabun, sabun_by_label = [], {}, {}
    for s in sabuns:
        nm = emp_lookup[s]
        lab = f"{s} - {nm}" if nm else s
        labels.append(lab)
        label_by_sabun[s] = lab
        sabun_by_label[lab] = s

    # 표시용 DF 정규화 유틸
    def _normalize_acl_display_df(df_in: pd.DataFrame) -> pd.DataFrame:
        headers = globals().get("AUTH_HEADERS", ["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"])
        df = (df_in.copy() if df_in is not None else pd.DataFrame(columns=headers))

        # 누락 컬럼 채우기
        for c in headers:
            if c not in df.columns:
                df[c] = "" if c != "활성" else False

        # 타입/결측값 정리
        str_cols = [c for c in headers if c != "활성"]
        for c in str_cols:
            df[c] = df[c].astype(object).where(pd.notna(df[c]), "")
        if "활성" in df.columns:
            def _to_b(x):
                if isinstance(x, bool): return x
                s = str(x).strip().lower()
                return s in ("true","1","y","yes","t","on","checked")
            df["활성"] = df["활성"].map(_to_b).fillna(False).astype(bool)

        # 완전 빈 행 제거
        def _row_empty(r):
            return (
                str(r.get("사번","")).strip()=="" and
                str(r.get("역할","")).strip()=="" and
                str(r.get("범위유형","")).strip()=="" and
                str(r.get("부서1","")).strip()=="" and
                str(r.get("부서2","")).strip()=="" and
                str(r.get("대상사번","")).strip()=="" and
                str(r.get("비고","")).strip()=="" and
                (bool(r.get("활성", False)) is False)
            )
        if len(df) > 0:
            df = df[~df.apply(_row_empty, axis=1)].reset_index(drop=True)

        # '사번'을 라벨형태("사번 - 이름")로 정규화
        if "사번" in df.columns:
            def _labelize(v):
                s = str(v).strip()
                if s in sabun_by_label:
                    return s
                if " - " in s:
                    return s
                return label_by_sabun.get(s, s)
            df["사번"] = df["사번"].map(_labelize)

        # '이름' 파생
        if "이름" in df.columns:
            df["이름"] = df["사번"].map(lambda lab: emp_lookup.get(sabun_by_label.get(str(lab).strip(), str(lab).split(" - ",1)[0].strip()), "")).fillna("").astype(str)

        # 컬럼 순서 맞추기
        for c in headers:
            if c not in df.columns:
                df[c] = "" if c != "활성" else False
        return df[headers].copy()

    # 최초 로드: 시트 → 세션
    if "acl_df" not in st.session_state:
        try:
            ws = get_book().worksheet(AUTH_SHEET)
            header = _retry(ws.row_values, 1) or AUTH_HEADERS
            vals = _retry(ws.get_all_values) or []
            rows = vals[1:] if len(vals) > 1 else []
            raw_df = pd.DataFrame(rows, columns=header).fillna("")
        except Exception:
            header = AUTH_HEADERS
            raw_df = pd.DataFrame(columns=header)
        disp_df = _normalize_acl_display_df(raw_df)
        st.session_state["acl_header"] = header
        st.session_state["acl_df"] = disp_df

    header = st.session_state["acl_header"]
    work = _normalize_acl_display_df(st.session_state.get("acl_df", pd.DataFrame(columns=header)))

    # 드롭다운 옵션(부서)
    dept1_options = [""] + sorted({str(x).strip() for x in base.get("부서1", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()})
    dept2_options = [""] + sorted({str(x).strip() for x in base.get("부서2", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()})

    # 에디터 구성 ('이름'은 편집 제외)
    column_config = {
        "사번": st.column_config.SelectboxColumn("사번 - 이름", options=labels),
        "역할": st.column_config.SelectboxColumn("역할", options=["admin","manager"]),
        "범위유형": st.column_config.SelectboxColumn("범위유형", options=["","부서","개별"]),
        "부서1": st.column_config.SelectboxColumn("부서1", options=dept1_options),
        "부서2": st.column_config.SelectboxColumn("부서2", options=dept2_options),
        "대상사번": st.column_config.TextColumn("대상사번"),
        "활성": st.column_config.CheckboxColumn("활성"),
        "비고": st.column_config.TextColumn("비고"),
    }
    edit_cols = [c for c in header if c != "이름" and c in work.columns]
    edited = st.data_editor(
        work[edit_cols],
        key="acl_editor",
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        height=520,
        disabled=not am_admin,
        column_config=column_config,
    )

    # 변경 감지 → 정규화 후 세션 반영
    try:
        if not edited.equals(work[edit_cols]):
            new_df = _normalize_acl_display_df(edited)
            new_df["이름"] = new_df["사번"].map(lambda lab: emp_lookup.get(sabun_by_label.get(str(lab).strip(), str(lab).split(" - ",1)[0].strip()), "")).fillna("").astype(str)
            st.session_state["acl_df"] = new_df[[c for c in work.columns if c in new_df.columns]].copy()
    except Exception:
        st.session_state["acl_df"] = _normalize_acl_display_df(edited)

    # 저장 버튼(전체 반영: 헤더+모든 행 덮어쓰기)
    if st.button("권한 전체 반영", type="primary", use_container_width=True, disabled=not am_admin):
        try:
            ws = get_book().worksheet(AUTH_SHEET)
            header = [*st.session_state.get("acl_header", AUTH_HEADERS)] or AUTH_HEADERS

            # 1) 헤더 덮어쓰기
            _retry(ws.update, "1:1", [header], value_input_option="USER_ENTERED")

            # 2) 저장용 DF: 라벨→실사번, 이름 파생
            save_df = _normalize_acl_display_df(st.session_state.get("acl_df", pd.DataFrame(columns=header))).copy()

            def _sab_from_label(v: str):
                s = str(v).strip()
                return sabun_by_label.get(s) or (s.split(" - ", 1)[0].strip() if " - " in s else s)

            if "사번" in save_df.columns:
                save_df["사번"] = save_df["사번"].map(_sab_from_label)

            if "이름" not in save_df.columns:
                save_df.insert(1, "이름", "")
            save_df["이름"] = save_df["사번"].map(lambda s: emp_lookup.get(str(s).strip(), "")).fillna("").astype(str)

            for col in header:
                if col not in save_df.columns:
                    save_df[col] = "" if col != "활성" else False
            save_df = save_df[header]

            if "활성" in save_df.columns:
                def _to_bool_local(x):
                    if isinstance(x, bool): return x
                    s = str(x).strip().lower()
                    return s in ("true","1","y","yes","t","on","checked")
                save_df["활성"] = save_df["활성"].map(_to_bool_local).fillna(False).astype(bool)

            # 완전 빈 행 제거
            save_df = save_df[save_df.astype(str).apply(lambda r: "".join(r.values).strip() != "", axis=1)]

            # 3) 본문 덮어쓰기
            data = save_df.fillna("").values.tolist()
            try:
                _ensure_capacity(ws, (len(data) + 1) if data else 1, max(1, len(header)))
                if data:
                    _retry(ws.update, "A2", data, value_input_option="USER_ENTERED")
                _retry(ws.resize, rows=max(1, len(data) + 1))
            except Exception:
                if data:
                    _retry(ws.update, "A2", data, value_input_option="USER_ENTERED")

            st.success(f"업데이트 완료: {len(data)}행", icon="✅")
        except Exception as e:
            st.exception(e)

# ═════════════════════════════════════════════════════════════════════════════
# 도움말
# ═════════════════════════════════════════════════════════════════════════════
def tab_help():
    st.markdown("""
**도움말**
- 좌측에서 `검색(사번/이름)` 후 **Enter** → 첫 번째 결과가 자동으로 선택됩니다.
- 대상선택(드롭다운박스)로 직원을 선택해도 됩니다.
- 선택된 직원은 우측 모든 탭과 동기화됩니다.
- 권한(ACL)에 따라 보이는 직원 범위가 달라집니다. 관리자는 전 직원이 보입니다.
- 로그인: `사번` 입력 후 **Enter** → `PIN` 포커스 / `PIN` 입력 후 **Enter** → 로그인.
- 인사평가: 평가 항목은 관리자 메뉴의 **평가 항목 관리**에서 활성/순서를 조정합니다.
- 직무기술서/직무능력평가: 동기화된 대상자를 기준으로 편집·제출합니다.
- PIN/평가항목/권한관리: 관리자 탭에서 처리합니다.
- 구글시트 구조
    - 직원: `직원` 시트
    - 권한: `권한` 시트 (역할=admin/manager, 범위유형: 공란=전체 · 부서 · 개별)
    - 평가 항목: `평가_항목` 시트
    - 인사평가: `인사평가_YYYY` 시트
    - 직무기술서: `직무기술서` 시트
    - 직무기술서(부서장 승인): `직무기술서_승인` 시트
    - 직무능력평가: `직무능력평가_YYYY` 시트
""")

# ═════════════════════════════════════════════════════════════════════════════
# Main App (optimized) + Robust Batch Helpers
# ═════════════════════════════════════════════════════════════════════════════
# 드롭인 교체용: 본 블록을 기존 Main App/배치 헬퍼 부분과 교체하세요.
# - 성능: 불필요한 copy 제거, 배치 쓰기 헬퍼 강화(청크/그룹핑), 캐시 무효화 지점 최소화
# - 안정: gspread 429/네트워크 대비 _retry 경유, 예외 시 친절 경고
# - 일관: 주석/구획 표준화
#
# 의존: 앱 상단의 공용 유틸/상수/탭 함수가 이미 로드되어 있어야 합니다.
#       (예: read_emp_df, _session_valid, kst_now_str, logout, force_sync,
#        render_staff_picker_left, tab_eval, tab_job_desc, tab_competency,
#        tab_staff_admin, tab_admin_pin, tab_admin_eval_items, tab_admin_acl, tab_help,
#        is_admin, get_book, _retry, APP_TITLE, st, pd 등)
from typing import Iterable, Tuple, Dict, Any

try:
    from gspread.utils import rowcol_to_a1  # 이미 상단에서 임포트되어 있을 수 있음
except Exception:
    pass

# ═════════════════════════════════════════════════════════════════════════════
# Main App
# ═════════════════════════════════════════════════════════════════════════════
def main():
    _suppress_magic_booleans()

    emp_df = read_emp_df()
    st.session_state["emp_df"] = emp_df

    # ⬇️ 전역 토스트(쿨다운 중이면 "동기화" 버튼 위에 고정 표시, 아니면 제거)
    mount_sync_toast()

    # 로그인 유도
    if not _session_valid():
        st.markdown(f"<div class='app-title-hero'>{APP_TITLE}</div>", unsafe_allow_html=True)
        show_login(emp_df)
        return

    # 세션 소유자 정합성
    require_login(emp_df)

    # 레이아웃
    left, right = st.columns([1.35, 3.65], gap="large")

    # ────────────────────────────────────────────────────────────────────────
    # Left Pane
    # ────────────────────────────────────────────────────────────────────────
    with left:
        u = st.session_state.get("user", {}) or {}
        st.markdown(f"<div class='app-title-hero'>{APP_TITLE}</div>", unsafe_allow_html=True)
        st.caption(f"DB연결 {kst_now_str()}")
        st.markdown(f"- 사용자: **{u.get('이름','')} ({u.get('사번','')})**")

        c1, c2 = st.columns([1, 1], gap="small")
        with c1:
            if st.button("로그아웃", key="btn_logout", use_container_width=True):
                logout()
        with c2:
            # 버튼만 남김(토스트는 전역 mount_sync_toast가 처리)
            if st.button("🔄 동기화", key="sync_left", use_container_width=True,
                         help="캐시를 비우고 구글시트에서 다시 불러옵니다."):
                force_sync()

        # ⬇️ 반환값을 삼켜서 'False' 렌더링 방지
        _ = render_staff_picker_left(emp_df)

    # ────────────────────────────────────────────────────────────────────────
    # Right Pane
    # ────────────────────────────────────────────────────────────────────────
    with right:
        tabs = st.tabs(["인사평가", "직무기술서", "직무능력평가", "관리자", "도움말"])

        with tabs[0]:
            tab_eval(emp_df)

        with tabs[1]:
            tab_job_desc(emp_df)

        with tabs[2]:
            tab_competency(emp_df)

        with tabs[3]:
            me = str(st.session_state.get("user", {}).get("사번", ""))
            if not is_admin(me):
                st.warning("관리자 전용 메뉴입니다.", icon="🔒")
            else:
                a1, a2, a3, a4 = st.tabs(["직원", "PIN 관리", "평가 항목 관리", "권한 관리"])
                with a1: tab_staff_admin(emp_df)
                with a2: tab_admin_pin(emp_df)
                with a3: tab_admin_eval_items()
                with a4: tab_admin_acl(emp_df)

        with tabs[4]:
            tab_help()

if __name__ == "__main__":
    main()

# ═════════════════════════════════════════════════════════════════════════════
# PATCH 2025-10-17: robust get_jd_approval_map_cached (append-only)
# ═════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=120, show_spinner=False)
def get_jd_approval_map_cached(_year: int, _rev: int = 0) -> dict:
    """
    Robust version: 시트 비었거나 헤더 누락/타입 캐스팅 실패해도 안전.
    결과: {(사번, 버전)->(상태, 승인시각)}
    """
    sheet_name = globals().get("JD_APPROVAL_SHEET", "직무기술서_승인")
    default_headers = ["연도","사번","이름","버전","승인자사번","승인자이름","상태","승인시각","비고"]
    headers = globals().get("JD_APPROVAL_HEADERS", default_headers)

    # Ensure sheet 존재 보장(가능할 때만)
    try:
        ensure_fn = globals().get("ensure_jd_approval_sheet")
        if callable(ensure_fn):
            ensure_fn()
    except Exception:
        pass

    # Records 로드
    df = None
    try:
        _ws_func = globals().get("_ws")
        _get_records = globals().get("_ws_get_all_records")
        if callable(_ws_func) and callable(_get_records):
            ws = _ws_func(sheet_name)
            raw = _get_records(ws)
            df = pd.DataFrame(raw)
    except Exception:
        df = None

    if df is None:
        try:
            get_df = globals().get("get_sheet_as_df")
            if callable(get_df):
                df = get_df(sheet_name)
        except Exception:
            df = None

    if df is None or df.empty:
        df = pd.DataFrame(columns=headers)

    # 컬럼 보장
    for c in headers:
        if c not in df.columns:
            df[c] = ""

    # 타입 정리
    df["연도"] = pd.to_numeric(df["연도"], errors="coerce").fillna(0).astype(int)
    if "버전" in df.columns:
        df["버전"] = pd.to_numeric(df["버전"], errors="coerce").fillna(0).astype(int)
    else:
        df["버전"] = 0
    for c in ["사번","상태","승인시각"]:
        if c in df.columns:
            df[c] = df[c].astype(str)
        else:
            df[c] = ""

    # 연도 필터(안전)
    try:
        df = df[df["연도"] == int(_year)]
    except Exception:
        df = df.iloc[0:0]

    # 맵 구축
    out = {}
    if not df.empty:
        sort_cols = [c for c in ["사번","버전","승인시각"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=[True]*len(sort_cols), kind="stable").reset_index(drop=True)
        for _, rr in df.iterrows():
            k = (str(rr.get("사번","")), int(rr.get("버전",0)))
            out[k] = (str(rr.get("상태","")), str(rr.get("승인시각","")))
    return out

# ═════════════════════════════════════════════════════════════════════════════
# Robust Batch Write Helpers (Queue → values_batch_update)
# ═════════════════════════════════════════════════════════════════════════════
def _gs_queue_init():
    """세션 로컬 큐 초기화."""
    if "gs_queue" not in st.session_state:
        st.session_state.gs_queue = []

def gs_enqueue_range(ws, range_a1: str, values_2d, value_input_option: str = "USER_ENTERED"):
    """
    A1 범위 + 2D values를 큐에 적재.
    - range_a1: "'시트'!A1:B2" 또는 "A1:B2" (시트명 자동 보강)
    - values_2d: [[...], [...], ...]
    """
    _gs_queue_init()
    rng = range_a1 if "!" in range_a1 else f"{ws.title}!{range_a1}"
    st.session_state.gs_queue.append({
        "range": rng, "values": values_2d, "value_input_option": value_input_option
    })

def gs_enqueue_cell(ws, row: int, col: int, value, value_input_option: str = "USER_ENTERED"):
    """단일 셀 업데이트를 큐에 적재."""
    _gs_queue_init()
    try:
        a1 = rowcol_to_a1(int(row), int(col))
    except Exception:
        # Fallback: 최소 방어
        a1 = f"R{int(row)}C{int(col)}"
    rng = f"{ws.title}!{a1}"
    st.session_state.gs_queue.append({
        "range": rng, "values": [[value]], "value_input_option": value_input_option
    })

def gs_flush(max_ranges_per_call: int = 400):
    """
    큐 적재분을 Google Sheets values_batch_update로 전송.
    - value_input_option 별로 그룹핑 후, API 제한 고려해 CHUNK 전송
    - 오류 시 batch_update로 폴백 시도
    """
    q = st.session_state.get("gs_queue") or []
    if not q:
        return

    # value_input_option 별 그룹핑
    grouped: Dict[str, list] = {}
    for item in q:
        grouped.setdefault(item.get("value_input_option", "USER_ENTERED"), []).append({
            "range": item["range"],
            "values": item["values"]
        })

    sh = get_book()  # gspread.Spreadsheet
    sent = 0
    try:
        for mode, payload in grouped.items():
            # 청크 분할
            for i in range(0, len(payload), max_ranges_per_call):
                chunk = payload[i:i + max_ranges_per_call]
                body = {"valueInputOption": mode, "data": chunk}
                try:
                    _retry(sh.values_batch_update, body)
                except Exception:
                    # 폴백
                    _retry(sh.batch_update, body)
                sent += len(chunk)
    finally:
        st.session_state.gs_queue = []

    return sent
