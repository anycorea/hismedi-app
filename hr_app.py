# -*- coding: utf-8 -*-
"""
HISMEDI - 인사/HR (0920, 5 Tabs, cache tuned, Enter-sync, JD Summary scroll, JD tab restored)
- 메인 탭: 인사평가 / 직무기술서 / 직무능력평가 / 관리자 / 도움말
- 로그인: Enter(사번→PIN, PIN→로그인)
- 좌측 검색 Enter → 대상 선택 자동 동기화
- 캐시 TTL 확대(300~600), 자동 pip 설치 제거
- 직무능력평가 탭: "직무기술서 요약" 스크롤 영역(고정 높이)
- 직무기술서 탭: 편집/보기 기능 복원
"""

# ══════════════════════════════════════════════════════════════════════════════
# Imports
# ══════════════════════════════════════════════════════════════════════════════
import re, time, random, hashlib, secrets as pysecrets
from datetime import datetime, timedelta
from typing import Any, Tuple
import pandas as pd
import streamlit as st


def tab_admin_acl(emp_df: pd.DataFrame):
    st.markdown("### 권한 관리")
    # --- Define default headers (fallback) ---
    DEFAULT_AUTH_HEADERS = ["사번","이름","역할","범위유형","부서1","부서2","대상사번","활성","비고"]
    global AUTH_HEADERS
    if "AUTH_HEADERS" not in globals() or not isinstance(AUTH_HEADERS, list) or not AUTH_HEADERS:
        AUTH_HEADERS = DEFAULT_AUTH_HEADERS

    # --- Try to read sheet; never fail to render ---
    df = None
    read_err = None
    try:
        df = read_auth_df().copy()
    except Exception as e:
        read_err = e
        df = pd.DataFrame(columns=AUTH_HEADERS)

    # Normalize columns and ensure presence
    if df is None or df.empty:
        df = pd.DataFrame(columns=AUTH_HEADERS)
    for c in AUTH_HEADERS:
        if c not in df.columns:
            df[c] = ""
    # Types & maps
    role_opts = ["admin","manager","evaluator"]
    scope_opts = ["부서","개별"]
    emp_map = {}
    try:
        emp_map = {str(r["사번"]): str(r.get("이름","")) for _,r in emp_df.iterrows() if str(r.get("사번","")).strip()}
    except Exception:
        pass

    # Fill name column from 사번 when possible
    try:
        df["사번"] = df["사번"].astype(str)
    except Exception:
        df["사번"] = df["사번"].apply(lambda x: str(x) if x is not None else "")
    if "이름" in df.columns:
        df["이름"] = df["사번"].map(emp_map).fillna(df.get("이름",""))
    if "활성" in df.columns:
        df["활성"] = df["활성"].map(_to_bool) if hasattr(pd.Series, "map") else df["활성"]

    # If there was a read error, surface it but continue with empty grid
    if read_err:
        st.info("권한 시트를 아직 만들지 않았습니다. 아래에서 행을 추가한 뒤 **전체 저장**을 누르면 시트가 생성됩니다.", icon="ℹ️")
        with st.expander("진단 정보", expanded=False):
            st.write(read_err)

    column_config = {
        "사번": st.column_config.TextColumn("사번", help="권한 부여자 사번", width="small"),
        "이름": st.column_config.TextColumn("이름", help="자동 맵핑(사번→이름)", width="small", disabled=True),
        "역할": st.column_config.SelectboxColumn("역할", options=role_opts, width="small"),
        "범위유형": st.column_config.SelectboxColumn("범위유형", options=scope_opts, width="small"),
        "부서1": st.column_config.TextColumn("부서1", width="small"),
        "부서2": st.column_config.TextColumn("부서2", width="small"),
        "대상사번": st.column_config.TextColumn("대상사번", help="개별일 때 콤마/공백 구분", width="medium"),
        "활성": st.column_config.CheckboxColumn("활성", width="small"),
        "비고": st.column_config.TextColumn("비고", width="medium"),
    }
    st.caption("행을 추가한 뒤 **전체 저장**을 누르면 권한 시트가 (없다면 생성되어) 전체 덮어쓰기 됩니다.")
    edited = st.data_editor(
        df[AUTH_HEADERS],
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
        key="acl_editor",
    )

    # Validation
    errors = []
    if not edited.empty:
        edited = edited.reset_index(drop=True)
        for i, r in edited.iterrows():
            sab = str(r.get("사번","")).strip()
            role = str(r.get("역할","")).strip()
            scope = str(r.get("범위유형","")).strip()
            act = _to_bool(r.get("활성", True))
            if not sab:
                errors.append(f"{i+1}행: 사번 필수")
            if role and role not in ("admin","manager","evaluator"):
                errors.append(f"{i+1}행: 역할 값 오류")
            if scope and scope not in ("부서","개별"):
                errors.append(f"{i+1}행: 범위유형 값 오류")
            if act and role in ("manager","evaluator"):
                if scope=="부서" and not (str(r.get("부서1","")).strip() or str(r.get("부서2","")).strip()):
                    errors.append(f"{i+1}행: 범위유형=부서인 경우 부서1/부서2 중 하나 이상 필요")
                if scope=="개별" and not str(r.get("대상사번","")).strip():
                    errors.append(f"{i+1}행: 범위유형=개별인 경우 대상사번 필요")
            # name sync
            if sab and sab in emp_map:
                edited.at[i,"이름"] = emp_map[sab]
        # Duplicate check
        keys = edited.apply(lambda r: (str(r.get("사번","")).strip(),
                                       str(r.get("역할","")).strip(),
                                       str(r.get("범위유형","")).strip(),
                                       str(r.get("부서1","")).strip(),
                                       str(r.get("부서2","")).strip(),
                                       str(r.get("대상사번","")).strip()), axis=1)
        dup = keys.duplicated(keep=False)
        if dup.any():
            idxs = [str(i+1) for i, v in enumerate(dup) if v]
            errors.append(f"중복 행 존재: {', '.join(idxs)}행")

    # Actions
    c1, c2 = st.columns([1,1])
    with c1:
        do_save = st.button("전체 저장", type="primary", use_container_width=True, key="acl_save")
    with c2:
        do_reload = st.button("새로고침", use_container_width=True, key="acl_reload")

    if do_reload:
        try: read_auth_df.clear()
        except Exception: pass
        st.rerun()

    if do_save:
        if errors:
            st.error("저장 불가:
- " + "
- ".join(errors))
        else:
            try:
                # Ensure sheet exists and write
                wb = get_book()
                try:
                    ws = wb.worksheet(AUTH_SHEET)
                except Exception:
                    # Create if missing
                    ws = _retry(wb.add_worksheet, title=AUTH_SHEET, rows=max(1000, len(edited)+10), cols=len(AUTH_HEADERS)+2)
                # Clear & write
                _retry(ws.clear)
                _retry(ws.update, "A1", [AUTH_HEADERS])
                values = []
                for _, r in edited.iterrows():
                    row = [str(r.get(h,"")) for h in AUTH_HEADERS]
                    i_act = AUTH_HEADERS.index("활성")
                    row[i_act] = "TRUE" if _to_bool(row[i_act]) else "FALSE"
                    values.append(row)
                if values:
                    _retry(ws.update, "A2", values)
                try: read_auth_df.clear()
                except Exception: pass
                st.success("권한 저장 완료", icon="✅")
                st.rerun()
            except Exception as e:
                st.exception(e)

