import streamlit as st
import streamlit.components.v1 as components

# 페이지 설정
st.set_page_config(layout="wide", page_title="약제 관리 시스템")

# 1. 전체 HTML/CSS/JS 코드 (요청하신 모든 기능 포함)
html_code = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <style>
        :root {
            --primary-color: #3498db;
            --secondary-color: #2c3e50;
            --border-color: #ddd;
            --bg-color: #f4f7f6;
        }

        body {
            font-family: 'Pretendard', 'Malgun Gothic', sans-serif;
            margin: 0;
            display: flex;
            height: 100vh;
            background-color: var(--bg-color);
            overflow: hidden;
        }

        /* 2. 왼쪽 사이드바 레이아웃 */
        .sidebar {
            width: 260px;
            background-color: #fff;
            border-right: 1px solid var(--border-color);
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 15px;
            flex-shrink: 0;
        }

        .input-group {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .input-group label {
            font-size: 13px;
            color: #444;
            font-weight: bold;
        }
        .input-group input {
            padding: 10px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
            font-size: 14px;
        }

        /* 왼쪽 하단 탭 (2개씩 배치) */
        .left-tabs {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-top: 10px;
        }
        .left-tabs button {
            padding: 12px 5px;
            border: 1px solid var(--border-color);
            background: #fff;
            cursor: pointer;
            font-size: 13px;
            border-radius: 4px;
            font-weight: 500;
        }
        .left-tabs button:hover { background: #f8f9fa; }
        .left-tabs button.active {
            background: var(--secondary-color);
            color: white;
            border-color: var(--secondary-color);
        }

        /* 메인 영역 */
        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 25px;
            overflow-y: auto;
        }

        /* 1. 오른쪽 상단 메뉴 (2개만 배치 + 권한관리) */
        .top-nav {
            display: flex;
            justify-content: flex-end;
            align-items: center;
            gap: 12px;
            margin-bottom: 25px;
        }
        .right-tabs {
            display: flex;
            gap: 8px;
        }
        .right-tabs button {
            padding: 10px 22px;
            border: 1px solid var(--border-color);
            background: #fff;
            cursor: pointer;
            font-weight: bold;
            border-radius: 6px;
            font-size: 14px;
        }
        .right-tabs button.active {
            background: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }

        /* 3. 권한관리 박스 (1452) */
        .auth-box {
            display: flex;
            align-items: center;
            gap: 6px;
            background: #f1f1f1;
            padding: 6px 12px;
            border-radius: 6px;
            border: 1px solid #ccc;
        }
        .auth-box input {
            width: 55px;
            border: 1px solid #bbb;
            text-align: center;
            padding: 4px;
            border-radius: 4px;
            font-weight: bold;
        }

        /* 테이블 스타일 */
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 10px rgba(0,0,0,0.05);
        }
        th, td {
            border: 1px solid var(--border-color);
            padding: 14px;
            text-align: center;
        }
        th { background: #f8f9fa; font-weight: 600; color: #555; }

        /* 권한 없을 때 편집 금지 스타일 */
        .readonly-mode input, .readonly-mode select, .readonly-mode [contenteditable] {
            pointer-events: none;
            background-color: #f9f9f9;
            color: #888;
        }
        
        .status-msg {
            margin-top: 10px;
            font-size: 13px;
            color: #e74c3c;
            font-weight: 500;
        }
        .status-msg.granted { color: #27ae60; }

    </style>
</head>
<body>

    <!-- 2. 왼쪽 메뉴: 신청자, 날짜선택, 탭 배치 -->
    <div class="sidebar">
        <div class="input-group">
            <label>신청자 성명</label>
            <input type="text" placeholder="성명 입력">
        </div>
        <div class="input-group">
            <label>날짜 선택</label>
            <input type="date" id="mainDate">
        </div>
        
        <hr style="width:100%; border:0; border-top:1px solid #eee; margin: 10px 0;">
        
        <div class="left-tabs">
            <button onclick="setTab(this)">사용중지</button>
            <button onclick="setTab(this)">신규입고</button>
            <button onclick="setTab(this)">대체입고</button>
            <button onclick="setTab(this)">급여코드변경</button>
            <button onclick="setTab(this)">단가인하▼</button>
            <button onclick="setTab(this)">단가인상▲</button>
        </div>
    </div>

    <!-- 메인 영역 -->
    <div class="main-content">
        <!-- 1. 오른쪽 상단 메뉴 (2개 배치) -->
        <div class="top-nav">
            <div class="right-tabs">
                <button class="active" onclick="showView('status')">📊 진행현황</button>
                
                <!-- 3. 권한관리 (진행현황과 약가조회 사이) -->
                <div class="auth-box">
                    <span style="font-size:12px; color:#666;">편집권한</span>
                    <input type="password" id="authCode" maxlength="4" placeholder="****" oninput="checkAuth()">
                </div>

                <button onclick="showView('search')">🔍 약가조회</button>
            </div>
        </div>

        <div id="status-view">
            <h2 style="margin-top:0;">📊 진행현황</h2>
            <table id="targetTable" class="readonly-mode">
                <thead>
                    <tr>
                        <th>구분</th>
                        <th>약품명</th>
                        <th>진행상태</th>
                        <th>완료일 (기본:오늘)</th>
                        <th>비고</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>사용중지</td>
                        <td contenteditable="true">아스피린정 100mg</td>
                        <td>
                            <select>
                                <option>검토중</option>
                                <option>승인완료</option>
                                <option>반려</option>
                            </select>
                        </td>
                        <td><input type="date" class="row-date"></td>
                        <td contenteditable="true">원료 수급 문제</td>
                    </tr>
                    <tr>
                        <td>신규입고</td>
                        <td contenteditable="true">타이레놀서방정</td>
                        <td>
                            <select>
                                <option>검토중</option>
                                <option>승인완료</option>
                            </select>
                        </td>
                        <td><input type="date" class="row-date"></td>
                        <td contenteditable="true">-</td>
                    </tr>
                </tbody>
            </table>
            <div id="authMsg" class="status-msg">🔒 숫자 4자리(1452)를 입력하면 편집할 수 있습니다.</div>
        </div>

        <div id="search-view" style="display:none;">
            <h2>🔍 약가조회</h2>
            <p>준비 중인 화면입니다.</p>
        </div>
    </div>

    <script>
        // 1. 페이지 로드 시 오늘 날짜 기본값 세팅
        window.onload = function() {
            const today = new Date().toISOString().split('T')[0];
            document.getElementById('mainDate').value = today;
            
            // 표 내부의 모든 날짜 input에 오늘 날짜 채우기
            const rowDates = document.querySelectorAll('.row-date');
            rowDates.forEach(el => el.value = today);
        };

        // 2. 권한 관리 함수 (1452)
        function checkAuth() {
            const code = document.getElementById('authCode').value;
            const table = document.getElementById('targetTable');
            const msg = document.getElementById('authMsg');

            if (code === "1452") {
                table.classList.remove('readonly-mode');
                msg.textContent = "✅ 편집 권한이 승인되었습니다.";
                msg.className = "status-msg granted";
            } else {
                table.classList.add('readonly-mode');
                msg.textContent = "🔒 숫자 4자리(1452)를 입력하면 편집할 수 있습니다.";
                msg.className = "status-msg";
            }
        }

        // 탭 메뉴 보기 전환
        function showView(type) {
            if(type === 'status') {
                document.getElementById('status-view').style.display = 'block';
                document.getElementById('search-view').style.display = 'none';
            } else {
                document.getElementById('status-view').style.display = 'none';
                document.getElementById('search-view').style.display = 'block';
            }
        }

        function setTab(btn) {
            const btns = document.querySelectorAll('.left-tabs button');
            btns.forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
        }
    </script>
</body>
</html>
"""

# Streamlit에 HTML 출력
components.html(html_code, height=800, scrolling=True)
