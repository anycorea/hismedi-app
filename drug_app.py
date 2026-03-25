<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>약제 관리 시스템</title>
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
        }

        /* 1 & 2. 레이아웃 구조 */
        .sidebar {
            width: 260px;
            background-color: #fff;
            border-right: 1px solid var(--border-color);
            padding: 20px;
            display: flex;
            flex-direction: column;
            gap: 15px;
        }

        .main-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            padding: 20px;
            overflow-y: auto;
        }

        /* 왼쪽 메뉴 구성 */
        .input-group {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }
        .input-group label {
            font-size: 12px;
            color: #666;
            font-weight: bold;
        }
        .input-group input {
            padding: 10px;
            border: 1px solid var(--border-color);
            border-radius: 4px;
        }

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
            transition: 0.2s;
        }
        .left-tabs button:hover { background: #f0f0f0; }
        .left-tabs button.active {
            background: var(--secondary-color);
            color: white;
            border-color: var(--secondary-color);
        }

        /* 오른쪽 상단 메뉴 구성 */
        .top-nav {
            display: flex;
            justify-content: flex-end;
            align-items: center;
            gap: 10px;
            margin-bottom: 20px;
        }
        .right-tabs {
            display: flex;
            gap: 5px;
        }
        .right-tabs button {
            padding: 10px 20px;
            border: 1px solid var(--border-color);
            background: #fff;
            cursor: pointer;
            font-weight: bold;
            border-radius: 4px;
        }
        .right-tabs button.active {
            background: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }

        /* 권한 관리 박스 */
        .auth-box {
            display: flex;
            align-items: center;
            gap: 5px;
            background: #eee;
            padding: 5px 10px;
            border-radius: 4px;
            border: 1px solid #ccc;
        }
        .auth-box input {
            width: 50px;
            border: 1px solid #bbb;
            text-align: center;
            padding: 3px;
            border-radius: 3px;
        }
        .auth-status {
            font-size: 11px;
            color: #e74c3c;
            font-weight: bold;
        }
        .auth-status.granted { color: #27ae60; }

        /* 테이블 스타일 */
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        th, td {
            border: 1px solid var(--border-color);
            padding: 12px;
            text-align: center;
        }
        th { background: #f8f9fa; }

        /* 입력 폼 비활성화 스타일 */
        .readonly-mode input, .readonly-mode select, .readonly-mode [contenteditable] {
            pointer-events: none;
            background-color: #f9f9f9;
            border: none;
        }
    </style>
</head>
<body>

    <!-- 왼쪽 메뉴 영역 -->
    <div class="sidebar">
        <div class="input-group">
            <label>신청자 성명</label>
            <input type="text" id="applicantName" placeholder="성명을 입력하세요">
        </div>
        <div class="input-group">
            <label>날짜 선택</label>
            <input type="date" id="selectDate">
        </div>
        
        <hr style="width:100%; border:0; border-top:1px solid #eee; margin: 10px 0;">
        
        <div class="left-tabs">
            <button onclick="switchTab('사용중지')">사용중지</button>
            <button onclick="switchTab('신규입고')">신규입고</button>
            <button onclick="switchTab('대체입고')">대체입고</button>
            <button onclick="switchTab('급여코드변경')">급여코드변경</button>
            <button onclick="switchTab('단가인하')">단가인하▼</button>
            <button onclick="switchTab('단가인상')">단가인상▲</button>
        </div>
    </div>

    <!-- 메인 컨텐츠 영역 -->
    <div class="main-content">
        <!-- 오른쪽 상단 메뉴 -->
        <div class="top-nav">
            <div class="right-tabs">
                <button id="btn-status" class="active" onclick="switchMain('status')">📊 진행현황</button>
                <div class="auth-box">
                    <span style="font-size:11px">권한</span>
                    <input type="password" id="authCode" maxlength="4" placeholder="****" oninput="checkAuth()">
                </div>
                <button id="btn-search" onclick="switchMain('search')">🔍 약가조회</button>
            </div>
        </div>

        <!-- 진행현황판 -->
        <div id="content-area">
            <h3 id="view-title">📊 진행현황</h3>
            <table id="statusTable" class="readonly-mode">
                <thead>
                    <tr>
                        <th>구분</th>
                        <th>약품명</th>
                        <th>진행상태</th>
                        <th>완료일 (선택 가능)</th>
                        <th>비고</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>사용중지</td>
                        <td contenteditable="true">아스피린정</td>
                        <td>
                            <select>
                                <option>검토중</option>
                                <option>승인완료</option>
                            </select>
                        </td>
                        <td><input type="date" class="complete-date"></td>
                        <td contenteditable="true">특이사항 없음</td>
                    </tr>
                    <!-- 추가 행 생략 -->
                </tbody>
            </table>
            <p id="auth-msg" class="auth-status">⚠️ 4자리 코드를 입력하면 편집이 가능합니다.</p>
        </div>
    </div>

    <script>
        // 1. 초기화 (오늘 날짜 설정)
        window.onload = function() {
            const today = new Date().toISOString().split('T')[0];
            document.getElementById('selectDate').value = today;
            
            // 표의 모든 완료일 기본값을 오늘로 설정
            const dateInputs = document.querySelectorAll('.complete-date');
            dateInputs.forEach(input => {
                input.value = today;
            });
        };

        // 2. 권한 관리 로직 (1452 입력 시 편집 허용)
        function checkAuth() {
            const code = document.getElementById('authCode').value;
            const table = document.getElementById('statusTable');
            const msg = document.getElementById('auth-msg');

            if (code === "1452") {
                table.classList.remove('readonly-mode');
                msg.textContent = "✅ 편집 권한이 활성화되었습니다.";
                msg.className = "auth-status granted";
            } else {
                table.classList.add('readonly-mode');
                msg.textContent = "⚠️ 4자리 코드를 입력하면 편집이 가능합니다.";
                msg.className = "auth-status";
            }
        }

        // 3. 탭 전환 로직
        function switchTab(tabName) {
            // 왼쪽 탭 활성화 표시
            const buttons = document.querySelectorAll('.left-tabs button');
            buttons.forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            alert(tabName + " 메뉴로 이동합니다. (신청서 양식 로드)");
        }

        function switchMain(type) {
            const btnStatus = document.getElementById('btn-status');
            const btnSearch = document.getElementById('btn-search');
            const title = document.getElementById('view-title');

            if (type === 'status') {
                btnStatus.classList.add('active');
                btnSearch.classList.remove('active');
                title.innerText = "📊 진행현황";
                document.getElementById('statusTable').style.display = "table";
                document.getElementById('auth-msg').style.display = "block";
            } else {
                btnSearch.classList.add('active');
                btnStatus.classList.remove('active');
                title.innerText = "🔍 약가조회";
                document.getElementById('statusTable').style.display = "none";
                document.getElementById('auth-msg').style.display = "none";
            }
        }
    </script>
</body>
</html>
