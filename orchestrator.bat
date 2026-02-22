@echo off
REM =============================================================================
REM orchestrator.bat — Liquidity Data Pipeline orchestrator (Windows)
REM
REM Usage:
REM   Double-click orchestrator.bat  OR  run from Command Prompt / PowerShell
REM
REM What it does:
REM   1. Creates / activates a Python virtual environment
REM   2. Installs dependencies from backend\requirements.txt
REM   3. Starts the FastAPI server in a new window
REM   4. Waits for the server to be ready
REM   5. Runs the full pipeline in the required order:
REM        company -> revenue -> expense -> fx_rate -> financials/calculate
REM   6. Runs all GET / audit read endpoints and prints results
REM =============================================================================

setlocal enabledelayedexpansion

set PORT=8004
set HOST=http://localhost:%PORT%
set BASE_DIR=%~dp0
set VENV_DIR=%BASE_DIR%.venv
set LOG_FILE=%BASE_DIR%server.log

echo.
echo ============================================================
echo  Liquidity Data Pipeline — Orchestrator
echo ============================================================
echo.

REM ── Step 1: Virtual environment ──────────────────────────────
echo [Step 1] Setting up Python virtual environment...
if not exist "%VENV_DIR%" (
    python -m venv "%VENV_DIR%"
    echo [  OK  ] Virtual environment created at %VENV_DIR%
) else (
    echo [  OK  ] Virtual environment already exists.
)

call "%VENV_DIR%\Scripts\activate.bat"
echo [  OK  ] Virtual environment activated.
echo.

REM ── Step 2: Install dependencies ────────────────────────────
echo [Step 2] Installing dependencies...
pip install --quiet --upgrade pip
pip install --quiet -r "%BASE_DIR%backend\requirements.txt"
echo [  OK  ] Dependencies installed.
echo.

REM ── Step 3: Start the FastAPI server ────────────────────────
echo [Step 3] Starting FastAPI server on port %PORT%...
start "FastAPI Server" /min cmd /c "python -m uvicorn backend.app.main:app --host 0.0.0.0 --port %PORT% --log-level info > %LOG_FILE% 2>&1"
echo [  OK  ] Server starting. Logs -> %LOG_FILE%
echo.

REM ── Step 4: Wait for server to be ready ─────────────────────
echo [Step 4] Waiting for server to be ready...
set WAITED=0
:WAIT_LOOP
    curl --silent --fail "%HOST%/docs" >nul 2>&1
    if %errorlevel%==0 goto SERVER_READY
    set /a WAITED+=1
    if %WAITED% geq 60 (
        echo [ FAIL ] Server did not start within 60s. Check %LOG_FILE%.
        exit /b 1
    )
    timeout /t 1 /nobreak >nul
    goto WAIT_LOOP
:SERVER_READY
echo [  OK  ] Server is ready (waited %WAITED%s).
echo.

REM ── Step 5: Run pipeline in order ───────────────────────────
echo ============================================================
echo [Step 5] Running pipeline in required order...
echo ============================================================
echo.

echo [POST] /company — Load company master (dim_company)
curl -s -X POST "%HOST%/company" | python -m json.tool
echo.

echo [POST] /revenue — Load monthly revenue
curl -s -X POST "%HOST%/revenue" | python -m json.tool
echo.

echo [POST] /expense — Load monthly expenses
curl -s -X POST "%HOST%/expense" | python -m json.tool
echo.

echo [POST] /fx_rate — Load FX rates
curl -s -X POST "%HOST%/fx_rate" | python -m json.tool
echo.

echo [POST] /financials/calculate — Compute financial metrics
curl -s -X POST "%HOST%/financials/calculate" | python -m json.tool
echo.

REM ── Step 6: Read endpoints ───────────────────────────────────
echo ============================================================
echo [Step 6] Running read endpoints...
echo ============================================================
echo.

echo [GET] /company
curl -s -X GET "%HOST%/company" | python -m json.tool
echo.

echo [GET] /revenue
curl -s -X GET "%HOST%/revenue" | python -m json.tool
echo.

echo [GET] /expense
curl -s -X GET "%HOST%/expense" | python -m json.tool
echo.

echo [GET] /financials
curl -s -X GET "%HOST%/financials" | python -m json.tool
echo.

echo [GET] /fx_rate
curl -s -X GET "%HOST%/fx_rate" | python -m json.tool
echo.

REM ── Step 7: Audit endpoints ──────────────────────────────────
echo ============================================================
echo [Step 7] Querying dataload_audit table...
echo ============================================================
echo.

echo [GET] /audit — All audit records
curl -s -X GET "%HOST%/audit" | python -m json.tool
echo.

echo [GET] /audit?entity=company
curl -s -X GET "%HOST%/audit?entity=company" | python -m json.tool
echo.

echo [GET] /audit?entity=revenue
curl -s -X GET "%HOST%/audit?entity=revenue" | python -m json.tool
echo.

echo [GET] /audit?entity=expense
curl -s -X GET "%HOST%/audit?entity=expense" | python -m json.tool
echo.

echo [GET] /audit?stage=GOLDEN^&status=SUCCESS
curl -s -X GET "%HOST%/audit?stage=GOLDEN&status=SUCCESS" | python -m json.tool
echo.

echo [GET] /audit?stage=GOLDEN^&status=FAILURE
curl -s -X GET "%HOST%/audit?stage=GOLDEN&status=FAILURE" | python -m json.tool
echo.

REM ── Done — ask user whether to stop or keep the server running ─────────────
echo ============================================================
echo [  OK  ] Pipeline orchestration complete.
echo ============================================================
echo.
echo  The FastAPI server is still running in the background.
echo  Logs: %LOG_FILE%
echo  API docs: http://localhost:%PORT%/docs
echo.

:ASK_STOP
set /p STOP_ANSWER="  Stop the server now? [y/N]: "
if /i "%STOP_ANSWER%"=="y"   goto DO_STOP
if /i "%STOP_ANSWER%"=="yes" goto DO_STOP
if /i "%STOP_ANSWER%"=="n"   goto DO_KEEP
if /i "%STOP_ANSWER%"=="no"  goto DO_KEEP
if "%STOP_ANSWER%"==""       goto DO_KEEP
echo [ WARN ] Please answer y or n.
goto ASK_STOP

:DO_STOP
echo.
echo [  OK  ] Stopping server...
taskkill /FI "WINDOWTITLE eq FastAPI Server" /F >nul 2>&1
echo [  OK  ] Server stopped.
goto END

:DO_KEEP
echo.
echo [  OK  ] Server left running in the background.
echo          Close the "FastAPI Server" window manually to stop it.

:END
echo.
endlocal
pause

