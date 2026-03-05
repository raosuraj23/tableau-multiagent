@echo off
REM ============================================================
REM Tableau Multi-Agent System — setup.bat
REM Run once to set up the full project environment on Windows
REM Requirements: Python 3.12 installed and in PATH
REM ============================================================

setlocal EnableDelayedExpansion

echo.
echo =====================================================
echo   Tableau Multi-Agent System — Environment Setup
echo =====================================================
echo.

REM --- Check Python version ---
python --version 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python not found in PATH.
    echo         Install Python 3.12 from https://python.org
    echo         Make sure to check "Add Python to PATH" during install.
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python --version') do set PYVER=%%v
echo [OK] Python version: %PYVER%

REM --- Check Python 3.12 ---
echo %PYVER% | findstr /r "^3\.12\." >nul
if %errorlevel% neq 0 (
    echo [WARN] Expected Python 3.12.x, found %PYVER%. Continuing anyway...
)

REM --- Create virtual environment ---
echo.
echo [STEP 1/6] Creating virtual environment...
if exist venv (
    echo         venv already exists, skipping creation.
) else (
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo [OK] Virtual environment created at .\venv
)

REM --- Activate venv ---
echo.
echo [STEP 2/6] Activating virtual environment...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo [ERROR] Failed to activate virtual environment.
    pause
    exit /b 1
)
echo [OK] Virtual environment activated.

REM --- Install requirements ---
echo.
echo [STEP 3/6] Upgrading pip and setuptools...
python -m pip install --upgrade pip setuptools wheel --quiet
echo [OK] pip upgraded.

echo.
echo [STEP 4/6] Installing requirements (this may take 3-5 minutes)...
echo         Note: pip will resolve the LangChain ecosystem versions automatically.
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] Failed to install requirements.
    echo.
    echo  Common causes:
    echo    1. No internet connection
    echo    2. Corporate proxy blocking pip  ^(set HTTPS_PROXY=... first^)
    echo    3. Conflicting packages from a previous install
    echo.
    echo  Try this fix:
    echo    pip install --upgrade pip setuptools wheel
    echo    pip install -r requirements.txt --no-cache-dir
    echo.
    pause
    exit /b 1
)
echo [OK] All requirements installed.

REM --- Create .env from template ---
echo.
echo [STEP 5/6] Setting up environment configuration...
if exist .env (
    echo         .env already exists, skipping. Edit it manually if needed.
) else (
    copy .env.template .env >nul
    echo [OK] Created .env from template.
    echo.
    echo  *** IMPORTANT: Open .env and fill in your credentials ***
    echo      - ANTHROPIC_API_KEY
    echo      - GOOGLE_API_KEY
    echo      - TABLEAU_SERVER_URL, TABLEAU_SITE_ID, TABLEAU_PAT_NAME, TABLEAU_PAT_SECRET
    echo      - SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, etc.
    echo.
)

REM --- Create required directories ---
echo.
echo [STEP 6/6] Creating required directories...
if not exist logs mkdir logs
if not exist state mkdir state
if not exist state\checkpoints mkdir state\checkpoints
if not exist state\snapshots mkdir state\snapshots
if not exist models\tds mkdir models\tds
if not exist models\twb mkdir models\twb
if not exist tableau\output mkdir tableau\output
if not exist csv_inputs mkdir csv_inputs
echo [OK] Directories ready.

REM --- Run validation script ---
echo.
echo =====================================================
echo   Running setup validation...
echo =====================================================
python validate_setup.py
if %errorlevel% neq 0 (
    echo.
    echo [WARN] Some validations failed. See output above.
    echo        Fill in .env credentials and run validate_setup.py again.
) else (
    echo.
    echo [SUCCESS] Setup complete!
)

echo.
echo =====================================================
echo   Next Steps:
echo   1. Edit .env with your credentials
echo   2. Fill in csv_inputs\ with your project data
echo      (copy from csv_inputs\examples\ as starting point)
echo.
echo   *** IMPORTANT — run these commands every new terminal: ***
echo   cd D:\Projects\Tableau Agent\tableau-multiagent
echo   venv\Scripts\activate
echo   python validate_setup.py
echo.
echo   Your prompt should show (venv) when active.
echo =====================================================
echo.

pause
