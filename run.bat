@echo off
REM ============================================================
REM run.bat — always activates venv before running any command
REM
REM Usage:
REM   run.bat validate_setup.py
REM   run.bat cli.py validate --csv-dir csv_inputs\
REM   run.bat cli.py run --env dev --dry-run
REM   run.bat cli.py run --env prod
REM ============================================================

if not exist venv\Scripts\activate.bat (
    echo [ERROR] venv not found. Run setup.bat first.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
python %*
