@echo off
REM Start Famfolioz on Windows

cd /d "%~dp0"

REM Check setup
if not exist "venv" (
    echo First time? Running setup...
    python -m venv venv
    call venv\Scripts\activate
    pip install --upgrade pip --quiet
    pip install -r requirements.txt --quiet
    pip install -e . --quiet
) else (
    call venv\Scripts\activate
)

echo Starting Famfolioz...
echo Open http://127.0.0.1:5000 in your browser
echo Press Ctrl+C to stop.
echo.

python -m cas_parser.webapp.app
