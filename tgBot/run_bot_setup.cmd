@echo off
setlocal
cd /d "%~dp0"

REM Ищем py launcher (обычно C:\Windows\py.exe)
set "PYLAUNCHER=%SystemRoot%\py.exe"
if not exist "%PYLAUNCHER%" set "PYLAUNCHER=py"

REM Создаём venv на 3.11 (или 3.x)
"%PYLAUNCHER%" -3.11 -m venv .venv || "%PYLAUNCHER%" -3 -m venv .venv || python -m venv .venv || (
  echo [ERROR] Cannot create venv. >> "%cd%\bot.err.log"
  exit /b 1
)

set "PY=%cd%\.venv\Scripts\python.exe"
"%PY%" -m pip -q install --upgrade pip
REM КРИТИЧНО: tzdata для ZoneInfo на Windows
"%PY%" -m pip -q install aiogram==3.* apscheduler yt-dlp python-dotenv tzdata

echo [OK] venv ready. >> "%cd%\bot.log"