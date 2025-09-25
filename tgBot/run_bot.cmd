@echo off
setlocal
cd /d "%~dp0"

REM --- Жёстко используем venv. Если нет — пишем в лог и выходим с ошибкой ---
set "PYVENV=%cd%\.venv\Scripts\python.exe"
if not exist "%PYVENV%" (
  echo [%date% %time%] .venv not found. Run run_bot_setup.cmd once to create it.>> "%cd%\bot.err.log"
  exit /b 1
)

REM --- Запуск с логами ---
echo [START %date% %time%] >> "%cd%\bot.log"
"%PYVENV%" -u bot.py >> "%cd%\bot.log" 2>>"%cd%\bot.err.log"
exit /b %ERRORLEVEL%