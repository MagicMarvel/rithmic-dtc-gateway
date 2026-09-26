@echo off
setlocal
cd /d "%~dp0"
rem No credentials are stored in this script. Put your Rithmic account in .env
rem (RITHMIC_DEMO_USER / RITHMIC_DEMO_PW) or enter it in the terminal's account drawer.
if not exist ".env" (
  echo Missing .env in %~dp0
  echo Create it from .env.example and fill the Rithmic Demo settings.
  pause
  exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in (`findstr /R /B /C:"RITHMIC_DEMO_URL=" ".env"`) do set "DEMO_URL=%%B"
if not defined DEMO_URL (
  echo RITHMIC_DEMO_URL is empty in .env
  set /p "DEMO_URL=Enter the Paper primary WebSocket URL (wss://...): "
  if not defined DEMO_URL (
    echo A primary URL is required.
    pause
    exit /b 1
  )
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$p='.env';$c=Get-Content -LiteralPath $p;$c=$c -replace '^RITHMIC_DEMO_URL=.*$','RITHMIC_DEMO_URL='+$env:DEMO_URL;[System.IO.File]::WriteAllLines($p,$c,(New-Object System.Text.UTF8Encoding($false)))"
)
for /f "usebackq tokens=1,* delims==" %%A in (`findstr /R /B /C:"RITHMIC_DEMO_ALT_URL=" ".env"`) do set "DEMO_ALT_URL=%%B"
if not defined DEMO_ALT_URL (
  echo RITHMIC_DEMO_ALT_URL is empty in .env
  set /p "DEMO_ALT_URL=Enter the Paper alternative WebSocket URL (wss://...): "
  if not defined DEMO_ALT_URL (
    echo An alternative URL is required.
    pause
    exit /b 1
  )
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$p='.env';$c=Get-Content -LiteralPath $p;$c=$c -replace '^RITHMIC_DEMO_ALT_URL=.*$','RITHMIC_DEMO_ALT_URL='+$env:DEMO_ALT_URL;[System.IO.File]::WriteAllLines($p,$c,(New-Object System.Text.UTF8Encoding($false)))"
)

echo Starting Rithmic Flow Paper terminal...
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":11200 .*LISTENING"') do set "EXISTING_PID=%%P"
if defined EXISTING_PID (
  echo ODT TITAN is already running on http://127.0.0.1:11200/.
  start "" "http://127.0.0.1:11200/"
  endlocal
  exit /b 0
)
start "ODT Titan Terminal" /D "%~dp0" cmd /k cargo run --release --bin trading_terminal
echo Waiting for ODT TITAN local server...
for /L %%N in (1,1,60) do (
  netstat -ano | findstr /R /C:":11200 .*LISTENING" >nul && goto terminal_ready
  timeout /t 1 /nobreak >nul
)
echo ODT TITAN did not start within 60 seconds. Check the terminal window for details.
pause
exit /b 1

:terminal_ready
start "" "http://127.0.0.1:11200/"
endlocal
