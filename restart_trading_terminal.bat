@echo off
setlocal EnableExtensions
cd /d "%~dp0"
rem No credentials are stored in this script. Put your Rithmic account in .env
rem (RITHMIC_DEMO_USER / RITHMIC_DEMO_PW) or enter it in the terminal's account drawer.
echo ========================================
echo ODT TITAN Trading Terminal Restart
echo ========================================
echo.

echo [1/4] Closing the previous terminal on port 11200...
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; Get-Process trading_terminal | Stop-Process -Force; $connections=Get-NetTCPConnection -LocalPort 11200 -State Listen; $pids=$connections | Select-Object -ExpandProperty OwningProcess -Unique; foreach($processId in $pids){ Stop-Process -Id $processId -Force }; Start-Sleep -Milliseconds 1000"

echo [2/4] Building the latest release...
cargo build --release --bin trading_terminal
if errorlevel 1 (
  echo.
  echo Build failed. The terminal was not started.
  pause
  exit /b 1
)

if not exist "data" mkdir "data"

echo [3/4] Starting the new terminal process...
start "ODT TITAN Trading Terminal" cmd /k ""%~dp0target\release\trading_terminal.exe""

echo Waiting for http://127.0.0.1:11200/ ...
for /L %%N in (1,1,60) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "$c=Get-NetTCPConnection -LocalPort 11200 -State Listen -ErrorAction SilentlyContinue; if($c){exit 0}else{exit 1}" >nul 2>&1
  if not errorlevel 1 goto terminal_ready
  timeout /t 1 /nobreak >nul
)

echo.
echo The new terminal did not start within 60 seconds.
echo Check the new terminal window and data\terminal.stderr.log.
pause
exit /b 1

:terminal_ready
echo [4/4] Opening the web terminal...
start "" "http://127.0.0.1:11200/"
echo.
echo Restart completed. The new terminal window must remain open.
endlocal
exit /b 0
