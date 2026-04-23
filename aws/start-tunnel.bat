@echo off
setlocal

set "SOURCE_KEY=%~dp0zev-key.pem"
set "KEY_DIR=%USERPROFILE%\.ssh\zev-dashboard"
set "KEY_PATH=%KEY_DIR%\zev-key.pem"

echo Starting SSH tunnel to RDS via EC2...
echo Source key: %SOURCE_KEY%
echo SSH key copy: %KEY_PATH%

if not exist "%SOURCE_KEY%" (
    echo ERROR: SSH key not found: %SOURCE_KEY%
    pause
    exit /b 1
)

if not exist "%KEY_DIR%" mkdir "%KEY_DIR%"
copy /Y "%SOURCE_KEY%" "%KEY_PATH%" >nul

icacls "%KEY_PATH%" /inheritance:r >nul
icacls "%KEY_PATH%" /remove:g "Users" "Authenticated Users" "Everyone" "CodexSandboxUsers" >nul 2>nul
icacls "%KEY_PATH%" /grant:r "%USERNAME%:R" >nul

ssh -i "%KEY_PATH%" -L 5433:zev-perf.chw0mom2oauu.us-east-2.rds.amazonaws.com:5432 ubuntu@3.140.99.50
pause
