@echo off
setlocal enabledelayedexpansion
title Bot Launcher

echo Starting...
echo.

rem Check python
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python not found.
    pause
    exit /b
)

rem Virtual Environment Path
set VENV_PY=venv\Scripts\python.exe

rem If venv is missing, initialize it
if not exist "!VENV_PY!" (
    echo [STATUS] Missing venv. Initializing...
    if exist "venv" rd /s /q venv
    python -m venv venv
    if %errorlevel% neq 0 (
        echo [ERROR] Failed to create venv.
        pause
        exit /b
    )
    echo [STATUS] Installing dependencies...
    venv\Scripts\python.exe -m pip install --upgrade pip
    venv\Scripts\python.exe -m pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo [ERROR] Install failed.
        pause
        exit /b
    )
)

rem Verify if telethon is installed
"!VENV_PY!" -c "import telethon" >nul 2>nul
if %errorlevel% neq 0 (
    echo [STATUS] Dependencies missing. Installing now...
    "!VENV_PY!" -m pip install --upgrade pip
    "!VENV_PY!" -m pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo [ERROR] Installation failed.
        pause
        exit /b
    )
)

echo [STATUS] Starting Bot...
echo [INFO] DO NOT CLOSE THIS WINDOW.
echo.

rem Run bot
"!VENV_PY!" src/search_bot.py

if %errorlevel% neq 0 (
    echo.
    echo [CRITICAL] Bot stopped with error code: %errorlevel%
    pause
)

echo.
echo [DONE] Session ended.
pause
