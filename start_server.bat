@echo off
REM Start FastAPI using the ROOT venv’s interpreter
cd /d "%~dp0"
.\venv\Scripts\python.exe -m uvicorn brains.main:app --host 127.0.0.1 --port 8000
