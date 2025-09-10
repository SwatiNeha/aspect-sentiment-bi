@echo off
cd /d E:\Project\aspect-sentiment-bi-realtime

REM --- Ensure PowerShell execution policy is safe ---
powershell -Command "Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force"

REM --- Activate virtual environment ---
call .venv\Scripts\activate

REM --- Step 1: Ingest new Reddit data ---
python realtime\ingest_reddit_stream.py

REM --- Step 2: Process new reviews (sentiment + aspects) ---
python realtime\process_new_phase3.py

REM --- Step 3: Export CSVs for Power BI ---
python tools\export_for_powerbi.py

pause