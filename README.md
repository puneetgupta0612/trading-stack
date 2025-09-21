# trading-stack

Local FastAPI “Signal & Logger” + Google Sheets backfill.

## Quickstart
- Activate venv: `.\brains\.venv\Scripts\Activate.ps1`
- Run API: `python -m uvicorn brains.main:app --host 127.0.0.1 --port 8001 --reload`
- Health: `GET /health`
- Sheets debug: `GET /debug/sheets` (requires `x-api-key`)
- Backfill trade-date (dry-run):  
  `POST /admin/sheets/backfill_trade_date?start_row=2&max_rows=5000&dry_run=1`
- Backfill trade-date (apply):  
  `POST /admin/sheets/backfill_trade_date?start_row=2&max_rows=5000&dry_run=0`
