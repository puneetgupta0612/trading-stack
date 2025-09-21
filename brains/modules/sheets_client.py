from __future__ import annotations
import time
from typing import List
import gspread
from gspread.exceptions import WorksheetNotFound, APIError
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

TRADES_HEADER = [
    "ts","symbol","dir","entry","sl","tp","lots","status",
    "ticket","magic","spread_pts","slippage","exit","pips","profit","net","comment"
]

class SheetsClient:
    def __init__(self, sheet_id: str, creds_path: str = "service_account.json"):
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        gc = gspread.authorize(creds)
        self.sh = gc.open_by_key(sheet_id)

    def _ensure_tab(self, tab: str):
        try:
            ws = self.sh.worksheet(tab)
        except WorksheetNotFound:
            # Create with header
            ws = self.sh.add_worksheet(title=tab, rows=1000, cols=max(20, len(TRADES_HEADER)+2))
            ws.append_row(TRADES_HEADER, value_input_option="RAW")
            try:
                ws.freeze(rows=1)
            except Exception:
                pass
            return ws

        # If the sheet is empty, lay down the header row to match API writer
        try:
            first_row = ws.row_values(1)
            if len(first_row) == 0:
                ws.append_row(TRADES_HEADER, value_input_option="RAW")
        except Exception:
            pass
        return ws

    def append_row(self, tab: str, row: List):
        ws = self._ensure_tab(tab)
        try:
            ws.append_row(row, value_input_option="RAW")
        except APIError:
            # Retry once (helps on transient 429s)
            time.sleep(1.0)
            ws.append_row(row, value_input_option="RAW")
