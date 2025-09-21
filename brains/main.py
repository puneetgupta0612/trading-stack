# main.py  (FULL REPLACEMENT)
from fastapi import FastAPI, Request, Depends, HTTPException, status
from pydantic import BaseModel

import os
import time
import json
import csv
import statistics
from pathlib import Path
from typing import Optional, Dict, Any
from collections import defaultdict
from threading import Lock, Event, Thread
from datetime import datetime, timezone, timedelta
import re
import anyio
import traceback
from dotenv import load_dotenv
from brains.modules.sheets_client import SheetsClient

# --- Pydantic v1/v2 compat ---
def _asdict(m):
    return m.model_dump() if hasattr(m, "model_dump") else m.dict()


# === NEW: strategy imports (lazy-safe) ===
try:
    from brains.modules import ensemble as strat_ensemble
    from brains.modules import strat_breakout_london as strat_breakout_london
    from brains.modules import strat_trend_ma as strat_trend_ma
    from brains.modules import strat_meanrev_band as strat_meanrev_band
    HAVE_STRATS = True
except Exception as _e:
    print(f"[STRATS] Strategy modules not loaded yet: {_e}")
    HAVE_STRATS = False

# --- API key auth helpers (unchanged) ---
def _get_api_keys() -> set[str]:
    keys = set()
    k1 = os.getenv("API_KEY", "").strip()
    if k1:
        keys.add(k1)
    ks = os.getenv("API_KEYS", "")
    if ks:
        keys |= {x.strip() for x in ks.split(",") if x.strip()}
    cfg_keys = (CONFIG.get("api_keys") if "CONFIG" in globals() else []) or []
    for k in cfg_keys:
        k = str(k).strip()
        if k:
            keys.add(k)
    return keys

def require_api_key(request: Request):
    keys = _get_api_keys()
    if not keys:
        # no keys configured → auth disabled
        return

    # 1) Header: x-api-key OR Authorization: Bearer <token>
    hdr = request.headers.get("x-api-key") or request.headers.get("authorization")
    candidate = None
    if hdr:
        candidate = hdr
        if candidate.lower().startswith("bearer "):
            candidate = candidate[7:].strip()

    # 2) Query param fallback: ?key=... or ?api_key=...
    if not candidate:
        qp = request.query_params
        candidate = qp.get("key") or qp.get("api_key")

    if candidate and candidate in keys:
        return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API key",
        headers={"WWW-Authenticate": "Bearer"},
    )

from contextlib import asynccontextmanager

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    # startup
    load_config()
    load_counters()
    global AUTOSAVE_THREAD
    AUTOSAVE_STOP.clear()
    AUTOSAVE_THREAD = Thread(target=_autosave_loop, daemon=True)
    AUTOSAVE_THREAD.start()
    try:
        yield
    finally:
        # shutdown
        try:
            AUTOSAVE_STOP.set()
            if AUTOSAVE_THREAD is not None:
                AUTOSAVE_THREAD.join(timeout=10.0)
        finally:
            save_counters()

app = FastAPI(title="Signal & Logger", lifespan=_lifespan)

def _path_allowed(p: Path) -> bool:
    """
    Allow absolute paths only if DEBUG_ALLOW_ABS_PATHS=1; otherwise require relative or default-probed files.
    """
    if not p.is_absolute():
        return True
    return os.getenv("DEBUG_ALLOW_ABS_PATHS", "0") == "1"

def _bt_csv_default_path() -> Path:
    # MetaTrader common files path for backtests
    appdata = os.getenv("APPDATA", "")
    if not appdata:
        return Path("backtest_trades.csv")  # fallback: current dir
    return Path(appdata) / "MetaQuotes" / "Terminal" / "Common" / "Files" / "backtest_trades.csv"

def _bt_markers_default_path() -> Path:
    appdata = os.getenv("APPDATA", "")
    if not appdata:
        return Path("bt_markers.txt")
    return Path(appdata) / "MetaQuotes" / "Terminal" / "Common" / "Files" / "bt_markers.txt"

def _read_lines_guess_enc(path: Path):
    """
    Yield lines from `path` trying common encodings used by MetaTrader logs (UTF-16* etc.).
    Stops on first encoding that can read the whole file without exceptions.
    """
    encodings = ["utf-8-sig", "utf-16", "utf-16le", "utf-16be", "cp1252", "latin-1"]
    for enc in encodings:
        try:
            text = path.read_text(encoding=enc)
            for line in text.splitlines(True):
                yield line
            return
        except Exception:
            continue
    # Last resort: raw bytes → lenient decode
    with path.open("rb") as f:
        for b in f:
            try:
                yield b.decode("utf-8", errors="ignore")
            except Exception:
                yield b.decode("latin-1", errors="ignore")

# --- helpers for session scoping & timestamp sanity ---
_ISO_TS = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
def _is_iso_ts(s: str) -> bool:
    return bool(_ISO_TS.match((s or '').strip()))

_DOT_TS = re.compile(r'^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}$')

def _parse_any_ts(s: str) -> datetime | None:
    if not s:
        return None
    t = s.strip()
    try:
        if _ISO_TS.match(t):
            return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if _DOT_TS.match(t):
            return datetime.strptime(t, "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        return None
    return None

def _to_iso_z(dt: datetime | None) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""


def _last_session_start_iso(markers_path: Path | None = None) -> str | None:
    """
    Find the most recent 'session start' marker inside bt_markers.txt and return ISO Z.
    We consider several breadcrumbs written at EA OnInit / startup.
    """
    mpath = markers_path or _bt_markers_default_path()
    if not mpath.exists():
        return None

    start_phrases = (
        "OnInit:",                         # explicit EA init
        "AI Executor initialised",         # EA Print on init
        "CSV ensured (backtest_trades",    # our ensure header breadcrumb
        "CSV header WRITTEN",              # alternative header breadcrumb
        "CommonDataPath=",
        "CSV target:"
    )
    ts_re = re.compile(r'^\s*(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s*\|')
    best = None
    for raw in _read_lines_guess_enc(mpath):
        line = (raw or "").strip()
        if not line:
            continue
        m = ts_re.match(line)
        if not m:
            continue
        rest = line[m.end():]
        if any(p.lower() in rest.lower() for p in start_phrases):
            try:
                dt = datetime.strptime(m.group(1), "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
                if best is None or dt > best:
                    best = dt
            except Exception:
                continue
    return best.strftime("%Y-%m-%dT%H:%M:%SZ") if best else None


@app.get("/bt/summary", dependencies=[Depends(require_api_key)])
def bt_summary(path: str | None = None,
               last_session: int = 1,
               markers_path: str | None = None,
               from_iso: str | None = None,
               to_iso: str | None = None,
               money_mode: str | None = None):
    """
    Summarize Strategy Tester CSV produced by the EA (backtest_trades.csv).
    Uses PIPS for performance math because money columns are 0 in Tester.
    - trades = count of OPEN rows
    - deals  = count of CLOSE rows
    - last_session=1 scopes rows to the most recent EA run (from markers); set 0 to aggregate all
    """
    csv_path = Path(path) if path else _bt_csv_default_path()
    if not _path_allowed(csv_path):
        return {"ok": False, "error": "Absolute path not allowed (set DEBUG_ALLOW_ABS_PATHS=1 to enable)"}
    if not csv_path.exists():
        return {"ok": False, "error": f"CSV not found: {csv_path}"}

    # Where to start/end counting from
    session_cutoff_iso = None
    if from_iso:
        session_cutoff_iso = from_iso.strip()
    elif int(last_session or 0) == 1:
        session_cutoff_iso = _last_session_start_iso(Path(markers_path) if markers_path else None)

    # Parse window bounds (independent of the above branch)
    cutoff_dt = None
    if session_cutoff_iso:
        try:
            cutoff_dt = datetime.strptime(session_cutoff_iso.strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            cutoff_dt = None

    to_dt = None
    if to_iso:
        try:
            to_dt = datetime.strptime(to_iso.strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            to_dt = None

    # --- Header-aware tolerant reader: map by header positions if present; else positional fallback ---
    if True:
        opens_raw, closes_raw = [], []
        expected = ["ts","symbol","dir","entry","sl","tp","lots","status",
                    "ticket","magic","spread_pts","slippage","exit","pips","profit","net","comment"]
        try:
            with csv_path.open("r", newline="", encoding="utf-8-sig", errors="ignore") as f2:
                rr = csv.reader(f2)
                first = next(rr, None)

                def _norm(s: str) -> str:
                    return (s or "").replace("\ufeff","").replace("\u00a0"," ").strip()

                def _normlow(s: str) -> str:
                    return _norm(s).lower()

                # Detect header and build index map
                header_idx = {}
                is_header = False
                if first:
                    flat = [_normlow(x) for x in first]
                    if "status" in flat and "pips" in flat and "profit" in flat:
                        is_header = True
                        for name in expected:
                            if name in flat:
                                header_idx[name] = flat.index(name)

                # function to construct a normalized dict for a row
                def _make_row(cells: list[str]) -> dict:
                    cells = [_norm(c) for c in cells]
                    if is_header and header_idx:
                        get = lambda k: (cells[header_idx[k]] if k in header_idx and header_idx[k] < len(cells) else "")
                        m = {k: get(k) for k in expected}
                    else:
                        m = dict(zip(expected, cells + [""]*(len(expected)-len(cells))))
                    # fix status if shifted
                    st = _norm(m.get("status")).upper()
                    if st not in ("OPEN","CLOSE"):
                        for v in cells:
                            sv = _norm(v).upper()
                            if sv in ("OPEN","CLOSE"):
                                m["status"] = sv
                                break
                    return m

                # feed first logical data row(s)
                if first:
                    if not is_header:
                        m0 = _make_row(first)
                        st0 = _norm(m0.get("status")).upper()
                        if st0 in ("OPEN","CLOSE"):
                            (opens_raw if st0 == "OPEN" else closes_raw).append(m0)

                # iterate rest
                for row in rr:
                    m = _make_row(row)
                    su = _norm(m.get("status")).upper()
                    if su == "OPEN":
                        opens_raw.append(m)
                    elif su == "CLOSE":
                        closes_raw.append(m)

        except Exception:
            # Parsing via header-aware reader failed; allow the positional/header-sniff fallback below to try.
            opens_raw, closes_raw = [], []

    if not opens_raw and not closes_raw:
        # fallback by index / header sniff (STRICT: only keep OPEN/CLOSE rows)
        try:
            with csv_path.open("r", newline="", encoding="utf-8-sig", errors="ignore") as f2:
                rr = csv.reader(f2)
                first = next(rr, None)

                def _status_up(m): return (m.get("status") or "").strip().upper()

                # consume header if present (also catch space-joined 'ts symbol dir ...')
                is_header = False
                if first:
                    low0 = (first[0] or "").strip().lower()
                    flat = [x.strip().lower() for x in first]
                    is_header = (low0 == "ts") or ("status" in flat) or low0.startswith("ts symbol")
                if not is_header and first:
                    m = dict(zip(expected, first + [""]*(len(expected)-len(first))))
                    if _status_up(m) in ("OPEN", "CLOSE"):
                        (opens_raw if (_status_up(m) == "OPEN") else closes_raw).append(m)

                # rest
                for row in rr:
                    m = dict(zip(expected, row + [""]*(len(expected)-len(row))))
                    su = _status_up(m)
                    if su == "OPEN":
                        opens_raw.append(m)
                    elif su == "CLOSE":
                        closes_raw.append(m)
        except Exception:
            return {"ok": False, "error": "Failed to parse CSV", "path": str(csv_path)}

    # --------- optional session filter ----------
    def _after_cut_soft(row):
        if not cutoff_dt:
            return True
        dt = _parse_any_ts(row.get("ts") or "")
        return bool(dt and dt >= cutoff_dt)

    if session_cutoff_iso:
        opens_raw  = [r for r in opens_raw  if _after_cut_soft(r)]
        closes_raw = [r for r in closes_raw if _after_cut_soft(r)]

    # strict row filtering: valid status + proper ISO ts + (optional) session scope
    def _valid_status_row(r):
        st = (r.get("status") or "").strip().upper()
        return st in ("OPEN", "CLOSE")

    def _after_cut_hard(r):
        dt = _parse_any_ts(r.get("ts") or "")
        if cutoff_dt and not (dt and dt >= cutoff_dt):
            return False
        if to_dt and not (dt and dt <= to_dt):
            return False
        return True

    opens = [r for r in opens_raw  if _valid_status_row(r) and _after_cut_hard(r)]
    closes = [r for r in closes_raw if _valid_status_row(r) and _after_cut_hard(r)]

    if not closes:
        return {"ok": False, "error": "No CLOSE rows found (need CLOSE rows to summarize).", "path": str(csv_path)}

    # --------- tolerant parsers ----------
    def _num(s, default=0.0):
        if s is None:
            return default
        t = str(s).strip()
        if not t or t.lower() in ("nan","null","none") or "missed string parameter" in t.lower():
            return default
        try:
            return float(t.replace(",", ""))
        except Exception:
            m = re.search(r'[-+]?\d+(?:\.\d+)?', t)
            return float(m.group(0)) if m else default

    def _int(s, default=0):
        if s is None:
            return default
        t = str(s).strip()
        if not t or "missed string parameter" in t.lower():
            return default
        try:
            return int(float(t))
        except Exception:
            return default

    # --------- de-duplication (tickets) ----------
    seen_open: set[tuple[str, str]] = set()
    seen_close: set[tuple[str, str]] = set()

    def _iso_ts_of(row):
        return _to_iso_z(_parse_any_ts(row.get("ts") or "")) or ""

    dedup_opens = []
    for o in opens:
        tk = (o.get("ticket") or "").strip()
        key = (tk, _iso_ts_of(o))
        if tk and key in seen_open:
            continue
        if tk:
            seen_open.add(key)
        dedup_opens.append(o)
    opens = dedup_opens

    dedup_closes = []
    for c in closes:
        tk = (c.get("ticket") or "").strip()
        key = (tk, _iso_ts_of(c))
        if tk and key in seen_close:
            continue
        if tk:
            seen_close.add(key)
        dedup_closes.append(c)
    closes = dedup_closes

    # --------- OPEN stats (avg SL/TP/LOTS) ----------
    def _pip_size_for(sym: str) -> float:
        s = (sym or "").upper()
        if "JPY" in s:
            return 0.01
        if any(k in s for k in ("XAU", "GOLD")):
            return 0.1
        if any(k in s for k in ("XAG", "SILVER")):
            return 0.01
        if any(k in s for k in ("WTI", "XTI", "UKOIL", "BRENT")):
            return 0.01
        if any(k in s for k in ("BTC", "XBT", "ETH")):
            return 1.0
        if any(k in s for k in ("US30", "DJI", "DE40", "GER40", "NAS", "NDX", "SPX", "US500", "UK100")):
            return 1.0
        return 0.0001


    sl_pips_list, tp_pips_list, lots_open_list = [], [], []
    for o in opens:
        pip   = _pip_size_for(o.get("symbol") or "")
        entry = _num(o.get("entry"))
        sl    = _num(o.get("sl"))
        tp    = _num(o.get("tp"))
        lotso = _num(o.get("lots"))
        dirv  = _int(o.get("dir") or o.get("direction"))
        if lotso > 0:
            lots_open_list.append(lotso)
        if entry > 0 and sl > 0 and tp > 0 and dirv in (1,-1):
            sl_pips = (entry - sl)/pip if dirv == 1 else (sl - entry)/pip
            tp_pips = (tp - entry)/pip if dirv == 1 else (entry - tp)/pip
            sl_pips_list.append(sl_pips)
            tp_pips_list.append(tp_pips)

    # --------- CLOSE + MONEY stats ----------
    def _money_from_row(crow: dict) -> float:
        net_raw = str(crow.get("net") or "").strip()
        if net_raw != "":
            return _num(net_raw, 0.0)
        p  = _num(crow.get("profit"), 0.0)
        co = _num(crow.get("commission"), 0.0)
        sw = _num(crow.get("swap"), 0.0)
        return p + co + sw

    closes_f = [{
        "ts":          _to_iso_z(_parse_any_ts(c.get("ts",""))),
        "pips":        _num(c.get("pips"), 0.0),
        "dir":         _int(c.get("dir") or c.get("direction"), 0),
        "lots":        _num(c.get("lots"), 0.0),
        "net":         _money_from_row(c),
        "profit":      _num(c.get("profit"), 0.0),
        "position_id": (str(c.get("position_id") or "").strip() or str(c.get("ticket") or "").strip()),
    } for c in closes]

    deals  = len(closes_f)
    trades = len(opens)

    total_pips        = sum(x["pips"] for x in closes_f)
    gross_profit_pips = sum(x["pips"] for x in closes_f if x["pips"] > 0)
    gross_loss_pips   = sum(x["pips"] for x in closes_f if x["pips"] < 0)
    avg_pips          = (total_pips / deals) if deals else 0.0
    profit_factor = (float('inf') if gross_loss_pips == 0 and gross_profit_pips > 0
                     else (gross_profit_pips / abs(gross_loss_pips)) if gross_loss_pips < 0 else 0.0)
    expected_payoff   = avg_pips

    wins_pips   = sum(1 for x in closes_f if x["pips"] > 0)
    losses_pips = deals - wins_pips
    winrate     = (wins_pips / deals * 100.0) if deals else 0.0

    cum = peak = max_dd = 0.0
    for x in closes_f:
        cum += x["pips"]
        if cum > peak: peak = cum
        dd = peak - cum
        if dd > max_dd: max_dd = dd

    total_money = sum(x["net"] for x in closes_f)
    avg_money   = (total_money / deals) if deals else 0.0

    def _profit_of_row(crow: dict) -> float:
        return _num(crow.get("profit"), 0.0)

    profits = [_profit_of_row(c) for c in closes]
    any_profit_nonzero = any(p != 0.0 for p in profits)

    _req_mode = (money_mode or "").strip().lower()
    if _req_mode not in ("deal_profit", "deal_net", "position_net"):
        _req_mode = "deal_profit" if any_profit_nonzero else "deal_net"
    mode = _req_mode

    def _sign(v: float) -> int:
        return 1 if v > 0 else (-1 if v < 0 else 0)

    sign_mismatch = sum(1 for cf in closes_f if _sign(float(cf.get("profit", 0.0))) != _sign(float(cf["net"])))

    if mode == "position_net":
        by_pos: dict[str, float] = {}
        for cf in closes_f:
            pid = str(cf.get("position_id") or "").strip()
            if not pid:
                mode = "deal_net"
                break
            by_pos[pid] = by_pos.get(pid, 0.0) + float(cf["net"])
        if mode == "position_net":
            gross_profit_money = sum(v for v in by_pos.values() if v > 0)
            gross_loss_money   = sum(v for v in by_pos.values() if v < 0)
            wins_money         = sum(1 for v in by_pos.values() if v > 0)
            losses_money       = max(0, len(by_pos) - wins_money)
        else:
            gross_profit_money = sum(x["net"] for x in closes_f if x["net"] > 0)
            gross_loss_money   = sum(x["net"] for x in closes_f if x["net"] < 0)
            wins_money         = sum(1 for x in closes_f if x["net"] > 0)
            losses_money       = deals - wins_money
    elif mode == "deal_profit":
        gross_profit_money = sum(p for p in profits if p > 0)
        gross_loss_money   = sum(p for p in profits if p < 0)
        wins_money         = sum(1 for p in profits if p > 0)
        losses_money       = deals - wins_money
    else:  # "deal_net"
        gross_profit_money = sum(x["net"] for x in closes_f if x["net"] > 0)
        gross_loss_money   = sum(x["net"] for x in closes_f if x["net"] < 0)
        wins_money         = sum(1 for x in closes_f if x["net"] > 0)
        losses_money       = deals - wins_money

    profit_factor_money = (float('inf') if gross_loss_money == 0 and gross_profit_money > 0
                           else (gross_profit_money / abs(gross_loss_money)) if gross_loss_money < 0 else 0.0)
    winrate_money       = (wins_money / (wins_money + losses_money) * 100.0) if (wins_money + losses_money) else 0.0

    cum_m = peak_m = max_dd_m = 0.0
    for x in closes_f:
        cum_m += x["net"]
        if cum_m > peak_m:
            peak_m = cum_m
        ddm = peak_m - cum_m
        if ddm > max_dd_m:
            max_dd_m = ddm

    avg_sl   = statistics.fmean(sl_pips_list) if sl_pips_list else 0.0
    avg_tp   = statistics.fmean(tp_pips_list) if tp_pips_list else 0.0
    avg_lots = statistics.fmean(lots_open_list) if lots_open_list else 0.0

    tail = [{
        "ts":   x["ts"],
        "pips": round(x["pips"], 2),
        "net":  round(x["net"],  2),
        "dir":  x["dir"],
        "lots": x["lots"]
    } for x in closes_f[-10:]]

    return {
        "ok": True,
        "path": str(csv_path),
        "scoped_from": session_cutoff_iso,
        "trades": trades,
        "deals": deals,
        "wins": wins_pips,
        "losses": losses_pips,
        "winrate_pct": round(winrate, 2),
        "total_pips": round(total_pips, 2),
        "avg_pips": round(avg_pips, 2),
        "gross_profit_pips": round(gross_profit_pips, 2),
        "gross_loss_pips": round(gross_loss_pips, 2),
        "profit_factor": ("inf" if profit_factor == float('inf') else round(profit_factor, 2)),
        "expected_payoff_pips": round(expected_payoff, 2),
        "max_drawdown_pips": round(max_dd, 2),
        "money_wins": wins_money,
        "money_losses": losses_money,
        "winrate_money_pct": round(winrate_money, 2),
        "money_total": round(total_money, 2),
        "money_avg": round(avg_money, 2),
        "money_gross_profit": round(gross_profit_money, 2),
        "money_gross_loss": round(gross_loss_money, 2),
        "money_profit_factor": ("inf" if profit_factor_money == float('inf') else round(profit_factor_money, 2)),
        "expected_payoff_money": round(avg_money, 2),
        "max_drawdown_money": round(max_dd_m, 2),
        "money_mode": mode,
        "money_sign_mismatch_deals": sign_mismatch,
        "avg_sl_pips": round(avg_sl, 2),
        "avg_tp_pips": round(avg_tp, 2),
        "avg_lots": round(avg_lots, 3),
        "last_closes": tail,
    }

@app.get("/bt/rebuild_csv_from_markers", dependencies=[Depends(require_api_key)])
def bt_rebuild_from_markers(markers_path: str | None = None,
                            csv_path: str | None = None,
                            dry_run: int = 0,
                            overwrite: int = 0,
                            last_session: int = 0,
                            since_iso: str | None = None):
    """
    Reconstruct backtest_trades.csv from bt_markers.txt.
    Tolerant parsing: matches 'Order ok:' and 'Close:' with flexible spacing.
    Query:
      - markers_path: override bt_markers.txt path
      - csv_path:     override output CSV path
      - dry_run=1:    parse only, do not write CSV; returns sample matches
    """
    mpath = Path(markers_path) if markers_path else _bt_markers_default_path()
    out_csv = Path(csv_path) if csv_path else _bt_csv_default_path()
    if not _path_allowed(mpath):
        return {"ok": False, "error": "Absolute path not allowed (set DEBUG_ALLOW_ABS_PATHS=1 to enable)"}
    if not _path_allowed(out_csv):
        return {"ok": False, "error": "Absolute path not allowed (set DEBUG_ALLOW_ABS_PATHS=1 to enable)"}

    if not mpath.exists():
        return {"ok": False, "error": f"Markers not found: {mpath}"}

    # Flexible token getters -------------------------------------------------
    def _get_token(line: str, key: str):
        # finds key=VALUE; returns VALUE or ""
        i = line.find(key + "=")
        if i < 0:
            return ""
        j = i + len(key) + 1
        # read until whitespace or end
        k = j
        while k < len(line) and line[k] not in " \t\r\n|,":
            k += 1
        return line[j:k].strip()

    def _split_ts(line: str):
        # expects "TS | rest..."
        parts = line.split("|", 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return "", line.strip()

    # Pass 1: parse markers into open/close rows -----------------------------
    opens_rows = []
    closes_rows = []

    # Accept both "TS | rest" and bare lines (no pipe).
    ts_split = re.compile(r'^\s*(?P<ts>\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s*\|\s*(?P<rest>.*)$')
    def _split_ts_flex(line: str):
        m = ts_split.match(line)
        if m:
            return m.group("ts").strip(), m.group("rest").strip()
        return "", line.strip()

    # Very tolerant (case-insensitive), allow extra punctuation/whitespace
    pat_open  = re.compile(
        r'order\s*ok[:\s].*?\bticket\s*=\s*(?P<ticket>\d+).*?\bfill\s*=\s*(?P<fill>-?\d+(?:\.\d+)?)',
        re.IGNORECASE
    )
    pat_close = re.compile(
        r'close\s*[:\s].*?\bdeal\s*=\s*(?P<deal>\d+).*?(?:\bdir\s*=\s*(?P<dir>-?1|0|1).*?)?\bpips\s*=\s*(?P<pips>-?\d+(?:\.\d+)?).*?\bnet\s*=\s*(?P<net>-?\d+(?:\.\d+)?)',
        re.IGNORECASE
    )


    # --- First pass scan (properly indented) ---
    # Optional cutoff (only parse markers at/after this)
    cutoff_dt = None
    if since_iso:
        try:
            cutoff_dt = datetime.strptime(since_iso.strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            cutoff_dt = None
    elif int(last_session or 0) == 1:
        iso = _last_session_start_iso(mpath)
        if iso:
            try:
                cutoff_dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except Exception:
                cutoff_dt = None

    def _pass_cut(ts_str: str) -> bool:
        if not cutoff_dt:
            return True
        try:
            dt = datetime.strptime((ts_str or ""), "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return dt >= cutoff_dt
        except Exception:
            return False

    for raw in _read_lines_guess_enc(mpath):
        line = (raw or "").strip()
        if not line:
            continue

        ts, rest = _split_ts_flex(line)
        if cutoff_dt and (not _pass_cut(ts)):
            continue

        mo = pat_open.search(rest)
        if mo:
            opens_rows.append({
                "ts": ts,
                "symbol": "",
                "dir": "",
                "entry": mo.group("fill") or "",
                "sl": "",
                "tp": "",
                "lots": "",
                "status": "OPEN",
                "ticket": mo.group("ticket") or "",
                "magic": "",
                "spread_pts": "",
                "slippage": "",
                "exit": "",
                "pips": "",
                "profit": "",
                "net": "",
                "comment": ""
            })
            continue

        mc = pat_close.search(rest)
        if mc:
            closes_rows.append({
                "ts": ts,
                "symbol": "",
                "dir": mc.group("dir") or "",
                "entry": "",
                "sl": "",
                "tp": "",
                "lots": "",
                "status": "CLOSE",
                "ticket": mc.group("deal") or "",
                "magic": "",
                "spread_pts": "",
                "slippage": "",
                "exit": "",
                "pips": mc.group("pips") or "",
                "profit": "",
                "net": mc.group("net") or "",
                "comment": ""
            })
            continue

    # --- Fallback pass (still inside the function) ---
    if not opens_rows and not closes_rows:
        for raw in _read_lines_guess_enc(mpath):
            line = (raw or "").strip()
            if not line:
                continue
            ts, rest = _split_ts_flex(line)
            if cutoff_dt and (not _pass_cut(ts)):
                continue

            low = rest.lower()
            if "order ok" in low:
                ticket = _get_token(rest, "ticket")
                fill   = _get_token(rest, "fill")
                if ticket or fill:
                    opens_rows.append({
                        "ts": ts, "symbol": "", "dir": "", "entry": fill or "",
                        "sl": "", "tp": "", "lots": "",
                        "status": "OPEN", "ticket": ticket or "",
                        "magic": "", "spread_pts": "", "slippage": "",
                        "exit": "", "pips": "", "profit": "", "net": "", "comment": ""
                    })
                    continue

            if ("close:" in low) or (" close " in low) or low.startswith("close"):
                deal = _get_token(rest, "deal")
                dirv = _get_token(rest, "dir")
                pips = _get_token(rest, "pips")
                netv = _get_token(rest, "net")
                if deal or (pips or netv):
                    closes_rows.append({
                        "ts": ts, "symbol": "", "dir": dirv or "",
                        "entry": "", "sl": "", "tp": "", "lots": "",
                        "status": "CLOSE", "ticket": deal or "",
                        "magic": "", "spread_pts": "", "slippage": "",
                        "exit": "", "pips": pips or "", "profit": "", "net": netv or "", "comment": ""
                    })
                    continue

    if dry_run:
        return {
            "ok": True,
            "markers": str(mpath),
            "dry_run": True,
            "found_opens": len(opens_rows),
            "found_closes": len(closes_rows),
            "sample_open": (opens_rows[0] if opens_rows else None),
            "sample_close": (closes_rows[0] if closes_rows else None),
        }

    def _iso_from_marker(ts_str: str) -> str:
        try:
            return datetime.strptime(ts_str, "%Y.%m.%d %H:%M:%S").replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return ts_str or ""


    # Write CSV --------------------------------------------------------------
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    existed = out_csv.exists()
    do_overwrite = bool(int(overwrite or 0))
    mode = "w" if (do_overwrite or not existed or out_csv.stat().st_size == 0) else "a"

    # Deduplicate against existing file if appending
    existing_keys = set()
    if mode == "a" and out_csv.exists():
        try:
            with out_csv.open("r", newline="", encoding="utf-8-sig", errors="ignore") as rf:
                rr = csv.reader(rf)
                hdr = next(rr, None)
                for row in rr:
                    if not row:
                        continue
                    ts = (row[0] or "").strip()
                    status = (row[7] or "").strip().upper() if len(row) > 7 else ""
                    ticket = (row[8] or "").strip() if len(row) > 8 else ""
                    existing_keys.add((status, ticket, ts))
        except Exception:
            pass

    def _norm_row(r: dict) -> list[str]:
        return [
            _iso_from_marker(r["ts"]), r["symbol"], r["dir"], r["entry"], r["sl"], r["tp"], r["lots"], r["status"],
            r["ticket"], r["magic"], r["spread_pts"], r["slippage"], r["exit"], r["pips"], r["profit"], r["net"], r["comment"]
        ]

    pending = []
    for r in opens_rows:
        row = _norm_row(r)
        key = (row[7].upper(), row[8], row[0])  # status, ticket, ts
        if key not in existing_keys:
            pending.append(row)
            existing_keys.add(key)
    for r in closes_rows:
        row = _norm_row(r)
        key = (row[7].upper(), row[8], row[0])
        if key not in existing_keys:
            pending.append(row)
            existing_keys.add(key)

    def _ts_to_dt(ts: str):
        try:
            return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    pending.sort(key=lambda row: _ts_to_dt(row[0]))

    with out_csv.open(mode, newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if mode == "w" or out_csv.stat().st_size == 0:
            w.writerow([
                "ts","symbol","dir","entry","sl","tp","lots","status",
                "ticket","magic","spread_pts","slippage","exit","pips","profit","net","comment"
            ])
        w.writerows(pending)

    return {
        "ok": True,
        "markers": str(mpath),
        "csv": str(out_csv),
        "opens_appended": len(opens_rows),
        "closes_appended": len(closes_rows),
        "note": "Parsed by tolerant token scanner. If you want richer rows, enable direct CSV logging in the EA."
    }

@app.get("/bt/debug/markers_scan", dependencies=[Depends(require_api_key)])
def bt_debug_markers_scan(path: str | None = None):
    mpath = Path(path) if path else _bt_markers_default_path()
    if not _path_allowed(mpath):
        return {"ok": False, "error": "Absolute path not allowed (set DEBUG_ALLOW_ABS_PATHS=1 to enable)"}
    if not mpath.exists():
        return {"ok": False, "error": f"Markers not found: {mpath}"}

    open_hits = 0
    close_hits = 0
    sample_open = {}
    sample_close = {}

    ts_split = re.compile(r'^\s*(?P<ts>\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2})\s*\|\s*(?P<rest>.*)$')
    pat_open  = re.compile(r'order\s*ok', re.IGNORECASE)
    pat_close = re.compile(r'\bclose\b', re.IGNORECASE)

    total = 0
    for raw in _read_lines_guess_enc(mpath):
        total += 1
        line = raw.strip()
        if not line:
            continue
        parts = ts_split.match(line)
        rest = parts.group("rest").strip() if parts else line

        if (open_hits < 3) and pat_open.search(rest):
            open_hits += 1
            if open_hits == 1:
                sample_open = {"ts": (parts.group("ts") if parts else ""), "rest": rest}

        if (close_hits < 3) and pat_close.search(rest):
            close_hits += 1
            if close_hits == 1:
                sample_close = {"ts": (parts.group("ts") if parts else ""), "rest": rest}

    return {
        "ok": True,
        "path": str(mpath),
        "total_lines": total,
        "open_hits": open_hits,
        "close_hits": close_hits,
        "sample_open": sample_open,
        "sample_close": sample_close,
        "note": "Encoding-tolerant scan. If hits are 0 with visible lines in Notepad, please paste one example line here."
    }

@app.get("/bt/debug/session_start", dependencies=[Depends(require_api_key)])
def bt_debug_session_start(path: str | None = None):
    """
    Show what timestamp bt_summary will use when last_session=1.
    """
    mpath = Path(path) if path else _bt_markers_default_path()
    if not _path_allowed(mpath):
        return {"path": str(mpath), "error": "Absolute path not allowed (set DEBUG_ALLOW_ABS_PATHS=1 to enable)"}
    iso = _last_session_start_iso(mpath)
    return {"path": str(mpath), "last_session_start_iso": iso}

# --- RUNTIME CONTROLS (simple risk gates) ---
CONFIG = {
    "trading_enabled": True,
    "allowed_hours_utc": list(range(0, 24)),
    "max_trades_per_day": 20,
    "max_loss_streak_per_day": 5,
    "max_daily_loss_abs": 0.0,
    "default_sl_pips": 4.0,
    "default_tp_pips": 6.0,
    "symbol_overrides": {},
    "allowed_symbols": [],
    "symbol_hours": {},
    "symbol_caps": {},
    "max_account_daily_loss_abs": 0.0,
    "max_account_daily_profit_abs": 0.0,
    "account_cap_latch": False,
    "allowed_days_utc": [0,1,2,3,4,5,6],
    "symbol_days": {},
    "api_keys": [],

    # per-symbol cap latch
    "symbol_cap_latch": False,

    # concurrency + hedging
    "max_open_positions_global": 0,  # 0 = unlimited
    "no_hedge": False,

    # cool-off
    "cool_off_minutes_after_loss": 0,
    "cool_off_minutes_after_win": 0,
    "symbol_cool_off": {},

    # --- Strategy defaults (non-breaking; keeps toy mode unless changed via /config)
    "strategy_mode": "toy",  # "toy" or "ensemble"
    "strategies": {
        "breakout_london": True,
        "trend_ma": True,
        "meanrev_band": True,
    },
    "strategy_weights": {
        "breakout_london": 1.0,
        "trend_ma": 1.0,
        "meanrev_band": 1.0,
    },

    # optional: fixed SL/TP defaults for strategies if none returned
    "strategy_fallback_sl_pips": 6.0,
    "strategy_fallback_tp_pips": 9.0,
    "lob_session_start_utc": "07:00",
    "lob_session_end_utc": "13:00",
    
}

# per-day per-symbol state
TRADE_COUNT = defaultdict(int)
LOSS_STREAK = defaultdict(int)
DAILY_NET = defaultdict(float)
ACCOUNT_DAILY_NET = defaultdict(float)
ACCOUNT_LATCHED = defaultdict(bool)
SYMBOL_LATCHED = defaultdict(bool)
SYMBOL_COOLOFF_UNTIL = defaultdict(float)
OPEN_POS: dict[str, dict] = {}

def _open_pos_items_snapshot():
    with COUNTERS_LOCK:
        return list(OPEN_POS.items())

# Persistence
COUNTERS_PATH = Path(__file__).with_name("runtime_counters.json")
COUNTERS_LOCK = Lock()
AUTOSAVE_STOP = Event()
AUTOSAVE_THREAD: Thread | None = None
AUTOSAVE_INTERVAL_SEC = 30
CONFIG_PATH = Path(__file__).with_name("runtime_config.json")

def save_config():
    try:
        tmp = CONFIG_PATH.with_suffix(CONFIG_PATH.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(CONFIG, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        tmp.replace(CONFIG_PATH)
    except Exception as e:
        print(f"[PERSIST] save_config failed: {e}")

def load_config():
    if not CONFIG_PATH.exists():
        return
    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for k in list(CONFIG.keys()):
            if k in data:
                CONFIG[k] = data[k]
    except Exception as e:
        print(f"[PERSIST] load_config failed: {e}")

def utc_now_ts():
    return datetime.now(timezone.utc)

def today_utc_key(symbol: str) -> str:
    d = utc_now_ts().date()
    sym_u = (symbol or "").upper().strip()
    return f"{sym_u}|{d.isoformat()}"

def _effective_for_symbol(symbol: str) -> dict:
    sy = (symbol or "").upper().strip()
    hours = (CONFIG.get("symbol_hours") or {}).get(sy, []) or CONFIG.get("allowed_hours_utc", [])
    days  = (CONFIG.get("symbol_days")  or {}).get(sy, []) or CONFIG.get("allowed_days_utc", [])
    raw_caps = (CONFIG.get("symbol_caps") or {}).get(sy, {})
    caps = {
        "max_trades_per_day": int(raw_caps.get("max_trades_per_day", CONFIG.get("max_trades_per_day", 999_999))),
        "max_loss_streak_per_day": int(raw_caps.get("max_loss_streak_per_day", CONFIG.get("max_loss_streak_per_day", 999_999))),
        "max_daily_loss_abs": float(raw_caps.get("max_daily_loss_abs", CONFIG.get("max_daily_loss_abs", 0.0))),
        "max_daily_profit_abs": float(raw_caps.get("max_daily_profit_abs", 0.0)),
        "max_open_positions": int(raw_caps.get("max_open_positions", 0)),
        "cap_latch": bool(raw_caps.get("cap_latch", CONFIG.get("symbol_cap_latch", False))),
    }
    rules = {"no_hedge": bool(raw_caps.get("no_hedge", CONFIG.get("no_hedge", False)))}
    return {"hours": hours, "days": days, "caps": caps, "rules": rules}

def can_trade_now(symbol: str) -> tuple[bool, str]:
    if not CONFIG.get("trading_enabled", True):
        return (False, "disabled")

    sy = (symbol or "").upper().strip()

    allowed_syms = CONFIG.get("allowed_symbols", [])
    if allowed_syms:
        syms_up = [s.upper().strip() for s in allowed_syms if str(s).strip()]
        if sy not in syms_up:
            return (False, "symbol_blocked")

    hr = utc_now_ts().hour
    sym_u = (symbol or "").upper().strip()
    per_sym_hours = (CONFIG.get("symbol_hours") or {}).get(sym_u, [])
    allowed_hours = per_sym_hours if per_sym_hours else CONFIG.get("allowed_hours_utc", [])
    if allowed_hours and hr not in allowed_hours:
        return (False, f"hour_blocked_{hr:02d}Z")

    dow = datetime.now(timezone.utc).weekday()
    per_sym_days = (CONFIG.get("symbol_days") or {}).get(sym_u, [])
    allowed_days = per_sym_days if per_sym_days else CONFIG.get("allowed_days_utc", [])
    if allowed_days and dow not in allowed_days:
        return (False, f"day_blocked_{dow}")

    key = today_utc_key(symbol)
    eff = _effective_for_symbol(sy)
    caps = eff["caps"]
    max_trades_cap = int(caps.get("max_trades_per_day", CONFIG.get("max_trades_per_day", 999_999)))
    max_streak_cap = int(caps.get("max_loss_streak_per_day", CONFIG.get("max_loss_streak_per_day", 999_999)))
    daily_loss_cap = float(caps.get("max_daily_loss_abs", CONFIG.get("max_daily_loss_abs", 0.0)))
    sym_max_open   = int(caps.get("max_open_positions", 0))

    if TRADE_COUNT.get(key, 0) >= max_trades_cap:
        return (False, "daily_cap_reached")
    if LOSS_STREAK.get(key, 0) >= max_streak_cap:
        return (False, "loss_streak_cap")
    if daily_loss_cap > 0 and DAILY_NET.get(key, 0.0) <= -abs(daily_loss_cap):
        return (False, "daily_net_loss_cap")

    daily_profit_cap = float(caps.get("max_daily_profit_abs", 0.0))
    if daily_profit_cap > 0 and DAILY_NET.get(key, 0.0) >= abs(daily_profit_cap):
        return (False, "daily_net_profit_cap")

    sym_latch_enabled = bool(caps.get("cap_latch", False) or CONFIG.get("symbol_cap_latch", False))
    if sym_latch_enabled and bool(SYMBOL_LATCHED.get(key, False)):
        return (False, "symbol_latched")

    co_until = float(SYMBOL_COOLOFF_UNTIL.get(sy, 0.0) or 0.0)
    if co_until > time.time():
        return (False, "cooloff_active")

    open_total = len(OPEN_POS)
    open_for_symbol = 0
    for _k, meta in _open_pos_items_snapshot():
        if meta.get("symbol") == sy:
            open_for_symbol += 1
    if sym_max_open > 0 and open_for_symbol >= sym_max_open:
        return (False, "max_open_positions_symbol")

    global_cap = int(CONFIG.get("max_open_positions_global", 0))
    if global_cap > 0 and open_total >= global_cap:
        return (False, "max_open_positions_global")

    today_str = utc_now_ts().date().isoformat()
    acc_net = float(ACCOUNT_DAILY_NET.get(today_str, 0.0))
    if bool(CONFIG.get("account_cap_latch", False)) and bool(ACCOUNT_LATCHED.get(today_str, False)):
        return (False, "account_latched")
    acc_cap_loss = float(CONFIG.get("max_account_daily_loss_abs", 0.0))
    if acc_cap_loss > 0 and acc_net <= -abs(acc_cap_loss):
        return (False, "account_daily_net_loss_cap")
    acc_cap_profit = float(CONFIG.get("max_account_daily_profit_abs", 0.0))
    if acc_cap_profit > 0 and acc_net >= abs(acc_cap_profit):
        return (False, "account_daily_profit_cap")

    return (True, "ok")

def can_trade_explain(symbol: str) -> dict:
    now = utc_now_ts()
    sy  = (symbol or "").upper().strip()
    key = today_utc_key(sy)
    eff = _effective_for_symbol(sy)
    hours = eff["hours"]; days = eff["days"]; caps = eff["caps"]; rules = eff.get("rules", {})

    trades_today = int(TRADE_COUNT.get(key, 0))
    streak_today = int(LOSS_STREAK.get(key, 0))
    net_today    = float(DAILY_NET.get(key, 0.0))

    open_total = len(OPEN_POS)
    open_for_symbol = 0
    for _k, meta in _open_pos_items_snapshot():
        if meta.get("symbol") == sy:
            open_for_symbol += 1

    checks = []
    enabled = bool(CONFIG.get("trading_enabled", True))
    checks.append({"name": "trading_enabled", "ok": enabled})

    allowed_syms = [s.upper().strip() for s in (CONFIG.get("allowed_symbols") or []) if str(s).strip()]
    sym_ok = True if not allowed_syms else (sy in allowed_syms)
    checks.append({"name": "symbol_allowed", "ok": sym_ok, "allowed_symbols": allowed_syms})

    today_py = now.weekday()
    day_ok = True if not days else (today_py in days)
    checks.append({"name": "day_gate", "ok": day_ok, "today_py": today_py, "allowed_days_utc": days})

    hr = now.hour
    hour_ok = True if not hours else (hr in hours)
    checks.append({"name": "hour_gate", "ok": hour_ok, "now_hour_utc": hr, "allowed_hours_utc": hours})

    cap_trades_ok = trades_today < caps["max_trades_per_day"]
    checks.append({"name": "daily_open_cap", "ok": cap_trades_ok, "value": trades_today, "limit": caps["max_trades_per_day"]})

    cap_streak_ok = streak_today < caps["max_loss_streak_per_day"]
    checks.append({"name": "loss_streak_cap", "ok": cap_streak_ok, "value": streak_today, "limit": caps["max_loss_streak_per_day"]})

    cap_loss_ok = not (caps["max_daily_loss_abs"] > 0 and net_today <= -abs(caps["max_daily_loss_abs"]))
    checks.append({"name": "daily_net_loss_cap", "ok": cap_loss_ok, "value": net_today, "limit": -abs(caps["max_daily_loss_abs"])})

    cap_profit_ok = not (float(caps.get("max_daily_profit_abs", 0.0)) > 0 and net_today >= abs(float(caps.get("max_daily_profit_abs", 0.0))))
    checks.append({"name": "daily_net_profit_cap", "ok": cap_profit_ok, "value": net_today, "limit": abs(float(caps.get("max_daily_profit_abs", 0.0)))})

    co_until = float(SYMBOL_COOLOFF_UNTIL.get(sy, 0.0))
    now_ts_num = time.time()
    co_ok = not (co_until > now_ts_num)
    checks.append({
        "name": "cooloff_active",
        "ok": co_ok,
        "cooloff_until_unix": co_until,
        "sec_remaining": max(0, int(co_until - now_ts_num))
    })

    sym_max_open = int(caps.get("max_open_positions", 0))
    sym_open_ok  = not (sym_max_open > 0 and open_for_symbol >= sym_max_open)
    checks.append({"name": "max_open_positions_symbol", "ok": sym_open_ok, "value": open_for_symbol, "limit": sym_max_open})

    global_cap = int(CONFIG.get("max_open_positions_global", 0))
    global_open_ok = not (global_cap > 0 and open_total >= global_cap)
    checks.append({"name": "max_open_positions_global", "ok": global_open_ok, "value": open_total, "limit": global_cap})

    no_hedge_eff = bool(rules.get("no_hedge", CONFIG.get("no_hedge", False)))
    with COUNTERS_LOCK:
        _snapshot = list(OPEN_POS.values())
    has_buy_open  = any(meta.get("symbol") == sy and int(meta.get("direction") or 0) == 1 for meta in _snapshot)
    has_sell_open = any(meta.get("symbol") == sy and int(meta.get("direction") or 0) == -1 for meta in _snapshot)
    hedge_ok = not (no_hedge_eff and has_buy_open and has_sell_open)
    checks.append({
        "name": "no_hedge_rule",
        "ok": hedge_ok,
        "enabled": no_hedge_eff,
        "has_buy_open": has_buy_open,
        "has_sell_open": has_sell_open
    })

    sym_caps_cfg = (CONFIG.get("symbol_caps") or {}).get(sy, {}) or {}
    latch_on_symbol = (
        bool(CONFIG.get("symbol_cap_latch", False)) or
        bool(sym_caps_cfg.get("cap_latch", False)) or
        bool(caps.get("cap_latch", False))
    )
    sym_latched = bool(SYMBOL_LATCHED.get(key, False))
    sym_latch_ok = not (latch_on_symbol and sym_latched)
    checks.append({"name": "symbol_latched", "ok": sym_latch_ok, "latched": sym_latched, "latch_enabled": latch_on_symbol})

    acc_today = float(ACCOUNT_DAILY_NET.get(now.date().isoformat(), 0.0))
    acc_cap_loss = float(CONFIG.get("max_account_daily_loss_abs", 0.0))
    acc_cap_profit = float(CONFIG.get("max_account_daily_profit_abs", 0.0))
    latch_enabled = bool(CONFIG.get("account_cap_latch", False))
    latched = bool(ACCOUNT_LATCHED.get(now.date().isoformat(), False))

    latch_ok = not (latch_enabled and latched)
    checks.append({"name": "account_latched", "ok": latch_ok, "latched": latched, "latch_enabled": latch_enabled})

    loss_ok = not (acc_cap_loss > 0 and acc_today <= -abs(acc_cap_loss))
    checks.append({"name": "account_daily_net_loss_cap", "ok": loss_ok, "value": acc_today, "limit": -abs(acc_cap_loss)})

    profit_ok = not (acc_cap_profit > 0 and acc_today >= abs(acc_cap_profit))
    checks.append({"name": "account_daily_profit_cap", "ok": profit_ok, "value": acc_today, "limit": abs(acc_cap_profit)})

    overall_ok = all(ch["ok"] for ch in checks)
    reason = "ok" if overall_ok else next(ch["name"] for ch in checks if not ch["ok"])

    ov = (CONFIG.get("symbol_overrides") or {}).get(sy, {})
    eff_sl = float(ov["sl_pips"]) if "sl_pips" in ov and ov["sl_pips"] is not None else float(CONFIG.get("default_sl_pips", 0.0))
    eff_tp = float(ov["tp_pips"]) if "tp_pips" in ov and ov["tp_pips"] is not None else float(CONFIG.get("default_tp_pips", 0.0))

    return {
        "symbol": sy,
        "utc_now": now.isoformat(),
        "result": {"ok": overall_ok, "reason": reason},
        "effective": {"hours": hours, "days": days, "caps": caps},
        "effective_rules": {"no_hedge": bool(rules.get("no_hedge", CONFIG.get("no_hedge", False)))},
        "effective_account": {
            "max_account_daily_loss_abs": float(CONFIG.get("max_account_daily_loss_abs", 0.0)),
            "max_account_daily_profit_abs": float(CONFIG.get("max_account_daily_profit_abs", 0.0)),
            "max_open_positions_global": int(CONFIG.get("max_open_positions_global", 0)),
            "account_cap_latch": latch_enabled,
            "latched_today": latched,
            "account_net_today": acc_today
        },
        "open_positions": {"symbol": open_for_symbol, "global": open_total},
        "counters_today": {"TRADE_COUNT": trades_today, "LOSS_STREAK": streak_today, "DAILY_NET": net_today},
        "checks": checks,
        "effective_sl_tp": {"sl_pips": eff_sl, "tp_pips": eff_tp},
    }

@app.get("/debug/openpos", dependencies=[Depends(require_api_key)])
def debug_openpos():
    by_symbol: dict[str, int] = {}
    for k, meta in _open_pos_items_snapshot():
        sy = meta.get("symbol", "")
        if sy:
            by_symbol[sy] = by_symbol.get(sy, 0) + 1
    return {
        "total": len(OPEN_POS),
        "by_symbol": by_symbol,
        "items": [{"key": k, "symbol": v.get("symbol"), "direction": int(v.get("direction") or 0)} for k, v in _open_pos_items_snapshot()]
    }

@app.get("/debug/cantrade", dependencies=[Depends(require_api_key)])
def debug_cantrade(symbol: str = "EURUSD"):
    # base result from in-memory view
    result = can_trade_explain(symbol)

    # --- BEGIN: enrich no_hedge rule with side visibility (file-backed sides) ---
    try:
        c = _load_counters()
        _ensure_side_structs(c)

        sym = str(symbol).upper().strip()
        sides = c.get("OPEN_POS_SIDES", {}).get(sym, {})
        has_buy_open = bool(int(sides.get("buy", 0)) > 0)
        has_sell_open = bool(int(sides.get("sell", 0)) > 0)

        checks = result.get("checks", [])
        updated = False
        for chk in checks:
            if chk.get("name") == "no_hedge_rule":
                chk["has_buy_open"] = has_buy_open
                chk["has_sell_open"] = has_sell_open
                updated = True
                break
        if not updated:
            checks.append({
                "name": "no_hedge_rule",
                "ok": True,
                "enabled": bool(result.get("effective_rules", {}).get("no_hedge", False)),
                "has_buy_open": has_buy_open,
                "has_sell_open": has_sell_open,
            })
        result["checks"] = checks

        op = result.get("open_positions", {}) or {}
        op["symbol"] = int(c.get("OPEN_POS_BY_SYMBOL", {}).get(sym, 0))
        op["global"] = int(c.get("OPEN_POS_TOTAL", 0))
        result["open_positions"] = op
    except Exception:
        pass
    # --- END: enrich no_hedge rule with side visibility ---

    return result

def _atomic_write_json(path: Path, data: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)

def _serialize_counters() -> dict:
    return {
        "TRADE_COUNT": dict(TRADE_COUNT),
        "LOSS_STREAK": dict(LOSS_STREAK),
        "DAILY_NET":   dict(DAILY_NET),
        "ACCOUNT_DAILY_NET": dict(ACCOUNT_DAILY_NET),
        "ACCOUNT_LATCHED": {k: bool(v) for k, v in ACCOUNT_LATCHED.items()},
        "SYMBOL_LATCHED": {k: bool(v) for k, v in SYMBOL_LATCHED.items()},
        "SYMBOL_COOLOFF_UNTIL": dict(SYMBOL_COOLOFF_UNTIL),
        "OPEN_POS": OPEN_POS,
        "updated_utc": datetime.now(timezone.utc).isoformat()
    }

def _prune_old(days:int=3):
    today = datetime.now(timezone.utc).date()
    keep_suffixes = { (today).isoformat() }
    for i in range(1, days):
        keep_suffixes.add((today - timedelta(days=i)).isoformat())

    def prune(d:dict):
        drop = [k for k in d.keys() if not any(k.endswith(sfx) for sfx in keep_suffixes)]
        for k in drop:
            d.pop(k, None)

    prune(TRADE_COUNT)
    prune(LOSS_STREAK)
    prune(DAILY_NET)
    prune(ACCOUNT_DAILY_NET)
    prune(ACCOUNT_LATCHED)
    prune(SYMBOL_LATCHED)

    now_ts = time.time()
    to_drop = [s for s, ts in SYMBOL_COOLOFF_UNTIL.items() if float(ts) <= now_ts]
    for s in to_drop:
        SYMBOL_COOLOFF_UNTIL.pop(s, None)

def save_counters():
    with COUNTERS_LOCK:
        _prune_old(3)
        data = _serialize_counters()
        try:
            _atomic_write_json(COUNTERS_PATH, data)
        except Exception as e:
            print(f"[PERSIST] save failed: {e}")

def load_counters():
    if not COUNTERS_PATH.exists():
        return
    try:
        with COUNTERS_LOCK, COUNTERS_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        TRADE_COUNT.clear(); LOSS_STREAK.clear(); DAILY_NET.clear()
        for k, v in (data.get("TRADE_COUNT") or {}).items():
            TRADE_COUNT[k] = int(v)
        for k, v in (data.get("LOSS_STREAK") or {}).items():
            LOSS_STREAK[k] = int(v)
        for k, v in (data.get("DAILY_NET") or {}).items():
            DAILY_NET[k] = float(v)
        for d, v in (data.get("ACCOUNT_DAILY_NET") or {}).items():
            ACCOUNT_DAILY_NET[d] = float(v)

        ACCOUNT_LATCHED.clear()
        for d, v in (data.get("ACCOUNT_LATCHED") or {}).items():
            ACCOUNT_LATCHED[d] = bool(v)

        SYMBOL_LATCHED.clear()
        for k, v in (data.get("SYMBOL_LATCHED") or {}).items():
            SYMBOL_LATCHED[k] = bool(v)

        SYMBOL_COOLOFF_UNTIL.clear()
        for s, ts in (data.get("SYMBOL_COOLOFF_UNTIL") or {}).items():
            try:
                SYMBOL_COOLOFF_UNTIL[str(s).upper()] = float(ts)
            except:
                pass

        OPEN_POS.clear()
        for k, v in (data.get("OPEN_POS") or {}).items():
            try:
                sym_u = str(v.get("symbol","")).upper().strip()
                ddir  = int(v.get("direction") or 0)
                if sym_u and str(k).strip():
                    OPEN_POS[str(k).strip()] = {"symbol": sym_u, "direction": ddir}
            except:
                pass

        _prune_old(3)
    except Exception as e:
        print(f"[PERSIST] load failed: {e}")

def _autosave_loop():
    while not AUTOSAVE_STOP.is_set():
        AUTOSAVE_STOP.wait(AUTOSAVE_INTERVAL_SEC)
        if AUTOSAVE_STOP.is_set():
            break
        try:
            save_counters()
        except Exception as e:
            print(f"[PERSIST] autosave failed: {e}")

# Try .env in brains\ first, then in project root
ENV_PATH_USED = None
ENV_CANDIDATES = [
    Path(__file__).with_name("API.env"),
    Path(__file__).with_name(".env"),
    Path(__file__).resolve().parent.parent / ".env",
    Path(__file__).resolve().parent.parent / "API.env",
]
for _env in ENV_CANDIDATES:
    if _env.exists():
        load_dotenv(dotenv_path=_env, override=True)
        ENV_PATH_USED = str(_env)
        break

FALLBACK_SHEET_ID = "1SyJu6jLoqVcA9Uc2iQ4D6LgmIrLdullANeJ-ULIUmmk"
SHEET_ID = os.getenv("SHEET_ID") or FALLBACK_SHEET_ID
SHEET_TRADES_TAB = os.getenv("SHEET_TRADES_TAB", "trades_raw")

sheets = None
CREDS_PATH_USED = None
if SHEET_ID:
    creds_candidates = [
        Path(__file__).with_name("service_account.json"),
        Path(__file__).resolve().parent.parent / "service_account.json",
    ]
    for p in creds_candidates:
        if p.exists():
            CREDS_PATH_USED = str(p)
            break
    try:
        if CREDS_PATH_USED:
            sheets = SheetsClient(sheet_id=SHEET_ID, creds_path=CREDS_PATH_USED)
            print(f"[SHEETS] Connected to {SHEET_ID}, tab={SHEET_TRADES_TAB}, creds={CREDS_PATH_USED}, env={ENV_PATH_USED}")
        else:
            print("[SHEETS] No service_account.json found in brains\\ or project root.")
    except Exception as e:
        print(f"[SHEETS] Failed to init: {e}")
        sheets = None


@app.get("/debug/sheets", dependencies=[Depends(require_api_key)])
def debug_sheets():
    """
    Environment & Sheets wiring status (non-sensitive, masked IDs).
    """
    sheet_id = SHEET_ID or ""
    masked_id = (sheet_id[:6] + "..." + sheet_id[-4:]) if sheet_id else ""
    brains_creds = Path(__file__).with_name("service_account.json")
    root_creds = Path(__file__).resolve().parent.parent / "service_account.json"

    info = {
        "cwd": os.getcwd(),
        "env_path_used": ENV_PATH_USED,
        "env_sheet_id_present": bool(sheet_id),
        "env_sheet_id_masked": masked_id,
        "trades_tab": SHEET_TRADES_TAB,
        "service_account_in_brains": brains_creds.exists(),
        "service_account_in_root": root_creds.exists(),
        "creds_path_used": CREDS_PATH_USED,
        "sheets_connected": sheets is not None,
        "worksheets": [],
        "trades_tab_exists": None,
        "note": "IDs masked; this endpoint is for local debugging only.",
    }
    try:
        if sheets is not None:
            titles = [ws.title for ws in sheets.sh.worksheets()]
            info["worksheets"] = titles
            info["trades_tab_exists"] = (SHEET_TRADES_TAB in titles)
    except Exception as e:
        info["error"] = f"{type(e).__name__}: {e}"

    return info


@app.get("/debug/sheets/schema", dependencies=[Depends(require_api_key)])
def debug_sheets_schema():
    """
    Returns the header row (sheet schema) and a couple of sample rows
    for the configured trades tab. Useful to verify column order.
    """
    if sheets is None:
        return {"ok": False, "error": "Sheets client not connected"}
    try:
        ws = sheets.sh.worksheet(SHEET_TRADES_TAB)
        header = ws.row_values(1)

        # Grab a few rows after header and a few non-empty rows from the end
        all_vals = ws.get_all_values()
        sample_top = all_vals[1:6]  # rows 2..6
        non_empty = [r for r in all_vals[1:] if any((c or "").strip() for c in r)]
        sample_bottom = non_empty[-5:] if len(non_empty) >= 5 else non_empty

        return {
            "ok": True,
            "sheet_id_masked": (SHEET_ID[:6] + "..." + SHEET_ID[-4:]) if SHEET_ID else "",
            "tab": SHEET_TRADES_TAB,
            "header": header,
            "sample_top": sample_top,
            "sample_bottom": sample_bottom,
            "note": "header is the schema; rows shown for context"
        }
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

@app.post("/admin/sheets/backfill_trade_date", dependencies=[Depends(require_api_key)])
def admin_backfill_trade_date(start_row: int = 2, max_rows: int = 5000, dry_run: int = 1):
    """
    Backfills the last 'trade-date' column using the 'ts' column.
    - Only fills rows where the trade-date cell is blank or looks like an error (#VALUE!, #REF!).
    - Parses multiple TS formats:
        * ISO:            2025-09-14T16:33:36.084798+00:00  (or ...Z)
        * Dotted:         2025.08.18 00:17:39
        * DMY 24h:        18-08-2025 05:47
    Args (query params):
      - start_row: first data row to check (default 2; row 1 is header)
      - max_rows:  safety cap (default 5000)
      - dry_run:   1 = report only, 0 = write values
    """
    if sheets is None:
        return {"ok": False, "error": "Sheets client not connected"}

    try:
        ws = sheets.sh.worksheet(SHEET_TRADES_TAB)
        header = ws.row_values(1)
        if not header:
            return {"ok": False, "error": "Empty header row"}

        # Guardrail: the last column MUST be literally "trade-date"
        if header[-1] != "trade-date":
            return {
                "ok": False,
                "error": "Header mismatch: last column is not 'trade-date'",
                "seen_last_header": header[-1],
                "header": header,
            }

        # Where are our columns?
        try:
            ts_idx = header.index("ts") + 1        # 1-based
        except ValueError:
            return {"ok": False, "error": "Header missing 'ts' column"}
        try:
            td_idx = header.index("trade-date") + 1
        except ValueError:
            return {"ok": False, "error": "Header missing 'trade-date' column"}

        # Helper: A1 column name
        def _col_letter(n: int) -> str:
            s = ""
            while n > 0:
                n, r = divmod(n - 1, 26)
                s = chr(65 + r) + s
            return s

        # Pull a window of rows
        last_row = ws.row_count
        end_row = min(last_row, start_row - 1 + max_rows)
        if end_row < start_row:
            return {"ok": True, "checked": 0, "changed": 0, "note": "No rows in range"}

        # Read TS + trade-date slices
        ts_col_letter = _col_letter(ts_idx)
        td_col_letter = _col_letter(td_idx)
        # Worksheet-level reads must NOT include the tab name
        ts_range = f"{ts_col_letter}{start_row}:{ts_col_letter}{end_row}"
        td_range = f"{td_col_letter}{start_row}:{td_col_letter}{end_row}"

        ts_vals = ws.get(ts_range)  # list of lists (one cell per row)
        td_vals = ws.get(td_range)

        # Normalize to equal length
        rows_in_window = end_row - start_row + 1
        def _pad(vals):
            out = list(vals or [])
            while len(out) < rows_in_window:
                out.append([])
            return out
        ts_vals = _pad(ts_vals)
        td_vals = _pad(td_vals)

        # Parsers
        import re
        from datetime import datetime, timezone

        iso_re = re.compile(r'^\d{4}-\d{2}-\d{2}')
        dot_re = re.compile(r'^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}:\d{2}$')
        dmy_re = re.compile(r'^\d{2}-\d{2}-\d{4}\s+\d{2}:\d{2}$')

        def _parse_to_ymd(ts: str) -> str | None:
            t = (ts or "").strip()
            if not t:
                return None
            # ISO variants (with Z or offset)
            if iso_re.match(t):
                try:
                    # Try strict Z first
                    if t.endswith("Z"):
                        dt = datetime.strptime(t[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                    else:
                        # strip microseconds / timezone safely
                        # take first 19 chars YYYY-mm-ddTHH:MM:SS
                        dt = datetime.strptime(t[:19], "%Y-%m-%dT%H:%M:%S")
                    return dt.date().isoformat()
                except Exception:
                    # Fallback: just take YYYY-mm-dd
                    return t[:10]
            # Dotted: 2025.08.18 00:17:39
            if dot_re.match(t):
                try:
                    dt = datetime.strptime(t, "%Y.%m.%d %H:%M:%S")
                    return dt.date().isoformat()
                except Exception:
                    return None
            # DMY: 18-08-2025 05:47
            if dmy_re.match(t):
                try:
                    dt = datetime.strptime(t, "%d-%m-%Y %H:%M")
                    return dt.date().isoformat()
                except Exception:
                    return None
            # If ts already looks like a date literal in sheet
            if re.match(r'^\d{4}-\d{2}-\d{2}$', t):
                return t
            return None

        def _is_error_or_empty(s: str) -> bool:
            v = (s or "").strip().upper()
            return (v == "" or v == "#VALUE!" or v == "#REF!" or v.startswith("#N/A"))

        updates = []
        changed = 0
        checked = 0

        for i in range(rows_in_window):
            checked += 1
            ts_cell = ts_vals[i][0] if ts_vals[i] else ""
            td_cell = td_vals[i][0] if td_vals[i] else ""

            if not _is_error_or_empty(td_cell):
                continue  # leave existing values alone

            ymd = _parse_to_ymd(ts_cell)
            if ymd:
                # Build A1 for the trade-date cell at this row
                row_num = start_row + i
                # Spreadsheet-level batch update needs full A1 with sheet tab
                a1 = f"'{SHEET_TRADES_TAB}'!{td_col_letter}{row_num}"
                updates.append({"range": a1, "values": [[ymd]]})
                changed += 1

        if dry_run:
            return {
                "ok": True,
                "tab": SHEET_TRADES_TAB,
                "start_row": start_row,
                "end_row": end_row,
                "checked": checked,
                "would_change": changed,
                "dry_run": True
            }

                # Apply updates in batches to be kind to the API
        if updates:
            try:
                # Worksheet-scoped values batch update (calls Sheets Values API)
                to_send = []
                for u in updates:
                    rng = u["range"]
                    # Convert "'Tab'!A1" → "A1" because Worksheet.* ranges are local
                    if "!" in rng:
                        rng = rng.split("!", 1)[1]
                    to_send.append({"range": rng, "values": u["values"]})

                # gspread Worksheet.batch_update → values:batchUpdate under the hood
                ws.batch_update(to_send, value_input_option="RAW")

            except Exception as e:
                # Fallback: per-cell updates (slower but reliable)
                try:
                    for u in updates:
                        rng = u["range"]
                        if "!" in rng:
                            rng = rng.split("!", 1)[1]
                        ws.update(rng, u["values"], value_input_option="RAW")
                except Exception as e2:
                    return {
                        "ok": False,
                        "error": f"BatchUpdateError: {type(e).__name__}: {e}; FallbackError: {type(e2).__name__}: {e2}"
                    }

        return {
            "ok": True,
            "tab": SHEET_TRADES_TAB,
            "start_row": start_row,
            "end_row": end_row,
            "checked": checked,
            "changed": changed,
            "dry_run": False
        }


    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@app.get("/health")
def health():
    return {"ok": True, "utc": datetime.now(timezone.utc).isoformat()}

@app.get("/debug/echo", dependencies=[Depends(require_api_key)])
def debug_echo(req: Request):
    # Helps confirm headers MT5 actually sends and which key path succeeded
    hdrs = {k.lower(): v for k, v in req.headers.items()}
    # Determine which vector seems to have been used
    if hdrs.get("x-api-key"):
        auth_via = "header:x-api-key"
        used = hdrs.get("x-api-key")
    elif hdrs.get("authorization"):
        auth_via = "header:authorization"
        used = hdrs.get("authorization")
    elif "api_key" in req.query_params or "key" in req.query_params:
        auth_via = "query:param"
        used = req.query_params.get("api_key") or req.query_params.get("key")
    else:
        auth_via = "unknown"
        used = ""

    def _mask_secret(s: str | None) -> str:
        if not s:
            return ""
        s = str(s)
        if len(s) <= 6:
            return "***"
        return s[:3] + "..." + s[-3:]

    return {
        "ok": True,
        "auth_via": auth_via,
        "used_value": _mask_secret(used),
        "seen_x_api_key": _mask_secret(hdrs.get("x-api-key")),
        "seen_authorization": _mask_secret(hdrs.get("authorization")),
        "query_params": dict(req.query_params),
        "auth_note": "Both X-API-Key and Authorization: Bearer are accepted; query ?api_key= also allowed.",
        "time": datetime.now(timezone.utc).isoformat(),
    }

@app.post("/admin/flush", dependencies=[Depends(require_api_key)])
def admin_flush():
    save_config()
    save_counters()
    return {
        "ok": True,
        "config_path": str(CONFIG_PATH),
        "counters_path": str(COUNTERS_PATH),
        "utc": datetime.now(timezone.utc).isoformat()
    }

@app.get("/counters", dependencies=[Depends(require_api_key)])
def counters():
    today = datetime.now(timezone.utc).date().isoformat()
    now_ts = time.time()
    cool = {
        s: {"until_unix": float(ts), "sec_remaining": max(0, int(float(ts) - now_ts))}
        for s, ts in SYMBOL_COOLOFF_UNTIL.items() if float(ts) > now_ts
    }
    open_by_symbol: dict[str,int] = {}
    for _k, meta in _open_pos_items_snapshot():
        sy = meta.get("symbol","")
        if sy:
            open_by_symbol[sy] = open_by_symbol.get(sy, 0) + 1
    return {
        "file": str(COUNTERS_PATH),
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "today": today,
        "TRADE_COUNT": {k: v for k, v in TRADE_COUNT.items() if k.endswith(today)},
        "LOSS_STREAK": {k: v for k, v in LOSS_STREAK.items() if k.endswith(today)},
        "DAILY_NET": {k: v for k, v in DAILY_NET.items() if k.endswith(today)},
        "ACCOUNT_DAILY_NET": {today: ACCOUNT_DAILY_NET.get(today, 0.0)},
        "ACCOUNT_LATCHED": {today: bool(ACCOUNT_LATCHED.get(today, False))},
        "SYMBOL_LATCHED_TODAY": {
            k.split("|",1)[0]: bool(v)
            for k, v in SYMBOL_LATCHED.items() if k.endswith(today)
        },
        "SYMBOL_COOLOFF": cool,
        "OPEN_POS_TOTAL": len(OPEN_POS),
        "OPEN_POS_BY_SYMBOL": open_by_symbol,
    }

class ConfigUpdate(BaseModel):
    trading_enabled: Optional[bool] = None
    allowed_hours_utc: Optional[list[int]] = None
    allowed_days_utc: Optional[list[int]] = None   # <— add this
    max_trades_per_day: Optional[int] = None
    max_loss_streak_per_day: Optional[int] = None
    max_daily_loss_abs: Optional[float] = None

    # Global (account) caps
    max_account_daily_loss_abs: Optional[float] = None
    max_account_daily_profit_abs: Optional[float] = None
    account_cap_latch: Optional[bool] = None

    # Symbol-level controls
    allowed_symbols: Optional[list[str]] = None
    symbol_hours: Optional[dict[str, list[int]]] = None
    symbol_days: Optional[dict[str, list[int]]] = None
    symbol_overrides: Optional[dict] = None
    symbol_caps: Optional[dict] = None
    symbol_cap_latch: Optional[bool] = None

    # Defaults
    default_sl_pips: Optional[float] = None
    default_tp_pips: Optional[float] = None

    # Cool-off
    cool_off_minutes_after_loss: Optional[int] = None
    cool_off_minutes_after_win: Optional[int] = None
    symbol_cool_off: Optional[dict[str, dict[str, int]]] = None

    # Concurrency / hedging
    max_open_positions_global: Optional[int] = None
    no_hedge: Optional[bool] = None

    # API keys
    api_keys: Optional[list[str]] = None

    # Reset
    reset_counters: Optional[bool] = None

    # Strategies (ensemble)
    strategy_mode: Optional[str] = None              # "toy" or "ensemble"
    strategies: Optional[dict] = None                # {"breakout_london": true, ...}
    strategy_weights: Optional[dict] = None          # {"breakout_london": 1.0, ...}
    strategy_fallback_sl_pips: Optional[float] = None
    strategy_fallback_tp_pips: Optional[float] = None
    lob_session_start_utc: Optional[str] = None
    lob_session_end_utc: Optional[str] = None


@app.get("/config", dependencies=[Depends(require_api_key)])
def get_config():
    today = datetime.now(timezone.utc).date().isoformat()
    today_counts = {k: v for k, v in TRADE_COUNT.items() if k.endswith(today)}
    today_streaks = {k: v for k, v in LOSS_STREAK.items() if k.endswith(today)}
    today_net = {k: v for k, v in DAILY_NET.items() if k.endswith(today)}
    return {
        "config": CONFIG,
        "today_utc": datetime.now(timezone.utc).isoformat(),
        "today_counts": today_counts,
        "today_loss_streaks": today_streaks,
        "today_daily_net": today_net,
        "keys_note": "keys are 'SYMBOL|YYYY-MM-DD' in UTC"
    }

@app.post("/config", dependencies=[Depends(require_api_key)])
async def update_config(update: ConfigUpdate):
    if update.trading_enabled is not None:
        CONFIG["trading_enabled"] = bool(update.trading_enabled)
    if update.allowed_hours_utc is not None:
        try:
            hours = sorted({int(h) for h in update.allowed_hours_utc if 0 <= int(h) <= 23})
            CONFIG["allowed_hours_utc"] = hours
        except Exception:
            return {"ok": False, "error": "allowed_hours_utc must be a list of integers 0..23"}
    if update.allowed_symbols is not None:
        try:
            syms = sorted({str(s).upper().strip() for s in update.allowed_symbols if str(s).strip()})
            CONFIG["allowed_symbols"] = syms
        except Exception:
            return {"ok": False, "error": "allowed_symbols must be a list of strings"}
    if getattr(update, "allowed_days_utc", None) is not None:
        try:
            days = sorted({int(d) for d in (update.allowed_days_utc or []) if 0 <= int(d) <= 6})
            CONFIG["allowed_days_utc"] = days
        except Exception:
            return {"ok": False, "error": "allowed_days_utc must be a list of integers 0..6"}
    if getattr(update, "symbol_days", None) is not None:
        try:
            norm_days: dict[str, list[int]] = {}
            for sym, days in (update.symbol_days or {}).items():
                s = (str(sym) or "").strip().upper()
                if not s:
                    continue
                sanitized = sorted({int(d) for d in (days or []) if 0 <= int(d) <= 6})
                norm_days[s] = sanitized
            CONFIG["symbol_days"] = norm_days
        except Exception:
            return {"ok": False, "error": 'symbol_days must look like {"EURUSD":[0,1,2,3,4]}'}
    if update.max_trades_per_day is not None:
        try:
            CONFIG["max_trades_per_day"] = max(0, int(update.max_trades_per_day))
        except Exception:
            return {"ok": False, "error": "max_trades_per_day must be an integer"}
    if update.max_loss_streak_per_day is not None:
        try:
            CONFIG["max_loss_streak_per_day"] = max(0, int(update.max_loss_streak_per_day))
        except Exception:
            return {"ok": False, "error": "max_loss_streak_per_day must be an integer"}
    if update.max_daily_loss_abs is not None:
        try:
            val = float(update.max_daily_loss_abs)
            CONFIG["max_daily_loss_abs"] = max(0.0, val)
        except Exception:
            return {"ok": False, "error": "max_daily_loss_abs must be a number"}
    if getattr(update, "max_account_daily_loss_abs", None) is not None:
        try:
            val = float(update.max_account_daily_loss_abs)
            CONFIG["max_account_daily_loss_abs"] = max(0.0, val)
        except Exception:
            return {"ok": False, "error": "max_account_daily_loss_abs must be a number"}
    if getattr(update, "max_account_daily_profit_abs", None) is not None:
        try:
            val = float(update.max_account_daily_profit_abs)
            CONFIG["max_account_daily_profit_abs"] = max(0.0, val)
        except Exception:
            return {"ok": False, "error": "max_account_daily_profit_abs must be a number"}
    if getattr(update, "account_cap_latch", None) is not None:
        CONFIG["account_cap_latch"] = bool(update.account_cap_latch)
    if getattr(update, "symbol_cap_latch", None) is not None:
        CONFIG["symbol_cap_latch"] = bool(update.symbol_cap_latch)
    if getattr(update, "max_open_positions_global", None) is not None:
        try:
            CONFIG["max_open_positions_global"] = max(0, int(update.max_open_positions_global))
        except Exception:
            return {"ok": False, "error": "max_open_positions_global must be an integer >= 0"}
    if getattr(update, "no_hedge", None) is not None:
        CONFIG["no_hedge"] = bool(update.no_hedge)
    if update.default_sl_pips is not None:
        try:
            CONFIG["default_sl_pips"] = max(0.0, float(update.default_sl_pips))
        except Exception:
            return {"ok": False, "error": "default_sl_pips must be a number"}
    if getattr(update, "symbol_hours", None) is not None:
        try:
            norm_hours: dict[str, list[int]] = {}
            for sym, hours in (update.symbol_hours or {}).items():
                s = (str(sym) or "").strip().upper()
                if not s:
                    continue
                sanitized = sorted({int(h) for h in (hours or []) if 0 <= int(h) <= 23})
                norm_hours[s] = sanitized
            CONFIG["symbol_hours"] = norm_hours
        except Exception:
            return {"ok": False, "error": 'symbol_hours must look like {"EURUSD":[6,7,8]}'}
    if update.default_tp_pips is not None:
        try:
            CONFIG["default_tp_pips"] = max(0.0, float(update.default_tp_pips))
        except Exception:
            return {"ok": False, "error": "default_tp_pips must be a number"}
    if getattr(update, "cool_off_minutes_after_loss", None) is not None:
        try:
            CONFIG["cool_off_minutes_after_loss"] = max(0, int(update.cool_off_minutes_after_loss))
        except Exception:
            return {"ok": False, "error": "cool_off_minutes_after_loss must be an integer >= 0"}
    if getattr(update, "cool_off_minutes_after_win", None) is not None:
        try:
            CONFIG["cool_off_minutes_after_win"] = max(0, int(update.cool_off_minutes_after_win))
        except Exception:
            return {"ok": False, "error": "cool_off_minutes_after_win must be an integer >= 0"}
    if getattr(update, "symbol_cool_off", None) is not None:
        try:
            norm: dict[str, dict[str, int]] = {}
            for sym, cfg in (update.symbol_cool_off or {}).items():
                if not isinstance(cfg, dict):
                    continue
                s = (str(sym) or "").strip().upper()
                if not s:
                    continue
                a_loss = max(0, int(cfg.get("after_loss_min", 0) or 0))
                a_win  = max(0, int(cfg.get("after_win_min", 0) or 0))
                norm[s] = {"after_loss_min": a_loss, "after_win_min": a_win}
            CONFIG["symbol_cool_off"] = norm
        except Exception:
            return {"ok": False, "error": 'symbol_cool_off must look like {"EURUSD":{"after_loss_min":15,"after_win_min":0}}'}

    if getattr(update, "api_keys", None) is not None:
        try:
            keys = [str(x).strip() for x in (update.api_keys or []) if str(x).strip()]
            CONFIG["api_keys"] = keys
        except Exception:
            return {"ok": False, "error": "api_keys must be a list of strings"}
    if getattr(update, "symbol_caps", None) is not None:
        try:
            norm_caps: dict[str, dict[str, object]] = {}
            for sym, cap in (update.symbol_caps or {}).items():
                if not isinstance(cap, dict):
                    continue
                s = (str(sym) or "").strip().upper()
                if not s:
                    continue
                item: dict[str, object] = {}
                if "max_trades_per_day" in cap and cap["max_trades_per_day"] is not None:
                    item["max_trades_per_day"] = max(0, int(cap["max_trades_per_day"]))
                if "max_loss_streak_per_day" in cap and cap["max_loss_streak_per_day"] is not None:
                    item["max_loss_streak_per_day"] = max(0, int(cap["max_loss_streak_per_day"]))
                if "max_daily_loss_abs" in cap and cap["max_daily_loss_abs"] is not None:
                    item["max_daily_loss_abs"] = max(0.0, float(cap["max_daily_loss_abs"]))
                if "max_daily_profit_abs" in cap and cap["max_daily_profit_abs"] is not None:
                    item["max_daily_profit_abs"] = max(0.0, float(cap["max_daily_profit_abs"]))
                if "cap_latch" in cap and cap["cap_latch"] is not None:
                    item["cap_latch"] = bool(cap["cap_latch"])
                if "max_open_positions" in cap and cap["max_open_positions"] is not None:
                    item["max_open_positions"] = max(0, int(cap["max_open_positions"]))
                if "no_hedge" in cap and cap["no_hedge"] is not None:
                    item["no_hedge"] = bool(cap["no_hedge"])
                if item:
                    norm_caps[s] = item
            CONFIG["symbol_caps"] = norm_caps
        except Exception:
            return {
                "ok": False,
                "error": "symbol_caps must look like {\"EURUSD\":{\"max_open_positions\":1,\"no_hedge\":true,\"max_trades_per_day\":2,\"max_loss_streak_per_day\":1,\"max_daily_loss_abs\":50,\"max_daily_profit_abs\":30,\"cap_latch\":true}}"
            }
    raw_overrides = getattr(update, "symbol_overrides", None)
    if raw_overrides is not None:
        try:
            norm: dict[str, dict[str, float]] = {}
            for sym, ov in (raw_overrides or {}).items():
                if not isinstance(ov, dict):
                    continue
                s = (sym or "").strip().upper()
                if not s:
                    continue
                item: dict[str, float] = {}
                if "sl_pips" in ov and ov["sl_pips"] is not None:
                    item["sl_pips"] = max(0.0, float(ov["sl_pips"]))
                if "tp_pips" in ov and ov["tp_pips"] is not None:
                    item["tp_pips"] = max(0.0, float(ov["tp_pips"]))
                if item:
                    norm[s] = item
            CONFIG["symbol_overrides"] = norm
        except Exception:
            return {"ok": False, "error": 'symbol_overrides must look like {"EURUSD":{"sl_pips":6,"tp_pips":9}}'}

    # === NEW: strategy config ===
    if getattr(update, "strategy_mode", None) is not None:
        mode = str(update.strategy_mode).strip().lower()
        CONFIG["strategy_mode"] = "ensemble" if mode == "ensemble" else "toy"
    if getattr(update, "strategies", None) is not None:
        try:
            new_flags = {}
            for k, v in (update.strategies or {}).items():
                new_flags[str(k)] = bool(v)
            CONFIG["strategies"] = new_flags
        except Exception:
            return {"ok": False, "error": "strategies must be a dict of booleans"}
    if getattr(update, "strategy_weights", None) is not None:
        try:
            new_w = {}
            for k, v in (update.strategy_weights or {}).items():
                new_w[str(k)] = float(v)
            CONFIG["strategy_weights"] = new_w
        except Exception:
            return {"ok": False, "error": "strategy_weights must be a dict of numbers"}
    if getattr(update, "strategy_fallback_sl_pips", None) is not None:
        try:
            CONFIG["strategy_fallback_sl_pips"] = max(0.0, float(update.strategy_fallback_sl_pips))
        except Exception:
            return {"ok": False, "error": "strategy_fallback_sl_pips must be a number"}
    if getattr(update, "strategy_fallback_tp_pips", None) is not None:
        try:
            CONFIG["strategy_fallback_tp_pips"] = max(0.0, float(update.strategy_fallback_tp_pips))
        except Exception:
            return {"ok": False, "error": "strategy_fallback_tp_pips must be a number"}

    if getattr(update, "lob_session_start_utc", None) is not None:
        CONFIG["lob_session_start_utc"] = str(update.lob_session_start_utc)
    if getattr(update, "lob_session_end_utc", None) is not None:
        CONFIG["lob_session_end_utc"] = str(update.lob_session_end_utc)


    if update.reset_counters:
        TRADE_COUNT.clear(); LOSS_STREAK.clear(); DAILY_NET.clear()
        ACCOUNT_DAILY_NET.clear(); ACCOUNT_LATCHED.clear()
        SYMBOL_COOLOFF_UNTIL.clear(); OPEN_POS.clear()
        save_counters()

    save_config()
    today = datetime.now(timezone.utc).date().isoformat()
    today_counts = {k: v for k, v in TRADE_COUNT.items() if k.endswith(today)}
    today_streaks = {k: v for k, v in LOSS_STREAK.items() if k.endswith(today)}
    return {"ok": True, "config": CONFIG, "today_counts": today_counts, "today_loss_streaks": today_streaks, "keys_note": "keys are 'SYMBOL|YYYY-MM-DD' in UTC"}

@app.post("/admin/preset/test", dependencies=[Depends(require_api_key)])
def admin_preset_test():
    CONFIG["allowed_hours_utc"] = list(range(0, 24))
    CONFIG["symbol_hours"] = {}
    CONFIG["max_daily_loss_abs"] = 0.0
    CONFIG["max_account_daily_loss_abs"] = 0.0
    CONFIG["max_account_daily_profit_abs"] = 0.0
    CONFIG["account_cap_latch"] = False
    CONFIG["max_trades_per_day"] = 999_999
    CONFIG["max_loss_streak_per_day"] = 999_999
    CONFIG["symbol_caps"] = {}
    CONFIG["no_hedge"] = False
    CONFIG["max_open_positions_global"] = 0
    CONFIG["cool_off_minutes_after_loss"] = 0
    CONFIG["cool_off_minutes_after_win"] = 0
    CONFIG["symbol_cool_off"] = {}
    ACCOUNT_LATCHED.clear()
    SYMBOL_LATCHED.clear()
    SYMBOL_COOLOFF_UNTIL.clear()
    OPEN_POS.clear()
    CONFIG["symbol_cap_latch"] = False

    # keep strategies configured but default to toy in test preset
    CONFIG["strategy_mode"] = "toy"

    save_config()
    return {"ok": True, "applied": "test_preset", "config": CONFIG}

@app.post("/admin/preset/live", dependencies=[Depends(require_api_key)])
def admin_preset_live():
    """
    A conservative live preset. Adjust numbers to taste.
    """
    CONFIG["trading_enabled"] = True
    CONFIG["allowed_hours_utc"] = list(range(6, 21))       # 06:00–20:59Z
    CONFIG["allowed_days_utc"] = [0,1,2,3,4]               # Mon–Fri
    CONFIG["max_trades_per_day"] = 20
    CONFIG["max_loss_streak_per_day"] = 5
    CONFIG["max_daily_loss_abs"] = 0.0                     # per-symbol off by default

    # Account-level daily caps
    CONFIG["max_account_daily_loss_abs"] = 500.0           # example
    CONFIG["max_account_daily_profit_abs"] = 1000.0        # example
    CONFIG["account_cap_latch"] = True

    # Concurrency & hedging
    CONFIG["max_open_positions_global"] = 3
    CONFIG["no_hedge"] = True

    # Cool-offs
    CONFIG["cool_off_minutes_after_loss"] = 5
    CONFIG["cool_off_minutes_after_win"]  = 0
    CONFIG["symbol_cool_off"] = {}

    # Strategies: keep mode explicit; you can flip later via /config
    CONFIG["strategy_mode"] = "toy"

    save_config()
    return {"ok": True, "applied": "live_preset", "config": CONFIG}


@app.post("/admin/clear-latches", dependencies=[Depends(require_api_key)])
def admin_clear_latches():
    """
    Clears per-day latches (account & symbol) without touching counters/net.
    Useful during testing/tuning sessions.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    ACCOUNT_LATCHED[today] = False
    # Clear all symbol latches for today
    for k in list(SYMBOL_LATCHED.keys()):
        if k.endswith(today):
            SYMBOL_LATCHED[k] = False
    save_counters()
    return {"ok": True, "cleared_for_utc_day": today}


# --- Logging defaults ---
DEFAULT_SL_PIPS = 4.0
DEFAULT_TP_PIPS = 6.0
EXPIRY_SECONDS  = 60
LOG_PATH        = Path("trade_log.csv")
CSV_HEADER = [
    "ts","status","symbol","direction","entry","sl","tp","exit","pips",
    "profit","commission","swap","net","lots","ticket","position_id","magic","payload"
]

def ensure_log():
    if not LOG_PATH.exists():
        with LOG_PATH.open("w", newline="") as f:
            csv.writer(f).writerow(CSV_HEADER)

@app.get("/debug/strategy", dependencies=[Depends(require_api_key)])
def debug_strategy(symbol: str = "EURUSD", tf: str = "M5", trace: int = 0):
    """
    Returns raw signals from individual strategy modules, the weights used,
    and the final ensemble vote (if ensemble mode is active).
    """
    res = {
        "mode": str(CONFIG.get("strategy_mode", "toy")),
        "symbol": symbol.upper().strip(),
        "tf": tf.upper().strip(),
        "weights": CONFIG.get("strategy_weights", {}),
        "enabled": CONFIG.get("strategies", {}),
        "signals": [],
        "vote": None,
    }

    if not HAVE_STRATS:
        res["note"] = "Strategy modules not available"
        return res

    flags = CONFIG.get("strategies", {})
    ctx = {
        "symbol": res["symbol"],
        "tf": res["tf"],
        "utc_now": datetime.now(timezone.utc),
        "config": CONFIG,
    }

    if bool(flags.get("breakout_london", True)):
        try:
            res["signals"].append({"name": "breakout_london", **strat_breakout_london.signal(ctx)})
        except Exception as e:
            info = {"name": "breakout_london", "direction": 0, "reason": f"error:{e}"}
            if trace:
                info["traceback"] = traceback.format_exc()
            res["signals"].append(info)

    if bool(flags.get("trend_ma", True)):
        try:
            res["signals"].append({"name": "trend_ma", **strat_trend_ma.signal(ctx)})
        except Exception as e:
            info = {"name": "trend_ma", "direction": 0, "reason": f"error:{e}"}
            if trace:
                info["traceback"] = traceback.format_exc()
            res["signals"].append(info)

    if bool(flags.get("meanrev_band", True)):
        try:
            res["signals"].append({"name": "meanrev_band", **strat_meanrev_band.signal(ctx)})
        except Exception as e:
            info = {"name": "meanrev_band", "direction": 0, "reason": f"error:{e}"}
            if trace:
                info["traceback"] = traceback.format_exc()
            res["signals"].append(info)


    try:
        res["vote"] = strat_ensemble.vote(ctx, res["signals"], CONFIG.get("strategy_weights", {}))
    except Exception as e:
        res["vote"] = {"direction": 0, "reason": f"vote_error:{e}"}
        if trace:
            res["vote"]["traceback"] = traceback.format_exc()


    return res


# === NEW: strategy runner ===
def _run_ensemble(symbol: str, tf: str):
    if not HAVE_STRATS:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "strategies_not_available"}

    flags = CONFIG.get("strategies", {})
    weights = CONFIG.get("strategy_weights", {})
    enabled = {
        "breakout_london": bool(flags.get("breakout_london", True)),
        "trend_ma": bool(flags.get("trend_ma", True)),
        "meanrev_band": bool(flags.get("meanrev_band", True)),
    }

    ctx = {
        "symbol": (symbol or "").upper().strip(),
        "tf": tf.upper().strip(),
        "utc_now": datetime.now(timezone.utc),
        "config": CONFIG,  # give strategies access to relevant defaults
    }

    signals = []
    if enabled["breakout_london"]:
        try:
            s = strat_breakout_london.signal(ctx)
            s["_name"] = "breakout_london"
            signals.append(s)
        except Exception as e:
            signals.append({"_name": "breakout_london", "direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": f"breakout_error:{e}"})
    if enabled["trend_ma"]:
        try:
            s = strat_trend_ma.signal(ctx)
            s["_name"] = "trend_ma"
            signals.append(s)
        except Exception as e:
            signals.append({"_name": "trend_ma", "direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": f"trend_ma_error:{e}"})
    if enabled["meanrev_band"]:
        try:
            s = strat_meanrev_band.signal(ctx)
            s["_name"] = "meanrev_band"
            signals.append(s)
        except Exception as e:
            signals.append({"_name": "meanrev_band", "direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": f"meanrev_error:{e}"})


    voted = strat_ensemble.vote(ctx, signals, weights)
    # Apply fallbacks for SL/TP
    if voted.get("direction", 0) != 0:
        if float(voted.get("sl_pips") or 0) <= 0:
            voted["sl_pips"] = float(CONFIG.get("strategy_fallback_sl_pips", CONFIG.get("default_sl_pips", DEFAULT_SL_PIPS)))
        if float(voted.get("tp_pips") or 0) <= 0:
            voted["tp_pips"] = float(CONFIG.get("strategy_fallback_tp_pips", CONFIG.get("default_tp_pips", DEFAULT_TP_PIPS)))
    return voted

@app.get("/signal", dependencies=[Depends(require_api_key)])
def signal(symbol: str = "EURUSD", tf: str = "M5",
           sl_pips: Optional[float] = None, tp_pips: Optional[float] = None):
    """
    Toy alternator OR Ensemble (configurable via strategy_mode).
    EA will convert sl_pips/tp_pips to absolute prices; absolute sl/tp left as 0.0.
    """
    ok, reason = can_trade_now(symbol)
    now = int(utc_now_ts().timestamp())

    if not ok:
        return {
            "symbol": symbol, "direction": 0, "entry": 0.0, "sl": 0.0, "tp": 0.0,
            "sl_pips": 0.0, "tp_pips": 0.0,
            "expiry_ts": now + EXPIRY_SECONDS,
            "reason": reason,
        }


    # Optional no-hedge (needs direction knowledge; for ensemble it’s post-vote)
    def _hedge_block(dir_val: int) -> bool:
        sym_u = (symbol or "").strip().upper()
        scaps = (CONFIG.get("symbol_caps") or {}).get(sym_u, {})
        no_hedge_eff = bool(scaps.get("no_hedge", CONFIG.get("no_hedge", False)))
        if not no_hedge_eff or dir_val not in (1, -1):
            return False

        # 1) In-memory OPEN_POS (fast path)
        for _k, meta in _open_pos_items_snapshot():
            if meta.get("symbol") == sym_u and int(meta.get("direction") or 0) == (-dir_val):
                return True

        # 2) File-backed sides (robust path if in-memory missed it)
        try:
            c = _load_counters()
            _ensure_side_structs(c)
            sides = c.get("OPEN_POS_SIDES", {}).get(sym_u, {})
            has_buy  = int(sides.get("buy", 0))  > 0
            has_sell = int(sides.get("sell", 0)) > 0
            if (dir_val > 0 and has_sell) or (dir_val < 0 and has_buy):
                return True
        except Exception:
            pass

        return False

    mode = str(CONFIG.get("strategy_mode","toy")).lower().strip()
    if mode == "ensemble":
        voted = _run_ensemble(symbol, tf)
        dir_v = int(voted.get("direction") or 0)
        if dir_v in (1,-1) and _hedge_block(dir_v):
            return {
                "symbol": symbol, "direction": 0, "entry": 0.0, "sl": 0.0, "tp": 0.0,
                "sl_pips": 0.0, "tp_pips": 0.0,
                "expiry_ts": now + EXPIRY_SECONDS,
                "reason": "no_hedge",
            }
        if dir_v == 0:
            # Even on a neutral vote, surface per-symbol overrides (or global defaults)
            sym_u = (symbol or "").strip().upper()
            ov = (CONFIG.get("symbol_overrides") or {}).get(sym_u, {})
            ov_sl = ov.get("sl_pips", None)
            ov_tp = ov.get("tp_pips", None)
            slp = float(ov_sl) if ov_sl is not None else float(CONFIG.get("default_sl_pips", DEFAULT_SL_PIPS))
            tpp = float(ov_tp) if ov_tp is not None else float(CONFIG.get("default_tp_pips", DEFAULT_TP_PIPS))
            return {
                "symbol": symbol, "direction": 0, "entry": 0.0, "sl": 0.0, "tp": 0.0,
                "sl_pips": max(0.0, slp), "tp_pips": max(0.0, tpp),
                "expiry_ts": now + EXPIRY_SECONDS,
                "reason": voted.get("reason","neutral")
            }


        # per-symbol overrides
        sym_u = (symbol or "").strip().upper()
        ov = (CONFIG.get("symbol_overrides") or {}).get(sym_u, {})
        ov_sl = ov.get("sl_pips", None)
        ov_tp = ov.get("tp_pips", None)

        # allow explicit query overrides to win; else per-symbol override; else strategy vote; else globals
        slp = (
            float(sl_pips) if (sl_pips and sl_pips > 0) else
            (float(ov_sl) if ov_sl is not None else float(voted.get("sl_pips") or CONFIG.get("default_sl_pips", DEFAULT_SL_PIPS)))
        )
        tpp = (
            float(tp_pips) if (tp_pips and tp_pips > 0) else
            (float(ov_tp) if ov_tp is not None else float(voted.get("tp_pips") or CONFIG.get("default_tp_pips", DEFAULT_TP_PIPS)))
        )

        return {
            "symbol": symbol,
            "direction": dir_v,
            "entry": 0.0,
            "sl": 0.0,
            "tp": 0.0,
            "sl_pips": max(0.0, slp),
            "tp_pips": max(0.0, tpp),
            "expiry_ts": now + EXPIRY_SECONDS,
            "reason": voted.get("reason","vote")
        }


    # === default TOY alternator (existing behavior) ===
    tf_map = {"M1":60, "M5":300, "M15":900, "M30":1800, "H1":3600, "H4":14400, "D1":86400}
    bar_secs = tf_map.get(tf.upper(), 300)
    bar_bucket = now // bar_secs
    direction = 1 if (bar_bucket % 2 == 0) else -1

    # hedging check for toy
    if _hedge_block(direction):
        return {
            "symbol": symbol,
            "direction": 0, "entry": 0.0, "sl": 0.0, "tp": 0.0,
            "sl_pips": 0.0, "tp_pips": 0.0,
            "expiry_ts": now + EXPIRY_SECONDS,
            "reason": "no_hedge"
        }


    sym_u = (symbol or "").strip().upper()
    ov = (CONFIG.get("symbol_overrides") or {}).get(sym_u, {})
    ov_sl = ov.get("sl_pips", None)
    ov_tp = ov.get("tp_pips", None)
    slp = float(sl_pips) if (sl_pips and sl_pips > 0) else (
        float(ov_sl) if ov_sl is not None else float(CONFIG.get("default_sl_pips", DEFAULT_SL_PIPS))
    )
    tpp = float(tp_pips) if (tp_pips and tp_pips > 0) else (
        float(ov_tp) if ov_tp is not None else float(CONFIG.get("default_tp_pips", DEFAULT_TP_PIPS))
    )

    return {
        "symbol": symbol, "direction": direction, "entry": 0.0,
        "sl": 0.0, "tp": 0.0, "sl_pips": slp, "tp_pips": tpp,
        "expiry_ts": now + EXPIRY_SECONDS,
        "reason": "toy"
    }


class SignalRequest(BaseModel):
    symbol: Optional[str] = "EURUSD"
    tf: Optional[str] = "M5"
    sl_pips: Optional[float] = None
    tp_pips: Optional[float] = None

@app.post("/signal", dependencies=[Depends(require_api_key)])
def signal_post(req: SignalRequest):
    return signal(
        symbol=req.symbol or "EURUSD",
        tf=req.tf or "M5",
        sl_pips=req.sl_pips,
        tp_pips=req.tp_pips
    )

class LogPayload(BaseModel):
    symbol: Optional[str] = None
    direction: Optional[int] = None
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    exit: Optional[float] = None
    pips: Optional[float] = None
    profit: Optional[float] = None
    commission: Optional[float] = None
    swap: Optional[float] = None
    net: Optional[float] = None
    lots: Optional[float] = None
    ticket: Optional[str] = None
    position_id: Optional[str] = None
    magic: Optional[int] = None
    status: Optional[str] = None

# --- BEGIN: no-hedge side tracking helpers ---

COUNTERS_FILE = Path("runtime_counters.json")

def _load_counters() -> Dict[str, Any]:
    """
    Safe loader used only for OPEN/CLOSE book-keeping when /logtrade runs
    (keeps compatibility with existing file if the app already writes it).
    """
    try:
        if COUNTERS_FILE.exists():
            return json.loads(COUNTERS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_counters_unlocked(c: Dict[str, Any]) -> None:
    """Atomic write without taking COUNTERS_LOCK (caller must hold it if needed)."""
    try:
        _atomic_write_json(COUNTERS_FILE, c)
    except Exception:
        pass

def _ensure_side_structs(c: Dict[str, Any]) -> None:
    c.setdefault("OPEN_TICKETS", {})        # ticket -> {symbol, dir}
    c.setdefault("OPEN_POS_SIDES", {})      # symbol -> {buy: int, sell: int}
    c.setdefault("OPEN_POS_TOTAL", 0)
    c.setdefault("OPEN_POS_BY_SYMBOL", {})

def _inc_side(c: Dict[str, Any], symbol: str, dir_: int) -> None:
    sides = c["OPEN_POS_SIDES"].setdefault(symbol, {"buy": 0, "sell": 0})
    if int(dir_) > 0:
        sides["buy"] += 1
    elif int(dir_) < 0:
        sides["sell"] += 1

def _dec_side(c: Dict[str, Any], symbol: str, dir_: int) -> None:
    sides = c["OPEN_POS_SIDES"].setdefault(symbol, {"buy": 0, "sell": 0})
    if int(dir_) > 0:
        sides["buy"] = max(0, sides["buy"] - 1)
    elif int(dir_) < 0:
        sides["sell"] = max(0, sides["sell"] - 1)

def _on_open(symbol: str, dir_: int, ticket: str) -> None:
    with COUNTERS_LOCK:
        c = _load_counters()
        _ensure_side_structs(c)
        c["OPEN_TICKETS"][str(ticket)] = {"symbol": symbol, "dir": int(dir_)}
        c["OPEN_POS_TOTAL"] = int(c.get("OPEN_POS_TOTAL", 0)) + 1
        by = c["OPEN_POS_BY_SYMBOL"]
        by[symbol] = int(by.get(symbol, 0)) + 1
        _inc_side(c, symbol, dir_)
        _save_counters_unlocked(c)

def _on_close(symbol: str, dir_: int, ticket: str) -> None:
    with COUNTERS_LOCK:
        c = _load_counters()
        _ensure_side_structs(c)

        tinfo = c["OPEN_TICKETS"].get(str(ticket))
        if tinfo:
            symbol = tinfo.get("symbol", symbol)
            dir_ = int(tinfo.get("dir", dir_))

        c["OPEN_POS_TOTAL"] = max(0, int(c.get("OPEN_POS_TOTAL", 0)) - 1)
        by = c["OPEN_POS_BY_SYMBOL"]
        if symbol in by:
            by[symbol] = max(0, int(by.get(symbol, 0)) - 1)

        _dec_side(c, symbol, dir_)
        c["OPEN_TICKETS"].pop(str(ticket), None)

        _save_counters_unlocked(c)

# --- END: no-hedge side tracking helpers ---


@app.post("/logtrade", dependencies=[Depends(require_api_key)])
async def logtrade(payload: LogPayload, req: Request):

    # --- BEGIN: side book-keeping tap-in ---
    # These calls are idempotent and only update additive keys in runtime_counters.json
    # They DO NOT interfere with your existing Google Sheets append or in-memory counters.
    try:
        d_ = _asdict(payload)
        trade_status = str(d_.get("status") or "").upper().strip()
        sym    = str(d_.get("symbol") or "").upper().strip()
        ticket = str(d_.get("ticket") or "").strip()

        dir_raw = d_.get("dir", d_.get("direction"))
        try:
            dir_ = int(dir_raw) if dir_raw is not None and str(dir_raw) != "" else 0
        except Exception:
            try:
                dir_ = int(float(str(dir_raw)))
            except Exception:
                dir_ = 0

        if trade_status == "OPEN" and sym and ticket:
            _on_open(sym, dir_, ticket)
        elif trade_status == "CLOSE" and ticket:
            _on_close(sym, dir_, ticket)
    except Exception:
        # swallow to avoid affecting primary /logtrade path
        pass
    # --- END: side book-keeping tap-in ---

    ts_epoch = int(time.time())
    ts_iso = datetime.now(timezone.utc).isoformat()
    d = _asdict(payload)

    pos_key = (str(d.get("position_id") or d.get("ticket") or "").strip())
    sym_u = (str(d.get("symbol") or "").strip().upper())
    ddir_raw = d.get("dir") if d.get("dir") is not None else d.get("direction", None)
    try:
        ddir = int(ddir_raw) if ddir_raw is not None and str(ddir_raw) != "" else 0
    except:
        try:
            ddir = int(float(str(ddir_raw)))
        except:
            ddir = 0
    stat = (str(d.get("status") or "").upper().strip())

    try:
        with COUNTERS_LOCK:
            if stat == "OPEN":
                if not pos_key:
                    pos_key = f"AUTO_{sym_u}_{int(time.time()*1000)}"
                OPEN_POS[pos_key] = {"symbol": sym_u, "direction": ddir}
            elif stat == "CLOSE":
                removed = False
                if pos_key and pos_key in OPEN_POS:
                    OPEN_POS.pop(pos_key, None)
                    removed = True
                else:
                    if sym_u:
                        candidates = [k for k, meta in OPEN_POS.items()
                                      if meta.get("symbol") == sym_u
                                      and int(meta.get("direction") or 0) == ddir
                                      and ddir in (1, -1)]
                        if not candidates:
                            candidates = [k for k, meta in OPEN_POS.items()
                                          if meta.get("symbol") == sym_u]
                        if candidates:
                            oldest = sorted(candidates, key=lambda x: str(x))
                            OPEN_POS.pop(oldest[0], None)
                            removed = True
    except Exception:
        pass



    try:
        if sym_u and stat == "OPEN":
            key = today_utc_key(sym_u)
            with COUNTERS_LOCK:
                TRADE_COUNT[key] += 1
            # release lock before saving to avoid deadlock
            save_counters()
    except Exception:
        pass



    try:
        if sym_u and stat == "CLOSE":
            net_raw = d.get("net", None)
            net_val = None
            try:
                if net_raw is not None and str(net_raw) != "":
                    net_val = float(net_raw)
            except:
                try:
                    net_val = float(str(net_raw).replace(",", "."))
                except:
                    net_val = None
            if net_val is None:
                p = d.get("profit", 0) or 0
                c = d.get("commission", 0) or 0
                s = d.get("swap", 0) or 0
                try:
                    net_val = float(p) + float(c) + float(s)
                except:
                    net_val = 0.0

            with COUNTERS_LOCK:
                key = today_utc_key(sym_u)
                if net_val <= 0:
                    LOSS_STREAK[key] = LOSS_STREAK.get(key, 0) + 1
                else:
                    LOSS_STREAK[key] = 0
                DAILY_NET[key] = DAILY_NET.get(key, 0.0) + float(net_val)

                day_str = utc_now_ts().date().isoformat()
                ACCOUNT_DAILY_NET[day_str] = ACCOUNT_DAILY_NET.get(day_str, 0.0) + float(net_val)

                try:
                    if bool(CONFIG.get("account_cap_latch", False)):
                        acc_loss_cap = float(CONFIG.get("max_account_daily_loss_abs", 0.0))
                        acc_profit_cap = float(CONFIG.get("max_account_daily_profit_abs", 0.0))
                        acc_net_now = float(ACCOUNT_DAILY_NET.get(day_str, 0.0))
                        if (acc_loss_cap > 0 and acc_net_now <= -abs(acc_loss_cap)) or (acc_profit_cap > 0 and acc_net_now >= abs(acc_profit_cap)):
                            ACCOUNT_LATCHED[day_str] = True
                except Exception:
                    pass

                try:
                    eff_caps = _effective_for_symbol(sym_u)["caps"]
                    sym_latch_enabled = bool(eff_caps.get("cap_latch", False) or CONFIG.get("symbol_cap_latch", False))
                    if sym_latch_enabled:
                        sym_loss_cap   = float(eff_caps.get("max_daily_loss_abs", 0.0))
                        sym_profit_cap = float(eff_caps.get("max_daily_profit_abs", 0.0))
                        sym_net_now = float(DAILY_NET.get(key, 0.0))
                        if (sym_loss_cap > 0 and sym_net_now <= -abs(sym_loss_cap)) or (sym_profit_cap > 0 and sym_net_now >= abs(sym_profit_cap)):
                            SYMBOL_LATCHED[key] = True
                except Exception:
                    pass

                try:
                    cfg_sym = (CONFIG.get("symbol_cool_off") or {}).get(sym_u, {}) or {}
                    loss_min = int(cfg_sym.get("after_loss_min", CONFIG.get("cool_off_minutes_after_loss", 0) or 0))
                    win_min  = int(cfg_sym.get("after_win_min",  CONFIG.get("cool_off_minutes_after_win",  0) or 0))
                    now_set_ts = time.time()
                    if net_val is not None and net_val <= 0 and loss_min > 0:
                        SYMBOL_COOLOFF_UNTIL[sym_u] = max(SYMBOL_COOLOFF_UNTIL.get(sym_u, 0.0), now_set_ts + loss_min * 60)
                    elif net_val is not None and net_val > 0 and win_min > 0:
                        SYMBOL_COOLOFF_UNTIL[sym_u] = max(SYMBOL_COOLOFF_UNTIL.get(sym_u, 0.0), now_set_ts + win_min * 60)
                except Exception:
                    pass

        # release lock before saving to avoid deadlock
        save_counters()
    except Exception:
        pass

# === Google Sheets (matching trades_raw columns) ===
    if sheets is not None and SHEET_TRADES_TAB:
        # Compute trade-date as YYYY-MM-DD (so Sheets doesn't need to parse ISO TZ)
        try:
            _dt = _parse_any_ts(str(d.get("ts") or "")) or datetime.now(timezone.utc)
            trade_date = _dt.date().isoformat()  # e.g., 2025-09-14
        except Exception:
            trade_date = datetime.now(timezone.utc).date().isoformat()

        row = [
            d.get("ts", ts_iso),
            d.get("symbol", ""),
            (d.get("dir") if d.get("dir") is not None else d.get("direction", "")),
            d.get("entry", ""),
            d.get("sl", ""),
            d.get("tp", ""),
            d.get("lots", ""),
            d.get("status", ""),
            d.get("ticket", ""),
            d.get("magic", ""),
            d.get("spread_pts", ""),
            d.get("slippage", ""),
            d.get("exit", ""),
            d.get("pips", ""),
            d.get("profit", ""),
            d.get("net", ""),
            d.get("comment", ""),
            trade_date,  # <— 18th column to match header 'trade-date'
        ]
        try:
            # Run the blocking Sheets append in a worker thread
            await anyio.to_thread.run_sync(lambda: sheets.append_row(SHEET_TRADES_TAB, row))
            return {"ok": True, "saved_to": "google_sheets"}
        except Exception as e:
            print(f"[SHEETS] Append failed, falling back to CSV: {e}")


    # === CSV fallback (unchanged shape) ===
    ensure_log()
    row = [
        ts_epoch,
        d.get("status", ""),
        d.get("symbol", ""),
        (d.get("dir") if d.get("dir") is not None else d.get("direction", "")),
        d.get("entry", ""),
        d.get("sl", ""),
        d.get("tp", ""),
        d.get("exit", ""),
        d.get("pips", ""),
        d.get("profit", ""),
        d.get("commission", ""),
        d.get("swap", ""),
        d.get("net", ""),
        d.get("lots", ""),
        d.get("ticket", ""),
        d.get("position_id", ""),
        d.get("magic", ""),
        ""
    ]
    with LOG_PATH.open("a", newline="") as f:
        csv.writer(f).writerow(row)
    return {"ok": True, "saved_to": str(LOG_PATH)}
