# data_feeds.py (FULL REPLACEMENT)
from __future__ import annotations
from datetime import datetime, timezone, time
from typing import Optional, Tuple
import os
import pandas as pd
import numpy as np

# --- Time helpers ---
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def in_utc_range(now: datetime, start_h: int, end_h: int) -> bool:
    """True if hour in [start_h, end_h) UTC, wrapping across midnight if needed."""
    h = now.hour
    if start_h == end_h:
        return True
    if start_h < end_h:
        return start_h <= h < end_h
    return (h >= start_h) or (h < end_h)

# --- Price step helpers (EURUSD defaults) ---
PIP = 0.0001
def pips_to_price(pips: float) -> float:
    return pips * PIP

def price_to_pips(dprice: float) -> float:
    return dprice / PIP

# --- IO: load OHLC candles from CSV ---
# Expected CSV columns: time,open,high,low,close,volume
# Time column parsed to UTC tz-aware index
def _candidate_ohlc_paths(symbol: str, tf: str) -> list[str]:
    """Return a list of plausible CSV paths to try, in priority order."""
    fname = f"{symbol.upper()}_{tf.upper()}.csv"
    module_dir = os.path.abspath(os.path.dirname(__file__))
    candidates = []

    # 1) Explicit env dir
    env_dir = os.getenv("OHLC_DIR")
    if env_dir:
        candidates.append(os.path.abspath(os.path.join(env_dir, fname)))

    # 2) brains/data (relative to this module)
    candidates.append(os.path.abspath(os.path.join(module_dir, "..", "data", fname)))

    # 3) current working directory /data
    candidates.append(os.path.abspath(os.path.join(os.getcwd(), "data", fname)))

    # 4) project root guess: two levels up + /data
    candidates.append(os.path.abspath(os.path.join(module_dir, "..", "..", "data", fname)))

    # de-dup while keeping order
    seen = set()
    uniq = []
    for p in candidates:
        if p not in seen:
            seen.add(p)
            uniq.append(p)
    return uniq

def load_ohlc(symbol: str, tf: str, max_bars: int = 1500, path: Optional[str] = None) -> Optional[pd.DataFrame]:
    # Try explicit path first, else search common folders
    tried = []
    target = None
    if path:
        p = os.path.abspath(path)
        tried.append(p)
        if os.path.exists(p):
            target = p
    if target is None:
        for p in _candidate_ohlc_paths(symbol, tf):
            tried.append(p)
            if os.path.exists(p):
                target = p
                break
    if target is None:
        print(f"[OHLC] Not found. Tried: {tried}")
        return None

    print(f"[OHLC] Using: {target}")
    df = pd.read_csv(target)
    # normalize headers
    cols = {c.lower(): c for c in df.columns}
    # tolerant rename
    for k in ["time","open","high","low","close","volume"]:
        if k not in cols:
            # try case-insensitive exact match
            for c in df.columns:
                if c.strip().lower() == k:
                    cols[k] = c
                    break
    # minimal validation
    for k in ["time","open","high","low","close"]:
        if k not in cols:
            return None

    df = df.rename(columns={cols.get("time","time"): "time",
                            cols.get("open","open"): "open",
                            cols.get("high","high"): "high",
                            cols.get("low","low"): "low",
                            cols.get("close","close"): "close",
                            cols.get("volume","volume"):"volume" if "volume" in cols else "volume"})
    # volume optional
    if "volume" not in df.columns:
        df["volume"] = np.nan

    # time parsing
    t = pd.to_datetime(df["time"], utc=True, infer_datetime_format=True, errors="coerce")
    df = df.assign(time=t).dropna(subset=["time"]).set_index("time").sort_index()
    if max_bars and len(df) > max_bars:
        df = df.tail(max_bars)
    return df

# --- Indicators ---
def sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n, min_periods=n).mean()

def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

def bbands(df: pd.DataFrame, n: int = 20, k: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    basis = sma(df["close"], n)
    std = df["close"].rolling(n, min_periods=n).std()
    upper = basis + k * std
    lower = basis - k * std
    return basis, upper, lower

def vwap(df: pd.DataFrame) -> pd.Series:
    if "volume" not in df.columns:
        return df["close"].expanding().mean()
    pv = (df["close"] * df["volume"]).cumsum()
    vol = df["volume"].replace(0, np.nan).cumsum()
    return pv / vol

def ma_slope(series: pd.Series, n: int = 50) -> float:
    """Return absolute slope (price per bar) from a simple linear fit over last n bars."""
    s = series.tail(n).dropna()
    if len(s) < n:
        return float("inf")
    y = s.values
    x = np.arange(len(y), dtype=float)
    # slope in price units per bar
    slope = np.polyfit(x, y, 1)[0]
    return abs(float(slope))

# --- Sessions / Asian box ---
def asian_box(df: pd.DataFrame, start_utc="00:00", end_utc="07:00") -> Optional[Tuple[float,float]]:
    """Return (box_high, box_low) over today’s Asian session window; None if window has no bars."""
    if df is None or df.empty:
        return None
    last_ts = df.index[-1]
    day_start = last_ts.floor("D")
    s_h, s_m = map(int, start_utc.split(":"))
    e_h, e_m = map(int, end_utc.split(":"))
    start = day_start.replace(hour=s_h, minute=s_m)
    end = day_start.replace(hour=e_h, minute=e_m)
    window = df[(df.index >= start) & (df.index < end)]
    if window.empty:
        return None
    return float(window["high"].max()), float(window["low"].min())
