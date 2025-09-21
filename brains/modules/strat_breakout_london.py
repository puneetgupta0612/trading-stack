# strat_breakout_london.py (FULL REPLACEMENT)
from __future__ import annotations
from typing import Dict, Any
from .data_feeds import utcnow, in_utc_range, load_ohlc, atr, asian_box, price_to_pips, pips_to_price, PIP

def signal(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    London Breakout:
      - Build Asian box (00:00–07:00 UTC) on M5 bars.
      - Entry when price breaks the box with a small buffer (in pips).
      - Skip if box too small/too large.
      - SL: half-box (default) or ATR-based; TP: 1.0–1.5R.
      - Time stop: neutral after 12:00 UTC.
    """
    cfg = ctx.get("config") or {}
    symbol = (ctx.get("symbol") or "EURUSD").upper().strip()
    tf = (ctx.get("tf") or "M5").upper().strip()
    now = utcnow()

    # Time window: London 07:00–12:00 UTC
    def _hm(s: str) -> tuple[int, int]:
        try:
            h, m = str(s).split(":")
            return int(h), int(m)
        except Exception:
            return 7, 0

    sh, sm = _hm(cfg.get("lob_session_start_utc", "07:00"))
    eh, em = _hm(cfg.get("lob_session_end_utc",  "12:00"))

    cur_min    = now.hour * 60 + now.minute
    start_min  = sh * 60 + sm
    end_min    = eh * 60 + em  # end-exclusive
    if not (start_min <= cur_min < end_min):
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "outside_london_session"}

    df = load_ohlc(symbol, "M5", max_bars=2000)
    if df is None or len(df) < 200:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_data"}

    box = asian_box(df, "00:00", "07:00")
    if not box:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_asian_box"}
    box_high, box_low = box
    box_size_pips = price_to_pips(box_high - box_low)

    # Configurable guards
    buf_pips = float(cfg.get("lob_buffer_pips", 2.0))
    min_box = float(cfg.get("lob_min_box_pips", 5.0))
    max_box = float(cfg.get("lob_max_box_pips", 60.0))
    use_half_box = bool(cfg.get("lob_sl_half_box", True))
    r_mult = float(cfg.get("lob_r", 1.2))

    if not (min_box <= box_size_pips <= max_box):
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "box_size_skip"}

    last = float(df["close"].iloc[-1])
    up_trig = box_high + pips_to_price(buf_pips)
    dn_trig = box_low  - pips_to_price(buf_pips)

    # ATR for alternative SL/TP calc
    atr_val = float(atr(df, 14).iloc[-1])
    atr_pips = price_to_pips(atr_val)

    if last > up_trig:
        # Long breakout
        if use_half_box:
            sl_price = (box_high + box_low) / 2.0
            sl_pips = price_to_pips(last - sl_price)
        else:
            sl_pips = max(6.0, 1.0 * atr_pips)
        tp_pips = max(sl_pips, r_mult * sl_pips)
        return {"direction": 1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.6, "reason": "london_break_long"}

    if last < dn_trig:
        # Short breakout
        if use_half_box:
            sl_price = (box_high + box_low) / 2.0
            sl_pips = price_to_pips(sl_price - last)
        else:
            sl_pips = max(6.0, 1.0 * atr_pips)
        tp_pips = max(sl_pips, r_mult * sl_pips)
        return {"direction": -1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.6, "reason": "london_break_short"}

    return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_break"}
