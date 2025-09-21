# strat_meanrev_band.py (FULL REPLACEMENT)
from __future__ import annotations
from typing import Dict, Any
from .data_feeds import load_ohlc, bbands, vwap, atr, ma_slope, price_to_pips

def signal(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mean reversion:
      - Range regime prefilter via MA slope (flatness).
      - Entry on Bollinger band touch; target mid/VWAP; ATR stop.
      - Smaller targets than trend; neutral otherwise.
    """
    cfg = ctx.get("config") or {}
    symbol = (ctx.get("symbol") or "EURUSD").upper().strip()

    df = load_ohlc(symbol, "M5", max_bars=1500)
    if df is None or len(df) < 100:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_data"}

    basis, upper, lower = bbands(df, n=int(cfg.get("mr_band_n", 20)), k=float(cfg.get("mr_band_k", 2.0)))
    vw = vwap(df)
    last = float(df["close"].iloc[-1])
    last_upper = float(upper.iloc[-1])
    last_lower = float(lower.iloc[-1])
    last_basis = float(basis.iloc[-1])
    last_vw = float(vw.iloc[-1])

    # Regime: slope of MA50 should be small
    slope_threshold = float(cfg.get("mr_slope_threshold", 0.00003))  # ~0.3 pip per bar
    regime_ok = ma_slope(df["close"], n=50) < slope_threshold
    if not regime_ok:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "not_range"}

    atr_val = float(atr(df, 14).iloc[-1])
    atr_pips = price_to_pips(atr_val)

    # Long on lower band touch
    if last <= last_lower:
        sl_pips = max(5.0, float(cfg.get("mr_sl_atr_mult", 1.0)) * atr_pips)
        # target towards VWAP/basis (use the closer one above price)
        tgt = min(x for x in [last_vw, last_basis] if x >= last) if any(x >= last for x in [last_vw, last_basis]) else last_basis
        tp_pips = max(6.0, price_to_pips(tgt - last))
        return {"direction": 1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.5, "reason": "meanrev_long"}

    # Short on upper band touch
    if last >= last_upper:
        sl_pips = max(5.0, float(cfg.get("mr_sl_atr_mult", 1.0)) * atr_pips)
        tgt = max(x for x in [last_vw, last_basis] if x <= last) if any(x <= last for x in [last_vw, last_basis]) else last_basis
        tp_pips = max(6.0, price_to_pips(last - tgt))
        return {"direction": -1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.5, "reason": "meanrev_short"}

    return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "inside_bands"}
