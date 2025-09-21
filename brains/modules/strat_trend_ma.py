# strat_trend_ma.py (FULL REPLACEMENT)
from __future__ import annotations
from typing import Dict, Any
from .data_feeds import load_ohlc, sma, atr, price_to_pips

def signal(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trend-follow:
      - Bias from H1 MA50 vs MA200.
      - Entry on M5 pullback to MA20 in direction of bias.
      - SL/TP from M5 ATR (defaults: SL=1.0*ATR, TP=1.5*ATR).
      - ATR floor filter to avoid dead regimes.
    """
    cfg = ctx.get("config") or {}
    symbol = (ctx.get("symbol") or "EURUSD").upper().strip()

    df_h1 = load_ohlc(symbol, "H1", max_bars=1000)
    df_m5 = load_ohlc(symbol, "M5", max_bars=1500)
    if df_h1 is None or len(df_h1) < 220 or df_m5 is None or len(df_m5) < 200:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_data"}

    ma_fast = float(sma(df_h1["close"], 50).iloc[-1])
    ma_slow = float(sma(df_h1["close"], 200).iloc[-1])
    bias = 1 if ma_fast > ma_slow else (-1 if ma_fast < ma_slow else 0)
    if bias == 0:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_bias"}

    ma20 = float(sma(df_m5["close"], 20).iloc[-1])
    last = float(df_m5["close"].iloc[-1])
    atr_val = float(atr(df_m5, 14).iloc[-1])
    atr_pips = price_to_pips(atr_val)

    atr_floor = float(cfg.get("trend_atr_floor_pips", 5.0))
    if atr_pips < atr_floor:
        return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "atr_floor"}

    # Pullback logic: in up-bias, price near/above MA20; in down-bias, price near/below MA20
    if bias > 0 and last >= ma20:
        sl_pips = max(6.0, float(cfg.get("trend_sl_atr_mult", 1.0)) * atr_pips)
        tp_pips = max(6.0, float(cfg.get("trend_tp_atr_mult", 1.5)) * atr_pips)
        return {"direction": 1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.55, "reason": "trend_long"}
    if bias < 0 and last <= ma20:
        sl_pips = max(6.0, float(cfg.get("trend_sl_atr_mult", 1.0)) * atr_pips)
        tp_pips = max(6.0, float(cfg.get("trend_tp_atr_mult", 1.5)) * atr_pips)
        return {"direction": -1, "sl_pips": round(sl_pips, 2), "tp_pips": round(tp_pips, 2), "confidence": 0.55, "reason": "trend_short"}

    return {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_pullback"}
