# ensemble.py
from __future__ import annotations
from typing import List, Dict

from . import strat_breakout_london as s_breakout
from . import strat_trend_ma as s_trend
from . import strat_meanrev_band as s_meanrev

ALL = {
    "breakout_london": s_breakout,
    "trend_ma": s_trend,
    "meanrev_band": s_meanrev,
}

def vote(ctx: dict, signals: List[dict], weights: Dict[str, float]) -> dict:
    """
    Weighted majority on direction (confidence * weight).
    Carries SL/TP from the strongest signal *in the winning direction*.
    Expects each signal to include "_name" or "name".
    """
    out = {"direction": 0, "sl_pips": 0.0, "tp_pips": 0.0, "confidence": 0.0, "reason": "no_signals"}
    if not signals:
        return out

    score = 0.0
    best = None
    total_conf = 0.0

    for s in signals:
        name = s.get("_name") or s.get("name") or "unknown"
        w = float(weights.get(name, 1.0))
        dirv = int(s.get("direction") or 0)
        conf = float(s.get("confidence") or 0.0)
        # aggregate directional score
        score += w * conf * (1 if dirv > 0 else (-1 if dirv < 0 else 0))
        total_conf += max(0.0, conf)

        if dirv != 0:
            if best is None or float(s.get("confidence") or 0.0) > float(best.get("confidence") or 0.0):
                best = s

    # Guard small drift
    threshold = 0.25
    if score > threshold:
        agg_dir = 1
    elif score < -threshold:
        agg_dir = -1
    else:
        out["reason"] = "neutral_vote"
        return out

    out["direction"] = agg_dir
    out["confidence"] = round(min(1.0, max(0.0, abs(score))), 3)

    if best and int(best.get("direction") or 0) == agg_dir:
        out["sl_pips"] = float(best.get("sl_pips") or 0.0)
        out["tp_pips"] = float(best.get("tp_pips") or 0.0)
        out["reason"] = f"vote_from_{best.get('_name') or best.get('name') or 'strategy'}"
    else:
        out["reason"] = "vote_weight_only"

    return out
