from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from .schema import Direction, PatternInstance, PatternType, VenueType


def _mk_instance(pattern_type: PatternType, direction: Direction, asset: str, venue_type: str, timeframe: str, t_start, t_end, t_confirm, score: float, score_components: dict, geometry: dict) -> PatternInstance:
    return PatternInstance(
        pattern_id=str(uuid.uuid4()),
        asset=asset,
        venue_type=VenueType(venue_type),
        timeframe=timeframe,
        pattern_type=pattern_type,
        t_start=t_start,
        t_end=t_end,
        t_confirm=t_confirm,
        direction=direction,
        score=score,
        score_components=score_components,
        geometry_params=geometry,
    )


def detect_hs(df: pd.DataFrame, pivots: pd.DataFrame, timeframe: str, eps_shoulder: float = 0.08, eps_trough: float = 0.1, beta: float = 0.2) -> list[PatternInstance]:
    out = []
    for i in range(len(pivots) - 4):
        s = pivots.iloc[i : i + 5]
        kinds = tuple(s["kind"].tolist())
        if kinds not in [(1, -1, 1, -1, 1), (-1, 1, -1, 1, -1)]:
            continue
        x = s["price"].to_numpy()
        t = s["ts_utc"].to_list()
        top = kinds[0] == 1
        head_ok = (x[2] > x[0] and x[2] > x[4]) if top else (x[2] < x[0] and x[2] < x[4])
        if not head_ok:
            continue
        shoulder = abs(x[0] - x[4]) / max((abs(x[0]) + abs(x[4])) / 2, 1e-12)
        trough = abs(x[1] - x[3]) / max((abs(x[1]) + abs(x[3])) / 2, 1e-12)
        if shoulder > eps_shoulder or trough > eps_trough:
            continue

        t2_idx = int(df.index[df["ts_utc"] == t[1]][0])
        t4_idx = int(df.index[df["ts_utc"] == t[3]][0])
        if t4_idx == t2_idx:
            continue
        m = (x[3] - x[1]) / (t4_idx - t2_idx)

        confirm_idx = None
        for j in range(int(df.index[df["ts_utc"] == t[4]][0]) + 1, len(df)):
            neckline = x[1] + m * (j - t2_idx)
            atr = df.loc[j, "atr"] if pd.notna(df.loc[j, "atr"]) else 0
            c = df.loc[j, "close"]
            if top and c < neckline - beta * atr:
                confirm_idx = j
                break
            if (not top) and c > neckline + beta * atr:
                confirm_idx = j
                break
        if confirm_idx is None:
            continue

        vec = x - np.array([x[1], x[1], x[1], x[1], x[1]])
        vec = vec / (abs(vec[2]) + 1e-12)
        template = np.array([0.7, 0, 1.0, 0, 0.7]) if top else np.array([-0.7, 0, -1.0, 0, -0.7])
        score = float(np.sqrt(np.mean((vec - template) ** 2)))
        out.append(_mk_instance(
            PatternType.HS_TOP if top else PatternType.HS_INV,
            Direction.SHORT if top else Direction.LONG,
            df["asset"].iloc[0],
            df["venue_type"].iloc[0],
            timeframe,
            t[0], t[4], df.loc[confirm_idx, "ts_utc"], score,
            {"shoulder_similarity": shoulder, "trough_similarity": trough},
            {"neckline_slope": m},
        ))
    return out


def detect_double(df: pd.DataFrame, pivots: pd.DataFrame, timeframe: str, peak_tol: float = 0.04, beta: float = 0.2, kappa: float = 1.0) -> list[PatternInstance]:
    out = []
    for i in range(len(pivots) - 2):
        s = pivots.iloc[i : i + 3]
        kinds = tuple(s["kind"].tolist())
        if kinds not in [(1, -1, 1), (-1, 1, -1)]:
            continue
        x = s["price"].to_numpy()
        t = s["ts_utc"].to_list()
        sim = abs(x[0] - x[2]) / max((abs(x[0]) + abs(x[2])) / 2, 1e-12)
        if sim > peak_tol:
            continue
        mid_idx = int(df.index[df["ts_utc"] == t[1]][0])
        atr = df.loc[mid_idx, "atr"] if pd.notna(df.loc[mid_idx, "atr"]) else 0
        top = kinds[0] == 1
        depth = (min(x[0], x[2]) - x[1]) if top else (x[1] - max(x[0], x[2]))
        if depth < kappa * atr:
            continue
        confirm_idx = None
        for j in range(int(df.index[df["ts_utc"] == t[2]][0]) + 1, len(df)):
            c = df.loc[j, "close"]
            a = df.loc[j, "atr"] if pd.notna(df.loc[j, "atr"]) else 0
            if top and c < x[1] - beta * a:
                confirm_idx = j
                break
            if (not top) and c > x[1] + beta * a:
                confirm_idx = j
                break
        if confirm_idx is None:
            continue
        score = float(sim + (kappa * max(atr, 1e-12)) / max(depth, 1e-12))
        out.append(_mk_instance(
            PatternType.DT if top else PatternType.DB,
            Direction.SHORT if top else Direction.LONG,
            df["asset"].iloc[0],
            df["venue_type"].iloc[0],
            timeframe,
            t[0], t[2], df.loc[confirm_idx, "ts_utc"], score,
            {"similarity": sim, "depth": float(depth)},
            {},
        ))
    return out


def detect_triangles(df: pd.DataFrame, pivots: pd.DataFrame, timeframe: str, window: int = 8, beta: float = 0.2) -> list[PatternInstance]:
    out = []
    if len(pivots) < window:
        return out
    for i in range(len(pivots) - window + 1):
        s = pivots.iloc[i : i + window]
        highs = s[s["kind"] == 1]
        lows = s[s["kind"] == -1]
        if len(highs) < 2 or len(lows) < 2:
            continue
        h_idx = highs["bar_index"].to_numpy()
        l_idx = lows["bar_index"].to_numpy()
        h_p = highs["price"].to_numpy()
        l_p = lows["price"].to_numpy()
        a_u, b_u = np.polyfit(h_idx, h_p, 1)
        a_l, b_l = np.polyfit(l_idx, l_p, 1)
        t_s, t_e = int(s["bar_index"].iloc[0]), int(s["bar_index"].iloc[-1])
        w_s = (a_u * t_s + b_u) - (a_l * t_s + b_l)
        w_e = (a_u * t_e + b_u) - (a_l * t_e + b_l)
        if w_s <= 0 or w_e > 0.7 * w_s:
            continue
        ru = float(np.max(np.abs(h_p - (a_u * h_idx + b_u))))
        rl = float(np.max(np.abs(l_p - (a_l * l_idx + b_l))))

        confirm_idx = None
        direction = None
        for j in range(t_e + 1, len(df)):
            u = a_u * j + b_u
            l = a_l * j + b_l
            atr = df.loc[j, "atr"] if pd.notna(df.loc[j, "atr"]) else 0
            c = df.loc[j, "close"]
            if c > u + beta * atr:
                confirm_idx, direction = j, Direction.LONG
                break
            if c < l - beta * atr:
                confirm_idx, direction = j, Direction.SHORT
                break
        if confirm_idx is None:
            continue
        if abs(a_u) < 1e-3 and a_l > 1e-3:
            ptype = PatternType.TRIANGLE_ASC
        elif abs(a_l) < 1e-3 and a_u < -1e-3:
            ptype = PatternType.TRIANGLE_DESC
        elif a_u < 0 and a_l > 0:
            ptype = PatternType.TRIANGLE_SYM
        else:
            ptype = PatternType.TRIANGLE_GENERIC
        score = float(max(ru, rl) / max(w_s, 1e-12) + w_e / max(w_s, 1e-12))
        out.append(_mk_instance(
            ptype,
            direction,
            df["asset"].iloc[0],
            df["venue_type"].iloc[0],
            timeframe,
            s["ts_utc"].iloc[0], s["ts_utc"].iloc[-1], df.loc[confirm_idx, "ts_utc"], score,
            {"residual_u": ru, "residual_l": rl, "width_start": float(w_s), "width_end": float(w_e)},
            {"a_u": float(a_u), "b_u": float(b_u), "a_l": float(a_l), "b_l": float(b_l)},
        ))
    return out
