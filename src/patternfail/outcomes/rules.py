from __future__ import annotations


def level_with_atr(base: float, atr: float, mult: float, up: bool) -> float:
    return base + mult * atr if up else base - mult * atr
