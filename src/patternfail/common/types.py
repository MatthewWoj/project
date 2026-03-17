from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class VenueType(str, Enum):
    EQUITY_US = "equity_us"
    FX = "fx"
    METAL = "metal"
    CRYPTO = "crypto"


class PatternType(str, Enum):
    HS_TOP = "HS_TOP"
    HS_INV = "HS_INV"
    HS_TOP_IEEE = "HS_TOP_IEEE"
    HS_INV_IEEE = "HS_INV_IEEE"
    DT = "DT"
    DB = "DB"
    TRIANGLE_ASC = "TRIANGLE_ASC"
    TRIANGLE_DESC = "TRIANGLE_DESC"
    TRIANGLE_SYM = "TRIANGLE_SYM"
    TRIANGLE_GENERIC = "TRIANGLE_GENERIC"
    CHANNEL = "CHANNEL"


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class PatternInstance:
    pattern_id: str
    asset: str
    venue_type: str
    timeframe: str
    pattern_type: str
    direction: str
    t_start_utc: Any
    t_end_utc: Any
    t_confirm_utc: Any
    score: float
    geometry_params: Dict[str, Any] = field(default_factory=dict)
    detector_family: str = "geometric"
    detector_name: str = ""
    context_labels: Dict[str, Any] = field(default_factory=dict)
    nested_in_pattern_id: Optional[str] = None
    outcome_labels: Optional[Dict[str, Any]] = None
