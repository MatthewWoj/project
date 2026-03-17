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
    venue_type: VenueType
    timeframe: str
    pattern_type: PatternType
    t_start: Any
    t_end: Any
    t_confirm: Any
    direction: Direction
    geometry_params: Dict[str, Any] = field(default_factory=dict)
    score: float = 0.0
    score_components: Dict[str, float] = field(default_factory=dict)
    context_labels: Dict[str, Any] = field(default_factory=dict)
    outcome_labels: Optional[Dict[str, Any]] = None
