from dataclasses import dataclass, field
from typing import Sequence


@dataclass(frozen=True)
class SplitConfig:
    train_start: str = "2022-01-01"
    train_end: str = "2024-12-31"
    test_start: str = "2025-01-01"
    test_end: str = "2025-12-31"


@dataclass(frozen=True)
class ThresholdConfig:
    atr_period: int = 14
    zigzag_lambda: float = 2.0
    neckline_beta: float = 0.2
    shoulder_tolerance: float = 0.08
    trough_tolerance: float = 0.1
    peak_tolerance: float = 0.04
    dt_depth_kappa: float = 1.0
    triangle_convergence_gamma: float = 0.7
    triangle_residual_eta: float = 0.75
    triangle_width_omega: float = 2.0
    stale_n: int = 10
    stale_pct: float = 0.5
    channel_score_threshold: float = 0.75


@dataclass(frozen=True)
class SaxConfig:
    window: int = 80
    paa_segments: int = 12
    alphabet_size: int = 7


@dataclass(frozen=True)
class PipelineConfig:
    assets: Sequence[str] = field(default_factory=lambda: ["AAPL", "NVDA", "SPY", "QQQ", "EURUSD", "GBPUSD", "XAUUSD", "BTCUSDT"])
    output_timeframes: Sequence[str] = field(default_factory=lambda: ["3min", "5min", "15min", "1h", "4h", "1d", "1w"])
    split: SplitConfig = field(default_factory=SplitConfig)
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)
    sax: SaxConfig = field(default_factory=SaxConfig)
