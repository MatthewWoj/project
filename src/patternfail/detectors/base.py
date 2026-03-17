from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class Detector(ABC):
    name: str
    family: str

    @abstractmethod
    def detect(self, bars: pd.DataFrame, pivots: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
