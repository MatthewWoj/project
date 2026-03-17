from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class AppConfig:
    raw: Dict[str, Any]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AppConfig":
        with open(path, "r", encoding="utf-8") as f:
            return cls(raw=yaml.safe_load(f))

    def __getitem__(self, k: str) -> Any:
        return self.raw[k]

    def get(self, k: str, default=None) -> Any:
        return self.raw.get(k, default)
