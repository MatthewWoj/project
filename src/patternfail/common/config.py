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
        path = Path(path)
        text = path.read_text(encoding="utf-8")
        try:
            return cls(raw=yaml.safe_load(text))
        except yaml.YAMLError as exc:
            hint = ""
            if "\\" in text and ('"' in text or "'" in text):
                hint = (
                    "\nHint: Windows paths inside YAML should use forward slashes "
                    "(e.g. C:/Users/name/file.csv) or single-quoted strings."
                )
            raise ValueError(f"Failed to parse YAML config at {path}.{hint}") from exc

    def __getitem__(self, k: str) -> Any:
        return self.raw[k]

    def get(self, k: str, default=None) -> Any:
        return self.raw.get(k, default)
