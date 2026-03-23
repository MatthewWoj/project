from pathlib import Path

import pytest

from patternfail.common.config import AppConfig


def test_config_loader_gives_windows_path_hint(tmp_path: Path):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text('input_csv:\n  BTCUSDT: "C:\\Users\\Mateo\\Desktop\\bb\\BTCUSDT.csv"\n', encoding="utf-8")

    with pytest.raises(ValueError) as excinfo:
        AppConfig.from_yaml(cfg)

    assert "Windows paths inside YAML should use forward slashes" in str(excinfo.value)
