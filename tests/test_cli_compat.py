import argparse

import patternfail.cli as cli


def test_call_run_pipeline_filters_unsupported_kwargs(monkeypatch):
    called = {}

    def old_run_pipeline(config_path: str, stage: str = "full"):
        called["config"] = config_path
        called["stage"] = stage

    monkeypatch.setattr(cli, "run_pipeline", old_run_pipeline)
    args = argparse.Namespace(
        config="configs/default.yaml",
        stage="detect",
        surrogates_n=5,
        max_workers=4,
        progress_every=2,
    )

    cli._call_run_pipeline(args)
    assert called == {"config": "configs/default.yaml", "stage": "detect"}


def test_call_run_pipeline_passes_new_kwargs(monkeypatch):
    called = {}

    def new_run_pipeline(config_path: str, stage: str = "full", surrogates_n_override=None, max_workers_override=None, progress_every_override=None):
        called.update(
            {
                "config": config_path,
                "stage": stage,
                "surrogates_n_override": surrogates_n_override,
                "max_workers_override": max_workers_override,
                "progress_every_override": progress_every_override,
            }
        )

    monkeypatch.setattr(cli, "run_pipeline", new_run_pipeline)
    args = argparse.Namespace(
        config="configs/default.yaml",
        stage="detect",
        surrogates_n=5,
        max_workers=4,
        progress_every=2,
    )

    cli._call_run_pipeline(args)
    assert called == {
        "config": "configs/default.yaml",
        "stage": "detect",
        "surrogates_n_override": 5,
        "max_workers_override": 4,
        "progress_every_override": 2,
    }
