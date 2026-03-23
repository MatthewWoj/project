from patternfail.pipeline import run_pipeline


def test_pipeline_module_imports() -> None:
    assert callable(run_pipeline)
