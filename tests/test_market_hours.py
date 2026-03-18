import pandas as pd

import patternfail.data.market_hours as mh


class _FakeCalendar:
    def __init__(self, schedule: pd.DataFrame):
        self.schedule = schedule


class _FakeXcals:
    def __init__(self, schedule: pd.DataFrame):
        self._schedule = schedule

    def get_calendar(self, _name: str):
        return _FakeCalendar(self._schedule)


def test_active_minutes_mask_supports_open_close_columns(monkeypatch):
    idx = pd.date_range("2025-01-02 14:30", periods=3, freq="min", tz="UTC")
    sched = pd.DataFrame(
        {
            "open": [pd.Timestamp("2025-01-02 14:30", tz="UTC")],
            "close": [pd.Timestamp("2025-01-02 21:00", tz="UTC")],
        },
        index=[pd.Timestamp("2025-01-02")],
    )
    monkeypatch.setattr(mh, "xcals", _FakeXcals(sched))

    mask = mh.active_minutes_mask(idx, "equity_us")
    assert mask.all()


def test_active_minutes_mask_supports_market_open_close_columns(monkeypatch):
    idx = pd.date_range("2025-01-02 14:30", periods=3, freq="min", tz="UTC")
    sched = pd.DataFrame(
        {
            "market_open": [pd.Timestamp("2025-01-02 14:30", tz="UTC")],
            "market_close": [pd.Timestamp("2025-01-02 21:00", tz="UTC")],
        },
        index=[pd.Timestamp("2025-01-02")],
    )
    monkeypatch.setattr(mh, "xcals", _FakeXcals(sched))

    mask = mh.active_minutes_mask(idx, "equity_us")
    assert mask.all()
