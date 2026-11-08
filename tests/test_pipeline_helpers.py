from airline_ops.pipeline import percent, source_month_from_path


def test_source_month_is_read_from_bts_landing_path():
    path = "/Volumes/workspace/airline_ops/raw_files/bts/on_time/source_month=2026-08/flights.csv"
    assert source_month_from_path(path) == "2026-08"


def test_source_month_rejects_missing_or_invalid_month():
    import pytest

    with pytest.raises(ValueError):
        source_month_from_path("/raw_files/no_month/flights.csv")
    with pytest.raises(ValueError):
        source_month_from_path("/raw_files/source_month=2026-13/flights.csv")


def test_percent_handles_empty_group_and_rounds():
    assert percent(0, 0) == 0.0
    assert percent(1, 3) == 33.33
