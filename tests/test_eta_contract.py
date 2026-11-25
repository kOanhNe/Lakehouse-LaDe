import pandas as pd
import pytest

from eta.features import (
    FeatureContractError,
    assert_truncation_invariant,
    build_query_features,
    load_schema,
    select_role,
    truncate_as_of,
    validate_columns,
)
from eta.splits import query_grid, time_boundaries, assign_time_split


def event_frame():
    schema = load_schema()
    cols = [item["name"] for item in schema["layers"]["silver"]["columns"]]
    base = {column: None for column in cols}

    def row(order_id, accept, delivery, delivery_lat, delivery_lng, courier=1):
        item = base.copy()
        item.update({
            "order_id": order_id, "region_id": 1, "city": "Yantai", "courier_id": courier,
            "lng": 121.40, "lat": 37.36, "aoi_id": 10, "aoi_type": 2,
            "accept_ts": pd.Timestamp(accept), "accept_gps_ts": pd.Timestamp(accept),
            "accept_gps_lng": 121.39, "accept_gps_lat": 37.37,
            "delivery_ts": pd.Timestamp(delivery) if delivery else pd.NaT,
            "delivery_gps_ts": pd.Timestamp(delivery) if delivery else pd.NaT,
            "delivery_gps_lng": delivery_lng, "delivery_gps_lat": delivery_lat,
            "ds": "0601", "ds_date": pd.Timestamp("2022-06-01"),
            "delivery_minutes": 60.0 if delivery else None, "day_gap": 0,
            "distance_km": 1.0, "delivery_stop_distance_km": 1.0,
            "source_file": "fixture.csv", "run_id": "fixture",
        })
        return item

    return pd.DataFrame([
        row("completed", "2022-06-01 08:00", "2022-06-01 09:00", 37.35, 121.41),
        row("candidate", "2022-06-01 10:00", "2022-06-01 11:00", 37.80, 121.80),
        row("future", "2022-06-01 12:00", "2022-06-01 13:00", 38.00, 122.00),
    ])


def test_unknown_columns_fail_and_ignored_columns_are_not_features():
    frame = event_frame()
    validate_columns(frame, "silver")
    assert "run_id" not in select_role(frame, "candidate")
    with pytest.raises(FeatureContractError, match="Undeclared"):
        validate_columns(frame.assign(unregistered_future_feature=1), "silver")


def test_truncation_keeps_pending_but_hides_delivery_observations():
    frame = event_frame()
    visible = truncate_as_of(frame, "2022-06-01 10:30")
    assert set(visible.order_id) == {"completed", "candidate"}
    candidate = visible.loc[visible.order_id.eq("candidate")].iloc[0]
    assert pd.isna(candidate.delivery_ts)
    assert pd.isna(candidate.delivery_gps_lng)
    assert visible.loc[visible.order_id.eq("completed"), "delivery_gps_lng"].iloc[0] == 121.41


def test_future_delivery_changes_cannot_change_features():
    frame = event_frame()
    mutated = frame.copy()
    mutated.loc[mutated.order_id.eq("candidate"), ["delivery_ts", "delivery_gps_ts", "delivery_gps_lng", "delivery_gps_lat"]] = [
        pd.Timestamp("2022-06-03 23:00"), pd.Timestamp("2022-06-03 23:00"), 0.0, 0.0]
    first = build_query_features(frame, "2022-06-01 10:30", "candidate")
    second = build_query_features(mutated, "2022-06-01 10:30", "candidate")
    pd.testing.assert_frame_equal(first, second, check_dtype=False, check_exact=False)
    assert_truncation_invariant(frame, "2022-06-01 10:30", "candidate")


def test_query_grid_is_deterministic():
    frame = event_frame()
    first = query_grid(frame, max_per_courier_day=2, seed=42)
    second = query_grid(frame.sample(frac=1, random_state=9), max_per_courier_day=2, seed=42)
    pd.testing.assert_frame_equal(first, second)


def test_time_split_has_non_overlapping_dates():
    frame = event_frame().assign(accept_ts=pd.date_range("2022-06-01", periods=3, freq="D"))
    boundaries = time_boundaries(frame)
    labels = assign_time_split(frame, boundaries)
    assert set(labels.dropna()) == {"train", "validation", "test"}
    assert labels.tolist() == ["train", "validation", "test"]
