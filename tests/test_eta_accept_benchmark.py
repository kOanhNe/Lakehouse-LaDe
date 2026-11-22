from pathlib import Path

import pandas as pd

from scripts.run_eta_accept_benchmark import validation_rows
from src.eta.dataset import build_accept_time_features, build_eta_samples, read_lade_csv


def test_accept_time_features_use_observed_aoi_and_gps(write_lade_csv, tmp_path):
    source = write_lade_csv(tmp_path / "yantai.csv", [
        {"order_id": "completed", "delivery_time": "06-04 08:30:00"},
        {"order_id": "pending", "delivery_time": "06-04 12:00:00"},
        {"order_id": "other-aoi", "aoi_id": 200, "delivery_time": "06-04 08:30:00"},
        {"order_id": "candidate", "accept_time": "06-04 09:00:00", "delivery_time": "06-04 11:00:00",
         "accept_gps_time": "06-04 09:01:00"},
    ])
    events = read_lade_csv(source).drop(columns="delivery_minutes")
    features = build_accept_time_features(events)
    assert features.loc["candidate", "aoi_past_count"] == 1
    assert features.loc["candidate", "aoi_past_avg_min"] == 30
    assert pd.isna(features.loc["candidate", "accept_to_stop_km"])
    assert features.loc["completed", "accept_to_stop_km"] > 0


def test_yantai_features_are_invariant_when_future_events_are_removed():
    source = Path(__file__).resolve().parents[1] / "data" / "delivery_yt.csv"
    events = read_lade_csv(source)
    cutoff = pd.Timestamp("2022-08-15")
    past = events.loc[events.accept_ts.lt(cutoff)].copy()
    assert 0 < len(past) < len(events)
    future_deliveries = past.delivery_ts.ge(cutoff)
    assert future_deliveries.any()
    past.loc[future_deliveries, "delivery_ts"] = pd.NaT

    original = build_accept_time_features(events).loc[past.order_id]
    truncated = build_accept_time_features(past)
    pd.testing.assert_frame_equal(original, truncated, check_exact=True)


def test_training_labels_are_available_before_validation(write_lade_csv, tmp_path):
    source = write_lade_csv(tmp_path / "yantai.csv", [
        {"order_id": "train", "accept_time": "06-01 08:00:00", "delivery_time": "06-01 09:00:00"},
        {"order_id": "late-label", "accept_time": "06-02 08:00:00", "delivery_time": "06-03 00:00:00"},
        {"order_id": "validation", "accept_time": "06-03 08:00:00", "delivery_time": "06-03 09:00:00"},
        {"order_id": "heldout", "accept_time": "06-04 08:00:00", "delivery_time": "06-04 09:00:00"},
    ])
    events = read_lade_csv(source)
    base_samples = build_eta_samples(events)
    samples = build_eta_samples(events, include_aoi_gps=True)
    pd.testing.assert_frame_equal(samples.loc[:, base_samples.columns], base_samples)
    assert "aoi_past_count" in samples
    train, validation = validation_rows(samples, events.set_index("order_id"))
    assert train.order_id.tolist() == ["train"]
    assert validation.order_id.tolist() == ["validation"]
