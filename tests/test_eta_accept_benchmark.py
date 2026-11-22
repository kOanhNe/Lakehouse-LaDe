import pandas as pd

from scripts.run_eta_accept_benchmark import accept_time_features, validation_rows
from src.eta.dataset import build_eta_samples, read_lade_csv


def test_accept_time_features_use_observed_aoi_and_gps(write_lade_csv, tmp_path):
    source = write_lade_csv(tmp_path / "yantai.csv", [
        {"order_id": "completed", "delivery_time": "06-04 08:30:00"},
        {"order_id": "pending", "delivery_time": "06-04 12:00:00"},
        {"order_id": "other-aoi", "aoi_id": 200, "delivery_time": "06-04 08:30:00"},
        {"order_id": "candidate", "accept_time": "06-04 09:00:00", "delivery_time": "06-04 11:00:00",
         "accept_gps_time": "06-04 09:01:00"},
    ])
    features = accept_time_features(read_lade_csv(source).set_index("order_id"))
    assert features.loc["candidate", "aoi_past_count"] == 1
    assert features.loc["candidate", "aoi_past_avg_min"] == 30
    assert pd.isna(features.loc["candidate", "accept_to_stop_km"])
    assert features.loc["completed", "accept_to_stop_km"] > 0


def test_training_labels_are_available_before_validation(write_lade_csv, tmp_path):
    source = write_lade_csv(tmp_path / "yantai.csv", [
        {"order_id": "train", "accept_time": "06-01 08:00:00", "delivery_time": "06-01 09:00:00"},
        {"order_id": "late-label", "accept_time": "06-02 08:00:00", "delivery_time": "06-03 00:00:00"},
        {"order_id": "validation", "accept_time": "06-03 08:00:00", "delivery_time": "06-03 09:00:00"},
        {"order_id": "heldout", "accept_time": "06-04 08:00:00", "delivery_time": "06-04 09:00:00"},
    ])
    events = read_lade_csv(source)
    samples = build_eta_samples(events)
    train, validation = validation_rows(samples, events.set_index("order_id"))
    assert train.order_id.tolist() == ["train"]
    assert validation.order_id.tolist() == ["validation"]
