from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts.experiments import run_eta_accept_benchmark as benchmark
from eta.dataset import (
    QUEUE_HISTORY_VARIANT, TARGET, build_accept_time_features, build_eta_samples, build_protocol_b_samples,
    read_lade_csv, rolling_origin_rows, validation_rows,
)
from eta.features import build_observed_accept_features
from eta.metrics import accept_eta_metrics, paired_cluster_bootstrap
from eta import model as eta_model


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

    observed = build_observed_accept_features(events).loc[past.order_id]
    truncated_observed = build_observed_accept_features(past)
    pd.testing.assert_frame_equal(observed, truncated_observed, check_exact=True)


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


def test_observed_queue_recent_history_and_batch_by_hand(write_lade_csv, tmp_path):
    source = write_lade_csv(tmp_path / "yantai.csv", [
        {"order_id": "old", "accept_time": "05-01 08:00:00", "delivery_time": "05-01 09:00:00"},
        {"order_id": "yesterday", "accept_time": "06-03 08:00:00", "delivery_time": "06-03 09:00:00"},
        {"order_id": "other-aoi", "aoi_id": 200, "courier_id": 2,
         "accept_time": "06-03 08:45:00", "delivery_time": "06-03 09:15:00"},
        {"order_id": "first", "accept_time": "06-04 07:30:00", "delivery_time": "06-04 08:00:00"},
        {"order_id": "second", "accept_time": "06-04 08:00:00", "delivery_time": "06-04 08:20:00"},
        {"order_id": "tie", "accept_time": "06-04 07:50:00", "delivery_time": "06-04 08:30:00"},
        {"order_id": "no-label", "accept_time": "06-04 08:10:00", "delivery_time": ""},
        {"order_id": "candidate", "accept_time": "06-04 08:30:00"},
        {"order_id": "batch-peer", "accept_time": "06-04 08:30:00", "lat": 37.35},
        {"order_id": "later", "accept_time": "06-04 08:31:00"},
    ])
    events = read_lade_csv(source)
    row = build_observed_accept_features(events).loc["candidate"]
    assert row.pending_order_count == 2
    assert row.courier_accepted_today == 4
    assert row.courier_delivered_today == 2
    assert row.courier_idle_min == 10
    assert row.courier_recent_gap_avg_min == 20
    assert row.aoi_past_count == 4
    assert row.aoi_past_avg_min == 42.5
    assert row.aoi_recent_count == row.region_hour_recent_count == 3
    assert row.aoi_recent_avg_min == pytest.approx(110 / 3)
    assert row.region_hour_recent_avg_min == pytest.approx(110 / 3)
    assert row.batch_size == 2 and row.batch_distance_rank == 1

    changed = events.loc[~events.order_id.isin(["batch-peer", "later"])].copy()
    changed.loc[changed.order_id.isin(["tie", "candidate"]), "delivery_ts"] = pd.NaT
    row_without_peers = build_observed_accept_features(changed).loc["candidate"]
    assert row_without_peers.batch_size == 1
    columns = row.index.difference(["batch_size", "batch_distance_rank"])
    pd.testing.assert_series_equal(row[columns], row_without_peers[columns])


def test_rolling_origin_keeps_common_queries_and_censors_fit_and_calibration(write_lade_csv, tmp_path):
    rows = [
        {"order_id": f"order-{day}", "accept_time": f"06-{day:02d} 08:00:00",
         "delivery_time": f"06-{day:02d} 09:00:00"}
        for day in range(1, 13)
    ]
    rows.extend([
        {"order_id": "train-late", "accept_time": "06-06 08:00:00", "delivery_time": "06-07 00:00:00"},
        {"order_id": "calibration-late", "accept_time": "06-07 08:00:00", "delivery_time": "06-08 00:00:00"},
    ])
    events = read_lade_csv(write_lade_csv(tmp_path / "yantai.csv", rows))
    config = {"max_queries_per_courier_day": 12, "query_seed": 42}
    registered = build_protocol_b_samples(events, config)
    observed = build_protocol_b_samples(events, config, observed=True)
    identity = ["order_id", "query_ts", TARGET, "split"]
    pd.testing.assert_frame_equal(registered[identity], observed[identity])

    orders = events.set_index("order_id")
    windows = list(rolling_origin_rows(observed, orders))
    train, calibration, _ = windows[0]
    assert "train-late" not in set(train.order_id)
    assert "calibration-late" not in set(calibration.order_id)
    for fit, calibration, score in windows:
        assert fit.order_id.map(orders.delivery_ts).max() < calibration.query_ts.min().normalize()
        assert calibration.order_id.map(orders.delivery_ts).max() < score.query_ts.min().normalize()
        assert set(score.split) == {"validation"}
    actual = pd.concat([score for _, _, score in windows]).order_id
    assert actual.is_unique and set(actual) == set(registered.loc[registered.split.eq("validation"), "order_id"])


def test_accept_fit_uses_train_medians_for_calibration_and_prediction(monkeypatch):
    train = pd.DataFrame({"distance": [2., 4., np.nan], TARGET: [10., 20., 30.]})
    calibration = pd.DataFrame({"distance": [np.nan, 100.], TARGET: [100., 200.]})

    class Estimator:
        def predict(self, rows):
            np.testing.assert_array_equal(rows.distance, [3.])
            return np.array([-1.])

    def fit(train_inputs, calibration_inputs, features, seed):
        assert features == ["distance"] and seed == 43
        np.testing.assert_array_equal(train_inputs.distance, [2., 4., 3.])
        np.testing.assert_array_equal(calibration_inputs.distance, [3., 100.])
        return Estimator()

    monkeypatch.setattr(eta_model, "fit_lightgbm", fit)
    fitted = eta_model.fit_accept_eta(train, calibration, ["distance"], seed=43)
    np.testing.assert_array_equal(fitted.predict(pd.DataFrame({"distance": [np.nan]})), [0.])


def test_wape_and_paired_bootstrap_weight_orders_and_cluster_by_courier_day():
    scored = pd.DataFrame({
        "y": [10., 10., 10.], "candidate": [10., 10., 0.], "reference": [20., 20., 10.],
        "cluster": ["courier1|2022-06-01", "courier1|2022-06-01", "courier2|2022-06-01"],
    })
    assert accept_eta_metrics(scored.y, scored.candidate)["wape_pct"] == pytest.approx(100 / 3)
    result = paired_cluster_bootstrap(scored, "y", "candidate", "reference", "cluster", n_boot=200)
    assert result["clusters"] == 2 and result["pairs"] == 3
    assert result["difference_mae_left_minus_right"] == pytest.approx(-10 / 3)
    assert (result["ci95_low"], result["ci95_high"]) == (-10, 10)


def test_rolling_report_preserves_model_seed_when_recording_bootstrap(write_lade_csv, tmp_path, monkeypatch):
    rows = [
        {"order_id": f"{courier}-{day}", "courier_id": courier,
         "accept_time": f"06-{day:02d} 08:00:00", "delivery_time": f"06-{day:02d} 09:00:00"}
        for day in range(1, 13) for courier in (1, 2)
    ]
    source = write_lade_csv(tmp_path / "yantai.csv", rows)
    orders = read_lade_csv(source).set_index("order_id")
    config = {"max_queries_per_courier_day": 12, "query_seed": 42}
    samples = build_protocol_b_samples(orders.reset_index(), config)
    prereg = {"model": {"seeds": [43]}, "bootstrap": {"n_resamples": 10, "seed": 2022}}
    monkeypatch.setattr(benchmark, "load_benchmark_samples", lambda _: (prereg, orders, samples))

    class ConstantModel:
        def predict(self, evaluation):
            return np.full(len(evaluation), 60.)

    monkeypatch.setattr(benchmark, "fit_accept_eta", lambda *_: ConstantModel())
    output = tmp_path / "rolling.csv"
    benchmark.run_rolling_origin(source, output)
    result = pd.read_csv(output)
    row = result.loc[result.scope.eq("pooled") & result.variant.eq(QUEUE_HISTORY_VARIANT)].iloc[0]
    assert row.seed == 43 and row.bootstrap_seed == 2022
    assert row.n == 4 and row.clusters == 4 and row.mae == 0
