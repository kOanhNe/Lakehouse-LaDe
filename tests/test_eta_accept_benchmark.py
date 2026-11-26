from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from scripts.experiments import run_eta_accept_benchmark as benchmark
from eta.dataset import (
    QUEUE_HISTORY_VARIANT, TARGET, build_accept_time_features, build_eta_samples, build_protocol_b_samples,
    read_lade_csv, rolling_origin_rows, split_interval_calibration, validation_rows,
)
from eta.features import build_observed_accept_features
from eta.intervals import EtaIntervalCalibration, QUANTILE_LEVELS
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


def test_conformal_ranks_crossings_and_deadline_flag():
    quantiles = np.tile([20., 25., 30.], (9, 1))
    calibration = EtaIntervalCalibration.fit(np.arange(30., 39.), quantiles)
    assert calibration.interval_offset_min == 7
    assert calibration.promise_offset_min == 8

    rows = calibration.predict(np.array([[30., 20., 25.], [-5., 3., 1.]]), deadline_minutes=37.5)
    assert rows.p50_minutes.tolist() == [25., 1.]
    assert rows.lower_80_minutes.tolist() == [13., 0.]
    assert rows.upper_80_minutes.tolist() == [37., 10.]
    assert rows.promise_p90_minutes.tolist() == [38., 11.]
    assert rows.late_risk.tolist() == [True, False]
    assert not calibration.predict(quantiles[:1], deadline_minutes=38.).late_risk.iloc[0]

    scored = rows.assign(**{TARGET: [40., 20.]}, deadline_minutes=37.5, promise_delta_vs_region_hour_min=0.)
    metrics = benchmark.interval_metrics(scored)
    assert metrics["deadline_late_base_pct"] == metrics["late_risk_flag_rate_pct"] == 50.
    assert metrics["late_risk_precision_pct"] == metrics["late_risk_recall_pct"] == 100.
    assert metrics["late_risk_lift"] == 2.
    no_flags = benchmark.interval_metrics(scored.assign(late_risk=False))
    assert no_flags["late_risk_recall_pct"] == 0.
    assert no_flags["late_risk_precision_pct"] is None and no_flags["late_risk_lift"] is None
    no_late = benchmark.interval_metrics(scored.assign(**{TARGET: [30., 20.]}))
    assert no_late["late_risk_precision_pct"] == 0.
    assert no_late["late_risk_recall_pct"] is None and no_late["late_risk_lift"] is None

    shrink = EtaIntervalCalibration.fit(np.full(9, 25.), quantiles)
    tight = shrink.predict(np.array([[29., 30., 31.]]))
    assert tight.lower_80_minutes.iloc[0] == tight.upper_80_minutes.iloc[0] == 30.
    assert tight.promise_p90_minutes.iloc[0] == 30.
    with pytest.raises(ValueError, match="Too few calibration labels"):
        EtaIntervalCalibration.fit(np.arange(30., 38.), quantiles[:8])


def test_interval_calibration_keeps_days_together_and_censors_late_tuning_labels():
    rows = pd.DataFrame({
        "order_id": ["early", "late", "same-day", "conformal", "last"],
        "query_ts": pd.to_datetime(["2022-06-01 08:00", "2022-06-02 08:00", "2022-06-02 08:00",
                                    "2022-06-03 08:00", "2022-06-04 08:00"]),
    })
    orders = rows.assign(delivery_ts=pd.to_datetime([
        "2022-06-01 09:00", "2022-06-03 00:00", "2022-06-02 09:00", "2022-06-03 09:00", "2022-06-04 09:00",
    ])).set_index("order_id")
    tuning, conformal = split_interval_calibration(rows, orders)
    assert tuning.order_id.tolist() == ["early", "same-day"]
    assert conformal.order_id.tolist() == ["conformal", "last"]


def test_region_hour_quantiles_fall_back_to_fit_history_for_sparse_buckets():
    durations = np.append(np.arange(1., 31.), 300.)
    fit = pd.DataFrame({"candidate_region_id": [1] * 30 + [2], "accept_hour": 8., TARGET: durations})
    scored = pd.DataFrame({"candidate_region_id": [1, 2, 1, None], "accept_hour": [10., 8., 12., 8.]})
    expected = np.vstack([np.quantile(durations[:30], QUANTILE_LEVELS),
                          np.tile(np.quantile(durations, QUANTILE_LEVELS), (3, 1))])
    np.testing.assert_allclose(benchmark.region_hour_quantiles(fit, scored), expected)


def test_interval_report_scores_only_registered_development_orders(write_lade_csv, tmp_path, monkeypatch):
    rows = []
    for day in pd.date_range("2022-05-01", periods=60):
        for courier in (1, 2, 3):
            rows.append({"order_id": f"{courier}-{day:%m%d}", "courier_id": courier,
                         "accept_time": f"{day:%m-%d} 08:00:00", "delivery_time": f"{day:%m-%d} 09:00:00"})
    source = write_lade_csv(tmp_path / "yantai.csv", rows)
    orders = read_lade_csv(source).set_index("order_id")
    config = {"max_queries_per_courier_day": 12, "query_seed": 42}
    samples = build_protocol_b_samples(orders.reset_index(), config)
    prereg = {"bootstrap": {"n_resamples": 10, "seed": 2022}}
    monkeypatch.setattr(benchmark, "load_benchmark_samples", lambda _: (prereg, orders, samples))

    levels = []

    def fit(train, tuning, features, seed, objective, params):
        assert seed == 43 and objective == "quantile" and params["metric"] == "quantile"
        level = params["alpha"]
        levels.append(level)
        assert train.order_id.map(orders.delivery_ts).max() < tuning.query_ts.min().normalize()
        return SimpleNamespace(estimator=SimpleNamespace(best_iteration_=10),
                               predict=lambda frame: np.full(len(frame), 50. + 20. * level))

    monkeypatch.setattr(benchmark, "fit_accept_eta", fit)
    output, per_order = tmp_path / "intervals.csv", tmp_path / "per-order.csv"
    benchmark.run_interval_benchmark(source, output, seeds=[43], predictions_output=per_order, deadline_minutes=55.)

    assert levels == list(QUANTILE_LEVELS) * 3
    report = pd.read_csv(output)
    candidate = report.loc[report.scope.eq("pooled") & report.variant.eq(QUEUE_HISTORY_VARIANT)].iloc[0]
    assert candidate.seed == 43 and candidate.bootstrap_seed == 2022
    assert candidate.interval_coverage_pct == candidate.promise_coverage_pct == 100
    predicted = pd.read_csv(per_order).loc[lambda frame: frame.variant.eq(QUEUE_HISTORY_VARIANT)]
    assert predicted.order_id.is_unique
    assert set(predicted.order_id) == set(samples.loc[samples.split.eq("validation"), "order_id"])
    assert predicted.late_risk.all() and candidate.flagged_n == len(predicted)
