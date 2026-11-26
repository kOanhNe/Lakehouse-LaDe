from datetime import date, datetime
import json

import pytest

from lakehouse import notebooks


def test_pipeline_notebook_definitions_compile():
    assert notebooks.compile_notebook_steps()


def test_notebook_loader_skips_unmarked_examples(tmp_path, monkeypatch):
    name = "stage.ipynb"
    example = {"cell_type": "code", "id": "example", "source": "result = fail()", "metadata": {}}
    cells = [
        {"cell_type": "code", "id": "definitions", "metadata": {"tags": ["pipeline-step"]},
         "source": "OFFSET = 4\n\ndef transform(value):\n    return value + OFFSET"},
        example,
    ]
    path = tmp_path / name
    path.write_text(json.dumps({"cells": cells}), encoding="utf-8")
    monkeypatch.setattr(notebooks, "NOTEBOOK_DIR", tmp_path)
    monkeypatch.setattr(notebooks, "PIPELINE_NOTEBOOKS", (name,))

    steps = notebooks.load_notebook_steps()
    assert steps.transform(3) == 7

    example["metadata"]["tags"] = ["pipeline-step"]
    path.write_text(json.dumps({"cells": cells}), encoding="utf-8")
    with pytest.raises(ValueError, match="Move runnable examples"):
        notebooks.load_notebook_steps()


@pytest.fixture(scope="module")
def steps(local_spark):
    return notebooks.load_notebook_steps()


def test_bronze_contract_and_unique_keys(local_spark, steps, tmp_path, write_lade_csv):
    path = write_lade_csv(tmp_path / "orders.csv", [{"order_id": "001"}, {"order_id": "002"}])
    raw = steps.read_bronze_csv(local_spark, path.as_uri())
    steps.require_unique_orders(raw)
    assert set(row.order_id for row in raw.collect()) == {"001", "002"}
    malformed = write_lade_csv(tmp_path / "wrong.csv", [{}], columns=["order_id"])
    with pytest.raises(ValueError, match="CSV LaDe-D"):
        steps.read_bronze_csv(local_spark, malformed.as_uri())
    for invalid_id in ("001", "   "):
        bad = write_lade_csv(tmp_path / "ids.csv", [{"order_id": "001"}, {"order_id": invalid_id}])
        with pytest.raises(ValueError, match="order_id"):
            steps.require_unique_orders(steps.read_bronze_csv(local_spark, bad.as_uri()))


def test_silver_cohort_flags_and_rejected_reasons(local_spark, steps, tmp_path, write_lade_csv):
    rows = [
        {"order_id": "normal"},
        {"order_id": "over24", "delivery_time": "06-06 09:00:00"},
        {"order_id": "far", "accept_gps_lat": 35.0},
        {"order_id": "gps", "accept_gps_lat": 0, "accept_gps_lng": 0},
        {"order_id": "invalid", "delivery_time": "malformed"},
        {"order_id": "zero", "delivery_time": "06-04 08:00:00"},
        {"order_id": "negative", "delivery_time": "06-04 07:59:00"},
    ]
    path = write_lade_csv(tmp_path / "orders.csv", rows)
    raw = steps.read_bronze_csv(local_spark, path.as_uri())
    silver, rejected = steps.split_silver(raw)
    assert steps.reconcile_silver(raw, silver, rejected) == {
        "bronze_rows": 7, "silver_rows": 4, "rejected_rows": 3,
    }
    accepted = {row.order_id: row for row in silver.collect()}
    assert accepted["normal"].delivery_minutes == 60
    assert accepted["over24"].q_duration_gt24h and accepted["over24"].day_gap == 2
    assert accepted["far"].q_accept_distance_gt50km
    assert accepted["gps"].q_accept_gps_invalid and accepted["gps"].distance_km is None
    assert {row.order_id: row.reject_reason for row in rejected.collect()} == {
        "invalid": "invalid_timestamp", "zero": "non_positive_duration", "negative": "non_positive_duration",
    }
    # A duplicate and an omitted ID can leave the total row count unchanged.
    damaged = silver.filter("order_id <> 'normal'").unionByName(silver.filter("order_id = 'far'"))
    with pytest.raises(ValueError, match="order_id"):
        steps.reconcile_silver(raw, damaged, rejected)
    with pytest.raises(ValueError, match="lý do"):
        steps.reconcile_silver(raw, silver, rejected.withColumn("reject_reason", rejected.order_id))


def test_scd2_tie_boundaries_and_join_grain(local_spark):
    from lakehouse.common import courier_history, join_courier_history

    orders = local_spark.createDataFrame([
        ("a", 1, 20, datetime(2022, 6, 4, 1)),
        ("b", 1, 10, datetime(2022, 6, 4, 2)),
        ("c", 1, 20, datetime(2022, 6, 5, 1)),
        ("d", 1, 10, datetime(2022, 6, 6, 1)),
    ], ["order_id", "courier_id", "region_id", "accept_ts"])
    history = courier_history(orders)
    rows = history.orderBy("effective_date").collect()
    assert [row.region_id for row in rows] == [10, 20, 10]
    assert [row.end_date for row in rows] == [date(2022, 6, 5), date(2022, 6, 6), date.max]
    assert [row.is_active for row in rows] == [False, False, True]
    assert all(row.effective_date < row.end_date for row in rows)
    joined = join_courier_history(orders, history).orderBy("order_id").collect()
    assert [row.courier_region_id for row in joined] == [10, 10, 20, 10]
    assert joined[0].region_id == 20 and joined[0].courier_region_id == 10
    with pytest.raises(ValueError, match="order_id"):
        join_courier_history(orders, history.unionByName(history))
    with pytest.raises(ValueError, match="exactly one"):
        join_courier_history(orders, history.filter("region_id = 10"))


def test_haversine_valid_and_invalid_coordinates(local_spark):
    from lakehouse.common import haversine

    coordinates = local_spark.createDataFrame([
        (37.0, 121.0, 37.0, 121.0),
        (37.0, 121.0, None, None),
        (37.0, 121.0, 0.0, 0.0),
        (37.0, 121.0, 200.0, 121.0),
        (37.0, 121.0, float("nan"), 121.0),
    ], ["la1", "ln1", "la2", "ln2"])
    distances = coordinates.select(haversine("la1", "ln1", "la2", "ln2").alias("km")).collect()
    assert distances[0].km == 0
    assert all(row.km is None for row in distances[1:])


def test_spark_baseline_features_do_not_depend_on_future_labels(local_spark, steps, tmp_path, write_lade_csv):
    from pyspark.sql import functions as F

    source = write_lade_csv(tmp_path / "baseline.csv", [
        {"order_id": "done", "delivery_time": "06-04 08:20:00"},
        {"order_id": "long", "accept_time": "06-04 08:10:00", "delivery_time": "06-06 10:00:00",
         "accept_gps_lat": 37.6},
        {"order_id": "unknown-label", "accept_time": "06-04 08:30:00", "delivery_time": "",
         "accept_gps_lat": 37.4},
        {"order_id": "ends-at-query", "accept_time": "06-04 08:45:00", "delivery_time": "06-04 09:00:00"},
        {"order_id": "candidate", "accept_time": "06-04 09:00:00", "delivery_time": "06-05 00:00:00",
         "accept_gps_time": "06-04 09:01:00"},
        {"order_id": "peer", "accept_time": "06-04 09:00:00", "delivery_time": "06-04 11:00:00"},
        {"order_id": "test-long", "accept_time": "06-05 09:00:00", "delivery_time": "06-07 12:00:00",
         "accept_gps_time": "06-05 09:01:00"},
    ])
    bronze = steps.read_bronze_csv(local_spark, source.as_uri())
    observed = {r.order_id: r for r in steps.spark_baseline_features(bronze).collect()}
    candidate = observed["candidate"]
    assert candidate.order_rank == observed["peer"].order_rank == 5
    assert candidate.courier_completed_count == 1 and candidate.courier_completed_avg_min == 20
    prior_ids = ("done", "long", "unknown-label", "ends-at-query")
    assert candidate.courier_prior_distance_km == pytest.approx(
        sum(observed[order_id].gps_distance_km for order_id in prior_ids) / 4,
    )
    assert candidate.gps_missing == 1 and candidate.gps_distance_km is None
    assert observed["test-long"].order_rank == 1

    hidden = (bronze.filter(~F.col("order_id").isin("peer", "test-long"))
              .withColumn("delivery_time", F.when(F.col("delivery_time") < "06-04 09:00:00",
                                                  F.col("delivery_time"))))
    truncated = steps.spark_baseline_features(hidden).filter(F.col("order_id") == "candidate").first()
    assert truncated == candidate

    fact, _rejected = steps.split_silver(bronze)
    cutoff = "2022-06-05"
    fit, score = steps.split_spark_baseline_rows(fact, cutoff)
    fit_ids = {r.order_id for r in fit.select("order_id").collect()}
    assert fit_ids == {"done", "ends-at-query", "peer"}
    assert [(r.order_id, r.delivery_minutes) for r in score.select("order_id", "delivery_minutes").collect()] == [
        ("test-long", 3060),
    ]

    before_cutoff = steps.parse_timestamp("delivery_time") < F.lit(cutoff).cast("timestamp")
    masked_bronze = bronze.withColumn("delivery_time", F.when(before_cutoff, F.col("delivery_time")))
    masked = {r.order_id: r for r in steps.spark_baseline_features(masked_bronze).collect()}
    assert all(masked[order_id] == observed[order_id] for order_id in fit_ids)
    assert observed["test-long"].courier_completed_count == masked["test-long"].courier_completed_count + 1


def test_star_schema_and_fact_foreign_keys(local_spark):
    from lakehouse.common import attach_dimension_keys, build_dimensions

    fact = local_spark.createDataFrame([
        ("a", 10, 100, 2, 37.3, 121.4, datetime(2022, 6, 4, 15), datetime(2022, 6, 5, 1)),
        ("b", 20, 100, 2, 37.4, 121.5, datetime(2022, 6, 6, 8), datetime(2022, 6, 6, 9)),
    ], ["order_id", "region_id", "aoi_id", "aoi_type", "lat", "lng", "accept_ts", "delivery_ts"])
    dims = build_dimensions(fact)
    assert dims["dim_region"].count() == 2
    assert dims["dim_aoi"].count() == 1
    assert dims["dim_hour"].count() == 24
    days = dims["dim_date"].orderBy("date_key").collect()
    assert [(row.date_key, row.is_weekend) for row in days] == [
        (20220604, True), (20220605, True), (20220606, False),
    ]
    attached = attach_dimension_keys(fact, dims).orderBy("order_id").collect()
    assert len(attached) == 2
    assert attached[0].aoi_sk == attached[1].aoi_sk
    assert attached[0].region_sk != attached[1].region_sk
    assert (attached[0].accept_date_key, attached[0].delivery_date_key) == (20220604, 20220605)
    incomplete = {**dims, "dim_date": dims["dim_date"].filter("date_key <> 20220605")}
    with pytest.raises(ValueError, match="delivery_date_key"):
        attach_dimension_keys(fact, incomplete)
    conflict = fact.withColumn("aoi_type", fact.aoi_type + (fact.order_id == "a").cast("int"))
    with pytest.raises(ValueError, match="aoi_type"):
        build_dimensions(conflict)


def test_serving_aggregates_known_orders(local_spark, steps):
    fact = local_spark.createDataFrame([
        ("a", 1, 10, 1.0, 30.0, date(2022, 6, 4), datetime(2022, 6, 4, 15)),
        ("b", 1, 10, 2.0, 70.0, date(2022, 6, 4), datetime(2022, 6, 4, 16)),
    ], ["order_id", "courier_id", "region_id", "distance_km", "delivery_minutes", "order_date", "accept_ts"])
    pred = local_spark.createDataFrame([
        ("b", 1, 10, 2, 2.0, 16.0, 70.0, 60.0, 55.0, 50.0),
    ], ["order_id", "courier_id", "region_id", "aoi_type", "distance_km", "accept_hour", "delivery_minutes",
        "pred_gbt", "pred_lr", "pred_median"])
    serving = steps.serving_frames(fact, pred, 50)
    overview = serving["agg_overview"].first()
    assert (overview.total_orders, overview.avg_delivery_minutes, overview.late_rate) == (2, 50.0, 0.5)
    assert serving["agg_region_error"].first().mae_gbt == 10.0
    assert steps._metrics(pred, "delivery_minutes", "pred_gbt") == (1, 10.0, 10.0, None)
