from datetime import date, datetime

import pytest


def test_bronze_contract_and_unique_keys(local_spark, tmp_path, write_lade_csv):
    from steps import read_bronze_csv, require_unique_orders

    path = write_lade_csv(tmp_path / "orders.csv", [{"order_id": "001"}, {"order_id": "002"}])
    raw = read_bronze_csv(local_spark, path.as_uri())
    require_unique_orders(raw)
    assert set(row.order_id for row in raw.collect()) == {"001", "002"}
    malformed = write_lade_csv(tmp_path / "wrong.csv", [{}], columns=["order_id"])
    with pytest.raises(ValueError, match="CSV LaDe-D"):
        read_bronze_csv(local_spark, malformed.as_uri())
    for invalid_id in ("001", "   "):
        bad = write_lade_csv(tmp_path / "ids.csv", [{"order_id": "001"}, {"order_id": invalid_id}])
        with pytest.raises(ValueError, match="order_id"):
            require_unique_orders(read_bronze_csv(local_spark, bad.as_uri()))


def test_silver_cohort_flags_and_rejected_reasons(local_spark, tmp_path, write_lade_csv):
    from steps import read_bronze_csv, reconcile_silver, split_silver

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
    raw = read_bronze_csv(local_spark, path.as_uri())
    silver, rejected = split_silver(raw)
    assert reconcile_silver(raw, silver, rejected) == {
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
        reconcile_silver(raw, damaged, rejected)
    with pytest.raises(ValueError, match="lý do"):
        reconcile_silver(raw, silver, rejected.withColumn("reject_reason", rejected.order_id))


def test_scd2_tie_boundaries_and_join_grain(local_spark):
    from lakehouse_common import courier_history, join_courier_history

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
    joined = join_courier_history(orders, history).orderBy("order_id").collect()
    assert [row.courier_region_id for row in joined] == [10, 10, 20, 10]
    with pytest.raises(ValueError, match="order_id"):
        join_courier_history(orders, history.unionByName(history))
    with pytest.raises(ValueError, match="exactly one"):
        join_courier_history(orders, history.filter("region_id = 10"))


def test_star_schema_and_fact_foreign_keys(local_spark):
    from lakehouse_common import attach_dimension_keys, build_dimensions

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


def test_serving_aggregates_known_orders(local_spark):
    from steps import _metrics, serving_frames

    fact = local_spark.createDataFrame([
        ("a", 1, 10, 1.0, 30.0, date(2022, 6, 4), datetime(2022, 6, 4, 15)),
        ("b", 1, 10, 2.0, 70.0, date(2022, 6, 4), datetime(2022, 6, 4, 16)),
    ], ["order_id", "courier_id", "region_id", "distance_km", "delivery_minutes", "order_date", "accept_ts"])
    pred = local_spark.createDataFrame([
        ("b", 1, 10, 2, 2.0, 16.0, 70.0, 60.0, 55.0, 50.0),
    ], ["order_id", "courier_id", "region_id", "aoi_type", "distance_km", "accept_hour", "delivery_minutes",
        "pred_gbt", "pred_lr", "pred_median"])
    serving = serving_frames(fact, pred, 50)
    overview = serving["agg_overview"].first()
    assert (overview.total_orders, overview.avg_delivery_minutes, overview.late_rate) == (2, 50.0, 0.5)
    assert serving["agg_region_error"].first().mae_gbt == 10.0
    assert _metrics(pred, "delivery_minutes", "pred_gbt") == (1, 10.0, 10.0, None)
