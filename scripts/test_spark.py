"""Spark regression checks: local data only, no MinIO/MySQL required."""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from lakehouse_common import (attach_dimension_keys, build_dimensions, courier_history,
                              haversine, join_courier_history)

spark = (SparkSession.builder.master('local[2]').appName('lakehouse-regression')
         .config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    # One courier serves two regions on a day; deterministic tie, repeat, switch, return.
    rows = [
        ('a', 1, 20, datetime(2022, 6, 1, 8)),
        ('b', 1, 10, datetime(2022, 6, 1, 9)),
        ('c', 1, 10, datetime(2022, 6, 2, 8)),
        ('d', 1, 20, datetime(2022, 6, 3, 8)),
        ('e', 1, 10, datetime(2022, 6, 4, 8)),
    ]
    orders = spark.createDataFrame(rows, ['order_id', 'courier_id', 'region_id', 'accept_ts'])
    history = courier_history(orders)
    actual = history.orderBy('effective_date').collect()
    assert [r.region_id for r in actual] == [10, 20, 10], actual
    assert sum(r.is_active for r in actual) == 1
    assert all(r.effective_date < r.end_date for r in actual)
    fact = join_courier_history(orders, history)
    assert fact.count() == 5
    assert fact.filter("order_id = 'a'").first().region_id == 20
    assert fact.filter("order_id = 'a'").first().courier_region_id == 10
    # Broken history must fail loudly instead of silently deduplicating fact rows.
    try:
        join_courier_history(orders, history.unionByName(history))
    except ValueError:
        pass
    else:
        raise AssertionError('Overlapping history was accepted')
    try:
        join_courier_history(orders, history.filter('region_id = 20'))
    except ValueError:
        pass
    else:
        raise AssertionError('Missing history was accepted')
    coordinates = spark.createDataFrame([
        (37.0, 121.0, 37.0, 121.0),
        (37.0, 121.0, None, None),
        (37.0, 121.0, 0.0, 0.0),
        (37.0, 121.0, 200.0, 121.0),
        (37.0, 121.0, float('nan'), 121.0),
    ], ['la1', 'ln1', 'la2', 'ln2'])
    distances = coordinates.select(haversine('la1', 'ln1', 'la2', 'ln2').alias('km')).collect()
    assert distances[0].km == 0
    assert all(row.km is None for row in distances[1:]), distances
    # Star schema: dimension keys, grain preserved, AOI in two regions handled.
    star = spark.createDataFrame([
        ('a', 1, 10, 100, 2, 37.30, 121.40, datetime(2022, 6, 4, 8, 30), datetime(2022, 6, 4, 23, 59)),
        ('b', 1, 10, 100, 2, 37.32, 121.42, datetime(2022, 6, 4, 9, 0), datetime(2022, 6, 5, 0, 10)),
        ('c', 2, 20, 100, 2, 37.40, 121.50, datetime(2022, 6, 6, 7, 0), datetime(2022, 6, 6, 8, 0)),
    ], ['order_id', 'courier_id', 'region_id', 'aoi_id', 'aoi_type', 'lat', 'lng', 'accept_ts', 'delivery_ts'])
    dims = build_dimensions(star)
    assert dims['dim_region'].count() == 2 and dims['dim_aoi'].count() == 1
    assert dims['dim_date'].count() == 3 and dims['dim_hour'].count() == 24   # 4,5,6/6 va 24 gio
    sat = dims['dim_date'].filter('full_date = DATE "2022-06-04"').first()
    assert sat.is_weekend and sat.day_of_week == 6, sat
    keyed = attach_dimension_keys(star, dims)
    assert keyed.count() == 3 and keyed.filter('delivery_date_key <> accept_date_key').count() == 1
    bad = dims.copy(); bad['dim_date'] = dims['dim_date'].filter('date_key <> 20220605')
    try:
        attach_dimension_keys(star, bad)
    except ValueError:
        pass
    else:
        raise AssertionError('Missing date dimension row was accepted')
    conflict = star.withColumn('aoi_type', F.when(F.col('order_id') == 'a', 3).otherwise(2))
    try:
        build_dimensions(conflict)
    except ValueError:
        pass
    else:
        raise AssertionError('AOI with two types was accepted')
    print('PASS: star dimensions, FK integrity, AOI/type uniqueness')
    print('PASS: SCD2 changes/ties, fact grain, order region, missing/overlapping history, GPS null/NaN/range')
finally:
    spark.stop()
