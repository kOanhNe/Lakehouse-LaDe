"""Các bước xử lý Spark của pipeline (lấy từ notebook 01-05, gom lại để Prefect gọi)."""
import os
import sys
from datetime import datetime
import uuid

sys.path.insert(0, os.getenv("NOTEBOOK_DIR", "/home/jovyan/work"))

import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql.window import Window

from lakehouse_common import (
    courier_history, create_spark, haversine, join_courier_history,
    parse_timestamp, require_unique_orders, valid_coordinates,
)

BASE = "s3a://lakehouse/warehouse/"
BRONZE = BASE + "delivery_bronze"
SILVER = BASE + "delivery_silver"
REJECTED = BASE + "delivery_rejected"
DIM = BASE + "dim_courier_silver"
FACT = BASE + "fact_delivery_task_gold"
PRED = BASE + "eta_predictions_gold"
RUN_INFO = BASE + "pipeline_run_info"

NUM_FEATURES = ["distance_km", "accept_hour", "dow", "order_rank",
                "cum_avg_dist", "courier_train_avg_min"]
ETA_SAFE = BASE + "eta_safe_gold"
ETA_SAFE_METRICS = BASE + "eta_safe_metrics_gold"


def get_spark(name="Delivery - Prefect pipeline"):
    return create_spark(name)


def _exists(spark, path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)


def _jdbc():
    return {
        "url": os.getenv("MYSQL_JDBC_URL",
                         "jdbc:mysql://mysql:3306/delivery_source?allowPublicKeyRetrieval=true&useSSL=false"),
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": os.getenv("MYSQL_USER", "delivery"),
        "password": os.getenv("MYSQL_PASSWORD", "delivery"),
        "truncate": "true",
    }


def write_mysql(df, table):
    df.coalesce(1).write.format("jdbc").options(**_jdbc()).option("dbtable", table).mode("overwrite").save()
    return df.count()


# ---------------------------------------------------------------- 1. Bronze
def ingest_bronze(spark):
    src = os.getenv("SOURCE_PATH", "s3a://lakehouse/delivery_yt.csv")
    if not _exists(spark, src):
        src = os.getenv("LOCAL_SOURCE", "file:///home/jovyan/data/delivery_yt.csv")
    df = spark.read.csv(src, header=True, inferSchema=False, mode="FAILFAST")
    require_unique_orders(df)
    n = df.count()
    if n == 0:
        raise ValueError("CSV rỗng")
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(BRONZE))
    return {"source": src, "bronze_rows": n}


# ---------------------------------------------------------------- 2. Silver
def transform_silver(spark):
    df = spark.read.format("delta").load(BRONZE)
    require_unique_orders(df)
    d = (df.withColumn("accept_ts", parse_timestamp("accept_time"))
           .withColumn("delivery_ts", parse_timestamp("delivery_time")))
    for c in ["lng", "lat", "accept_gps_lng", "accept_gps_lat", "delivery_gps_lng", "delivery_gps_lat"]:
        d = d.withColumn(c, F.col(c).cast("double"))
    for c in ["region_id", "courier_id", "aoi_id", "aoi_type"]:
        d = d.withColumn(c, F.col(c).cast("long"))
    d = (d.withColumn("delivery_minutes",
                      (F.col("delivery_ts").cast("long") - F.col("accept_ts").cast("long")) / 60.0)
         .withColumn("day_gap", F.datediff(F.to_date("delivery_ts"), F.to_date("accept_ts")))
         .withColumn("distance_km", haversine("lat", "lng", "accept_gps_lat", "accept_gps_lng"))
         .withColumn("delivery_stop_distance_km",
                     haversine("lat", "lng", "delivery_gps_lat", "delivery_gps_lng")))
    d = (d.withColumn("q_invalid_timestamp", F.col("accept_ts").isNull() | F.col("delivery_ts").isNull())
         .withColumn("q_zero_duration", F.col("delivery_minutes") == 0)
         .withColumn("q_negative_duration", F.col("delivery_minutes") < 0)
         .withColumn("q_cross_day", F.col("day_gap") > 0)
         .withColumn("q_duration_gt24h", F.col("delivery_minutes") > 1440)
         .withColumn("q_accept_gps_invalid", ~valid_coordinates("accept_gps_lat", "accept_gps_lng"))
         .withColumn("q_delivery_gps_invalid", ~valid_coordinates("delivery_gps_lat", "delivery_gps_lng"))
         .withColumn("q_accept_distance_gt50km", F.col("distance_km") > 50)
         .withColumn("q_delivery_distance_gt50km", F.col("delivery_stop_distance_km") > 50)
         .withColumn("eligible_duration", F.col("accept_ts").isNotNull() & F.col("delivery_ts").isNotNull()
                     & (F.col("delivery_minutes") > 0))
         .withColumn("eligible_ml_distance", F.col("eligible_duration") & F.col("distance_km").isNotNull()
                     & ~F.col("q_accept_gps_invalid")))
    rejected = (d.filter(~F.col("eligible_duration"))
                .withColumn("reject_reason",
                            F.when(F.col("q_invalid_timestamp"), "invalid_timestamp")
                             .when(F.col("q_negative_duration") | F.col("q_zero_duration"), "non_positive_duration")
                             .otherwise("duration_not_eligible")))
    final = d.filter(F.col("eligible_duration"))
    before, after, rej = df.count(), final.count(), rejected.count()
    if after + rej != before:
        raise ValueError("Silver + rejected không bằng Bronze")
    final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(SILVER)
    rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(REJECTED)
    return {"bronze_rows": before, "silver_rows": after, "rejected_rows": rej}


# ------------------------------------------------------- 3. dim_courier SCD2
def build_dim_courier(spark):
    silver = spark.read.format("delta").load(SILVER)
    h = courier_history(silver)
    if h.filter("effective_date >= end_date").count():
        raise ValueError("SCD2: có dòng effective_date >= end_date")
    if h.groupBy("courier_id", "effective_date").count().filter("count > 1").count():
        raise ValueError("SCD2: trùng (courier_id, effective_date)")
    h.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DIM)
    return {"dim_rows": h.count(), "couriers": h.select("courier_id").distinct().count()}


# --------------------------------------------------------- 4. Fact + ML
def _metrics(df, label, pred_col):
    r = df.select(
        F.count("*").alias("n"),
        F.avg(F.abs(F.col(pred_col) - F.col(label))).alias("mae"),
        F.sqrt(F.avg((F.col(pred_col) - F.col(label)) ** 2)).alias("rmse"),
        F.sum((F.col(pred_col) - F.col(label)) ** 2).alias("sse"),
        F.sum((F.col(label) - F.lit(df.agg(F.avg(label)).first()[0])) ** 2).alias("sst"),
    ).first()
    return r["n"], float(r["mae"]), float(r["rmse"]), float(1 - r["sse"] / r["sst"])


def build_fact_and_train(spark, train_end="2022-10-01", late_threshold_min=None):
    silver = spark.read.format("delta").load(SILVER)
    dim = spark.read.format("delta").load(DIM)
    fact = join_courier_history(silver, dim)
    n_silver, n_fact = silver.count(), fact.count()
    if n_fact != n_silver:
        raise ValueError(f"Fact ({n_fact}) khác Silver ({n_silver})")
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(FACT)
    fact = spark.read.format("delta").load(FACT)

    # Cohort ML: GPS nhận đơn hợp lệ, <= 24h, khoảng cách <= 50 km
    ml = fact.filter(F.col("eligible_ml_distance") & (F.col("delivery_minutes") <= 1440)
                     & (F.col("distance_km") <= 50))
    w = (Window.partitionBy("courier_id", "order_date").orderBy("accept_ts", "order_id")
         .rowsBetween(Window.unboundedPreceding, Window.currentRow))
    ml = (ml.withColumn("accept_hour", F.hour("accept_ts") + F.minute("accept_ts") / 60)
            .withColumn("dow", F.dayofweek("order_date"))
            .withColumn("order_rank", F.count("*").over(w).cast("double"))
            .withColumn("cum_avg_dist", F.avg("distance_km").over(w))
            .withColumn("aoi_type_str", F.col("aoi_type").cast("string"))
            .withColumn("region_str", F.col("region_id").cast("string"))
            .withColumn("label_log", F.log1p("delivery_minutes")))

    train = ml.filter(F.col("order_date") < train_end)
    test = ml.filter(F.col("order_date") >= train_end)

    # Lịch sử hiệu suất shipper: chỉ tính từ tập train
    cstat = train.groupBy("courier_id").agg(F.avg("delivery_minutes").alias("courier_train_avg_min"))
    global_avg = train.agg(F.avg("delivery_minutes")).first()[0]
    if global_avg is None:
        raise ValueError("Tập train rỗng, kiểm tra train_end")
    train = train.join(cstat, "courier_id", "left").fillna({"courier_train_avg_min": global_avg})
    test = test.join(cstat, "courier_id", "left").fillna({"courier_train_avg_min": global_avg})
    train, test = train.cache(), test.cache()
    n_train, n_test = train.count(), test.count()
    if n_train < 2 or n_test < 2:
        raise ValueError("Train/test quá nhỏ, chỉnh train_end")

    idx = Pipeline(stages=[
        StringIndexer(inputCol="aoi_type_str", outputCol="aoi_idx", handleInvalid="keep"),
        StringIndexer(inputCol="region_str", outputCol="region_idx", handleInvalid="keep"),
    ]).fit(train)
    tr, te = idx.transform(train), idx.transform(test)
    ohe = OneHotEncoder(inputCols=["aoi_idx", "region_idx"], outputCols=["aoi_ohe", "region_ohe"],
                        handleInvalid="keep").fit(tr)
    tr, te = ohe.transform(tr), ohe.transform(te)
    a_lr = VectorAssembler(inputCols=NUM_FEATURES + ["aoi_ohe", "region_ohe"], outputCol="features_lr")
    a_gbt = VectorAssembler(inputCols=NUM_FEATURES + ["aoi_idx", "region_idx"], outputCol="features_gbt")
    tr = a_gbt.transform(a_lr.transform(tr))
    te = a_gbt.transform(a_lr.transform(te))

    lr = LinearRegression(featuresCol="features_lr", labelCol="label_log",
                          predictionCol="p_lr_log").fit(tr)
    gbt = GBTRegressor(featuresCol="features_gbt", labelCol="label_log", predictionCol="p_gbt_log",
                       maxIter=80, maxDepth=6, maxBins=256, seed=42).fit(tr)

    med = train.approxQuantile("delivery_minutes", [0.5], 0.01)[0]
    pred = (gbt.transform(lr.transform(te))
            .withColumn("pred_lr", F.greatest(F.expm1("p_lr_log"), F.lit(0.0)))
            .withColumn("pred_gbt", F.greatest(F.expm1("p_gbt_log"), F.lit(0.0)))
            .withColumn("pred_median", F.lit(float(med)))
            .withColumn("pred_mean", F.lit(float(global_avg)))
            .select("order_id", "courier_id", "region_id", "aoi_type", "order_date", "distance_km",
                    "accept_hour", "delivery_minutes", "pred_lr", "pred_gbt", "pred_median", "pred_mean")
            .cache())
    pred.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(PRED)

    run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for name, col in [("Baseline: trung vị", "pred_median"), ("Baseline: trung bình", "pred_mean"),
                      ("Linear Regression", "pred_lr"), ("GBT", "pred_gbt")]:
        n, mae, rmse, r2 = _metrics(pred, "delivery_minutes", col)
        rows.append((name, "test (<=24h, GPS hợp lệ)", n, round(mae, 2), round(rmse, 2), round(r2, 4), run_ts))
    b_mae, b_rmse = os.getenv("BENCHMARK_MAE"), os.getenv("BENCHMARK_RMSE")
    if b_mae and b_rmse:
        rows.append((os.getenv("BENCHMARK_NAME", "Benchmark bài báo gốc"), "theo bài báo",
                     None, float(b_mae), float(b_rmse), None, run_ts))
    metrics = spark.createDataFrame(
        rows, "model string, cohort string, n long, mae double, rmse double, r2 double, run_ts string")
    write_mysql(metrics, "ml_metrics")

    names = NUM_FEATURES + ["aoi_type", "region_id"]
    imp = spark.createDataFrame(
        [(n_, float(v)) for n_, v in zip(names, gbt.featureImportances.toArray())],
        "feature string, importance double")
    write_mysql(imp, "ml_feature_importance")

    if late_threshold_min is None:
        late_threshold_min = float(train.approxQuantile("delivery_minutes", [0.9], 0.01)[0])
    info = spark.createDataFrame(
        [(run_ts, str(train_end), n_silver, n_train, n_test, float(late_threshold_min))],
        "run_ts string, train_end string, silver_rows long, train_rows long, test_rows long, "
        "late_threshold_min double")
    info.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(RUN_INFO)
    write_mysql(info, "pipeline_run_info")
    out = {r[0]: {"mae": r[3], "rmse": r[4], "r2": r[5]} for r in rows}
    out.update({"train_rows": n_train, "test_rows": n_test, "late_threshold_min": late_threshold_min})
    return out


# --------------------------------------------------------- 4b. Leakage-safe ETA
def build_eta_safe(spark, seeds=(42, 43, 44)):
    """Run the registered pandas/LightGBM ETA protocol and publish Delta outputs.

    Bronze is converted once to pandas because this dataset is 206K rows; the
    report records that limitation. Spark/Delta remains responsible for storage
    and the rest of the Lakehouse ETL.
    """
    from src.eta.dataset import build_eta_samples, parse_lade_frame
    from src.eta.model import evaluate, fit_lightgbm

    raw = spark.read.format("delta").load(BRONZE).toPandas()
    events = parse_lade_frame(raw)
    samples = build_eta_samples(events, "B")
    features = ["pending_order_count", "completed_order_count", "courier_avg_min",
                "distance_to_last_delivery_km", "candidate_lat", "candidate_lng",
                "candidate_aoi_type", "candidate_region_id", "accept_hour"]
    train = samples[samples.split == "train"].copy()
    validation = samples[samples.split == "validation"].copy()
    test = samples[samples.split == "test"].copy()
    medians = train[features].median(numeric_only=True)
    for frame in [train, validation, test]:
        frame.loc[:, features] = frame[features].fillna(medians)
    models = {seed: fit_lightgbm(train, validation, features, seed=seed) for seed in seeds}
    primary_seed = seeds[0]
    model = models[primary_seed]
    scored = test[["order_id", "courier_id", "query_ts", "courier_day",
                   "target_remaining_minutes"]].copy()
    scored["prediction_minutes"] = evaluate(model, test, features)["predictions"]
    scored["run_id"] = uuid.uuid4().hex
    metrics = []
    for seed, fitted in models.items():
        for split, frame in [("validation", validation), ("test", test)]:
            result = evaluate(fitted, frame, features)
            metrics.append((scored.run_id.iloc[0], "B", split, result["n"], result["mae"], result["rmse"], seed, "bronze_to_pandas"))
    spark.createDataFrame(scored).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ETA_SAFE)
    spark.createDataFrame(metrics, "run_id string, protocol string, split string, n long, mae double, rmse double, seed int, execution_mode string") \
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(ETA_SAFE_METRICS)
    write_mysql(spark.createDataFrame(
        metrics,
        "run_id string, protocol string, split string, n long, mae double, rmse double, seed int, execution_mode string"),
        "eta_safe_metrics")
    return {"run_id": scored.run_id.iloc[0], "train_rows": len(train), "validation_rows": len(validation),
            "test_rows": len(test), "test_mae": next(row[4] for row in metrics if row[2] == "test" and row[6] == primary_seed),
            "test_rmse": next(row[5] for row in metrics if row[2] == "test" and row[6] == primary_seed), "seeds": list(seeds)}


# ------------------------------------------------------------ 5. Serving
def serve_to_mysql(spark):
    fact = spark.read.format("delta").load(FACT)
    pred = spark.read.format("delta").load(PRED)
    thr = spark.read.format("delta").load(RUN_INFO).first()["late_threshold_min"]
    late = (F.col("delivery_minutes") > F.lit(thr)).cast("double")

    reg = fact.groupBy("courier_id", "region_id").count()
    pick = Window.partitionBy("courier_id").orderBy(F.desc("count"), F.asc("region_id"))
    main = (reg.withColumn("rn", F.row_number().over(pick)).filter("rn = 1")
            .select("courier_id", F.col("region_id").alias("main_region_id")))

    cp = (pred.groupBy("courier_id").agg(
            F.round(F.avg("pred_gbt"), 2).alias("avg_pred_eta_minutes"),
            F.round(F.avg(F.abs(F.col("pred_gbt") - F.col("delivery_minutes"))), 2).alias("mae_gbt_test")))
    agg_courier = (fact.groupBy("courier_id").agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
            F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
            F.round(F.expr("percentile_approx(delivery_minutes, 0.5)"), 2).alias("median_delivery_minutes"),
            F.round(F.avg(late), 4).alias("late_rate"),
            F.round(F.max("delivery_minutes"), 2).alias("max_delivery_minutes"))
        .join(main, "courier_id", "left").join(cp, "courier_id", "left")
        .withColumn("late_threshold_min", F.lit(float(thr))))

    agg_region_daily = fact.groupBy("region_id", "order_date").agg(
        F.count("order_id").alias("total_orders"),
        F.countDistinct("courier_id").alias("active_couriers"),
        F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
        F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.5)"), 2).alias("median_delivery_minutes"),
        F.round(F.avg(late), 4).alias("late_rate"))

    agg_hourly = (fact.withColumn("accept_hour", F.hour("accept_ts")).groupBy("accept_hour").agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.5)"), 2).alias("median_delivery_minutes"),
        F.round(F.avg(late), 4).alias("late_rate")))

    over24 = (F.col("delivery_minutes") > 1440).cast("double")
    agg_region_summary = fact.groupBy("region_id").agg(
        F.count("order_id").alias("total_orders"),
        F.countDistinct("courier_id").alias("couriers"),
        F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.5)"), 2).alias("median_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.9)"), 2).alias("p90_delivery_minutes"),
        F.round(F.avg(late), 4).alias("late_rate"),
        F.round(F.avg(over24), 4).alias("over_24h_rate"))

    agg_overview = fact.agg(
        F.count("order_id").alias("total_orders"),
        F.countDistinct("courier_id").alias("couriers"),
        F.countDistinct("region_id").alias("regions"),
        F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.5)"), 2).alias("median_delivery_minutes"),
        F.round(F.expr("percentile_approx(delivery_minutes, 0.9)"), 2).alias("p90_delivery_minutes"),
        F.round(F.avg(late), 4).alias("late_rate"),
        F.round(F.avg(over24), 4).alias("over_24h_rate"))

    agg_region_error = pred.groupBy("region_id").agg(
        F.count("*").alias("n_test"),
        F.round(F.avg(F.abs(F.col("pred_median") - F.col("delivery_minutes"))), 2).alias("mae_baseline"),
        F.round(F.avg(F.abs(F.col("pred_lr") - F.col("delivery_minutes"))), 2).alias("mae_lr"),
        F.round(F.avg(F.abs(F.col("pred_gbt") - F.col("delivery_minutes"))), 2).alias("mae_gbt"))

    sample = (pred.orderBy(F.rand(42)).limit(3000).select(
        F.col("order_id").cast("string").alias("order_id"), "courier_id", "region_id", "aoi_type",
        F.round("distance_km", 2).alias("distance_km"), F.round("accept_hour", 2).alias("accept_hour"),
        F.round("delivery_minutes", 1).alias("actual_minutes"),
        F.round("pred_lr", 1).alias("pred_lr"), F.round("pred_gbt", 1).alias("pred_gbt")))

    out = {}
    for name, df in [("agg_courier_performance", agg_courier), ("agg_region_daily", agg_region_daily),
                     ("agg_hourly", agg_hourly), ("agg_region_error", agg_region_error),
                     ("agg_region_summary", agg_region_summary), ("agg_overview", agg_overview),
                     ("eta_prediction_sample", sample)]:
        out[name] = write_mysql(df, name)
        if out[name] == 0:
            raise ValueError(f"Bảng {name} rỗng")
    return out
