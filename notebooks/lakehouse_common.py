"""Shared Spark runtime for the local delivery lakehouse notebooks."""
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "io.delta:delta-spark_2.12:3.3.2,"
    "com.mysql:mysql-connector-j:8.0.33"
)


def create_spark(app_name):
    local = os.getenv("APP_ENV", "local").lower() in ("local", "dev")
    defaults = {
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
    }
    settings = {k: os.getenv(k, v if local else "") for k, v in defaults.items()}
    if not all(settings.values()):
        raise ValueError("Set MINIO_ENDPOINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY")
    builder = SparkSession.builder.appName(app_name)
    # In Docker, packages are supplied once through PYSPARK_SUBMIT_ARGS.  When
    # running outside Docker, keep the builder fallback for a normal checkout.
    if not os.getenv("PYSPARK_SUBMIT_ARGS", "").strip():
        builder = builder.config("spark.jars.packages", PACKAGES)
    return (builder
            .config("spark.jars.ivy", os.getenv("SPARK_IVY_DIR", "/tmp/lakehouse-ivy"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3a.endpoint", settings["MINIO_ENDPOINT"])
            .config("spark.hadoop.fs.s3a.access.key", settings["MINIO_ACCESS_KEY"])
            .config("spark.hadoop.fs.s3a.secret.key", settings["MINIO_SECRET_KEY"])
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(settings["MINIO_ENDPOINT"].startswith("https:")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.session.timeZone", os.getenv("DATA_TIMEZONE", "Asia/Shanghai"))
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
            .getOrCreate())


def parse_timestamp(column):
    year = os.getenv("DATA_YEAR", "2022")
    return F.to_timestamp(F.concat(F.lit(year + "-"), F.col(column)), "yyyy-MM-dd HH:mm:ss")


def valid_coordinates(lat, lng):
    la, ln = F.col(lat).cast("double"), F.col(lng).cast("double")
    return F.coalesce(
        la.isNotNull() & ln.isNotNull() & ~F.isnan(la) & ~F.isnan(ln)
        & la.between(-90, 90) & ln.between(-180, 180)
        & ~((la == 0) & (ln == 0)), F.lit(False))


def haversine(lat1, lng1, lat2, lng2):
    la1, ln1, la2, ln2 = [F.col(c).cast("double") for c in (lat1, lng1, lat2, lng2)]
    a = (F.sin(F.radians(la2 - la1) / 2) ** 2
         + F.cos(F.radians(la1)) * F.cos(F.radians(la2))
         * F.sin(F.radians(ln2 - ln1) / 2) ** 2)
    # Guard validity before clamping: Spark least/greatest skip NULL operands.
    return F.when(valid_coordinates(lat1, lng1) & valid_coordinates(lat2, lng2),
                  2 * 6371.0 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), a)))))


def require_unique_orders(df):
    if df.filter(F.col("order_id").isNull() | (F.trim(F.col("order_id").cast("string")) == "")).limit(1).count():
        raise ValueError("Missing order_id")
    if df.groupBy("order_id").count().filter("count > 1").limit(1).count():
        raise ValueError("Duplicate order_id; resolve source conflicts before writing")


def courier_history(deliveries):
    """Daily dominant region; ties resolve by smallest region_id. Batch SCD2."""
    daily = (deliveries.withColumn("event_date", F.to_date("accept_ts"))
             .groupBy("courier_id", "event_date", "region_id").count())
    pick = Window.partitionBy("courier_id", "event_date").orderBy(F.desc("count"), F.asc("region_id"))
    daily = daily.withColumn("rank", F.row_number().over(pick)).filter("rank = 1")
    window = Window.partitionBy("courier_id").orderBy("event_date")
    changes = (daily.withColumn("previous_region", F.lag("region_id").over(window))
               .filter(F.col("previous_region").isNull() | ~F.col("previous_region").eqNullSafe(F.col("region_id"))))
    return (changes.withColumn("effective_date", F.col("event_date"))
            .withColumn("next_date", F.lead("event_date").over(window))
            .withColumn("end_date", F.coalesce("next_date", F.lit("9999-12-31").cast("date")))
            .withColumn("is_active", F.col("next_date").isNull())
            .withColumn("courier_sk", F.xxhash64("courier_id", "effective_date"))
            .select("courier_sk", "courier_id", "region_id", "effective_date", "end_date", "is_active"))


def join_courier_history(deliveries, history):
    require_unique_orders(deliveries)
    orders = deliveries.withColumn("order_date", F.to_date("accept_ts"))
    joined = (orders.alias("d").join(history.alias("c"),
              (F.col("d.courier_id") == F.col("c.courier_id"))
              & (F.col("d.order_date") >= F.col("c.effective_date"))
              & (F.col("d.order_date") < F.col("c.end_date")), "left")
              .select("d.*", "c.courier_sk", F.col("c.region_id").alias("courier_region_id")))
    require_unique_orders(joined)
    if joined.filter("courier_sk is null").limit(1).count() or joined.count() != orders.count():
        raise ValueError("Courier SCD2 join must match exactly one history row per order")
    return joined


# ------------------------------------------------------------ star schema
def build_dimensions(silver):
    """Các bảng dimension của star schema, dựng từ Silver.

    Không đặt tên/thuộc tính nào mà dữ liệu không có: thuộc tính của region và AOI
    là thuộc tính *suy ra* từ tọa độ các đơn (tâm, hộp bao). Lưu ý đã kiểm tra trên
    dữ liệu: mỗi aoi_id chỉ có một aoi_type, nhưng một số AOI xuất hiện ở nhiều
    region, nên region KHÔNG là thuộc tính của AOI mà là khóa ngoại riêng của fact.
    """
    ok = silver.filter(valid_coordinates("lat", "lng"))

    region = (ok.groupBy("region_id").agg(
        F.avg("lat").alias("centroid_lat"), F.avg("lng").alias("centroid_lng"),
        F.min("lat").alias("min_lat"), F.max("lat").alias("max_lat"),
        F.min("lng").alias("min_lng"), F.max("lng").alias("max_lng"),
        F.countDistinct("aoi_id").alias("distinct_aoi_observed")))
    all_regions = silver.select("region_id").distinct()
    region = (all_regions.join(region, "region_id", "left")
              .withColumn("region_sk", F.row_number().over(Window.orderBy("region_id")).cast("long")))
    region = region.select("region_sk", "region_id", "centroid_lat", "centroid_lng", "min_lat", "max_lat",
                           "min_lng", "max_lng", "distinct_aoi_observed")

    types = silver.groupBy("aoi_id").agg(F.countDistinct("aoi_type").alias("n_types"),
                                         F.min("aoi_type").alias("aoi_type"))
    if types.filter("n_types > 1").limit(1).count():
        raise ValueError("Có aoi_id mang nhiều aoi_type: dim_aoi không còn đúng một dòng cho mỗi AOI")
    centroid = ok.groupBy("aoi_id").agg(F.avg("lat").alias("centroid_lat"), F.avg("lng").alias("centroid_lng"))
    aoi = (types.drop("n_types").join(centroid, "aoi_id", "left")
           .withColumn("aoi_sk", F.row_number().over(Window.orderBy("aoi_id")).cast("long"))
           .select("aoi_sk", "aoi_id", "aoi_type", "centroid_lat", "centroid_lng"))

    bounds = silver.agg(F.min(F.least(F.to_date("accept_ts"), F.to_date("delivery_ts"))).alias("lo"),
                        F.max(F.greatest(F.to_date("accept_ts"), F.to_date("delivery_ts"))).alias("hi")).first()
    days = (silver.sparkSession.range(1).select(
        F.explode(F.sequence(F.lit(bounds.lo), F.lit(bounds.hi))).alias("full_date")))
    iso_dow = ((F.dayofweek("full_date") + 5) % 7 + 1)
    date = (days.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("full_date")).withColumn("month", F.month("full_date"))
            .withColumn("day", F.dayofmonth("full_date")).withColumn("week_of_year", F.weekofyear("full_date"))
            .withColumn("day_of_week", iso_dow)
            .withColumn("day_name", F.date_format("full_date", "EEEE"))
            .withColumn("is_weekend", iso_dow >= 6)
            .select("date_key", "full_date", "year", "month", "day", "week_of_year",
                    "day_of_week", "day_name", "is_weekend"))

    h = silver.sparkSession.range(24).select(F.col("id").cast("int").alias("hour_key"))
    hour = h.withColumn("part_of_day",
                        F.when(F.col("hour_key") < 6, "Đêm").when(F.col("hour_key") < 11, "Sáng")
                         .when(F.col("hour_key") < 14, "Trưa").when(F.col("hour_key") < 18, "Chiều")
                         .when(F.col("hour_key") < 22, "Tối").otherwise("Đêm"))
    return {"dim_region": region, "dim_aoi": aoi, "dim_date": date, "dim_hour": hour}


def _require_all_matched(fact, key, dim, dim_key, name):
    missing = fact.join(dim.select(F.col(dim_key).alias("_k")), fact[key] == F.col("_k"), "left_anti").count()
    if missing:
        raise ValueError(f"{missing} dòng fact không có khóa {key} trong {name}")


def attach_dimension_keys(fact, dims):
    """Gắn khóa dimension vào fact và chứng minh grain không đổi, mọi khóa ngoại đều khớp."""
    before = fact.count()
    out = (fact
           .join(dims["dim_region"].select("region_id", "region_sk"), "region_id", "left")
           .join(dims["dim_aoi"].select("aoi_id", "aoi_sk"), "aoi_id", "left")
           .withColumn("accept_date_key", F.date_format("accept_ts", "yyyyMMdd").cast("int"))
           .withColumn("delivery_date_key", F.date_format("delivery_ts", "yyyyMMdd").cast("int"))
           .withColumn("accept_hour_key", F.hour("accept_ts")))
    if out.count() != before:
        raise ValueError("Gắn khóa dimension làm đổi số dòng fact (khóa dimension không duy nhất)")
    for col, dim, dkey in [("region_sk", "dim_region", "region_sk"), ("aoi_sk", "dim_aoi", "aoi_sk"),
                           ("accept_date_key", "dim_date", "date_key"), ("delivery_date_key", "dim_date", "date_key"),
                           ("accept_hour_key", "dim_hour", "hour_key")]:
        if out.filter(F.col(col).isNull()).limit(1).count():
            raise ValueError(f"Khóa {col} bị rỗng")
        _require_all_matched(out, col, dims[dim], dkey, dim)
    return out
