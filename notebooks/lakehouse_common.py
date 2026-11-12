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
    return (SparkSession.builder.appName(app_name)
            .config("spark.jars.packages", PACKAGES)
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
        & ~((la.abs() < 0.01) & (ln.abs() < 0.01)), F.lit(False))


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
