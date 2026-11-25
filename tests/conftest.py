import pytest


@pytest.fixture(scope="session")
def local_spark():
    pytest.importorskip("pyspark", reason="Spark unit tests run in the Jupyter container")
    from pyspark.sql import SparkSession

    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
        spark = (SparkSession.builder.master("local[2]").appName("lakehouse-unit")
                 .config("spark.sql.shuffle.partitions", "2")
                 .config("spark.sql.session.timeZone", "Asia/Shanghai")
                 .config("spark.sql.ansi.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        try:
            yield spark
        finally:
            spark.stop()


@pytest.fixture
def write_lade_csv():
    import csv
    from eta.dataset import SOURCE_COLUMNS

    def write(path, overrides, columns=SOURCE_COLUMNS):
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in overrides:
                writer.writerow({
                    "order_id": "order", "courier_id": 1, "region_id": 10, "city": "Yantai",
                    "lat": 37.3, "lng": 121.4, "aoi_id": 100, "aoi_type": 2,
                    "accept_time": "06-04 08:00:00", "delivery_time": "06-04 09:00:00",
                    "accept_gps_lat": 37.31, "accept_gps_lng": 121.41,
                    "delivery_gps_lat": 37.3, "delivery_gps_lng": 121.4,
                    "accept_gps_time": "06-04 08:00:00", "delivery_gps_time": "06-04 09:00:00",
                    "ds": "0604", **row,
                })
        return path

    return write
