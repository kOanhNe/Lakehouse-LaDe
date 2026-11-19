from datetime import date, timedelta
import os
from urllib.request import urlopen
from uuid import uuid4

import pytest


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def integration_spark():
    from steps import get_spark

    api = os.environ["PREFECT_API_URL"]
    with urlopen(f"{api.rstrip('/')}/health", timeout=10) as response:
        assert response.status == 200
    spark = get_spark("lakehouse-integration")
    spark.sparkContext.setLogLevel("ERROR")
    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture
def isolated_warehouse(integration_spark, monkeypatch):
    import run_state
    import steps

    spark = integration_spark
    prefix = f"ct_{uuid4().hex[:12]}"
    uri = f"s3a://lakehouse/integration/{prefix}/"
    table_names = set()

    for name in ("BRONZE", "SILVER", "REJECTED", "DIM", "FACT", "PRED", "RUN_INFO",
                 "ETA_SAFE", "ETA_SAFE_METRICS", "ETA_PAPER", "ETA_PAPER_METRICS"):
        monkeypatch.setattr(steps, name, uri + getattr(steps, name).split("/")[-1])
    monkeypatch.setattr(steps, "DIMS", {name: uri + path.split("/")[-1] for name, path in steps.DIMS.items()})
    monkeypatch.setattr(run_state, "LOCK", f"lakehouse.integration.{prefix}")
    monkeypatch.setattr(run_state, "TABLE", f"{prefix}_runs")
    table_names.add(run_state.TABLE)
    write_mysql = steps.write_mysql

    def write(frame, table, *, run_id=None):
        name = f"{prefix}_{table}"
        table_names.add(name)
        return write_mysql(frame, name, run_id=run_id)

    def read(table):
        return spark.read.format("jdbc").options(**steps._jdbc()).option("dbtable", f"{prefix}_{table}").load()

    def latest_run():
        query = (
            f"(SELECT run_id, status, stage, owner_connection = IS_USED_LOCK('{run_state.LOCK}') AS owns_lock "
            f"FROM {run_state.TABLE} ORDER BY seq DESC LIMIT 1) latest"
        )
        row = spark.read.format("jdbc").options(**steps._jdbc()).option("dbtable", query).load().first()
        return row.asDict() if row else None

    monkeypatch.setattr(steps, "write_mysql", write)
    monkeypatch.setattr(steps, "get_spark", lambda: spark)
    try:
        yield spark, read, latest_run
    finally:
        connection = run_state.connect(spark, steps._jdbc())
        try:
            for name in table_names:
                run_state.execute(connection, f"DROP TABLE IF EXISTS {name}")
        finally:
            connection.close()
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(uri)
        path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()).delete(path, True)


def delivery_rows():
    rows = []
    for n in range(16):
        day = date(2022, 9, 23) + timedelta(days=n)
        day_prefix = day.strftime("%m-%d")
        region = 10 if n < 4 or n >= 10 else 20
        for i in range(6):
            rows.append({
                "order_id": f"{n:02}-{i}",
                "region_id": region,
                "aoi_id": 100 + i % 2,
                "aoi_type": 1 + i % 2,
                "lat": 37.30 + i * 0.002,
                "accept_time": f"{day_prefix} 08:{i:02}:00",
                "delivery_time": f"{day_prefix} 09:{i * 8 + n % 3:02}:00",
                "ds": day.strftime("%m%d"),
            })
    rows.append({**rows[0], "order_id": "zero", "delivery_time": rows[0]["accept_time"]})
    return rows


def test_prefect_reruns_after_partial_serving_failure(isolated_warehouse, tmp_path, write_lade_csv, monkeypatch):
    import flow
    import steps
    from dashboard.serving import TABLES, load_snapshot

    spark, read, latest_run = isolated_warehouse
    rows = delivery_rows()
    path = write_lade_csv(tmp_path / "orders.csv", rows)
    monkeypatch.setenv("SOURCE_PATH", path.as_uri())
    monkeypatch.setattr(flow, "t_serve", flow.t_serve.with_options(retries=0))

    def validate(result):
        bronze, silver, rejected, history, fact = [
            spark.read.format("delta").load(path)
            for path in (steps.BRONZE, steps.SILVER, steps.REJECTED, steps.DIM, steps.FACT)
        ]
        assert {r.order_id for r in bronze.select("order_id").collect()} == {r["order_id"] for r in rows}
        assert rejected.select("order_id", "reject_reason").first() == ("zero", "non_positive_duration")
        assert silver.count() == fact.count() == len(rows) - 1
        assert fact.select("order_id").distinct().count() == fact.count()
        assert [r.region_id for r in history.orderBy("effective_date").collect()] == [10, 20, 10]
        assert spark.read.format("delta").load(steps.DIMS["dim_aoi"]).count() == 2
        assert fact.filter("courier_sk IS NULL OR region_sk IS NULL OR aoi_sk IS NULL").count() == 0
        state = latest_run()
        assert (state["status"], state["run_id"]) == ("COMPLETED", result["run_id"])
        status, run, tables = load_snapshot(latest_run, lambda table: read(table).toPandas())
        assert status == "COMPLETED" and run == state
        assert set(tables) == set(TABLES)
        assert tables["agg_overview"].total_orders.iloc[0] == fact.count()
        assert set(tables["eta_paper_metrics"].run_id) == {result["run_id"]}
        assert set(tables["eta_safe_metrics"].run_id) == {result["run_id"]}
        return [r.asDict() for r in fact.select("order_id", "courier_sk", "region_sk", "aoi_sk").orderBy("order_id").collect()]

    first = flow.delivery_pipeline()
    keys = validate(first)
    write = steps.write_mysql
    with monkeypatch.context() as faults:
        def break_serving(frame, table, *, run_id=None):
            if table == "agg_region_daily":
                raise RuntimeError("injected failure after one serving table was written")
            return write(frame, table, run_id=run_id)

        faults.setattr(steps, "write_mysql", break_serving)
        with pytest.raises(RuntimeError, match="injected failure"):
            flow.delivery_pipeline()
    failed = latest_run()
    assert (failed["status"], failed["stage"]) == ("FAILED", "serving")
    assert failed["run_id"] != first["run_id"]
    assert read("agg_courier_performance").first().pipeline_run_id == failed["run_id"]
    assert read("agg_region_daily").first().pipeline_run_id == first["run_id"]
    assert load_snapshot(latest_run, lambda _: pytest.fail("Failed run exposed a table")) == ("FAILED", failed, {})

    recovered = flow.delivery_pipeline()
    assert recovered["run_id"] not in {first["run_id"], failed["run_id"]}
    assert validate(recovered) == keys


def test_mysql_lock_loss_is_visible_and_next_attempt_recovers(isolated_warehouse):
    from dashboard.serving import snapshot_status
    import run_state
    import steps

    spark, _read, latest_run = isolated_warehouse
    jdbc = steps._jdbc()
    with pytest.raises(Exception, match="closed"):
        with run_state.track_run(spark, jdbc, str(uuid4())) as active:
            running = latest_run()
            assert snapshot_status(running) == "RUNNING"
            with pytest.raises(RuntimeError, match="already owns"):
                with run_state.track_run(spark, jdbc, str(uuid4())):
                    pytest.fail("Concurrent run acquired the lock")
            active.connection.close()
    assert snapshot_status(latest_run()) == "INTERRUPTED"
    with run_state.track_run(spark, jdbc, str(uuid4())) as new:
        previous_status = run_state.scalar(
            new.connection, f"SELECT status FROM {run_state.TABLE} WHERE run_id = ?", running["run_id"],
        )
        assert previous_status == "INTERRUPTED"
    assert latest_run()["status"] == "COMPLETED"
