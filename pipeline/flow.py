"""Pipeline end-to-end bằng Prefect: Bronze -> Silver -> SCD2 -> Gold + ML -> MySQL.

Chạy một lần:      python -m pipeline.flow
Chạy theo lịch:    python -m pipeline.flow --serve
"""
import argparse

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from pipeline.run_state import track_run
from lakehouse.notebooks import load_notebook_steps

steps = load_notebook_steps()


@task(name="1. Bronze: nạp CSV", retries=1, retry_delay_seconds=30)
def t_bronze():
    out = steps.ingest_bronze(steps.get_spark())
    get_run_logger().info(out)
    return out


@task(name="2. Silver: làm sạch + cột phái sinh", retries=1, retry_delay_seconds=30)
def t_silver(_upstream):
    out = steps.transform_silver(steps.get_spark())
    get_run_logger().info(out)
    return out


@task(name="3. SCD2: dim_courier", retries=1, retry_delay_seconds=30)
def t_dim(_upstream):
    out = steps.build_dim_courier(steps.get_spark())
    get_run_logger().info(out)
    return out


@task(name="3b. Star: dim_region, dim_aoi, dim_date, dim_hour", retries=1, retry_delay_seconds=30)
def t_star_dims(_upstream):
    out = steps.build_star_dimensions(steps.get_spark())
    get_run_logger().info(out)
    return out


@task(name="4. Gold: fact + huấn luyện ETA", retries=1, retry_delay_seconds=30)
def t_gold(_upstream, train_end, late_threshold_min, run_id):
    out = steps.build_fact_and_train(steps.get_spark(), train_end, late_threshold_min, run_id=run_id)
    get_run_logger().info(out)
    return out


@task(name="4b. Gold: ETA tại lúc nhận đơn", retries=1, retry_delay_seconds=30)
def t_eta_safe(_upstream, run_id):
    out = steps.build_eta_safe(steps.get_spark(), run_id=run_id)
    get_run_logger().info(out)
    return out


@task(name="4c. Gold: ETA theo bài báo LaDe (Protocol P)", retries=1, retry_delay_seconds=30)
def t_eta_paper(_upstream, run_id):
    out = steps.build_eta_paper(steps.get_spark(), run_id=run_id)
    get_run_logger().info(out)
    return out


@task(name="5. Serving: đẩy bảng sang MySQL", retries=1, retry_delay_seconds=30)
def t_serve(_upstream, run_id):
    out = steps.serve_to_mysql(steps.get_spark(), run_id=run_id)
    get_run_logger().info(out)
    return out


@flow(name="delivery-lakehouse-pipeline", log_prints=True)
def delivery_pipeline(train_end: str = "2022-10-01", late_threshold_min: float | None = None):
    with track_run(steps.get_spark(), steps._jdbc(), flow_run.id) as run:
        get_run_logger().info("Pipeline run_id=%s", run.run_id)
        run.stage("bronze")
        bronze = t_bronze()
        run.stage("silver")
        silver = t_silver(bronze)
        run.stage("courier_scd2")
        dim = t_dim(silver)
        run.stage("star_dimensions")
        star = t_star_dims(dim)
        run.stage("eta_safe")
        eta_safe = t_eta_safe(dim, run.run_id)
        run.stage("eta_paper")
        eta_paper = t_eta_paper(dim, run.run_id)
        run.stage("fact_and_baseline")
        gold = t_gold(star, train_end, late_threshold_min, run.run_id)
        run.stage("serving")
        served = t_serve(gold, run.run_id)
    return {"run_id": run.run_id, "bronze": bronze, "silver": silver, "dim": dim,
            "star_dims": star, "eta_safe": eta_safe, "eta_paper": eta_paper, "gold": gold, "served": served}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="chạy nền theo lịch (mặc định 02:00 mỗi ngày)")
    ap.add_argument("--cron", default="0 2 * * *")
    ap.add_argument("--train-end", default="2022-10-01")
    ap.add_argument("--late-threshold", type=float, default=None,
                    help="ngưỡng 'trễ' tính bằng phút; bỏ trống = p90 của tập train")
    a = ap.parse_args()
    if a.serve:
        delivery_pipeline.serve(name="delivery-scheduled", cron=a.cron,
                                parameters={"train_end": a.train_end,
                                            "late_threshold_min": a.late_threshold})
    else:
        delivery_pipeline(train_end=a.train_end, late_threshold_min=a.late_threshold)
