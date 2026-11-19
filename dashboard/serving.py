"""Select a complete, single-run MySQL snapshot for the dashboard."""

TABLES = (
    "agg_overview", "agg_region_daily", "agg_hourly", "agg_region_summary",
    "agg_region_error", "agg_courier_performance", "ml_metrics",
    "ml_feature_importance", "eta_prediction_sample", "eta_safe_metrics",
    "eta_paper_metrics", "pipeline_run_info",
)

LATEST_RUN_SQL = """
    SELECT run_id, prefect_run_id, status, stage, ended_at,
           CASE WHEN status = 'RUNNING'
                THEN owner_connection = IS_USED_LOCK('lakehouse.delivery-pipeline') ELSE 0 END AS owns_lock
    FROM lakehouse_pipeline_runs ORDER BY seq DESC LIMIT 1
"""


def snapshot_status(run):
    if run is None:
        return "EMPTY"
    if run["status"] == "RUNNING" and not run["owns_lock"]:
        return "INTERRUPTED"
    return run["status"]


def load_snapshot(read_run, read_table):
    run = read_run()
    status = snapshot_status(run)
    if status != "COMPLETED":
        return status, run, {}

    tables = {}
    for name in TABLES:
        df = read_table(name)
        if (df is None or df.empty or "pipeline_run_id" not in df
                or not df["pipeline_run_id"].eq(run["run_id"]).fillna(False).all()):
            break
        tables[name] = df.drop(columns="pipeline_run_id")

    latest = read_run()
    if latest != run:
        return "CHANGED", latest, {}
    if len(tables) != len(TABLES):
        return "INCOMPLETE", run, {}
    return status, run, tables
