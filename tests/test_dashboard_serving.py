import pandas as pd
import pytest

from dashboard.serving import TABLES, load_snapshot


def run(status="COMPLETED", **overrides):
    return {"run_id": "fresh", "status": status, "owns_lock": False, **overrides}


@pytest.mark.parametrize(("row", "status"), [
    (None, "EMPTY"), (run("RUNNING", owns_lock=True), "RUNNING"),
    (run("RUNNING"), "INTERRUPTED"), (run("FAILED"), "FAILED"),
    (run("INTERRUPTED"), "INTERRUPTED"),
])
def test_unpublished_runs_do_not_read_tables(row, status):
    def unread(_name):
        pytest.fail("An unpublished run must never read serving tables")

    assert load_snapshot(lambda: row, unread) == (status, row, {})


def test_snapshot_requires_every_table_to_belong_to_one_completed_run():
    good = pd.DataFrame({"pipeline_run_id": ["fresh"], "value": [42]})
    status, _, tables = load_snapshot(lambda: run(), lambda _: good)
    assert status == "COMPLETED"
    assert set(tables) == set(TABLES)
    assert list(tables["agg_overview"]) == ["value"]
    for invalid in (good.iloc[0:0], good.drop(columns="pipeline_run_id"),
                    pd.concat([good, good.assign(pipeline_run_id="old")])):
        status, _, tables = load_snapshot(
            lambda: run(), lambda name: invalid if name == "eta_paper_metrics" else good,
        )
        assert (status, tables) == ("INCOMPLETE", {})


def test_snapshot_is_discarded_if_another_run_starts_while_reading():
    next_run = run("RUNNING", run_id="next", owns_lock=True)
    statuses = iter([run(), next_run])
    assert load_snapshot(lambda: next(statuses),
                         lambda _: pd.DataFrame({"pipeline_run_id": ["fresh"]})) == ("CHANGED", next_run, {})
