
import pandas as pd

from dashboard import insights


def metrics():
    rows = []
    for model, vals in {"median_train": (28.5, 32.7, 36.6), "speed": (33.6, 40.0, 40.7), "lightgbm": (20.7, 24.9, 59.2)}.items():
        for seed in ([0] if model != "lightgbm" else [42, 43, 44]):
            rows += [(model, seed, "mae", vals[0]), (model, seed, "rmse_paper", vals[1]), (model, seed, "acc20", vals[2])]
    return pd.DataFrame(rows, columns=["model", "seed", "metric", "value"])


def test_paper_insights_use_numbers_from_data():
    text = " ".join(insights.paper_insights(metrics()))
    assert "20.7" in text and "33.6" in text and "SPEED" in text


def test_paper_insights_empty_is_safe():
    assert insights.paper_insights(pd.DataFrame(columns=["model", "seed", "metric", "value"])) == ["Chưa đủ metric để nhận xét."]


def test_spark_baseline_excludes_legacy_paper_reference():
    frame = pd.DataFrame({
        "model": ["Linear Regression", "GBT", "Legacy paper benchmark"],
        "cohort": ["test (<=24h, GPS hợp lệ)", "test (<=24h, GPS hợp lệ)", "theo bài báo"],
        "mae": [109.88, 92.29, 20.7],
    })
    assert insights.spark_baseline_metrics(frame).model.tolist() == ["Linear Regression", "GBT"]


def test_overview_and_courier_insights():
    o = pd.Series({"avg_delivery_minutes": 268.0, "median_delivery_minutes": 208.0})
    h = pd.DataFrame({"accept_hour": [8, 9], "total_orders": [10, 50], "median_delivery_minutes": [100., 300.]})
    r = pd.DataFrame({"region_id": [1, 2], "late_rate": [.1, .3]})
    assert len(insights.overview_insights(o, h, r)) == 3
    c = pd.DataFrame({"total_orders": [1000, 50, 10, 5]})
    assert any("dưới 100 đơn" in s for s in insights.courier_insights(c))
