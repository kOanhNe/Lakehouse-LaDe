"""Độ nhạy của kết quả Protocol P theo ngưỡng nhãn (bài báo dùng 120 phút).

Mục đích: cho thấy bộ lọc loại bao nhiêu cặp và sai số thay đổi thế nào khi nới/siết ngưỡng.
    python scripts/run_label_sensitivity.py
"""
import argparse
from pathlib import Path
import pandas as pd

from lakehouse import PROJECT_ROOT as ROOT
from eta import paper_protocol as pp

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--output", type=Path, default=ROOT / "reports" / "eta" / "eta_paper_label_sensitivity.csv")
    a = ap.parse_args()
    raw = pd.read_csv(a.source, dtype={"ds": str, "courier_id": str})
    rows = []
    for cap in (60, 120, 240, 480):
        m, st, _ = pp.run(raw, seeds=(42,), label_max=cap)
        w = m.groupby(["model", "metric"]).value.mean().unstack()
        rows.append({"label_max_min": cap, "pairs_kept_pct": round(100 * (1 - st["pairs_removed_by_label_filter"]), 1),
                     "test_pairs": st["pairs"]["test"],
                     "median_mae": round(w.loc["median_train", "mae"], 2),
                     "speed_mae": round(w.loc["speed", "mae"], 2),
                     "lightgbm_mae": round(w.loc["lightgbm", "mae"], 2),
                     "lightgbm_acc20": round(w.loc["lightgbm", "acc20"], 1)})
        print(rows[-1], flush=True)
    out = pd.DataFrame(rows)
    a.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.output, index=False)
    print(out.to_string(index=False))
