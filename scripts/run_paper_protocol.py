"""Chạy Protocol P (bài toán ETA của bài báo LaDe) và in bảng so sánh.

    python scripts/run_paper_protocol.py --source data/delivery_yt.csv
"""
import argparse, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.eta import paper_protocol

PAPER = {"speed": (33.97, 40.27), "lightgbm": (23.32, 27.82), "M2G4RTP (best)": (17.21, 19.71)}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--output", type=Path, default=ROOT / "reports" / "eta_paper_metrics.csv")
    a = ap.parse_args()
    raw = pd.read_csv(a.source, dtype={"ds": str, "courier_id": str})
    metrics, stats, _ = paper_protocol.run(raw)
    print(stats)
    summary = metrics.groupby(["model", "metric"]).value.mean().unstack()
    print(summary.round(2))
    print("Bài báo (Yantai) MAE/RMSE:", PAPER)
    a.output.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(a.output, index=False)
