"""Đo phần MAE của Protocol P do leakage bằng đối chứng ghép cặp (cùng cặp truy vấn-đơn, cùng nhãn).

    python scripts/run_paper_protocol_pit.py --source data/delivery_yt.csv
"""
import argparse, sys, time
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.eta import paper_protocol as pp
from src.eta import paper_protocol_pit as pit
from src.eta.model import fit_lightgbm

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--output", type=Path, default=ROOT / "reports" / "eta_paper_pit_metrics.csv")
    ap.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44])
    ap.add_argument("--n-jobs", type=int, default=1)
    a = ap.parse_args()

    t0 = time.time()
    raw = pd.read_csv(a.source, dtype={"ds": str, "courier_id": str})
    df, stats, spd = pp.clean(raw)
    split = pp.split_dates(df.ds)
    print("clean:", stats, f"({time.time() - t0:.0f}s)", flush=True)

    S, _ = pit.build_samples_pit(df, spd, split)
    # Kiểm tra thiết kế ghép cặp: các cột gốc phải trùng đúng Protocol P đã lưu
    S0, _ = pp.build_samples(df, spd, split)
    pd.testing.assert_frame_equal(S[pit.BASE_COLUMNS].reset_index(drop=True), S0.reset_index(drop=True))
    print("pairs:", S.split.value_counts().to_dict(), "| cột gốc khớp Protocol P", f"({time.time() - t0:.0f}s)", flush=True)

    tr, va, te = (S[S["split"] == k] for k in ("train", "validation", "test"))
    y = "target_remaining_minutes"
    rows = []
    def add(model, seed, m):
        rows.extend((model, seed, k, float(v)) for k, v in m.items())

    add("median_train", 0, pp.metrics(te, pd.Series(tr[y].median(), index=te.index)))
    add("speed (spd cả file)", 0, pp.metrics(te, (te.dis / te.spd).clip(lower=0)))
    add("speed (spd_pit)", 0, pp.metrics(te, (te.dis / te.spd_pit).clip(lower=0)))
    for name, feats in pit.variants().items():
        for seed in a.seeds:
            m = fit_lightgbm(tr, va, feats, seed=seed, params={"n_jobs": a.n_jobs})
            pred = pd.Series(np.maximum(m.predict(te[feats]), 0), index=te.index)
            add(f"lightgbm | {name}", seed, pp.metrics(te, pred))
        print(name, "xong", f"({time.time() - t0:.0f}s)", flush=True)

    out = pd.DataFrame(rows, columns=["model", "seed", "metric", "value"])
    a.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.output, index=False)
    summary = out.groupby(["model", "metric"]).value.mean().unstack()[["mae", "rmse_pooled", "acc20", "acc30"]]
    print(summary.round(2).to_string())
