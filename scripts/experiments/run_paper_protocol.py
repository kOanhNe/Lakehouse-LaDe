"""Chạy tái lập Protocol P hoặc một trong hai phép đo leakage."""
import argparse
from pathlib import Path
import time

import numpy as np
import pandas as pd

from eta import paper_protocol as pp
from eta import paper_protocol_pit as pit
from eta.model import fit_lightgbm
from lakehouse import PROJECT_ROOT as ROOT

PAPER = {"speed": (33.97, 40.27), "lightgbm": (23.32, 27.82), "M2G4RTP (best)": (17.21, 19.71)}

NO_CAP = 10 ** 6
REPORTS = {
    "paper": "eta_paper_metrics.csv",
    "pit": "eta_paper_pit_metrics.csv",
    "cohort": "eta_paper_cohort_metrics.csv",
}


def run_paper(raw: pd.DataFrame) -> pd.DataFrame:
    metrics, stats, _ = pp.run(raw)
    print(stats)
    summary = metrics.groupby(["model", "metric"]).value.mean().unstack()
    print(summary.round(2))
    print("Bài báo (Yantai) MAE/RMSE:", PAPER)
    return metrics


def run_pit(raw: pd.DataFrame, seeds: list[int], n_jobs: int) -> pd.DataFrame:
    t0 = time.time()
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
        for seed in seeds:
            m = fit_lightgbm(tr, va, feats, seed=seed, params={"n_jobs": n_jobs})
            pred = pd.Series(np.maximum(m.predict(te[feats]), 0), index=te.index)
            add(f"lightgbm | {name}", seed, pp.metrics(te, pred))
        print(name, "xong", f"({time.time() - t0:.0f}s)", flush=True)

    out = pd.DataFrame(rows, columns=["model", "seed", "metric", "value"])
    summary = out.groupby(["model", "metric"]).value.mean().unstack()[["mae", "rmse_pooled", "acc20", "acc30"]]
    print(summary.round(2).to_string())
    return out


def run_cohort(raw: pd.DataFrame, seed: int, n_jobs: int) -> pd.DataFrame:
    df, _, spd = pp.clean(raw)
    split = pp.split_dates(df.ds)
    feats = pit.variants()["sửa cả ba (PIT)"]
    y = "target_remaining_minutes"

    S120, _ = pit.build_samples_pit(df, spd, split, label_max=pp.LABEL_MAX)
    Sall, _ = pit.build_samples_pit(df, spd, split, label_max=NO_CAP)
    parts = lambda S: tuple(S[S.split == k] for k in ("train", "validation", "test"))
    tr1, va1, te1 = parts(S120)
    tra, vaa, tea = parts(Sall)

    rows = []
    def report(tag, name, te, pred):
        m = pp.metrics(te, pd.Series(pred, index=te.index))
        rows.append((tag, name, len(te), m["mae"], m["rmse_pooled"], m["acc20"], m["acc30"]))
        print(f"{tag} {name:46s} n={len(te):7d} MAE={m['mae']:7.2f} RMSE={m['rmse_pooled']:7.2f}", flush=True)

    p = {"n_jobs": n_jobs}
    m1 = fit_lightgbm(tr1, va1, feats, seed=seed, params=p)
    report("A", "train<=120 -> test<=120 (Protocol P, PIT)", te1, np.maximum(m1.predict(te1[feats]), 0))
    report("A", "median(train<=120) -> test<=120", te1, np.full(len(te1), tr1[y].median()))
    report("B", "train<=120 -> test mọi đơn đang cầm", tea, np.maximum(m1.predict(tea[feats]), 0))
    report("B", "median(train<=120) -> test mọi đơn đang cầm", tea, np.full(len(tea), tr1[y].median()))
    m2 = fit_lightgbm(tra, vaa, feats, seed=seed, params=p)
    report("C", "train mọi đơn -> test mọi đơn", tea, np.maximum(m2.predict(tea[feats]), 0))
    report("C", "median(train mọi đơn) -> test mọi đơn", tea, np.full(len(tea), tra[y].median()))
    sub = tea[tea[y] <= pp.LABEL_MAX]
    report("D", "như C, chỉ cặp có nhãn <= 120", sub, np.maximum(m2.predict(sub[feats]), 0))
    print(f"Tỉ lệ cặp test có nhãn <= 120 trong tập mọi đơn đang cầm: {(tea[y] <= pp.LABEL_MAX).mean():.1%}")

    columns = ["case", "model", "n", "mae", "rmse_pooled", "acc20", "acc30"]
    return pd.DataFrame(rows, columns=columns)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=tuple(REPORTS), default="paper", help="Tái lập, đối chứng PIT hoặc cohort")
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44], help="Seed cho biến thể pit")
    parser.add_argument("--seed", type=int, default=42, help="Seed cho biến thể cohort")
    parser.add_argument("--n-jobs", type=int, default=1, help="Số luồng LightGBM cho pit và cohort")
    args = parser.parse_args()

    raw = pd.read_csv(args.source, dtype={"ds": str, "courier_id": str})
    if args.variant == "pit":
        metrics = run_pit(raw, args.seeds, args.n_jobs)
    elif args.variant == "cohort":
        metrics = run_cohort(raw, args.seed, args.n_jobs)
    else:
        metrics = run_paper(raw)

    output = args.output or ROOT / "reports" / "eta" / REPORTS[args.variant]
    output.parent.mkdir(parents=True, exist_ok=True)
    metrics.to_csv(output, index=False)


if __name__ == "__main__":
    main()
