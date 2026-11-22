"""Đo ảnh hưởng của việc chọn cohort theo nhãn (0 < nhãn <= 120) lên MAE của Protocol P.

Dùng feature point-in-time (bộ "sửa cả ba" của paper_protocol_pit) để tách riêng hiệu ứng cohort.

    python scripts/run_paper_protocol_cohort.py --source data/delivery_yt.csv

Các dòng in ra:
  A  train/test trên cohort <= 120 phút (đúng Protocol P, feature PIT)
  B  mô hình của A, chấm trên MỌI đơn đang cầm tại thời điểm truy vấn (tình huống triển khai)
  C  train và test trên mọi đơn đang cầm
  D  như C nhưng chỉ tính riêng cặp có nhãn <= 120 (đối chiếu với A)
"""
import argparse, sys, warnings
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.eta import paper_protocol as pp
from src.eta import paper_protocol_pit as pit
from src.eta.model import fit_lightgbm

NO_CAP = 10 ** 6   # không lọc nhãn: mọi đơn đã nhận, chưa giao (nhãn > 0)

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--output", type=Path, default=ROOT / "reports" / "eta_paper_cohort_metrics.csv")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--n-jobs", type=int, default=1)
    a = ap.parse_args()

    raw = pd.read_csv(a.source, dtype={"ds": str, "courier_id": str})
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

    p = {"n_jobs": a.n_jobs}
    m1 = fit_lightgbm(tr1, va1, feats, seed=a.seed, params=p)
    report("A", "train<=120 -> test<=120 (Protocol P, PIT)", te1, np.maximum(m1.predict(te1[feats]), 0))
    report("A", "median(train<=120) -> test<=120", te1, np.full(len(te1), tr1[y].median()))
    report("B", "train<=120 -> test mọi đơn đang cầm", tea, np.maximum(m1.predict(tea[feats]), 0))
    report("B", "median(train<=120) -> test mọi đơn đang cầm", tea, np.full(len(tea), tr1[y].median()))
    m2 = fit_lightgbm(tra, vaa, feats, seed=a.seed, params=p)
    report("C", "train mọi đơn -> test mọi đơn", tea, np.maximum(m2.predict(tea[feats]), 0))
    report("C", "median(train mọi đơn) -> test mọi đơn", tea, np.full(len(tea), tra[y].median()))
    sub = tea[tea[y] <= pp.LABEL_MAX]
    report("D", "như C, chỉ cặp có nhãn <= 120", sub, np.maximum(m2.predict(sub[feats]), 0))
    print(f"Tỉ lệ cặp test có nhãn <= 120 trong tập mọi đơn đang cầm: {(tea[y] <= pp.LABEL_MAX).mean():.1%}")

    a.output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=["case", "model", "n", "mae", "rmse_pooled", "acc20", "acc30"]).to_csv(a.output, index=False)
