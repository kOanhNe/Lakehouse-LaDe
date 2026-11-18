"""Metrics and paired courier-day bootstrap."""
from __future__ import annotations

import numpy as np
import pandas as pd


def regression_metrics(y_true, y_pred) -> dict:
    y_true, y_pred = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    error = y_pred - y_true
    return {"n": int(len(error)), "mae": float(np.mean(np.abs(error))), "rmse": float(np.sqrt(np.mean(error ** 2)))}


def paired_cluster_bootstrap(frame: pd.DataFrame, truth: str, left: str, right: str, cluster: str,
                             n_boot: int = 2000, seed: int = 42) -> dict:
    required = {truth, left, right, cluster}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing bootstrap columns: {sorted(missing)}")
    groups = [group for _, group in frame.groupby(cluster, sort=True)]
    if len(groups) < 2:
        raise ValueError("At least two clusters are required")
    rng = np.random.default_rng(seed)
    estimates = np.empty(n_boot)
    for i in range(n_boot):
        sample = [groups[j] for j in rng.integers(0, len(groups), len(groups))]
        joined = pd.concat(sample, ignore_index=True)
        y = joined[truth].to_numpy(float)
        estimates[i] = np.mean(np.abs(joined[left].to_numpy(float) - y)) - np.mean(np.abs(joined[right].to_numpy(float) - y))
    return {"clusters": len(groups), "pairs": int(len(frame)), "difference_mae_left_minus_right": float(np.mean(estimates)),
            "ci95_low": float(np.quantile(estimates, .025)), "ci95_high": float(np.quantile(estimates, .975)),
            "seed": seed, "n_boot": n_boot}
