"""Metrics and paired courier-day bootstrap."""
from __future__ import annotations

import numpy as np
import pandas as pd


def regression_metrics(y_true, y_pred) -> dict:
    y_true, y_pred = np.asarray(y_true, dtype=float), np.asarray(y_pred, dtype=float)
    error = y_pred - y_true
    return {"n": int(len(error)), "mae": float(np.mean(np.abs(error))), "rmse": float(np.sqrt(np.mean(error ** 2)))}


def accept_eta_metrics(y_true, y_pred) -> dict:
    actual = np.asarray(y_true, dtype=float)
    prediction = np.maximum(np.asarray(y_pred, dtype=float), 0)
    error = np.abs(actual - prediction)
    total = np.abs(actual).sum()
    return {
        **regression_metrics(actual, prediction),
        "acc20_pct": float(100 * np.mean(error <= 20)),
        "acc30_pct": float(100 * np.mean(error <= 30)),
        "acc60_pct": float(100 * np.mean(error <= 60)),
        "p90_abs_error_min": float(np.quantile(error, 0.9)),
        "wape_pct": float(100 * error.sum() / total) if total > 0 else float("nan"),
    }


def paired_cluster_bootstrap(frame: pd.DataFrame, truth: str, left: str, right: str, cluster: str,
                             n_boot: int = 2000, seed: int = 42) -> dict:
    required = {truth, left, right, cluster}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Missing bootstrap columns: {sorted(missing)}")
    if frame[cluster].isna().any():
        raise ValueError("Every paired observation needs a cluster")
    y = frame[truth].to_numpy(float)
    difference = np.abs(frame[left].to_numpy(float) - y) - np.abs(frame[right].to_numpy(float) - y)
    if not np.isfinite(difference).all():
        raise ValueError("Paired outcomes and predictions must be finite")
    by_cluster = pd.Series(difference, index=frame.index).groupby(frame[cluster], sort=True).agg(["sum", "size"])
    clusters = len(by_cluster)
    if clusters < 2:
        raise ValueError("At least two clusters are required")
    totals = by_cluster["sum"].to_numpy()
    sizes = by_cluster["size"].to_numpy()
    rng = np.random.default_rng(seed)
    estimates = np.empty(n_boot)
    for i in range(n_boot):
        sample = rng.integers(0, clusters, clusters)
        estimates[i] = totals[sample].sum() / sizes[sample].sum()
    return {"clusters": clusters, "pairs": int(len(frame)),
            "difference_mae_left_minus_right": float(totals.sum() / sizes.sum()),
            "ci95_low": float(np.quantile(estimates, .025)), "ci95_high": float(np.quantile(estimates, .975)),
            "seed": seed, "n_boot": n_boot}
