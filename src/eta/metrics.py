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
    y = frame[truth].to_numpy(float)
    difference = np.abs(frame[left].to_numpy(float) - y) - np.abs(frame[right].to_numpy(float) - y)
    result = cluster_mean_bootstrap(difference, frame[cluster], n_boot=n_boot, seed=seed)
    return {"clusters": result["clusters"], "pairs": int(len(frame)),
            "difference_mae_left_minus_right": result["mean"],
            "ci95_low": result["ci95_low"], "ci95_high": result["ci95_high"], "seed": seed, "n_boot": n_boot}


def cluster_mean_bootstrap(values, groups, *, n_boot: int, seed: int) -> dict:
    """Resample whole clusters and weight each draw by its number of orders."""
    values = pd.Series(np.asarray(values, dtype=float))
    groups = pd.Series(groups).reset_index(drop=True)
    if len(values) != len(groups) or groups.isna().any():
        raise ValueError("Every observation needs a cluster")
    if not np.isfinite(values).all() or n_boot < 1:
        raise ValueError("Bootstrap requires finite observations and a positive number of draws")
    by_cluster = values.groupby(groups, sort=True).agg(["sum", "size"])
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
    return {"clusters": clusters, "mean": float(totals.sum() / sizes.sum()),
            "ci95_low": float(np.quantile(estimates, .025)), "ci95_high": float(np.quantile(estimates, .975))}
