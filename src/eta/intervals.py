"""Split conformal intervals and one-sided promises in minutes from acceptance."""
from __future__ import annotations

from dataclasses import dataclass
from math import ceil

import numpy as np
import pandas as pd

QUANTILE_LEVELS = (0.10, 0.50, 0.90)
QUANTILE_COLUMNS = ("p10_minutes", "p50_minutes", "p90_minutes")


def _ordered_quantiles(quantiles: np.ndarray) -> np.ndarray:
    values = np.asarray(quantiles, dtype=float)
    if values.ndim != 2 or values.shape[1] != len(QUANTILE_LEVELS) or not np.isfinite(values).all():
        raise ValueError("Expected finite predictions in three quantile columns")
    return np.sort(np.maximum(values, 0.0), axis=1)


def _conformal_offset(scores: np.ndarray, coverage: float) -> float:
    rank = ceil((len(scores) + 1) * coverage)
    if rank > len(scores):
        raise ValueError(f"Too few calibration labels for finite {coverage:.0%} coverage")
    return float(np.partition(scores, rank - 1)[rank - 1])


@dataclass(frozen=True)
class EtaIntervalCalibration:
    interval_offset_min: float
    promise_offset_min: float

    @classmethod
    def fit(cls, actual: np.ndarray, quantiles: np.ndarray) -> EtaIntervalCalibration:
        lower, _, upper = _ordered_quantiles(quantiles).T
        actual = np.asarray(actual, dtype=float)
        if actual.shape != lower.shape or not np.isfinite(actual).all() or (actual < 0).any():
            raise ValueError("Expected one finite nonnegative duration per calibration row")
        interval_scores = np.maximum(lower - actual, actual - upper)
        return cls(_conformal_offset(interval_scores, 0.80), _conformal_offset(actual - upper, 0.90))

    def predict(self, quantiles: np.ndarray, *, deadline_minutes: float | None = None) -> pd.DataFrame:
        ordered = _ordered_quantiles(quantiles)
        lower, median, upper = ordered.T
        # Retain the median if a negative offset would shrink a tight interval past it.
        result = pd.DataFrame(ordered, columns=QUANTILE_COLUMNS).assign(
            lower_80_minutes=np.minimum(median, np.maximum(0.0, lower - self.interval_offset_min)),
            upper_80_minutes=np.maximum(median, upper + self.interval_offset_min),
            promise_p90_minutes=np.maximum(median, upper + self.promise_offset_min),
        )
        if deadline_minutes is not None:
            if not np.isfinite(deadline_minutes) or deadline_minutes <= 0:
                raise ValueError("The deadline must be a positive finite number of minutes after acceptance")
            result["late_risk"] = result.promise_p90_minutes.gt(deadline_minutes)
        return result
