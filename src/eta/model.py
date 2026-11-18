"""Small LightGBM adapter used by the registered experiment script."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .metrics import regression_metrics


def fit_lightgbm(train: pd.DataFrame, validation: pd.DataFrame, features: list[str],
                 objective: str = "regression_l1", seed: int = 42, params: dict | None = None):
    try:
        from lightgbm import LGBMRegressor, early_stopping
    except ImportError as exc:
        raise RuntimeError("Install lightgbm to run the registered ETA experiment") from exc
    defaults = dict(n_estimators=2000, learning_rate=.03, num_leaves=31, max_depth=-1,
                    subsample=.8, colsample_bytree=.9, reg_lambda=1.0, n_jobs=1,
                    objective=objective, random_state=seed, deterministic=True,
                    force_row_wise=True, bagging_seed=seed, feature_fraction_seed=seed,
                    data_random_seed=seed, verbosity=-1)
    defaults.update(params or {})
    model = LGBMRegressor(**defaults)
    model.fit(train[features], train["target_remaining_minutes"],
              eval_set=[(validation[features], validation["target_remaining_minutes"])],
              callbacks=[early_stopping(100, verbose=False)])
    return model


def evaluate(model, frame: pd.DataFrame, features: list[str]) -> dict:
    prediction = np.maximum(model.predict(frame[features]), 0)
    result = regression_metrics(frame.target_remaining_minutes, prediction)
    result["predictions"] = prediction
    return result
