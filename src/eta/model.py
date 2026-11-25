"""Small LightGBM adapter used by the registered experiment script."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from .dataset import TARGET
from .metrics import regression_metrics

if TYPE_CHECKING:
    from lightgbm import LGBMRegressor
    from pyspark.sql import SparkSession


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


def _accept_inputs(frame: pd.DataFrame, medians: pd.Series) -> pd.DataFrame:
    return frame.loc[:, medians.index].fillna(medians)


@dataclass(frozen=True)
class AcceptEtaModel:
    estimator: LGBMRegressor
    medians: pd.Series

    def inputs(self, frame: pd.DataFrame) -> pd.DataFrame:
        return _accept_inputs(frame, self.medians)

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return np.maximum(self.estimator.predict(self.inputs(frame)), 0)


def fit_accept_eta(train: pd.DataFrame, calibration: pd.DataFrame,
                   features: list[str], seed: int = 42) -> AcceptEtaModel:
    medians = train[features].median()
    fit_rows = _accept_inputs(train, medians).assign(**{TARGET: train[TARGET]})
    calibration_rows = _accept_inputs(calibration, medians).assign(**{TARGET: calibration[TARGET]})
    estimator = fit_lightgbm(fit_rows, calibration_rows, features, seed=seed)
    return AcceptEtaModel(estimator, medians)


def spark_accept_eta_predictions(
    spark: SparkSession, train: pd.DataFrame, calibration: pd.DataFrame,
    scored: pd.DataFrame, features: list[str], variant: str, seed: int,
) -> np.ndarray:
    """Fit the Spark baselines on the same rows and feature columns as LightGBM."""
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor, LinearRegression

    medians = train[features].median().fillna(0.)
    fit_rows = _accept_inputs(train, medians).astype(float).assign(**{TARGET: train[TARGET]})
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    if variant == "spark_gbt_absolute":
        calibration_rows = _accept_inputs(calibration, medians).astype(float).assign(**{TARGET: calibration[TARGET]})
        fit_rows = pd.concat([fit_rows.assign(calibrate=False), calibration_rows.assign(calibrate=True)])
        estimator = GBTRegressor(
            labelCol=TARGET, lossType="absolute", maxIter=80, maxDepth=6, maxBins=256,
            seed=seed, validationIndicatorCol="calibrate",
        )
    elif variant == "spark_lr":
        estimator = LinearRegression(labelCol=TARGET)
    else:
        raise ValueError(f"Unknown Spark ETA variant: {variant}")

    model = estimator.fit(assembler.transform(spark.createDataFrame(fit_rows)))
    inputs = _accept_inputs(scored, medians).astype(float).assign(position=np.arange(len(scored)))
    predictions = model.transform(assembler.transform(spark.createDataFrame(inputs)))
    ordered = predictions.orderBy("position").select("prediction").toPandas()
    return np.maximum(ordered.prediction.to_numpy(), 0)


def evaluate(model, frame: pd.DataFrame, features: list[str]) -> dict:
    prediction = np.maximum(model.predict(frame[features]), 0)
    result = regression_metrics(frame.target_remaining_minutes, prediction)
    result["predictions"] = prediction
    return result
