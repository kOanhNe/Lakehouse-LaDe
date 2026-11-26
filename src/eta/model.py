"""Small LightGBM adapter used by the registered experiment script."""
from __future__ import annotations

from dataclasses import dataclass
import json
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from .dataset import TARGET
from .metrics import regression_metrics

SPARK_GBT_CANDIDATES = ((40, 0.05), (80, 0.10))
SPARK_GBT_BASELINE = (40, 0.10)

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
    *, diagnostics: dict | None = None,
) -> np.ndarray:
    """Fit the Spark baselines on the same rows and feature columns as LightGBM."""
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor, LinearRegression
    from pyspark.sql import functions as F

    medians = train[features].median().fillna(0.)
    fit_rows = _accept_inputs(train, medians).astype(float).assign(**{TARGET: train[TARGET]})
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    trial_stats = []
    if variant == "spark_gbt_absolute":
        calibration_rows = _accept_inputs(calibration, medians).astype(float).assign(**{TARGET: calibration[TARGET]})
        combined = pd.concat([fit_rows.assign(calibrate=False), calibration_rows.assign(calibrate=True)])
        estimator = GBTRegressor(
            labelCol=TARGET, lossType="absolute", maxIter=80, maxDepth=6, maxBins=256,
            seed=seed, validationIndicatorCol="calibrate",
        )
        model = estimator.fit(assembler.transform(spark.createDataFrame(combined)))
    elif variant == "spark_gbt_calibrated":
        calibration_rows = _accept_inputs(calibration, medians).astype(float).assign(**{TARGET: calibration[TARGET]})
        fit = assembler.transform(spark.createDataFrame(fit_rows)).cache()
        validation = assembler.transform(spark.createDataFrame(calibration_rows)).cache()
        try:
            best_mae = np.inf
            for max_iter, step_size in SPARK_GBT_CANDIDATES:
                candidate = GBTRegressor(
                    labelCol=TARGET, lossType="absolute", maxIter=max_iter, stepSize=step_size,
                    maxDepth=6, maxBins=256, seed=seed,
                ).fit(fit)
                prediction = F.greatest(F.col("prediction"), F.lit(0.0))
                mae = float(candidate.transform(validation).agg(F.avg(F.abs(prediction - F.col(TARGET)))).first()[0])
                trial_stats.append({
                    "max_iter": max_iter, "step_size": step_size, "trees": candidate.getNumTrees, "mae": mae,
                })
                if mae < best_mae:
                    model, best_mae = candidate, mae
        finally:
            validation.unpersist()
            fit.unpersist()
    elif variant == "spark_lr":
        model = LinearRegression(labelCol=TARGET).fit(assembler.transform(spark.createDataFrame(fit_rows)))
    else:
        raise ValueError(f"Unknown Spark ETA variant: {variant}")

    if diagnostics is not None and variant != "spark_lr":
        diagnostics.update({
            "gbt_trees": model.getNumTrees,
            "gbt_max_iter": model.getMaxIter(),
            "gbt_step_size": model.getStepSize(),
            "gbt_max_depth": model.getMaxDepth(),
            "gbt_max_bins": model.getMaxBins(),
            "gbt_calibration_mae": best_mae if trial_stats else None,
            "gbt_calibration_trials": json.dumps(trial_stats) if trial_stats else None,
        })
    inputs = _accept_inputs(scored, medians).astype(float).assign(position=np.arange(len(scored)))
    predictions = model.transform(assembler.transform(spark.createDataFrame(inputs)))
    ordered = predictions.orderBy("position").select("prediction").toPandas()
    return np.maximum(ordered.prediction.to_numpy(), 0)


def evaluate(model, frame: pd.DataFrame, features: list[str]) -> dict:
    prediction = np.maximum(model.predict(frame[features]), 0)
    result = regression_metrics(frame.target_remaining_minutes, prediction)
    result["predictions"] = prediction
    return result
