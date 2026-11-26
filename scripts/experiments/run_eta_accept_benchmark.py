"""Compare accept-time ETA on the shared Protocol B query set."""
from __future__ import annotations

import argparse
import hashlib
from datetime import timedelta
from math import ceil
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from lakehouse import PROJECT_ROOT as ROOT

from eta.dataset import (
    OBSERVED_ACCEPT_VARIANTS, QUEUE_HISTORY_VARIANT, REGISTERED_ACCEPT_VARIANTS as VARIANTS, TARGET,
    attach_observed_context, build_protocol_b_samples, labels_available_before,
    load_accept_prereg, read_lade_csv, rolling_origin_rows, split_interval_calibration, validation_rows,
)
from eta.intervals import EtaIntervalCalibration, QUANTILE_COLUMNS, QUANTILE_LEVELS
from eta.metrics import accept_eta_metrics, cluster_mean_bootstrap, paired_cluster_bootstrap
from eta.model import fit_accept_eta, spark_accept_eta_predictions
from eta.splits import time_boundaries

REFERENCE_VARIANT = "protocol_b_aoi_gps"


def validation_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    error = np.abs(actual - np.maximum(predicted, 0))
    return {
        "acc20_pct": 100 * np.mean(error <= 20),
        "acc30_pct": 100 * np.mean(error <= 30),
        "mae_min": error.mean(),
        "p90_abs_error_min": np.quantile(error, 0.9),
    }


def load_benchmark_samples(source: Path) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    prereg = load_accept_prereg()
    config = prereg["data"]
    with source.open("rb") as csv_file:
        source_hash = hashlib.file_digest(csv_file, "sha256").hexdigest()
    if source_hash != config["sha256"]:
        raise ValueError(f"Dataset SHA-256 mismatch for {source}. Expected {config['sha256']}, got {source_hash}")

    events = read_lade_csv(source)
    orders = events.set_index("order_id", verify_integrity=True)
    samples = build_protocol_b_samples(events, config)
    return prereg, orders, samples


def write_report(rows: list[dict], output: Path) -> None:
    report = pd.DataFrame(rows)
    report["seed"] = report["seed"].astype("Int64")
    output.parent.mkdir(parents=True, exist_ok=True)
    report.to_csv(output, index=False, float_format="%.8f")
    summary = report.loc[report.scope.eq("pooled")] if "scope" in report else report
    print(summary.dropna(axis="columns", how="all").round(2).to_string(index=False))


def run(source: Path, output: Path, seeds: list[int] | None = None) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, samples = load_benchmark_samples(source)
    if seeds is None:
        seeds = prereg["model"]["seeds"]
    train, validation = validation_rows(samples, orders)
    sample_counts = {
        "train_rows": len(train),
        "validation_rows": len(validation),
        "heldout_rows": int(samples.split.eq("test").sum()),
    }
    actual = validation[TARGET].to_numpy()
    median = np.full(len(validation), train[TARGET].median())
    results = [{
        "variant": "median_train", "seed": None, **sample_counts, **validation_metrics(actual, median),
    }]
    for variant, features in VARIANTS.items():
        for seed in seeds:
            model = fit_accept_eta(train, validation, features, seed=seed)
            predicted = model.predict(validation)
            results.append({
                "variant": variant, "seed": seed, **sample_counts, **validation_metrics(actual, predicted),
            })

    write_report(results, output)


def region_hour_median(train: pd.DataFrame, scored: pd.DataFrame) -> np.ndarray:
    """Median by region and four-hour bucket, with a pooled train fallback."""
    group = ["candidate_region_id", "hour_bucket"]
    train_groups = train.assign(hour_bucket=train.accept_hour.floordiv(4))
    stats = train_groups.groupby(group)[TARGET].agg(["median", "size"])
    medians = stats.loc[stats["size"].ge(30), "median"].rename("region_hour_median")
    scored_groups = scored.assign(hour_bucket=scored.accept_hour.floordiv(4))
    return scored_groups.join(medians, on=group)["region_hour_median"].fillna(train[TARGET].median()).to_numpy()


def region_hour_quantiles(train: pd.DataFrame, scored: pd.DataFrame) -> np.ndarray:
    """Use training quantiles for the median baseline's region and time buckets."""
    group = ["candidate_region_id", "hour_bucket"]
    history = train.assign(hour_bucket=train.accept_hour.floordiv(4)).groupby(group)[TARGET]
    quantiles = history.quantile(QUANTILE_LEVELS).unstack().reindex(columns=QUANTILE_LEVELS)
    quantiles = quantiles.loc[history.size().ge(30)]
    fallback = train[TARGET].quantile(QUANTILE_LEVELS)
    scored_groups = scored.assign(hour_bucket=scored.accept_hour.floordiv(4))
    return scored_groups.join(quantiles, on=group)[list(QUANTILE_LEVELS)].fillna(fallback).to_numpy()


def holdout_metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    residual = actual - predicted
    metrics = validation_metrics(actual, predicted)
    metrics["acc60_pct"] = 100 * np.mean(np.abs(residual) <= 60)
    metrics["underestimate_gt60_pct"] = 100 * np.mean(residual > 60)
    return metrics


def candidate_metrics(actual: np.ndarray, predicted: np.ndarray,
                      validation_residual: np.ndarray) -> dict[str, float]:
    lower_offset, upper_offset = np.quantile(validation_residual, [0.10, 0.90])
    lower = np.maximum(0, predicted + lower_offset)
    upper = np.maximum(0, predicted + upper_offset)

    slow = actual > 180
    flagged = np.argsort(-predicted, kind="stable")[:ceil(0.10 * len(actual))]
    return {
        "interval_coverage_pct": 100 * np.mean((actual >= lower) & (actual <= upper)),
        "interval_median_width_min": np.median(upper - lower),
        "over_180_prevalence_pct": 100 * np.mean(slow),
        "flagged_rows": len(flagged),
        "flagged_over_180_pct": 100 * np.mean(slow[flagged]),
        "over_180_recall_pct": 100 * np.sum(slow[flagged]) / max(1, np.sum(slow)),
    }


def run_holdout(source: Path, output: Path) -> None:
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, samples = load_benchmark_samples(source)
    train, validation = validation_rows(samples, orders)
    test_start = time_boundaries(samples, "query_ts").validation_end
    validation = labels_available_before(validation, orders, test_start)
    test = samples.loc[samples.split.eq("test")].sort_values(["query_ts", "order_id"])
    sample_counts = {
        "train_rows": len(train), "validation_rows": len(validation), "test_rows": len(test),
    }
    print(f"Fit/validation/test rows {sample_counts}, holdout from {test_start.date()}", flush=True)
    actual = test[TARGET].to_numpy()
    results = []
    for variant, predicted in (
        ("median_train", np.full(len(test), train[TARGET].median())),
        ("median_region_hour", region_hour_median(train, test)),
    ):
        results.append({"variant": variant, "seed": None, **sample_counts, **holdout_metrics(actual, predicted)})

    for variant in ("protocol_b", "protocol_b_aoi_gps"):
        features = VARIANTS[variant]
        for seed in prereg["model"]["seeds"]:
            model = fit_accept_eta(train, validation, features, seed=seed)
            predicted = model.predict(test)
            row = {"variant": variant, "seed": seed, **sample_counts, **holdout_metrics(actual, predicted)}
            if variant == "protocol_b_aoi_gps":
                residual = validation[TARGET].to_numpy() - model.predict(validation)
                row.update(candidate_metrics(actual, predicted, residual))
            results.append(row)
            print(f"Scored {variant}, seed {seed}", flush=True)

    write_report(results, output)


def rolling_metrics(actual, predicted) -> dict:
    metrics = accept_eta_metrics(actual, predicted)
    metrics.pop("rmse")
    return metrics


def accept_slice_indices(scored: pd.DataFrame) -> dict:
    slices = {
        "batch_size": pd.cut(scored.batch_size, [0, 1, 3, np.inf], labels=["1", "2-3", "4+"]),
        "accept_hour_4h": scored.accept_hour.floordiv(4).mul(4).astype(int),
        "region": scored.candidate_region_id,
        "duration": np.where(scored[TARGET] <= 1440, "<=1440", ">1440"),
    }
    return {scope: scored.groupby(labels, observed=True, sort=True).indices for scope, labels in slices.items()}


def run_rolling_origin(source: Path, output: Path, seeds: list[int] | None = None, *, spark=None) -> None:
    """Score only development query dates, on the same orders for every variant."""
    if source.resolve() == output.resolve():
        raise ValueError("Output path must differ from the source CSV")
    prereg, orders, registered = load_benchmark_samples(source)
    seeds = seeds or prereg["model"]["seeds"]
    observed = attach_observed_context(registered, orders.reset_index())
    spark_variants = ("spark_lr", "spark_gbt_absolute", "spark_gbt_calibrated")
    if spark is None:
        reference = REFERENCE_VARIANT
        variants = {reference: VARIANTS[reference], **OBSERVED_ACCEPT_VARIANTS}
    else:
        reference = QUEUE_HISTORY_VARIANT
        feature_set = OBSERVED_ACCEPT_VARIANTS[reference]
        variants = dict.fromkeys((reference, *spark_variants), feature_set)
    predictions = {
        ("median_region_hour", None): [],
        **{(variant, seed): [] for variant in variants for seed in seeds},
    }
    score_windows = []
    results = []
    for fold, (train, calibration, scored) in enumerate(rolling_origin_rows(registered, orders), 1):
        actual = scored[TARGET].to_numpy()
        start = scored.query_ts.min().date()
        end = scored.query_ts.max().date() + timedelta(days=1)
        scope = {
            "scope": "window", "group": str(fold), "score_start": start,
            "score_end_exclusive": end, "train_rows": len(train), "calibration_rows": len(calibration),
        }
        score_windows.append(observed.loc[scored.index])
        print(f"Window {fold} [{start}, {end}) | fit={len(train)} "
              f"calibration={len(calibration)} score={len(scored)}", flush=True)
        for (variant, seed), previous in predictions.items():
            diagnostics = {}
            if seed is None:
                predicted = region_hour_median(train, scored)
            elif variant in spark_variants:
                predicted = spark_accept_eta_predictions(
                    spark, observed.loc[train.index], observed.loc[calibration.index],
                    observed.loc[scored.index], variants[variant], variant, seed,
                    diagnostics=diagnostics,
                )
            else:
                frame = registered if variant == REFERENCE_VARIANT else observed
                model = fit_accept_eta(frame.loc[train.index], frame.loc[calibration.index], variants[variant], seed)
                predicted = model.predict(frame.loc[scored.index])
            previous.append(predicted)
            metrics = rolling_metrics(actual, predicted)
            results.append({"variant": variant, "seed": seed, **scope, **metrics, **diagnostics})
            tree_count = f" trees={diagnostics['gbt_trees']}" if diagnostics else ""
            print(f"  {variant} seed={seed} MAE={metrics['mae']:.2f}{tree_count}", flush=True)

    scored = pd.concat(score_windows, verify_integrity=True)
    actual = scored[TARGET].to_numpy()
    cluster = scored.courier_id.astype(str) + "|" + scored.courier_day
    slice_indices = accept_slice_indices(scored)
    for (variant, seed), windows in predictions.items():
        predicted = np.concatenate(windows)
        row = {"variant": variant, "seed": seed, "scope": "pooled", "group": "all",
               **rolling_metrics(actual, predicted)}
        if seed is not None and variant != reference and cluster.nunique() >= 2:
            paired = pd.DataFrame({
                TARGET: actual, "candidate": predicted,
                "reference": np.concatenate(predictions[(reference, seed)]),
                "courier_day": cluster.to_numpy(),
            })
            cfg = prereg["bootstrap"]
            bootstrap = paired_cluster_bootstrap(
                paired, TARGET, "candidate", "reference", "courier_day", cfg["n_resamples"], cfg["seed"],
            )
            bootstrap["bootstrap_seed"] = bootstrap.pop("seed")
            row.update(bootstrap)
            row["reference_variant"] = reference
        results.append(row)
        for scope, groups in slice_indices.items():
            for label, positions in groups.items():
                results.append({
                    "variant": variant, "seed": seed, "scope": scope, "group": str(label),
                    **rolling_metrics(actual[positions], predicted[positions]),
                })

    write_report(results, output)


def interval_metrics(scored: pd.DataFrame, bootstrap: dict | None = None) -> dict:
    actual = scored[TARGET]
    width = scored.upper_80_minutes - scored.lower_80_minutes
    covered = actual.between(scored.lower_80_minutes, scored.upper_80_minutes)
    on_time = actual.le(scored.promise_p90_minutes)
    result = {
        "n": len(scored), "nominal_interval_pct": 80, "nominal_promise_pct": 90,
        "raw_interval_coverage_pct": 100 * actual.between(scored.p10_minutes, scored.p90_minutes).mean(),
        "interval_coverage_pct": 100 * covered.mean(),
        "mean_width_min": width.mean(), "median_width_min": width.median(),
        "median_upper_80_min": scored.upper_80_minutes.median(),
        "promise_coverage_pct": 100 * on_time.mean(), "late_after_promise_pct": 100 * (~on_time).mean(),
        "mean_promise_min": scored.promise_p90_minutes.mean(),
        "median_promise_min": scored.promise_p90_minutes.median(),
        "mean_promise_delta_vs_region_hour_min": scored.promise_delta_vs_region_hour_min.mean(),
    }
    if "late_risk" in scored:
        late_at_deadline = actual.gt(scored.deadline_minutes)
        flagged_and_late = (scored.late_risk & late_at_deadline).sum()
        result.update(deadline_minutes=scored.deadline_minutes.iloc[0], flagged_n=scored.late_risk.sum(),
                      observed_late_n=late_at_deadline.sum(), flagged_and_late_n=flagged_and_late)
    if bootstrap is not None:
        groups = scored.courier_id.astype(str) + "|" + scored.courier_day
        if groups.nunique() >= 2:
            for name, success in (("interval", covered), ("promise", on_time)):
                ci = cluster_mean_bootstrap(100 * success, groups, n_boot=bootstrap["n_resamples"],
                                            seed=bootstrap["seed"])
                result.update({f"{name}_ci95_low_pct": ci["ci95_low"], f"{name}_ci95_high_pct": ci["ci95_high"]})
            result.update(clusters=groups.nunique(), n_boot=bootstrap["n_resamples"], bootstrap_seed=bootstrap["seed"])
    return result


def run_interval_benchmark(source: Path, output: Path, seeds: list[int] | None = None, *,
                           predictions_output: Path | None = None, deadline_minutes: float | None = None) -> None:
    """Evaluate quantiles on the registered rolling windows with a dedicated conformal split."""
    paths = [path.resolve() for path in (source, output, predictions_output) if path is not None]
    if len(paths) != len(set(paths)):
        raise ValueError("Source, report and per-order output paths must differ")
    if deadline_minutes is not None and (not np.isfinite(deadline_minutes) or deadline_minutes <= 0):
        raise ValueError("The deadline must be a positive finite number of minutes after acceptance")

    prereg, orders, registered = load_benchmark_samples(source)
    seeds = seeds or [42]
    if len(seeds) != len(set(seeds)):
        raise ValueError("Each seed should appear only once")
    observed = attach_observed_context(registered, orders.reset_index())
    features = OBSERVED_ACCEPT_VARIANTS[QUEUE_HISTORY_VARIANT]
    baseline = ("region_hour_quantiles", None)
    variants = [baseline] + [(QUEUE_HISTORY_VARIANT, seed) for seed in seeds]
    windows = {key: [] for key in variants}
    results = []

    for fold, (train, validation, scored) in enumerate(rolling_origin_rows(observed, orders), 1):
        early_stopping, calibration = split_interval_calibration(validation, orders)
        evaluation = pd.concat([calibration, scored], verify_integrity=True)
        start = scored.query_ts.min().date()
        scope = {
            "scope": "window", "group": str(fold), "score_start": start,
            "score_end_exclusive": scored.query_ts.max().date() + timedelta(days=1), "train_rows": len(train),
            "early_stopping_start": early_stopping.query_ts.min().date(), "early_stopping_rows": len(early_stopping),
            "conformal_start": calibration.query_ts.min().date(), "conformal_rows": len(calibration),
        }
        print(f"Window {fold} from {start} | fit={len(train)} early_stop={len(early_stopping)} "
              f"conformal={len(calibration)} score={len(scored)}", flush=True)
        scored_keys = scored.loc[:, ["order_id", "query_ts", "courier_id", "courier_day", "batch_size",
                                     "candidate_region_id", "accept_hour", TARGET]].reset_index(drop=True)
        window_predictions = {}
        for variant, seed in windows:
            diagnostics = {}
            if seed is None:
                quantiles = region_hour_quantiles(train, evaluation)
            else:
                models = [fit_accept_eta(train, early_stopping, features, seed, objective="quantile",
                                         params={"alpha": alpha, "metric": "quantile"}) for alpha in QUANTILE_LEVELS]
                quantiles = np.column_stack([model.predict(evaluation) for model in models])
                diagnostics.update({f"{name}_best_iteration": model.estimator.best_iteration_
                                    for name, model in zip(("p10", "p50", "p90"), models)})
            adjustment = EtaIntervalCalibration.fit(calibration[TARGET].to_numpy(), quantiles[:len(calibration)])
            predictions = adjustment.predict(quantiles[len(calibration):], deadline_minutes=deadline_minutes)
            if deadline_minutes is not None:
                predictions["deadline_minutes"] = deadline_minutes
            window_predictions[(variant, seed)] = (
                scored_keys.join(predictions).assign(variant=variant, seed=seed, window=fold),
                {**diagnostics, "interval_offset_min": adjustment.interval_offset_min,
                 "promise_offset_min": adjustment.promise_offset_min},
            )

        reference_promise = window_predictions[baseline][0].promise_p90_minutes
        for key, (predictions, diagnostics) in window_predictions.items():
            variant, seed = key
            predictions["promise_delta_vs_region_hour_min"] = predictions.promise_p90_minutes - reference_promise
            windows[key].append(predictions)
            metrics = interval_metrics(predictions)
            results.append({"variant": variant, "seed": seed, **scope, **metrics, **diagnostics})
            print(f"  {variant} seed={seed} coverage={metrics['interval_coverage_pct']:.2f}% "
                  f"median width={metrics['median_width_min']:.2f} min", flush=True)

    per_order = []
    for (variant, seed), prediction_windows in windows.items():
        predictions = pd.concat(prediction_windows, ignore_index=True)
        identity = {"variant": variant, "seed": seed}
        results.append({**identity, "scope": "pooled", "group": "all",
                        **interval_metrics(predictions, prereg["bootstrap"])})
        for scope, groups in accept_slice_indices(predictions).items():
            for group, indices in groups.items():
                results.append({**identity, "scope": scope, "group": str(group),
                                **interval_metrics(predictions.iloc[indices])})
        if predictions_output is not None:
            columns = ["variant", "seed", "window", "order_id", "query_ts", *QUANTILE_COLUMNS,
                       "lower_80_minutes", "upper_80_minutes", "promise_p90_minutes", TARGET]
            if deadline_minutes is not None:
                columns.extend(["deadline_minutes", "late_risk"])
            per_order.append(predictions.loc[:, columns])

    write_report(results, output)
    if predictions_output is not None:
        predictions_output.parent.mkdir(parents=True, exist_ok=True)
        pd.concat(per_order, ignore_index=True).to_csv(predictions_output, index=False, float_format="%.8f")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--seeds", "--seed", type=int, nargs="+", help="Model seeds (intervals and Spark default to 42)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--holdout", action="store_true", help="Evaluate the frozen validation-selected candidate")
    mode.add_argument("--rolling-origin", action="store_true", help="Compare features on development dates only")
    mode.add_argument("--intervals", action="store_true", help="Evaluate calibrated ETA intervals on development dates")
    parser.add_argument("--spark", action="store_true", help="Compare Spark LR/GBT with LightGBM on rolling-origin rows")
    parser.add_argument("--predictions-output", type=Path, help="Per-order CSV for --intervals")
    parser.add_argument("--deadline-minutes", type=float, help="Optional deadline from acceptance for interval risk flags")
    args = parser.parse_args()
    if args.spark and not args.rolling_origin:
        parser.error("--spark uses the --rolling-origin evaluation")
    if not args.intervals and (args.predictions_output is not None or args.deadline_minutes is not None):
        parser.error("--predictions-output and --deadline-minutes use the --intervals evaluation")
    if args.intervals:
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_intervals_rolling.csv"
        run_interval_benchmark(args.source, output, args.seeds, predictions_output=args.predictions_output,
                               deadline_minutes=args.deadline_minutes)
    elif args.holdout:
        if args.seeds is not None:
            parser.error("--holdout uses the frozen seeds from prereg.yaml")
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_holdout.csv"
        run_holdout(args.source, output)
    elif args.rolling_origin:
        if args.spark:
            from pyspark.sql import SparkSession

            spark = (SparkSession.builder.master("local[2]").appName("ETA rolling-origin")
                     .config("spark.sql.shuffle.partitions", "8")
                     .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
            spark.sparkContext.setLogLevel("ERROR")
            output = args.output or ROOT / "reports" / "eta" / "eta_accept_spark_calibrated_rolling.csv"
            with TemporaryDirectory(prefix="eta-spark-checkpoints-") as checkpoint_dir:
                try:
                    spark.sparkContext.setCheckpointDir(Path(checkpoint_dir).as_uri())
                    run_rolling_origin(args.source, output, args.seeds or [42], spark=spark)
                finally:
                    spark.stop()
        else:
            output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_rolling.csv"
            run_rolling_origin(args.source, output, args.seeds)
    else:
        output = args.output or ROOT / "reports" / "eta" / "eta_accept_benchmark_validation.csv"
        run(args.source, output, args.seeds)


if __name__ == "__main__":
    main()
