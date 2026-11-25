"""Run the registered A-lite/B experiment outside notebooks.

The script selects hyperparameters on validation only. A test result is printed
only after the caller has created ``config/freeze-v1.yaml``.
"""
import argparse
from pathlib import Path
import json

import pandas as pd

from lakehouse import PROJECT_ROOT as ROOT

from eta.dataset import B_FEATURES as FEATURES, build_eta_samples, read_lade_csv
from eta.model import evaluate, fit_lightgbm


def run(source: Path, protocol: str, output: Path, seed: int = 42):
    events = read_lade_csv(source)
    samples = build_eta_samples(events, protocol)
    train, val, test = [samples.loc[samples.split.eq(name)].copy() for name in ["train", "validation", "test"]]
    medians = train[FEATURES].median(numeric_only=True)
    train[FEATURES] = train[FEATURES].fillna(medians)
    val[FEATURES] = val[FEATURES].fillna(medians)
    test[FEATURES] = test[FEATURES].fillna(medians)
    model = fit_lightgbm(train, val, FEATURES, seed=seed)
    val_result = evaluate(model, val, FEATURES)
    val_result.pop("predictions")
    test_result = evaluate(model, test, FEATURES)
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"order_id": test.order_id, "courier_day": test.courier_day,
                  "truth": test.target_remaining_minutes, "prediction": test_result.pop("predictions")}).to_csv(output, index=False)
    result = {"protocol": protocol, "train_rows": len(train), "validation_rows": len(val),
              "test_rows": len(test), "validation": val_result, "test": test_result}
    print(json.dumps(result, indent=2, default=float))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    parser.add_argument("--protocol", choices=["A-lite", "B"], default="B")
    parser.add_argument("--output", type=Path, default=ROOT / "reports" / "eta" / "eta_predictions.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(args.source, args.protocol, args.output, args.seed)
