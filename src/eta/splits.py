"""Deterministic time splits and protocol-independent query sampling."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class TimeBoundaries:
    train_end: pd.Timestamp
    validation_end: pd.Timestamp


def time_boundaries(frame: pd.DataFrame, date_col: str = "accept_ts", train_ratio=.6, validation_ratio=.2) -> TimeBoundaries:
    dates = pd.to_datetime(frame[date_col]).dt.normalize().dropna().drop_duplicates().sort_values().tolist()
    if len(dates) < 3:
        raise ValueError("At least three distinct dates are required")
    train_idx = max(1, min(len(dates) - 2, int(len(dates) * train_ratio)))
    val_idx = max(train_idx + 1, min(len(dates) - 1, int(len(dates) * (train_ratio + validation_ratio))))
    return TimeBoundaries(train_end=dates[train_idx], validation_end=dates[val_idx])


def assign_time_split(frame: pd.DataFrame, boundaries: TimeBoundaries, date_col: str = "accept_ts") -> pd.Series:
    dates = pd.to_datetime(frame[date_col]).dt.normalize()
    return pd.Series(pd.NA, index=frame.index, dtype="string").mask(dates.lt(boundaries.train_end), "train").mask(
        dates.ge(boundaries.train_end) & dates.lt(boundaries.validation_end), "validation").mask(
        dates.ge(boundaries.validation_end), "test")


def _stable_hash(key: str, seed: int) -> str:
    return hashlib.sha256(f"{seed}|{key}".encode("utf-8")).hexdigest()


def query_grid(frame: pd.DataFrame, max_per_courier_day: int = 12, seed: int = 42) -> pd.DataFrame:
    """Sample from a raw candidate-time grid before protocol filtering."""
    data = frame.copy()
    data["accept_ts"] = pd.to_datetime(data["accept_ts"])
    data["courier_day"] = data["accept_ts"].dt.strftime("%Y-%m-%d")
    grid = data[["courier_id", "courier_day", "accept_ts", "order_id"]].drop_duplicates()
    grid["key"] = (grid.courier_id.astype(str) + "|" + grid.courier_day + "|"
                   + grid.accept_ts.dt.strftime("%Y-%m-%dT%H:%M:%S") + "|" + grid.order_id.astype(str))
    grid["hash"] = grid["key"].map(lambda key: _stable_hash(key, seed))
    return (grid.sort_values(["courier_id", "courier_day", "hash", "order_id"])
            .groupby(["courier_id", "courier_day"], sort=False, group_keys=False).head(max_per_courier_day)
            .sort_values(["courier_id", "courier_day", "accept_ts", "order_id"])
            .reset_index(drop=True))
