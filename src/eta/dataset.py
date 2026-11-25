"""Vectorized construction of an accept-time ETA experiment table.

Phạm vi: đây là bài toán PHỤ "ETA lúc nhận đơn" (query_ts = accept_ts, nhãn =
delivery_minutes). Nó dùng để định lượng leakage (A-lite so với B). Bài toán ETA
của bài báo LaDe (truy vấn lúc giao xong, nhãn 1-120 phút) nằm ở paper_protocol.py.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from lakehouse import PROJECT_ROOT

from .features import (
    acceptance_distance, build_observed_accept_features, haversine_km, load_schema, role_columns,
)
from .splits import assign_time_split, query_grid, time_boundaries

TARGET = "target_remaining_minutes"
B_FEATURES = [
    "pending_order_count", "completed_order_count", "courier_avg_min",
    "distance_to_last_delivery_km", "candidate_lat", "candidate_lng",
    "candidate_aoi_type", "candidate_region_id", "accept_hour",
]
AOI_FEATURES = ["aoi_past_avg_min", "aoi_past_count"]
REGISTERED_ACCEPT_VARIANTS = {
    "protocol_b": B_FEATURES,
    "protocol_b_aoi": [*B_FEATURES, *AOI_FEATURES],
    "protocol_b_aoi_gps": [*B_FEATURES, *AOI_FEATURES, "accept_to_stop_km"],
}
OBSERVED_BASE_VARIANT = "protocol_b_observed"
QUEUE_HISTORY_VARIANT = "protocol_b_queue_history"
BATCH_VARIANT = "protocol_b_queue_history_batch"
QUEUE_HISTORY_FEATURES = [
    "courier_accepted_today", "courier_delivered_today", "courier_idle_min",
    "aoi_recent_count", "aoi_recent_avg_min", "region_hour_recent_count",
    "region_hour_recent_avg_min", "courier_recent_avg_min", "courier_recent_gap_avg_min",
]
OBSERVED_ACCEPT_VARIANTS = {
    OBSERVED_BASE_VARIANT: REGISTERED_ACCEPT_VARIANTS["protocol_b_aoi_gps"],
    QUEUE_HISTORY_VARIANT: [
        *REGISTERED_ACCEPT_VARIANTS["protocol_b_aoi_gps"], *QUEUE_HISTORY_FEATURES,
    ],
    BATCH_VARIANT: [
        *REGISTERED_ACCEPT_VARIANTS["protocol_b_aoi_gps"], *QUEUE_HISTORY_FEATURES,
        "batch_size", "batch_distance_rank",
    ],
}
ACCEPT_VARIANTS = {**REGISTERED_ACCEPT_VARIANTS, **OBSERVED_ACCEPT_VARIANTS}
FLOW_ACCEPT_VARIANT = QUEUE_HISTORY_VARIANT

SOURCE_COLUMNS = [
    "order_id", "region_id", "city", "courier_id", "lng", "lat", "aoi_id", "aoi_type",
    "accept_time", "accept_gps_time", "accept_gps_lng", "accept_gps_lat", "delivery_time",
    "delivery_gps_time", "delivery_gps_lng", "delivery_gps_lat", "ds",
]


def read_lade_csv(path: str | Path, year: int = 2022) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype="string", keep_default_na=False)
    return parse_lade_frame(raw, year)


def parse_lade_frame(raw: pd.DataFrame, year: int = 2022) -> pd.DataFrame:
    """Parse an already loaded Bronze frame using the same CSV contract."""
    if list(raw.columns) != SOURCE_COLUMNS:
        raise ValueError(f"Expected LaDe-D columns, received {list(raw.columns)}")
    out = raw.copy()
    for col in ["region_id", "courier_id", "aoi_id", "aoi_type", "lng", "lat",
                "accept_gps_lng", "accept_gps_lat", "delivery_gps_lng", "delivery_gps_lat"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    for source, target in [("accept_time", "accept_ts"), ("delivery_time", "delivery_ts"),
                           ("accept_gps_time", "accept_gps_ts"), ("delivery_gps_time", "delivery_gps_ts")]:
        out[target] = pd.to_datetime(f"{year}-" + out[source].astype(str), format="%Y-%m-%d %H:%M:%S", errors="coerce")
    out["delivery_minutes"] = (out.delivery_ts - out.accept_ts).dt.total_seconds() / 60
    out["ds_date"] = pd.to_datetime(f"{year}-" + out.ds.astype(str).str.zfill(4), format="%Y-%m%d", errors="coerce")
    return out


def build_accept_time_features(events: pd.DataFrame) -> pd.DataFrame:
    """Build AOI history and distance to destination at acceptance, indexed by order ID."""
    schema = load_schema()
    candidates = events.loc[:, role_columns("candidate", schema)].set_index("order_id", verify_integrity=True)
    completed = events.loc[:, role_columns("completed", schema)].set_index("order_id")
    duration = (completed.delivery_ts - completed.accept_ts).dt.total_seconds() / 60
    history = completed.loc[duration.gt(0), ["aoi_id", "delivery_ts"]].assign(duration_minutes=duration)
    history = history.sort_values("delivery_ts", kind="stable")

    accept_times = candidates.accept_ts.to_numpy()
    candidate_groups = candidates.groupby("aoi_id", sort=False).indices
    past_counts = np.zeros(len(candidates), dtype=int)
    past_averages = np.full(len(candidates), np.nan)
    for aoi_id, deliveries in history.groupby("aoi_id", sort=False):
        positions = candidate_groups[aoi_id]
        positions = positions[~np.isnat(accept_times[positions])]
        counts = np.searchsorted(deliveries.delivery_ts.to_numpy(), accept_times[positions], side="right")
        totals = np.r_[0., deliveries.duration_minutes.to_numpy(dtype=float).cumsum()]
        averages = np.divide(totals[counts], counts, out=np.full(len(counts), np.nan), where=counts > 0)
        past_counts[positions] = counts
        past_averages[positions] = averages

    features = pd.DataFrame({"aoi_past_count": past_counts, "aoi_past_avg_min": past_averages}, index=candidates.index)
    features["accept_to_stop_km"] = acceptance_distance(candidates)
    return features


def build_eta_samples(events: pd.DataFrame, protocol: str = "B", max_per_courier_day: int | None = 12,
                      seed: int = 42, *, include_aoi_gps: bool = False) -> pd.DataFrame:
    """Create one label row per accepted candidate.

    Protocol A-lite uses the courier's full-file average and is intentionally
    leaky. Protocol B uses only deliveries completed by the query timestamp.
    Both protocols use the same candidate rows and event-time computation.

    Theo prereg.yaml, mỗi (shipper, ngày) chỉ giữ tối đa ``max_per_courier_day`` truy vấn,
    chọn bằng băm xác định (``query_grid``, seed cố định) nên A-lite và B có CÙNG tập
    truy vấn ("common spine"). Đặt None để giữ mọi đơn. Đặc trưng vẫn tính trên toàn bộ
    lịch sử của shipper trước khi lấy mẫu.

    Set ``include_aoi_gps`` to attach AOI history and GPS distance for the validation experiment.
    """
    if protocol not in {"A-lite", "B"}:
        raise ValueError("protocol must be 'A-lite' or 'B'")
    required = {"order_id", "courier_id", "lat", "lng", "accept_ts", "delivery_ts",
                "delivery_gps_lat", "delivery_gps_lng", "delivery_minutes"}
    missing = required - set(events.columns)
    if missing:
        raise ValueError(f"Missing ETA input columns: {sorted(missing)}")
    data = events.loc[events.delivery_minutes.gt(0) & events.accept_ts.notna() & events.delivery_ts.notna()].copy()
    data = data.sort_values(["courier_id", "accept_ts", "order_id"]).reset_index(drop=True)
    outputs = []
    for courier_id, group in data.groupby("courier_id", sort=False):
        group = group.reset_index(drop=True)
        accept_ns = group.accept_ts.astype("int64").to_numpy()
        delivery_ns = group.delivery_ts.astype("int64").to_numpy()
        accepted_order = np.argsort(accept_ns, kind="stable")
        completed_order = np.argsort(delivery_ns, kind="stable")
        sorted_accept = accept_ns[accepted_order]
        sorted_delivery = delivery_ns[completed_order]
        durations = group.delivery_minutes.to_numpy(float)[completed_order]
        prefix = np.concatenate([[0.0], np.cumsum(durations)])
        query_k = np.searchsorted(sorted_delivery, accept_ns, side="right")
        accepted_k = np.searchsorted(sorted_accept, accept_ns, side="right")
        pending = np.maximum(accepted_k - query_k - 1, 0)
        past_avg = np.full(len(group), np.nan, dtype=float)
        has_history = query_k > 0
        past_avg[has_history] = prefix[query_k[has_history]] / query_k[has_history]
        all_avg = float(group.delivery_minutes.mean())
        last_pos = np.full(len(group), -1, dtype=int)
        has_last = query_k > 0
        last_pos[has_last] = completed_order[query_k[has_last] - 1]
        last_lat = np.full(len(group), np.nan)
        last_lng = np.full(len(group), np.nan)
        last_lat[has_last] = group.delivery_gps_lat.to_numpy(float)[last_pos[has_last]]
        last_lng[has_last] = group.delivery_gps_lng.to_numpy(float)[last_pos[has_last]]
        distance = haversine_km(group.lat.to_numpy(float), group.lng.to_numpy(float), last_lat, last_lng)
        distance[~has_last] = np.nan
        out = pd.DataFrame({
            "order_id": group.order_id.astype(str), "courier_id": courier_id,
            "query_ts": group.accept_ts, "courier_day": group.accept_ts.dt.strftime("%Y-%m-%d"),
            "target_remaining_minutes": group.delivery_minutes.astype(float),
            "pending_order_count": pending.astype(int), "completed_order_count": query_k.astype(int),
            "courier_avg_min": (all_avg if protocol == "A-lite" else past_avg),
            "distance_to_last_delivery_km": distance,
            "candidate_lat": group.lat, "candidate_lng": group.lng,
            "candidate_aoi_type": group.aoi_type, "candidate_region_id": group.region_id,
            "accept_hour": group.accept_ts.dt.hour + group.accept_ts.dt.minute / 60,
            "protocol": protocol,
        })
        outputs.append(out)
    result = pd.concat(outputs, ignore_index=True)
    if max_per_courier_day is not None:
        grid = query_grid(data, max_per_courier_day, seed)
        result = result.loc[result.order_id.isin(set(grid.order_id.astype(str)))].reset_index(drop=True)
    boundaries = time_boundaries(result, "query_ts")
    result["split"] = assign_time_split(result, boundaries, "query_ts")
    if include_aoi_gps:
        result = result.join(build_accept_time_features(events), on="order_id", validate="one_to_one")
    return result


def load_accept_prereg() -> dict:
    with (PROJECT_ROOT / "config" / "prereg.yaml").open(encoding="utf-8") as settings:
        return yaml.safe_load(settings)


def build_protocol_b_samples(events: pd.DataFrame, data_config: dict, *, observed: bool = False) -> pd.DataFrame:
    """Share the registered query IDs between the notebook, flow and benchmark."""
    samples = build_eta_samples(
        events, "B", data_config["max_queries_per_courier_day"], data_config["query_seed"],
        include_aoi_gps=not observed,
    )
    return attach_observed_context(samples, events) if observed else samples


def attach_observed_context(samples: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    features = build_observed_accept_features(events)
    return samples.drop(columns=samples.columns.intersection(features.columns)).join(
        features, on="order_id", validate="one_to_one",
    )


def labels_available_before(samples: pd.DataFrame, orders: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Select rows whose delivery label was observable before the fit cutoff."""
    return samples.loc[samples.order_id.map(orders.delivery_ts).lt(cutoff)]


def validation_rows(samples: pd.DataFrame, orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    boundaries = time_boundaries(samples, "query_ts")
    train = labels_available_before(samples.loc[samples.split.eq("train")], orders, boundaries.train_end)
    return train, samples.loc[samples.split.eq("validation")]


def rolling_origin_rows(samples: pd.DataFrame, orders: pd.DataFrame, *, last_only: bool = False):
    """Three successive score windows inside development, with separate calibration."""
    days = samples.query_ts.dt.normalize()
    development_days = pd.DatetimeIndex(days[samples.split.eq("validation")].unique()).sort_values()
    if len(development_days) == 0:
        raise ValueError("No development dates for accept-time ETA")
    windows = np.array_split(development_days.to_numpy(), min(3, len(development_days)))
    if last_only:
        windows = windows[-1:]
    for window in windows:
        cutoff = pd.Timestamp(window[0])
        earlier_days = pd.DatetimeIndex(days[days.lt(cutoff)].unique()).sort_values()
        if len(earlier_days) < 2:
            raise ValueError("Rolling-origin evaluation requires two earlier dates for fit and calibration")
        n_calibration_days = min(14, max(1, len(earlier_days) // 5))
        calibration_start = earlier_days[-n_calibration_days]
        train = labels_available_before(samples.loc[days.lt(calibration_start)], orders, calibration_start)
        calibration = labels_available_before(
            samples.loc[days.ge(calibration_start) & days.lt(cutoff)], orders, cutoff,
        )
        if train.empty or calibration.empty:
            raise ValueError(f"No observed fit or calibration labels before {cutoff.date()}")
        yield train, calibration, samples.loc[days.isin(window)]


def flow_evaluation_rows(samples: pd.DataFrame, orders: pd.DataFrame):
    """Publish the last development window, with calibration for short datasets."""
    earlier_dates = samples.loc[samples.split.eq("train"), "query_ts"].dt.normalize().nunique()
    if earlier_dates >= 2:
        train, calibration, scored = next(rolling_origin_rows(samples, orders, last_only=True))
        return train, calibration, scored, "rolling_validation"

    train, validation = validation_rows(samples, orders)
    holdout_start = time_boundaries(samples, "query_ts").validation_end
    calibration = labels_available_before(validation, orders, holdout_start)
    if train.empty or calibration.empty:
        raise ValueError("No observed fit or calibration labels for accept-time ETA")
    return train, calibration, calibration, "calibration"
