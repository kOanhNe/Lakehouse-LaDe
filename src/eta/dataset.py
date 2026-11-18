"""Vectorized construction of an accept-time ETA experiment table.

This module is deliberately separate from the notebook. It provides the
auditable A-lite/B contrast used by the thesis; the full paper reproduction can
be added without changing the feature contract.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .splits import assign_time_split, time_boundaries

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


def _haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def build_eta_samples(events: pd.DataFrame, protocol: str = "B") -> pd.DataFrame:
    """Create one label row per accepted candidate.

    Protocol A-lite uses the courier's full-file average and is intentionally
    leaky. Protocol B uses only deliveries completed by the query timestamp.
    Both protocols use the same candidate rows and event-time computation.
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
        distance = _haversine(group.lat.to_numpy(float), group.lng.to_numpy(float), last_lat, last_lng)
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
    boundaries = time_boundaries(result, "query_ts")
    result["split"] = assign_time_split(result, boundaries, "query_ts")
    return result
