"""Event-time ETA features with an explicit column contract.

The feature builder never uses delivery fields from a candidate or a pending
order. Historical delivery GPS is allowed only after the delivery event.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import yaml

from lakehouse import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "config" / "schema.yaml"


class FeatureContractError(ValueError):
    """Raised when an input violates the declared data contract."""


def load_schema(path: Path = SCHEMA_PATH) -> dict:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _schema_columns(schema: dict, layer: str) -> dict:
    return {item["name"]: item for item in schema["layers"][layer]["columns"]}


def validate_columns(frame: pd.DataFrame, layer: str = "silver", schema: dict | None = None) -> None:
    schema = schema or load_schema()
    declared = _schema_columns(schema, layer)
    unknown = sorted(set(frame.columns) - set(declared))
    if unknown:
        raise FeatureContractError(f"Undeclared {layer} columns: {unknown}")
    missing = sorted(name for name in set(declared) - set(frame.columns)
                     if declared[name]["status"] != "ignored")
    if missing:
        raise FeatureContractError(f"Missing {layer} columns: {missing}")
    ignored = [name for name, item in declared.items() if item["status"] == "ignored"]
    for name in ignored:
        reason = declared[name].get("reason")
        if not reason:
            raise FeatureContractError(f"Ignored column {name!r} needs a reason")


def role_columns(role: str, schema: dict | None = None) -> list[str]:
    schema = schema or load_schema()
    try:
        return list(schema["feature_roles"][role])
    except KeyError as exc:
        raise FeatureContractError(f"Unknown feature role: {role}") from exc


def select_role(frame: pd.DataFrame, role: str, schema: dict | None = None) -> pd.DataFrame:
    schema = schema or load_schema()
    validate_columns(frame, "silver", schema)
    cols = role_columns(role, schema)
    return frame.loc[:, cols].copy()


def _ts(value) -> pd.Timestamp:
    value = pd.Timestamp(value)
    if value.tzinfo is not None:
        value = value.tz_localize(None)
    return value


def truncate_as_of(frame: pd.DataFrame, query_ts, schema: dict | None = None) -> pd.DataFrame:
    """Return the event state visible at ``query_ts``.

    Rows accepted after the query are removed. Pending rows remain, but fields
    observed only at delivery are hidden. The original frame is not modified;
    its label may be retained outside this function for evaluation only.
    """
    schema = schema or load_schema()
    validate_columns(frame, "silver", schema)
    t = _ts(query_ts)
    accept = pd.to_datetime(frame["accept_ts"], errors="coerce")
    delivery = pd.to_datetime(frame["delivery_ts"], errors="coerce")
    visible = frame.loc[accept.le(t)].copy()
    pending = delivery.loc[visible.index].isna() | delivery.loc[visible.index].gt(t)
    hide = ["delivery_ts", "delivery_gps_ts", "delivery_gps_lng", "delivery_gps_lat"]
    for col in hide:
        if col in visible:
            visible.loc[pending, col] = pd.NaT if col.endswith("_ts") else np.nan
    return visible


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _valid_coordinates(lat: pd.Series, lng: pd.Series) -> pd.Series:
    return lat.between(-90, 90) & lng.between(-180, 180) & ~(lat.eq(0) & lng.eq(0))


def acceptance_distance(candidates: pd.DataFrame) -> pd.Series:
    valid = (
        candidates.accept_gps_ts.le(candidates.accept_ts)
        & _valid_coordinates(candidates.accept_gps_lat, candidates.accept_gps_lng)
        & _valid_coordinates(candidates.lat, candidates.lng)
    )
    return haversine_km(candidates.accept_gps_lat, candidates.accept_gps_lng,
                        candidates.lat, candidates.lng).where(valid)


def _completion_history(
    candidates: pd.DataFrame, completed: pd.DataFrame, keys: str | list[str],
    value: str, days: int | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Count and average observed completions in [query - days, query)."""
    positions = candidates.groupby(keys, sort=False).indices
    query = candidates.accept_ts.to_numpy(dtype="datetime64[ns]")
    counts = np.zeros(len(candidates), dtype=int)
    averages = np.full(len(candidates), np.nan)
    for key, history in completed.loc[completed[value].notna()].groupby(keys, sort=False):
        idx = positions.get(key)
        if idx is None:
            continue
        times = history.delivery_ts.to_numpy(dtype="datetime64[ns]")
        end = np.searchsorted(times, query[idx], side="left")
        start = 0 if days is None else np.searchsorted(
            times, query[idx] - np.timedelta64(days, "D"), side="left",
        )
        totals = np.r_[0., history[value].to_numpy(dtype=float).cumsum()]
        n = end - start
        counts[idx] = n
        averages[idx] = np.divide(totals[end] - totals[start], n,
                                  out=np.full(len(idx), np.nan), where=n > 0)
    return counts, averages


def _observed_courier_state(candidates: pd.DataFrame, completed: pd.DataFrame) -> pd.DataFrame:
    delivery_groups = {courier: rows for courier, rows in completed.groupby("courier_id", sort=False)}
    query = candidates.accept_ts.to_numpy(dtype="datetime64[ns]")
    day = candidates.accept_ts.dt.normalize().to_numpy(dtype="datetime64[ns]")
    accepted_today = np.zeros(len(candidates), dtype=int)
    delivered_today = np.zeros(len(candidates), dtype=int)
    pending = np.zeros(len(candidates), dtype=int)
    idle = np.full(len(candidates), np.nan)
    distance = np.full(len(candidates), np.nan)
    for courier, idx in candidates.groupby("courier_id", sort=False).indices.items():
        accepted = np.sort(query[idx])
        earlier = np.searchsorted(accepted, query[idx], side="left")
        accepted_today[idx] = earlier - np.searchsorted(accepted, day[idx], side="left")
        pending[idx] = earlier
        history = delivery_groups.get(courier)
        if history is None:
            continue
        times = history.delivery_ts.to_numpy(dtype="datetime64[ns]")
        seen = np.searchsorted(times, query[idx], side="left")
        pending[idx] -= seen
        delivered_today[idx] = seen - np.searchsorted(times, day[idx], side="left")
        has_last = seen > 0
        eligible = idx[has_last]
        last = history.iloc[seen[has_last] - 1]
        idle[eligible] = (query[eligible] - times[seen[has_last] - 1]) / np.timedelta64(1, "m")
        current = candidates.iloc[eligible]
        valid = (_valid_coordinates(current.lat, current.lng).to_numpy(dtype=bool, na_value=False)
                 & _valid_coordinates(last.lat, last.lng).to_numpy(dtype=bool, na_value=False))
        distance[eligible] = np.where(valid, haversine_km(
            current.lat.to_numpy(dtype=float, na_value=np.nan),
            current.lng.to_numpy(dtype=float, na_value=np.nan),
            last.lat.to_numpy(dtype=float, na_value=np.nan),
            last.lng.to_numpy(dtype=float, na_value=np.nan),
        ), np.nan)
    return pd.DataFrame({
        "pending_order_count": pending, "courier_accepted_today": accepted_today,
        "courier_delivered_today": delivered_today, "courier_idle_min": idle,
        "distance_to_last_delivery_km": distance,
    }, index=candidates.index)


def build_observed_accept_features(events: pd.DataFrame) -> pd.DataFrame:
    """Use earlier events even when their eventual labels are missing.

    The optional batch columns assume every acceptance with the same courier
    and timestamp is visible together. The other columns exclude those peers.
    """
    schema = load_schema()
    candidates = events.loc[events.accept_ts.notna(), role_columns("candidate", schema)].copy()
    candidates["order_id"] = candidates.order_id.astype(str)
    candidates = candidates.set_index("order_id", verify_integrity=True)
    completed = events.loc[:, role_columns("completed", schema)].reset_index(drop=True)
    duration = (completed.delivery_ts - completed.accept_ts).dt.total_seconds() / 60
    completed = completed.loc[duration.gt(0)].assign(duration_min=duration)
    completed = completed.sort_values(["delivery_ts", "order_id"], kind="stable")
    candidates["hour"] = candidates.accept_ts.dt.hour
    completed["hour"] = completed.accept_ts.dt.hour
    delivered_on = completed.delivery_ts.dt.normalize()
    gaps = completed.groupby(["courier_id", delivered_on], sort=False).delivery_ts.diff()
    completed["gap_min"] = gaps.dt.total_seconds() / 60

    features = _observed_courier_state(candidates, completed)
    features["completed_order_count"], features["courier_avg_min"] = _completion_history(
        candidates, completed, "courier_id", "duration_min",
    )
    features["aoi_past_count"], features["aoi_past_avg_min"] = _completion_history(
        candidates, completed, "aoi_id", "duration_min",
    )
    features["aoi_recent_count"], features["aoi_recent_avg_min"] = _completion_history(
        candidates, completed, "aoi_id", "duration_min", days=28,
    )
    features["region_hour_recent_count"], features["region_hour_recent_avg_min"] = _completion_history(
        candidates, completed, ["region_id", "hour"], "duration_min", days=28,
    )
    _, features["courier_recent_avg_min"] = _completion_history(
        candidates, completed, "courier_id", "duration_min", days=7,
    )
    _, features["courier_recent_gap_avg_min"] = _completion_history(
        candidates, completed, "courier_id", "gap_min", days=7,
    )
    features["accept_to_stop_km"] = acceptance_distance(candidates)
    batches = features.accept_to_stop_km.groupby([candidates.courier_id, candidates.accept_ts], sort=False)
    features["batch_size"] = batches.transform("size")
    features["batch_distance_rank"] = batches.rank(method="min")
    return features


def build_query_features(frame: pd.DataFrame, query_ts, candidate_order_id: str | int, schema: dict | None = None) -> pd.DataFrame:
    """Build one candidate row using only information available at query time."""
    schema = schema or load_schema()
    visible = truncate_as_of(frame, query_ts, schema)
    candidate_id = str(candidate_order_id)
    ids = visible["order_id"].astype(str)
    candidate_mask = ids.eq(candidate_id)
    if not candidate_mask.any():
        raise FeatureContractError("Candidate is not accepted and visible at query time")
    candidate = select_role(visible.loc[candidate_mask], "candidate", schema).iloc[0]
    delivery = pd.to_datetime(visible["delivery_ts"], errors="coerce")
    t = _ts(query_ts)
    completed_mask = delivery.le(t).fillna(False)
    pending_mask = ~completed_mask
    completed = select_role(visible.loc[completed_mask], "completed", schema)
    pending = select_role(visible.loc[pending_mask & ~candidate_mask], "pending", schema)
    courier = candidate["courier_id"]
    completed_courier = completed.loc[completed["courier_id"].eq(courier)]
    last = completed_courier.sort_values(["delivery_ts", "order_id"]).tail(1)
    last_lat = last["delivery_gps_lat"].iloc[0] if len(last) else np.nan
    last_lng = last["delivery_gps_lng"].iloc[0] if len(last) else np.nan
    distance = haversine_km(candidate["lat"], candidate["lng"], last_lat, last_lng) if len(last) else np.nan
    if len(completed_courier):
        durations = (pd.to_datetime(completed_courier["delivery_ts"]) - pd.to_datetime(completed_courier["accept_ts"])).dt.total_seconds() / 60
        courier_avg = float(durations.mean())
    else:
        courier_avg = np.nan
    return pd.DataFrame([{
        "order_id": candidate["order_id"],
        "courier_id": courier,
        "query_ts": t,
        "accept_hour": t.hour + t.minute / 60.0,
        "pending_order_count": int(len(pending)),
        "completed_order_count": int(len(completed_courier)),
        "courier_completed_avg_min": courier_avg,
        "distance_to_last_delivery_km": distance,
        "candidate_lat": candidate["lat"],
        "candidate_lng": candidate["lng"],
        "candidate_aoi_type": candidate["aoi_type"],
        "candidate_region_id": candidate["region_id"],
    }])


def assert_truncation_invariant(frame: pd.DataFrame, query_ts, candidate_order_id, schema: dict | None = None) -> None:
    """Fail if future delivery fields can change query-time features."""
    schema = schema or load_schema()
    original = build_query_features(frame, query_ts, candidate_order_id, schema)
    truncated = build_query_features(truncate_as_of(frame, query_ts, schema), query_ts, candidate_order_id, schema)
    pd.testing.assert_frame_equal(original, truncated, check_dtype=False, check_exact=False, rtol=1e-10, atol=1e-10)
