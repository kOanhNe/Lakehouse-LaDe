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

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "config" / "schema.yaml"


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


def _haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


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
    distance = _haversine(candidate["lat"], candidate["lng"], last_lat, last_lng) if len(last) else np.nan
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
