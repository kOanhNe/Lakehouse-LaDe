# Data contract

`delivery_yt.csv` is an extract of LaDe-D. The raw Bronze table must retain all 17 source columns as strings plus ingestion metadata. Casts belong in Silver and must be reproducible.

| Column | Type after Silver | Role | Null policy |
|---|---:|---|---|
| `order_id` | string | unique order grain | reject only missing/duplicate key |
| `region_id`, `courier_id`, `aoi_id`, `aoi_type` | long | categorical identifiers | flag missing |
| `city` | string | source city | flag missing |
| `lng`, `lat` | double | stop coordinates | flag invalid range/NaN/zero island |
| `accept_time`, `delivery_time` | timestamp | event times reconstructed with `DATA_YEAR` | flag parse failure |
| `accept_gps_time`, `delivery_gps_time` | timestamp | nearest GPS event times | flag parse failure and future relative to event |
| GPS lng/lat pairs | double | event coordinates | independent missing/range flags |
| `ds` | string + `ds_date` | source day code and reconstructed date | compare with both event dates |
| `delivery_minutes` | double | accept-to-completion label | `>0` for duration analyses |
| `day_gap` | integer | calendar-day difference | descriptive flag |
| `distance_km` | double | straight-line stop-to-accept-GPS distance | null when coordinates invalid |

The raw event timestamp has minute precision. Same-timestamp rows are ties; `order_id` is only a deterministic tie breaker and is not a physical route sequence.
