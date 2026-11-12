"""Dataset contract and lossless quality annotations for the local LaDe-D extract.

No quantile, radius, duration cap or inferred region is a deletion rule.
Identifiers remain strings; raw columns remain unchanged.
"""
from pathlib import Path

import numpy as np
import pandas as pd

SOURCE_COLUMNS = ['order_id', 'region_id', 'city', 'courier_id', 'lng', 'lat',
                  'aoi_id', 'aoi_type', 'accept_time', 'accept_gps_time',
                  'accept_gps_lng', 'accept_gps_lat', 'delivery_time',
                  'delivery_gps_time', 'delivery_gps_lng', 'delivery_gps_lat', 'ds']
MISSING_TOKENS = {'', 'nan', 'null', 'none', 'na', 'n/a'}
POLICY_VERSION = 'lade-d-quality-v1'


def missing(values):
    return values.astype('string').str.strip().str.lower().isin(MISSING_TOKENS) | values.isna()


def read_source(path):
    raw = pd.read_csv(Path(path), dtype='string', keep_default_na=False)
    if list(raw.columns) != SOURCE_COLUMNS:
        raise ValueError(f'Expected the 17 LaDe-D columns in order, received {list(raw.columns)}')
    return raw


def coordinate_status(lat, lng):
    la, ln = pd.to_numeric(lat, errors='coerce').astype(float), pd.to_numeric(lng, errors='coerce').astype(float)
    status = pd.Series('valid', index=lat.index, dtype='string')
    status.loc[(la.abs() < 0.01) & (ln.abs() < 0.01)] = 'zero_island'
    status.loc[~la.between(-90, 90) | ~ln.between(-180, 180)] = 'out_of_range'
    status.loc[~np.isfinite(la) | ~np.isfinite(ln)] = 'non_numeric_or_nonfinite'
    status.loc[missing(lat) | missing(lng)] = 'missing'
    return la, ln, status


def haversine(la1, ln1, la2, ln2):
    a = (np.sin(np.radians(la2-la1)/2)**2
         + np.cos(np.radians(la1))*np.cos(np.radians(la2))*np.sin(np.radians(ln2-ln1)/2)**2)
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def annotate(raw, year=2022):
    """One output row per input row, including every invalid observation.

    year is an explicit reconstruction assumption: source timestamps omit it.
    GPS availability is conservative: nearest point must not be later than accept.
    """
    data = raw.copy()
    data['source_row_number'] = np.arange(2, len(raw)+2)
    data['quality_policy_version'] = POLICY_VERSION
    for column in ['accept_time', 'delivery_time', 'accept_gps_time', 'delivery_gps_time']:
        ts = column.replace('_time', '_ts')
        data[ts] = pd.to_datetime(str(year)+'-'+raw[column].str.strip(), format='%Y-%m-%d %H:%M:%S', errors='coerce')
        data['q_invalid_'+column] = data[ts].isna()
    data['ds_date'] = pd.to_datetime(str(year)+'-'+raw.ds.str.strip().str.zfill(4), format='%Y-%m%d', errors='coerce')
    data['delivery_minutes'] = (data.delivery_ts-data.accept_ts).dt.total_seconds()/60
    data['day_gap'] = (data.delivery_ts.dt.normalize()-data.accept_ts.dt.normalize()).dt.days
    data['accept_gps_lag_minutes'] = (data.accept_gps_ts-data.accept_ts).dt.total_seconds()/60
    data['delivery_gps_lag_minutes'] = (data.delivery_gps_ts-data.delivery_ts).dt.total_seconds()/60
    data['q_missing_order_id'] = missing(raw.order_id)
    normalized_id = raw.order_id.str.strip()
    data['q_duplicate_order_id'] = normalized_id.duplicated(keep=False) & ~data.q_missing_order_id
    for key in ['region_id', 'courier_id', 'aoi_id', 'aoi_type', 'city']:
        data['q_missing_'+key] = missing(raw[key])
    data['q_invalid_ds'] = data.ds_date.isna()
    data['q_ds_delivery_mismatch'] = (data.ds_date.notna() & data.delivery_ts.notna()
                                    & data.ds_date.ne(data.delivery_ts.dt.normalize()))
    data['q_negative_duration'] = data.delivery_minutes.lt(0)
    data['q_zero_duration'] = data.delivery_minutes.eq(0)
    data['q_cross_day'] = data.day_gap.gt(0)
    data['q_duration_gt24h'] = data.delivery_minutes.gt(1440)
    data['q_duration_gt30d'] = data.delivery_minutes.gt(30*1440)
    coordinates = {}
    for label, prefix in [('stop', ''), ('accept', 'accept_gps_'), ('delivery', 'delivery_gps_')]:
        la, ln, status = coordinate_status(raw[prefix+'lat'], raw[prefix+'lng'])
        coordinates[label] = (la, ln, status.eq('valid'))
        data[label+'_coordinate_status'] = status
        data['q_'+label+'_coordinate_invalid'] = ~status.eq('valid')
    for name, first, second in [('distance_km', 'stop', 'accept'),
                                ('delivery_stop_distance_km', 'stop', 'delivery'),
                                ('accept_delivery_displacement_km', 'accept', 'delivery')]:
        la1, ln1, ok1 = coordinates[first]
        la2, ln2, ok2 = coordinates[second]
        data[name] = haversine(la1, ln1, la2, ln2).where(ok1 & ok2)
    data['q_accept_distance_gt50km'] = data.distance_km.gt(50)
    data['q_delivery_distance_gt50km'] = data.delivery_stop_distance_km.gt(50)
    data['q_accept_gps_after_accept'] = data.accept_gps_lag_minutes.gt(0)
    data['accept_distance_available'] = (data.distance_km.notna() & data.accept_gps_ts.notna()
                                         & data.accept_ts.notna() & data.accept_gps_lag_minutes.le(0))
    data['distance_at_accept_km'] = data.distance_km.where(data.accept_distance_available)
    # Identity issues prevent an unambiguous fact grain. Other issues remain in Silver.
    data['silver_eligible'] = ~data.q_missing_order_id & ~data.q_duplicate_order_id
    data['duration_eligible'] = (data.silver_eligible & data.accept_ts.notna() & data.delivery_ts.notna()
                                 & data.delivery_minutes.gt(0))
    data['ml_distance_eligible'] = data.duration_eligible & data.accept_distance_available
    data['cohort_le24h'] = data.duration_eligible & data.delivery_minutes.le(1440)
    flags = [c for c in data.columns if c.startswith('q_')]
    data['quality_reasons'] = [';'.join(c[2:] for c, v in zip(flags, row) if v)
                               for row in data[flags].itertuples(index=False, name=None)]
    return data
