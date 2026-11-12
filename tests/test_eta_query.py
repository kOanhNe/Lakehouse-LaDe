import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
from eta_query import build_query_samples

def fixture():
    return pd.DataFrame([
        dict(order_id='a',courier_id=1,region_id=1,aoi_type=1,accept_ts='2022-08-01 08:00',delivery_ts='2022-08-01 10:00',lat=37,lng=121,accept_gps_lat=37,accept_gps_lng=121,delivery_gps_lat=37.1,delivery_gps_lng=121.1,distance_at_accept_km=1),
        dict(order_id='b',courier_id=1,region_id=1,aoi_type=2,accept_ts='2022-08-01 08:05',delivery_ts='2022-08-01 12:00',lat=37.2,lng=121.2,accept_gps_lat=37,accept_gps_lng=121,delivery_gps_lat=37.2,delivery_gps_lng=121.2,distance_at_accept_km=2),
        dict(order_id='c',courier_id=1,region_id=1,aoi_type=3,accept_ts='2022-08-01 08:10',delivery_ts='2022-08-01 13:00',lat=37.3,lng=121.3,accept_gps_lat=37,accept_gps_lng=121,delivery_gps_lat=37.3,delivery_gps_lng=121.3,distance_at_accept_km=3)])

def test_pending_count_and_target():
    out=build_query_samples(fixture()); x=out[out.query_ts==pd.Timestamp('2022-08-01 10:00')]
    assert set(x.order_id)=={'b','c'} and x.pending_count.eq(2).all() and x.delivered_count.eq(1).all()
    assert x.target_remaining_minutes.tolist()==[120.,180.]

def test_future_delivery_does_not_change_features():
    base=build_query_samples(fixture()); changed=fixture(); changed.loc[changed.order_id=='c','delivery_ts']='2022-08-01 18:00'; alt=build_query_samples(changed)
    cols=['order_id','query_ts','pending_count','delivered_count','minutes_from_first_accept','minutes_since_last_delivery','distance_from_last_position_km','distance_to_pending_centroid_km','pending_distance_rank']; filt=lambda x:x[x.order_id.isin(['b','c']) & (x.query_ts==pd.Timestamp('2022-08-01 10:00'))][cols].sort_values('order_id').reset_index(drop=True)
    pd.testing.assert_frame_equal(filt(base),filt(alt))
