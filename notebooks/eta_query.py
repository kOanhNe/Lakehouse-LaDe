"""Leakage-safe query-time ETA features used by notebook 06 and pandas tests."""
import numpy as np
import pandas as pd

def _haversine(la1, ln1, la2, ln2):
    a = np.sin(np.radians(la2-la1)/2)**2 + np.cos(np.radians(la1))*np.cos(np.radians(la2))*np.sin(np.radians(ln2-ln1)/2)**2
    return 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

def build_query_samples(frame, add_accept_queries=False):
    d = frame.copy(); d['accept_ts'] = pd.to_datetime(d['accept_ts']); d['delivery_ts'] = pd.to_datetime(d['delivery_ts'])
    d = d.sort_values(['courier_id','accept_ts','order_id']).reset_index(drop=True); d['day'] = d.accept_ts.dt.normalize(); out=[]
    for (courier, day), group in d.groupby(['courier_id','day'], sort=False):
        group=group.sort_values(['accept_ts','order_id']); points=[(t,'delivery') for t in group.delivery_ts]
        if add_accept_queries: points += [(t,'accept') for t in group.accept_ts]
        for t, point_type in sorted(set(points), key=lambda x:(x[0],x[1])):
            observed=group[group.accept_ts<=t]; pending=observed[observed.delivery_ts>t].copy()
            if pending.empty: continue
            delivered=group[group.delivery_ts<=t].sort_values(['delivery_ts','order_id']); last=delivered.iloc[-1] if len(delivered) else None
            first=observed.accept_ts.min(); last_ts=delivered.delivery_ts.max() if len(delivered) else pd.NaT
            if last is None: la,ln=pending.accept_gps_lat.iloc[0],pending.accept_gps_lng.iloc[0]
            else:
                la,ln=last.delivery_gps_lat,last.delivery_gps_lng
                if pd.isna(la) or pd.isna(ln): la,ln=last.lat,last.lng
            dist=_haversine(la,ln,pending.lat.to_numpy(float),pending.lng.to_numpy(float)); cent=_haversine(pending.lat.mean(),pending.lng.mean(),pending.lat.to_numpy(float),pending.lng.to_numpy(float)); order=np.argsort(dist,kind='stable'); rank=np.empty(len(order)); rank[order]=np.arange(1,len(order)+1)
            hist=group[group.delivery_ts<day]; hd=hist.get('distance_at_accept_km',pd.Series(index=hist.index,dtype=float)); speed=(hd/(hist.delivery_ts-hist.accept_ts).dt.total_seconds()*3600).replace([np.inf,-np.inf],np.nan).dropna(); speed=speed[speed.between(.1,100)]; speed=float(speed.median()) if len(speed) else np.nan
            for i,(_,row) in enumerate(pending.reset_index(drop=True).iterrows()):
                out.append({'order_id':row.order_id,'courier_id':courier,'query_ts':t,'query_point_type':point_type,'region_id':row.region_id,'aoi_type':row.aoi_type,'pending_count':len(pending),'delivered_count':len(delivered),'minutes_from_first_accept':(t-first).total_seconds()/60,'minutes_since_last_delivery':0 if pd.isna(last_ts) else (t-last_ts).total_seconds()/60,'query_hour':t.hour+t.minute/60,'query_dow':t.day_of_week,'distance_from_last_position_km':float(dist[i]),'distance_to_pending_centroid_km':float(cent[i]),'pending_distance_rank':float(rank[i]),'historical_speed_kmh':speed,'target_remaining_minutes':(row.delivery_ts-t).total_seconds()/60,'original_duration_minutes':(row.delivery_ts-row.accept_ts).total_seconds()/60,'cohort':'le24h' if (row.delivery_ts-row.accept_ts).total_seconds()<=86400 else 'gt24h'})
    return pd.DataFrame(out)
