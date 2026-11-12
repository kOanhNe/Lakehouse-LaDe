"""Reproducible full-file audit. Run: python scripts/profile_delivery.py."""
import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'notebooks'))
from delivery_data import read_source, annotate, missing, POLICY_VERSION


def run(source, output, reference=None):
    output.mkdir(parents=True, exist_ok=True)
    tables = output/'tables'
    tables.mkdir(exist_ok=True)
    raw = read_source(source)
    data = annotate(raw)
    def save(name, frame):
        frame.to_csv(tables/(name+'.csv'), index=False, encoding='utf-8-sig')
    def n(mask):
        return int(mask.sum())
    def unique_group(keys, values):
        return raw.groupby(keys, dropna=False)[values].nunique().reset_index()

    profile = []
    for col in raw:
        nonmissing = raw.loc[~missing(raw[col]), col]
        profile.append(dict(column=col, missing=n(missing(raw[col])), unique=int(nonmissing.nunique()),
                            examples=' | '.join(nonmissing.drop_duplicates().head(5).tolist()),
                            top_value=nonmissing.mode().iloc[0] if len(nonmissing) else '',
                            top_count=int(nonmissing.value_counts().iloc[0]) if len(nonmissing) else 0))
    save('column_profile', pd.DataFrame(profile))
    flags = [c for c in data if c.startswith('q_')]
    save('quality_flags', pd.DataFrame([{'flag': c, 'rows': n(data[c]), 'percent': round(data[c].mean()*100, 4)} for c in flags]))
    metrics = ['delivery_minutes','distance_km','delivery_stop_distance_km','accept_delivery_displacement_km',
               'accept_gps_lag_minutes','delivery_gps_lag_minutes']
    quantiles = data[metrics].quantile([0,.01,.05,.25,.5,.75,.9,.95,.99,.999,1]).T
    quantiles.columns = ['min','p01','p05','p25','p50','p75','p90','p95','p99','p999','max']
    quantiles['nonmissing'] = data[metrics].count()
    save('quantiles', quantiles.rename_axis('metric').reset_index())
    save('day_gap', data.groupby('day_gap', dropna=False).size().rename('rows').reset_index())
    save('aoi_types', raw.groupby('aoi_type').size().rename('rows').reset_index())
    save('coordinate_status', pd.concat([data[c].value_counts().rename_axis('status').reset_index(name='rows').assign(pair=c)
                                       for c in ['stop_coordinate_status','accept_coordinate_status','delivery_coordinate_status']]))
    daily = data.assign(date=data.delivery_ts.dt.normalize()).groupby('date').agg(
        orders=('order_id','size'), couriers=('courier_id','nunique'), median_minutes=('delivery_minutes','median'),
        gps_invalid=('q_accept_coordinate_invalid','mean'), over24h=('q_duration_gt24h','mean')).reset_index()
    save('daily_delivery', daily)
    save('hourly_accept', data.assign(hour=data.accept_ts.dt.hour).groupby('hour').agg(
        orders=('order_id','size'), median_minutes=('delivery_minutes','median'), p90_minutes=('delivery_minutes',lambda s:s.quantile(.9)),
        over24h=('q_duration_gt24h','mean')).reset_index())
    data['accept_date'] = data.accept_ts.dt.normalize()
    courier_day = data.groupby(['courier_id','accept_date']).agg(orders=('order_id','size'), regions=('region_id','nunique'), aois=('aoi_id','nunique')).reset_index()
    save('courier_day', courier_day)
    save('courier_profile', data.groupby('courier_id').agg(orders=('order_id','size'), regions=('region_id','nunique'),
        days=('accept_date','nunique'), gps_invalid=('q_accept_coordinate_invalid','sum'), over24h=('q_duration_gt24h','sum'),
        median_minutes=('delivery_minutes','median')).reset_index().sort_values('gps_invalid', ascending=False))
    aoi = unique_group('aoi_id',['aoi_type','region_id','city'])
    save('aoi_relations', aoi)
    save('region_relations', unique_group('region_id',['city','courier_id','aoi_id']))
    # Count exact receiver coordinate pairs, not separate lat/lng cardinalities.
    aoi_coords = raw[['aoi_id','lat','lng']].drop_duplicates().groupby('aoi_id').size()
    batches = data.groupby(['courier_id','accept_ts']).size()
    split = np.where(data.accept_ts < pd.Timestamp('2022-10-01'), 'train', 'test')
    data['split'] = split
    save('split_profile', data.groupby('split').agg(rows=('order_id','size'),couriers=('courier_id','nunique'),
        median_minutes=('delivery_minutes','median'), gps_invalid=('q_accept_coordinate_invalid','mean'), over24h=('q_duration_gt24h','mean')).reset_index())
    train = data[data.split.eq('train') & data.duration_eligible]
    test = data[data.split.eq('test') & data.duration_eligible]
    baselines=[]
    for label, value in [('train_median',train.delivery_minutes.median()),('train_mean',train.delivery_minutes.mean())]:
        for cohort, mask in [('all_positive',pd.Series(True,index=test.index)),('le24h',test.delivery_minutes.le(1440)),('gt24h',test.delivery_minutes.gt(1440))]:
            errors=test.loc[mask,'delivery_minutes']-value
            baselines.append(dict(model=label,cohort=cohort,rows=len(errors),prediction=float(value),mae=float(errors.abs().mean()),rmse=float(np.sqrt((errors**2).mean()))))
    save('baseline_holdout',pd.DataFrame(baselines))
    cohorts = {'all':pd.Series(True,index=data.index), 'positive_duration':data.duration_eligible,
               'accept_gps_invalid':data.q_accept_coordinate_invalid, 'duration_gt24h':data.q_duration_gt24h,
               'accept_distance_gt50km':data.q_accept_distance_gt50km,
               'old_filter':data.duration_eligible & ~data.q_stop_coordinate_invalid & ~data.q_accept_coordinate_invalid & data.delivery_minutes.le(1440) & data.distance_km.le(50)}
    sensitivity=[]
    for name, mask in cohorts.items():
        d=data[mask]
        corr=d[['distance_km','delivery_minutes']].dropna()
        sensitivity.append(dict(cohort=name,rows=len(d),percent=len(d)/len(data)*100,couriers=d.courier_id.nunique(),
             median_minutes=d.delivery_minutes.median(),mean_minutes=d.delivery_minutes.mean(),p95_minutes=d.delivery_minutes.quantile(.95),
             pearson=corr.distance_km.corr(corr.delivery_minutes),spearman=corr.distance_km.rank().corr(corr.delivery_minutes.rank())))
    save('cohort_sensitivity',pd.DataFrame(sensitivity))
    old=cohorts['old_filter']
    save('old_filter_bias_by_courier',data.assign(old_retained=old).groupby('courier_id').agg(
        total=('order_id','size'),retained=('old_retained','sum'),retention_rate=('old_retained','mean')).reset_index().sort_values('retention_rate'))
    evidence=[]
    for flag in flags:
        sample=data.loc[data[flag]].sort_values('order_id').head(8).copy()
        sample.insert(0,'evidence_for',flag)
        evidence.append(sample)
    pd.concat(evidence).to_csv(tables/'anomaly_examples.csv',index=False,encoding='utf-8-sig')
    # Compact per-row audit, raw CSV is never rewritten.
    data[['source_row_number','order_id','quality_reasons','silver_eligible','duration_eligible',
          'ml_distance_eligible','distance_at_accept_km','delivery_minutes']].to_csv(output/'row_quality.csv.gz',index=False,compression='gzip')
    unknown_courier_mask = ~test.courier_id.isin(train.courier_id)
    unknown_aoi_mask = ~test.aoi_id.isin(train.aoi_id)
    summary = dict(policy=POLICY_VERSION,sha256=hashlib.sha256(source.read_bytes()).hexdigest(),rows=len(raw),columns=len(raw.columns),
        exact_duplicate_rows=int(raw.duplicated().sum()),unique={c:int(raw[c].nunique()) for c in ['order_id','city','courier_id','region_id','aoi_id','aoi_type']},
        time_ranges={c:[str(data[c].min()),str(data[c].max())] for c in ['accept_ts','delivery_ts','accept_gps_ts','delivery_gps_ts','ds_date']},
        flags={c:n(data[c]) for c in flags},
        ds_matches_accept=n(data.ds_date.eq(data.accept_ts.dt.normalize())),ds_matches_delivery=n(data.ds_date.eq(data.delivery_ts.dt.normalize())),
        accept_gps_same_event=n(data.accept_gps_ts.eq(data.accept_ts)),delivery_gps_same_event=n(data.delivery_gps_ts.eq(data.delivery_ts)),
        courier_days=len(courier_day),courier_days_multi_region=n(courier_day.regions.gt(1)),courier_days_multi_aoi=n(courier_day.aois.gt(1)),
        aois_multi_type=n(aoi.aoi_type.gt(1)),aois_multi_region=n(aoi.region_id.gt(1)),aois_multi_coordinate=n(aoi_coords.gt(1)),
        accept_batches=len(batches),max_accept_batch=int(batches.max()),rows_in_simultaneous_accept_batches=int(batches[batches.gt(1)].sum()),
        accept_nonzero_seconds=n(data.accept_ts.dt.second.ne(0)),delivery_nonzero_seconds=n(data.delivery_ts.dt.second.ne(0)),
        old_filter_retained=n(old),silver_retained=n(data.silver_eligible),duration_eligible=n(data.duration_eligible),ml_distance_eligible=n(data.ml_distance_eligible),
        unknown_test_courier_ids=int(test.loc[unknown_courier_mask, 'courier_id'].nunique()),
        unknown_test_courier_rows=int(unknown_courier_mask.sum()),
        unknown_test_aoi_ids=int(test.loc[unknown_aoi_mask, 'aoi_id'].nunique()),
        unknown_test_aoi_rows=int(unknown_aoi_mask.sum()))
    if reference and reference.exists():
        ref=pd.read_parquet(reference).astype('string')
        left=raw.set_index('order_id').sort_index()
        right=ref.set_index('order_id').sort_index()
        same_keys=left.index.equals(right.index)
        differences={}
        if same_keys:
            for col in left:
                a,b=left[col],right[col]
                if col not in ['city','accept_time','delivery_time','accept_gps_time','delivery_gps_time']:
                    x,y=pd.to_numeric(a,errors='coerce'),pd.to_numeric(b,errors='coerce')
                    equal=x.eq(y)|(x.isna()&y.isna())
                else:
                    equal=a.eq(b)|(a.isna()&b.isna())
                differences[col]=int((~equal.fillna(False)).sum())
        summary['official_reference']={'sha256':hashlib.sha256(reference.read_bytes()).hexdigest(),'rows':len(ref),'same_order_ids':same_keys,'different_rows_per_column':differences}
    (output/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    fig,axes=plt.subplots(2,2,figsize=(13,9))
    axes[0,0].hist(np.log1p(data.loc[data.duration_eligible,'delivery_minutes']),bins=80)
    axes[0,0].set(xlabel='log(1 + accept-to-completion minutes)',ylabel='Orders',title='Duration distribution (all positive labels)')
    axes[0,1].plot(daily.date,daily.orders);axes[0,1].tick_params(axis='x',rotation=30)
    axes[0,1].set(title='Orders by completion date',ylabel='Orders')
    axes[1,0].hist(np.log1p(data.distance_km.dropna()),bins=80)
    axes[1,0].set(xlabel='log(1 + straight-line km)',title='Accept GPS to stop (valid coordinates only)')
    rates=data.groupby('courier_id').q_accept_coordinate_invalid.mean().sort_values(ascending=False).head(20)
    rates.plot.bar(ax=axes[1,1]);axes[1,1].set(title='20 couriers with highest invalid accept-GPS rate',ylabel='Rate')
    fig.tight_layout();fig.savefig(output/'overview.png',dpi=160);plt.close(fig)
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--source',type=Path,default=ROOT/'data/delivery_yt.csv')
    parser.add_argument('--output',type=Path,default=ROOT/'reports/data')
    parser.add_argument('--reference',type=Path)
    args=parser.parse_args()
    run(args.source,args.output,args.reference)
