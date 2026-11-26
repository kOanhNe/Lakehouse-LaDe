"""Đo ảnh hưởng của feature ``courier_train_avg_min`` trong baseline Spark LR/GBT cũ.

Baseline cũ tính trung bình thời gian giao theo shipper trên TOÀN BỘ tập train rồi gán lại cho
chính các dòng train (target encoding trong mẫu: nhãn của dòng nằm trong feature của nó). Script này dựng
lại đúng cohort ML (train/test khớp số dòng của Spark) và so sánh bằng LightGBM:

  1. không có feature đó
  2. cách của baseline cũ (trung bình trên toàn train, gồm chính dòng)
  3. chỉ dùng đơn của shipper đã GIAO XONG trước thời điểm nhận đơn (point-in-time)

Đây là xấp xỉ bằng pandas + LightGBM, không phải Spark GBT, nên chỉ so sánh tương đối giữa 3 dòng.

    python -m scripts.checks.check_courier_avg_leak --source data/delivery_yt.csv
"""
import argparse, warnings
from pathlib import Path
import lightgbm as lgb
import numpy as np
import pandas as pd

from lakehouse import PROJECT_ROOT as ROOT
Y = "delivery_minutes"
BASE = ["distance_km", "accept_hour", "dow", "order_rank", "cum_avg_dist", "aoi_type", "region_id"]


def haversine_km(la1, ln1, la2, ln2):
    a = np.sin(np.radians(la2 - la1) / 2) ** 2 + np.cos(np.radians(la1)) * np.cos(np.radians(la2)) * np.sin(np.radians(ln2 - ln1) / 2) ** 2
    return 2 * 6371.0 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def load_cohort(source, year="2022-"):
    d = pd.read_csv(source)
    for col, src in (("accept_ts", "accept_time"), ("delivery_ts", "delivery_time")):
        d[col] = pd.to_datetime(year + d[src], format="%Y-%m-%d %H:%M:%S", errors="coerce")
    d[Y] = (d.delivery_ts - d.accept_ts).dt.total_seconds() / 60
    d = d[d[Y] > 0].copy()
    ok = (d.accept_gps_lat.between(-90, 90) & d.accept_gps_lng.between(-180, 180)
          & ~((d.accept_gps_lat == 0) & (d.accept_gps_lng == 0)))
    d["distance_km"] = np.where(ok, haversine_km(d.lat, d.lng, d.accept_gps_lat, d.accept_gps_lng), np.nan)
    ml = d[ok & (d[Y] <= 1440) & (d.distance_km <= 50)].copy()
    ml["order_date"] = ml.accept_ts.dt.normalize()
    ml = ml.sort_values(["courier_id", "order_date", "accept_ts", "order_id"])
    g = ml.groupby(["courier_id", "order_date"], sort=False)
    ml["order_rank"] = g.cumcount() + 1
    ml["cum_avg_dist"] = g.distance_km.transform(lambda s: s.expanding().mean())
    ml["accept_hour"] = ml.accept_ts.dt.hour + ml.accept_ts.dt.minute / 60
    ml["dow"] = ml.order_date.dt.dayofweek
    return ml


def past_mean(frame, fallback):
    """Trung bình thời gian giao của các đơn cùng shipper đã giao xong trước lúc nhận đơn."""
    out = pd.Series(np.nan, index=frame.index)
    for _, grp in frame.groupby("courier_id"):
        dl = grp.delivery_ts.to_numpy(); order = np.argsort(dl)
        cum = np.concatenate([[0.0], np.cumsum(grp[Y].to_numpy()[order])])
        k = np.searchsorted(dl[order], grp.accept_ts.to_numpy(), side="right")
        out.loc[grp.index] = np.where(k > 0, cum[k] / np.maximum(k, 1), np.nan)
    return out.fillna(fallback)


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--train-end", default="2022-10-01")
    a = ap.parse_args()

    ml = load_cohort(a.source)
    cut = pd.Timestamp(a.train_end)
    tr, te = ml[ml.order_date < cut].copy(), ml[ml.order_date >= cut].copy()
    print(f"train={len(tr)} test={len(te)} | median baseline MAE={np.abs(te[Y] - tr[Y].median()).mean():.2f}")

    gavg = tr[Y].mean()
    by_courier = tr.groupby("courier_id")[Y].mean()
    tr["cta_insample"] = tr.courier_id.map(by_courier)
    te["cta_insample"] = te.courier_id.map(by_courier).fillna(gavg)
    pm = past_mean(pd.concat([tr, te]), gavg)
    tr["cta_past"], te["cta_past"] = pm.loc[tr.index], pm.loc[te.index]

    def run(label, col):
        cols = BASE + ([] if col is None else [col])
        model = lgb.LGBMRegressor(n_estimators=300, learning_rate=.05, num_leaves=31, objective="regression_l1",
                                  random_state=42, verbosity=-1, n_jobs=2).fit(tr[cols], tr[Y])
        err = model.predict(te[cols]) - te[Y]
        print(f"{label:62s} MAE={err.abs().mean():7.2f} RMSE={np.sqrt((err ** 2).mean()):7.2f}")

    run("không có feature trung bình theo shipper", None)
    run("trung bình trên toàn train, gồm chính dòng (baseline cũ)", "cta_insample")
    run("chỉ từ đơn đã giao xong trước lúc nhận đơn (point-in-time)", "cta_past")
