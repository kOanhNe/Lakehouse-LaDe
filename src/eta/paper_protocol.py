"""Protocol P: ETA theo đúng quy trình LaDe (arXiv 2306.10675, thư mục time_prediction).

Bài toán: shipper vừa giao xong một đơn (thời điểm truy vấn t). Với mỗi đơn còn
đang cầm (đã nhận trước t, giao sau t) dự đoán còn bao nhiêu phút nữa sẽ giao.
Chỉ dùng pandas/numpy/lightgbm nên chạy và test được không cần Spark.

Khác bản gốc: dùng haversine thay geopy.geodesic (lệch rất nhỏ).
"""
import numpy as np
import pandas as pd

R = 6371008.8
T = 12          # số bước truy vấn tối đa mỗi cửa sổ
LABEL_MAX = 120  # nhãn hợp lệ: 0 < phút còn lại <= 120
INPUT_COLUMNS = ["courier_id", "ds", "lat", "lng", "aoi_type", "accept_time", "delivery_time"]
FEATURES = ["dis", "elapsed", "tmin", "ndone", "ntodo", "rank", "aoi", "spd"]
KEY = ["courier_id", "ds"]


def haversine_m(la1, ln1, la2, ln2):
    a = (np.sin(np.radians(la2 - la1) / 2) ** 2
         + np.cos(np.radians(la1)) * np.cos(np.radians(la2)) * np.sin(np.radians(ln2 - ln1) / 2) ** 2)
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _minute(s):  # 'MM-DD HH:MM:SS' -> phút trong ngày
    return s.str[6:8].astype(int) * 60 + s.str[9:11].astype(int)


def _to_last(d):
    gb = d.groupby(KEY, sort=False)
    dis = haversine_m(gb.lat.shift(), gb.lng.shift(), d.lat, d.lng).fillna(0)
    tm = (d.fin - gb.fin.shift()).fillna(0)
    return dis, tm


def clean(raw):
    """Lọc như preprocess_delivery.py. Trả về (df, thống kê, tốc độ shipper)."""
    df = raw[INPUT_COLUMNS].copy()
    df["courier_id"] = df.courier_id.astype(str)
    df["ds"] = df.ds.astype(str).str.zfill(4)
    for column in ("lat", "lng"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    # Bronze can expose AOI codes as strings, while LightGBM requires numeric features.
    df["aoi_type"] = pd.to_numeric(df["aoi_type"], errors="coerce").fillna(-1).astype("int32")
    df = df.drop_duplicates()
    df["fin"] = _minute(df.delivery_time)
    df["acc"] = _minute(df.accept_time)
    df.loc[df.accept_time.str[:5] != df.delivery_time.str[:5], "acc"] -= 1440
    df = df.sort_values(["ds", "courier_id", "delivery_time"], kind="stable").reset_index(drop=True)
    df["dis"], df["tm"] = _to_last(df)
    speed = np.where(df.tm == 0, 0, df.dis / df.tm.replace(0, np.nan))
    flag = pd.Series(speed > 500, index=df.index)
    nxt = flag.groupby([df.courier_id, df.ds], sort=False).shift(-1, fill_value=False)
    drop_a = flag & nxt
    df = df[~drop_a].copy()
    cs = df.groupby("courier_id").agg(d=("dis", "sum"), t=("tm", "sum"))
    cs["spd"] = np.where(cs.t > 0, cs.d / cs.t.replace(0, np.nan), 5.0)
    cs["spd"] = cs.spd.fillna(5.0).where(cs.spd > 0, 5.0)
    first = df.groupby(KEY, sort=False).cumcount() == 0
    drop_c = (~first) & (df.dis == 0) & (df.tm < 1)
    df = df[~drop_c].reset_index(drop=True)
    df["dis"], df["tm"] = _to_last(df)
    return df, {"raw": len(raw), "drop_step_A": int(drop_a.sum()), "drop_step_C": int(drop_c.sum()),
                "kept": len(df)}, cs.spd


def split_dates(dates):
    d = sorted(set(dates)); n = len(d)
    out = {x: "train" for x in d[:int(.6 * n)]}
    out.update({x: "validation" for x in d[int(.6 * n) + 1:int(.8 * n)]})
    out.update({x: "test" for x in d[int(.8 * n) + 1:]})
    return out


def build_samples(df, spd, split, label_max=LABEL_MAX):
    """Mỗi dòng = (một lần truy vấn, một đơn còn cầm). Giữ nguyên luật cửa sổ 24 đơn / T=12."""
    rows, zero, over, total = [], 0, 0, 0  # cặp bị loại: nhãn <=0 / nhãn > label_max
    for (cid, ds), c in df.groupby(KEY, sort=False):
        part = split.get(ds)
        if part is None:
            continue
        L = len(c); fin, acc = c.fin.values, c.acc.values
        lat, lng, at = c.lat.values, c.lng.values, c.aoi_type.values
        wn = 2 + (L - 24) // 12 if L >= 24 else 1
        for w in range(wn):
            s = w * 12; e = s + 24 if w < wn - 1 else L
            steps, bad, recs = 0, False, []
            for i in range(s, e):
                if steps >= T:
                    break
                todo = [j for j in range(i + 1, L) if acc[j] < fin[i]]
                lab = [fin[j] - fin[i] for j in todo]
                total += len(todo); zero += sum(1 for l in lab if l <= 0); over += sum(1 for l in lab if l > label_max)
                ok = [(j, l) for j, l in zip(todo, lab) if 0 < l <= label_max]
                if len(ok) < 2:
                    continue
                if any(j - s >= 24 for j, _ in ok):
                    if part == "test":
                        bad = True; break
                    ok = [(j, l) for j, l in ok if j - s < 24]
                    if len(ok) < 2:
                        continue
                steps += 1
                idx = [j for j, _ in ok]
                d = haversine_m(lat[i], lng[i], lat[idx], lng[idx])
                rk = d.argsort().argsort()
                for k, (j, l) in enumerate(ok):
                    recs.append((part, f"{cid}_{ds}_{i}", l, d[k], fin[i] - acc[j], fin[i],
                                 i, len(ok), rk[k], at[j], spd[cid]))
            if not bad:
                rows += recs
    S = pd.DataFrame(rows, columns=["split", "query_id", "target_remaining_minutes", *FEATURES])
    return S, {"pairs_candidate": total,
               "pairs_removed_by_label_filter": round((zero + over) / max(total, 1), 4),
               "removed_label_zero": round(zero / max(total, 1), 4),
               f"removed_label_over_{label_max}": round(over / max(total, 1), 4)}


def metrics(S, pred):
    err = (pred - S.target_remaining_minutes).abs()
    q = pd.DataFrame({"q": S.query_id.values, "se": ((pred - S.target_remaining_minutes) ** 2).values}).groupby("q").se.agg(["mean", "size"])
    rmse_q = float((np.sqrt(q["mean"]) * q["size"]).sum() / q["size"].sum())  # kiểu bài báo
    return {"mae": float(err.mean()), "rmse_pooled": float(np.sqrt(((pred - S.target_remaining_minutes) ** 2).mean())),
            "rmse_paper": rmse_q, "acc20": float(100 * (err <= 20).mean()),
            "acc30": float(100 * (err <= 30).mean())}


def run(raw, seeds=(42, 43, 44), label_max=LABEL_MAX):
    """Trả về (bảng metric dài, thống kê lọc, bảng dự đoán test của seed đầu)."""
    from .model import fit_lightgbm
    df, stats, spd = clean(raw)
    S, info = build_samples(df, spd, split_dates(df.ds), label_max)
    tr, va, te = (S[S["split"] == k] for k in ("train", "validation", "test"))
    y = "target_remaining_minutes"
    rows = []
    def add(model, seed, m):
        rows.extend((model, seed, k, float(v)) for k, v in m.items())
    add("median_train", 0, metrics(te, pd.Series(tr[y].median(), index=te.index)))
    add("speed", 0, metrics(te, (te.dis / te.spd).clip(lower=0)))
    scored = None
    for seed in seeds:
        m = fit_lightgbm(tr, va, FEATURES, seed=seed)
        pred = np.maximum(m.predict(te[FEATURES]), 0)
        add("lightgbm", seed, metrics(te, pd.Series(pred, index=te.index)))
        if scored is None:
            scored = te[["query_id", y]].assign(prediction_minutes=pred)
    stats.update(info, pairs={k: int((S["split"] == k).sum()) for k in ("train", "validation", "test")})
    return pd.DataFrame(rows, columns=["model", "seed", "metric", "value"]), stats, scored
