import numpy as np
import pandas as pd

from src.eta import paper_protocol as pp


def raw_frame():
    # 1 shipper, 1 ngày: nhận 3 đơn lúc 08:00, giao lần lượt 09:00, 09:30, 10:00
    base = dict(courier_id="1", ds="0601", aoi_type=1)
    rows = [dict(base, lat=37.30 + .01 * i, lng=121.40, accept_time="06-01 08:00:00",
                 delivery_time=f"06-01 {h}:00:00") for i, h in enumerate(["09", "09", "10"])]
    rows[1]["delivery_time"] = "06-01 09:30:00"
    return pd.DataFrame(rows)


def test_minute_and_label_range():
    df, stats, spd = pp.clean(raw_frame())
    assert df.fin.tolist() == [540, 570, 600]
    S, info = pp.build_samples(df, spd, {"0601": "train"})
    # truy vấn tại 09:00: còn 2 đơn (nhãn 30 và 60) -> hợp lệ; các truy vấn sau chỉ còn 1 đơn -> bị bỏ
    assert sorted(S.target_remaining_minutes.tolist()) == [30, 60]
    assert S.query_id.nunique() == 1


def test_labels_never_exceed_cap_and_are_positive():
    df, _, spd = pp.clean(raw_frame())
    S, _ = pp.build_samples(df, spd, {"0601": "train"})
    assert S.target_remaining_minutes.between(1, pp.LABEL_MAX).all()


def test_split_dates_disjoint():
    s = pp.split_dates([f"06{d:02d}" for d in range(1, 21)])
    assert set(s.values()) == {"train", "validation", "test"}
    assert list(s.values()).count("train") == 12


def test_query_grid_is_used_and_shared_by_both_protocols():
    from src.eta.dataset import build_eta_samples
    n = 20
    ts = pd.date_range("2022-06-01 08:00", periods=n, freq="10min")
    ev = pd.DataFrame({
        "order_id": [str(i) for i in range(n)], "courier_id": 1, "region_id": 1, "aoi_type": 2,
        "lat": 37.3, "lng": 121.4, "accept_ts": ts, "delivery_ts": ts + pd.Timedelta(minutes=45),
        "delivery_gps_lat": 37.31, "delivery_gps_lng": 121.41, "delivery_minutes": 45.0})
    ev = pd.concat([ev, ev.assign(order_id=ev.order_id + "x", accept_ts=ev.accept_ts + pd.Timedelta(days=1),
                                  delivery_ts=ev.delivery_ts + pd.Timedelta(days=1)),
                    ev.assign(order_id=ev.order_id + "y", accept_ts=ev.accept_ts + pd.Timedelta(days=2),
                              delivery_ts=ev.delivery_ts + pd.Timedelta(days=2))], ignore_index=True)
    a = build_eta_samples(ev, "A-lite", max_per_courier_day=5)
    b = build_eta_samples(ev, "B", max_per_courier_day=5)
    assert a.groupby("courier_day").size().max() == 5
    assert set(a.order_id) == set(b.order_id)            # common spine
    assert len(build_eta_samples(ev, "B", max_per_courier_day=None)) == len(ev)
