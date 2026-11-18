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
