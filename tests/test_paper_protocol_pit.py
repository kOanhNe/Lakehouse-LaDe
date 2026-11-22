"""Kiểm thử phản thực tế cho các cột point-in-time của Protocol P-PIT."""
import pandas as pd

from src.eta import paper_protocol as pp
from src.eta import paper_protocol_pit as pit


def raw_frame():
    # 1 shipper, 1 ngày: nhận 3 đơn lúc 08:00, giao lần lượt 09:00, 09:30, 10:00
    base = dict(courier_id="1", ds="0601", aoi_type=1)
    rows = [dict(base, lat=37.30 + .01 * i, lng=121.40, accept_time="06-01 08:00:00",
                 delivery_time=f"06-01 {h}:00:00") for i, h in enumerate(["09", "09", "10"])]
    rows[1]["delivery_time"] = "06-01 09:30:00"
    return pd.DataFrame(rows)


def test_original_columns_unchanged_by_pit_builder():
    df, _, spd = pp.clean(raw_frame())
    split = {"0601": "train"}
    base, _ = pp.build_samples(df, spd, split)
    extended, _ = pit.build_samples_pit(df, spd, split)
    pd.testing.assert_frame_equal(extended[pit.BASE_COLUMNS].reset_index(drop=True), base.reset_index(drop=True))


def test_spd_pit_ignores_future_days_but_paper_spd_does_not():
    today = raw_frame()
    tomorrow = today.assign(
        ds="0602", accept_time=today.accept_time.str.replace("06-01", "06-02"),
        delivery_time=today.delivery_time.str.replace("06-01", "06-02"),
        lat=[37.30, 37.36, 37.41],
    )
    split = {"0601": "train"}
    d0, _, s0 = pp.clean(today)
    d1, _, s1 = pp.clean(pd.concat([today, tomorrow]))
    before, _ = pit.build_samples_pit(d0, s0, split)
    after, _ = pit.build_samples_pit(d1, s1, split)
    assert (after.spd > before.spd).all()              # tốc độ cả file bị ngày sau kéo đi
    pd.testing.assert_series_equal(before.spd_pit.reset_index(drop=True), after.spd_pit.reset_index(drop=True))


def test_ntodo_and_rank_pit_ignore_future_label_cap():
    raw = raw_frame().assign(aoi_type=[1, 2, 3])
    near = raw.iloc[[1]].assign(lat=37.305, aoi_type=4, delivery_time="06-01 09:45:00")
    split = {"0601": "train"}

    def first_query(extra):
        ordered, _, spd = pp.clean(pd.concat([raw, extra]))
        samples, _ = pit.build_samples_pit(ordered, spd, split)
        return samples.loc[samples.query_id.eq("1_0601_0") & samples.aoi.eq(2)].iloc[0]

    near_completion = first_query(near)
    late_completion = first_query(near.assign(delivery_time="06-01 12:30:00"))   # nhãn 210 > 120
    assert (near_completion.ntodo, late_completion.ntodo) == (3, 2)             # feature gốc rò rỉ
    assert (near_completion["rank"], late_completion["rank"]) == (1, 0)
    assert near_completion.ntodo_pit == late_completion.ntodo_pit == 3         # PIT không đổi
    assert near_completion.rank_pit == late_completion.rank_pit == 1


def test_causal_speed_uses_only_rows_up_to_current():
    df, _, _ = pp.clean(raw_frame())
    full = pit.causal_speed(df)
    head = pit.causal_speed(df.iloc[:2])
    pd.testing.assert_series_equal(full.iloc[:2], head)
