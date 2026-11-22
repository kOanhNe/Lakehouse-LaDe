"""Protocol P-PIT: kiểm tra leakage của Protocol P bằng đối chứng ghép cặp.

Giữ NGUYÊN tập cặp (truy vấn, đơn), nhãn, cửa sổ và cách chia ngày của Protocol P
(``paper_protocol.build_samples``). Chỉ thêm 3 cột tính theo point-in-time (chỉ dùng thông tin
có tại thời điểm truy vấn) để so với 3 feature đang phụ thuộc dữ liệu tương lai:

    spd   -> spd_pit    tốc độ shipper từ lịch sử TRƯỚC (và tại) truy vấn, không dùng cả file
    ntodo -> ntodo_pit  số đơn đang cầm = mọi đơn đã nhận, chưa giao (không lọc theo nhãn <= 120)
    rank  -> rank_pit   hạng khoảng cách trong TOÀN BỘ đơn đang cầm (không lọc theo nhãn)

Hai kiểm thử phản thực tế trong tests/test_paper_protocol.py đã chỉ ra 3 đường phụ thuộc này;
module này đo phần MAE do chúng gây ra (docs/leak_inventory.md ghi là "chưa đo").
"""
import numpy as np
import pandas as pd

from .paper_protocol import KEY, LABEL_MAX, T, FEATURES, haversine_m

PIT_COLUMNS = ["ntodo_pit", "rank_pit", "spd_pit"]
BASE_COLUMNS = ["split", "query_id", "target_remaining_minutes", *FEATURES]


def causal_speed(df: pd.DataFrame) -> pd.Series:
    """Tốc độ trung bình của shipper (m/phút) tích lũy tới dòng hiện tại.

    ``dis`` và ``tm`` của dòng i chỉ biết được tại thời điểm giao xong đơn i, tức tại thời điểm
    truy vấn. ``df`` đã sắp theo (ds, courier_id, delivery_time) nên cumsum theo shipper là theo thời gian.
    Cùng quy ước với ``clean``: thiếu lịch sử hoặc tốc độ <= 0 thì dùng 5.0.
    """
    g = df.groupby("courier_id", sort=False)
    dist, mins = g.dis.cumsum(), g.tm.cumsum()
    spd = pd.Series(np.where(mins > 0, dist / mins.replace(0, np.nan), 5.0), index=df.index).fillna(5.0)
    return spd.where(spd > 0, 5.0)


def build_samples_pit(df, spd, split, label_max=LABEL_MAX):
    """Giống ``paper_protocol.build_samples`` nhưng thêm ntodo_pit, rank_pit, spd_pit."""
    spd_causal = causal_speed(df).to_numpy()
    rows, zero, over, total = [], 0, 0, 0
    for (cid, ds), c in df.groupby(KEY, sort=False):
        part = split.get(ds)
        if part is None:
            continue
        L = len(c); fin, acc = c.fin.values, c.acc.values
        lat, lng, at = c.lat.values, c.lng.values, c.aoi_type.values
        spc = spd_causal[c.index.to_numpy()]
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
                # point-in-time: toàn bộ đơn đang cầm tại fin[i], không phụ thuộc nhãn tương lai
                d_all = haversine_m(lat[i], lng[i], lat[todo], lng[todo])
                rk_all = d_all.argsort().argsort()
                where = {j: k for k, j in enumerate(todo)}
                for k, (j, l) in enumerate(ok):
                    recs.append((part, f"{cid}_{ds}_{i}", l, d[k], fin[i] - acc[j], fin[i],
                                 i, len(ok), rk[k], at[j], spd[cid],
                                 len(todo), rk_all[where[j]], spc[i]))
            if not bad:
                rows += recs
    S = pd.DataFrame(rows, columns=[*BASE_COLUMNS, *PIT_COLUMNS])
    return S, {"pairs_candidate": total}


def variants():
    """Các bộ feature dùng để tách phần MAE do từng nguồn leakage."""
    swap = lambda f, new: [new if x == f else x for x in FEATURES]
    pit_both = [{"ntodo": "ntodo_pit", "rank": "rank_pit", "spd": "spd_pit"}.get(x, x) for x in FEATURES]
    return {
        "P (gốc, có leak)": FEATURES,
        "chỉ sửa spd": swap("spd", "spd_pit"),
        "chỉ sửa ntodo+rank": [{"ntodo": "ntodo_pit", "rank": "rank_pit"}.get(x, x) for x in FEATURES],
        "sửa cả ba (PIT)": pit_both,
    }
