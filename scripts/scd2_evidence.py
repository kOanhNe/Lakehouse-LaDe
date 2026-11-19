"""Số liệu chứng minh vì sao cần SCD2 cho shipper (không phải chứng minh ETA tốt hơn).

Đo: nếu dùng "khu vực hiện tại" (bản ghi mới nhất, kiểu bảng không có lịch sử) thay vì
khu vực tại ngày nhận đơn (SCD2), bao nhiêu đơn bị gán sai khu vực?
    python scripts/scd2_evidence.py
"""
import argparse, json, sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def evidence(df: pd.DataFrame) -> dict:
    d = df[["order_id", "courier_id", "region_id", "accept_time"]].copy()
    d["day"] = d.accept_time.str[:5]
    # khu vực chiếm ưu thế mỗi (shipper, ngày); hòa thì lấy region_id nhỏ nhất (giống courier_history)
    cnt = d.groupby(["courier_id", "day", "region_id"]).size().rename("n").reset_index()
    cnt["region_num"] = cnt.region_id.astype(int)
    daily = (cnt.sort_values(["courier_id", "day", "n", "region_num"], ascending=[True, True, False, True])
             .drop_duplicates(["courier_id", "day"]).sort_values(["courier_id", "day"]))
    daily["prev"] = daily.groupby("courier_id").region_id.shift()
    changes = daily[daily.prev.notna() & (daily.prev != daily.region_id)]
    scd2_rows = int((daily.prev.isna() | (daily.prev != daily.region_id)).sum())
    latest = daily.drop_duplicates("courier_id", keep="last").set_index("courier_id").region_id
    dom = daily.set_index(["courier_id", "day"]).region_id
    d["scd2_region"] = [dom.get((c, y)) for c, y in zip(d.courier_id, d.day)]
    d["latest_region"] = d.courier_id.map(latest)
    wrong = (d.scd2_region != d.latest_region)
    moved = changes.courier_id.nunique()
    n_c = d.courier_id.nunique()
    return {
        "couriers": int(n_c), "couriers_changed_region": int(moved),
        "couriers_changed_region_pct": round(100 * moved / n_c, 1),
        "region_change_events": int(len(changes)), "scd2_rows": scd2_rows,
        "orders": int(len(d)), "orders_mislabeled_if_latest_region": int(wrong.sum()),
        "orders_mislabeled_if_latest_region_pct": round(100 * wrong.mean(), 1),
        "reading": "Nếu chỉ lưu khu vực hiện tại, tỉ lệ đơn trên bị gán sai khu vực của shipper tại thời điểm nhận đơn. "
                   "Đây là lý do dùng SCD2; chưa chứng minh SCD2 làm ETA chính xác hơn.",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "delivery_yt.csv")
    ap.add_argument("--output", type=Path, default=ROOT / "reports" / "scd2_evidence.json")
    a = ap.parse_args()
    res = evidence(pd.read_csv(a.source, dtype=str))
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(res, ensure_ascii=False, indent=2))
