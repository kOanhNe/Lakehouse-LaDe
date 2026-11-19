"""Nhận xét tự động cho dashboard. Mọi con số đều tính từ bảng serving, không gõ cứng."""
from __future__ import annotations

import pandas as pd

PAPER = {  # Bài báo LaDe (arXiv 2306.10675), Yantai, bài toán ETA: MAE, RMSE, ACC@20 (%)
    "speed": (33.97, 40.27, 42.03),
    "lightgbm": (23.32, 27.82, 51.22),
    "M2G4RTP (tốt nhất)": (17.21, 19.71, 69.20),
}
LABELS = {"median_train": "Trung vị train", "speed": "SPEED", "lightgbm": "LightGBM"}


def spark_baseline_metrics(metrics: pd.DataFrame) -> pd.DataFrame:
    """Select rows evaluated on the Spark cohort, including its reference baselines."""
    return metrics.loc[metrics["cohort"].eq("test (<=24h, GPS hợp lệ)")]


def paper_summary(m: pd.DataFrame) -> pd.DataFrame:
    """Bảng rộng: mỗi mô hình một dòng, trung bình qua các seed."""
    wide = m.groupby(["model", "metric"]).value.mean().unstack()
    wide.index = [LABELS.get(i, i) for i in wide.index]
    return wide


def paper_insights(m: pd.DataFrame) -> list[str]:
    w = paper_summary(m)
    out = []
    need = {"mae", "rmse_paper", "acc20"}
    if not need <= set(w.columns) or not {"SPEED", "LightGBM"} <= set(w.index):
        return ["Chưa đủ metric để nhận xét."]
    sp, lg = w.loc["SPEED"], w.loc["LightGBM"]
    gain = (sp.mae - lg.mae) / sp.mae
    out.append(f"LightGBM sai trung bình {lg.mae:.1f} phút, thấp hơn SPEED ({sp.mae:.1f} phút) {gain:.0%}; "
               f"tỷ lệ đoán sai không quá 20 phút tăng từ {sp.acc20:.1f}% lên {lg.acc20:.1f}%.")
    ps, pl = PAPER["speed"], PAPER["lightgbm"]
    out.append(f"SPEED của đồ án lệch {abs(sp.mae - ps[0]):.1f} phút so với bài báo ({ps[0]}). "
               "Hai số gần nhau cho thấy quy trình lấy mẫu và chia dữ liệu bám sát bài báo.")
    if lg.mae < pl[0]:
        out.append(f"LightGBM của đồ án ({lg.mae:.1f}) tốt hơn LightGBM trong bài báo ({pl[0]}). "
                   "Chưa có code gốc của họ nên chưa kết luận do mô hình hay do feature khác nhau.")
    else:
        out.append(f"LightGBM của đồ án ({lg.mae:.1f}) chưa vượt LightGBM trong bài báo ({pl[0]}).")
    best = PAPER["M2G4RTP (tốt nhất)"][0]
    out.append(f"Mô hình tốt nhất trong bài báo (M2G4RTP) đạt {best} phút, còn thấp hơn đồ án {lg.mae - best:.1f} phút; "
               "mô hình đó dùng cả thứ tự giao của shipper, đồ án chưa mô hình hóa.")
    if "median_train" in w.index:
        md = w.loc["median_train"]
        out.append(f"Mốc 'luôn đoán trung vị' cho {md.mae:.1f} phút; SPEED ({sp.mae:.1f}) còn tệ hơn mốc này, "
                   "vì chỉ tính khoảng cách chia tốc độ mà bỏ qua việc shipper dừng gom đơn.")
    return out


def overview_insights(o: pd.Series, hourly: pd.DataFrame | None, region: pd.DataFrame | None) -> list[str]:
    out = []
    if {"median_delivery_minutes", "avg_delivery_minutes"} <= set(o.index):
        r = float(o["avg_delivery_minutes"]) / float(o["median_delivery_minutes"])
        out.append(f"Trung bình ({o['avg_delivery_minutes']:.0f} phút) gấp {r:.1f} lần trung vị "
                   f"({o['median_delivery_minutes']:.0f} phút): một số đơn rất dài kéo trung bình lên, nên dùng trung vị.")
    if hourly is not None and not hourly.empty:
        busy = hourly.loc[hourly.total_orders.idxmax()]
        slow = hourly.loc[hourly.median_delivery_minutes.idxmax()]
        out.append(f"Nhận nhiều đơn nhất lúc {int(busy.accept_hour)}h ({int(busy.total_orders):,} đơn); "
                   f"đơn nhận lúc {int(slow.accept_hour)}h có thời gian trung vị dài nhất ({slow.median_delivery_minutes:.0f} phút).")
    if region is not None and not region.empty:
        hi, lo = region.loc[region.late_rate.idxmax()], region.loc[region.late_rate.idxmin()]
        out.append(f"Khu vực {hi.region_id} có tỷ lệ chậm cao nhất ({hi.late_rate:.1%}), khu vực {lo.region_id} thấp nhất ({lo.late_rate:.1%}).")
    return out


def courier_insights(c: pd.DataFrame) -> list[str]:
    if c is None or c.empty:
        return []
    top = c.nlargest(max(1, len(c) // 10), "total_orders")
    share = top.total_orders.sum() / c.total_orders.sum()
    out = [f"10% shipper đông đơn nhất ({len(top)} người) xử lý {share:.0%} tổng số đơn."]
    small = int((c.total_orders < 100).sum())
    if small:
        out.append(f"{small} shipper có dưới 100 đơn; chỉ số của họ dao động mạnh, không nên xếp hạng theo tỷ lệ chậm.")
    return out
